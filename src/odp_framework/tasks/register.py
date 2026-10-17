"""``register`` task of the job (python_wheel_task, entry point ``odp-register``).

First task of every run:

1. Auto Loader (availableNow) reads the new manifests from ``_manifests/`` and
   appends them to ``odp_manifests``. Fixed checkpoint in the ``odp_system`` Volume.
2. Rebuilds the ``odp_entities`` registry from all manifests
   (``odp_framework.registry``) and overwrites it.
3. Decides which tables to load: the active ones with a manifest newer than
   their last successful load (``odp_load_log``). Tables that failed before are
   picked up automatically.
4. Rebuilds ``odp_rejected_runs``: runs whose files sit in a table's folder but
   whose manifest is rejected. Bronze loads them (Auto Loader reads the whole
   folder); silver excludes them.
5. Publishes the task values used by the job: ``entities_to_load`` (for_each
   input), ``load_count`` (condition) and ``active_entities``.
6. Stores the findings (invalid manifests, conflicts, pending tables).

An invalid or conflicting manifest does not stop the other tables: it is
reported, and the ``report`` task fails the job at the end.
"""

from __future__ import annotations

import sys
from datetime import datetime

from ..findings import ERROR, FINDINGS_SCHEMA_DDL, WARN, Finding, findings_rows, has_errors, utc_now
from ..ingest import SUCCESS
from ..manifest import MANIFEST_SCHEMA_DDL
from ..registry import (
    ACTIVE,
    REGISTRY_SCHEMA_DDL,
    REJECTED_RUNS_SCHEMA_DDL,
    build_registry,
    entities_to_load,
    registry_rows,
    rejected_rows,
    rejected_runs,
)
from ..runtime import INGESTED_AT_COLUMN, RuntimeConfig
from ._common import base_parser, exit_with, fit_task_value, get_spark, resolve_config, set_task_value


def ingest_manifests(spark, cfg: RuntimeConfig) -> None:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("pathGlobFilter", "*.json")
        .schema(MANIFEST_SCHEMA_DDL)
    )
    if cfg.file_events:
        reader = reader.option("cloudFiles.useManagedFileEvents", "true")
    query = (
        reader.load(cfg.manifests_root)
        .selectExpr("*", "_metadata.file_path AS _manifest_file",
                    f"current_timestamp() AS {INGESTED_AT_COLUMN}")
        .writeStream.option("checkpointLocation", cfg.manifest_checkpoint)
        .trigger(availableNow=True)
        .toTable(cfg.manifest_fqn)
    )
    query.awaitTermination()


def read_all_manifests(spark, cfg: RuntimeConfig) -> list[dict]:
    if not spark.catalog.tableExists(cfg.manifest_fqn):
        return []
    return [r.asDict(recursive=True) for r in spark.read.table(cfg.manifest_fqn).collect()]


def last_successful_loads(spark, cfg: RuntimeConfig) -> dict[str, datetime]:
    if not spark.catalog.tableExists(cfg.load_log_fqn):
        return {}
    rows = (
        spark.read.table(cfg.load_log_fqn)
        .where(f"status = '{SUCCESS}'")
        .groupBy("entity")
        .agg({"started_at": "max"})
        .collect()
    )
    return {r["entity"]: r["max(started_at)"] for r in rows}


def run(spark, cfg: RuntimeConfig, job_run_id: str = "",
        now: datetime | None = None) -> tuple[list[str], int, list[Finding]]:
    """Returns (tables to load, number of active tables, findings)."""
    started = now or utc_now()
    ingest_manifests(spark, cfg)
    manifests = read_all_manifests(spark, cfg)
    entries, findings = build_registry(cfg, manifests, since=started)

    (
        spark.createDataFrame(registry_rows(entries), schema=REGISTRY_SCHEMA_DDL)
        .write.mode("overwrite")
        .saveAsTable(cfg.registry_fqn)
    )
    # Before silver: the pipeline reads this list when it defines its graph.
    (
        spark.createDataFrame(rejected_rows(rejected_runs(cfg, manifests, entries)),
                              schema=REJECTED_RUNS_SCHEMA_DDL)
        .write.mode("overwrite")
        .saveAsTable(cfg.rejected_runs_fqn)
    )

    to_load, pending = fit_task_value(entities_to_load(entries, last_successful_loads(spark, cfg)))
    if pending:
        findings.append(Finding(WARN, "LOAD_LIST_TRUNCATED", None, None,
                                f"{len(pending)} tables left for the next run "
                                f"(for_each limit). First: {pending[0]}"))
    if findings:
        (
            spark.createDataFrame(
                findings_rows(findings, now=started, job_run_id=job_run_id, phase="register",
                              container=cfg.container),
                schema=FINDINGS_SCHEMA_DDL,
            )
            .write.mode("append")
            .saveAsTable(cfg.findings_fqn)
        )
    active = sum(1 for e in entries.values() if e.status == ACTIVE)
    return to_load, active, findings


def main(argv: list[str] | None = None) -> int:
    parser = base_parser("odp-register")
    parser.add_argument("--fail_on_error", default="true")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    cfg = resolve_config(args, parser)

    to_load, active, findings = run(get_spark(), cfg, job_run_id=args.job_run_id)
    set_task_value("entities_to_load", to_load)
    set_task_value("load_count", len(to_load))
    set_task_value("active_entities", active)

    print(f"[odp-register] {cfg.container}: {active} active tables, {len(to_load)} to load, "
          f"{sum(f.severity == ERROR for f in findings)} errors, {sum(f.severity == WARN for f in findings)} warnings")
    for f in findings:
        if f.severity in (ERROR, WARN):
            print(f"  {f.severity:5} {f.code:28} {f.entity or '-'} {f.run_id or '-'}: {f.detail}")
    # Errors do not stop the other tables from loading: report (run_if
    # ALL_DONE) surfaces them. If there is nothing to load, the job never
    # reaches report, so it fails here.
    if has_errors(findings) and not to_load and args.fail_on_error.lower() == "true":
        return 1
    return 0


def entrypoint() -> None:  # pragma: no cover - used by python_wheel_task
    exit_with(main(), "odp-register")
