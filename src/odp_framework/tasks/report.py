"""``report`` task of the job (python_wheel_task, entry point ``odp-report``).

Always runs at the end (``run_if: ALL_DONE``):

1. Receives the container configuration (``config_json``) and reads the active
   tables from the registry.
2. Reconciles the recent manifests with the rows loaded into bronze per run.
3. Stores the findings in ``odp_run_findings`` and fails the job if there are
   errors of its own or from the ``register`` task of the same run (this
   triggers the notifications).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from ..findings import ERROR, FINDINGS_SCHEMA_DDL, WARN, Finding, findings_rows, has_errors, utc_now
from ..reconcile import evaluate
from ..registry import load_active_entities, load_rejected_runs
from ..runtime import RUN_ID_COLUMN, RuntimeConfig
from ..sql import sql_literal
from ._common import base_parser, exit_with, get_spark, resolve_config

_MANIFEST_COLUMNS = (
    "run_id",
    "row_count",
    "_ingested_at",
    "load_spec.target_table AS entity",
    "load_spec.load_type AS load_type",
    "load_spec.source_path AS source_path",
)


def _read_manifests(spark, cfg: RuntimeConfig, since: datetime) -> list[dict]:
    if not spark.catalog.tableExists(cfg.manifest_fqn):
        return []
    since_lit = since.strftime("%Y-%m-%d %H:%M:%S")
    rows = (
        spark.read.table(cfg.manifest_fqn)
        .where(f"_ingested_at >= timestamp'{since_lit}'")
        .selectExpr(*_MANIFEST_COLUMNS)
        .collect()
    )
    return [r.asDict() for r in rows]


def _bronze_counts(spark, cfg: RuntimeConfig, manifests: list[dict]) -> dict[tuple[str, str], int]:
    runs_by_entity: dict[str, set[str]] = {}
    for m in manifests:
        if cfg.entity_by_name(m.get("entity") or "") and m.get("run_id"):
            runs_by_entity.setdefault(m["entity"], set()).add(m["run_id"])
    counts: dict[tuple[str, str], int] = {}
    for name, run_ids in runs_by_entity.items():
        fqn = cfg.bronze_fqn(cfg.entity_by_name(name))
        if not spark.catalog.tableExists(fqn):
            continue
        in_list = ", ".join(sql_literal(r) for r in sorted(run_ids))
        for row in (
            spark.read.table(fqn)
            .where(f"{RUN_ID_COLUMN} IN ({in_list})")
            .groupBy(RUN_ID_COLUMN)
            .count()
            .collect()
        ):
            counts[(name, row[RUN_ID_COLUMN])] = int(row["count"])
    return counts


def _register_errors(spark, cfg: RuntimeConfig, job_run_id: str) -> int:
    if not job_run_id or not spark.catalog.tableExists(cfg.findings_fqn):
        return 0
    return (
        spark.read.table(cfg.findings_fqn)
        .where(f"job_run_id = {sql_literal(job_run_id)} AND phase = 'register' AND severity = '{ERROR}'")
        .count()
    )


def run(spark, cfg: RuntimeConfig, job_run_id: str = "", now: datetime | None = None) -> list[Finding]:
    now = now or utc_now()
    cfg = cfg.with_entities(load_active_entities(spark, cfg))
    manifests = _read_manifests(spark, cfg, now - timedelta(days=cfg.report_lookback_days))
    findings = evaluate(cfg, manifests, _bronze_counts(spark, cfg, manifests), now,
                        rejected=load_rejected_runs(spark, cfg))
    if findings:
        (
            spark.createDataFrame(
                findings_rows(findings, now=now, job_run_id=job_run_id, phase="report",
                              container=cfg.container),
                schema=FINDINGS_SCHEMA_DDL,
            )
            .write.mode("append")
            .saveAsTable(cfg.findings_fqn)
        )
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = base_parser("odp-report")
    parser.add_argument("--fail_on_error", default="true")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    cfg = resolve_config(args, parser)

    spark = get_spark()
    findings = run(spark, cfg, job_run_id=args.job_run_id)
    register_errors = _register_errors(spark, cfg, args.job_run_id)

    errors = [f for f in findings if f.severity == ERROR]
    warnings = [f for f in findings if f.severity == WARN]
    print(f"[odp-report] {cfg.container}: {len(errors)} errors, {len(warnings)} warnings "
          f"(+{register_errors} errors in register)")
    for f in findings:
        if f.severity in (ERROR, WARN):
            print(f"  {f.severity:5} {f.code:20} {f.entity or '-'} {f.run_id or '-'}: {f.detail}")

    if (has_errors(findings) or register_errors) and args.fail_on_error.lower() == "true":
        print(f"[odp-report] Details in {cfg.findings_fqn}")
        return 1
    return 0


def entrypoint() -> None:  # pragma: no cover - used by python_wheel_task
    exit_with(main(), "odp-report")
