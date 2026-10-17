"""Tarea ``register`` del job (python_wheel_task, entry point ``odp-register``).

Primera tarea de cada ejecución:

1. Auto Loader (availableNow) lee los manifiestos nuevos de ``_manifests/`` y
   los añade a ``odp_manifests``. Checkpoint fijo en el Volume ``odp_system``.
2. Recalcula el registro ``odp_entities`` desde todos los manifiestos
   (``odp_framework.registry``) y lo sobrescribe.
3. Decide qué tablas cargar: las activas con algún manifiesto posterior a su
   última carga correcta (``odp_load_log``). Las que fallaron antes entran solas.
4. Recalcula ``odp_rejected_runs``: runs cuyos ficheros están en la carpeta de
   una tabla pero cuyo manifiesto se rechaza. Bronze los carga (Auto Loader lee
   la carpeta entera); silver los excluye.
5. Publica los task values que usa el job: ``entities_to_load`` (entrada del
   for_each), ``load_count`` (condición) y ``active_entities``.
6. Guarda los hallazgos (manifiestos inválidos, conflictos, pendientes).

Un manifiesto inválido o en conflicto no para el resto de tablas: se avisa, y
la tarea ``report`` hace fallar el job al final.
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
    """Devuelve (tablas a cargar, nº de tablas activas, hallazgos)."""
    started = now or utc_now()
    ingest_manifests(spark, cfg)
    manifests = read_all_manifests(spark, cfg)
    entries, findings = build_registry(cfg, manifests, since=started)

    (
        spark.createDataFrame(registry_rows(entries), schema=REGISTRY_SCHEMA_DDL)
        .write.mode("overwrite")
        .saveAsTable(cfg.registry_fqn)
    )
    # Antes que silver: la pipeline lee esta lista al definir su grafo.
    (
        spark.createDataFrame(rejected_rows(rejected_runs(cfg, manifests, entries)),
                              schema=REJECTED_RUNS_SCHEMA_DDL)
        .write.mode("overwrite")
        .saveAsTable(cfg.rejected_runs_fqn)
    )

    to_load, pending = fit_task_value(entities_to_load(entries, last_successful_loads(spark, cfg)))
    if pending:
        findings.append(Finding(WARN, "LOAD_LIST_TRUNCATED", None, None,
                                f"{len(pending)} tablas quedan para la siguiente ejecución "
                                f"(límite del for_each). Primera: {pending[0]}"))
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

    print(f"[odp-register] {cfg.container}: {active} tablas activas, {len(to_load)} a cargar, "
          f"{sum(f.severity == ERROR for f in findings)} errores, {sum(f.severity == WARN for f in findings)} avisos")
    for f in findings:
        if f.severity in (ERROR, WARN):
            print(f"  {f.severity:5} {f.code:28} {f.entity or '-'} {f.run_id or '-'}: {f.detail}")
    # Los errores no paran la carga del resto de tablas: report (run_if
    # ALL_DONE) los hace visibles. Si no hay nada que cargar, el job no llega
    # a report, así que se falla aquí.
    if has_errors(findings) and not to_load and args.fail_on_error.lower() == "true":
        return 1
    return 0


def entrypoint() -> None:  # pragma: no cover - usado por python_wheel_task
    exit_with(main(), "odp-register")
