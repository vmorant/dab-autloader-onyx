"""``ingest`` task (python_wheel_task inside the for_each, entry point ``odp-ingest``).

One iteration = one table. Reads its specification from the registry (the one
that arrived in the manifest and ``register`` validated), runs Auto Loader into
bronze and records the outcome in ``odp_load_log``. If it fails, the iteration
fails (the for_each shows it per table) and the table is put back on the list
of the next run.
"""

from __future__ import annotations

import sys
from datetime import datetime

from ..findings import as_utc, utc_now
from ..ingest import FAILED, LOAD_LOG_SCHEMA_DDL, SUCCESS, ingest_entity
from ..registry import load_active_entities
from ..runtime import RuntimeConfig
from ._common import base_parser, get_spark, resolve_config


def _log(spark, cfg: RuntimeConfig, entity: str, job_run_id: str, status: str,
         started: datetime, rows: int, attempts: int, error: str | None) -> None:
    naive = lambda ts: as_utc(ts).replace(tzinfo=None)  # noqa: E731
    row = (entity, job_run_id, status, naive(started), naive(utc_now()), rows, attempts, error)
    (
        spark.createDataFrame([row], schema=LOAD_LOG_SCHEMA_DDL)
        .write.mode("append")          # concurrent appends from the for_each: no conflicts
        .saveAsTable(cfg.load_log_fqn)
    )


def run(spark, cfg: RuntimeConfig, entity_name: str, job_run_id: str = "",
        now: datetime | None = None) -> int:
    started = now or utc_now()
    entity = next((e for e in load_active_entities(spark, cfg) if e.entity == entity_name), None)
    if entity is None:
        raise ValueError(f"{entity_name} is not ACTIVE in {cfg.registry_fqn}")
    try:
        result = ingest_entity(spark, cfg, entity)
    except Exception as exc:
        _log(spark, cfg, entity_name, job_run_id, FAILED, started, 0, 0, str(exc)[:4000])
        raise
    _log(spark, cfg, entity_name, job_run_id, SUCCESS, started, result.rows, result.attempts, None)
    print(f"[odp-ingest] {entity_name}: {result.rows} rows into {cfg.bronze_fqn(entity)}"
          f"{' (table created)' if result.created_table else ''}")
    return result.rows


def main(argv: list[str] | None = None) -> int:
    parser = base_parser("odp-ingest")
    parser.add_argument("--entity", required=True)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    cfg = resolve_config(args, parser)
    run(get_spark(), cfg, args.entity, job_run_id=args.job_run_id)
    return 0


def entrypoint() -> None:  # pragma: no cover - used by python_wheel_task
    main()
