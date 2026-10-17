"""Bronze layer: landing → bronze with Auto Loader (no SDP).

One run per table (one iteration of the job's for_each):

* Reads the container's ``data/{source_path}/`` with Auto Loader in
  ``availableNow`` mode: processes everything new and stops.
* Appends directly to the bronze table (``toTable``), exactly-once thanks to
  the checkpoint.
* Checkpoint and inferred schema live at fixed paths in the ``odp_system``
  Volume (``RuntimeConfig.checkpoint`` / ``schema_location``), never taken
  from the manifest.
* New columns at the source: Auto Loader stops the stream on purpose
  (``UnknownFieldException``) after updating the schema; it is restarted here
  up to ``MAX_SCHEMA_RESTARTS`` times and the table adds the column
  (``mergeSchema``).

Bronze is the immutable record: it is created with ``delta.appendOnly = true``.
A GDPR deletion is done by an administrator who temporarily disables that
property.
"""

from __future__ import annotations

from dataclasses import dataclass

from .runtime import (
    INGESTED_AT_COLUMN,
    RUN_ID_COLUMN,
    SOURCE_FILE_COLUMN,
    RuntimeConfig,
    RuntimeEntity,
)
from .sql import sql_literal

MAX_SCHEMA_RESTARTS = 3
_SCHEMA_CHANGE_MARKERS = ("UnknownFieldException", "UNKNOWN_FIELD_EXCEPTION")

BRONZE_TABLE_PROPERTIES = {
    "delta.appendOnly": "true",
    "odp.layer": "bronze",
}

LOAD_LOG_SCHEMA_DDL = ", ".join(
    [
        "entity STRING",
        "job_run_id STRING",
        "status STRING",
        "started_at TIMESTAMP",
        "finished_at TIMESTAMP",
        "rows_ingested BIGINT",
        "attempts INT",
        "error STRING",
    ]
)
SUCCESS = "SUCCESS"
FAILED = "FAILED"


@dataclass(frozen=True)
class IngestResult:
    rows: int
    attempts: int
    created_table: bool


def bronze_select(run_partition: str) -> tuple[str, ...]:
    # _metadata is selected on the direct read (hidden column of the file
    # source) and the run_id=... partition becomes _run_id as a string.
    return (
        f"* EXCEPT ({run_partition})",
        f"CAST({run_partition} AS STRING) AS {RUN_ID_COLUMN}",
        f"_metadata.file_path AS {SOURCE_FILE_COLUMN}",
        f"current_timestamp() AS {INGESTED_AT_COLUMN}",
    )


def is_schema_change(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}"
    return any(marker in text for marker in _SCHEMA_CHANGE_MARKERS)


def _rows_from_progress(progress) -> int:
    total = 0
    for p in progress or []:
        value = p.get("numInputRows") if isinstance(p, dict) else getattr(p, "numInputRows", 0)
        total += int(value or 0)
    return total


def build_stream(spark, cfg: RuntimeConfig, entity: RuntimeEntity):
    """Auto Loader → append to bronze. Returns the DataStreamWriter ready to start."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", "*.parquet")
        .option("cloudFiles.partitionColumns", cfg.run_partition)   # run_id=... → column
        .option("cloudFiles.schemaLocation", cfg.schema_location(entity))
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if entity.schema_hints:
        reader = reader.option("cloudFiles.schemaHints", entity.schema_hints)
    if cfg.file_events:
        # With file events Auto Loader does not list the container: it reads events.
        reader = reader.option("cloudFiles.useManagedFileEvents", "true")
    # With an inferred schema Auto Loader adds _rescued_data (data that does not
    # fit the type): it stays in bronze and does not travel to silver.
    df = reader.load(cfg.data_path(entity)).selectExpr(*bronze_select(cfg.run_partition))
    return (
        df.writeStream.option("checkpointLocation", cfg.checkpoint(entity))
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
    )


def _set_bronze_properties(spark, cfg: RuntimeConfig, entity: RuntimeEntity) -> None:
    fqn = cfg.bronze_fqn(entity)
    props = {
        **BRONZE_TABLE_PROPERTIES,
        "odp.load_type": entity.load_type,
        "odp.source_path": entity.source_path,
    }
    assignments = ", ".join(f"{sql_literal(k)} = {sql_literal(v)}" for k, v in props.items())
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ({assignments})")
    comment = (f"Bronze ({entity.load_type}) of {entity.source_path} — container {cfg.container}. "
               "Append-only record of everything delivered by CLEX.")
    spark.sql(f"COMMENT ON TABLE {fqn} IS {sql_literal(comment)}")
    if entity.is_full:
        # The silver snapshot filters by _run_id: clustering avoids reading
        # every historical snapshot.
        spark.sql(f"ALTER TABLE {fqn} CLUSTER BY ({RUN_ID_COLUMN})")


def ingest_entity(spark, cfg: RuntimeConfig, entity: RuntimeEntity) -> IngestResult:
    fqn = cfg.bronze_fqn(entity)
    existed = spark.catalog.tableExists(fqn)
    rows = 0
    attempts = 0
    while True:
        attempts += 1
        query = build_stream(spark, cfg, entity).toTable(fqn)
        try:
            query.awaitTermination()
            rows += _rows_from_progress(query.recentProgress)
            break
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is a schema change
            rows += _rows_from_progress(getattr(query, "recentProgress", None))
            if is_schema_change(exc) and attempts <= MAX_SCHEMA_RESTARTS:
                print(f"[odp-ingest] {entity.entity}: new column at the source; retry {attempts}")
                continue
            raise
    created = not existed and spark.catalog.tableExists(fqn)
    if created:
        _set_bronze_properties(spark, cfg, entity)
    return IngestResult(rows=rows, attempts=attempts, created_table=created)
