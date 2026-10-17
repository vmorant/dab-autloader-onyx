"""Capa bronze: landing → bronze con Auto Loader (sin SDP).

Una ejecución por tabla (una iteración del for_each del job):

* Lee ``data/{source_path}/`` del contenedor con Auto Loader en modo
  ``availableNow``: procesa todo lo nuevo y termina.
* Escribe en append directo a la tabla bronze (``toTable``), con garantía
  exactly-once gracias al checkpoint.
* Checkpoint y esquema inferido en rutas fijas del Volume ``odp_system``
  (``RuntimeConfig.checkpoint`` / ``schema_location``), nunca desde el manifiesto.
* Columnas nuevas en origen: Auto Loader para el stream a propósito
  (``UnknownFieldException``) tras actualizar el esquema; se relanza aquí
  mismo hasta ``MAX_SCHEMA_RESTARTS`` veces y la tabla añade la columna
  (``mergeSchema``).

Bronze es el registro inmutable: se crea con ``delta.appendOnly = true``. Un
borrado RGPD lo hace un administrador desactivando esa propiedad temporalmente.
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
    # _metadata se selecciona sobre la lectura directa (columna oculta del file
    # source) y la partición run_id=... pasa a _run_id como texto.
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
    """Auto Loader → append a bronze. Devuelve el DataStreamWriter listo para arrancar."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("pathGlobFilter", "*.parquet")
        .option("cloudFiles.partitionColumns", cfg.run_partition)   # run_id=... → columna
        .option("cloudFiles.schemaLocation", cfg.schema_location(entity))
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )
    if entity.schema_hints:
        reader = reader.option("cloudFiles.schemaHints", entity.schema_hints)
    if cfg.file_events:
        # Con file events Auto Loader no lista el contenedor: lee eventos.
        reader = reader.option("cloudFiles.useManagedFileEvents", "true")
    # Con esquema inferido, Auto Loader añade _rescued_data (datos que no
    # encajan en el tipo): se queda en bronze y no viaja a silver.
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
    comment = (f"Bronze ({entity.load_type}) de {entity.source_path} — contenedor {cfg.container}. "
               "Append de todo lo entregado por CLEX.")
    spark.sql(f"COMMENT ON TABLE {fqn} IS {sql_literal(comment)}")
    if entity.is_full:
        # El snapshot de silver filtra por _run_id: clustering para no leer
        # todos los snapshots históricos.
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
        except Exception as exc:  # noqa: BLE001 - se relanza si no es cambio de esquema
            rows += _rows_from_progress(getattr(query, "recentProgress", None))
            if is_schema_change(exc) and attempts <= MAX_SCHEMA_RESTARTS:
                print(f"[odp-ingest] {entity.entity}: columna nueva en origen; reintento {attempts}")
                continue
            raise
    created = not existed and spark.catalog.tableExists(fqn)
    if created:
        _set_bronze_properties(spark, cfg, entity)
    return IngestResult(rows=rows, attempts=attempts, created_table=created)
