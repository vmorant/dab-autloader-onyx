"""Capa silver: consolidación SCD1 = réplica de la tabla de origen.

* CDC  → ``create_auto_cdc_flow`` sobre el stream de bronze: aplica I/U/D en
  orden de secuencia (gestiona eventos desordenados y deletes).
* Full → ``create_auto_cdc_from_snapshot_flow``: compara el snapshot nuevo con
  el anterior y deduce inserts, updates y deletes. Solo se aplican runs
  completos (ver ``odp_framework.snapshot``).

Bronze lo escribe la tarea ``ingest`` del job (Auto Loader, sin SDP); esta
pipeline solo consolida. Una tabla del registro cuya bronze aún no existe (su
primera carga falló) se omite hasta que exista.

Runs rechazados (``odp_rejected_runs``, los calcula ``register``): sus filas
están en bronze, porque Auto Loader lee la carpeta entera de la tabla, pero
silver no las aplica. Si un run se rechaza después de haberse aplicado (sus
parquet llegaron antes que su manifiesto), el arreglo es un full refresh de esa
tabla: silver se reconstruye desde bronze con el filtro ya en vigor.

Silver se puede reconstruir siempre desde bronze (full refresh permitido).
"""

from __future__ import annotations

from typing import Mapping, Protocol

from ..runtime import NOT_IN_SILVER, RUN_ID_COLUMN, RuntimeConfig, RuntimeEntity
from ..snapshot import RunState, candidate_run_ids, has_applicable_run, select_next_run
from ..sql import in_condition, sql_literal

SILVER_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
    "odp.layer": "silver",
}

# Cuánto recuerda AUTO CDC un delete (tombstone). Un evento más antiguo que el
# delete que llegue después de este plazo resucitaría la fila. El valor por
# defecto son 2 días; 7 cubre reintentos de CLEX y cargas fallidas recuperadas
# en la semana. Una reconstrucción desde bronze (full refresh) no depende de él.
CDC_TOMBSTONE_RETENTION_SECONDS = 7 * 24 * 3600


class RunStateLoader(Protocol):
    """Acceso a manifiestos y bronze. Se inyecta para poder probar sin Spark."""

    def silver_exists(self, entity: RuntimeEntity) -> bool: ...

    def bronze_exists(self, entity: RuntimeEntity) -> bool: ...

    def run_states(self, entity: RuntimeEntity, latest_version: int | None) -> list[RunState]: ...


class SparkRunStateLoader:
    def __init__(self, spark, cfg: RuntimeConfig, rejected: Mapping[str, frozenset[str]] | None = None):
        self.spark = spark
        self.cfg = cfg
        self.rejected = rejected or {}

    def silver_exists(self, entity: RuntimeEntity) -> bool:
        return self.spark.catalog.tableExists(self.cfg.silver_fqn(entity))

    def bronze_exists(self, entity: RuntimeEntity) -> bool:
        return self.spark.catalog.tableExists(self.cfg.bronze_fqn(entity))

    def run_states(self, entity: RuntimeEntity, latest_version: int | None) -> list[RunState]:
        if not self.spark.catalog.tableExists(self.cfg.manifest_fqn):
            return []
        # Solo los runs de esta tabla con la misma especificación registrada.
        manifest_rows = (
            self.spark.read.table(self.cfg.manifest_fqn)
            .where(
                f"load_spec.target_table = {sql_literal(entity.entity)} "
                f"AND load_spec.source_path = {sql_literal(entity.source_path)} "
                "AND load_spec.load_type = 'full'"
            )
            .select("run_id", "row_count")
            .distinct()
            .collect()
        )
        excluded = self.rejected.get(entity.entity, frozenset())
        candidates = candidate_run_ids(
            ((r["run_id"], r["row_count"]) for r in manifest_rows if r["run_id"] not in excluded),
            latest_version,
        )
        if not candidates:
            return []
        if not self.spark.catalog.tableExists(self.cfg.bronze_fqn(entity)):
            counts: dict[str, int] = {}
        else:
            # run_id ya validado por patrón en candidate_run_ids → literales seguros
            in_list = ", ".join(sql_literal(r) for r in candidates)
            counts = {
                r[RUN_ID_COLUMN]: r["count"]
                for r in (
                    self.spark.read.table(self.cfg.bronze_fqn(entity))
                    .where(f"{RUN_ID_COLUMN} IN ({in_list})")
                    .groupBy(RUN_ID_COLUMN)
                    .count()
                    .collect()
                )
            }
        return [
            RunState(run_id=run_id, version=version, manifest_rows=rows,
                     bronze_rows=int(counts.get(run_id, 0)))
            for run_id, (version, rows) in candidates.items()
        ]


def register_silver(spark, dp, cfg: RuntimeConfig, loader: RunStateLoader | None = None,
                    rejected: Mapping[str, frozenset[str]] | None = None) -> dict:
    """Declara la réplica SCD1 de cada entidad activa.

    ``rejected``: ``{tabla: run_ids}`` que no se aplican (``load_rejected_runs``).

    Devuelve ``{"declared": [...], "skipped": [...]}``. Una entidad full sin
    ningún snapshot completo todavía se omite en esta actualización: el primer
    snapshot de un flow nunca puede ser ``None`` (fallaría toda la pipeline).
    """
    rejected = rejected or {}
    loader = loader or SparkRunStateLoader(spark, cfg, rejected)
    declared: list[str] = []
    skipped: list[str] = []
    for entity in cfg.entities:
        if not loader.bronze_exists(entity):
            skipped.append(entity.entity)
            print(f"[odp] {entity.entity}: todavía no hay tabla bronze; se declara tras su primera carga.")
        elif entity.is_cdc:
            _register_cdc(spark, dp, cfg, entity, rejected.get(entity.entity, frozenset()))
            declared.append(entity.entity)
        elif _full_entity_ready(loader, entity):
            _register_full(spark, dp, cfg, entity, loader)
            declared.append(entity.entity)
        else:
            skipped.append(entity.entity)
            print(f"[odp] {entity.entity}: sin snapshot completo todavía; se declara cuando llegue.")
    return {"declared": declared, "skipped": skipped}


def _full_entity_ready(loader: RunStateLoader, entity: RuntimeEntity) -> bool:
    if loader.silver_exists(entity):
        return True
    return has_applicable_run(loader.run_states(entity, None), entity.allow_empty_snapshot)


def _sequence_by(entity: RuntimeEntity):
    if len(entity.sequence_by) == 1:
        return entity.sequence_by[0]
    from pyspark.sql import functions as F  # solo en Databricks

    return F.struct(*entity.sequence_by)


def _create_target(dp, cfg: RuntimeConfig, entity: RuntimeEntity) -> None:
    extra = (
        {"pipelines.cdc.tombstoneGCThresholdInSeconds": str(CDC_TOMBSTONE_RETENTION_SECONDS)}
        if entity.is_cdc else {}
    )
    dp.create_streaming_table(
        name=entity.entity,
        comment=(
            f"Réplica SCD1 de {entity.source_path} ({entity.load_type}) — contenedor "
            f"{cfg.container}. Consolidada desde {cfg.bronze_fqn(entity)}."
        ),
        table_properties={
            **SILVER_TABLE_PROPERTIES,
            "odp.load_type": entity.load_type,
            "odp.spec_version": str(entity.spec_version),
            **extra,
        },
    )


def exclude_runs_condition(run_ids) -> str | None:
    """Filtro que deja fuera los runs rechazados (conserva filas sin ``_run_id``)."""
    if not run_ids:
        return None
    in_list = ", ".join(sql_literal(r) for r in sorted(run_ids))
    return f"({RUN_ID_COLUMN} IS NULL OR {RUN_ID_COLUMN} NOT IN ({in_list}))"


def _register_cdc(spark, dp, cfg: RuntimeConfig, entity: RuntimeEntity,
                  rejected_run_ids: frozenset[str] = frozenset()) -> None:
    _create_target(dp, cfg, entity)
    bronze = cfg.bronze_fqn(entity)
    view_name = f"v_cdc_{entity.entity}"
    exclude = exclude_runs_condition(rejected_run_ids)

    @dp.temporary_view(name=view_name)
    def cdc_changes():
        # skipChangeCommits: un borrado RGPD en bronze no rompe el stream.
        df = spark.readStream.option("skipChangeCommits", "true").table(bronze)
        if exclude:
            # Filtro sin estado: cambiarlo entre actualizaciones no invalida el checkpoint.
            df = df.where(exclude)
        # Las columnas técnicas de bronze no forman parte de la réplica.
        return df.drop(*NOT_IN_SILVER)

    dp.create_auto_cdc_flow(
        target=entity.entity,
        source=view_name,
        keys=list(entity.keys),
        sequence_by=_sequence_by(entity),
        apply_as_deletes=in_condition(entity.operation_column, entity.delete_values),
        # _op solo decide la operación; la secuencia (_seq) se conserva
        except_column_list=[entity.operation_column],
        stored_as_scd_type=1,
        name=f"cdc_{entity.entity}",
    )


def make_snapshot_source(spark, cfg: RuntimeConfig, entity: RuntimeEntity, loader: RunStateLoader):
    bronze = cfg.bronze_fqn(entity)

    def next_snapshot(latest_version):
        runs = loader.run_states(entity, latest_version)
        nxt = select_next_run(runs, latest_version, entity.allow_empty_snapshot)
        if nxt is None:
            return None
        print(f"[odp] {entity.entity}: aplicando snapshot {nxt.run_id} ({nxt.bronze_rows} filas)")
        df = (
            spark.read.table(bronze)
            .where(f"{RUN_ID_COLUMN} = {sql_literal(nxt.run_id)}")
            .drop(*NOT_IN_SILVER)
        )
        return (df, nxt.version)

    return next_snapshot


def _register_full(spark, dp, cfg: RuntimeConfig, entity: RuntimeEntity, loader: RunStateLoader) -> None:
    _create_target(dp, cfg, entity)
    dp.create_auto_cdc_from_snapshot_flow(
        target=entity.entity,
        source=make_snapshot_source(spark, cfg, entity, loader),
        keys=list(entity.keys),
        stored_as_scd_type=1,
    )
