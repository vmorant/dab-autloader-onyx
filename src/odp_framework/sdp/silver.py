"""Silver layer: SCD1 consolidation = replica of the source table.

* CDC  → ``create_auto_cdc_flow`` over the bronze stream: applies I/U/D in
  sequence order (handles out-of-order events and deletes).
* Full → ``create_auto_cdc_from_snapshot_flow``: compares the new snapshot with
  the previous one and derives inserts, updates and deletes. Only complete runs
  are applied (see ``odp_framework.snapshot``).

Bronze is written by the job's ``ingest`` task (Auto Loader, no SDP); this
pipeline only consolidates. A registry table whose bronze table does not exist
yet (its first load failed) is skipped until it exists.

Rejected runs (``odp_rejected_runs``, computed by ``register``): their rows are
in bronze, because Auto Loader reads the table's whole folder, but silver does
not apply them. If a run is rejected after it was applied (its parquet files
arrived before its manifest), the fix is a full refresh of that table: silver
is rebuilt from bronze with the filter already in place.

Silver can always be rebuilt from bronze (full refresh allowed).
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

# How long AUTO CDC remembers a delete (tombstone). An event older than the
# delete that arrives after this period would bring the row back. The default
# is 2 days; 7 covers CLEX retries and failed loads recovered within the week.
# A rebuild from bronze (full refresh) does not depend on it.
CDC_TOMBSTONE_RETENTION_SECONDS = 7 * 24 * 3600


class RunStateLoader(Protocol):
    """Access to manifests and bronze. Injected so it can be tested without Spark."""

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
        # Only the runs of this table with the same registered specification.
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
            # run_id already validated by pattern in candidate_run_ids → safe literals
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
    """Declares the SCD1 replica of each active entity.

    ``rejected``: ``{table: run_ids}`` that are not applied (``load_rejected_runs``).

    Returns ``{"declared": [...], "skipped": [...]}``. A full entity without any
    complete snapshot yet is skipped in this update: the first snapshot of a
    flow can never be ``None`` (it would fail the whole pipeline).
    """
    rejected = rejected or {}
    loader = loader or SparkRunStateLoader(spark, cfg, rejected)
    declared: list[str] = []
    skipped: list[str] = []
    for entity in cfg.entities:
        if not loader.bronze_exists(entity):
            skipped.append(entity.entity)
            print(f"[odp] {entity.entity}: no bronze table yet; declared after its first load.")
        elif entity.is_cdc:
            _register_cdc(spark, dp, cfg, entity, rejected.get(entity.entity, frozenset()))
            declared.append(entity.entity)
        elif _full_entity_ready(loader, entity):
            _register_full(spark, dp, cfg, entity, loader)
            declared.append(entity.entity)
        else:
            skipped.append(entity.entity)
            print(f"[odp] {entity.entity}: no complete snapshot yet; declared when one arrives.")
    return {"declared": declared, "skipped": skipped}


def _full_entity_ready(loader: RunStateLoader, entity: RuntimeEntity) -> bool:
    if loader.silver_exists(entity):
        return True
    return has_applicable_run(loader.run_states(entity, None), entity.allow_empty_snapshot)


def _sequence_by(entity: RuntimeEntity):
    if len(entity.sequence_by) == 1:
        return entity.sequence_by[0]
    from pyspark.sql import functions as F  # only on Databricks

    return F.struct(*entity.sequence_by)


def _create_target(dp, cfg: RuntimeConfig, entity: RuntimeEntity) -> None:
    extra = (
        {"pipelines.cdc.tombstoneGCThresholdInSeconds": str(CDC_TOMBSTONE_RETENTION_SECONDS)}
        if entity.is_cdc else {}
    )
    dp.create_streaming_table(
        name=entity.entity,
        comment=(
            f"SCD1 replica of {entity.source_path} ({entity.load_type}) — container "
            f"{cfg.container}. Consolidated from {cfg.bronze_fqn(entity)}."
        ),
        table_properties={
            **SILVER_TABLE_PROPERTIES,
            "odp.load_type": entity.load_type,
            "odp.spec_version": str(entity.spec_version),
            **extra,
        },
    )


def exclude_runs_condition(run_ids) -> str | None:
    """Filter that excludes rejected runs (keeps rows without ``_run_id``)."""
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
        # skipChangeCommits: a GDPR deletion in bronze does not break the stream.
        df = spark.readStream.option("skipChangeCommits", "true").table(bronze)
        if exclude:
            # Stateless filter: changing it between updates does not invalidate the checkpoint.
            df = df.where(exclude)
        # Bronze technical columns are not part of the replica.
        return df.drop(*NOT_IN_SILVER)

    dp.create_auto_cdc_flow(
        target=entity.entity,
        source=view_name,
        keys=list(entity.keys),
        sequence_by=_sequence_by(entity),
        apply_as_deletes=in_condition(entity.operation_column, entity.delete_values),
        # _op only decides the operation; the sequence (_seq) is kept
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
        print(f"[odp] {entity.entity}: applying snapshot {nxt.run_id} ({nxt.bronze_rows} rows)")
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
