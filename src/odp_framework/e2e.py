"""End-to-end test on a real workspace: simulates CLEX and checks the result.

Runs as the ``e2e_<container>`` job, which is only generated for containers
with ``e2e: true`` (never in pro), on a dedicated landing container::

    reset → publish_1 → run_1 → check_1 → … → publish_3 → run_3 → check_3 → cleanup

* ``publish_N`` writes the step's parquet files and then its manifests, the way
  CLEX would.
* ``run_N`` runs the container's ingestion job (``run_job_task``): the real
  register, ingest, silver and report, with the wheel pinned in the target.
* ``check_N`` checks bronze, silver, the registry and the findings with SQL and
  fails if anything does not match.
* ``reset`` and ``cleanup`` remove everything the e2e created in this
  deployment (``e2e_*`` tables, their rows in the control tables, checkpoints
  and landing). ``cleanup`` only runs if everything succeeded: if something
  fails, the data stays for investigation until the next run, which starts
  with ``reset``.

Isolation: each run uses its own tables (``e2e_<job_run_id>_<table>``) and each
deployment its own subtree of the container (the generator adds the
deployment's schema to ``data/`` and ``_manifests/``; in dev the schema carries
the developer prefix). A deployment does not see another one's manifests.

Scenario:

1. Initial load: ``customers`` (CDC, 5 inserts) and ``policies`` (full, 3 rows).
2. Changes. CDC: update, delete, two out-of-order updates of the same PK in the
   same file, insert and a new column (schema evolution). Full: snapshot with
   an insert, a delete and an update.
3. Guardrails. Manifest with changed keys in the ``customers`` folder
   (SPEC_CONFLICT: reaches bronze, not silver), late CDC events (must neither
   overwrite nor resurrect a deleted row), incomplete full snapshot (not
   applied) and a new table (automatic registration).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Protocol, Sequence

from .manifest import CONTRACT_VERSION, expected_data_path
from .runtime import RUN_ID_COLUMN, RuntimeConfig
from .sql import sql_literal

STEPS = (1, 2, 3)
TABLES = ("customers", "policies", "agents")
SOURCE_ROOT = "e2e-clex/E2E"                 # simulated {connection}/{database}

CUSTOMERS_V1 = "customer_id BIGINT, name STRING, _op STRING, _seq BIGINT"
CUSTOMERS_V2 = CUSTOMERS_V1 + ", segment STRING"   # step 2: new column
POLICIES = "policy_id STRING, product STRING, premium BIGINT"
AGENTS = "agent_id STRING, name STRING"

CDC_SPEC = {"sequence_by": ["_seq"], "operation_column": "_op", "delete_values": ["D"]}
_E2E_ID = re.compile(r"^[a-z0-9]{1,24}$")


@dataclass(frozen=True)
class Delivery:
    """A simulated CLEX extraction: one run of one table."""

    table: str
    slot: int                           # order within the step (sets the run_id)
    load_type: str
    keys: tuple[str, ...]
    schema: str                         # DDL of the parquet files
    rows: tuple[tuple, ...]
    manifest_row_count: int | None = None   # None = len(rows); any other value = incomplete run
    note: str = ""

    @property
    def columns(self) -> list[str]:
        return [c.strip().split(" ")[0] for c in self.schema.split(",")]

    @property
    def row_count(self) -> int:
        return len(self.rows) if self.manifest_row_count is None else self.manifest_row_count


@dataclass(frozen=True)
class Check:
    name: str
    sql: str                            # returns a single value
    expected: Any
    op: str = "eq"                      # eq | ge
    tables: tuple[str, ...] = ()        # tables it uses; if one is missing, the value is ``default``
    default: Any = None


@dataclass(frozen=True)
class CheckResult:
    check: Check
    actual: Any
    ok: bool


@dataclass(frozen=True)
class E2EContext:
    cfg: RuntimeConfig
    e2e_id: str                         # {{job.run_id}} of the e2e job
    start: datetime                     # {{job.start_time}} (UTC)

    @classmethod
    def from_args(cls, cfg: RuntimeConfig, e2e_id: str, start_ms: str | int) -> "E2EContext":
        e2e_id = str(e2e_id).strip().lower()
        if not _E2E_ID.match(e2e_id):
            raise ValueError(f"invalid e2e_id {e2e_id!r}: [a-z0-9]{{1,24}}")
        start = datetime.fromtimestamp(int(start_ms) / 1000, tz=timezone.utc).replace(microsecond=0)
        return cls(cfg=cfg, e2e_id=e2e_id, start=start)

    # ---- names of this run ------------------------------------------------
    def entity(self, table: str) -> str:
        return f"e2e_{self.e2e_id}_{table}"

    @property
    def entities(self) -> list[str]:
        return [self.entity(t) for t in TABLES]

    @property
    def source_root(self) -> str:
        return f"{SOURCE_ROOT}/r{self.e2e_id}"

    def source_path(self, table: str) -> str:
        return f"{self.source_root}/{table}"

    def extracted_at(self, step: int, slot: int) -> datetime:
        return self.start + timedelta(minutes=10 * step, seconds=slot)

    def run_id(self, step: int, slot: int) -> str:
        return f"{self.extracted_at(step, slot):%Y%m%dT%H%M%S}Z_e2e{self.e2e_id}-s{step}-{slot}"

    def bronze(self, table: str) -> str:
        return f"{self.cfg.bronze_catalog}.{self.cfg.schema}.{self.entity(table)}"

    def silver(self, table: str) -> str:
        return f"{self.cfg.silver_catalog}.{self.cfg.schema}.{self.entity(table)}"

    # ---- landing paths -----------------------------------------------------
    def data_dir(self, step: int, d: Delivery) -> str:
        rel = expected_data_path(self.cfg.data_dir, self.source_path(d.table), self.cfg.run_partition,
                                 self.run_id(step, d.slot))
        return f"{self.cfg.landing_root.rstrip('/')}/{rel}"

    def manifest_file(self, step: int, d: Delivery) -> str:
        return f"{self.cfg.manifests_root}{self.source_path(d.table)}/{self.run_id(step, d.slot)}.json"


# ------------------------------------------------------------------- scenario --
def deliveries(step: int) -> list[Delivery]:
    """What "CLEX extracts" at each step."""
    if step == 1:
        return [
            Delivery("customers", 1, "cdc", ("customer_id",), CUSTOMERS_V1, (
                (1, "Ana", "I", 1), (2, "Bruno", "I", 2), (3, "Carla", "I", 3),
                (4, "Dani", "I", 4), (5, "Eva", "I", 5),
            ), note="initial load as inserts"),
            Delivery("policies", 2, "full", ("policy_id",), POLICIES, (
                ("P1", "auto", 100), ("P2", "home", 200), ("P3", "auto", 300),
            ), note="first snapshot"),
        ]
    if step == 2:
        return [
            Delivery("customers", 1, "cdc", ("customer_id",), CUSTOMERS_V2, (
                (1, "Ana v2", "U", 10, "gold"),
                (2, None, "D", 11, None),
                (3, "Carla v2", "U", 13, "silver"),     # arrives before its previous version
                (3, "Carla v1", "U", 12, None),
                (6, "Fede", "I", 14, "bronze"),
            ), note="update, delete, out-of-order events and new column"),
            Delivery("policies", 2, "full", ("policy_id",), POLICIES, (
                ("P1", "auto", 150), ("P3", "auto", 300), ("P4", "life", 400),
            ), note="P1 changes, P2 disappears, P4 is new"),
        ]
    if step == 3:
        return [
            Delivery("customers", 1, "cdc", ("customer_id", "segment"), CUSTOMERS_V2, (
                (1, "MUST NOT REACH SILVER", "U", 20, "gold"),
            ), note="changed keys → SPEC_CONFLICT"),
            Delivery("customers", 2, "cdc", ("customer_id",), CUSTOMERS_V2, (
                (7, "Gema", "I", 21, "gold"),
                (4, "Dani stale", "U", 3, None),         # older than the insert (seq 4)
                (2, "Bruno zombie", "U", 9, None),       # older than the delete (seq 11)
            ), note="insert and late events"),
            Delivery("policies", 3, "full", ("policy_id",), POLICIES, (
                ("P1", "auto", 160), ("P3", "auto", 300), ("P4", "life", 400),
            ), manifest_row_count=4, note="incomplete snapshot (manifest says 4 rows)"),
            Delivery("agents", 4, "full", ("agent_id",), AGENTS, (
                ("A1", "Lola"), ("A2", "Mario"),
            ), note="new table"),
        ]
    raise ValueError(f"step {step} does not exist: {STEPS}")


def build_manifest(ctx: E2EContext, step: int, d: Delivery) -> dict[str, Any]:
    """Contract 2.0 manifest, the same one CLEX writes."""
    run_id = ctx.run_id(step, d.slot)
    source_path = ctx.source_path(d.table)
    is_cdc = d.load_type == "cdc"
    seqs = [row[d.columns.index("_seq")] for row in d.rows] if is_cdc else []
    return {
        "contract_version": CONTRACT_VERSION,
        "run_id": run_id,
        "data_path": expected_data_path(ctx.cfg.data_dir, source_path, ctx.cfg.run_partition, run_id),
        "file_count": 1,
        "row_count": d.row_count,
        "seq_min": str(min(seqs)) if seqs else None,
        "seq_max": str(max(seqs)) if seqs else None,
        "schema_hash": "sha256:" + hashlib.sha256(d.schema.encode()).hexdigest(),
        "extracted_at": ctx.extracted_at(step, d.slot).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "clex": {"pipeline_run_id": f"e2e-{ctx.e2e_id}", "connector": "e2e-simulator", "instance": "e2e"},
        "load_spec": {
            "target_table": ctx.entity(d.table),
            "source_path": source_path,
            "load_type": d.load_type,
            "keys": list(d.keys),
            "spec_version": 1,
            "schema_hints": None,
            "cdc": dict(CDC_SPEC) if is_cdc else None,
            "full": None if is_cdc else {"allow_empty_snapshot": False},
        },
    }


# ------------------------------------------------------------------ publishing --
class LandingIO(Protocol):
    def write_parquet(self, path: str, schema: str, rows: Sequence[tuple]) -> None: ...

    def put_text(self, path: str, text: str) -> None: ...

    def remove(self, path: str) -> None: ...


class SparkLandingIO:
    """Writes to the landing with Spark and dbutils (job identity: WRITE FILES on the external location)."""

    def __init__(self, spark, dbutils=None):
        self.spark = spark
        self._dbutils = dbutils

    @property
    def dbutils(self):
        if self._dbutils is None:
            from databricks.sdk.runtime import dbutils  # only on Databricks

            self._dbutils = dbutils
        return self._dbutils

    def write_parquet(self, path: str, schema: str, rows: Sequence[tuple]) -> None:
        # One file per run, like an ADF copy. Never overwrites: a new run_id every time.
        self.spark.createDataFrame(list(rows), schema).coalesce(1).write.mode("errorifexists").parquet(path)

    def put_text(self, path: str, text: str) -> None:
        self.dbutils.fs.put(path, text, False)

    def remove(self, path: str) -> None:
        self.dbutils.fs.rm(path, True)


def publish(io: LandingIO, ctx: E2EContext, step: int) -> list[tuple[str, str, str]]:
    """Writes the step like CLEX: all parquet files first, then the manifests."""
    batch = deliveries(step)
    for d in batch:
        io.write_parquet(ctx.data_dir(step, d), d.schema, d.rows)
    published = []
    for d in batch:
        io.put_text(ctx.manifest_file(step, d), json.dumps(build_manifest(ctx, step, d), indent=2))
        published.append((ctx.entity(d.table), ctx.run_id(step, d.slot), d.note))
    return published


# ---------------------------------------------------------------------- checks --
def _count(name: str, table: str, expected: int, where: str | None = None, op: str = "eq") -> Check:
    sql = f"SELECT count(*) FROM {table}" + (f" WHERE {where}" if where else "")
    return Check(name, sql, expected, op=op, tables=(table,), default=0)


def _value(name: str, table: str, column: str, where: str, expected: Any) -> Check:
    return Check(name, f"SELECT max({column}) FROM {table} WHERE {where}", expected, tables=(table,))


def _in(values: Iterable[str]) -> str:
    return ", ".join(sql_literal(v) for v in values)


def checks(ctx: E2EContext, step: int) -> list[Check]:
    cfg = ctx.cfg
    mine = f"entity IN ({_in(ctx.entities)})"
    customers, policies, agents = (ctx.entity(t) for t in TABLES)

    def no_errors() -> Check:
        return _count("no errors in this run", cfg.findings_fqn, 0,
                      f"severity = 'ERROR' AND {mine}")

    def finding(code: str, table: str, step_: int, slot: int) -> Check:
        return _count(f"finding {code} on {table}", cfg.findings_fqn, 1,
                      f"code = {sql_literal(code)} AND entity = {sql_literal(ctx.entity(table))} "
                      f"AND run_id = {sql_literal(ctx.run_id(step_, slot))}", op="ge")

    if step == 1:
        return [
            _count("registry: 2 ACTIVE tables", cfg.registry_fqn, 2, f"{mine} AND status = 'ACTIVE'"),
            _count("load log: both tables loaded", cfg.load_log_fqn, 2,
                   f"{mine} AND status = 'SUCCESS'", op="ge"),
            _count("bronze customers: 5 events", ctx.bronze("customers"), 5),
            _count("silver customers: 5 rows", ctx.silver("customers"), 5),
            _count("bronze policies: 3 rows", ctx.bronze("policies"), 3),
            _count("silver policies: 3 rows", ctx.silver("policies"), 3),
            no_errors(),
        ]
    if step == 2:
        return [
            _count("bronze customers: 10 events", ctx.bronze("customers"), 10),
            _count("bronze: new column loaded", ctx.bronze("customers"), 3, "segment IS NOT NULL"),
            _count("silver customers: 5 rows", ctx.silver("customers"), 5),
            _count("delete applied (2)", ctx.silver("customers"), 0, "customer_id = 2"),
            _value("update applied (1)", ctx.silver("customers"), "name", "customer_id = 1", "Ana v2"),
            _value("out of order: highest sequence wins (3)", ctx.silver("customers"), "name",
                   "customer_id = 3", "Carla v2"),
            _value("new column in silver (6)", ctx.silver("customers"), "segment",
                   "customer_id = 6", "bronze"),
            _count("silver policies: 3 rows", ctx.silver("policies"), 3),
            _count("snapshot: delete applied (P2)", ctx.silver("policies"), 0, "policy_id = 'P2'"),
            _value("snapshot: update applied (P1)", ctx.silver("policies"), "premium",
                   "policy_id = 'P1'", 150),
            _count("snapshot: insert applied (P4)", ctx.silver("policies"), 1, "policy_id = 'P4'"),
            no_errors(),
        ]
    if step == 3:
        conflict_run = ctx.run_id(3, 1)
        return [
            finding("SPEC_CONFLICT", "customers", 3, 1),
            _count("conflicting run in odp_rejected_runs", cfg.rejected_runs_fqn, 1,
                   f"entity = {sql_literal(customers)} AND run_id = {sql_literal(conflict_run)}"),
            _count("conflicting run loaded into bronze (evidence)", ctx.bronze("customers"), 1,
                   f"{RUN_ID_COLUMN} = {sql_literal(conflict_run)}"),
            _value("conflicting run does not reach silver (1)", ctx.silver("customers"), "name",
                   "customer_id = 1", "Ana v2"),
            _count("insert from the valid run (7)", ctx.silver("customers"), 1, "customer_id = 7"),
            _value("late event does not overwrite (4)", ctx.silver("customers"), "name", "customer_id = 4", "Dani"),
            _count("late event does not resurrect a delete (2)", ctx.silver("customers"), 0,
                   "customer_id = 2"),
            _count("silver customers: 6 rows", ctx.silver("customers"), 6),
            finding("ROW_COUNT_MISMATCH", "policies", 3, 3),
            _value("incomplete snapshot not applied (P1)", ctx.silver("policies"), "premium",
                   "policy_id = 'P1'", 150),
            _count("new table registered: 3 ACTIVE", cfg.registry_fqn, 3, f"{mine} AND status = 'ACTIVE'"),
            _count("new table in silver", ctx.silver("agents"), 2),
            _count("no unexpected errors", cfg.findings_fqn, 0,
                   f"severity = 'ERROR' AND {mine} AND code NOT IN ('SPEC_CONFLICT', 'ROW_COUNT_MISMATCH')"),
        ]
    raise ValueError(f"step {step} does not exist: {STEPS}")


def _matches(check: Check, actual: Any) -> bool:
    if check.op == "ge":
        return actual is not None and actual >= check.expected
    return actual == check.expected


def run_checks(spark, items: Iterable[Check]) -> list[CheckResult]:
    results = []
    for c in items:
        if any(not spark.catalog.tableExists(t) for t in c.tables):
            actual: Any = c.default
        else:
            try:
                row = spark.sql(c.sql).collect()[0]
                actual = row[0]
            except Exception as exc:  # noqa: BLE001 - the failure is the check result
                actual = f"ERROR {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
        results.append(CheckResult(c, actual, _matches(c, actual)))
    return results


def format_results(step: int, results: Sequence[CheckResult]) -> str:
    lines = [f"[odp-e2e] step {step}: {sum(r.ok for r in results)}/{len(results)} OK"]
    for r in results:
        sign = ">=" if r.check.op == "ge" else "=="
        lines.append(f"  {'OK  ' if r.ok else 'FAIL'} {r.check.name}: {r.actual!r} (expected {sign} {r.check.expected!r})")
        if not r.ok:
            lines.append(f"       {r.check.sql}")
    return "\n".join(lines)


# --------------------------------------------------------------------- cleanup --
TABLE_PREFIX = "e2e_"


def _e2e_tables(spark, catalog: str, schema: str) -> list[str]:
    try:
        rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    except Exception:  # noqa: BLE001 - schema not created yet
        return []
    return sorted(f"{catalog}.{schema}.{r['tableName']}" for r in rows
                  if str(r["tableName"]).startswith(TABLE_PREFIX))


def cleanup(spark, io: LandingIO, cfg: RuntimeConfig) -> list[str]:
    """Removes everything the e2e created in this deployment of the test container.

    ``e2e_*`` tables in bronze and silver, their rows in ``odp_manifests``,
    ``odp_entities`` and ``odp_rejected_runs``, Auto Loader checkpoints and the
    simulated landing. Keeps ``odp_load_log`` and ``odp_run_findings``
    (history). Best effort: returns warnings instead of failing.
    """
    warnings: list[str] = []
    root_literal = sql_literal(f"{SOURCE_ROOT}/")
    prefix_literal = sql_literal(TABLE_PREFIX)
    statements: list[tuple[str, str]] = [
        (cfg.manifest_fqn,
         f"DELETE FROM {cfg.manifest_fqn} WHERE startswith(load_spec.source_path, {root_literal})"),
        (cfg.registry_fqn, f"DELETE FROM {cfg.registry_fqn} WHERE startswith(entity, {prefix_literal})"),
        (cfg.rejected_runs_fqn,
         f"DELETE FROM {cfg.rejected_runs_fqn} WHERE startswith(entity, {prefix_literal})"),
    ]
    # Silver first (the pipeline stops declaring them once they leave the registry), then bronze.
    for catalog in (cfg.silver_catalog, cfg.bronze_catalog):
        statements += [(t, f"DROP TABLE IF EXISTS {t}") for t in _e2e_tables(spark, catalog, cfg.schema)]
    for table, statement in statements:
        try:
            if spark.catalog.tableExists(table):
                spark.sql(statement)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"{statement}: {str(exc).splitlines()[0][:200]}")

    landing = cfg.landing_root.rstrip("/")
    paths = [
        f"{landing}/{cfg.data_dir}/{SOURCE_ROOT}/",
        f"{cfg.manifests_root}{SOURCE_ROOT}/",
        f"{cfg.system_volume_path}/autoloader/",       # the e2e container's Volume only holds e2e tables
    ]
    for path in paths:
        try:
            io.remove(path)
        except Exception as exc:  # noqa: BLE001
            message = str(exc).splitlines()[0][:200]
            if "FileNotFound" not in message and "does not exist" not in message:
                warnings.append(f"rm {path}: {message}")
    return warnings
