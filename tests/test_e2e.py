"""e2e kit: scenario, CLEX simulator, checks and cleanup.

``test_scenario_checks_hold_on_the_reference_model`` replays the scenario with
the framework's pure logic (registry, rejected runs, what to load, snapshots,
reconciliation) and an SCD1 reference, and runs each check's SQL on SQLite: if
an e2e expectation does not match what the framework does, it fails here and
not in the workspace.
"""

from __future__ import annotations

import json
import sqlite3
import types
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from odp_framework import e2e
from odp_framework.config import load_containers
from odp_framework.manifest import RUN_ID_PATTERN, run_version, validate_manifest
from odp_framework.reconcile import evaluate
from odp_framework.registry import ACTIVE, build_registry, entities_to_load, rejected_runs
from odp_framework.snapshot import RunState, select_next_run
from odp_framework.tasks import e2e as e2e_task

from .conftest import CONFIG_DIR

START_MS = 1_792_000_000_000          # 2026-10-14T17:46:40Z
E2E_ID = "987654321"


@pytest.fixture
def cfg(settings):
    return next(s for s in load_containers(CONFIG_DIR, settings) if s.container == "e2e-selftest").runtime


@pytest.fixture
def ctx(cfg):
    return e2e.E2EContext.from_args(cfg, E2E_ID, START_MS)


def _all_deliveries():
    return [(step, d) for step in e2e.STEPS for d in e2e.deliveries(step)]


# ----------------------------------------------------------------- scenario --
def test_context_names(ctx, cfg):
    assert ctx.entity("customers") == "e2e_987654321_customers"
    assert ctx.source_path("customers") == "e2e-clex/E2E/r987654321/customers"
    assert ctx.bronze("policies") == f"{cfg.bronze_catalog}.e2e_selftest.e2e_987654321_policies"
    with pytest.raises(ValueError, match="e2e_id"):
        e2e.E2EContext.from_args(cfg, "../x", START_MS)


def test_run_ids_follow_the_contract_and_grow_with_each_delivery(ctx):
    run_ids = [ctx.run_id(step, d.slot) for step, d in _all_deliveries()]
    assert all(RUN_ID_PATTERN.match(r) for r in run_ids)
    versions = [run_version(r) for r in run_ids]
    assert versions == sorted(versions) and len(set(versions)) == len(versions)


def test_manifests_follow_the_contract(ctx, cfg):
    for step, d in _all_deliveries():
        m = e2e.build_manifest(ctx, step, d)
        assert validate_manifest(m, data_dir=cfg.data_dir, run_partition=cfg.run_partition) == []
        assert m["row_count"] == d.row_count
    incomplete = next(d for d in e2e.deliveries(3) if d.manifest_row_count)
    assert e2e.build_manifest(ctx, 3, incomplete)["row_count"] == len(incomplete.rows) + 1


class RecordingIO:
    def __init__(self):
        self.ops = []

    def write_parquet(self, path, schema, rows):
        self.ops.append(("parquet", path, schema, list(rows)))

    def put_text(self, path, text):
        self.ops.append(("put", path, text))

    def remove(self, path):
        self.ops.append(("rm", path))


def test_publish_writes_all_data_before_any_manifest(ctx, cfg):
    io = RecordingIO()
    published = e2e.publish(io, ctx, 3)
    kinds = [op[0] for op in io.ops]
    assert kinds == ["parquet"] * 4 + ["put"] * 4
    first = io.ops[0]
    assert first[1] == (f"{cfg.landing_root}/data/e2e-clex/E2E/r{E2E_ID}/customers/"
                        f"run_id={ctx.run_id(3, 1)}/")
    manifest_op = io.ops[4]
    assert manifest_op[1] == f"{cfg.manifests_root}e2e-clex/E2E/r{E2E_ID}/customers/{ctx.run_id(3, 1)}.json"
    assert json.loads(manifest_op[2])["load_spec"]["keys"] == ["customer_id", "segment"]
    assert [p[0] for p in published] == [ctx.entity(t) for t in ("customers", "customers", "policies", "agents")]


# ------------------------------------------------------------ reference model --
T0 = datetime(2026, 10, 15, 8, 0, tzinfo=timezone.utc)


def _cdc_reference(rows, keys, op_column, delete_values):
    """AUTO CDC SCD1: per PK the event with the highest sequence wins; if it is a delete, there is no row."""
    latest = {}
    for r in rows:
        k = tuple(r[c] for c in keys)
        if k not in latest or r["_seq"] > latest[k]["_seq"]:
            latest[k] = r
    return [{c: v for c, v in r.items() if c not in (op_column, "_run_id")}
            for r in latest.values() if r[op_column] not in delete_values]


def simulate(ctx, cfg, upto_step):
    """What the ingestion job would leave after ``upto_step`` steps, table by table."""
    landing = {}                 # (source_path, run_id) -> Delivery
    ingested = set()
    manifests, findings, load_log = [], [], []
    bronze = defaultdict(list)
    silver = {}
    full_version = {}
    entries, rejected = {}, []

    for step in range(1, upto_step + 1):
        t = T0 + timedelta(hours=step)
        for d in e2e.deliveries(step):
            run_id = ctx.run_id(step, d.slot)
            landing[(ctx.source_path(d.table), run_id)] = d
            manifests.append({**e2e.build_manifest(ctx, step, d), "_ingested_at": t + timedelta(seconds=1)})

        # register
        entries, reg_findings = build_registry(cfg, manifests, since=t)
        findings += [("register", f) for f in reg_findings]
        rejected = rejected_runs(cfg, manifests, entries)
        rejected_map = defaultdict(frozenset)
        for r in rejected:
            rejected_map[r.entity] = rejected_map[r.entity] | {r.run_id}
        last_success = {}
        for entity, status, started in load_log:
            if status == "SUCCESS":
                last_success[entity] = max(started, last_success.get(entity, started))

        # ingest: Auto Loader loads everything new in the table's folder
        for name in entities_to_load(entries, last_success):
            path = entries[name].spec.source_path
            for (p, run_id), d in sorted(landing.items()):
                if p == path and (p, run_id) not in ingested:
                    ingested.add((p, run_id))
                    bronze[name] += [{**dict(zip(d.columns, row)), "_run_id": run_id} for row in d.rows]
            load_log.append((name, "SUCCESS", t))

        # silver
        active = [e.spec for e in entries.values() if e.status == ACTIVE]
        for spec in active:
            if spec.entity not in bronze:
                continue
            excluded = rejected_map[spec.entity]
            if spec.is_cdc:
                rows = [r for r in bronze[spec.entity] if r["_run_id"] not in excluded]
                silver[spec.entity] = _cdc_reference(rows, spec.keys, spec.operation_column, spec.delete_values)
                continue
            counts = Counter(r["_run_id"] for r in bronze[spec.entity])
            runs = [RunState(m["run_id"], run_version(m["run_id"]), m["row_count"], counts[m["run_id"]])
                    for m in manifests
                    if m["load_spec"]["target_table"] == spec.entity and m["run_id"] not in excluded]
            while (nxt := select_next_run(runs, full_version.get(spec.entity))) is not None:
                full_version[spec.entity] = nxt.version
                silver[spec.entity] = [{c: v for c, v in r.items() if c != "_run_id"}
                                       for r in bronze[spec.entity] if r["_run_id"] == nxt.run_id]

        # report
        view = [{"entity": m["load_spec"]["target_table"], "run_id": m["run_id"],
                 "load_type": m["load_spec"]["load_type"], "source_path": m["load_spec"]["source_path"],
                 "row_count": m["row_count"], "_ingested_at": m["_ingested_at"]} for m in manifests]
        counts = Counter((name, r["_run_id"]) for name, rows in bronze.items() for r in rows)
        findings += [("report", f) for f in evaluate(cfg.with_entities(active), view, counts,
                                                      t + timedelta(minutes=10), rejected=rejected_map)]

    tables = {
        cfg.registry_fqn: [{"entity": n, "status": e.status} for n, e in entries.items()],
        cfg.rejected_runs_fqn: [{"entity": r.entity, "run_id": r.run_id, "reason": r.reason} for r in rejected],
        cfg.load_log_fqn: [{"entity": e, "status": s} for e, s, _ in load_log],
        cfg.findings_fqn: [{"phase": ph, "severity": f.severity, "code": f.code, "entity": f.entity,
                            "run_id": f.run_id} for ph, f in findings],
    }
    for name, rows in bronze.items():
        tables[f"{cfg.bronze_catalog}.{cfg.schema}.{name}"] = rows
    for name, rows in silver.items():
        tables[f"{cfg.silver_catalog}.{cfg.schema}.{name}"] = rows
    return tables


class SqliteSpark:
    """Just enough Spark for ``run_checks``: tableExists and sql(...).collect()."""

    COLUMNS = {"registry": ["entity", "status"]}

    def __init__(self, tables):
        self.names = sorted(tables, key=len, reverse=True)
        self.db = sqlite3.connect(":memory:")
        for name, rows in tables.items():
            cols = sorted({c for r in rows for c in r}) or ["entity", "status", "severity", "code", "run_id"]
            self.db.execute(f'CREATE TABLE "{name}" ({", ".join(cols)})')
            for r in rows:
                self.db.execute(f'INSERT INTO "{name}" ({", ".join(r)}) VALUES ({", ".join("?" * len(r))})',
                                list(r.values()))
        self.catalog = types.SimpleNamespace(tableExists=lambda n: n in tables)

    def sql(self, query):
        for name in self.names:
            query = query.replace(f" {name}", f' "{name}"')
        return types.SimpleNamespace(collect=lambda: self.db.execute(query).fetchall())


@pytest.mark.parametrize("step", e2e.STEPS)
def test_scenario_checks_hold_on_the_reference_model(ctx, cfg, step):
    results = e2e.run_checks(SqliteSpark(simulate(ctx, cfg, step)), e2e.checks(ctx, step))
    assert all(r.ok for r in results), e2e.format_results(step, results)


def test_reference_model_catches_a_broken_expectation(ctx, cfg):
    # Without the rejected-runs filter, the conflicting run would overwrite row 1 in silver.
    tables = simulate(ctx, cfg, 3)
    silver_customers = f"{cfg.silver_catalog}.{cfg.schema}.{ctx.entity('customers')}"
    for row in tables[silver_customers]:
        if row["customer_id"] == 1:
            row["name"] = "MUST NOT REACH SILVER"
    results = e2e.run_checks(SqliteSpark(tables), e2e.checks(ctx, 3))
    assert [r.check.name for r in results if not r.ok] == ["conflicting run does not reach silver (1)"]


def test_check_names_are_unique(ctx):
    for step in e2e.STEPS:
        names = [c.name for c in e2e.checks(ctx, step)]
        assert len(names) == len(set(names))


# --------------------------------------------------------- execution and cleanup --
def test_run_checks_reports_missing_tables_and_sql_errors():
    def boom(query):
        raise RuntimeError("[UNRESOLVED_COLUMN] segment\nmore detail")

    spark = types.SimpleNamespace(catalog=types.SimpleNamespace(tableExists=lambda n: n == "t"), sql=boom)
    results = e2e.run_checks(spark, [
        e2e.Check("count without table", "SELECT count(*) FROM x", 0, tables=("x",), default=0),
        e2e.Check("value without table", "SELECT max(a) FROM x", "a", tables=("x",)),
        e2e.Check("broken sql", "SELECT max(segment) FROM t", "gold", tables=("t",)),
    ])
    assert [r.ok for r in results] == [True, False, False]
    assert results[2].actual == "ERROR RuntimeError: [UNRESOLVED_COLUMN] segment"
    assert "FAIL broken sql" in e2e.format_results(2, results)


class RecordingSpark:
    def __init__(self, existing, failing=(), tables=None):
        self.existing, self.failing, self.statements = set(existing), failing, []
        self.tables = tables or {}
        self.catalog = types.SimpleNamespace(tableExists=lambda n: n in self.existing)

    def sql(self, statement):
        if statement.startswith("SHOW TABLES IN "):
            catalog = statement.split()[-1].split(".")[0]
            rows = [{"tableName": t} for t in self.tables.get(catalog, [])]
            return types.SimpleNamespace(collect=lambda: rows)
        if any(f in statement for f in self.failing):
            raise RuntimeError("cannot drop\n...")
        self.statements.append(statement)


def test_cleanup_removes_only_e2e_objects_of_this_deployment(ctx, cfg):
    bronze_e2e, silver_e2e = ctx.bronze("customers"), ctx.silver("customers")
    spark = RecordingSpark(
        existing={cfg.manifest_fqn, cfg.registry_fqn, bronze_e2e, silver_e2e},
        failing=(f"DROP TABLE IF EXISTS {silver_e2e}",),
        tables={cfg.bronze_catalog: ["odp_manifests", "odp_entities", ctx.entity("customers")],
                cfg.silver_catalog: [ctx.entity("customers")]},
    )
    io = RecordingIO()
    warnings = e2e.cleanup(spark, io, cfg)
    assert spark.statements[0] == (f"DELETE FROM {cfg.manifest_fqn} "
                                   "WHERE startswith(load_spec.source_path, 'e2e-clex/E2E/')")
    assert spark.statements[1] == f"DELETE FROM {cfg.registry_fqn} WHERE startswith(entity, 'e2e_')"
    assert f"DROP TABLE IF EXISTS {bronze_e2e}" in spark.statements
    assert not any("odp_manifests" in s and s.startswith("DROP") for s in spark.statements)
    assert len(warnings) == 1 and silver_e2e in warnings[0]       # best effort
    assert [op[1] for op in io.ops] == [f"{cfg.landing_root}/data/e2e-clex/E2E/",
                                        f"{cfg.manifests_root}e2e-clex/E2E/",
                                        f"{cfg.system_volume_path}/autoloader/"]


def test_scenario_holds_in_a_namespaced_landing(cfg):
    from dataclasses import replace as dc_replace

    ns = dc_replace(cfg, data_dir="data/dev_victor_e2e_selftest", manifests_dir="_manifests/dev_victor_e2e_selftest")
    ctx = e2e.E2EContext.from_args(ns, E2E_ID, START_MS)
    d = e2e.deliveries(1)[0]
    m = e2e.build_manifest(ctx, 1, d)
    assert validate_manifest(m, data_dir=ns.data_dir, run_partition=ns.run_partition) == []
    assert ctx.manifest_file(1, d).startswith(f"{ns.landing_root}/_manifests/dev_victor_e2e_selftest/e2e-clex/")
    results = e2e.run_checks(SqliteSpark(simulate(ctx, ns, 3)), e2e.checks(ctx, 3))
    assert all(r.ok for r in results), e2e.format_results(3, results)


def test_e2e_task_main(cfg, monkeypatch, capsys):
    tables = {}
    monkeypatch.setattr(e2e_task, "get_spark", lambda: SqliteSpark(tables))
    base = [f"--config_json={cfg.to_json()}", f"--e2e_id={E2E_ID}", f"--start_ms={START_MS}"]

    io = RecordingIO()
    assert e2e_task.main(base + ["--action=publish", "--step=1"], io=io) == 0
    assert len(io.ops) == 4 and "step 1" in capsys.readouterr().out
    assert e2e_task.main(base + ["--action=check", "--step=1"], io=io) == 1        # nothing loaded
    assert "FAIL" in capsys.readouterr().out
    assert e2e_task.main(base + ["--action=cleanup"], io=io) == 0
    with pytest.raises(SystemExit):
        e2e_task.main(base + ["--action=check", "--step=7"], io=io)
