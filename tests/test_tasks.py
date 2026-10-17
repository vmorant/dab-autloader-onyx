"""Tareas register, ingest y report de punta a punta con un Spark de mentira."""

from __future__ import annotations

import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

from odp_framework.config import load_containers
from odp_framework.manifest import MANIFEST_SCHEMA_DDL
from odp_framework.registry import REGISTRY_SCHEMA_DDL, build_registry, registry_rows
from odp_framework.ingest import IngestResult
from odp_framework.tasks import ingest, register, report

from .conftest import CONFIG_DIR, make_manifest

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
RUN = "20261016T020000Z_8f3a1c2d"
REGISTRY_COLUMNS = [c.split(" ")[0] for c in REGISTRY_SCHEMA_DDL.split(", ")]


class Row(dict):
    def asDict(self, recursive=False):
        return dict(self)


class Query:
    def __init__(self, rows):
        self.rows = rows
        self.conditions = []

    def where(self, cond):
        self.conditions.append(cond)
        return self

    def select(self, *cols):
        return self

    def selectExpr(self, *exprs):
        return self

    def groupBy(self, col):
        return self

    def agg(self, spec):
        return self

    def count(self):
        return self if self.conditions and "IN (" in self.conditions[-1] else len(self.rows)

    def collect(self):
        return self.rows


class Writer:
    def __init__(self, spark, rows, schema):
        self.spark, self.rows, self.schema, self._mode = spark, rows, schema, None

    def mode(self, mode):
        self._mode = mode
        return self

    def saveAsTable(self, name):
        self.spark.writes.append((name, self._mode, self.schema, self.rows))


class StreamChain:
    """readStream...load(...).selectExpr(...).writeStream...toTable(...).awaitTermination()."""

    def __init__(self, spark):
        self.spark = spark
        self.options = {}
        self.writeStream = self

    def format(self, f):
        self.options["format"] = f
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def schema(self, ddl):
        self.options["schema"] = ddl
        return self

    def load(self, path):
        self.options["path"] = path
        return self

    def selectExpr(self, *e):
        return self

    def trigger(self, **kw):
        self.options["trigger"] = kw
        return self

    def toTable(self, name):
        self.options["table"] = name
        self.spark.streams.append(self.options)
        return self

    def awaitTermination(self):
        # Simula la ingesta: los manifiestos "nuevos" pasan a la tabla.
        self.spark.tables.setdefault(self.options["table"], []).extend(self.spark.landing)


class FakeSpark:
    def __init__(self, tables=None, landing=None):
        self.tables = tables or {}
        self.landing = landing or []
        self.streams, self.writes = [], []
        self.catalog = types.SimpleNamespace(tableExists=lambda name: name in self.tables)
        self.read = types.SimpleNamespace(table=lambda name: Query(list(self.tables[name])))

    @property
    def readStream(self):
        return StreamChain(self)

    def createDataFrame(self, rows, schema):
        return types.SimpleNamespace(write=Writer(self, rows, schema))


@pytest.fixture
def cfg(settings):
    return next(s for s in load_containers(CONFIG_DIR, settings) if s.container == "motors-sqlserver").runtime


# ---------------------------------------------------------------- register --
def _ts(**delta):
    return (NOW + timedelta(**delta)).replace(tzinfo=None)


def test_register_ingests_manifests_rebuilds_registry_and_plans_loads(cfg):
    new = Row(make_manifest(run_id=RUN, ingested_at=_ts(seconds=5)))
    bad = Row(make_manifest(run_id="20261016T030000Z_x", target="otro.esquema.t", ingested_at=_ts(seconds=5)))
    spark = FakeSpark(landing=[new, bad])

    to_load, active, findings = register.run(spark, cfg, job_run_id="77", now=NOW)

    (stream,) = spark.streams
    assert stream["path"] == cfg.manifests_root
    assert stream["schema"] == MANIFEST_SCHEMA_DDL
    assert stream["checkpointLocation"] == cfg.manifest_checkpoint
    assert stream["trigger"] == {"availableNow": True} and stream["table"] == cfg.manifest_fqn

    assert to_load == ["gdwr_customers"] and active == 1
    assert [f.code for f in findings] == ["INVALID_MANIFEST"]
    (reg_write, rejected_write, find_write) = spark.writes
    assert reg_write[:3] == (cfg.registry_fqn, "overwrite", REGISTRY_SCHEMA_DDL)
    assert [r[0] for r in reg_write[3]] == ["gdwr_customers"]
    # el inválido apunta a la carpeta de gdwr_customers: su run no llega a silver
    assert rejected_write[:2] == (cfg.rejected_runs_fqn, "overwrite")
    assert rejected_write[3] == [("gdwr_customers", "20261016T030000Z_x", "INVALID_MANIFEST", "otro.esquema.t")]
    assert find_write[:2] == (cfg.findings_fqn, "append")
    assert find_write[3][0][1:4] == ("77", "register", "motors-sqlserver")


def test_register_skips_tables_already_loaded(cfg):
    manifest = Row(make_manifest(run_id=RUN, ingested_at=_ts(hours=-2)))
    loaded = Row(entity="gdwr_customers", **{"max(started_at)": _ts(hours=-1)})
    spark = FakeSpark(tables={cfg.manifest_fqn: [manifest], cfg.load_log_fqn: [loaded]})
    to_load, active, findings = register.run(spark, cfg, now=NOW)
    assert to_load == [] and active == 1 and findings == []


def test_register_main_publishes_task_values(cfg, monkeypatch):
    spark = FakeSpark(landing=[Row(make_manifest(run_id=RUN, ingested_at=_ts()))])
    published = {}
    monkeypatch.setattr(register, "get_spark", lambda: spark)
    monkeypatch.setattr(register, "set_task_value", lambda k, v: published.setdefault(k, v))

    assert register.main([f"--config_json={cfg.to_json()}"]) == 0
    assert published == {"entities_to_load": ["gdwr_customers"], "load_count": 1, "active_entities": 1}


def test_register_fails_when_nothing_loads_and_there_are_errors(cfg, monkeypatch):
    # main() usa la hora real: el manifiesto tiene que ser "nuevo" respecto a ella
    just_now = (datetime.now(timezone.utc) + timedelta(minutes=1)).replace(tzinfo=None)
    bad = Row(make_manifest(target="otro.esquema.t", ingested_at=just_now))
    monkeypatch.setattr(register, "get_spark", lambda: FakeSpark(landing=[bad]))
    monkeypatch.setattr(register, "set_task_value", lambda k, v: None)
    assert register.main([f"--config_json={cfg.to_json()}"]) == 1


def test_load_list_is_capped_for_the_for_each(cfg, monkeypatch):
    from odp_framework.tasks import _common

    names = [f"tabla_{i:04d}" for i in range(10)]
    kept, pending = _common.fit_task_value(names, limit=60)
    assert kept and pending and kept + pending == names
    assert len(__import__("json").dumps(kept)) <= 60


# ------------------------------------------------------------------ ingest --
def test_ingest_task_logs_success(cfg, monkeypatch):
    spark = FakeSpark(tables={cfg.registry_fqn: _registry_table(cfg)})
    calls = []
    monkeypatch.setattr(ingest, "ingest_entity",
                        lambda s, c, e: calls.append(e.entity) or IngestResult(rows=42, attempts=1, created_table=True))
    assert ingest.run(spark, cfg, "gdwr_customers", job_run_id="5", now=NOW) == 42
    assert calls == ["gdwr_customers"]
    ((table, mode, schema, rows),) = spark.writes
    assert (table, mode) == (cfg.load_log_fqn, "append")
    entity, job_run_id, status, started, _, n, attempts, error = rows[0]
    assert (entity, job_run_id, status, n, attempts, error) == ("gdwr_customers", "5", "SUCCESS", 42, 1, None)
    assert started == NOW.replace(tzinfo=None)


def test_ingest_task_logs_failure_and_reraises(cfg, monkeypatch):
    spark = FakeSpark(tables={cfg.registry_fqn: _registry_table(cfg)})

    def boom(*a):
        raise RuntimeError("sin permisos en la external location")

    monkeypatch.setattr(ingest, "ingest_entity", boom)
    with pytest.raises(RuntimeError):
        ingest.run(spark, cfg, "gdwr_customers", now=NOW)
    (write,) = spark.writes
    assert write[3][0][2] == "FAILED" and "external location" in write[3][0][7]


def test_ingest_task_rejects_tables_not_active(cfg):
    spark = FakeSpark(tables={cfg.registry_fqn: _registry_table(cfg)})
    with pytest.raises(ValueError, match="no está ACTIVE"):
        ingest.run(spark, cfg, "gdwr_otra", now=NOW)


def test_ingest_main(cfg, monkeypatch):
    spark = FakeSpark(tables={cfg.registry_fqn: _registry_table(cfg)})
    monkeypatch.setattr(ingest, "get_spark", lambda: spark)
    monkeypatch.setattr(ingest, "ingest_entity", lambda *a: IngestResult(rows=1, attempts=1, created_table=False))
    assert ingest.main([f"--config_json={cfg.to_json()}", "--entity=gdwr_customers", "--job_run_id=3"]) == 0


# ------------------------------------------------------------------ report --
def _registry_table(cfg):
    entries, _ = build_registry(cfg, [make_manifest(run_id=RUN)])
    return [Row(zip(REGISTRY_COLUMNS, r)) for r in registry_rows(entries)]


def _manifest_row(rows=100):
    return Row(entity="gdwr_customers", run_id=RUN, load_type="cdc",
               source_path="srv-motors-01/GDWR/dbo/customers", row_count=rows,
               _ingested_at=(NOW - timedelta(hours=3)).replace(tzinfo=None))


def test_report_run_writes_findings(cfg):
    bronze = f"{cfg.bronze_catalog}.{cfg.schema}.gdwr_customers"
    spark = FakeSpark(tables={
        cfg.registry_fqn: _registry_table(cfg),
        cfg.manifest_fqn: [_manifest_row(rows=100)],
        bronze: [Row(_run_id=RUN, count=60)],
    })
    findings = report.run(spark, cfg, job_run_id="42", now=NOW)
    assert [f.code for f in findings] == ["ROW_COUNT_MISMATCH"]
    ((table, mode, _, rows),) = spark.writes
    assert (table, mode) == (cfg.findings_fqn, "append")
    assert rows[0][1:7] == ("42", "report", "motors-sqlserver", "ERROR", "ROW_COUNT_MISMATCH", "gdwr_customers")


def test_report_without_tables_is_quiet(cfg):
    spark = FakeSpark()
    assert report.run(spark, cfg, now=NOW) == []
    assert spark.writes == []


def test_report_fails_on_register_errors_of_the_same_run(cfg, tmp_path, monkeypatch, capsys):
    spark = FakeSpark(tables={
        cfg.registry_fqn: _registry_table(cfg),
        cfg.manifest_fqn: [_manifest_row(rows=100)],
        f"{cfg.bronze_catalog}.{cfg.schema}.gdwr_customers": [Row(_run_id=RUN, count=100)],
        cfg.findings_fqn: [Row(code="SPEC_CONFLICT")],          # error de register en este run
    })
    monkeypatch.setattr(report, "get_spark", lambda: spark)
    config_file = tmp_path / "odp.config.json"
    config_file.write_text(cfg.to_json())

    assert report.main([f"--config_file={config_file}", "--job_run_id=9"]) == 1
    assert "+1 errores en register" in capsys.readouterr().out
    assert report.main([f"--config_json={cfg.to_json()}"]) == 0          # sin job_run_id no mira register
    assert report.main([f"--config_file={config_file}", "--job_run_id=9", "--fail_on_error=false"]) == 0


def test_tasks_require_a_config_source():
    for task in (report, register):
        with pytest.raises(SystemExit):
            task.main([])
    with pytest.raises(SystemExit):
        ingest.main(["--entity=x"])


def test_get_spark_uses_active_session(monkeypatch):
    from odp_framework.tasks import _common

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: "spark"))
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    assert _common.get_spark() == "spark"


def test_failed_main_raises_so_the_wheel_task_fails():
    from odp_framework.tasks._common import TaskFailed, exit_with

    exit_with(0, "odp-report")
    with pytest.raises(TaskFailed, match="odp-report"):
        exit_with(1, "odp-report")
