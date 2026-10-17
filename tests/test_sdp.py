"""Pipeline de silver (SDP) con dobles de prueba (sin Spark ni Databricks)."""

from __future__ import annotations

import sys
import types

import pytest

from odp_framework.config import load_containers
from odp_framework.manifest import run_version
from odp_framework.runtime import NOT_IN_SILVER
from odp_framework.sdp.silver import SparkRunStateLoader, make_snapshot_source, register_silver
from odp_framework.snapshot import RunState

from .conftest import CONFIG_DIR, CONTRACTS, CUSTOMERS, POLICIES

R1 = "20261015T021500Z_aaaa"
R2 = "20261016T021500Z_bbbb"


@pytest.fixture
def specs(settings):
    return {s.container: s for s in load_containers(CONFIG_DIR, settings)}


@pytest.fixture
def motors(specs):
    # Entidades tal y como las devuelve el registro (origen: manifiestos de CLEX)
    return specs["motors-sqlserver"].runtime.with_entities([CUSTOMERS, POLICIES])


class FakeLoader:
    def __init__(self, runs=None, silver_exists=False, missing_bronze=()):
        self.runs = runs or []
        self.exists = silver_exists
        self.missing_bronze = set(missing_bronze)

    def silver_exists(self, entity):
        return self.exists

    def bronze_exists(self, entity):
        return entity.entity not in self.missing_bronze

    def run_states(self, entity, latest_version):
        return [r for r in self.runs if latest_version is None or r.version > latest_version]


# ------------------------------------------------------------------ silver --
def test_silver_cdc_flow(fake_spark, fake_dp, motors):
    result = register_silver(fake_spark, fake_dp, motors, loader=FakeLoader())
    assert result == {"declared": ["gdwr_customers"], "skipped": ["gdwr_policies"]}

    target = fake_dp.streaming_tables["gdwr_customers"]
    assert target["table_properties"]["delta.enableChangeDataFeed"] == "true"
    assert target["table_properties"]["pipelines.cdc.tombstoneGCThresholdInSeconds"] == str(7 * 24 * 3600)

    (flow,) = fake_dp.cdc_flows
    assert flow["target"] == "gdwr_customers"
    assert flow["source"] == "v_cdc_gdwr_customers"
    assert flow["keys"] == ["customer_id"]
    assert flow["sequence_by"] == "_seq"
    assert flow["apply_as_deletes"] == "`_op` IN ('D')"
    assert flow["except_column_list"] == ["_op"]
    assert flow["stored_as_scd_type"] == 1

    view = fake_dp.views["v_cdc_gdwr_customers"]["fn"]()
    assert view.source.table_name == "odp_datateam01_dev_bronze.motors_sqlserver.gdwr_customers"
    assert view.source.options["skipChangeCommits"] == "true"
    assert view.ops == [("drop", NOT_IN_SILVER)]
    assert "_rescued_data" in NOT_IN_SILVER and "_run_id" in NOT_IN_SILVER


def test_silver_full_registered_once_a_complete_snapshot_exists(fake_spark, fake_dp, motors):
    loader = FakeLoader(runs=[RunState(R1, run_version(R1), 10, 10)])
    result = register_silver(fake_spark, fake_dp, motors, loader=loader)
    assert result["skipped"] == []
    (flow,) = fake_dp.snapshot_flows
    assert flow["target"] == "gdwr_policies" and flow["keys"] == ["policy_id"]
    assert flow["stored_as_scd_type"] == 1


def test_silver_waits_for_the_first_bronze_load(fake_spark, fake_dp, motors):
    result = register_silver(fake_spark, fake_dp, motors, loader=FakeLoader(missing_bronze=["gdwr_customers"]))
    assert result["skipped"] == ["gdwr_customers", "gdwr_policies"]
    assert fake_dp.cdc_flows == [] and "gdwr_customers" not in fake_dp.streaming_tables


def test_silver_full_registered_when_table_exists(fake_spark, fake_dp, motors):
    result = register_silver(fake_spark, fake_dp, motors, loader=FakeLoader(silver_exists=True))
    assert "gdwr_policies" in result["declared"]


def test_snapshot_source_applies_complete_runs_in_order(fake_spark, motors):
    loader = FakeLoader(runs=[
        RunState(R2, run_version(R2), 12, 12),
        RunState(R1, run_version(R1), 10, 10),
    ])
    source = make_snapshot_source(fake_spark, motors, POLICIES, loader)

    # en marcha (último aplicado R1): el siguiente en orden
    df, version = source(run_version(R1))
    assert version == run_version(R2)
    assert ("where", f"_run_id = '{R2}'") in df.ops
    assert ("drop", NOT_IN_SILVER) in df.ops
    assert df.source.table_name.endswith(".motors_sqlserver.gdwr_policies")
    assert source(run_version(R2)) is None


def test_snapshot_source_starts_from_the_newest_complete_run(fake_spark, motors):
    # primera actualización o full refresh: no repasa el histórico de bronze
    loader = FakeLoader(runs=[RunState(R1, run_version(R1), 10, 10), RunState(R2, run_version(R2), 12, 12)])
    df, version = make_snapshot_source(fake_spark, motors, POLICIES, loader)(None)
    assert version == run_version(R2) and ("where", f"_run_id = '{R2}'") in df.ops


def test_snapshot_source_skips_incomplete(fake_spark, motors):
    loader = FakeLoader(runs=[RunState(R1, run_version(R1), 10, 4)])
    assert make_snapshot_source(fake_spark, motors, POLICIES, loader)(None) is None


def test_spark_loader_filters_manifests_by_registered_spec(motors):
    class Q:
        def __init__(self, rows):
            self.rows, self.conditions = rows, []

        def where(self, cond):
            self.conditions.append(cond)
            return self

        def select(self, *c):
            return self

        def distinct(self):
            return self

        def groupBy(self, c):
            return self

        def count(self):
            return self

        def collect(self):
            return self.rows

    queries = {
        motors.manifest_fqn: Q([{"run_id": R1, "row_count": 10}, {"run_id": "malo", "row_count": 1}]),
        motors.bronze_fqn(POLICIES): Q([{"_run_id": R1, "count": 10}]),
    }
    spark = types.SimpleNamespace(
        catalog=types.SimpleNamespace(tableExists=lambda name: True),
        read=types.SimpleNamespace(table=lambda name: queries[name]),
    )
    states = SparkRunStateLoader(spark, motors).run_states(POLICIES, None)
    assert states == [RunState(R1, run_version(R1), 10, 10)]
    (cond,) = queries[motors.manifest_fqn].conditions
    assert "load_spec.target_table = 'gdwr_policies'" in cond
    assert f"load_spec.source_path = '{POLICIES.source_path}'" in cond
    assert queries[motors.bronze_fqn(POLICIES)].conditions == [f"_run_id IN ('{R1}')"]
    # un run rechazado por register nunca es candidato
    rejected = SparkRunStateLoader(spark, motors, {"gdwr_policies": frozenset({R1})})
    assert rejected.run_states(POLICIES, None) == []


def test_multi_column_sequence_uses_struct(fake_spark, fake_dp, specs, monkeypatch):
    functions = types.ModuleType("pyspark.sql.functions")
    functions.struct = lambda *cols: ("struct", cols)
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", types.ModuleType("pyspark.sql"))
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    sys.modules["pyspark.sql"].functions = functions

    home = specs["home-sqlserver"].runtime.with_entities([CONTRACTS])
    register_silver(fake_spark, fake_dp, home, loader=FakeLoader())
    (flow,) = fake_dp.cdc_flows
    assert flow["keys"] == ["contract_id", "line_no"]
    assert flow["sequence_by"] == ("struct", ("_seq", "_seq_tiebreak"))


def test_silver_cdc_skips_rejected_runs(fake_spark, fake_dp, motors):
    register_silver(fake_spark, fake_dp, motors, loader=FakeLoader(),
                    rejected={"gdwr_customers": frozenset({R2, R1})})
    view = fake_dp.views["v_cdc_gdwr_customers"]["fn"]()
    assert view.ops == [
        ("where", f"(_run_id IS NULL OR _run_id NOT IN ('{R1}', '{R2}'))"),
        ("drop", NOT_IN_SILVER),
    ]
