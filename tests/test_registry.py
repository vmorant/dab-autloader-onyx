"""Entity registry derived from the manifests (guardrails included)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from odp_framework.config import load_containers
from odp_framework.findings import ERROR, INFO, WARN
from odp_framework.registry import (
    ACTIVE,
    DISABLED,
    PENDING_APPROVAL,
    build_registry,
    entities_to_load,
    entity_from_row,
    load_active_entities,
    load_rejected_runs,
    registry_rows,
    rejected_rows,
    rejected_runs,
)

from .conftest import CONFIG_DIR, CUSTOMERS, make_manifest

T0 = datetime(2026, 10, 16, 3, 0)
R1 = "20261014T020000Z_aaaa"
R2 = "20261015T020000Z_bbbb"
R3 = "20261016T020000Z_cccc"
POLICIES_PATH = "srv-motors-01/GDWR/dbo/policies"


@pytest.fixture
def cfg(settings):
    return next(s for s in load_containers(CONFIG_DIR, settings) if s.container == "motors-sqlserver").runtime


def _codes(findings):
    return {(f.severity, f.code) for f in findings}


def test_new_tables_register_from_manifests(cfg):
    manifests = [
        make_manifest(run_id=R1, schema_hints="customer_id BIGINT, _seq BIGINT"),
        make_manifest(target="gdwr_policies", source_path=POLICIES_PATH, load_type="full",
                      keys=["policy_id"], run_id=R2),
    ]
    entries, findings = build_registry(cfg, manifests)
    assert findings == []
    assert sorted(entries) == ["gdwr_customers", "gdwr_policies"]
    assert entries["gdwr_customers"].spec == CUSTOMERS
    assert entries["gdwr_customers"].status == ACTIVE
    assert entries["gdwr_policies"].spec.is_full and entries["gdwr_policies"].spec.operation_column is None


def test_runs_are_tracked_in_version_order(cfg):
    entries, _ = build_registry(cfg, [make_manifest(run_id=R3), make_manifest(run_id=R1), make_manifest(run_id=R2)])
    e = entries["gdwr_customers"]
    assert (e.first_run_id, e.last_run_id) == (R1, R3)


def test_immutable_fields_cannot_change(cfg):
    manifests = [
        make_manifest(run_id=R1),
        make_manifest(run_id=R2, keys=["customer_id", "branch"], spec_version=2),
        make_manifest(run_id=R3, source_path="srv-motors-01/GDWR/dbo/clients", spec_version=3),
    ]
    entries, findings = build_registry(cfg, manifests)
    assert entries["gdwr_customers"].spec == replace(CUSTOMERS, schema_hints=None)
    assert entries["gdwr_customers"].last_run_id == R1          # conflicting runs are not associated
    assert [f.code for f in findings] == ["SPEC_CONFLICT", "SPEC_CONFLICT"]
    assert "keys" in findings[0].detail and "source_path" in findings[1].detail


def test_mutable_fields_change_only_with_new_spec_version(cfg):
    manifests = [
        make_manifest(run_id=R1),
        make_manifest(run_id=R2, schema_hints="customer_id BIGINT"),                 # without bumping the version
        make_manifest(run_id=R3, schema_hints="customer_id BIGINT", spec_version=2),
    ]
    entries, findings = build_registry(cfg, manifests)
    assert _codes(findings) == {(WARN, "SPEC_CHANGED_WITHOUT_VERSION"), (INFO, "SPEC_UPDATED")}
    assert entries["gdwr_customers"].spec.schema_hints == "customer_id BIGINT"
    assert entries["gdwr_customers"].spec.spec_version == 2


def test_two_tables_cannot_read_the_same_path(cfg):
    manifests = [make_manifest(run_id=R1), make_manifest(target="gdwr_clients", run_id=R2)]
    entries, findings = build_registry(cfg, manifests)
    assert list(entries) == ["gdwr_customers"]
    assert _codes(findings) == {(ERROR, "SOURCE_PATH_CONFLICT")}


def test_invalid_manifests_are_reported_not_registered(cfg):
    entries, findings = build_registry(cfg, [make_manifest(target="other.schema.table")])
    assert entries == {}
    assert _codes(findings) == {(ERROR, "INVALID_MANIFEST")}


def test_status_comes_from_platform_config(cfg):
    manifests = [make_manifest(run_id=R1),
                 make_manifest(target="gdwr_policies", source_path=POLICIES_PATH, load_type="full",
                               keys=["policy_id"], run_id=R2)]
    manual = replace(cfg, registration="manual", approved_entities=("gdwr_policies",))
    entries, findings = build_registry(manual, manifests)
    assert entries["gdwr_customers"].status == PENDING_APPROVAL
    assert entries["gdwr_policies"].status == ACTIVE
    assert _codes(findings) == {(WARN, "PENDING_APPROVAL")}

    disabled = replace(cfg, disabled_entities=("gdwr_customers",))
    entries, findings = build_registry(disabled, manifests)
    assert entries["gdwr_customers"].status == DISABLED
    assert _codes(findings) == {(WARN, "DISABLED_ENTITY")}


def test_findings_only_for_new_manifests(cfg):
    old = make_manifest(run_id=R1, ingested_at=T0 - timedelta(days=2))
    old_conflict = make_manifest(run_id=R2, keys=["other"], ingested_at=T0 - timedelta(days=1))
    new_conflict = make_manifest(run_id=R3, keys=["other"], ingested_at=T0 + timedelta(minutes=1))
    _, findings = build_registry(cfg, [old, old_conflict, new_conflict], since=T0)
    assert [(f.code, f.run_id) for f in findings] == [("SPEC_CONFLICT", R3)]


def test_registry_rows_roundtrip(cfg):
    entries, _ = build_registry(cfg, [make_manifest(run_id=R1, ingested_at=T0,
                                                    schema_hints="customer_id BIGINT, _seq BIGINT")])
    (row,) = registry_rows(entries)
    columns = ["entity", "source_path", "load_type", "keys", "sequence_by", "operation_column",
               "delete_values", "schema_hints", "allow_empty_snapshot", "spec_version", "status",
               "first_run_id", "last_run_id", "registered_at", "updated_at", "last_manifest_at"]
    as_dict = dict(zip(columns, row))
    assert as_dict["status"] == ACTIVE and as_dict["registered_at"] == T0
    assert as_dict["last_manifest_at"] == T0
    assert entity_from_row(as_dict) == CUSTOMERS


class _Row(dict):
    def asDict(self):
        return dict(self)


def test_load_active_entities(cfg):
    class Spark:
        catalog = type("C", (), {"tableExists": staticmethod(lambda name: True)})()

        class read:  # noqa: N801
            @staticmethod
            def table(name):
                class Q:
                    def where(self, cond):
                        assert cond == "status = 'ACTIVE'"
                        return self

                    def collect(self):
                        entries, _ = build_registry(cfg, [make_manifest(run_id=R1,
                                                    schema_hints="customer_id BIGINT, _seq BIGINT")])
                        cols = ["entity", "source_path", "load_type", "keys", "sequence_by",
                                "operation_column", "delete_values", "schema_hints",
                                "allow_empty_snapshot", "spec_version", "status", "first_run_id",
                                "last_run_id", "registered_at", "updated_at"]
                        return [_Row(zip(cols, r)) for r in registry_rows(entries)]

                return Q()

    assert load_active_entities(Spark(), cfg) == [CUSTOMERS]


def test_entities_to_load(cfg):
    manifests = [
        make_manifest(run_id=R1, ingested_at=T0),
        make_manifest(run_id=R2, ingested_at=T0 + timedelta(hours=1)),
        make_manifest(target="gdwr_policies", source_path=POLICIES_PATH, load_type="full",
                      keys=["policy_id"], run_id=R2, ingested_at=T0),
    ]
    entries, _ = build_registry(cfg, manifests)
    assert entries["gdwr_customers"].last_manifest_at == T0 + timedelta(hours=1)

    # never loaded → both
    assert entities_to_load(entries, {}) == ["gdwr_customers", "gdwr_policies"]
    # policies loaded after its manifest; customers has a manifest later than its load
    done = {"gdwr_customers": T0 + timedelta(minutes=30), "gdwr_policies": T0 + timedelta(minutes=30)}
    assert entities_to_load(entries, done) == ["gdwr_customers"]
    # inactive ones are not loaded
    entries, _ = build_registry(replace(cfg, disabled_entities=("gdwr_customers",)), manifests)
    assert entities_to_load(entries, {}) == ["gdwr_policies"]


# -------------------------------------------------------------- rejected runs --
R4 = "20261017T020000Z_dddd"


def _rejected(cfg, manifests):
    entries, _ = build_registry(cfg, manifests)
    return rejected_rows(rejected_runs(cfg, manifests, entries))


def test_conflicting_run_in_the_same_folder_is_rejected(cfg):
    manifests = [make_manifest(run_id=R1), make_manifest(run_id=R2, keys=["customer_id", "branch"])]
    assert _rejected(cfg, manifests) == [("gdwr_customers", R2, "SPEC_CONFLICT", "gdwr_customers")]


def test_run_written_into_another_tables_folder_is_rejected_for_the_owner(cfg):
    manifests = [make_manifest(run_id=R1), make_manifest(target="gdwr_clients", run_id=R2)]
    assert _rejected(cfg, manifests) == [("gdwr_customers", R2, "SOURCE_PATH_CONFLICT", "gdwr_clients")]


def test_invalid_manifest_in_a_registered_folder_is_rejected(cfg):
    bad = make_manifest(run_id=R2)
    bad["row_count"] = -1
    assert _rejected(cfg, [make_manifest(run_id=R1), bad]) == [
        ("gdwr_customers", R2, "INVALID_MANIFEST", "gdwr_customers")]


def test_accepted_runs_and_unowned_folders_are_not_rejected(cfg):
    manifests = [
        make_manifest(run_id=R1),
        make_manifest(run_id=R2, schema_hints="x INT"),                       # change without version: accepted
        make_manifest(run_id=R3, source_path="srv-motors-01/GDWR/dbo/clients", spec_version=2),  # no table reads that folder
        make_manifest(run_id=R4, keys=["x"]),                                 # conflict...
        make_manifest(run_id=R4),                                             # ...but the same run is also accepted
    ]
    assert _rejected(cfg, manifests) == []


def test_load_rejected_runs(cfg):
    class Q:
        def select(self, *cols):
            return self

        def collect(self):
            return [_Row(entity="gdwr_customers", run_id=R1), _Row(entity="gdwr_customers", run_id=R2)]

    spark = type("S", (), {})()
    spark.catalog = type("C", (), {"tableExists": staticmethod(lambda name: name == cfg.rejected_runs_fqn)})()
    spark.read = type("R", (), {"table": staticmethod(lambda name: Q())})()
    assert load_rejected_runs(spark, cfg) == {"gdwr_customers": frozenset({R1, R2})}
    spark.catalog = type("C", (), {"tableExists": staticmethod(lambda name: False)})()
    assert load_rejected_runs(spark, cfg) == {}


# ---------------------------------------------------------- review findings --
def test_registered_spec_is_the_first_accepted_not_the_oldest_run_id(cfg):
    # A manifest with an earlier run_id and other keys arrives late: it cannot
    # change the specification already registered.
    first = make_manifest(run_id=R2, ingested_at=T0)
    late = make_manifest(run_id=R1, keys=["customer_id", "branch"], ingested_at=T0 + timedelta(hours=1))
    entries, findings = build_registry(cfg, [late, first])
    assert entries["gdwr_customers"].spec.keys == ("customer_id",)
    assert [(f.code, f.run_id) for f in findings] == [("SPEC_CONFLICT", R1)]
    assert _rejected(cfg, [late, first]) == [("gdwr_customers", R1, "SPEC_CONFLICT", "gdwr_customers")]


def test_nested_source_paths_conflict(cfg):
    parent = make_manifest(run_id=R1)
    child = make_manifest(target="gdwr_customers_hist", run_id=R2,
                          source_path="srv-motors-01/GDWR/dbo/customers/hist")
    entries, findings = build_registry(cfg, [parent, child])
    assert list(entries) == ["gdwr_customers"]
    assert [f.code for f in findings] == ["SOURCE_PATH_CONFLICT"]
    # its files are inside the gdwr_customers folder: its Auto Loader reads them
    assert _rejected(cfg, [parent, child]) == [
        ("gdwr_customers", R2, "SOURCE_PATH_CONFLICT", "gdwr_customers_hist")]


def test_rejection_also_covers_the_folder_named_in_data_path(cfg):
    bad = make_manifest(run_id=R2)
    bad["data_path"] = f"data/srv-motors-01/GDWR/dbo/customers/run_id={R3}/"      # does not match run_id
    assert _rejected(cfg, [make_manifest(run_id=R1), bad]) == [
        ("gdwr_customers", R2, "INVALID_MANIFEST", "gdwr_customers"),
        ("gdwr_customers", R3, "INVALID_MANIFEST", "gdwr_customers"),
    ]
