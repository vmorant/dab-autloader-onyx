from __future__ import annotations

import json

import pytest

from odp_framework.manifest import run_version, validate_manifest
from odp_framework.snapshot import RunState, candidate_run_ids, has_applicable_run, select_next_run

from .conftest import REPO_ROOT, make_manifest


def test_samples_follow_contract():
    for name in ("manifest_cdc.json", "manifest_full.json"):
        manifest = json.loads((REPO_ROOT / "samples" / name).read_text())
        assert validate_manifest(manifest) == [], name


@pytest.mark.parametrize(
    ("manifest", "expected"),
    [
        (make_manifest(run_id="2026-10-16_x"), "run_id"),
        (make_manifest(target="other_catalog.schema.table"), "target_table"),
        (make_manifest(target="Customers"), "target_table"),
        (make_manifest(source_path="srv/../../other-container/t"), "source_path"),
        (make_manifest(source_path="/srv/GDWR/dbo/t"), "source_path"),
        (make_manifest(keys=[]), "keys"),
        (make_manifest(keys=["customer id"]), "keys"),
        (make_manifest(spec_version=0), "spec_version"),
        (make_manifest(load_type="delta"), "load_type"),
        (make_manifest(cdc={"sequence_by": [], "operation_column": "_op", "delete_values": ["D"]}), "sequence_by"),
        (make_manifest(cdc={"sequence_by": ["_seq"], "operation_column": "customer_id",
                            "delete_values": ["D"]}), "cannot be keys"),
        ({**make_manifest(), "data_path": "data/other/path/"}, "data_path"),
        ({**make_manifest(), "seq_max": None}, "seq_max"),
        ({**make_manifest(), "row_count": -1}, "row_count"),
        ({**make_manifest(), "contract_version": "1.0"}, "contract_version"),
        ({**make_manifest(), "load_spec": None}, "load_spec"),
    ],
)
def test_manifest_guardrails(manifest, expected):
    errors = validate_manifest(manifest)
    assert any(expected in e for e in errors), errors


def test_valid_builders():
    assert validate_manifest(make_manifest()) == []
    assert validate_manifest(make_manifest(target="gdwr_policies", source_path="srv-motors-01/GDWR/dbo/policies",
                                           load_type="full", keys=["policy_id"])) == []


def test_run_version():
    assert run_version("20261016T020000Z_8f3a1c2d") == 20261016020000
    assert run_version("20261016T020000_8f3a") is None
    assert run_version(None) is None


def _run(run_id, manifest_rows, bronze_rows):
    return RunState(run_id, run_version(run_id), manifest_rows, bronze_rows)


R1 = "20261014T020000Z_a"
R2 = "20261015T020000Z_b"
R3 = "20261016T020000Z_c"


def test_select_next_run_in_version_order():
    runs = [_run(R2, 10, 10), _run(R1, 10, 10), _run(R3, 10, 10)]
    assert select_next_run(runs, None).run_id == R3            # from scratch: the most recent complete one
    assert select_next_run(runs, run_version(R1)).run_id == R2  # running: in order
    assert select_next_run(runs, run_version(R3)) is None


def test_incomplete_runs_are_not_applied_and_are_superseded():
    runs = [_run(R1, 10, 7), _run(R2, 10, 10)]          # R1 half-loaded
    assert select_next_run(runs, None).run_id == R2       # the new full replaces it
    assert select_next_run([_run(R1, 10, 7)], None) is None
    assert select_next_run([_run(R1, None, 10)], None) is None  # no manifest


def test_empty_snapshot_needs_explicit_permission():
    runs = [_run(R1, 0, 0)]
    assert select_next_run(runs, None) is None
    assert select_next_run(runs, None, allow_empty=True).run_id == R1
    assert not has_applicable_run(runs)
    assert has_applicable_run(runs, allow_empty=True)


def test_candidate_run_ids_filters_invalid_and_old():
    manifests = [(R1, 5), (R2, 6), ("bad", 1), (R3, None)]
    out = candidate_run_ids(manifests, run_version(R1))
    assert out == {R2: (run_version(R2), 6), R3: (run_version(R3), None)}
