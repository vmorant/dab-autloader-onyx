from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from odp_framework.config import load_containers
from odp_framework.findings import ERROR, INFO, WARN, has_errors
from odp_framework.reconcile import evaluate

from .conftest import CONFIG_DIR, CUSTOMERS, POLICIES

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
RUN = "20261016T020000Z_8f3a1c2d"
RUN_FULL = "20261016T021500Z_a71b0e44"


@pytest.fixture
def cfg(settings):
    rt = next(s for s in load_containers(CONFIG_DIR, settings) if s.container == "motors-sqlserver").runtime
    return rt.with_entities([CUSTOMERS, POLICIES])


def _m(**over):
    base = {
        "entity": "gdwr_customers",
        "run_id": RUN,
        "load_type": "cdc",
        "source_path": CUSTOMERS.source_path,
        "row_count": 100,
        "_ingested_at": (NOW - timedelta(hours=2)).replace(tzinfo=None),
    }
    return {**base, **over}


def _codes(findings):
    return {(f.severity, f.code) for f in findings}


def test_clean_delivery_has_no_findings(cfg):
    assert evaluate(cfg, [_m()], {("gdwr_customers", RUN): 100}, NOW) == []


def test_manifests_outside_the_registry_are_left_to_register(cfg):
    # no activa, con otra ruta (conflicto) o con otro tipo de carga: ya lo avisa register
    manifests = [_m(entity="gdwr_nueva"), _m(source_path="otra/ruta"), _m(load_type="full")]
    assert evaluate(cfg, manifests, {}, NOW) == []


def test_row_count_states_by_age(cfg):
    recent = _m(_ingested_at=NOW - timedelta(minutes=10))
    assert _codes(evaluate(cfg, [recent], {}, NOW)) == {(INFO, "PENDING")}
    assert _codes(evaluate(cfg, [_m()], {}, NOW)) == {(WARN, "NOT_INGESTED")}
    old = _m(_ingested_at=NOW - timedelta(hours=30))
    assert _codes(evaluate(cfg, [old], {}, NOW)) == {(ERROR, "STALE_PENDING")}
    partial = evaluate(cfg, [_m()], {("gdwr_customers", RUN): 60}, NOW)
    assert _codes(partial) == {(ERROR, "ROW_COUNT_MISMATCH")}
    assert has_errors(partial)


def test_empty_full_snapshot_is_an_error(cfg):
    m = _m(entity="gdwr_policies", run_id=RUN_FULL, load_type="full", row_count=0,
           source_path=POLICIES.source_path)
    assert _codes(evaluate(cfg, [m], {}, NOW)) == {(ERROR, "EMPTY_SNAPSHOT")}


def test_duplicate_versions(cfg):
    other = RUN.replace("8f3a1c2d", "ffff0000")
    counts = {("gdwr_customers", RUN): 100, ("gdwr_customers", other): 100}
    assert _codes(evaluate(cfg, [_m(), _m(run_id=other)], counts, NOW)) == {(WARN, "DUPLICATE_VERSION")}


def test_rejected_runs_are_left_to_register(cfg):
    # SPEC_CONFLICT con la misma ruta: register ya lo avisa y silver no lo aplica
    findings = evaluate(cfg, [_m()], {}, NOW, rejected={"gdwr_customers": frozenset({RUN})})
    assert findings == []
