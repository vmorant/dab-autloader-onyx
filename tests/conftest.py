"""Test doubles for Spark and for the SDP module (``pyspark.pipelines``)."""

from __future__ import annotations

from pathlib import Path

import pytest

from odp_framework.config import DeploySettings
from odp_framework.runtime import RuntimeEntity

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "config"


@pytest.fixture
def settings() -> DeploySettings:
    return DeploySettings(
        team_id="datateam01",
        env="dev",
        landing_storage_account="odpdatateam01devlzsto01",
        framework_wheel="/Volumes/odp_platform_dev/libs/wheels/odp_framework-0.4.0-py3-none-any.whl",
        pipeline_policy_id="POLICY-PIPE",
        job_policy_id="POLICY-JOB",
        notification_emails=("equipo@example.com",),
    )


# ---------------------------------------------------------------- builders --
def make_manifest(target="gdwr_customers", source_path="srv-motors-01/GDWR/dbo/customers",
                  run_id="20261016T020000Z_8f3a1c2d", load_type="cdc", row_count=100,
                  ingested_at=None, **spec_over):
    """Contract 2.0 manifest (as it comes from the odp_manifests table)."""
    spec = {
        "target_table": target,
        "source_path": source_path,
        "load_type": load_type,
        "keys": ["customer_id"],
        "spec_version": 1,
        "schema_hints": None,
        "cdc": {"sequence_by": ["_seq"], "operation_column": "_op", "delete_values": ["D"]}
        if load_type == "cdc" else None,
        "full": {"allow_empty_snapshot": False} if load_type == "full" else None,
    }
    spec.update(spec_over)
    return {
        "contract_version": "2.0",
        "run_id": run_id,
        "data_path": f"data/{source_path}/run_id={run_id}/",
        "file_count": 1,
        "row_count": row_count,
        "seq_min": "1" if load_type == "cdc" else None,
        "seq_max": "9" if load_type == "cdc" else None,
        "schema_hash": "sha256:x",
        "extracted_at": "2026-10-16T02:00:00Z",
        "clex": {"pipeline_run_id": "p", "connector": "sqlserver", "instance": "i"},
        "load_spec": spec,
        "_ingested_at": ingested_at,
    }


CUSTOMERS = RuntimeEntity(entity="gdwr_customers", source_path="srv-motors-01/GDWR/dbo/customers",
                          load_type="cdc", keys=("customer_id",), spec_version=1, sequence_by=("_seq",),
                          operation_column="_op", delete_values=("D",),
                          schema_hints="customer_id BIGINT, _seq BIGINT")
POLICIES = RuntimeEntity(entity="gdwr_policies", source_path="srv-motors-01/GDWR/dbo/policies",
                         load_type="full", keys=("policy_id",), spec_version=1)
CONTRACTS = RuntimeEntity(entity="hbas_contracts", source_path="srv-home-02/HBAS/dbo/contracts",
                          load_type="cdc", keys=("contract_id", "line_no"), spec_version=2,
                          sequence_by=("_seq", "_seq_tiebreak"), operation_column="_op", delete_values=("D",))


# ---------------------------------------------------------------- Spark fake --
class FakeDF:
    def __init__(self, source: "FakeReader"):
        self.source = source
        self.ops: list[tuple] = []

    def withColumnRenamed(self, old, new):
        self.ops.append(("rename", old, new))
        return self

    def selectExpr(self, *exprs):
        self.ops.append(("selectExpr", exprs))
        return self

    def where(self, condition):
        self.ops.append(("where", condition))
        return self

    def drop(self, *cols):
        self.ops.append(("drop", cols))
        return self


class FakeReader:
    def __init__(self, streaming: bool):
        self.streaming = streaming
        self.fmt = None
        self.options: dict[str, str] = {}
        self.schema_ddl = None
        self.path = None
        self.table_name = None

    def format(self, fmt):
        self.fmt = fmt
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def schema(self, ddl):
        self.schema_ddl = ddl
        return self

    def load(self, path):
        self.path = path
        return FakeDF(self)

    def table(self, name):
        self.table_name = name
        return FakeDF(self)


class FakeSpark:
    def __init__(self):
        self.readers: list[FakeReader] = []

    @property
    def readStream(self):
        reader = FakeReader(streaming=True)
        self.readers.append(reader)
        return reader

    @property
    def read(self):
        reader = FakeReader(streaming=False)
        self.readers.append(reader)
        return reader


# ------------------------------------------------------------------ SDP fake --
class FakeDP:
    def __init__(self):
        self.tables: dict[str, dict] = {}
        self.views: dict[str, dict] = {}
        self.streaming_tables: dict[str, dict] = {}
        self.cdc_flows: list[dict] = []
        self.snapshot_flows: list[dict] = []

    def table(self, **kwargs):
        def decorator(fn):
            self.tables[kwargs["name"]] = {"kwargs": kwargs, "fn": fn}
            return fn

        return decorator

    def temporary_view(self, **kwargs):
        def decorator(fn):
            self.views[kwargs["name"]] = {"kwargs": kwargs, "fn": fn}
            return fn

        return decorator

    def create_streaming_table(self, **kwargs):
        self.streaming_tables[kwargs["name"]] = kwargs

    def create_auto_cdc_flow(self, **kwargs):
        self.cdc_flows.append(kwargs)

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.snapshot_flows.append(kwargs)


@pytest.fixture
def fake_spark() -> FakeSpark:
    return FakeSpark()


@pytest.fixture
def fake_dp() -> FakeDP:
    return FakeDP()
