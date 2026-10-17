"""Bronze with Auto Loader (no SDP), with a fake Spark."""

from __future__ import annotations

from dataclasses import replace

import pytest

from odp_framework.config import load_containers
from odp_framework.ingest import MAX_SCHEMA_RESTARTS, ingest_entity, is_schema_change

from .conftest import CONFIG_DIR, CUSTOMERS, POLICIES


class UnknownFieldException(Exception):
    pass


class Query:
    def __init__(self, spark, table, error=None, rows=10):
        self.spark, self.table, self.error = spark, table, error
        self.recentProgress = [{"numInputRows": rows}]

    def awaitTermination(self):
        if self.error:
            raise self.error
        self.spark.existing.add(self.table)


class Stream:
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

    def load(self, path):
        self.options["path"] = path
        return self

    def selectExpr(self, *exprs):
        self.options["select"] = exprs
        return self

    def trigger(self, **kw):
        self.options["trigger"] = kw
        return self

    def toTable(self, name):
        self.spark.streams.append(self.options)
        error = self.spark.errors.pop(0) if self.spark.errors else None
        return Query(self.spark, name, error)


class Spark:
    def __init__(self, errors=(), existing=()):
        self.errors = list(errors)
        self.existing = set(existing)
        self.streams, self.sql_calls = [], []

    @property
    def readStream(self):
        return Stream(self)

    @property
    def catalog(self):
        spark = self

        class Catalog:
            @staticmethod
            def tableExists(name):
                return name in spark.existing

        return Catalog()

    def sql(self, statement):
        self.sql_calls.append(statement)


@pytest.fixture
def cfg(settings):
    return next(s for s in load_containers(CONFIG_DIR, settings) if s.container == "motors-sqlserver").runtime


def test_autoloader_stream_by_convention(cfg):
    spark = Spark()
    result = ingest_entity(spark, cfg, CUSTOMERS)
    (stream,) = spark.streams
    assert stream["format"] == "cloudFiles" and stream["cloudFiles.format"] == "parquet"
    assert stream["path"] == cfg.landing_root + "/data/srv-motors-01/GDWR/dbo/customers/"
    assert stream["cloudFiles.partitionColumns"] == "run_id"
    assert stream["cloudFiles.schemaHints"] == "customer_id BIGINT, _seq BIGINT"
    # Checkpoint and schema: fixed paths in the platform Volume
    vol = "/Volumes/odp_datateam01_dev_bronze/motors_sqlserver/odp_system/autoloader/gdwr_customers"
    assert stream["checkpointLocation"] == f"{vol}/checkpoint"
    assert stream["cloudFiles.schemaLocation"] == f"{vol}/schema"
    assert stream["mergeSchema"] == "true"
    assert stream["trigger"] == {"availableNow": True}
    assert stream["select"] == (
        "* EXCEPT (run_id)",
        "CAST(run_id AS STRING) AS _run_id",
        "_metadata.file_path AS _source_file",
        "current_timestamp() AS _ingested_at",
    )
    assert "cloudFiles.useManagedFileEvents" not in stream
    assert result.rows == 10 and result.attempts == 1 and result.created_table


def test_new_bronze_table_gets_append_only_and_comment(cfg):
    spark = Spark()
    ingest_entity(spark, cfg, CUSTOMERS)
    props, comment = spark.sql_calls
    assert props.startswith(f"ALTER TABLE {cfg.bronze_fqn(CUSTOMERS)} SET TBLPROPERTIES")
    assert "'delta.appendOnly' = 'true'" in props and "'odp.layer' = 'bronze'" in props
    assert comment.startswith(f"COMMENT ON TABLE {cfg.bronze_fqn(CUSTOMERS)}")


def test_full_tables_are_clustered_by_run_id(cfg):
    spark = Spark()
    ingest_entity(spark, cfg, POLICIES)
    assert spark.sql_calls[-1] == f"ALTER TABLE {cfg.bronze_fqn(POLICIES)} CLUSTER BY (_run_id)"


def test_existing_table_is_not_altered(cfg):
    spark = Spark(existing={cfg.bronze_fqn(CUSTOMERS)})
    result = ingest_entity(spark, cfg, CUSTOMERS)
    assert spark.sql_calls == [] and not result.created_table


def test_schema_change_restarts_the_stream(cfg):
    spark = Spark(errors=[UnknownFieldException("[UNKNOWN_FIELD_EXCEPTION] new column")])
    result = ingest_entity(spark, cfg, CUSTOMERS)
    assert result.attempts == 2 and len(spark.streams) == 2
    assert result.rows == 20                      # progress of both attempts


def test_schema_changes_have_a_limit(cfg):
    errors = [UnknownFieldException("x")] * (MAX_SCHEMA_RESTARTS + 1)
    with pytest.raises(UnknownFieldException):
        ingest_entity(Spark(errors=errors), cfg, CUSTOMERS)


def test_other_errors_are_not_retried(cfg):
    spark = Spark(errors=[RuntimeError("permission denied")])
    with pytest.raises(RuntimeError):
        ingest_entity(spark, cfg, CUSTOMERS)
    assert len(spark.streams) == 1


def test_file_events_option(cfg):
    spark = Spark()
    ingest_entity(spark, replace(cfg, file_events=True), POLICIES)
    assert spark.streams[0]["cloudFiles.useManagedFileEvents"] == "true"


def test_is_schema_change():
    assert is_schema_change(UnknownFieldException("x"))
    assert is_schema_change(RuntimeError("[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] ..."))
    assert not is_schema_change(RuntimeError("another error"))
