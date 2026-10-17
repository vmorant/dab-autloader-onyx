"""Contract of the manifest that CLEX writes when each extraction finishes.

The manifest is both the trigger and the source of the load configuration: it
says which table it is (``load_spec.target_table``), where its data is
(``load_spec.source_path``) and how to consolidate it (``load_type``, keys,
sequence...). The CLEX configuration is approved by PR in its repo; the
manifest carries it.

Layout in the landing container (one container = custodian × CLEX)::

    data/{source_path}/run_id={run_id}/part-*.parquet
    _manifests/{source_path}/{run_id}.json          <- written LAST

Rules for CLEX:

* The parquet files of a run are written in their ``run_id=...`` folder and
  never overwritten; repeating an extraction produces a new ``run_id``.
* The manifest is written once all the parquet files of the run are written.
* ``run_id`` starts with the extraction instant in UTC
  (``YYYYMMDDTHHMMSSZ``): the sortable snapshot version comes from it.
* CDC: normalized ``operation_column`` and ``sequence_by`` increasing per PK
  across windows, never NULL. The initial load is sent as inserts.
* Checkpoints are NOT part of the contract: the platform manages them.
"""

from __future__ import annotations

import re
from typing import Any

CONTRACT_VERSION = "2.0"

RUN_ID_PATTERN = re.compile(r"^(?P<date>\d{8})T(?P<time>\d{6})Z_[A-Za-z0-9-]{1,64}$")
TABLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,254}$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9_.$-]+$")

LOAD_SPEC_DDL = (
    "STRUCT<"
    "target_table: STRING, source_path: STRING, load_type: STRING, keys: ARRAY<STRING>, "
    "cdc: STRUCT<sequence_by: ARRAY<STRING>, operation_column: STRING, delete_values: ARRAY<STRING>>, "
    "full: STRUCT<allow_empty_snapshot: BOOLEAN>, "
    "schema_hints: STRING, spec_version: INT"
    ">"
)

# Explicit schema for Auto Loader: the manifest is a contract, it is not inferred.
MANIFEST_SCHEMA_DDL = ", ".join(
    [
        "contract_version STRING",
        "run_id STRING",
        "data_path STRING",
        "file_count BIGINT",
        "row_count BIGINT",
        "seq_min STRING",
        "seq_max STRING",
        "schema_hash STRING",
        "extracted_at TIMESTAMP",
        "clex STRUCT<pipeline_run_id: STRING, connector: STRING, instance: STRING>",
        f"load_spec {LOAD_SPEC_DDL}",
    ]
)

_REQUIRED = ("contract_version", "run_id", "data_path", "file_count", "row_count",
             "extracted_at", "load_spec")
_SPEC_REQUIRED = ("target_table", "source_path", "load_type", "keys", "spec_version")


def run_version(run_id: str | None) -> int | None:
    """Sortable version (YYYYMMDDHHMMSS as an integer) or None if the run_id does not comply."""
    if not run_id:
        return None
    m = RUN_ID_PATTERN.match(run_id)
    if not m:
        return None
    return int(m.group("date") + m.group("time"))


def expected_data_path(data_dir: str, source_path: str, run_partition: str, run_id: str) -> str:
    return f"{data_dir}/{source_path}/{run_partition}={run_id}/"


def is_valid_source_path(source_path: Any) -> bool:
    if not isinstance(source_path, str) or source_path != source_path.strip("/"):
        return False
    segments = source_path.split("/")
    return len(segments) >= 2 and all(
        s not in ("", ".", "..") and _PATH_SEGMENT.match(s) for s in segments
    )


def _identifiers(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and bool(value) and all(
        isinstance(v, str) and IDENTIFIER_PATTERN.match(v) for v in value
    )


def validate_load_spec(spec: Any) -> list[str]:
    """Platform rules on what CLEX may request (guardrails)."""
    if not isinstance(spec, dict):
        return ["missing 'load_spec'"]
    errors = [f"missing 'load_spec.{k}'" for k in _SPEC_REQUIRED if spec.get(k) in (None, "", [])]
    if errors:
        return errors

    if not TABLE_NAME_PATTERN.match(str(spec["target_table"])):
        errors.append(f"target_table {spec['target_table']!r}: only lowercase letters, digits and _ (no catalog or schema)")
    if not is_valid_source_path(spec["source_path"]):
        errors.append(f"source_path {spec['source_path']!r}: {{connection}}/{{database}}/{{schema}}/{{table}} without '..' or a leading '/'")
    if not _identifiers(spec["keys"]):
        errors.append("keys must be a list of identifiers")
    version = spec["spec_version"]
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        errors.append("spec_version must be an integer >= 1")

    load_type = spec["load_type"]
    if load_type == "cdc":
        cdc = spec.get("cdc") or {}
        if not _identifiers(cdc.get("sequence_by")):
            errors.append("cdc.sequence_by is required (list of columns)")
        op = cdc.get("operation_column")
        if not isinstance(op, str) or not IDENTIFIER_PATTERN.match(op):
            errors.append("cdc.operation_column is required")
        deletes = cdc.get("delete_values")
        if not isinstance(deletes, (list, tuple)) or not deletes or not all(isinstance(v, str) for v in deletes):
            errors.append("cdc.delete_values is required (e.g. ['D'])")
        keys = set(spec["keys"] or ())
        if op in keys or keys & set(cdc.get("sequence_by") or ()):
            errors.append("operation_column and sequence_by cannot be keys")
    elif load_type != "full":
        errors.append(f"load_type {load_type!r} is neither 'cdc' nor 'full'")
    return errors


def validate_manifest(manifest: dict[str, Any], *, data_dir: str = "data", run_partition: str = "run_id") -> list[str]:
    """Validates an already parsed manifest. Returns the list of errors (empty = valid)."""
    errors = [f"missing '{key}'" for key in _REQUIRED if manifest.get(key) in (None, "")]
    if errors:
        return errors

    if manifest["contract_version"] != CONTRACT_VERSION:
        errors.append(f"contract_version {manifest['contract_version']!r} != {CONTRACT_VERSION!r}")
    if run_version(manifest["run_id"]) is None:
        errors.append(f"run_id {manifest['run_id']!r} does not match YYYYMMDDTHHMMSSZ_<id>")
    for key in ("file_count", "row_count"):
        value = manifest[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            errors.append(f"'{key}' must be an integer >= 0")

    spec_errors = validate_load_spec(manifest["load_spec"])
    errors.extend(spec_errors)
    if spec_errors:
        return errors

    spec = manifest["load_spec"]
    expected = expected_data_path(data_dir, spec["source_path"], run_partition, manifest["run_id"])
    if str(manifest["data_path"]).rstrip("/") != expected.rstrip("/"):
        errors.append(f"data_path {manifest['data_path']!r} != {expected!r}")
    if spec["load_type"] == "cdc":
        for key in ("seq_min", "seq_max"):
            if manifest.get(key) in (None, ""):
                errors.append(f"CDC without '{key}'")
    return errors
