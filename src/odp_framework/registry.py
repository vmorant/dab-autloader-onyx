"""Entity registry of a container, derived from the CLEX manifests.

The ``register`` task recomputes it on every run from **all** the manifests
received (deterministic and idempotent result). It is used by:

* ``register``: decides which tables to load into bronze (``entities_to_load``).
* ``ingest``: takes the specification of its table from here.
* the silver pipeline: defines its graph at the start of every update, and the
  flow of a table must always exist, even when nothing arrives for it that
  day; that is why it reads the registry and not individual manifests.

Guardrails on what CLEX may request:

* The manifest must comply with the contract (``manifest.validate_manifest``):
  the target is only a table name (catalog and schema are set by the
  platform), the path stays within the container's ``data/``...
* Once a table is registered, ``source_path``, ``load_type``, ``keys``,
  ``sequence_by`` and ``operation_column`` do not change: changing them would
  break the replica or the stream. A manifest that tries is ``SPEC_CONFLICT``
  and its run is not associated with the table.
* The rest of the specification (schema_hints, delete_values,
  allow_empty_snapshot) only changes by bumping ``spec_version``.
* Two tables cannot read the same ``source_path`` nor nested paths
  (``a/b/c`` and ``a/b/c/d``): Auto Loader reads the folder recursively.
* Manifests are processed in arrival order (``_ingested_at``) and, on ties, in
  version order: the registered specification is the first one accepted, even
  if a manifest with an earlier ``run_id`` arrives later.
* The status is decided by the platform: ``disabled_entities`` →  DISABLED; in
  ``manual`` mode only the ``approved_entities`` become ACTIVE.

Rejected runs (``odp_rejected_runs``): Auto Loader reads the whole folder of
each table, so the parquet files of a run whose manifest is rejected (invalid,
``SPEC_CONFLICT`` or ``SOURCE_PATH_CONFLICT``) still reach bronze if they are
in the folder of a registered table. Bronze keeps them as evidence; silver
excludes them by ``_run_id``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .findings import ERROR, INFO, WARN, Finding, as_utc
from .manifest import run_version, validate_manifest
from .runtime import RuntimeConfig, RuntimeEntity

ACTIVE = "ACTIVE"
PENDING_APPROVAL = "PENDING_APPROVAL"
DISABLED = "DISABLED"

IMMUTABLE_FIELDS = ("source_path", "load_type", "keys", "sequence_by", "operation_column")

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _arrival_key(manifest: Mapping[str, Any]) -> tuple:
    return (as_utc(manifest.get("_ingested_at")) or _EPOCH, run_version(manifest["run_id"]), manifest["run_id"])


def paths_overlap(a: str, b: str) -> bool:
    """Same path or one inside the other (Auto Loader reads recursively)."""
    return a == b or a.startswith(b + "/") or b.startswith(a + "/")

REGISTRY_SCHEMA_DDL = ", ".join(
    [
        "entity STRING",
        "source_path STRING",
        "load_type STRING",
        "keys ARRAY<STRING>",
        "sequence_by ARRAY<STRING>",
        "operation_column STRING",
        "delete_values ARRAY<STRING>",
        "schema_hints STRING",
        "allow_empty_snapshot BOOLEAN",
        "spec_version INT",
        "status STRING",
        "first_run_id STRING",
        "last_run_id STRING",
        "registered_at TIMESTAMP",
        "updated_at TIMESTAMP",
        "last_manifest_at TIMESTAMP",
    ]
)


@dataclass(frozen=True)
class RegistryEntry:
    spec: RuntimeEntity
    status: str
    first_run_id: str
    last_run_id: str
    registered_at: datetime | None
    updated_at: datetime | None
    last_manifest_at: datetime | None = None    # last valid manifest received


def spec_from_manifest(manifest: Mapping[str, Any]) -> RuntimeEntity:
    """Load specification of an already validated manifest."""
    spec = manifest["load_spec"]
    cdc = spec.get("cdc") or {}
    full = spec.get("full") or {}
    is_cdc = spec["load_type"] == "cdc"
    return RuntimeEntity(
        entity=spec["target_table"],
        source_path=spec["source_path"],
        load_type=spec["load_type"],
        keys=tuple(spec["keys"]),
        spec_version=int(spec["spec_version"]),
        sequence_by=tuple(cdc.get("sequence_by") or ()) if is_cdc else (),
        operation_column=cdc.get("operation_column") if is_cdc else None,
        delete_values=tuple(cdc.get("delete_values") or ()) if is_cdc else (),
        schema_hints=spec.get("schema_hints") or None,
        allow_empty_snapshot=bool(full.get("allow_empty_snapshot") or False) if not is_cdc else False,
    )


def _target_of(manifest: Mapping[str, Any]) -> str | None:
    spec = manifest.get("load_spec")
    return spec.get("target_table") if isinstance(spec, Mapping) else None


def status_for(cfg: RuntimeConfig, entity: str) -> str:
    if entity in cfg.disabled_entities:
        return DISABLED
    if cfg.registration == "manual" and entity not in cfg.approved_entities:
        return PENDING_APPROVAL
    return ACTIVE


def build_registry(
    cfg: RuntimeConfig,
    manifests: Iterable[Mapping[str, Any]],
    since: datetime | None = None,
) -> tuple[dict[str, RegistryEntry], list[Finding]]:
    """Recomputes the registry from all the manifests.

    Findings are only emitted for manifests ingested since ``since`` (the new
    ones in this run): a conflict is reported when it arrives, not in every
    later run.
    """
    since_utc = as_utc(since)
    findings: list[Finding] = []

    def emit(manifest: Mapping[str, Any], finding: Finding) -> None:
        ingested = as_utc(manifest.get("_ingested_at"))
        if since_utc is None or ingested is None or ingested >= since_utc:
            findings.append(finding)

    valid: list[Mapping[str, Any]] = []
    for m in manifests:
        errors = validate_manifest(dict(m), data_dir=cfg.data_dir, run_partition=cfg.run_partition)
        if errors:
            emit(m, Finding(ERROR, "INVALID_MANIFEST", _target_of(m), m.get("run_id"), "; ".join(errors)))
        else:
            valid.append(m)
    valid.sort(key=_arrival_key)

    state: dict[str, dict[str, Any]] = {}
    path_owner: dict[str, str] = {}
    for m in valid:
        spec = spec_from_manifest(m)
        run_id = m["run_id"]
        ts = m.get("_ingested_at")
        current = state.get(spec.entity)

        if current is None:
            clash = next((p for p in path_owner if paths_overlap(spec.source_path, p)), None)
            if clash is not None:
                emit(m, Finding(ERROR, "SOURCE_PATH_CONFLICT", spec.entity, run_id,
                                f"{spec.source_path} is the same as or nested with {clash}, already read by "
                                f"{path_owner[clash]}; not registered"))
                continue
            state[spec.entity] = {"spec": spec, "first_run_id": run_id, "last_run_id": run_id,
                                  "registered_at": ts, "updated_at": ts, "last_manifest_at": ts}
            path_owner[spec.source_path] = spec.entity
            continue

        registered: RuntimeEntity = current["spec"]
        diffs = [f for f in IMMUTABLE_FIELDS if getattr(spec, f) != getattr(registered, f)]
        if diffs:
            emit(m, Finding(ERROR, "SPEC_CONFLICT", spec.entity, run_id,
                            f"Changes immutable fields {diffs}; the run is not loaded. It requires a "
                            "new table or a migration controlled by the platform."))
            continue

        if spec.spec_version > registered.spec_version:
            current["spec"] = spec
            current["updated_at"] = ts
            emit(m, Finding(INFO, "SPEC_UPDATED", spec.entity, run_id,
                            f"spec_version {registered.spec_version} → {spec.spec_version}"))
        elif spec != registered:
            emit(m, Finding(WARN, "SPEC_CHANGED_WITHOUT_VERSION", spec.entity, run_id,
                            f"The specification changes without bumping spec_version ({spec.spec_version}); "
                            "the registered one is kept"))
        current["last_run_id"] = run_id
        current["last_manifest_at"] = _latest(current["last_manifest_at"], ts)

    entries: dict[str, RegistryEntry] = {}
    for name, s in state.items():
        status = status_for(cfg, name)
        entries[name] = RegistryEntry(spec=s["spec"], status=status, first_run_id=s["first_run_id"],
                                      last_run_id=s["last_run_id"], registered_at=s["registered_at"],
                                      updated_at=s["updated_at"], last_manifest_at=s["last_manifest_at"])

    for m in valid:
        entry = entries.get(spec_from_manifest(m).entity)
        if entry is None or entry.status == ACTIVE:
            continue
        code = "PENDING_APPROVAL" if entry.status == PENDING_APPROVAL else "DISABLED_ENTITY"
        emit(m, Finding(WARN, code, entry.spec.entity, m["run_id"],
                        "Delivery received but the table is not active: not loaded."))
    return entries, findings


REJECTED_RUNS_SCHEMA_DDL = "entity STRING, run_id STRING, reason STRING, manifest_target STRING"


@dataclass(frozen=True)
class RejectedRun:
    entity: str                     # registered table whose folder contains the run
    run_id: str
    reason: str                     # INVALID_MANIFEST | SPEC_CONFLICT | SOURCE_PATH_CONFLICT
    manifest_target: str | None     # what the rejected manifest requested


def _rejection_reason(cfg: RuntimeConfig, manifest: Mapping[str, Any], owner: RegistryEntry) -> str | None:
    """Why the manifest is not valid for the table that owns its folder (None = accepted)."""
    if validate_manifest(dict(manifest), data_dir=cfg.data_dir, run_partition=cfg.run_partition):
        return "INVALID_MANIFEST"
    spec = spec_from_manifest(manifest)
    if spec.entity != owner.spec.entity:
        return "SOURCE_PATH_CONFLICT"
    if any(getattr(spec, f) != getattr(owner.spec, f) for f in IMMUTABLE_FIELDS):
        return "SPEC_CONFLICT"
    return None


def _owner_of(path: str, owners: Mapping[str, RegistryEntry]) -> RegistryEntry | None:
    """Table whose Auto Loader reads ``path``: the one with the same path or with the longest parent path."""
    if path in owners:
        return owners[path]
    parents = [p for p in owners if path.startswith(p + "/")]
    return owners[max(parents, key=len)] if parents else None


def _run_locations(cfg: RuntimeConfig, manifest: Mapping[str, Any]) -> set[tuple[str, str]]:
    """Folders ``(path, run_id)`` where the manifest's parquet files may be.

    The one given by ``source_path`` + ``run_id`` and the one given by
    ``data_path`` (they may differ in an invalid manifest; in bronze the folder
    is what counts).
    """
    locations: set[tuple[str, str]] = set()
    spec = manifest.get("load_spec")
    path = spec.get("source_path") if isinstance(spec, Mapping) else None
    run_id = manifest.get("run_id")
    if isinstance(path, str) and path and isinstance(run_id, str) and run_id:
        locations.add((path, run_id))
    data_path = manifest.get("data_path")
    if isinstance(data_path, str):
        match = re.match(rf"^{re.escape(cfg.data_dir)}/(?P<path>.+)/{re.escape(cfg.run_partition)}"
                         r"=(?P<run>[^/]+)/?$", data_path)
        if match:
            locations.add((match["path"], match["run"]))
    return locations


def rejected_runs(
    cfg: RuntimeConfig,
    manifests: Iterable[Mapping[str, Any]],
    entries: Mapping[str, RegistryEntry],
) -> list[RejectedRun]:
    """Runs whose files fall in the folder of a registered table without a manifest accepted for it.

    Deterministic: recomputed on every ``register`` from all the manifests.
    "Falling in the folder" includes subfolders (Auto Loader reads recursively)
    and the folder given by ``data_path``. If the same run also has a manifest
    accepted for that table, it is not rejected. A rejected run whose folder is
    not read by any table never reaches bronze and does not appear here.

    Runs without a manifest (CLEX cut off halfway) are not included: CDC applies
    them, full does not (there is no manifest to declare them complete).
    """
    owners = {e.spec.source_path: e for e in entries.values()}
    accepted: set[tuple[str, str]] = set()
    candidates: dict[tuple[str, str], RejectedRun] = {}
    for m in manifests:
        for path, run_id in _run_locations(cfg, m):
            owner = _owner_of(path, owners)
            if owner is None:
                continue
            key = (owner.spec.entity, run_id)
            reason = _rejection_reason(cfg, m, owner)
            if reason is None:
                accepted.add(key)
            else:
                candidates.setdefault(key, RejectedRun(owner.spec.entity, run_id, reason, _target_of(m)))
    return [candidates[k] for k in sorted(candidates) if k not in accepted]


def rejected_rows(runs: Iterable[RejectedRun]) -> list[tuple]:
    return [(r.entity, r.run_id, r.reason, r.manifest_target) for r in runs]


def load_rejected_runs(spark, cfg: RuntimeConfig) -> dict[str, frozenset[str]]:
    """``{table: run_ids}`` that silver must not apply. Read by the silver pipeline and report."""
    if not spark.catalog.tableExists(cfg.rejected_runs_fqn):
        return {}
    out: dict[str, set[str]] = {}
    for row in spark.read.table(cfg.rejected_runs_fqn).select("entity", "run_id").collect():
        out.setdefault(row["entity"], set()).add(row["run_id"])
    return {k: frozenset(v) for k, v in out.items()}


def registry_rows(entries: Mapping[str, RegistryEntry]) -> list[tuple]:
    rows = []
    for name in sorted(entries):
        e = entries[name]
        s = e.spec
        rows.append((
            s.entity, s.source_path, s.load_type, list(s.keys), list(s.sequence_by),
            s.operation_column, list(s.delete_values), s.schema_hints, s.allow_empty_snapshot,
            s.spec_version, e.status, e.first_run_id, e.last_run_id,
            _naive(e.registered_at), _naive(e.updated_at), _naive(e.last_manifest_at),
        ))
    return rows


def _latest(a: datetime | None, b: datetime | None) -> datetime | None:
    if a is None or b is None:
        return a or b
    return a if as_utc(a) >= as_utc(b) else b


def entities_to_load(
    entries: Mapping[str, RegistryEntry],
    last_success: Mapping[str, datetime],
) -> list[str]:
    """Active tables with a manifest later than the start of their last successful load.

    Automatically includes those that failed in a previous run (no successful
    load after their last manifest) and excludes those that have received
    nothing new. Auto Loader is idempotent: loading too much does not duplicate.
    """
    out = []
    for name in sorted(entries):
        entry = entries[name]
        if entry.status != ACTIVE or entry.last_manifest_at is None:
            continue
        done = last_success.get(name)
        if done is None or as_utc(entry.last_manifest_at) > as_utc(done):
            out.append(name)
    return out


def _naive(ts: datetime | None) -> datetime | None:
    utc = as_utc(ts)
    return utc.replace(tzinfo=None) if utc else None


def entity_from_row(row: Mapping[str, Any]) -> RuntimeEntity:
    return RuntimeEntity(
        entity=row["entity"],
        source_path=row["source_path"],
        load_type=row["load_type"],
        keys=tuple(row["keys"] or ()),
        spec_version=int(row["spec_version"]),
        sequence_by=tuple(row.get("sequence_by") or ()),
        operation_column=row.get("operation_column"),
        delete_values=tuple(row.get("delete_values") or ()),
        schema_hints=row.get("schema_hints"),
        allow_empty_snapshot=bool(row.get("allow_empty_snapshot") or False),
    )


def load_active_entities(spark, cfg: RuntimeConfig) -> list[RuntimeEntity]:
    """Used by the pipelines when defining their graph."""
    if not spark.catalog.tableExists(cfg.registry_fqn):
        return []
    rows = spark.read.table(cfg.registry_fqn).where(f"status = '{ACTIVE}'").collect()
    return sorted((entity_from_row(r.asDict()) for r in rows), key=lambda e: e.entity)
