"""Registro de entidades de un contenedor, derivado de los manifiestos de CLEX.

La tarea ``register`` lo recalcula en cada ejecución a partir de **todos** los
manifiestos recibidos (resultado determinista e idempotente). Lo usan:

* ``register``: decide qué tablas cargar en bronze (``entities_to_load``).
* ``ingest``: toma de aquí la especificación de su tabla.
* la pipeline de silver: define su grafo al arrancar cada actualización y el
  flow de una tabla tiene que existir siempre, también cuando ese día no
  llega nada de ella; por eso lee el registro y no los manifiestos sueltos.

Guardarraíles sobre lo que CLEX puede pedir:

* El manifiesto tiene que cumplir el contrato (``manifest.validate_manifest``):
  el destino es solo un nombre de tabla (el catálogo y el esquema los pone la
  plataforma), la ruta queda dentro de ``data/`` del contenedor...
* Una vez registrada una tabla, ``source_path``, ``load_type``, ``keys``,
  ``sequence_by`` y ``operation_column`` no cambian: cambiarlos rompería la
  réplica o el stream. Un manifiesto que lo intente es ``SPEC_CONFLICT`` y su
  run no se asocia a la tabla.
* El resto de la especificación (schema_hints, delete_values,
  allow_empty_snapshot) solo cambia subiendo ``spec_version``.
* Dos tablas no pueden leer la misma ``source_path`` ni rutas anidadas
  (``a/b/c`` y ``a/b/c/d``): Auto Loader lee la carpeta de forma recursiva.
* Los manifiestos se procesan en orden de llegada (``_ingested_at``) y, a
  igualdad, de versión: la especificación registrada es la primera que se
  aceptó, aunque después llegue un manifiesto con un ``run_id`` anterior.
* El estado lo decide la plataforma: ``disabled_entities`` →  DISABLED; en modo
  ``manual`` solo las ``approved_entities`` quedan ACTIVE.

Runs rechazados (``odp_rejected_runs``): Auto Loader lee la carpeta completa de
cada tabla, así que los parquet de un run cuyo manifiesto se rechaza (inválido,
``SPEC_CONFLICT`` o ``SOURCE_PATH_CONFLICT``) llegan igualmente a bronze si
están en la carpeta de una tabla registrada. Bronze los conserva como
evidencia; silver los excluye por ``_run_id``.
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
    """Misma ruta o una dentro de la otra (Auto Loader lee recursivamente)."""
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
    last_manifest_at: datetime | None = None    # último manifiesto válido recibido


def spec_from_manifest(manifest: Mapping[str, Any]) -> RuntimeEntity:
    """Especificación de carga de un manifiesto ya validado."""
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
    """Recalcula el registro desde todos los manifiestos.

    Los hallazgos solo se emiten para manifiestos ingeridos desde ``since``
    (los nuevos de esta ejecución): un conflicto se avisa cuando llega, no en
    todas las ejecuciones posteriores.
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
                                f"{spec.source_path} coincide o se anida con {clash}, que ya lee "
                                f"{path_owner[clash]}; no se registra"))
                continue
            state[spec.entity] = {"spec": spec, "first_run_id": run_id, "last_run_id": run_id,
                                  "registered_at": ts, "updated_at": ts, "last_manifest_at": ts}
            path_owner[spec.source_path] = spec.entity
            continue

        registered: RuntimeEntity = current["spec"]
        diffs = [f for f in IMMUTABLE_FIELDS if getattr(spec, f) != getattr(registered, f)]
        if diffs:
            emit(m, Finding(ERROR, "SPEC_CONFLICT", spec.entity, run_id,
                            f"Cambia campos inmutables {diffs}; el run no se carga. Requiere una "
                            "tabla nueva o una migración controlada por la plataforma."))
            continue

        if spec.spec_version > registered.spec_version:
            current["spec"] = spec
            current["updated_at"] = ts
            emit(m, Finding(INFO, "SPEC_UPDATED", spec.entity, run_id,
                            f"spec_version {registered.spec_version} → {spec.spec_version}"))
        elif spec != registered:
            emit(m, Finding(WARN, "SPEC_CHANGED_WITHOUT_VERSION", spec.entity, run_id,
                            f"La especificación cambia sin subir spec_version ({spec.spec_version}); "
                            "se mantiene la registrada"))
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
                        "Entrega recibida pero la tabla no está activa: no se carga."))
    return entries, findings


REJECTED_RUNS_SCHEMA_DDL = "entity STRING, run_id STRING, reason STRING, manifest_target STRING"


@dataclass(frozen=True)
class RejectedRun:
    entity: str                     # tabla registrada cuya carpeta contiene el run
    run_id: str
    reason: str                     # INVALID_MANIFEST | SPEC_CONFLICT | SOURCE_PATH_CONFLICT
    manifest_target: str | None     # lo que pedía el manifiesto rechazado


def _rejection_reason(cfg: RuntimeConfig, manifest: Mapping[str, Any], owner: RegistryEntry) -> str | None:
    """Motivo por el que el manifiesto no vale para la tabla dueña de su carpeta (None = aceptado)."""
    if validate_manifest(dict(manifest), data_dir=cfg.data_dir, run_partition=cfg.run_partition):
        return "INVALID_MANIFEST"
    spec = spec_from_manifest(manifest)
    if spec.entity != owner.spec.entity:
        return "SOURCE_PATH_CONFLICT"
    if any(getattr(spec, f) != getattr(owner.spec, f) for f in IMMUTABLE_FIELDS):
        return "SPEC_CONFLICT"
    return None


def _owner_of(path: str, owners: Mapping[str, RegistryEntry]) -> RegistryEntry | None:
    """Tabla cuyo Auto Loader lee ``path``: la de la misma ruta o la de la ruta padre más larga."""
    if path in owners:
        return owners[path]
    parents = [p for p in owners if path.startswith(p + "/")]
    return owners[max(parents, key=len)] if parents else None


def _run_locations(cfg: RuntimeConfig, manifest: Mapping[str, Any]) -> set[tuple[str, str]]:
    """Carpetas ``(ruta, run_id)`` donde pueden estar los parquet del manifiesto.

    La que dicen ``source_path`` + ``run_id`` y la que dice ``data_path`` (en un
    manifiesto inválido pueden no coincidir; en bronze manda la carpeta).
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
    """Runs cuyos ficheros caen en la carpeta de una tabla registrada sin un manifiesto aceptado para ella.

    Determinista: se recalcula en cada ``register`` desde todos los manifiestos.
    "Caer en la carpeta" incluye subcarpetas (Auto Loader lee recursivamente) y
    la carpeta que indica ``data_path``. Si el mismo run tiene además un
    manifiesto aceptado para esa tabla, no se rechaza. Un run rechazado cuya
    carpeta no lee ninguna tabla no llega a bronze y no aparece aquí.

    Los runs sin manifiesto (CLEX cortado a mitad) no están: CDC los aplica,
    full no (no hay manifiesto que los dé por completos).
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
    """``{tabla: run_ids}`` que silver no debe aplicar. Lo leen la pipeline de silver y report."""
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
    """Tablas activas con algún manifiesto posterior al inicio de su última carga correcta.

    Incluye solas las que fallaron en una ejecución anterior (no tienen carga
    correcta posterior a su último manifiesto) y excluye las que no han
    recibido nada nuevo. Auto Loader es idempotente: cargar de más no duplica.
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
    """Lo usan las pipelines al definir su grafo."""
    if not spark.catalog.tableExists(cfg.registry_fqn):
        return []
    rows = spark.read.table(cfg.registry_fqn).where(f"status = '{ACTIVE}'").collect()
    return sorted((entity_from_row(r.asDict()) for r in rows), key=lambda e: e.entity)
