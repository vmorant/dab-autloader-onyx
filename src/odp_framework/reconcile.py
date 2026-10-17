"""Manifest ↔ bronze reconciliation (pure logic, no Spark).

Run by the ``report`` task at the end of every job, whether it succeeded or
not. It only looks at deliveries of active tables whose specification matches
the registry: invalid manifests, conflicting ones or those of inactive tables
are already flagged by the ``register`` task when they arrive, and their runs
(``odp_rejected_runs``) do not count.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, Mapping

from .findings import ERROR, INFO, WARN, Finding, as_utc
from .manifest import run_version
from .runtime import RuntimeConfig


def _age_hours(ingested_at: datetime | None, now: datetime) -> float:
    ingested = as_utc(ingested_at)
    if ingested is None:
        return 0.0
    return (as_utc(now) - ingested).total_seconds() / 3600


def evaluate(
    cfg: RuntimeConfig,
    manifests: Iterable[Mapping[str, Any]],
    bronze_counts: Mapping[tuple[str, str], int],
    now: datetime,
    rejected: Mapping[str, frozenset[str]] | None = None,
) -> list[Finding]:
    """``manifests``: dicts with entity, run_id, load_type, source_path, row_count, _ingested_at.

    ``cfg.entities`` are the ACTIVE entities of the registry; ``rejected``,
    ``{table: run_ids}`` rejected by register.
    """
    rejected = rejected or {}
    findings: list[Finding] = []
    versions: dict[str, dict[int, set[str]]] = defaultdict(lambda: defaultdict(set))

    for m in manifests:
        entity = cfg.entity_by_name(m.get("entity") or "")
        run_id = m.get("run_id")
        version = run_version(run_id)
        if (entity is None or version is None
                or m.get("source_path") != entity.source_path
                or m.get("load_type") != entity.load_type
                or run_id in rejected.get(entity.entity, ())):
            continue
        versions[entity.entity][version].add(run_id)

        rows = m.get("row_count")
        if entity.is_full and rows == 0 and not entity.allow_empty_snapshot:
            findings.append(Finding(ERROR, "EMPTY_SNAPSHOT", entity.entity, run_id,
                                    "Empty full snapshot: not applied (it would delete the replica). If it is "
                                    "legitimate, set allow_empty_snapshot: true in the CLEX config (new spec_version)."))
            continue

        loaded = int(bronze_counts.get((entity.entity, run_id), 0))
        if rows is None or loaded == rows:
            continue
        age = _age_hours(m.get("_ingested_at"), now)
        if age * 60 < cfg.row_count_grace_minutes:
            findings.append(Finding(INFO, "PENDING", entity.entity, run_id,
                                    f"{loaded}/{rows} rows in bronze; within the grace period"))
        elif loaded == 0 and age < cfg.pending_alert_hours:
            findings.append(Finding(WARN, "NOT_INGESTED", entity.entity, run_id,
                                    f"Manifest {age:.1f} h old and 0/{rows} rows in bronze"))
        elif loaded == 0:
            findings.append(Finding(ERROR, "STALE_PENDING", entity.entity, run_id,
                                    f"Manifest {age:.1f} h old (> {cfg.pending_alert_hours} h) and not loaded; "
                                    "risk of losing the run when the landing expires"))
        else:
            findings.append(Finding(ERROR, "ROW_COUNT_MISMATCH", entity.entity, run_id,
                                    f"{loaded} rows in bronze, {rows} in the manifest"))

    for entity_name, by_version in versions.items():
        for version, run_ids in by_version.items():
            if len(run_ids) > 1:
                findings.append(Finding(WARN, "DUPLICATE_VERSION", entity_name, ",".join(sorted(run_ids)),
                                        f"Several runs with the same version {version}; only one is applied"))
    return findings
