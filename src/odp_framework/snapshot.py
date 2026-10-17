"""Selection of the next complete snapshot for full entities.

The SCD1 consolidation of a full entity uses
``create_auto_cdc_from_snapshot_flow``: it compares each snapshot with the
previous one and derives inserts, updates and deletes. An incomplete snapshot
would delete rows from silver, so a run is only applied if:

* it has a manifest (CLEX writes it last), and
* the rows in bronze for that ``run_id`` match the manifest, and
* it is not empty (unless ``allow_empty_snapshot``).

Pending runs are applied in version order, all in the same update but each in
its own commit, so that the silver change data feed reflects the changes of
each snapshot. If a newer run is already complete, the older incomplete ones
are skipped: a full snapshot replaces the previous ones.

When starting from scratch (first update or full refresh) the flow starts from
the most recent complete snapshot: in SCD1 the final state is the same, and
replaying the whole bronze history one by one would be very expensive.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .manifest import run_version


@dataclass(frozen=True)
class RunState:
    run_id: str
    version: int
    manifest_rows: int | None
    bronze_rows: int


def is_complete(run: RunState, allow_empty: bool) -> bool:
    if run.manifest_rows is None:
        return False
    if run.manifest_rows == 0 and not allow_empty:
        return False
    return run.bronze_rows == run.manifest_rows


def candidate_run_ids(
    manifest_runs: Iterable[tuple[str, int | None]],
    latest_version: int | None,
) -> dict[str, tuple[int, int | None]]:
    """Runs with a valid version later than ``latest_version``.

    Returns ``{run_id: (version, manifest_rows)}``. Used to count in bronze only
    the runs that can be applied.
    """
    out: dict[str, tuple[int, int | None]] = {}
    for run_id, rows in manifest_runs:
        version = run_version(run_id)
        if version is None:
            continue
        if latest_version is not None and version <= latest_version:
            continue
        out[run_id] = (version, rows)
    return out


def select_next_run(
    runs: Iterable[RunState],
    latest_version: int | None,
    allow_empty: bool = False,
) -> RunState | None:
    """Next run to apply.

    With ``latest_version`` (flow running): the complete one with the lowest later version.
    Without it (first time or full refresh): the most recent complete one.
    """
    complete = [
        r for r in runs
        if (latest_version is None or r.version > latest_version) and is_complete(r, allow_empty)
    ]
    if not complete:
        return None
    if latest_version is None:
        return max(complete, key=lambda r: (r.version, r.run_id))
    return min(complete, key=lambda r: (r.version, r.run_id))


def has_applicable_run(runs: Iterable[RunState], allow_empty: bool = False) -> bool:
    return any(is_complete(r, allow_empty) for r in runs)
