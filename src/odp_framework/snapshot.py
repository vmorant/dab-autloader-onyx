"""Selección del siguiente snapshot completo para las entidades full.

La consolidación SCD1 de una entidad full usa
``create_auto_cdc_from_snapshot_flow``: compara cada snapshot con el anterior y
deduce inserts, updates y deletes. Un snapshot incompleto borraría filas de
silver, así que solo se aplica un run si:

* tiene manifiesto (CLEX lo escribe al final), y
* las filas que hay en bronze para ese ``run_id`` coinciden con las del manifiesto, y
* no está vacío (salvo ``allow_empty_snapshot``).

Los runs pendientes se aplican en orden de versión, todos en la misma
actualización pero cada uno en su commit, para que el change data feed de
silver refleje los cambios de cada snapshot. Si un run
más nuevo ya está completo, los anteriores incompletos se saltan: un snapshot
full sustituye a los previos.

Al empezar de cero (primera actualización o full refresh) se parte del snapshot
completo más reciente: en SCD1 el estado final es el mismo, y repasar todo el
histórico de bronze uno a uno sería muy caro.
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
    """Runs con versión válida posterior a ``latest_version``.

    Devuelve ``{run_id: (version, manifest_rows)}``. Sirve para contar en bronze
    solo los runs que pueden aplicarse.
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
    """Siguiente run a aplicar.

    Con ``latest_version`` (flow en marcha): el completo de menor versión posterior.
    Sin ella (primera vez o full refresh): el completo más reciente.
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
