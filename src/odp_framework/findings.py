"""Hallazgos (register y report) y utilidades de tiempo comunes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

ERROR = "ERROR"
WARN = "WARN"
INFO = "INFO"

FINDINGS_SCHEMA_DDL = ", ".join(
    [
        "report_ts TIMESTAMP",
        "job_run_id STRING",
        "phase STRING",
        "container STRING",
        "severity STRING",
        "code STRING",
        "entity STRING",
        "run_id STRING",
        "detail STRING",
    ]
)


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    entity: str | None
    run_id: str | None
    detail: str


def has_errors(findings: Iterable[Finding]) -> bool:
    return any(f.severity == ERROR for f in findings)


def as_utc(ts: datetime | None) -> datetime | None:
    """Spark devuelve timestamps sin zona (sesión en UTC): se tratan como UTC."""
    if ts is None:
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def findings_rows(findings: Iterable[Finding], *, now: datetime, job_run_id: str, phase: str,
                  container: str) -> list[tuple]:
    ts = as_utc(now).replace(tzinfo=None)
    return [
        (ts, job_run_id, phase, container, f.severity, f.code, f.entity, f.run_id, f.detail)
        for f in findings
    ]
