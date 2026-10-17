"""Utilities shared by the job's wheel tasks."""

from __future__ import annotations

import argparse
import json

from ..runtime import RuntimeConfig, load_runtime_config

# Limit of a task value feeding a for_each (48 KB), with a safety margin.
TASK_VALUE_LIMIT = 45_000


def base_parser(prog: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=prog)
    p.add_argument("--config_json", help="odp.config of the container (set by the deploy)")
    p.add_argument("--config_file", help="Local alternative: file with the odp.config JSON")
    p.add_argument("--job_run_id", default="")
    return p


def resolve_config(args: argparse.Namespace, parser: argparse.ArgumentParser) -> RuntimeConfig:
    if args.config_json:
        return load_runtime_config(args.config_json)
    if args.config_file:
        with open(args.config_file, encoding="utf-8") as fh:
            return load_runtime_config(fh.read())
    parser.error("--config_json or --config_file is required")
    raise AssertionError  # pragma: no cover


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def set_task_value(key: str, value) -> None:
    """Publishes a task value (for_each and job conditions). Does nothing outside Databricks."""
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - outside Databricks
        return
    dbutils.jobs.taskValues.set(key=key, value=value)


class TaskFailed(RuntimeError):
    """Fails the job task (and triggers its notifications)."""


def exit_with(rc: int, task: str) -> None:
    """Turns the return code of ``main`` into the task result.

    An exception fails a python_wheel_task without depending on how the
    runtime treats a ``SystemExit``.
    """
    if rc:
        raise TaskFailed(f"{task} finished with errors (code {rc}); details are in the task output")


def fit_task_value(names: list[str], limit: int = TASK_VALUE_LIMIT) -> tuple[list[str], list[str]]:
    """Trims the list so it fits in a task value. Returns (fits, pending)."""
    kept: list[str] = []
    for i, name in enumerate(names):
        if len(json.dumps(kept + [name])) > limit:
            return kept, names[i:]
        kept.append(name)
    return kept, []
