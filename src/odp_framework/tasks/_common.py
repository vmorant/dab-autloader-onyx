"""Utilidades compartidas por las tareas wheel del job."""

from __future__ import annotations

import argparse
import json

from ..runtime import RuntimeConfig, load_runtime_config

# Límite de un task value que alimenta un for_each (48 KB): margen de seguridad.
TASK_VALUE_LIMIT = 45_000


def base_parser(prog: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=prog)
    p.add_argument("--config_json", help="odp.config del contenedor (lo pone el deploy)")
    p.add_argument("--config_file", help="Alternativa local: fichero con el JSON de odp.config")
    p.add_argument("--job_run_id", default="")
    return p


def resolve_config(args: argparse.Namespace, parser: argparse.ArgumentParser) -> RuntimeConfig:
    if args.config_json:
        return load_runtime_config(args.config_json)
    if args.config_file:
        with open(args.config_file, encoding="utf-8") as fh:
            return load_runtime_config(fh.read())
    parser.error("se necesita --config_json o --config_file")
    raise AssertionError  # pragma: no cover


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def set_task_value(key: str, value) -> None:
    """Publica un task value (for_each y condiciones del job). Fuera de Databricks no hace nada."""
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:  # pragma: no cover - fuera de Databricks
        return
    dbutils.jobs.taskValues.set(key=key, value=value)


class TaskFailed(RuntimeError):
    """Hace fallar la tarea del job (y dispara sus notificaciones)."""


def exit_with(rc: int, task: str) -> None:
    """Traduce el código de ``main`` a resultado de la tarea.

    Una excepción hace fallar una python_wheel_task sin depender de cómo trate
    el runtime un ``SystemExit``.
    """
    if rc:
        raise TaskFailed(f"{task} terminó con errores (código {rc}); el detalle está en la salida de la tarea")


def fit_task_value(names: list[str], limit: int = TASK_VALUE_LIMIT) -> tuple[list[str], list[str]]:
    """Recorta la lista para que quepa en un task value. Devuelve (cabe, pendiente)."""
    kept: list[str] = []
    for i, name in enumerate(names):
        if len(json.dumps(kept + [name])) > limit:
            return kept, names[i:]
        kept.append(name)
    return kept, []
