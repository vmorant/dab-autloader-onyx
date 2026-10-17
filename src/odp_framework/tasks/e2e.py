"""Tareas del job e2e (python_wheel_task, entry point ``odp-e2e``). Ver ``odp_framework.e2e``.

    odp-e2e --config_json=... --e2e_id={{job.run_id}} --start_ms={{job.start_time.timestamp_ms}} \\
            --action=publish|check|cleanup [--step=N]

``cleanup`` borra todo lo del e2e en este despliegue: es la tarea ``reset`` (al
empezar) y ``cleanup`` (al terminar, solo si todo ha ido bien).
"""

from __future__ import annotations

import sys

from .. import e2e
from ._common import base_parser, exit_with, get_spark, resolve_config


def main(argv: list[str] | None = None, io=None) -> int:
    parser = base_parser("odp-e2e")
    parser.add_argument("--action", choices=("publish", "check", "cleanup"), required=True)
    parser.add_argument("--step", type=int, default=0)
    parser.add_argument("--e2e_id", required=True, help="{{job.run_id}} del job e2e")
    parser.add_argument("--start_ms", required=True, help="{{job.start_time.timestamp_ms}}")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    if args.action != "cleanup" and args.step not in e2e.STEPS:
        parser.error(f"--step debe ser uno de {e2e.STEPS}")

    ctx = e2e.E2EContext.from_args(resolve_config(args, parser), args.e2e_id, args.start_ms)
    spark = get_spark()
    io = io or e2e.SparkLandingIO(spark)

    if args.action == "publish":
        for entity, run_id, note in e2e.publish(io, ctx, args.step):
            print(f"[odp-e2e] paso {args.step}: {entity} {run_id} — {note}")
        return 0

    if args.action == "check":
        results = e2e.run_checks(spark, e2e.checks(ctx, args.step))
        print(e2e.format_results(args.step, results))
        return 0 if all(r.ok for r in results) else 1

    warnings = e2e.cleanup(spark, io, ctx.cfg)
    for w in warnings:
        print(f"[odp-e2e] aviso de limpieza: {w}")
    print(f"[odp-e2e] limpieza del e2e en {ctx.cfg.schema} terminada ({len(warnings)} avisos)")
    return 0


def entrypoint() -> None:  # pragma: no cover - usado por python_wheel_task
    exit_with(main(), "odp-e2e")
