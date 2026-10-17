"""Carga de recursos del bundle (Python DABs).

Lee ``config/`` y genera, por cada contenedor del landing, un job y
una pipeline SDP (silver). Las tablas no se configuran aquí: las definen los manifiestos
de CLEX. Toda la lógica está en ``odp_framework.bundle`` (misma wheel
que ejecutan las pipelines), así generador y runtime van siempre en la misma
versión.
"""

from pathlib import Path

from databricks.bundles.core import Bundle, Resources
from databricks.bundles.jobs import Job
from databricks.bundles.pipelines import Pipeline
from databricks.bundles.schemas import Schema
from databricks.bundles.volumes import Volume

from odp_framework.bundle import build_plan
from odp_framework.config import ConfigError, DeploySettings

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def load_resources(bundle: Bundle) -> Resources:
    resources = Resources()
    try:
        settings = DeploySettings.from_variables(bundle.variables)
        plan = build_plan(CONFIG_DIR, settings)
    except ConfigError as exc:
        for error in exc.errors:
            resources.add_diagnostic_error(error)
        return resources

    for key, spec in plan.schemas.items():
        resources.add_schema(key, Schema.from_dict(spec))
    for key, spec in plan.volumes.items():
        resources.add_volume(key, Volume.from_dict(spec))
    for key, spec in plan.pipelines.items():
        resources.add_pipeline(key, Pipeline.from_dict(spec))
    for key, spec in plan.jobs.items():
        resources.add_job(key, Job.from_dict(spec))
    for warning in plan.warnings:
        resources.add_diagnostic_warning(warning)
    return resources
