"""Loading of the bundle resources (Python DABs).

Reads ``config/`` and generates, for each landing container, a job and an SDP
pipeline (silver). Tables are not configured here: the CLEX manifests define
them. All the logic lives in ``odp_framework.bundle`` (the same wheel the
pipelines run), so generator and runtime are always on the same version.
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
