"""Generation of the bundle resources (Python DABs) from ``config/``.

For each active landing container:

* job ``ingest_<c>`` — file arrival trigger on ``_manifests/``::

      register  (wheel)      new manifests → odp_manifests → odp_entities registry
                             → which tables to load
      has_data               condition: any table with new data?
      ingest    (for_each)   one iteration per table: Auto Loader → bronze (append)
      silver    (pipeline)   SDP: SCD1 consolidation bronze → silver (AUTO CDC)
      report    (wheel)      reconciliation; run_if ALL_DONE

  register, the ingest iterations and report share one classic job cluster;
  silver uses its pipeline's cluster.

* pipeline ``silver_<c>`` (SDP, classic compute)
* (optional, ``manage_schemas``) bronze/silver schemas, with read grants for
  ``readers``, and the ``odp_system`` Volume
* (only containers with ``e2e: true``) job ``e2e_<c>``: simulates CLEX in three
  steps and runs the ingest job after each one (``odp_framework.e2e``)

The tables of each container do not appear here: CLEX defines them in the
manifest. Onboarding a new table does not require a redeploy.

Returns plain dictionaries (testable without the CLI); ``resources/__init__.py``
turns them into typed resources.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from . import __version__
from .config import ContainerSpec, DeploySettings, load_containers
from .runtime import SYSTEM_VOLUME

SILVER_SOURCE = "src/pipelines/silver.py"
TASKS_CLUSTER = "tasks"

# The run_as_sp writes to schemas and a Volume created (and owned) by the deploy SP.
RUN_AS_SCHEMA_PRIVILEGES = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
RUN_AS_VOLUME_PRIVILEGES = ["READ_VOLUME", "WRITE_VOLUME"]


@dataclass
class BundlePlan:
    pipelines: dict[str, dict] = field(default_factory=dict)
    jobs: dict[str, dict] = field(default_factory=dict)
    schemas: dict[str, dict] = field(default_factory=dict)
    volumes: dict[str, dict] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def _key(prefix: str, container: str) -> str:
    return f"{prefix}_{container.replace('-', '_')}"


def _tags(spec: ContainerSpec, resource_type: str) -> dict[str, str]:
    tags = dict(spec.tags)
    tags.setdefault("odp-container", spec.container)
    tags.setdefault("odp-custodian", spec.custodian)
    tags["resource-type"] = resource_type
    return tags


def silver_pipeline_spec(spec: ContainerSpec, settings: DeploySettings, schema_ref: str | None) -> dict:
    p = spec.pipelines
    cluster: dict = {
        "label": "default",
        "autoscale": {
            "min_workers": int(p["min_workers"]),
            "max_workers": int(p["max_workers"]),
            "mode": "ENHANCED",
        },
        "custom_tags": _tags(spec, "databricks-pipeline"),
    }
    if settings.pipeline_policy_id:
        cluster["policy_id"] = settings.pipeline_policy_id
        cluster["apply_policy_default_values"] = True

    return {
        "name": spec.names["silver_pipeline_name"],
        "catalog": spec.runtime.silver_catalog,
        "schema": schema_ref or spec.runtime.schema,
        "serverless": False,
        "continuous": False,
        "photon": bool(p["photon"]),
        "edition": str(p["edition"]),
        "channel": str(p["channel"]),
        "clusters": [cluster],
        "configuration": {
            # Container configuration; the tables come from the registry.
            "odp.config": spec.runtime.to_json(),
        },
        "libraries": [{"file": {"path": SILVER_SOURCE}}],
        # Versioned code: the wheel pinned for this target.
        "environment": {"dependencies": [settings.framework_wheel]},
        "tags": _tags(spec, "databricks-pipeline"),
    }


def _wheel_task(task_key: str, entry_point: str, description: str, spec: ContainerSpec,
                settings: DeploySettings, extra_params: dict | None = None, **extra) -> dict:
    params = {
        "config_json": spec.runtime.to_json(),
        "job_run_id": "{{job.run_id}}",
        **(extra_params or {}),
    }
    return {
        "task_key": task_key,
        "description": description,
        "job_cluster_key": TASKS_CLUSTER,
        "python_wheel_task": {
            "package_name": "odp_framework",
            "entry_point": entry_point,
            "named_parameters": params,
        },
        "libraries": [{"whl": settings.framework_wheel}],
        **extra,
    }


def _tasks_cluster(spec: ContainerSpec, settings: DeploySettings) -> dict:
    ingest = spec.ingest
    cluster: dict = {
        "spark_version": settings.spark_version,
        "autoscale": {"min_workers": int(ingest["min_workers"]), "max_workers": int(ingest["max_workers"])},
        "data_security_mode": "SINGLE_USER",
        "custom_tags": _tags(spec, "databricks-job"),
    }
    if settings.job_policy_id:
        cluster["policy_id"] = settings.job_policy_id
        cluster["apply_policy_default_values"] = True
    return cluster


def job_spec(spec: ContainerSpec, settings: DeploySettings, silver_key: str) -> dict:
    silver_ref = f"${{resources.pipelines.{silver_key}.id}}"
    trigger = spec.trigger
    ingest = spec.ingest
    fail_on_error = str(bool(spec.report["fail_on_error"])).lower()

    job: dict = {
        "name": spec.names["job_name"],
        "description": (
            f"Ingestion of container {spec.container} (custodian {spec.custodian}). Triggered when CLEX "
            "manifests arrive: register → ingest (Auto Loader per table) → silver (SCD1) → report."
        ),
        "tags": _tags(spec, "databricks-job"),
        # Never two runs at the same time on the same tables; runs triggered
        # in the meantime wait in the queue.
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "trigger": {
            "file_arrival": {
                "url": spec.runtime.manifests_root,
                "wait_after_last_change_seconds": int(trigger["wait_after_last_change_seconds"]),
                "min_time_between_triggers_seconds": int(trigger["min_time_between_triggers_seconds"]),
            }
        },
        "job_clusters": [{"job_cluster_key": TASKS_CLUSTER, "new_cluster": _tasks_cluster(spec, settings)}],
        "tasks": [
            _wheel_task("register", "odp-register",
                        "New manifests → table registry → list of tables to load",
                        spec, settings, {"fail_on_error": fail_on_error}, max_retries=0),
            {
                "task_key": "has_data",
                "description": "Without tables with new data there is nothing to load",
                "depends_on": [{"task_key": "register"}],
                "condition_task": {
                    "op": "GREATER_THAN",
                    "left": "{{tasks.register.values.load_count}}",
                    "right": "0",
                },
            },
            {
                "task_key": "ingest",
                "description": "One iteration per table: Auto Loader landing → bronze",
                "depends_on": [{"task_key": "has_data", "outcome": "true"}],
                "for_each_task": {
                    "inputs": "{{tasks.register.values.entities_to_load}}",
                    "concurrency": int(ingest["concurrency"]),
                    "task": _wheel_task(
                        "ingest_entity", "odp-ingest", "Auto Loader of one table into bronze",
                        spec, settings, {"entity": "{{input}}"},
                        max_retries=int(ingest["max_retries"]),
                        min_retry_interval_millis=60_000,
                    ),
                },
            },
            {
                "task_key": "silver",
                "description": "SCD1 consolidation bronze → silver (replica of the source)",
                "depends_on": [{"task_key": "ingest"}],
                # If one table fails, the rest are still consolidated: bronze is
                # append exactly-once and full snapshots are only applied when complete.
                "run_if": "ALL_DONE",
                "pipeline_task": {"pipeline_id": silver_ref, "full_refresh": False},
            },
            _wheel_task("report", "odp-report", "Manifest ↔ bronze reconciliation and alerts",
                        spec, settings, {"fail_on_error": fail_on_error},
                        depends_on=[{"task_key": "silver"}], run_if="ALL_DONE", max_retries=0),
        ],
    }
    if settings.notification_emails:
        job["email_notifications"] = {"on_failure": list(settings.notification_emails)}
    return job


def e2e_job_spec(spec: ContainerSpec, settings: DeploySettings, ingest_key: str) -> dict:
    """reset → publish_N → run_N (ingest job) → check_N, for N = 1..3 → cleanup."""
    from .e2e import STEPS

    ingest_ref = f"${{resources.jobs.{ingest_key}.id}}"
    # Same values in every task: names and run_ids of this execution.
    run_params = {"e2e_id": "{{job.run_id}}", "start_ms": "{{job.start_time.timestamp_ms}}"}

    def e2e_task(key: str, action: str, step: int, after: str | None, description: str, **extra) -> dict:
        params = {**run_params, "action": action, "step": str(step)}
        depends = {"depends_on": [{"task_key": after}]} if after else {}
        return _wheel_task(key, "odp-e2e", description, spec, settings, params,
                           max_retries=0, **depends, **extra)

    # Starts by deleting whatever a previous failed execution left in this deployment.
    tasks: list[dict] = [e2e_task("reset", "cleanup", 0, None, "Deletes leftovers of previous e2e executions")]
    previous: str | None = "reset"
    for step in STEPS:
        tasks.append(e2e_task(f"publish_{step}", "publish", step, previous,
                              f"Simulates CLEX: parquet files and manifests of step {step}"))
        tasks.append({
            "task_key": f"run_{step}",
            "description": f"Container ingest job after step {step}",
            "depends_on": [{"task_key": f"publish_{step}"}],
            "run_job_task": {"job_id": ingest_ref},
        })
        tasks.append(e2e_task(f"check_{step}", "check", step, f"run_{step}",
                              f"Checks bronze, silver, registry and findings of step {step}"))
        previous = f"check_{step}"
    # Only if everything succeeded (default run_if): on failure, the data stays for investigation.
    tasks.append(e2e_task("cleanup", "cleanup", 0, previous, "Deletes what this execution created"))

    job: dict = {
        "name": spec.names["e2e_job_name"],
        "description": (
            f"End-to-end test of container {spec.container}: simulates CLEX deliveries in three steps "
            "(initial load, changes, guardrails) and checks bronze, silver and findings. dev/pre only."
        ),
        "tags": _tags(spec, "databricks-job"),
        "max_concurrent_runs": 1,
        "timeout_seconds": 3 * 60 * 60,
        "job_clusters": [{"job_cluster_key": TASKS_CLUSTER, "new_cluster": _tasks_cluster(spec, settings)}],
        "tasks": tasks,
    }
    if settings.notification_emails:
        job["email_notifications"] = {"on_failure": list(settings.notification_emails)}
    return job


def build_plan(config_dir: Path, settings: DeploySettings) -> BundlePlan:
    plan = BundlePlan()
    if f"-{__version__}-" not in Path(settings.framework_wheel).name:
        plan.warnings.append(
            f"framework_wheel ({Path(settings.framework_wheel).name}) is not the generator version "
            f"({__version__}). Pipelines and tasks will run the wheel pinned in the target."
        )
    for spec in load_containers(config_dir, settings):
        silver_key = _key("silver", spec.container)
        silver_schema_ref: str | None = None

        if settings.manage_schemas:
            refs: dict[str, str] = {}
            for layer, catalog in (("bronze", spec.runtime.bronze_catalog),
                                   ("silver", spec.runtime.silver_catalog)):
                key = _key(f"{layer}_schema", spec.container)
                plan.schemas[key] = {
                    "catalog_name": catalog,
                    "name": spec.runtime.schema,
                    "comment": f"{layer} of container {spec.container} (custodian {spec.custodian})",
                }
                grants = [
                    # Read access for the custodian / team; only the run_as_sp writes.
                    {"principal": group, "privileges": ["USE_SCHEMA", "SELECT"]}
                    for group in spec.readers
                ]
                if settings.run_as_sp:
                    grants.append({"principal": settings.run_as_sp, "privileges": list(RUN_AS_SCHEMA_PRIVILEGES)})
                if grants:
                    plan.schemas[key]["grants"] = grants
                refs[layer] = f"${{resources.schemas.{key}.name}}"
            silver_schema_ref = refs["silver"]
            # In mode: development the bundle prefixes the schemas (dev_<user>_...):
            # the runtime configuration uses the deployed name, not the naming one.
            # Bronze and silver share the schema name (and prefix).
            spec = replace(spec, runtime=replace(spec.runtime, schema=refs["bronze"]))
            plan.volumes[_key("system_volume", spec.container)] = {
                "catalog_name": spec.runtime.bronze_catalog,
                "schema_name": refs["bronze"],
                "name": SYSTEM_VOLUME,
                "volume_type": "MANAGED",
                "comment": "Platform Auto Loader checkpoints and schemas. Do not touch.",
            }
            if settings.run_as_sp:
                plan.volumes[_key("system_volume", spec.container)]["grants"] = [
                    {"principal": settings.run_as_sp, "privileges": list(RUN_AS_VOLUME_PRIVILEGES)}]

        elif spec.readers:
            plan.warnings.append(
                f"{spec.container}: 'readers' only applies with manage_schemas; the grants on its "
                "schemas must be given outside the bundle."
            )

        if spec.e2e:
            # Each deployment (each developer in dev, the CI) uses its own subtree of the
            # e2e container: it does not register another one's manifests and they do not collide.
            ns = spec.runtime.schema
            spec = replace(spec, runtime=replace(
                spec.runtime,
                data_dir=f"{spec.runtime.data_dir}/{ns}",
                manifests_dir=f"{spec.runtime.manifests_dir}/{ns}",
            ))

        # The pipeline is kept even when the container is paused: deleting it
        # would delete its silver tables.
        plan.pipelines[silver_key] = silver_pipeline_spec(spec, settings, silver_schema_ref)
        if not spec.active:
            plan.warnings.append(f"{spec.container}: active: false — no job; schemas, Volume and pipeline are kept.")
            continue
        ingest_key = _key("ingest", spec.container)
        plan.jobs[ingest_key] = job_spec(spec, settings, silver_key)
        if spec.e2e:
            plan.jobs[_key("e2e", spec.container)] = e2e_job_spec(spec, settings, ingest_key)
    return plan
