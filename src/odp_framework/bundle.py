"""Generación de recursos del bundle (Python DABs) a partir de ``config/``.

Por cada contenedor activo del landing:

* job ``ingest_<c>`` — trigger file arrival sobre ``_manifests/``::

      register  (wheel)      manifiestos nuevos → odp_manifests → registro odp_entities
                             → qué tablas cargar
      has_data               condición: ¿alguna tabla con datos nuevos?
      ingest    (for_each)   una iteración por tabla: Auto Loader → bronze (append)
      silver    (pipeline)   SDP: consolidación SCD1 bronze → silver (AUTO CDC)
      report    (wheel)      reconciliación; run_if ALL_DONE

  register, las iteraciones de ingest y report comparten un job cluster
  clásico; silver usa el cluster de su pipeline.

* pipeline ``silver_<c>`` (SDP, compute clásico)
* (opcional, ``manage_schemas``) esquemas bronze/silver, con grants de lectura
  para ``readers``, y Volume ``odp_system``
* (solo contenedores con ``e2e: true``) job ``e2e_<c>``: simula a CLEX en tres
  pasos y ejecuta el job de ingesta tras cada uno (``odp_framework.e2e``)

Las tablas de cada contenedor no aparecen aquí: las define CLEX en el manifiesto.
Dar de alta una tabla nueva no requiere redesplegar.

Devuelve diccionarios planos (testeables sin el CLI); ``resources/__init__.py``
los convierte en recursos tipados.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from . import __version__
from .config import ContainerSpec, DeploySettings, load_containers
from .runtime import SYSTEM_VOLUME

SILVER_SOURCE = "src/pipelines/silver.py"
TASKS_CLUSTER = "tasks"

# El run_as_sp escribe en esquemas y Volume que crea (y posee) el SP de deploy.
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
            # Configuración del contenedor; las tablas salen del registro.
            "odp.config": spec.runtime.to_json(),
        },
        "libraries": [{"file": {"path": SILVER_SOURCE}}],
        # Código versionado: la wheel fijada para este target.
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
            f"Ingesta del contenedor {spec.container} (custodian {spec.custodian}). Se dispara al llegar "
            "manifiestos de CLEX: register → ingest (Auto Loader por tabla) → silver (SCD1) → report."
        ),
        "tags": _tags(spec, "databricks-job"),
        # Nunca dos ejecuciones a la vez sobre las mismas tablas; las que
        # lleguen mientras tanto esperan en cola.
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
                        "Manifiestos nuevos → registro de tablas → lista de tablas a cargar",
                        spec, settings, {"fail_on_error": fail_on_error}, max_retries=0),
            {
                "task_key": "has_data",
                "description": "Sin tablas con datos nuevos no hay nada que cargar",
                "depends_on": [{"task_key": "register"}],
                "condition_task": {
                    "op": "GREATER_THAN",
                    "left": "{{tasks.register.values.load_count}}",
                    "right": "0",
                },
            },
            {
                "task_key": "ingest",
                "description": "Una iteración por tabla: Auto Loader landing → bronze",
                "depends_on": [{"task_key": "has_data", "outcome": "true"}],
                "for_each_task": {
                    "inputs": "{{tasks.register.values.entities_to_load}}",
                    "concurrency": int(ingest["concurrency"]),
                    "task": _wheel_task(
                        "ingest_entity", "odp-ingest", "Auto Loader de una tabla a bronze",
                        spec, settings, {"entity": "{{input}}"},
                        max_retries=int(ingest["max_retries"]),
                        min_retry_interval_millis=60_000,
                    ),
                },
            },
            {
                "task_key": "silver",
                "description": "Consolidación SCD1 bronze → silver (réplica de origen)",
                "depends_on": [{"task_key": "ingest"}],
                # Si falla una tabla, el resto se consolida igual: bronze es
                # append exactly-once y los snapshots full solo se aplican completos.
                "run_if": "ALL_DONE",
                "pipeline_task": {"pipeline_id": silver_ref, "full_refresh": False},
            },
            _wheel_task("report", "odp-report", "Reconciliación manifiesto ↔ bronze y alertas",
                        spec, settings, {"fail_on_error": fail_on_error},
                        depends_on=[{"task_key": "silver"}], run_if="ALL_DONE", max_retries=0),
        ],
    }
    if settings.notification_emails:
        job["email_notifications"] = {"on_failure": list(settings.notification_emails)}
    return job


def e2e_job_spec(spec: ContainerSpec, settings: DeploySettings, ingest_key: str) -> dict:
    """reset → publish_N → run_N (job de ingesta) → check_N, para N = 1..3 → cleanup."""
    from .e2e import STEPS

    ingest_ref = f"${{resources.jobs.{ingest_key}.id}}"
    # Mismos valores en todas las tareas: nombres y run_ids de esta ejecución.
    run_params = {"e2e_id": "{{job.run_id}}", "start_ms": "{{job.start_time.timestamp_ms}}"}

    def e2e_task(key: str, action: str, step: int, after: str | None, description: str, **extra) -> dict:
        params = {**run_params, "action": action, "step": str(step)}
        depends = {"depends_on": [{"task_key": after}]} if after else {}
        return _wheel_task(key, "odp-e2e", description, spec, settings, params,
                           max_retries=0, **depends, **extra)

    # Empieza borrando lo que dejara una ejecución anterior fallida en este despliegue.
    tasks: list[dict] = [e2e_task("reset", "cleanup", 0, None, "Borra restos de ejecuciones anteriores del e2e")]
    previous: str | None = "reset"
    for step in STEPS:
        tasks.append(e2e_task(f"publish_{step}", "publish", step, previous,
                              f"Simula a CLEX: parquet y manifiestos del paso {step}"))
        tasks.append({
            "task_key": f"run_{step}",
            "description": f"Job de ingesta del contenedor tras el paso {step}",
            "depends_on": [{"task_key": f"publish_{step}"}],
            "run_job_task": {"job_id": ingest_ref},
        })
        tasks.append(e2e_task(f"check_{step}", "check", step, f"run_{step}",
                              f"Comprueba bronze, silver, registro y hallazgos del paso {step}"))
        previous = f"check_{step}"
    # Solo si todo ha ido bien (run_if por defecto): si falla, los datos se quedan para investigar.
    tasks.append(e2e_task("cleanup", "cleanup", 0, previous, "Borra lo creado por esta ejecución"))

    job: dict = {
        "name": spec.names["e2e_job_name"],
        "description": (
            f"Prueba end-to-end del contenedor {spec.container}: simula entregas de CLEX en tres pasos "
            "(carga inicial, cambios, guardarraíles) y comprueba bronze, silver y hallazgos. Solo dev/pre."
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
            f"framework_wheel ({Path(settings.framework_wheel).name}) no es la versión del generador "
            f"({__version__}). Pipelines y tareas ejecutarán la wheel fijada en el target."
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
                    "comment": f"{layer} del contenedor {spec.container} (custodian {spec.custodian})",
                }
                grants = [
                    # Lectura para el custodian / equipo; escribir solo lo hace el run_as_sp.
                    {"principal": group, "privileges": ["USE_SCHEMA", "SELECT"]}
                    for group in spec.readers
                ]
                if settings.run_as_sp:
                    grants.append({"principal": settings.run_as_sp, "privileges": list(RUN_AS_SCHEMA_PRIVILEGES)})
                if grants:
                    plan.schemas[key]["grants"] = grants
                refs[layer] = f"${{resources.schemas.{key}.name}}"
            silver_schema_ref = refs["silver"]
            # En mode: development el bundle prefija los esquemas (dev_<usuario>_...):
            # la configuración de runtime usa el nombre desplegado, no el del naming.
            # Bronze y silver comparten nombre de esquema (y prefijo).
            spec = replace(spec, runtime=replace(spec.runtime, schema=refs["bronze"]))
            plan.volumes[_key("system_volume", spec.container)] = {
                "catalog_name": spec.runtime.bronze_catalog,
                "schema_name": refs["bronze"],
                "name": SYSTEM_VOLUME,
                "volume_type": "MANAGED",
                "comment": "Checkpoints y esquemas de Auto Loader de la plataforma. No tocar.",
            }
            if settings.run_as_sp:
                plan.volumes[_key("system_volume", spec.container)]["grants"] = [
                    {"principal": settings.run_as_sp, "privileges": list(RUN_AS_VOLUME_PRIVILEGES)}]

        elif spec.readers:
            plan.warnings.append(
                f"{spec.container}: 'readers' solo se aplica con manage_schemas; los grants de sus "
                "esquemas hay que darlos fuera del bundle."
            )

        if spec.e2e:
            # Cada despliegue (cada desarrollador en dev, el CI) usa su subárbol del
            # contenedor e2e: no registra los manifiestos de otro ni se pisan.
            ns = spec.runtime.schema
            spec = replace(spec, runtime=replace(
                spec.runtime,
                data_dir=f"{spec.runtime.data_dir}/{ns}",
                manifests_dir=f"{spec.runtime.manifests_dir}/{ns}",
            ))

        # La pipeline se mantiene aunque el contenedor esté en pausa: borrarla
        # borraría sus tablas de silver.
        plan.pipelines[silver_key] = silver_pipeline_spec(spec, settings, silver_schema_ref)
        if not spec.active:
            plan.warnings.append(f"{spec.container}: active: false — sin job; esquemas, Volume y pipeline se conservan.")
            continue
        ingest_key = _key("ingest", spec.container)
        plan.jobs[ingest_key] = job_spec(spec, settings, silver_key)
        if spec.e2e:
            plan.jobs[_key("e2e", spec.container)] = e2e_job_spec(spec, settings, ingest_key)
    return plan
