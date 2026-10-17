"""Carga y validación de la configuración del repo (solo en el deploy).

Estructura::

    config/
      platform.yml       convenciones de plataforma (naming, compute, trigger...)
      containers.yml     LISTA de contenedores del landing: una entrada = un job

Dar de alta un contenedor nuevo es añadir una entrada a ``containers.yml`` (PR)
y desplegar: el generador crea su job, su pipeline de silver, sus esquemas y su
Volume de sistema. La ruta del landing no se escribe: sale del naming
(``abfss://{container}@{storage_account}...``). Con ``environments`` se puede
dar de alta primero solo en dev y promocionarlo después. ``active: false`` es una
pausa: no se genera el job, pero esquemas, Volume y pipeline (y con ella las
tablas de silver) se conservan.

Las tablas de cada contenedor NO se configuran aquí: las define CLEX en el
manifiesto de cada extracción (``load_spec``) y la tarea ``register`` las
consolida en el registro ``odp_entities``.

Cualquier error se acumula y se informa de una vez: el deploy falla antes de
tocar el workspace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from . import __version__
from .manifest import TABLE_NAME_PATTERN
from .runtime import REGISTRATION_MODES, RuntimeConfig
from .sql import is_identifier

PLATFORM_FILE = "platform.yml"
CONTAINERS_FILE = "containers.yml"
ENVIRONMENTS = ("dev", "pre", "pro")

_CONTAINER_NAME = re.compile(r"^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$")

PLATFORM_DEFAULTS: dict[str, Any] = {
    "naming": {
        "bronze_catalog": "odp_{team_id}_{env}_bronze",
        "silver_catalog": "odp_{team_id}_{env}_silver",
        "schema": "{container_snake}",
        "landing_root": "abfss://{container}@{storage_account}.dfs.core.windows.net",
        "job_name": "job-{team_id}-{env}-{container}-ingest",
        "silver_pipeline_name": "sdp-{team_id}-{env}-{container}-silver",
        "e2e_job_name": "job-{team_id}-{env}-{container}-e2e",
    },
    "layout": {"data_dir": "data", "manifests_dir": "_manifests", "run_partition": "run_id"},
    "registration": {"mode": "auto"},
    "ingest": {"concurrency": 8, "min_workers": 1, "max_workers": 4, "max_retries": 1},
    "pipelines": {"edition": "PRO", "channel": "CURRENT", "photon": False,
                  "min_workers": 1, "max_workers": 4},
    "trigger": {"wait_after_last_change_seconds": 300, "min_time_between_triggers_seconds": 900},
    "report": {"pending_alert_hours": 24, "row_count_grace_minutes": 60,
               "lookback_days": 7, "fail_on_error": True},
    "tags": {},
}

_CONTAINER_KEYS = {"container", "custodian", "description", "data_class", "active",
                   "environments", "storage_account", "readers",
                   "registration", "approved_entities", "disabled_entities",
                   "ingest", "trigger", "pipelines", "report", "e2e"}


class ConfigError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("Configuración inválida:\n  - " + "\n  - ".join(errors))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class DeploySettings:
    """Valores por target que llegan como variables del bundle."""

    team_id: str
    env: str
    landing_storage_account: str
    framework_wheel: str
    pipeline_policy_id: str = ""
    job_policy_id: str = ""
    spark_version: str = "16.4.x-scala2.12"
    notification_emails: tuple[str, ...] = ()
    file_events: bool = False
    manage_schemas: bool = False
    run_as_sp: str = ""                 # application id; vacío en dev (run_as = quien despliega)

    @classmethod
    def from_variables(cls, variables: Mapping[str, Any]) -> "DeploySettings":
        def get(name: str, default: str = "") -> str:
            value = variables.get(name, default)
            return "" if value is None else str(value).strip()

        missing = [n for n in ("team_id", "env", "landing_storage_account", "framework_wheel") if not get(n)]
        if missing:
            raise ConfigError([f"variable del bundle sin valor: {n}" for n in missing])
        emails = tuple(e.strip() for e in get("notification_emails").split(",") if e.strip())
        return cls(
            team_id=get("team_id"),
            env=get("env"),
            landing_storage_account=get("landing_storage_account"),
            framework_wheel=get("framework_wheel"),
            pipeline_policy_id=get("pipeline_policy_id"),
            job_policy_id=get("job_policy_id"),
            spark_version=get("spark_version", "16.4.x-scala2.12"),
            notification_emails=emails,
            file_events=_as_bool(get("file_events", "false")),
            manage_schemas=_as_bool(get("manage_schemas", "false")),
            run_as_sp=get("run_as_sp"),
        )


@dataclass(frozen=True)
class ContainerSpec:
    container: str
    custodian: str
    description: str
    data_class: str
    runtime: RuntimeConfig
    names: dict[str, str]
    ingest: dict[str, Any]
    pipelines: dict[str, Any]
    trigger: dict[str, Any]
    report: dict[str, Any]
    tags: dict[str, str]
    readers: tuple[str, ...] = ()       # grupos con lectura sobre los esquemas del contenedor
    e2e: bool = False                   # genera además el job e2e (simulador de CLEX)
    active: bool = True                 # False = pausa: sin job; datos, esquemas y pipeline se conservan


def _read_yaml(path: Path) -> Any:
    import yaml  # dependencia de desarrollo: solo en el deploy

    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _merge(base: Mapping[str, Any], override: Mapping[str, Any] | None) -> dict[str, Any]:
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in base.items()}
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


def _render(template: str, ctx: Mapping[str, str], where: str, errors: list[str]) -> str:
    try:
        return template.format(**ctx)
    except (KeyError, IndexError, ValueError) as exc:
        errors.append(f"{where}: plantilla {template!r} inválida ({exc})")
        return template


def load_platform(config_dir: Path) -> dict[str, Any]:
    path = config_dir / PLATFORM_FILE
    data = _read_yaml(path) if path.exists() else {}
    return _merge(PLATFORM_DEFAULTS, data)


def _table_list(raw: Mapping[str, Any], key: str, where: str, errors: list[str]) -> tuple[str, ...]:
    values = raw.get(key) or []
    if not isinstance(values, list) or not all(isinstance(v, str) and TABLE_NAME_PATTERN.match(v) for v in values):
        errors.append(f"{where}: '{key}' debe ser una lista de nombres de tabla")
        return ()
    return tuple(values)


def _string_list(raw: Mapping[str, Any], key: str, where: str, errors: list[str]) -> tuple[str, ...]:
    values = raw.get(key) or []
    if not isinstance(values, list) or not all(isinstance(v, str) and v.strip() for v in values):
        errors.append(f"{where}: '{key}' debe ser una lista de textos")
        return ()
    return tuple(values)


def applies_to(raw: Mapping[str, Any], env: str) -> bool:
    """¿Se despliega este contenedor en el entorno? (si se indica, tiene que estar en environments)."""
    envs = raw.get("environments")
    return envs is None or env in envs


def load_container(raw: Mapping[str, Any], where: str, platform: Mapping[str, Any],
                   settings: DeploySettings) -> ContainerSpec:
    errors: list[str] = []
    if not isinstance(raw, Mapping):
        raise ConfigError([f"{where}: cada contenedor debe ser un mapa YAML"])
    unknown = set(raw) - _CONTAINER_KEYS
    if unknown:
        errors.append(f"{where}: claves desconocidas {sorted(unknown)}")

    container = str(raw.get("container", ""))
    if not _CONTAINER_NAME.match(container):
        errors.append(f"{where}: 'container' {container!r} no es un nombre de contenedor Azure válido")
    custodian = str(raw.get("custodian", ""))
    if not custodian:
        errors.append(f"{where}: 'custodian' obligatorio")

    envs = raw.get("environments")
    if envs is not None and (not isinstance(envs, list) or not envs or set(envs) - set(ENVIRONMENTS)):
        errors.append(f"{where}: 'environments' debe ser una lista con valores de {ENVIRONMENTS}")
    storage_account = str(raw.get("storage_account") or settings.landing_storage_account)
    if not re.fullmatch(r"[a-z0-9]{3,24}", storage_account):
        errors.append(f"{where}: 'storage_account' {storage_account!r} no es un nombre de cuenta válido")
    readers = _string_list(raw, "readers", where, errors)

    registration = str(raw.get("registration", platform["registration"]["mode"]))
    if registration not in REGISTRATION_MODES:
        errors.append(f"{where}: 'registration' debe ser uno de {REGISTRATION_MODES}")
    approved = _table_list(raw, "approved_entities", where, errors)
    disabled = _table_list(raw, "disabled_entities", where, errors)
    if set(approved) & set(disabled):
        errors.append(f"{where}: tablas a la vez aprobadas y deshabilitadas {sorted(set(approved) & set(disabled))}")

    e2e = raw.get("e2e", False)
    if not isinstance(e2e, bool):
        errors.append(f"{where}: 'e2e' debe ser true o false")
    elif e2e:
        # El job e2e escribe en el landing y borra lo que crea: nunca en pro.
        if not isinstance(envs, list) or "pro" in envs:
            errors.append(f"{where}: un contenedor e2e necesita 'environments' sin pro (p. ej. [dev])")
        if registration != "auto":
            errors.append(f"{where}: un contenedor e2e necesita registration auto")

    ctx = {
        "team_id": settings.team_id,
        "env": settings.env,
        "env_upper": settings.env.upper(),
        "container": container,
        "container_snake": container.replace("-", "_"),
        "storage_account": storage_account,
        "custodian": custodian,
        "data_class": str(raw.get("data_class", "")),
    }
    naming = {k: _render(v, ctx, f"platform.naming.{k}", errors) for k, v in platform["naming"].items()}
    tags = {k: _render(str(v), ctx, f"platform.tags.{k}", errors) for k, v in platform["tags"].items()}
    for key in ("bronze_catalog", "silver_catalog", "schema"):
        if not is_identifier(naming[key]):
            errors.append(f"naming.{key} resuelve a {naming[key]!r}, que no es un identificador UC válido")

    ingest = _merge(platform["ingest"], raw.get("ingest"))
    if not 1 <= int(ingest["concurrency"]) <= 100:
        errors.append(f"{where}: ingest.concurrency debe estar entre 1 y 100")
    pipelines = _merge(platform["pipelines"], raw.get("pipelines"))
    trigger = _merge(platform["trigger"], raw.get("trigger"))
    report = _merge(platform["report"], raw.get("report"))
    layout = platform["layout"]

    if errors:
        raise ConfigError(errors)

    runtime = RuntimeConfig(
        container=container,
        bronze_catalog=naming["bronze_catalog"],
        silver_catalog=naming["silver_catalog"],
        schema=naming["schema"],
        landing_root=naming["landing_root"],
        data_dir=layout["data_dir"],
        manifests_dir=layout["manifests_dir"],
        run_partition=layout["run_partition"],
        file_events=settings.file_events,
        registration=registration,
        approved_entities=approved,
        disabled_entities=disabled,
        pending_alert_hours=int(report["pending_alert_hours"]),
        row_count_grace_minutes=int(report["row_count_grace_minutes"]),
        report_lookback_days=int(report["lookback_days"]),
        framework_version=__version__,
    )
    return ContainerSpec(
        container=container,
        custodian=custodian,
        description=str(raw.get("description", "")),
        data_class=str(raw.get("data_class", "")),
        runtime=runtime,
        names=naming,
        ingest=ingest,
        pipelines=pipelines,
        trigger=trigger,
        report=report,
        tags=tags,
        readers=readers,
        e2e=bool(e2e) if isinstance(e2e, bool) else False,
        active=_as_bool(raw.get("active", True)),
    )


def load_containers(config_dir: Path, settings: DeploySettings) -> list[ContainerSpec]:
    """Contenedores de ``containers.yml`` que aplican al entorno. Errores de todos juntos."""
    platform = load_platform(config_dir)
    path = config_dir / CONTAINERS_FILE
    data = _read_yaml(path) if path.exists() else {}
    entries = data.get("containers") if isinstance(data, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ConfigError([f"{path}: falta la lista 'containers'"])

    specs: list[ContainerSpec] = []
    errors: list[str] = []
    seen: dict[str, str] = {}
    for i, raw in enumerate(entries):
        name = raw.get("container") if isinstance(raw, Mapping) else None
        where = f"{path.name}[{i}]" + (f" ({name})" if name else "")
        if name in seen:
            errors.append(f"{where}: contenedor repetido (ya en {seen[name]})")
            continue
        if name:
            seen[name] = where
        # Se valida siempre, aunque no aplique al entorno: un error en una
        # entrada solo de dev rompe el deploy de dev, no espera a pro.
        try:
            spec = load_container(raw, where, platform, settings)
        except ConfigError as exc:
            errors.extend(exc.errors)
            continue
        if applies_to(raw, settings.env):
            specs.append(spec)
    if errors:
        raise ConfigError(errors)
    return sorted(specs, key=lambda s: s.container)
