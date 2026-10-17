"""Configuración de runtime de un contenedor.

Dos piezas:

* ``RuntimeConfig`` (nivel contenedor): catálogos, esquema, raíz del landing,
  modo de registro... La genera el deploy y viaja a las tareas del job
  (parámetro ``config_json``) y a la pipeline de silver
  (``configuration["odp.config"]``).
* ``RuntimeEntity`` (nivel tabla): sale del registro ``odp_entities``, que la
  tarea ``register`` construye a partir de los manifiestos de CLEX.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace

from . import RUNTIME_CONTRACT

MANIFEST_TABLE = "odp_manifests"
REGISTRY_TABLE = "odp_entities"
LOAD_LOG_TABLE = "odp_load_log"
FINDINGS_TABLE = "odp_run_findings"
REJECTED_RUNS_TABLE = "odp_rejected_runs"
SYSTEM_VOLUME = "odp_system"

# Columnas técnicas que añade bronze y que no viajan a silver.
RUN_ID_COLUMN = "_run_id"
SOURCE_FILE_COLUMN = "_source_file"
INGESTED_AT_COLUMN = "_ingested_at"
BRONZE_TECH_COLUMNS = (RUN_ID_COLUMN, SOURCE_FILE_COLUMN, INGESTED_AT_COLUMN)
RESCUED_DATA_COLUMN = "_rescued_data"     # lo añade Auto Loader al inferir esquema
# Columnas de bronze que no forman parte de la réplica en silver. DataFrame.drop
# ignora las que no existan.
NOT_IN_SILVER = (*BRONZE_TECH_COLUMNS, RESCUED_DATA_COLUMN)

REGISTRATION_MODES = ("auto", "manual")


@dataclass(frozen=True)
class RuntimeEntity:
    entity: str                     # tabla destino en bronze y silver (load_spec.target_table)
    source_path: str                # {conexion}/{bbdd}/{esquema}/{tabla}
    load_type: str                  # "cdc" | "full"
    keys: tuple[str, ...]
    spec_version: int
    sequence_by: tuple[str, ...] = ()
    operation_column: str | None = None
    delete_values: tuple[str, ...] = ()
    schema_hints: str | None = None
    allow_empty_snapshot: bool = False

    @property
    def is_cdc(self) -> bool:
        return self.load_type == "cdc"

    @property
    def is_full(self) -> bool:
        return self.load_type == "full"


@dataclass(frozen=True)
class RuntimeConfig:
    container: str
    bronze_catalog: str
    silver_catalog: str
    schema: str
    landing_root: str               # abfss://{container}@{cuenta}.dfs.core.windows.net
    data_dir: str = "data"
    manifests_dir: str = "_manifests"
    run_partition: str = "run_id"
    file_events: bool = False
    registration: str = "auto"      # auto | manual (solo entran las approved_entities)
    approved_entities: tuple[str, ...] = ()
    disabled_entities: tuple[str, ...] = ()
    pending_alert_hours: int = 24
    row_count_grace_minutes: int = 60
    report_lookback_days: int = 7
    contract: int = RUNTIME_CONTRACT
    framework_version: str = ""
    # No se serializa: se rellena en runtime desde el registro.
    entities: tuple[RuntimeEntity, ...] = ()

    # ---- rutas del landing -------------------------------------------------
    @property
    def manifests_root(self) -> str:
        return f"{self.landing_root.rstrip('/')}/{self.manifests_dir}/"

    def data_path(self, entity: RuntimeEntity) -> str:
        return f"{self.landing_root.rstrip('/')}/{self.data_dir}/{entity.source_path}/"

    # ---- objetos de Unity Catalog -----------------------------------------
    @property
    def manifest_fqn(self) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{MANIFEST_TABLE}"

    @property
    def registry_fqn(self) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{REGISTRY_TABLE}"

    @property
    def load_log_fqn(self) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{LOAD_LOG_TABLE}"

    @property
    def findings_fqn(self) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{FINDINGS_TABLE}"

    @property
    def rejected_runs_fqn(self) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{REJECTED_RUNS_TABLE}"

    # ---- checkpoints: rutas fijas por convención, nunca desde el manifiesto ---
    # Son la identidad de cada stream de Auto Loader: si cambiaran, se releería
    # todo el landing y se duplicaría bronze.
    @property
    def system_volume_path(self) -> str:
        return f"/Volumes/{self.bronze_catalog}/{self.schema}/{SYSTEM_VOLUME}"

    @property
    def manifest_checkpoint(self) -> str:
        return f"{self.system_volume_path}/checkpoints/{MANIFEST_TABLE}"

    def checkpoint(self, entity: RuntimeEntity) -> str:
        return f"{self.system_volume_path}/autoloader/{entity.entity}/checkpoint"

    def schema_location(self, entity: RuntimeEntity) -> str:
        return f"{self.system_volume_path}/autoloader/{entity.entity}/schema"

    def bronze_fqn(self, entity: RuntimeEntity) -> str:
        return f"{self.bronze_catalog}.{self.schema}.{entity.entity}"

    def silver_fqn(self, entity: RuntimeEntity) -> str:
        return f"{self.silver_catalog}.{self.schema}.{entity.entity}"

    def entity_by_name(self, name: str) -> RuntimeEntity | None:
        for e in self.entities:
            if e.entity == name:
                return e
        return None

    def with_entities(self, entities) -> "RuntimeConfig":
        return replace(self, entities=tuple(entities))

    # ---- serialización (sin entidades) ------------------------------------
    def to_json(self) -> str:
        data = asdict(self)
        data.pop("entities")
        return json.dumps(data, sort_keys=True, separators=(",", ":"))


def load_runtime_config(raw: str | None) -> RuntimeConfig:
    """Reconstruye la configuración del contenedor desde ``odp.config``."""
    if not raw:
        raise ValueError(
            "La pipeline no tiene 'odp.config' en su configuración. "
            "Se genera en el deploy del bundle; no se edita a mano."
        )
    data = json.loads(raw)
    contract = data.get("contract")
    if contract != RUNTIME_CONTRACT:
        raise ValueError(
            f"Contrato de configuración {contract!r} no soportado por esta versión "
            f"del framework (espera {RUNTIME_CONTRACT}). Redespliega el bundle con "
            "la misma versión de la wheel que usa la pipeline."
        )
    data.pop("entities", None)
    for key in ("approved_entities", "disabled_entities"):
        data[key] = tuple(data.get(key) or ())
    return RuntimeConfig(**data)
