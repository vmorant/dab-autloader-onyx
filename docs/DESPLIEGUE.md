# Despliegue y pruebas

Cómo llevar el bundle de ingesta de un portátil a pro, y cómo probarlo en cada paso.

| Nivel | Dónde | Qué prueba | Cuándo |
|---|---|---|---|
| 1. Tests | local y CI | Lógica del framework, guardarraíles, generación del bundle y coherencia del escenario e2e (133 tests, sin Spark) | Cada commit |
| 2. `validate` | contra el workspace | `containers.yml`, variables, referencias, `run_as`, permisos | Cada deploy |
| 3. **e2e** | dev (a mano y en CI) | El flujo completo con un CLEX simulado: Auto Loader, AUTO CDC, snapshots, guardarraíles, wheel en la pipeline clásica | Cada cambio de wheel o de bundle |
| 4. CLEX real | dev | Contrato real con CLEX, escritura cross-tenant, trigger file arrival | Alta de cada contenedor o conector |
| 5. Volumen | pre | Tiempos, coste, file events, concurrencia | Antes de pro |

---

## 0. Prerrequisitos (una vez por workspace)

### 0.1 Valores en `databricks.yml`

| Dónde | Valor |
|---|---|
| `targets.*.workspace.host` | URL de `odp-dbx-datateam01-<env>` |
| `pre`/`pro` → `pipeline_policy_id`, `job_policy_id` | Ids de las cluster policies (0.2) |
| `pre`/`pro` → `run_as_sp`, `deploy_sp` | Application ids de `svc-dbks-datateam01-<env>-run` y `-deploy` |
| `pro` → `notification_emails` | Buzón del equipo |
| `landing_storage_account` | Si el naming real difiere de `odpdatateam01<env>lzsto01` |

En dev las policies pueden quedar vacías mientras se prueba; en pre y pro no.

### 0.2 Infraestructura (Group / plataforma)

| Elemento | Detalle |
|---|---|
| Catálogos | `odp_datateam01_<env>_bronze` y `odp_datateam01_<env>_silver` |
| Storage de landing | Un contenedor por entrada de `containers.yml`. En dev, además `e2e-selftest` |
| Escritura de CLEX | SPN de CLEX (otro tenant) con escritura **solo** en su contenedor |
| External location | Sobre el landing (por cuenta o por contenedor). Si es por cuenta, conviene una propia para `e2e-selftest`, para dar `WRITE FILES` solo ahí |
| Volume de wheels | `odp_platform_<env>.libs.wheels` |
| Cluster policies | Pipeline: `cluster_type: dlt`, modo de acceso standard, tags. Job: `SINGLE_USER`, tags |
| Service principals | `svc-dbks-datateam01-<env>-deploy` (CI) y `-run` (ejecución), añadidos al workspace |
| Rol sobre el SP de ejecución | El SP de deploy necesita el rol **Service Principal User** sobre el de ejecución; sin él, el deploy no puede fijar `run_as` |
| File events (opcional al principio) | Roles del Access Connector, resource provider de Event Grid y owner o `MANAGE` en la external location para activarlos. Mientras no estén, `file_events: "false"` |

### 0.3 Permisos de Unity Catalog

Los esquemas por contenedor y el Volume `odp_system` los crea el bundle, y con ellos los grants de lectura
(`readers`) y, en pre y pro, los de escritura del SP de ejecución.

**Quién manda en esos grants.** Los grants de un recurso del bundle son declarativos: es muy probable que un
deploy revoque los que se hayan dado a mano, o desde governance, sobre esos esquemas y Volumes (sin
confirmar en la documentación). O todos en el bundle (`readers` en `containers.yml`) o todos fuera
(`manage_schemas: "false"`), nunca mezclados. Comprobarlo en el plan del primer deploy de pre
(`databricks bundle plan -t pre`) y acordarlo con governance.

Lo que va fuera del bundle:

```sql
-- ===== dev: grupo de desarrolladores del equipo (despliegan y ejecutan como ellos mismos)
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_dev_bronze TO `grp-datateam01-dbx-dev`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_dev_silver TO `grp-datateam01-dbx-dev`;
GRANT READ FILES ON EXTERNAL LOCATION `<el-landing-dev>` TO `grp-datateam01-dbx-dev`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `<el-landing-dev-e2e>` TO `grp-datateam01-dbx-dev`;
GRANT USE CATALOG ON CATALOG odp_platform_dev TO `grp-datateam01-dbx-dev`;
GRANT USE SCHEMA ON SCHEMA odp_platform_dev.libs TO `grp-datateam01-dbx-dev`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME odp_platform_dev.libs.wheels TO `grp-datateam01-dbx-dev`;
-- El SP del CI en dev (despliega y ejecuta el e2e): los mismos grants que el grupo.

-- ===== pre / pro (sustituir <env>)
-- SP de deploy: crea esquemas y Volumes, publica la wheel
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_<env>_bronze TO `<app-id-deploy>`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_<env>_silver TO `<app-id-deploy>`;
GRANT USE CATALOG ON CATALOG odp_platform_<env> TO `<app-id-deploy>`;
GRANT USE SCHEMA ON SCHEMA odp_platform_<env>.libs TO `<app-id-deploy>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME odp_platform_<env>.libs.wheels TO `<app-id-deploy>`;
-- SP de ejecución: el bundle le da escritura en los esquemas y en odp_system; aquí, el resto
GRANT USE CATALOG ON CATALOG odp_datateam01_<env>_bronze TO `<app-id-run>`;
GRANT USE CATALOG ON CATALOG odp_datateam01_<env>_silver TO `<app-id-run>`;
GRANT READ FILES ON EXTERNAL LOCATION `<el-landing-<env>>` TO `<app-id-run>`;
GRANT USE CATALOG ON CATALOG odp_platform_<env> TO `<app-id-run>`;
GRANT USE SCHEMA ON SCHEMA odp_platform_<env>.libs TO `<app-id-run>`;
GRANT READ VOLUME ON VOLUME odp_platform_<env>.libs.wheels TO `<app-id-run>`;
```

El trigger file arrival necesita lectura sobre la ubicación que vigila (`_manifests/`) y `CAN MANAGE` sobre el
job. La documentación no aclara con qué identidad lo comprueba: en pre, con el trigger activo, verificar que
salta con el `run_as_sp` como dueño. Para activar file events en la external location hay que ser su owner o
tener `MANAGE`.

**Consumidores de silver.** Las tablas de silver son streaming tables de SDP. Quien no es su dueño solo las
puede leer desde un SQL warehouse, un cluster con modo de acceso standard o un cluster dedicado con DBR 15.4+
en un workspace con serverless (el filtrado se ejecuta en serverless). Sin serverless, los jobs de gold
(Pegasus Enhanced migrado) tienen que usar clusters standard o SQL warehouse.

### 0.4 Comprobación rápida

```bash
databricks auth login --host https://adb-XXXX.azuredatabricks.net --profile odp-dev
databricks current-user me -p odp-dev
databricks catalogs get odp_datateam01_dev_bronze -p odp-dev
databricks fs ls dbfs:/Volumes/odp_platform_dev/libs/wheels/ -p odp-dev
```

Y el landing, desde un SQL warehouse o un notebook (el CLI no lista rutas `abfss://`):

```sql
LIST 'abfss://motors-sqlserver@odpdatateam01devlzsto01.dfs.core.windows.net/';
LIST 'abfss://e2e-selftest@odpdatateam01devlzsto01.dfs.core.windows.net/';
```

Si falla, falta la external location o el `READ FILES`: el job fallaría igual.

---

## 1. Local

```bash
uv sync                       # .venv con databricks-bundles (lo usa el CLI para generar los recursos)
uv run pytest -q              # 133 tests
databricks bundle validate -t dev -p odp-dev
```

`validate` ya ejecuta el generador: un error en `containers.yml` sale aquí, con todos los errores juntos, antes
de tocar el workspace. CLI y `databricks-bundles` en la misma versión (1.21).

## 2. Publicar la wheel en dev

```bash
scripts/publish_wheel.sh dev odp-dev
# → /Volumes/odp_platform_dev/libs/wheels/odp_framework-0.4.0-py3-none-any.whl
```

Si la versión ya está publicada con el mismo contenido no hace nada; con otro contenido falla: hay que subir la
versión (`pyproject.toml`, `src/odp_framework/__init__.py` y `framework_wheel` de cada target).

## 3. Desplegar en dev

```bash
databricks bundle deploy  -t dev -p odp-dev
databricks bundle summary -t dev -p odp-dev
```

Qué crea `mode: development`:

- Jobs y pipelines con prefijo `[dev <usuario>]` y **triggers en pausa**.
- **Esquemas propios**: `odp_datateam01_dev_bronze.dev_<usuario>_motors_sqlserver`, y lo mismo en silver. Cada
  desarrollador tiene su registro, bronze, silver y checkpoints; el landing es común, así que cada uno carga lo
  que haya en él en sus tablas.
- Pipelines en modo desarrollo: sin reintentos automáticos y el cluster se queda encendido un rato tras cada
  actualización para reutilizarse.
- Por contenedor: `ingest_<contenedor>` y `silver_<contenedor>`. Para `e2e-selftest`, además `e2e_e2e_selftest`.

## 4. Prueba end-to-end con el simulador de CLEX

```bash
databricks bundle run e2e_e2e_selftest -t dev -p odp-dev
```

El job `e2e_e2e_selftest` hace de CLEX sobre el contenedor `e2e-selftest` y ejecuta el job de ingesta real
(`run_job_task`) tras cada paso:

```
reset → publish_1 → run_1 → check_1 → publish_2 → run_2 → check_2 → publish_3 → run_3 → check_3 → cleanup
```

| Paso | Qué entrega el simulador | Qué comprueba `check_N` |
|---|---|---|
| 1. Carga inicial | `customers` CDC (5 inserts), `policies` full (3 filas) | Registro con 2 tablas ACTIVE, load log, bronze y silver con 5 y 3 filas, sin errores |
| 2. Cambios | CDC: update, delete, dos updates desordenados de la misma PK en el mismo fichero, insert y **columna nueva**. Full: alta, baja y cambio | Delete aplicado, gana la secuencia mayor, columna nueva en bronze y en silver, snapshot aplicado, sin errores |
| 3. Guardarraíles | Manifiesto de `customers` con las keys cambiadas (SPEC_CONFLICT) en su misma carpeta; eventos CDC atrasados; snapshot full **incompleto** (el manifiesto dice 4 filas, llegan 3); tabla nueva `agents` | Hallazgos SPEC_CONFLICT y ROW_COUNT_MISMATCH; el run en conflicto está en bronze y **no** en silver; los atrasados no pisan ni resucitan un borrado; el snapshot incompleto no se aplica; `agents` registrada y en silver |

Detalles:

- **Aislamiento.** Cada despliegue usa su propio subárbol del contenedor (`data/<esquema>/…` y
  `_manifests/<esquema>/…`; en dev el esquema lleva el prefijo del desarrollador). El de un desarrollador no
  ve los manifiestos del CI ni los de otro. Cada ejecución, además, crea tablas propias
  (`e2e_<job_run_id>_<tabla>`).
- **Resultado**: cada `check_N` imprime una línea por comprobación (`OK`/`FAIL`, valor y esperado) y falla si
  alguna no cuadra. `bundle run` devuelve código distinto de 0.
- **Si falla**, `cleanup` no se ejecuta y las tablas se quedan para investigar (consultas en el punto 6). La
  siguiente ejecución empieza con `reset`, que borra todo lo del e2e en ese despliegue (tablas `e2e_*`, sus
  filas de control, checkpoints y landing): se puede repetir sin limpiar a mano.
- Si el `DROP` de las tablas de silver no estuviera permitido desde el job cluster, `reset` y `cleanup` lo
  dejan como aviso: esas tablas quedan inactivas al salir del registro y se borran a mano.
- **Duración**: 45-60 minutos, casi todo arranques de cluster (job cluster del e2e, job cluster de la ingesta y
  cluster de la pipeline, tres veces).
- El contenedor e2e tiene `fail_on_error: false`: el paso 3 provoca errores a propósito y el que decide si la
  prueba pasa es el e2e, no el job de ingesta.

Qué valida del plan de PoC:

| Pendiente de la PoC | Dónde lo verifica el e2e |
|---|---|
| Wheel desde un Volume en la pipeline **clásica** (`environment.dependencies`, Public Preview) | `run_1`: si no instala, la tarea `silver` falla en el paso 1 |
| Flow full sin snapshot nuevo en una actualización | Paso 3: snapshot incompleto, `select_next_run` devuelve `None` |
| Columna nueva: Auto Loader en bronze y evolución del destino de AUTO CDC | Paso 2 |
| Desorden, deletes y tombstones de AUTO CDC | Pasos 2 y 3 |
| Runs rechazados fuera de silver | Paso 3 |

Atajo para el primer punto sin esperar al e2e completo, cuando ya hay alguna tabla registrada:
`databricks bundle run silver_e2e_selftest -t dev --validate-only` resuelve el grafo (instala la wheel y lee el
registro) sin procesar datos.

## 5. Prueba con CLEX real (dev)

1. **Contrato.** Pasar al equipo de CLEX la sección "Contrato con CLEX" del README, `samples/` y el validador.
   Antes de escribir en el landing pueden validar sus manifiestos:

   ```bash
   uv run python -c "import json,sys; from odp_framework.manifest import validate_manifest; \
   print(validate_manifest(json.load(open(sys.argv[1]))) or 'OK')" manifiesto.json
   ```

2. **Entrega.** CLEX dev extrae a `motors-sqlserver` del landing de dev: parquet en
   `data/{source_path}/run_id={run_id}/` y el manifiesto en `_manifests/{source_path}/{run_id}.json`, al final.
3. **Ejecución.** A mano:

   ```bash
   databricks bundle run ingest_motors_sqlserver -t dev -p odp-dev
   databricks bundle run ingest_motors_sqlserver -t dev -p odp-dev --only register   # solo registrar
   ```

   O con el trigger: descomentar `presets.trigger_pause_status: UNPAUSED` en el target dev y redesplegar.
   No usar `--python-named-params`: sustituiría `config_json` en todas las tareas.
4. **Escenarios** a provocar desde CLEX:

| Escenario | Cómo | Resultado esperado |
|---|---|---|
| Carga inicial CDC | Primera extracción, todo como inserts | Bronze = filas del manifiesto; silver = réplica |
| Incremental con deletes | Ventana con I/U/D | Silver refleja los deletes |
| Full | Extracción completa de una tabla full | Silver = snapshot; filas que desaparecen se borran |
| Columna nueva | `ALTER TABLE ADD` en origen | Una ejecución con reintento interno en ingest; columna en bronze y en silver |
| Tabla nueva | Añadir una tabla a la config de CLEX (PR suyo) | Se registra sola (`auto`); en `home-sqlserver` (`manual`) queda `PENDING_APPROVAL` hasta añadirla a `approved_entities` |
| Cambio de keys | Cambiar keys de una tabla ya registrada | `SPEC_CONFLICT`, el job avisa; el run llega a bronze, no a silver |
| CLEX cortado a mitad | Parquet sin manifiesto | Bronze los carga cuando la tabla vuelva a cargarse; CDC los aplica a silver, full no. Hoy no hay aviso (siguientes pasos del README) |
| Reintento de CLEX | Repetir una extracción | `run_id` nuevo; en CDC la secuencia evita duplicados en silver |

## 6. Consultas de seguimiento

`<esq>` es el esquema del contenedor: `dev_<usuario>_motors_sqlserver` en dev y `motors_sqlserver` en pre y pro.

```sql
-- Hallazgos de las últimas 24 h
SELECT report_ts, phase, severity, code, entity, run_id, detail
FROM odp_datateam01_dev_bronze.<esq>.odp_run_findings
WHERE report_ts >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY report_ts DESC;

-- Registro de tablas
SELECT entity, load_type, status, spec_version, first_run_id, last_run_id, last_manifest_at
FROM odp_datateam01_dev_bronze.<esq>.odp_entities ORDER BY entity;

-- Cargas fallidas
SELECT entity, job_run_id, started_at, attempts, error
FROM odp_datateam01_dev_bronze.<esq>.odp_load_log
WHERE status = 'FAILED' ORDER BY started_at DESC;

-- Manifiesto frente a bronze, por run, de una tabla
SELECT m.run_id, m.row_count AS manifiesto, count(b._run_id) AS bronze
FROM odp_datateam01_dev_bronze.<esq>.odp_manifests m
LEFT JOIN odp_datateam01_dev_bronze.<esq>.gdwr_customers b ON b._run_id = m.run_id
WHERE m.load_spec.target_table = 'gdwr_customers'
GROUP BY m.run_id, m.row_count ORDER BY m.run_id DESC;

-- Runs que silver no aplica
SELECT * FROM odp_datateam01_dev_bronze.<esq>.odp_rejected_runs;
```

Coste y tiempos por contenedor (pre/pro): `system.billing.usage` filtrando por `custom_tags['odp-container']`
y `system.lakeflow.job_run_timeline` por `job_id`. Inicialización del grafo de silver: event log de la
pipeline, eventos `update_progress` (de `INITIALIZING` a `RUNNING`).

## 7. Pre y pro con CI (Azure DevOps)

`ci/azure-pipelines.yml`:

```
PR a main → tests
main      → tests → dev (wheel + deploy + e2e) → pre (aprobación) → pro (aprobación)
```

Montaje, una vez:

1. Service connections ARM con workload identity federation (`sc-odp-datateam01-<env>`), cada una con su
   `svc-dbks-datateam01-<env>-deploy`. Sin secretos: el CLI usa `DATABRICKS_AUTH_TYPE=azure-cli`.
2. Variable groups `odp-datateam01-<env>` con `DATABRICKS_HOST`.
3. Environments `odp-datateam01-pre` y `-pro` con aprobación manual.
4. El SP del CI en dev con los grants de dev (0.3), incluido `WRITE FILES` en `e2e-selftest`.

Qué cambia en `mode: production` (pre y pro): sin prefijos (esquemas `motors_sqlserver`), **triggers activos**,
jobs y pipelines como `run_as_sp`, permisos `CAN_MANAGE` para el SP de deploy y `CAN_VIEW` para el grupo.

El CI despliega **sin `--auto-approve`**: si un cambio fuera a borrar o recrear un esquema, un Volume o una
pipeline (que se lleva sus tablas de silver), el CLI se para y el pipeline falla. Ese cambio se revisa y se
aplica a mano.

**Pruebas en pre** (volumen real de CLEX pre):

- Tiempo de inicialización del grafo de silver con todas las tablas del contenedor.
- `file_events: "true"` en el target una vez activados en la external location; comprobar que el trigger y
  Auto Loader siguen detectando ficheros.
- Concurrencia del for_each (`ingest.concurrency`) y tamaño del cluster frente a duración y coste.
- Qué hacen `silver` y `report` cuando `has_data` es falso (ejecución sin datos nuevos).
- Alta progresiva: un contenedor nuevo entra con `environments: [dev]`, luego `[dev, pre]` y, al quitar el
  campo, en todos.

## 8. Operación

| Necesidad | Cómo |
|---|---|
| Nueva versión del framework | Subir versión → PR → CI: tests, wheel, dev + e2e, pre, pro |
| Marcha atrás | `git revert` del PR. La wheel anterior sigue en el Volume (inmutable) y el CI redespliega config y versión |
| Reconstruir una tabla de silver | `databricks bundle run silver_<contenedor> -t <env> --full-refresh <tabla>`. CDC relee todo bronze; full parte del snapshot completo más reciente. Es también el arreglo si un run se rechaza después de haberse aplicado, o si un evento CDC llega más de 7 días tarde respecto a un delete |
| Dar de baja una tabla | `disabled_entities` en `containers.yml` (PR). Deja de cargarse; sus datos se quedan |
| Pausar un contenedor | `active: false`: se quita el job; esquemas, Volume, pipeline y datos se conservan |
| Baja definitiva de un contenedor | Borrar su entrada. El deploy borra pipeline (y tablas de silver), job, Volume y esquemas: pide confirmación y el CI se para |
| Recargar bronze de una tabla | Evitarlo: bronze es la única copia una vez caduca el landing (30 días en pro). Si hay duplicados por haber perdido un checkpoint, se quitan con `_source_file` |
