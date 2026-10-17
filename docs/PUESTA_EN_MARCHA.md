# Puesta en marcha en Databricks

Qué tiene que existir para que el bundle funcione de punta a punta, quién lo aporta, cómo comprobarlo, y cómo
desplegarlo de dev a pro. Las pruebas y la operación del día a día están en
[`PRUEBAS_Y_OPERACION.md`](PRUEBAS_Y_OPERACION.md).

Contenido:

1. [Qué despliega el bundle y qué no](#1-qué-despliega-el-bundle-y-qué-no)
2. [Requisitos](#2-requisitos): Azure, red, Unity Catalog, identidades, workspace, CLEX, CI/CD
3. [Decisiones abiertas que afectan al despliegue](#3-decisiones-abiertas-que-afectan-al-despliegue)
4. [Configuración del repo](#4-configuración-del-repo)
5. [Despliegue paso a paso](#5-despliegue-paso-a-paso): dev, CI, pre, pro
6. [Verificación después de desplegar](#6-verificación-después-de-desplegar)
7. [Errores frecuentes al desplegar](#7-errores-frecuentes-al-desplegar)

---

## 1. Qué despliega el bundle y qué no

**Lo crea el bundle** (`databricks bundle deploy`), por cada entrada de `config/containers.yml` que aplique al
entorno:

| Recurso | Nombre (pre/pro; en dev con prefijo del desarrollador) |
|---|---|
| Job de ingesta (register → ingest → silver → report), trigger file arrival sobre `_manifests/` | `job-datateam01-<env>-<contenedor>-ingest` |
| Pipeline SDP de silver (compute clásico, edición PRO) | `sdp-datateam01-<env>-<contenedor>-silver` |
| Esquema en bronze y en silver | `odp_datateam01_<env>_bronze.<contenedor>`, `odp_datateam01_<env>_silver.<contenedor>` |
| Volume de checkpoints de Auto Loader | `odp_datateam01_<env>_bronze.<contenedor>.odp_system` |
| Grants de lectura (`readers`) y de escritura del SP de ejecución sobre esos esquemas y el Volume | — |
| Permisos de jobs y pipelines: `CAN_MANAGE` SP de deploy, `CAN_VIEW` grupo del equipo | — |
| Solo dev: job `e2e_e2e_selftest` (simulador de CLEX) | `job-datateam01-dev-e2e-selftest-e2e` |

Las tablas (bronze, silver y las de control `odp_*`) no se despliegan: las crean los jobs al ejecutarse, a
partir de los manifiestos de CLEX.

**No lo crea el bundle** (es infraestructura y gobierno, apartado 2): workspace, storage de landing y sus
contenedores, red, Access Connector, storage credential, external locations, catálogos, Volume de wheels,
service principals, grupos, cluster policies, conexión de CLEX y CI/CD.

---

## 2. Requisitos

Cada requisito indica en qué entorno bloquea. "Group" es el equipo de cloud/infra que da el servicio (AXA GO
Cloud Broker / plataforma, normalmente por Terraform).

### 2.1 Azure

| # | Requisito | Detalle | Quién | Comprobación | Bloquea |
|---|---|---|---|---|---|
| A1 | Workspace por entorno | `odp-dbx-datateam01-<env>`, con Unity Catalog, VNet injection y asignado al metastore de la región | Group | `databricks metastores current -p <perfil>` | dev |
| A2 | Storage de landing | ADLS Gen2 (namespace jerárquico) `odpdatateam01<env>lzsto01`. Un contenedor por entrada de `containers.yml`; en dev, además `e2e-selftest` | Group | `LIST 'abfss://<contenedor>@<cuenta>.dfs.core.windows.net/'` desde un notebook o SQL warehouse | dev |
| A3 | Lifecycle del landing | Borrado a 30 días en pro y 7 en dev/pre. Bronze es el registro permanente | Group | Reglas de lifecycle de la cuenta | pro |
| A4 | Access Connector | Identidad gestionada para Unity Catalog con **Storage Blob Data Contributor** sobre la cuenta de landing | Group | Rol asignado en la cuenta | dev |
| A5 | File events (opcional al empezar) | Resource provider `Microsoft.EventGrid` registrado y, para el Access Connector, además: EventGrid EventSubscription Contributor y Storage Queue Data Contributor (lista de la comunidad de Databricks, a confirmar). Sin file events: máximo 50 jobs con trigger por workspace y 10.000 ficheros por ruta vigilada | Group | Ver B6 | pro (por escala) |

### 2.2 Red

Requisito no funcional de la plataforma: sin IPs públicas, solo private endpoints.

| # | Requisito | Detalle | Quién | Comprobación | Bloquea |
|---|---|---|---|---|---|
| N1 | Clusters → landing | Private endpoints `dfs` y `blob` de la cuenta de landing accesibles desde las subnets del workspace (y `queue` si se activan file events) | Group | El `LIST` de A2 desde un cluster del workspace | dev |
| N2 | CLEX → landing | El ADF de CLEX está en **otro tenant**. Necesita una ruta privada a la cuenta de landing, por ejemplo un managed private endpoint desde su ADF. La conexión la aprueba en AXA el owner de la cuenta | Group + CLEX | Una extracción de prueba de CLEX deja ficheros en su contenedor | dev (CLEX real) |
| N3 | Trigger file arrival con el storage cerrado | El trigger lo evalúa el servicio de jobs de Databricks sin ningún cluster vuestro encendido. La documentación no aclara cómo llega a un storage solo con private endpoints. **Verificar en dev** con el trigger activo (`presets.trigger_pause_status: UNPAUSED`); si no salta, hay que pedir a Group una Network Connectivity Configuration con private endpoint a la cuenta, o cambiar a un trigger programado | Plataforma | El job arranca solo al dejar un manifiesto | pre |
| N4 | Despliegue → workspace | Si el workspace solo es accesible desde la red de AXA, quien despliega también tiene que estar en ella: portátil con VPN en dev y **agentes self-hosted** de Azure DevOps en la red de AXA para el CI | Plataforma / DevOps | `databricks current-user me` desde el agente | dev (CI) |
| N5 | Salida a internet de los agentes | `uv`, `databricks-bundles` (PyPI) y el instalador del CLI (GitHub), o sus equivalentes en el feed interno de artefactos | DevOps | El paso `steps-setup.yml` del CI | dev (CI) |

### 2.3 Unity Catalog

| # | Requisito | Detalle | Quién | Comprobación | Bloquea |
|---|---|---|---|---|---|
| B1 | Storage credential | Sobre el Access Connector (A4) | Admin del metastore | `databricks storage-credentials get <nombre>` | dev |
| B2 | External location del landing | Sobre `abfss://...@odpdatateam01<env>lzsto01.dfs.core.windows.net/` (por cuenta o por contenedor). En dev conviene una aparte para `e2e-selftest`, para dar `WRITE FILES` solo ahí | Admin del metastore | `databricks external-locations get <nombre>` | dev |
| B3 | Catálogos | `odp_datateam01_<env>_bronze` y `odp_datateam01_<env>_silver`, con managed location y vinculados al workspace (ver decisión D1) | Admin del metastore | `databricks catalogs get <catálogo>` | dev |
| B4 | Volume de wheels | `odp_platform_<env>.libs.wheels`: escritura solo para quien publica, lectura para quien ejecuta | Plataforma | `databricks fs ls dbfs:/Volumes/odp_platform_<env>/libs/wheels/` | dev |
| B5 | Grants | Los del bloque SQL de abajo | Admin del metastore | `SHOW GRANTS ON CATALOG ...` | dev |
| B6 | File events en la external location | Requiere owner o `MANAGE` sobre la external location. Después, `file_events: "true"` en el target | Admin del metastore | Ajustes de la external location | pro (por escala) |

Grants que van fuera del bundle (los de los esquemas por contenedor y `odp_system` los pone el bundle):

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

El trigger file arrival necesita lectura sobre la ruta que vigila y `CAN MANAGE` sobre el job; la
documentación no aclara con qué identidad lo comprueba. Con el `run_as_sp` como dueño del job debería bastar
con el `READ FILES` de arriba, pero hay que verificarlo en dev o pre (N3).

### 2.4 Identidades

| # | Requisito | Detalle | Quién | Comprobación | Bloquea |
|---|---|---|---|---|---|
| C1 | Service principals por entorno (pre/pro) | `svc-dbks-datateam01-<env>-deploy` (CI) y `svc-dbks-datateam01-<env>-run` (ejecución), en Entra, añadidos a la cuenta de Databricks y al workspace | Group + admin de cuenta | `databricks service-principals list --filter "displayName eq '<nombre>'"` | pre |
| C2 | Rol sobre el SP de ejecución | El SP de deploy necesita **Service Principal User** sobre el SP de ejecución; sin él, el deploy no puede fijar `run_as` | Admin del workspace | Primer `bundle deploy -t pre` | pre |
| C3 | SP del CI en dev | El que despliega dev y lanza el e2e en el CI (puede ser el `-deploy` de dev), con los grants de dev | Group | Stage dev del CI | dev (CI) |
| C4 | Workload identity federation | Credencial federada de cada SP de deploy para su service connection de Azure DevOps: sin secretos | Group / DevOps | La service connection se verifica | dev (CI) |
| C5 | Grupos (cuenta) | `grp-datateam01-dbx-dev` (desarrolladores), `grp-datateam01-dbx-reader` (`team_group`, `CAN_VIEW`), y los `readers` de cada contenedor (`grp-datateam01-<custodian>-dbx-reader`), asignados al workspace | Group / admin de cuenta | `databricks groups list` | dev |
| C6 | Uso de las cluster policies | `CAN_USE` de las dos policies para el SP de ejecución (pre/pro) y para el grupo de desarrolladores y el SP del CI (dev) | Admin del workspace | Permisos de cada policy | dev |

### 2.5 Workspace

| # | Requisito | Detalle | Quién | Comprobación | Bloquea |
|---|---|---|---|---|---|
| W1 | Cluster policy de jobs | Modo de acceso dedicado (`SINGLE_USER`), DBR 16.4 LTS permitido, tipos de nodo, tags obligatorios de FinOps. El bundle pone `custom_tags` desde `platform.yml`: la policy no debe fijar valores distintos | Plataforma | `databricks cluster-policies get <id>` | pre |
| W2 | Cluster policy de pipelines | `cluster_type: dlt`, tipos de nodo y tags. El modo de acceso lo gestiona la pipeline (standard en UC) | Plataforma | Ídem | pre |
| W3 | `environment.dependencies` en pipelines clásicas | La wheel se instala en la pipeline de silver con esta función, en Public Preview. Si el workspace muestra la preview en la página *Previews*, tiene que estar activada | Admin del workspace | `databricks bundle run silver_<contenedor> --validate-only` (con alguna tabla registrada) o el paso 1 del e2e | dev |
| W4 | DBR 16.4 LTS | Variable `spark_version` del bundle | Plataforma | Al crear un cluster | dev |
| W5 | Serverless | **No** es necesario para el bundle. Sí lo es para que quien no sea dueño lea silver desde un cluster dedicado (ver D5) | — | — | — |
| W6 | System tables | `system.billing` y `system.lakeflow` para coste y tiempos (opcional, recomendable) | Admin de cuenta | `SELECT 1 FROM system.billing.usage LIMIT 1` | pro |

En dev, mientras no haya policies, `pipeline_policy_id` y `job_policy_id` pueden quedar vacías. En pre y pro, no.

### 2.6 CLEX

| # | Requisito | Detalle | Quién |
|---|---|---|---|
| X1 | Contrato 2.0 | Parquet en `data/{source_path}/run_id={run_id}/`, manifiesto en `_manifests/{source_path}/{run_id}.json` **al final**, `run_id` = `YYYYMMDDTHHMMSSZ_<id>`, `load_spec` completo. Ver "Contrato con CLEX" en el README, `samples/` y el validador (`odp_framework.manifest.validate_manifest`) | Equipo de CLEX |
| X2 | Una instancia por contenedor | Cada CLEX escribe solo en el contenedor de su custodian (A2, N2) | Equipo de CLEX + Group |
| X3 | Configuración por PR | La lista de tablas y su `load_spec` viven en el repo de CLEX y se aprueban por PR; el bundle no lista tablas | Equipo de CLEX + custodian |

### 2.7 CI/CD (Azure DevOps)

El pipeline está en `ci/`. Corre en Azure DevOps desde un repo de ADO, o desde este repo de GitHub
conectando ADO con una service connection de GitHub.

| # | Requisito | Detalle | Quién |
|---|---|---|---|
| D-1 | Service connections ARM | `sc-odp-datateam01-dev`, `-pre`, `-pro`, con workload identity federation (C4) | DevOps |
| D-2 | Variable groups | `odp-datateam01-<env>` con `DATABRICKS_HOST` | DevOps |
| D-3 | Environments | `odp-datateam01-pre` y `odp-datateam01-pro` con aprobación manual (pro: CAB) | DevOps |
| D-4 | Agentes | Pool self-hosted en la red de AXA (N4) con salida a PyPI/GitHub o al feed interno (N5). Cambiar `pool` en `ci/azure-pipelines.yml` | DevOps |

---

## 3. Decisiones abiertas que afectan al despliegue

| # | Tema | Situación | Qué hacer antes de pre |
|---|---|---|---|
| D1 | Diseño de catálogos | El bundle implementa **un catálogo por capa** (`odp_datateam01_<env>_bronze` / `_silver`) y **un esquema por contenedor**. La decisión anterior de la plataforma era un catálogo por workspace (`odp_datateam01_<env>`) con esquemas `bronze`/`silver`/`gold` | Cerrarla. Volver al diseño anterior no es solo cambiar `platform.yml`: el runtime asume el mismo nombre de esquema en bronze y silver, así que habría que adaptar el código |
| D2 | Cuenta de landing | Naming por equipo (`odpdatateam01<env>lzsto01`); la wiki también describe un landing central | Si es central, cambiar `landing_storage_account`, o `storage_account` por contenedor |
| D3 | Quién gestiona los grants de los esquemas | Los grants de recursos del bundle son declarativos: es probable que un deploy revoque los que se den a mano o desde governance (sin confirmar en la documentación) | Acordarlo con governance. O todo en el bundle (`readers`) o todo fuera (`manage_schemas: "false"`). Revisar `databricks bundle plan -t pre` |
| D4 | Trigger con el storage cerrado | Ver N3 | Verificarlo en dev; si no funciona, NCC o trigger programado |
| D5 | Lectura de silver por los consumidores | Las tablas de silver son streaming tables de SDP. Quien no sea su dueño solo puede leerlas desde un SQL warehouse, un cluster standard o un cluster dedicado con DBR 15.4+ **en un workspace con serverless** | Los jobs de gold (Pegasus migrado) y los analistas, en clusters standard o SQL warehouse mientras no haya serverless |

---

## 4. Configuración del repo

### `databricks.yml`

| Valor | dev | pre | pro | De dónde sale |
|---|---|---|---|---|
| `workspace.host` | ✔ | ✔ | ✔ | URL de `odp-dbx-datateam01-<env>` (A1) |
| `landing_storage_account` | ✔ | ✔ | ✔ | A2 (por defecto `odpdatateam01<env>lzsto01`) |
| `framework_wheel` | ✔ | ✔ | ✔ | `/Volumes/odp_platform_<env>/libs/wheels/odp_framework-<versión>-py3-none-any.whl` (B4) |
| `pipeline_policy_id`, `job_policy_id` | opcional | ✔ | ✔ | W1, W2 |
| `run_as_sp`, `deploy_sp` | — | ✔ | ✔ | Application ids de C1 |
| `team_group` | — | ✔ | ✔ | C5 |
| `notification_emails` | — | opcional | ✔ | Buzón del equipo |
| `file_events` | `"false"` | `"false"` → `"true"` | `"true"` | Cuando B6 esté hecho |

`team_id` (`datateam01`) y `manage_schemas` (`"true"`) son comunes a los tres entornos.

### `config/containers.yml`

Una entrada por contenedor real del landing (custodian × instancia de CLEX), con sus `readers`. Para un alta
progresiva, `environments: [dev]`, luego `[dev, pre]`, y quitar el campo cuando llegue a pro. El contenedor
`e2e-selftest` solo existe en dev (está prohibido en pro).

### `config/platform.yml`

Naming (ver D1), tags obligatorios de FinOps (`global-Team`, `global-Source`, `global-Environment`,
`global-DataClass`, `Project`; el bundle añade `resource-type`), concurrencia, tamaño de clusters y trigger.

---

## 5. Despliegue paso a paso

### Fase 1 — dev, a mano (un desarrollador, en la red de AXA)

Requisitos: A1, A2, A4, N1, B1–B5, C5, C6, W3, W4.

```bash
git clone https://github.com/vmorant/dab-autloader-onyx && cd dab-autloader-onyx
# databricks.yml: host de dev (y landing_storage_account si difiere)

uv sync                                     # .venv con databricks-bundles (lo usa el CLI)
uv run pytest -q                            # 133 tests

databricks auth login --host https://<workspace-dev> --profile odp-dev
databricks bundle validate -t dev -p odp-dev
scripts/publish_wheel.sh dev odp-dev        # wheel inmutable en el Volume de dev
databricks bundle deploy  -t dev -p odp-dev
databricks bundle summary -t dev -p odp-dev
```

El CLI y `databricks-bundles` van en la misma versión (1.21; `databricks_cli_version` en `databricks.yml`).

En dev (`mode: development`) cada desarrollador tiene sus propios esquemas (`dev_<usuario>_<contenedor>`), y
los jobs y pipelines llevan el prefijo `[dev <usuario>]` y tienen los **triggers en pausa**. El landing es común.

Después: verificación (apartado 6) y prueba e2e:

```bash
databricks bundle run e2e_e2e_selftest -t dev -p odp-dev
```

Detalle del e2e y de la prueba con CLEX real en [`PRUEBAS_Y_OPERACION.md`](PRUEBAS_Y_OPERACION.md).

### Fase 2 — CI

Requisitos: C3, C4, N4, N5, D-1 a D-4.

1. Ajustar `pool` en `ci/azure-pipelines.yml` (agentes self-hosted).
2. Crear el pipeline en Azure DevOps apuntando a `ci/azure-pipelines.yml`.
3. Primera ejecución en `main`: tests, después dev (wheel, deploy y e2e con el SP del CI).

El CI despliega **sin `--auto-approve`**: si un cambio fuera a borrar o recrear un esquema, un Volume o una
pipeline (que se lleva sus tablas de silver), el CLI se para y el pipeline falla.

### Fase 3 — pre

Requisitos: todo el apartado 2 para pre, menos B6. Decisiones D1–D3 cerradas.

1. Valores de pre en `databricks.yml` (apartado 4) por PR.
2. Merge a `main`: el CI despliega dev con e2e y, tras aprobación, pre.
3. Verificación (apartado 6) y pruebas con CLEX pre: volumen, tiempos, coste, trigger con el storage cerrado
   (N3) y file events (B6), en [`PRUEBAS_Y_OPERACION.md`](PRUEBAS_Y_OPERACION.md).

`mode: production`: sin prefijos (esquemas `<contenedor>`), **triggers activos**, jobs y pipelines como
`run_as_sp`.

### Fase 4 — pro (salida a producción)

Checklist:

- [ ] Todo el apartado 2 para pro, incluidos A3 (lifecycle) y B6 (file events), con `file_events: "true"`.
- [ ] `notification_emails` con el buzón del equipo.
- [ ] CLEX pro escribiendo en su contenedor (N2) y validado en pre con el mismo contrato.
- [ ] D3 resuelta: grants de lectura de los custodians en `readers` o gestionados fuera, no ambos.
- [ ] Consumidores de silver sobre clusters standard o SQL warehouse (D5).
- [ ] Aprobación de CAB en el environment `odp-datateam01-pro`.
- [ ] Marcha atrás preparada: la wheel anterior sigue en el Volume y basta revertir el PR.
- [ ] Seguimiento: consultas de `odp_run_findings` y alertas sobre el fallo del job (notificaciones).

---

## 6. Verificación después de desplegar

| Qué | Dónde | Correcto si |
|---|---|---|
| Recursos | `databricks bundle summary -t <env>` | Un job y una pipeline por contenedor; esquemas y Volume si `manage_schemas` |
| Job | Jobs → `job-datateam01-<env>-<contenedor>-ingest` | Tareas `register → has_data → ingest (for each) → silver → report`; trigger file arrival sobre `…/_manifests/` (en pausa en dev); *Run as* = SP de ejecución en pre/pro; job cluster con la policy |
| Parámetros | Tarea `register` → `config_json` | `schema` es el nombre real (en dev `dev_<usuario>_<contenedor>`), no una referencia `${…}` |
| Pipeline | Pipelines → `sdp-datateam01-<env>-<contenedor>-silver` | Catálogo `…_silver`, esquema del contenedor, compute clásico con la policy, dependencia de la wheel de la versión fijada |
| Unity Catalog | Catalog Explorer | Esquemas en bronze y silver con los grants esperados; Volume `odp_system` |
| Permisos | Job y pipeline → Permisos | `CAN_MANAGE` SP de deploy y `CAN_VIEW` grupo del equipo (pre/pro) |
| Arranque | `databricks bundle run ingest_<contenedor> -t <env> --only register` | Termina bien; sin manifiestos, no hay nada que cargar |
| De punta a punta | dev: e2e; pre: una entrega real de CLEX | Ver [`PRUEBAS_Y_OPERACION.md`](PRUEBAS_Y_OPERACION.md) |

---

## 7. Errores frecuentes al desplegar

| Síntoma | Causa probable | Arreglo |
|---|---|---|
| `validate` falla al cargar los recursos de Python | No hay `.venv` con `databricks-bundles` | `uv sync` en la raíz del repo |
| `validate` lista errores de `containers.yml` | Entrada inválida (nombre, custodian, `environments`, e2e en pro…) | Corregir la entrada; salen todos los errores juntos |
| `validate` pide otra versión del CLI | CLI anterior a `databricks_cli_version` | Actualizar el CLI (misma versión que `databricks-bundles`) |
| Deploy sin permiso para crear esquemas | Faltan `USE CATALOG` / `CREATE SCHEMA` (B5) | Grants del bloque SQL |
| Deploy no puede fijar `run_as` | Falta el rol Service Principal User (C2) | Asignarlo sobre el SP de ejecución |
| El job no puede crear su cluster | Falta `CAN_USE` de la policy (C6) o la policy rechaza la configuración | Permiso o ajuste de la policy (W1, W2) |
| La tarea no encuentra la wheel | No publicada, versión distinta en `framework_wheel` o falta `READ VOLUME` | `scripts/publish_wheel.sh <env>`, revisar la ruta, grants de B4 |
| La pipeline de silver falla al instalar dependencias | `environment.dependencies` no disponible en pipelines clásicas, o sin `READ VOLUME` | W3 y B4 |
| `register` falla al leer `abfss://…` | Falta external location, `READ FILES` o ruta de red (B2, B5, N1) | El `LIST` de A2 desde un cluster del workspace |
| El trigger no salta | En pausa (dev), sin lectura sobre la ruta, límite de 50 triggers sin file events, o storage cerrado (N3) | Revisar en ese orden |
| El deploy pide confirmar un borrado | Se ha quitado un contenedor o cambiado algo que recrea un esquema, Volume o pipeline | Revisarlo: la pipeline se lleva sus tablas de silver y los esquemas, sus datos |
| El CI no llega al workspace | Agente fuera de la red de AXA (N4) | Pool self-hosted |
| El CI falla al autenticar | Credencial federada o SP no añadido al workspace (C1, C4) | Revisar la service connection y la asignación del SP |
