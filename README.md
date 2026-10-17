# odp-ingestion-bundle

Bundle de ingesta CLEX → bronze → silver de la ONYX Data Platform (ODP), desplegado con DABs.

- **Un job por contenedor del landing** (contenedor = data custodian × instancia de CLEX), con N tablas.
  Los contenedores se listan en `config/containers.yml`: una entrada es un job.
- **El manifiesto de CLEX define cada tabla**: destino, ruta, tipo de carga, keys y secuencia.
- **Bronze con Auto Loader** (sin SDP): una iteración de un for_each por tabla.
- **Silver con SDP**: consolidación SCD1 con AUTO CDC, réplica de la tabla de origen.
- **Código en una wheel versionada** (`odp_framework`), fijada por entorno.

```
CLEX (otro tenant)                   Workspace odp-dbx-{team}-{env}
──────────────────                   ──────────────────────────────────────────────────────────────────
data/<tabla>/run_id=R/*.parquet ┐
_manifests/<tabla>/R.json  ─────┼──► job-{team}-{env}-{contenedor}-ingest          (trigger: file arrival)
  (al final; lleva load_spec)   │      register  wheel     manifiestos nuevos → odp_manifests
                                │                          → registro odp_entities → tablas a cargar
                                │      has_data            ¿alguna tabla con datos nuevos?
                                │      ingest    for_each  1 iteración por tabla: Auto Loader → bronze
                                │      silver    SDP       AUTO CDC / FROM SNAPSHOT → réplica SCD1
                                │      report    wheel     reconciliación (run_if ALL_DONE)
```

register, las iteraciones de ingest y report comparten un job cluster clásico con autoscaling. Solo silver
levanta un cluster propio, el de su pipeline.

## Por qué bronze sin SDP y silver con SDP

- **Bronze (landing → append)**: Auto Loader ya resuelve la detección de ficheros, el exactly-once y la
  evolución de esquema. Sin SDP:
  - cada tabla es una iteración que se ve y se reintenta por separado;
  - solo se procesan las tablas que han recibido datos;
  - los checkpoints están donde decide la plataforma;
  - la wheel se instala de la forma estándar en una tarea.
- **Silver (SCD1)**: AUTO CDC aplica inserts, updates y deletes en orden de secuencia, con eventos
  desordenados y tombstones. AUTO CDC FROM SNAPSHOT deduce los cambios comparando snapshots. Es justo el
  código delicado que no queremos mantener a mano.

## Flujo de una ejecución

1. **register** (`odp-register`):
   - Auto Loader `availableNow` lee los manifiestos nuevos y los añade a `odp_manifests`.
   - Recalcula `odp_entities` desde **todos** los manifiestos aplicando los guardarraíles. El resultado es
     determinista e idempotente.
   - Decide las tablas a cargar: las activas con algún manifiesto posterior al inicio de su última carga
     correcta (`odp_load_log`). Las que fallaron antes entran solas; las que no recibieron nada, no.
   - Publica los task values `entities_to_load` (entrada del for_each, recortada al límite de 48 KB),
     `load_count` y `active_entities`.
2. **ingest** (`odp-ingest`, una iteración por tabla con concurrencia configurable):
   - Lee la spec de su tabla del registro.
   - Auto Loader `availableNow` sobre `data/{source_path}/` hace append directo (`toTable`, exactly-once) a
     `{bronze}.{contenedor}.{target_table}`, añadiendo `_run_id`, `_source_file` e `_ingested_at`.
   - Si llega una columna nueva, Auto Loader para el stream a propósito; se relanza dentro de la tarea (hasta
     3 veces) y la tabla la añade (`mergeSchema`).
   - Al crear la tabla le pone `delta.appendOnly = true`. Las tablas full se agrupan con `CLUSTER BY (_run_id)`.
   - Deja constancia en `odp_load_log`, tanto si va bien como si falla.
   - Recalcula `odp_rejected_runs`: runs cuyos parquet están en la carpeta de una tabla registrada pero cuyo
     manifiesto se rechaza (inválido, `SPEC_CONFLICT`, `SOURCE_PATH_CONFLICT`).
3. **silver** (pipeline SDP, `run_if ALL_DONE`): lee el registro y declara una réplica SCD1 por tabla que ya
   tenga bronze. Si falla la carga de una tabla, el resto se consolida igual: bronze es append exactly-once
   y los snapshots full solo se aplican completos. Los runs rechazados no se aplican.
4. **report** (`odp-report`): cruza manifiestos y filas en bronze por run, y hace fallar el job si hay
   errores propios o de register en esta ejecución, lo que dispara las notificaciones.

## Alta de un contenedor nuevo

Se añade una entrada a `config/containers.yml` y se despliega:

```yaml
containers:
  - container: claims-postgres          # nombre del contenedor en el storage de landing
    custodian: claims
    data_class: confidential
    readers: [grp-datateam01-claims-dbx-reader]
    environments: [dev]                 # opcional: primero en dev; luego [dev, pre], y quitar el campo = todos
```

El deploy genera, a partir de la entrada:
- el job `job-{team}-{env}-claims-postgres-ingest`, con trigger en `.../claims-postgres/_manifests/`;
- la pipeline `sdp-{team}-{env}-claims-postgres-silver`;
- los esquemas `claims_postgres` en bronze y en silver, con lectura para `readers`;
- el Volume `odp_system`.

La ruta no se escribe: sale del naming, `abfss://{container}@{storage_account}...`. Solo hay que indicar
`storage_account` si el contenedor está en otra cuenta. Overrides opcionales por contenedor: `ingest`,
`pipelines`, `trigger` y `report`. `active: false` es una pausa: quita el job y conserva esquemas, Volume,
pipeline y datos. Borrar la entrada es la baja definitiva: el deploy borraría la pipeline, y con ella sus
tablas de silver, y los esquemas; el CLI pide confirmación y el CI, sin `--auto-approve`, se para.

**Fuera del bundle** (infraestructura, una vez por contenedor):
1. Crear el contenedor en el storage de landing (Terraform).
2. Dar al SPN de CLEX escritura solo en ese contenedor.
3. Si las external locations son por cuenta, no hay nada más que hacer. Si son por contenedor, crear la
   nueva con `READ FILES` para el `run_as_sp` y activarle file events.

Las **tablas** de un contenedor no se configuran: llegan solas con los manifiestos (ver más abajo).

## Checkpoints: rutas fijas, nunca desde el manifiesto

El checkpoint es la identidad de un stream: si cambiara, Auto Loader volvería a leer todo el landing y
duplicaría bronze. Todos viven en el Volume de plataforma del contenedor:

```
/Volumes/{bronze}/{contenedor}/odp_system/
  checkpoints/odp_manifests           lectura de manifiestos (register)
  autoloader/{tabla}/checkpoint       Auto Loader de cada tabla (ingest)
  autoloader/{tabla}/schema           esquema inferido de cada tabla
```

Los checkpoints de silver los gestiona SDP. El Volume solo lo escribe el `run_as_sp`. Si se borra el
checkpoint de una tabla, se relee lo que quede en el landing y hay duplicados en bronze, que se pueden
quitar con `_source_file`.

## Guardarraíles sobre lo que pide CLEX

| Regla | Si no se cumple |
|---|---|
| Manifiesto conforme al contrato (`odp_framework.manifest.validate_manifest`) | `INVALID_MANIFEST`, no se registra |
| `target_table` es solo un nombre de tabla: el catálogo y el esquema los pone la plataforma | inválido |
| `source_path` dentro de `data/` del contenedor, sin `..`; `data_path` = `data/{source_path}/run_id={run_id}/` | inválido |
| `source_path`, `load_type`, `keys`, `sequence_by` y `operation_column` no cambian una vez registrada la tabla | `SPEC_CONFLICT`, el run no se asocia |
| `schema_hints`, `delete_values` y `allow_empty_snapshot` solo cambian subiendo `spec_version` | `SPEC_CHANGED_WITHOUT_VERSION` |
| Dos tablas no pueden leer la misma `source_path` ni rutas anidadas (`a/b/c` y `a/b/c/d`: Auto Loader lee recursivamente) | `SOURCE_PATH_CONFLICT` |
| La especificación registrada es la del primer manifiesto aceptado, por orden de llegada; uno que llegue tarde con un `run_id` anterior no la cambia | `SPEC_CONFLICT` |

Un manifiesto erróneo no para el resto de tablas. Los hallazgos se avisan una vez, cuando llega el
manifiesto.

**Runs rechazados.** Auto Loader lee la carpeta entera de una tabla (y sus subcarpetas), así que los parquet
de un run cuyo manifiesto se rechaza llegan a bronze si caen en la carpeta de una tabla registrada, ya sea la
que dice `source_path` o la que dice `data_path`. Bronze los conserva como evidencia y silver los excluye por
`_run_id` (lista en `odp_rejected_runs`). Los runs sin manifiesto (CLEX cortado a mitad) no están en la lista:
CDC los aplica y full no. Si los parquet de un run llegan
y se aplican en silver antes que su manifiesto, y ese manifiesto resulta rechazado, el arreglo es un full
refresh de esa tabla de silver: se reconstruye desde bronze con el filtro ya en vigor.

**Quién aprueba una tabla nueva:**
- `registration: auto`: el control es el PR de la configuración de CLEX.
- `registration: manual`: solo se cargan las tablas de `approved_entities` del `container.yml`. Es una
  segunda aprobación por PR, en Databricks.
- `disabled_entities` da de baja una tabla: deja de cargarse y sus datos se conservan.

## Tablas que genera

| Capa | Tabla | Contenido |
|---|---|---|
| bronze | `odp_manifests` | Todos los manifiestos |
| bronze | `odp_entities` | Registro de tablas, su estado (ACTIVE / PENDING_APPROVAL / DISABLED) y su último manifiesto |
| bronze | `odp_load_log` | Una fila por carga de tabla: estado, filas, intentos y error |
| bronze | `{target_table}` | Todo lo que entrega CLEX, tal cual, más `_run_id`, `_source_file` e `_ingested_at`. Append-only |
| silver | `{target_table}` | **Réplica SCD1 de la tabla de origen** (con change data feed para las capas siguientes) |
| bronze | `odp_run_findings` | Hallazgos de register y report |
| bronze | `odp_rejected_runs` | Runs en bronze que silver no aplica (manifiesto rechazado) |

**Snapshots full: solo se aplican completos.** Un run solo se aplica si tiene manifiesto, las filas en
bronze para su `run_id` coinciden con `row_count` y no está vacío (salvo `allow_empty_snapshot`). Se
aplican en orden de versión, que sale de la fecha del `run_id`, cada uno en su commit (el change data feed
refleja cada snapshot). Al empezar de cero (primera vez o full refresh) se parte del snapshot completo más
reciente, sin repasar el histórico de bronze.

**Deletes CDC tardíos.** AUTO CDC recuerda cada delete (tombstone) 7 días
(`pipelines.cdc.tombstoneGCThresholdInSeconds`; el valor por defecto son 2). Un evento más antiguo que el delete
que llegue después de ese plazo resucitaría la fila; un full refresh desde bronze no depende de este plazo.

## Estructura

```
databricks.yml                 targets dev/pre/pro y variables (wheel, policies, SPs, storage)
resources/__init__.py          load_resources → odp_framework.bundle.build_plan
config/
  platform.yml                 naming, layout, registro, ingest (concurrencia, cluster), silver, trigger
  containers.yml               lista de contenedores: una entrada = un job (custodian, readers,
                               environments, registration, approved/disabled_entities, overrides)
src/pipelines/silver.py        fuente SDP de silver: config del contenedor + tablas del registro → wheel
src/odp_framework/             la wheel
  manifest.py                  contrato con CLEX y guardarraíles
  registry.py                  manifiestos → registro de tablas; qué tablas cargar
  ingest.py                    bronze con Auto Loader (una tabla)
  sdp/silver.py                consolidación SCD1 (AUTO CDC / FROM SNAPSHOT)
  snapshot.py  reconcile.py    selección de snapshot y reconciliación (lógica pura)
  tasks/register.py  tasks/ingest.py  tasks/report.py    entry points odp-register, odp-ingest, odp-report
  e2e.py  tasks/e2e.py         prueba end-to-end: simulador de CLEX, escenario y comprobaciones (odp-e2e)
  config.py  bundle.py         deploy: validación de container.yml y generación de recursos
samples/                       manifiestos de ejemplo (contrato 2.0) y disposición del landing
scripts/publish_wheel.sh       publicación inmutable (e idempotente) de la wheel en el Volume del entorno
ci/                            Azure DevOps: tests → dev + e2e → pre → pro
docs/DESPLIEGUE.md             cómo desplegar y probar, paso a paso
tests/                         133 tests (sin Spark: dobles de prueba para spark y dp; el escenario e2e
                               se comprueba contra un modelo de referencia en SQLite)
```

## Contrato con CLEX (2.0)

```
data/{source_path}/run_id={run_id}/part-*.parquet
_manifests/{source_path}/{run_id}.json        ← al final
```

```json
{
  "contract_version": "2.0",
  "run_id": "20261016T020000Z_8f3a1c2d",
  "data_path": "data/srv-motors-01/GDWR/dbo/customers/run_id=20261016T020000Z_8f3a1c2d/",
  "file_count": 2, "row_count": 120345, "seq_min": "...", "seq_max": "...",
  "schema_hash": "sha256:...", "extracted_at": "2026-10-16T02:00:00Z",
  "clex": {"pipeline_run_id": "...", "connector": "sqlserver", "instance": "..."},
  "load_spec": {
    "target_table": "gdwr_customers",
    "source_path": "srv-motors-01/GDWR/dbo/customers",
    "load_type": "cdc",
    "keys": ["customer_id"],
    "cdc": {"sequence_by": ["_seq"], "operation_column": "_op", "delete_values": ["D"]},
    "full": {"allow_empty_snapshot": false},
    "schema_hints": "customer_id BIGINT",
    "spec_version": 1
  }
}
```

- `run_id` = `YYYYMMDDTHHMMSSZ_<id>` (UTC de la extracción). Una carpeta por run; nunca se sobrescribe.
- CDC: operación normalizada, secuencia creciente por PK y nunca NULL, deletes incluidos, carga inicial como
  inserts.
- Validador para el equipo de CLEX: `odp_framework.manifest.validate_manifest(manifiesto)`.

## Versionado del código (wheel)

1. Cambio en `src/odp_framework` → subir versión en `pyproject.toml` y `__init__.py`.
2. CI: tests → `scripts/publish_wheel.sh <env>` publica la wheel en `/Volumes/odp_platform_<env>/libs/wheels/`.
   Nunca sobrescribe: si la versión ya existe con el mismo contenido no hace nada, y si existe con otro
   contenido falla.
3. PR que cambia `framework_wheel` del target → pasa por dev (con e2e) y pre antes de pro.

Usan la wheel las tareas del job (`libraries.whl`) y la pipeline de silver (`environment.dependencies`). El
deploy avisa si la versión fijada no coincide con la del generador.

## Despliegue y pruebas

Paso a paso en [`docs/DESPLIEGUE.md`](docs/DESPLIEGUE.md): prerrequisitos, primer despliegue en dev, prueba
end-to-end, prueba con CLEX real, promoción a pre y pro por CI, operación y marcha atrás.

```bash
uv sync && uv run pytest
scripts/publish_wheel.sh dev
databricks bundle deploy -t dev
databricks bundle run e2e_e2e_selftest -t dev      # simula a CLEX en 3 pasos y comprueba bronze/silver
```

En `dev` (`mode: development`) cada desarrollador tiene sus propios esquemas (`dev_<usuario>_<contenedor>`),
jobs con prefijo `[dev usuario]` y triggers en pausa. En pre y pro despliega el CI (`ci/`) con
`svc-dbks-{team}-{env}-deploy` y jobs y pipelines se ejecutan como `run_as_sp`.

## Requisitos en el workspace

- Catálogos `odp_{team}_{env}_bronze` y `_silver`. Con `manage_schemas: "true"` (valor por defecto) el
  bundle crea los esquemas de cada contenedor (con grants para `readers`) y el Volume `odp_system`. Quien
  despliega necesita `USE CATALOG` y `CREATE SCHEMA` en ambos catálogos.
- External location sobre el storage de landing con `READ FILES` para el `run_as_sp`. El contenedor
  `e2e-selftest` necesita además `WRITE FILES` para quien ejecute el e2e (el simulador escribe en él).
- **File events** en esa external location (`file_events: "true"`). Sin ellos hay un máximo de 50 jobs con
  trigger por workspace y 10.000 ficheros por ruta. Requieren roles del Access Connector sobre el storage
  (Storage Blob Data Contributor, EventGrid EventSubscription Contributor, Storage Queue Data Contributor;
  lista sacada de la comunidad de Databricks, a confirmar en la documentación oficial) y el resource
  provider de Event Grid registrado. Hay que pedírselos a Group.
- Cluster policies:
  - pipeline de silver: `cluster_type: dlt`, tags obligatorios y modo de acceso standard, que es el que
    exige Unity Catalog en pipelines clásicas;
  - job cluster: tags obligatorios y modo de acceso dedicado (`SINGLE_USER`) para el `run_as_sp`.
- Volume `odp_platform_<env>.libs.wheels` de solo lectura para los equipos.
- **Lectura de silver por los consumidores.** Las tablas de silver son streaming tables de SDP. Su dueño
  (el `run_as_sp`) las lee desde cualquier compute. Los demás, desde un SQL warehouse, un cluster con modo de
  acceso standard, o un cluster dedicado con DBR 15.4+ **solo si el workspace tiene serverless** (el filtrado
  se ejecuta en serverless). Sin serverless, los jobs de gold y los analistas usan standard o SQL warehouse.

## Qué está validado y qué falta probar

Validado aquí:

- 133 tests en Python 3.11 y 3.13:
  - lista de contenedores (validación, alta progresiva por entorno, overrides, grants, e2e solo fuera de pro);
  - contrato y guardarraíles (rutas anidadas, orden de llegada); runs rechazados;
  - registro: alta, conflictos, versiones, aprobación y qué tablas cargar;
  - Auto Loader de bronze: rutas, checkpoints, reintento por cambio de esquema, propiedades;
  - silver: AUTO CDC, snapshots, espera a bronze, exclusión de runs rechazados;
  - las tareas de punta a punta con dobles de prueba;
  - el escenario e2e reproducido con la lógica del framework y una referencia SCD1: cada comprobación del
    e2e se ejecuta en SQLite antes de llegar al workspace.
- Recursos generados contra los modelos tipados de `databricks-bundles` 1.21.
- `databricks bundle validate` (CLI 1.21) de dev, pre y pro contra un workspace simulado, y `bundle plan`
  de dev: el esquema prefijado de `mode: development` llega resuelto a `config_json`, `odp.config` y el Volume.

Lo cubre el job e2e en el workspace (docs/DESPLIEGUE.md, paso 4):

1. Wheel desde un Volume en la pipeline **clásica** de silver (`environment.dependencies`, en Public Preview).
2. Que un flow full no falle en las actualizaciones en las que no llega ningún snapshot nuevo (paso 3:
   snapshot incompleto).
3. Columna nueva de punta a punta: reintento de Auto Loader en bronze y evolución del destino de AUTO CDC.
4. Desorden, deletes y eventos atrasados en AUTO CDC; altas, bajas y cambios en FROM SNAPSHOT.
5. Run en conflicto: llega a bronze y no a silver.

Pendiente de medir con volumen real (pre):

6. Cuánto tarda la pipeline de silver en definir el grafo leyendo el registro, con cientos de tablas.
7. Auto Loader con file events sobre `_manifests/` anidado y la partición `run_id=`.
8. Qué hacen silver y report (`run_if ALL_DONE`) cuando `has_data` es falso. Si no hay nada que cargar y hay
   errores, `register` ya falla por su cuenta.
9. Coste por iteración del for_each, el tiempo de arrancar cada tarea, frente a la concurrencia elegida.

## Siguientes pasos

- Avisar de runs en bronze sin manifiesto (CLEX cortado a mitad de escritura): en CDC ya se aplican a silver;
  en full no, porque no hay manifiesto que los dé por completos.
- Refrescar en silver solo las tablas cargadas en esta ejecución (`refresh_selection` de la tarea de
  pipeline), a partir de la lista de register.
- Columnas aprobadas por tabla en `load_spec`, si el custodian quiere controlar las columnas nuevas antes de
  silver. Hoy bronze y silver las absorben.
- Retención de snapshots full antiguos en bronze (mantener los últimos N runs).
- Registro incremental cuando `odp_manifests` crezca mucho (hoy se recalcula desde todos los manifiestos).
  Lo mismo para `odp_rejected_runs`, que no se poda y entra en el plan de silver como lista `NOT IN`.
- Grants de los esquemas del bundle: decidir con governance quién los gestiona (ver docs/DESPLIEGUE.md, 0.3).
