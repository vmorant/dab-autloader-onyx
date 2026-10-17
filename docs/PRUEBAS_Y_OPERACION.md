# Pruebas y operación

Cómo probar el bundle en cada entorno y cómo operarlo. Los requisitos y el despliegue están en
[`PUESTA_EN_MARCHA.md`](PUESTA_EN_MARCHA.md).

| Nivel | Dónde | Qué prueba | Cuándo |
|---|---|---|---|
| 1. Tests | local y CI | Lógica del framework, guardarraíles, generación del bundle y coherencia del escenario e2e (133 tests, sin Spark) | Cada commit |
| 2. `validate` | contra el workspace | `containers.yml`, variables, referencias, `run_as`, permisos | Cada deploy |
| 3. **e2e** | dev (a mano y en CI) | El flujo completo con un CLEX simulado: Auto Loader, AUTO CDC, snapshots, guardarraíles, wheel en la pipeline clásica | Cada cambio de wheel o de bundle |
| 4. CLEX real | dev | Contrato real con CLEX, escritura cross-tenant, trigger file arrival | Alta de cada contenedor o conector |
| 5. Volumen | pre | Tiempos, coste, file events, concurrencia | Antes de pro |

Los niveles 1 y 2 están en la fase 1 de [`PUESTA_EN_MARCHA.md`](PUESTA_EN_MARCHA.md#5-despliegue-paso-a-paso).

---

## 1. Prueba end-to-end con el simulador de CLEX

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
- **Si falla**, `cleanup` no se ejecuta y las tablas se quedan para investigar (consultas en el punto 3). La
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

## 2. Prueba con CLEX real (dev)

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

## 3. Consultas de seguimiento

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

## 4. Pruebas en pre

Con el bundle desplegado en pre por el CI (fase 3 de [`PUESTA_EN_MARCHA.md`](PUESTA_EN_MARCHA.md)) y CLEX
pre entregando volumen real:

- Tiempo de inicialización del grafo de silver con todas las tablas del contenedor.
- Trigger file arrival con el storage cerrado (requisito N3) y, después, `file_events: "true"` en el target
  una vez activados en la external location: el trigger y Auto Loader tienen que seguir detectando ficheros.
- Concurrencia del for_each (`ingest.concurrency`) y tamaño del cluster frente a duración y coste.
- Qué hacen `silver` y `report` cuando `has_data` es falso (ejecución sin datos nuevos).
- Alta progresiva: un contenedor nuevo entra con `environments: [dev]`, luego `[dev, pre]` y, al quitar el
  campo, en todos.

## 5. Operación

| Necesidad | Cómo |
|---|---|
| Nueva versión del framework | Subir versión → PR → CI: tests, wheel, dev + e2e, pre, pro |
| Marcha atrás | `git revert` del PR. La wheel anterior sigue en el Volume (inmutable) y el CI redespliega config y versión |
| Reconstruir una tabla de silver | `databricks bundle run silver_<contenedor> -t <env> --full-refresh <tabla>`. CDC relee todo bronze; full parte del snapshot completo más reciente. Es también el arreglo si un run se rechaza después de haberse aplicado, o si un evento CDC llega más de 7 días tarde respecto a un delete |
| Dar de baja una tabla | `disabled_entities` en `containers.yml` (PR). Deja de cargarse; sus datos se quedan |
| Pausar un contenedor | `active: false`: se quita el job; esquemas, Volume, pipeline y datos se conservan |
| Baja definitiva de un contenedor | Borrar su entrada. El deploy borra pipeline (y tablas de silver), job, Volume y esquemas: pide confirmación y el CI se para |
| Recargar bronze de una tabla | Evitarlo: bronze es la única copia una vez caduca el landing (30 días en pro). Si hay duplicados por haber perdido un checkpoint, se quitan con `_source_file` |
