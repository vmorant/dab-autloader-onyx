# odp-ingestion-bundle

CLEX → bronze → silver ingestion bundle for the ONYX Data Platform (ODP), deployed with DABs.

- **One job per landing container** (container = data custodian × CLEX instance), with N tables.
  Containers are listed in `config/containers.yml`: one entry is one job.
- **The CLEX manifest defines each table**: target, path, load type, keys and sequence.
- **Bronze with Auto Loader** (no SDP): one for_each iteration per table.
- **Silver with SDP**: SCD1 consolidation with AUTO CDC, a replica of the source table.
- **Code in a versioned wheel** (`odp_framework`), pinned per environment.

```
CLEX (another tenant)                Workspace odp-dbx-{team}-{env}
──────────────────                   ──────────────────────────────────────────────────────────────────
data/<table>/run_id=R/*.parquet ┐
_manifests/<table>/R.json  ─────┼──► job-{team}-{env}-{container}-ingest           (trigger: file arrival)
  (last; carries load_spec)     │      register  wheel     new manifests → odp_manifests
                                │                          → registry odp_entities → tables to load
                                │      has_data            any table with new data?
                                │      ingest    for_each  1 iteration per table: Auto Loader → bronze
                                │      silver    SDP       AUTO CDC / FROM SNAPSHOT → SCD1 replica
                                │      report    wheel     reconciliation (run_if ALL_DONE)
```

register, the ingest iterations and report share one classic job cluster with autoscaling. Only silver
starts a cluster of its own, the pipeline's.

## Why bronze without SDP and silver with SDP

- **Bronze (landing → append)**: Auto Loader already handles file discovery, exactly-once and schema
  evolution. Without SDP:
  - each table is an iteration that is visible and retried on its own;
  - only tables that received data are processed;
  - checkpoints live where the platform decides;
  - the wheel is installed the standard way, in a task.
- **Silver (SCD1)**: AUTO CDC applies inserts, updates and deletes in sequence order, with out-of-order
  events and tombstones. AUTO CDC FROM SNAPSHOT derives the changes by comparing snapshots. That is exactly
  the delicate code we do not want to maintain by hand.

## Flow of a run

1. **register** (`odp-register`):
   - Auto Loader `availableNow` reads the new manifests and appends them to `odp_manifests`.
   - Recomputes `odp_entities` from **all** manifests, applying the guardrails. The result is
     deterministic and idempotent.
   - Recomputes `odp_rejected_runs`: runs whose parquet files are in the folder of a registered table but
     whose manifest is rejected (invalid, `SPEC_CONFLICT`, `SOURCE_PATH_CONFLICT`).
   - Decides which tables to load: the active ones with some manifest later than the start of their last
     successful load (`odp_load_log`). Tables that failed before are picked up automatically; those that
     received nothing are not.
   - Publishes the task values `entities_to_load` (for_each input, trimmed to the 48 KB limit),
     `load_count` and `active_entities`.
2. **ingest** (`odp-ingest`, one iteration per table with configurable concurrency):
   - Reads its table's spec from the registry.
   - Auto Loader `availableNow` on `data/{source_path}/` appends directly (`toTable`, exactly-once) to
     `{bronze}.{container}.{target_table}`, adding `_run_id`, `_source_file` and `_ingested_at`.
   - When a new column arrives, Auto Loader stops the stream on purpose; it is restarted inside the task (up
     to 3 times) and the table adds the column (`mergeSchema`).
   - When it creates the table it sets `delta.appendOnly = true`. Full tables are clustered with
     `CLUSTER BY (_run_id)`.
   - Records the outcome in `odp_load_log`, whether it succeeds or fails.
3. **silver** (SDP pipeline, `run_if ALL_DONE`): reads the registry and declares an SCD1 replica for each
   table that already has bronze. If one table's load fails, the rest is still consolidated: bronze is
   exactly-once append and full snapshots are only applied when complete. Rejected runs are not applied.
4. **report** (`odp-report`): cross-checks manifests against bronze rows per run, and fails the job if
   there are errors of its own or from register in this run, which triggers the notifications.

## Onboarding a new container

Add an entry to `config/containers.yml` and deploy:

```yaml
containers:
  - container: claims-postgres          # name of the container in the landing storage
    custodian: claims
    data_class: confidential
    readers: [grp-datateam01-claims-dbx-reader]
    environments: [dev]                 # optional: dev first; then [dev, pre], and removing the field = all
```

From that entry, the deploy generates:
- the job `job-{team}-{env}-claims-postgres-ingest`, with a trigger on `.../claims-postgres/_manifests/`;
- the pipeline `sdp-{team}-{env}-claims-postgres-silver`;
- the `claims_postgres` schemas in bronze and in silver, readable by `readers`;
- the `odp_system` Volume.

The path is not written anywhere: it comes from the naming, `abfss://{container}@{storage_account}...`.
`storage_account` is only needed if the container lives in another account. Optional per-container
overrides: `ingest`, `pipelines`, `trigger` and `report`. `active: false` is a pause: it removes the job and
keeps schemas, Volume, pipeline and data. Deleting the entry is the permanent removal: the deploy would
delete the pipeline, and with it its silver tables, and the schemas; the CLI asks for confirmation and the
CI, without `--auto-approve`, stops.

**Outside the bundle** (infrastructure, once per container):
1. Create the container in the landing storage (Terraform).
2. Give the CLEX SPN write access to that container only.
3. If external locations are per account, there is nothing else to do. If they are per container, create
   the new one with `READ FILES` for the `run_as_sp` and enable file events on it.

The **tables** of a container are not configured: they arrive on their own with the manifests (see below).

## Checkpoints: fixed paths, never from the manifest

A checkpoint is the identity of a stream: if it changed, Auto Loader would read the whole landing again
and duplicate bronze. They all live in the container's platform Volume:

```
/Volumes/{bronze}/{container}/odp_system/
  checkpoints/odp_manifests           manifest reading (register)
  autoloader/{table}/checkpoint       Auto Loader for each table (ingest)
  autoloader/{table}/schema           inferred schema of each table
```

SDP manages the silver checkpoints. Only the `run_as_sp` writes to the Volume. If a table's checkpoint is
deleted, whatever is left in the landing is reread and bronze gets duplicates, which can be removed with
`_source_file`.

## Guardrails on what CLEX asks for

| Rule | If it is not met |
|---|---|
| Manifest compliant with the contract (`odp_framework.manifest.validate_manifest`) | `INVALID_MANIFEST`, not registered |
| `target_table` is only a table name: the platform sets the catalog and the schema | invalid |
| `source_path` inside the container's `data/`, no `..`; `data_path` = `data/{source_path}/run_id={run_id}/` | invalid |
| `source_path`, `load_type`, `keys`, `sequence_by` and `operation_column` do not change once the table is registered | `SPEC_CONFLICT`, the run is not associated |
| `schema_hints`, `delete_values` and `allow_empty_snapshot` only change by bumping `spec_version` | `SPEC_CHANGED_WITHOUT_VERSION` |
| Two tables cannot read the same `source_path` nor nested paths (`a/b/c` and `a/b/c/d`: Auto Loader reads recursively) | `SOURCE_PATH_CONFLICT` |
| The registered spec is the one from the first accepted manifest, in arrival order; a late manifest with an earlier `run_id` does not change it | `SPEC_CONFLICT` |

A faulty manifest does not stop the other tables. Findings are reported once, when the manifest arrives.

**Rejected runs.** Auto Loader reads a table's whole folder (and its subfolders), so the parquet files of a
run whose manifest is rejected reach bronze if they land in the folder of a registered table, whether the
one given by `source_path` or by `data_path`. Bronze keeps them as evidence and silver excludes them by
`_run_id` (listed in `odp_rejected_runs`). Runs without a manifest (CLEX cut off midway) are not on the
list: CDC applies them and full does not. If a run's parquet files arrive and are applied to silver before
its manifest, and that manifest turns out to be rejected, the fix is a full refresh of that silver table:
it is rebuilt from bronze with the filter already in place.

**Who approves a new table:**
- `registration: auto`: the control is the PR on the CLEX configuration.
- `registration: manual`: only the tables in `approved_entities` of `container.yml` are loaded. It is a
  second approval by PR, on the Databricks side.
- `disabled_entities` retires a table: it stops loading and its data is kept.

## Generated tables

| Layer | Table | Contents |
|---|---|---|
| bronze | `odp_manifests` | All manifests |
| bronze | `odp_entities` | Table registry, its status (ACTIVE / PENDING_APPROVAL / DISABLED) and its latest manifest |
| bronze | `odp_load_log` | One row per table load: status, rows, attempts and error |
| bronze | `{target_table}` | Everything CLEX delivers, as is, plus `_run_id`, `_source_file` and `_ingested_at`. Append-only |
| silver | `{target_table}` | **SCD1 replica of the source table** (with change data feed for the downstream layers) |
| bronze | `odp_run_findings` | Findings from register and report |
| bronze | `odp_rejected_runs` | Runs in bronze that silver does not apply (rejected manifest) |

**Full snapshots: only applied when complete.** A run is only applied if it has a manifest, the bronze rows
for its `run_id` match `row_count`, and it is not empty (unless `allow_empty_snapshot`). They are applied
in version order, which comes from the `run_id` timestamp, each in its own commit (the change data feed
reflects each snapshot). When starting from scratch (first time or full refresh) it starts from the most
recent complete snapshot, without replaying the bronze history.

**Late CDC deletes.** AUTO CDC remembers each delete (tombstone) for 7 days
(`pipelines.cdc.tombstoneGCThresholdInSeconds`; the default is 2). An event older than the delete that
arrives after that window would resurrect the row; a full refresh from bronze does not depend on this
window.

## Structure

```
databricks.yml                 dev/pre/pro targets and variables (wheel, policies, SPs, storage)
resources/__init__.py          load_resources → odp_framework.bundle.build_plan
config/
  platform.yml                 naming, layout, registry, ingest (concurrency, cluster), silver, trigger
  containers.yml               list of containers: one entry = one job (custodian, readers,
                               environments, registration, approved/disabled_entities, overrides)
src/pipelines/silver.py        silver SDP source: container config + registry tables → wheel
src/odp_framework/             the wheel
  manifest.py                  CLEX contract and guardrails
  registry.py                  manifests → table registry; which tables to load
  ingest.py                    bronze with Auto Loader (one table)
  sdp/silver.py                SCD1 consolidation (AUTO CDC / FROM SNAPSHOT)
  snapshot.py  reconcile.py    snapshot selection and reconciliation (pure logic)
  tasks/register.py  tasks/ingest.py  tasks/report.py    entry points odp-register, odp-ingest, odp-report
  e2e.py  tasks/e2e.py         end-to-end test: CLEX simulator, scenario and checks (odp-e2e)
  config.py  bundle.py         deploy: container.yml validation and resource generation
samples/                       sample manifests (contract 2.0) and landing layout
scripts/publish_wheel.sh       immutable (and idempotent) publication of the wheel to the environment Volume
ci/                            Azure DevOps: tests → dev + e2e → pre → pro
docs/DEPLOYMENT.md             what is needed and how to deploy to Databricks, from dev to pro
docs/TESTING_AND_OPERATIONS.md e2e, test with real CLEX, monitoring queries and operations
tests/                         133 tests (no Spark: test doubles for spark and dp; the e2e scenario
                               is checked against a reference model in SQLite)
```

## CLEX contract (2.0)

```
data/{source_path}/run_id={run_id}/part-*.parquet
_manifests/{source_path}/{run_id}.json        ← last
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

- `run_id` = `YYYYMMDDTHHMMSSZ_<id>` (UTC of the extraction). One folder per run; never overwritten.
- CDC: normalised operation, sequence increasing per PK and never NULL, deletes included, initial load as
  inserts.
- Validator for the CLEX team: `odp_framework.manifest.validate_manifest(manifest)`.

## Code versioning (wheel)

1. Change in `src/odp_framework` → bump the version in `pyproject.toml` and `__init__.py`.
2. CI: tests → `scripts/publish_wheel.sh <env>` publishes the wheel to `/Volumes/odp_platform_<env>/libs/wheels/`.
   It never overwrites: if the version already exists with the same content it does nothing, and if it
   exists with different content it fails.
3. PR changing the target's `framework_wheel` → goes through dev (with e2e) and pre before pro.

The job tasks (`libraries.whl`) and the silver pipeline (`environment.dependencies`) use the wheel. The
deploy warns if the pinned version does not match the generator's.

## Deployment and testing

- [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md): requirements (who, how to check them), open decisions,
  configuration, deployment from dev to pro, verification and common errors.
- [`docs/TESTING_AND_OPERATIONS.md`](docs/TESTING_AND_OPERATIONS.md): end-to-end test, test with real CLEX,
  monitoring queries, testing in pre and operations.

```bash
uv sync && uv run pytest
scripts/publish_wheel.sh dev
databricks bundle deploy -t dev
databricks bundle run e2e_e2e_selftest -t dev      # simulates CLEX in 3 steps and checks bronze/silver
```

In `dev` (`mode: development`) each developer gets their own schemas (`dev_<user>_<container>`), jobs
prefixed `[dev user]` and paused triggers. In pre and pro the CI (`ci/`) deploys with
`svc-dbks-{team}-{env}-deploy`, and jobs and pipelines run as `run_as_sp`.

## Workspace requirements

Full inventory, with owner and how to check each item, in
[`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md#2-requirements). The essentials:

- Azure and network: workspace with Unity Catalog, landing account with one container per entry in
  `containers.yml`, Access Connector, private endpoints from the workspace and a private route from the
  CLEX ADF (another tenant).
- Unity Catalog: storage credential, landing external location, catalogs `odp_{team}_{env}_bronze` and
  `_silver`, Volume `odp_platform_<env>.libs.wheels` and the grants that live outside the bundle.
- Identities: deploy and run-as SPs per environment, Service Principal User role of the former on the
  latter, groups and `CAN_USE` on the cluster policies.
- Workspace: job and pipeline cluster policies, DBR 16.4 LTS and `environment.dependencies` in classic
  pipelines.
- Silver consumers: without serverless, SQL warehouse or standard clusters (the tables are SDP streaming
  tables).

## What is validated and what remains to be tested

Validated here:

- 133 tests on Python 3.11 and 3.13:
  - container list (validation, progressive rollout per environment, overrides, grants, e2e never in pro);
  - contract and guardrails (nested paths, arrival order); rejected runs;
  - registry: onboarding, conflicts, versions, approval and which tables to load;
  - bronze Auto Loader: paths, checkpoints, retry on schema change, properties;
  - silver: AUTO CDC, snapshots, waiting for bronze, exclusion of rejected runs;
  - the tasks end to end with test doubles;
  - the e2e scenario replayed with the framework logic and an SCD1 reference: every e2e check runs on
    SQLite before reaching the workspace.
- Resources generated against the typed models of `databricks-bundles` 1.21.
- `databricks bundle validate` (CLI 1.21) for dev, pre and pro against a simulated workspace, and
  `bundle plan` for dev: the prefixed schema of `mode: development` reaches `config_json`, `odp.config` and
  the Volume resolved.

Covered by the e2e job in the workspace (docs/TESTING_AND_OPERATIONS.md, section 1):

1. Wheel from a Volume in the **classic** silver pipeline (`environment.dependencies`, in Public Preview).
2. That a full flow does not fail in updates where no new snapshot arrives (step 3: incomplete snapshot).
3. New column end to end: Auto Loader retry in bronze and AUTO CDC target evolution.
4. Out-of-order events, deletes and late events in AUTO CDC; inserts, deletes and changes in FROM SNAPSHOT.
5. Conflicting run: reaches bronze and not silver.

To be measured with real volume (pre):

6. How long the silver pipeline takes to define its graph reading the registry, with hundreds of tables.
7. Auto Loader with file events on nested `_manifests/` and the `run_id=` partition.
8. What silver and report (`run_if ALL_DONE`) do when `has_data` is false. If there is nothing to load and
   there are errors, `register` already fails on its own.
9. Cost per for_each iteration, the start-up time of each task, against the chosen concurrency.

## Next steps

- Alert on runs in bronze without a manifest (CLEX cut off mid-write): in CDC they are already applied to
  silver; in full they are not, because there is no manifest declaring them complete.
- Refresh in silver only the tables loaded in this run (`refresh_selection` of the pipeline task), from
  register's list.
- Approved columns per table in `load_spec`, if the custodian wants to control new columns before silver.
  Today bronze and silver absorb them.
- Retention of old full snapshots in bronze (keep the last N runs).
- Incremental registry once `odp_manifests` grows large (today it is recomputed from all manifests). The
  same for `odp_rejected_runs`, which is never pruned and enters the silver plan as a `NOT IN` list.
- Grants on the bundle's schemas: decide with governance who manages them (see docs/DEPLOYMENT.md, D3).
