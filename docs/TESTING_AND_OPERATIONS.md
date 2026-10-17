# Testing and operations

How to test the bundle in each environment and how to operate it. Requirements and deployment are in
[`DEPLOYMENT.md`](DEPLOYMENT.md).

| Level | Where | What it tests | When |
|---|---|---|---|
| 1. Tests | local and CI | Framework logic, guardrails, bundle generation and consistency of the e2e scenario (133 tests, no Spark) | Every commit |
| 2. `validate` | against the workspace | `containers.yml`, variables, references, `run_as`, permissions | Every deploy |
| 3. **e2e** | dev (by hand and in CI) | The full flow with a simulated CLEX: Auto Loader, AUTO CDC, snapshots, guardrails, wheel in the classic pipeline | Every wheel or bundle change |
| 4. Real CLEX | dev | Real CLEX contract, cross-tenant writes, file arrival trigger | Onboarding of each container or connector |
| 5. Volume | pre | Timings, cost, file events, concurrency | Before pro |

Levels 1 and 2 are covered in phase 1 of [`DEPLOYMENT.md`](DEPLOYMENT.md#5-step-by-step-deployment).

---

## 1. End-to-end test with the CLEX simulator

```bash
databricks bundle run e2e_e2e_selftest -t dev -p odp-dev
```

The `e2e_e2e_selftest` job plays CLEX on the `e2e-selftest` container and runs the real ingestion job
(`run_job_task`) after each step:

```
reset → publish_1 → run_1 → check_1 → publish_2 → run_2 → check_2 → publish_3 → run_3 → check_3 → cleanup
```

| Step | What the simulator delivers | What `check_N` verifies |
|---|---|---|
| 1. Initial load | `customers` CDC (5 inserts), `policies` full (3 rows) | Registry with 2 ACTIVE tables, load log, bronze and silver with 5 and 3 rows, no errors |
| 2. Changes | CDC: update, delete, two out-of-order updates of the same PK in the same file, insert and a **new column**. Full: insert, delete and change | Delete applied, highest sequence wins, new column in bronze and in silver, snapshot applied, no errors |
| 3. Guardrails | `customers` manifest with changed keys (SPEC_CONFLICT) in its own folder; late CDC events; **incomplete** full snapshot (the manifest says 4 rows, 3 arrive); new table `agents` | SPEC_CONFLICT and ROW_COUNT_MISMATCH findings; the conflicting run is in bronze and **not** in silver; late events neither overwrite nor resurrect a deleted row; the incomplete snapshot is not applied; `agents` registered and in silver |

Details:

- **Isolation.** Each deployment uses its own subtree of the container (`data/<schema>/…` and
  `_manifests/<schema>/…`; in dev the schema carries the developer's prefix). A developer's deployment does
  not see the CI's manifests or another developer's. Each run also creates its own tables
  (`e2e_<job_run_id>_<table>`).
- **Result**: each `check_N` prints one line per check (`OK`/`FAIL`, value and expected) and fails if any
  does not match. `bundle run` returns a non-zero exit code.
- **If it fails**, `cleanup` does not run and the tables are left for investigation (queries in section 3).
  The next run starts with `reset`, which deletes everything e2e in that deployment (`e2e_*` tables, their
  control rows, checkpoints and landing): it can be repeated without manual cleanup.
- If `DROP` of the silver tables were not allowed from the job cluster, `reset` and `cleanup` report it as a
  warning: those tables become inactive once they leave the registry and are dropped by hand.
- **Duration**: 45–60 minutes, almost all of it cluster start-up (e2e job cluster, ingestion job cluster and
  pipeline cluster, three times).
- The e2e container has `fail_on_error: false`: step 3 causes errors on purpose, and it is the e2e, not the
  ingestion job, that decides whether the test passes.

What it validates from the PoC plan:

| PoC open item | Where the e2e verifies it |
|---|---|
| Wheel from a Volume in the **classic** pipeline (`environment.dependencies`, Public Preview) | `run_1`: if it does not install, the `silver` task fails in step 1 |
| Full flow with no new snapshot in an update | Step 3: incomplete snapshot, `select_next_run` returns `None` |
| New column: Auto Loader in bronze and AUTO CDC target evolution | Step 2 |
| Out-of-order events, deletes and tombstones in AUTO CDC | Steps 2 and 3 |
| Rejected runs kept out of silver | Step 3 |

Shortcut for the first item without waiting for the full e2e, once some table is registered:
`databricks bundle run silver_e2e_selftest -t dev --validate-only` resolves the graph (installs the wheel and
reads the registry) without processing data.

## 2. Test with real CLEX (dev)

1. **Contract.** Give the CLEX team the "CLEX contract" section of the README, `samples/` and the
   validator. They can validate their manifests before writing to the landing:

   ```bash
   uv run python -c "import json,sys; from odp_framework.manifest import validate_manifest; \
   print(validate_manifest(json.load(open(sys.argv[1]))) or 'OK')" manifest.json
   ```

2. **Delivery.** CLEX dev extracts into `motors-sqlserver` in the dev landing: parquet in
   `data/{source_path}/run_id={run_id}/` and the manifest in `_manifests/{source_path}/{run_id}.json`, last.
3. **Run.** By hand:

   ```bash
   databricks bundle run ingest_motors_sqlserver -t dev -p odp-dev
   databricks bundle run ingest_motors_sqlserver -t dev -p odp-dev --only register   # register only
   ```

   Or with the trigger: uncomment `presets.trigger_pause_status: UNPAUSED` in the dev target and redeploy.
   Do not use `--python-named-params`: it would replace `config_json` in every task.
4. **Scenarios** to trigger from CLEX:

| Scenario | How | Expected result |
|---|---|---|
| CDC initial load | First extraction, everything as inserts | Bronze = manifest rows; silver = replica |
| Incremental with deletes | Window with I/U/D | Silver reflects the deletes |
| Full | Full extraction of a full table | Silver = snapshot; rows that disappear are deleted |
| New column | `ALTER TABLE ADD` at the source | One run with an internal retry in ingest; column in bronze and in silver |
| New table | Add a table to the CLEX config (their PR) | Registers by itself (`auto`); in `home-sqlserver` (`manual`) it stays `PENDING_APPROVAL` until added to `approved_entities` |
| Keys change | Change the keys of an already registered table | `SPEC_CONFLICT`, the job alerts; the run reaches bronze, not silver |
| CLEX cut off midway | Parquet without a manifest | Bronze loads them the next time the table is loaded; CDC applies them to silver, full does not. No alert today (next steps in the README) |
| CLEX retry | Repeat an extraction | New `run_id`; in CDC the sequence avoids duplicates in silver |

## 3. Monitoring queries

`<sch>` is the container's schema: `dev_<user>_motors_sqlserver` in dev and `motors_sqlserver` in pre and pro.

```sql
-- Findings in the last 24 h
SELECT report_ts, phase, severity, code, entity, run_id, detail
FROM odp_datateam01_dev_bronze.<sch>.odp_run_findings
WHERE report_ts >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY report_ts DESC;

-- Table registry
SELECT entity, load_type, status, spec_version, first_run_id, last_run_id, last_manifest_at
FROM odp_datateam01_dev_bronze.<sch>.odp_entities ORDER BY entity;

-- Failed loads
SELECT entity, job_run_id, started_at, attempts, error
FROM odp_datateam01_dev_bronze.<sch>.odp_load_log
WHERE status = 'FAILED' ORDER BY started_at DESC;

-- Manifest versus bronze, per run, for one table
SELECT m.run_id, m.row_count AS manifest, count(b._run_id) AS bronze
FROM odp_datateam01_dev_bronze.<sch>.odp_manifests m
LEFT JOIN odp_datateam01_dev_bronze.<sch>.gdwr_customers b ON b._run_id = m.run_id
WHERE m.load_spec.target_table = 'gdwr_customers'
GROUP BY m.run_id, m.row_count ORDER BY m.run_id DESC;

-- Runs that silver does not apply
SELECT * FROM odp_datateam01_dev_bronze.<sch>.odp_rejected_runs;
```

Cost and timings per container (pre/pro): `system.billing.usage` filtered by `custom_tags['odp-container']`
and `system.lakeflow.job_run_timeline` by `job_id`. Silver graph initialisation: the pipeline event log,
`update_progress` events (from `INITIALIZING` to `RUNNING`).

## 4. Testing in pre

With the bundle deployed to pre by the CI (phase 3 of [`DEPLOYMENT.md`](DEPLOYMENT.md)) and CLEX pre
delivering real volume:

- Silver graph initialisation time with all the tables of the container.
- File arrival trigger with locked-down storage (requirement N3) and, afterwards, `file_events: "true"` in
  the target once they are enabled on the external location: the trigger and Auto Loader must keep
  detecting files.
- for_each concurrency (`ingest.concurrency`) and cluster size against duration and cost.
- What `silver` and `report` do when `has_data` is false (run with no new data).
- Progressive rollout: a new container goes in with `environments: [dev]`, then `[dev, pre]` and, once the
  field is removed, everywhere.

## 5. Operations

| Need | How |
|---|---|
| New framework version | Bump the version → PR → CI: tests, wheel, dev + e2e, pre, pro |
| Rollback | `git revert` of the PR. The previous wheel is still in the Volume (immutable) and the CI redeploys config and version |
| Rebuild a silver table | `databricks bundle run silver_<container> -t <env> --full-refresh <table>`. CDC rereads all of bronze; full starts from the most recent complete snapshot. It is also the fix if a run is rejected after it was applied, or if a CDC event arrives more than 7 days after a delete |
| Retire a table | `disabled_entities` in `containers.yml` (PR). It stops loading; its data stays |
| Pause a container | `active: false`: the job is removed; schemas, Volume, pipeline and data are kept |
| Permanently remove a container | Delete its entry. The deploy deletes the pipeline (and silver tables), job, Volume and schemas: it asks for confirmation and the CI stops |
| Reload bronze for a table | Avoid it: bronze is the only copy once the landing expires (30 days in pro). If there are duplicates because a checkpoint was lost, remove them with `_source_file` |
