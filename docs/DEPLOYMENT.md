# Deploying to Databricks

What has to exist for the bundle to work end to end, who provides it, how to check it, and how to deploy
it from dev to pro. Testing and day-to-day operations are in
[`TESTING_AND_OPERATIONS.md`](TESTING_AND_OPERATIONS.md).

Contents:

1. [What the bundle deploys and what it doesn't](#1-what-the-bundle-deploys-and-what-it-doesnt)
2. [Requirements](#2-requirements): Azure, network, Unity Catalog, identities, workspace, CLEX, CI/CD
3. [Open decisions that affect deployment](#3-open-decisions-that-affect-deployment)
4. [Repository configuration](#4-repository-configuration)
5. [Step-by-step deployment](#5-step-by-step-deployment): dev, CI, pre, pro
6. [Post-deployment verification](#6-post-deployment-verification)
7. [Common deployment errors](#7-common-deployment-errors)

---

## 1. What the bundle deploys and what it doesn't

**Created by the bundle** (`databricks bundle deploy`), for each entry in `config/containers.yml` that
applies to the environment:

| Resource | Name (pre/pro; in dev with the developer's prefix) |
|---|---|
| Ingestion job (register → ingest → silver → report), file arrival trigger on `_manifests/` | `job-datateam01-<env>-<container>-ingest` |
| Silver SDP pipeline (classic compute, PRO edition) | `sdp-datateam01-<env>-<container>-silver` |
| Schema in bronze and in silver | `odp_datateam01_<env>_bronze.<container>`, `odp_datateam01_<env>_silver.<container>` |
| Volume for Auto Loader checkpoints | `odp_datateam01_<env>_bronze.<container>.odp_system` |
| Read grants (`readers`) and run-as SP write grants on those schemas and the Volume | — |
| Job and pipeline permissions: `CAN_MANAGE` for the deploy SP, `CAN_VIEW` for the team group | — |
| dev only: job `e2e_e2e_selftest` (CLEX simulator) | `job-datateam01-dev-e2e-selftest-e2e` |

Tables (bronze, silver and the `odp_*` control tables) are not deployed: the jobs create them when they run,
from the CLEX manifests.

**Not created by the bundle** (infrastructure and governance, section 2): workspace, landing storage and
its containers, network, Access Connector, storage credential, external locations, catalogs, wheels
Volume, service principals, groups, cluster policies, CLEX connectivity and CI/CD.

---

## 2. Requirements

Each requirement states the environment from which it blocks. "Group" is the cloud/infra team that
provides the service (AXA GO Cloud Broker / platform, usually through Terraform).

### 2.1 Azure

| # | Requirement | Detail | Who | Check | Blocks |
|---|---|---|---|---|---|
| A1 | One workspace per environment | `odp-dbx-datateam01-<env>`, with Unity Catalog, VNet injection and assigned to the region's metastore | Group | `databricks metastores current -p <profile>` | dev |
| A2 | Landing storage | ADLS Gen2 (hierarchical namespace) `odpdatateam01<env>lzsto01`. One container per entry in `containers.yml`; in dev, also `e2e-selftest` | Group | `LIST 'abfss://<container>@<account>.dfs.core.windows.net/'` from a notebook or SQL warehouse | dev |
| A3 | Landing lifecycle | Deletion after 30 days in pro and 7 in dev/pre. Bronze is the permanent record | Group | Lifecycle rules on the account | pro |
| A4 | Access Connector | Managed identity for Unity Catalog with **Storage Blob Data Contributor** on the landing account | Group | Role assigned on the account | dev |
| A5 | File events (optional at first) | `Microsoft.EventGrid` resource provider registered and, for the Access Connector, also EventGrid EventSubscription Contributor and Storage Queue Data Contributor (list from the Databricks community, to be confirmed). Without file events: at most 50 jobs with a trigger per workspace and 10,000 files per watched path | Group | See B6 | pro (for scale) |

### 2.2 Network

Platform non-functional requirement: no public IPs, private endpoints only.

| # | Requirement | Detail | Who | Check | Blocks |
|---|---|---|---|---|---|
| N1 | Clusters → landing | `dfs` and `blob` private endpoints of the landing account reachable from the workspace subnets (and `queue` if file events are enabled) | Group | The `LIST` from A2, run on a workspace cluster | dev |
| N2 | CLEX → landing | The CLEX ADF lives in **another tenant**. It needs a private route to the landing account, for example a managed private endpoint from its ADF. The owner of the account approves the connection on the AXA side | Group + CLEX | A CLEX test extraction leaves files in its container | dev (real CLEX) |
| N3 | File arrival trigger with locked-down storage | The trigger is evaluated by the Databricks jobs service, with none of your clusters running. The documentation does not say how it reaches storage that only has private endpoints. **Verify in dev** with the trigger active (`presets.trigger_pause_status: UNPAUSED`); if it does not fire, ask Group for a Network Connectivity Configuration with a private endpoint to the account, or switch to a scheduled trigger | Platform | The job starts on its own when a manifest is dropped | pre |
| N4 | Deployment → workspace | If the workspace is only reachable from the AXA network, whoever deploys must be on it too: laptop on VPN in dev, and **self-hosted** Azure DevOps agents on the AXA network for CI | Platform / DevOps | `databricks current-user me` from the agent | dev (CI) |
| N5 | Agent internet egress | `uv`, `databricks-bundles` (PyPI) and the CLI installer (GitHub), or their equivalents in the internal artifact feed | DevOps | The `steps-setup.yml` step of the CI | dev (CI) |

### 2.3 Unity Catalog

| # | Requirement | Detail | Who | Check | Blocks |
|---|---|---|---|---|---|
| B1 | Storage credential | On the Access Connector (A4) | Metastore admin | `databricks storage-credentials get <name>` | dev |
| B2 | Landing external location | On `abfss://...@odpdatateam01<env>lzsto01.dfs.core.windows.net/` (per account or per container). In dev, a separate one for `e2e-selftest` is advisable, so `WRITE FILES` is granted only there | Metastore admin | `databricks external-locations get <name>` | dev |
| B3 | Catalogs | `odp_datateam01_<env>_bronze` and `odp_datateam01_<env>_silver`, with a managed location and bound to the workspace (see decision D1) | Metastore admin | `databricks catalogs get <catalog>` | dev |
| B4 | Wheels Volume | `odp_platform_<env>.libs.wheels`: write only for whoever publishes, read for whoever runs | Platform | `databricks fs ls dbfs:/Volumes/odp_platform_<env>/libs/wheels/` | dev |
| B5 | Grants | The ones in the SQL block below | Metastore admin | `SHOW GRANTS ON CATALOG ...` | dev |
| B6 | File events on the external location | Requires owner or `MANAGE` on the external location. Then set `file_events: "true"` in the target | Metastore admin | External location settings | pro (for scale) |

Grants that live outside the bundle (the bundle sets the ones on the per-container schemas and `odp_system`):

```sql
-- ===== dev: the team's developer group (they deploy and run as themselves)
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_dev_bronze TO `grp-datateam01-dbx-dev`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_dev_silver TO `grp-datateam01-dbx-dev`;
GRANT READ FILES ON EXTERNAL LOCATION `<el-landing-dev>` TO `grp-datateam01-dbx-dev`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `<el-landing-dev-e2e>` TO `grp-datateam01-dbx-dev`;
GRANT USE CATALOG ON CATALOG odp_platform_dev TO `grp-datateam01-dbx-dev`;
GRANT USE SCHEMA ON SCHEMA odp_platform_dev.libs TO `grp-datateam01-dbx-dev`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME odp_platform_dev.libs.wheels TO `grp-datateam01-dbx-dev`;
-- The CI SP in dev (deploys and runs the e2e): the same grants as the group.

-- ===== pre / pro (replace <env>)
-- Deploy SP: creates schemas and Volumes, publishes the wheel
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_<env>_bronze TO `<app-id-deploy>`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG odp_datateam01_<env>_silver TO `<app-id-deploy>`;
GRANT USE CATALOG ON CATALOG odp_platform_<env> TO `<app-id-deploy>`;
GRANT USE SCHEMA ON SCHEMA odp_platform_<env>.libs TO `<app-id-deploy>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME odp_platform_<env>.libs.wheels TO `<app-id-deploy>`;
-- Run-as SP: the bundle grants it write access on the schemas and on odp_system; the rest goes here
GRANT USE CATALOG ON CATALOG odp_datateam01_<env>_bronze TO `<app-id-run>`;
GRANT USE CATALOG ON CATALOG odp_datateam01_<env>_silver TO `<app-id-run>`;
GRANT READ FILES ON EXTERNAL LOCATION `<el-landing-<env>>` TO `<app-id-run>`;
GRANT USE CATALOG ON CATALOG odp_platform_<env> TO `<app-id-run>`;
GRANT USE SCHEMA ON SCHEMA odp_platform_<env>.libs TO `<app-id-run>`;
GRANT READ VOLUME ON VOLUME odp_platform_<env>.libs.wheels TO `<app-id-run>`;
```

The file arrival trigger needs read access on the path it watches and `CAN MANAGE` on the job; the
documentation does not say which identity it checks with. With the `run_as_sp` as job owner, the
`READ FILES` above should be enough, but it has to be verified in dev or pre (N3).

### 2.4 Identities

| # | Requirement | Detail | Who | Check | Blocks |
|---|---|---|---|---|---|
| C1 | Service principals per environment (pre/pro) | `svc-dbks-datateam01-<env>-deploy` (CI) and `svc-dbks-datateam01-<env>-run` (execution), in Entra, added to the Databricks account and to the workspace | Group + account admin | `databricks service-principals list --filter "displayName eq '<name>'"` | pre |
| C2 | Role on the run-as SP | The deploy SP needs **Service Principal User** on the run-as SP; without it, the deploy cannot set `run_as` | Workspace admin | First `bundle deploy -t pre` | pre |
| C3 | CI SP in dev | The one that deploys dev and runs the e2e in CI (it can be the dev `-deploy` SP), with the dev grants | Group | dev stage of the CI | dev (CI) |
| C4 | Workload identity federation | Federated credential for each deploy SP, for its Azure DevOps service connection: no secrets | Group / DevOps | The service connection verifies | dev (CI) |
| C5 | Groups (account level) | `grp-datateam01-dbx-dev` (developers), `grp-datateam01-dbx-reader` (`team_group`, `CAN_VIEW`), and the `readers` of each container (`grp-datateam01-<custodian>-dbx-reader`), assigned to the workspace | Group / account admin | `databricks groups list` | dev |
| C6 | Use of the cluster policies | `CAN_USE` on both policies for the run-as SP (pre/pro) and for the developer group and the CI SP (dev) | Workspace admin | Permissions on each policy | dev |

### 2.5 Workspace

| # | Requirement | Detail | Who | Check | Blocks |
|---|---|---|---|---|---|
| W1 | Job cluster policy | Dedicated access mode (`SINGLE_USER`), DBR 16.4 LTS allowed, node types, mandatory FinOps tags. The bundle sets `custom_tags` from `platform.yml`: the policy must not pin different values | Platform | `databricks cluster-policies get <id>` | pre |
| W2 | Pipeline cluster policy | `cluster_type: dlt`, node types and tags. The access mode is managed by the pipeline (standard in UC) | Platform | Same | pre |
| W3 | `environment.dependencies` in classic pipelines | The wheel is installed in the silver pipeline through this feature, in Public Preview. If the workspace lists the preview on the *Previews* page, it must be enabled | Workspace admin | `databricks bundle run silver_<container> --validate-only` (with some table registered) or step 1 of the e2e | dev |
| W4 | DBR 16.4 LTS | Bundle variable `spark_version` | Platform | When creating a cluster | dev |
| W5 | Serverless | **Not** needed by the bundle. It is needed for non-owners to read silver from a dedicated cluster (see D5) | — | — | — |
| W6 | System tables | `system.billing` and `system.lakeflow` for cost and timings (optional, recommended) | Account admin | `SELECT 1 FROM system.billing.usage LIMIT 1` | pro |

In dev, while there are no policies, `pipeline_policy_id` and `job_policy_id` can stay empty. In pre and
pro they cannot.

### 2.6 CLEX

| # | Requirement | Detail | Who |
|---|---|---|---|
| X1 | Contract 2.0 | Parquet in `data/{source_path}/run_id={run_id}/`, manifest in `_manifests/{source_path}/{run_id}.json` **last**, `run_id` = `YYYYMMDDTHHMMSSZ_<id>`, complete `load_spec`. See "CLEX contract" in the README, `samples/` and the validator (`odp_framework.manifest.validate_manifest`) | CLEX team |
| X2 | One instance per container | Each CLEX writes only to its custodian's container (A2, N2) | CLEX team + Group |
| X3 | Configuration through PRs | The list of tables and their `load_spec` live in the CLEX repo and are approved by PR; the bundle does not list tables | CLEX team + custodian |

### 2.7 CI/CD (Azure DevOps)

The pipeline is in `ci/`. It runs in Azure DevOps from an ADO repo, or from this GitHub repo by connecting
ADO to it with a GitHub service connection.

| # | Requirement | Detail | Who |
|---|---|---|---|
| D-1 | ARM service connections | `sc-odp-datateam01-dev`, `-pre`, `-pro`, with workload identity federation (C4) | DevOps |
| D-2 | Variable groups | `odp-datateam01-<env>` with `DATABRICKS_HOST` | DevOps |
| D-3 | Environments | `odp-datateam01-pre` and `odp-datateam01-pro` with manual approval (pro: CAB) | DevOps |
| D-4 | Agents | Self-hosted pool on the AXA network (N4) with egress to PyPI/GitHub or to the internal feed (N5). Change `pool` in `ci/azure-pipelines.yml` | DevOps |

---

## 3. Open decisions that affect deployment

| # | Topic | Status | What to do before pre |
|---|---|---|---|
| D1 | Catalog design | The bundle implements **one catalog per layer** (`odp_datateam01_<env>_bronze` / `_silver`) and **one schema per container**. The platform's earlier decision was one catalog per workspace (`odp_datateam01_<env>`) with `bronze`/`silver`/`gold` schemas | Settle it. Going back to the earlier design is not just a change in `platform.yml`: the runtime assumes the same schema name in bronze and silver, so the code would have to change |
| D2 | Landing account | Per-team naming (`odpdatateam01<env>lzsto01`); the wiki also describes a central landing | If it is central, change `landing_storage_account`, or `storage_account` per container |
| D3 | Who manages the schema grants | Grants on bundle resources are declarative: a deploy will probably revoke grants given by hand or by governance (not confirmed in the documentation) | Agree it with governance. Either everything in the bundle (`readers`) or everything outside it (`manage_schemas: "false"`). Review `databricks bundle plan -t pre` |
| D4 | Trigger with locked-down storage | See N3 | Verify it in dev; if it does not work, NCC or a scheduled trigger |
| D5 | Consumers reading silver | Silver tables are SDP streaming tables. Non-owners can only read them from a SQL warehouse, a standard cluster, or a dedicated cluster with DBR 15.4+ **in a workspace with serverless** | Gold jobs (migrated Pegasus) and analysts on standard clusters or a SQL warehouse while there is no serverless |

---

## 4. Repository configuration

### `databricks.yml`

| Value | dev | pre | pro | Where it comes from |
|---|---|---|---|---|
| `workspace.host` | ✔ | ✔ | ✔ | URL of `odp-dbx-datateam01-<env>` (A1) |
| `landing_storage_account` | ✔ | ✔ | ✔ | A2 (default `odpdatateam01<env>lzsto01`) |
| `framework_wheel` | ✔ | ✔ | ✔ | `/Volumes/odp_platform_<env>/libs/wheels/odp_framework-<version>-py3-none-any.whl` (B4) |
| `pipeline_policy_id`, `job_policy_id` | optional | ✔ | ✔ | W1, W2 |
| `run_as_sp`, `deploy_sp` | — | ✔ | ✔ | Application ids from C1 |
| `team_group` | — | ✔ | ✔ | C5 |
| `notification_emails` | — | optional | ✔ | Team mailbox |
| `file_events` | `"false"` | `"false"` → `"true"` | `"true"` | Once B6 is done |

`team_id` (`datateam01`) and `manage_schemas` (`"true"`) are the same in all three environments.

### `config/containers.yml`

One entry per real landing container (custodian × CLEX instance), with its `readers`. For a progressive
rollout, `environments: [dev]`, then `[dev, pre]`, and remove the field when it reaches pro. The
`e2e-selftest` container only exists in dev (it is forbidden in pro).

### `config/platform.yml`

Naming (see D1), mandatory FinOps tags (`global-Team`, `global-Source`, `global-Environment`,
`global-DataClass`, `Project`; the bundle adds `resource-type`), concurrency, cluster sizes and trigger.

---

## 5. Step-by-step deployment

### Phase 1 — dev, by hand (one developer, on the AXA network)

Requirements: A1, A2, A4, N1, B1–B5, C5, C6, W3, W4.

```bash
git clone https://github.com/vmorant/dab-autloader-onyx && cd dab-autloader-onyx
# databricks.yml: dev host (and landing_storage_account if it differs)

uv sync                                     # .venv with databricks-bundles (used by the CLI)
uv run pytest -q                            # 133 tests

databricks auth login --host https://<workspace-dev> --profile odp-dev
databricks bundle validate -t dev -p odp-dev
scripts/publish_wheel.sh dev odp-dev        # immutable wheel in the dev Volume
databricks bundle deploy  -t dev -p odp-dev
databricks bundle summary -t dev -p odp-dev
```

The CLI and `databricks-bundles` must be the same version (1.21; `databricks_cli_version` in
`databricks.yml`).

In dev (`mode: development`) each developer gets their own schemas (`dev_<user>_<container>`), and jobs and
pipelines carry the `[dev <user>]` prefix and have their **triggers paused**. The landing is shared.

Then: verification (section 6) and the e2e test:

```bash
databricks bundle run e2e_e2e_selftest -t dev -p odp-dev
```

Details of the e2e and of the test with real CLEX in [`TESTING_AND_OPERATIONS.md`](TESTING_AND_OPERATIONS.md).

### Phase 2 — CI

Requirements: C3, C4, N4, N5, D-1 to D-4.

1. Set `pool` in `ci/azure-pipelines.yml` (self-hosted agents).
2. Create the pipeline in Azure DevOps pointing at `ci/azure-pipelines.yml`.
3. First run on `main`: tests, then dev (wheel, deploy and e2e with the CI SP).

The CI deploys **without `--auto-approve`**: if a change would delete or recreate a schema, a Volume or a
pipeline (which takes its silver tables with it), the CLI stops and the pipeline fails.

### Phase 3 — pre

Requirements: all of section 2 for pre, except B6. Decisions D1–D3 settled.

1. pre values in `databricks.yml` (section 4) through a PR.
2. Merge to `main`: the CI deploys dev with the e2e and, after approval, pre.
3. Verification (section 6) and tests with CLEX pre: volume, timings, cost, trigger with locked-down storage
   (N3) and file events (B6), in [`TESTING_AND_OPERATIONS.md`](TESTING_AND_OPERATIONS.md).

`mode: production`: no prefixes (schemas named `<container>`), **triggers active**, jobs and pipelines run
as `run_as_sp`.

### Phase 4 — pro (go-live)

Checklist:

- [ ] All of section 2 for pro, including A3 (lifecycle) and B6 (file events), with `file_events: "true"`.
- [ ] `notification_emails` set to the team mailbox.
- [ ] CLEX pro writing to its container (N2) and validated in pre with the same contract.
- [ ] D3 settled: custodian read grants either in `readers` or managed outside the bundle, not both.
- [ ] Silver consumers on standard clusters or a SQL warehouse (D5).
- [ ] CAB approval in the `odp-datateam01-pro` environment.
- [ ] Rollback ready: the previous wheel is still in the Volume and reverting the PR is enough.
- [ ] Monitoring: queries on `odp_run_findings` and alerts on job failure (notifications).

---

## 6. Post-deployment verification

| What | Where | Correct if |
|---|---|---|
| Resources | `databricks bundle summary -t <env>` | One job and one pipeline per container; schemas and Volume if `manage_schemas` |
| Job | Jobs → `job-datateam01-<env>-<container>-ingest` | Tasks `register → has_data → ingest (for each) → silver → report`; file arrival trigger on `…/_manifests/` (paused in dev); *Run as* = run-as SP in pre/pro; job cluster with the policy |
| Parameters | `register` task → `config_json` | `schema` is the real name (in dev `dev_<user>_<container>`), not a `${…}` reference |
| Pipeline | Pipelines → `sdp-datateam01-<env>-<container>-silver` | Catalog `…_silver`, the container's schema, classic compute with the policy, dependency on the pinned wheel version |
| Unity Catalog | Catalog Explorer | Schemas in bronze and silver with the expected grants; Volume `odp_system` |
| Permissions | Job and pipeline → Permissions | `CAN_MANAGE` for the deploy SP and `CAN_VIEW` for the team group (pre/pro) |
| Startup | `databricks bundle run ingest_<container> -t <env> --only register` | Finishes successfully; with no manifests there is nothing to load |
| End to end | dev: e2e; pre: a real CLEX delivery | See [`TESTING_AND_OPERATIONS.md`](TESTING_AND_OPERATIONS.md) |

---

## 7. Common deployment errors

| Symptom | Likely cause | Fix |
|---|---|---|
| `validate` fails while loading the Python resources | No `.venv` with `databricks-bundles` | `uv sync` at the root of the repo |
| `validate` lists errors in `containers.yml` | Invalid entry (name, custodian, `environments`, e2e in pro…) | Fix the entry; all errors are reported together |
| `validate` asks for another CLI version | CLI older than `databricks_cli_version` | Upgrade the CLI (same version as `databricks-bundles`) |
| Deploy has no permission to create schemas | Missing `USE CATALOG` / `CREATE SCHEMA` (B5) | Grants from the SQL block |
| Deploy cannot set `run_as` | Missing Service Principal User role (C2) | Assign it on the run-as SP |
| The job cannot create its cluster | Missing `CAN_USE` on the policy (C6) or the policy rejects the configuration | Permission or policy adjustment (W1, W2) |
| The task cannot find the wheel | Not published, different version in `framework_wheel`, or missing `READ VOLUME` | `scripts/publish_wheel.sh <env>`, check the path, B4 grants |
| The silver pipeline fails installing dependencies | `environment.dependencies` not available in classic pipelines, or no `READ VOLUME` | W3 and B4 |
| `register` fails reading `abfss://…` | Missing external location, `READ FILES` or network route (B2, B5, N1) | The `LIST` from A2 on a workspace cluster |
| The trigger does not fire | Paused (dev), no read access on the path, 50-trigger limit without file events, or locked-down storage (N3) | Check in that order |
| The deploy asks to confirm a deletion | A container was removed, or something changed that recreates a schema, Volume or pipeline | Review it: the pipeline takes its silver tables with it, and the schemas their data |
| The CI cannot reach the workspace | Agent outside the AXA network (N4) | Self-hosted pool |
| The CI fails to authenticate | Federated credential, or SP not added to the workspace (C1, C4) | Check the service connection and the SP assignment |
