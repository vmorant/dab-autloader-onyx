"""Bundle generator: plans and validation against the databricks-bundles models."""

from __future__ import annotations

from dataclasses import replace

import pytest

from odp_framework.bundle import build_plan
from odp_framework.runtime import load_runtime_config

from .conftest import CONFIG_DIR


def _tasks(job):
    return {t["task_key"]: t for t in job["tasks"]}


def test_one_job_and_one_silver_pipeline_per_container(settings):
    plan = build_plan(CONFIG_DIR, settings)
    assert set(plan.jobs) == {"ingest_motors_sqlserver", "ingest_home_sqlserver", "ingest_motors_oracle",
                              "ingest_e2e_selftest", "e2e_e2e_selftest"}
    assert set(plan.pipelines) == {"silver_motors_sqlserver", "silver_home_sqlserver", "silver_motors_oracle",
                                   "silver_e2e_selftest"}
    assert plan.schemas == {} and plan.volumes == {}
    # readers without manage_schemas: warning that the grants go outside the bundle
    assert all("readers" in w for w in plan.warnings) and len(plan.warnings) == 2


def test_new_container_is_one_list_entry(tmp_path, settings):
    (tmp_path / "containers.yml").write_text(
        "containers:\n  - {container: claims-postgres, custodian: claims}\n")
    plan = build_plan(tmp_path, settings)
    job = plan.jobs["ingest_claims_postgres"]
    assert job["name"] == "job-datateam01-dev-claims-postgres-ingest"
    assert job["trigger"]["file_arrival"]["url"] == \
        "abfss://claims-postgres@odpdatateam01devlzsto01.dfs.core.windows.net/_manifests/"
    assert plan.pipelines["silver_claims_postgres"]["schema"] == "claims_postgres"


def test_progressive_rollout_by_environment(settings):
    pro = build_plan(CONFIG_DIR, replace(settings, env="pro")).jobs
    assert "ingest_motors_oracle" not in pro
    assert not any("e2e" in key for key in pro)


def test_e2e_job_shape(settings):
    plan = build_plan(CONFIG_DIR, settings)
    job = plan.jobs["e2e_e2e_selftest"]
    assert job["name"] == "job-datateam01-dev-e2e-selftest-e2e"
    assert "trigger" not in job and job["max_concurrent_runs"] == 1
    tasks = _tasks(job)
    assert list(tasks) == ["reset", "publish_1", "run_1", "check_1", "publish_2", "run_2", "check_2",
                           "publish_3", "run_3", "check_3", "cleanup"]
    assert "depends_on" not in tasks["reset"]
    assert tasks["reset"]["python_wheel_task"]["named_parameters"]["action"] == "cleanup"
    assert tasks["publish_1"]["depends_on"] == [{"task_key": "reset"}]
    assert tasks["publish_2"]["depends_on"] == [{"task_key": "check_1"}]
    assert tasks["run_2"]["run_job_task"] == {"job_id": "${resources.jobs.ingest_e2e_selftest.id}"}
    params = tasks["check_3"]["python_wheel_task"]["named_parameters"]
    assert tasks["check_3"]["python_wheel_task"]["entry_point"] == "odp-e2e"
    assert (params["action"], params["step"]) == ("check", "3")
    assert params["e2e_id"] == "{{job.run_id}}" and params["start_ms"] == "{{job.start_time.timestamp_ms}}"
    assert tasks["cleanup"]["depends_on"] == [{"task_key": "check_3"}] and "run_if" not in tasks["cleanup"]
    # the e2e container does not fail its ingest job: the e2e job checks the errors
    ingest_params = _tasks(plan.jobs["ingest_e2e_selftest"])["report"]["python_wheel_task"]["named_parameters"]
    assert ingest_params["fail_on_error"] == "false"


def test_job_shape(settings):
    job = build_plan(CONFIG_DIR, settings).jobs["ingest_motors_sqlserver"]
    assert job["name"] == "job-datateam01-dev-motors-sqlserver-ingest"
    assert job["max_concurrent_runs"] == 1 and job["queue"] == {"enabled": True}
    fa = job["trigger"]["file_arrival"]
    assert fa["url"] == "abfss://motors-sqlserver@odpdatateam01devlzsto01.dfs.core.windows.net/_manifests/"

    tasks = _tasks(job)
    assert list(tasks) == ["register", "has_data", "ingest", "silver", "report"]

    register = tasks["register"]
    params = register["python_wheel_task"]["named_parameters"]
    assert register["python_wheel_task"]["entry_point"] == "odp-register"
    assert load_runtime_config(params["config_json"]).container == "motors-sqlserver"
    assert params["job_run_id"] == "{{job.run_id}}"
    assert register["libraries"] == [{"whl": settings.framework_wheel}]

    assert tasks["has_data"]["condition_task"] == {
        "op": "GREATER_THAN", "left": "{{tasks.register.values.load_count}}", "right": "0"}

    ingest = tasks["ingest"]
    assert ingest["depends_on"] == [{"task_key": "has_data", "outcome": "true"}]
    fe = ingest["for_each_task"]
    assert fe["inputs"] == "{{tasks.register.values.entities_to_load}}"
    assert fe["concurrency"] == 8
    inner = fe["task"]
    assert inner["python_wheel_task"]["entry_point"] == "odp-ingest"
    assert inner["python_wheel_task"]["named_parameters"]["entity"] == "{{input}}"
    assert inner["job_cluster_key"] == "tasks" and inner["max_retries"] == 1

    silver = tasks["silver"]
    assert silver["depends_on"] == [{"task_key": "ingest"}] and silver["run_if"] == "ALL_DONE"
    assert silver["pipeline_task"]["pipeline_id"] == "${resources.pipelines.silver_motors_sqlserver.id}"
    assert tasks["report"]["run_if"] == "ALL_DONE"
    assert tasks["report"]["python_wheel_task"]["entry_point"] == "odp-report"

    (cluster,) = job["job_clusters"]
    assert cluster["new_cluster"]["autoscale"] == {"min_workers": 1, "max_workers": 4}
    assert cluster["new_cluster"]["policy_id"] == "POLICY-JOB"
    assert job["email_notifications"] == {"on_failure": ["equipo@example.com"]}


def test_e2e_container_uses_a_landing_subtree_per_deployment(settings):
    plan = build_plan(CONFIG_DIR, replace(settings, manage_schemas=True))
    ref = "${resources.schemas.bronze_schema_e2e_selftest.name}"
    job = plan.jobs["ingest_e2e_selftest"]
    cfg = load_runtime_config(_tasks(job)["register"]["python_wheel_task"]["named_parameters"]["config_json"])
    assert (cfg.data_dir, cfg.manifests_dir) == (f"data/{ref}", f"_manifests/{ref}")
    assert job["trigger"]["file_arrival"]["url"].endswith(f".net/_manifests/{ref}/")
    e2e_cfg = load_runtime_config(_tasks(plan.jobs["e2e_e2e_selftest"])["publish_1"]
                                  ["python_wheel_task"]["named_parameters"]["config_json"])
    assert e2e_cfg == cfg
    # real containers do not change
    real = load_runtime_config(_tasks(plan.jobs["ingest_motors_sqlserver"])["register"]
                               ["python_wheel_task"]["named_parameters"]["config_json"])
    assert (real.data_dir, real.manifests_dir) == ("data", "_manifests")


def test_paused_container_keeps_schemas_volume_and_pipeline(tmp_path, settings):
    (tmp_path / "containers.yml").write_text(
        "containers:\n  - {container: motors-sqlserver, custodian: motors, active: false}\n")
    plan = build_plan(tmp_path, replace(settings, manage_schemas=True))
    assert plan.jobs == {}
    assert set(plan.pipelines) == {"silver_motors_sqlserver"}
    assert len(plan.schemas) == 2 and len(plan.volumes) == 1
    assert any("active: false" in w for w in plan.warnings)


def test_container_overrides(tmp_path, settings):
    (tmp_path / "containers.yml").write_text(
        "containers:\n  - {container: motors-sqlserver, custodian: motors, "
        "ingest: {concurrency: 20, max_workers: 8}}\n")
    job = build_plan(tmp_path, settings).jobs["ingest_motors_sqlserver"]
    assert _tasks(job)["ingest"]["for_each_task"]["concurrency"] == 20
    assert job["job_clusters"][0]["new_cluster"]["autoscale"]["max_workers"] == 8


def test_silver_pipeline_shape(settings):
    silver = build_plan(CONFIG_DIR, settings).pipelines["silver_motors_sqlserver"]
    assert silver["name"] == "sdp-datateam01-dev-motors-sqlserver-silver"
    assert silver["catalog"] == "odp_datateam01_dev_silver" and silver["schema"] == "motors_sqlserver"
    assert silver["serverless"] is False and silver["edition"] == "PRO"
    assert silver["clusters"][0]["policy_id"] == "POLICY-PIPE"
    assert silver["libraries"] == [{"file": {"path": "src/pipelines/silver.py"}}]
    assert silver["environment"] == {"dependencies": [settings.framework_wheel]}
    cfg = load_runtime_config(silver["configuration"]["odp.config"])
    assert cfg.container == "motors-sqlserver" and cfg.entities == ()   # tables: from the registry


def test_managed_schemas_and_system_volume(settings):
    plan = build_plan(CONFIG_DIR, replace(settings, manage_schemas=True))
    assert plan.warnings == []
    assert plan.schemas["bronze_schema_motors_sqlserver"]["name"] == "motors_sqlserver"
    assert plan.schemas["silver_schema_motors_sqlserver"]["grants"] == [
        {"principal": "grp-datateam01-motors-dbx-reader", "privileges": ["USE_SCHEMA", "SELECT"]}]
    assert "grants" not in plan.schemas["silver_schema_motors_oracle"]
    vol = plan.volumes["system_volume_motors_sqlserver"]
    assert vol["schema_name"] == "${resources.schemas.bronze_schema_motors_sqlserver.name}"
    assert vol["name"] == "odp_system" and vol["volume_type"] == "MANAGED"
    assert plan.pipelines["silver_motors_sqlserver"]["schema"] == \
        "${resources.schemas.silver_schema_motors_sqlserver.name}"
    # mode: development prefixes the schemas: the runtime uses the deployed name
    ref = "${resources.schemas.bronze_schema_motors_sqlserver.name}"
    odp_config = load_runtime_config(plan.pipelines["silver_motors_sqlserver"]["configuration"]["odp.config"])
    assert odp_config.schema == ref
    assert odp_config.manifest_checkpoint == \
        f"/Volumes/odp_datateam01_dev_bronze/{ref}/odp_system/checkpoints/odp_manifests"
    register = _tasks(plan.jobs["ingest_motors_sqlserver"])["register"]
    assert load_runtime_config(register["python_wheel_task"]["named_parameters"]["config_json"]).schema == ref


def test_run_as_sp_gets_write_grants_on_what_the_deploy_sp_creates(settings):
    plan = build_plan(CONFIG_DIR, replace(settings, manage_schemas=True, run_as_sp="app-id-run"))
    run_as = {"principal": "app-id-run", "privileges": ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]}
    assert plan.schemas["bronze_schema_motors_sqlserver"]["grants"] == [
        {"principal": "grp-datateam01-motors-dbx-reader", "privileges": ["USE_SCHEMA", "SELECT"]}, run_as]
    assert plan.schemas["silver_schema_motors_oracle"]["grants"] == [run_as]
    assert plan.volumes["system_volume_motors_sqlserver"]["grants"] == [
        {"principal": "app-id-run", "privileges": ["READ_VOLUME", "WRITE_VOLUME"]}]


def test_wheel_version_mismatch_warns(settings):
    plan = build_plan(CONFIG_DIR, replace(settings, framework_wheel="/Volumes/x/odp_framework-0.0.9-py3-none-any.whl"))
    assert any("is not the generator version" in w for w in plan.warnings)


# ---------------------------------------- against the real databricks-bundles --
bundles = pytest.importorskip("databricks.bundles.core")


def _variables(**over):
    base = {
        "team_id": "datateam01", "env": "dev", "landing_storage_account": "odpdatateam01devlzsto01",
        "framework_wheel": "/Volumes/odp_platform_dev/libs/wheels/odp_framework-0.4.0-py3-none-any.whl",
        "pipeline_policy_id": "", "job_policy_id": "", "spark_version": "16.4.x-scala2.12",
        "notification_emails": "", "file_events": "false", "manage_schemas": "true",
        "run_as_sp": "", "deploy_sp": "", "team_group": "grp",
    }
    return {**base, **over}


def test_load_resources_builds_typed_resources():
    from databricks.bundles.core import Bundle

    from resources import load_resources

    res = load_resources(Bundle(target="dev", variables=_variables()))
    assert not res.diagnostics.has_error(), res.diagnostics
    pre = load_resources(Bundle(target="pre", variables=_variables(env="pre", run_as_sp="app-id-run")))
    assert not pre.diagnostics.has_error(), pre.diagnostics
    assert pre.volumes["system_volume_motors_sqlserver"].as_dict()["grants"] == [
        {"principal": "app-id-run", "privileges": ["READ_VOLUME", "WRITE_VOLUME"]}]
    assert set(res.jobs) == {"ingest_motors_sqlserver", "ingest_home_sqlserver", "ingest_motors_oracle",
                             "ingest_e2e_selftest", "e2e_e2e_selftest"}
    assert len(res.pipelines) == 4 and len(res.schemas) == 8 and len(res.volumes) == 4
    e2e_tasks = {t["task_key"]: t for t in res.jobs["e2e_e2e_selftest"].as_dict()["tasks"]}
    assert e2e_tasks["run_1"]["run_job_task"]["job_id"] == "${resources.jobs.ingest_e2e_selftest.id}"
    grants = res.schemas["silver_schema_motors_sqlserver"].as_dict()["grants"]
    assert grants == [{"principal": "grp-datateam01-motors-dbx-reader", "privileges": ["USE_SCHEMA", "SELECT"]}]

    job = res.jobs["ingest_motors_sqlserver"].as_dict()
    assert [t["task_key"] for t in job["tasks"]] == ["register", "has_data", "ingest", "silver", "report"]
    for_each = job["tasks"][2]["for_each_task"]
    assert for_each["task"]["python_wheel_task"]["entry_point"] == "odp-ingest"
    pipeline = res.pipelines["silver_motors_sqlserver"].as_dict()
    assert pipeline["environment"]["dependencies"][0].endswith("odp_framework-0.4.0-py3-none-any.whl")
    assert "policy_id" not in pipeline["clusters"][0]          # dev without a policy


def test_load_resources_reports_config_errors_as_diagnostics():
    from databricks.bundles.core import Bundle

    from resources import load_resources

    res = load_resources(Bundle(target="dev", variables=_variables(framework_wheel="")))
    assert res.diagnostics.has_error()
    assert not res.jobs
