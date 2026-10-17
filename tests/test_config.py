from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from odp_framework.config import ConfigError, DeploySettings, load_containers
from odp_framework.runtime import load_runtime_config

from .conftest import CONFIG_DIR, CUSTOMERS


def _write_containers(root: Path, body: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "containers.yml").write_text(body, encoding="utf-8")
    return root


def _by_name(specs):
    return {s.container: s for s in specs}


def test_repo_config_resolves(settings):
    specs = _by_name(load_containers(CONFIG_DIR, settings))
    assert sorted(specs) == ["e2e-selftest", "home-sqlserver", "motors-oracle", "motors-sqlserver"]
    assert specs["e2e-selftest"].e2e and not specs["motors-sqlserver"].e2e
    assert specs["e2e-selftest"].report["fail_on_error"] is False

    rt = specs["motors-sqlserver"].runtime
    assert rt.bronze_catalog == "odp_datateam01_dev_bronze"
    assert rt.silver_catalog == "odp_datateam01_dev_silver"
    assert rt.schema == "motors_sqlserver"
    assert rt.landing_root == "abfss://motors-sqlserver@odpdatateam01devlzsto01.dfs.core.windows.net"
    assert rt.manifests_root.endswith(".net/_manifests/")
    assert rt.registration == "auto" and rt.entities == ()
    assert rt.registry_fqn == "odp_datateam01_dev_bronze.motors_sqlserver.odp_entities"
    assert rt.manifest_checkpoint == \
        "/Volumes/odp_datateam01_dev_bronze/motors_sqlserver/odp_system/checkpoints/odp_manifests"
    assert rt.data_path(CUSTOMERS).endswith("/data/srv-motors-01/GDWR/dbo/customers/")
    assert specs["motors-sqlserver"].readers == ("grp-datateam01-motors-dbx-reader",)

    home = specs["home-sqlserver"].runtime
    assert home.registration == "manual" and home.approved_entities == ("hbas_contracts",)
    assert specs["motors-oracle"].ingest["concurrency"] == 4
    assert specs["motors-sqlserver"].tags["global-Environment"] == "DEV"


def test_environments_allow_progressive_rollout(settings):
    assert "motors-oracle" in _by_name(load_containers(CONFIG_DIR, settings))
    pro = _by_name(load_containers(CONFIG_DIR, replace(settings, env="pro")))
    assert sorted(pro) == ["home-sqlserver", "motors-sqlserver"]


def test_storage_account_override(tmp_path, settings):
    root = _write_containers(tmp_path, """
containers:
  - {container: manual-uploads, custodian: motors, storage_account: odpdatateam01devmansto01}
""")
    (spec,) = load_containers(root, settings)
    assert spec.runtime.landing_root == "abfss://manual-uploads@odpdatateam01devmansto01.dfs.core.windows.net"


def test_runtime_json_roundtrip_excludes_entities(settings):
    rt = load_containers(CONFIG_DIR, settings)[0].runtime
    with_entities = rt.with_entities([CUSTOMERS])
    assert "gdwr_customers" not in with_entities.to_json()
    assert load_runtime_config(with_entities.to_json()) == rt


def test_runtime_rejects_other_contract(settings):
    rt = load_containers(CONFIG_DIR, settings)[0].runtime
    with pytest.raises(ValueError, match="Contrato"):
        load_runtime_config(rt.to_json().replace(f'"contract":{rt.contract}', '"contract":0'))
    with pytest.raises(ValueError, match="odp.config"):
        load_runtime_config(None)


def test_inactive_container_is_paused_not_removed(tmp_path, settings):
    root = _write_containers(tmp_path, """
containers:
  - {container: motors-sqlserver, custodian: motors}
  - {container: home-sqlserver, custodian: home, active: false}
""")
    specs = _by_name(load_containers(root, settings))
    assert specs["motors-sqlserver"].active and not specs["home-sqlserver"].active


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ("{container: motors-sqlserver, custodian: motors, registration: magic}", "registration"),
        ("{container: motors-sqlserver, custodian: motors, approved_entities: [Bad.Name]}", "approved_entities"),
        ("{container: motors-sqlserver, custodian: motors, approved_entities: [a_b], disabled_entities: [a_b]}",
         "aprobadas y deshabilitadas"),
        ("{container: motors-sqlserver, custodian: motors, entities: []}", "claves desconocidas"),
        ("{container: motors-sqlserver}", "custodian"),
        ("{container: Motors_SQL, custodian: motors}", "nombre de contenedor"),
        ("{container: motors-sqlserver, custodian: motors, ingest: {concurrency: 500}}", "concurrency"),
        ("{container: motors-sqlserver, custodian: motors, environments: [qa]}", "environments"),
        ("{container: motors-sqlserver, custodian: motors, storage_account: Bad-Account}", "storage_account"),
        ("{container: motors-sqlserver, custodian: motors, readers: grp-x}", "readers"),
        ("{container: e2e-x, custodian: p, e2e: true}", "sin pro"),
        ("{container: e2e-x, custodian: p, e2e: true, environments: [dev, pro]}", "sin pro"),
        ("{container: e2e-x, custodian: p, e2e: true, environments: [dev], registration: manual}",
         "registration auto"),
        ("{container: e2e-x, custodian: p, e2e: 'yes', environments: [dev]}", "true o false"),
    ],
)
def test_container_validation_errors(tmp_path, settings, entry, expected):
    root = _write_containers(tmp_path, f"containers:\n  - {entry}\n")
    with pytest.raises(ConfigError) as exc:
        load_containers(root, settings)
    assert any(expected in e for e in exc.value.errors), exc.value.errors


def test_entries_for_other_environments_are_still_validated(tmp_path, settings):
    root = _write_containers(tmp_path, """
containers:
  - {container: motors-sqlserver, custodian: motors}
  - {container: motors-oracle, environments: [pro]}
""")
    with pytest.raises(ConfigError, match="custodian"):
        load_containers(root, settings)                 # target dev


def test_duplicates_and_aggregated_errors(tmp_path, settings):
    root = _write_containers(tmp_path, """
containers:
  - {container: motors-sqlserver, custodian: motors}
  - {container: motors-sqlserver, custodian: motors}
  - {container: home-sqlserver, registration: x}
""")
    with pytest.raises(ConfigError) as exc:
        load_containers(root, settings)
    joined = "\n".join(exc.value.errors)
    assert "repetido" in joined and "registration" in joined and "custodian" in joined


def test_missing_list(tmp_path, settings):
    with pytest.raises(ConfigError, match="containers"):
        load_containers(_write_containers(tmp_path, "otra_cosa: 1\n"), settings)


def test_settings_from_bundle_variables():
    s = DeploySettings.from_variables({
        "team_id": "datateam01", "env": "pre", "landing_storage_account": "acc",
        "framework_wheel": "/Volumes/x/odp_framework-0.4.0-py3-none-any.whl",
        "notification_emails": "a@x.com, b@x.com", "file_events": "true", "manage_schemas": False,
        "run_as_sp": " 1111-2222 ",
    })
    assert s.notification_emails == ("a@x.com", "b@x.com") and s.run_as_sp == "1111-2222"
    assert s.file_events is True and s.manage_schemas is False
    with pytest.raises(ConfigError, match="framework_wheel"):
        DeploySettings.from_variables({"team_id": "t", "env": "dev", "landing_storage_account": "a"})
