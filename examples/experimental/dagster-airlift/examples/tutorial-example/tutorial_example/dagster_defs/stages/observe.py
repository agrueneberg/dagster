import os
from pathlib import Path
from typing import Sequence, Union

from dagster import AssetExecutionContext, AssetsDefinition, AssetSpec, Definitions
from dagster_airlift.core import (
    AirflowInstance,
    BasicAuthBackend,
    assets_with_task_mappings,
    build_defs_from_airflow_instance,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


def dbt_project_path() -> Path:
    env_val = os.getenv("TUTORIAL_DBT_PROJECT_DIR")
    assert env_val, "TUTORIAL_DBT_PROJECT_DIR must be set"
    return Path(env_val)


def rebuild_customer_list_assets() -> Sequence[Union[AssetsDefinition, AssetSpec]]:
    @dbt_assets(
        manifest=dbt_project_path() / "target" / "manifest.json",
        project=DbtProject(dbt_project_path()),
    )
    def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return assets_with_task_mappings(
        dag_id="rebuild_customers_list",
        task_mappings={
            "load_raw_customers": [AssetSpec(key=["raw_data", "raw_customers"])],
            "build_dbt_models": [dbt_project_assets],
            "export_customers": [AssetSpec(key="customers_csv", deps=["customers"])],
        },
    )


defs = build_defs_from_airflow_instance(
    airflow_instance=AirflowInstance(
        auth_backend=BasicAuthBackend(
            webserver_url="http://localhost:8080",
            username="admin",
            password="admin",
        ),
        name="airflow_instance_one",
    ),
    defs=Definitions(
        assets=rebuild_customer_list_assets(),
        resources={"dbt": DbtCliResource(project_dir=dbt_project_path())},
    ),
)
