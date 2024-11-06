from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import my_dbt_trino_project_project


@dbt_assets(manifest=my_dbt_trino_project_project.manifest_path)
def my_dbt_trino_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    