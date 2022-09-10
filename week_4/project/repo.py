from dagster import repository, with_resources, load_assets_from_modules
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from project.dbt_config import DBT_PROJECT_PATH
from project.resources import postgres_resource
from project.week_4 import (
    get_s3_data_docker,
    process_data_docker,
    put_redis_data_docker,
    temp,
)
from project import week_4_challenge


@repository
def repo():
    return [get_s3_data_docker, process_data_docker, put_redis_data_docker, temp]


dbt_assets = load_assets_from_dbt_project(project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROJECT_PATH)
wk4_chl_assets = load_assets_from_modules(
    [week_4_challenge],
    key_prefix="postgresql"
)


@repository
def assets_dbt():
    return with_resources(
            dbt_assets + wk4_chl_assets,
            resource_defs={
                "database": postgres_resource, 
                "dbt": dbt_cli_resource.configured({"project_dir": DBT_PROJECT_PATH, "profiles_dir":DBT_PROJECT_PATH})
            },
            resource_config_by_key={
                "database": {
                    "config": {
                        "host": "postgresql",
                        "user": "postgres_user",
                        "password": "postgres_password",
                        "database": "postgres_db",
                    }
                },
            },
        )
