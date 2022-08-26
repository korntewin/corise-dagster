from random import randint

from dagster import In, Nothing, String, graph, op, OpExecutionContext, Out, Output
from dagster_dbt import dbt_cli_resource, dbt_run_op, dbt_test_op
from dagster_ucr.resources import postgres_resource, Dbt

DBT_PROJECT_PATH = "/opt/dagster/dagster_home/dagster_ucr/dbt_test_project/."


@op(
    config_schema={"table_name": String},
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def create_dbt_table(context) -> String:
    table_name = context.op_config["table_name"]
    schema_name = table_name.split(".")[0]
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    context.resources.database.execute_query(sql)
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (column_1 VARCHAR(100), column_2 VARCHAR(100), column_3 VARCHAR(100));"
    context.resources.database.execute_query(sql)
    return table_name


@op(
    required_resource_keys={"database"},
    tags={"kind": "postgres"},
)
def insert_dbt_data(context, table_name: String) -> None:
    sql = f"INSERT INTO {table_name} (column_1, column_2, column_3) VALUES ('A', 'B', 'C');"

    number_of_rows = randint(1, 10)
    for _ in range(number_of_rows):
        context.resources.database.execute_query(sql)
        context.log.info("Inserted a row")

    context.log.info("Batch inserted")


@op(
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"},
)
def dbt_run(context: OpExecutionContext, _start) -> bool:

    dbt: Dbt = context.resources.dbt
    dbt.run()
    context.log.info("Finish running dbt")
    return True


@op(
    required_resource_keys={"dbt"},
    tags={"kind": "dbt"},
    out={"success": Out(is_required=False), "failure": Out(is_required=False)},
)
def dbt_test(context: OpExecutionContext, _start: bool):

    if _start: yield Output(1, "success")
    else: yield Output(2, "failure")


@op
def success_run(context: OpExecutionContext, _start):
    ...


@op
def failure_run(context: OpExecutionContext, _start):
    ...


@graph
def dbt():

    finish = insert_dbt_data(create_dbt_table())

    success, failure = dbt_test(dbt_run(_start=finish))

    success_run(success)
    failure_run(failure)


docker = {
    "resources": {
        "database": {
            "config": {
                "host": "postgresql",
                "user": "postgres_user",
                "password": "postgres_password",
                "database": "postgres_db",
            }
        },
        "dbt": {
            "config": {
                "project_dir": DBT_PROJECT_PATH,
                "profiles_dir": DBT_PROJECT_PATH,
                "ignore_handled_error": True,
                "target": "test",
            },
        },
    },
    "ops": {"create_dbt_table": {"config": {"table_name": "analytics.dbt_table"}}},
}


dbt_docker = dbt.to_job(
    name="dbt_docker",
    config=docker,
    resource_defs={
        "database": postgres_resource,
        "dbt": dbt_cli_resource,
    },
)
