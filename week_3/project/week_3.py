from typing import Iterator, List
import copy

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
    OpExecutionContext,
    SensorEvaluationContext
)
from project.resources import mock_s3_resource, redis_resource, s3_resource, S3, Redis
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str}
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3: S3 = context.resources.s3
    return [Stock.from_list(s) for s in s3.get_data(context.op_config["s3_key"])]


@op
def process_data(stocks: List[Stock]) -> Aggregation:
    highest = max(stocks, key = lambda s: s.high)
    return Aggregation(
        date=highest.date,
        high=highest.high
    )


@op(
    required_resource_keys={"redis"},
)
def put_redis_data(context: OpExecutionContext, agg: Aggregation) -> None:
    redis: Redis = context.resources.redis

    key, value = agg.date.strftime("%Y-%m-%d"), agg.high
    redis.put_data(name=key, value=value)
    context.log.info(
        "Finish putting redis data with "
        f"key: {key}, value: {value}"
    )


@graph
def week_3_pipeline():
    stocks = get_s3_data()
    agg = process_data(stocks)
    put_redis_data(agg)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    copy_docker = copy.deepcopy(docker)
    copy_docker["ops"]["get_s3_data"]["config"]["s3_key"] = f"prefix/stock_{partition_key}.csv"
    return copy_docker


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


@sensor(
    job=docker_week_3_pipeline,
)
def docker_week_3_sensor(context: SensorEvaluationContext):
    bucket = docker["resources"]["s3"]["config"]["bucket"]
    endpoint = docker["resources"]["s3"]["config"]["endpoint_url"]

    since_key = context.cursor
    new_keys = get_s3_keys(
        bucket=bucket,
        prefix="", 
        endpoint_url=endpoint,
        since_key=since_key, 
    )

    if new_keys:
        for key in new_keys:
            run_config = copy.deepcopy(docker)
            run_config["ops"]["get_s3_data"]["config"]["s3_key"] = key
            yield RunRequest(run_key=key, run_config=run_config)
        context.update_cursor(new_keys[-1])
    else:
        yield SkipReason("No new s3 files found in bucket.")