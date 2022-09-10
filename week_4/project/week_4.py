from tokenize import group
from typing import Iterable, Iterator, List

from dagster import Nothing, asset, with_resources, OpExecutionContext, Output
from project.resources import redis_resource, s3_resource, Redis
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    group_name="corise",
)
def get_s3_data(context: OpExecutionContext):
    # Use your op logic from week 3
    ls = context.resources.s3.get_data(context.op_config["s3_key"])
    return [*map(lambda text: Stock.from_list(text), ls)]


@asset(
    group_name="corise",
)
def process_data(get_s3_data) -> Aggregation:
    # Use your op logic from week 3 (you will need to make a slight change)
    highest = max(get_s3_data, key=lambda s: s.high)
    return Aggregation(date=highest.date, high=highest.high)


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation) -> None:
    # Use your op logic from week 3 (you will need to make a slight change)
    redis: Redis = context.resources.redis
    redis.put_data(name=process_data.date.strftime("%Y-%m-%d"), value=f"{process_data.high:02f}")


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
    }
)
