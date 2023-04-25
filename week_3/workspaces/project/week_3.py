from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    String,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(required_resource_keys={"s3"},
    config_schema={"s3_key": String})
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3_key = context.op_config['s3_key']
    stocks = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]
    return stocks


@op
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda s: s.high, reverse=True)
    max_stock = sorted_stocks[0]
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=aggregation.date.strftime("%y%m%d"), value=str(aggregation.high))


@op(required_resource_keys={"s3"})
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key_name=aggregation.date.strftime("%y%m%d"), data=aggregation)

@graph
def machine_learning_graph():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(pk) for pk in range(1,11)])
def docker_config(partition_key: int):
    return {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={'s3': mock_s3_resource, 'redis': ResourceDefinition.mock_resource()}
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_docker,
    resource_defs={'s3': s3_resource, 'redis': redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10,  delay=1)
)



machine_learning_schedule_local = ScheduleDefinition(cron_schedule="*/15 * * * *", job=machine_learning_job_local)


@schedule(cron_schedule="0 * * * *", job=machine_learning_job_docker)
def machine_learning_schedule_docker():
    for partition_key in docker_config.get_partition_keys():
        yield RunRequest(run_key=partition_key, run_config=docker_config.get_run_config(partition_key))


@sensor(job=machine_learning_job_docker)
def machine_learning_sensor_docker():
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    if not s3_keys:
            yield SkipReason("No new s3 files found in bucket.")
    else:
        for key in s3_keys:
            yield RunRequest(
                run_key=key,
                run_config={
                    "resources": {
                        "s3": {"config": S3},
                        "redis": {"config": REDIS},
                    },
                    "ops": {"get_s3_data": {"config": {"s3_key": key}}},
}
            )
