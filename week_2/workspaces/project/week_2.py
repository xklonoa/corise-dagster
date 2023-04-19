from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
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
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
)
