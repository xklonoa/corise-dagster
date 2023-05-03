from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock


# @op(required_resource_keys={"s3"},
#     config_schema={"s3_key": String})
@asset(required_resource_keys={"s3"},
        config_schema={"s3_key": String}
        )
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3_key = context.op_config['s3_key']
    stocks = [Stock.from_list(stock) for stock in context.resources.s3.get_data(s3_key)]
    return stocks


# @op
@asset
def process_data(context: OpExecutionContext, stocks: List[Stock]) -> Aggregation:
    sorted_stocks = sorted(stocks, key=lambda s: s.high, reverse=True)
    max_stock = sorted_stocks[0]
    return Aggregation(date=max_stock.date, high=max_stock.high)


# @op(required_resource_keys={"redis"})
@asset(required_resource_keys={"redis"})
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=aggregation.date.strftime("%y%m%d"), value=str(aggregation.high))


# @op(required_resource_keys={"s3"})
@asset(required_resource_keys={"s3"})
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation) -> Nothing:
    context.resources.s3.put_data(key_name=aggregation.date.strftime("%y%m%d"), data=aggregation)


project_assets = load_assets_from_current_module()


machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
