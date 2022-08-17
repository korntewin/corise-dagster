import csv
from datetime import datetime
from typing import List

from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output


@op(
	config_schema={"nlargest": int},
	out=DynamicOut(dagster_type=Aggregation)
)
def process_data(context, stocks: List[Stock]):

	def stck_to_dagg(stock: Stock, key: str) -> DynamicOutput[Aggregation]:
		return DynamicOutput(
			Aggregation(
				date=stock.date,
				high=stock.high
			),
			mapping_key=key
		)

	n_largest = context.op_config["nlargest"]
	sorted_stocks = sorted(
		stocks,
		key=lambda stock: stock.high,
		reverse=True
	)[:n_largest]

	for i, stock in enumerate(sorted_stocks):
		yield stck_to_dagg(stock, str(i))


@op
def put_redis_data(agg: Aggregation):
    pass


@job
def week_1_pipeline():
	stocks = get_s3_data()
	aggs = process_data(stocks)
	aggs.map(put_redis_data)
