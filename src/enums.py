import enum
from typing import List

from google.cloud.bigquery import SchemaField

from src.queries import (
    LINE_DIM_QUERY,
    STOP_DIM_QUERY,
    VEHICLE_DIM_QUERY,
    WEATHER_DIM_QUERY,
    TIME_DIM_QUERY,
    DELAY_FACT_QUERY,
)
from src.schemas import (
    LINE_DIM_SCHEMA,
    STOP_DIM_SCHEMA,
    VEHICLE_DIM_SCHEMA,
    WEATHER_DIM_SCHEMA,
    TIME_DIM_SCHEMA,
    DELAY_FACT_SCHEMA,
)


class Table(enum.Enum):
    LINE = ("LineDim", ["id"], LINE_DIM_SCHEMA, LINE_DIM_QUERY)
    STOP = ("StopDim", ["id"], STOP_DIM_SCHEMA, STOP_DIM_QUERY)
    VEHICLE = ("VehicleDim", ["id"], VEHICLE_DIM_SCHEMA, VEHICLE_DIM_QUERY)
    WEATHER = ("WeatherDim", ["id"], WEATHER_DIM_SCHEMA, WEATHER_DIM_QUERY)
    TIME = ("TimeDim", ["id"], TIME_DIM_SCHEMA, TIME_DIM_QUERY)
    DELAY = (
        "DelayFact",
        ["time_id", "weather_id", "vehicle_id", "line_id", "stop_id"],
        DELAY_FACT_SCHEMA,
        DELAY_FACT_QUERY,
    )

    def __init__(
        self,
        bigquery_table: str,
        unique_key_columns: List[str],
        schema: List[SchemaField],
        duckdb_query: str,
    ):
        self.bigquery_table = bigquery_table
        self.unique_key_columns = unique_key_columns
        self.schema = schema
        self.duckdb_query = duckdb_query
