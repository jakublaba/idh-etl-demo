import duckdb
import pandas as pd
from pendulum import Date

# we only need a subset of GTFS files for our analysis
GTFS_FILES = [
    "routes",
    "stop_times",
    "stops",
    "trips",
]
GTFS_FILE_EXTENSION = "csv"
GTFS_BUCKET = "gtfs"


def load_gtfs_into_duckdb(
    as_of: Date,
    dbsession: duckdb.DuckDBPyConnection,
):
    for file_name in GTFS_FILES:
        path = f"data/gtfs/{as_of.year}/{as_of.month}/{as_of.day}/{file_name}.{GTFS_FILE_EXTENSION}"
        df = pd.read_csv(path)
        tmp_view_name = f"_tmp_{file_name}"
        dbsession.register(tmp_view_name, df)
        dbsession.execute(
            f"create or replace table {file_name} as select * from {tmp_view_name}"
        )
        dbsession.unregister(tmp_view_name)
