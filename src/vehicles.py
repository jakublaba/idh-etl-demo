import duckdb
import pandas as pd

VEHICLES_FILE_NAME = "ztm_vehicles_detailed.csv"


def load_vehicles_into_duckdb(
    dbsession: duckdb.DuckDBPyConnection,
):
    df = pd.read_csv(f"data/{VEHICLES_FILE_NAME}")
    tmp_view_name = "_tmp_vehicles"
    dbsession.register(tmp_view_name, df)
    dbsession.execute(
        f"create or replace table vehicles as select * from {tmp_view_name}"
    )
    dbsession.unregister(tmp_view_name)
