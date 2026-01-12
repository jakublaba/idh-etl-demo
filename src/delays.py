import os
from typing import List

import duckdb
import pandas as pd
import pendulum

DELAYS_BUCKET = "traffic"


def _get_delay_files(
    as_of: pendulum.Date,
) -> List[str]:
    prefix = as_of.strftime("%Y/%m/%d")
    files = [f for f in os.listdir(f"data/delays/{prefix}/") if f.endswith(".csv")]
    return [os.path.join(f"data/delays/{prefix}/", f) for f in files]


def _merge_delay_files(
    as_of: pendulum.Date,
) -> pd.DataFrame:
    files = _get_delay_files(as_of)
    dfs = [pd.read_csv(f) for f in files]
    return pd.DataFrame() if not dfs else pd.concat(dfs)


def _normalize_delay(delay_str: str) -> int:
    sign = -1 if "min przed czasem" in delay_str else 1
    cleaned_str = delay_str.replace(" min przed czasem", "").replace(" min", "")
    return sign * int(cleaned_str)


# we use hourly granularity, truncating rest of the timestamp to be joinable to TimeDim timestamps
def _normalize_timestamp(timestamp_str: str) -> pd.Timestamp:
    dt = pendulum.parse(timestamp_str)
    return pd.Timestamp(dt).floor("h")


def load_delays_into_duckdb(
    as_of: pendulum.Date,
    dbsession: duckdb.DuckDBPyConnection,
):
    df = _merge_delay_files(as_of)

    df["Vehicle No"] = df["Vehicle No"].apply(lambda x: None if pd.isna(x) else str(x))
    df["Delay"] = df["Delay"].apply(_normalize_delay)
    df["Timestamp"] = df["Timestamp"].apply(_normalize_timestamp)

    tmp_view_name = "_tmp_delays"
    dbsession.register(tmp_view_name, df)
    dbsession.execute(
        f"create or replace table delays as select * from {tmp_view_name}"
    )
    dbsession.unregister(tmp_view_name)
