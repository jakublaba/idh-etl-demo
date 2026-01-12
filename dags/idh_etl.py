import datetime
import os
import uuid
from typing import Optional

import dotenv
import duckdb
import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.utils.log.logging_mixin import LoggingMixin
from google.cloud import bigquery
from google.oauth2 import service_account
from pendulum import DateTime

from src.delays import load_delays_into_duckdb
from src.enums import Table
from src.gtfs import load_gtfs_into_duckdb, GTFS_FILES
from src.time_utils import MONTH_MAP, get_season, get_time_of_day
from src.vehicles import load_vehicles_into_duckdb
from src.weather import load_weather_into_duckdb

DUCKDB_SHARDS = {
    "time": ["time_dim"],
    "gtfs": GTFS_FILES,
    "delays": ["delays"],
    "vehicles": ["vehicles"],
    "weather": ["weather"],
}

DUCKDB_VOLUME_PATH = "/usr/local/airflow/duckdb"


def duckdb_path(logical_date: DateTime, shard: Optional[str] = None) -> str:
    path = f"{DUCKDB_VOLUME_PATH}/idh-{logical_date.strftime('%Y%m%d_%H%M%S')}.duckdb"
    if shard is not None:
        path = path.replace(".duckdb", f"-{shard}.duckdb")

    return path


DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": datetime.timedelta(seconds=30),
}


@dag(
    schedule="@hourly",
    start_date=datetime.datetime(2024, 12, 25, 00, 00, 00),
    end_date=datetime.datetime(2024, 12, 25, 23, 59, 59),
    catchup=True,
    is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS,
)
def idh_etl():
    dotenv.load_dotenv()
    gcp_credentials_file = "gcp-credentials.json"
    bigquery_project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")

    log = LoggingMixin().log

    bigquery_client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(
            filename=gcp_credentials_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        ),
        project=bigquery_project_id,
    )

    @task_group
    def load_duckdb():
        @task
        def time(logical_date: DateTime):
            df = pd.DataFrame(
                {
                    "id": [int(logical_date.strftime("%Y%m%d"))],
                    "full_timestamp": [pd.to_datetime(logical_date)],
                    "hour_": [logical_date.hour],
                    "weekday": [logical_date.day_of_week.name],
                    "weekday_num": [logical_date.weekday() + 1],
                    "month_": [MONTH_MAP[logical_date.month]],
                    "month_num": [logical_date.month],
                    "season": [get_season(logical_date.month).value],
                    "year_": [logical_date.year],
                    "time_of_day": [get_time_of_day(logical_date.hour).value],
                    "is_business_day": [logical_date.weekday() < 5],
                }
            )

            db_path = duckdb_path(logical_date, "time")
            with duckdb.connect(db_path) as dbsession:
                tmp_view_name = "_tmp_time"
                dbsession.register(tmp_view_name, df)
                dbsession.execute("drop table if exists time_dim")
                dbsession.execute(
                    f"create table time_dim as select * from {tmp_view_name}"
                )
                dbsession.unregister(tmp_view_name)

        @task
        def gtfs(logical_date: DateTime):
            db_path = duckdb_path(logical_date, "gtfs")
            with duckdb.connect(db_path) as dbsession:
                load_gtfs_into_duckdb(
                    logical_date,
                    dbsession,
                )
            log.info("GTFS loaded into DuckDB")

        @task
        def delays(logical_date: DateTime):
            db_path = duckdb_path(logical_date, "delays")
            with duckdb.connect(db_path) as dbsession:
                load_delays_into_duckdb(
                    logical_date.date(),
                    dbsession,
                )
            log.info("DELAYS loaded into DuckDB")

        @task
        def vehicles(logical_date: DateTime):
            db_path = duckdb_path(logical_date, "vehicles")
            with duckdb.connect(db_path) as dbsession:
                load_vehicles_into_duckdb(dbsession)
            log.info("VEHICLES loaded into DuckDB")

        @task
        def weather(logical_date: DateTime):
            db_path = duckdb_path(logical_date, "weather")
            with duckdb.connect(db_path) as dbsession:
                load_weather_into_duckdb(
                    logical_date.date(),
                    dbsession,
                )
            log.info("WEATHER loaded into DuckDB")

        @task
        def merge_shards(logical_date: DateTime):
            target_path = duckdb_path(logical_date)
            with duckdb.connect(target_path) as dbsession:
                for shard_name, tables in DUCKDB_SHARDS.items():
                    log.info(f"Merging shard: {shard_name}")
                    shard_path = duckdb_path(logical_date, shard_name)
                    if not os.path.exists(shard_path):
                        log.warning(
                            f"Shard path does not exist: {shard_path}, skipping"
                        )
                        continue
                    shard_alias = f"shard_{shard_name}"
                    dbsession.execute(
                        f"attach database '{shard_path}' as {shard_alias}"
                    )

                    for t in tables:
                        dbsession.execute(
                            f"create or replace table {t} as select * from {shard_alias}.{t}"
                        )

                    dbsession.execute(f"detach database {shard_alias}")
                    os.remove(shard_path)
                    log.info(
                        f"Successfully merged {shard_alias} and removed {shard_path}"
                    )

        @task
        def verify(logical_date: DateTime):
            tables = [*GTFS_FILES, "delays", "vehicles", "weather", "time_dim"]
            with duckdb.connect(duckdb_path(logical_date)) as dbsession:
                show_tables = dbsession.execute("show tables").df()
                log.info(f"Tables at verification step: {show_tables}")
                for t in tables:
                    log.info(f"Verifying table: {t}")
                    try:
                        dbsession.execute(f"select * from {t} limit 1").df()
                        log.info(f"Successfully queried table: {t}")
                    except Exception as e:
                        log.error(f"Failed to query table: {t} - {e}")

        [time(), gtfs(), delays(), vehicles(), weather()] >> merge_shards() >> verify()

    @task
    def write_table_to_bigquery(
        table: Table,
        logical_date: DateTime,
    ):
        log.info(f"Writing {table.bigquery_table} to BigQuery")
        with duckdb.connect(duckdb_path(logical_date), read_only=True) as dbsession:
            df = dbsession.execute(table.duckdb_query).df()
            log.info(f"Fetched {len(df)} rows from DuckDB for {table.bigquery_table}")

            if df.empty:
                log.info("No rows to upload; exiting")
                return

            key_columns = table.unique_key_columns or []
            if not key_columns:
                log.warning(
                    f"No unique_key_columns defined for {table.bigquery_table}; skipping write to avoid duplicates"
                )
                return

            # remove duplicated column names if any
            df = df.loc[:, ~df.columns.duplicated()]

            # ensure all key columns exist in the dataframe
            missing_keys = [k for k in key_columns if k not in df.columns]
            if missing_keys:
                log.warning(
                    f"Unique key columns {missing_keys} not present in query results for {table.bigquery_table}; skipping write to avoid duplicates"
                )
                return

            # drop duplicate rows based on the unique key columns
            before = len(df)
            df = df.drop_duplicates(subset=key_columns)
            removed = before - len(df)
            if removed > 0:
                log.info(
                    f"Removed {removed} duplicate rows based on keys {key_columns} for {table.bigquery_table}"
                )

            if df.empty:
                log.info("All rows were duplicates after deduplication; exiting")
                return

            staging_table = f"{table.bigquery_table}_staging_{uuid.uuid4().hex[:8]}"
            staging_table_id = f"{bigquery_project_id}.{dataset_id}.{staging_table}"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )

            try:
                load_job = bigquery_client.load_table_from_dataframe(
                    df, staging_table_id, job_config=job_config
                )
                load_job.result()
                log.info(f"Loaded {len(df)} rows into staging table {staging_table_id}")

                on_clause = " AND ".join([f"T.`{c}` = S.`{c}`" for c in key_columns])
                cols = [c for c in df.columns]
                cols_escaped = ", ".join([f"`{c}`" for c in cols])
                values = ", ".join([f"S.`{c}`" for c in cols])

                merge_sql = f"""
                MERGE `{bigquery_project_id}.{dataset_id}.{table.bigquery_table}` T
                USING `{bigquery_project_id}.{dataset_id}.{staging_table}` S
                ON {on_clause}
                WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({cols_escaped}) VALUES ({values})
                """

                query_job = bigquery_client.query(merge_sql)
                query_job.result()
                log.info(
                    f"MERGE completed into {table.bigquery_table} from staging {staging_table}"
                )

            finally:
                try:
                    bigquery_client.delete_table(staging_table_id, not_found_ok=True)
                    log.info(f"Removed staging table {staging_table_id}")
                except Exception as e:
                    log.warning(
                        f"Failed to remove staging table {staging_table_id}: {e}"
                    )

        log.info(f"Successfully written new rows to {table.bigquery_table} in BigQuery")

    load_duckdb() >> write_table_to_bigquery.expand(table=list(Table))


idh_etl()
