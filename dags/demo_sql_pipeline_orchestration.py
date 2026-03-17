from __future__ import annotations

import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")


@dag(
    dag_id="demo_sql_pipeline_orchestration",
    description="Plan-driven SQL pipeline update with source watermark sensor",
    schedule_interval="25 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "portfolio_demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["demo", "clickhouse", "sql", "sensor", "orchestration"],
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_sql_pipeline_orchestration():
    @task.sensor(
        poke_interval=5 * 60,
        timeout=25 * 60,
        mode="reschedule",
        soft_fail=True,
    )
    def check_source_update(**context):
        try:
            client = con.get_clickhouse_client()
            try:
                last_loaded_at = pd.to_datetime(
                    client.query_df(
                        """
                        SELECT max(max_updated_at) AS max_updated_at
                        FROM analytics_demo.unified_records_watermark FINAL
                        """
                    )["max_updated_at"].values[0]
                )

                source_updated_at = pd.to_datetime(
                    client.query_df(
                        """
                        SELECT max(updated_at) AS max_updated_at
                        FROM analytics_demo_raw.source_events
                        """
                    )["max_updated_at"].values[0]
                )

                if pd.isna(source_updated_at):
                    print("[demo_sql_pipeline] Source has no data yet. Waiting...")
                    return False

                if not pd.isna(last_loaded_at) and last_loaded_at >= source_updated_at:
                    print("[demo_sql_pipeline] Source watermark is not ready yet. Waiting...")
                    return False

                client.command(
                    """
                    INSERT INTO analytics_demo.unified_records_watermark
                    SELECT
                      'source_events' AS table_name,
                      count() AS row_count,
                      max(created_at) AS max_created_at,
                      max(updated_at) AS max_updated_at,
                      now() AS loaded_at
                    FROM analytics_demo_raw.source_events
                    """
                )
                return True
            finally:
                client.close()
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    @task
    def populate_dataset(**context):
        db_name = "analytics_demo"
        current_directory = Variable.get("etl_directory")
        sql_directory = f"{current_directory}/sql/demo"
        update_plan = {
            "unified_records_stage_1": "unified_records_stage_1.sql",
            "unified_records_stage_2": "unified_records_stage_2.sql",
            "unified_records_stage_3": "unified_records_stage_3.sql",
        }
        try:
            client = con.get_clickhouse_client()
            try:
                for table, sql_file in update_plan.items():
                    start_ts = time.time()
                    print(f"[demo_sql_pipeline] Start updating table '{table}'")
                    if table != "unified_records_stage_3":
                        client.command(f"TRUNCATE TABLE {db_name}.{table}")
                    sql_script = con.get_sql_script(f"{sql_directory}/{sql_file}")
                    client.command(f"INSERT INTO {db_name}.{table} {sql_script}")
                    duration = round(time.time() - start_ts, 2)
                    row_count = int(
                        client.query_df(f"SELECT count(*) AS cnt FROM {db_name}.{table}")["cnt"].values[0]
                    )
                    print(
                        f"[demo_sql_pipeline] Table '{table}' updated in {duration}s, rows={row_count:,}"
                    )
            finally:
                client.close()
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    check_source_update() >> populate_dataset()


demo_sql_pipeline_orchestration()
