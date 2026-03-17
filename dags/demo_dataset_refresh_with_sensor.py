from __future__ import annotations

import time
import warnings
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")


@dag(
    dag_id="demo_dataset_refresh_with_sensor",
    description="Dataset refresh pipeline with sensor-gated orchestration and downstream trigger",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "portfolio_demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["demo", "sensor", "orchestration", "trigger", "clickhouse"],
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_dataset_refresh_with_sensor():
    def _count_active_runs(session=None):
        watched_dags = [
            "demo_external_system_incremental_collector",
            "demo_external_system_followup_pipeline",
            "demo_dataset_materialization_pipeline",
        ]
        total_running = 0
        dags_running = []
        for watched_dag_id in watched_dags:
            running_count = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == watched_dag_id,
                    DagRun.state.in_([State.RUNNING, State.QUEUED]),
                )
                .count()
            )
            if running_count:
                total_running += running_count
                dags_running.append(watched_dag_id)
        return total_running, dags_running

    @task.sensor(
        poke_interval=5 * 60,
        timeout=25 * 60,
        mode="reschedule",
        soft_fail=True,
    )
    @provide_session
    def check_flow_active_runs(session=None):
        total_running, dags_running = _count_active_runs(session=session)
        if total_running:
            print(
                f"[demo_dataset_refresh] Active runs found: {total_running}, DAGs={dags_running}. Waiting..."
            )
            return False
        print("[demo_dataset_refresh] No active dependent DAGs. Continue.")
        return True

    @task()
    @provide_session
    def update_datasets(session=None, **context):
        try:
            schema_name = "analytics_demo"
            insert_plan = {
                "external_entity_snapshot": [
                    "v_external_entity_snapshot_segment_a",
                    "v_external_entity_snapshot_segment_b",
                    "v_external_entity_snapshot_segment_c",
                ]
            }

            client = con.get_clickhouse_client()
            try:
                for target_table, source_list in insert_plan.items():
                    client.command(f"TRUNCATE TABLE {schema_name}.{target_table}")
                    print(f"[demo_dataset_refresh] Target table '{target_table}' was truncated")
                    for src in source_list:
                        start_ts = time.time()
                        print(
                            f"[demo_dataset_refresh] Start loading source '{src}' into '{target_table}'"
                        )
                        client.command(
                            f"""
                            INSERT INTO {schema_name}.{target_table}
                            (
                                group_id,
                                entity_id,
                                entity_uuid,
                                created_at,
                                updated_at,
                                tenant_id,
                                lookup_type,
                                lookup_key
                            )
                            SELECT * FROM {schema_name}.{src}
                            """
                        )
                        duration = time.time() - start_ts
                        print(
                            f"[demo_dataset_refresh] Source '{src}' inserted into '{target_table}' in {duration:.2f}s"
                        )
            finally:
                client.close()

            collector_dag_id = "demo_external_system_incremental_collector"
            running_count = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == collector_dag_id,
                    DagRun.state.in_([State.RUNNING, State.QUEUED]),
                )
                .count()
            )
            if running_count:
                raise AirflowSkipException(f"DAG '{collector_dag_id}' is already running")
        except AirflowSkipException:
            raise
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    check_flow = check_flow_active_runs()
    update_task = update_datasets()

    collector_trigger = TriggerDagRunOperator(
        task_id="trigger_demo_external_system_incremental_collector",
        trigger_dag_id="demo_external_system_incremental_collector",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    check_flow >> update_task >> collector_trigger


demo_dataset_refresh_with_sensor()
