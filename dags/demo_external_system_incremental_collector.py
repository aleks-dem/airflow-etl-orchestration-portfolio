from __future__ import annotations

import json
import time
import warnings
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")


def get_params(key: str):
    params = {
        "schema_name": "analytics_demo",
        "table_name": "external_entity_details",
        "api_chunk_size": 100,
        "api_pause_secs": 0.5,
        "attempts_cnt": 3,
        "insert_batch_size": 50_000,
        "view_limit": 100_000,
        "config_file_variable": "external_system_config_path",
    }
    return params[key]


def encrypt_payload(plain_text: str) -> str:
    key = bytes.fromhex(Variable.get("demo_encrypt_key_hex"))
    iv = bytes.fromhex(Variable.get("demo_encrypt_iv_hex"))
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plain_text.encode("utf-8")) + padder.finalize()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return ciphertext.hex()


def get_signature(secret: str, message: str) -> str:
    import hashlib
    import hmac

    return hmac.new(secret.encode(), message.encode(), hashlib.sha512).hexdigest()


def format_body(body: dict) -> str:
    return json.dumps(body, separators=(",", ":"))


def extract_params(params: dict) -> str:
    return "".join([str(v) for _, v in dict(sorted(params.items())).items()]) if params else ""


def execute_request(
    url: str,
    headers: dict,
    request_type: str = "get",
    params: dict | None = None,
    body: str | None = None,
) -> requests.Response:
    if request_type.lower() == "get":
        return requests.get(url, headers=headers, params=params, timeout=60)
    return requests.post(url, headers=headers, data=body, timeout=60)


def extract_data(
    creds: dict,
    endpoint: str,
    request_type: str = "get",
    params: dict | None = None,
    body: dict | None = None,
) -> requests.Response:
    required_keys = ("base_url", "client_id", "client_secret")
    missing_keys = [key for key in required_keys if key not in creds or not creds[key]]
    if missing_keys:
        raise ValueError(f"Missing external_system credentials keys: {missing_keys}")

    base_url = str(creds["base_url"]).strip().rstrip("/")
    client_id = str(creds["client_id"]).strip()
    client_secret = str(creds["client_secret"]).strip()

    body_string = format_body(body or {}) if request_type.lower() == "post" else ""
    body_for_sign = body_string if request_type.lower() == "post" else extract_params(params or {})
    message = client_id + endpoint + body_for_sign
    signature = get_signature(client_secret, message)
    headers = {
        "Content-Type": "application/json",
        "X-Client-Id": client_id,
        "X-Signature": signature,
    }
    url = base_url + endpoint
    return execute_request(url, headers, request_type, params, body_string)


@dag(
    dag_id="demo_external_system_incremental_collector",
    description="Incremental external system collector with enrichment and branching notifications",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "portfolio_demo",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["demo", "api", "hmac", "clickhouse", "branching", "trigger"],
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_external_system_incremental_collector():
    @task()
    def get_data_from_external_system(**context):
        try:
            etl_dir = Variable.get("etl_directory")
            default_cfg_path = f"{etl_dir}/configs/external_system_config.json"
            cfg_path = Variable.get(
                get_params("config_file_variable"),
                default_var=default_cfg_path,
            )
            with open(cfg_path, "r", encoding="utf-8") as cfg_file:
                cfg_json = json.load(cfg_file)

            creds_api = cfg_json["external_system"]
            schema_name = get_params("schema_name")
            table_name = get_params("table_name")
            api_chunk_size = get_params("api_chunk_size")
            api_pause_secs = get_params("api_pause_secs")
            attempts_cnt = get_params("attempts_cnt")
            insert_batch_size = get_params("insert_batch_size")
            view_limit = get_params("view_limit")

            request_type = "post"
            endpoint = "/v1/entity/details"
            body = {}
            search_field_name = "requested_id"
            join_field = "lookup_key"
            result_fields = [
                "group_id",
                "entity_id",
                "entity_uuid",
                "raw_payload",
                "ingested_at",
            ]
            df_columns = {
                join_field: "object",
                "raw_payload": "object",
                "ingested_at": "datetime64[ns]",
            }
            dedup_field = "entity_uuid"
            processed_ids = pd.Series(dtype="object")
            total_requests = 0
            successful_requests = 0

            while True:
                client = con.get_clickhouse_client()
                try:
                    df = client.query_df(f"SELECT * FROM {schema_name}.v_{table_name}")
                finally:
                    client.close()

                original_len = len(df)
                if original_len == 0:
                    break

                df = df[~df[dedup_field].isin(processed_ids)]
                if df.empty:
                    break

                df_from_api = pd.DataFrame(columns=df_columns.keys()).astype(df_columns)
                for (tenant_id, lookup_type), grouped_df in df.groupby(
                    ["tenant_id", "lookup_type"],
                    dropna=False,
                ):
                    filtered_df = grouped_df[join_field].values.tolist()
                    if not filtered_df:
                        continue
                    chunks = [
                        filtered_df[i : i + api_chunk_size]
                        for i in range(0, len(filtered_df), api_chunk_size)
                    ]
                    for idx, chunk in enumerate(chunks):
                        total_requests += 1
                        body["tenant"] = tenant_id
                        body["lookup_type"] = lookup_type
                        body["ids"] = chunk
                        for _ in range(attempts_cnt):
                            time.sleep(api_pause_secs)
                            result = extract_data(
                                creds=creds_api,
                                endpoint=endpoint,
                                request_type=request_type,
                                body=body,
                            )
                            if result.status_code != 200:
                                continue

                            payload = result.json()
                            if not isinstance(payload, list):
                                payload = []
                            df_result = pd.DataFrame(
                                [
                                    (
                                        row[search_field_name],
                                        json.dumps(row, separators=(",", ":")),
                                        datetime.now(timezone.utc).replace(tzinfo=None),
                                    )
                                    for row in payload
                                    if search_field_name in row
                                ],
                                columns=df_columns.keys(),
                            )
                            if not df_result.empty:
                                df_from_api = pd.concat([df_from_api, df_result], axis=0)
                            if (idx + 1) % 500 == 0:
                                print(f"tenant_id={tenant_id}, chunk {idx + 1}/{len(chunks)}")
                            successful_requests += 1
                            break
                        else:
                            print(
                                f"Request chunk failed after {attempts_cnt} attempts; tenant_id={tenant_id}, lookup_type={lookup_type}"
                            )

                if df_from_api.empty:
                    break

                df_insert = pd.merge(left=df, right=df_from_api, on=join_field)[result_fields]
                df_insert["raw_payload"] = df_insert["raw_payload"].apply(encrypt_payload)
                con.insert_df_to_dwh(
                    df=df_insert,
                    db=schema_name,
                    table=table_name,
                    batch_size=insert_batch_size,
                )

                if original_len < view_limit:
                    break
                processed_ids = pd.concat([processed_ids, df[dedup_field]], ignore_index=True)
                print(f"processed_ids={len(processed_ids)}")
                time.sleep(5)

            success_pct = 100.0 if total_requests == 0 else successful_requests / total_requests * 100
            print(f"Successful requests: {successful_requests}")
            print(f"All requests: {total_requests}")
            print(f"Success percentage: {success_pct:.2f}%")

            context["task_instance"].xcom_push(
                key="requests_list",
                value=[successful_requests, total_requests],
            )
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    @task.branch()
    def is_required_to_notify(**context):
        request_stats = context["task_instance"].xcom_pull(
            task_ids="get_data_from_external_system",
            key="requests_list",
        ) or [0, 0]
        successful, total = request_stats
        if total == 0:
            return "skip_notification"
        if successful / total <= 0.85:
            return "notification"
        return "skip_notification"

    skip_notification = EmptyOperator(task_id="skip_notification")

    @task(trigger_rule="all_success")
    def notification(**context):
        request_stats = context["task_instance"].xcom_pull(
            task_ids="get_data_from_external_system",
            key="requests_list",
        ) or [0, 0]
        successful, total = request_stats
        if total == 0:
            return
        lost = total - successful
        pct = (lost / total) * 100
        message = (
            "#### External integration partial outage detected:\n"
            f"- Successful responses: *{successful:,}*\n"
            f"- Lost requests: *{lost:,}*\n"
            f"- Total requests: *{total:,}*\n"
            f"- Loss rate: *{pct:.2f}%*"
        )
        channel_id = context["params"].get("receiver_channel_id")
        if channel_id:
            msg.send_team_chat_message(message=message, channel_id=channel_id)

    @task(trigger_rule="one_success")
    @provide_session
    def check_downstream_dag(session=None, **_):
        downstream_dag_id = "demo_external_system_followup_pipeline"
        running_count = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == downstream_dag_id,
                DagRun.state.in_([State.RUNNING, State.QUEUED]),
            )
            .count()
        )
        if running_count:
            raise AirflowSkipException(f"DAG '{downstream_dag_id}' is already running")

    downstream_trigger = TriggerDagRunOperator(
        task_id="trigger_demo_external_system_followup_pipeline",
        trigger_dag_id="demo_external_system_followup_pipeline",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    get_data = get_data_from_external_system()
    branch = is_required_to_notify()
    notify = notification()
    check_dag = check_downstream_dag()
    get_data >> branch >> [notify, skip_notification] >> check_dag >> downstream_trigger


demo_external_system_incremental_collector()
