from __future__ import annotations

import json
import warnings
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")


def get_params(key: str):
    params = {
        "schema": "analytics_demo",
        "table": "work_item_status_history",
    }
    return params[key]


def tracker_session() -> tuple[requests.Session, str, str]:
    token = Variable.get("work_item_tracker_api_token")
    base_url = Variable.get(
        "work_item_tracker_base_url",
        default_var="https://tracker.example.com/api/v1",
    ).rstrip("/")
    workspace_id = Variable.get("work_item_tracker_workspace_id")

    if not token:
        raise ValueError("work_item_tracker_api_token is not configured")
    if not workspace_id:
        raise ValueError("work_item_tracker_workspace_id is not configured")

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    )
    return session, base_url, workspace_id


def _extract_attr(attributes: dict | list | None, candidates: list[str]) -> str | None:
    if isinstance(attributes, dict):
        lowered = {str(key).lower(): value for key, value in attributes.items()}
        for candidate in candidates:
            val = lowered.get(candidate.lower())
            if val is not None:
                return str(val).strip() or None
        return None

    if isinstance(attributes, list):
        for item in attributes:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).lower().strip()
            if name not in {c.lower() for c in candidates}:
                continue
            value = item.get("display_value")
            if value is None:
                value = item.get("text_value") or (item.get("enum_value") or {}).get("name")
            return (str(value).strip() if value is not None else None) or None
    return None


def _extract_items(payload: dict | list) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("items", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def normalize_status(status: str | None) -> str | None:
    if not status:
        return None
    status = status.strip()
    if status.lower().startswith("reject"):
        return "Rejected"
    return status


def normalize_flag(flag: str | None) -> str | None:
    return flag.strip().lower() if flag else None


def serialize_dataframe(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    local_df = df.copy()
    tz_columns = local_df.select_dtypes(include=["datetimetz"]).columns
    for col in tz_columns:
        local_df[col] = local_df[col].dt.tz_localize(None)
    return json.loads(local_df.to_json(orient="records", date_format="iso"))


@dag(
    "demo_work_item_status_alerts",
    description="Incremental work-item tracker polling with status change notifications",
    schedule_interval="10,30,50 8-20 * * 1-5",
    catchup=False,
    tags=["demo", "work-items", "api", "notifications", "incremental"],
    default_args={
        "owner": "portfolio_demo",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    max_active_runs=1,
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_work_item_status_alerts():
    @task(show_return_value_in_logs=False)
    def get_data_from_tracker(**context):
        try:
            client = con.get_clickhouse_client()
            try:
                history_df = client.query_df(
                    f"SELECT * FROM {get_params('schema')}.v_{get_params('table')}"
                )
            finally:
                client.close()

            if history_df.empty or history_df["updated_at"].isna().all():
                modified_since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat().replace(
                    "+00:00", "Z"
                )
            else:
                modified_since = (
                    history_df["updated_at"].max() - timedelta(seconds=1)
                ).isoformat().replace("+00:00", "Z")
            print(f"Capturing tracker items modified since: {modified_since}")

            session, base_url, workspace_id = tracker_session()
            response = session.get(
                f"{base_url}/work-items",
                params={
                    "workspace_id": workspace_id,
                    "modified_since": modified_since,
                    "limit": 500,
                },
                timeout=60,
            )
            response.raise_for_status()
            items = _extract_items(response.json())
            print(f"Total items modified: {len(items)}")

            rows = []
            captured_at = datetime.now(timezone.utc)
            for item in items:
                attributes = item.get("attributes") or item.get("custom_fields")
                item_id = item.get("id") or item.get("gid")
                item_name = item.get("title") or item.get("name") or ""
                item_url = item.get("url") or item.get("permalink_url") or ""
                updated_at_raw = item.get("updated_at") or item.get("modified_at")
                if not item_id or not updated_at_raw:
                    continue

                updated_at = datetime.fromisoformat(str(updated_at_raw).replace("Z", "+00:00"))
                topic = _extract_attr(attributes, ["topic", "stream", "category", "method"])
                status = normalize_status(_extract_attr(attributes, ["status", "workflow_status", "state"]))
                rollout_flag = normalize_flag(
                    _extract_attr(attributes, ["rollout_enabled", "feature_flag"])
                )
                rows.append(
                    (
                        item_id,
                        item_name,
                        topic,
                        status,
                        rollout_flag,
                        item_url,
                        updated_at,
                        captured_at,
                    )
                )

            df_new = pd.DataFrame(
                rows,
                columns=[
                    "item_id",
                    "item_name",
                    "topic",
                    "status",
                    "rollout_flag",
                    "item_url",
                    "updated_at",
                    "captured_at",
                ],
            )
            if df_new.empty:
                return {"changes": [], "new_rows": []}

            df_new["item_id"] = pd.to_numeric(df_new["item_id"], errors="coerce")
            df_new = df_new[df_new["item_id"].notna()].copy()
            if df_new.empty:
                return {"changes": [], "new_rows": []}
            df_new["item_id"] = df_new["item_id"].astype("int64")

            merged = df_new.merge(
                history_df,
                how="left",
                on=["item_id"],
                suffixes=["_new", "_old"],
            )
            merged = merged.replace({None: np.nan})
            changes = merged.loc[
                (merged["status_new"] != merged["status_old"])
                | (merged["status_new"].notna() & merged["status_old"].isna()),
                :,
            ]
            return {
                "changes": serialize_dataframe(changes),
                "new_rows": serialize_dataframe(df_new),
            }
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    @task()
    def notify_changes_and_insert_updates(payload, **context):
        try:
            payload = payload or {}
            df_changes = pd.DataFrame(payload.get("changes", []))
            df_new = pd.DataFrame(payload.get("new_rows", []))
            if df_new.empty:
                return

            for dt_col in ("updated_at", "captured_at"):
                if dt_col in df_new.columns:
                    df_new[dt_col] = pd.to_datetime(df_new[dt_col], errors="coerce")

            ts = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S UTC")
            title = f"#### Work item status changes as of {ts}:\n"
            lines = ""
            line_index = 0

            for _, row in df_changes.iterrows():
                topic = row.get("topic_new")
                status = row.get("status_new")
                rollout_flag = row.get("rollout_flag_new")
                url = row.get("item_url_new")
                item_name = row.get("item_name_new")

                if status in {"In Progress", "Completed", "Ready", "Blocked", "Rejected"}:
                    line_index += 1
                else:
                    continue

                idx = line_index
                if status == "In Progress":
                    lines += f"{idx}. Item <{url}|{item_name}> topic *{topic}* moved to in-progress\n"
                elif status == "Completed" and rollout_flag == "yes":
                    lines += (
                        f"{idx}. Item <{url}|{item_name}> topic *{topic}* completed with rollout enabled\n"
                    )
                elif status == "Completed":
                    lines += (
                        f"{idx}. Item <{url}|{item_name}> topic *{topic}* completed without rollout enabled\n"
                    )
                elif status == "Ready":
                    lines += f"{idx}. Item <{url}|{item_name}> topic *{topic}* is ready\n"
                elif status == "Blocked":
                    lines += f"{idx}. Item <{url}|{item_name}> topic *{topic}* is blocked\n"
                elif status == "Rejected":
                    lines += f"{idx}. Item <{url}|{item_name}> topic *{topic}* was rejected\n"

            channel_id = context["params"].get("receiver_channel_id")
            if channel_id and lines:
                msg.send_team_chat_message(message=(title + lines), channel_id=channel_id)

            con.insert_df_to_dwh(
                df=df_new,
                db=get_params("schema"),
                table=get_params("table"),
                batch_size=1_000_000,
            )
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    get_task = get_data_from_tracker()
    notify_task = notify_changes_and_insert_updates(get_task)
    get_task >> notify_task


demo_work_item_status_alerts()
