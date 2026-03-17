from __future__ import annotations

import warnings
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.models import Variable

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")


def get_params(key: str):
    params = {
        "channel_id_var": "demo_metric_alert_channel_id",
        "source_code_var": "demo_metric_source_code",
    }
    return params[key]


def format_readable_date(value):
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            parsed = datetime.fromisoformat(value).date()
    elif isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        raise TypeError("Expected str | datetime | date")
    return parsed.strftime("%d %B %Y")


def to_naive_dt(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    return value


@dag(
    "demo_hourly_metric_notifier",
    description="Hourly metric alerts for drop monitoring",
    schedule_interval="4 8-23 * * *",
    catchup=False,
    tags=["demo", "monitoring", "alerts", "clickhouse"],
    default_args={
        "owner": "portfolio_demo",
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    max_active_runs=1,
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_hourly_metric_notifier():
    @task(show_return_value_in_logs=False)
    def get_data_and_notify(**context):
        try:
            source_code = int(Variable.get(get_params("source_code_var"), default_var="1001"))
            channel_id = Variable.get(get_params("channel_id_var"))
            source_label = f"source_{source_code}"

            client = con.get_clickhouse_client()
            try:
                df = client.query_df(
                    f"""
                    WITH
                      now64(0, 'UTC') AS current_dt,
                      toStartOfHour(current_dt) AS current_hour_start,
                      current_hour_start - INTERVAL 1 HOUR AS previous_hour_start
                    SELECT
                      coalesce(src.source_name, '{source_label}') AS source_name,
                      date(e.created_at) AS event_date,
                      toHour(e.created_at) AS event_hour,
                      uniqExact(e.event_id) AS cnt_total,
                      countIf(e.event_id, e.status_code = 1) AS cnt_success,
                      countIf(e.event_id, e.status_code = 2) AS cnt_errors,
                      countIf(e.event_id, e.status_code = 2) / uniqExact(e.event_id) AS error_share,
                      countIf(e.event_id, e.status_code = 1) / uniqExact(e.event_id) AS success_rate,
                      previous_hour_start,
                      current_hour_start
                    FROM analytics_demo_raw.source_events e
                    LEFT JOIN analytics_demo_raw.source_dictionary src
                      ON toInt64(src.source_id) = toInt64(e.source_id)
                    WHERE e.created_at >= previous_hour_start
                      AND e.created_at < current_hour_start
                      AND e.source_id = {source_code}
                    GROUP BY source_name, event_date, event_hour
                    """
                )
            finally:
                client.close()

            if df.empty:
                current_hour_start_dt = datetime.now(ZoneInfo("UTC")).replace(
                    minute=0, second=0, microsecond=0
                )
                previous_hour_start_dt = current_hour_start_dt - timedelta(hours=1)
                start_hour = previous_hour_start_dt.strftime("%H:%M")
                end_hour = current_hour_start_dt.strftime("%H:%M")
                msg.send_team_chat_message(
                    channel_id=channel_id,
                    message=(
                        f"**Hourly monitor: {source_label}**\n\n"
                        f"Date: `{format_readable_date(previous_hour_start_dt)}`\n"
                        f"Hour: `{start_hour} - {end_hour} (UTC)`\n\n"
                        "No events were registered during the last hour."
                    ),
                )
                return

            dt = to_naive_dt(df.at[0, "previous_hour_start"])
            success_rate = round(df.at[0, "success_rate"], 4)
            source_name = df.at[0, "source_name"] or source_label
            cnt_total = int(df.at[0, "cnt_total"])
            cnt_success = int(df.at[0, "cnt_success"])
            start_hour = to_naive_dt(df.at[0, "previous_hour_start"]).strftime("%H:%M")
            end_hour = to_naive_dt(df.at[0, "current_hour_start"]).strftime("%H:%M")

            if success_rate == 0:
                text = (
                    f"**Critical metric alert for {source_name}**\n\n"
                    f"Date: `{format_readable_date(dt)}`\n"
                    f"Hour: `{start_hour} - {end_hour} (UTC)`\n\n"
                    f"- Success rate: **{success_rate:.2%}**\n"
                    f"- Successful events: **{cnt_success}**\n"
                    f"- Total events: **{cnt_total}**\n"
                )
            elif success_rate < 0.1:
                text = (
                    f"**Metric alert for {source_name}**\n\n"
                    f"Date: `{format_readable_date(dt)}`\n"
                    f"Hour: `{start_hour} - {end_hour} (UTC)`\n\n"
                    f"- Success rate: **{success_rate:.2%}**\n"
                    f"- Successful events: **{cnt_success}**\n"
                    f"- Total events: **{cnt_total}**\n"
                )
            else:
                text = None

            if text:
                msg.send_team_chat_message(channel_id=channel_id, message=text)
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    get_data_and_notify()


demo_hourly_metric_notifier()
