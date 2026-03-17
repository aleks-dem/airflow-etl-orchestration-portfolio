from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from utils import connectors as con
from utils import messages as msg

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)


def build_subject(dt: str):
    current_part, next_part, splitter = "", "", ""
    dt_parsed = datetime.strptime(dt, "%Y-%m-%d")
    month_start_wd = dt_parsed.replace(day=1).weekday()
    month_start_wd = month_start_wd if month_start_wd < 4 else 0
    week_number = (dt_parsed.day + month_start_wd) // 7 + 1
    dt_end = dt_parsed + timedelta(days=4)
    dt_end_wd = dt_end.replace(day=1).weekday()
    current_part = f"week {week_number} of {dt_parsed.strftime('%B')}"

    if dt_parsed.month != dt_end.month and dt_end_wd < 4:
        next_part = f"week 1 of {dt_end.strftime('%B')}"
        if dt_end_wd < 2:
            current_part = ""
        if current_part and next_part:
            splitter = " / "
    return (
        f"Work item digest {current_part}{splitter}{next_part} "
        f"{dt_parsed.strftime('%d.%m.%Y')}-{dt_end.strftime('%d.%m.%Y')}"
    )


@dag(
    "demo_weekly_work_item_digest_email",
    description="Weekly email digest of work items",
    schedule_interval="10 6 * * 1",
    catchup=False,
    tags=["demo", "email", "digest", "clickhouse"],
    default_args={
        "owner": "portfolio_demo",
        "start_date": datetime(2025, 1, 1),
    },
    max_active_runs=1,
    params={
        "receiver_channel_id": "",
        "owner_channel_id": "",
    },
)
def demo_weekly_work_item_digest_email():
    @task(task_id="get_weekly_items")
    def get_weekly_items(**context):
        try:
            client = con.get_clickhouse_client()
            try:
                df = con.get_clickhouse_df(
                    """
                    WITH
                      date_trunc('week', date_add(day, -7, today())) AS dt_start,
                      date_trunc('week', today()) AS dt_end
                    SELECT
                      row_number() OVER (
                        ORDER BY
                          multiIf(status = 'completed', 1, status = 'in_progress', 2, 3),
                          item_name
                      ) AS rn,
                      item_url AS url,
                      item_name,
                      created_at,
                      completed_at,
                      status
                    FROM analytics_demo.weekly_work_item_digest
                    WHERE (
                      completed_at >= dt_start
                      AND completed_at < dt_end
                    ) OR (
                      completed_at IS NULL
                      AND status IN ('in_progress', 'backlog')
                    )
                    ORDER BY rn
                    """,
                    client,
                )
            finally:
                client.close()
            logger.info("Dataframe with %s rows was fetched", len(df))
            context["task_instance"].xcom_push(
                key="weekly_rows",
                value=json.loads(df.to_json(orient="records", date_format="iso")),
            )
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    @task(task_id="send_email")
    def send_email(**context):
        try:
            rows = context["task_instance"].xcom_pull(task_ids="get_weekly_items", key="weekly_rows") or []
            if not rows:
                return

            anchor_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
            subject = build_subject((anchor_date - timedelta(days=anchor_date.isoweekday() - 1)).strftime("%Y-%m-%d"))

            message = """
<html>
<head><meta charset="utf-8"></head>
<body>
<p>Hello team,</p>
<p>Here is the work item status update for the previous week:</p>
<table border="0" cellspacing="0" cellpadding="5">
"""
            for i, row in enumerate(rows):
                status = row.get("status", "")
                if status == "completed":
                    color = "#00B050"
                elif status == "in_progress":
                    color = "#0070C0"
                else:
                    color = "#E97132"
                message += (
                    "<tr><td>"
                    f"{i + 1}. {row.get('item_name', '')} "
                    f"<a href=\"{row.get('url', '')}\">{row.get('url', '')}</a> "
                    f"(<b><span style=\"color:{color}\">{status}</span></b>)"
                    "</td></tr>"
                )

            message += """
</table>
<br/>
<p><b>Automated Weekly Digest</b></p>
</body>
</html>
"""
            recipients = Variable.get("demo_digest_to_emails", deserialize_json=True)
            cc_recipients = Variable.get("demo_digest_cc_emails", deserialize_json=True)
            msg.send_exchange_message(
                message,
                subject,
                to_recipients=recipients,
                cc_recipients=cc_recipients,
            )
        except Exception as exc:
            msg.notify_on_failure(context, exc)
            raise

    get_weekly_items() >> send_email()


demo_weekly_work_item_digest_email()
