import json
import os
from datetime import datetime, timezone

import requests
from airflow.models import Variable


def send_exchange_message(
    msg: str,
    subject: str,
    to_recipients: list[str],
    cc_recipients: list[str] | None = None,
    attachments=None,
):
    from exchangelib import DELEGATE, Account, Configuration, Credentials, HTMLBody, Message

    email_creds = Variable.get("smtp_mail_creds", deserialize_json=True)
    smtp_server = email_creds.get("server")
    smtp_account = email_creds.get("mailbox")
    if not smtp_server or not smtp_account:
        raise ValueError(
            "smtp_mail_creds must contain 'server' and 'mailbox' keys in Airflow Variable"
        )

    creds = Credentials(username=email_creds["user"], password=email_creds["password"])
    mail_config = Configuration(server=smtp_server, credentials=creds)
    account = Account(
        primary_smtp_address=smtp_account,
        autodiscover=False,
        config=mail_config,
        access_type=DELEGATE,
    )
    message = Message(
        account=account,
        subject=subject,
        body=HTMLBody(msg),
        to_recipients=to_recipients,
        cc_recipients=cc_recipients,
    )
    if attachments:
        for attachment in attachments:
            message.attach(attachment)
    message.send_and_save()

def _get_chat_creds() -> dict:
    return Variable.get("team_chat_creds", deserialize_json=True)


def _chat_base_url(creds: dict) -> str:
    return (creds.get("base_url") or creds.get("host") or "").rstrip("/")


def _chat_session(creds: dict) -> requests.Session:
    session = requests.Session()
    token = creds.get("token", "")
    user_id = creds.get("user", "")
    if token:
        session.headers.update({"Authorization": f"Bearer {token}"})
    if user_id:
        session.headers.update({"X-User-Id": user_id})
    return session


def _raise_for_status(resp: requests.Response, where: str) -> None:
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"{where} failed: {resp.status_code} {resp.text}")


def send_team_chat_message(
    message: str,
    channel_id: str,
    file_path: str | None = None,
    description: str | None = None,
):
    creds = _get_chat_creds()
    base = _chat_base_url(creds)
    session = _chat_session(creds)
    message_endpoint = creds.get("message_endpoint", "/api/v1/messages")
    upload_endpoint = creds.get("upload_endpoint", "/api/v1/files")

    if not base:
        raise ValueError("team_chat_creds must define 'base_url' or 'host'")

    if not file_path:
        payload = {"channel_id": channel_id, "text": message}
        resp = session.post(f"{base}{message_endpoint}", json=payload, timeout=30)
        _raise_for_status(resp, "send_message")
        return resp.json() if resp.content else {}

    filename = os.path.basename(file_path)
    with open(file_path, "rb") as file_stream:
        files = {"file": (filename, file_stream)}
        data = {
            "channel_id": channel_id,
            "text": message,
        }
        if description is not None:
            data["description"] = description
        resp = session.post(
            f"{base}{upload_endpoint}",
            files=files,
            data=data,
            timeout=120,
        )

    _raise_for_status(resp, "upload_file")
    return resp.json() if resp.content else {}


def _get_fallback_recipients():
    value = Variable.get("fallback_failure_emails", default_var="[]")
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return []


def notify_on_failure(context, e=""):
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    dag_id = dag_run.dag_id if dag_run else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    owner = dag.owner if dag else "unknown_owner"
    airflow_base_url = Variable.get("airflow_webserver_url", default_var="http://localhost:8080")
    dag_url = f"{airflow_base_url}/dags/{dag_id}/grid?tab=graph"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    err_text = str(e) if e else "Task failure callback triggered"

    channel_id = context.get("params", {}).get("owner_channel_id")
    if not channel_id:
        channel_id = Variable.get("default_owner_channel_id", default_var="")

    markdown_message = (
        f"*{now}*\n"
        "DAG run failed. Details:\n"
        f"*DAG Id:* <{dag_url}|{dag_id}>\n"
        f"*Owner:* {owner}\n"
        f"*Task:* {task_id}\n"
        f"*Error:* {err_text}"
    )

    if channel_id:
        try:
            send_team_chat_message(message=markdown_message, channel_id=channel_id)
            return
        except Exception as notify_exc:
            print(f"[notify_on_failure] Team chat notification failed: {notify_exc}")

    fallback_emails = _get_fallback_recipients()
    if not fallback_emails:
        print("[notify_on_failure] No fallback email recipients configured")
        return

    html = (
        "<html><body>"
        f"<p><b>{now}</b></p>"
        "<p>DAG run failed. Details:</p>"
        f"<p><b>DAG Id:</b> <a href='{dag_url}'>{dag_id}</a></p>"
        f"<p><b>Owner:</b> {owner}</p>"
        f"<p><b>Task:</b> {task_id}</p>"
        f"<p><b>Error:</b> {err_text}</p>"
        "</body></html>"
    )
    send_exchange_message(
        html,
        f"DAG failure - {dag_id}",
        to_recipients=fallback_emails,
    )
