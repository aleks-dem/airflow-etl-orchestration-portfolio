import os
from ftplib import FTP
from io import BytesIO, StringIO
from pathlib import Path
from typing import Union

import clickhouse_connect
import pandas as pd
from airflow.models import Variable


def get_clickhouse_client(
    connection_var: str = "clickhouse_connection",
    send_receive_timeout: int = 43200,
    connect_timeout: int = 600,
    query_limit: int = 10_000_000,
):
    creds = Variable.get(connection_var, deserialize_json=True)
    client = clickhouse_connect.get_client(
        host=creds["host"],
        username=creds["user"],
        password=creds["password"],
        port=creds.get("port", 8443),
        secure=creds.get("secure", True),
        send_receive_timeout=send_receive_timeout,
        connect_timeout=connect_timeout,
        query_limit=query_limit,
    )
    client.command("SET http_receive_timeout = 0")
    client.command("SET max_execution_time = 0")
    client.command("SET http_wait_end_of_query = 1")
    return client


def get_clickhouse_df(query: str, client):
    result = client.query(query)
    return pd.DataFrame(data=result.result_set, columns=result.column_names)


def insert_df_to_dwh(
    df: pd.DataFrame,
    db: str,
    table: str,
    batch_size: int,
    trunc: bool = False,
    connection_var: str = "clickhouse_connection",
    cluster_name_var: str = "clickhouse_cluster_name",
):
    if df is None or df.empty:
        print(f"[insert_df_to_dwh] Skip insert: {db}.{table} dataframe is empty")
        return

    client = get_clickhouse_client(connection_var=connection_var)
    try:
        if trunc:
            cluster_name = Variable.get(cluster_name_var, default_var="")
            if cluster_name:
                client.command(
                    f"TRUNCATE TABLE {db}_sharded.{table} ON CLUSTER {cluster_name} SETTINGS alter_sync=2"
                )
            else:
                client.command(f"TRUNCATE TABLE {db}.{table}")
            print(f"[insert_df_to_dwh] Table {db}.{table} truncated")

        num_batches = (len(df) + batch_size - 1) // batch_size
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(df))
            batch_data = df.iloc[start_idx:end_idx]
            if batch_data.empty:
                continue
            client.insert_df(
                table=f"{db}.{table}",
                df=batch_data,
                settings={"insert_distributed_sync": 1},
            )
            print(
                f"[insert_df_to_dwh] Inserted batch {i + 1}/{num_batches}, rows={len(batch_data):,}"
            )
    finally:
        client.close()


def get_sql_script(sql_path: str):
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return path.read_text(encoding="utf8")


def _build_ftp_path(folder_path: str, file_name: str) -> str:
    folder = (folder_path or "").strip().strip("/")
    return f"/{folder}/{file_name}" if folder else f"/{file_name}"


def upload_to_ftp(
    input_data: Union[pd.DataFrame, str],
    output_type: str = None,
    ftp_cred_variable: str = "ftp_connection",
    ftp_folder_path: str = "",
    file_name: str = "output",
):
    ftp_connect_dict = Variable.get(ftp_cred_variable, deserialize_json=True)
    ftp_host = ftp_connect_dict["host"]
    ftp_user = ftp_connect_dict["user"]
    ftp_password = ftp_connect_dict["password"]

    try:
        with FTP(ftp_host) as ftp:
            ftp.login(user=ftp_user, passwd=ftp_password)
            print(f"[upload_to_ftp] Connected to FTP: {ftp_host}")

            if isinstance(input_data, pd.DataFrame):
                if not output_type:
                    raise ValueError(
                        "Parameter 'output_type' is mandatory for DataFrame input (csv or xlsx)"
                    )
                buffer = StringIO() if output_type == "csv" else BytesIO()
                if output_type == "xlsx":
                    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                        input_data.to_excel(writer, index=False, sheet_name="Sheet1")
                    buffer.seek(0)
                    ftp_file_path = _build_ftp_path(ftp_folder_path, f"{file_name}.xlsx")
                elif output_type == "csv":
                    input_data.to_csv(buffer, index=False)
                    buffer.seek(0)
                    ftp_file_path = _build_ftp_path(ftp_folder_path, f"{file_name}.csv")
                else:
                    raise ValueError("Invalid output_type. Allowed values: 'csv', 'xlsx'")

                payload = (
                    buffer
                    if isinstance(buffer, BytesIO)
                    else BytesIO(buffer.getvalue().encode("utf-8"))
                )
                ftp.storbinary(f"STOR {ftp_file_path}", payload)
                print(f"[upload_to_ftp] Uploaded dataframe file: {ftp_file_path}")
            elif isinstance(input_data, str):
                ext = os.path.splitext(input_data)[-1]
                ftp_file_path = _build_ftp_path(ftp_folder_path, f"{file_name}{ext}")
                with open(input_data, "rb") as file:
                    ftp.storbinary(f"STOR {ftp_file_path}", file)
                print(f"[upload_to_ftp] Uploaded local file: {ftp_file_path}")
            else:
                raise TypeError("input_data must be DataFrame or file path")
    except Exception as exc:
        print(f"[upload_to_ftp] Error: {exc}")
        raise


def get_google_services(
    dag_directory: str,
    key_file_variable: str = "google_connection_filename",
    delegated_user_variable: str = "google_delegated_user",
):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    key_name = Variable.get(key_file_variable)
    delegated_user = Variable.get(delegated_user_variable)
    service_account_file = f"{dag_directory}/{key_name}"

    creds = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=scopes,
        subject=delegated_user,
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


