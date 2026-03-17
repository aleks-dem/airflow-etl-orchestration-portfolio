# Airflow ETL Portfolio Demo

An anonymized, production-style Airflow ETL portfolio project focused on orchestration patterns, resilient ingestion, SQL pipelines, and operations monitoring.

All identifiers, endpoints, recipients, and credentials are placeholders.

## Core Patterns Demonstrated

- Incremental external-system ingestion with signed API requests
- DAG-to-DAG trigger orchestration
- Sensor-gated execution and source watermark checks
- SQL-stage population in ClickHouse
- Cloud spreadsheet ingestion with schema validation and quality reporting
- Alerting and digest automation via generic communication integrations

## Repository Structure

- `dags/` Airflow DAG definitions
- `sql/demo/` SQL scripts for staged dataset refresh
- `utils/` shared connectors, notifier helpers, and config utilities
- `configs/` non-sensitive config templates
- `tests/dags/` DAG integration tests (`DagBag` import/metadata checks)
- `scripts/` governance and automation scripts

## DAGs

- `demo_external_system_incremental_collector`
- `demo_dataset_refresh_with_sensor`
- `demo_sql_pipeline_orchestration`
- `demo_cloud_sheet_reference_loader`
- `demo_work_item_status_alerts`
- `demo_hourly_metric_notifier`
- `demo_weekly_work_item_digest_email`

## DAG Dependency Graph

```text
demo_dataset_refresh_with_sensor
  -> trigger demo_external_system_incremental_collector
      -> trigger demo_external_system_followup_pipeline (external DAG, not included)

Independent DAGs:
- demo_sql_pipeline_orchestration
- demo_cloud_sheet_reference_loader
- demo_work_item_status_alerts
- demo_hourly_metric_notifier
- demo_weekly_work_item_digest_email
```

## Prerequisites

- Python 3.11+ (recommended for development and CI consistency)
- Apache Airflow 2.9.3 runtime
- ClickHouse access
- Cloud Drive / Spreadsheet API access (for reference-loader DAG)
- Generic work-item tracker REST API (for status-alert DAG)
- Generic team-chat REST API (for alerts)

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Install dev/test/governance dependencies:

```bash
pip install -r requirements-dev.txt
```

## Airflow Variables

Core:

- `etl_directory`
- `clickhouse_connection` (JSON: `host`, `user`, `password`, optional `port`, `secure`)
- `clickhouse_cluster_name` (optional)
- `airflow_webserver_url`

Notifications:

- `team_chat_creds` (JSON: `base_url`, `token`, optional `message_endpoint`, `upload_endpoint`)
- `default_owner_channel_id` (optional)
- `smtp_mail_creds` (JSON: `server`, `mailbox`, `user`, `password`)
- `fallback_failure_emails` (JSON list)

External collector:

- `external_system_config_path` (optional, default `configs/external_system_config.json`)
- `demo_encrypt_key_hex`
- `demo_encrypt_iv_hex`

Work-item tracker DAG:

- `work_item_tracker_api_token`
- `work_item_tracker_base_url`
- `work_item_tracker_workspace_id`

Cloud sheet loader DAG:

- `google_connection_filename`
- `google_delegated_user`
- `demo_reference_google_folder_id`
- `demo_reference_sheet_name` (optional, default `Data`)

Metric notifier DAG:

- `demo_metric_alert_channel_id`
- `demo_metric_source_code`

Weekly digest DAG:

- `demo_digest_to_emails` (JSON list)
- `demo_digest_cc_emails` (JSON list)

## Local Quality Checks

Syntax validation:

```bash
python -m py_compile dags/*.py utils/*.py scripts/*.py
```

DAG integration tests (requires Airflow installed):

```bash
pytest tests/dags/test_dag_integrity.py -q
```

## Dependency Governance

This repo uses three dependency files:

- `requirements.in` direct dependency intent
- `requirements.txt` pinned runtime dependencies
- `requirements.lock.txt` lockfile baseline (regenerate in Python 3.11 for full transitive lock)

Refresh lock file in Python 3.11 environment:

```bash
python -m piptools compile --no-header --strip-extras --output-file requirements.lock.txt requirements.in
```

Transitive license policy check:

```bash
pip-licenses --format=json --with-urls --output-file build/licenses.json
python scripts/check_licenses.py build/licenses.json
```

## CI

`/.github/workflows/ci.yml` includes:

- Syntax checks
- DAG integration tests on Airflow 2.9.3
- Dependency governance (install from lock + transitive license policy)

`/.github/workflows/lockfile-refresh.yml` provides manual lock regeneration in Python 3.11.

## Security and Privacy Notes

- No real credentials are committed.
- No internal domains or company identifiers are included.
- Credentials and endpoints are externalized to Airflow Variables.

See `SECURITY.md` for reporting and hardening guidance.

## Release

- Semantic Versioning (`vMAJOR.MINOR.PATCH`)
- Release workflow: `.github/workflows/release.yml`
- Release notes draft: `RELEASE_NOTES_v1.0.0.md`
- Checklist: `RELEASE_CHECKLIST.md`

## License

This repository is licensed under the MIT License. See `LICENSE`.

For dependency license summary, see `DEPENDENCY_LICENSES.md`.
