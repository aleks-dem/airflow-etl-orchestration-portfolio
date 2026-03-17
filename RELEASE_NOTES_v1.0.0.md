# v1.0.0 - Public Portfolio Baseline

## Highlights

- First public release of an anonymized Airflow ETL orchestration portfolio project.
- Demonstrates production-style patterns across ingestion, SQL transformation, and operational alerting.
- Includes release-ready repository hygiene and dependency governance.

## What's Included

### Data Orchestration

- Incremental external-system collector with signed API requests and retry logic.
- Sensor-gated dataset refresh and DAG-to-DAG triggers.
- SQL orchestration with source watermark checks.
- Cloud spreadsheet ingestion with schema validation and quality issue reporting.

### Monitoring and Alerts

- Work-item status polling with change notifications.
- Hourly metric anomaly notifications.
- Weekly work-item digest email generation.

### Engineering Quality and Public-Repo Readiness

- Neutralized domain and vendor-specific naming in code and DAGs.
- Generic REST integrations for tracker/chat communication.
- JSON-safe XCom payload handling for cross-task reliability.
- Added CI (`.github/workflows/ci.yml`) and release workflow (`.github/workflows/release.yml`).
- Added `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`, `RELEASE_CHECKLIST.md`, and `CHANGELOG.md`.
- Added dependency governance assets: `requirements.in`, `requirements.lock.txt`, `scripts/check_licenses.py`.

## Breaking Changes

- DAG IDs and variable names were generalized for stronger anonymization.

## Known Limitations

- Some downstream DAG triggers are intentionally out-of-scope for this public demo.
- Runtime still requires Airflow Variables and external systems configured by the user.
