# Dependency License Audit

Audit date: 2026-03-17
Source: PyPI metadata for pinned direct dependencies in `requirements.txt`

| Package | Version | License (metadata) | Risk Note |
|---|---:|---|---|
| clickhouse-connect | 0.7.8 | Apache-2.0 | Low |
| cryptography | 44.0.1 | Apache-2.0 OR BSD-3-Clause | Low |
| exchangelib | 5.4.2 | BSD-2-Clause | Low |
| google-api-python-client | 2.169.0 | Apache-2.0 | Low |
| google-auth | 2.39.0 | Apache-2.0 | Low |
| numpy | 1.26.4 | BSD-style (with bundled third-party notices) | Medium (review bundled binary notices when shipping artifacts) |
| openpyxl | 3.1.2 | MIT | Low |
| pandas | 2.2.2 | BSD-3-Clause | Low |
| PyYAML | 6.0.2 | MIT | Low |
| requests | 2.31.0 | Apache-2.0 | Low |
| xlsxwriter | 3.2.0 | BSD-2-Clause | Low |

## Conclusion

No direct copyleft blockers were found for publishing this repository as open source.

## Important Caveat

This table covers direct dependencies. Full transitive license scanning is enforced in CI via `pip-licenses` and policy checks (`scripts/check_licenses.py`).
