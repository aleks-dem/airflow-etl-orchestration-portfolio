# Contributing

Thanks for contributing.

## Development Setup

1. Create and activate a Python virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

3. Validate syntax before commit:

```bash
python -m py_compile dags/*.py utils/*.py
```

## Code Standards

- Keep DAGs idempotent and side-effect-aware.
- Use Airflow Variables for runtime configuration.
- Avoid hardcoded credentials, endpoints, and private identifiers.
- Prefer explicit error handling and actionable logs.
- Keep comments and user-facing text in English.
- Keep dependency inputs in `requirements.in` and refresh `requirements.lock.txt` in a Python 3.11 environment.

## Pull Request Checklist

- [ ] No secrets or private business details added
- [ ] DAG imports and Python syntax pass
- [ ] README/Docs updated when behavior changes
- [ ] Changelog updated under `Unreleased`
- [ ] New variables/configs documented
- [ ] License policy check passes (`python scripts/check_licenses.py build/licenses.json`)

## Commit Style

Use clear, imperative commit messages, for example:

- `fix: make task-tracker XCom payload JSON-safe`
- `docs: expand architecture and setup guide`
- `ci: add release workflow`
