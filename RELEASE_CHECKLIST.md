# Release Checklist

## Pre-release

- [ ] `main` is green in CI
- [ ] No high/critical dependency vulnerabilities
- [ ] No secrets detected in repository scan
- [ ] `README.md` matches current behavior
- [ ] `CHANGELOG.md` updated
- [ ] `requirements.lock.txt` refreshed from `requirements.in` (Python 3.11 environment)
- [ ] License policy check passes (`scripts/check_licenses.py`)

## Versioning

- [ ] Choose SemVer version (`vMAJOR.MINOR.PATCH`)
- [ ] Create tag from `main`
- [ ] Draft GitHub Release notes

## Verification

- [ ] DAG files compile (`python -m py_compile dags/*.py utils/*.py`)
- [ ] Required Airflow Variables documented
- [ ] Release artifacts and links are correct

## Post-release

- [ ] Announce release summary
- [ ] Start next `Unreleased` section in `CHANGELOG.md`
