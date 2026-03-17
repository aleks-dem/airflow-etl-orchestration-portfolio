# Security Policy

## Supported Versions

This repository is a portfolio demo and is maintained on a best-effort basis.
Security fixes are applied to the latest `main` branch state.

## Reporting a Vulnerability

If you discover a security issue:

1. Do **not** open a public issue with exploit details.
2. Send a private report to the repository owner via email.
3. Include reproduction steps, impact, and affected files/components.

You should receive an acknowledgment within 3 business days.

## Scope

Typical in-scope findings:

- Credential leaks in code, docs, or history
- Injection risks (SQL, command, template)
- Unsafe handling of secrets in DAG/runtime configuration
- Misconfigured GitHub Actions or release pipeline

Out of scope:

- Issues requiring access to private infrastructure not included in this repo
- Third-party package vulnerabilities without a working impact path in this codebase

## Security Hardening Notes

- Keep all credentials in Airflow Variables or external secret stores.
- Never commit key files or `.env` content.
- Rotate tokens before reusing this code in real environments.
- Run dependency and secret scans in CI before release.
