# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

### Changed

### Fixed

### Security

## [1.0.0] - 2026-03-17

### Added
- Public portfolio-ready Airflow ETL demo structure and documentation.
- CI jobs for syntax checks, DAG integration tests, and dependency governance.
- Security, contributing, and release checklist documents.

### Changed
- Deep anonymization of DAG names, table names, variable names, and user-facing labels.
- Replaced vendor-specific tracker/chat integrations with generic REST-based integrations.
- Added lockfile inputs (`requirements.in`, `requirements.lock.txt`) and license-policy script.

### Fixed
- Removed implicit dependency on dictionary key order in API signing flow.
- Fixed SQL casting syntax in metric alert query.
- Prevented duplicate accumulation in dataset refresh target by truncating before reload.
- Made FTP upload utility fail-fast instead of swallowing exceptions.

### Security
- Confirmed no real credentials are committed.
- Added automated transitive license policy checks in CI.
