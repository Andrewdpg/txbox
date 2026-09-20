# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project is pre-1.0: breaking changes may land in any minor release.

## Unreleased

### Added

### Changed

### Removed

### Fixed

## 0.1.0 - 2026-09-19

- Transactional inbox pattern: commits a processed-message record in the
  same database transaction as the handler's effect.
- PostgreSQL and SQLite backends behind the `postgres` and `sqlite` features.
- `Consumer::process` for one-message-at-a-time handling, plus `claim`/`begin`/`commit`
  for batching several claims into one transaction.
- Optional claim lock timeout (`with_lock_timeout`), returning `InboxError::Contended`
  instead of blocking on a contended row.
- `RetentionPolicy` and `InboxStore::purge` for pruning old inbox rows.
- Validated `ConsumerId` and `MessageId` newtypes, with `MessageId::scoped`
  for cross-producer disambiguation.
