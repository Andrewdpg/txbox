# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project is pre-1.0: breaking changes may land in any minor release.

## Unreleased

### Added

- `LockTimeout` marker trait for backends that enforce a claim lock timeout.
- `InboxStore::rollback` (default: drop the transaction) and
  `Consumer::rollback`. `process` now rolls back explicitly when the
  handler fails.
- `testing` feature with `txbox::testing::conformance`, a suite backend
  authors run against their own `InboxStore`.
- `Consumer::claim_many` and `InboxStore::claim_many`, with `ClaimBatch`.
- `Consumer::process_many` over the new `Savepoints` trait, returning one
  `ProcessResult<T>` per message.
- `txbox::testing::savepoints_conformance`.
- MySQL backend behind the `mysql` feature (MySQL 8.0.17+), with
  `claim_many`, lock timeouts (whole seconds) and `process_many`.

### Changed

- `Consumer::with_lock_timeout` now requires `S: LockTimeout`. It used to
  compile on SQLite and silently do nothing; now it doesn't compile there.
- PostgreSQL: a deadlock victim (`40P01`) during a claim now returns
  `InboxError::Contended` instead of `InboxError::Backend`.
- `InboxError::Contended` now also covers deadlock victims, and its message
  reads "another consumer holds the row".
- The `sqlite` feature enables sqlx's `json` feature.
- SQLite: a claim refused with `SQLITE_BUSY` or `SQLITE_LOCKED` now returns
  `InboxError::Contended` instead of `InboxError::Backend`, like the other
  backends, and `is_known_duplicate` answers instead of always `false`.
- `sqlx` is now optional and only pulled in by a backend feature. The tokio
  runtime moved to a `runtime-tokio` feature, on by default. If you set
  `default-features = false`, add `features = ["runtime-tokio", ...]` or
  enable another sqlx runtime, or sqlx panics at first use.

### Removed

### Fixed

- PostgreSQL: a zero or sub-millisecond `with_lock_timeout` was sent as
  `0ms`, which PostgreSQL reads as "no timeout", so the claim blocked. It
  now rounds up to at least `1ms`.

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
