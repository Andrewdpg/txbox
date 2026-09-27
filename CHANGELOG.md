# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project is pre-1.0: breaking changes may land in any minor release.

## Unreleased

### Added

- `LockTimeout` marker trait for backends that enforce a claim lock timeout.

### Changed

- `Consumer::with_lock_timeout` now requires the backend to implement the
  new `LockTimeout` trait. It used to compile on SQLite and silently do
  nothing; now it doesn't compile there. Generic code that calls it needs an
  `S: LockTimeout` bound.
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
