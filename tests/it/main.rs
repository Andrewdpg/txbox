// One test binary: the suites link once and run in parallel.
mod common;
mod conformance;
mod mysql_store;
mod postgres_concurrency;
mod postgres_store;
mod runner_semantics;
mod sqlite_store;
mod tracing_levels;
