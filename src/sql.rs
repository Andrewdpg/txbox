//! Helpers shared by the sqlx backends.

use std::collections::HashSet;

use crate::error::InboxError;
use crate::store::BoxFuture;
use crate::types::Claim;

// One savepoint name, owned by txbox, on every backend.
pub(crate) const SAVEPOINT_SQL: &str = "SAVEPOINT txbox_process_many";
// Release and the next savepoint in one round-trip.
pub(crate) const RELEASE_AND_SAVEPOINT_SQL: &str =
    "RELEASE SAVEPOINT txbox_process_many; SAVEPOINT txbox_process_many";
pub(crate) const RELEASE_SQL: &str = "RELEASE SAVEPOINT txbox_process_many";
pub(crate) const ROLLBACK_TO_SQL: &str = "ROLLBACK TO SAVEPOINT txbox_process_many";

/// Runs a fixed statement on `conn`.
pub(crate) fn execute<'a, C>(
    conn: &'a mut C,
    sql: &'static str,
) -> BoxFuture<'a, Result<(), InboxError>>
where
    C: Send,
    for<'c> &'c mut C: sqlx::Executor<'c>,
{
    Box::pin(async move {
        sqlx::raw_sql(sql).execute(conn).await?;
        Ok(())
    })
}

/// One claim per id, in order: `Fresh` for the ids the insert reported back.
pub(crate) fn claims_from_fresh(ids: &[&str], fresh: &[String]) -> Vec<Claim> {
    let fresh: HashSet<&str> = fresh.iter().map(String::as_str).collect();
    ids.iter()
        .map(|id| {
            if fresh.contains(id) {
                Claim::Fresh
            } else {
                Claim::Duplicate
            }
        })
        .collect()
}
