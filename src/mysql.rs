//! MySQL implementation of [`InboxStore`].
//!
//! Needs MySQL 8.0.17+ (`utf8mb4_0900_bin`). The table compares ids byte for
//! byte: MySQL's default collation ignores case and accents, and the older
//! `utf8mb4_bin` pads with spaces (`'a' = 'a '`), both of which would merge
//! distinct message ids.

use std::time::Duration;

use sqlx::migrate::Migrator;
use sqlx::mysql::MySqlDatabaseError;
use sqlx::types::Json;
use sqlx::{MySql, MySqlConnection, MySqlPool, Transaction};
use tracing::Instrument;

use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::sql::{self, RELEASE_AND_SAVEPOINT_SQL, RELEASE_SQL, ROLLBACK_TO_SQL, SAVEPOINT_SQL};
use crate::store::{BoxFuture, InboxStore, LockTimeout, Savepoints};
use crate::types::{Claim, ClaimBatch, ClaimRequest, ConsumerId, MessageId};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/mysql");

/// The exact DDL `migrate()` applies on MySQL. Sourced with `include_str!`
/// from the same file `MIGRATOR` runs.
pub const MIGRATION_SQL: &str =
    include_str!("../migrations/mysql/20260926000001_create_inbox_messages.sql");

/// `ER_LOCK_WAIT_TIMEOUT`: `innodb_lock_wait_timeout` expired.
const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// `ER_LOCK_DEADLOCK`: this transaction was the deadlock victim.
const ER_LOCK_DEADLOCK: u16 = 1213;

// A lock timeout is a session variable: it outlives the statement and the
// transaction that set it (MySQL ignores the per-statement SET_VAR hint for
// it), and the pool may be shared with code that isn't txbox. So a claim
// saves the session's own value, sets its timeout, and restores the saved
// value right after the statement, on success or error. A claim cancelled in
// between leaves the saved value behind, and the next BEGIN puts it back in
// the same round-trip; with nothing saved, BEGIN changes nothing, so a value
// the application set in `after_connect` survives. The casts are needed:
// a user variable once set to NULL no longer passes as an integer.
const BEGIN_SQL: &str = "SET SESSION innodb_lock_wait_timeout = \
                             CAST(COALESCE(@txbox_lock_wait_timeout, @@SESSION.innodb_lock_wait_timeout) AS UNSIGNED), \
                         @txbox_lock_wait_timeout = NULL; \
                         BEGIN";
const SET_LOCK_TIMEOUT_SQL: &str = "SET @txbox_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout, \
                                    SESSION innodb_lock_wait_timeout = ?";
const RESTORE_LOCK_TIMEOUT_SQL: &str = "SET SESSION innodb_lock_wait_timeout = CAST(@txbox_lock_wait_timeout AS UNSIGNED), \
                                        @txbox_lock_wait_timeout = NULL";

// `INSERT IGNORE`, not a no-op `ON DUPLICATE KEY UPDATE`: sqlx always sets
// CLIENT_FOUND_ROWS, under which the latter reports one affected row for a
// duplicate as well as for a fresh insert. IGNORE reports 1 and 0. The errors
// IGNORE would downgrade (length, nulls) are ruled out by id validation, and
// lock errors still raise.
const CLAIM_SQL: &str = "INSERT IGNORE INTO inbox_messages (consumer_id, message_id, processed_at) \
                         VALUES (?, ?, UTC_TIMESTAMP(6))";

// MySQL has no RETURNING. Fresh rows are stamped with a token unique to this
// call and read back through the primary key. The token must never repeat:
// if two calls shared one, a row the first claimed would read as fresh to
// the second. `ORDER BY` pins the lock order, as on PostgreSQL.
const CLAIM_MANY_SQL: &str = "INSERT INTO inbox_messages (consumer_id, message_id, processed_at, claim_token) \
     SELECT ?, j.id, UTC_TIMESTAMP(6), ? \
     FROM JSON_TABLE(?, '$[*]' COLUMNS (id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PATH '$')) AS j \
     ORDER BY j.id \
     ON DUPLICATE KEY UPDATE consumer_id = inbox_messages.consumer_id";

const CLAIMED_BY_TOKEN_SQL: &str = "SELECT m.message_id \
     FROM JSON_TABLE(?, '$[*]' COLUMNS (id VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PATH '$')) AS j \
     JOIN inbox_messages m ON m.consumer_id = ? AND m.message_id = j.id \
     WHERE m.claim_token = ?";

const UNCLAIM_SQL: &str = "DELETE FROM inbox_messages WHERE consumer_id = ? AND message_id = ?";

const PURGE_SQL: &str = "DELETE FROM inbox_messages \
                         WHERE processed_at < UTC_TIMESTAMP(6) - INTERVAL ? MICROSECOND \
                         ORDER BY processed_at LIMIT ?";

const KNOWN_DUPLICATE_SQL: &str = "SELECT EXISTS ( \
                                       SELECT 1 FROM inbox_messages \
                                       WHERE consumer_id = ? AND message_id = ? \
                                   )";

/// `innodb_lock_wait_timeout` for `timeout`: whole seconds, rounded up, at
/// least 1 (MySQL stores 0 as 1 anyway) and at most MySQL's maximum.
fn mysql_lock_timeout(timeout: Duration) -> u64 {
    let secs = timeout.as_nanos().div_ceil(1_000_000_000).max(1);
    u64::try_from(secs).unwrap_or(u64::MAX).min(1_073_741_824)
}

async fn set_lock_timeout(
    conn: &mut MySqlConnection,
    timeout: Option<Duration>,
) -> Result<(), sqlx::Error> {
    if let Some(timeout) = timeout {
        sqlx::query(SET_LOCK_TIMEOUT_SQL)
            .bind(mysql_lock_timeout(timeout))
            .execute(conn)
            .await?;
    }
    Ok(())
}

async fn restore_lock_timeout(
    conn: &mut MySqlConnection,
    timeout: Option<Duration>,
) -> Result<(), sqlx::Error> {
    if timeout.is_some() {
        sqlx::raw_sql(RESTORE_LOCK_TIMEOUT_SQL)
            .execute(conn)
            .await?;
    }
    Ok(())
}

/// Both errors mean another consumer holds the row: retryable contention,
/// not a backend failure.
fn claim_error(e: sqlx::Error) -> InboxError {
    if let sqlx::Error::Database(db) = &e
        && let Some(mysql) = db.try_downcast_ref::<MySqlDatabaseError>()
        && matches!(mysql.number(), ER_LOCK_WAIT_TIMEOUT | ER_LOCK_DEADLOCK)
    {
        return InboxError::Contended;
    }
    e.into()
}

/// An inbox backed by MySQL.
#[derive(Debug, Clone)]
pub struct MySqlInbox {
    pool: MySqlPool,
}

impl MySqlInbox {
    /// Wraps an existing pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Borrows the underlying pool.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Applies this crate's migrations.
    ///
    /// Call it explicitly, from your deployment path. A library must never
    /// alter a production schema on its own at startup.
    pub async fn migrate(&self) -> Result<(), InboxError> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| InboxError::Backend(Box::new(e)))
    }
}

impl InboxStore for MySqlInbox {
    type Conn = MySqlConnection;
    type Tx = Transaction<'static, MySql>;

    fn begin(&self) -> BoxFuture<'_, Result<Self::Tx, InboxError>> {
        Box::pin(async move { Ok(self.pool.begin_with(BEGIN_SQL).await?) })
    }

    fn commit(&self, tx: Self::Tx) -> BoxFuture<'_, Result<(), InboxError>> {
        Box::pin(async move { Ok(tx.commit().await?) })
    }

    fn rollback(&self, tx: Self::Tx) -> BoxFuture<'_, Result<(), InboxError>> {
        Box::pin(async move { Ok(tx.rollback().await?) })
    }

    fn claim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        request: ClaimRequest<'a>,
    ) -> BoxFuture<'a, Result<Claim, InboxError>> {
        let ClaimRequest {
            consumer,
            id,
            lock_timeout,
        } = request;
        let span = tracing::debug_span!(
            "inbox.claim",
            consumer = %consumer,
            message_id = %id,
            backend = "mysql"
        );
        Box::pin(
            async move {
                set_lock_timeout(&mut *conn, lock_timeout).await?;
                let result = sqlx::query(CLAIM_SQL)
                    .bind(consumer.as_str())
                    .bind(id.as_str())
                    .execute(&mut *conn)
                    .await;
                // Restored on the error path too; the claim's error wins.
                let restored = restore_lock_timeout(&mut *conn, lock_timeout).await;
                let affected = result.map_err(claim_error)?.rows_affected();
                restored?;
                Ok(if affected == 1 {
                    Claim::Fresh
                } else {
                    Claim::Duplicate
                })
            }
            .instrument(span),
        )
    }

    fn claim_many<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        batch: ClaimBatch<'a>,
    ) -> BoxFuture<'a, Result<Vec<Claim>, InboxError>> {
        let span = tracing::debug_span!(
            "inbox.claim_many",
            consumer = %batch.consumer,
            count = batch.ids.len(),
            backend = "mysql"
        );
        Box::pin(
            async move {
                let token = getrandom::u64().map_err(|e| InboxError::Backend(Box::new(e)))?;
                let ids: Vec<&str> = batch.ids.iter().map(|id| id.as_str()).collect();
                set_lock_timeout(&mut *conn, batch.lock_timeout).await?;
                let result = sqlx::query(CLAIM_MANY_SQL)
                    .bind(batch.consumer.as_str())
                    .bind(token)
                    .bind(Json(&ids))
                    .execute(&mut *conn)
                    .await;
                let restored = restore_lock_timeout(&mut *conn, batch.lock_timeout).await;
                result.map_err(claim_error)?;
                restored?;
                let fresh: Vec<String> = sqlx::query_scalar(CLAIMED_BY_TOKEN_SQL)
                    .bind(Json(&ids))
                    .bind(batch.consumer.as_str())
                    .bind(token)
                    .fetch_all(&mut *conn)
                    .await?;
                Ok(sql::claims_from_fresh(&ids, &fresh))
            }
            .instrument(span),
        )
    }

    fn is_known_duplicate<'a>(
        &'a self,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<bool, InboxError>> {
        let span = tracing::debug_span!(
            "inbox.is_known_duplicate",
            consumer = %consumer,
            message_id = %id,
            backend = "mysql"
        );
        Box::pin(
            async move {
                let known: bool = sqlx::query_scalar(KNOWN_DUPLICATE_SQL)
                    .bind(consumer.as_str())
                    .bind(id.as_str())
                    .fetch_one(&self.pool)
                    .await?;
                Ok(known)
            }
            .instrument(span),
        )
    }

    fn purge<'a>(&'a self, policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async move {
            let max_age = u64::try_from(policy.max_age().as_micros()).unwrap_or(u64::MAX);
            let batch = policy.batch_size();

            let mut total = 0u64;
            loop {
                let affected = sqlx::query(PURGE_SQL)
                    .bind(max_age)
                    .bind(batch)
                    .execute(&self.pool)
                    .await?
                    .rows_affected();

                total += affected;
                if affected < u64::from(batch) {
                    break;
                }
            }
            Ok(total)
        })
    }
}

impl LockTimeout for MySqlInbox {}

impl Savepoints for MySqlInbox {
    fn savepoint<'a>(&'a self, conn: &'a mut Self::Conn) -> BoxFuture<'a, Result<(), InboxError>> {
        sql::execute(conn, SAVEPOINT_SQL)
    }

    fn release_and_savepoint<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        sql::execute(conn, RELEASE_AND_SAVEPOINT_SQL)
    }

    fn release<'a>(&'a self, conn: &'a mut Self::Conn) -> BoxFuture<'a, Result<(), InboxError>> {
        sql::execute(conn, RELEASE_SQL)
    }

    fn rollback_to<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        sql::execute(conn, ROLLBACK_TO_SQL)
    }

    fn unclaim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async move {
            sqlx::query(UNCLAIM_SQL)
                .bind(consumer.as_str())
                .bind(id.as_str())
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::mysql_lock_timeout;

    #[test]
    fn lock_timeout_rounds_up_to_whole_seconds_and_never_zero() {
        assert_eq!(mysql_lock_timeout(Duration::ZERO), 1);
        assert_eq!(mysql_lock_timeout(Duration::from_millis(1)), 1);
        assert_eq!(mysql_lock_timeout(Duration::from_millis(1001)), 2);
        assert_eq!(mysql_lock_timeout(Duration::from_secs(30)), 30);
        assert_eq!(mysql_lock_timeout(Duration::MAX), 1_073_741_824);
    }
}
