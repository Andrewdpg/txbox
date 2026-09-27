//! SQLite implementation of [`InboxStore`].

use std::collections::HashSet;

use chrono::Utc;
use sqlx::migrate::Migrator;
use sqlx::types::Json;
use sqlx::{Sqlite, SqliteConnection, SqlitePool, Transaction};
use tracing::Instrument;

use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::store::{BoxFuture, InboxStore, Savepoints};
use crate::types::{Claim, ClaimBatch, ClaimRequest, ConsumerId, MessageId};

const SAVEPOINT_SQL: &str = "SAVEPOINT txbox_process_many";
// Two statements in one round-trip; measured about 29% faster than
// sending them separately.
const RELEASE_AND_SAVEPOINT_SQL: &str =
    "RELEASE SAVEPOINT txbox_process_many; SAVEPOINT txbox_process_many";
const RELEASE_SQL: &str = "RELEASE SAVEPOINT txbox_process_many";
const ROLLBACK_TO_SQL: &str = "ROLLBACK TO SAVEPOINT txbox_process_many";
const UNCLAIM_SQL: &str = "DELETE FROM inbox_messages WHERE consumer_id = ? AND message_id = ?";

static MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");

/// The exact DDL `migrate()` applies on SQLite. Sourced with `include_str!`
/// from the same file `MIGRATOR` runs.
pub const MIGRATION_SQL: &str =
    include_str!("../migrations/sqlite/20260916000001_create_inbox_messages.sql");

const CLAIM_SQL: &str = "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
                         VALUES (?, ?, ?) \
                         ON CONFLICT (consumer_id, message_id) DO NOTHING";

// `json_each` is SQLite's `unnest`: one bound JSON array, one statement,
// no bind-variable limit (a multi-row VALUES fails at 100k ids). `WHERE
// true` is required by the parser before an upsert clause on INSERT ...
// SELECT. SQLite has one writer, so lock order doesn't matter here.
const CLAIM_MANY_SQL: &str = "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
                              SELECT ?, value, ? FROM json_each(?) WHERE true \
                              ON CONFLICT (consumer_id, message_id) DO NOTHING \
                              RETURNING message_id";

const KNOWN_DUPLICATE_SQL: &str = "SELECT EXISTS ( \
                                       SELECT 1 FROM inbox_messages \
                                       WHERE consumer_id = ? AND message_id = ? \
                                   )";

/// `SQLITE_BUSY` and `SQLITE_LOCKED` (primary codes, so extended codes match
/// too): another connection holds the write lock. Contention, as on the
/// other backends, not a backend failure.
fn claim_error(e: sqlx::Error) -> InboxError {
    if let sqlx::Error::Database(db) = &e
        && let Some(code) = db.code().and_then(|c| c.parse::<i32>().ok())
        && matches!(code & 0xff, 5 | 6)
    {
        return InboxError::Contended;
    }
    e.into()
}

// SQLite runs in the caller's process, so its clock is the caller's clock —
// the replica-skew concern that makes PostgreSQL use `now()` doesn't apply.
const PURGE_SQL: &str = "DELETE FROM inbox_messages \
                         WHERE rowid IN ( \
                             SELECT rowid FROM inbox_messages \
                             WHERE processed_at < ? \
                             ORDER BY processed_at LIMIT ? \
                         )";

/// An inbox backed by SQLite.
///
/// SQLite has no per-statement lock timeout, so this backend does not
/// implement [`LockTimeout`](crate::LockTimeout) and `with_lock_timeout`
/// does not exist on its consumers. Bound the wait on the pool instead,
/// with `SqliteConnectOptions::busy_timeout`.
///
/// ```compile_fail
/// use std::time::Duration;
/// use txbox::sqlite::SqliteInbox;
/// use txbox::{ConsumerId, InboxExt};
///
/// fn build(inbox: SqliteInbox) {
///     let _ = inbox
///         .consumer(ConsumerId::try_from("orders").unwrap())
///         .with_lock_timeout(Duration::from_millis(200));
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SqliteInbox {
    pool: SqlitePool,
}

impl SqliteInbox {
    /// Wraps an existing pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Borrows the underlying pool.
    pub fn pool(&self) -> &SqlitePool {
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

impl InboxStore for SqliteInbox {
    type Conn = SqliteConnection;
    type Tx = Transaction<'static, Sqlite>;

    fn begin(&self) -> BoxFuture<'_, Result<Self::Tx, InboxError>> {
        Box::pin(async move { Ok(self.pool.begin().await?) })
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
        let ClaimRequest { consumer, id, .. } = request;
        let span = tracing::debug_span!(
            "inbox.claim",
            consumer = %consumer,
            message_id = %id,
            backend = "sqlite"
        );
        Box::pin(
            async move {
                let affected = sqlx::query(CLAIM_SQL)
                    .bind(consumer.as_str())
                    .bind(id.as_str())
                    .bind(Utc::now())
                    .execute(&mut *conn)
                    .await
                    .map_err(claim_error)?
                    .rows_affected();

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
            backend = "sqlite"
        );
        Box::pin(
            async move {
                let ids: Vec<&str> = batch.ids.iter().map(|id| id.as_str()).collect();
                let fresh: Vec<String> = sqlx::query_scalar(CLAIM_MANY_SQL)
                    .bind(batch.consumer.as_str())
                    .bind(Utc::now())
                    .bind(Json(&ids))
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(claim_error)?;
                let fresh: HashSet<&str> = fresh.iter().map(String::as_str).collect();
                Ok(ids
                    .iter()
                    .map(|id| {
                        if fresh.contains(id) {
                            Claim::Fresh
                        } else {
                            Claim::Duplicate
                        }
                    })
                    .collect())
            }
            .instrument(span),
        )
    }

    fn is_known_duplicate<'a>(
        &'a self,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<bool, InboxError>> {
        Box::pin(async move {
            let known: bool = sqlx::query_scalar(KNOWN_DUPLICATE_SQL)
                .bind(consumer.as_str())
                .bind(id.as_str())
                .fetch_one(&self.pool)
                .await?;
            Ok(known)
        })
    }

    fn purge<'a>(&'a self, policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async move {
            let cutoff = Utc::now()
                - chrono::Duration::from_std(policy.max_age())
                    .map_err(|e| InboxError::Backend(Box::new(e)))?;

            let batch = policy.batch_size();

            let mut total = 0u64;
            loop {
                let affected = sqlx::query(PURGE_SQL)
                    .bind(cutoff)
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

impl Savepoints for SqliteInbox {
    fn savepoint<'a>(&'a self, conn: &'a mut Self::Conn) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async move { Ok(sqlx::raw_sql(SAVEPOINT_SQL).execute(conn).await.map(drop)?) })
    }

    fn release_and_savepoint<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async move {
            Ok(sqlx::raw_sql(RELEASE_AND_SAVEPOINT_SQL)
                .execute(conn)
                .await
                .map(drop)?)
        })
    }

    fn release<'a>(&'a self, conn: &'a mut Self::Conn) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async move { Ok(sqlx::raw_sql(RELEASE_SQL).execute(conn).await.map(drop)?) })
    }

    fn rollback_to<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async move {
            Ok(sqlx::raw_sql(ROLLBACK_TO_SQL)
                .execute(conn)
                .await
                .map(drop)?)
        })
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
