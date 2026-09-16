//! SQLite implementation of [`InboxStore`].

use chrono::Utc;
use sqlx::migrate::Migrator;
use sqlx::{Sqlite, SqliteConnection, SqlitePool, Transaction};
use tracing::Instrument;

use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::store::{BoxFuture, InboxStore};
use crate::types::{Claim, ConsumerId, MessageId};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");

const CLAIM_SQL: &str = "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
                         VALUES (?, ?, ?) \
                         ON CONFLICT (consumer_id, message_id) DO NOTHING";

// Unlike the PostgreSQL backend, this one keeps taking the timestamp in the
// caller's process. There is nowhere else to take it from: SQLite runs inside
// that process, so its clock *is* the caller's clock and the skew between
// replicas that motivates `now()` on PostgreSQL cannot arise here.
//
// `rowid` addresses the row directly, and `ORDER BY processed_at` takes the
// oldest rows first so repeated batches move forward predictably.
const PURGE_SQL: &str = "DELETE FROM inbox_messages \
                         WHERE rowid IN ( \
                             SELECT rowid FROM inbox_messages \
                             WHERE processed_at < ? \
                             ORDER BY processed_at LIMIT ? \
                         )";

/// An inbox backed by SQLite.
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

    fn claim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<Claim, InboxError>> {
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
                    .await?
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

    fn purge<'a>(&'a self, policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async move {
            let cutoff = Utc::now()
                - chrono::Duration::from_std(policy.max_age())
                    .map_err(|e| InboxError::Backend(Box::new(e)))?;

            let mut total = 0u64;
            loop {
                let affected = sqlx::query(PURGE_SQL)
                    .bind(cutoff)
                    .bind(i64::from(policy.batch_size()))
                    .execute(&self.pool)
                    .await?
                    .rows_affected();

                total += affected;
                if affected < u64::from(policy.batch_size()) {
                    break;
                }
            }
            Ok(total)
        })
    }
}
