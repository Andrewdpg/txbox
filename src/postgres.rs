//! PostgreSQL implementation of [`InboxStore`].

use sqlx::migrate::Migrator;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use tracing::Instrument;

use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::store::{BoxFuture, InboxStore};
use crate::types::{Claim, ConsumerId, MessageId};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/postgres");

// `now()` is the database's clock, and the database is the one clock every
// replica already shares. Retention is a temporal invariant — `max_age` must
// exceed the broker's redelivery window — so a timestamp taken from whichever
// replica happened to write the row is only as trustworthy as that replica's
// clock. A slow one writes rows that look older than they are, and the purge
// deletes them while the broker can still redeliver. Reading the clock here
// removes the failure rather than asking operators to keep NTP healthy.
const CLAIM_SQL: &str = "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
                         VALUES ($1, $2, now()) \
                         ON CONFLICT (consumer_id, message_id) DO NOTHING";

// `ctid` addresses the row directly, so the delete is a fetch by physical
// location rather than a second lookup through the primary key. `ORDER BY
// processed_at` costs nothing — the index on that column already supplies the
// order — and makes each batch take the oldest rows, so repeated passes move
// forward predictably instead of deleting an arbitrary subset each time.
const PURGE_SQL: &str = "DELETE FROM inbox_messages \
                         WHERE ctid IN ( \
                             SELECT ctid FROM inbox_messages \
                             WHERE processed_at < now() - make_interval(secs => $1) \
                             ORDER BY processed_at LIMIT $2 \
                         )";

/// An inbox backed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct PgInbox {
    pool: PgPool,
}

impl PgInbox {
    /// Wraps an existing pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrows the underlying pool.
    pub fn pool(&self) -> &PgPool {
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

impl InboxStore for PgInbox {
    type Conn = PgConnection;
    type Tx = Transaction<'static, Postgres>;

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
            backend = "postgres"
        );
        Box::pin(
            async move {
                let affected = sqlx::query(CLAIM_SQL)
                    .bind(consumer.as_str())
                    .bind(id.as_str())
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
            let max_age = policy.max_age().as_secs_f64();
            let batch = i64::from(policy.batch_size());

            let mut total = 0u64;
            loop {
                let affected = sqlx::query(PURGE_SQL)
                    .bind(max_age)
                    .bind(batch)
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
