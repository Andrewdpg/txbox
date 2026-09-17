//! PostgreSQL implementation of [`InboxStore`].

use sqlx::migrate::Migrator;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use tracing::Instrument;

use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::store::{BoxFuture, InboxStore};
use crate::types::{Claim, ClaimRequest, ConsumerId, MessageId};

/// PostgreSQL's SQLSTATE for `lock_not_available`, raised when
/// `SET LOCAL lock_timeout` expires while waiting on a contended row.
const LOCK_NOT_AVAILABLE: &str = "55P03";

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

// `set_config` with `is_local = true` is the parameterisable equivalent of
// `SET LOCAL lock_timeout = ...`: `SET` itself does not accept a bind
// parameter over the wire protocol `sqlx` uses, but `set_config` is an
// ordinary function call and does. `is_local = true` is what makes this
// transaction-scoped rather than session-scoped: the setting reverts when
// this transaction ends (commit or rollback), so it can never leak onto the
// next caller of a connection returned to the pool.
const SET_LOCK_TIMEOUT_SQL: &str = "SELECT set_config('lock_timeout', $1, true)";

const KNOWN_DUPLICATE_SQL: &str = "SELECT EXISTS ( \
                                       SELECT 1 FROM inbox_messages \
                                       WHERE consumer_id = $1 AND message_id = $2 \
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
            backend = "postgres"
        );
        Box::pin(
            async move {
                if let Some(timeout) = lock_timeout {
                    // Scoped to this transaction via `is_local = true` — see
                    // the comment on `SET_LOCK_TIMEOUT_SQL`. Must run before
                    // the claim INSERT below, which is the statement it is
                    // meant to bound.
                    sqlx::query(SET_LOCK_TIMEOUT_SQL)
                        .bind(format!("{}ms", timeout.as_millis()))
                        .execute(&mut *conn)
                        .await?;
                }

                let result = sqlx::query(CLAIM_SQL)
                    .bind(consumer.as_str())
                    .bind(id.as_str())
                    .execute(&mut *conn)
                    .await;

                let affected = match result {
                    Ok(done) => done.rows_affected(),
                    Err(sqlx::Error::Database(db_err))
                        if db_err.code().as_deref() == Some(LOCK_NOT_AVAILABLE) =>
                    {
                        return Err(InboxError::Contended);
                    }
                    Err(e) => return Err(e.into()),
                };

                Ok(if affected == 1 {
                    Claim::Fresh
                } else {
                    Claim::Duplicate
                })
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
            backend = "postgres"
        );
        Box::pin(
            async move {
                // Deliberately on the pool rather than in a transaction: the
                // point is to answer without opening one. A row read here is
                // committed, which is what makes the `true` answer safe.
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
