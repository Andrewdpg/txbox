use std::time::Duration;

use tracing::Instrument;

use crate::error::{HandlerError, InboxError};
use crate::store::{BoxFuture, InboxStore};
use crate::types::{Claim, ClaimRequest, ConsumerId, MessageId, Outcome};

/// One logical consumer of one stream.
///
/// A `ConsumerId` never varies across the messages a given consumer handles, so
/// it belongs here rather than in every call. Settling it once also settles
/// what used to be kept by discipline: the identity of the stream travelled as
/// an argument while its configuration sat on the store, and nothing stopped
/// one consumer's identifier from being used with another's settings.
///
/// Tuning is per consumer for the same reason. The share of traffic that
/// arrives twice is a property of a queue — an orders topic living through
/// rebalance storms and a payments topic in a healthy steady state want
/// opposite answers — and both routinely share one pool inside one process.
///
/// Cloning is cheap: backends hold a connection pool, which is itself a handle.
#[derive(Debug, Clone)]
pub struct Consumer<S> {
    store: S,
    id: ConsumerId,
    lock_timeout: Option<Duration>,
}

impl<S> Consumer<S> {
    pub(crate) fn new(store: S, id: ConsumerId) -> Self {
        Self {
            store,
            id,
            lock_timeout: None,
        }
    }

    /// The identifier this consumer records messages under.
    pub fn id(&self) -> &ConsumerId {
        &self.id
    }

    /// Borrows the backend this consumer records into.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Bounds how long [`claim`](Self::claim) (and therefore
    /// [`process`](Self::process)) will wait for a contended row before
    /// giving up.
    ///
    /// Unset by default, which preserves today's behaviour: a consumer that
    /// loses the claim race blocks until the winner's transaction resolves,
    /// holding a pool connection for the winner's entire handler duration. On
    /// PostgreSQL, setting this issues `SET LOCAL lock_timeout` inside the
    /// claiming transaction before the claim `INSERT`, so a consumer that
    /// cannot acquire the row within the timeout gets
    /// [`InboxError::Contended`] instead of blocking.
    ///
    /// **This is a no-op on SQLite.** SQLite has no per-transaction lock
    /// timeout; the closest equivalent is connection-level
    /// (`SqliteConnectOptions::busy_timeout`), which belongs to the pool the
    /// caller builds, not to a single consumer's configuration. Setting this
    /// on a SQLite-backed consumer is accepted but has no effect — documented
    /// here rather than silently ignored.
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }
}

impl<S: InboxStore> Consumer<S> {
    /// Reports whether this consumer has already recorded `id`, without
    /// opening a transaction.
    ///
    /// This is a deliberately public, explicitly-callable method for
    /// scenarios like a replay or a backfill, where an operator re-consumes a
    /// topic from the start and wants to skip already-known messages cheaply.
    /// `process` never calls it: under MVCC a plain `SELECT` does not see a
    /// concurrent, uncommitted claim, so it can only ever answer `false` for a
    /// message another consumer is claiming right now — the answer would be
    /// wrong at the exact moment it matters most. That is why `false` means
    /// unknown rather than fresh, exactly as on
    /// [`InboxStore::is_known_duplicate`], and why this can never safely
    /// replace the transactional path in `process`.
    pub fn is_known_duplicate<'a>(
        &'a self,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<bool, InboxError>> {
        self.store.is_known_duplicate(&self.id, id)
    }

    /// Opens a transaction on the backend.
    ///
    /// For claiming several messages at once. [`process`](Self::process)
    /// commits per message; a batch amortises that over one commit, at the cost
    /// of making the batch a single unit of failure. See the crate README.
    pub fn begin(&self) -> BoxFuture<'_, Result<S::Tx, InboxError>> {
        self.store.begin()
    }

    /// Commits a transaction opened with [`begin`](Self::begin).
    pub fn commit(&self, tx: S::Tx) -> BoxFuture<'_, Result<(), InboxError>> {
        self.store.commit(tx)
    }

    /// Records `id` for this consumer on `conn`, reporting whether it was new.
    ///
    /// The statement must run on the same connection as the effects it guards,
    /// or the all-or-nothing guarantee is lost. Taking the consumer's own
    /// identifier rather than accepting one is the point: a batch loop is
    /// exactly where the wrong identifier would otherwise be easy to pass.
    pub fn claim<'a>(
        &'a self,
        conn: &'a mut S::Conn,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<Claim, InboxError>> {
        let request = ClaimRequest {
            lock_timeout: self.lock_timeout,
            ..ClaimRequest::new(&self.id, id)
        };
        self.store.claim(conn, request)
    }

    /// Runs `handler` exactly once for `id`.
    ///
    /// The inbox row and the handler's effects share one transaction. If the
    /// handler fails, both are rolled back and the message stays unprocessed,
    /// so the broker's redelivery will retry it.
    pub fn process<'a, F, T>(
        &'a self,
        id: &'a MessageId,
        handler: F,
    ) -> BoxFuture<'a, Result<Outcome<T>, InboxError>>
    where
        F: FnOnce(&mut S::Conn) -> BoxFuture<'_, Result<T, HandlerError>> + Send + 'a,
        T: Send + 'a,
    {
        let span = tracing::debug_span!("inbox.process", consumer = %self.id, message_id = %id);
        Box::pin(
            async move {
                let mut tx = self.store.begin().await?;
                let request = ClaimRequest {
                    lock_timeout: self.lock_timeout,
                    ..ClaimRequest::new(&self.id, id)
                };

                match self.store.claim(&mut tx, request).await? {
                    Claim::Duplicate => {
                        tracing::debug!(
                            consumer = %self.id,
                            message_id = %id,
                            "duplicate message skipped"
                        );
                        // Dropping the transaction rolls it back. Nothing to keep.
                        Ok(Outcome::Skipped)
                    }
                    Claim::Fresh => {
                        let value = handler(&mut tx).await.map_err(InboxError::Handler)?;
                        self.store.commit(tx).await?;
                        tracing::debug!(
                            consumer = %self.id,
                            message_id = %id,
                            "message processed"
                        );
                        Ok(Outcome::Processed(value))
                    }
                }
            }
            .instrument(span),
        )
    }
}
