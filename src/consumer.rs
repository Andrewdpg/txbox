use std::time::Duration;

use tracing::Instrument;

use crate::error::{HandlerError, InboxError};
use crate::store::{BoxFuture, InboxStore};
use crate::types::{Claim, ClaimRequest, ConsumerId, MessageId, Outcome};

/// One logical consumer of one stream.
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
    /// Unset by default: a consumer that loses the claim race blocks until
    /// the winner's transaction resolves. On PostgreSQL, setting this issues
    /// `SET LOCAL lock_timeout` before the claim, so losing the race returns
    /// [`InboxError::Contended`] instead of blocking.
    ///
    /// No-op on SQLite — there's no per-transaction lock timeout there; use
    /// `SqliteConnectOptions::busy_timeout` on the pool instead.
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }
}

impl<S: InboxStore> Consumer<S> {
    /// Reports whether this consumer has already recorded `id`, without
    /// opening a transaction. `false` means unknown, not fresh — see
    /// [`InboxStore::is_known_duplicate`]. `process` never calls this.
    pub fn is_known_duplicate<'a>(
        &'a self,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<bool, InboxError>> {
        self.store.is_known_duplicate(&self.id, id)
    }

    /// Opens a transaction on the backend, for claiming several messages at
    /// once. [`process`](Self::process) commits per message; a batch amortises
    /// that over one commit, at the cost of making the batch a single unit of
    /// failure.
    pub fn begin(&self) -> BoxFuture<'_, Result<S::Tx, InboxError>> {
        self.store.begin()
    }

    /// Commits a transaction opened with [`begin`](Self::begin).
    pub fn commit(&self, tx: S::Tx) -> BoxFuture<'_, Result<(), InboxError>> {
        self.store.commit(tx)
    }

    /// Records `id` for this consumer on `conn`, reporting whether it was new.
    /// Must run on the same connection as the effects it guards.
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
                        // Separate target so duplicate volume can be watched
                        // (RUST_LOG=txbox::duplicate=debug) without enabling
                        // debug logging for the whole crate.
                        tracing::debug!(
                            target: "txbox::duplicate",
                            consumer = %self.id,
                            message_id = %id,
                            "duplicate message skipped"
                        );
                        Ok(Outcome::Duplicate)
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
