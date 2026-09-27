use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tracing::Instrument;

use crate::error::{HandlerError, InboxError};
use crate::store::{BoxFuture, InboxStore, LockTimeout, Savepoints};
use crate::types::{Claim, ClaimBatch, ClaimRequest, ConsumerId, MessageId, Outcome};

/// What [`Consumer::process_many`] reports for one message: its outcome, or
/// the error its handler returned (that message was rolled back and stays
/// unclaimed).
pub type ProcessResult<T> = Result<Outcome<T>, HandlerError>;

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
}

impl<S: LockTimeout> Consumer<S> {
    /// Bounds how long [`claim`](Self::claim) (and therefore
    /// [`process`](Self::process)) will wait for a contended row before
    /// giving up with [`InboxError::Contended`].
    ///
    /// Unset by default: a consumer that loses the claim race blocks until
    /// the winner's transaction resolves. Only available on backends that
    /// implement [`LockTimeout`].
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }
}

impl<S: Savepoints> Consumer<S> {
    /// Runs `handler` once for every distinct, unprocessed id in `ids`, all
    /// in one transaction.
    ///
    /// Each handler runs inside a savepoint. When one fails, its effects and
    /// its claim roll back and the rest of the batch still commits, so a
    /// poison message costs only itself. The result is aligned with `ids`:
    /// `Ok(Outcome)` per message, or that message's handler error, ready to
    /// map onto a per-message ack or nack.
    ///
    /// The outer `Err` is a backend failure: nothing was committed.
    pub fn process_many<'a, F, T>(
        &'a self,
        ids: &'a [MessageId],
        mut handler: F,
    ) -> BoxFuture<'a, Result<Vec<ProcessResult<T>>, InboxError>>
    where
        F: for<'c> FnMut(&'c mut S::Conn, &'c MessageId) -> BoxFuture<'c, Result<T, HandlerError>>
            + Send
            + 'a,
        T: Send + 'a,
    {
        let span =
            tracing::debug_span!("inbox.process_many", consumer = %self.id, count = ids.len());
        Box::pin(
            async move {
                let mut tx = self.store.begin().await?;
                let claims = self.claim_many(&mut tx, ids).await?;
                let mut pending = claims.iter().filter(|c| **c == Claim::Fresh).count();
                let mut results = Vec::with_capacity(ids.len());
                // Ids whose handler failed. `claim_many` answers a repeat with
                // `Duplicate`, which a caller would ack, dropping a message that
                // was never processed; repeats of these report the failure.
                let mut failed: HashSet<&str> = HashSet::new();

                if pending > 0 {
                    self.store.savepoint(&mut tx).await?;
                }
                for (id, claim) in ids.iter().zip(claims) {
                    if claim == Claim::Duplicate {
                        results.push(if failed.contains(id.as_str()) {
                            Err(
                                format!("an earlier delivery of `{id}` in this batch failed")
                                    .into(),
                            )
                        } else {
                            Ok(Outcome::Duplicate)
                        });
                        continue;
                    }
                    pending -= 1;
                    match handler(&mut tx, id).await {
                        Ok(value) => results.push(Ok(Outcome::Processed(value))),
                        Err(e) => {
                            self.store.rollback_to(&mut tx).await?;
                            self.store.unclaim(&mut tx, &self.id, id).await?;
                            failed.insert(id.as_str());
                            results.push(Err(e));
                        }
                    }
                    if pending > 0 {
                        self.store.release_and_savepoint(&mut tx).await?;
                    } else {
                        self.store.release(&mut tx).await?;
                    }
                }
                self.store.commit(tx).await?;
                Ok(results)
            }
            .instrument(span),
        )
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

    /// Rolls back a transaction opened with [`begin`](Self::begin),
    /// discarding every claim and effect in it.
    pub fn rollback(&self, tx: S::Tx) -> BoxFuture<'_, Result<(), InboxError>> {
        self.store.rollback(tx)
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

    /// Records every id in `ids` for this consumer on `conn` in one backend
    /// call, reporting for each, in order, whether it was new. A repeated id
    /// gets the database's answer the first time and `Duplicate` after, so a
    /// handler can't run twice for one message. Must run on the same
    /// connection as the effects it guards.
    pub fn claim_many<'a>(
        &'a self,
        conn: &'a mut S::Conn,
        ids: &'a [MessageId],
    ) -> BoxFuture<'a, Result<Vec<Claim>, InboxError>> {
        Box::pin(async move {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // `str` orders by bytes, which is the order backends lock in.
            let mut unique: Vec<&MessageId> = ids.iter().collect();
            unique.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
            unique.dedup();

            let batch = ClaimBatch {
                lock_timeout: self.lock_timeout,
                ..ClaimBatch::new(&self.id, &unique)
            };
            let claims = self.store.claim_many(conn, batch).await?;
            if claims.len() != unique.len() {
                return Err(InboxError::Backend(
                    format!(
                        "claim_many returned {} claims for {} ids",
                        claims.len(),
                        unique.len()
                    )
                    .into(),
                ));
            }

            // The first occurrence takes the backend's answer; `insert`
            // leaves `Duplicate` behind for every repeat.
            let mut answers: HashMap<&str, Claim> =
                unique.iter().map(|id| id.as_str()).zip(claims).collect();
            Ok(ids
                .iter()
                .map(|id| {
                    answers
                        .insert(id.as_str(), Claim::Duplicate)
                        .expect("every id was sent to the backend")
                })
                .collect())
        })
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
                    Claim::Fresh => match handler(&mut tx).await {
                        Ok(value) => {
                            self.store.commit(tx).await?;
                            tracing::debug!(
                                consumer = %self.id,
                                message_id = %id,
                                "message processed"
                            );
                            Ok(Outcome::Processed(value))
                        }
                        Err(e) => {
                            // The handler's error is the one the caller can act
                            // on; a failed rollback still leaves the transaction
                            // to roll back on drop.
                            if let Err(rollback) = self.store.rollback(tx).await {
                                tracing::warn!(
                                    consumer = %self.id,
                                    message_id = %id,
                                    error = %rollback,
                                    "rollback after a handler failure failed"
                                );
                            }
                            Err(InboxError::Handler(e))
                        }
                    },
                }
            }
            .instrument(span),
        )
    }
}
