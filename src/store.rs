use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;

use tracing::Instrument;

use crate::error::{HandlerError, InboxError};
use crate::retention::RetentionPolicy;
use crate::types::{Claim, ConsumerId, MessageId, Outcome};

/// A boxed, `Send` future.
///
/// Every async method on [`InboxStore`] returns this rather than using
/// `async fn`. `async fn` in traits is not dyn-compatible, and its auto traits
/// do not propagate reliably through generic code. Boxing costs one allocation
/// per call — negligible next to a database round trip — and buys both
/// `dyn InboxStore` and a guaranteed `Send` future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A backend that can record processed messages.
///
/// Implementors supply the concrete connection and transaction types. Neither
/// associated type carries a lifetime, so no generic associated types are
/// required: `sqlx`'s `Pool::begin()` yields a `Transaction<'static, DB>`,
/// which dereferences to the pool's connection type.
pub trait InboxStore: Send + Sync {
    /// The backend's connection type, as handed to message handlers.
    type Conn: Send;

    /// The backend's transaction type. Dropping it without committing must
    /// roll back.
    type Tx: DerefMut<Target = Self::Conn> + Send;

    /// Opens a new transaction.
    fn begin(&self) -> BoxFuture<'_, Result<Self::Tx, InboxError>>;

    /// Commits a transaction.
    fn commit(&self, tx: Self::Tx) -> BoxFuture<'_, Result<(), InboxError>>;

    /// Records `id` for `consumer` on `conn`, reporting whether it was new.
    ///
    /// Implementations MUST perform this as a single atomic statement. A
    /// read followed by a conditional write is a time-of-check-to-time-of-use
    /// race: two concurrent consumers would both observe the message as unseen
    /// and both apply the business effect.
    ///
    /// The statement MUST be executed on `conn` — the same connection the
    /// handler will use for its own effects. Issuing it on a separate
    /// connection or against the pool directly commits the inbox row
    /// independently of the handler's transaction and destroys the
    /// all-or-nothing guarantee this crate exists to provide.
    fn claim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<Claim, InboxError>>;

    /// Deletes entries older than the policy's window. Returns rows removed.
    fn purge<'a>(&'a self, policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>>;
}

/// Extension trait layered over [`InboxStore`].
///
/// Blanket-implemented for every store, in the manner of `futures::StreamExt`.
/// Import it alongside [`InboxStore`] — the `Ext` suffix signals that this
/// trait extends the other rather than standing as a peer choice; without it
/// in scope, `process` will not resolve on a store. It is generic over the
/// handler and so is not dyn-compatible; erase [`InboxStore`] instead when a
/// trait object is needed.
pub trait InboxExt: InboxStore {
    /// Runs `handler` exactly once for `(consumer, id)`.
    ///
    /// The inbox row and the handler's effects share one transaction. If the
    /// handler fails, both are rolled back and the message stays unprocessed,
    /// so the broker's redelivery will retry it.
    fn process<'a, F, T>(
        &'a self,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
        handler: F,
    ) -> BoxFuture<'a, Result<Outcome<T>, InboxError>>
    where
        F: FnOnce(&mut Self::Conn) -> BoxFuture<'_, Result<T, HandlerError>> + Send + 'a,
        T: Send + 'a;
}

impl<S: InboxStore> InboxExt for S {
    fn process<'a, F, T>(
        &'a self,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
        handler: F,
    ) -> BoxFuture<'a, Result<Outcome<T>, InboxError>>
    where
        F: FnOnce(&mut Self::Conn) -> BoxFuture<'_, Result<T, HandlerError>> + Send + 'a,
        T: Send + 'a,
    {
        let span = tracing::debug_span!("inbox.process", consumer = %consumer, message_id = %id);
        Box::pin(
            async move {
                let mut tx = self.begin().await?;

                match self.claim(&mut tx, consumer, id).await? {
                    Claim::Duplicate => {
                        tracing::debug!(
                            consumer = %consumer,
                            message_id = %id,
                            "duplicate message skipped"
                        );
                        // Dropping the transaction rolls it back. Nothing to keep.
                        Ok(Outcome::Skipped)
                    }
                    Claim::Fresh => {
                        let value = handler(&mut tx).await.map_err(InboxError::Handler)?;
                        self.commit(tx).await?;
                        tracing::debug!(
                            consumer = %consumer,
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
