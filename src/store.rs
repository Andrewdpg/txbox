use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;

use crate::consumer::Consumer;
use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::types::{Claim, ClaimRequest, ConsumerId, MessageId};

/// A boxed, `Send` future.
///
/// Used instead of `async fn` in the trait below, since `async fn` in traits
/// isn't dyn-compatible and doesn't reliably propagate `Send`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A backend that can record processed messages.
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
    /// Must be a single atomic statement — a read followed by a conditional
    /// write is a race two concurrent consumers can both win. Must run on
    /// `conn`, the same connection the handler uses for its own effects, or
    /// the all-or-nothing guarantee is lost.
    ///
    /// `request.lock_timeout`, when set, bounds how long this call waits for
    /// a row contended by another consumer before returning
    /// [`InboxError::Contended`] instead of blocking. Backends with no way to
    /// bound the wait (SQLite) ignore it.
    fn claim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        request: ClaimRequest<'a>,
    ) -> BoxFuture<'a, Result<Claim, InboxError>>;

    /// Reports whether `(consumer, id)` is already recorded, without opening a
    /// transaction.
    ///
    /// `true` is definitive. `false` means unknown, not fresh — under MVCC a
    /// plain read doesn't see a concurrent, uncommitted claim, so `process`
    /// never uses this to skip the transactional path. Useful on its own for
    /// a replay or backfill that wants to skip already-known messages cheaply.
    ///
    /// The default answers `false` always, so third-party backends need not
    /// implement it.
    fn is_known_duplicate<'a>(
        &'a self,
        consumer: &'a ConsumerId,
        id: &'a MessageId,
    ) -> BoxFuture<'a, Result<bool, InboxError>> {
        let _ = (consumer, id);
        Box::pin(async { Ok(false) })
    }

    /// Deletes entries older than the policy's window. Returns rows removed.
    fn purge<'a>(&'a self, policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>>;
}

/// Extension trait layered over [`InboxStore`]. Import it alongside
/// [`InboxStore`] to get `.consumer(...)` on a store.
pub trait InboxExt: InboxStore + Clone + Sized {
    /// Opens a handle for one logical consumer of one stream.
    ///
    /// Two services consuming the same stream must use different `id`s: the
    /// dedup key is `(consumer, message)`, so sharing one makes whichever
    /// service processes a message first cause the other to skip it.
    fn consumer(&self, id: ConsumerId) -> Consumer<Self> {
        Consumer::new(self.clone(), id)
    }
}

impl<S: InboxStore + Clone> InboxExt for S {}
