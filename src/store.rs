use std::future::Future;
use std::ops::DerefMut;
use std::pin::Pin;

use crate::consumer::Consumer;
use crate::error::InboxError;
use crate::retention::RetentionPolicy;
use crate::types::{Claim, ClaimRequest, ConsumerId, MessageId};

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
    ///
    /// `request.lock_timeout`, when set, bounds how long this call will wait
    /// for a row contended by another consumer's in-flight claim before
    /// returning [`InboxError::Contended`] instead of blocking. A backend
    /// that has no way to bound the wait (SQLite) ignores it; PostgreSQL
    /// scopes it to this transaction with `SET LOCAL lock_timeout`.
    fn claim<'a>(
        &'a self,
        conn: &'a mut Self::Conn,
        request: ClaimRequest<'a>,
    ) -> BoxFuture<'a, Result<Claim, InboxError>>;

    /// Reports whether `(consumer, id)` is already recorded, without opening a
    /// transaction.
    ///
    /// The two answers are not symmetric, and the asymmetry is the whole
    /// contract. `true` is definitive: a committed inbox row can never become
    /// uncommitted, so the message has been processed and the handler must not
    /// run. `false` proves nothing — the message may be new, or a concurrent
    /// claim may be in flight and not yet committed — so the caller must fall
    /// through to the transactional path and let `claim` decide.
    ///
    /// This is why `process` never calls this method to skip the
    /// transactional path: under MVCC, a plain `SELECT` does not see a
    /// concurrent claim that has not committed yet, so it answers `false` for
    /// exactly the message that is contended right now — the one case where
    /// skipping the transaction would be wrong. It stays useful as an
    /// explicitly-called method for a replay or backfill, where an operator
    /// re-consuming a topic from the start wants to skip already-known
    /// messages cheaply.
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

/// Extension trait layered over [`InboxStore`].
///
/// Blanket-implemented for every cloneable store, in the manner of
/// `futures::StreamExt`. Import it alongside [`InboxStore`] — the `Ext` suffix
/// signals that this trait extends the other rather than standing as a peer
/// choice; without it in scope, `consumer` will not resolve on a store.
pub trait InboxExt: InboxStore + Clone + Sized {
    /// Opens a handle for one logical consumer of one stream.
    ///
    /// Messages are recorded under `id`, and the returned [`Consumer`] carries
    /// its own configuration, so two queues sharing a pool can be tuned
    /// independently.
    ///
    /// Two services consuming the same stream must use different identifiers.
    /// The dedup key is `(consumer, message)`, so sharing one identifier makes
    /// whichever service processes a message first cause the other to skip it
    /// as a duplicate it never actually ran.
    fn consumer(&self, id: ConsumerId) -> Consumer<Self> {
        Consumer::new(self.clone(), id)
    }
}

impl<S: InboxStore + Clone> InboxExt for S {}
