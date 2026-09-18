/// An error returned by a user-supplied message handler.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// An identifier rejected at construction.
///
/// Validation happens when the newtype is built, not when the database is
/// touched, so a bad identifier can never reach `claim`. That matters for
/// classification: a backend failure is usually transient and worth retrying,
/// whereas this is permanent. A caller that could not tell them apart would
/// retry an unprocessable message forever and stall its partition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidId {
    /// The identifier was empty.
    ///
    /// Every empty identifier is equal to every other, so a producer emitting
    /// keyless messages would see all but the first silently skipped as
    /// duplicates of one another.
    #[error("identifier is empty")]
    Empty,

    /// The identifier was longer than the backend can index.
    ///
    /// PostgreSQL rejects a btree entry larger than roughly 2704 bytes, and
    /// the inbox's primary key covers both identifiers. The limits here are
    /// well inside that budget and generous next to any real broker key: a
    /// UUID is 36 bytes.
    #[error("identifier is {len} bytes, exceeding the {max}-byte limit")]
    TooLong {
        /// The length of the rejected identifier, in bytes.
        len: usize,
        /// The maximum length for this identifier, in bytes.
        max: usize,
    },

    /// [`MessageId::scoped`](crate::MessageId::scoped) was given a `scope`
    /// containing a `':'`.
    ///
    /// The separator is rejected in `scope` only, not in `id`: without this
    /// rule, `scoped("a:b", "c")` and `scoped("a", "b:c")` would join to the
    /// same string and collide. A scope is a short controlled identifier (a
    /// producer, an app, a tenant) with no legitimate reason to contain a
    /// colon, whereas a message id can — e.g. a Kafka `topic:partition:offset`
    /// composite key.
    #[error("scope contains the ':' separator")]
    ScopeContainsSeparator,

    /// The identifier had leading or trailing whitespace.
    ///
    /// This is rejected rather than trimmed away. Trimming would silently
    /// rewrite the key the caller passed in, and a library that quietly
    /// changes your deduplication key is worse than one that refuses it:
    /// `try_from("mt5 ")` and `try_from("mt5")` would then produce the same
    /// `MessageId`, and the caller would never learn that the value it built
    /// (say, by string-formatting a producer's raw output) carried stray
    /// whitespace. Rejecting surfaces that bug at the producer, where it can
    /// actually be fixed, instead of papering over it here. A whitespace-only
    /// value is reported as [`Empty`](InvalidId::Empty) instead, since "this
    /// id is blank" is the more useful message.
    #[error("identifier has leading or trailing whitespace")]
    SurroundingWhitespace,
}

/// Errors produced by the inbox.
///
/// The two variants are deliberately distinct because they demand different
/// responses: a backend failure is usually transient and worth retrying,
/// whereas a handler failure is business logic and the caller must decide
/// between retrying and dead-lettering.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InboxError {
    /// The database rejected or failed to execute an operation.
    ///
    /// This wraps the backend's own error type, so callers who need to tell
    /// a transient failure (worth retrying) from a permanent one (worth
    /// dead-lettering) can downcast to it:
    ///
    /// ```no_run
    /// # fn classify(err: &txbox::InboxError) {
    /// use txbox::InboxError;
    ///
    /// if let InboxError::Backend(source) = err {
    ///     if let Some(sqlx_err) = source.downcast_ref::<sqlx::Error>() {
    ///         // e.g. sqlx::Error::PoolTimedOut is transient; retry it.
    ///         // A constraint violation or bad credential usually is not.
    ///     }
    /// }
    /// # }
    /// ```
    #[error("inbox backend failure: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The user-supplied handler returned an error. The transaction was
    /// rolled back, so the message remains unprocessed and will be redelivered.
    #[error("inbox handler failure: {0}")]
    Handler(#[source] HandlerError),

    /// [`Consumer::claim`](crate::Consumer::claim) could not acquire the row
    /// within the configured [`with_lock_timeout`](crate::Consumer::with_lock_timeout).
    ///
    /// This means another consumer is claiming this exact message right now,
    /// not that the backend has failed. The correct response is to let the
    /// broker redeliver the message — **never** to dead-letter it: the
    /// message itself is not malformed or errored, it is simply contended at
    /// this instant, and the contending consumer is expected to commit and
    /// leave the row valid for a normal duplicate check on redelivery.
    ///
    /// On PostgreSQL this maps from SQLSTATE `55P03` (`lock_not_available`),
    /// which the backend raises when `SET LOCAL lock_timeout` expires.
    #[error("inbox claim contended: lock timeout exceeded")]
    Contended,

    /// An identifier failed validation.
    ///
    /// Unlike [`Backend`](InboxError::Backend), which is usually transient
    /// and worth retrying, this is permanent: the identifier is malformed and
    /// will fail identically on every retry. Retrying it only redelivers the
    /// same bad identifier forever, so a message that produces this belongs
    /// in a dead-letter queue, not a retry loop.
    #[error("invalid identifier: {0}")]
    InvalidId(#[source] InvalidId),
}

impl From<sqlx::Error> for InboxError {
    fn from(value: sqlx::Error) -> Self {
        InboxError::Backend(Box::new(value))
    }
}

impl From<InvalidId> for InboxError {
    fn from(value: InvalidId) -> Self {
        InboxError::InvalidId(value)
    }
}
