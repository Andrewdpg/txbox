/// An error returned by a user-supplied message handler.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// An identifier rejected at construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidId {
    /// The identifier was empty (or whitespace-only).
    #[error("identifier is empty")]
    Empty,

    /// The identifier was longer than the backend can index.
    #[error("identifier is {len} bytes, exceeding the {max}-byte limit")]
    TooLong {
        /// The length of the rejected identifier, in bytes.
        len: usize,
        /// The maximum length for this identifier, in bytes.
        max: usize,
    },

    /// [`MessageId::scoped`](crate::MessageId::scoped) rejects a `scope`
    /// containing `':'`, since it's the join separator.
    #[error("scope contains the ':' separator")]
    ScopeContainsSeparator,

    /// The identifier had leading or trailing whitespace.
    ///
    /// Rejected rather than trimmed, so a caller can't silently change its
    /// dedup key by accident.
    #[error("identifier has leading or trailing whitespace")]
    SurroundingWhitespace,
}

/// Errors produced by the inbox.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InboxError {
    /// The database rejected or failed to execute an operation. Wraps the
    /// backend's own error type; downcast to it (e.g. to `sqlx::Error`) to
    /// tell a transient failure from a permanent one.
    #[error("inbox backend failure: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The user-supplied handler returned an error. The transaction was
    /// rolled back, so the message remains unprocessed and will be redelivered.
    #[error("inbox handler failure: {0}")]
    Handler(#[source] HandlerError),

    /// A claim lost to another consumer holding the same row: the lock
    /// timeout ([`with_lock_timeout`](crate::Consumer::with_lock_timeout))
    /// expired, the database chose this transaction as a deadlock victim, or
    /// (SQLite) another connection held the write lock.
    ///
    /// Not a backend failure: retry the message, don't dead-letter it. Retry
    /// the whole unit in a new transaction, though. The current one is done:
    /// PostgreSQL has aborted it, and on MySQL a deadlock has already rolled it
    /// back, so anything run on it afterwards commits statement by statement.
    /// Roll it back or drop it first.
    #[error("inbox claim contended: another consumer holds the row")]
    Contended,

    /// An identifier failed validation. Permanent — retrying redelivers the
    /// same bad identifier, so this belongs in a dead-letter queue.
    #[error("invalid identifier: {0}")]
    InvalidId(#[source] InvalidId),
}

#[cfg(any(feature = "postgres", feature = "sqlite", feature = "mysql"))]
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
