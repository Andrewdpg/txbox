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
}

impl From<sqlx::Error> for InboxError {
    fn from(value: sqlx::Error) -> Self {
        InboxError::Backend(Box::new(value))
    }
}
