/// An error returned by a user-supplied message handler.
pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;

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
}

impl From<sqlx::Error> for InboxError {
    fn from(value: sqlx::Error) -> Self {
        InboxError::Backend(Box::new(value))
    }
}
