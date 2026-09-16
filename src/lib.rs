//! Transactional inbox pattern for reliable message processing.
//!
//! See the crate README for usage. Backends are selected with the
//! `postgres` and `sqlite` Cargo features; neither is enabled by default.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod retention;
mod store;
mod types;

pub use error::{HandlerError, InboxError};
pub use retention::RetentionPolicy;
pub use store::{BoxFuture, InboxExt, InboxStore};
pub use types::{Claim, ConsumerId, MessageId, Outcome};
