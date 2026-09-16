//! Transactional inbox pattern for reliable message processing.
//!
//! See the crate README for usage. Backends are selected with the
//! `postgres` and `sqlite` Cargo features; neither is enabled by default.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub mod postgres;
mod retention;
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub mod sqlite;
mod store;
mod types;

pub use error::{HandlerError, InboxError};
pub use retention::RetentionPolicy;
pub use store::{BoxFuture, InboxExt, InboxStore};
pub use types::{Claim, ConsumerId, MessageId, Outcome};
