//! Transactional inbox pattern for reliable message processing.
//!
//! Backends are selected with the `postgres` and `sqlite` Cargo features;
//! neither is enabled by default.
//!
//! The README follows, included so its examples compile as doctests. Gated
//! on `postgres` since the examples use that backend.
#![cfg_attr(feature = "postgres", doc = include_str!("../README.md"))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// The `sqlx` version this crate was built against. Re-exported so handlers
/// writing their own SQL can reach it as `txbox::sqlx` without declaring the
/// dependency twice.
pub use sqlx;

mod consumer;
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

pub use consumer::Consumer;
pub use error::{HandlerError, InboxError, InvalidId};
pub use retention::RetentionPolicy;
pub use store::{BoxFuture, InboxExt, InboxStore};
pub use types::{Claim, ClaimRequest, ConsumerId, MessageId, Outcome};
