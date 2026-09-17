//! Transactional inbox pattern for reliable message processing.
//!
//! Backends are selected with the `postgres` and `sqlite` Cargo features;
//! neither is enabled by default.
//!
//! The README follows. It is included rather than summarised so that its
//! examples compile as doctests: documentation that is never built is
//! documentation that silently rots. The include is gated on `postgres`
//! because the examples use that backend; `docs.rs` builds with it enabled.
#![cfg_attr(feature = "postgres", doc = include_str!("../README.md"))]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// The `sqlx` version this crate was built against.
///
/// Handlers write their own SQL and backends are constructed from an `sqlx`
/// pool, so callers need the same `sqlx` this crate uses. Re-exporting it means
/// they cannot end up on a different version by accident, and can reach it as
/// `txbox::sqlx` without declaring the dependency twice.
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
