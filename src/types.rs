use std::time::Duration;

use crate::error::InvalidId;

/// Identifies the logical consumer that processed a message.
///
/// Two independent consumers of the same topic must use different values,
/// otherwise the first one to process a message would cause the second to
/// skip it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerId(String);

/// Identifies a single message, as supplied by the broker or the producer.
///
/// A raw sequence number is only unique *within the producer that emitted
/// it*. When several producers publish to one queue and each numbers
/// messages with its own local sequence, two of them can legitimately emit
/// the same raw id — and the second is discarded as a duplicate it never
/// actually was. This mirrors the risk documented on [`ConsumerId`]: sharing
/// one dedup key across independent sources loses information silently
/// rather than failing loudly. Use [`MessageId::scoped`] to fold the
/// producer's identity into the key and avoid the collision.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(String);

macro_rules! string_newtype {
    ($name:ident, $max_len:expr) => {
        impl $name {
            /// The longest value this identifier accepts, in bytes.
            pub const MAX_LEN: usize = $max_len;

            /// Borrows the underlying string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidId;

            fn try_from(value: String) -> Result<Self, InvalidId> {
                if value.trim().is_empty() {
                    // Catches both "" and whitespace-only values: either way
                    // the id is blank, and that is the more useful report
                    // than SurroundingWhitespace.
                    return Err(InvalidId::Empty);
                }
                if value.trim().len() != value.len() {
                    return Err(InvalidId::SurroundingWhitespace);
                }
                if value.len() > Self::MAX_LEN {
                    return Err(InvalidId::TooLong {
                        len: value.len(),
                        max: Self::MAX_LEN,
                    });
                }
                Ok(Self(value))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = InvalidId;

            fn try_from(value: &str) -> Result<Self, InvalidId> {
                Self::try_from(value.to_owned())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype!(ConsumerId, 255);
string_newtype!(MessageId, 512);

impl MessageId {
    /// Builds a message id scoped to `scope`, joining the two with `':'`.
    ///
    /// Use this when several producers publish to one queue and each numbers
    /// messages with its own local sequence: two producers can legitimately
    /// emit the same raw id, and without a scope the second is silently
    /// discarded as a duplicate of the first. See the type-level docs on
    /// [`MessageId`] for the full risk.
    ///
    /// `scope` and `id` are each rejected if blank (empty or whitespace-only,
    /// reported as [`InvalidId::Empty`]) or surrounded by whitespace
    /// (reported as [`InvalidId::SurroundingWhitespace`]) — checked on each
    /// part individually, not on the joined string, because whitespace
    /// sitting right against the `':'` separator (e.g. a trailing space in
    /// `scope`, or a leading one in `id`) would not be at either edge of the
    /// joined value and would otherwise slip through silently. A scope like
    /// `" mt5"` is rejected rather than joined into a different key than
    /// `"mt5"` would have produced — see
    /// [`InvalidId::SurroundingWhitespace`] for why this crate refuses
    /// rather than trims. The joined value must also respect
    /// [`MessageId::MAX_LEN`], enforced by [`MessageId::try_from`].
    ///
    /// The `':'` separator is rejected in `scope` but allowed in `id`. This
    /// asymmetry is deliberate, not an oversight: without it,
    /// `scoped("a:b", "c")` and `scoped("a", "b:c")` would both join to
    /// `"a:b:c"` and collide. A scope is a short, controlled identifier — a
    /// producer, an app, a tenant — that has no legitimate reason to contain
    /// a colon. A message id can: Kafka users commonly compose
    /// `topic:partition:offset` as their raw id, and that must pass through
    /// unscathed.
    pub fn scoped(scope: &str, id: &str) -> Result<Self, InvalidId> {
        if scope.contains(':') {
            return Err(InvalidId::ScopeContainsSeparator);
        }
        for part in [scope, id] {
            if part.trim().is_empty() {
                return Err(InvalidId::Empty);
            }
            if part.trim().len() != part.len() {
                return Err(InvalidId::SurroundingWhitespace);
            }
        }
        Self::try_from(format!("{scope}:{id}"))
    }
}

/// The result of attempting to record a message in the inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// The message had not been seen before and was recorded.
    Fresh,
    /// The message was already recorded by this consumer.
    Duplicate,
}

/// A request to record a message in the inbox, submitted to
/// [`InboxStore::claim`](crate::store::InboxStore::claim) and answered with a
/// [`Claim`].
///
/// [`Consumer`](crate::consumer::Consumer) builds one internally on every call
/// to `claim` or `process`; ordinary users of this crate never construct one.
/// This type exists for third-party backend implementors, who read its fields
/// to perform the claim.
///
/// It bundles the claim's identity — `consumer` and `id`, both required — with
/// its policy — currently just `lock_timeout`, optional — behind one
/// parameter instead of a growing list of positional arguments. `#[non_exhaustive]`
/// so a future policy field (a new backend hint, say) can be added without
/// breaking every implementor's call site.
///
/// Fields are public rather than hidden behind accessors: `#[non_exhaustive]`
/// blocks external struct-literal construction and exhaustive destructuring,
/// but field reads are unaffected, so a plain struct is simplest for
/// implementors who only ever read it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct ClaimRequest<'a> {
    /// The consumer performing the claim.
    pub consumer: &'a ConsumerId,
    /// The message being claimed.
    pub id: &'a MessageId,
    /// Bounds how long the backend will wait for a contended row. See
    /// [`InboxStore::claim`](crate::store::InboxStore::claim) for the
    /// per-backend contract.
    pub lock_timeout: Option<Duration>,
}

impl<'a> ClaimRequest<'a> {
    /// Builds a request for `consumer` claiming `id`, with no lock timeout.
    pub fn new(consumer: &'a ConsumerId, id: &'a MessageId) -> Self {
        Self {
            consumer,
            id,
            lock_timeout: None,
        }
    }

    /// Sets the lock timeout.
    pub fn with_lock_timeout(mut self, lock_timeout: Duration) -> Self {
        self.lock_timeout = Some(lock_timeout);
        self
    }
}

/// The result of running a handler through the inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    /// The handler ran and produced this value.
    Processed(T),
    /// The message was a duplicate; the handler did not run.
    Duplicate,
}

impl<T> Outcome<T> {
    /// Returns the handler's value, or `None` if the message was a duplicate.
    pub fn processed(self) -> Option<T> {
        match self {
            Outcome::Processed(value) => Some(value),
            Outcome::Duplicate => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The limit is a byte budget, because that is what the index measures.
    /// Counting characters instead would let a multi-byte identifier through
    /// and move the failure back into the database.
    #[test]
    fn the_length_limit_counts_bytes_not_characters() {
        let multibyte = "\u{00e9}".repeat(MessageId::MAX_LEN);
        assert_eq!(multibyte.chars().count(), MessageId::MAX_LEN);
        assert!(MessageId::try_from(multibyte).is_err());
    }

    #[test]
    fn a_message_id_beyond_the_length_limit_is_rejected() {
        // PostgreSQL refuses a btree entry larger than ~2704 bytes. Without
        // this check the INSERT in `claim` fails permanently, is reported as a
        // backend error (which callers are told to retry), and stalls the
        // partition forever on one malformed message.
        let too_long = "x".repeat(MessageId::MAX_LEN + 1);
        assert!(MessageId::try_from(too_long).is_err());
    }

    #[test]
    fn an_empty_message_id_is_rejected() {
        // A producer that sends messages without a key would otherwise collapse
        // every one of them onto the same id, and all but the first would be
        // silently skipped as duplicates.
        assert!(MessageId::try_from("").is_err());
    }

    #[test]
    fn try_from_rejects_a_whitespace_only_id_as_empty() {
        assert_eq!(MessageId::try_from("   "), Err(InvalidId::Empty));
        assert_eq!(ConsumerId::try_from("   "), Err(InvalidId::Empty));
    }

    #[test]
    fn try_from_rejects_an_id_with_a_leading_space() {
        assert_eq!(
            MessageId::try_from(" m-1"),
            Err(InvalidId::SurroundingWhitespace)
        );
        assert_eq!(
            ConsumerId::try_from(" billing"),
            Err(InvalidId::SurroundingWhitespace)
        );
    }

    #[test]
    fn try_from_rejects_an_id_with_a_trailing_space() {
        assert_eq!(
            MessageId::try_from("m-1 "),
            Err(InvalidId::SurroundingWhitespace)
        );
        assert_eq!(
            ConsumerId::try_from("billing "),
            Err(InvalidId::SurroundingWhitespace)
        );
    }

    #[test]
    fn ids_are_constructible_from_str_and_string() {
        assert_eq!(ConsumerId::try_from("billing").unwrap().as_str(), "billing");
        assert_eq!(
            MessageId::try_from(String::from("m-1")).unwrap().as_str(),
            "m-1"
        );
    }

    #[test]
    fn outcome_processed_yields_the_value() {
        let outcome: Outcome<u8> = Outcome::Processed(7);
        assert_eq!(outcome.processed(), Some(7));
    }

    #[test]
    fn outcome_duplicate_yields_nothing() {
        let outcome: Outcome<u8> = Outcome::Duplicate;
        assert_eq!(outcome.processed(), None);
    }

    #[test]
    fn scoped_rejects_both_parts_empty() {
        assert_eq!(MessageId::scoped("", ""), Err(InvalidId::Empty));
    }

    #[test]
    fn scoped_rejects_an_empty_scope() {
        assert_eq!(MessageId::scoped("", "1"), Err(InvalidId::Empty));
    }

    #[test]
    fn scoped_rejects_an_empty_id() {
        assert_eq!(MessageId::scoped("producer-a", ""), Err(InvalidId::Empty));
    }

    #[test]
    fn scoped_rejects_a_colon_in_the_scope() {
        // Without this, scoped("a:b", "c") and scoped("a", "b:c") would both
        // join to "a:b:c" and collide.
        assert_eq!(
            MessageId::scoped("a:b", "c"),
            Err(InvalidId::ScopeContainsSeparator)
        );
    }

    #[test]
    fn scoped_allows_a_colon_in_the_id() {
        // Kafka users commonly compose `topic:partition:offset` as their raw
        // id, and that must pass through unscathed.
        let id = MessageId::scoped("producer-a", "topic:partition:offset").unwrap();
        assert_eq!(id.as_str(), "producer-a:topic:partition:offset");
    }

    #[test]
    fn scoped_enforces_the_combined_length_limit() {
        let scope = "s";
        let id = "x".repeat(MessageId::MAX_LEN); // + "s:" pushes it over MAX_LEN
        assert!(MessageId::scoped(scope, &id).is_err());
    }

    #[test]
    fn scoped_rejects_a_scope_with_surrounding_whitespace_instead_of_a_different_key() {
        // Without this check, scoped(" mt5", "1") and scoped("mt5", "1")
        // would silently produce two different keys for the same producer.
        assert_eq!(
            MessageId::scoped(" mt5", "1"),
            Err(InvalidId::SurroundingWhitespace)
        );
        assert_eq!(
            MessageId::scoped("mt5 ", "1"),
            Err(InvalidId::SurroundingWhitespace)
        );
    }

    #[test]
    fn scoped_rejects_an_id_with_surrounding_whitespace() {
        assert_eq!(
            MessageId::scoped("mt5", " 1"),
            Err(InvalidId::SurroundingWhitespace)
        );
        assert_eq!(
            MessageId::scoped("mt5", "1 "),
            Err(InvalidId::SurroundingWhitespace)
        );
    }
}
