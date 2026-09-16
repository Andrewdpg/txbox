use crate::error::InvalidId;

/// Identifies the logical consumer that processed a message.
///
/// Two independent consumers of the same topic must use different values,
/// otherwise the first one to process a message would cause the second to
/// skip it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerId(String);

/// Identifies a single message, as supplied by the broker or the producer.
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
                if value.is_empty() {
                    return Err(InvalidId::Empty);
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

/// The result of attempting to record a message in the inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// The message had not been seen before and was recorded.
    Fresh,
    /// The message was already recorded by this consumer.
    Duplicate,
}

/// The result of running a handler through the inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<T> {
    /// The handler ran and produced this value.
    Processed(T),
    /// The message was a duplicate; the handler did not run.
    Skipped,
}

impl<T> Outcome<T> {
    /// Returns the handler's value, or `None` if the message was skipped.
    pub fn processed(self) -> Option<T> {
        match self {
            Outcome::Processed(value) => Some(value),
            Outcome::Skipped => None,
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
    fn outcome_skipped_yields_nothing() {
        let outcome: Outcome<u8> = Outcome::Skipped;
        assert_eq!(outcome.processed(), None);
    }
}
