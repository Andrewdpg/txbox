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
    ($name:ident) => {
        impl $name {
            /// Borrows the underlying string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_newtype!(ConsumerId);
string_newtype!(MessageId);

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

    #[test]
    fn ids_are_constructible_from_str_and_string() {
        assert_eq!(ConsumerId::from("billing").as_str(), "billing");
        assert_eq!(MessageId::from(String::from("m-1")).as_str(), "m-1");
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
