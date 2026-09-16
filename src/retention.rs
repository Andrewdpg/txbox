use std::time::Duration;

/// How long processed messages are retained before being purged.
///
/// `max_age` must exceed the broker's redelivery window (Kafka topic
/// retention, SQS visibility timeout, and so on). An entry deleted while the
/// broker can still redeliver its message will allow that message to be
/// processed twice.
///
/// There is deliberately no row-count limit. The correctness rule is temporal;
/// a size cap would, under a traffic spike, delete entries still inside the
/// redelivery window — letting duplicates through at exactly peak load.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    max_age: Duration,
    batch_size: u32,
}

impl RetentionPolicy {
    /// Retains entries for at least `max_age`.
    pub fn new(max_age: Duration) -> Self {
        Self {
            max_age,
            batch_size: 1000,
        }
    }

    /// Sets how many rows a single purge statement deletes before looping.
    ///
    /// Batching keeps the delete from locking the whole table. A `batch_size`
    /// of zero is clamped to one.
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// The retention window.
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// Rows deleted per statement.
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }
}
