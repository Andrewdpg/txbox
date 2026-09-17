//! End-to-end example: a Kafka consumer made idempotent with txbox.
//!
//! Run with:
//!   cargo run --example kafka_consumer --features example-kafka

use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use sqlx::postgres::PgPoolOptions;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxExt, InboxStore, MessageId, Outcome, RetentionPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let inbox = PgInbox::new(pool);
    inbox.migrate().await?;

    // One handle per queue, built once. The identifier is settled here instead
    // of being passed on every message, and tuning lives here too — so another
    // queue sharing this pool is unaffected by how this one is configured.
    //
    // Two services consuming the same topic MUST use different identifiers, or
    // the second will skip every message the first has already handled.
    let orders = inbox.consumer(ConsumerId::try_from("orders-billing")?);

    let kafka: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "orders-billing")
        // Offsets are committed by hand, only after the inbox transaction has
        // committed. With auto-commit the offset would be committed on a timer,
        // decoupled from processing: a crash between delivery and the database
        // commit would advance the offset anyway and the message would never be
        // redelivered. That turns at-least-once into at-most-once and removes
        // the very guarantee this crate exists to provide.
        .set("enable.auto.commit", "false")
        .create()?;
    kafka.subscribe(&["orders.created"])?;

    // Purging runs on one instance only. With N replicas, N in-process loops
    // would compete to delete the same rows; prefer a scheduled job instead.
    let purge_inbox = inbox.clone();
    tokio::spawn(async move {
        // max_age must exceed the Kafka topic's retention window.
        let policy = RetentionPolicy::new(Duration::from_secs(7 * 24 * 60 * 60));
        let mut ticker = tokio::time::interval(Duration::from_secs(3600));
        loop {
            ticker.tick().await;
            match purge_inbox.purge(&policy).await {
                Ok(removed) => tracing::info!(removed, "inbox purged"),
                Err(error) => tracing::error!(%error, "inbox purge failed"),
            }
        }
    });

    // A production consumer needs per-message error isolation (skip, retry
    // with backoff, or dead-letter); a malformed record or a transient
    // database blip should not take down the whole process. This example
    // propagates every error with `?` and exits instead, only to stay
    // readable — do not copy that part into production code.
    loop {
        let message = kafka.recv().await?;

        // The only broker-specific line in the whole program: pick a stable
        // identifier. A producer-supplied key is better than the offset,
        // which changes if the message is republished.
        let key = message.key_view::<str>().transpose()?;

        // A missing or malformed key is a poison message: it fails the same way
        // on every redelivery, so retrying it would stall the partition
        // forever. Commit past it instead. A real consumer dead-letters it
        // first, rather than dropping it as this example does.
        let id = match key.map(MessageId::try_from) {
            Some(Ok(id)) => id,
            rejected => {
                tracing::error!(?rejected, "unprocessable message id, skipping");
                kafka.commit_message(&message, CommitMode::Async)?;
                continue;
            }
        };

        let payload = message.payload().unwrap_or_default().to_vec();

        let outcome = orders
            .process(&id, move |conn| {
                Box::pin(async move {
                    sqlx::query("INSERT INTO orders (payload) VALUES ($1)")
                        .bind(&payload[..])
                        .execute(&mut *conn)
                        .await?;
                    Ok::<_, txbox::HandlerError>(())
                })
            })
            .await?;

        match outcome {
            Outcome::Processed(()) => tracing::info!(%id, "order stored"),
            Outcome::Skipped => tracing::debug!(%id, "duplicate ignored"),
        }

        // Commit only after the transaction committed. Crashing between these two
        // points is safe and expected: the broker redelivers, and the inbox
        // recognises the message as a duplicate. That window is precisely what the
        // inbox pattern exists to make harmless.
        kafka.commit_message(&message, CommitMode::Async)?;
    }
}
