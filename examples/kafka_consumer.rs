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

    // Two services consuming the same topic MUST use different identifiers.
    let orders = inbox.consumer(ConsumerId::try_from("orders-billing")?);

    let kafka: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "orders-billing")
        // Manual commit: with auto-commit, a crash after delivery but before
        // the inbox transaction commits would still advance the offset and
        // lose the message.
        .set("enable.auto.commit", "false")
        .create()?;
    kafka.subscribe(&["orders.created"])?;

    // Purging on one instance only; with N replicas prefer a scheduled job.
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

    // This example propagates errors with `?` and exits for readability; a
    // production consumer needs per-message error isolation instead.
    loop {
        let message = kafka.recv().await?;

        // A producer-supplied key is better than the offset, which changes
        // if the message is republished.
        let key = message.key_view::<str>().transpose()?;

        // A missing/malformed key fails the same way on every redelivery and
        // would stall the partition forever; skip it instead (a real
        // consumer would dead-letter it first).
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
            Outcome::Duplicate => tracing::debug!(%id, "duplicate ignored"),
        }

        // Commit only after the transaction committed; a crash in between is
        // safe, since redelivery will hit the inbox as a duplicate.
        kafka.commit_message(&message, CommitMode::Async)?;
    }
}
