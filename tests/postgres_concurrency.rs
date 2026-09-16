#![cfg(feature = "postgres")]

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxExt, MessageId, Outcome};

/// Two consumers race on the same message, exactly as they would during a
/// partition rebalance. Precisely one must win, and the business effect must
/// be applied exactly once.
///
/// If `claim` were ever rewritten as a SELECT followed by an INSERT, this test
/// is what catches it: both tasks would read "not present" and both would
/// insert an `effects` row.
#[tokio::test]
async fn concurrent_delivery_applies_the_effect_exactly_once() {
    // The container handle must stay alive for as long as the pool is used;
    // dropping it stops the database.
    let container = PostgresImage::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("map port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to postgres");

    sqlx::query("CREATE TABLE effects (message_id TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create effects table");

    let inbox = Arc::new(PgInbox::new(pool.clone()));
    inbox.migrate().await.expect("run migrations");

    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-1").unwrap();

    let mut handles = Vec::new();
    for _ in 0..2 {
        let inbox = Arc::clone(&inbox);
        let consumer = consumer.clone();
        let id = id.clone();

        handles.push(tokio::spawn(async move {
            inbox
                .process(&consumer, &id, |conn| {
                    Box::pin(async move {
                        sqlx::query("INSERT INTO effects (message_id) VALUES ('contended-1')")
                            .execute(&mut *conn)
                            .await?;
                        Ok::<_, txbox::HandlerError>(())
                    })
                })
                .await
        }));
    }

    let mut processed = 0;
    let mut skipped = 0;
    for handle in handles {
        match handle
            .await
            .expect("task did not panic")
            .expect("no inbox error")
        {
            Outcome::Processed(()) => processed += 1,
            Outcome::Skipped => skipped += 1,
        }
    }

    assert_eq!(
        processed, 1,
        "exactly one consumer must process the message"
    );
    assert_eq!(skipped, 1, "exactly one consumer must skip the message");

    let effects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
        .fetch_one(&pool)
        .await
        .expect("count effects");
    assert_eq!(
        effects, 1,
        "the business effect must be applied exactly once"
    );
}
