#![cfg(feature = "postgres")]

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use txbox::postgres::PgInbox;
use txbox::{Claim, ConsumerId, InboxExt, InboxStore, MessageId, Outcome, RetentionPolicy};

/// The container handle must stay alive for as long as the pool is used;
/// dropping it stops the database.
async fn inbox() -> (ContainerAsync<PostgresImage>, PgInbox) {
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

    let inbox = PgInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox)
}

#[tokio::test]
async fn claim_is_fresh_once_then_duplicate() {
    let (_container, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox.claim(&mut tx, &consumer, &id).await.unwrap(),
        Claim::Fresh
    );
    inbox.commit(tx).await.unwrap();

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox.claim(&mut tx, &consumer, &id).await.unwrap(),
        Claim::Duplicate
    );
    inbox.commit(tx).await.unwrap();
}

#[tokio::test]
async fn distinct_consumers_both_process_the_same_message() {
    let (_container, inbox) = inbox().await;
    let id = MessageId::try_from("shared-1").unwrap();

    for consumer in ["billing", "notifications"] {
        let outcome = inbox
            .process(&ConsumerId::try_from(consumer).unwrap(), &id, |_c| {
                Box::pin(async { Ok(()) })
            })
            .await
            .unwrap();
        assert_eq!(outcome, Outcome::Processed(()));
    }
}

#[tokio::test]
async fn purge_deletes_only_entries_outside_the_window() {
    let (_container, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();

    inbox
        .process(&consumer, &MessageId::try_from("old").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    sqlx::query("UPDATE inbox_messages SET processed_at = $1 WHERE message_id = 'old'")
        .bind(chrono::Utc::now() - chrono::Duration::hours(1))
        .execute(inbox.pool())
        .await
        .unwrap();

    inbox
        .process(&consumer, &MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    let policy = RetentionPolicy::new(Duration::from_secs(600));
    assert_eq!(inbox.purge(&policy).await.unwrap(), 1);

    let outcome = inbox
        .process(&consumer, &MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Skipped);
}

/// A deterministic, effectively incompressible string.
///
/// PostgreSQL compresses index entries, so a repeated character fits in the
/// index no matter how long it is — the size limit applies after compression.
/// Real broker keys that get anywhere near the limit are base64 tokens, hashes
/// or concatenated fields, none of which compress. This models those.
fn incompressible(len: usize) -> String {
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(33 + (state % 94) as u8)
        })
        .collect()
}

/// `MessageId::MAX_LEN` is not an arbitrary number: PostgreSQL refuses to index
/// a btree entry whose compressed size exceeds roughly 2704 bytes, and the
/// inbox's primary key spans both identifiers. This pins that premise. If it
/// ever stops holding — a new PostgreSQL release, a different index type — the
/// limit should be revisited rather than quietly kept.
#[tokio::test]
async fn postgres_refuses_an_identifier_too_large_to_index() {
    let (_container, inbox) = inbox().await;

    let oversized = incompressible(4096);
    let result = sqlx::query(
        "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
         VALUES ($1, $2, $3)",
    )
    .bind("billing")
    .bind(&oversized)
    .bind(chrono::Utc::now())
    .execute(inbox.pool())
    .await;

    let error = result.expect_err("postgres must refuse to index this");
    assert!(
        error.to_string().contains("exceeds btree"),
        "expected a btree size rejection, got: {error}"
    );

    // And the guard means this can never be reached through the public API.
    assert!(MessageId::try_from(oversized).is_err());
}
