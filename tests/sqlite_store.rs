#![cfg(feature = "sqlite")]

use std::time::Duration;

use sqlx::sqlite::SqlitePoolOptions;
use txbox::sqlite::SqliteInbox;
use txbox::{Claim, ConsumerId, InboxExt, InboxStore, MessageId, Outcome, RetentionPolicy};

async fn inbox() -> SqliteInbox {
    // max_connections(1) keeps every operation on the same in-memory database;
    // each new SQLite memory connection would otherwise get its own empty one.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("connect to in-memory sqlite");

    let inbox = SqliteInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    inbox
}

#[tokio::test]
async fn claim_is_fresh_once_then_duplicate() {
    let inbox = inbox().await;
    let consumer = ConsumerId::from("billing");
    let id = MessageId::from("m-1");

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
async fn a_rolled_back_claim_leaves_no_trace() {
    let inbox = inbox().await;
    let consumer = ConsumerId::from("billing");
    let id = MessageId::from("m-1");

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox.claim(&mut tx, &consumer, &id).await.unwrap(),
        Claim::Fresh
    );
    drop(tx); // rollback

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox.claim(&mut tx, &consumer, &id).await.unwrap(),
        Claim::Fresh
    );
    inbox.commit(tx).await.unwrap();
}

#[tokio::test]
async fn a_failing_handler_lets_the_retry_succeed() {
    let inbox = inbox().await;
    let consumer = ConsumerId::from("billing");
    let id = MessageId::from("m-1");

    let failed = inbox
        .process(&consumer, &id, |_conn| {
            Box::pin(async { Err::<(), _>("handler exploded".into()) })
        })
        .await;
    assert!(failed.is_err());

    let retried = inbox
        .process(&consumer, &id, |_conn| Box::pin(async { Ok(42u8) }))
        .await
        .unwrap();
    assert_eq!(retried, Outcome::Processed(42));
}

#[tokio::test]
async fn purge_deletes_only_entries_outside_the_window() {
    let inbox = inbox().await;
    let consumer = ConsumerId::from("billing");

    inbox
        .process(&consumer, &MessageId::from("old"), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    // Backdate the first entry by an hour.
    sqlx::query("UPDATE inbox_messages SET processed_at = ? WHERE message_id = 'old'")
        .bind(chrono::Utc::now() - chrono::Duration::hours(1))
        .execute(inbox.pool())
        .await
        .unwrap();

    inbox
        .process(&consumer, &MessageId::from("new"), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    let policy = RetentionPolicy::new(Duration::from_secs(600));
    assert_eq!(inbox.purge(&policy).await.unwrap(), 1);

    // The recent entry survived, so it is still seen as a duplicate.
    let outcome = inbox
        .process(&consumer, &MessageId::from("new"), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Skipped);
}

#[tokio::test]
async fn purge_batches_across_multiple_passes() {
    let inbox = inbox().await;
    let consumer = ConsumerId::from("billing");

    for message in ["m-1", "m-2", "m-3"] {
        inbox
            .process(&consumer, &MessageId::from(message), |_c| {
                Box::pin(async { Ok(()) })
            })
            .await
            .unwrap();
    }

    // Backdate all three entries beyond the retention window.
    sqlx::query("UPDATE inbox_messages SET processed_at = ?")
        .bind(chrono::Utc::now() - chrono::Duration::hours(1))
        .execute(inbox.pool())
        .await
        .unwrap();

    // batch_size(1) forces three loop iterations plus a terminating pass,
    // exercising the batching path that a batch_size of 1000 (the default)
    // never touches with this few rows.
    let policy = RetentionPolicy::new(Duration::from_secs(600)).with_batch_size(1);
    assert_eq!(inbox.purge(&policy).await.unwrap(), 3);
}
