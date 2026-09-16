#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use txbox::sqlite::SqliteInbox;
use txbox::{
    Claim, ConsumerId, InboxError, InboxExt, InboxStore, MessageId, Outcome, RetentionPolicy,
};

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
async fn a_rolled_back_claim_leaves_no_trace() {
    let inbox = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();

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
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();

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
    let consumer = ConsumerId::try_from("billing").unwrap();

    inbox
        .process(&consumer, &MessageId::try_from("old").unwrap(), |_c| {
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
        .process(&consumer, &MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    let policy = RetentionPolicy::new(Duration::from_secs(600));
    assert_eq!(inbox.purge(&policy).await.unwrap(), 1);

    // The recent entry survived, so it is still seen as a duplicate.
    let outcome = inbox
        .process(&consumer, &MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Skipped);
}

#[tokio::test]
async fn purge_batches_across_multiple_passes() {
    let inbox = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();

    for message in ["m-1", "m-2", "m-3"] {
        inbox
            .process(&consumer, &MessageId::try_from(message).unwrap(), |_c| {
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

/// PostgreSQL and SQLite diverge under a claim race, and the README says so:
/// PostgreSQL blocks the loser until the winner resolves, while file-backed
/// SQLite with more than one connection refuses it outright. Both are correct —
/// redelivery reprocesses the message either way — but a caller that only ever
/// ran against one of them would be surprised by the other.
///
/// The rest of this suite uses a single in-memory connection, where a race is
/// impossible, so nothing exercised this until now.
#[tokio::test]
async fn on_sqlite_the_loser_of_a_claim_race_is_refused_rather_than_blocked() {
    const HANDLER: Duration = Duration::from_millis(1500);

    let path = std::env::temp_dir().join(format!("txbox-busy-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        // Left at its default, sqlx waits on a locked database and the loser
        // would eventually succeed, hiding the very divergence under test.
        .busy_timeout(Duration::ZERO);

    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("open file-backed sqlite");

    let inbox = Arc::new(SqliteInbox::new(pool));
    inbox.migrate().await.expect("run migrations");

    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-1").unwrap();

    let winner = tokio::spawn({
        let inbox = Arc::clone(&inbox);
        let consumer = consumer.clone();
        let id = id.clone();
        async move {
            inbox
                .process(&consumer, &id, |_conn| {
                    Box::pin(async move {
                        tokio::time::sleep(HANDLER).await;
                        Ok::<_, txbox::HandlerError>(())
                    })
                })
                .await
        }
    });

    // Let the winner take the write lock before the loser attempts the same
    // insert.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let result = inbox
        .process(&consumer, &id, |_conn| Box::pin(async { Ok(()) }))
        .await;

    // Asserting on the variant alone would also accept an unrelated backend
    // failure, such as a pool timeout. Pin the actual SQLite code, which also
    // exercises the downcast recipe documented on `InboxError::Backend`.
    let error = result.expect_err("the loser must be refused by the backend");
    let InboxError::Backend(source) = &error else {
        panic!("expected a backend failure, got {error}");
    };
    let code = source
        .downcast_ref::<sqlx::Error>()
        .and_then(|e| e.as_database_error())
        .and_then(|e| e.code())
        .map(|c| c.into_owned());
    assert_eq!(
        code.as_deref(),
        Some("5"),
        "expected SQLITE_BUSY, got {error}"
    );
    assert_eq!(
        winner
            .await
            .expect("task did not panic")
            .expect("no inbox error"),
        Outcome::Processed(()),
        "the winner must still process the message"
    );

    let _ = std::fs::remove_file(&path);
}
