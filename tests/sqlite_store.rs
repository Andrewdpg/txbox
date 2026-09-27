#![cfg(feature = "sqlite")]

use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use txbox::sqlite::SqliteInbox;
use txbox::{
    Claim, ClaimRequest, ConsumerId, InboxError, InboxExt, InboxStore, MessageId, Outcome,
    RetentionPolicy,
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
        inbox
            .claim(&mut tx, ClaimRequest::new(&consumer, &id))
            .await
            .unwrap(),
        Claim::Fresh
    );
    inbox.commit(tx).await.unwrap();

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox
            .claim(&mut tx, ClaimRequest::new(&consumer, &id))
            .await
            .unwrap(),
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
        inbox
            .claim(&mut tx, ClaimRequest::new(&consumer, &id))
            .await
            .unwrap(),
        Claim::Fresh
    );
    drop(tx); // rollback

    let mut tx = inbox.begin().await.unwrap();
    assert_eq!(
        inbox
            .claim(&mut tx, ClaimRequest::new(&consumer, &id))
            .await
            .unwrap(),
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
        .consumer(consumer.clone())
        .process(&id, |_conn| {
            Box::pin(async { Err::<(), _>("handler exploded".into()) })
        })
        .await;
    assert!(failed.is_err());

    let retried = inbox
        .consumer(consumer.clone())
        .process(&id, |_conn| Box::pin(async { Ok(42u8) }))
        .await
        .unwrap();
    assert_eq!(retried, Outcome::Processed(42));
}

#[tokio::test]
async fn purge_deletes_only_entries_outside_the_window() {
    let inbox = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();

    inbox
        .consumer(consumer.clone())
        .process(&MessageId::try_from("old").unwrap(), |_c| {
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
        .consumer(consumer.clone())
        .process(&MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    let policy = RetentionPolicy::new(Duration::from_secs(600));
    assert_eq!(inbox.purge(&policy).await.unwrap(), 1);

    // The recent entry survived, so it is still seen as a duplicate.
    let outcome = inbox
        .consumer(consumer.clone())
        .process(&MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Duplicate);
}

#[tokio::test]
async fn purge_batches_across_multiple_passes() {
    let inbox = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();

    for message in ["m-1", "m-2", "m-3"] {
        inbox
            .consumer(consumer.clone())
            .process(&MessageId::try_from(message).unwrap(), |_c| {
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

/// File-backed SQLite with more than one connection refuses the loser of a
/// claim race (SQLITE_BUSY) instead of blocking it. That is contention, the
/// same `Contended` the other backends return, not a backend failure.
#[tokio::test]
async fn on_sqlite_the_loser_of_a_claim_race_gets_contended() {
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
                .consumer(consumer.clone())
                .process(&id, |_conn| {
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
        .consumer(consumer.clone())
        .process(&id, |_conn| Box::pin(async { Ok(()) }))
        .await;

    assert!(
        matches!(result, Err(InboxError::Contended)),
        "expected Contended, got {result:?}"
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

#[tokio::test]
async fn claim_many_round_trips_awkward_ids() {
    let inbox = inbox().await;
    let consumer = inbox.consumer(ConsumerId::try_from("odd").unwrap());
    let raw = [
        "q\"uote",
        "back\\slash",
        "\u{f1}-\u{6f22}-\u{1F600}",
        "tab\tinside",
        "[1,2]",
        "{\"a\":1}",
    ];
    let ids: Vec<MessageId> = raw
        .iter()
        .map(|r| MessageId::try_from(*r).unwrap())
        .collect();

    let mut tx = consumer.begin().await.unwrap();
    let first = consumer.claim_many(&mut tx, &ids).await.unwrap();
    let again = consumer.claim_many(&mut tx, &ids).await.unwrap();
    consumer.commit(tx).await.unwrap();

    assert!(first.iter().all(|c| *c == Claim::Fresh), "{first:?}");
    assert!(again.iter().all(|c| *c == Claim::Duplicate), "{again:?}");
    let stored: Vec<String> =
        sqlx::query_scalar("SELECT message_id FROM inbox_messages ORDER BY message_id")
            .fetch_all(inbox.pool())
            .await
            .unwrap();
    let mut want: Vec<String> = raw.iter().map(|r| r.to_string()).collect();
    want.sort();
    assert_eq!(stored, want, "ids must be stored byte for byte");
}

#[tokio::test]
async fn process_many_runs_the_handler_once_per_distinct_id() {
    let inbox = inbox().await;
    let consumer = inbox.consumer(ConsumerId::try_from("many").unwrap());
    let ids: Vec<MessageId> = ["a", "b", "a"]
        .iter()
        .map(|r| MessageId::try_from(*r).unwrap())
        .collect();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));

    let results = consumer
        .process_many(&ids, |_conn, id| {
            let calls = Arc::clone(&calls);
            let id = id.as_str().to_owned();
            Box::pin(async move {
                calls.lock().unwrap().push(id);
                Ok(())
            })
        })
        .await
        .unwrap();

    assert_eq!(*calls.lock().unwrap(), ["a", "b"]);
    assert!(matches!(results[0], Ok(Outcome::Processed(()))));
    assert!(matches!(results[1], Ok(Outcome::Processed(()))));
    assert!(matches!(results[2], Ok(Outcome::Duplicate)));
}

/// A repeat of an id whose handler failed must not come back as
/// `Duplicate`: a caller acking per message would ack the repeat and drop a
/// message that was never processed.
#[tokio::test]
async fn process_many_reports_a_repeat_of_a_failed_id_as_failed() {
    let inbox = inbox().await;
    let consumer_id = ConsumerId::try_from("repeat-poison").unwrap();
    let consumer = inbox.consumer(consumer_id.clone());
    let ids: Vec<MessageId> = ["a", "a"]
        .iter()
        .map(|r| MessageId::try_from(*r).unwrap())
        .collect();

    let results = consumer
        .process_many(&ids, |_conn, _id| {
            Box::pin(async { Err::<(), txbox::HandlerError>("boom".into()) })
        })
        .await
        .unwrap();

    assert!(results.iter().all(Result::is_err), "{results:?}");
    assert!(
        !inbox
            .is_known_duplicate(&consumer_id, &ids[0])
            .await
            .unwrap(),
        "a failed message must stay unclaimed"
    );
}

#[tokio::test]
async fn a_committed_claim_is_a_known_duplicate() {
    let inbox = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();
    assert!(!inbox.is_known_duplicate(&consumer, &id).await.unwrap());

    let mut tx = inbox.begin().await.unwrap();
    inbox
        .claim(&mut tx, ClaimRequest::new(&consumer, &id))
        .await
        .unwrap();
    inbox.commit(tx).await.unwrap();

    assert!(inbox.is_known_duplicate(&consumer, &id).await.unwrap());
}
