#![cfg(feature = "postgres")]

use crate::common::postgres;

use std::time::Duration;

use chrono::{DateTime, Utc};
use txbox::{
    Claim, ClaimRequest, ConsumerId, InboxExt, InboxStore, MessageId, Outcome, RetentionPolicy,
};

#[tokio::test]
async fn claim_is_fresh_once_then_duplicate() {
    let (_container, inbox) = postgres(8).await;
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
async fn distinct_consumers_both_process_the_same_message() {
    let (_container, inbox) = postgres(8).await;
    let id = MessageId::try_from("shared-1").unwrap();

    for consumer in ["billing", "notifications"] {
        let outcome = inbox
            .consumer(ConsumerId::try_from(consumer).unwrap())
            .process(&id, |_c| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
        assert_eq!(outcome, Outcome::Processed(()));
    }
}

#[tokio::test]
async fn purge_deletes_only_entries_outside_the_window() {
    let (_container, inbox) = postgres(8).await;
    let consumer = ConsumerId::try_from("billing").unwrap();

    inbox
        .consumer(consumer.clone())
        .process(&MessageId::try_from("old").unwrap(), |_c| {
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
        .consumer(consumer.clone())
        .process(&MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();

    let policy = RetentionPolicy::new(Duration::from_secs(600));
    assert_eq!(inbox.purge(&policy).await.unwrap(), 1);

    let outcome = inbox
        .consumer(consumer.clone())
        .process(&MessageId::try_from("new").unwrap(), |_c| {
            Box::pin(async { Ok(()) })
        })
        .await
        .unwrap();
    assert_eq!(outcome, Outcome::Duplicate);
}

/// `processed_at` must come from the database clock: a replica with a slow
/// clock would write rows that look older than they are, and purge could drop
/// them inside the broker's redelivery window. `now()` is the transaction's
/// start time, so the stored value must equal it exactly.
#[tokio::test]
async fn processed_at_comes_from_the_database_clock() {
    let (_container, inbox) = postgres(8).await;
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

    let (stored, transaction_start): (DateTime<Utc>, DateTime<Utc>) =
        sqlx::query_as("SELECT processed_at, now() FROM inbox_messages WHERE message_id = $1")
            .bind(id.as_str())
            .fetch_one(&mut *tx)
            .await
            .unwrap();

    assert_eq!(
        stored, transaction_start,
        "processed_at must be the database's clock, not the caller's"
    );

    inbox.commit(tx).await.unwrap();
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
    let (_container, inbox) = postgres(8).await;

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

/// One message per transaction means one fsync per message. `claim` is public
/// and takes `&mut Conn`, so a caller can claim a whole batch inside a single
/// transaction and amortise that cost.
///
/// The property that makes it safe is subtle: a claim is visible to later
/// statements in its own transaction before it is visible to anyone else. A
/// message repeated inside one batch is therefore caught by the same mechanism
/// that catches a redelivery, with no bookkeeping by the caller.
#[tokio::test]
async fn a_duplicate_inside_one_batch_is_caught_before_the_commit() {
    let (_container, inbox) = postgres(8).await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let batch = ["a", "b", "a"].map(|id| MessageId::try_from(id).unwrap());

    let mut tx = inbox.begin().await.unwrap();
    let mut claims = Vec::new();
    for id in &batch {
        claims.push(
            inbox
                .claim(&mut tx, ClaimRequest::new(&consumer, id))
                .await
                .unwrap(),
        );
    }
    inbox.commit(tx).await.unwrap();

    assert_eq!(claims, [Claim::Fresh, Claim::Fresh, Claim::Duplicate]);
}

/// A batch is one transaction, so it is also one unit of failure: if anything
/// in it fails, every message goes back to unclaimed and the broker redelivers
/// the whole batch.
#[tokio::test]
async fn a_batch_that_rolls_back_leaves_every_message_unclaimed() {
    let (_container, inbox) = postgres(8).await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let batch = ["a", "b"].map(|id| MessageId::try_from(id).unwrap());

    let mut tx = inbox.begin().await.unwrap();
    for id in &batch {
        assert_eq!(
            inbox
                .claim(&mut tx, ClaimRequest::new(&consumer, id))
                .await
                .unwrap(),
            Claim::Fresh
        );
    }
    tx.rollback().await.unwrap();

    for id in &batch {
        let mut tx = inbox.begin().await.unwrap();
        assert_eq!(
            inbox
                .claim(&mut tx, ClaimRequest::new(&consumer, id))
                .await
                .unwrap(),
            Claim::Fresh,
            "a rolled back batch must leave {id} redeliverable"
        );
        inbox.commit(tx).await.unwrap();
    }
}

/// `is_known_duplicate` is a plain read outside any transaction, kept as an
/// explicitly-called method for replay and backfill scenarios. A committed
/// inbox row can never become uncommitted, so a read that finds one is
/// definitive; a read that finds nothing proves nothing — the message may be
/// fresh, or a concurrent claim may be in flight — so a caller relying on this
/// method rather than `process` must still treat `false` as "unknown", never
/// "fresh".

#[tokio::test]
async fn a_committed_claim_is_a_known_duplicate() {
    let (_container, db) = postgres(8).await;
    let billing = db.consumer(ConsumerId::try_from("billing").unwrap());
    let id = MessageId::try_from("m-1").unwrap();

    assert!(
        !billing.is_known_duplicate(&id).await.unwrap(),
        "an unseen message cannot be known to be a duplicate"
    );

    billing
        .process(&id, |_c| Box::pin(async { Ok(()) }))
        .await
        .unwrap();

    assert!(
        billing.is_known_duplicate(&id).await.unwrap(),
        "a committed claim must be visible to is_known_duplicate"
    );
}

#[tokio::test]
async fn claim_many_claims_a_large_batch_in_one_call() {
    let (_container, inbox) = postgres(8).await;
    let consumer = inbox.consumer(ConsumerId::try_from("bulk").unwrap());
    let ids: Vec<MessageId> = (0..10_000)
        .map(|i| MessageId::try_from(format!("m{i}")).unwrap())
        .collect();

    let mut tx = consumer.begin().await.unwrap();
    let first = consumer.claim_many(&mut tx, &ids[..5_000]).await.unwrap();
    let second = consumer.claim_many(&mut tx, &ids).await.unwrap();
    consumer.commit(tx).await.unwrap();

    assert!(first.iter().all(|c| *c == Claim::Fresh));
    assert!(second[..5_000].iter().all(|c| *c == Claim::Duplicate));
    assert!(second[5_000..].iter().all(|c| *c == Claim::Fresh));
}
