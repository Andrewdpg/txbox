#![cfg(feature = "postgres")]

use crate::common::{postgres, shuffled};

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxError, InboxExt, MessageId, Outcome};

async fn inbox() -> (ContainerAsync<PostgresImage>, PgPool, Arc<PgInbox>) {
    let (container, inbox) = postgres(8).await;
    (container, inbox.pool().clone(), Arc::new(inbox))
}

/// Two consumers race on the same message, exactly as they would during a
/// partition rebalance. Precisely one must win, and the business effect must
/// be applied exactly once.
///
/// If `claim` were ever rewritten as a SELECT followed by an INSERT, this test
/// is what catches it: both tasks would read "not present" and both would
/// insert an `effects` row.
#[tokio::test]
async fn concurrent_delivery_applies_the_effect_exactly_once() {
    let (_container, pool, inbox) = inbox().await;

    sqlx::query("CREATE TABLE effects (message_id TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create effects table");

    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-1").unwrap();

    let mut handles = Vec::new();
    for _ in 0..2 {
        let inbox = Arc::clone(&inbox);
        let consumer = consumer.clone();
        let id = id.clone();

        handles.push(tokio::spawn(async move {
            inbox
                .consumer(consumer.clone())
                .process(&id, |conn| {
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
            Outcome::Duplicate => skipped += 1,
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

/// The README states that on PostgreSQL the loser of a claim race blocks until
/// the winner's transaction resolves, rather than returning immediately. That
/// is a consequence of how `INSERT ... ON CONFLICT DO NOTHING` treats an
/// uncommitted conflicting row, and it means the loser's latency is bounded by
/// the winner's *handler*, not by the database.
#[tokio::test]
async fn the_loser_of_a_claim_race_waits_for_the_winner_to_finish() {
    const HANDLER: Duration = Duration::from_millis(1500);
    const CLAIM_HEADSTART: Duration = Duration::from_millis(250);

    let (_container, _pool, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-2").unwrap();

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

    // Let the winner take the row before the loser attempts the same insert.
    tokio::time::sleep(CLAIM_HEADSTART).await;

    let started = Instant::now();
    let outcome = inbox
        .consumer(consumer.clone())
        .process(&id, |_conn| Box::pin(async { Ok(()) }))
        .await
        .expect("no inbox error");
    let waited = started.elapsed();

    assert_eq!(outcome, Outcome::Duplicate, "the loser must skip");
    assert_eq!(
        winner
            .await
            .expect("task did not panic")
            .expect("no inbox error"),
        Outcome::Processed(()),
        "the winner must process"
    );

    // The loser cannot have returned before the winner committed. The bound is
    // deliberately loose — half the remaining handler time — so that the test
    // proves blocking without becoming a timing flake.
    let floor = (HANDLER - CLAIM_HEADSTART) / 2;
    assert!(
        waited >= floor,
        "the loser returned after {waited:?}, which is less than {floor:?}: \
         it did not block on the winner's transaction"
    );
}

/// `with_lock_timeout` exists to turn the blocking wait proven above into a
/// fast, explicit error. Consumer A claims the row and holds its transaction
/// open for a long handler; consumer B, configured with a short lock
/// timeout, must come back with `InboxError::Contended` quickly rather than
/// waiting out A's handler. Once A commits, B's retry must see a plain
/// duplicate.
///
/// The assertion on `waited` is deliberately tight around the timeout rather
/// than the handler duration — the whole point is that B does *not* wait
/// anywhere near `HANDLER`. A timing-free assertion (checking only the
/// error variant) would pass even if `SET LOCAL lock_timeout` were silently
/// dropped and B blocked for the full handler, so it would not catch that
/// regression.
#[tokio::test]
async fn a_short_lock_timeout_returns_contended_quickly_instead_of_blocking() {
    const HANDLER: Duration = Duration::from_secs(5);
    const CLAIM_HEADSTART: Duration = Duration::from_millis(250);
    const LOCK_TIMEOUT: Duration = Duration::from_millis(200);

    let (_container, _pool, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-4").unwrap();

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

    // Let the winner take the row before the contended consumer attempts it.
    tokio::time::sleep(CLAIM_HEADSTART).await;

    let contended = inbox
        .consumer(consumer.clone())
        .with_lock_timeout(LOCK_TIMEOUT);

    let started = Instant::now();
    let result = contended
        .process(&id, |_conn| Box::pin(async { Ok(()) }))
        .await;
    let waited = started.elapsed();

    match result {
        Err(InboxError::Contended) => {}
        other => panic!("expected InboxError::Contended, got {other:?}"),
    }

    // The floor is well under HANDLER: a regression that fell back to
    // blocking would take seconds, not tens of milliseconds. The ceiling
    // gives CI jitter room without letting a multi-second block sneak past.
    assert!(
        waited < HANDLER / 2,
        "the contended claim returned after {waited:?}, which is not \
         meaningfully faster than the {HANDLER:?} handler it should have \
         avoided waiting for — the lock timeout did not take effect"
    );

    assert_eq!(
        winner
            .await
            .expect("task did not panic")
            .expect("no inbox error"),
        Outcome::Processed(()),
        "the winner must still process the message"
    );

    // Now that the winner has committed, the row exists and the retry finds
    // a plain, already-processed duplicate — no contention left to hit.
    let retried = inbox
        .consumer(consumer.clone())
        .with_lock_timeout(LOCK_TIMEOUT)
        .process(&id, |_conn| Box::pin(async { Ok(()) }))
        .await
        .expect("no inbox error");
    assert_eq!(
        retried,
        Outcome::Duplicate,
        "the retry must see a duplicate"
    );
}

/// PostgreSQL reads `lock_timeout = 0` as "no timeout". A zero (or
/// sub-millisecond) `Duration` must still mean "don't wait", so the
/// contended claim has to come back with `Contended` right away instead of
/// blocking behind the winner's whole handler.
#[tokio::test]
async fn a_zero_lock_timeout_fails_fast_instead_of_disabling_the_timeout() {
    const HANDLER: Duration = Duration::from_secs(5);
    const CLAIM_HEADSTART: Duration = Duration::from_millis(250);

    let (_container, _pool, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("contended-zero").unwrap();

    let winner = tokio::spawn({
        let inbox = Arc::clone(&inbox);
        let consumer = consumer.clone();
        let id = id.clone();
        async move {
            inbox
                .consumer(consumer)
                .process(&id, |_conn| {
                    Box::pin(async move {
                        tokio::time::sleep(HANDLER).await;
                        Ok::<_, txbox::HandlerError>(())
                    })
                })
                .await
        }
    });

    tokio::time::sleep(CLAIM_HEADSTART).await;

    let started = Instant::now();
    let result = inbox
        .consumer(consumer)
        .with_lock_timeout(Duration::ZERO)
        .process(&id, |_conn| Box::pin(async { Ok(()) }))
        .await;
    let waited = started.elapsed();

    assert!(
        matches!(result, Err(InboxError::Contended)),
        "expected InboxError::Contended, got {result:?}"
    );
    assert!(
        waited < HANDLER / 2,
        "a zero timeout waited {waited:?}: it was sent as `0ms`, which disables the timeout"
    );
    winner
        .await
        .expect("task did not panic")
        .expect("no inbox error");
}

/// Four consumers claim the same 2000 ids at once, each in a different
/// random order. Sorting the lock order in SQL makes a deadlock impossible.
#[tokio::test]
async fn crossed_claim_many_batches_never_deadlock() {
    let (_container, _pool, inbox) = inbox().await;
    let consumer = ConsumerId::try_from("crossed").unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(4));

    let mut workers = Vec::new();
    for w in 0..4u64 {
        let (inbox, consumer, barrier) =
            (Arc::clone(&inbox), consumer.clone(), Arc::clone(&barrier));
        workers.push(tokio::spawn(async move {
            let batch = shuffled(2000, w);
            let consumer = inbox.consumer(consumer);
            let mut tx = consumer.begin().await.unwrap();
            barrier.wait().await;
            let claims = consumer.claim_many(&mut tx, &batch).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            consumer.commit(tx).await?;
            Ok::<_, InboxError>(claims.iter().filter(|c| **c == txbox::Claim::Fresh).count())
        }));
    }

    let mut fresh = 0;
    for w in workers {
        fresh += w.await.unwrap().expect("no worker may fail");
    }
    assert_eq!(fresh, 2000, "every id is fresh for exactly one worker");
}

/// Two single-message claims crossed in opposite order: PostgreSQL aborts
/// one as a deadlock victim. That is contention, not a backend failure.
#[tokio::test]
async fn a_deadlock_victim_gets_contended() {
    let (_container, _pool, inbox) = inbox().await;
    let consumer = inbox.consumer(ConsumerId::try_from("deadlock").unwrap());
    let (a, b) = (
        MessageId::try_from("a").unwrap(),
        MessageId::try_from("b").unwrap(),
    );

    let mut tx1 = consumer.begin().await.unwrap();
    let mut tx2 = consumer.begin().await.unwrap();
    consumer.claim(&mut tx1, &a).await.unwrap();
    consumer.claim(&mut tx2, &b).await.unwrap();

    let (first, second) = tokio::join!(consumer.claim(&mut tx1, &b), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        consumer.claim(&mut tx2, &a).await
    });
    let errors: Vec<_> = [first, second]
        .into_iter()
        .filter_map(Result::err)
        .collect();

    assert_eq!(errors.len(), 1, "exactly one side is the deadlock victim");
    assert!(
        matches!(errors[0], InboxError::Contended),
        "got {:?}",
        errors[0]
    );
}
