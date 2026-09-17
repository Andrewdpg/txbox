#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxError, InboxExt, MessageId, Outcome};

/// The container handle must stay alive for as long as the pool is used;
/// dropping it stops the database.
async fn inbox() -> (ContainerAsync<PostgresImage>, PgPool, Arc<PgInbox>) {
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

    let inbox = Arc::new(PgInbox::new(pool.clone()));
    inbox.migrate().await.expect("run migrations");
    (container, pool, inbox)
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

/// The README states that on PostgreSQL the loser of a claim race blocks until
/// the winner's transaction resolves, rather than returning immediately. That
/// is a consequence of how `INSERT ... ON CONFLICT DO NOTHING` treats an
/// uncommitted conflicting row, and it means the loser's latency is bounded by
/// the winner's *handler*, not by the database.
///
/// Nothing verified that claim until now. It is pinned here because the cost it
/// describes is exactly what any future optimisation of the duplicate path
/// would change.
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

    assert_eq!(outcome, Outcome::Skipped, "the loser must skip");
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

// A test previously lived here asserting that the (now-removed) duplicate
// fast path still applied the effect exactly once under a race. `process`
// no longer has a fast path to race against — it always takes claim's
// transactional path — so that scenario no longer exists. The property it
// was guarding (`claim` arbitrates a race correctly) is already covered by
// `concurrent_delivery_applies_the_effect_exactly_once` above, which doesn't
// depend on the removed toggle at all.

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
    assert_eq!(retried, Outcome::Skipped, "the retry must see a duplicate");
}
