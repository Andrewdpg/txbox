use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use txbox::{
    BoxFuture, Claim, ClaimBatch, ClaimRequest, Consumer, ConsumerId, InboxError, InboxExt,
    InboxStore, MessageId, Outcome, RetentionPolicy, Savepoints,
};

/// A fake connection that records the business effects applied to it.
#[derive(Default)]
struct FakeConn {
    effects: Vec<String>,
}

/// A fake transaction. Dropping it without committing discards its effects,
/// mirroring a real database rollback.
struct FakeTx {
    conn: FakeConn,
}

impl Deref for FakeTx {
    type Target = FakeConn;
    fn deref(&self) -> &FakeConn {
        &self.conn
    }
}

impl DerefMut for FakeTx {
    fn deref_mut(&mut self) -> &mut FakeConn {
        &mut self.conn
    }
}

/// Cloned by `consumer()`, so the recorded state is shared rather than copied:
/// a clone that forgot what the original had seen would report every message as
/// fresh and quietly invalidate these tests.
#[derive(Default, Clone)]
struct FakeStore {
    seen: Arc<Mutex<HashSet<(String, String)>>>,
    committed: Arc<Mutex<Vec<String>>>,
    rollbacks: Arc<Mutex<usize>>,
    fail_rollback: bool,
    batches: Arc<Mutex<Vec<Vec<String>>>>,
    short_batches: bool,
    begins: Arc<Mutex<usize>>,
}

impl InboxStore for FakeStore {
    type Conn = FakeConn;
    type Tx = FakeTx;

    fn begin(&self) -> BoxFuture<'_, Result<FakeTx, InboxError>> {
        *self.begins.lock().unwrap() += 1;
        Box::pin(async {
            Ok(FakeTx {
                conn: FakeConn::default(),
            })
        })
    }

    fn commit(&self, tx: FakeTx) -> BoxFuture<'_, Result<(), InboxError>> {
        Box::pin(async move {
            self.committed.lock().unwrap().extend(tx.conn.effects);
            Ok(())
        })
    }

    // Mutating `self.seen` here instead of going through `_conn` is only
    // acceptable because this fake exists to test `InboxExt`'s control
    // flow (does the handler run, does it roll back, ...), not transactional
    // semantics. A real `InboxStore` backend MUST perform its claim on
    // `conn` — see the contract on `InboxStore::claim`.
    fn claim<'a>(
        &'a self,
        _conn: &'a mut FakeConn,
        request: ClaimRequest<'a>,
    ) -> BoxFuture<'a, Result<Claim, InboxError>> {
        Box::pin(async move {
            let inserted = self.seen.lock().unwrap().insert((
                request.consumer.as_str().to_owned(),
                request.id.as_str().to_owned(),
            ));
            Ok(if inserted {
                Claim::Fresh
            } else {
                Claim::Duplicate
            })
        })
    }

    fn claim_many<'a>(
        &'a self,
        conn: &'a mut FakeConn,
        batch: ClaimBatch<'a>,
    ) -> BoxFuture<'a, Result<Vec<Claim>, InboxError>> {
        Box::pin(async move {
            self.batches
                .lock()
                .unwrap()
                .push(batch.ids.iter().map(|id| id.as_str().to_owned()).collect());
            let mut claims = Vec::new();
            for id in batch.ids {
                claims.push(
                    self.claim(conn, ClaimRequest::new(batch.consumer, id))
                        .await?,
                );
            }
            if self.short_batches {
                claims.pop();
            }
            Ok(claims)
        })
    }

    fn rollback(&self, tx: FakeTx) -> BoxFuture<'_, Result<(), InboxError>> {
        Box::pin(async move {
            drop(tx);
            *self.rollbacks.lock().unwrap() += 1;
            if self.fail_rollback {
                Err(InboxError::Backend("rollback failed".into()))
            } else {
                Ok(())
            }
        })
    }

    fn purge<'a>(&'a self, _policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async { Ok(0) })
    }
}

// Enough to reach `process_many`; these tests don't exercise savepoints.
impl Savepoints for FakeStore {
    fn savepoint<'a>(&'a self, _: &'a mut FakeConn) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async { Ok(()) })
    }
    fn release_and_savepoint<'a>(
        &'a self,
        _: &'a mut FakeConn,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async { Ok(()) })
    }
    fn release<'a>(&'a self, _: &'a mut FakeConn) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async { Ok(()) })
    }
    fn rollback_to<'a>(&'a self, _: &'a mut FakeConn) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async { Ok(()) })
    }
    fn unclaim<'a>(
        &'a self,
        _: &'a mut FakeConn,
        _: &'a ConsumerId,
        _: &'a MessageId,
    ) -> BoxFuture<'a, Result<(), InboxError>> {
        Box::pin(async { Ok(()) })
    }
}

fn billing(store: &FakeStore) -> Consumer<FakeStore> {
    store.consumer(ConsumerId::try_from("billing").unwrap())
}

#[tokio::test]
async fn first_delivery_runs_the_handler() {
    let store = FakeStore::default();
    let id = MessageId::try_from("m-1").unwrap();

    let outcome = billing(&store)
        .process(&id, |conn| {
            Box::pin(async move {
                conn.effects.push("charged".to_owned());
                Ok(1u8)
            })
        })
        .await
        .unwrap();

    assert_eq!(outcome, Outcome::Processed(1));
    assert_eq!(store.committed.lock().unwrap().as_slice(), ["charged"]);
}

#[tokio::test]
async fn second_delivery_is_skipped_and_the_handler_never_runs() {
    let store = FakeStore::default();
    let id = MessageId::try_from("m-1").unwrap();

    billing(&store)
        .process(&id, |conn| {
            Box::pin(async move {
                conn.effects.push("charged".to_owned());
                Ok(1u8)
            })
        })
        .await
        .unwrap();

    let outcome = billing(&store)
        .process::<_, u8>(&id, |_conn| {
            Box::pin(async { panic!("handler must not run for a duplicate") })
        })
        .await
        .unwrap();

    assert_eq!(outcome, Outcome::Duplicate);
    assert_eq!(store.committed.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn distinct_consumers_both_process_the_same_message() {
    let store = FakeStore::default();
    let id = MessageId::try_from("m-1").unwrap();

    for name in ["billing", "notifications"] {
        let outcome = store
            .consumer(ConsumerId::try_from(name).unwrap())
            .process(&id, |conn| {
                Box::pin(async move {
                    conn.effects.push("handled".to_owned());
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(outcome, Outcome::Processed(()));
    }

    assert_eq!(store.committed.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_failing_handler_discards_the_effect() {
    let store = FakeStore::default();

    let result = billing(&store)
        .process(&MessageId::try_from("m-1").unwrap(), |conn| {
            Box::pin(async move {
                conn.effects.push("charged".to_owned());
                Err::<(), _>("boom".into())
            })
        })
        .await;

    assert!(matches!(result, Err(InboxError::Handler(_))));
    assert!(store.committed.lock().unwrap().is_empty());
}

/// Compile-time proof that the trait is dyn-compatible and that the returned
/// futures are `Send`. If either property regresses this test stops compiling.
#[test]
fn store_is_dyn_compatible_and_futures_are_send() {
    fn assert_send<T: Send>(_: &T) {}

    let store = FakeStore::default();
    let erased: &dyn InboxStore<Conn = FakeConn, Tx = FakeTx> = &store;
    let policy = RetentionPolicy::new(Duration::from_secs(60));
    assert_send(&erased.purge(&policy));
}

#[tokio::test]
async fn a_failing_handler_is_rolled_back_explicitly() {
    let store = FakeStore::default();
    let id = MessageId::try_from("m-1").unwrap();

    let result = billing(&store)
        .process::<_, ()>(&id, |_conn| Box::pin(async { Err("boom".into()) }))
        .await;

    assert!(matches!(result, Err(InboxError::Handler(_))));
    assert_eq!(
        *store.rollbacks.lock().unwrap(),
        1,
        "process must call InboxStore::rollback"
    );
}

#[tokio::test]
async fn a_failing_rollback_still_reports_the_handler_error() {
    let store = FakeStore {
        fail_rollback: true,
        ..FakeStore::default()
    };
    let id = MessageId::try_from("m-1").unwrap();

    let result = billing(&store)
        .process::<_, ()>(&id, |_conn| Box::pin(async { Err("boom".into()) }))
        .await;

    match result {
        Err(InboxError::Handler(e)) => assert_eq!(e.to_string(), "boom"),
        other => panic!("expected the handler's error, got {other:?}"),
    }
}

fn ids(raw: &[&str]) -> Vec<MessageId> {
    raw.iter()
        .map(|id| MessageId::try_from(*id).unwrap())
        .collect()
}

#[tokio::test]
async fn claim_many_maps_repeats_to_duplicate() {
    let store = FakeStore::default();
    let consumer = billing(&store);
    let mut tx = consumer.begin().await.unwrap();

    let claims = consumer
        .claim_many(&mut tx, &ids(&["b", "a", "b"]))
        .await
        .unwrap();

    assert_eq!(claims, [Claim::Fresh, Claim::Fresh, Claim::Duplicate]);
}

#[tokio::test]
async fn claim_many_sends_unique_sorted_ids_to_the_backend() {
    let store = FakeStore::default();
    let consumer = billing(&store);
    let mut tx = consumer.begin().await.unwrap();

    consumer
        .claim_many(&mut tx, &ids(&["c", "a", "c", "b"]))
        .await
        .unwrap();

    assert_eq!(*store.batches.lock().unwrap(), [["a", "b", "c"]]);
}

#[tokio::test]
async fn claim_many_of_nothing_never_reaches_the_backend() {
    let store = FakeStore::default();
    let consumer = billing(&store);
    let mut tx = consumer.begin().await.unwrap();

    let claims = consumer.claim_many(&mut tx, &[]).await.unwrap();

    assert!(claims.is_empty());
    assert!(store.batches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_backend_returning_too_few_claims_is_an_error() {
    let store = FakeStore {
        short_batches: true,
        ..FakeStore::default()
    };
    let consumer = billing(&store);
    let mut tx = consumer.begin().await.unwrap();

    let result = consumer.claim_many(&mut tx, &ids(&["a", "b"])).await;

    assert!(
        matches!(result, Err(InboxError::Backend(_))),
        "got {result:?}"
    );
}

#[test]
fn claim_batch_builds_a_lock_timeout_like_claim_request() {
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();
    let ids = [&id];

    let batch = ClaimBatch::new(&consumer, &ids).with_lock_timeout(Duration::from_millis(200));

    assert_eq!(batch.lock_timeout, Some(Duration::from_millis(200)));
}

#[tokio::test]
async fn a_duplicate_is_rolled_back_explicitly() {
    let store = FakeStore::default();
    let id = MessageId::try_from("m-1").unwrap();
    fn noop(_: &mut FakeConn) -> BoxFuture<'_, Result<(), txbox::HandlerError>> {
        Box::pin(async { Ok(()) })
    }

    billing(&store).process(&id, noop).await.unwrap();
    let outcome = billing(&store).process(&id, noop).await.unwrap();

    assert_eq!(outcome, Outcome::Duplicate);
    assert_eq!(*store.rollbacks.lock().unwrap(), 1);
}

#[tokio::test]
async fn process_many_of_nothing_never_opens_a_transaction() {
    let store = FakeStore::default();

    let results = billing(&store)
        .process_many::<_, ()>(&[], |_conn, _id| Box::pin(async { Ok(()) }))
        .await
        .unwrap();

    assert!(results.is_empty());
    assert_eq!(*store.begins.lock().unwrap(), 0);
}
