use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use txbox::{
    BoxFuture, Claim, ClaimRequest, Consumer, ConsumerId, InboxError, InboxExt, InboxStore,
    MessageId, Outcome, RetentionPolicy,
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
}

impl InboxStore for FakeStore {
    type Conn = FakeConn;
    type Tx = FakeTx;

    fn begin(&self) -> BoxFuture<'_, Result<FakeTx, InboxError>> {
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

    fn purge<'a>(&'a self, _policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async { Ok(0) })
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
