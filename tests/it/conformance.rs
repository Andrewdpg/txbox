#![cfg(feature = "testing")]

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use txbox::testing::{conformance, savepoints_conformance};
use txbox::{BoxFuture, Claim, ClaimRequest, InboxError, InboxStore, RetentionPolicy};

type Key = (String, String);

/// Pending claims of one transaction.
#[derive(Default)]
struct FakeConn {
    pending: HashSet<Key>,
}

struct FakeTx {
    conn: FakeConn,
    store: FakeStore,
    done: bool,
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

impl Drop for FakeTx {
    fn drop(&mut self) {
        // A leaky backend returns the connection to the pool with the
        // transaction still open: the next transaction inherits its writes.
        if !self.done && self.store.leak_on_drop {
            let pending = std::mem::take(&mut self.conn.pending);
            self.store.leftover.lock().unwrap().extend(pending);
        }
    }
}

/// An in-memory, transactional store. `fold_case` and `leak_on_drop` each
/// break one contract the conformance suite must catch.
#[derive(Default, Clone)]
struct FakeStore {
    committed: Arc<Mutex<HashSet<Key>>>,
    leftover: Arc<Mutex<HashSet<Key>>>,
    fold_case: bool,
    leak_on_drop: bool,
}

impl FakeStore {
    fn key(&self, request: &ClaimRequest<'_>) -> Key {
        let id = request.id.as_str();
        let id = if self.fold_case {
            id.to_lowercase()
        } else {
            id.to_owned()
        };
        (request.consumer.as_str().to_owned(), id)
    }
}

impl InboxStore for FakeStore {
    type Conn = FakeConn;
    type Tx = FakeTx;

    fn begin(&self) -> BoxFuture<'_, Result<FakeTx, InboxError>> {
        Box::pin(async move {
            let pending = std::mem::take(&mut *self.leftover.lock().unwrap());
            Ok(FakeTx {
                conn: FakeConn { pending },
                store: self.clone(),
                done: false,
            })
        })
    }

    // Settled before the future: an `async move` touching only `tx.done`
    // captures that field alone, and `tx` itself would drop unsettled.
    fn commit(&self, mut tx: FakeTx) -> BoxFuture<'_, Result<(), InboxError>> {
        tx.done = true;
        let pending = std::mem::take(&mut tx.conn.pending);
        self.committed.lock().unwrap().extend(pending);
        Box::pin(async { Ok(()) })
    }

    fn rollback(&self, mut tx: FakeTx) -> BoxFuture<'_, Result<(), InboxError>> {
        tx.done = true;
        drop(tx);
        Box::pin(async { Ok(()) })
    }

    fn claim<'a>(
        &'a self,
        conn: &'a mut FakeConn,
        request: ClaimRequest<'a>,
    ) -> BoxFuture<'a, Result<Claim, InboxError>> {
        Box::pin(async move {
            let key = self.key(&request);
            let known = self.committed.lock().unwrap().contains(&key);
            Ok(if known || !conn.pending.insert(key) {
                Claim::Duplicate
            } else {
                Claim::Fresh
            })
        })
    }

    fn purge<'a>(&'a self, _policy: &'a RetentionPolicy) -> BoxFuture<'a, Result<u64, InboxError>> {
        Box::pin(async { Ok(0) })
    }
}

#[tokio::test]
async fn transactional_fake_passes_conformance() {
    conformance(FakeStore::default()).await;
}

#[tokio::test]
#[should_panic(expected = "compare byte for byte")]
async fn case_folding_store_fails_conformance() {
    conformance(FakeStore {
        fold_case: true,
        ..FakeStore::default()
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "dropped without commit")]
async fn leaky_store_fails_conformance() {
    conformance(FakeStore {
        leak_on_drop: true,
        ..FakeStore::default()
    })
    .await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_conforms() {
    let inbox = crate::common::sqlite().await;
    // Twice on one database: runs must not see each other's rows.
    conformance(inbox.clone()).await;
    conformance(inbox.clone()).await;
    savepoints_conformance(inbox).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_conforms() {
    let (_container, inbox) = crate::common::postgres(1).await;
    conformance(inbox.clone()).await;
    savepoints_conformance(inbox).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_conforms() {
    let (_container, inbox) = crate::common::mysql(1).await;
    conformance(inbox.clone()).await;
    savepoints_conformance(inbox).await;
}
