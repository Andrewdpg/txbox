#![cfg(feature = "testing")]

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use txbox::testing::conformance;
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
async fn sqlite_passes_conformance_twice_on_one_database() {
    use sqlx::sqlite::SqlitePoolOptions;
    use txbox::sqlite::SqliteInbox;

    // One connection: a transaction leaked back to the pool is only visible
    // when the next caller gets the same connection.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let inbox = SqliteInbox::new(pool);
    inbox.migrate().await.unwrap();
    conformance(inbox.clone()).await;
    conformance(inbox).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_passes_conformance() {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers_modules::postgres::Postgres as PostgresImage;
    use testcontainers_modules::testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use txbox::postgres::PgInbox;

    let container = PostgresImage::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("map port");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("connect to postgres");
    let inbox = PgInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    conformance(inbox).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_passes_savepoints_conformance() {
    use sqlx::sqlite::SqlitePoolOptions;
    use txbox::sqlite::SqliteInbox;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let inbox = SqliteInbox::new(pool);
    inbox.migrate().await.unwrap();
    txbox::testing::savepoints_conformance(inbox).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_passes_savepoints_conformance() {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers_modules::postgres::Postgres as PostgresImage;
    use testcontainers_modules::testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use txbox::postgres::PgInbox;

    let container = PostgresImage::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("map port");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("connect to postgres");
    let inbox = PgInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    txbox::testing::savepoints_conformance(inbox).await;
}

#[cfg(feature = "mysql")]
async fn mysql_one_connection() -> (
    testcontainers_modules::testcontainers::ContainerAsync<testcontainers_modules::mysql::Mysql>,
    txbox::mysql::MySqlInbox,
) {
    use sqlx::mysql::MySqlPoolOptions;
    use testcontainers_modules::mysql::Mysql as MysqlImage;
    use testcontainers_modules::testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    let container = MysqlImage::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("start mysql");
    let port = container.get_host_port_ipv4(3306).await.expect("map port");
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&format!("mysql://root@127.0.0.1:{port}/test"))
        .await
        .expect("connect to mysql");
    let inbox = txbox::mysql::MySqlInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox)
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_passes_conformance() {
    let (_container, inbox) = mysql_one_connection().await;
    conformance(inbox).await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_passes_savepoints_conformance() {
    let (_container, inbox) = mysql_one_connection().await;
    txbox::testing::savepoints_conformance(inbox).await;
}
