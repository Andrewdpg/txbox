#![cfg(feature = "mysql")]

use std::time::Duration;

use sqlx::mysql::MySqlPoolOptions;
use testcontainers_modules::mysql::Mysql as MysqlImage;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use txbox::mysql::MySqlInbox;
use txbox::{Claim, ClaimRequest, ConsumerId, InboxExt, InboxStore, MessageId, RetentionPolicy};

/// The container handle must stay alive for as long as the pool is used.
async fn inbox(max_connections: u32) -> (ContainerAsync<MysqlImage>, MySqlInbox) {
    let container = MysqlImage::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("start mysql");
    let port = container.get_host_port_ipv4(3306).await.expect("map port");
    let pool = MySqlPoolOptions::new()
        .max_connections(max_connections)
        .connect(&format!("mysql://root@127.0.0.1:{port}/test"))
        .await
        .expect("connect to mysql");
    let inbox = MySqlInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox)
}

/// sqlx always sets CLIENT_FOUND_ROWS, under which a no-op
/// `ON DUPLICATE KEY UPDATE` reports one affected row for a duplicate too.
/// A duplicate must still read as a duplicate.
#[tokio::test]
async fn claim_is_fresh_once_then_duplicate() {
    let (_container, inbox) = inbox(2).await;
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();

    for expected in [Claim::Fresh, Claim::Duplicate] {
        let mut tx = inbox.begin().await.unwrap();
        let claim = inbox
            .claim(&mut tx, ClaimRequest::new(&consumer, &id))
            .await
            .unwrap();
        inbox.commit(tx).await.unwrap();
        assert_eq!(claim, expected);
    }
    assert!(inbox.is_known_duplicate(&consumer, &id).await.unwrap());
}

#[tokio::test]
async fn purge_deletes_only_entries_outside_the_window() {
    let (_container, inbox) = inbox(2).await;
    let consumer = inbox.consumer(ConsumerId::try_from("billing").unwrap());
    for id in ["old", "new"] {
        let mut tx = consumer.begin().await.unwrap();
        consumer
            .claim(&mut tx, &MessageId::try_from(id).unwrap())
            .await
            .unwrap();
        consumer.commit(tx).await.unwrap();
    }
    sqlx::query(
        "UPDATE inbox_messages SET processed_at = UTC_TIMESTAMP(6) - INTERVAL 2 HOUR WHERE message_id = 'old'",
    )
    .execute(inbox.pool())
    .await
    .unwrap();

    let removed = inbox
        .purge(&RetentionPolicy::new(Duration::from_secs(3600)).with_batch_size(1))
        .await
        .unwrap();

    assert_eq!(removed, 1);
    let left: Vec<String> = sqlx::query_scalar("SELECT message_id FROM inbox_messages")
        .fetch_all(inbox.pool())
        .await
        .unwrap();
    assert_eq!(left, ["new"]);
}

#[tokio::test]
async fn claim_many_twice_in_one_transaction_sees_its_own_claims() {
    let (_container, inbox) = inbox(2).await;
    let consumer = inbox.consumer(ConsumerId::try_from("twice").unwrap());
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

fn shuffled(n: usize, seed: u64) -> Vec<MessageId> {
    let mut v: Vec<MessageId> = (0..n)
        .map(|i| MessageId::try_from(format!("m{i:05}")).unwrap())
        .collect();
    let mut s = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    for i in (1..v.len()).rev() {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        v.swap(i, (s >> 33) as usize % (i + 1));
    }
    v
}

/// Measured before the fix: 87 of 120 such transactions deadlocked.
#[tokio::test]
async fn crossed_claim_many_batches_never_deadlock() {
    let (_container, inbox) = inbox(8).await;
    let consumer = ConsumerId::try_from("crossed").unwrap();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(4));

    let mut workers = Vec::new();
    for w in 0..4u64 {
        let (inbox, consumer, barrier) = (inbox.clone(), consumer.clone(), barrier.clone());
        workers.push(tokio::spawn(async move {
            let batch = shuffled(2000, w);
            let consumer = inbox.consumer(consumer);
            let mut tx = consumer.begin().await.unwrap();
            barrier.wait().await;
            let claims = consumer.claim_many(&mut tx, &batch).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            consumer.commit(tx).await?;
            Ok::<_, txbox::InboxError>(claims.iter().filter(|c| **c == Claim::Fresh).count())
        }));
    }

    let mut fresh = 0;
    for w in workers {
        fresh += w.await.unwrap().expect("no worker may fail");
    }
    assert_eq!(fresh, 2000);
}

#[tokio::test]
async fn a_lock_timeout_returns_contended_and_does_not_leak_to_the_next_transaction() {
    let (_container, inbox) = inbox(2).await;
    let consumer_id = ConsumerId::try_from("contended").unwrap();
    let id = MessageId::try_from("m").unwrap();

    let holder = inbox.consumer(consumer_id.clone());
    let mut held = holder.begin().await.unwrap();
    holder.claim(&mut held, &id).await.unwrap();

    let contended = inbox
        .consumer(consumer_id)
        .with_lock_timeout(Duration::from_millis(1));
    let mut tx = contended.begin().await.unwrap();
    let started = std::time::Instant::now();
    let result = contended.claim(&mut tx, &id).await;
    let waited = started.elapsed();
    drop(tx);

    assert!(
        matches!(result, Err(txbox::InboxError::Contended)),
        "got {result:?}"
    );
    assert!(
        waited < Duration::from_secs(5),
        "rounded up to 1s, not MySQL's 50s default; waited {waited:?}"
    );

    // The holder still has the other connection, so the next transaction gets
    // the one that just carried the 1s timeout. It must be back on the default.
    let mut next = inbox.begin().await.unwrap();
    let timeout: u64 = sqlx::query_scalar("SELECT @@SESSION.innodb_lock_wait_timeout")
        .fetch_one(&mut *next)
        .await
        .unwrap();
    assert_eq!(timeout, 50, "a lock timeout leaked through the pool");
    drop(next);
    holder.rollback(held).await.unwrap();
}

#[tokio::test]
async fn a_deadlock_victim_gets_contended() {
    let (_container, inbox) = inbox(2).await;
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
        matches!(errors[0], txbox::InboxError::Contended),
        "got {:?}",
        errors[0]
    );
}

/// One pool, one connection: txbox and a plain query share it, as they do
/// in an application that hands txbox its only pool.
async fn one_connection_inbox(
    after_connect_timeout: Option<u64>,
) -> (ContainerAsync<MysqlImage>, MySqlInbox, String) {
    let container = MysqlImage::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("start mysql");
    let port = container.get_host_port_ipv4(3306).await.expect("map port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                if let Some(secs) = after_connect_timeout {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "SET SESSION innodb_lock_wait_timeout = {secs}"
                    )))
                    .execute(conn)
                    .await?;
                }
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("connect to mysql");
    let inbox = MySqlInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox, url)
}

async fn session_lock_timeout(inbox: &MySqlInbox) -> u64 {
    sqlx::query_scalar("SELECT @@SESSION.innodb_lock_wait_timeout")
        .fetch_one(inbox.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_lock_timeout_does_not_leak_to_other_users_of_the_pool() {
    let (_container, inbox, _url) = one_connection_inbox(None).await;
    let consumer = inbox
        .consumer(ConsumerId::try_from("leak").unwrap())
        .with_lock_timeout(Duration::from_secs(1));
    let ids = [MessageId::try_from("a").unwrap()];

    let mut tx = consumer.begin().await.unwrap();
    consumer.claim(&mut tx, &ids[0]).await.unwrap();
    consumer.claim_many(&mut tx, &ids).await.unwrap();
    consumer.commit(tx).await.unwrap();

    assert_eq!(
        session_lock_timeout(&inbox).await,
        50,
        "a plain query inherited txbox's timeout"
    );
}

#[tokio::test]
async fn a_session_lock_timeout_set_by_the_application_is_kept() {
    let (_container, inbox, _url) = one_connection_inbox(Some(7)).await;
    let consumer = inbox.consumer(ConsumerId::try_from("kept").unwrap());

    let mut tx = consumer.begin().await.unwrap();
    consumer.commit(tx).await.unwrap();
    assert_eq!(
        session_lock_timeout(&inbox).await,
        7,
        "BEGIN overwrote the application's value"
    );

    let consumer = consumer.with_lock_timeout(Duration::from_secs(1));
    tx = consumer.begin().await.unwrap();
    consumer
        .claim(&mut tx, &MessageId::try_from("a").unwrap())
        .await
        .unwrap();
    consumer.commit(tx).await.unwrap();
    assert_eq!(
        session_lock_timeout(&inbox).await,
        7,
        "the claim didn't restore the application's value"
    );
}

#[tokio::test]
async fn a_contended_claim_still_restores_the_lock_timeout() {
    let (_container, inbox, url) = one_connection_inbox(None).await;
    // The holder sits on its own pool, so the inbox's only connection is free.
    let holder = MySqlInbox::new(
        MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap(),
    );
    let consumer_id = ConsumerId::try_from("contended").unwrap();
    let id = MessageId::try_from("m").unwrap();
    let holding = holder.consumer(consumer_id.clone());
    let mut held = holding.begin().await.unwrap();
    holding.claim(&mut held, &id).await.unwrap();

    let consumer = inbox
        .consumer(consumer_id)
        .with_lock_timeout(Duration::from_secs(1));
    let mut tx = consumer.begin().await.unwrap();
    let result = consumer.claim(&mut tx, &id).await;
    drop(tx);

    assert!(
        matches!(result, Err(txbox::InboxError::Contended)),
        "got {result:?}"
    );
    assert_eq!(
        session_lock_timeout(&inbox).await,
        50,
        "the error path left txbox's timeout behind"
    );
    holding.rollback(held).await.unwrap();
}

/// A claim cancelled between setting its timeout and restoring it leaves
/// the saved value behind; the next BEGIN must put it back.
#[tokio::test]
async fn begin_restores_a_lock_timeout_left_by_a_cancelled_claim() {
    let (_container, inbox, _url) = one_connection_inbox(Some(7)).await;
    // The state a claim cancelled mid-way leaves on the connection.
    sqlx::raw_sql(
        "SET @txbox_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout, \
             SESSION innodb_lock_wait_timeout = 1",
    )
    .execute(inbox.pool())
    .await
    .unwrap();

    let tx = inbox.begin().await.unwrap();
    inbox.rollback(tx).await.unwrap();

    assert_eq!(session_lock_timeout(&inbox).await, 7);
}

/// `utf8mb4_bin` is PAD SPACE, so `'a'` and `'a '` would be one key. Ids
/// reject trailing whitespace today; the table must not depend on that.
#[tokio::test]
async fn the_table_does_not_pad_ids_with_spaces() {
    let (_container, inbox) = inbox(1).await;
    for id in ["a", "a "] {
        sqlx::query(
            "INSERT INTO inbox_messages (consumer_id, message_id, processed_at) \
             VALUES ('c', ?, UTC_TIMESTAMP(6))",
        )
        .bind(id)
        .execute(inbox.pool())
        .await
        .unwrap_or_else(|e| panic!("inserting {id:?}: {e}"));
    }
}
