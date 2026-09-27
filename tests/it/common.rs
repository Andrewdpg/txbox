//! Databases for the suites. A container handle must outlive every pool on
//! it: dropping it stops the database.

#[cfg(feature = "mysql")]
use testcontainers_modules::mysql::Mysql as MysqlImage;
#[cfg(feature = "postgres")]
use testcontainers_modules::postgres::Postgres as PostgresImage;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
#[cfg(any(feature = "postgres", feature = "mysql"))]
use txbox::MessageId;

#[cfg(feature = "postgres")]
pub async fn postgres(
    max_connections: u32,
) -> (ContainerAsync<PostgresImage>, txbox::postgres::PgInbox) {
    let container = PostgresImage::default()
        .with_tag("15-alpine")
        .start()
        .await
        .expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.expect("map port");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("connect to postgres");
    let inbox = txbox::postgres::PgInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox)
}

#[cfg(feature = "mysql")]
pub async fn mysql_url() -> (ContainerAsync<MysqlImage>, String) {
    let container = MysqlImage::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("start mysql");
    let port = container.get_host_port_ipv4(3306).await.expect("map port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

#[cfg(feature = "mysql")]
pub async fn mysql(max_connections: u32) -> (ContainerAsync<MysqlImage>, txbox::mysql::MySqlInbox) {
    let (container, url) = mysql_url().await;
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("connect to mysql");
    let inbox = txbox::mysql::MySqlInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    (container, inbox)
}

/// One connection: every new in-memory connection opens its own empty database.
#[cfg(feature = "sqlite")]
pub async fn sqlite() -> txbox::sqlite::SqliteInbox {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("connect to in-memory sqlite");
    let inbox = txbox::sqlite::SqliteInbox::new(pool);
    inbox.migrate().await.expect("run migrations");
    inbox
}

/// `n` ids in an order that depends on `seed`.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub fn shuffled(n: usize, seed: u64) -> Vec<MessageId> {
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
