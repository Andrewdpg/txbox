# txbox

[![CI](https://github.com/Andrewdpg/txbox/actions/workflows/ci.yml/badge.svg)](https://github.com/Andrewdpg/txbox/actions/workflows/ci.yml)

Transactional inbox pattern for reliable message processing in Rust.

## The problem

Under at-least-once delivery, a consumer will eventually receive the same
message more than once. If your handler applies a business effect that
isn't idempotent by construction, duplicates corrupt your data. `txbox`
records the message as processed in the *same database transaction* as the
effect: both commit, or both roll back.

This guarantee only covers effects applied through the `&mut Conn` the
handler is handed. Anything the handler does outside that connection — an
HTTP call, a publish to another broker, a write to a different database —
is not part of the transaction and will be repeated on redelivery.

See [`docs/design.md`](docs/design.md#before-and-after) for a side-by-side
comparison of a consumer with and without `txbox`.

## Quickstart

```rust
use sqlx::postgres::PgPoolOptions;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxExt, InboxStore, MessageId, Outcome};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
let pool = PgPoolOptions::new()
    .connect("postgres://localhost/mydb")
    .await?;

let inbox = PgInbox::new(pool);
inbox.migrate().await?; // explicit — see "Migrations" below

let orders = inbox.consumer(ConsumerId::try_from("orders-billing")?);
let id = MessageId::try_from("msg-123")?;

let outcome = orders
    .process(&id, |conn| {
        Box::pin(async move {
            sqlx::query("INSERT INTO orders (payload) VALUES ($1)")
                .bind("...")
                .execute(&mut *conn)
                .await?;
            Ok::<_, txbox::HandlerError>(())
        })
    })
    .await?;

match outcome {
    Outcome::Processed(()) => println!("handled"),
    Outcome::Duplicate => println!("duplicate, ignored"),
}
Ok(()) }
```

Import both: `InboxStore` for `migrate`/`purge`, `InboxExt` for
`.consumer(...)`.

## Backend support

| Backend    | Feature    | Crate      |
|------------|------------|------------|
| PostgreSQL | `postgres` | `sqlx` 0.9 |
| SQLite     | `sqlite`   | `sqlx` 0.9 |

Neither is enabled by default. `sqlx` is re-exported as `txbox::sqlx`, so
handlers writing their own SQL stay on the same version.

## Read before you rely on this

- **The inbox must live in the same database as the effect.** A shared
  central database for the inbox breaks the transaction silently — see
  [`docs/operations.md`](docs/operations.md#the-inbox-must-live-in-the-same-database-as-the-effect).
- **Two services consuming the same topic must use different `ConsumerId`s.**
  The dedup key is `(consumer_id, message_id)`; sharing one makes whichever
  service processes a message first cause the other to silently skip it.
- **`max_age` on `RetentionPolicy` must exceed the broker's redelivery
  window** (Kafka `retention.ms`, an SQS visibility timeout, etc). Purging a
  row the broker can still redeliver lets that message be processed twice.
- **Effects outside the handler's connection aren't covered.** An HTTP call
  or a write to another database inside the handler still repeats on
  redelivery.
- **Commit the broker offset only after the inbox transaction commits.**
  With Kafka's `enable.auto.commit`, the offset can advance on a timer
  regardless of whether your handler ran, turning at-least-once delivery
  into at-most-once. See `examples/kafka_consumer.rs`.

More on choosing a message id and consumer id: [`docs/guide.md`](docs/guide.md#choosing-a-message-id).

## Migrations

`migrate()` runs `sqlx::migrate!` against the crate's bundled migrations,
called explicitly — `txbox` never alters your schema on its own at startup.

### PostgreSQL

```sql
CREATE TABLE IF NOT EXISTS inbox_messages (
    consumer_id  TEXT        NOT NULL,
    message_id   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_inbox_processed_at
    ON inbox_messages (processed_at);
```

Also exposed as `postgres::MIGRATION_SQL` if you manage schema with your own
tooling instead of calling `migrate()`.

### SQLite

Same shape, with `processed_at TEXT`. Also exposed as `sqlite::MIGRATION_SQL`.

Running a per-tenant schema? See [`docs/guide.md`](docs/guide.md#multi-tenant-schemas).

## More

- [Batching several claims into one transaction](docs/guide.md#batching)
- [Recording what a handler decided](docs/guide.md#recording-what-a-handler-decided)
- [Sizing the connection pool, and contention under load](docs/operations.md#sizing-and-contention)
- [Purge scheduling and observability](docs/operations.md#purge-scheduling)
- [Notes before copying the bundled example into production](docs/operations.md#dont-copy-this-into-production)
- [What this crate is deliberately out of scope for](docs/design.md#scope)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
