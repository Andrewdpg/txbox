# txbox

[![CI](https://github.com/Andrewdpg/txbox/actions/workflows/ci.yml/badge.svg)](https://github.com/Andrewdpg/txbox/actions/workflows/ci.yml)

Transactional inbox pattern for reliable message processing in Rust.

## The problem

Under at-least-once delivery — the only guarantee most brokers make — a
consumer will eventually receive the same message more than once: a
retry after a slow acknowledgement, a partition rebalance, a producer
resending after a timeout it can't distinguish from a lost write. If your
handler applies a business effect (an insert, a charge, a state
transition) and that effect isn't idempotent by construction, duplicates
corrupt your data. `txbox` closes that gap by recording the message as
processed in the *same database transaction* as the business effect: both
commit, or both roll back. There is no window where the effect happened
but the record didn't, or vice versa.

This guarantee holds only for effects applied through the `&mut Conn` the
handler is handed. Anything the handler does outside that connection — an
HTTP call to a payment API, publishing to another broker, writing to a
different database — is **not** part of the transaction and **will** be
repeated on redelivery.

## Before and after

Here is a consumer that looks correct — the kind of code a competent
engineer would write and ship — without `txbox`:

```rust,ignore
loop {
    let message = consumer.recv().await?;
    let id = message.key_view::<str>().transpose()?.ok_or("no key")?;
    let payload = message.payload().unwrap_or_default().to_vec();

    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO orders (payload) VALUES ($1)")
        .bind(&payload[..])
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    consumer.commit_message(&message, CommitMode::Async)?;
}
```

Nothing here is careless: it opens a transaction, applies the effect, commits
it, then commits the offset — in the right order. The problem is what this
code has no way to know: whether `id` has already been applied. Nothing
records that fact anywhere, so every redelivery — a rebalance, a retry, a
second consumer sharing a group — reruns the insert.

The same consumer with `txbox`:

```rust,ignore
loop {
    let message = consumer.recv().await?;
    let id = MessageId::try_from(message.key_view::<str>().transpose()?.ok_or("no key")?)?;
    let payload = message.payload().unwrap_or_default().to_vec();

    let outcome = inbox
        .process(&consumer_id, &id, move |conn| {
            Box::pin(async move {
                sqlx::query("INSERT INTO orders (payload) VALUES ($1)")
                    .bind(&payload[..])
                    .execute(&mut *conn)
                    .await?;
                Ok::<_, txbox::HandlerError>(())
            })
        })
        .await?;

    consumer.commit_message(&message, CommitMode::Async)?;
}
```

The insert and the "I've seen this message" record now live in the same
transaction, so `process` can tell a redelivery from a first delivery instead
of just trusting it.

| What happens | Without txbox | With txbox |
|---|---|---|
| Consumer crashes after the business effect commits, before the offset commits | Broker redelivers; insert runs again — effect applied twice | Broker redelivers; `claim` finds the row already present, handler doesn't run — effect applied once |
| Partition rebalance redelivers an already-processed batch | Every message in the batch is reapplied | Each message is skipped as a duplicate; only genuinely new ones run the handler |
| The handler fails partway through | Same in both: the broker's own delivery semantics decide whether it retries — `txbox` doesn't change this | Same in both, but if it does retry, the aborted attempt left no partial row (transaction rolled back), so the retry is treated as fresh, not as a duplicate |
| Same message delivered concurrently to two instances of the same consumer | Both instances see no prior record and both apply the effect — a race, not a possibility | One claims the row and proceeds; the other loses the race and skips (`tests/postgres_concurrency.rs` asserts exactly one `Processed` and one `Skipped` for a concurrent pair on PostgreSQL) |
| Two different services consume the same topic | Neither has any dedup signal at all; both simply run their own logic | Only correct if each service uses its own `ConsumerId` — the dedup key is `(consumer_id, message_id)`, so two services sharing one `ConsumerId` will see the second service's messages silently skipped as duplicates it never actually ran |

None of this extends past the connection `process()` hands the handler. As
noted above, an HTTP call, a publish to another broker, or a write to a
different database happens outside that transaction and will still run again
on redelivery — `txbox` only makes the effects applied through `&mut Conn`
exactly-once.

## Backend support

| Backend    | Feature    | Crate         |
|------------|------------|---------------|
| PostgreSQL | `postgres` | `sqlx` 0.9    |
| SQLite     | `sqlite`   | `sqlx` 0.9    |

Neither feature is enabled by default; enable the one you need.

Handlers write their own SQL and backends are built from an `sqlx` pool,
so `sqlx` is re-exported as `txbox::sqlx`. Reach it there rather than
declaring the dependency separately, and the two cannot drift onto
different versions.

Two backend behaviors differ and are worth knowing:

- On PostgreSQL, a consumer that loses the claim race **blocks** until
  the winner's transaction resolves, then sees `Duplicate`. On
  file-backed SQLite with more than one connection, the loser instead
  gets `SQLITE_BUSY`, surfacing as `InboxError::Backend`. Both are
  correct — redelivery reprocesses the message either way — but the
  failure mode differs.
- SQLite stores `processed_at` as `TEXT` and the purge compares it
  lexicographically. This is correct for values written by this crate,
  but a different tool writing that column in another format would
  break purging. Another reason to let `migrate()` own the schema.

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
    Outcome::Skipped => println!("duplicate, ignored"),
}
Ok(()) }
```

Import both: `InboxStore` for `migrate` and `purge`, `InboxExt` for
`consumer` — `InboxExt` is an extension trait, blanket-implemented for
every store.

A [`Consumer`] is one logical consumer of one stream. It carries its own
identifier, so that identifier is settled once instead of being passed on
every call, and its own tuning, so two queues sharing a pool can be
configured independently. `migrate` and `purge` stay on the backend: they
belong to the database, not to a queue.

## Migrations

`migrate()` runs `sqlx::migrate!` against the crate's bundled migrations.
It is explicit: `txbox` never alters your schema on its own at startup.
Migrations belong to your deployment cycle, not to a library's
constructor. If you manage schema with Liquibase, Flyway, Atlas, or your
own tooling, apply the SQL below directly instead of calling `migrate()`.

### PostgreSQL (`migrations/postgres/20260916000001_create_inbox_messages.sql`)

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

### SQLite (`migrations/sqlite/20260916000001_create_inbox_messages.sql`)

```sql
CREATE TABLE IF NOT EXISTS inbox_messages (
    consumer_id  TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (consumer_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_inbox_processed_at
    ON inbox_messages (processed_at);
```

## Batching

`process` opens a transaction per message, so a high-volume consumer pays
one commit — and one fsync — per message. A consumer also exposes the
transaction directly, so a caller can claim a whole batch inside one:

```rust
use txbox::postgres::PgInbox;
use txbox::sqlx;
use txbox::{Claim, Consumer, MessageId};

async fn run(
    orders: &Consumer<PgInbox>,
    batch: &[MessageId],
) -> Result<(), Box<dyn std::error::Error>> {
let mut tx = orders.begin().await?;

for id in batch {
    if orders.claim(&mut tx, id).await? == Claim::Fresh {
        sqlx::query("INSERT INTO orders (message_id) VALUES ($1)")
            .bind(id.as_str())
            .execute(&mut *tx)
            .await?;
    }
}

orders.commit(tx).await?;
Ok(()) }
```

Two things make this work, and one of them is not obvious:

- **A repeated message inside one batch is caught for free.** A claim is
  visible to later statements in its own transaction before it is visible
  to anyone else, so the second occurrence of an id sees the first and
  reports `Duplicate`. The caller needs no bookkeeping of its own.
- **A batch is one unit of failure.** If anything in it fails, the
  transaction rolls back and *every* message it covered becomes unclaimed
  again — the broker redelivers the whole batch and the handlers that had
  already succeeded run a second time. Choosing a batch size is choosing
  how much work one failure repeats.

Two costs come with it. The transaction is held open for the whole
batch's work, so a concurrent consumer racing for any id in that batch
blocks for that entire span rather than for one message. And commit the
broker's offsets only after `commit` returns, for the batch as a whole.

To abandon a batch, drop the transaction: that rolls it back. `InboxStore`
has no explicit rollback, so generic code relies on the drop; code written
against a concrete backend can call `rollback()` on the transaction
itself.

## Sizing

`process` holds its transaction — and therefore a pooled connection — for
the whole handler. That is the price of the guarantee: the inbox row and
the handler's effects must commit together, so the connection cannot go
back to the pool while the handler is still working.

That makes throughput arithmetic. With `P` connections and a handler
taking `H`, no more than `P/H` messages complete per second, however many
tasks are pushing. Measured with a pool of 8 and a 25 ms handler, against
an arithmetic ceiling of 320 msg/s:

| workers | throughput | p50 latency |
|---|---|---|
| 4  | 148 msg/s (46% of ceiling) | 27 ms |
| 8  | 291 msg/s (91%) | 27 ms |
| 16 | 289 msg/s (90%) | 55 ms |
| 32 | 291 msg/s (91%) | 110 ms |
| 64 | 292 msg/s (91%) | 218 ms |

Throughput stops at the pool. Latency does not: it doubles with every
doubling of the worker count, because each message waits behind the
others for a connection. At 64 workers over 8 connections a message waits
out eight handlers before its own runs.

**Size the worker count to the pool, not to the machine.** Workers past
the pool buy queueing and nothing else. If you need more throughput,
raise `max_connections` or shorten the handler — those are the only two
terms in the ceiling.

### Contention

A consumer that loses a claim race waits for the winner's transaction,
which means it waits out the winner's handler — and holds its own pooled
connection the whole time it waits. It is fair to wonder whether that
compounds when several consumers race at once. It does not:

| contenders | winner | slowest loser |
|---|---|---|
| 2  | 508 ms | 509 ms |
| 4  | 506 ms | 506 ms |
| 8  | 503 ms | 503 ms |
| 16 | 503 ms | 504 ms |

The losers all wait on the same uncommitted row, wake together when the
winner commits, and then find a committed row and skip immediately. A
rebalance storm that delivers one message to sixteen consumers costs one
handler, not sixteen.

The caveat is the pool again: every blocked loser is holding a connection
while it waits. Contention does not multiply the delay, but it does
occupy connections for the duration — with an 8-connection pool and a
70-second handler, eight contenders parked for 70 seconds each exhausts
the pool outright.

`Consumer::with_lock_timeout` converts that wait into a fast, explicit
error instead of a block:

```rust
# use sqlx::PgPool;
use std::time::Duration;
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxExt};

# fn build(pool: PgPool) -> Result<(), txbox::InvalidId> {
let db = PgInbox::new(pool);
let orders = db
    .consumer(ConsumerId::try_from("orders")?)
    .with_lock_timeout(Duration::from_millis(200));
# let _ = orders;
# Ok(()) }
```

On PostgreSQL, this issues `SET LOCAL lock_timeout` inside the claiming
transaction, so a consumer that cannot acquire a contended row within the
timeout gets `InboxError::Contended` back instead of blocking for the
winner's handler. `Contended` means another consumer is processing this
exact message right now — the broker will redeliver it, so the correct
response is to let that happen, never to dead-letter it.

This is unset by default, preserving the blocking behaviour described
above. It is also a no-op on SQLite: SQLite's equivalent is
connection-level (`SqliteConnectOptions::busy_timeout`), which belongs to
the pool the caller builds, not to a single consumer's configuration.

## Retention

```rust
use std::time::Duration;
use txbox::RetentionPolicy;

let policy = RetentionPolicy::new(Duration::from_secs(7 * 24 * 60 * 60))
    .with_batch_size(1000); // rows deleted per statement, default 1000
```

**`max_age` must exceed the broker's redelivery window** — a Kafka
topic's `retention.ms`, an SQS queue's visibility timeout, or whatever
the equivalent is for your transport. If an inbox row is purged while the
broker can still redeliver the message it guards, that redelivery will be
treated as fresh and the handler will run again — exactly the duplicate
this crate exists to prevent.

There is deliberately no row-count limit. The correctness rule here is
temporal, not volumetric: a message is safe to forget once its
redelivery window has passed, regardless of how many other rows exist.
A size cap (e.g. "keep only the last N rows") would, under a traffic
spike, delete entries that are still inside the redelivery window just
because enough newer rows pushed them out — letting duplicates through
at exactly the moment of peak load, which is the worst possible time.

On PostgreSQL, `processed_at` and the purge cutoff both come from the
database's own clock (`now()`), not from the process that happens to be
running. That matters because the rule above is temporal: a replica with
a lagging clock would write rows that look older than they are, and a
purge host running fast would delete rows still inside the window — both
at the worst possible moment, under the load that put the extra replicas
there. The database is the one clock every replica already shares, so
there is nothing to keep in sync.

On SQLite the timestamp still comes from the calling process, because
there is nowhere else for it to come from: SQLite runs inside that
process, so its clock *is* the caller's clock and the skew described
above cannot arise.

### What retention costs

Measured by filling the inbox to 300 000 rows and sampling as it grew:

| rows | table | index | claim p50 |
|---|---|---|---|
| 50 000  |  5.1 MB |  2.1 MB | 456 µs |
| 150 000 | 15.5 MB |  6.8 MB | 456 µs |
| 300 000 | 31.0 MB | 13.6 MB | 448 µs |

Growth is linear at roughly 150 bytes per retained message across table
and index together, so a week of retention at ten messages a second is
about 900 MB. **Claiming does not get slower as the table grows** — the
median held between 409 µs and 465 µs across the whole range, which is
noise. A btree deepens logarithmically, and 300 000 rows do not move it.

Purging removed 301 200 rows in 781 ms, near 385 000 rows a second. The
purge will not be your bottleneck.

What does deserve attention is that **`DELETE` reclaims nothing on its
own**. After the purge above, the table still occupied its full 31 MB
until `VACUUM` ran. Autovacuum normally handles this, but the shape of
the recovery is worth knowing: `VACUUM` truncated the table to 16 MB
because this run deleted every row and left empty pages at the end of the
file, while the index did not shrink at all — its pages are marked
reusable and stay put.

A live inbox never has that shape. Retention deletes the oldest rows
while new ones are appended, so the freed space sits in the middle of the
file and is reused rather than returned. **Size the disk for what
retention holds, and do not expect a purge to shrink anything.**

## Purge scheduling

`purge` must run on exactly one instance. With N replicas each running
their own in-process loop, all N compete to delete the same rows —
wasted work at best, lock contention at worst. Prefer a Kubernetes
`CronJob`, a single leader-elected process, or any other mechanism that
guarantees one runner:

```rust
use std::time::Duration;
use txbox::{InboxStore, RetentionPolicy};

async fn run_purge_loop(inbox: impl InboxStore) {
    let policy = RetentionPolicy::new(Duration::from_secs(7 * 24 * 60 * 60));
    let mut ticker = tokio::time::interval(Duration::from_secs(3600));
    loop {
        ticker.tick().await;
        match inbox.purge(&policy).await {
            Ok(removed) => tracing::info!(removed, "inbox purged"),
            Err(error) => tracing::error!(%error, "inbox purge failed"),
        }
    }
}
```

If you run more than one instance of your service, do not spawn this
loop from inside it — run it as a separate, singleton job instead.

## Scope

This crate deliberately does not do the following.

- **No runtime backend switching.** A message handler receives
  `&mut Self::Conn` — the backend's concrete connection type — and
  writes its own SQL against it. That ties handler code to one backend
  at compile time; erasing it would require a full ORM abstraction over
  SQL dialects, which this crate does not attempt. `InboxStore` decouples
  the *crate* from any one backend (so `txbox` itself supports both), it
  does not decouple *your application* from the backend it was written
  against. Backend selection — `postgres` vs. `sqlite` — is a Cargo
  feature and a generic parameter, resolved at compile time.
- **No outbox pattern.** The crate is named `txbox` to leave room for a
  future `txbox::outbox` module, but 0.1.0 implements the inbox side
  only.
- **No broker integration.** `txbox` never talks to Kafka, SQS, or
  anything else. It receives a `MessageId` you extracted yourself and
  has no opinion about where it came from.

## Don't copy this into production

The bundled `examples/kafka_consumer.rs` is written for clarity, not for
production use. Specifically:

- **Offsets are committed manually, after the inbox transaction
  commits**, with `enable.auto.commit` set to `"false"`. This is the
  single most important operational detail in this README. Auto-commit
  stores the offset when the message is delivered and commits it later
  on a timer — decoupled from whether your handler ever ran. If the
  process crashes between delivery and your database commit, auto-commit
  can still advance the offset, and the message is never redelivered.
  That silently turns your at-least-once broker into an at-most-once
  one, and the inbox pattern cannot save you from a message you never
  see again. Commit the offset yourself, only after the inbox
  transaction has committed.
- **Errors are propagated out of `main` with `?`**, which kills the
  whole consumer on one bad record. Production code needs per-message
  isolation: skip, retry with backoff, or dead-letter, depending on the
  failure.
- `bootstrap.servers`, `group.id`, and the topic name are placeholders —
  replace them with your own.
- The purge loop in the example runs in-process for readability. As
  described above, run it as a scheduled job instead.

## Identifiers

`ConsumerId` and `MessageId` are validated when they are built, not when
the database is touched. Both reject an empty value and anything longer
than their byte limit — 255 for `ConsumerId`, 512 for `MessageId` —
returning [`InvalidId`].

Both rules guard a real failure, and both failures are silent or
permanent rather than merely inconvenient:

- **Empty.** Every empty identifier equals every other one. A producer
  emitting keyless messages would have all of them collapse onto a
  single inbox row, and every message after the first would be skipped
  as a duplicate of a message it has nothing to do with. Nothing logs an
  error; the data is simply missing.
- **Too long.** PostgreSQL rejects a btree index entry larger than about
  2704 bytes, and the inbox's primary key spans both identifiers. Past
  that, the `INSERT` in `claim` fails permanently. A caller following
  this crate's own advice — treat `InboxError::Backend` as transient and
  retry it — would retry forever and stall the partition on one
  malformed message. SQLite has no such limit, so the failure would not
  reproduce in local development.

Validating at construction makes the distinction impossible to get
wrong: `InvalidId` is a separate type from `InboxError`, so an
unprocessable identifier cannot be mistaken for a retryable backend
blip. A message whose id fails to build is a poison message — dead-letter
it and commit past it, as `examples/kafka_consumer.rs` shows. Retrying it
cannot help, because it will fail identically every time.

## Consumer ids

Two services consuming the same topic **must** use different
`ConsumerId` values. The dedup key is `(consumer_id, message_id)`; if two
services share a `ConsumerId`, whichever processes a message first will
cause the second to silently skip it as a duplicate, even though it
never ran its own handler.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
