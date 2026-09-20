# Operations

Sizing, purge scheduling, sharding, and running the bundled example.

## Sizing and contention

`process` holds its transaction — and therefore a pooled connection — for
the whole handler. With `P` connections and a handler taking `H`, no more
than `P/H` messages complete per second. Measured with a pool of 8 and a
25 ms handler (ceiling 320 msg/s):

| workers | throughput | p50 latency |
|---|---|---|
| 4  | 148 msg/s (46%) | 27 ms |
| 8  | 291 msg/s (91%) | 27 ms |
| 16 | 289 msg/s (90%) | 55 ms |
| 32 | 291 msg/s (91%) | 110 ms |
| 64 | 292 msg/s (91%) | 218 ms |

Throughput stops at the pool; latency keeps climbing as workers queue behind
it. Size the worker count to the pool, not the machine — raise
`max_connections` or shorten the handler if you need more throughput.

### Contention

A consumer that loses a claim race waits for the winner's transaction, and
holds its own pooled connection the whole time. That wait does not compound
across contenders:

| contenders | winner | slowest loser |
|---|---|---|
| 2  | 508 ms | 509 ms |
| 4  | 506 ms | 506 ms |
| 8  | 503 ms | 503 ms |
| 16 | 503 ms | 504 ms |

Losers wake together when the winner commits and find an already-committed
row. The caveat is still the pool: every blocked loser occupies a
connection for the wait, so a rebalance storm with a slow handler can
exhaust the pool even though contention itself doesn't add latency.

`Consumer::with_lock_timeout` converts the wait into a fast error instead:

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

Unset by default (preserves blocking); no-op on SQLite.

### What retention costs

Filling the inbox to 300 000 rows:

| rows | table | index | claim p50 |
|---|---|---|---|
| 50 000  |  5.1 MB |  2.1 MB | 456 µs |
| 150 000 | 15.5 MB |  6.8 MB | 456 µs |
| 300 000 | 31.0 MB | 13.6 MB | 448 µs |

Claim latency doesn't move with table size (btree depth grows
logarithmically). Purging removed 301 200 rows in 781 ms.

`DELETE` doesn't reclaim space on its own — after the purge above the table
still occupied its full 31 MB until `VACUUM` ran. A live inbox rarely needs
that: retention deletes the oldest rows while new ones are appended, so
freed space is reused rather than returned. Size the disk for what
retention holds; don't expect a purge to shrink anything.

## Purge scheduling

`purge` must run on exactly one instance — with N replicas each running
their own loop, all N compete to delete the same rows. Prefer a Kubernetes
`CronJob`, a leader-elected process, or any other mechanism that guarantees
one runner:

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

If you run more than one instance of your service, don't spawn this loop
from inside it — run it as a separate, singleton job.

## Observability

`process` and `claim` are wrapped in an `inbox.claim` span. With
`tracing-opentelemetry`, that span exports as a claim-latency histogram with
no extra metrics hook needed.

Duplicate skips log under their own target, `txbox::duplicate`, so
`RUST_LOG=txbox::duplicate=debug` watches duplicate volume without turning
on debug logging for the rest of the crate.

## The inbox must live in the same database as the effect

The inbox row and the business effect are only one transaction if they're
one connection to one database. If your business data is sharded across N
databases, you need N `Consumer<S>` instances, each pointed at its own
store, plus a router that sends each message to the right one.

Putting the inbox in a single central database "to keep it in one place"
silently destroys the guarantee: the inbox `INSERT` and the effect's
`INSERT` now target different databases, so they can't share a transaction.
Nothing throws — the duplicate just gets processed twice the first time
load is actually split across shards.

`txbox` doesn't provide the router: sharding topology and routing are
decisions specific to your system.

## Don't copy this into production

`examples/kafka_consumer.rs` is written for clarity, not production use:

- Offsets are committed manually, after the inbox transaction commits, with
  `enable.auto.commit` set to `"false"` — see the README warning on offset
  ordering.
- Errors are propagated out of `main` with `?`, which kills the whole
  consumer on one bad record. Production code needs per-message isolation:
  skip, retry with backoff, or dead-letter.
- `bootstrap.servers`, `group.id`, and the topic name are placeholders.
- The purge loop runs in-process for readability; run it as a scheduled job
  instead (see "Purge scheduling").
