# Guide

Working with identifiers, batches, multi-tenant schemas, and audit trails.

## Choosing a message id

The identifier must come from the producer and survive the producer's own
retry. A value the broker or transport assigns on delivery — an offset, a
receipt handle, its own per-attempt id — is not stable across a retry and
will make a genuine retry look like a brand-new message.

| Broker | What to use | The trap |
|---|---|---|
| Kafka | the producer's key | The offset changes on republish, and is per partition — you'd need `topic:partition:offset` |
| AMQP / RabbitMQ | `message_id`, scoped by `app_id` | `message_id` is only unique within one producer |
| SQS standard | an attribute set by the producer | SQS's own `MessageId` changes on every `SendMessage` |
| SQS FIFO | `MessageDeduplicationId` | FIFO queues only; its dedup window is 5 minutes |
| Pub/Sub | `messageId` | Stable for redeliveries of one publish, new if the producer retries |

For the AMQP case, `MessageId::scoped(app_id, message_id)` folds the
producer's identity into the key so two producers can't collide:

```rust
use txbox::MessageId;

# fn build() -> Result<(), txbox::InvalidId> {
let id = MessageId::scoped("orders-producer", "msg-123")?;
assert_eq!(id.as_str(), "orders-producer:msg-123");
# Ok(()) }
```

### Validation

`ConsumerId` and `MessageId` are validated at construction: empty, too long
(255 bytes for `ConsumerId`, 512 for `MessageId`), or surrounded by
whitespace all return `InvalidId`, a type distinct from `InboxError` so a
malformed identifier can't be mistaken for a retryable backend blip. Treat
it as a poison message — dead-letter it and commit past it.

## Batching

`process` opens a transaction per message. A consumer also exposes the
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

let claims = orders.claim_many(&mut tx, batch).await?;
for (id, claim) in batch.iter().zip(claims) {
    if claim == Claim::Fresh {
        sqlx::query("INSERT INTO orders (message_id) VALUES ($1)")
            .bind(id.as_str())
            .execute(&mut *tx)
            .await?;
    }
}

orders.commit(tx).await?;
Ok(()) }
```

The batch is one unit of failure: if anything in it fails, the whole
transaction rolls back and every message in it becomes unclaimed again, so
the broker redelivers the whole batch. The transaction is also held open for
the whole batch, so a concurrent consumer racing for any id in it blocks for
that entire span. Commit the broker's offsets only after `commit` returns.

If a claim returns `InboxError::Contended`, roll the transaction back and
retry the whole batch in a new one. Don't keep using it: on MySQL a
deadlock has already rolled it back, and later statements on it commit one
by one. To abandon a batch, call `orders.rollback(tx)`.

`claim_many` claims the whole batch in one statement (PostgreSQL `unnest`,
SQLite `json_each`; MySQL has no `RETURNING`, so it takes two, a
`JSON_TABLE` insert and a read-back) and answers per id, in input order. A
repeated id gets `Duplicate` after its first occurrence. Row locks are taken
in byte order, so overlapping batches from concurrent consumers don't
deadlock, and held until commit, so a bigger batch keeps competing consumers
waiting longer ([numbers](operations.md#batches)).

When the effect can be written in bulk too, keep it to two statements:

```rust,ignore
let mut tx = orders.begin().await?;
let claims = orders.claim_many(&mut tx, batch).await?;
let fresh: Vec<&str> = batch
    .iter()
    .zip(&claims)
    .filter(|(_, c)| **c == Claim::Fresh)
    .map(|(id, _)| id.as_str())
    .collect();
sqlx::query("INSERT INTO orders (message_id) SELECT unnest($1::text[])")
    .bind(&fresh)
    .execute(&mut *tx)
    .await?;
orders.commit(tx).await?;
```

That whole batch still fails as one unit. `process_many` runs a handler
per message inside a savepoint instead: a failing handler rolls back only
its own effects and claim, and you get one result per message to ack or
nack. It needs a backend implementing `Savepoints`.

## Multi-tenant schemas

The table name `inbox_messages` is hardcoded — there's no option to rename
it. For a per-tenant PostgreSQL schema, set `search_path` on the connection
instead; the unqualified table name resolves to whichever schema is first
on that path.

```rust
use sqlx::postgres::PgConnectOptions;

let options = PgConnectOptions::new().options([("search_path", "tenant_42")]);
# let _ = options;
```

Pass `options` to `PgPoolOptions::connect_with`, or, if the pool is built
from a URL, set it per-connection with `PgPoolOptions::after_connect`:

```rust
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;

# async fn build() -> Result<(), Box<dyn std::error::Error>> {
let pool = PgPoolOptions::new()
    .after_connect(|conn, _meta| {
        Box::pin(async move {
            conn.execute("SET search_path = 'tenant_42'").await?;
            Ok(())
        })
    })
    .connect("postgres://localhost/mydb")
    .await?;
# let _ = pool;
# Ok(()) }
```

## Recording what a handler decided

The inbox row only stores `(consumer_id, message_id, processed_at)` —
enough to answer "have we seen this?", nothing about what the handler did.
Let the handler record that itself, in its own table, inside the same
transaction `process` already gives it:

```rust
use txbox::postgres::PgInbox;
use txbox::{ConsumerId, InboxExt, MessageId};

async fn run(inbox: &PgInbox, id: &MessageId) -> Result<(), Box<dyn std::error::Error>> {
let orders = inbox.consumer(ConsumerId::try_from("orders")?);
let id_for_log = id.as_str().to_owned();

orders
    .process(id, move |conn| {
        Box::pin(async move {
            sqlx::query("INSERT INTO orders (payload) VALUES ($1)")
                .bind("...")
                .execute(&mut *conn)
                .await?;

            sqlx::query(
                "INSERT INTO order_processing_log (message_id, decision) \
                 VALUES ($1, $2)",
            )
            .bind(id_for_log)
            .bind("applied")
            .execute(&mut *conn)
            .await?;

            Ok::<_, txbox::HandlerError>(())
        })
    })
    .await?;
Ok(()) }
```

`txbox` doesn't store this for you — the shape of that decision (an enum, a
result payload, a version number) is yours to pick, and a generic library
shouldn't guess it.

## Writing a backend

Implement `InboxStore` and run the conformance suite from your tests with
the `testing` feature:

```rust,ignore
#[tokio::test]
async fn my_backend_conforms() {
    // A pool of exactly one connection, so a transaction leaked back to the
    // pool is caught.
    txbox::testing::conformance(MyInbox::new(one_connection_pool().await)).await;
}
```

It checks that duplicates are detected, that a failed handler and a
dropped transaction roll back, and that ids of maximum length that differ
only in their last byte, in case or in accents stay distinct. The last two
catch the usual schema mistakes: a narrow column, or a default collation
that ignores case (MySQL's `utf8mb4_0900_ai_ci`, SQL Server's
`SQL_Latin1_General_CP1_CI_AS`).

If your driver's transaction borrows its connection (tokio-postgres's
`Transaction<'_>`), own the pooled connection instead, issue `BEGIN` and
`COMMIT` yourself, and roll back in `Drop` by moving the connection into a
spawned task. Returning it to the pool with the transaction open lets the
next caller commit it.

## Writing a handler

Pass the handler inline or as a `fn`. A closure stored in a `let` first
loses the higher-ranked lifetime `process` needs, and the error ("one type
is more general than the other") doesn't say why.
