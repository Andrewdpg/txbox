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

A repeated id inside one batch is caught for free — a claim is visible to
later statements in its own transaction, so the second occurrence sees the
first and reports `Duplicate`.

The batch is one unit of failure: if anything in it fails, the whole
transaction rolls back and every message in it becomes unclaimed again, so
the broker redelivers the whole batch. The transaction is also held open for
the whole batch, so a concurrent consumer racing for any id in it blocks for
that entire span. Commit the broker's offsets only after `commit` returns.

To abandon a batch, drop the transaction — `InboxStore` has no explicit
rollback, so generic code relies on the drop.

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
