# Design

What this crate is deliberately out of scope for, and why it looks the way it does.

## Scope

This crate deliberately does not do the following.

- **No runtime backend switching.** A handler receives `&mut Self::Conn`,
  the backend's concrete connection type, and writes its own SQL against
  it. `InboxStore` decouples the crate from any one backend; it doesn't
  decouple your application from the backend it was written against.
  Backend selection is a Cargo feature and a generic parameter, resolved
  at compile time.
- **No outbox pattern.** The crate is named `txbox` to leave room for a
  future `txbox::outbox` module, but 0.1.0 is inbox-only.
- **No broker integration.** `txbox` never talks to Kafka, SQS, or
  anything else — it receives a `MessageId` you extracted yourself.
- **Not needed when the effect already carries its own idempotency
  check.** If the effect is an append to an event store with optimistic
  concurrency, the append itself already rejects a redelivery, and adding
  `txbox` on top would be a second dedup mechanism guarding a write that
  already refuses to be duplicated.

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
        .process(&id, move |conn| {
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
| The handler fails partway through | Same in both: the broker's own delivery semantics decide whether it retries | Same in both, but if it does retry, the aborted attempt left no partial row (transaction rolled back), so the retry is treated as fresh |
| Same message delivered concurrently to two instances of the same consumer | Both instances see no prior record and both apply the effect — a race | One claims the row and proceeds; the other loses the race and skips |
| Two different services consume the same topic | Neither has any dedup signal at all | Only correct if each service uses its own `ConsumerId` |
