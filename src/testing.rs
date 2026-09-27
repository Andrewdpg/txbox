//! A conformance suite for [`InboxStore`] backends.
//!
//! Call [`conformance`] from a backend's own tests. Every check scopes its
//! rows under a consumer id unique to the run, so the database needn't be
//! empty and the suite can run repeatedly. Give it a store whose pool has
//! exactly one connection: a transaction leaked back to the pool only shows
//! when the next caller gets the same connection.

use std::hash::BuildHasher;
use std::time::SystemTime;

use crate::{
    Claim, ClaimRequest, ConsumerId, HandlerError, InboxError, InboxExt, InboxStore, MessageId,
    Outcome, Savepoints,
};

/// Runs every check against `store`, panicking on the first violated
/// contract with a message that names it.
pub async fn conformance<S: InboxStore + Clone + 'static>(store: S) {
    let run = Run::new();
    fresh_then_duplicate(&store, &run).await;
    handler_failure_rolls_back(&store, &run).await;
    explicit_rollback_discards(&store, &run).await;
    dropped_transaction_does_not_persist(&store, &run).await;
    max_length_ids_are_accepted(&store, &run).await;
    ids_compare_byte_for_byte(&store, &run).await;
    claim_many_matches_single_claims(&store, &run).await;
}

/// Scopes one run's rows so repeated runs never see each other.
struct Run {
    nonce: String,
}

impl Run {
    fn new() -> Self {
        // Not cryptographic, just distinct per run: RandomState is seeded
        // from the OS and differs on every construction.
        let n = std::collections::hash_map::RandomState::new().hash_one(SystemTime::now());
        Self {
            nonce: format!("{n:016x}"),
        }
    }

    fn prefix(&self) -> String {
        format!("conformance-{}-", self.nonce)
    }

    fn consumer(&self, check: &str) -> ConsumerId {
        ConsumerId::try_from(format!("{}{check}", self.prefix())).expect("valid consumer id")
    }
}

fn message(id: &str) -> MessageId {
    MessageId::try_from(id).expect("valid message id")
}

async fn claim_and_commit<S: InboxStore>(
    store: &S,
    consumer: &ConsumerId,
    id: &MessageId,
) -> Claim {
    let mut tx = store.begin().await.expect("begin");
    let claim = store
        .claim(&mut tx, ClaimRequest::new(consumer, id))
        .await
        .expect("claim");
    store.commit(tx).await.expect("commit");
    claim
}

/// Whether `(consumer, id)` is committed. Probes with a claim inside a
/// transaction that is always rolled back, so probing changes nothing.
async fn is_committed<S: InboxStore>(store: &S, consumer: &ConsumerId, id: &MessageId) -> bool {
    let mut tx = store.begin().await.expect("begin");
    let claim = store
        .claim(&mut tx, ClaimRequest::new(consumer, id))
        .await
        .expect("claim");
    store.rollback(tx).await.expect("rollback");
    claim == Claim::Duplicate
}

async fn fresh_then_duplicate<S: InboxStore + Clone>(store: &S, run: &Run) {
    let consumer = store.consumer(run.consumer("fresh"));
    let id = message("m");
    let first = consumer
        .process(&id, |_| Box::pin(async { Ok(()) }))
        .await
        .expect("first process");
    let second = consumer
        .process(&id, |_| Box::pin(async { Ok(()) }))
        .await
        .expect("second process");
    assert_eq!(
        first,
        Outcome::Processed(()),
        "a new message must be processed"
    );
    assert_eq!(
        second,
        Outcome::Duplicate,
        "a redelivered message must be a duplicate"
    );
}

async fn handler_failure_rolls_back<S: InboxStore + Clone + 'static>(store: &S, run: &Run) {
    let inbox = run.consumer("handler-failure");
    let effects = run.consumer("handler-failure-effect");
    let id = message("m");
    let (effect_store, effect_consumer, effect_id) = (store.clone(), effects.clone(), id.clone());

    let result = store
        .consumer(inbox.clone())
        .process::<_, ()>(&id, move |conn| {
            Box::pin(async move {
                // The effect is a claim under another consumer: a write on
                // `conn` the suite can observe without knowing any SQL.
                effect_store
                    .claim(conn, ClaimRequest::new(&effect_consumer, &effect_id))
                    .await?;
                Err::<(), HandlerError>("deliberate handler failure".into())
            })
        })
        .await;

    assert!(
        matches!(result, Err(InboxError::Handler(_))),
        "a failing handler must surface as InboxError::Handler, got {result:?}"
    );
    assert!(
        !is_committed(store, &inbox, &id).await,
        "a failed handler's claim must roll back"
    );
    assert!(
        !is_committed(store, &effects, &id).await,
        "a failed handler's effect must roll back with its claim"
    );
}

async fn explicit_rollback_discards<S: InboxStore + Clone>(store: &S, run: &Run) {
    let consumer_id = run.consumer("rollback");
    let consumer = store.consumer(consumer_id.clone());
    let id = message("m");
    let mut tx = consumer.begin().await.expect("begin");
    consumer.claim(&mut tx, &id).await.expect("claim");
    consumer.rollback(tx).await.expect("rollback");
    assert!(
        !is_committed(store, &consumer_id, &id).await,
        "InboxStore::rollback must discard the claim"
    );
}

async fn dropped_transaction_does_not_persist<S: InboxStore + Clone>(store: &S, run: &Run) {
    let consumer_id = run.consumer("dropped");
    let consumer = store.consumer(consumer_id.clone());
    let dropped = message("dropped");
    let later = message("later");
    {
        let mut tx = consumer.begin().await.expect("begin");
        consumer.claim(&mut tx, &dropped).await.expect("claim");
    }
    let mut tx = consumer.begin().await.expect("begin");
    consumer.claim(&mut tx, &later).await.expect("claim");
    consumer.commit(tx).await.expect("commit");

    assert!(
        is_committed(store, &consumer_id, &later).await,
        "a committed claim must persist"
    );
    assert!(
        !is_committed(store, &consumer_id, &dropped).await,
        "a transaction dropped without commit must roll back; its claim was \
         committed later, likely by the next transaction on the same pooled connection"
    );
}

async fn max_length_ids_are_accepted<S: InboxStore>(store: &S, run: &Run) {
    let prefix = run.prefix();
    let room = ConsumerId::MAX_LEN - prefix.len();
    let ascii_consumer =
        ConsumerId::try_from(format!("{prefix}{}", "x".repeat(room))).expect("max consumer id");
    // 4-byte characters as far as they fit, ASCII for the remainder.
    let wide_consumer = ConsumerId::try_from(format!(
        "{prefix}{}{}",
        "\u{1F600}".repeat(room / 4),
        "y".repeat(room % 4)
    ))
    .expect("max consumer id");
    let ascii_id = message(&"m".repeat(MessageId::MAX_LEN));
    let wide_id = message(&"\u{1F600}".repeat(MessageId::MAX_LEN / 4));

    for (consumer, id) in [(&ascii_consumer, &ascii_id), (&wide_consumer, &wide_id)] {
        assert_eq!(
            claim_and_commit(store, consumer, id).await,
            Claim::Fresh,
            "a max-length id ({} bytes) must be accepted",
            id.as_str().len()
        );
        assert_eq!(
            claim_and_commit(store, consumer, id).await,
            Claim::Duplicate,
            "a max-length id must deduplicate"
        );
    }
}

async fn ids_compare_byte_for_byte<S: InboxStore>(store: &S, run: &Run) {
    let consumer = run.consumer("bytes");
    let long = "m".repeat(MessageId::MAX_LEN - 1);
    let ids = [
        format!("{long}a"),
        format!("{long}b"),
        "Order-1".to_owned(),
        "order-1".to_owned(),
        "caf\u{e9}".to_owned(),
        "cafe".to_owned(),
    ];
    for id in &ids {
        assert_eq!(
            claim_and_commit(store, &consumer, &message(id)).await,
            Claim::Fresh,
            "`{}` collided with an earlier, different id: ids must compare byte for \
             byte (a truncating column or a case- or accent-insensitive collation \
             merges distinct messages)",
            if id.len() > 40 {
                &id[id.len() - 40..]
            } else {
                id
            }
        );
    }
}

async fn claim_many_matches_single_claims<S: InboxStore + Clone>(store: &S, run: &Run) {
    let consumer_id = run.consumer("claim-many");
    let consumer = store.consumer(consumer_id.clone());
    let known = message("b");
    assert_eq!(
        claim_and_commit(store, &consumer_id, &known).await,
        Claim::Fresh
    );

    let batch = [message("c"), message("b"), message("a"), message("c")];
    let mut tx = consumer.begin().await.expect("begin");
    let claims = consumer
        .claim_many(&mut tx, &batch)
        .await
        .expect("claim_many");
    consumer.commit(tx).await.expect("commit");

    assert_eq!(
        claims,
        [
            Claim::Fresh,
            Claim::Duplicate,
            Claim::Fresh,
            Claim::Duplicate
        ],
        "claim_many must answer like one claim per id, in input order, with repeats as duplicates"
    );
    for id in ["a", "c"] {
        assert!(
            is_committed(store, &consumer_id, &message(id)).await,
            "claim_many's fresh claims must commit"
        );
    }
}

/// Checks [`process_many`](crate::Consumer::process_many) against `store`:
/// with poisons at the first, a middle and the last position, every other
/// message and its effect commit, and each poison and its effect roll back
/// and stay unclaimed. Panics on the first violation.
pub async fn savepoints_conformance<S: Savepoints + Clone + 'static>(store: S) {
    let run = Run::new();
    let inbox = run.consumer("process-many");
    let effects = run.consumer("process-many-effect");
    // A repeat of a poison must fail too, and a repeat of a success is a
    // duplicate.
    let ids: Vec<MessageId> = (0..10)
        .map(|i| format!("m{i}"))
        .chain(["m5".to_owned(), "m3".to_owned()])
        .map(|id| message(&id))
        .collect();
    let poisons = ["m0", "m5", "m9"];
    // Processed before the batch: its handler must not run again.
    let known = "m7";
    assert_eq!(
        claim_and_commit(&store, &inbox, &message(known)).await,
        Claim::Fresh
    );

    let (effect_store, effect_consumer) = (store.clone(), effects.clone());
    let results = store
        .consumer(inbox.clone())
        .process_many(&ids, move |conn, id| {
            let (effect_store, effect_consumer) = (effect_store.clone(), effect_consumer.clone());
            let poisoned = poisons.contains(&id.as_str());
            Box::pin(async move {
                effect_store
                    .claim(conn, ClaimRequest::new(&effect_consumer, id))
                    .await?;
                if poisoned {
                    Err::<(), HandlerError>("poison".into())
                } else {
                    Ok(())
                }
            })
        })
        .await
        .expect("process_many");

    for (id, result) in ids.iter().zip(&results) {
        if id.as_str() == known {
            assert!(
                matches!(result, Ok(Outcome::Duplicate)),
                "{id} was processed before the batch and must be a duplicate, got {result:?}"
            );
            assert!(
                !is_committed(&store, &effects, id).await,
                "{id}: the handler must not run for an already-processed message"
            );
            continue;
        }
        let poisoned = poisons.contains(&id.as_str());
        assert_eq!(
            result.is_err(),
            poisoned,
            "result for {id} must be an error iff it is a poison"
        );
        assert_eq!(
            is_committed(&store, &inbox, id).await,
            !poisoned,
            "{id}: claim must commit iff its handler succeeded"
        );
        assert_eq!(
            is_committed(&store, &effects, id).await,
            !poisoned,
            "{id}: effect must commit iff its handler succeeded"
        );
    }
}
