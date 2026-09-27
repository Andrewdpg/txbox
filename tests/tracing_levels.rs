#![cfg(feature = "sqlite")]

use std::sync::{Arc, Mutex};

use sqlx::sqlite::SqlitePoolOptions;
use tracing::Level;
use txbox::sqlite::SqliteInbox;
use txbox::{ConsumerId, InboxExt, MessageId};

/// Records the target and level of every event emitted while it is installed.
#[derive(Clone, Default)]
struct LevelSpy(Arc<Mutex<Vec<(String, Level)>>>);

impl tracing::subscriber::Subscriber for LevelSpy {
    fn enabled(&self, _m: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _a: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }
    fn record(&self, _s: &tracing::Id, _v: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _s: &tracing::Id, _f: &tracing::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        // Only record events emitted by this crate. sqlx logs its own events
        // (e.g. a slow-statement warning at WARN with a 1s default threshold)
        // on the same thread; without this filter, a loaded CI runner crossing
        // that threshold on an in-memory insert would fail this test by
        // blaming txbox for something sqlx did.
        if event.metadata().target().starts_with("txbox") {
            self.0.lock().unwrap().push((
                event.metadata().target().to_owned(),
                *event.metadata().level(),
            ));
        }
    }
    fn enter(&self, _s: &tracing::Id) {}
    fn exit(&self, _s: &tracing::Id) {}
}

#[tokio::test]
async fn duplicates_are_logged_at_debug_never_warn() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let inbox = SqliteInbox::new(pool);
    inbox.migrate().await.unwrap();

    let spy = LevelSpy::default();
    let consumer = ConsumerId::try_from("billing").unwrap();
    let id = MessageId::try_from("m-1").unwrap();

    inbox
        .consumer(consumer.clone())
        .process(&id, |_c| Box::pin(async { Ok(()) }))
        .await
        .unwrap();

    let recorded = spy.0.clone();
    {
        // `set_default` returns a drop guard and installs the subscriber on THIS
        // thread, so it can wrap an `.await`. Do not build a nested runtime here:
        // constructing a Tokio runtime inside `#[tokio::test]` panics at runtime.
        // This relies on `#[tokio::test]`'s default current-thread runtime: switching
        // to `flavor = "multi_thread"` would silently stop capturing events emitted
        // on other worker threads, since `set_default` is thread-local.
        let _guard = tracing::subscriber::set_default(spy);
        inbox
            .consumer(consumer.clone())
            .process(&id, |_c| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
    }

    let levels = recorded.lock().unwrap();
    assert!(!levels.is_empty(), "the duplicate path must emit an event");
    // tracing orders levels by verbosity: ERROR < WARN < INFO < DEBUG < TRACE.
    // `>= DEBUG` therefore admits only DEBUG and TRACE, and rejects WARN.
    assert!(
        levels.iter().all(|(_, l)| *l >= Level::DEBUG),
        "duplicates are normal under at-least-once delivery and must never be warnings"
    );
}

/// Duplicate volume is watched through the `txbox::duplicate` target
/// (`RUST_LOG=txbox::duplicate=debug`); `process_many` must feed it too.
#[tokio::test]
async fn process_many_logs_duplicates_under_the_duplicate_target() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let inbox = SqliteInbox::new(pool);
    inbox.migrate().await.unwrap();
    let consumer = inbox.consumer(ConsumerId::try_from("billing").unwrap());
    let ids = [MessageId::try_from("m-1").unwrap()];

    consumer
        .process_many(&ids, |_c, _id| Box::pin(async { Ok(()) }))
        .await
        .unwrap();

    let spy = LevelSpy::default();
    let recorded = spy.0.clone();
    {
        // Thread-local, like the test above: relies on the current-thread runtime.
        let _guard = tracing::subscriber::set_default(spy);
        consumer
            .process_many(&ids, |_c, _id| Box::pin(async { Ok(()) }))
            .await
            .unwrap();
    }

    let events = recorded.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|(target, level)| target == "txbox::duplicate" && *level == Level::DEBUG),
        "process_many must log duplicates under txbox::duplicate at DEBUG, got {events:?}"
    );
}
