//! Integration tests against a real Postgres.
//!
//! Connects to `TEST_DATABASE_URL` (or the conventional local docker test PG
//! `postgres://postgres:postgres@localhost:55432/animus_test`). When no
//! Postgres is reachable the tests SKIP (print + return) so a CI box without a
//! database still passes — the test bodies below are the durable behavioral
//! contract regardless of whether this particular runner can execute them.
//!
//! Each test uses a unique table name so they can run in parallel and never
//! clobber a shared queue.

use animus_queue_postgres::config::QueueConfig;
use animus_queue_postgres::store::Store;
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::Utc;

fn test_database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "postgres://postgres:postgres@localhost:55432/animus_test".to_string())
}

fn config_with_table(table: &str, lease_ttl_secs: i64) -> QueueConfig {
    QueueConfig {
        database_url: test_database_url(),
        lease_ttl_secs,
        table: table.to_string(),
    }
}

fn unique_table(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("qpgtest_{prefix}_{}", &suffix[..12])
}

/// Open a store, or `None` (with a skip note) when no Postgres is reachable.
async fn open_or_skip(config: &QueueConfig) -> Option<Store> {
    match Store::open(config).await {
        Ok(store) => Some(store),
        Err(error) => {
            eprintln!("SKIP: no reachable Postgres for integration test: {error}");
            None
        }
    }
}

async fn cleanup(table: &str) {
    if let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&test_database_url())
        .await
    {
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await;
        let _ = sqlx::query(&format!("DROP SEQUENCE IF EXISTS {table}_ordinal_seq"))
            .execute(&pool)
            .await;
    }
}

fn task(task_id: &str, workflow_ref: &str) -> SubjectDispatch {
    SubjectDispatch::for_subject_with_metadata(
        SubjectRef::task(task_id),
        workflow_ref,
        "integration-test",
        Utc::now(),
    )
}

#[tokio::test]
async fn enqueue_lease_and_list_round_trip() {
    let table = unique_table("basic");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    let a = store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();
    let b = store
        .enqueue(task("TASK-2", "standard"), None, None)
        .await
        .unwrap();
    assert!(a.enqueued && b.enqueued);
    assert_ne!(a.entry_id, b.entry_id);

    let listed = store.list(&[], None, None).await.unwrap();
    assert_eq!(listed.stats.pending, 2);
    assert_eq!(listed.entries[0].subject_id, "TASK-1", "FIFO order");

    let leased = store.lease(2, None, None).await.unwrap();
    assert_eq!(leased.leased.len(), 2);
    assert_eq!(leased.leased[0].status, "assigned");

    let after = store.stats().await.unwrap();
    assert_eq!(after.assigned, 2);
    assert_eq!(after.pending, 0);

    cleanup(&table).await;
}

#[tokio::test]
async fn lease_expiry_reclaims_crashed_daemon_work() {
    // The headline durability guarantee: a leased entry whose lease TTL has
    // elapsed (e.g. the daemon crashed/redeployed mid-workflow) is re-leasable
    // so the work is re-dispatched instead of lost.
    let table = unique_table("reclaim");
    let config = config_with_table(&table, 1); // 1-second lease TTL
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    let enq = store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();

    // First lease claims it.
    let first = store
        .lease(1, Some(vec!["wf-1".into()]), None)
        .await
        .unwrap();
    assert_eq!(first.leased.len(), 1);
    assert_eq!(first.leased[0].entry_id, enq.entry_id);

    // Immediately re-leasing finds nothing — the lease is still live.
    let still_held = store.lease(1, None, None).await.unwrap();
    assert!(
        still_held.leased.is_empty(),
        "a live lease must not be re-handed-out"
    );

    // Let the 1s lease TTL expire (simulating the holder dying).
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;

    // The expired lease is reclaimed and re-dispatched.
    let reclaimed = store
        .lease(1, Some(vec!["wf-2".into()]), None)
        .await
        .unwrap();
    assert_eq!(reclaimed.leased.len(), 1, "expired lease must be reclaimed");
    assert_eq!(
        reclaimed.leased[0].entry_id, enq.entry_id,
        "same entry re-dispatched"
    );
    assert_eq!(reclaimed.leased[0].workflow_id.as_deref(), Some("wf-2"));

    cleanup(&table).await;
}

#[tokio::test]
async fn hold_release_drop_and_release_pending() {
    let table = unique_table("mutate");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    let e = store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();

    // hold → held, idempotent
    assert!(store.hold(&e.entry_id).await.unwrap().changed);
    assert!(!store.hold(&e.entry_id).await.unwrap().changed);
    assert_eq!(store.stats().await.unwrap().held, 1);
    // held entry is not leasable
    assert!(store.lease(5, None, None).await.unwrap().leased.is_empty());

    // release → pending
    assert!(store.release(&e.entry_id).await.unwrap().changed);
    assert_eq!(store.stats().await.unwrap().pending, 1);

    // lease then release_pending → back to pending
    let leased = store
        .lease(1, Some(vec!["wf-x".into()]), None)
        .await
        .unwrap();
    assert_eq!(leased.leased.len(), 1);
    let rp = store
        .release_pending(&e.entry_id, "duplicate-in-flight")
        .await
        .unwrap();
    assert_eq!(rp.status, "pending");
    assert_eq!(store.stats().await.unwrap().pending, 1);

    // drop → gone from the live queue
    assert!(store.drop_entry(&e.entry_id).await.unwrap().changed);
    assert_eq!(store.stats().await.unwrap().total, 0);
    // dropping again → not_found
    let again = store.drop_entry(&e.entry_id).await.unwrap();
    assert!(!again.changed && again.not_found);

    cleanup(&table).await;
}

#[tokio::test]
async fn completion_prunes_only_leased_entries() {
    let table = unique_table("completion");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    let e = store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();

    // completion on a pending (not-leased) entry is a no-op.
    let noop = store
        .completion(&e.entry_id, "completed", None, None)
        .await
        .unwrap();
    assert!(!noop.changed && noop.not_found);
    assert_eq!(store.stats().await.unwrap().pending, 1);

    // lease then complete → pruned.
    store
        .lease(1, Some(vec!["wf-1".into()]), None)
        .await
        .unwrap();
    let done = store
        .completion(&e.entry_id, "completed", Some("standard"), Some("wf-1"))
        .await
        .unwrap();
    assert!(done.changed);
    assert_eq!(store.stats().await.unwrap().total, 0);

    cleanup(&table).await;
}

#[tokio::test]
async fn deferred_entry_not_leased_until_due_and_swept_when_expired() {
    let table = unique_table("deferred");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    // Future run_at → pending but not leasable; contributes to next_deadline.
    let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    store
        .enqueue(task("FUTURE", "standard"), Some(future.clone()), None)
        .await
        .unwrap();
    assert!(store.lease(5, None, None).await.unwrap().leased.is_empty());
    let stats = store.stats().await.unwrap();
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.deferred, 1);
    assert!(store.next_deadline().await.unwrap().next_run_at.is_some());

    // Past run_at + tiny grace → swept (dropped), never dispatched late.
    let past = (Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
    store
        .enqueue(task("EXPIRED", "standard"), Some(past), Some(60))
        .await
        .unwrap();
    let leased = store.lease(5, None, None).await.unwrap();
    assert!(leased.leased.is_empty());
    // FUTURE remains pending; EXPIRED swept.
    assert_eq!(store.stats().await.unwrap().pending, 1);

    cleanup(&table).await;
}

#[tokio::test]
async fn reorder_moves_named_entries_to_front() {
    let table = unique_table("reorder");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    let a = store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();
    let _b = store
        .enqueue(task("TASK-2", "standard"), None, None)
        .await
        .unwrap();
    let c = store
        .enqueue(task("TASK-3", "standard"), None, None)
        .await
        .unwrap();

    let res = store
        .reorder(&[c.entry_id.clone(), a.entry_id.clone()])
        .await
        .unwrap();
    assert!(res.reordered_count > 0);

    let listed = store.list(&[], None, None).await.unwrap();
    assert_eq!(listed.entries[0].subject_id, "TASK-3");
    assert_eq!(listed.entries[1].subject_id, "TASK-1");
    assert_eq!(listed.entries[2].subject_id, "TASK-2");

    cleanup(&table).await;
}

#[tokio::test]
async fn exclude_subjects_skips_in_flight_subjects() {
    let table = unique_table("exclude");
    let config = config_with_table(&table, 1800);
    let Some(store) = open_or_skip(&config).await else {
        return;
    };

    store
        .enqueue(task("TASK-1", "standard"), None, None)
        .await
        .unwrap();
    store
        .enqueue(task("TASK-2", "standard"), None, None)
        .await
        .unwrap();

    // Exclude TASK-1: only TASK-2 should be leased.
    let leased = store
        .lease(5, None, Some(vec!["TASK-1".to_string()]))
        .await
        .unwrap();
    assert_eq!(leased.leased.len(), 1);
    assert_eq!(leased.leased[0].subject_id, "TASK-2");

    cleanup(&table).await;
}
