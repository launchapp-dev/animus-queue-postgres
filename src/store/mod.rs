//! Postgres-backed durable queue store.
//!
//! All queue state lives in one table (`queue_item`, configurable). Every
//! mutation is keyed by `entry_id` (the row PK, a UUID v4 minted on enqueue),
//! mirroring `animus-queue-default`'s `entry_id`-keyed contract.
//!
//! Concurrency model:
//!
//! - `queue/lease` runs inside a single transaction and claims rows with
//!   `SELECT ... FOR UPDATE SKIP LOCKED`, so two daemons (or a daemon and a
//!   manual CLI lease) never hand out the same entry twice.
//! - **Lease-expiry reclaim**: the lease-eligible set is `state = 'pending'`
//!   (and due) OR `state = 'leased' AND lease_expires_at < now()`. A crashed or
//!   redeployed daemon's stale lease is therefore re-dispatched on the next
//!   `queue/lease` instead of being lost.
//! - Single-row mutations (`hold` / `release` / `drop` / `mark_assigned` /
//!   `completion` / `release_pending`) lock the target row `FOR UPDATE` inside
//!   a transaction for read-modify-write safety.

use animus_queue_protocol::{
    completion_status, status, QueueEntry, QueueLeaseResponse, QueueListResponse,
    QueueMutationResponse, QueueNextDeadlineResponse, QueueReleasePendingResponse,
    QueueReorderResponse, QueueStats,
};
use animus_subject_protocol::SubjectDispatch;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use crate::config::QueueConfig;

/// Durable `state` column values. The first three map 1:1 onto the wire
/// `status` vocabulary; `done` / `dropped` are terminal soft-delete states
/// excluded from list / stats / lease.
mod db_state {
    pub const PENDING: &str = "pending";
    pub const LEASED: &str = "leased";
    pub const HELD: &str = "held";
    pub const DONE: &str = "done";
    pub const DROPPED: &str = "dropped";

    /// The three non-terminal states a live entry can be in.
    pub const LIVE: [&str; 3] = [PENDING, LEASED, HELD];
}

/// Outcome of a single `enqueue`.
#[derive(Debug, Clone)]
pub struct EnqueueOutcome {
    /// Always `true` — every enqueue creates a row (collisions surface as a
    /// `warning`, matching queue-default's non-idempotent v0.3 behavior).
    pub enqueued: bool,
    /// Stable entry id (UUID v4) of the new row.
    pub entry_id: String,
    /// Subject key from the dispatch envelope.
    pub subject_id: String,
    /// Non-fatal advisory when a live entry already targets this subject.
    pub warning: Option<String>,
}

/// Typed errors specific to `queue/release_pending`.
#[derive(Debug, thiserror::Error)]
pub enum QueueReleasePendingError {
    /// Entry id was not found (or is in a terminal state).
    #[error("entry_id not found: {entry_id}")]
    NotFound {
        /// Entry id from the request.
        entry_id: String,
    },
    /// Entry exists but is not in the Assigned (leased) state.
    #[error("entry {entry_id} is in state '{actual_state}', expected 'assigned'")]
    NotAssigned {
        /// Entry id from the request.
        entry_id: String,
        /// Actual wire status (`pending` / `held`).
        actual_state: String,
    },
    /// Wrapped backend error (I/O, persistence).
    #[error(transparent)]
    Backend(anyhow::Error),
}

/// Postgres-backed queue store.
#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    table: String,
    lease_ttl_secs: i64,
}

impl Store {
    /// Connect to Postgres and apply the idempotent schema.
    pub async fn open(config: &QueueConfig) -> Result<Self> {
        // LAZY connect + background migrate so a cold DB dial on (re)spawn never
        // blocks the daemon initialize handshake (which surfaced as "plugin
        // connection lost"). First query connects; migration is idempotent.
        let pool = PgPoolOptions::new()
            // Small pool: this plugin shares one Railway Postgres with Better
            // Auth + the config/subject/chat backends. Keep the shared
            // connection budget well under Postgres `max_connections`.
            .max_connections(4)
            .connect_lazy(&config.database_url)
            .context("failed to create Postgres pool")?;
        let store = Self {
            pool,
            table: config.table.clone(),
            lease_ttl_secs: config.lease_ttl_secs,
        };
        let migrate_store = Self {
            pool: store.pool.clone(),
            table: store.table.clone(),
            lease_ttl_secs: store.lease_ttl_secs,
        };
        tokio::spawn(async move {
            if let Err(error) = migrate_store.migrate().await {
                eprintln!("[animus-queue-postgres] background migrate failed (schema likely present; retried next spawn): {error:#}");
            }
        });
        Ok(store)
    }

    /// In-process constructor for tests / embedders that already hold a pool.
    pub fn from_pool(pool: PgPool, config: &QueueConfig) -> Self {
        Self {
            pool,
            table: config.table.clone(),
            lease_ttl_secs: config.lease_ttl_secs,
        }
    }

    fn seq(&self) -> String {
        format!("{}_ordinal_seq", self.table)
    }

    /// Idempotent schema migration. Safe to run on every boot.
    pub async fn migrate(&self) -> Result<()> {
        let t = &self.table;
        let seq = self.seq();

        sqlx::query(&format!("CREATE SEQUENCE IF NOT EXISTS {seq}"))
            .execute(&self.pool)
            .await
            .context("failed to create ordinal sequence")?;

        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {t} ( \
                 id text PRIMARY KEY, \
                 subject_kind text NOT NULL, \
                 subject_id text NOT NULL, \
                 workflow_ref text NOT NULL, \
                 state text NOT NULL, \
                 lease_owner text, \
                 lease_expires_at timestamptz, \
                 workflow_id text, \
                 priority int NOT NULL DEFAULT 0, \
                 ordinal bigint NOT NULL, \
                 enqueued_at timestamptz NOT NULL, \
                 assigned_at timestamptz, \
                 held_at timestamptz, \
                 run_at timestamptz, \
                 expire_after_secs bigint, \
                 audit_log jsonb NOT NULL DEFAULT '[]'::jsonb, \
                 updated_at timestamptz NOT NULL, \
                 payload jsonb NOT NULL \
             )"
        ))
        .execute(&self.pool)
        .await
        .context("failed to create queue_item table")?;

        // Dispatch hot path: lease/list scan live rows in ordinal order.
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {t}_state_ordinal_idx ON {t}(state, ordinal)"
        ))
        .execute(&self.pool)
        .await
        .context("failed to create state/ordinal index")?;
        // Subject collision + exclude_subjects lookups.
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {t}_subject_idx ON {t}(subject_id)"
        ))
        .execute(&self.pool)
        .await
        .context("failed to create subject index")?;

        Ok(())
    }

    /// Health probe: a trivial `SELECT 1`.
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("Postgres ping failed")?;
        Ok(())
    }

    // ============================================================
    // queue/enqueue
    // ============================================================

    /// Append a dispatch to the queue. Never idempotent: a subject collision
    /// is surfaced via [`EnqueueOutcome::warning`], not rejected.
    pub async fn enqueue(
        &self,
        dispatch: SubjectDispatch,
        run_at: Option<String>,
        expire_after_secs: Option<u64>,
    ) -> Result<EnqueueOutcome> {
        // Drop expired deferred entries before counting duplicates so the
        // advisory reflects the live queue.
        self.sweep_expired().await?;

        let subject_key = dispatch.subject_key();
        let subject_kind = dispatch.subject_kind().to_string();
        let workflow_ref = dispatch.workflow_ref.clone();
        let priority = priority_to_int(dispatch.priority.as_deref());

        let dup_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {} WHERE subject_id = $1 AND state = ANY($2)",
            self.table
        ))
        .bind(&subject_key)
        .bind(&db_state::LIVE[..])
        .fetch_one(&self.pool)
        .await
        .context("failed to count duplicate queue entries")?;

        let warning = (dup_count > 0).then(|| {
            format!(
                "subject {subject_key} already has {dup_count} queued entr{}; duplicate enqueued",
                if dup_count == 1 { "y" } else { "ies" }
            )
        });

        let entry_id = uuid::Uuid::new_v4().to_string();
        let payload = serde_json::to_value(&dispatch).context("failed to encode dispatch")?;
        let run_at_ts = run_at.as_deref().and_then(parse_rfc3339);
        let expire_after: Option<i64> = expire_after_secs.map(|s| s as i64);

        sqlx::query(&format!(
            "INSERT INTO {t} ( \
                 id, subject_kind, subject_id, workflow_ref, state, priority, ordinal, \
                 enqueued_at, run_at, expire_after_secs, updated_at, payload \
             ) VALUES ( \
                 $1, $2, $3, $4, '{pending}', $5, nextval('{seq}'), \
                 now(), $6, $7, now(), $8 \
             )",
            t = self.table,
            pending = db_state::PENDING,
            seq = self.seq(),
        ))
        .bind(&entry_id)
        .bind(&subject_kind)
        .bind(&subject_key)
        .bind(&workflow_ref)
        .bind(priority)
        .bind(run_at_ts)
        .bind(expire_after)
        .bind(&payload)
        .execute(&self.pool)
        .await
        .context("failed to insert queue entry")?;

        Ok(EnqueueOutcome {
            enqueued: true,
            entry_id,
            subject_id: subject_key,
            warning,
        })
    }

    // ============================================================
    // queue/list + queue/stats
    // ============================================================

    /// Paginated, status-filtered view of the live queue plus aggregate stats.
    pub async fn list(
        &self,
        status_filter: &[String],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<QueueListResponse> {
        let rows = self.fetch_live_entries().await?;
        let stats = stats_from_entries(&rows);

        let mut filtered: Vec<QueueEntry> = rows
            .into_iter()
            .filter(|entry| status_filter.is_empty() || status_filter.contains(&entry.status))
            .collect();

        let total = filtered.len();
        let offset = offset.unwrap_or(0);
        if offset >= filtered.len() {
            filtered.clear();
        } else {
            filtered.drain(0..offset);
        }
        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(QueueListResponse {
            entries: filtered,
            total,
            stats,
        })
    }

    /// Aggregate counts over live rows.
    pub async fn stats(&self) -> Result<QueueStats> {
        let rows = self.fetch_live_entries().await?;
        Ok(stats_from_entries(&rows))
    }

    /// Fetch all live (`pending`/`leased`/`held`) entries in dispatch order
    /// (ordinal ASC), mapped to wire [`QueueEntry`]s. Rows whose payload can no
    /// longer decode into a `SubjectDispatch` are logged and skipped.
    async fn fetch_live_entries(&self) -> Result<Vec<QueueEntry>> {
        let rows = sqlx::query(&format!(
            "SELECT id, subject_id, state, workflow_id, enqueued_at, assigned_at, held_at, \
                    run_at, expire_after_secs, payload \
             FROM {} WHERE state = ANY($1) ORDER BY ordinal ASC",
            self.table
        ))
        .bind(&db_state::LIVE[..])
        .fetch_all(&self.pool)
        .await
        .context("failed to read queue entries")?;

        Ok(rows.iter().filter_map(row_to_entry).collect())
    }

    // ============================================================
    // queue/lease (atomic dispatch + lease-expiry reclaim)
    // ============================================================

    /// Atomic dispatch. Claims up to `max` lease-eligible entries — pending
    /// (and due) entries OR leased entries whose lease has expired (reclaim) —
    /// attaches workflow ids, transitions each to `leased`, and returns them.
    pub async fn lease(
        &self,
        max: usize,
        workflow_ids: Option<Vec<String>>,
        exclude_subjects: Option<Vec<String>>,
    ) -> std::result::Result<QueueLeaseResponse, QueueLeaseError> {
        if let Some(ids) = workflow_ids.as_ref() {
            if ids.len() != max {
                return Err(QueueLeaseError::WorkflowIdCountMismatch {
                    expected: max,
                    actual: ids.len(),
                });
            }
        }
        if max == 0 {
            return Ok(QueueLeaseResponse { leased: Vec::new() });
        }

        self.sweep_expired()
            .await
            .map_err(QueueLeaseError::Backend)?;

        let mut tx = self.pool.begin().await.map_err(|e| {
            QueueLeaseError::Backend(anyhow::Error::from(e).context("failed to begin lease tx"))
        })?;

        // Lock every lease-eligible row in dispatch order. SKIP LOCKED lets a
        // concurrent lease proceed on rows we don't hold. The reclaim arm
        // (`state = 'leased' AND lease_expires_at < now()`) is what makes a
        // crashed daemon's unfinished work re-dispatchable.
        let candidate_rows = sqlx::query(&format!(
            "SELECT id, subject_id, payload FROM {t} \
             WHERE ( \
                 (state = '{pending}' AND (run_at IS NULL OR run_at <= now())) \
                 OR (state = '{leased}' AND lease_expires_at IS NOT NULL AND lease_expires_at < now()) \
             ) \
             ORDER BY ordinal ASC \
             FOR UPDATE SKIP LOCKED",
            t = self.table,
            pending = db_state::PENDING,
            leased = db_state::LEASED,
        ))
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| {
            QueueLeaseError::Backend(anyhow::Error::from(e).context("failed to select lease candidates"))
        })?;

        let mut exclude_set: std::collections::HashSet<String> =
            exclude_subjects.into_iter().flatten().collect();

        let mut chosen: Vec<(String, String)> = Vec::new(); // (entry_id, subject_key)
        for row in &candidate_rows {
            if chosen.len() == max {
                break;
            }
            let id: String = row.get("id");
            let subject_id: String = row.get("subject_id");
            // Corrupt payload — can't return it over the wire (subject_dispatch
            // is required). Skip instead of poisoning the lease.
            let payload: Value = row.get("payload");
            if serde_json::from_value::<SubjectDispatch>(payload).is_err() {
                tracing::warn!(entry_id = %id, "queue/lease: skipping entry with undecodable dispatch payload");
                continue;
            }
            // Don't lease two entries for the same subject in one batch, and
            // honor caller-supplied in-flight subjects.
            if exclude_set.contains(&subject_id) {
                continue;
            }
            exclude_set.insert(subject_id.clone());
            chosen.push((id, subject_id));
        }

        let mut leased: Vec<QueueEntry> = Vec::with_capacity(chosen.len());
        for (idx, (entry_id, _subject_id)) in chosen.iter().enumerate() {
            let workflow_id = match workflow_ids.as_ref() {
                Some(ids) => ids[idx].clone(),
                None => uuid::Uuid::new_v4().to_string(),
            };
            let row = sqlx::query(&format!(
                "UPDATE {t} SET state = '{leased}', workflow_id = $1, lease_owner = $1, \
                     lease_expires_at = now() + make_interval(secs => $2::int), \
                     assigned_at = now(), updated_at = now() \
                 WHERE id = $3 \
                 RETURNING id, subject_id, state, workflow_id, enqueued_at, assigned_at, \
                           held_at, run_at, expire_after_secs, payload",
                t = self.table,
                leased = db_state::LEASED,
            ))
            .bind(&workflow_id)
            .bind(self.lease_ttl_secs)
            .bind(entry_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                QueueLeaseError::Backend(
                    anyhow::Error::from(e).context("failed to mark entry leased"),
                )
            })?;
            if let Some(entry) = row_to_entry(&row) {
                leased.push(entry);
            }
        }

        tx.commit().await.map_err(|e| {
            QueueLeaseError::Backend(anyhow::Error::from(e).context("failed to commit lease tx"))
        })?;

        Ok(QueueLeaseResponse { leased })
    }

    // ============================================================
    // queue/next_deadline
    // ============================================================

    /// Earliest future `run_at` across pending deferred entries.
    pub async fn next_deadline(&self) -> Result<QueueNextDeadlineResponse> {
        self.sweep_expired().await?;
        let next: Option<DateTime<Utc>> = sqlx::query_scalar(&format!(
            "SELECT min(run_at) FROM {} WHERE state = '{}' AND run_at IS NOT NULL AND run_at > now()",
            self.table,
            db_state::PENDING,
        ))
        .fetch_one(&self.pool)
        .await
        .context("failed to compute next deadline")?;
        Ok(QueueNextDeadlineResponse {
            next_run_at: next.map(|ts| ts.to_rfc3339()),
        })
    }

    // ============================================================
    // queue/hold + queue/release + queue/drop
    // ============================================================

    /// Hold a Pending entry. Idempotent on already-held.
    pub async fn hold(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |state| match state {
            db_state::HELD => Ok(MutationPlan::NoChange),
            db_state::PENDING => Ok(MutationPlan::Sql(format!(
                "UPDATE {} SET state = '{}', held_at = now(), updated_at = now() WHERE id = $1",
                self.table,
                db_state::HELD
            ))),
            _ => Err(MutationError::NotPending),
        })
        .await
    }

    /// Release a Held entry back to Pending. Idempotent on already-pending.
    pub async fn release(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |state| match state {
            db_state::PENDING => Ok(MutationPlan::NoChange),
            db_state::HELD => Ok(MutationPlan::Sql(format!(
                "UPDATE {} SET state = '{}', held_at = NULL, updated_at = now() WHERE id = $1",
                self.table,
                db_state::PENDING
            ))),
            _ => Err(MutationError::NotPending),
        })
        .await
    }

    /// Drop a live entry from the queue (soft-delete → `dropped`).
    pub async fn drop_entry(&self, entry_id: &str) -> Result<QueueMutationResponse> {
        self.mutate_entry(entry_id, |_state| {
            Ok(MutationPlan::Sql(format!(
                "UPDATE {} SET state = '{}', updated_at = now() WHERE id = $1",
                self.table,
                db_state::DROPPED
            )))
        })
        .await
    }

    /// Transition a single Pending entry to Assigned (leased).
    pub async fn mark_assigned(
        &self,
        entry_id: &str,
        workflow_id: Option<String>,
    ) -> Result<QueueMutationResponse> {
        let wid = workflow_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin mark_assigned tx")?;
        let row = sqlx::query(&format!(
            "SELECT state FROM {} WHERE id = $1 AND state = ANY($2) FOR UPDATE",
            self.table
        ))
        .bind(entry_id)
        .bind(&db_state::LIVE[..])
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load entry for mark_assigned")?;

        let Some(row) = row else {
            tx.rollback().await.ok();
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let state: String = row.get("state");
        match state.as_str() {
            // Already leased: idempotent no-op.
            db_state::LEASED => {
                tx.rollback().await.ok();
                Ok(QueueMutationResponse {
                    changed: false,
                    not_found: false,
                })
            }
            db_state::PENDING => {
                sqlx::query(&format!(
                    "UPDATE {} SET state = '{}', workflow_id = $1, lease_owner = $1, \
                         lease_expires_at = now() + make_interval(secs => $2::int), \
                         assigned_at = now(), updated_at = now() WHERE id = $3",
                    self.table,
                    db_state::LEASED,
                ))
                .bind(&wid)
                .bind(self.lease_ttl_secs)
                .bind(entry_id)
                .execute(&mut *tx)
                .await
                .context("failed to mark entry assigned")?;
                tx.commit()
                    .await
                    .context("failed to commit mark_assigned tx")?;
                Ok(QueueMutationResponse {
                    changed: true,
                    not_found: false,
                })
            }
            // held (or anything else live) cannot jump straight to assigned.
            _ => {
                tx.rollback().await.ok();
                anyhow::bail!("queue entry {entry_id} is not in the expected pre-mutation status")
            }
        }
    }

    /// Mark a workflow's terminal status; prune (→ `done`) the matching leased
    /// entry. Stale/misrouted completions for non-leased entries are no-ops.
    pub async fn completion(
        &self,
        entry_id: &str,
        status: &str,
        workflow_ref: Option<&str>,
        workflow_id: Option<&str>,
    ) -> Result<QueueMutationResponse> {
        if !matches!(
            status,
            completion_status::COMPLETED | completion_status::FAILED | completion_status::CANCELLED
        ) {
            anyhow::bail!(
                "invalid completion status: '{status}' (expected one of: completed, failed, cancelled)"
            );
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin completion tx")?;
        let row = sqlx::query(&format!(
            "SELECT state, workflow_id, payload FROM {} WHERE id = $1 FOR UPDATE",
            self.table
        ))
        .bind(entry_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load entry for completion")?;

        let Some(row) = row else {
            tx.rollback().await.ok();
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };

        let state: String = row.get("state");
        // Only leased (assigned) entries are prunable. A completion frame for a
        // pending/held/terminal entry must NOT delete un-leased work.
        if state != db_state::LEASED {
            tx.rollback().await.ok();
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        }
        // Optional workflow_ref / workflow_id match guards.
        if let Some(want_ref) = workflow_ref {
            let payload: Value = row.get("payload");
            let dispatch_ref = payload.get("workflow_ref").and_then(Value::as_str);
            if dispatch_ref.is_some_and(|r| r != want_ref) {
                tx.rollback().await.ok();
                return Ok(QueueMutationResponse {
                    changed: false,
                    not_found: true,
                });
            }
        }
        if let Some(want_wid) = workflow_id {
            let existing: Option<String> = row.get("workflow_id");
            if existing.as_deref().is_some_and(|w| w != want_wid) {
                tx.rollback().await.ok();
                return Ok(QueueMutationResponse {
                    changed: false,
                    not_found: true,
                });
            }
        }

        sqlx::query(&format!(
            "UPDATE {} SET state = '{}', updated_at = now() WHERE id = $1",
            self.table,
            db_state::DONE
        ))
        .bind(entry_id)
        .execute(&mut *tx)
        .await
        .context("failed to mark entry done")?;
        tx.commit()
            .await
            .context("failed to commit completion tx")?;

        Ok(QueueMutationResponse {
            changed: true,
            not_found: false,
        })
    }

    // ============================================================
    // queue/reorder
    // ============================================================

    /// Reorder entries by id: named ids move to the front in the requested
    /// order; the rest keep their relative order. Returns the count of entries
    /// whose absolute position changed.
    pub async fn reorder(&self, entry_ids: &[String]) -> Result<QueueReorderResponse> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin reorder tx")?;

        // Current live order. Lock the rows so a concurrent enqueue/lease can't
        // interleave new ordinals while we rewrite them.
        let rows = sqlx::query(&format!(
            "SELECT id FROM {} WHERE state = ANY($1) ORDER BY ordinal ASC FOR UPDATE",
            self.table
        ))
        .bind(&db_state::LIVE[..])
        .fetch_all(&mut *tx)
        .await
        .context("failed to read entries for reorder")?;

        let original: Vec<String> = rows.iter().map(|r| r.get::<String, _>("id")).collect();
        let live: std::collections::HashSet<&String> = original.iter().collect();

        let mut new_order: Vec<String> = Vec::with_capacity(original.len());
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Named ids first (dedup, existing-live only).
        for id in entry_ids {
            if live.contains(id) && seen.insert(id.clone()) {
                new_order.push(id.clone());
            }
        }
        // Then the rest in original order.
        for id in &original {
            if seen.insert(id.clone()) {
                new_order.push(id.clone());
            }
        }

        let reordered_count = new_order
            .iter()
            .zip(original.iter())
            .filter(|(after, before)| after != before)
            .count();

        if reordered_count == 0 {
            tx.rollback().await.ok();
            return Ok(QueueReorderResponse { reordered_count: 0 });
        }

        // Assign fresh monotonically-increasing ordinals in the new order so a
        // later `ORDER BY ordinal ASC` reproduces it.
        for id in &new_order {
            sqlx::query(&format!(
                "UPDATE {t} SET ordinal = nextval('{seq}'), updated_at = now() WHERE id = $1",
                t = self.table,
                seq = self.seq(),
            ))
            .bind(id)
            .execute(&mut *tx)
            .await
            .context("failed to rewrite ordinal during reorder")?;
        }
        tx.commit().await.context("failed to commit reorder tx")?;

        Ok(QueueReorderResponse { reordered_count })
    }

    // ============================================================
    // queue/release_pending
    // ============================================================

    /// Atomically return a leased entry to Pending, clearing the lease fields
    /// and appending an audit-log entry describing why.
    pub async fn release_pending(
        &self,
        entry_id: &str,
        reason: &str,
    ) -> std::result::Result<QueueReleasePendingResponse, QueueReleasePendingError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| QueueReleasePendingError::Backend(anyhow::Error::from(e)))?;

        let row = sqlx::query(&format!(
            "SELECT state FROM {} WHERE id = $1 FOR UPDATE",
            self.table
        ))
        .bind(entry_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| QueueReleasePendingError::Backend(anyhow::Error::from(e)))?;

        let Some(row) = row else {
            return Err(QueueReleasePendingError::NotFound {
                entry_id: entry_id.to_string(),
            });
        };
        let state: String = row.get("state");
        // Terminal rows are "gone" for the purposes of this contract.
        if state == db_state::DONE || state == db_state::DROPPED {
            return Err(QueueReleasePendingError::NotFound {
                entry_id: entry_id.to_string(),
            });
        }
        if state != db_state::LEASED {
            return Err(QueueReleasePendingError::NotAssigned {
                entry_id: entry_id.to_string(),
                actual_state: db_state_to_wire(&state).to_string(),
            });
        }

        let audit = json!({
            "at": Utc::now().to_rfc3339(),
            "method": "queue/release_pending",
            "from_status": status::ASSIGNED,
            "to_status": status::PENDING,
            "reason": reason,
        });

        sqlx::query(&format!(
            "UPDATE {t} SET state = '{pending}', assigned_at = NULL, workflow_id = NULL, \
                 lease_owner = NULL, lease_expires_at = NULL, \
                 audit_log = audit_log || $1::jsonb, updated_at = now() \
             WHERE id = $2",
            t = self.table,
            pending = db_state::PENDING,
        ))
        .bind(&audit)
        .bind(entry_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| QueueReleasePendingError::Backend(anyhow::Error::from(e)))?;

        tx.commit()
            .await
            .map_err(|e| QueueReleasePendingError::Backend(anyhow::Error::from(e)))?;

        Ok(QueueReleasePendingResponse {
            entry_id: entry_id.to_string(),
            status: status::PENDING.to_string(),
        })
    }

    // ============================================================
    // internal helpers
    // ============================================================

    /// Soft-delete pending deferred entries past their `run_at +
    /// expire_after_secs` window (→ `dropped`) instead of dispatching late.
    async fn sweep_expired(&self) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {t} SET state = '{dropped}', updated_at = now() \
             WHERE state = '{pending}' AND run_at IS NOT NULL AND expire_after_secs IS NOT NULL \
               AND now() > run_at + ((expire_after_secs::text) || ' seconds')::interval",
            t = self.table,
            dropped = db_state::DROPPED,
            pending = db_state::PENDING,
        ))
        .execute(&self.pool)
        .await
        .context("failed to sweep expired deferred entries")?;
        Ok(())
    }

    /// Read-modify-write a single live entry under a `FOR UPDATE` row lock,
    /// where the mutation is a single SQL statement bound to `id = $1`.
    async fn mutate_entry<F>(&self, entry_id: &str, plan: F) -> Result<QueueMutationResponse>
    where
        F: FnOnce(&str) -> std::result::Result<MutationPlan, MutationError>,
    {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin mutate tx")?;
        let row = sqlx::query(&format!(
            "SELECT state FROM {} WHERE id = $1 AND state = ANY($2) FOR UPDATE",
            self.table
        ))
        .bind(entry_id)
        .bind(&db_state::LIVE[..])
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load entry for mutation")?;

        let Some(row) = row else {
            tx.rollback().await.ok();
            return Ok(QueueMutationResponse {
                changed: false,
                not_found: true,
            });
        };
        let state: String = row.get("state");

        match plan(&state) {
            Ok(MutationPlan::NoChange) => {
                tx.rollback().await.ok();
                Ok(QueueMutationResponse {
                    changed: false,
                    not_found: false,
                })
            }
            Ok(MutationPlan::Sql(sql)) => {
                sqlx::query(&sql)
                    .bind(entry_id)
                    .execute(&mut *tx)
                    .await
                    .context("failed to apply entry mutation")?;
                tx.commit().await.context("failed to commit mutate tx")?;
                Ok(QueueMutationResponse {
                    changed: true,
                    not_found: false,
                })
            }
            Err(MutationError::NotPending) => {
                tx.rollback().await.ok();
                anyhow::bail!("queue entry {entry_id} is not in the expected pre-mutation status")
            }
        }
    }

    /// Test helper: total row count (any state).
    #[doc(hidden)]
    pub async fn total_rows(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar(&format!("SELECT count(*) FROM {}", self.table))
                .fetch_one(&self.pool)
                .await?,
        )
    }
}

/// Plan returned by a mutation precondition closure.
enum MutationPlan {
    /// Idempotent no-op (entry already in target state).
    NoChange,
    /// A single SQL statement bound to `id = $1`.
    Sql(String),
}

/// Precondition failure surfaced as `QUEUE_ENTRY_NOT_PENDING`.
enum MutationError {
    NotPending,
}

/// Typed errors specific to `queue/lease`.
#[derive(Debug, thiserror::Error)]
pub enum QueueLeaseError {
    /// `workflow_ids.len()` did not match `max`.
    #[error("workflow_ids length {actual} did not match max {expected}")]
    WorkflowIdCountMismatch {
        /// `max` from the request.
        expected: usize,
        /// `workflow_ids.len()` from the request.
        actual: usize,
    },
    /// Wrapped backend error.
    #[error(transparent)]
    Backend(anyhow::Error),
}

/// Map a durable `state` value to the wire `status` vocabulary.
fn db_state_to_wire(state: &str) -> &'static str {
    match state {
        db_state::PENDING => status::PENDING,
        db_state::LEASED => status::ASSIGNED,
        db_state::HELD => status::HELD,
        // Terminal states never surface over the wire.
        _ => "unknown",
    }
}

/// Map a `SubjectDispatch` priority hint to an int (stored for observability;
/// dispatch order stays strict FIFO to match the reference plugin).
fn priority_to_int(priority: Option<&str>) -> i32 {
    match priority {
        Some("critical") => 40,
        Some("high") => 30,
        Some("medium") => 20,
        Some("low") => 10,
        _ => 0,
    }
}

/// Parse an RFC 3339 string into a UTC instant; `None` on malformed input
/// (treated as immediate, matching the reference plugin's lenient handling).
fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Build a wire [`QueueEntry`] from a queue row. Returns `None` (logging) when
/// the payload cannot decode into a `SubjectDispatch`.
fn row_to_entry(row: &sqlx::postgres::PgRow) -> Option<QueueEntry> {
    let entry_id: String = row.get("id");
    let payload: Value = row.get("payload");
    let dispatch: SubjectDispatch = match serde_json::from_value(payload) {
        Ok(d) => d,
        Err(error) => {
            tracing::warn!(entry_id = %entry_id, %error, "skipping queue row with undecodable dispatch payload");
            return None;
        }
    };
    let state: String = row.get("state");
    let subject_id: String = row.get("subject_id");
    let workflow_id: Option<String> = row.get("workflow_id");
    let enqueued_at: DateTime<Utc> = row.get("enqueued_at");
    let assigned_at: Option<DateTime<Utc>> = row.get("assigned_at");
    let held_at: Option<DateTime<Utc>> = row.get("held_at");
    let run_at: Option<DateTime<Utc>> = row.get("run_at");
    let expire_after_secs: Option<i64> = row.get("expire_after_secs");

    Some(QueueEntry {
        entry_id,
        subject_id,
        task_id: dispatch.task_id().map(ToOwned::to_owned),
        subject_dispatch: dispatch,
        status: db_state_to_wire(&state).to_string(),
        workflow_id,
        enqueued_at: enqueued_at.to_rfc3339(),
        assigned_at: assigned_at.map(|t| t.to_rfc3339()),
        held_at: held_at.map(|t| t.to_rfc3339()),
        run_at: run_at.map(|t| t.to_rfc3339()),
        expire_after_secs: expire_after_secs.map(|s| s as u64),
    })
}

/// Compute aggregate stats from already-mapped live entries.
fn stats_from_entries(entries: &[QueueEntry]) -> QueueStats {
    let now = Utc::now();
    let mut stats = QueueStats {
        total: entries.len(),
        pending: 0,
        assigned: 0,
        held: 0,
        deferred: 0,
    };
    for entry in entries {
        match entry.status.as_str() {
            s if s == status::PENDING => {
                stats.pending += 1;
                if let Some(run_at) = entry.run_at.as_deref().and_then(parse_rfc3339) {
                    if run_at > now {
                        stats.deferred += 1;
                    }
                }
            }
            s if s == status::ASSIGNED => stats.assigned += 1,
            s if s == status::HELD => stats.held += 1,
            _ => {}
        }
    }
    stats
}
