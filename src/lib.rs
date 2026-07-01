//! `animus-queue-postgres`: a durable, Postgres-backed `queue` plugin for
//! Animus.
//!
//! This is a drop-in replacement for `launchapp-dev/animus-queue-default`. It
//! speaks the EXACT same `queue/*` JSON-RPC contract (from
//! `animus-queue-protocol` v0.5.10, the tag queue-default v0.3.3 pins) so the
//! daemon needs no change to use it. The only difference is the storage layer:
//! the reference plugin keeps queue state in a file-locked JSON blob under the
//! project root (ephemeral on a container redeploy), whereas this plugin keeps
//! it in a shared Postgres table (`queue_item`) so the dispatch queue SURVIVES
//! daemon restarts and container redeploys.
//!
//! ## Durability + lease-expiry reclaim
//!
//! The headline feature is **lease-expiry reclaim**. Each `queue/lease`
//! stamps `lease_owner` + `lease_expires_at` on the claimed rows. A `leased`
//! row whose `lease_expires_at` has passed is treated as re-leasable: the next
//! `queue/lease` reclaims it via an atomic `SELECT ... FOR UPDATE SKIP LOCKED`.
//! So if the daemon crashes or the container redeploys mid-workflow, the
//! unfinished work is re-dispatched instead of being lost (the file backend
//! would have lost the whole queue).
//!
//! ## State model
//!
//! The wire `status` vocabulary stays `pending` / `assigned` / `held`
//! (`animus_queue_protocol::status`). The durable `queue_item.state` column
//! adds two terminal soft-delete states so completed/dropped work leaves an
//! audit trail instead of being physically removed:
//!
//! | DB `state` | wire `status` | meaning                                   |
//! |------------|---------------|-------------------------------------------|
//! | `pending`  | `pending`     | waiting to be leased                      |
//! | `leased`   | `assigned`    | leased; a workflow is running against it  |
//! | `held`     | `held`        | held by operator action; non-dispatchable |
//! | `done`     | (excluded)    | workflow reached a terminal state         |
//! | `dropped`  | (excluded)    | dropped by operator / expired deferral    |
//!
//! `done` / `dropped` rows are excluded from `queue/list`, `queue/stats`, and
//! `queue/lease` — functionally identical to the reference plugin (which
//! deletes them), but durable and auditable.

#![warn(missing_docs)]
// `RpcResponse` (from animus-plugin-protocol) is a large enum; several helpers
// return it in a `Result`'s `Err` arm to keep the JSON-RPC dispatch ergonomic.
// The reference `animus-queue-default` makes the same trade-off.
#![allow(clippy::result_large_err)]

pub mod config;
pub mod plugin;
pub mod store;

pub use config::QueueConfig;
pub use store::Store;
