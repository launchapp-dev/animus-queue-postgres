//! Environment-driven configuration for `animus-queue-postgres`.

use anyhow::{anyhow, Result};

/// Primary Postgres connection URL env var (libpq / sqlx URL form, e.g.
/// `postgres://user:pass@host:5432/dbname`).
pub const ENV_DATABASE_URL: &str = "DATABASE_URL";

/// Fallback Postgres connection URL env var, used when `DATABASE_URL` is unset.
pub const ENV_POSTGRES_URL: &str = "ANIMUS_POSTGRES_URL";

/// Override the lease time-to-live, in seconds. A leased entry whose
/// `lease_expires_at` (= lease time + this TTL) has passed is reclaimable by a
/// later `queue/lease`. Default [`DEFAULT_LEASE_TTL_SECS`].
pub const ENV_LEASE_TTL_SECS: &str = "ANIMUS_QUEUE_LEASE_TTL_SECS";

/// Override the table name (default `queue_item`). Useful when more than one
/// logical queue shares a database.
pub const ENV_TABLE: &str = "ANIMUS_QUEUE_TABLE";

/// Default lease TTL: 30 minutes. Long enough that a healthy in-flight
/// workflow's lease never expires under it, short enough that a crashed
/// daemon's work is reclaimed promptly on the next lease.
pub const DEFAULT_LEASE_TTL_SECS: i64 = 1800;

/// Default durable queue table name.
pub const DEFAULT_TABLE: &str = "queue_item";

/// Runtime configuration for the Postgres queue backend.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Postgres connection URL.
    pub database_url: String,
    /// Lease TTL in seconds (drives `lease_expires_at` + reclaim window).
    pub lease_ttl_secs: i64,
    /// Durable queue table name.
    pub table: String,
}

impl QueueConfig {
    /// Load configuration from environment variables. Requires a Postgres URL
    /// in either `DATABASE_URL` or `ANIMUS_POSTGRES_URL`.
    pub fn from_env() -> Result<Self> {
        let database_url = std::env::var(ENV_DATABASE_URL)
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var(ENV_POSTGRES_URL)
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .ok_or_else(|| {
                anyhow!(
                    "no Postgres URL configured: set {ENV_DATABASE_URL} (or {ENV_POSTGRES_URL})"
                )
            })?;

        let lease_ttl_secs = std::env::var(ENV_LEASE_TTL_SECS)
            .ok()
            .and_then(|raw| raw.trim().parse::<i64>().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(DEFAULT_LEASE_TTL_SECS);

        let table = std::env::var(ENV_TABLE)
            .ok()
            .map(|raw| raw.trim().to_string())
            .filter(|s| is_safe_ident(s))
            .unwrap_or_else(|| DEFAULT_TABLE.to_string());

        Ok(Self {
            database_url,
            lease_ttl_secs,
            table,
        })
    }

    /// In-process builder for tests / embedders.
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            lease_ttl_secs: DEFAULT_LEASE_TTL_SECS,
            table: DEFAULT_TABLE.to_string(),
        }
    }
}

/// `true` when `s` is a safe SQL identifier (the table name is interpolated
/// into DDL/DML strings, so it must never carry injection-bearing characters).
/// ASCII alnum + underscore, non-empty, not starting with a digit.
pub fn is_safe_ident(s: &str) -> bool {
    !s.is_empty()
        && !s.chars().next().is_some_and(|c| c.is_ascii_digit())
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let cfg = QueueConfig::new("postgres://localhost/portal");
        assert_eq!(cfg.lease_ttl_secs, DEFAULT_LEASE_TTL_SECS);
        assert_eq!(cfg.table, DEFAULT_TABLE);
    }

    #[test]
    fn rejects_injection_in_table_name() {
        assert!(is_safe_ident("queue_item"));
        assert!(is_safe_ident("queue_item_2"));
        assert!(!is_safe_ident("queue_item; DROP TABLE x"));
        assert!(!is_safe_ident("9bad"));
        assert!(!is_safe_ident(""));
        assert!(!is_safe_ident("a-b"));
    }
}
