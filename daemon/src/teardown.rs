//! Orphaned-instance dead-letter (lnrent-urw.2 / GATE-1 PR-6).
//!
//! When reconcile's retention/cancel `fire_destroy` runs the recipe `destroy` hook and it FAILS, the
//! subscription still terminates and its capacity hold is still released (unchanged) — but the
//! provider-side resource (e.g. a DigitalOcean droplet) may not have been torn down, so it keeps
//! billing the operator, invisibly. This module is the durable record of that owed cleanup: a
//! `teardown_failure` row per `(subscription_id, hook)`, retried with capped backoff until the
//! idempotent hook finally succeeds (§7.2). It surfaces via `lnrent teardowns` and fires the
//! `TeardownFailed` operator alert (lnrent-urw.1). No money is read or moved here — this is an infra
//! record, not a ledger entry.

use anyhow::Result;
use rusqlite::{params, OptionalExtension, Transaction};

use crate::store::Store;

/// Cap on the stored `last_error` (mirrors op_dispatch's `MAX_ERROR_MESSAGE_CHARS`) so a hostile
/// hook's stderr can't bloat the row.
pub const MAX_TEARDOWN_ERROR_CHARS: usize = 256;

/// Base retry interval; the actual wait is `2^min(attempts,6) * BASE` after `last_attempt_at`
/// (1min → 2 → 4 → … → capped at 64min). No max-attempts park: a monthly-billing orphan is exactly
/// the thing to keep retrying, and the alert cooldown bounds the noise.
pub const TEARDOWN_RETRY_BASE_S: i64 = 60;

/// The stable dead-letter row id for `(subscription_id, hook)`, so a repeat failure UPSERTs the same
/// row (attempts bump) rather than piling up rows.
pub fn row_id(subscription_id: &str, hook: &str) -> String {
    format!("td:{subscription_id}:{hook}")
}

/// The backoff after `attempts` failures: `2^min(attempts,6) * TEARDOWN_RETRY_BASE_S` seconds.
pub fn backoff_s(attempts: i64) -> i64 {
    let shift = attempts.clamp(0, 6) as u32;
    (1i64 << shift) * TEARDOWN_RETRY_BASE_S
}

/// An open dead-letter row (a provider-side cleanup still owed).
#[derive(Debug, Clone)]
pub struct TeardownRow {
    pub id: String,
    pub subscription_id: String,
    pub hook: String,
    pub handles_json: Option<String>,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub first_failed_at: i64,
    pub last_attempt_at: i64,
}

impl TeardownRow {
    /// A human/agent-facing JSON view (for `lnrent teardowns`), with derived ages/backoff at `now`.
    pub fn to_value(&self, now: i64) -> serde_json::Value {
        serde_json::json!({
            "subscription_id": self.subscription_id,
            "hook": self.hook,
            "attempts": self.attempts,
            "last_error": self.last_error,
            "first_failed_at": self.first_failed_at,
            "last_attempt_at": self.last_attempt_at,
            "owed_for_s": now - self.first_failed_at,
            "next_retry_in_s": (self.last_attempt_at + backoff_s(self.attempts) - now).max(0),
        })
    }
}

fn cap_error(error: &str) -> String {
    if error.chars().count() <= MAX_TEARDOWN_ERROR_CHARS {
        return error.to_string();
    }
    let mut out: String = error.chars().take(MAX_TEARDOWN_ERROR_CHARS).collect();
    out.push('…');
    out
}

/// Insert-or-update the OPEN dead-letter row for `(subscription_id, hook)` in `tx`: on first failure
/// create it (attempts=1); on a repeat bump attempts + refresh `last_error`/`last_attempt_at` +
/// re-open (`resolved_at=NULL`) so a resource that broke again after a prior resolve is tracked
/// afresh. `handles_json` is refreshed so the retry always re-runs the hook with the latest handles.
/// Returns the new attempts count.
pub fn record_failure_txn(
    tx: &Transaction,
    subscription_id: &str,
    hook: &str,
    handles_json: Option<&str>,
    error: &str,
    now: i64,
) -> rusqlite::Result<i64> {
    let id = row_id(subscription_id, hook);
    let capped = cap_error(error);
    tx.execute(
        "INSERT INTO teardown_failure
            (id, subscription_id, hook, handles_json, attempts, last_error,
             first_failed_at, last_attempt_at, resolved_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?6, NULL)
         ON CONFLICT(id) DO UPDATE SET
            attempts        = attempts + 1,
            handles_json    = excluded.handles_json,
            last_error      = excluded.last_error,
            last_attempt_at = excluded.last_attempt_at,
            resolved_at     = NULL",
        params![id, subscription_id, hook, handles_json, capped, now],
    )?;
    let attempts: i64 = tx.query_row(
        "SELECT attempts FROM teardown_failure WHERE id=?1",
        params![id],
        |r| r.get(0),
    )?;
    Ok(attempts)
}

/// Insert-or-update the dead-letter row for a failed teardown; returns the new attempts count.
pub async fn record_failure(
    store: &Store,
    subscription_id: &str,
    hook: &str,
    handles_json: Option<String>,
    error: &str,
    now: i64,
) -> Result<i64> {
    let (sub, hook, error) = (subscription_id.to_string(), hook.to_string(), error.to_string());
    store
        .transaction(move |tx| {
            Ok(record_failure_txn(
                tx,
                &sub,
                &hook,
                handles_json.as_deref(),
                &error,
                now,
            )?)
        })
        .await
}

/// Mark the `(subscription_id, hook)` dead-letter row resolved (the retry hook finally succeeded).
/// A no-op if there is no open row. Guarded on `resolved_at IS NULL` so a concurrent resolve is safe.
pub async fn mark_resolved(store: &Store, subscription_id: &str, hook: &str, now: i64) -> Result<()> {
    let id = row_id(subscription_id, hook);
    store
        .transaction(move |tx| {
            tx.execute(
                "UPDATE teardown_failure SET resolved_at=?2
                 WHERE id=?1 AND resolved_at IS NULL",
                params![id, now],
            )?;
            Ok(())
        })
        .await
}

/// Every OPEN dead-letter row, newest-failure-first, for the `lnrent teardowns` view.
pub async fn open_rows(store: &Store) -> Result<Vec<TeardownRow>> {
    store
        .read(|c| {
            let mut stmt = c.prepare(
                "SELECT id, subscription_id, hook, handles_json, attempts, last_error,
                        first_failed_at, last_attempt_at
                 FROM teardown_failure WHERE resolved_at IS NULL
                 ORDER BY last_attempt_at DESC",
            )?;
            let rows = stmt
                .query_map([], row_from)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
}

/// OPEN rows whose backoff has elapsed (`last_attempt_at + backoff(attempts) <= now`) — the retry set.
pub async fn open_due_rows(store: &Store, now: i64) -> Result<Vec<TeardownRow>> {
    Ok(open_rows(store)
        .await?
        .into_iter()
        .filter(|r| r.last_attempt_at + backoff_s(r.attempts) <= now)
        .collect())
}

/// Count of OPEN dead-letter rows (folded into `open_teardowns` alongside the provision backlog).
pub async fn open_count(store: &Store) -> Result<i64> {
    store
        .read(|c| {
            Ok(c.query_row(
                "SELECT count(*) FROM teardown_failure WHERE resolved_at IS NULL",
                [],
                |r| r.get(0),
            )?)
        })
        .await
}

fn row_from(r: &rusqlite::Row) -> rusqlite::Result<TeardownRow> {
    Ok(TeardownRow {
        id: r.get(0)?,
        subscription_id: r.get(1)?,
        hook: r.get(2)?,
        handles_json: r.get(3)?,
        attempts: r.get(4)?,
        last_error: r.get(5)?,
        first_failed_at: r.get(6)?,
        last_attempt_at: r.get(7)?,
    })
}

/// The single open row for `(subscription_id, hook)`, if any (test/introspection helper).
pub async fn open_row(store: &Store, subscription_id: &str, hook: &str) -> Result<Option<TeardownRow>> {
    let id = row_id(subscription_id, hook);
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT id, subscription_id, hook, handles_json, attempts, last_error,
                        first_failed_at, last_attempt_at
                 FROM teardown_failure WHERE id=?1 AND resolved_at IS NULL",
                params![id],
                row_from,
            )
            .optional()?)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    #[test]
    fn backoff_is_capped_at_64min() {
        assert_eq!(backoff_s(0), 60);
        assert_eq!(backoff_s(1), 120);
        assert_eq!(backoff_s(6), 64 * 60);
        assert_eq!(backoff_s(50), 64 * 60, "shift is clamped at 6");
    }

    #[tokio::test]
    async fn record_dedups_on_sub_hook_and_bumps_attempts() {
        let store = mem_store();
        let a1 = record_failure(&store, "sub-1", "destroy", Some("{\"id\":1}".into()), "boom", 100)
            .await
            .unwrap();
        let a2 = record_failure(&store, "sub-1", "destroy", Some("{\"id\":1}".into()), "boom again", 200)
            .await
            .unwrap();
        assert_eq!((a1, a2), (1, 2), "same (sub,hook) upserts and bumps attempts");
        assert_eq!(open_count(&store).await.unwrap(), 1, "one open row, not two");

        let row = open_row(&store, "sub-1", "destroy").await.unwrap().unwrap();
        assert_eq!(row.attempts, 2);
        assert_eq!(row.last_error.as_deref(), Some("boom again"));
        assert_eq!(row.first_failed_at, 100, "first_failed_at is preserved");
        assert_eq!(row.last_attempt_at, 200);
    }

    #[tokio::test]
    async fn resolve_drops_row_from_the_open_view() {
        let store = mem_store();
        record_failure(&store, "sub-1", "destroy", None, "boom", 100).await.unwrap();
        assert_eq!(open_count(&store).await.unwrap(), 1);
        mark_resolved(&store, "sub-1", "destroy", 300).await.unwrap();
        assert_eq!(open_count(&store).await.unwrap(), 0, "resolved row drops out");
        assert!(open_row(&store, "sub-1", "destroy").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn open_due_respects_backoff() {
        let store = mem_store();
        // attempts=1 → backoff 120s from last_attempt_at=100 → due at 220.
        record_failure(&store, "sub-1", "destroy", None, "boom", 100).await.unwrap();
        assert!(open_due_rows(&store, 219).await.unwrap().is_empty(), "not yet due");
        assert_eq!(open_due_rows(&store, 220).await.unwrap().len(), 1, "due at last+backoff");
    }

    #[tokio::test]
    async fn last_error_is_capped() {
        let store = mem_store();
        let huge = "x".repeat(10_000);
        record_failure(&store, "sub-1", "destroy", None, &huge, 100).await.unwrap();
        let row = open_row(&store, "sub-1", "destroy").await.unwrap().unwrap();
        assert!(
            row.last_error.unwrap().chars().count() <= MAX_TEARDOWN_ERROR_CHARS + 1,
            "last_error is capped"
        );
    }
}
