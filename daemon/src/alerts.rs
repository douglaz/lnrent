//! GATE-1 alert dispatcher (lnrent-urw.1 / production-readiness.md PR-5).
//!
//! A **thin** edge-triggered sink — deliberately NOT a monitoring framework. It turns a condition
//! the money/provisioning path already detects into a durable NIP-17 DM to the operator's chosen
//! recipient, riding the SAME outbox as every other DM (so it inherits the existing
//! drain/retry/FAILED machinery — zero new infra: no HTTP server, no metrics lib, no email/webhook
//! sinks). Each alert is additive to the existing `tracing` log line at its call site.
//!
//! Design points that are load-bearing:
//! - [`AlertKind`] is a CLOSED enum. New kinds are added only by the owning bead (PR-6
//!   `TeardownFailed`, PR-9c `RelayBlackout`, PR-16 `HoldingsLow`, gate1-operator-sweep
//!   `SweepFailed`). There is deliberately no `BalanceQueryFailed`: the ledger-authoritative
//!   revision (ADR-0016) retires the automatic balance read, so nothing is left to fail.
//! - **Edge-triggered** with a per-`(kind, subject)` cooldown ([`ALERT_COOLDOWN_S`]), held in an
//!   in-memory map. A restart resets it — worst case one duplicate alert per condition per restart,
//!   which is why the map is deliberately NOT persisted.
//! - The dispatcher NEVER reads the federation balance; it fires only on ledger/log conditions the
//!   daemon already surfaces. Honest caveat: a `RelayBlackout` alert is precisely the one that
//!   cannot be delivered while the relay pool is down — it queues in the outbox like any DM, and
//!   §C's out-of-band relay status query is how the operator reads that condition instead.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Result;
use rusqlite::params;

use lnrent_wire::{Msg, OperatorAlert};

use crate::clock::Clock;
use crate::store::Store;
use std::sync::Arc;

/// How long the same `(kind, subject)` is suppressed after firing once (6h). Edge-triggered: a
/// level condition that re-detects every drive (e.g. a refund stuck PENDING) alerts at most once
/// per this window, then re-fires after it.
pub const ALERT_COOLDOWN_S: i64 = 6 * 3600;

/// The CLOSED set of alertable conditions (production-readiness.md PR-5 §A). Extended ONLY by the
/// owning beads listed in the module doc — do not add free-form kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertKind {
    /// A refund exhausted its retry budget (or a definitive backend failure) and was parked FAILED.
    RefundParked,
    /// A refund has sat PENDING past the stuck threshold without progressing.
    RefundStuck,
    /// A `destroy` hook failed and the orphaned instance was dead-lettered (wired by PR-6).
    TeardownFailed,
    /// The relay pool has zero connectivity (wired by PR-9c).
    RelayBlackout,
    /// Ledger-expected holdings fell below the operator's floor (wired by PR-16).
    HoldingsLow,
}

impl AlertKind {
    /// The stable wire spelling carried in `Msg::OperatorAlert.kind` and the outbox row id.
    pub fn wire_str(self) -> &'static str {
        match self {
            AlertKind::RefundParked => "refund_parked",
            AlertKind::RefundStuck => "refund_stuck",
            AlertKind::TeardownFailed => "teardown_failed",
            AlertKind::RelayBlackout => "relay_blackout",
            AlertKind::HoldingsLow => "holdings_low",
        }
    }
}

/// One alert instance: a `kind` plus human-readable `subject` (the cooldown key alongside `kind`,
/// e.g. the refund id) and `detail`.
#[derive(Debug, Clone)]
pub struct Alert {
    pub kind: AlertKind,
    pub subject: String,
    pub detail: String,
}

impl Alert {
    pub fn new(kind: AlertKind, subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind,
            subject: subject.into(),
            detail: detail.into(),
        }
    }
}

/// The supervisor-owned alert sink. Cheap to share behind an `Arc`; the refunder (and, later, the
/// teardown/relay/holdings paths) hold a clone and call [`AlertDispatcher::dispatch`].
pub struct AlertDispatcher {
    store: Store,
    clock: Arc<dyn Clock>,
    enabled: bool,
    /// Resolved recipient pubkey hex — the operator's `alert_npub`, or the daemon's own operator
    /// key as the self-DM fallback. Empty only when `enabled` is false.
    recipient_hex: String,
    /// Last-sent unix time per `(kind, subject)`, for edge-triggering. In-memory, not persisted.
    last_sent: Mutex<HashMap<(AlertKind, String), i64>>,
}

impl AlertDispatcher {
    /// An enabled dispatcher delivering to `recipient_hex` (the operator's personal npub hex, or the
    /// operator key hex for the self-DM fallback).
    pub fn new(store: Store, clock: Arc<dyn Clock>, recipient_hex: String) -> Self {
        Self {
            store,
            clock,
            enabled: true,
            recipient_hex,
            last_sent: Mutex::new(HashMap::new()),
        }
    }

    /// A no-op dispatcher: `dispatch` writes nothing. Used for the mock/tests default and whenever
    /// `alerts_enabled=false`.
    pub fn disabled(store: Store, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            clock,
            enabled: false,
            recipient_hex: String::new(),
            last_sent: Mutex::new(HashMap::new()),
        }
    }

    /// True if this dispatcher will actually enqueue (enabled with a recipient). Callers may skip
    /// building an [`Alert`] when this is false, but `dispatch` is always safe to call.
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.recipient_hex.is_empty()
    }

    /// The resolved recipient pubkey hex, for callers that enqueue the alert row inside their own
    /// transaction (see [`terminal_alert_row`](Self::terminal_alert_row)).
    pub fn recipient_hex(&self) -> &str {
        &self.recipient_hex
    }

    /// Enqueue a durable `operator.alert` DM for a RECURRING condition (e.g. a refund stuck PENDING,
    /// re-detected every drive), unless disabled or suppressed by the per-`(kind, subject)`
    /// cooldown. Edge-triggered: the first call sends; repeats within [`ALERT_COOLDOWN_S`] are
    /// dropped; a call past the window re-fires. Best-effort — the caller keeps its own log line and
    /// must not fail its work on an alert error. NOT for terminal one-shot conditions: those enqueue
    /// atomically via [`terminal_alert_row`](Self::terminal_alert_row) instead.
    ///
    /// The cooldown is stamped only AFTER the enqueue COMMITS, so a transient store failure lets the
    /// next drive retry rather than muting the condition for the whole window (coderabbit/codex).
    pub async fn dispatch(&self, alert: Alert) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        let now = self.clock.now();

        // Cooldown check under the lock (no await held). Do NOT stamp yet — a failed enqueue below
        // must be retryable on the next drive, not suppressed for 6h.
        {
            let map = self.last_sent.lock().expect("alert cooldown map poisoned");
            if let Some(&last) = map.get(&(alert.kind, alert.subject.clone())) {
                if now - last < ALERT_COOLDOWN_S {
                    return Ok(());
                }
            }
        }

        let payload = serde_json::to_string(&Msg::OperatorAlert(OperatorAlert {
            kind: alert.kind.wire_str().to_string(),
            subject: alert.subject.clone(),
            detail: alert.detail.clone(),
        }))?;
        // Distinct id per fire (cooldown already bounds the rate), so a legitimate re-fire past the
        // window is never swallowed by ON CONFLICT; the conflict guard only absorbs a same-second
        // boundary race.
        let outbox_id = format!(
            "outbox:alert:{}:{}:{now}",
            alert.kind.wire_str(),
            alert.subject
        );
        let recipient = self.recipient_hex.clone();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO outbox
                        (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                     VALUES (?1, ?2, NULL, 'operator.alert', ?3, 'PENDING', 0, ?4)
                     ON CONFLICT(id) DO NOTHING",
                    params![outbox_id, recipient, payload, now],
                )?;
                Ok(())
            })
            .await?;

        // Committed — now stamp the cooldown so repeats within the window are suppressed.
        self.last_sent
            .lock()
            .expect("alert cooldown map poisoned")
            .insert((alert.kind, alert.subject), now);
        Ok(())
    }

    /// Build a durable outbox row for a TERMINAL (one-shot) alert, for the caller to insert INSIDE
    /// its own state-transition transaction so the alert commits atomically with the condition it
    /// reports (codex: a parked refund must not lose its alert to a crash between commit and a
    /// best-effort enqueue). `None` when disabled. No cooldown — a terminal condition fires once, and
    /// the stable id + the caller's `ON CONFLICT DO NOTHING` make a re-drive idempotent.
    pub fn terminal_alert_row(
        &self,
        kind: AlertKind,
        subject: &str,
        detail: &str,
    ) -> Option<AlertRow> {
        if !self.is_enabled() {
            return None;
        }
        let payload = serde_json::to_string(&Msg::OperatorAlert(OperatorAlert {
            kind: kind.wire_str().to_string(),
            subject: subject.to_string(),
            detail: detail.to_string(),
        }))
        .expect("serialize operator alert (three owned strings) is infallible");
        Some(AlertRow {
            id: format!("outbox:alert:{}:{subject}", kind.wire_str()),
            recipient: self.recipient_hex.clone(),
            payload,
        })
    }
}

/// A prepared `operator.alert` outbox row for a terminal alert. The caller inserts it inside its own
/// transaction (see [`AlertDispatcher::terminal_alert_row`]); `msg_type`/`state`/`created_at` are
/// fixed at insert time.
pub struct AlertRow {
    pub id: String,
    pub recipient: String,
    pub payload: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().expect("open memory db");
        conn.execute_batch(SCHEMA).expect("apply schema");
        Store::spawn(conn)
    }

    async fn alert_rows(store: &Store) -> i64 {
        store
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT count(*) FROM outbox WHERE msg_type='operator.alert'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .expect("count alert rows")
    }

    async fn only_alert(store: &Store) -> (String, OperatorAlert) {
        store
            .read(|conn| {
                Ok(conn.query_row(
                    "SELECT recipient, payload_json FROM outbox WHERE msg_type='operator.alert'",
                    [],
                    |r| {
                        let recipient: String = r.get(0)?;
                        let payload: String = r.get(1)?;
                        Ok((recipient, payload))
                    },
                )?)
            })
            .await
            .map(|(recipient, payload)| match serde_json::from_str::<Msg>(&payload).unwrap() {
                Msg::OperatorAlert(a) => (recipient, a),
                other => panic!("expected OperatorAlert, got {other:?}"),
            })
            .expect("read the one alert row")
    }

    #[tokio::test]
    async fn disabled_dispatcher_writes_nothing() {
        let store = mem_store();
        let clock = Arc::new(TestClock::new(1000));
        let d = AlertDispatcher::disabled(store.clone(), clock);
        d.dispatch(Alert::new(AlertKind::RefundParked, "r1", "boom"))
            .await
            .unwrap();
        assert_eq!(alert_rows(&store).await, 0);
    }

    #[tokio::test]
    async fn enabled_dispatch_enqueues_one_addressed_alert() {
        let store = mem_store();
        let clock = Arc::new(TestClock::new(1000));
        let d = AlertDispatcher::new(store.clone(), clock, "deadbeef".into());
        d.dispatch(Alert::new(AlertKind::RefundParked, "r1", "boom"))
            .await
            .unwrap();

        assert_eq!(alert_rows(&store).await, 1);
        let (recipient, alert) = only_alert(&store).await;
        assert_eq!(recipient, "deadbeef");
        assert_eq!(alert.kind, "refund_parked");
        assert_eq!(alert.subject, "r1");
        assert_eq!(alert.detail, "boom");
    }

    #[tokio::test]
    async fn cooldown_suppresses_repeat_then_refires_past_window() {
        let store = mem_store();
        let clock = Arc::new(TestClock::new(1000));
        let d = AlertDispatcher::new(store.clone(), clock.clone(), "npub".into());

        // First fire: sent.
        d.dispatch(Alert::new(AlertKind::RefundStuck, "r1", "stuck"))
            .await
            .unwrap();
        // A different subject is a distinct key — not suppressed.
        d.dispatch(Alert::new(AlertKind::RefundStuck, "r2", "stuck"))
            .await
            .unwrap();
        // Same (kind, subject) inside the window: suppressed.
        clock.set(1000 + ALERT_COOLDOWN_S - 1);
        d.dispatch(Alert::new(AlertKind::RefundStuck, "r1", "stuck again"))
            .await
            .unwrap();
        assert_eq!(alert_rows(&store).await, 2, "repeat within cooldown suppressed");

        // Past the window: re-fires.
        clock.set(1000 + ALERT_COOLDOWN_S);
        d.dispatch(Alert::new(AlertKind::RefundStuck, "r1", "stuck again"))
            .await
            .unwrap();
        assert_eq!(alert_rows(&store).await, 3, "re-fires past the cooldown");
    }
}
