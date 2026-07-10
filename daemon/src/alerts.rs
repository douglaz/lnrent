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
//!   `TeardownFailed`, PR-9c `RelayBlackout`, PR-16 `HoldingsLow`, gate1-operator-sweep (urw.3)
//!   `SweepFailed`, PR-21 `PaidServiceDestroyed`). There is deliberately no `BalanceQueryFailed`: the ledger-authoritative
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

/// Cap on the alert `detail` before it is serialized into the outbox payload. `detail` can embed
/// buyer/endpoint-derived text (e.g. a structural refund-resolution error from a hostile LNURL
/// endpoint), so an unbounded detail would let the operator.alert row exceed the NIP-59 gift-wrap
/// transport ceiling — the wrap fails, the drain treats it as transient, and the row wedges PENDING
/// forever, undelivered (codex xhigh). Capping here (the single serialization point) bounds EVERY
/// alert kind and keeps the wrapped DM far under [`lnrent_wire::MAX_INBOUND_CONTENT_BYTES`].
const MAX_ALERT_DETAIL_CHARS: usize = 1024;
/// Cap on the alert `subject` (the outbox-id tail / cooldown key); bounded for the same reason.
const MAX_ALERT_SUBJECT_CHARS: usize = 256;

/// Truncate `s` to at most `max` chars (on a char boundary), appending `…` when it was cut.
fn cap_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

/// The subject form used in the outbox row **id** — bounded AND collision-resistant. A short subject
/// (the real case: a refund id) is used verbatim; a subject past the cap is replaced by a hash of
/// the FULL subject, so two distinct long subjects of the same kind can never collide on the id
/// (codex: a plain truncation would make `ON CONFLICT DO NOTHING` drop the second alert while
/// `dispatch` stamps it sent, suppressing it for the cooldown window without ever enqueuing a DM).
fn id_subject(subject: &str) -> String {
    if subject.chars().count() <= MAX_ALERT_SUBJECT_CHARS {
        return subject.to_string();
    }
    use sha2::{Digest, Sha256};
    format!("h:{}", hex::encode(Sha256::digest(subject.as_bytes())))
}

/// The serialized `operator.alert` payload with `subject`/`detail` capped so the wrapped DM can
/// never exceed the transport ceiling. Serializing three owned strings is infallible.
fn alert_payload(kind: AlertKind, subject: &str, detail: &str) -> String {
    serde_json::to_string(&Msg::OperatorAlert(OperatorAlert {
        kind: kind.wire_str().to_string(),
        subject: cap_chars(subject, MAX_ALERT_SUBJECT_CHARS),
        detail: cap_chars(detail, MAX_ALERT_DETAIL_CHARS),
    }))
    .expect("serialize operator alert (three owned strings) is infallible")
}

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
    /// A retention `destroy` raced a renewal settlement and tore down a box the buyer just paid for
    /// (wired by PR-21). The subscription stays alive; a refund for the un-provided period follows.
    PaidServiceDestroyed,
    /// An operator sweep was parked FAILED (a gateway-fee rise refused the capped pay, or a
    /// crash-recovered intent was superseded by a new liability) — gate1-operator-sweep (urw.3).
    SweepFailed,
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
            AlertKind::PaidServiceDestroyed => "paid_service_destroyed",
            AlertKind::SweepFailed => "sweep_failed",
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

        let payload = alert_payload(alert.kind, &alert.subject, &alert.detail);
        // Distinct id per fire (cooldown already bounds the rate), so a legitimate re-fire past the
        // window is never swallowed by ON CONFLICT; the conflict guard only absorbs a same-second
        // boundary race. `id_subject` keeps the id bounded WITHOUT letting two distinct long
        // subjects collide (codex).
        let outbox_id = format!(
            "outbox:alert:{}:{}:{now}",
            alert.kind.wire_str(),
            id_subject(&alert.subject)
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
        let payload = alert_payload(kind, subject, detail);
        Some(AlertRow {
            id: format!("outbox:alert:{}:{}", kind.wire_str(), id_subject(subject)),
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

    // codex xhigh: a detail carrying hostile-endpoint text (a structural refund-resolution error
    // relayed from a buyer-controlled LNURL endpoint) must be capped, so the operator.alert row can
    // always be gift-wrapped — an over-cap payload would fail the wrap, be treated as transient, and
    // wedge the row PENDING forever, undelivered. Both enqueue paths cap identically.
    #[tokio::test]
    async fn oversized_detail_is_capped_below_the_transport_ceiling() {
        let store = mem_store();
        let clock = Arc::new(TestClock::new(1000));
        let d = AlertDispatcher::new(store.clone(), clock, "npub".into());

        // 200 KiB — well past the ~40 KiB NIP-59 rumor-content ceiling.
        let huge = "x".repeat(200 * 1024);
        d.dispatch(Alert::new(AlertKind::RefundParked, "r1", huge.clone()))
            .await
            .unwrap();
        let (_recipient, alert) = only_alert(&store).await;
        assert!(
            alert.detail.chars().count() <= MAX_ALERT_DETAIL_CHARS + 1,
            "detail capped to the bound (+1 for the ellipsis)"
        );
        assert!(alert.detail.ends_with('…'), "truncation is marked");
        let async_payload = serde_json::to_string(&Msg::OperatorAlert(alert)).unwrap();

        // The terminal (in-txn) path caps identically.
        let row = d
            .terminal_alert_row(AlertKind::RefundParked, "r2", &huge)
            .expect("enabled dispatcher builds a row");

        // Both payloads sit far under the inbound content bound, so the wrapped DM is deliverable.
        let ceiling = lnrent_wire::MAX_INBOUND_CONTENT_BYTES;
        assert!(async_payload.len() < ceiling / 8, "async payload well under the ceiling");
        assert!(row.payload.len() < ceiling / 8, "terminal payload well under the ceiling");
    }

    // codex (PR #15): two DISTINCT long subjects sharing the first 256 chars must NOT collide on the
    // outbox id — else the second alert's ON CONFLICT would drop it while the sender is stamped sent.
    // The id hashes the FULL subject once it exceeds the cap, so both terminal rows are distinct and
    // both enqueue.
    #[tokio::test]
    async fn distinct_long_subjects_do_not_collide_on_the_outbox_id() {
        let store = mem_store();
        let clock = Arc::new(TestClock::new(1000));
        let d = AlertDispatcher::new(store.clone(), clock, "npub".into());

        let shared_prefix = "s".repeat(MAX_ALERT_SUBJECT_CHARS);
        let subj_a = format!("{shared_prefix}-AAA");
        let subj_b = format!("{shared_prefix}-BBB");
        let row_a = d
            .terminal_alert_row(AlertKind::RefundParked, &subj_a, "a")
            .unwrap();
        let row_b = d
            .terminal_alert_row(AlertKind::RefundParked, &subj_b, "b")
            .unwrap();
        assert_ne!(row_a.id, row_b.id, "distinct long subjects get distinct ids");

        // Both actually persist (no ON CONFLICT drop) when inserted in the same txn window.
        let (a, b) = (row_a, row_b);
        store
            .transaction(move |tx| {
                for r in [&a, &b] {
                    tx.execute(
                        "INSERT INTO outbox
                            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                         VALUES (?1, ?2, NULL, 'operator.alert', ?3, 'PENDING', 0, 1000)
                         ON CONFLICT(id) DO NOTHING",
                        rusqlite::params![r.id, r.recipient, r.payload],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(alert_rows(&store).await, 2, "both long-subject alerts enqueue");
    }
}
