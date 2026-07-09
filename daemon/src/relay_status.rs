//! GATE-1 PR-9c (lnrent-urw.6 / gate1-alerting-operability.md §C): relay-pool status projection +
//! a single edge-triggered zero-connectivity alert.
//!
//! A **thin transport-liveness surface** — deliberately NOT reconnection or failover logic, not a
//! relay-set management verb, and (per ADR-0016) NOT a balance read. It answers exactly two
//! operator questions: "is each relay connected?" (the [`RelayStatusRow`] projection, surfaced over
//! IPC as `Request::Relays` and folded into `lnrent status`) and "has the whole pool gone dark long
//! enough to alert?" (the [`RelayBlackoutMonitor`]).
//!
//! **Honest deliverability note (§A):** the `RelayBlackout` alert is precisely the one alert that
//! CANNOT be delivered while the pool is down — it queues in the durable outbox and drains when
//! connectivity returns (self-limiting). The `Request::Relays` status query is the out-of-band read
//! an operator uses when the DM cannot arrive; a prolonged silence from a daemon known to be up
//! still warrants a direct `lnrent status` check.

use serde::Serialize;
use std::sync::{Arc, Mutex};

/// All relays must be *continuously* disconnected at least this long before a blackout alert fires
/// (§C). 15min tolerates ordinary relay churn/reconnect flaps without paging the operator.
pub const RELAY_BLACKOUT_ALERT_S: i64 = 15 * 60;

/// One relay's liveness projection from the nostr-sdk pool. `last_connected_at` is `None` when the
/// relay has never connected in this process (a fresh, still-connecting relay), else the unix secs
/// of its most recent connection.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RelayStatusRow {
    pub url: String,
    pub connected: bool,
    /// The nostr-sdk `RelayStatus` spelled out (Connected / Disconnected / Pending / Connecting /
    /// Terminated / …) for the human `lnrent status` view.
    pub status: String,
    pub last_connected_at: Option<i64>,
}

/// Project one relay's live pool fields into a [`RelayStatusRow`]. `connected_at_secs` is the
/// nostr-sdk `stats().connected_at()` as raw unix secs (0 = never connected in this process). Pure
/// so the mapping is unit-testable without a live pool.
pub fn project(
    url: String,
    status: impl std::fmt::Display,
    connected: bool,
    connected_at_secs: u64,
) -> RelayStatusRow {
    RelayStatusRow {
        url,
        connected,
        status: status.to_string(),
        last_connected_at: (connected_at_secs != 0).then_some(connected_at_secs as i64),
    }
}

/// True when the pool is non-empty and NO relay is connected. An EMPTY pool is never a blackout —
/// there is nothing to be disconnected from, and a missing relay config is a separate (bootstrap)
/// concern, not a runtime liveness alert.
pub fn all_disconnected(rows: &[RelayStatusRow]) -> bool {
    !rows.is_empty() && rows.iter().all(|r| !r.connected)
}

/// A cheap shared snapshot of the last relay projection. The pool itself lives in the supervisor's
/// engine task; the maintenance loop refreshes this cell each tick ([`RelayStatusCell::set`]) and
/// the IPC `Request::Relays`/`Status` paths read it ([`RelayStatusCell::get`]) — so IPC never
/// touches the async pool directly. Cloneable (shared `Arc`); the default is an empty snapshot.
#[derive(Clone, Default)]
pub struct RelayStatusCell(Arc<Mutex<Vec<RelayStatusRow>>>);

impl RelayStatusCell {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, rows: Vec<RelayStatusRow>) {
        *self.0.lock().expect("relay status cell poisoned") = rows;
    }

    pub fn get(&self) -> Vec<RelayStatusRow> {
        self.0.lock().expect("relay status cell poisoned").clone()
    }
}

/// Edge-triggered zero-connectivity monitor. Tracks the ONSET of an all-disconnected window and
/// fires EXACTLY ONCE per onset — and only once the pool has been continuously all-disconnected for
/// [`RELAY_BLACKOUT_ALERT_S`]. A single connected relay clears the onset and re-arms, so the *next*
/// onset re-alerts. In-memory only (like the alert dispatcher's cooldown map): a restart re-arms,
/// worst case one duplicate alert per blackout that straddles a restart.
#[derive(Debug, Default)]
pub struct RelayBlackoutMonitor {
    /// When the current all-disconnected window began; `None` when not currently in one.
    onset: Option<i64>,
    /// Whether the alert already fired for the CURRENT onset.
    fired: bool,
}

impl RelayBlackoutMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed this tick's projection + `now`. Returns `true` IFF the pool has been continuously
    /// all-disconnected for at least [`RELAY_BLACKOUT_ALERT_S`] and no alert has been CONFIRMED for
    /// this onset yet. Any connectivity re-arms the monitor.
    ///
    /// This deliberately does NOT stamp the onset as fired — the caller confirms via [`mark_fired`]
    /// only once the alert is durably enqueued, so a failed dispatch leaves the onset eligible to
    /// retry on the next tick (coderabbit) instead of being silently swallowed until reconnect.
    pub fn onset_due(&mut self, rows: &[RelayStatusRow], now: i64) -> bool {
        if all_disconnected(rows) {
            let onset = *self.onset.get_or_insert(now);
            !self.fired && now.saturating_sub(onset) >= RELAY_BLACKOUT_ALERT_S
        } else {
            self.onset = None;
            self.fired = false;
            false
        }
    }

    /// The unix time the current all-disconnected window began, or `None` when not in one. The caller
    /// keys the alert on this so a NEW blackout soon after recovery re-alerts despite the dispatcher's
    /// per-`(kind, subject)` 6h cooldown (codex) — each distinct onset is a distinct subject.
    pub fn onset(&self) -> Option<i64> {
        self.onset
    }

    /// Confirm the alert for the current onset was durably enqueued — call ONLY after a successful
    /// dispatch. A no-op if the monitor re-armed in between (no active onset).
    pub fn mark_fired(&mut self) {
        if self.onset.is_some() {
            self.fired = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(url: &str, connected: bool, last: Option<i64>) -> RelayStatusRow {
        RelayStatusRow {
            url: url.to_string(),
            connected,
            status: if connected { "Connected" } else { "Disconnected" }.to_string(),
            last_connected_at: last,
        }
    }

    #[test]
    fn project_maps_pool_fields_and_never_connected_is_none() {
        let connected = project("wss://a".into(), "Connected", true, 1000);
        assert_eq!(
            connected,
            RelayStatusRow {
                url: "wss://a".into(),
                connected: true,
                status: "Connected".into(),
                last_connected_at: Some(1000),
            }
        );
        // connected_at == 0 => never connected in this process => None.
        let fresh = project("wss://b".into(), "Pending", false, 0);
        assert_eq!(fresh.last_connected_at, None);
        assert!(!fresh.connected);
        assert_eq!(fresh.status, "Pending");
    }

    #[test]
    fn all_disconnected_predicate() {
        assert!(!all_disconnected(&[]), "empty pool is never a blackout");
        assert!(!all_disconnected(&[row("a", true, Some(1)), row("b", false, None)]));
        assert!(all_disconnected(&[row("a", false, Some(1)), row("b", false, None)]));
    }

    /// Simulate the maintenance caller with a SUCCESSFUL dispatch: fire once, then confirm.
    fn tick_ok(m: &mut RelayBlackoutMonitor, rows: &[RelayStatusRow], now: i64) -> bool {
        let due = m.onset_due(rows, now);
        if due {
            m.mark_fired();
        }
        due
    }

    #[test]
    fn monitor_fires_once_per_onset_after_threshold_and_rearms() {
        let mut m = RelayBlackoutMonitor::new();
        let down = [row("a", false, Some(100)), row("b", false, None)];
        let up = [row("a", true, Some(100)), row("b", false, None)];

        // Onset at t=1000; not yet past the threshold -> no fire.
        assert!(!tick_ok(&mut m, &down, 1000), "onset alone does not fire");
        assert_eq!(m.onset(), Some(1000), "onset recorded at first all-disconnected tick");
        assert!(
            !tick_ok(&mut m, &down, 1000 + RELAY_BLACKOUT_ALERT_S - 1),
            "one second short of the threshold does not fire"
        );
        // Crossing the threshold fires exactly once.
        assert!(
            tick_ok(&mut m, &down, 1000 + RELAY_BLACKOUT_ALERT_S),
            "fires once the pool has been dark for the threshold"
        );
        assert!(
            !tick_ok(&mut m, &down, 1000 + RELAY_BLACKOUT_ALERT_S + 60),
            "does not re-fire for the same onset"
        );

        // A single connected relay re-arms (onset cleared).
        assert!(!tick_ok(&mut m, &up, 2000));
        assert_eq!(m.onset(), None, "reconnect clears the onset");
        // A NEW onset re-alerts after the threshold — and carries a distinct onset key.
        assert!(!tick_ok(&mut m, &down, 3000), "new onset, not yet past threshold");
        assert_eq!(m.onset(), Some(3000), "the second onset is keyed distinctly from the first");
        assert!(
            tick_ok(&mut m, &down, 3000 + RELAY_BLACKOUT_ALERT_S),
            "the next onset re-alerts after reconnect"
        );
    }

    #[test]
    fn failed_dispatch_leaves_the_onset_eligible_to_retry() {
        let mut m = RelayBlackoutMonitor::new();
        let down = [row("a", false, Some(100))];
        // Record the onset at t=0, then cross the threshold.
        assert!(!m.onset_due(&down, 0), "onset recorded, not yet due");
        assert!(m.onset_due(&down, RELAY_BLACKOUT_ALERT_S), "due once past the threshold");
        // The caller's dispatch FAILED, so it does NOT mark_fired -> the next tick is still due.
        assert!(
            m.onset_due(&down, RELAY_BLACKOUT_ALERT_S + 5),
            "a failed enqueue keeps the onset eligible to retry"
        );
        // A successful confirm stamps it; subsequent ticks in the same onset stop firing.
        m.mark_fired();
        assert!(!m.onset_due(&down, RELAY_BLACKOUT_ALERT_S + 10));
    }

    #[test]
    fn empty_pool_never_fires() {
        let mut m = RelayBlackoutMonitor::new();
        assert!(!tick_ok(&mut m, &[], 1_000_000));
        assert!(!tick_ok(&mut m, &[], 1_000_000 + RELAY_BLACKOUT_ALERT_S * 10));
        assert_eq!(m.onset(), None);
    }
}
