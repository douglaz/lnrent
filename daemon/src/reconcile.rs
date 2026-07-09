//! Deadline reconcile (lnrent-7fp.9, SPEC.md §6.3/§6.5). The finite subscription state machine's
//! TIME-DRIVEN transitions: one [`Reconciler::reconcile_tick`] scans the subscriptions whose
//! `next_deadline` cursor has come due (`<= now`) and fires the single DUE transition for each,
//! recomputing the cursor. A `next_deadline <= now` scan IS the scheduler — there is no separate
//! timer wheel. The daemon supervisor (lnrent-7fp.21) calls this on an interval; this module only
//! exposes the function.
//!
//! Every transition is a COMPARE-AND-SWAP `UPDATE` guarded on `(state, next_deadline)`, so a
//! replayed or stale fire (the cursor already advanced, or the state already moved) affects 0 rows
//! and is a no-op. Every mutation is journaled to `event_log` in the same transaction (the
//! sole-writer store actor serializes them, ADR-0001/§6.5). Buyer DMs are never published here —
//! they are ENQUEUED as `PENDING` `outbox` rows that the delivery sender (lnrent-7fp.10) publishes.
//!
//! Downtime credit (§6.5, ADR-0005, lnrent-7fp.22): an operator outage must never suspend a buyer for
//! the operator's own downtime. Each [`Reconciler::reconcile_tick`] stamps a liveness heartbeat into
//! `daemon_state`; on restart [`Reconciler::apply_restart_downtime_credit`] reads the gap since that
//! heartbeat and, for every ACTIVE sub whose `soft_date`/`paid_through` fell inside the outage, raises
//! a NULLABLE `suspend_not_before` FLOOR so the buyer still gets their full `renew_lead` window of
//! availability before suspension, and a SUSPENDED sub still gets its remaining retention window before
//! destroy. `paid_through` is NEVER moved (it anchors prepaid money AND the
//! `renew:auto:<sub>:<paid_through>` invoice key); the floor self-expires once a later renewal pushes
//! `paid_through` past it. The credit path NEVER mints invoices — a missed soft reminder fires via the
//! existing soft-date transition on the boot catch-up tick. The ACTIVE suspend transition then gates
//! on `effective_suspend_at = max(paid_through, suspend_not_before)`.
//!
//! The five transitions (the machine is finite; reconcile fires exactly these):
//! 1. **PENDING order expiry** — an unpaid order's invoice expired: sub `-> EXPIRED`, its OPEN order
//!    invoice `-> EXPIRED`, its reservation `-> RELEASED`.
//! 2. **soft_date reminder** — an ACTIVE sub reached its renew-recommended date: issue the renewal
//!    invoice (idempotent `renew:auto:<sub>:<paid_through>`), enqueue `billing.invoice` +
//!    `billing.notice`, advance the cursor to `paid_through`. Self-contained issuance — it does NOT
//!    call order_intake and does NOT extend `paid_through` (that is capture's job, on settlement).
//! 3. **suspend** — `paid_through` passed unpaid: ACTIVE `-> SUSPENDED`, run the `suspend` hook
//!    (best-effort), enqueue `billing.notice`, advance the cursor to the retention end.
//! 4. **destroy / terminate** — the retention window ended: SUSPENDED/CANCELLED `-> TERMINATED`, run
//!    the `destroy` hook (best-effort), RELEASE the reservation in the same txn, clear the cursor.
//! 5. **renewal invoice expiring unpaid** — only that invoice `-> EXPIRED`; the SUBSCRIPTION state
//!    is unchanged (the `paid_through` timeline governs it, not the renewal invoice).
//!
//! Totality: any other `(state, due)` with no transition above is a LOGGED no-op, never a
//! panic/error. Reconcile does NOT handle settlement — that is invoice-status-first and owned by
//! capture (lnrent-7fp.8).

use std::sync::Arc;

use anyhow::Result;
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use lnrent_wire::{BillingInvoice, BillingNotice, Msg};

use crate::alerts::{Alert, AlertDispatcher, AlertKind};
use crate::backends::{PaymentBackend, PaymentStatus};
use crate::recipe::Recipe;
use crate::reservation;
use crate::runner::{run_hook, DEFAULT_TIMEOUT};
use crate::store::Store;
use crate::teardown;

/// FLOOR for the soft-date auto-renewal invoice's Lightning expiry (seconds). The actual expiry is
/// sized to the renewal WINDOW — from soft_date through the CREDITED resumable boundary
/// `effective_suspend_at + retention_s` (= `max(paid_through, suspend_not_before) + retention_s`,
/// §6.5) — so the proactively-issued invoice is payable for its whole advertised window, even after
/// a downtime credit pushes that boundary out (a renewal has no capacity-reservation hold, unlike a
/// first order, so the 1h order horizon does NOT apply). This floor only guards a degenerate
/// too-small window. An internal default, not a knob.
const RENEWAL_INVOICE_MIN_EXPIRY_S: u32 = 3600;

/// What one [`Reconciler::reconcile_tick`] did. Every count is a fired transition; all are normal
/// results, not errors. The supervisor (lnrent-7fp.21) can log it; tests assert on rows directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    /// PENDING orders whose invoice expired (`-> EXPIRED`).
    pub expired: usize,
    /// soft-date renewal reminders issued (a renewal invoice + DMs enqueued).
    pub reminded: usize,
    /// ACTIVE subs suspended for non-payment (`-> SUSPENDED`).
    pub suspended: usize,
    /// SUSPENDED/CANCELLED subs whose retention ended (`-> TERMINATED`).
    pub terminated: usize,
    /// OPEN renewal invoices expired (sub state unchanged).
    pub invoices_expired: usize,
    /// Due rows with no transition (stale/replayed CAS, or a state the machine does not act on).
    pub noops: usize,
}

/// Fires the time-driven subscription transitions. Holds the injected seams; the supervisor
/// (lnrent-7fp.21) constructs it and calls [`Reconciler::reconcile_tick`] on an interval, passing
/// the clock's `now`.
pub struct Reconciler {
    store: Store,
    payment: Arc<dyn PaymentBackend>,
    /// The recipe this operator serves (M1a is single-recipe): the `suspend`/`destroy` lifecycle
    /// hooks and the renewal price.
    recipe: Recipe,
    /// Optional GATE-1 alert sink (lnrent-urw.2): a failed retention `destroy` hook records a
    /// `teardown_failure` dead-letter and fires a `TeardownFailed` operator DM. `None` in focused
    /// unit tests / mock wiring; the supervisor injects the real one via [`Reconciler::with_alerts`].
    alerts: Option<Arc<AlertDispatcher>>,
}

/// A subscription whose deadline cursor is due this tick (`next_deadline <= now`).
struct DueSub {
    id: String,
    state: String,
    buyer_pubkey: String,
    paid_through: Option<i64>,
    retention_s: i64,
    next_deadline: i64,
    /// The downtime-credit suspend FLOOR (§6.5); NULL when no outage was credited.
    suspend_not_before: Option<i64>,
}

/// An ACTIVE subscription read while applying restart downtime credit (§6.5): its timers + the current
/// floor/cursor, so the credit math runs entirely off DB rows.
struct CreditCandidate {
    id: String,
    renew_lead_s: Option<i64>,
    soft_date: Option<i64>,
    paid_through: Option<i64>,
    suspend_not_before: Option<i64>,
    next_deadline: Option<i64>,
}

/// A SUSPENDED subscription read while applying restart downtime credit (§6.5): its effective
/// retention start and destroy cursor can be raised without touching paid_through.
struct SuspendedCreditCandidate {
    id: String,
    paid_through: i64,
    retention_s: i64,
    suspend_not_before: Option<i64>,
    next_deadline: i64,
}

/// Recorded instance facts passed to `suspend`/`destroy` hooks when the subscription has already
/// been provisioned. Hooks are recipe-owned and often need these handles to find the resource.
struct HookInstance {
    id: String,
    box_id: Option<String>,
    kind: Option<String>,
    handles_json: Option<String>,
    state: Option<String>,
}

impl Reconciler {
    pub fn new(store: Store, payment: Arc<dyn PaymentBackend>, recipe: Recipe) -> Self {
        Self {
            store,
            payment,
            recipe,
            alerts: None,
        }
    }

    /// Inject the GATE-1 alert sink (lnrent-urw.2) so a failed retention `destroy` surfaces a
    /// `TeardownFailed` operator DM; without it the dead-letter is still recorded + retried, just
    /// not alerted.
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Fire a `TeardownFailed` alert best-effort (cooldown-suppressed; keyed on the sub so repeated
    /// retries collapse to one DM per 6h). Never fails the reconcile tick.
    async fn alert_teardown(&self, sub_id: &str, hook: &str, attempts: i64, error: &str) {
        let Some(alerts) = &self.alerts else { return };
        let detail = format!(
            "provider teardown owed: `{hook}` hook for sub {sub_id} has failed {attempts} time(s); \
             the resource may still be billing. Latest error: {error}"
        );
        if let Err(e) = alerts
            .dispatch(Alert::new(AlertKind::TeardownFailed, sub_id, detail))
            .await
        {
            tracing::warn!(error = %format!("{e:#}"), "failed to enqueue TeardownFailed alert");
        }
    }

    /// Scan every subscription whose `next_deadline` is due (`<= now`) and fire its single DUE
    /// transition, then expire any OPEN renewal invoice past its own expiry. Each transition is a
    /// guarded CAS, so a replay/stale fire is a no-op. Returns a per-transition [`TickReport`].
    pub async fn reconcile_tick(&self, now: i64) -> Result<TickReport> {
        let mut report = TickReport::default();

        // Scan A — subscription deadline cursors. The single transition per sub is chosen by its
        // state (and, for an ACTIVE sub, whether the cursor sits at soft_date or paid_through).
        for d in self.due_subscriptions(now).await? {
            // Each arm fires its guarded CAS and tallies the result at the SAME site, so the
            // per-transition count is single-source — it cannot drift from the dispatch decision.
            // A CAS that touches 0 rows (a stale/replayed fire) and any due state with no transition
            // both fall through to `noops` — totality, never a panic/error.
            match d.state.as_str() {
                "PENDING" => {
                    if self
                        .fire_pending_expiry(&d.id, d.next_deadline, now)
                        .await?
                    {
                        report.expired += 1;
                    } else {
                        report.noops += 1;
                    }
                }
                "ACTIVE" => match d.paid_through {
                    Some(pt) => {
                        // Downtime credit (§6.5): never suspend before this FLOOR. paid_through is the
                        // natural floor when no outage was credited (suspend_not_before NULL).
                        let effective_suspend_at = pt.max(d.suspend_not_before.unwrap_or(pt));
                        if d.next_deadline < pt {
                            // Cursor before paid_through => it sits at soft_date: the renewal reminder.
                            if self
                                .fire_soft_reminder(
                                    &d.id,
                                    &d.buyer_pubkey,
                                    pt,
                                    effective_suspend_at,
                                    d.retention_s,
                                    d.next_deadline,
                                    now,
                                )
                                .await?
                            {
                                report.reminded += 1;
                            } else {
                                report.noops += 1;
                            }
                        } else if self
                            // Cursor at/after paid_through: the unpaid grace ran out — suspend, but
                            // only once `now` reaches the credited floor. Retention runs from the
                            // CREDITED suspension (§6.5): the destroy deadline is
                            // effective_suspend_at + retention_s, so a credited sub gets its full
                            // retention window AFTER it actually suspends — never suspended then
                            // immediately destroyed because pt + retention_s already passed. For an
                            // uncredited sub effective_suspend_at == pt, so this is unchanged.
                            .fire_suspend(
                                &d.id,
                                &d.buyer_pubkey,
                                d.next_deadline,
                                effective_suspend_at,
                                effective_suspend_at + d.retention_s,
                                now,
                            )
                            .await?
                        {
                            report.suspended += 1;
                        } else {
                            report.noops += 1;
                        }
                    }
                    // An ACTIVE sub with no paid_through is anomalous — log and leave it.
                    None => {
                        tracing::warn!(sub = %d.id, "reconcile: ACTIVE sub has no paid_through — no-op");
                        report.noops += 1;
                    }
                },
                "SUSPENDED" | "CANCELLED" => {
                    if self
                        .fire_destroy(&d.id, &d.buyer_pubkey, d.next_deadline, now)
                        .await?
                    {
                        report.terminated += 1;
                    } else {
                        report.noops += 1;
                    }
                }
                "RESUMING" => {
                    tracing::debug!(
                        sub = %d.id,
                        "reconcile: RESUMING sub is owned by the resume driver — no-op"
                    );
                    report.noops += 1;
                }
                // Totality: PROVISIONING / EXPIRED / TERMINATED / REFUND_DUE / a stale cursor on an
                // already-moved sub — no time transition. A logged no-op, never a panic/error.
                other => {
                    tracing::debug!(sub = %d.id, state = other, "reconcile: no transition for due state — no-op");
                    report.noops += 1;
                }
            }
        }

        // Scan B — OPEN renewal invoices past their own expiry. These are NOT on the subscription
        // cursor (the cursor tracks the paid_through timeline), so they are reconciled separately;
        // only the invoice flips, the subscription is untouched.
        report.invoices_expired = self.expire_open_renewals(now).await?;

        // Stamp the liveness heartbeat AFTER the due scan, so the next restart's downtime credit
        // (§6.5) measures the outage from the last time the daemon was known up.
        self.write_heartbeat(now).await?;

        // Idempotency-cache GC runs AFTER the heartbeat and is BEST-EFFORT: a sweep DB error must not
        // skip the liveness stamp or fail the reconcile tick (it is a housekeeping chore, not a
        // correctness gate).
        match self.store.prune_idempotency_caches(now).await {
            Ok(pruned) => tracing::debug!(
                op_invocation = pruned.op_invocation,
                inbound_request = pruned.inbound_request,
                "reconcile: idempotency cache sweep"
            ),
            Err(e) => tracing::warn!(error = %e, "reconcile: idempotency cache sweep failed (non-fatal)"),
        }

        // Retry any owed provider teardowns (lnrent-urw.2) whose backoff has elapsed. Best-effort:
        // a retry error must not fail the tick — the row stays open for the next pass.
        match self.retry_teardowns(now).await {
            Ok(resolved) if resolved > 0 => {
                tracing::info!(resolved, "reconcile: teardown dead-letters resolved")
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "reconcile: teardown retry pass failed (non-fatal)"),
        }

        Ok(report)
    }

    /// Record this tick's wall-clock as the daemon liveness heartbeat (single-row `daemon_state`,
    /// rowid 1). [`Reconciler::apply_restart_downtime_credit`] reads this on the next boot to size the
    /// outage to credit. Written every tick, so the heartbeat tracks liveness at the reconcile cadence.
    async fn write_heartbeat(&self, now: i64) -> Result<()> {
        self.store
            .transaction(move |tx| {
                write_heartbeat_txn(tx, now)?;
                Ok(())
            })
            .await
    }

    /// Boot downtime credit (§6.5, ADR-0005). In ONE txn: read the last liveness heartbeat; if there
    /// is none (first boot) just establish it and credit nothing. Otherwise, for every ACTIVE sub
    /// whose `soft_date` or `paid_through` fell inside the outage window `[last, now]`, raise a
    /// `suspend_not_before` FLOOR so the buyer still gets their full `renew_lead` window of operator
    /// availability before suspension. It also raises that same floor for SUSPENDED subs whose retention
    /// window was still open when the outage began, so their destroy cursor gives back the remaining
    /// retention from restart. `paid_through` is NEVER moved and NO invoice is ever minted here — a
    /// missed soft reminder fires via the normal soft-date transition on the boot catch-up tick.
    ///
    /// For each credited sub: `lead = max(renew_lead_s, 0)`; `soft = soft_date ?? paid_through - lead`;
    /// `pre_available = clamp(last - soft, 0, lead)` is how much lead the buyer already saw before the
    /// outage; `target = now + (lead - pre_available)` gives back the rest from restart;
    /// `new_floor = max(suspend_not_before ?? paid_through, paid_through, target)` (so the floor never
    /// regresses and never precedes the prepaid window). The cursor moves to `new_floor` UNLESS the sub
    /// is still pre-reminder (`next_deadline < paid_through`), in which case it is LEFT so the missed
    /// soft reminder still fires on restart.
    ///
    /// For a SUSPENDED sub (lnrent-d6n) the credit gives back lost RETENTION instead of `renew_lead`,
    /// via the same floor. With `E_old = max(paid_through, suspend_not_before)` and destroy deadline
    /// `B_old = E_old + retention_s`, credit ONLY when `B_old > last` (retention still open at the
    /// outage start — otherwise it already lapsed and destroys normally, the anti-resurrection gate):
    /// `remaining = clamp(B_old - last, 0, retention_s)` is the un-consumed retention; `target_B =
    /// now + remaining` gives it back from restart; `new_B = max(target_B, B_old)` (monotonic) and the
    /// floor rises to `new_B - retention_s`. A SUSPENDED credit ALWAYS moves the cursor
    /// (`next_deadline = new_B`); the pre-reminder LEFT rule above is ACTIVE-only. Each UPDATE
    /// preserves the reconcile CAS shape. Returns the number of subs credited.
    pub async fn apply_restart_downtime_credit(&self, now: i64) -> Result<usize> {
        self.store
            .transaction(move |tx| {
                let last: Option<i64> = tx
                    .query_row(
                        "SELECT last_heartbeat FROM daemon_state WHERE rowid=1",
                        [],
                        |r| r.get(0),
                    )
                    .optional()?;
                // First boot (or a pre-heartbeat DB): establish the heartbeat, credit nothing — there
                // is no known prior uptime, so no outage to credit.
                let Some(last) = last else {
                    write_heartbeat_txn(tx, now)?;
                    return Ok(0);
                };
                // Clock did not advance past the last heartbeat (equal, or went backwards): nothing to
                // credit; leave the heartbeat as-is.
                if now <= last {
                    return Ok(0);
                }

                let candidates = {
                    // Credit a sub iff the outage window [last, now] OVERLAPS its renewal window
                    // [soft_date, effective_suspend_at] at all: `soft_date <= now AND
                    // effective_suspend_at >= last`, where the upper bound is the CREDITED floor
                    // `effective_suspend_at = max(paid_through, suspend_not_before)` (soft_date defaulted
                    // to `paid_through - renew_lead`). Using the EFFECTIVE floor (not raw paid_through) as
                    // the upper bound lets a REPEAT outage — one starting after an earlier credit already
                    // pushed the floor past paid_through — still be credited (lnrent-7fp.22 FIX 2);
                    // `paid_through >= last` would drop it as soon as `last > paid_through`. A BETWEEN-on-
                    // the-endpoints test would MISS an outage lying WHOLLY inside the window and silently
                    // drop that overlap (§6.5). For an uncredited sub the MAX is just paid_through, so the
                    // single-outage selection is unchanged; MAX(..) is NULL when paid_through is NULL (an
                    // anomalous ACTIVE sub), so those stay filtered out.
                    let mut stmt = tx.prepare(
                        "SELECT id, renew_lead_s, soft_date, paid_through, suspend_not_before, next_deadline
                           FROM subscription
                          WHERE state='ACTIVE'
                            AND COALESCE(soft_date, paid_through - COALESCE(renew_lead_s, 0)) <= ?2
                            AND MAX(paid_through, COALESCE(suspend_not_before, paid_through)) >= ?1",
                    )?;
                    let rows = stmt
                        .query_map(params![last, now], |r| {
                            Ok(CreditCandidate {
                                id: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                                renew_lead_s: r.get(1)?,
                                soft_date: r.get(2)?,
                                paid_through: r.get(3)?,
                                suspend_not_before: r.get(4)?,
                                next_deadline: r.get(5)?,
                            })
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    rows
                };

                let mut credited = 0;
                for c in candidates {
                    // The credit math needs both; an ACTIVE sub missing either is anomalous — skip it
                    // rather than fabricate a floor (and the CAS guard cannot match a NULL cursor).
                    let (Some(pt), Some(nd)) = (c.paid_through, c.next_deadline) else {
                        tracing::debug!(sub = %c.id, "downtime credit: ACTIVE sub missing paid_through/next_deadline — skipping");
                        continue;
                    };
                    let lead = c.renew_lead_s.unwrap_or(0).max(0);
                    let raw_soft = c.soft_date.unwrap_or(pt - lead);
                    let existing_floor = c.suspend_not_before.unwrap_or(pt).max(pt);
                    // Measure already-seen availability from the EFFECTIVE credited lead-window start,
                    // which shifts right as each credited outage advances the floor (the credited window
                    // ends at `existing_floor`, so it starts at `existing_floor - lead`). For a REPEAT
                    // outage this makes the credit add only the NON-overlapping operator-down time of the
                    // new outage, so the buyer still gets a full `renew_lead` of online time before
                    // suspension (lnrent-7fp.22 FIX 2). For an uncredited sub `existing_floor` ==
                    // paid_through, so the window start is the original soft_date and the single-outage
                    // result is unchanged.
                    let window_start = raw_soft.max(existing_floor - lead);
                    let pre_available = (last - window_start).clamp(0, lead);
                    let target = now + (lead - pre_available);
                    let new_floor = existing_floor.max(target);
                    // Pre-reminder (cursor at soft_date): LEAVE the cursor so the missed soft reminder
                    // still fires on the boot catch-up; it then advances the cursor to the floor.
                    let new_cursor = if nd < pt { nd } else { new_floor };
                    // A no-op credit whose computed effective floor AND cursor already equal the
                    // stored effective values still satisfies the CAS guard (the row matches, n==1),
                    // but it moves neither the suspend boundary nor the cursor — so it must not
                    // inflate the credited count or append a `downtime_credit` journal row
                    // (lnrent-7fp.22 FIX C). `suspend_not_before=NULL` and `new_floor=paid_through`
                    // are a no-op too: the effective floor was already paid_through.
                    if existing_floor == new_floor && new_cursor == nd {
                        continue;
                    }
                    let n = tx.execute(
                        "UPDATE subscription
                            SET suspend_not_before=?2, next_deadline=?3, updated_at=?4
                          WHERE id=?1 AND state='ACTIVE' AND next_deadline=?5",
                        params![c.id, new_floor, new_cursor, now, nd],
                    )?;
                    if n == 1 {
                        journal(tx, &c.id, "downtime_credit", now)?;
                        credited += 1;
                    }
                }

                let suspended_candidates = {
                    let mut stmt = tx.prepare(
                        "SELECT id, paid_through, retention_s, suspend_not_before, next_deadline
                           FROM subscription
                          WHERE state='SUSPENDED'
                            AND paid_through IS NOT NULL
                            AND retention_s IS NOT NULL
                            AND next_deadline IS NOT NULL
                            AND MAX(paid_through, COALESCE(suspend_not_before, paid_through)) <= ?2
                            AND MAX(paid_through, COALESCE(suspend_not_before, paid_through)) + retention_s > ?1",
                    )?;
                    let rows = stmt
                        .query_map(params![last, now], |r| {
                            Ok(SuspendedCreditCandidate {
                                id: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                                paid_through: r.get(1)?,
                                retention_s: r.get(2)?,
                                suspend_not_before: r.get(3)?,
                                next_deadline: r.get(4)?,
                            })
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    rows
                };

                for c in suspended_candidates {
                    let e_old = c
                        .paid_through
                        .max(c.suspend_not_before.unwrap_or(c.paid_through));
                    let b_old = e_old + c.retention_s;
                    let remaining = (b_old - last).clamp(0, c.retention_s);
                    let target_b = now + remaining;
                    let new_floor = e_old.max(c.paid_through).max(target_b - c.retention_s);
                    let new_b = c.paid_through.max(new_floor) + c.retention_s;
                    if new_b <= c.next_deadline {
                        continue;
                    }
                    let n = tx.execute(
                        "UPDATE subscription
                            SET suspend_not_before=?2, next_deadline=?3, updated_at=?4
                          WHERE id=?1 AND state='SUSPENDED' AND next_deadline=?5",
                        params![c.id, new_floor, new_b, now, c.next_deadline],
                    )?;
                    if n == 1 {
                        journal(tx, &c.id, "downtime_credit", now)?;
                        credited += 1;
                    }
                }

                // Establish the new heartbeat in the SAME txn, so the credited outage is consumed
                // exactly once even if the credit + the heartbeat must be all-or-nothing.
                write_heartbeat_txn(tx, now)?;
                Ok(credited)
            })
            .await
    }

    /// Read every subscription whose cursor is due. NULL cursors are excluded — a cleared cursor is
    /// a terminal/settled sub with nothing for reconcile to fire.
    async fn due_subscriptions(&self, now: i64) -> Result<Vec<DueSub>> {
        self.store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, state, buyer_pubkey, paid_through, retention_s, next_deadline,
                            suspend_not_before
                     FROM subscription
                     WHERE next_deadline IS NOT NULL AND next_deadline <= ?1
                     ORDER BY next_deadline",
                )?;
                let rows = stmt
                    .query_map(params![now], |r| {
                        Ok(DueSub {
                            id: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            state: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            buyer_pubkey: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            paid_through: r.get(3)?,
                            retention_s: r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                            next_deadline: r.get(5)?,
                            suspend_not_before: r.get(6)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    /// True when the locally OPEN order invoice is safe to expire. A backend `Paid` answer means
    /// capture has not applied the settlement yet; leave the PENDING row untouched so capture can
    /// route the money by invoice status. A lookup error also leaves the cursor due for retry rather
    /// than guessing terminal.
    async fn order_invoice_may_expire(&self, sub_id: &str) -> Result<bool> {
        let id = sub_id.to_string();
        let invoice_id: Option<String> = self
            .store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT id
                       FROM invoice
                      WHERE subscription_id=?1 AND kind='order' AND status='OPEN'
                      ORDER BY issued_at DESC
                      LIMIT 1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .await?;

        let Some(invoice_id) = invoice_id else {
            return Ok(true);
        };
        match self.payment.lookup(&invoice_id).await {
            Ok(PaymentStatus::Paid) => {
                tracing::warn!(
                    sub = %sub_id,
                    invoice = %invoice_id,
                    "reconcile: order invoice is paid at backend; leaving OPEN for capture"
                );
                Ok(false)
            }
            Ok(PaymentStatus::Open | PaymentStatus::Expired) => Ok(true),
            Err(e) => {
                tracing::warn!(
                    sub = %sub_id,
                    invoice = %invoice_id,
                    error = %e,
                    "reconcile: order invoice lookup failed; will retry expiry next tick"
                );
                Ok(false)
            }
        }
    }

    /// True when the sub has an OPEN renewal invoice the backend reports PAID — capture (.8) has not
    /// applied the settlement yet, so suspend / terminate must DEFER and leave it for capture to
    /// resume/extend, rather than suspend/terminate (and later refund) a TIMELY-paid renewal while
    /// the settlement watch lagged a reconcile tick (codex P1). A lookup error also defers (retry next
    /// tick) rather than guessing the renewal lapsed; only a definitive Open/Expired lets it proceed.
    async fn renewal_settlement_pending(&self, sub_id: &str) -> Result<bool> {
        let id = sub_id.to_string();
        let invoice_ids: Vec<String> = self
            .store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id FROM invoice
                      WHERE subscription_id=?1 AND kind='renewal' AND status='OPEN'",
                )?;
                let rows = stmt
                    .query_map(params![id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;
        for invoice_id in invoice_ids {
            match self.payment.lookup(&invoice_id).await {
                Ok(PaymentStatus::Paid) => {
                    tracing::warn!(sub = %sub_id, invoice = %invoice_id,
                        "reconcile: renewal invoice paid at backend; deferring suspend/terminate for capture");
                    return Ok(true);
                }
                Ok(PaymentStatus::Open | PaymentStatus::Expired) => {}
                Err(e) => {
                    tracing::warn!(sub = %sub_id, invoice = %invoice_id, error = %e,
                        "reconcile: renewal invoice lookup failed; deferring suspend/terminate");
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn subscription_matches(&self, sub_id: &str, state: &str, nd: i64) -> Result<bool> {
        let (id, state) = (sub_id.to_string(), state.to_string());
        self.store
            .read(move |c| {
                let n: i64 = c.query_row(
                    "SELECT count(*)
                       FROM subscription
                      WHERE id=?1 AND state=?2 AND next_deadline=?3",
                    params![id, state, nd],
                    |r| r.get(0),
                )?;
                Ok(n == 1)
            })
            .await
    }

    async fn subscription_matches_destroy(&self, sub_id: &str, nd: i64) -> Result<bool> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                let n: i64 = c.query_row(
                    "SELECT count(*)
                       FROM subscription
                      WHERE id=?1 AND state IN ('SUSPENDED','CANCELLED') AND next_deadline=?2",
                    params![id, nd],
                    |r| r.get(0),
                )?;
                Ok(n == 1)
            })
            .await
    }

    /// Run a lifecycle hook. A hook FAILURE is non-fatal (logged) and returned as `Some(error)` so
    /// the caller can act on it — `fire_destroy` dead-letters it (lnrent-urw.2), `fire_suspend`
    /// ignores it (a failed suspend burns no money). `None` = the hook ran cleanly.
    async fn run_lifecycle_hook(
        &self,
        hook: &str,
        sub_id: &str,
        buyer_hex: &str,
    ) -> Result<Option<String>> {
        let input = self.lifecycle_hook_input(sub_id, buyer_hex).await?;
        match run_hook(
            &self.recipe.hook(hook),
            &input,
            DEFAULT_TIMEOUT,
            &self.recipe.provisioning.env,
        )
        .await
        {
            Ok(_) => Ok(None),
            Err(e) => {
                let error = format!("{e:#}");
                tracing::warn!(
                    sub = %sub_id,
                    hook,
                    %error,
                    "reconcile lifecycle hook failed (non-fatal)"
                );
                Ok(Some(error))
            }
        }
    }

    /// Retry every OPEN teardown dead-letter whose backoff has elapsed (lnrent-urw.2): re-run its
    /// hook with the persisted handles (§7.2 idempotent, so re-running is safe). Success resolves the
    /// row; a repeat failure bumps attempts + re-alerts. Best-effort — never fails the tick. Returns
    /// how many rows resolved this pass.
    async fn retry_teardowns(&self, now: i64) -> Result<usize> {
        let mut resolved = 0;
        for row in teardown::open_due_rows(&self.store, now).await? {
            // Re-run the hook from the persisted handles alone — at retry time the sub is TERMINATED
            // and its instance row may be gone, so we do NOT re-read it.
            let handles: Value = row
                .handles_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(Value::Null);
            let input = json!({
                "subscription": { "id": row.subscription_id },
                "instance": { "subscription_id": row.subscription_id, "handles": handles.clone() },
                "handles": handles,
            });
            match run_hook(
                &self.recipe.hook(&row.hook),
                &input,
                DEFAULT_TIMEOUT,
                &self.recipe.provisioning.env,
            )
            .await
            {
                Ok(_) => {
                    teardown::mark_resolved(&self.store, &row.subscription_id, &row.hook, now)
                        .await?;
                    tracing::info!(sub = %row.subscription_id, hook = %row.hook, "teardown retry succeeded; dead-letter resolved");
                    resolved += 1;
                }
                Err(e) => {
                    let error = format!("{e:#}");
                    let attempts = teardown::record_failure(
                        &self.store,
                        &row.subscription_id,
                        &row.hook,
                        row.handles_json.clone(),
                        &error,
                        now,
                    )
                    .await?;
                    self.alert_teardown(&row.subscription_id, &row.hook, attempts, &error)
                        .await;
                }
            }
        }
        Ok(resolved)
    }

    async fn lifecycle_hook_input(&self, sub_id: &str, buyer_hex: &str) -> Result<Value> {
        let instance = self.hook_instance(sub_id).await?;
        let Some(instance) = instance else {
            return Ok(json!({
                "subscription": {
                    "id": sub_id,
                    "buyer_pubkey": buyer_hex,
                }
            }));
        };

        let handles = match instance.handles_json.as_deref() {
            Some(raw) => match serde_json::from_str::<Value>(raw) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        sub = %sub_id,
                        instance = %instance.id,
                        error = %e,
                        "reconcile: instance handles_json is invalid; hook gets null handles"
                    );
                    Value::Null
                }
            },
            None => Value::Null,
        };
        Ok(json!({
            "subscription": {
                "id": sub_id,
                "buyer_pubkey": buyer_hex,
            },
            "instance": {
                "id": instance.id,
                "subscription_id": sub_id,
                "box_id": instance.box_id,
                "kind": instance.kind,
                "state": instance.state,
                "handles": handles.clone(),
            },
            "handles": handles,
        }))
    }

    async fn hook_instance(&self, sub_id: &str) -> Result<Option<HookInstance>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT i.id, i.box_id, i.kind, i.handles_json, i.state
                       FROM subscription s
                       JOIN instance i ON i.id=s.instance_id
                      WHERE s.id=?1
                      LIMIT 1",
                    params![id],
                    |r| {
                        Ok(HookInstance {
                            id: r.get(0)?,
                            box_id: r.get(1)?,
                            kind: r.get(2)?,
                            handles_json: r.get(3)?,
                            state: r.get(4)?,
                        })
                    },
                )
                .optional()?)
            })
            .await
    }

    /// Transition 1 — PENDING order expiry. Before expiring a locally OPEN invoice, ask the payment
    /// backend whether it already saw settlement; a `Paid` answer is left for capture (lnrent-7fp.8)
    /// rather than being misclassified as an expired order. The mutation itself is still a CAS
    /// guarded on `(state='PENDING', next_deadline=nd)`: sub `-> EXPIRED`, the OPEN order invoice
    /// `-> EXPIRED`, its reservation `-> RELEASED`, cursor cleared. Returns whether it fired
    /// (`false` = paid backend invoice, backend lookup unavailable, or stale/replayed cursor).
    async fn fire_pending_expiry(&self, sub_id: &str, nd: i64, now: i64) -> Result<bool> {
        if !self.order_invoice_may_expire(sub_id).await? {
            return Ok(false);
        }

        let id = sub_id.to_string();
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "UPDATE subscription SET state='EXPIRED', next_deadline=NULL, updated_at=?2
                     WHERE id=?1 AND state='PENDING' AND next_deadline=?3",
                    params![id, now, nd],
                )?;
                if n == 0 {
                    return Ok(false);
                }
                tx.execute(
                    "UPDATE invoice SET status='EXPIRED'
                     WHERE subscription_id=?1 AND kind='order' AND status='OPEN'",
                    params![id],
                )?;
                reservation::release_txn(tx, &id, now)?;
                journal(tx, &id, "reconcile_order_expired", now)?;
                Ok(true)
            })
            .await
    }

    /// Transition 2 — soft_date reminder (self-contained issuance). Idempotent on the deterministic
    /// `renew:auto:<sub>:<paid_through>` external_id, so one cycle yields exactly one invoice. The
    /// invoice is created on the payment backend first (idempotent), then ONE txn does the guarded
    /// cursor advance + the OPEN renewal invoice + the `billing.invoice`/`billing.notice` outbox
    /// rows. `paid_through` is NOT extended here — that is capture's, on settlement. The cursor
    /// advances to `effective_suspend_at` (the downtime-credit FLOOR, §6.5), not raw `paid_through`,
    /// so a credited outage delays suspension; the invoice/`due_at` still key on `paid_through`.
    #[allow(clippy::too_many_arguments)]
    async fn fire_soft_reminder(
        &self,
        sub_id: &str,
        buyer_hex: &str,
        paid_through: i64,
        effective_suspend_at: i64,
        retention_s: i64,
        cursor_nd: i64,
        now: i64,
    ) -> Result<bool> {
        // external_id stays paid_through-anchored (one cycle = one invoice); only the expiry sizing
        // honors the credited boundary.
        let external_id = format!("renew:auto:{sub_id}:{paid_through}");
        // Size the expiry to the CREDITED renewal WINDOW (§6.5, lnrent-7fp.22): payable from now
        // (soft_date) through the resumable boundary B = effective_suspend_at + retention_s
        // (= max(paid_through, suspend_not_before) + retention_s) — the SAME boundary capture's
        // renewal refund gate honors, after which a settlement would be refunded anyway. Sizing to
        // the stale paid_through + retention_s would, after a long credited outage, mint an invoice
        // that expires before B, leaving the credited window unusable for payment. Floored at
        // RENEWAL_INVOICE_MIN_EXPIRY_S.
        let expiry_s = u32::try_from(
            (effective_suspend_at + retention_s - now).max(i64::from(RENEWAL_INVOICE_MIN_EXPIRY_S)),
        )
        .unwrap_or(u32::MAX);
        // Idempotent on external_id: a re-fire (or crash-retry) reuses the same invoice rather than
        // minting a second one. On a stale cursor the CAS below affects 0 rows and we never insert a
        // DB invoice row; the backend invoice minted just above is harmless (same idempotent
        // external_id, self-expiring), so no DB row is ever stranded.
        let invoice = match self
            .payment
            .create_invoice(
                self.recipe.pricing.amount_sat,
                &format!("lnrent renewal {sub_id}"),
                expiry_s,
                &external_id,
            )
            .await
        {
            Ok(inv) => inv,
            Err(e) => {
                // A transient backend outage must not abort the whole tick — leave the cursor so the
                // next tick retries.
                tracing::warn!(sub = %sub_id, error = %e, "reconcile: renewal invoice creation failed — will retry next tick");
                return Ok(false);
            }
        };
        let billing_invoice = Msg::BillingInvoice(BillingInvoice {
            subscription_id: sub_id.to_string(),
            request_id: None, // operator-initiated; no buyer request to correlate to
            bolt11: invoice.bolt11.clone(),
            amount_sat: invoice.amount_sat,
            due_at: paid_through,
            expires_at: invoice.expires_at,
        });
        let billing_notice = Msg::BillingNotice(BillingNotice {
            subscription_id: sub_id.to_string(),
            state: "ACTIVE".to_string(),
            message: "renewal available; pay before paid_through to avoid suspension".to_string(),
        });
        let write = SoftReminderWrite {
            sub_id: sub_id.to_string(),
            buyer_hex: buyer_hex.to_string(),
            paid_through,
            effective_suspend_at,
            cursor_nd,
            external_id,
            inv_id: invoice.id.clone(),
            backend_invoice_id: invoice.backend_invoice_id.clone(),
            payment_hash: invoice.payment_hash.clone(),
            bolt11: invoice.bolt11.clone(),
            amount_sat: invoice.amount_sat as i64,
            inv_expires_at: invoice.expires_at,
            billing_invoice_json: serde_json::to_string(&billing_invoice)?,
            billing_notice_json: serde_json::to_string(&billing_notice)?,
            now,
        };
        self.store.transaction(move |tx| write.write(tx)).await
    }

    /// Transition 3 — suspend. Gate on the downtime-credit FLOOR first (§6.5): never suspend before
    /// `effective_suspend_at`, even if the cursor came due. Then verify the cursor is still due and run
    /// the recipe `suspend` hook best-effort before the state/cursor move; a crash during the hook
    /// leaves the same due cursor for the next tick. Hook failure is logged and non-fatal per this
    /// bead's scope. The durable move remains a CAS guarded on `(state='ACTIVE', next_deadline=nd)`:
    /// ACTIVE `-> SUSPENDED`, cursor `-> retention_end`, and a `billing.notice` enqueued in one txn.
    /// `retention_end` is `effective_suspend_at + retention_s` (retention runs from the CREDITED
    /// suspension, §6.5), so a long-credited sub is never suspended-then-instantly-destroyed.
    async fn fire_suspend(
        &self,
        sub_id: &str,
        buyer_hex: &str,
        nd: i64,
        effective_suspend_at: i64,
        retention_end: i64,
        now: i64,
    ) -> Result<bool> {
        // Downtime credit (§6.5): a credited outage pushed the suspend FLOOR past `now` — do not
        // suspend the buyer for the operator's downtime. The cursor stays put; a later tick fires.
        if now < effective_suspend_at {
            return Ok(false);
        }
        if !self.subscription_matches(sub_id, "ACTIVE", nd).await? {
            return Ok(false);
        }
        // A renewal paid at the backend but not yet captured will extend paid_through and move this
        // deadline — don't suspend a timely-paid sub out from under the pending capture (codex P1).
        if self.renewal_settlement_pending(sub_id).await? {
            return Ok(false);
        }
        // A failed suspend is non-fatal and burns no money — log and proceed (no dead-letter).
        let _ = self.run_lifecycle_hook("suspend", sub_id, buyer_hex).await?;

        let notice = Msg::BillingNotice(BillingNotice {
            subscription_id: sub_id.to_string(),
            state: "SUSPENDED".to_string(),
            message: "subscription suspended for non-payment; renew to resume".to_string(),
        });
        let notice_json = serde_json::to_string(&notice)?;
        let (id, buyer) = (sub_id.to_string(), buyer_hex.to_string());
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "UPDATE subscription SET state='SUSPENDED', next_deadline=?2, updated_at=?3
                     WHERE id=?1 AND state='ACTIVE' AND next_deadline=?4",
                    params![id, retention_end, now, nd],
                )?;
                if n == 0 {
                    return Ok(false);
                }
                enqueue(
                    tx,
                    &format!("outbox:suspend-notice:{id}:{nd}"),
                    &buyer,
                    &id,
                    "billing.notice",
                    &notice_json,
                    now,
                )?;
                journal(tx, &id, "reconcile_suspend", now)?;
                Ok(true)
            })
            .await
    }

    /// Transition 4 — destroy / terminate. CAS guarded on
    /// `(state IN ('SUSPENDED','CANCELLED'), next_deadline=nd)`: `-> TERMINATED`, RELEASE the
    /// reservation in the SAME txn (the order id IS the subscription id, lnrent-7fp.17), cursor
    /// cleared. The `destroy` hook runs best-effort before that txn, so a crash before capacity is
    /// released leaves the retention cursor due for a retry. Hook failure is logged and non-fatal.
    async fn fire_destroy(&self, sub_id: &str, buyer_hex: &str, nd: i64, now: i64) -> Result<bool> {
        if !self.subscription_matches_destroy(sub_id, nd).await? {
            return Ok(false);
        }
        // A renewal paid within the resumable window (settled_at < paid_through + retention_s) will
        // RESUME the sub at capture — don't terminate it while that settlement is pending (codex P1).
        if self.renewal_settlement_pending(sub_id).await? {
            return Ok(false);
        }
        // A failed retention `destroy` is DEAD-LETTERED (lnrent-urw.2): the provider resource may not
        // have been torn down and keeps billing the operator invisibly. Purely additive — record the
        // owed cleanup + alert, then still TERMINATE + release below (unchanged: §9.3). The retry
        // loop re-runs the idempotent hook until it succeeds.
        if let Some(error) = self.run_lifecycle_hook("destroy", sub_id, buyer_hex).await? {
            let handles = self
                .hook_instance(sub_id)
                .await?
                .and_then(|i| i.handles_json);
            let attempts =
                teardown::record_failure(&self.store, sub_id, "destroy", handles, &error, now)
                    .await?;
            self.alert_teardown(sub_id, "destroy", attempts, &error).await;
        }

        let id = sub_id.to_string();
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "UPDATE subscription SET state='TERMINATED', next_deadline=NULL, updated_at=?2
                     WHERE id=?1 AND state IN ('SUSPENDED','CANCELLED') AND next_deadline=?3",
                    params![id, now, nd],
                )?;
                if n == 0 {
                    return Ok(false);
                }
                // Free the capacity hold atomically with the terminate (§9.3). Idempotent.
                reservation::release_txn(tx, &id, now)?;
                journal(tx, &id, "reconcile_terminate", now)?;
                Ok(true)
            })
            .await
    }

    /// Transition 5 — expire OPEN renewal invoices past their own expiry. Each flip is a CAS on
    /// `status='OPEN'` (a since-paid invoice affects 0 rows); the SUBSCRIPTION is never touched.
    /// Returns how many invoices expired.
    async fn expire_open_renewals(&self, now: i64) -> Result<usize> {
        let rows: Vec<(String, String)> = self
            .store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, subscription_id FROM invoice
                     WHERE kind='renewal' AND status='OPEN'
                       AND expires_at IS NOT NULL AND expires_at <= ?1",
                )?;
                let rows = stmt
                    .query_map(params![now], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;
        let mut expired = 0;
        for (inv_id, sub_id) in rows {
            // Don't expire a renewal invoice the backend reports PAID — leave it OPEN for capture
            // (.8) to apply (codex P1). A lookup error skips it this tick and retries next tick.
            match self.payment.lookup(&inv_id).await {
                Ok(PaymentStatus::Paid) => {
                    tracing::warn!(invoice = %inv_id, "reconcile: renewal invoice paid at backend; leaving OPEN for capture");
                    continue;
                }
                Ok(PaymentStatus::Open | PaymentStatus::Expired) => {}
                Err(e) => {
                    tracing::warn!(invoice = %inv_id, error = %e, "reconcile: renewal invoice lookup failed; will retry expiry next tick");
                    continue;
                }
            }
            let claimed = self
                .store
                .transaction(move |tx| {
                    let n = tx.execute(
                        "UPDATE invoice SET status='EXPIRED' WHERE id=?1 AND status='OPEN'",
                        params![inv_id],
                    )?;
                    if n == 0 {
                        return Ok(false);
                    }
                    journal(tx, &sub_id, "reconcile_renewal_invoice_expired", now)?;
                    Ok(true)
                })
                .await?;
            if claimed {
                expired += 1;
            }
        }
        Ok(expired)
    }
}

/// Owned inputs for the soft-date reminder's atomic write, so the txn closure is `move + 'static`
/// (the same pattern as order_intake's `RenewalWrite`).
struct SoftReminderWrite {
    sub_id: String,
    buyer_hex: String,
    paid_through: i64,
    /// Where the cursor advances to: the downtime-credit suspend FLOOR (§6.5), which equals
    /// `paid_through` when no outage was credited.
    effective_suspend_at: i64,
    /// The cursor value the fire was scheduled against — the CAS guard, so a stale fire is a no-op.
    cursor_nd: i64,
    external_id: String,
    inv_id: String,
    backend_invoice_id: String,
    payment_hash: String,
    bolt11: String,
    amount_sat: i64,
    inv_expires_at: i64,
    billing_invoice_json: String,
    billing_notice_json: String,
    now: i64,
}

impl SoftReminderWrite {
    /// Guarded cursor advance + OPEN renewal invoice + `billing.invoice`/`billing.notice` outbox
    /// rows, in one txn. Returns `false` (no-op) when the cursor already advanced.
    fn write(self, tx: &Transaction) -> Result<bool> {
        // CAS: advance ACTIVE's cursor from soft_date to the effective suspend time (the credit FLOOR,
        // = paid_through when uncredited). State stays ACTIVE.
        let n = tx.execute(
            "UPDATE subscription SET next_deadline=?2, updated_at=?3
             WHERE id=?1 AND state='ACTIVE' AND next_deadline=?4",
            params![
                self.sub_id,
                self.effective_suspend_at,
                self.now,
                self.cursor_nd
            ],
        )?;
        if n == 0 {
            return Ok(false);
        }
        // Idempotent on external_id: one cycle = one OPEN renewal invoice.
        tx.execute(
            "INSERT INTO invoice
                (id, subscription_id, external_id, backend_invoice_id, payment_hash, kind,
                 bolt11, amount_sat, status, expires_at, issued_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'renewal', ?6, ?7, 'OPEN', ?8, ?9)
             ON CONFLICT(external_id) DO NOTHING",
            params![
                self.inv_id,
                self.sub_id,
                self.external_id,
                self.backend_invoice_id,
                self.payment_hash,
                self.bolt11,
                self.amount_sat,
                self.inv_expires_at,
                self.now,
            ],
        )?;
        // Stable outbox ids (per cycle) make a redelivery a no-op insert — belt-and-suspenders with
        // the CAS guard above, mirroring the provision outbox (lnrent-7fp.10).
        enqueue(
            tx,
            &format!(
                "outbox:billing-invoice:{}:{}",
                self.sub_id, self.paid_through
            ),
            &self.buyer_hex,
            &self.sub_id,
            "billing.invoice",
            &self.billing_invoice_json,
            self.now,
        )?;
        enqueue(
            tx,
            &format!(
                "outbox:billing-notice:{}:{}",
                self.sub_id, self.paid_through
            ),
            &self.buyer_hex,
            &self.sub_id,
            "billing.notice",
            &self.billing_notice_json,
            self.now,
        )?;
        journal(tx, &self.sub_id, "reconcile_soft_renewal", self.now)?;
        Ok(true)
    }
}

/// Upsert the single-row liveness heartbeat (`daemon_state`, rowid 1) inside `tx` — the shared SQL
/// for both the per-tick heartbeat and the boot downtime credit's same-txn heartbeat (§6.5). The
/// write is MONOTONIC (`MAX(last_heartbeat, excluded.last_heartbeat)`): a tick whose wall-clock `now`
/// moved BACKWARD never regresses the heartbeat, so a later restart can't re-credit an interval the
/// daemon was actually alive for (the boot credit's `now <= last` early-return guards the same way).
fn write_heartbeat_txn(tx: &Transaction, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO daemon_state (rowid, last_heartbeat) VALUES (1, ?1)
         ON CONFLICT(rowid) DO UPDATE SET last_heartbeat=MAX(last_heartbeat, excluded.last_heartbeat)",
        params![now],
    )?;
    Ok(())
}

/// Journal a reconcile event to `event_log` in the same txn (every mutation is journaled,
/// ADR-0001/§6.5). `subscription_id` is the affected sub (or order).
fn journal(tx: &Transaction, sub_id: &str, kind: &str, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, '{}', ?3)",
        params![sub_id, kind, now],
    )?;
    Ok(())
}

/// Enqueue a buyer DM as a `PENDING` `outbox` row in the same txn — published only by the delivery
/// sender (lnrent-7fp.10), never directly. `ON CONFLICT(id) DO NOTHING` makes a stable-id re-enqueue
/// idempotent. `recipient` is the buyer's hex pubkey (the sender parses it).
#[allow(clippy::too_many_arguments)]
fn enqueue(
    tx: &Transaction,
    id: &str,
    recipient: &str,
    sub_id: &str,
    msg_type: &str,
    payload_json: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox
            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6)
         ON CONFLICT(id) DO NOTHING",
        params![id, recipient, sub_id, msg_type, payload_json, now],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Clock, TestClock};
    use crate::store::{migrate, Store};
    use rusqlite::Connection;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Build via migrate() (not raw SCHEMA) so the store carries every applied migration — including
    // `subscription.suspend_not_before` (migration 3, §6.5), which the downtime-credit tests read.
    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        Store::spawn(conn)
    }

    fn dummy_recipe() -> Recipe {
        Recipe::load(format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR")))
            .expect("dummy recipe")
    }

    /// A dummy-id recipe whose `suspend`/`destroy` hooks touch a marker file, so a test can assert
    /// the hook actually ran. Mirrors the marker-recipe helpers in provision.rs.
    fn marker_recipe() -> (Recipe, PathBuf, PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("lnrent-reconcile-{}-{seq}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let suspend_marker = dir.join("suspended");
        let destroy_marker = dir.join("destroyed");
        std::fs::write(
            dir.join("suspend"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                suspend_marker.display()
            ),
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                destroy_marker.display()
            ),
        )
        .unwrap();
        for h in ["suspend", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe(); // keep service.id = "dummy"; only the hook dir changes
        r.dir = dir;
        (r, suspend_marker, destroy_marker)
    }

    fn reconciler(store: Store, recipe: Recipe) -> Reconciler {
        Reconciler::new(store, Arc::new(crate::backends::MockPayment::new()), recipe)
    }

    /// A dummy-id recipe whose `destroy` hook FAILS (exit 1) — for the teardown dead-letter path
    /// (lnrent-urw.2). Returns the recipe + its dir so a test can later overwrite `destroy` to
    /// succeed and exercise the retry-resolution.
    fn failing_destroy_recipe() -> (Recipe, PathBuf) {
        let (mut r, _s, destroy) = marker_recipe();
        let dir = destroy.parent().unwrap().to_path_buf();
        std::fs::write(
            dir.join("destroy"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo 'boom: droplet delete failed' >&2; exit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(dir.join("destroy"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        r.dir = dir.clone();
        (r, dir)
    }

    /// A reconciler with an ENABLED alert sink delivering to `recipient`, so a teardown failure
    /// enqueues an `operator.alert` outbox row.
    fn reconciler_with_alerts(store: Store, recipe: Recipe, recipient: &str) -> Reconciler {
        let dispatcher = Arc::new(crate::alerts::AlertDispatcher::new(
            store.clone(),
            Arc::new(TestClock::new(0)),
            recipient.to_string(),
        ));
        reconciler(store, recipe).with_alerts(dispatcher)
    }

    async fn operator_alert_count(store: &Store) -> i64 {
        count(store, "SELECT count(*) FROM outbox WHERE msg_type='operator.alert'").await
    }

    fn settlement(external_id: &str, settled_at: i64) -> crate::backends::Settlement {
        crate::backends::Settlement {
            invoice_id: format!("inv-{external_id}"),
            external_id: external_id.to_string(),
            amount_sat: 100,
            settled_at,
        }
    }

    // ---- seed helpers -------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    async fn seed_sub(
        store: &Store,
        id: &str,
        state: &str,
        buyer: &str,
        paid_through: Option<i64>,
        retention_s: i64,
        next_deadline: Option<i64>,
    ) {
        let (id, state, buyer) = (id.to_string(), state.to_string(), buyer.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, state, buyer_pubkey, period_s, renew_lead_s, retention_s,
                         paid_through, next_deadline, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 100, 10, ?4, ?5, ?6, 0, 0)",
                    params![id, state, buyer, retention_s, paid_through, next_deadline],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_invoice(
        store: &Store,
        inv_id: &str,
        sub_id: &str,
        external_id: &str,
        kind: &str,
        status: &str,
        expires_at: Option<i64>,
    ) {
        let (inv_id, sub_id, external_id, kind, status) = (
            inv_id.to_string(),
            sub_id.to_string(),
            external_id.to_string(),
            kind.to_string(),
            status.to_string(),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, expires_at, issued_at)
                     VALUES (?1, ?2, ?3, ?4, 100, ?5, ?6, 0)",
                    params![inv_id, sub_id, external_id, kind, status, expires_at],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_reservation(store: &Store, order_id: &str) {
        let order_id = order_id.to_string();
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO reservation (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                     VALUES (?1, ?2, '{\"cpu\":1}', '{\"count\":0}', 'HELD', 0, 0)",
                    params![format!("res-{order_id}"), order_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    // ---- read helpers -------------------------------------------------------

    async fn sub_state(store: &Store, id: &str) -> String {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT state FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    async fn sub_next_deadline(store: &Store, id: &str) -> Option<i64> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT next_deadline FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    async fn inv_status(store: &Store, external_id: &str) -> String {
        let external_id = external_id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT status FROM invoice WHERE external_id=?1",
                    params![external_id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    // ---- downtime-credit helpers (§6.5, lnrent-7fp.22) ----------------------

    /// Seed an ACTIVE sub with full control over the credit-relevant timers (renew_lead_s, soft_date)
    /// that the trimmed `seed_sub` hard-codes; `suspend_not_before` starts NULL (no credit yet).
    #[allow(clippy::too_many_arguments)]
    async fn seed_active_sub(
        store: &Store,
        id: &str,
        buyer: &str,
        renew_lead_s: i64,
        soft_date: i64,
        paid_through: i64,
        retention_s: i64,
        next_deadline: i64,
    ) {
        let (id, buyer) = (id.to_string(), buyer.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, state, buyer_pubkey, period_s, renew_lead_s, retention_s,
                         paid_through, soft_date, next_deadline, created_at, updated_at)
                     VALUES (?1, 'ACTIVE', ?2, 100, ?3, ?4, ?5, ?6, ?7, 0, 0)",
                    params![
                        id,
                        buyer,
                        renew_lead_s,
                        retention_s,
                        paid_through,
                        soft_date,
                        next_deadline
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Stamp the liveness heartbeat (single-row `daemon_state`, rowid 1) so a restart credit measures
    /// the outage from `last`.
    async fn set_heartbeat(store: &Store, last: i64) {
        store
            .transaction(move |tx| {
                write_heartbeat_txn(tx, last)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn heartbeat(store: &Store) -> Option<i64> {
        store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT last_heartbeat FROM daemon_state WHERE rowid=1",
                    [],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .await
            .unwrap()
    }

    async fn sub_suspend_not_before(store: &Store, id: &str) -> Option<i64> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT suspend_not_before FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    /// Stamp the downtime-credit FLOOR directly (the credit normally computes it; tests that need a
    /// pre-existing floor set it here).
    async fn set_suspend_not_before(store: &Store, id: &str, floor: i64) {
        let id = id.to_string();
        store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE subscription SET suspend_not_before=?2 WHERE id=?1",
                    params![id, floor],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn inv_expires_at(store: &Store, external_id: &str) -> Option<i64> {
        let external_id = external_id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT expires_at FROM invoice WHERE external_id=?1",
                    params![external_id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    // Test 1a: a PENDING order past its invoice expiry -> sub EXPIRED + the OPEN order invoice EXPIRED.
    #[tokio::test]
    async fn pending_order_expiry_fires_at_deadline() {
        let store = mem_store();
        let clock = TestClock::new(50);
        seed_sub(&store, "o1", "PENDING", "buyer", None, 0, Some(100)).await;
        seed_invoice(
            &store,
            "inv-o1",
            "o1",
            "order:o1",
            "order",
            "OPEN",
            Some(100),
        )
        .await;
        seed_reservation(&store, "o1").await;
        let r = reconciler(store.clone(), dummy_recipe());

        // Before the deadline: nothing fires.
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.expired, 0);
        assert_eq!(sub_state(&store, "o1").await, "PENDING");

        // At/after the deadline: sub + invoice expire, cursor cleared.
        clock.set(150);
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.expired, 1);
        assert_eq!(sub_state(&store, "o1").await, "EXPIRED");
        assert_eq!(inv_status(&store, "order:o1").await, "EXPIRED");
        assert_eq!(sub_next_deadline(&store, "o1").await, None);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE order_id='o1' AND state='RELEASED'"
            )
            .await,
            1,
            "order expiry releases the reservation"
        );
    }

    #[tokio::test]
    async fn pending_order_paid_at_backend_is_left_for_capture() {
        let store = mem_store();
        let payment = Arc::new(crate::backends::MockPayment::new());
        let inv = payment
            .create_invoice(100, "lnrent order o1", 100, "order:o1")
            .await
            .unwrap();
        payment.settle("order:o1", 80).unwrap();
        seed_sub(
            &store,
            "o1",
            "PENDING",
            "buyer",
            None,
            0,
            Some(inv.expires_at),
        )
        .await;
        seed_invoice(
            &store,
            &inv.id,
            "o1",
            "order:o1",
            "order",
            "OPEN",
            Some(inv.expires_at),
        )
        .await;
        seed_reservation(&store, "o1").await;
        let r = Reconciler::new(store.clone(), payment.clone(), dummy_recipe());

        let rep = r.reconcile_tick(inv.expires_at).await.unwrap();
        assert_eq!(rep.expired, 0);
        assert_eq!(rep.noops, 1);
        assert_eq!(sub_state(&store, "o1").await, "PENDING");
        assert_eq!(inv_status(&store, "order:o1").await, "OPEN");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE order_id='o1' AND state='HELD'"
            )
            .await,
            1,
            "capacity remains held while capture applies the paid invoice"
        );
    }

    // Test 1b: an ACTIVE sub at its soft_date -> a renewal invoice (renew:auto external_id) +
    // billing.invoice + billing.notice outbox rows; the cursor advances to paid_through, state ACTIVE.
    #[tokio::test]
    async fn soft_date_reminder_issues_invoice_and_enqueues_dms() {
        let store = mem_store();
        // paid_through=1000, soft_date(cursor)=900.
        seed_sub(
            &store,
            "s1",
            "ACTIVE",
            "buyerhex",
            Some(1000),
            500,
            Some(900),
        )
        .await;
        let r = reconciler(store.clone(), dummy_recipe());

        let rep = r.reconcile_tick(900).await.unwrap();
        assert_eq!(rep.reminded, 1);

        // Exactly one OPEN renewal invoice with the deterministic auto external_id.
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal' AND status='OPEN' AND external_id='renew:auto:s1:1000'").await,
            1
        );
        // A billing.invoice and a billing.notice are enqueued PENDING to the buyer.
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox WHERE state='PENDING' AND msg_type='billing.invoice' AND recipient='buyerhex'").await,
            1
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM outbox WHERE state='PENDING' AND msg_type='billing.notice'"
            )
            .await,
            1
        );
        // Cursor advanced to paid_through; the sub is still ACTIVE (paid_through is NOT extended).
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1000));
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");
    }

    // Test 1c: an ACTIVE sub past paid_through -> SUSPENDED, the suspend hook ran, a billing.notice
    // is enqueued, and the cursor advances to the retention end (paid_through + retention_s).
    #[tokio::test]
    async fn suspend_fires_runs_hook_and_advances_to_retention_end() {
        let store = mem_store();
        let (recipe, suspend_marker, _destroy_marker) = marker_recipe();
        // paid_through=1000 (cursor at paid_through), retention=500 -> retention end 1500.
        seed_sub(
            &store,
            "s1",
            "ACTIVE",
            "buyerhex",
            Some(1000),
            500,
            Some(1000),
        )
        .await;
        let r = reconciler(store.clone(), recipe);

        let rep = r.reconcile_tick(1000).await.unwrap();
        assert_eq!(rep.suspended, 1);
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(1500),
            "cursor -> retention end"
        );
        assert!(suspend_marker.exists(), "suspend hook ran");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM outbox WHERE state='PENDING' AND msg_type='billing.notice'"
            )
            .await,
            1
        );
    }

    // codex P1: a renewal paid at the backend but not yet captured (settlement watch lagged this
    // tick) must NOT be suspended out from under the pending capture, which will extend paid_through.
    // reconcile defers the suspend; the sub stays ACTIVE and the renewal invoice stays OPEN.
    #[tokio::test]
    async fn renewal_paid_at_backend_defers_suspend() {
        let store = mem_store();
        let payment = Arc::new(crate::backends::MockPayment::new());
        let inv = payment
            .create_invoice(100, "lnrent renewal s1", 10000, "renew:auto:s1:1000")
            .await
            .unwrap();
        payment.settle("renew:auto:s1:1000", 980).unwrap(); // paid at backend; capture pending
                                                            // ACTIVE sub due for SUSPEND at paid_through=1000 (cursor == paid_through), retention 500.
        seed_sub(&store, "s1", "ACTIVE", "buyer", Some(1000), 500, Some(1000)).await;
        seed_invoice(
            &store,
            &inv.id,
            "s1",
            "renew:auto:s1:1000",
            "renewal",
            "OPEN",
            Some(inv.expires_at),
        )
        .await;
        let r = Reconciler::new(store.clone(), payment.clone(), dummy_recipe());

        let rep = r.reconcile_tick(1000).await.unwrap();
        assert_eq!(
            rep.suspended, 0,
            "a paid-pending renewal defers the suspend"
        );
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");
        assert_eq!(inv_status(&store, "renew:auto:s1:1000").await, "OPEN");
    }

    // Test 1d: a SUSPENDED sub past its retention end -> TERMINATED, the destroy hook ran, the
    // reservation is RELEASED in the same txn, the cursor is cleared.
    #[tokio::test]
    async fn destroy_fires_runs_hook_releases_reservation_and_terminates() {
        let store = mem_store();
        let (recipe, _suspend_marker, destroy_marker) = marker_recipe();
        seed_sub(
            &store,
            "s1",
            "SUSPENDED",
            "buyerhex",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        seed_reservation(&store, "s1").await; // order_id == subscription id (M1a)
        let r = reconciler(store.clone(), recipe);

        let rep = r.reconcile_tick(1500).await.unwrap();
        assert_eq!(rep.terminated, 1);
        assert_eq!(sub_state(&store, "s1").await, "TERMINATED");
        assert_eq!(sub_next_deadline(&store, "s1").await, None);
        assert!(destroy_marker.exists(), "destroy hook ran");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE order_id='s1' AND state='RELEASED'"
            )
            .await,
            1,
            "the reservation was released in the terminate txn"
        );
    }

    // urw.2: a FAILING retention `destroy` still TERMINATES + releases the reservation (unchanged),
    // AND records one open teardown_failure dead-letter + fires exactly one TeardownFailed alert.
    #[tokio::test]
    async fn failing_destroy_dead_letters_alerts_and_still_terminates() {
        let store = mem_store();
        let (recipe, _dir) = failing_destroy_recipe();
        seed_sub(&store, "s1", "SUSPENDED", "buyerhex", Some(1000), 500, Some(1500)).await;
        seed_reservation(&store, "s1").await;
        let r = reconciler_with_alerts(store.clone(), recipe, "op-npub");

        let rep = r.reconcile_tick(1500).await.unwrap();

        // The terminate + release are UNCHANGED by the dead-letter (§9.3).
        assert_eq!(rep.terminated, 1);
        assert_eq!(sub_state(&store, "s1").await, "TERMINATED");
        assert_eq!(
            count(&store, "SELECT count(*) FROM reservation WHERE order_id='s1' AND state='RELEASED'").await,
            1,
            "reservation still released in the terminate txn"
        );
        // The owed cleanup is recorded + alerted.
        let row = crate::teardown::open_row(&store, "s1", "destroy").await.unwrap().unwrap();
        assert_eq!(row.attempts, 1);
        assert!(row.last_error.as_deref().unwrap_or("").contains("exit"), "hook error captured");
        assert_eq!(operator_alert_count(&store).await, 1, "one TeardownFailed alert");
    }

    // urw.2: the retry loop re-runs the (now-fixed) destroy hook past its backoff and resolves the
    // dead-letter; a resolved row drops out of the open view.
    #[tokio::test]
    async fn teardown_retry_resolves_when_hook_later_succeeds() {
        let store = mem_store();
        let (recipe, dir) = failing_destroy_recipe();
        seed_sub(&store, "s1", "SUSPENDED", "buyerhex", Some(1000), 500, Some(1500)).await;
        seed_reservation(&store, "s1").await;
        let r = reconciler_with_alerts(store.clone(), recipe, "op-npub");

        // First tick: destroy fails → open dead-letter (attempts=1, last_attempt_at=1500).
        r.reconcile_tick(1500).await.unwrap();
        assert_eq!(crate::teardown::open_count(&store).await.unwrap(), 1);

        // Fix the destroy hook so the retry succeeds.
        std::fs::write(
            dir.join("destroy"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo '{\"ok\":true}'\n",
        )
        .unwrap();
        std::fs::set_permissions(dir.join("destroy"), std::fs::Permissions::from_mode(0o755)).unwrap();

        // A tick before the backoff (attempts=1 → 120s) elapses does NOT retry.
        r.reconcile_tick(1600).await.unwrap();
        assert_eq!(crate::teardown::open_count(&store).await.unwrap(), 1, "not yet due");

        // Past the backoff: the retry runs the fixed hook and resolves the row.
        r.reconcile_tick(1620).await.unwrap();
        assert_eq!(crate::teardown::open_count(&store).await.unwrap(), 0, "retry resolved the dead-letter");
    }

    // Test 2: a renewal invoice expiring unpaid changes ONLY the invoice (-> EXPIRED), never the sub.
    #[tokio::test]
    async fn renewal_invoice_expiry_changes_only_the_invoice() {
        let store = mem_store();
        // The sub's own cursor is far in the future, so scan A skips it entirely.
        seed_sub(
            &store,
            "s1",
            "ACTIVE",
            "buyerhex",
            Some(10_000),
            500,
            Some(10_000),
        )
        .await;
        seed_invoice(
            &store,
            "inv-r1",
            "s1",
            "renew:auto:s1:10000",
            "renewal",
            "OPEN",
            Some(100),
        )
        .await;
        let r = reconciler(store.clone(), dummy_recipe());

        let rep = r.reconcile_tick(150).await.unwrap();
        assert_eq!(rep.invoices_expired, 1);
        assert_eq!(rep.suspended, 0);
        assert_eq!(inv_status(&store, "renew:auto:s1:10000").await, "EXPIRED");
        // The subscription is untouched: still ACTIVE, cursor unchanged.
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(10_000));
    }

    // Test 3: a CANCELLED sub past its retention end -> TERMINATED, reservation RELEASED (the destroy
    // path serves CANCELLED as well as SUSPENDED).
    #[tokio::test]
    async fn cancelled_terminates_after_retention() {
        let store = mem_store();
        let (recipe, _s, destroy_marker) = marker_recipe();
        seed_sub(
            &store,
            "s1",
            "CANCELLED",
            "buyerhex",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        seed_reservation(&store, "s1").await;
        let r = reconciler(store.clone(), recipe);

        let rep = r.reconcile_tick(1500).await.unwrap();
        assert_eq!(rep.terminated, 1);
        assert_eq!(sub_state(&store, "s1").await, "TERMINATED");
        assert!(
            destroy_marker.exists(),
            "destroy hook ran for a CANCELLED sub"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM reservation WHERE order_id='s1' AND state='RELEASED'"
            )
            .await,
            1
        );
    }

    // Test 4a: the CAS guard makes a replayed fire affect 0 rows — a second tick at the same time
    // never re-fires (no duplicate event_log row, state unchanged), and the renewal reminder is not
    // re-issued because the cursor already advanced past `now`.
    #[tokio::test]
    async fn replayed_fire_is_a_zero_row_noop() {
        let store = mem_store();
        let r = reconciler(store.clone(), dummy_recipe());

        // PENDING expiry: fire once, then replay the same tick — no second effect.
        seed_sub(&store, "o1", "PENDING", "buyer", None, 0, Some(100)).await;
        seed_invoice(
            &store,
            "inv-o1",
            "o1",
            "order:o1",
            "order",
            "OPEN",
            Some(100),
        )
        .await;
        r.reconcile_tick(150).await.unwrap();
        let rep = r.reconcile_tick(150).await.unwrap();
        assert_eq!(
            rep.expired, 0,
            "the replay fires nothing (EXPIRED + NULL cursor is not scanned)"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM event_log WHERE subscription_id='o1' AND kind='reconcile_order_expired'").await,
            1,
            "exactly one expiry journal row across both ticks"
        );

        // soft_date: a re-tick at the same `now` does not re-issue — the cursor advanced to
        // paid_through (> now), so the sub is no longer due.
        seed_sub(
            &store,
            "s1",
            "ACTIVE",
            "buyerhex",
            Some(1000),
            500,
            Some(900),
        )
        .await;
        r.reconcile_tick(900).await.unwrap();
        let rep = r.reconcile_tick(900).await.unwrap();
        assert_eq!(rep.reminded, 0, "no second reminder at the same now");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM invoice WHERE kind='renewal' AND subscription_id='s1'"
            )
            .await,
            1,
            "exactly one renewal invoice for the cycle"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox WHERE subscription_id='s1' AND msg_type='billing.invoice'").await,
            1
        );
    }

    // Test 4b: totality — a due sub in a state with NO transition (PROVISIONING here) is a logged
    // no-op: its state and cursor are unchanged and it journals nothing.
    #[tokio::test]
    async fn due_state_without_a_transition_is_a_logged_noop() {
        let store = mem_store();
        // A stale cursor (<= now) on a state reconcile does not act on.
        seed_sub(&store, "p1", "PROVISIONING", "buyer", None, 0, Some(100)).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let rep = r.reconcile_tick(150).await.unwrap();
        assert_eq!(rep.noops, 1);
        assert_eq!(
            rep.expired + rep.reminded + rep.suspended + rep.terminated,
            0
        );
        assert_eq!(
            sub_state(&store, "p1").await,
            "PROVISIONING",
            "no state change"
        );
        assert_eq!(
            sub_next_deadline(&store, "p1").await,
            Some(100),
            "cursor untouched"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE subscription_id='p1'"
            )
            .await,
            0,
            "a no-op journals nothing"
        );
    }

    // ---- downtime credit (§6.5, ADR-0005, lnrent-7fp.22) -------------------

    // Test 1: the suspend deadline (paid_through) fell INSIDE the outage. Credit raises
    // suspend_not_before (paid_through UNCHANGED) so the buyer gets the rest of their renew_lead
    // window from restart; a boot reconcile BEFORE the credited time does NOT suspend, and suspension
    // fires exactly at the credited floor.
    #[tokio::test]
    async fn suspend_deadline_inside_outage_is_credited_not_suspended() {
        let store = mem_store();
        let clock = TestClock::new(1100);
        // renew_lead=100, soft_date=900, paid_through=1000; post-reminder so the cursor sits at
        // paid_through. Outage: last alive 950 (50s of lead already seen) -> restart at 1100.
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 500, 1000).await;
        set_heartbeat(&store, 950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(clock.now()).await.unwrap();
        assert_eq!(credited, 1);
        // pre_available = clamp(950-900,0,100)=50; target = 1100 + (100-50) = 1150.
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1150));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE id='s1' AND paid_through=1000"
            )
            .await,
            1,
            "paid_through is the prepaid-money anchor — never moved by credit"
        );
        // Cursor (post-reminder) advanced to the floor so reconcile re-checks at the credited time.
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1150));

        // A boot reconcile at restart does NOT suspend — the credited floor is still in the future.
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.suspended, 0, "no suspend before the credited floor");
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");

        // At the credited floor the suspend fires normally (CAS-guarded on the advanced cursor).
        clock.set(1150);
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.suspended, 1, "suspends exactly at the credited floor");
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
    }

    // Test 2: the deadline fell OUTSIDE the outage window — no credit, and the existing suspend timing
    // is unchanged.
    #[tokio::test]
    async fn deadline_outside_outage_is_not_credited() {
        let store = mem_store();
        // Same sub shape as Test 1 but the outage [500,600] precedes both soft_date(900)/paid_through(1000).
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 500, 1000).await;
        set_heartbeat(&store, 500).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(600).await.unwrap();
        assert_eq!(
            credited, 0,
            "deadline outside the outage -> nothing credited"
        );
        assert_eq!(sub_suspend_not_before(&store, "s1").await, None);
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(1000),
            "cursor untouched"
        );

        // Existing timing unchanged: still ACTIVE just before paid_through, suspends at paid_through.
        let rep = r.reconcile_tick(999).await.unwrap();
        assert_eq!(rep.suspended, 0);
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");
        let rep = r.reconcile_tick(1000).await.unwrap();
        assert_eq!(
            rep.suspended, 1,
            "suspend timing is unchanged (fires at paid_through)"
        );
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
    }

    // Test 3: a SOFT-DATE-only outage (soft_date in [last,now], paid_through still ahead). Credit is
    // stored, the cursor STAYS at soft_date, and the normal boot reconcile issues EXACTLY ONE
    // renew:auto:<sub>:<paid_through> invoice and advances the cursor to the credited suspend time.
    #[tokio::test]
    async fn soft_date_only_outage_credits_then_reminds_once() {
        let store = mem_store();
        // renew_lead=100, soft_date=900, paid_through=1000; pre-reminder (cursor at soft_date).
        // Outage: last alive 850 (no lead seen yet) -> restart at 950, before paid_through.
        seed_active_sub(&store, "s1", "buyerhex", 100, 900, 1000, 500, 900).await;
        set_heartbeat(&store, 850).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(950).await.unwrap();
        assert_eq!(credited, 1);
        // pre_available = clamp(850-900,0,100)=0; target = 950 + 100 = 1050.
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1050));
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(900),
            "pre-reminder: cursor LEFT at soft_date so the missed reminder still fires"
        );

        // The boot catch-up reconcile fires the soft reminder: ONE renew:auto invoice keyed on the
        // UNCHANGED paid_through, and the cursor advances to the credited floor (1050), not 1000.
        let rep = r.reconcile_tick(950).await.unwrap();
        assert_eq!(rep.reminded, 1);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM invoice WHERE external_id='renew:auto:s1:1000'"
            )
            .await,
            1,
            "exactly one renew:auto invoice keyed on the unchanged paid_through"
        );
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(1050),
            "cursor advanced to the credited suspend time, not raw paid_through"
        );
    }

    // Test 4: renew_lead_s == 0 (soft_date == paid_through). target == now, so there is no lead window
    // to credit and suspension may occur on restart — and NO auto invoice is created (the zero-lead
    // soft branch never fires, the credit path mints nothing).
    #[tokio::test]
    async fn zero_lead_credits_no_delay_and_no_invoice() {
        let store = mem_store();
        // renew_lead=0 => soft_date == paid_through == 1000; cursor at paid_through.
        seed_active_sub(&store, "s1", "buyer", 0, 1000, 1000, 500, 1000).await;
        set_heartbeat(&store, 950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(1100).await.unwrap();
        assert_eq!(credited, 1);
        // lead=0 => target=now=1100 => floor = max(1000,1000,1100) = 1100 (= now: no delay credited).
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1100));

        // Boot reconcile at restart: the suspend may fire (floor == now), and crucially NO renewal
        // invoice is minted (no soft window) — no auto-invoice duplication.
        let rep = r.reconcile_tick(1100).await.unwrap();
        assert_eq!(rep.reminded, 0, "zero-lead never takes the soft branch");
        assert_eq!(rep.suspended, 1, "no lead to credit -> suspends on restart");
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "the credit path mints nothing; zero-lead issues no auto invoice"
        );
    }

    // Test 5: idempotency — running credit + reconcile twice for the same cycle yields exactly ONE
    // renewal invoice row for the unchanged external_id (the cursor advance + the renew:auto
    // ON CONFLICT idempotency + the same-txn heartbeat together prevent any duplicate).
    #[tokio::test]
    async fn credit_plus_reconcile_is_idempotent_across_two_runs() {
        let store = mem_store();
        seed_active_sub(&store, "s1", "buyerhex", 100, 900, 1000, 500, 900).await;
        set_heartbeat(&store, 850).await;
        let r = reconciler(store.clone(), dummy_recipe());

        // First cycle: credit (sets the heartbeat to 950) then reconcile (mints the one invoice).
        assert_eq!(r.apply_restart_downtime_credit(950).await.unwrap(), 1);
        assert_eq!(r.reconcile_tick(950).await.unwrap().reminded, 1);
        assert_eq!(heartbeat(&store).await, Some(950));

        // Second cycle at the same now: credit no-ops (now <= last heartbeat), and reconcile finds the
        // cursor already advanced past now -> no second reminder.
        assert_eq!(
            r.apply_restart_downtime_credit(950).await.unwrap(),
            0,
            "no second credit for the same cycle (now <= last heartbeat)"
        );
        assert_eq!(r.reconcile_tick(950).await.unwrap().reminded, 0);

        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            1,
            "exactly one renewal invoice across both runs"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM invoice WHERE external_id='renew:auto:s1:1000'"
            )
            .await,
            1,
            "the unchanged external_id was never duplicated"
        );
    }

    // FIX 1 (§6.5, lnrent-7fp.22): a LONG outage credits a floor PAST the ORIGINAL paid_through +
    // retention_s. On restart the sub must NOT be immediately destroyed — it suspends at the credited
    // time, and retention runs from THAT credited suspension (destroy at effective_suspend_at +
    // retention_s), so the buyer gets the full retention window AFTER the credited suspension.
    #[tokio::test]
    async fn long_outage_suspends_at_credited_time_then_full_retention() {
        let store = mem_store();
        let clock = TestClock::new(2000);
        // renew_lead=100, soft_date=900, paid_through=1000, retention=50 -> ORIGINAL retention end
        // 1050. Post-reminder cursor at paid_through. Outage: last alive 950 -> restart at 2000 (LONG;
        // far past 1050), so the credited floor lands well beyond the original retention end.
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 50, 1000).await;
        set_heartbeat(&store, 950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(clock.now()).await.unwrap();
        assert_eq!(credited, 1);
        // pre_available = clamp(950-900,0,100)=50; target = 2000 + (100-50) = 2050 (> 1050, the
        // original paid_through+retention_s).
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(2050));
        // Post-reminder cursor advanced to the credited floor so reconcile re-checks at that time.
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(2050));

        // Boot reconcile at restart: the credited floor (2050) is still in the future, so the sub is
        // NOT due — crucially it is NOT suspended-then-immediately-destroyed.
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.suspended, 0, "not suspended before the credited floor");
        assert_eq!(
            rep.terminated, 0,
            "a long-credited sub is NOT immediately destroyed on restart"
        );
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");

        // At the credited floor: the suspend fires, and the destroy deadline runs from the CREDITED
        // suspension — effective_suspend_at + retention_s = 2050 + 50 = 2100 (NOT the stale 1050).
        clock.set(2050);
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.suspended, 1, "suspends exactly at the credited floor");
        assert_eq!(rep.terminated, 0, "it suspends, it does not destroy");
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(2100),
            "destroy deadline = effective_suspend_at + retention_s (full retention after suspension)"
        );

        // Retention actually elapses before destroy: still SUSPENDED just before 2100...
        let rep = r.reconcile_tick(2099).await.unwrap();
        assert_eq!(rep.terminated, 0);
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
        // ...and TERMINATED at the credited retention end.
        clock.set(2100);
        let rep = r.reconcile_tick(clock.now()).await.unwrap();
        assert_eq!(rep.terminated, 1, "destroys at the credited retention end");
        assert_eq!(sub_state(&store, "s1").await, "TERMINATED");
    }

    // FIX 3 (§6.5, lnrent-7fp.22): an outage lying WHOLLY INSIDE the renewal window — neither
    // soft_date nor paid_through falls in [last, now] — still OVERLAPS the renewal window, so it must
    // be credited (the reduced amount), not silently dropped by an endpoint-only BETWEEN filter.
    #[tokio::test]
    async fn interior_outage_overlapping_window_is_credited() {
        let store = mem_store();
        // renew_lead=100, soft_date=900, paid_through=1000; outage [950, 990] lies wholly inside the
        // renewal window [900, 1000] — neither 900 nor 1000 is in [950, 990].
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 500, 1000).await;
        set_heartbeat(&store, 950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(990).await.unwrap();
        assert_eq!(
            credited, 1,
            "an interior outage overlaps the window and IS credited"
        );
        // pre_available = clamp(950-900,0,100)=50; target = 990 + (100-50) = 1040 (the reduced credit
        // for the partially-elapsed lead).
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1040));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE id='s1' AND paid_through=1000"
            )
            .await,
            1,
            "paid_through is never moved by the credit"
        );
    }

    // FIX 4 (§6.5, lnrent-7fp.22): the per-tick heartbeat upsert is MONOTONIC. A tick whose
    // wall-clock `now` moved BACKWARD must not regress last_heartbeat, else a later restart could
    // re-credit an interval the daemon was actually alive for.
    #[tokio::test]
    async fn per_tick_heartbeat_is_monotonic() {
        let store = mem_store();
        let r = reconciler(store.clone(), dummy_recipe());

        // A normal tick stamps the heartbeat forward.
        r.reconcile_tick(1000).await.unwrap();
        assert_eq!(heartbeat(&store).await, Some(1000));

        // A backward `now` does NOT pull the heartbeat back.
        r.reconcile_tick(900).await.unwrap();
        assert_eq!(
            heartbeat(&store).await,
            Some(1000),
            "a backward now does not regress last_heartbeat"
        );

        // Moving forward again advances it normally.
        r.reconcile_tick(1100).await.unwrap();
        assert_eq!(heartbeat(&store).await, Some(1100));
    }

    // FIX B (§6.5, lnrent-7fp.22): the soft-date auto-reminder invoice's expiry must cover the
    // CREDITED boundary B = effective_suspend_at + retention_s, not the stale paid_through +
    // retention_s. After a long credited outage the freshly-minted auto-invoice would otherwise
    // expire before B, leaving the credited window unusable for payment.
    #[tokio::test]
    async fn soft_reminder_invoice_expiry_covers_credited_boundary() {
        let store = mem_store();
        // renew_lead=100, soft_date(cursor)=900, paid_through=1000, retention=500. A large credited
        // floor (5000) -> effective_suspend_at = max(1000,5000) = 5000 -> B = 5000+500 = 5500. The
        // raw boundary paid_through+retention_s = 1500 is far short of B.
        seed_active_sub(&store, "s1", "buyerhex", 100, 900, 1000, 500, 900).await;
        set_suspend_not_before(&store, "s1", 5000).await;
        let payment = Arc::new(crate::backends::MockPayment::new());
        let now = 900;
        payment.set_now(now); // mock stamps expires_at = now + expiry_s
        let r = Reconciler::new(store.clone(), payment, dummy_recipe());

        let rep = r.reconcile_tick(now).await.unwrap();
        assert_eq!(
            rep.reminded, 1,
            "the credited sub still fires its soft reminder"
        );

        // The invoice is keyed on the UNCHANGED paid_through, but its expiry covers B (5500).
        let b = 5000 + 500;
        let expires_at = inv_expires_at(&store, "renew:auto:s1:1000")
            .await
            .expect("renewal invoice has an expiry");
        assert!(
            expires_at >= b,
            "auto-invoice expiry {expires_at} must reach the credited boundary B={b} (raw \
             paid_through+retention_s=1500 would have it expire at 4500, before B)"
        );
        // Cursor advanced to the credited floor, as the cascade requires (state stays ACTIVE).
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(5000));
        assert_eq!(sub_state(&store, "s1").await, "ACTIVE");
    }

    // FIX C (§6.5, lnrent-7fp.22): a no-op credit — the computed floor AND cursor already equal the
    // stored values — still matches the CAS row, but it moves nothing, so it must NOT inflate the
    // credited count or append a `downtime_credit` journal row.
    #[tokio::test]
    async fn noop_credit_does_not_increment_count_or_journal() {
        let store = mem_store();
        // Already-credited, post-reminder sub: floor=2100, cursor advanced to the floor (2100).
        // renew_lead=100, soft_date=1900, paid_through=2000, retention=500.
        seed_active_sub(&store, "s1", "buyer", 100, 1900, 2000, 500, 2100).await;
        set_suspend_not_before(&store, "s1", 2100).await;
        // A clean restart (no real new outage): last alive 1950, restart at 1960. The candidate query
        // still selects it (soft_date 1900 <= 1960, paid_through 2000 >= 1950), but the math recomputes
        // the SAME floor: pre_available=clamp(1950-1900,0,100)=50; target=1960+(100-50)=2010;
        // new_floor = max(2100, 2000, 2010) = 2100 (== stored), new_cursor = 2100 (== stored cursor).
        set_heartbeat(&store, 1950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(1960).await.unwrap();
        assert_eq!(credited, 0, "a no-op credit is not counted");
        assert_eq!(
            sub_suspend_not_before(&store, "s1").await,
            Some(2100),
            "the floor is unchanged"
        );
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(2100),
            "the cursor is unchanged"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE subscription_id='s1' AND kind='downtime_credit'"
            )
            .await,
            0,
            "a no-op credit writes no downtime_credit journal row"
        );
        // The heartbeat still advances to the restart time (consumed exactly once).
        assert_eq!(heartbeat(&store).await, Some(1960));
    }

    // FIX 2 (§6.5, lnrent-7fp.22): a SECOND outage whose window overlaps an already-credited window
    // must add only its NON-overlapping operator-down time, so the buyer still gets a full renew_lead
    // of ONLINE time before suspension. Before this fix the credit SELECT dropped the sub as soon as
    // `last > paid_through`, so the second outage consumed previously-credited availability and the
    // sub could suspend after LESS than renew_lead of online time.
    #[tokio::test]
    async fn repeat_outage_credits_non_overlapping_downtime() {
        let store = mem_store();
        // renew_lead=100, soft_date=900, paid_through=1000, retention=500; post-reminder cursor at
        // paid_through.
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 500, 1000).await;
        let r = reconciler(store.clone(), dummy_recipe());

        // First outage: alive through 950 (50s of lead seen online) -> restart at 1100.
        set_heartbeat(&store, 950).await;
        assert_eq!(r.apply_restart_downtime_credit(1100).await.unwrap(), 1);
        // pre_available = clamp(950-900,0,100)=50; target = 1100 + (100-50) = 1150.
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1150));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1150));

        // The daemon then ran online until 1120 (20s more lead seen: 70s total) before a SECOND
        // outage, and restarts at 1200.
        set_heartbeat(&store, 1120).await;
        assert_eq!(
            r.apply_restart_downtime_credit(1200).await.unwrap(),
            1,
            "the second outage is still credited (selected via the EFFECTIVE floor, not paid_through)"
        );
        // window_start = max(900, 1150-100=1050)=1050; pre_available = clamp(1120-1050,0,100)=70;
        // target = 1200 + (100-70) = 1230. The floor advances by exactly the second outage's
        // operator-down time (1200-1120 = 80): 1150 + 80 = 1230. The buyer thus still gets a full
        // renew_lead (100s) of ONLINE time before suspension: 50 (900-950) + 20 (1100-1120) +
        // 30 (1200-1230) = 100.
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1230));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1230));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE id='s1' AND paid_through=1000"
            )
            .await,
            1,
            "paid_through is never moved by either credit"
        );
    }

    // FIX 4 (§6.5, lnrent-7fp.22): a credit whose computed new_floor == paid_through (here a restart
    // exactly at soft_date with the outage starting before it) leaves the EFFECTIVE suspend boundary
    // and the cursor unchanged, so it must NOT flip suspend_not_before NULL->paid_through, write a
    // `downtime_credit` journal row, or inflate the credited count.
    #[tokio::test]
    async fn noop_credit_at_paid_through_floor_does_not_journal_or_count() {
        let store = mem_store();
        // renew_lead=100, soft_date=900, paid_through=1000; pre-reminder cursor at soft_date.
        seed_active_sub(&store, "s1", "buyer", 100, 900, 1000, 500, 900).await;
        // Outage starts BEFORE soft_date (last=850, no lead seen) and the restart lands exactly at
        // soft_date (now=900): pre_available = clamp(850-900,0,100)=0; target = 900 + 100 = 1000 =
        // paid_through; new_floor = max(1000,1000) = 1000 == paid_through (a zero-effect credit).
        set_heartbeat(&store, 850).await;
        let r = reconciler(store.clone(), dummy_recipe());

        let credited = r.apply_restart_downtime_credit(900).await.unwrap();
        assert_eq!(credited, 0, "a new_floor == paid_through credit is a no-op");
        assert_eq!(
            sub_suspend_not_before(&store, "s1").await,
            None,
            "suspend_not_before is NOT flipped NULL -> paid_through"
        );
        assert_eq!(
            sub_next_deadline(&store, "s1").await,
            Some(900),
            "the cursor is left at soft_date"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE subscription_id='s1' AND kind='downtime_credit'"
            )
            .await,
            0,
            "a no-op credit writes no downtime_credit journal row"
        );
        // The heartbeat still advances to the restart time (consumed exactly once).
        assert_eq!(heartbeat(&store).await, Some(900));
    }

    #[tokio::test]
    async fn suspended_retention_overlap_extends_destroy_boundary_not_destroyed_on_boot() {
        let store = mem_store();
        // SUSPENDED in retention: E=paid_through=1000, B=1500. Outage [1200,1600] consumes 300s of
        // retention, so the credited destroy boundary is restart + remaining = 1600+300 = 1900.
        seed_sub(
            &store,
            "s1",
            "SUSPENDED",
            "buyer",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        set_heartbeat(&store, 1200).await;
        let r = reconciler(store.clone(), dummy_recipe());

        assert_eq!(r.apply_restart_downtime_credit(1600).await.unwrap(), 1);
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1400));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1900));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE id='s1' AND paid_through=1000"
            )
            .await,
            1,
            "paid_through is never moved by SUSPENDED downtime credit"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM invoice WHERE kind='renewal'").await,
            0,
            "the credit path does not mint renewal invoices"
        );

        let rep = r.reconcile_tick(1600).await.unwrap();
        assert_eq!(rep.terminated, 0, "boot catch-up does not destroy early");
        assert_eq!(sub_state(&store, "s1").await, "SUSPENDED");
    }

    #[tokio::test]
    async fn suspended_credit_skips_outage_before_retention_and_credits_partial_overlap() {
        let store = mem_store();
        // For s-before, outage [500,900] is wholly before E=1000 and must be a no-op.
        seed_sub(
            &store,
            "s-before",
            "SUSPENDED",
            "buyer",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        // For s-partial, E=800, B=1300. The same outage overlaps retention only from 800..900, so
        // B extends by exactly that 100s overlap to 1400.
        seed_sub(
            &store,
            "s-partial",
            "SUSPENDED",
            "buyer",
            Some(800),
            500,
            Some(1300),
        )
        .await;
        set_heartbeat(&store, 500).await;
        let r = reconciler(store.clone(), dummy_recipe());

        assert_eq!(r.apply_restart_downtime_credit(900).await.unwrap(), 1);
        assert_eq!(sub_suspend_not_before(&store, "s-before").await, None);
        assert_eq!(sub_next_deadline(&store, "s-before").await, Some(1500));
        assert_eq!(sub_suspend_not_before(&store, "s-partial").await, Some(900));
        assert_eq!(sub_next_deadline(&store, "s-partial").await, Some(1400));
    }

    #[tokio::test]
    async fn suspended_retention_already_ended_before_outage_is_not_credited_and_destroys() {
        let store = mem_store();
        // E=1000, B=1500. With last=1500, retention was already gone when the outage began, so the
        // anti-resurrection gate skips credit and normal destroy catch-up terminates the sub.
        seed_sub(
            &store,
            "s1",
            "SUSPENDED",
            "buyer",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        set_heartbeat(&store, 1500).await;
        let r = reconciler(store.clone(), dummy_recipe());

        assert_eq!(r.apply_restart_downtime_credit(1700).await.unwrap(), 0);
        assert_eq!(sub_suspend_not_before(&store, "s1").await, None);
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1500));

        let rep = r.reconcile_tick(1700).await.unwrap();
        assert_eq!(rep.terminated, 1);
        assert_eq!(sub_state(&store, "s1").await, "TERMINATED");
    }

    #[tokio::test]
    async fn suspended_credit_is_idempotent_and_monotonic_across_outages() {
        let store = mem_store();
        seed_sub(
            &store,
            "s1",
            "SUSPENDED",
            "buyer",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        let r = reconciler(store.clone(), dummy_recipe());

        set_heartbeat(&store, 1200).await;
        assert_eq!(r.apply_restart_downtime_credit(1600).await.unwrap(), 1);
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1400));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1900));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE subscription_id='s1' AND kind='downtime_credit'"
            )
            .await,
            1
        );

        assert_eq!(
            r.apply_restart_downtime_credit(1600).await.unwrap(),
            0,
            "same restart heartbeat is already consumed"
        );
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1400));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1900));

        set_heartbeat(&store, 1650).await;
        assert_eq!(r.apply_restart_downtime_credit(1700).await.unwrap(), 1);
        assert_eq!(
            sub_suspend_not_before(&store, "s1").await,
            Some(1450),
            "a later outage moves the floor forward, never backward"
        );
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1950));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE subscription_id='s1' AND kind='downtime_credit'"
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn paid_renewal_of_credited_suspended_sub_clears_floor() {
        let store = mem_store();
        seed_sub(
            &store,
            "s1",
            "SUSPENDED",
            "buyer",
            Some(1000),
            500,
            Some(1500),
        )
        .await;
        set_heartbeat(&store, 1200).await;
        let r = reconciler(store.clone(), dummy_recipe());
        assert_eq!(r.apply_restart_downtime_credit(1600).await.unwrap(), 1);
        assert_eq!(sub_suspend_not_before(&store, "s1").await, Some(1400));
        assert_eq!(sub_next_deadline(&store, "s1").await, Some(1900));

        seed_invoice(
            &store,
            "inv-renew-s1",
            "s1",
            "renew:auto:s1:1000",
            "renewal",
            "OPEN",
            None,
        )
        .await;
        assert_eq!(
            crate::capture::capture(&store, settlement("renew:auto:s1:1000", 1700), 1700)
                .await
                .unwrap(),
            crate::capture::Capture::Resumed
        );

        let (state, paid_through, floor): (String, i64, Option<i64>) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state, paid_through, suspend_not_before FROM subscription WHERE id='s1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(state, "RESUMING");
        assert_eq!(paid_through, 1800);
        assert_eq!(floor, None, "the paid renewal consumed the credit floor");
    }

    #[tokio::test]
    async fn active_credit_regression_and_resuming_cancelled_not_credited() {
        let store = mem_store();
        seed_active_sub(&store, "active", "buyer", 100, 900, 1000, 500, 1000).await;
        for (id, state) in [("resuming", "RESUMING"), ("cancelled", "CANCELLED")] {
            seed_sub(&store, id, state, "buyer", Some(1000), 500, Some(1500)).await;
        }
        set_heartbeat(&store, 950).await;
        let r = reconciler(store.clone(), dummy_recipe());

        assert_eq!(r.apply_restart_downtime_credit(1100).await.unwrap(), 1);
        assert_eq!(sub_suspend_not_before(&store, "active").await, Some(1150));
        assert_eq!(sub_next_deadline(&store, "active").await, Some(1150));
        for id in ["resuming", "cancelled"] {
            assert_eq!(sub_suspend_not_before(&store, id).await, None);
            assert_eq!(sub_next_deadline(&store, id).await, Some(1500));
            assert_eq!(
                count(
                    &store,
                    &format!(
                        "SELECT count(*) FROM event_log WHERE subscription_id='{id}' AND kind='downtime_credit'"
                    )
                )
                .await,
                0
            );
        }
    }
}
