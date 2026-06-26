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
//! Scope carve-out: §6.5's downtime credit — shifting these deadlines forward by an operator-outage
//! window so a buyer is never suspended for the operator's downtime (ADR-0005) — is a SEPARATE bead
//! (lnrent-7fp.22) and is NOT built here; reconcile fires purely on wall-clock `now`. The supervisor
//! (lnrent-7fp.21) MUST land .22 before wiring this live, or an operator outage would wrongly
//! suspend/terminate buyers.
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

use crate::backends::{PaymentBackend, PaymentStatus};
use crate::recipe::Recipe;
use crate::reservation;
use crate::runner::{run_hook, DEFAULT_TIMEOUT};
use crate::store::Store;

/// FLOOR for the soft-date auto-renewal invoice's Lightning expiry (seconds). The actual expiry is
/// sized to the renewal WINDOW — from soft_date through the resumable boundary `paid_through +
/// retention_s` — so the proactively-issued invoice is payable for its whole advertised window (a
/// renewal has no capacity-reservation hold, unlike a first order, so the 1h order horizon does NOT
/// apply). This floor only guards a degenerate too-small window. An internal default, not a knob.
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
}

/// A subscription whose deadline cursor is due this tick (`next_deadline <= now`).
struct DueSub {
    id: String,
    state: String,
    buyer_pubkey: String,
    paid_through: Option<i64>,
    retention_s: i64,
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
                    // Cursor before paid_through => it sits at soft_date: the renewal reminder.
                    Some(pt) if d.next_deadline < pt => {
                        if self
                            .fire_soft_reminder(
                                &d.id,
                                &d.buyer_pubkey,
                                pt,
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
                    }
                    // Cursor at/after paid_through: the unpaid grace ran out — suspend.
                    Some(pt) => {
                        if self
                            .fire_suspend(
                                &d.id,
                                &d.buyer_pubkey,
                                d.next_deadline,
                                pt + d.retention_s,
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

        Ok(report)
    }

    /// Read every subscription whose cursor is due. NULL cursors are excluded — a cleared cursor is
    /// a terminal/settled sub with nothing for reconcile to fire.
    async fn due_subscriptions(&self, now: i64) -> Result<Vec<DueSub>> {
        self.store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id, state, buyer_pubkey, paid_through, retention_s, next_deadline
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
        match self.payment.lookup(&invoice_id) {
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
            match self.payment.lookup(&invoice_id) {
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

    async fn run_lifecycle_hook(&self, hook: &str, sub_id: &str, buyer_hex: &str) -> Result<()> {
        let input = self.lifecycle_hook_input(sub_id, buyer_hex).await?;
        if let Err(e) = run_hook(&self.recipe.hook(hook), &input, DEFAULT_TIMEOUT).await {
            tracing::warn!(
                sub = %sub_id,
                hook,
                error = %e,
                "reconcile lifecycle hook failed (non-fatal)"
            );
        }
        Ok(())
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
    /// rows. `paid_through` is NOT extended here — that is capture's, on settlement.
    async fn fire_soft_reminder(
        &self,
        sub_id: &str,
        buyer_hex: &str,
        paid_through: i64,
        retention_s: i64,
        cursor_nd: i64,
        now: i64,
    ) -> Result<bool> {
        let external_id = format!("renew:auto:{sub_id}:{paid_through}");
        // Size the expiry to the renewal WINDOW: payable from now (soft_date) through the resumable
        // boundary paid_through + retention_s (after which capture would refund a settlement anyway —
        // the inclusive retention gate in .8), floored at RENEWAL_INVOICE_MIN_EXPIRY_S.
        let expiry_s = u32::try_from(
            (paid_through + retention_s - now).max(i64::from(RENEWAL_INVOICE_MIN_EXPIRY_S)),
        )
        .unwrap_or(u32::MAX);
        // Idempotent on external_id: a re-fire (or crash-retry) reuses the same invoice rather than
        // minting a second one. On a stale cursor the CAS below affects 0 rows and we never insert a
        // DB invoice row; the backend invoice minted just above is harmless (same idempotent
        // external_id, self-expiring), so no DB row is ever stranded.
        let invoice = match self.payment.create_invoice(
            self.recipe.pricing.amount_sat,
            &format!("lnrent renewal {sub_id}"),
            expiry_s,
            &external_id,
        ) {
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

    /// Transition 3 — suspend. First verify the cursor is still due, then run the recipe `suspend`
    /// hook best-effort before the state/cursor move; a crash during the hook leaves the same due
    /// cursor for the next tick. Hook failure is logged and non-fatal per this bead's scope. The
    /// durable move remains a CAS guarded on `(state='ACTIVE', next_deadline=nd)`: ACTIVE
    /// `-> SUSPENDED`, cursor `-> retention end`, and a `billing.notice` enqueued in one txn.
    async fn fire_suspend(
        &self,
        sub_id: &str,
        buyer_hex: &str,
        nd: i64,
        retention_end: i64,
        now: i64,
    ) -> Result<bool> {
        if !self.subscription_matches(sub_id, "ACTIVE", nd).await? {
            return Ok(false);
        }
        // A renewal paid at the backend but not yet captured will extend paid_through and move this
        // deadline — don't suspend a timely-paid sub out from under the pending capture (codex P1).
        if self.renewal_settlement_pending(sub_id).await? {
            return Ok(false);
        }
        self.run_lifecycle_hook("suspend", sub_id, buyer_hex)
            .await?;

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
        self.run_lifecycle_hook("destroy", sub_id, buyer_hex)
            .await?;

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
            match self.payment.lookup(&inv_id) {
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
        // CAS: advance ACTIVE's cursor from soft_date to paid_through. State stays ACTIVE.
        let n = tx.execute(
            "UPDATE subscription SET next_deadline=?2, updated_at=?3
             WHERE id=?1 AND state='ACTIVE' AND next_deadline=?4",
            params![self.sub_id, self.paid_through, self.now, self.cursor_nd],
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
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
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
        assert_eq!(rep.suspended, 0, "a paid-pending renewal defers the suspend");
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
}
