//! Capture-then-refund EXECUTOR (lnrent-7fp.11, SPEC.md §6.6 / ADR-0009). Upstream (capture,
//! provision) only DETECTS a refund: it writes a durable `PENDING` `refund_attempt` row keyed by a
//! UNIQUE `idempotency_key` (`refund:<external_id>`). This module DRAINS those rows — it pays the
//! refund and advances the ledger — and nothing else creates them.
//!
//! Crash-safety is the whole point and it rests on one fact: `payment.pay` is **idempotent on the
//! key**, so retrying `pay(key)` after a crash NEVER double-pays. The intent is already persisted
//! (PENDING), so [`Refunder::drive`] just re-pays every PENDING row and records the outcome in ONE
//! CAS-guarded transaction. That makes `drive` itself the restart-recovery path — calling it again
//! is always safe — so there is no separate `recover()`. The daemon supervisor (lnrent-7fp.21) calls
//! `drive` at boot and on an interval; this module only exposes the function.
//!
//! Per PENDING row: an optional fast-skip (`payment_status_by_key` already `Succeeded` => a prior
//! attempt paid; record SENT without paying again), else `pay`. On success: `refund_attempt -> SENT`
//! and, only when the sub is still `REFUND_DUE` (the provision-failure path), CAS it to `REFUNDED`
//! and release its still-HELD reservation in the SAME txn; a settled-but-terminal sub is already
//! terminal with its hold released, so it is left untouched. A `pay` error can be ambiguous (an
//! in-flight Lightning timeout), so it is re-checked against the key — if the refund actually
//! settled it is recorded SENT, and only a definite backend `Failed` status can consume the capped
//! failure path. `Pending`/`Unknown` stays recoverable. Once definitive failures reach
//! [`MAX_REFUND_ATTEMPTS`], the row goes `FAILED`, a failed `billing.refund` DM is enqueued, and an
//! operator-loud error is logged — a stuck refund is surfaced, never hidden. Every `billing.refund`
//! DM is ENQUEUED as a `PENDING` outbox row under a STABLE id (the OutboxSender, lnrent-7fp.10,
//! publishes it); a re-drive never double-enqueues.

use std::sync::Arc;

use anyhow::Result;
use rusqlite::{params, Transaction};

use lnrent_wire::{BillingRefund, Msg};

use crate::backends::{PayStatus, PaymentBackend};
use crate::clock::Clock;
use crate::reservation;
use crate::store::Store;

/// Cap on outbound `pay` attempts before a refund is parked as `FAILED` and surfaced to the
/// operator. A small bound: a refund that can't be sent in this many tries needs a human, not an
/// unbounded retry loop. An internal default, not a knob.
const MAX_REFUND_ATTEMPTS: i64 = 5;

/// What one [`Refunder::drive`] did. Every count is a normal result, not an error; the supervisor
/// (lnrent-7fp.21) can log it and tests assert on rows directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RefundReport {
    /// Refunds that reached `SENT` this drive (paid, or confirmed already-paid via the fast-skip).
    pub sent: usize,
    /// `pay` failed but the row stays `PENDING` (attempts bumped) for the next drive to retry.
    pub retried: usize,
    /// Refunds that hit [`MAX_REFUND_ATTEMPTS`] and were parked `FAILED` with a loud error log.
    pub failed: usize,
}

/// Drains PENDING `refund_attempt` rows. Holds the injected seams (the same store + payment the rest
/// of the money path uses, plus a clock); the supervisor (lnrent-7fp.21) constructs it and calls
/// [`Refunder::drive`].
pub struct Refunder {
    store: Store,
    payment: Arc<dyn PaymentBackend>,
    clock: Arc<dyn Clock>,
}

/// One PENDING `refund_attempt` row to drain (just the fields the executor needs). `recipient` is the
/// buyer's hex pubkey, read from the subscription for the `billing.refund` DM (the OutboxSender
/// parses it).
struct RefundRow {
    id: String,
    subscription_id: Option<String>,
    dest: Option<String>,
    amount_sat: Option<i64>,
    idempotency_key: String,
    recipient: Option<String>,
}

/// The per-row result, mapped 1:1 onto a [`RefundReport`] counter.
enum Outcome {
    Sent,
    Retried,
    Failed,
    Noop,
}

impl Refunder {
    pub fn new(store: Store, payment: Arc<dyn PaymentBackend>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            payment,
            clock,
        }
    }

    /// Process every non-terminal (`PENDING`) `refund_attempt` row. Idempotent and safe to call
    /// repeatedly: it IS the restart-recovery path (retrying `pay(key)` never double-pays, the
    /// success/failure bookkeeping is a single CAS-guarded txn, and the stable outbox id prevents a
    /// double-enqueue).
    pub async fn drive(&self) -> Result<RefundReport> {
        let mut report = RefundReport::default();
        for row in self.pending_refunds().await? {
            match self.process(row).await? {
                Outcome::Sent => report.sent += 1,
                Outcome::Retried => report.retried += 1,
                Outcome::Failed => report.failed += 1,
                Outcome::Noop => {}
            }
        }
        Ok(report)
    }

    /// Pay (or confirm already-paid) one refund and record the outcome.
    async fn process(&self, row: RefundRow) -> Result<Outcome> {
        let now = self.clock.now();
        // NULL amount is tolerated (provision records NULL when the order invoice had no amount):
        // refund 0 rather than panic. A negative figure is clamped for the same reason.
        let amount = row.amount_sat.unwrap_or(0).max(0) as u64;
        // Fast-skip: a prior attempt already paid on this key (e.g. a crash after pay but before the
        // SENT bookkeeping committed). Record SENT WITHOUT paying again. Only `Succeeded` skips — an
        // Unknown/Pending/Failed (or a lookup error) falls through to `pay`, which the key dedups.
        if self.already_paid(&row.idempotency_key) {
            // succeeded on a prior attempt; the backend payment id is unknown without re-paying.
            return self.finish_sent(&row, None, amount, now).await;
        }

        let Some(dest) = row.dest.as_deref().filter(|d| !d.is_empty()) else {
            tracing::error!(refund = %row.id, "refund has no destination; parking FAILED");
            return self.commit_structural_failure(&row, amount, now).await;
        };

        match self.payment.pay(dest, amount, &row.idempotency_key) {
            Ok(backend_payment_id) => {
                self.finish_sent(&row, Some(backend_payment_id), amount, now)
                    .await
            }
            Err(e) => match self.status_by_key_after_error(&row.idempotency_key) {
                PayStatus::Succeeded => self.finish_sent(&row, None, amount, now).await,
                PayStatus::Failed => {
                    tracing::warn!(
                        refund = %row.id,
                        error = %e,
                        "refund pay failed definitively; row stays PENDING until retry cap"
                    );
                    self.commit_pay_failure(&row, amount, now, true).await
                }
                status @ (PayStatus::Pending | PayStatus::Unknown) => {
                    tracing::warn!(
                        refund = %row.id,
                        error = %e,
                        ?status,
                        "refund pay failed ambiguously; row stays PENDING for recovery"
                    );
                    self.commit_pay_failure(&row, amount, now, false).await
                }
            },
        }
    }

    /// `Succeeded` per the backend's idempotency-key status — the refund already went out on this
    /// key. Only `Succeeded` counts as paid; an `Unknown`/`Pending`/`Failed` (or a lookup error) is
    /// not, so the caller falls through to `pay`, which the key dedups.
    fn already_paid(&self, idempotency_key: &str) -> bool {
        matches!(
            self.payment.payment_status_by_key(idempotency_key),
            Ok(PayStatus::Succeeded)
        )
    }

    /// Re-check the key after a `pay` error. Lookup errors are treated as `Unknown`: terminalizing
    /// while the backend cannot answer is unsafe because the payment may still settle later.
    fn status_by_key_after_error(&self, idempotency_key: &str) -> PayStatus {
        match self.payment.payment_status_by_key(idempotency_key) {
            Ok(status) => status,
            Err(e) => {
                tracing::warn!(
                    idempotency_key,
                    error = %e,
                    "refund status lookup failed after pay error"
                );
                PayStatus::Unknown
            }
        }
    }

    /// Commit the SENT bookkeeping and map the CAS outcome to a counter (a lost race is a `Noop`).
    async fn finish_sent(
        &self,
        row: &RefundRow,
        backend_payment_id: Option<String>,
        amount: u64,
        now: i64,
    ) -> Result<Outcome> {
        if self
            .commit_sent(row, backend_payment_id, amount, now)
            .await?
        {
            Ok(Outcome::Sent)
        } else {
            Ok(Outcome::Noop)
        }
    }

    /// SUCCESS bookkeeping in ONE txn: mark the row `SENT`; if the sub is still `REFUND_DUE` (the
    /// provision-failure path) CAS it to `REFUNDED` and release its still-HELD reservation in the
    /// same txn (a settled-but-terminal sub is already terminal with its hold released — left
    /// untouched); enqueue the `billing.refund` "sent" DM under a stable id; journal.
    async fn commit_sent(
        &self,
        row: &RefundRow,
        backend_payment_id: Option<String>,
        amount: u64,
        now: i64,
    ) -> Result<bool> {
        let external_id = external_id_of(row);
        let outbox_id = format!("outbox:refund:{external_id}");
        let payload = serde_json::to_string(&Msg::BillingRefund(BillingRefund {
            subscription_id: row.subscription_id.clone().unwrap_or_default(),
            amount_sat: amount,
            status: "sent".to_string(),
        }))?;
        let id = row.id.clone();
        let sub_id = row.subscription_id.clone();
        let recipient = row.recipient.clone();
        self.store
            .transaction(move |tx| {
                // COALESCE keeps any id a prior attempt already persisted when the fast-skip has none.
                let updated = tx.execute(
                    "UPDATE refund_attempt
                       SET status='SENT', backend_payment_id=COALESCE(?2, backend_payment_id),
                           attempts=COALESCE(attempts, 0)+1, updated_at=?3
                     WHERE id=?1 AND status='PENDING'",
                    params![id, backend_payment_id, now],
                )?;
                if updated == 0 {
                    return Ok(false);
                }
                if let Some(sub_id) = sub_id.as_deref() {
                    let moved = tx.execute(
                        "UPDATE subscription SET state='REFUNDED', updated_at=?2
                         WHERE id=?1 AND state='REFUND_DUE'",
                        params![sub_id, now],
                    )?;
                    // Only the provision-failure path (REFUND_DUE) still holds a reservation; release
                    // it in the SAME txn. A terminal sub's hold was already released by reconcile.
                    if moved > 0 {
                        reservation::release_txn(tx, sub_id, now)?;
                    }
                }
                enqueue_refund(
                    tx,
                    &outbox_id,
                    recipient.as_deref(),
                    sub_id.as_deref(),
                    &payload,
                    now,
                )?;
                journal(tx, sub_id.as_deref(), "refund_sent", &external_id, now)?;
                Ok(true)
            })
            .await
    }

    /// Structurally unsendable rows (e.g. missing destination) are parked `FAILED` immediately. No
    /// payment was attempted, the sub is left in `REFUND_DUE`, and the reservation is NOT released.
    async fn commit_structural_failure(
        &self,
        row: &RefundRow,
        amount: u64,
        now: i64,
    ) -> Result<Outcome> {
        let external_id = external_id_of(row);
        let id = row.id.clone();
        let sub_id = row.subscription_id.clone();
        let recipient = row.recipient.clone();
        let outbox_id = format!("outbox:refund:{external_id}");
        let payload = serde_json::to_string(&Msg::BillingRefund(BillingRefund {
            subscription_id: sub_id.clone().unwrap_or_default(),
            amount_sat: amount,
            status: "failed".to_string(),
        }))?;
        let outcome = self
            .store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE refund_attempt
                        SET status='FAILED', attempts=COALESCE(attempts, 0)+1, updated_at=?2
                      WHERE id=?1 AND status='PENDING'",
                    params![id, now],
                )?;
                if updated == 0 {
                    return Ok(Outcome::Noop);
                }
                enqueue_failed_refund(
                    tx,
                    recipient.as_deref(),
                    sub_id.as_deref(),
                    &outbox_id,
                    &payload,
                    &external_id,
                    now,
                )?;
                Ok(Outcome::Failed)
            })
            .await?;
        if matches!(outcome, Outcome::Failed) {
            tracing::error!(
                refund = %row.id,
                subscription = row.subscription_id.as_deref().unwrap_or(""),
                "refund parked FAILED because it has no destination"
            );
        }
        Ok(outcome)
    }

    /// FAILURE bookkeeping in ONE txn: bump `attempts` (status STAYS `PENDING` so the next drive
    /// retries — `pay(key)` is always safe). Only definitive backend failures are allowed to park
    /// the row `FAILED` at [`MAX_REFUND_ATTEMPTS`]; ambiguous `Pending`/`Unknown` statuses stay
    /// recoverable because they may settle after this drive returns.
    async fn commit_pay_failure(
        &self,
        row: &RefundRow,
        amount: u64,
        now: i64,
        allow_terminal: bool,
    ) -> Result<Outcome> {
        let external_id = external_id_of(row);
        let id = row.id.clone();
        let sub_id = row.subscription_id.clone();
        let recipient = row.recipient.clone();
        let outbox_id = format!("outbox:refund:{external_id}");
        let payload = serde_json::to_string(&Msg::BillingRefund(BillingRefund {
            subscription_id: sub_id.clone().unwrap_or_default(),
            amount_sat: amount,
            status: "failed".to_string(),
        }))?;
        let outcome = self
            .store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE refund_attempt
                        SET attempts=COALESCE(attempts, 0)+1, updated_at=?2
                      WHERE id=?1 AND status='PENDING'",
                    params![id, now],
                )?;
                if updated == 0 {
                    return Ok(Outcome::Noop);
                }

                let attempts: i64 = tx.query_row(
                    "SELECT COALESCE(attempts, 0) FROM refund_attempt WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?;
                if !allow_terminal || attempts < MAX_REFUND_ATTEMPTS {
                    journal(tx, sub_id.as_deref(), "refund_retry", &external_id, now)?;
                    return Ok(Outcome::Retried);
                }

                let updated = tx.execute(
                    "UPDATE refund_attempt SET status='FAILED', updated_at=?2
                     WHERE id=?1 AND status='PENDING'",
                    params![id, now],
                )?;
                if updated == 0 {
                    return Ok(Outcome::Noop);
                }
                enqueue_failed_refund(
                    tx,
                    recipient.as_deref(),
                    sub_id.as_deref(),
                    &outbox_id,
                    &payload,
                    &external_id,
                    now,
                )?;
                Ok(Outcome::Failed)
            })
            .await?;
        if matches!(outcome, Outcome::Failed) {
            tracing::error!(
                refund = %row.id,
                subscription = row.subscription_id.as_deref().unwrap_or(""),
                attempts = MAX_REFUND_ATTEMPTS,
                "refund parked FAILED after exhausting retry attempts"
            );
        }
        Ok(outcome)
    }

    /// Every PENDING refund row, with the buyer's pubkey joined in for the DM recipient. A missing
    /// sub leaves the recipient NULL (an orphan refund the OutboxSender will quarantine — not this
    /// module's concern).
    async fn pending_refunds(&self) -> Result<Vec<RefundRow>> {
        self.store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT r.id, r.subscription_id, r.dest, r.amount_sat, r.idempotency_key,
                            s.buyer_pubkey
                       FROM refund_attempt r
                       LEFT JOIN subscription s ON s.id = r.subscription_id
                      WHERE r.status='PENDING'
                      ORDER BY r.created_at, r.id",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(RefundRow {
                            id: r.get(0)?,
                            subscription_id: r.get(1)?,
                            dest: r.get(2)?,
                            amount_sat: r.get(3)?,
                            idempotency_key: r.get(4)?,
                            recipient: r.get(5)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }
}

/// The stable `external_id` the row was keyed on — strip `refund:` off the idempotency key (or
/// `ref-` off the id). It anchors the deterministic outbox id so a re-drive never double-enqueues.
fn external_id_of(row: &RefundRow) -> String {
    if let Some(ext) = row.idempotency_key.strip_prefix("refund:") {
        return ext.to_string();
    }
    row.id.strip_prefix("ref-").unwrap_or(&row.id).to_string()
}

/// Enqueue a `billing.refund` DM as a PENDING outbox row under a STABLE id (the OutboxSender
/// publishes it; ENQUEUE only). `ON CONFLICT(id) DO NOTHING` makes a re-drive idempotent.
fn enqueue_refund(
    tx: &Transaction,
    id: &str,
    recipient: Option<&str>,
    sub_id: Option<&str>,
    payload_json: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox
            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
         VALUES (?1, ?2, ?3, 'billing.refund', ?4, 'PENDING', 0, ?5)
         ON CONFLICT(id) DO NOTHING",
        params![id, recipient, sub_id, payload_json, now],
    )?;
    Ok(())
}

fn enqueue_failed_refund(
    tx: &Transaction,
    recipient: Option<&str>,
    sub_id: Option<&str>,
    outbox_id: &str,
    payload_json: &str,
    external_id: &str,
    now: i64,
) -> rusqlite::Result<()> {
    enqueue_refund(tx, outbox_id, recipient, sub_id, payload_json, now)?;
    journal(tx, sub_id, "refund_failed", external_id, now)?;
    Ok(())
}

/// Journal a refund-ledger event to `event_log` in the same txn (every mutation is journaled,
/// ADR-0001/§6.5).
fn journal(
    tx: &Transaction,
    sub_id: Option<&str>,
    kind: &str,
    external_id: &str,
    now: i64,
) -> rusqlite::Result<()> {
    let detail = serde_json::json!({ "external_id": external_id }).to_string();
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
        params![sub_id, kind, detail, now],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{Invoice, PaymentStatus, Settlement};
    use crate::clock::TestClock;
    use crate::store::{Store, SCHEMA};
    use rusqlite::{Connection, OptionalExtension};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    /// A configurable PaymentBackend for the refunder's tests. By default it dedups `pay` on the key
    /// (never pays twice) and counts calls, like `MockPayment`; `set_fail(true)` makes `pay` return
    /// `Err` with a definitive `Failed` status; tests can override that status to `Pending` or
    /// `Unknown` to model ambiguous in-flight errors. `mark_paid(key)` seeds a key as
    /// already-settled (the crash-after-pay fast-skip). Methods the refunder never calls are
    /// `unimplemented!()`. (MockPayment can't simulate a `pay` failure, and we must not edit
    /// backends.rs to add one.)
    #[derive(Default)]
    struct TestPayment {
        inner: Mutex<TestPayState>,
    }

    #[derive(Default)]
    struct TestPayState {
        paid: HashMap<String, String>, // idempotency_key -> backend payment id
        pay_calls: usize,
        seq: u64,
        fail: bool,
        failed_status: Option<PayStatus>,
        settle_then_fail: bool, // pay() records the settlement but returns Err (ambiguous timeout)
    }

    impl TestPayment {
        fn new() -> Self {
            Self::default()
        }
        fn set_fail(&self, fail: bool) {
            let mut st = self.inner.lock().unwrap();
            st.fail = fail;
            st.failed_status = fail.then_some(PayStatus::Failed);
        }
        fn set_failed_status(&self, status: PayStatus) {
            self.inner.lock().unwrap().failed_status = Some(status);
        }
        /// pay() settles the key (so `payment_status_by_key` -> Succeeded) but still returns Err —
        /// the ambiguous in-flight-timeout case where the refund went out yet `pay` reported failure.
        fn set_settle_then_fail(&self, settle_then_fail: bool) {
            self.inner.lock().unwrap().settle_then_fail = settle_then_fail;
        }
        fn pay_calls(&self) -> usize {
            self.inner.lock().unwrap().pay_calls
        }
        /// Simulate a refund that already settled on this key before a crash (no `pay` recorded).
        fn mark_paid(&self, key: &str) {
            let mut st = self.inner.lock().unwrap();
            let n = st.seq;
            st.seq += 1;
            st.paid.insert(key.to_string(), format!("test-pay-{n}"));
        }
    }

    impl PaymentBackend for TestPayment {
        fn pay(&self, _dest: &str, _amount_sat: u64, idempotency_key: &str) -> Result<String> {
            let mut st = self.inner.lock().unwrap();
            st.pay_calls += 1;
            if st.settle_then_fail {
                // The refund actually settled on the key, but pay() reports an error: a
                // re-query of the key must now read Succeeded so the row records SENT, not FAILED.
                if !st.paid.contains_key(idempotency_key) {
                    let n = st.seq;
                    st.seq += 1;
                    st.paid
                        .insert(idempotency_key.to_string(), format!("test-pay-{n}"));
                }
                anyhow::bail!("test backend: settled but pay reported an error");
            }
            if st.fail {
                anyhow::bail!("test backend: pay failed");
            }
            if let Some(pid) = st.paid.get(idempotency_key) {
                return Ok(pid.clone()); // idempotent on key -> never pays twice
            }
            let n = st.seq;
            st.seq += 1;
            let pid = format!("test-pay-{n}");
            st.paid.insert(idempotency_key.to_string(), pid.clone());
            Ok(pid)
        }
        fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
            let st = self.inner.lock().unwrap();
            Ok(if st.paid.contains_key(idempotency_key) {
                PayStatus::Succeeded
            } else {
                st.failed_status.unwrap_or(PayStatus::Unknown)
            })
        }
        fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("refunder never receives")
        }
        fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!("refunder never looks up invoices")
        }
        fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!("refunder checks by key, not id")
        }
        fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("refunder never watches")
        }
    }

    fn refunder(store: &Store, payment: &Arc<TestPayment>, clock: &TestClock) -> Refunder {
        Refunder::new(store.clone(), payment.clone(), Arc::new(clock.clone()))
    }

    // ---- seed + read helpers ------------------------------------------------

    async fn seed_sub(store: &Store, id: &str, state: &str, buyer: &str) {
        let (id, state, buyer) = (id.to_string(), state.to_string(), buyer.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, buyer_pubkey, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 0, 0)",
                    params![id, state, buyer],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Seed a PENDING refund_attempt row exactly as capture/provision would have written it upstream.
    async fn seed_refund(store: &Store, sub_id: &str, dest: Option<&str>, amount: Option<i64>) {
        let external_id = format!("order:{sub_id}");
        let (sub_id, dest) = (sub_id.to_string(), dest.map(|d| d.to_string()));
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO refund_attempt
                        (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts,
                         created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, 0, 0)",
                    params![
                        format!("ref-{external_id}"),
                        sub_id,
                        dest,
                        amount,
                        format!("refund:{external_id}"),
                    ],
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
                    "INSERT INTO reservation
                        (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                     VALUES (?1, ?2, '{\"cpu\":1}', '{\"count\":0}', 'HELD', 0, 0)",
                    params![format!("res-{order_id}"), order_id],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// `(status, attempts, backend_payment_id)` of a refund row.
    async fn refund_row(store: &Store, id: &str) -> (String, i64, Option<String>) {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT status, attempts, backend_payment_id FROM refund_attempt WHERE id=?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get::<_, Option<i64>>(1)?.unwrap_or(0),
                            r.get::<_, Option<String>>(2)?,
                        ))
                    },
                )?)
            })
            .await
            .unwrap()
    }

    async fn scalar(store: &Store, sql: &'static str) -> Option<String> {
        store
            .read(move |c| Ok(c.query_row(sql, [], |r| r.get(0)).optional()?))
            .await
            .unwrap()
    }

    /// Every `billing.refund` outbox payload, parsed back to its `status` ("sent"/"failed").
    async fn refund_outbox_statuses(store: &Store) -> Vec<String> {
        let payloads: Vec<String> = store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT payload_json FROM outbox WHERE msg_type='billing.refund' ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();
        payloads
            .iter()
            .map(|p| match serde_json::from_str::<Msg>(p).unwrap() {
                Msg::BillingRefund(b) => b.status,
                other => panic!("expected billing.refund, got {}", other.type_str()),
            })
            .collect()
    }

    // ---- tests --------------------------------------------------------------

    // 1. Provision-fail path: REFUND_DUE sub + PENDING refund + HELD reservation -> drive() -> refund
    //    SENT (backend id set), sub -> REFUNDED, reservation RELEASED, billing.refund(sent) enqueued.
    #[tokio::test]
    async fn provision_fail_refund_completes_and_releases() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        let (status, attempts, backend_id) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(attempts, 1);
        assert!(
            backend_id.is_some(),
            "backend_payment_id recorded from pay()"
        );
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUNDED".to_string())
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            Some("RELEASED".to_string()),
            "the still-HELD reservation is released in the same txn"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // 2. Refund send fails: fail-pay mode -> each drive leaves the row PENDING with attempts bumped
    //    and the sub still REFUND_DUE; after MAX_REFUND_ATTEMPTS drives -> FAILED + failed alert,
    //    sub STILL REFUND_DUE (a stuck refund is surfaced, not advanced).
    #[tokio::test]
    async fn repeated_pay_failure_parks_failed_and_alerts() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_fail(true);
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
        seed_reservation(&store, "sub-1").await;
        let r = refunder(&store, &payment, &clock);

        // Drives below the cap: PENDING, attempts climb, nothing enqueued, sub unchanged.
        for i in 1..MAX_REFUND_ATTEMPTS {
            let report = r.drive().await.unwrap();
            assert_eq!(report.retried, 1);
            let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
            assert_eq!(status, "PENDING");
            assert_eq!(attempts, i);
            assert_eq!(
                scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
                Some("REFUND_DUE".to_string())
            );
            assert!(refund_outbox_statuses(&store).await.is_empty());
        }

        // The drive that reaches the cap: FAILED + a single failed alert, sub still REFUND_DUE.
        let report = r.drive().await.unwrap();
        assert_eq!(report.failed, 1);
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(attempts, MAX_REFUND_ATTEMPTS);
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUND_DUE".to_string()),
            "a stuck refund leaves the sub REFUND_DUE"
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            Some("HELD".to_string()),
            "no reservation release on the failed path"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["failed".to_string()]
        );
    }

    #[tokio::test]
    async fn ambiguous_pay_failure_stays_recoverable_at_cap() {
        for backend_status in [PayStatus::Pending, PayStatus::Unknown] {
            let store = mem_store();
            let payment = Arc::new(TestPayment::new());
            payment.set_fail(true);
            payment.set_failed_status(backend_status);
            let clock = TestClock::new(1_000);
            seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
            seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
            seed_reservation(&store, "sub-1").await;
            let r = refunder(&store, &payment, &clock);

            for _ in 0..MAX_REFUND_ATTEMPTS {
                let report = r.drive().await.unwrap();
                assert_eq!(report.retried, 1);
                assert_eq!(report.failed, 0);
            }

            let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
            assert_eq!(status, "PENDING", "{backend_status:?} remains recoverable");
            assert_eq!(attempts, MAX_REFUND_ATTEMPTS);
            assert_eq!(
                scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
                Some("REFUND_DUE".to_string())
            );
            assert_eq!(
                scalar(
                    &store,
                    "SELECT state FROM reservation WHERE order_id='sub-1'"
                )
                .await,
                Some("HELD".to_string())
            );
            assert!(refund_outbox_statuses(&store).await.is_empty());
        }
    }

    #[tokio::test]
    async fn missing_destination_is_failed_without_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", None, Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "missing dest never calls pay");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(attempts, 1);
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUND_DUE".to_string())
        );
        assert_eq!(
            scalar(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            Some("HELD".to_string()),
            "no reservation release on structural failure"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["failed".to_string()]
        );
    }

    // 3. No double-pay on retry: drive() twice in success mode -> pay invoked once (the row is SENT
    //    after the first drive, so the second never re-lists it), one billing.refund enqueued.
    #[tokio::test]
    async fn second_drive_does_not_double_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
        seed_reservation(&store, "sub-1").await;
        let r = refunder(&store, &payment, &clock);

        let first = r.drive().await.unwrap();
        let second = r.drive().await.unwrap();

        assert_eq!(first.sent, 1);
        assert_eq!(second, RefundReport::default(), "second drive is a no-op");
        assert_eq!(payment.pay_calls(), 1, "pay invoked exactly once");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(attempts, 1, "not double-sent");
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // 4. Crash AFTER pay succeeded but BEFORE backend_payment_id was persisted: the key already reads
    //    Succeeded -> drive() completes to SENT via the fast-skip WITHOUT a second pay.
    #[tokio::test]
    async fn fast_skip_completes_without_second_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.mark_paid("refund:order:sub-1"); // a prior attempt paid, pre-crash
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(payment.pay_calls(), 0, "fast-skip never calls pay again");
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUNDED".to_string())
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // 5. Crash AFTER the PENDING row was written but BEFORE pay ever ran (the normal seed) -> drive()
    //    pays and completes to SENT; it is NOT stranded.
    #[tokio::test]
    async fn pending_row_is_not_stranded() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(payment.pay_calls(), 1, "pay(key) runs for the un-paid row");
        let (status, _, backend_id) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert!(backend_id.is_some());
    }

    // 6. Settled-but-terminal: a PENDING refund whose sub is already TERMINATED (not REFUND_DUE) ->
    //    drive() -> refund SENT; the sub state is UNCHANGED (never resurrected).
    #[tokio::test]
    async fn terminal_sub_is_refunded_without_resurrection() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "TERMINATED", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("TERMINATED".to_string()),
            "a terminal sub is not resurrected by the refund"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // 7. Ambiguous pay() error that actually settled: pay records the settlement on the key but
    //    returns Err (an in-flight timeout). The post-Err re-check reads Succeeded and records SENT
    //    — the refund is never falsely parked FAILED nor the buyer told "failed" on a refund that
    //    really went out (the case that would otherwise be unrecoverable on the FINAL attempt).
    #[tokio::test]
    async fn ambiguous_pay_error_that_settled_records_sent() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_settle_then_fail(true);
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(
            report.sent, 1,
            "a settled-but-errored pay records SENT, not FAILED"
        );
        assert_eq!(report.failed, 0);
        assert_eq!(payment.pay_calls(), 1, "pay attempted exactly once");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(attempts, 1);
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUNDED".to_string())
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // NULL amount_sat (provision recorded no amount) is tolerated: refund 0, never panic.
    #[tokio::test]
    async fn null_amount_is_tolerated() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some("ln-dest"), None).await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        // The DM carries the 0 default rather than crashing on the NULL.
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }
}
