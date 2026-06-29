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
use std::time::Duration;

use anyhow::Result;
use rusqlite::{params, Transaction};

use lnrent_wire::{BillingRefund, Msg};

use crate::backends::{PayStatus, PaymentBackend};
use crate::clock::Clock;
use crate::refund_resolver::{
    detect_form, DestForm, PassThroughResolver, RefundResolver, ResolveError, Resolved,
};
use crate::reservation;
use crate::store::Store;

/// Cap on outbound `pay` attempts before a refund is parked as `FAILED` and surfaced to the
/// operator. A small bound: a refund that can't be sent in this many tries needs a human, not an
/// unbounded retry loop. An internal default, not a knob.
const MAX_REFUND_ATTEMPTS: i64 = 5;

/// Total wall-clock bound on resolving ONE refund's destination per drive (the LNURL-pay flow: a DNS
/// lookup + the lnurlp fetch + the callback fetch, each already individually bounded). The drive
/// resolves rows SERIALLY, so without a per-row cap a single slow or hostile buyer endpoint could
/// stall every other pending refund behind it (review P2). On timeout the row stays PENDING (a
/// resolution defer, attempts UNCHANGED) and is retried next drive.
const RESOLUTION_DEADLINE: Duration = Duration::from_secs(60);

/// How long a refund may sit PENDING — repeatedly deferred WITHOUT a `pay` (a transient resolution
/// failure or a legacy in-flight wait, neither of which bumps the pay-attempts cap) — before every
/// drive logs an operator-LOUD error. A buyer endpoint stuck transiently-broken forever (always
/// 5xx / timeout / DNS failure) would otherwise retry silently with nobody notified and the buyer
/// never refunded; this surfaces it for operator action while STILL retrying, so a recovered endpoint
/// can yet refund the buyer (review P2). Time-based, NOT attempts-based, so it never starves the real
/// payment of its retry budget.
const RESOLUTION_STUCK_ALERT_S: i64 = 7 * 24 * 3600;

/// What one [`Refunder::drive`] did. Every count is a normal result, not an error; the supervisor
/// (lnrent-7fp.21) can log it and tests assert on rows directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RefundReport {
    /// Refunds that reached `SENT` this drive (paid, or confirmed already-paid via the fast-skip).
    pub sent: usize,
    /// A recoverable setback left the row `PENDING` for the next drive: either a definitive `pay`
    /// failure (`attempts` bumped, climbing toward [`MAX_REFUND_ATTEMPTS`]), or a NON-pay defer — a
    /// transient resolution failure or a legacy in-flight bare-key wait (`attempts` UNCHANGED, since
    /// no `pay` was attempted). Both are counted here.
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
    /// Turns a LN-address/LNURL `dest` into a payable bolt11 just before `pay()` (lnrent-ug8).
    resolver: Arc<dyn RefundResolver>,
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
    /// The resolver columns (lnrent-ug8). `resolved_bolt11` is the concrete bolt11 a LN-address/LNURL
    /// `dest` last resolved to; `resolved_expiry` its absolute expiry; `resolution_gen` the current
    /// generation (0 = bolt11 pass-through, never resolved).
    resolved_bolt11: Option<String>,
    resolved_expiry: Option<i64>,
    resolution_gen: i64,
    /// When the row was created (unix secs). Used to detect a refund stuck PENDING (deferred without a
    /// pay) past [`RESOLUTION_STUCK_ALERT_S`] so the operator is alerted (review P2).
    created_at: i64,
}

/// The per-row result, mapped 1:1 onto a [`RefundReport`] counter.
enum Outcome {
    Sent,
    Retried,
    Failed,
    Noop,
}

/// What [`Refunder::plan_payment`] decided to do this drive (before any `pay`).
enum PlanOutcome {
    /// Pay `bolt11` with the generation-bound key `refund:<external_id>:g<gen>`.
    Pay { bolt11: String, gen: i64 },
    /// The CURRENT generation already settled — record SENT without paying.
    AlreadySent,
    /// The row is no longer `PENDING` (a concurrent terminalizer won the status CAS as we resolved),
    /// so the resolution persist matched 0 rows. Do NOT pay: paying an UNPERSISTED invoice would
    /// reintroduce the double-pay window the generation-bound model exists to close. Drop to a Noop
    /// and let committed state drive any future drive (review P3).
    Skip,
}

/// Why planning the payment failed, mirroring [`ResolveError`]: a STRUCTURAL failure parks the refund
/// FAILED immediately (never burning the retry cap); a TRANSIENT one leaves it PENDING to retry.
enum PlanError {
    Structural(String),
    Transient(String),
}

impl From<ResolveError> for PlanError {
    fn from(e: ResolveError) -> Self {
        match e {
            ResolveError::Structural(m) => PlanError::Structural(m),
            ResolveError::Transient(m) => PlanError::Transient(m),
        }
    }
}

/// The generation-bound idempotency key handed to `pay()` / `payment_status_by_key()`. The
/// `refund_attempt.idempotency_key` column stays the STABLE ledger/UNIQUE anchor
/// (`refund:<external_id>`); only the value the BACKEND sees is gen-suffixed, so each (re-)resolution
/// gets its own backend payment row + status (lnrent-ug8 codex P0 fix).
fn gen_key(external_id: &str, gen: i64) -> String {
    format!("refund:{external_id}:g{gen}")
}

impl Refunder {
    /// Construct a Refunder with the DEFAULT [`PassThroughResolver`]: it pays the raw `dest`, which is
    /// correct for the v1 daemon's `MockPayment` backend (it accepts any `pay(dest)` string). A REAL
    /// backend (Fedimint) needs a concrete bolt11; the enable-fedimint wiring (lnrent-o6p) injects the
    /// production [`crate::refund_resolver::Resolver`] via [`Refunder::with_resolver`]. The supervisor
    /// (lnrent-7fp.21) calls this 3-arg form, so its signature is fixed.
    pub fn new(store: Store, payment: Arc<dyn PaymentBackend>, clock: Arc<dyn Clock>) -> Self {
        Self::with_resolver(store, payment, clock, Arc::new(PassThroughResolver))
    }

    /// Construct a Refunder with an explicit refund-dest resolver — the seam the enable-fedimint
    /// wiring (lnrent-o6p) uses to inject the real LNURL-pay [`crate::refund_resolver::Resolver`].
    pub fn with_resolver(
        store: Store,
        payment: Arc<dyn PaymentBackend>,
        clock: Arc<dyn Clock>,
        resolver: Arc<dyn RefundResolver>,
    ) -> Self {
        Self {
            store,
            payment,
            clock,
            resolver,
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

    /// Pay (or confirm already-paid) one refund and record the outcome. The destination is RESOLVED
    /// to a payable bolt11 first (lnrent-ug8) under the generation-bound idempotency model: a bolt11
    /// `dest` is paid directly (gen 0); a LN-address/LNURL `dest` is resolved to a persisted bolt11
    /// (gen 1+) whose own gen-bound key drives pay/status, so a retry never double-pays and only a
    /// CURRENT-gen Failed+expired invoice is ever re-resolved.
    async fn process(&self, row: RefundRow) -> Result<Outcome> {
        let now = self.clock.now();
        // NULL amount is tolerated (provision records NULL when the order invoice had no amount):
        // refund 0 rather than panic. A negative figure is clamped for the same reason.
        let amount = row.amount_sat.unwrap_or(0).max(0) as u64;
        let external_id = external_id_of(&row);

        // A MISSING destination can never be paid: park FAILED immediately (no pay, no retry cap).
        let Some(dest) = row.dest.as_deref().map(str::trim).filter(|d| !d.is_empty()) else {
            tracing::error!(refund = %row.id, "refund has no destination; parking FAILED");
            return self
                .commit_structural_failure(&row, amount, now, "no destination")
                .await;
        };

        // Legacy fast-skip + in-flight guard (upgrade safety, lnrent-ug8). A PRE-ug8 binary paid under
        // the STABLE key `refund:<external_id>` (the column value), NOT the gen-suffixed key this code
        // now hands to the backend, which does NOT dedup a gen-bound payment against the bare-key one.
        // So before resolving/paying a row the new resolver has NOT yet touched (`resolved_bolt11 IS
        // NULL`), consult the legacy bare key:
        //  - Succeeded -> the old binary already paid (and maybe crashed before recording SENT):
        //    record SENT WITHOUT a second pay.
        //  - Pending -> the old binary's pay is STILL in flight. Starting a new gen-bound payment now
        //    could DOUBLE-pay once the legacy one settles, so WAIT: leave the row PENDING (recoverable)
        //    until the bare key terminalizes, then the gen-bound path below takes over.
        // Safe on fresh DBs and resolved rows: the new binary never writes the bare key (it always
        // suffixes `:g<gen>`), so a never-paid row reads Unknown here (never Succeeded/Pending) and
        // this is a no-op; a resolved row (`resolved_bolt11` set) was created by THIS binary, so the
        // gen-bound status in `plan_payment` is authoritative and the bare key is irrelevant.
        if row.resolved_bolt11.is_none() {
            match self
                .payment
                .payment_status_by_key(&row.idempotency_key)
                .await
            {
                Ok(PayStatus::Succeeded) => return self.finish_sent(&row, None, amount, now).await,
                Ok(PayStatus::Pending) => {
                    tracing::warn!(
                        refund = %row.id,
                        "legacy bare-key refund payment in flight; deferring to it (no new pay)"
                    );
                    return self.commit_resolution_retry(&row, now).await;
                }
                // A status-lookup ERROR is NOT a no-record Unknown: the legacy bare-key payment could
                // be in flight or already Succeeded but momentarily unqueryable. Paying under the new
                // gen-suffixed key would NOT dedup against it, so an upgrade/restart could issue a
                // SECOND refund. Treat the error as unsafe-to-pay — leave the row PENDING and retry the
                // lookup next drive, exactly like the current-generation lookup path (review P1).
                Err(e) => {
                    tracing::warn!(
                        refund = %row.id,
                        error = %e,
                        "legacy bare-key refund status lookup failed; deferring (no new pay)"
                    );
                    return self.commit_resolution_retry(&row, now).await;
                }
                // Failed: a legacy attempt that terminally failed — safe to take the gen-bound path.
                // Unknown: AMBIGUOUS. Usually a fresh row (no backend record), but per the
                // PaymentBackend contract `Unknown` can ALSO be an in-flight, unconfirmable legacy
                // payment. Falling through then pays a gen-suffixed key that the legacy bare-key
                // payment can't dedup against -> a double-refund if the old one later settles. This is
                // SAFE with MockPayment (deterministic, never returns an in-flight Unknown), and is
                // precisely why enabling Fedimint (lnrent-o6p) is GATED on the fedimint_pay oplog
                // recovery, which disambiguates `Unknown` into a real status. Until then, fresh rows
                // must NOT stall (deferring every Unknown would mean no refund ever pays), so we fall
                // through; the residual upgrade-with-in-flight-legacy double-pay is a documented o6p
                // prerequisite (review P1), not a live risk on this branch.
                Ok(PayStatus::Unknown | PayStatus::Failed) => {}
            }
        }

        // Decide the bolt11 + generation to pay (resolving + persisting if needed), BEFORE pay().
        let (bolt11, gen) = match self
            .plan_payment(&row, &external_id, dest, amount, now)
            .await
        {
            Ok(PlanOutcome::Pay { bolt11, gen }) => (bolt11, gen),
            Ok(PlanOutcome::AlreadySent) => return self.finish_sent(&row, None, amount, now).await,
            // The row was terminalized between resolve and the resolution persist (0 rows updated):
            // never pay an unpersisted invoice. A no-op this drive (review P3).
            Ok(PlanOutcome::Skip) => return Ok(Outcome::Noop),
            // STRUCTURAL: BOLT12 / malformed / amount-or-hash mismatch / HTTPS/SSRF violation — can
            // never settle, so park FAILED immediately (mirrors the missing-dest path), NOT the
            // capped pay-Failed path.
            Err(PlanError::Structural(reason)) => {
                tracing::warn!(refund = %row.id, %reason, "refund destination is unresolvable; parking FAILED");
                return self
                    .commit_structural_failure(&row, amount, now, &reason)
                    .await;
            }
            // TRANSIENT: DNS/TLS/timeout/5xx — leave PENDING (never terminal) and retry next drive.
            // No `pay` was attempted, so this must NOT consume the pay-attempts cap (codex P2):
            // resolution flakiness while the buyer is offline must not starve the real payment of its
            // retry budget. Use the resolution-retry path (attempts UNCHANGED), not commit_pay_failure.
            Err(PlanError::Transient(reason)) => {
                tracing::warn!(refund = %row.id, %reason, "refund resolution failed transiently; row stays PENDING");
                return self.commit_resolution_retry(&row, now).await;
            }
        };

        let key = gen_key(&external_id, gen);
        // Fast-skip: this generation already settled (e.g. a crash after pay but before the SENT
        // bookkeeping committed). Record SENT WITHOUT paying again. Only `Succeeded` skips.
        if self.already_paid(&key).await {
            return self.finish_sent(&row, None, amount, now).await;
        }

        match self.payment.pay(&bolt11, amount, &key).await {
            Ok(backend_payment_id) => {
                self.finish_sent(&row, Some(backend_payment_id), amount, now)
                    .await
            }
            Err(e) => match self.status_by_key_after_error(&key).await {
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

    /// THE GENERATION GATE (lnrent-ug8 codex P0 fix). Pick the bolt11 + generation to pay using ONLY
    /// the CURRENT generation's status, so a stale OLDER-gen Failed can never trigger a re-resolution
    /// while a newer gen is in flight. A re-resolution to a NEW payment_hash happens ONLY when the
    /// current generation is BOTH past its persisted expiry AND a DEFINITE `Failed` — a payment that
    /// has terminally resolved (funds returned) on an invoice that can no longer be paid, so there is
    /// no outstanding HTLC a fresh payment_hash could double up. `Unknown` is NEVER a re-resolution
    /// trigger (review P1): per the [`crate::backends::PayStatus`] contract it is an in-flight payment
    /// that "can be neither confirmed nor refuted" (e.g. the Fedimint crash-after-pay-commit window),
    /// so the old HTLC may yet settle even after the invoice clock expires — minting a fresh
    /// payment_hash there could DOUBLE-pay. Unknown therefore REUSES the persisted invoice + key (the
    /// backend dedups on the stable payment_hash) until it resolves to a definite Failed. A
    /// status-lookup ERROR is treated as Transient (the row stays PENDING), never as a no-record
    /// Unknown. Each (re-)resolution is PERSISTED in a committed txn BEFORE pay(), so the persisted
    /// invoice + gen-bound key + backend per-payment-hash dedup cover every crash window.
    async fn plan_payment(
        &self,
        row: &RefundRow,
        external_id: &str,
        dest: &str,
        amount: u64,
        now: i64,
    ) -> Result<PlanOutcome, PlanError> {
        // bolt11 pass-through: pay it directly, generation stays 0 (no resolution).
        if matches!(detect_form(dest)?, DestForm::Bolt11) {
            return Ok(PlanOutcome::Pay {
                bolt11: dest.to_string(),
                gen: 0,
            });
        }

        let owed_msat = amount.saturating_mul(1000);
        match row.resolved_bolt11.as_deref() {
            // Never resolved -> generation 1: resolve, PERSIST (committed), then pay g1.
            None => {
                let resolved = self.resolve_dest(dest, owed_msat, now).await?;
                if !self.persist_resolution(&row.id, &resolved, 1, now).await? {
                    return Ok(PlanOutcome::Skip);
                }
                Ok(PlanOutcome::Pay {
                    bolt11: resolved.bolt11,
                    gen: 1,
                })
            }
            // Already resolved -> branch on the CURRENT generation's status.
            Some(bolt11) => {
                let gen = row.resolution_gen;
                let key = gen_key(external_id, gen);
                // A failed status LOOKUP is NOT a decision point: we can neither reuse nor re-resolve
                // against a backend we cannot query (re-resolving could double-pay a payment we simply
                // can't see right now). Leave the row PENDING and retry the lookup next drive — NOT
                // `unwrap_or(Unknown)`, which would let a transient lookup error masquerade as a
                // genuine no-record Unknown and (for an expired row) wrongly re-resolve.
                let st = self
                    .payment
                    .payment_status_by_key(&key)
                    .await
                    .map_err(|e| {
                        PlanError::Transient(format!("refund status lookup for {key} failed: {e}"))
                    })?;
                let expired = now >= row.resolved_expiry.unwrap_or(0);
                match st {
                    PayStatus::Succeeded => Ok(PlanOutcome::AlreadySent),
                    // RE-RESOLVE — the ONLY case that mints a new payment_hash — fires only when the
                    // CURRENT generation can NEVER settle: a DEFINITE `Failed` that is ALSO past its
                    // persisted expiry. Failed means the prior HTLC terminally resolved (funds
                    // returned); expired means the invoice itself can no longer be paid. With no
                    // outstanding payment, a fresh payment_hash cannot double up. Bump the gen, resolve
                    // fresh, PERSIST (committed), then pay the new gen. (Were we to keep re-paying the
                    // dead invoice it would never settle and the refund would strand PENDING until the
                    // cap parks it FAILED — re-resolving recovers it instead, review P2.)
                    //
                    // `Unknown` is DELIBERATELY excluded here (review P1): per the
                    // [`crate::backends::PayStatus`] contract it is an in-flight payment that "can be
                    // neither confirmed nor refuted", so the old HTLC may still settle even after the
                    // invoice expires (the Fedimint crash-after-pay-commit window). Re-resolving there
                    // would pay a NEW payment_hash that neither the gen-bound key nor the per-hash dedup
                    // can match against the still-live old one — a double refund. Unknown falls through
                    // to REUSE below, and only re-resolves once it has become a definite Failed.
                    PayStatus::Failed if expired => {
                        let next_gen = gen + 1;
                        let resolved = self.resolve_dest(dest, owed_msat, now).await?;
                        if !self
                            .persist_resolution(&row.id, &resolved, next_gen, now)
                            .await?
                        {
                            return Ok(PlanOutcome::Skip);
                        }
                        Ok(PlanOutcome::Pay {
                            bolt11: resolved.bolt11,
                            gen: next_gen,
                        })
                    }
                    // Everything else re-pays the SAME persisted invoice with the SAME gen (the
                    // backend dedups on the stable payment_hash, so this never double-pays): a payment
                    // still in flight (Pending); an in-flight/crash-window payment that can't be
                    // confirmed (Unknown — REUSED whether expired or not, the P1 fix above); or a
                    // returned-funds retry of an unexpired invoice (Failed, unexpired). We never
                    // re-resolve while the persisted invoice could still settle.
                    PayStatus::Pending | PayStatus::Unknown | PayStatus::Failed => {
                        Ok(PlanOutcome::Pay {
                            bolt11: bolt11.to_string(),
                            gen,
                        })
                    }
                }
            }
        }
    }

    /// Resolve `dest` to a bolt11, mapping a [`ResolveError`] to a [`PlanError`]. Bounded by a
    /// per-row [`RESOLUTION_DEADLINE`] so one slow/hostile buyer endpoint can't stall the serial
    /// drive (review P2); a deadline overrun is TRANSIENT — the row stays PENDING for the next drive.
    async fn resolve_dest(
        &self,
        dest: &str,
        owed_msat: u64,
        now: i64,
    ) -> Result<Resolved, PlanError> {
        match tokio::time::timeout(
            RESOLUTION_DEADLINE,
            self.resolver.resolve(dest, owed_msat, now),
        )
        .await
        {
            Ok(r) => Ok(r?),
            Err(_) => Err(PlanError::Transient(format!(
                "refund resolution exceeded the {RESOLUTION_DEADLINE:?} per-row deadline"
            ))),
        }
    }

    /// Persist `(resolved_bolt11, resolved_expiry, resolution_gen)` in ONE committed txn BEFORE pay()
    /// — the generation-bound invariant: pay() always pays a persisted invoice with the persisted
    /// gen's key, so a crash between persist and pay re-pays the SAME invoice on the next drive.
    /// Returns whether the row was still `PENDING` (i.e. the resolution actually committed): a 0-row
    /// update means a concurrent writer terminalized the row, and the caller MUST NOT pay the
    /// (now-unpersisted) invoice (review P3).
    async fn persist_resolution(
        &self,
        id: &str,
        resolved: &Resolved,
        gen: i64,
        now: i64,
    ) -> Result<bool, PlanError> {
        let (id, bolt11, expiry) = (id.to_string(), resolved.bolt11.clone(), resolved.expiry);
        self.store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE refund_attempt
                        SET resolved_bolt11=?2, resolved_expiry=?3, resolution_gen=?4, updated_at=?5
                      WHERE id=?1 AND status='PENDING'",
                    params![id, bolt11, expiry, gen, now],
                )?;
                Ok(updated > 0)
            })
            .await
            .map_err(|e| PlanError::Transient(format!("persisting refund resolution: {e}")))
    }

    /// `Succeeded` per the backend's idempotency-key status — the refund already went out on this
    /// key. Only `Succeeded` counts as paid; an `Unknown`/`Pending`/`Failed` (or a lookup error) is
    /// not, so the caller falls through to `pay`, which the key dedups.
    async fn already_paid(&self, idempotency_key: &str) -> bool {
        matches!(
            self.payment.payment_status_by_key(idempotency_key).await,
            Ok(PayStatus::Succeeded)
        )
    }

    /// Re-check the key after a `pay` error. Lookup errors are treated as `Unknown`: terminalizing
    /// while the backend cannot answer is unsafe because the payment may still settle later.
    async fn status_by_key_after_error(&self, idempotency_key: &str) -> PayStatus {
        match self.payment.payment_status_by_key(idempotency_key).await {
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

    /// Structurally unsendable rows (a missing destination, or a `dest` the resolver rejects as
    /// permanent — BOLT12 / malformed / amount-or-hash mismatch / HTTPS/SSRF violation) are parked
    /// `FAILED` immediately, NOT via the capped pay-Failed path. No payment was attempted, the sub is
    /// left in `REFUND_DUE`, and the reservation is NOT released.
    async fn commit_structural_failure(
        &self,
        row: &RefundRow,
        amount: u64,
        now: i64,
        reason: &str,
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
                %reason,
                "refund parked FAILED (structural, unresolvable destination)"
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

    /// A recoverable NON-pay setback that must NOT consume the pay-attempts cap (codex P2): a
    /// transient resolution failure (a flaky LNURL endpoint while the buyer is offline), or waiting on
    /// a legacy in-flight bare-key payment across an upgrade. No `pay` was attempted, so `attempts` is
    /// left UNCHANGED — only definitive PAY failures climb toward [`MAX_REFUND_ATTEMPTS`]. The row
    /// STAYS `PENDING` (never terminal) and is retried next drive; this touches `updated_at` and
    /// journals a retry for the audit trail, guarded on the row still being `PENDING` (a lost CAS is a
    /// `Noop`).
    async fn commit_resolution_retry(&self, row: &RefundRow, now: i64) -> Result<Outcome> {
        let external_id = external_id_of(row);
        let id = row.id.clone();
        let sub_id = row.subscription_id.clone();
        let outcome = self
            .store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE refund_attempt SET updated_at=?2 WHERE id=?1 AND status='PENDING'",
                    params![id, now],
                )?;
                if updated == 0 {
                    return Ok(Outcome::Noop);
                }
                journal(tx, sub_id.as_deref(), "refund_retry", &external_id, now)?;
                Ok(Outcome::Retried)
            })
            .await?;
        // ESCALATION (review P2): a refund that keeps deferring WITHOUT a pay — a permanently
        // transient-looking buyer endpoint, or a legacy payment in flight forever — would otherwise
        // strand money silently with nobody notified. Once it has been PENDING past the stuck
        // threshold, surface it with an operator-LOUD error EVERY drive. The row stays PENDING and
        // keeps retrying (a recovered endpoint can still refund the buyer); this is time-based, so it
        // never consumes the pay-attempts cap.
        if matches!(outcome, Outcome::Retried) {
            let age = now - row.created_at;
            if age >= RESOLUTION_STUCK_ALERT_S {
                tracing::error!(
                    refund = %row.id,
                    subscription = row.subscription_id.as_deref().unwrap_or(""),
                    age_s = age,
                    "refund stuck PENDING without payment past the alert threshold (resolution/in-flight not progressing); operator attention needed"
                );
            }
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
                            s.buyer_pubkey, r.resolved_bolt11, r.resolved_expiry, r.resolution_gen,
                            r.created_at
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
                            resolved_bolt11: r.get(6)?,
                            resolved_expiry: r.get(7)?,
                            resolution_gen: r.get::<_, Option<i64>>(8)?.unwrap_or(0),
                            created_at: r.get::<_, Option<i64>>(9)?.unwrap_or(0),
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
    use async_trait::async_trait;
    use rusqlite::{Connection, OptionalExtension};
    use std::collections::{HashMap, HashSet};
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
    /// already-settled (the crash-after-pay fast-skip). Faithful to a real backend, a key it has
    /// NEVER seen (never paid, never `pay()`-attempted, no explicit override) reports `Unknown`, not
    /// the global `failed_status` — so the legacy bare-key guard only fires for a key a (legacy) pay
    /// actually touched. Methods the refunder never calls are `unimplemented!()`. (MockPayment can't
    /// simulate a `pay` failure, and we must not edit backends.rs to add one.)
    #[derive(Default)]
    struct TestPayment {
        inner: Mutex<TestPayState>,
    }

    #[derive(Default)]
    struct TestPayState {
        paid: HashMap<String, String>, // idempotency_key -> backend payment id
        // Per-key status OVERRIDE (the generation gate seeds e.g. `:g1`->Failed, `:g2`->Unknown to
        // prove status is read per-generation). Consulted before the global `failed_status` default.
        key_status: HashMap<String, PayStatus>,
        // Keys `pay()` was attempted on (even if it errored). Only these fall back to `failed_status`;
        // a never-attempted key reports `Unknown`, as a real backend would for a key it never recorded.
        attempted: HashSet<String>,
        pay_calls: usize,
        seq: u64,
        fail: bool,
        failed_status: Option<PayStatus>,
        settle_then_fail: bool, // pay() records the settlement but returns Err (ambiguous timeout)
        status_lookup_fails: bool, // payment_status_by_key returns Err (an unqueryable backend)
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
        /// Make `payment_status_by_key` return `Err` — an unqueryable backend, so the gate must
        /// neither reuse nor re-resolve (it leaves the row PENDING and retries the lookup next drive).
        fn set_status_lookup_fails(&self, fails: bool) {
            self.inner.lock().unwrap().status_lookup_fails = fails;
        }
        /// Override the status reported for ONE key (a specific generation), without recording a
        /// payment — used by the generation-gate tests.
        fn set_key_status(&self, key: &str, status: PayStatus) {
            self.inner
                .lock()
                .unwrap()
                .key_status
                .insert(key.to_string(), status);
        }
        /// Whether a `pay` (or `mark_paid`) ever recorded this key.
        fn was_paid(&self, key: &str) -> bool {
            self.inner.lock().unwrap().paid.contains_key(key)
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

    #[async_trait]
    impl PaymentBackend for TestPayment {
        async fn pay(
            &self,
            _dest: &str,
            _amount_sat: u64,
            idempotency_key: &str,
        ) -> Result<String> {
            let mut st = self.inner.lock().unwrap();
            st.pay_calls += 1;
            st.attempted.insert(idempotency_key.to_string());
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
        async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
            let st = self.inner.lock().unwrap();
            if st.status_lookup_fails {
                anyhow::bail!("test backend: status lookup failed");
            }
            Ok(if st.paid.contains_key(idempotency_key) {
                PayStatus::Succeeded
            } else if let Some(s) = st.key_status.get(idempotency_key) {
                *s
            } else if st.attempted.contains(idempotency_key) {
                st.failed_status.unwrap_or(PayStatus::Unknown)
            } else {
                // A key the backend never recorded (never paid/attempted/seeded) -> Unknown.
                PayStatus::Unknown
            })
        }
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("refunder never receives")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!("refunder never looks up invoices")
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            unimplemented!("refunder never looks up invoices")
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!("refunder checks by key, not id")
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("refunder never watches")
        }
    }

    fn refunder(store: &Store, payment: &Arc<TestPayment>, clock: &TestClock) -> Refunder {
        Refunder::new(store.clone(), payment.clone(), Arc::new(clock.clone()))
    }

    fn refunder_with(
        store: &Store,
        payment: &Arc<TestPayment>,
        clock: &TestClock,
        resolver: Arc<dyn RefundResolver>,
    ) -> Refunder {
        Refunder::with_resolver(
            store.clone(),
            payment.clone(),
            Arc::new(clock.clone()),
            resolver,
        )
    }

    /// A LN-address `dest` used throughout the executor tests: the DEFAULT [`PassThroughResolver`]
    /// (which [`refunder`] wires) returns it verbatim as the bolt11, so the mock TestPayment pays it —
    /// exercising the resolution+persist path (gen 1) without standing up an LNURL server.
    const LN_ADDR: &str = "lnaddr@buyer";

    /// A fake resolver for the generation-gate tests: counts calls, returns a distinct bolt11 per
    /// call (`resolved-bolt11-N`), and can be set to fail structurally or transiently.
    #[derive(Default)]
    struct TestResolver {
        inner: Mutex<TestResolverState>,
    }
    #[derive(Default)]
    struct TestResolverState {
        calls: usize,
        expiry: i64,
        fail_structural: bool,
        fail_transient: bool,
    }
    impl TestResolver {
        fn new(expiry: i64) -> Self {
            Self {
                inner: Mutex::new(TestResolverState {
                    expiry,
                    ..Default::default()
                }),
            }
        }
        fn calls(&self) -> usize {
            self.inner.lock().unwrap().calls
        }
        fn with_structural(self) -> Self {
            self.inner.lock().unwrap().fail_structural = true;
            self
        }
        fn with_transient(self) -> Self {
            self.inner.lock().unwrap().fail_transient = true;
            self
        }
    }
    #[async_trait]
    impl RefundResolver for TestResolver {
        async fn resolve(
            &self,
            _dest: &str,
            _owed: u64,
            _now: i64,
        ) -> Result<Resolved, ResolveError> {
            let mut st = self.inner.lock().unwrap();
            st.calls += 1;
            if st.fail_structural {
                return Err(ResolveError::Structural("test structural failure".into()));
            }
            if st.fail_transient {
                return Err(ResolveError::Transient("test transient failure".into()));
            }
            Ok(Resolved {
                bolt11: format!("resolved-bolt11-{}", st.calls),
                expiry: st.expiry,
            })
        }
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

    /// Seed a PENDING refund_attempt that has ALREADY been resolved to `resolved_bolt11` at
    /// `resolved_expiry` with generation `gen` — the state a prior drive's resolution left behind, so
    /// the generation gate can be exercised directly.
    #[allow(clippy::too_many_arguments)]
    async fn seed_refund_resolved(
        store: &Store,
        sub_id: &str,
        dest: &str,
        amount: i64,
        resolved_bolt11: &str,
        resolved_expiry: i64,
        gen: i64,
    ) {
        let external_id = format!("order:{sub_id}");
        let (sub_id, dest, resolved_bolt11) = (
            sub_id.to_string(),
            dest.to_string(),
            resolved_bolt11.to_string(),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO refund_attempt
                        (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts,
                         resolved_bolt11, resolved_expiry, resolution_gen, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6, ?7, ?8, 0, 0)",
                    params![
                        format!("ref-{external_id}"),
                        sub_id,
                        dest,
                        amount,
                        format!("refund:{external_id}"),
                        resolved_bolt11,
                        resolved_expiry,
                        gen,
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// `(resolved_bolt11, resolution_gen)` of a refund row.
    async fn resolution_of(store: &Store, id: &str) -> (Option<String>, i64) {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT resolved_bolt11, resolution_gen FROM refund_attempt WHERE id=?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get::<_, Option<i64>>(1)?.unwrap_or(0))),
                )?)
            })
            .await
            .unwrap()
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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
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
            seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
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

    // 4. Crash AFTER pay succeeded but BEFORE backend_payment_id was persisted: the CURRENT gen's key
    //    already reads Succeeded -> drive() completes to SENT via the fast-skip WITHOUT a second pay.
    //    The resolution is already persisted (gen 1), as it must be (persist commits before pay).
    #[tokio::test]
    async fn fast_skip_completes_without_second_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.mark_paid("refund:order:sub-1:g1"); // a prior attempt paid the gen-1 key, pre-crash
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(
            &store,
            "sub-1",
            LN_ADDR,
            500,
            "resolved-bolt11-1",
            i64::MAX,
            1,
        )
        .await;
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

    // Upgrade safety (lnrent-ug8): a PRE-ug8 binary paid under the STABLE key `refund:<external_id>`
    // (no `:g<gen>` suffix) and crashed before recording SENT. The new gen-bound key would miss that
    // backend record and re-pay — a DOUBLE refund. The legacy fast-skip honors a Succeeded on the
    // bare stable key: the row records SENT with NO second pay and NO resolution.
    #[tokio::test]
    async fn legacy_stable_key_paid_before_upgrade_is_not_double_paid() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.mark_paid("refund:order:sub-1"); // the OLD binary paid the bare stable key, pre-crash
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            payment.pay_calls(),
            0,
            "a legacy-paid refund is never re-paid across the upgrade"
        );
        assert_eq!(
            resolver.calls(),
            0,
            "a legacy-paid refund is never (re-)resolved"
        );
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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

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
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
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
        seed_refund(&store, "sub-1", Some(LN_ADDR), None).await;

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

    // ---- the generation gate (lnrent-ug8 codex P0 fix) ----------------------

    // A bolt11 `dest` is paid DIRECTLY with the gen-0 key — no resolution, the resolver is never
    // called, and resolution_gen stays 0.
    #[tokio::test]
    async fn bolt11_dest_pays_generation_zero_without_resolving() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(i64::MAX));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let bolt11 = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400);
        seed_refund(&store, "sub-1", Some(&bolt11), Some(500)).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(resolver.calls(), 0, "a bolt11 dest is never resolved");
        assert!(
            payment.was_paid("refund:order:sub-1:g0"),
            "paid with the gen-0 key"
        );
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(resolved, None, "no resolved_bolt11 for a bolt11 dest");
        assert_eq!(gen, 0);
    }

    // First resolution of a LN-address: gen -> 1, the resolved bolt11 is PERSISTED, and it is paid
    // with the gen-1 key.
    #[tokio::test]
    async fn first_resolution_persists_generation_one_and_pays() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(resolver.calls(), 1, "resolved exactly once");
        assert!(
            payment.was_paid("refund:order:sub-1:g1"),
            "paid the gen-1 key"
        );
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(resolved.as_deref(), Some("resolved-bolt11-1"));
        assert_eq!(gen, 1, "first resolution is generation 1");
    }

    // THE GATE — gen-A Failed AND past its expiry -> RE-RESOLVE to gen-B (a NEW invoice), persist the
    // bumped generation, and pay gen-B. The only case that mints a new payment_hash.
    #[tokio::test]
    async fn gen_a_failed_and_expired_reresolves_to_gen_b() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Failed);
        let clock = TestClock::new(200); // now=200 >= the seeded expiry 100
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g1", 100, 1).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(resolver.calls(), 1, "exactly one re-resolution");
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(gen, 2, "generation bumped to 2");
        assert_eq!(
            resolved.as_deref(),
            Some("resolved-bolt11-1"),
            "a fresh bolt11 was persisted, replacing the expired gen-1 invoice"
        );
        assert!(
            payment.was_paid("refund:order:sub-1:g2"),
            "paid the gen-2 key"
        );
    }

    // THE GATE — gen-B status Unknown (the pay-commit/upsert crash window): REUSE the SAME persisted
    // gen-B invoice + key. No re-resolution, no new payment_hash.
    #[tokio::test]
    async fn gen_b_unknown_crash_window_reuses_without_reresolve() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new()); // unseen keys default to Unknown
        let clock = TestClock::new(200);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g2", 10_000, 2).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(resolver.calls(), 0, "Unknown REUSES, never re-resolves");
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(gen, 2, "generation unchanged");
        assert_eq!(
            resolved.as_deref(),
            Some("bolt11-g2"),
            "same invoice reused"
        );
        assert!(
            payment.was_paid("refund:order:sub-1:g2"),
            "re-paid the gen-2 key"
        );
    }

    // THE GATE (review P1) — gen-A Unknown AND past its expiry must REUSE the persisted invoice, NOT
    // re-resolve. `Unknown` is an in-flight payment the backend "can neither confirm nor refute" (the
    // Fedimint crash-after-pay-commit window included), so the old HTLC may still settle even though
    // the invoice clock has expired. Minting a fresh payment_hash there could double-refund the buyer,
    // so the gate re-pays the SAME gen-1 key (the backend dedups on the stable payment_hash) and
    // re-resolves ONLY once the status is a DEFINITE Failed+expired (see
    // `gen_a_failed_and_expired_reresolves_to_gen_b`). Reusing a genuinely-dead invoice does not
    // strand the refund: the re-pay fails, climbing the cap to a FAILED park + operator alert.
    #[tokio::test]
    async fn gen_unknown_and_expired_reuses_never_reresolves() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new()); // gen-1 key unseen -> Unknown (in-flight, unconfirmable)
        let clock = TestClock::new(200); // now=200 >= the seeded expiry 100
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g1", 100, 1).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            resolver.calls(),
            0,
            "an Unknown (in-flight) invoice REUSES even when expired — never re-resolves (no double-pay)"
        );
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(
            gen, 1,
            "generation unchanged — gen-1 was re-paid, not re-resolved"
        );
        assert_eq!(
            resolved.as_deref(),
            Some("bolt11-g1"),
            "the persisted gen-1 invoice is reused"
        );
        assert!(
            payment.was_paid("refund:order:sub-1:g1"),
            "re-paid the gen-1 key"
        );
    }

    // THE GATE (review P2) — a failed status LOOKUP on an expired, already-resolved row must NEITHER
    // reuse NOR re-resolve: re-resolving against a backend we cannot query could double-pay a payment
    // we simply can't see. It is a TRANSIENT setback — the row stays PENDING, nothing is paid or
    // resolved, the pay cap is untouched, and the lookup is retried next drive. This is the safety
    // boundary that keeps a lookup error from masquerading as the DEFINITE Failed that
    // `gen_a_failed_and_expired_reresolves_to_gen_b` acts on — only a real Failed+expired re-resolves.
    #[tokio::test]
    async fn status_lookup_error_is_transient_never_reresolves() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_status_lookup_fails(true);
        let clock = TestClock::new(200); // past the seeded expiry 100
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g1", 100, 1).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(
            report.retried, 1,
            "a failed status lookup leaves the row PENDING"
        );
        assert_eq!(
            resolver.calls(),
            0,
            "never re-resolves against an unqueryable backend (no double-pay)"
        );
        assert_eq!(payment.pay_calls(), 0, "and never pays");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "PENDING");
        assert_eq!(
            attempts, 0,
            "a failed lookup is not a pay attempt; the cap is untouched"
        );
    }

    // THE GATE — a stale OLDER-gen (g1) Failed must NEVER re-resolve a LIVE newer gen (g2): status is
    // read per-generation, so g2 (Pending) drives the decision and g1's Failed is ignored.
    #[tokio::test]
    async fn stale_gen_a_failed_does_not_reresolve_live_gen_b() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Failed); // stale, must be ignored
        payment.set_key_status("refund:order:sub-1:g2", PayStatus::Pending); // the live generation
        let clock = TestClock::new(200);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        // gen-2 is the current generation even though now(200) is past gen-1's old expiry (100).
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g2", 100, 2).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            resolver.calls(),
            0,
            "the stale gen-1 Failed never triggers a re-resolution of the live gen-2"
        );
        let (_, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(
            gen, 2,
            "generation unchanged — gen-2 was re-paid, not re-resolved"
        );
    }

    // A gen Failed but NOT expired -> re-pay the SAME invoice (the prior funds returned), same gen.
    #[tokio::test]
    async fn gen_failed_but_unexpired_reuses_same_invoice() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Failed);
        let clock = TestClock::new(200); // now=200 < the seeded expiry 10_000
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, "bolt11-g1", 10_000, 1).await;

        // Failed-but-unexpired re-pays the same key. The mock pay() then SUCCEEDS (not in fail mode),
        // so the refund records SENT — proving the unexpired invoice is retried, never re-resolved.
        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            resolver.calls(),
            0,
            "unexpired Failed reuses, never re-resolves"
        );
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(gen, 1, "generation unchanged");
        assert_eq!(resolved.as_deref(), Some("bolt11-g1"));
    }

    // A STRUCTURAL resolution failure parks FAILED IMMEDIATELY — one attempt, NOT the retry cap, and
    // pay() is never called.
    #[tokio::test]
    async fn structural_resolution_failure_parks_immediately_without_burning_cap() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000).with_structural());
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder_with(&store, &payment, &clock, resolver)
            .drive()
            .await
            .unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "structural failure never pays");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(attempts, 1, "parked after ONE attempt, not the retry cap");
        assert_eq!(
            scalar(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            Some("REFUND_DUE".to_string()),
            "structural park leaves the sub REFUND_DUE"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["failed".to_string()]
        );
    }

    // If the row is terminalized (e.g. a future concurrent writer) BETWEEN resolve() and the
    // resolution persist, the persist's `WHERE status='PENDING'` matches 0 rows. The drive must NOT
    // pay the unpersisted invoice — it Noops this drive, never double-paying (review P3). The resolver
    // here flips the row to FAILED mid-resolve to force that 0-row persist.
    #[tokio::test]
    async fn zero_row_resolution_persist_does_not_pay() {
        struct TerminalizeMidResolve {
            store: Store,
        }
        #[async_trait]
        impl RefundResolver for TerminalizeMidResolve {
            async fn resolve(
                &self,
                _dest: &str,
                _owed: u64,
                _now: i64,
            ) -> Result<Resolved, ResolveError> {
                // Simulate a concurrent terminalizer winning the CAS while we resolve.
                self.store
                    .transaction(|tx| {
                        tx.execute(
                            "UPDATE refund_attempt SET status='FAILED' WHERE id='ref-order:sub-1'",
                            [],
                        )?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                Ok(Resolved {
                    bolt11: "resolved-bolt11-x".into(),
                    expiry: i64::MAX,
                })
            }
        }

        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        let resolver = Arc::new(TerminalizeMidResolve {
            store: store.clone(),
        });

        let report = refunder_with(&store, &payment, &clock, resolver)
            .drive()
            .await
            .unwrap();

        assert_eq!(
            payment.pay_calls(),
            0,
            "a 0-row resolution persist must never pay an unpersisted invoice"
        );
        assert_eq!(
            report,
            RefundReport::default(),
            "the terminalized row is a no-op this drive"
        );
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(
            status, "FAILED",
            "the row stays terminalized; the drive did not advance it"
        );
    }

    // A BOLT12 offer `dest` is structurally rejected at detect_form -> immediate FAILED, no pay.
    #[tokio::test]
    async fn bolt12_dest_parks_immediately() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(
            &store,
            "sub-1",
            Some("lno1pqps7sjqpgtyzm3qv4uxzmtsd3jjqer9wd3hy6tsw35k7"),
            Some(500),
        )
        .await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "BOLT12 never pays");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(attempts, 1, "BOLT12 parks after one attempt");
    }

    // A TRANSIENT resolution failure leaves the row PENDING (never terminal) and is retried; it is
    // re-resolved each drive because no resolution was persisted. Crucially it does NOT bump the
    // pay-attempts cap (codex P2): no pay was attempted, so resolution flakiness must not starve the
    // real payment of its retry budget.
    #[tokio::test]
    async fn transient_resolution_failure_stays_pending_and_retries() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000).with_transient());
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        let r = refunder_with(&store, &payment, &clock, resolver.clone());

        for _ in 1..=3 {
            let report = r.drive().await.unwrap();
            assert_eq!(report.retried, 1);
            assert_eq!(report.failed, 0, "transient is NEVER terminal");
            let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
            assert_eq!(status, "PENDING");
            assert_eq!(
                attempts, 0,
                "a transient RESOLUTION failure does not consume the pay-attempts cap"
            );
        }
        assert_eq!(payment.pay_calls(), 0, "a failed resolution never pays");
        assert_eq!(
            resolver.calls(),
            3,
            "re-resolved each drive (nothing persisted)"
        );
        assert!(refund_outbox_statuses(&store).await.is_empty());
    }

    // A refund whose resolution keeps failing transiently for longer than the stuck-alert threshold
    // is surfaced with an operator-loud alert but STAYS PENDING — still retried, attempts UNCHANGED,
    // never declared FAILED — so a recovered endpoint can still refund the buyer (review P2). The seed
    // sets created_at=0, so a clock past the threshold trips the age branch.
    #[tokio::test]
    async fn stuck_transient_resolution_alerts_but_stays_pending() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(RESOLUTION_STUCK_ALERT_S + 1); // age = now - 0 > threshold
        let resolver = Arc::new(TestResolver::new(10_000).with_transient());
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

        let report = refunder_with(&store, &payment, &clock, resolver)
            .drive()
            .await
            .unwrap();

        assert_eq!(report.retried, 1);
        assert_eq!(
            report.failed, 0,
            "a stuck transient is alerted, NOT terminalized"
        );
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(
            status, "PENDING",
            "the row keeps retrying so a recovered endpoint still refunds"
        );
        assert_eq!(
            attempts, 0,
            "the age-based alert is time-based and never burns the pay-attempts cap"
        );
        assert_eq!(payment.pay_calls(), 0);
        assert!(
            refund_outbox_statuses(&store).await.is_empty(),
            "no buyer DM — the refund is not declared failed, only escalated to the operator"
        );
    }

    // Upgrade safety (lnrent-ug8): a PRE-ug8 binary submitted pay under the BARE stable key and it is
    // STILL in flight (Pending) when the new binary boots. Because the backend does NOT dedup a
    // gen-bound payment against the bare-key one, starting a new payment now could DOUBLE-pay once the
    // legacy one settles. The new binary must DEFER: leave the row PENDING (no new pay, no resolve, no
    // cap burn) until the bare key terminalizes — then resume normally (here: it settles -> SENT).
    #[tokio::test]
    async fn legacy_bare_key_in_flight_defers_until_terminal() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_key_status("refund:order:sub-1", PayStatus::Pending); // legacy pay in flight
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        let r = refunder_with(&store, &payment, &clock, resolver.clone());

        // While the legacy bare-key payment is Pending: defer. No pay, no resolve, no cap burn.
        let report = r.drive().await.unwrap();
        assert_eq!(report.retried, 1);
        assert_eq!(payment.pay_calls(), 0, "never starts a parallel payment");
        assert_eq!(resolver.calls(), 0, "never resolves while deferring");
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "PENDING");
        assert_eq!(attempts, 0, "deferring does not burn the pay-attempts cap");

        // The legacy payment settles on the bare key -> the next drive records SENT without re-paying.
        payment.set_key_status("refund:order:sub-1", PayStatus::Succeeded);
        let report = r.drive().await.unwrap();
        assert_eq!(report.sent, 1);
        assert_eq!(payment.pay_calls(), 0, "the legacy refund is never re-paid");
        assert_eq!(resolver.calls(), 0, "and never (re-)resolved");
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // Upgrade safety (review P1): for an UNRESOLVED row (`resolved_bolt11 IS NULL`, the pre-ug8 shape),
    // a FAILED status LOOKUP on the legacy bare key must NOT fall through to a gen-bound pay(). The old
    // bare-key payment could be in flight or already Succeeded but momentarily unqueryable, and a new
    // gen-suffixed key would not dedup against it — a double refund. The drive must DEFER: leave the
    // row PENDING (no pay, no resolve, no cap burn) and retry the lookup next drive. Contrast
    // `status_lookup_error_is_transient_never_reresolves`, which covers the SAME error on an
    // already-RESOLVED row (the per-generation lookup path).
    #[tokio::test]
    async fn legacy_bare_key_lookup_error_defers_without_paying() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_status_lookup_fails(true); // the legacy bare key is momentarily unqueryable
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await; // unresolved: resolved_bolt11 NULL

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(
            report.retried, 1,
            "an unqueryable legacy bare key leaves the row PENDING"
        );
        assert_eq!(
            payment.pay_calls(),
            0,
            "never pays under a gen key while the legacy bare key is unqueryable (no double-refund)"
        );
        assert_eq!(
            resolver.calls(),
            0,
            "defers before even planning the payment — never resolves"
        );
        let (status, attempts, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "PENDING");
        assert_eq!(
            attempts, 0,
            "a failed legacy lookup is not a pay attempt; the cap is untouched"
        );
    }
}
