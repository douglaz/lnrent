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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use lightning_invoice::Bolt11Invoice;
use rusqlite::{params, OptionalExtension, Transaction};

use lnrent_wire::{BillingRefund, Msg};

use crate::alerts::{Alert, AlertDispatcher, AlertKind, AlertRow};
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
/// failure, which does not bump the pay-attempts cap) — before every
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
    /// transient resolution failure (`attempts` UNCHANGED, since
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
    /// Optional GATE-1 alert sink (lnrent-urw.1): surfaces a parked/stuck refund as a durable
    /// operator DM, additive to the existing loud log. `None` in focused unit tests / mock wiring
    /// that build via [`Refunder::new`]; the supervisor injects the real one via
    /// [`Refunder::with_alerts`].
    alerts: Option<Arc<AlertDispatcher>>,
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
    /// Pay `bolt11` with the generation-bound key from [`gen_key`] (gen 0 = the bare
    /// `refund:<external_id>`; gen>=1 = `refund:<external_id>:g<gen>`). `pay_sat` is the exact
    /// fee-adjusted payout (INV-1): `net_cap` for a freshly resolved invoice, or the persisted/direct
    /// invoice's own whole-sat amount on a re-await or bolt11 pass-through. It is NOT the gross.
    Pay {
        bolt11: String,
        gen: i64,
        pay_sat: u64,
    },
    /// The CURRENT generation already settled — record SENT without paying. `pay_sat` is the amount
    /// that generation actually paid (the persisted/direct invoice's own whole-sat figure), so the
    /// "sent" DM reports the NET delivered, never the gross (review P2).
    AlreadySent { pay_sat: u64 },
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

/// The result of the INV-3 provenance check (spec §3.3). `Ok(received)` carries the gross sats the
/// order actually received (positive, and equal to `refund_attempt.amount_sat`). `Forbidden(reason)`
/// means there is no matching received payment — or the received amount is missing/non-positive, or it
/// mismatches the refund row — so the refund MUST park FAILED and never pay.
enum ProvenanceCheck {
    Ok(u64),
    Forbidden(String),
}

/// The generation-bound idempotency key handed to `pay()` / `payment_status_by_key()`. The
/// `refund_attempt.idempotency_key` column stays the STABLE ledger/UNIQUE anchor
/// (`refund:<external_id>`); only the value the BACKEND sees is gen-suffixed for gen>=1, so each LNURL
/// (re-)resolution gets its own backend payment row + status (lnrent-ug8 codex P0 fix).
///
/// GEN 0 (bolt11 pass-through / not-yet-resolved) is the BARE `refund:<external_id>` — the SAME key a
/// pre-ug8 binary paid bolt11 refunds under. So an in-flight or completed legacy bolt11 refund dedups
/// against the new binary's gen-0 pay on the IDENTICAL key (no upgrade double-pay, lnrent-4gt). GEN>=1
/// (resolved LNURL) keeps the `:g<gen>` suffix.
///
/// The intermediate ug8 scheme suffixed gen 0 as `refund:<external_id>:g0`; 4gt drops that suffix
/// (this fn). There is NO `:g0`-vs-bare upgrade double-pay window (review P1) because no deployed
/// binary has ever paid a REAL refund under EITHER key: the only money-moving backend (Fedimint) is
/// still o6p-gated and unwired in main.rs — and o6p is BLOCKED by this bead — while MockPayment moves
/// no money. The bare-key gen-0 convention is therefore fixed BEFORE the first real-money release, so
/// a live `:g0` payment can never exist for a bare-key gen-0 pay to double up against. The fuller
/// legacy-upgrade reasoning (incl. the LN-address caveat) lives at the `process()` legacy-safety block.
/// `pub(crate)`: the supervisor's refund-readiness probe keys on the SAME generation-bound pay-key
/// scheme; a private duplicate there once had to be changed in lockstep (lnrent-4gt) — one definition.
pub(crate) fn gen_key(external_id: &str, gen: i64) -> String {
    if gen == 0 {
        format!("refund:{external_id}")
    } else {
        format!("refund:{external_id}:g{gen}")
    }
}

impl Refunder {
    /// Construct a Refunder with the DEFAULT [`PassThroughResolver`]: it pays the raw `dest`, which is
    /// useful for focused tests and mock backends that accept any `pay(dest)` string. Production
    /// daemon wiring injects [`crate::refund_resolver::Resolver`] via [`Refunder::with_resolver`].
    pub fn new(store: Store, payment: Arc<dyn PaymentBackend>, clock: Arc<dyn Clock>) -> Self {
        Self::with_resolver(store, payment, clock, Arc::new(PassThroughResolver))
    }

    /// Construct a Refunder with an explicit refund-dest resolver — the seam production wiring uses to
    /// inject the real LNURL-pay [`crate::refund_resolver::Resolver`].
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
            alerts: None,
        }
    }

    /// Inject the GATE-1 alert sink (lnrent-urw.1). The supervisor calls this so a parked/stuck
    /// refund additionally surfaces as a durable operator DM; without it the refunder only logs.
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Fire a RECURRING alert best-effort (out of any txn): an enqueue failure is logged but NEVER
    /// fails the refund drive (the loud log line at the call site already recorded the condition).
    /// Terminal parks use [`Refunder::parked_alert_row`] instead, enqueued in their state txn.
    async fn alert(&self, alert: Alert) {
        if let Some(alerts) = &self.alerts {
            if let Err(e) = alerts.dispatch(alert).await {
                tracing::warn!(error = %format!("{e:#}"), "failed to enqueue operator alert");
            }
        }
    }

    /// The `RefundParked` alert row to enqueue INSIDE a park-FAILED transaction, or `None` when
    /// alerting is off. Terminal + atomic: it commits with the FAILED transition, so a crash can
    /// never drop the alert for a park the row will never re-enter (codex).
    fn parked_alert_row(&self, row: &RefundRow, reason: &str) -> Option<AlertRow> {
        let detail = format!(
            "refund {} parked FAILED ({reason}); sub {}",
            row.id,
            row.subscription_id.as_deref().unwrap_or("-")
        );
        self.alerts
            .as_ref()?
            .terminal_alert_row(AlertKind::RefundParked, &row.id, &detail)
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
        let external_id = external_id_of(&row);

        // INV-3 PROVENANCE GUARD FIRST (spec §3.3): a refund MUST correspond to a payment actually
        // received for this order, and the stored gross MUST match that received amount. No provenance,
        // a missing/non-positive received amount, or a row-vs-provenance mismatch parks FAILED at ERROR
        // — never pay, never clamp to 0. Both production writers (capture::refund_intent,
        // provision::RefundDueWrite) are downstream of received-payment handling; this execution-time
        // guard is the enforcement, not their position in the pipeline.
        //
        // Running this BEFORE the already-paid fast-skip below is deliberate and spec-mandated
        // (provenance-FIRST): a refund that somehow PAID without provenance is a serious invariant
        // violation that must park + alert the operator, not silently record SENT. That state is
        // unreachable in production — both writers create provenance upstream of (or in the same txn
        // as) the received payment — so a genuinely-paid refund always HAS provenance and is never
        // wrongly flipped to FAILED (review P3).
        let received = match self.verify_refund_provenance(&row, &external_id).await? {
            ProvenanceCheck::Ok(received) => received,
            ProvenanceCheck::Forbidden(reason) => {
                // The stored figure is the refund's recorded gross (0 when NULL/negative) — used only
                // for the failed DM; the refund parks without paying.
                let stored = row.amount_sat.unwrap_or(0).max(0) as u64;
                return self.commit_park_failed(&row, stored, now, &reason).await;
            }
        };

        // A MISSING destination can never be paid: park FAILED immediately (no pay, no retry cap).
        let Some(dest) = row.dest.as_deref().map(str::trim).filter(|d| !d.is_empty()) else {
            return self
                .commit_park_failed(&row, received, now, "no destination")
                .await;
        };

        // Legacy upgrade safety (lnrent-4gt): a pre-ug8 binary paid the raw `dest` under the BARE
        // `refund:<external_id>` key with NO form detection. Gen 0 IS that bare key now (see
        // `gen_key`), so a legacy BOLT11 refund takes the NORMAL gen-0 path below on the SAME key the
        // old binary used — the `already_paid` fast-skip short-circuits a completed legacy refund
        // (record SENT, no re-pay), and otherwise `pay(bare)` re-enters / re-awaits the in-flight
        // legacy op (the backend dedups on the key). No mismatched-key double-pay, so ug8's special
        // bare-key fall-through is gone.
        //
        // CAVEAT for a future o6p/Fedimint implementer (review P2): this bare-key dedup covers the
        // BOLT11 legacy path ONLY. A pre-ug8 binary ran no form detection, so an LN-ADDRESS `dest`
        // would in principle also have been paid under the bare key — but the new binary RESOLVES an
        // LN-address to a fresh bolt11 under `:g1`, whose `:g1` key does NOT dedup against any bare-key
        // payment. That is SAFE here, not COVERED, for two reasons: (a) a real LN backend rejects a
        // non-bolt11 `dest`, so no LIVE legacy LN-address payment can exist; and (b) no money has moved
        // yet at all — Fedimint is o6p-gated/unwired and MockPayment moves no money. Do NOT assume
        // LN-address legacy upgrades are deduped if that ever changes.

        // Decide the bolt11 + generation + fee-adjusted pay amount (resolving/quoting + persisting if
        // needed), BEFORE pay(). `received` is the GROSS liability; `pay_sat` is the INV-1 payout.
        let (bolt11, gen, pay_sat) = match self
            .plan_payment(&row, &external_id, dest, received, now)
            .await
        {
            Ok(PlanOutcome::Pay {
                bolt11,
                gen,
                pay_sat,
            }) => (bolt11, gen, pay_sat),
            Ok(PlanOutcome::AlreadySent { pay_sat }) => {
                return self.finish_sent(&row, None, pay_sat, now).await
            }
            // The row was terminalized between resolve and the resolution persist (0 rows updated):
            // never pay an unpersisted invoice. A no-op this drive (review P3).
            Ok(PlanOutcome::Skip) => return Ok(Outcome::Noop),
            // STRUCTURAL: BOLT12 / malformed / amount-or-hash mismatch / HTTPS/SSRF violation, a fixed
            // bolt11 above the fee-adjusted cap, or a true dust refund (net_cap==0) — none can be
            // auto-paid without operator loss, so park FAILED immediately (mirrors the missing-dest
            // path), NOT the capped pay-Failed path.
            Err(PlanError::Structural(reason)) => {
                tracing::warn!(refund = %row.id, %reason, "refund cannot be auto-paid; parking FAILED");
                return self.commit_park_failed(&row, received, now, &reason).await;
            }
            // TRANSIENT: DNS/TLS/timeout/5xx, or a gateway/quote outage — leave PENDING (never terminal)
            // and retry next drive. No `pay` was attempted, so this must NOT consume the pay-attempts
            // cap (codex P2): resolution/quote flakiness while the buyer is offline must not starve the
            // real payment of its retry budget. Use the resolution-retry path (attempts UNCHANGED).
            Err(PlanError::Transient(reason)) => {
                tracing::warn!(refund = %row.id, %reason, "refund resolution/quote failed transiently; row stays PENDING");
                return self.commit_resolution_retry(&row, now).await;
            }
        };

        let key = gen_key(&external_id, gen);
        // Fast-skip: this generation already settled (e.g. a crash after pay but before the SENT
        // bookkeeping committed). Record SENT WITHOUT paying again. Only `Succeeded` skips. The DM
        // reports the net `pay_sat` that went out, not the gross (review P2).
        if self.already_paid(&key).await {
            return self.finish_sent(&row, None, pay_sat, now).await;
        }

        // INV-1: pay the fee-adjusted `pay_sat` (<= net cap), bounded by `received` so the backend's
        // final cap preflight refuses any over-gross outlay. `refund_attempt.amount_sat` stays the GROSS
        // received amount (`received`); only the payout — and the buyer-facing "sent" DM (review P2) —
        // is reduced by the gateway fee.
        tracing::debug!(refund = %row.id, gross = received, net_pay = pay_sat, gen, "paying capped refund");
        match self
            .payment
            .pay_refund_capped(&bolt11, pay_sat, received, &key)
            .await
        {
            Ok(backend_payment_id) => {
                self.finish_sent(&row, Some(backend_payment_id), pay_sat, now)
                    .await
            }
            Err(e) => match self.status_by_key_after_error(&key).await {
                PayStatus::Succeeded => self.finish_sent(&row, None, pay_sat, now).await,
                PayStatus::Failed => {
                    tracing::warn!(
                        refund = %row.id,
                        error = %e,
                        "refund pay failed definitively; row stays PENDING until retry cap"
                    );
                    self.commit_pay_failure(&row, received, now, true).await
                }
                status @ (PayStatus::Pending | PayStatus::Unknown) => {
                    tracing::warn!(
                        refund = %row.id,
                        error = %e,
                        ?status,
                        "refund pay failed ambiguously; row stays PENDING for recovery"
                    );
                    self.commit_pay_failure(&row, received, now, false).await
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
        received: u64,
        now: i64,
    ) -> Result<PlanOutcome, PlanError> {
        // bolt11 pass-through (gen 0): the dest IS the payable invoice. The operator cannot rewrite a
        // buyer's fixed amount, so pay the invoice's OWN whole-sat amount; the cap only gates whether a
        // NEW payment may start (spec §3.1).
        if matches!(detect_form(dest)?, DestForm::Bolt11) {
            let bolt11_sat = parse_whole_sat(dest).map_err(PlanError::Structural)?;
            let key = gen_key(external_id, 0);
            // STATUS-FIRST: a started gen-0 op (Succeeded/Pending) is re-awaited with NO re-quote, so a
            // gateway-down quote can never strand an in-flight/settled gen-0. Only a NEW gen-0 payment
            // (absent -> Unknown, or a returned-funds Failed) quotes the cap.
            let st = self
                .payment
                .payment_status_by_key(&key)
                .await
                .map_err(|e| {
                    PlanError::Transient(format!("refund status lookup for {key} failed: {e}"))
                })?;
            // Unknown but the op actually STARTED (crash window: pay_bolt11_invoice committed before the
            // index row was written) is an in-flight refund that may yet settle — re-await it on the SAME
            // key/payment-hash (fedimint dedups), NEVER re-quote, or a gateway outage / fee rise could
            // strand a payment that can still land (codex P2). Only a not-started Unknown or a
            // returned-funds Failed quotes the cap for a genuinely new payment.
            let started_unknown = matches!(st, PayStatus::Unknown)
                && self
                    .payment
                    .payment_started_by_key(&key)
                    .await
                    .map_err(|e| {
                        PlanError::Transient(format!("refund started-check for {key}: {e}"))
                    })?;
            return match st {
                PayStatus::Succeeded => Ok(PlanOutcome::AlreadySent {
                    pay_sat: bolt11_sat,
                }),
                PayStatus::Pending => Ok(PlanOutcome::Pay {
                    bolt11: dest.to_string(),
                    gen: 0,
                    pay_sat: bolt11_sat,
                }),
                _ if started_unknown => Ok(PlanOutcome::Pay {
                    bolt11: dest.to_string(),
                    gen: 0,
                    pay_sat: bolt11_sat,
                }),
                PayStatus::Unknown | PayStatus::Failed => {
                    let net_cap = self.quote_net_cap(received).await?;
                    if bolt11_sat > net_cap {
                        return Err(PlanError::Structural(format!(
                            "buyer's fixed bolt11 ({bolt11_sat} sat) exceeds the fee-adjusted refund cap ({net_cap} sat)"
                        )));
                    }
                    Ok(PlanOutcome::Pay {
                        bolt11: dest.to_string(),
                        gen: 0,
                        pay_sat: bolt11_sat,
                    })
                }
            };
        }

        match row.resolved_bolt11.as_deref() {
            // Never resolved -> generation 1, a NEW payment: quote the INV-1 cap, resolve a bolt11 for
            // EXACTLY that net cap, PERSIST (committed), then pay g1 for the net amount.
            None => {
                let net_cap = self.quote_net_cap(received).await?;
                let owed_msat = net_cap.checked_mul(1000).ok_or_else(|| {
                    PlanError::Structural(format!(
                        "refund net amount {net_cap} sat overflows u64 msats"
                    ))
                })?;
                let resolved = self.resolve_dest(dest, owed_msat, now).await?;
                if !self.persist_resolution(&row.id, &resolved, 1, now).await? {
                    return Ok(PlanOutcome::Skip);
                }
                Ok(PlanOutcome::Pay {
                    bolt11: resolved.bolt11,
                    gen: 1,
                    pay_sat: net_cap,
                })
            }
            // Already resolved -> branch on the CURRENT generation's status. The quote is touched ONLY
            // on the re-resolve (new-payment) arm, so neither a fee change nor a gateway outage can
            // re-price or strand an existing generation (spec §3.1).
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
                    // The persisted invoice's own whole-sat amount is what was paid; fall back to the
                    // gross for a non-bolt11 pass-through dest (mock) that has no parseable amount.
                    PayStatus::Succeeded => Ok(PlanOutcome::AlreadySent {
                        pay_sat: parse_whole_sat(bolt11).unwrap_or(received),
                    }),
                    // RE-RESOLVE (the ONLY case that mints a new payment_hash) fires only on a DEFINITE
                    // `Failed`: the prior HTLC terminally resolved (funds returned) OR the cap preflight
                    // never started an op — either way nothing is outstanding, so a fresh payment_hash
                    // cannot double up. Re-query the CURRENT gateway fee and re-resolve when the current
                    // generation can never settle as-is: it is EXPIRED, or the (otherwise static) gateway
                    // fee was RAISED by its operator so the persisted invoice — quoted at the old fee —
                    // now exceeds the INV-1 cap and would fail the preflight on every retry (operator
                    // guidance). Otherwise the failure is transient (a gateway blip) and REUSING the
                    // persisted invoice on a plain retry is correct.
                    //
                    // `Pending`/`Unknown` are NEVER re-quoted or re-resolved (review P1): per the
                    // [`crate::backends::PayStatus`] contract `Unknown` is an in-flight payment that "can
                    // be neither confirmed nor refuted" (the Fedimint crash-after-pay-commit window), so
                    // the old HTLC may still settle; a new payment_hash there would double-refund. They
                    // re-await the persisted invoice below with NO re-quote, so a gateway outage can never
                    // strand an in-flight payment.
                    PayStatus::Failed => {
                        let net_cap = self.quote_net_cap(received).await?; // Err (gateway down) => Transient
                        let persisted_sat = parse_whole_sat(bolt11).unwrap_or(received);
                        if expired || persisted_sat > net_cap {
                            let owed_msat = net_cap.checked_mul(1000).ok_or_else(|| {
                                PlanError::Structural(format!(
                                    "refund net amount {net_cap} sat overflows u64 msats"
                                ))
                            })?;
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
                                pay_sat: net_cap,
                            })
                        } else {
                            Ok(PlanOutcome::Pay {
                                bolt11: bolt11.to_string(),
                                gen,
                                pay_sat: persisted_sat,
                            })
                        }
                    }
                    // Pending or Unknown RE-AWAIT the SAME persisted invoice with its PERSISTED pay amount
                    // and the SAME gen (the backend dedups on the stable payment_hash, so this never
                    // double-pays). NO re-quote, so a gateway outage can't strand an in-flight payment. A
                    // non-bolt11 pass-through dest (mock) falls back to the gross, which the backend's
                    // final cap preflight still bounds — so it can never overpay.
                    PayStatus::Pending | PayStatus::Unknown => {
                        let pay_sat = parse_whole_sat(bolt11).unwrap_or(received);
                        Ok(PlanOutcome::Pay {
                            bolt11: bolt11.to_string(),
                            gen,
                            pay_sat,
                        })
                    }
                }
            }
        }
    }

    /// Quote the INV-1 net cap for a NEW refund payment (spec §3.1): the largest fee-adjusted whole-sat
    /// payout for `received`. `Ok(0)` (true dust — no positive whole-sat payout plus fee fits inside the
    /// gross) is a STRUCTURAL park (FAILED, never retried). An `Err` (gateway unreadable / quote
    /// failure) is TRANSIENT (row stays PENDING, retried next drive) — NOT dust, so a gateway outage
    /// never parks a real liability as if it were unrefundable.
    async fn quote_net_cap(&self, received: u64) -> Result<u64, PlanError> {
        match self.payment.refund_net_sat(received).await {
            Ok(0) => Err(PlanError::Structural(
                "received amount below the network fee; cannot auto-refund without operator loss"
                    .to_string(),
            )),
            Ok(net) => Ok(net),
            Err(e) => Err(PlanError::Transient(format!(
                "refund fee quote failed: {e}"
            ))),
        }
    }

    /// INV-3 provenance guard (spec §3.3): a refund MUST correspond to a payment received for its
    /// order. Valid provenance is either an `invoice` row for `external_id` showing received funds
    /// (`status='PAID'` OR `settled_at IS NOT NULL` — a LATE settlement stamps `settled_at` on an
    /// already-terminal/EXPIRED invoice without flipping it back to PAID), or a settle-refund
    /// `event_log` entry for unmatched/orphan settlements that have no invoice row. The received amount
    /// comes from that source and MUST equal the refund row's gross `amount_sat`; no provenance, a
    /// missing/non-positive received amount, or a mismatch is `Forbidden` -> park FAILED.
    ///
    /// The exact `==` is safe across both creation sites today: provision's refund row and its
    /// provenance amount are read from the SAME invoice row, and capture's matched-settlement row
    /// amount equals the invoice amount because Lightning pays a bolt11 in full (settlement_amount ==
    /// invoice_amount). A future partial/over-payment path would have to revisit this equality before a
    /// legitimate refund could survive the guard (review P3).
    async fn verify_refund_provenance(
        &self,
        row: &RefundRow,
        external_id: &str,
    ) -> Result<ProvenanceCheck> {
        let received = match self.lookup_received_amount(external_id).await? {
            Some(a) if a > 0 => a as u64,
            Some(_) => {
                return Ok(ProvenanceCheck::Forbidden(format!(
                    "refund without matching received payment — forbidden: provenance for {external_id} has a non-positive received amount"
                )))
            }
            None => {
                return Ok(ProvenanceCheck::Forbidden(format!(
                    "refund without matching received payment — forbidden: no received-payment provenance for {external_id}"
                )))
            }
        };
        match row.amount_sat {
            Some(a) if a == received as i64 => Ok(ProvenanceCheck::Ok(received)),
            Some(a) => Ok(ProvenanceCheck::Forbidden(format!(
                "refund without matching received payment — forbidden: refund amount {a} != received {received} for {external_id}"
            ))),
            None => Ok(ProvenanceCheck::Forbidden(format!(
                "refund without matching received payment — forbidden: refund row has no amount but {received} was received for {external_id}"
            ))),
        }
    }

    /// The received gross sats for `external_id` from provenance: the paid/settled invoice first, else a
    /// settle-refund journal entry (unmatched/orphan settlements that have no invoice row). `None` when
    /// no provenance exists OR the source carries no usable amount.
    async fn lookup_received_amount(&self, external_id: &str) -> Result<Option<i64>> {
        let ext = external_id.to_string();
        self.store
            .read(move |c| {
                // (1) Invoice provenance — received funds for this external_id. When an invoice row
                // exists its amount decides (a NULL amount -> non-positive -> FAILED); we do NOT fall
                // back to the journal, which only covers the no-invoice (unmatched/orphan) case.
                let inv: Option<Option<i64>> = c
                    .query_row(
                        "SELECT amount_sat FROM invoice
                          WHERE external_id=?1 AND (status='PAID' OR settled_at IS NOT NULL)",
                        params![ext],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(amount) = inv {
                    return Ok(amount);
                }
                // (2) Settle-refund journal provenance (the unmatched/orphan settlement families).
                let j: Option<Option<i64>> = c
                    .query_row(
                        "SELECT json_extract(detail_json, '$.amount_sat') FROM event_log
                          WHERE kind IN ('settle_unmatched_refund', 'settle_terminal_refund',
                                         'settle_orphan_refund', 'settle_expired_refund')
                            AND json_extract(detail_json, '$.external_id') = ?1
                          ORDER BY id LIMIT 1",
                        params![ext],
                        |r| r.get(0),
                    )
                    .optional()?;
                Ok(j.flatten())
            })
            .await
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
    /// `paid_sat` is the NET amount actually delivered to the buyer (after the gateway fee, or a
    /// below-cap direct bolt11) — it is what the "sent" DM reports, distinct from the gross
    /// `refund_attempt.amount_sat` ledger figure, which this never rewrites (review P2 / AC-9).
    async fn finish_sent(
        &self,
        row: &RefundRow,
        backend_payment_id: Option<String>,
        paid_sat: u64,
        now: i64,
    ) -> Result<Outcome> {
        if self
            .commit_sent(row, backend_payment_id, paid_sat, now)
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
        paid_sat: u64,
        now: i64,
    ) -> Result<bool> {
        let external_id = external_id_of(row);
        let outbox_id = format!("outbox:refund:{external_id}");
        // The "sent" DM carries the NET amount delivered (`paid_sat`), not the gross liability — a
        // fee-deducted or below-cap refund must not tell the buyer the full gross went out (review P2).
        let payload = serde_json::to_string(&Msg::BillingRefund(BillingRefund {
            subscription_id: row.subscription_id.clone().unwrap_or_default(),
            amount_sat: paid_sat,
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

    /// Park a refund `FAILED` immediately in ONE txn (status->FAILED, bump attempts, enqueue the failed
    /// DM + journal), NOT via the capped pay-Failed path. No payment was attempted, the sub is left in
    /// `REFUND_DUE`, and the reservation is NOT released. Shared by every "can never be auto-paid"
    /// reason: a missing destination, a permanently-unresolvable `dest` (BOLT12 / malformed /
    /// amount-or-hash mismatch / HTTPS/SSRF violation), a fixed bolt11 above the fee-adjusted cap, a
    /// true dust refund, and an INV-3 provenance violation. The operator-loud ERROR carries `reason`.
    async fn commit_park_failed(
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
        // The parked alert is enqueued INSIDE the terminalizing txn (codex): once FAILED commits the
        // row is never re-selected, so a best-effort post-commit enqueue could lose the alert to a
        // crash — the exact terminal park the sink exists to surface. Built here, inserted below.
        let parked_alert = self.parked_alert_row(row, reason);
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
                if let Some(alert) = &parked_alert {
                    enqueue_alert_row(tx, alert, now)?;
                }
                Ok(Outcome::Failed)
            })
            .await?;
        if matches!(outcome, Outcome::Failed) {
            tracing::error!(
                refund = %row.id,
                subscription = row.subscription_id.as_deref().unwrap_or(""),
                %reason,
                "refund parked FAILED — manual handling required"
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
        // Parked alert enqueued IN the terminalizing txn (codex) — see `commit_park_failed`.
        let parked_alert = self.parked_alert_row(
            row,
            &format!("exhausted {MAX_REFUND_ATTEMPTS} pay attempts"),
        );
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
                if let Some(alert) = &parked_alert {
                    enqueue_alert_row(tx, alert, now)?;
                }
                Ok(Outcome::Failed)
            })
            .await?;
        match outcome {
            Outcome::Failed => tracing::error!(
                refund = %row.id,
                subscription = row.subscription_id.as_deref().unwrap_or(""),
                attempts = MAX_REFUND_ATTEMPTS,
                "refund parked FAILED after exhausting retry attempts"
            ),
            // A PENDING refund whose pay keeps failing ambiguously (Pending/Unknown, allow_terminal
            // false) never reaches the cap and so never parks — it bypasses `commit_resolution_retry`'s
            // stuck check entirely (codex). Cover it here: a retried pay-failure past the threshold is
            // surfaced too, cooldown-collapsed like the resolution-stuck path.
            Outcome::Retried => self.maybe_alert_stuck(row, now).await,
            _ => {}
        }
        Ok(outcome)
    }

    /// A recoverable NON-pay setback that must NOT consume the pay-attempts cap (codex P2): a
    /// transient resolution failure (a flaky LNURL endpoint while the buyer is offline). No `pay` was
    /// attempted, so `attempts` is
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
        // strand money silently with nobody notified. Surface it once it is PENDING past the stuck
        // threshold (shared with the pay-stuck path in `commit_pay_failure`).
        if matches!(outcome, Outcome::Retried) {
            self.maybe_alert_stuck(row, now).await;
        }
        Ok(outcome)
    }

    /// A refund still PENDING past [`RESOLUTION_STUCK_ALERT_S`] — deferring without progress, whether
    /// its `pay` keeps failing ambiguously or its destination keeps not resolving — is stranded money
    /// nobody is watching. Log operator-LOUD EVERY drive and fire a `RefundStuck` alert; the
    /// dispatcher's per-(kind,subject) 6h cooldown collapses the repeats to one DM. Time-based, so it
    /// never consumes the pay-attempts cap; the row stays PENDING and keeps retrying.
    async fn maybe_alert_stuck(&self, row: &RefundRow, now: i64) {
        let age = now - row.created_at;
        if age < RESOLUTION_STUCK_ALERT_S {
            return;
        }
        tracing::error!(
            refund = %row.id,
            subscription = row.subscription_id.as_deref().unwrap_or(""),
            age_s = age,
            "refund stuck PENDING past the alert threshold (not progressing); operator attention needed"
        );
        self.alert(Alert::new(
            AlertKind::RefundStuck,
            row.id.clone(),
            format!(
                "refund {} stuck PENDING {age}s without progress; sub {}",
                row.id,
                row.subscription_id.as_deref().unwrap_or("-")
            ),
        ))
        .await;
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

/// Parse a bolt11's amount as a WHOLE number of sats (spec §3.1). `Err` if the invoice is amountless
/// or its amount is not a whole sat (sub-sat msats) — neither is payable by the sat-only backends.
/// Used for the gen-0 pass-through (the buyer's fixed invoice) and to recover a persisted resolved
/// invoice's pay amount on a re-await. `pub(crate)`: also the supervisor readiness probe's parser
/// (was a private duplicate — one definition, see `gen_key`).
pub(crate) fn parse_whole_sat(bolt11: &str) -> Result<u64, String> {
    let inv = Bolt11Invoice::from_str(bolt11).map_err(|e| format!("bolt11 parse error: {e}"))?;
    let msat = inv
        .amount_milli_satoshis()
        .ok_or_else(|| "bolt11 has no amount".to_string())?;
    if msat % 1000 != 0 {
        return Err(format!(
            "bolt11 amount {msat} msat is not a whole number of sats"
        ));
    }
    Ok(msat / 1000)
}

/// The stable `external_id` the row was keyed on — strip `refund:` off the idempotency key (or
/// `ref-` off the id). It anchors the deterministic outbox id so a re-drive never double-enqueues.
fn external_id_of(row: &RefundRow) -> String {
    external_id_from(&row.idempotency_key, &row.id)
}

/// Derive a refund's external id from its `idempotency_key` (`refund:<ext>`) or, failing that, its
/// row id (`ref-<ext>`). Public so the retry actuator (ipc.rs, lnrent-urw.5) computes the SAME id.
pub fn external_id_from(idempotency_key: &str, refund_id: &str) -> String {
    if let Some(ext) = idempotency_key.strip_prefix("refund:") {
        return ext.to_string();
    }
    refund_id
        .strip_prefix("ref-")
        .unwrap_or(refund_id)
        .to_string()
}

/// The STABLE `billing.refund` outbox row id for a refund. Single source of truth so the retry
/// actuator can supersede the stale parked-FAILED DM before the refunder enqueues the success one
/// (lnrent-urw.5 / codex): both derive the id here.
pub fn refund_outbox_id(idempotency_key: &str, refund_id: &str) -> String {
    format!("outbox:refund:{}", external_id_from(idempotency_key, refund_id))
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

/// Insert a prepared terminal `operator.alert` outbox row (lnrent-urw.1) in the caller's txn, so the
/// alert commits atomically with the state transition it reports. `ON CONFLICT DO NOTHING` on the
/// stable id makes a re-drive idempotent.
fn enqueue_alert_row(tx: &Transaction, row: &AlertRow, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox
            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
         VALUES (?1, ?2, NULL, 'operator.alert', ?3, 'PENDING', 0, ?4)
         ON CONFLICT(id) DO NOTHING",
        params![row.id, row.recipient, row.payload, now],
    )?;
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
    /// the global `failed_status` — so the idempotency / in-flight checks only treat a key as failed
    /// when a `pay` for it actually failed. Methods the refunder never calls are `unimplemented!()`. (MockPayment can't
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
        pay_dest: HashMap<String, String>,
        // Keys `pay()` was attempted on (even if it errored). Only these fall back to `failed_status`;
        // a never-attempted key reports `Unknown`, as a real backend would for a key it never recorded.
        attempted: HashSet<String>,
        pay_calls: usize,
        seq: u64,
        fail: bool,
        failed_status: Option<PayStatus>,
        settle_then_fail: bool, // pay() records the settlement but returns Err (ambiguous timeout)
        status_lookup_fails: bool, // payment_status_by_key returns Err (an unqueryable backend)
        started: HashSet<String>, // keys payment_started_by_key reports as an in-flight (oplog) op
        net_cap: Option<u64>, // refund_net_sat override (the fee-adjusted cap); None => full gross
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
        /// Report `key` as a STARTED-but-maybe-unrecorded op (fedimint's crash window) so an `Unknown`
        /// status is treated as in-flight by `payment_started_by_key`.
        fn set_started(&self, key: &str) {
            self.inner.lock().unwrap().started.insert(key.to_string());
        }
        /// Override the fee-adjusted net cap that `refund_net_sat` returns.
        fn set_net_cap(&self, net_sat: u64) {
            self.inner.lock().unwrap().net_cap = Some(net_sat);
        }
        /// Whether a `pay` (or `mark_paid`) ever recorded this key.
        fn was_paid(&self, key: &str) -> bool {
            self.inner.lock().unwrap().paid.contains_key(key)
        }
        fn paid_dest(&self, key: &str) -> Option<String> {
            self.inner.lock().unwrap().pay_dest.get(key).cloned()
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
        async fn pay(&self, dest: &str, _amount_sat: u64, idempotency_key: &str) -> Result<String> {
            let mut st = self.inner.lock().unwrap();
            st.pay_calls += 1;
            st.attempted.insert(idempotency_key.to_string());
            st.pay_dest
                .insert(idempotency_key.to_string(), dest.to_string());
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
        async fn payment_started_by_key(&self, idempotency_key: &str) -> Result<bool> {
            Ok(self.inner.lock().unwrap().started.contains(idempotency_key))
        }
        async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
            Ok(self.inner.lock().unwrap().net_cap.unwrap_or(gross_sat))
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

    /// A refunder wired with an ENABLED GATE-1 alert sink (lnrent-urw.1) delivering to
    /// `recipient_hex`, so a parked/stuck refund enqueues an `operator.alert` outbox row.
    fn refunder_with_alerts(
        store: &Store,
        payment: &Arc<TestPayment>,
        clock: &TestClock,
        recipient_hex: &str,
    ) -> Refunder {
        let dispatcher = Arc::new(crate::alerts::AlertDispatcher::new(
            store.clone(),
            Arc::new(clock.clone()),
            recipient_hex.to_string(),
        ));
        refunder(store, payment, clock).with_alerts(dispatcher)
    }

    /// Every enqueued `operator.alert` DM as `(recipient_hex, kind_wire, subject)`, id-ordered.
    async fn operator_alerts(store: &Store) -> Vec<(String, String, String)> {
        let rows: Vec<(String, String)> = store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT recipient, payload_json FROM outbox WHERE msg_type='operator.alert' ORDER BY id",
                )?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap();
        rows.into_iter()
            .map(|(recipient, p)| match serde_json::from_str::<Msg>(&p).unwrap() {
                Msg::OperatorAlert(a) => (recipient, a.kind, a.subject),
                other => panic!("expected operator.alert, got {}", other.type_str()),
            })
            .collect()
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

    /// Seed a PENDING refund_attempt row exactly as capture/provision would have written it upstream,
    /// PLUS its INV-3 provenance: a PAID `order` invoice for the same external_id with the same amount
    /// (capture/provision only ever write a refund strictly downstream of a received payment). The
    /// driving tests need provenance or the execution-time guard would (correctly) park them FAILED.
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
                seed_paid_invoice_txn(tx, &external_id, &sub_id, "PAID", Some(30), amount)?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Insert a received-payment `order` invoice (the INV-3 provenance source) in `tx`. `status` /
    /// `settled_at` model the provenance variants: a normal PAID capture, or a LATE settlement that
    /// stamped `settled_at` on an already-terminal (e.g. EXPIRED) invoice without flipping it to PAID.
    fn seed_paid_invoice_txn(
        tx: &Transaction,
        external_id: &str,
        sub_id: &str,
        status: &str,
        settled_at: Option<i64>,
        amount: Option<i64>,
    ) -> rusqlite::Result<()> {
        tx.execute(
            "INSERT INTO invoice
                (id, subscription_id, external_id, kind, amount_sat, status, settled_at, issued_at)
             VALUES (?1, ?2, ?3, 'order', ?4, ?5, ?6, 0)",
            params![
                format!("inv-{external_id}"),
                sub_id,
                external_id,
                amount,
                status,
                settled_at,
            ],
        )?;
        Ok(())
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
                // INV-3 provenance, as for seed_refund: a PAID order invoice with the same amount.
                seed_paid_invoice_txn(tx, &external_id, &sub_id, "PAID", Some(30), Some(amount))?;
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

    /// Every `billing.refund` outbox payload as `(status, amount_sat)` — for asserting the amount the
    /// buyer is told, not just the status (review P2).
    async fn refund_outbox(store: &Store) -> Vec<(String, u64)> {
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
                Msg::BillingRefund(b) => (b.status, b.amount_sat),
                other => panic!("expected billing.refund, got {}", other.type_str()),
            })
            .collect()
    }

    /// The gross `amount_sat` ledger figure of a refund row (must stay the received amount, never the
    /// fee-deducted payout — AC-9).
    async fn refund_amount(store: &Store, id: &str) -> Option<i64> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT amount_sat FROM refund_attempt WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
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
    async fn legacy_null_destination_is_failed_without_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        // Legacy/manual-liability rows can predate the new order-intake refund_dest gate. They must
        // still park loudly for operator handling rather than being retried or silently dropped.
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

    // urw.1 site 1 (commit_pay_failure): a refund that exhausts its pay-retry cap parks FAILED and
    // enqueues EXACTLY ONE RefundParked operator alert, addressed to the configured recipient — and
    // only on the drive that reaches the cap, never on the earlier retried drives.
    #[tokio::test]
    async fn pay_exhaustion_park_enqueues_one_operator_alert() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_fail(true);
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        seed_reservation(&store, "sub-1").await;
        let r = refunder_with_alerts(&store, &payment, &clock, "op-npub-hex");

        for _ in 1..MAX_REFUND_ATTEMPTS {
            r.drive().await.unwrap();
            assert!(
                operator_alerts(&store).await.is_empty(),
                "no alert before the cap is reached"
            );
        }
        let report = r.drive().await.unwrap();
        assert_eq!(report.failed, 1);

        let alerts = operator_alerts(&store).await;
        assert_eq!(
            alerts,
            vec![(
                "op-npub-hex".to_string(),
                "refund_parked".to_string(),
                "ref-order:sub-1".to_string()
            )],
            "exactly one RefundParked alert to the configured recipient, keyed on the refund id"
        );
    }

    // urw.1 site 2 (commit_park_failed): a structural park (a legacy NULL destination that can never
    // be paid) parks FAILED on the first drive and enqueues EXACTLY ONE RefundParked operator alert.
    #[tokio::test]
    async fn structural_park_enqueues_one_operator_alert() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", None, Some(500)).await;
        seed_reservation(&store, "sub-1").await;

        let report = refunder_with_alerts(&store, &payment, &clock, "op-npub-hex")
            .drive()
            .await
            .unwrap();
        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "structural park never calls pay");

        let alerts = operator_alerts(&store).await;
        assert_eq!(alerts.len(), 1, "exactly one alert on the structural park");
        assert_eq!(alerts[0].0, "op-npub-hex");
        assert_eq!(alerts[0].1, "refund_parked");
        assert_eq!(alerts[0].2, "ref-order:sub-1");
    }

    // urw.1 finding 2 (codex): a refund whose pay keeps failing AMBIGUOUSLY (Pending/Unknown,
    // allow_terminal=false) never reaches the cap and never parks, so it bypassed the resolution-stuck
    // path. Past the stuck threshold it must still emit a RefundStuck alert (cooldown-collapsed to one
    // per 6h). Drive once well past the threshold and assert exactly one RefundStuck.
    #[tokio::test]
    async fn pay_stuck_ambiguous_refund_emits_refund_stuck_alert() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_fail(true);
        payment.set_failed_status(PayStatus::Pending);
        // created_at is 0 (seed_refund); a clock past the stuck threshold makes the row "stuck".
        let clock = TestClock::new(RESOLUTION_STUCK_ALERT_S + 1);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        seed_reservation(&store, "sub-1").await;
        let r = refunder_with_alerts(&store, &payment, &clock, "op-npub-hex");

        let report = r.drive().await.unwrap();
        assert_eq!(report.retried, 1, "ambiguous pay stays PENDING/retried, never parks");
        assert_eq!(report.failed, 0);

        let alerts = operator_alerts(&store).await;
        assert_eq!(
            alerts,
            vec![(
                "op-npub-hex".to_string(),
                "refund_stuck".to_string(),
                "ref-order:sub-1".to_string()
            )],
            "a pay-stuck refund past the threshold emits exactly one RefundStuck alert"
        );

        // Cooldown collapses the repeat: a second drive within 6h adds no new alert row.
        clock.set(RESOLUTION_STUCK_ALERT_S + 2);
        r.drive().await.unwrap();
        assert_eq!(
            operator_alerts(&store).await.len(),
            1,
            "the recurring stuck alert is cooldown-collapsed within 6h"
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

    // Upgrade safety (lnrent-4gt): a PRE-ug8 binary paid a bolt11 refund under the BARE
    // `refund:<external_id>` key and crashed before recording SENT. Gen 0 IS that bare key now, so the
    // legacy bolt11 refund takes the NORMAL gen-0 path and the `already_paid` fast-skip honors the
    // Succeeded on the SAME key: the row records SENT with NO second pay and NO resolution.
    #[tokio::test]
    async fn legacy_stable_key_paid_before_upgrade_is_not_double_paid() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.mark_paid("refund:order:sub-1"); // the OLD binary paid the bare gen-0 key, pre-crash
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        let bolt11 = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(&bolt11), Some(500)).await;
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

    // INV-3 (spec §3.3): a NULL refund amount (provision recorded no order amount) has no determinable
    // received figure, so it can no longer be auto-paid. It parks FAILED instead of refunding a guessed
    // 0 (the pre-hardening behavior); the seeded provenance invoice also has a NULL amount.
    #[tokio::test]
    async fn null_amount_parks_failed_under_inv3() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), None).await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "a NULL-amount refund never pays");
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["failed".to_string()]
        );
    }

    // ---- the generation gate (lnrent-ug8 codex P0 fix) ----------------------

    // A bolt11 `dest` is paid DIRECTLY under the gen-0 key, which is the BARE `refund:<external_id>`
    // (NOT `:g0`) — the SAME key a pre-ug8 binary paid bolt11 refunds under (lnrent-4gt). No
    // resolution, the resolver is never called, and resolution_gen stays 0.
    #[tokio::test]
    async fn bolt11_dest_pays_generation_zero_under_bare_key() {
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
            payment.was_paid("refund:order:sub-1"),
            "paid under the BARE gen-0 key (not `:g0`)"
        );
        assert!(
            !payment.was_paid("refund:order:sub-1:g0"),
            "gen 0 never suffixes `:g0`"
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

    #[tokio::test]
    async fn injected_resolver_result_is_paid_for_ln_address_refund() {
        struct RecordingResolver {
            bolt11: String,
            calls: Mutex<Vec<(String, u64, i64)>>,
        }

        #[async_trait]
        impl RefundResolver for RecordingResolver {
            async fn resolve(
                &self,
                dest: &str,
                owed_msat: u64,
                now: i64,
            ) -> Result<Resolved, ResolveError> {
                self.calls
                    .lock()
                    .unwrap()
                    .push((dest.to_string(), owed_msat, now));
                Ok(Resolved {
                    bolt11: self.bolt11.clone(),
                    expiry: 10_000,
                })
            }
        }

        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        let bolt11 = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400);
        let resolver = Arc::new(RecordingResolver {
            bolt11: bolt11.clone(),
            calls: Mutex::new(Vec::new()),
        });
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

        let report = refunder_with(&store, &payment, &clock, resolver.clone())
            .drive()
            .await
            .unwrap();

        assert_eq!(report.sent, 1);
        {
            let calls = resolver.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0.as_str(), LN_ADDR);
            assert_eq!(calls[0].1, 500_000);
            assert_eq!(calls[0].2, 1_000);
        }
        assert_eq!(
            payment.paid_dest("refund:order:sub-1:g1").as_deref(),
            Some(bolt11.as_str()),
            "payment backend receives the resolved bolt11, not the raw LN-address"
        );
        assert_ne!(
            payment.paid_dest("refund:order:sub-1:g1").as_deref(),
            Some(LN_ADDR)
        );
        let (resolved, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(resolved.as_deref(), Some(bolt11.as_str()));
        assert_eq!(gen, 1);
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

    // Upgrade safety (lnrent-4gt): a PRE-ug8 binary submitted a bolt11 refund pay under the BARE
    // `refund:<external_id>` key and it is STILL in flight (Pending) when the new binary boots. Gen 0
    // IS that bare key now, so the new binary must NOT defer-and-strand it — it RE-ENTERS pay() on the
    // SAME bare key, the refund reaches SENT, and a second drive is a no-op (the row is SENT) — no
    // double-pay. NOTE (review P3): this asserts the gen-0 path takes the NORMAL pay flow (no
    // defer/strand), NOT the backend-level dedup of an in-flight op. The mock's status override
    // (Pending) and its `paid` map are independent, so `pay()` here simply records the bare-key
    // payment; the real dedup-against-an-in-flight-op guarantee belongs to the Fedimint backend and is
    // exercised in fedimint_live (lnrent-4gt PART 2), not by this mock.
    #[tokio::test]
    async fn legacy_bare_key_pending_bolt11_reenters_pay() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        payment.set_key_status("refund:order:sub-1", PayStatus::Pending); // legacy bolt11 pay in flight
        let clock = TestClock::new(1_000);
        let resolver = Arc::new(TestResolver::new(10_000));
        let bolt11 = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund(&store, "sub-1", Some(&bolt11), Some(500)).await;
        let r = refunder_with(&store, &payment, &clock, resolver.clone());

        // The gen-0 path re-enters pay() on the bare key (it does NOT defer and strand the refund).
        let report = r.drive().await.unwrap();
        assert_eq!(report.sent, 1);
        assert_eq!(payment.pay_calls(), 1, "re-enters pay() on the bare key");
        assert_eq!(resolver.calls(), 0, "a bolt11 dest is never resolved");
        assert!(
            payment.was_paid("refund:order:sub-1"),
            "paid under the bare gen-0 key the legacy binary used"
        );
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");

        // A second drive is a no-op: the row is SENT, so it is never re-listed and never double-paid.
        let report = r.drive().await.unwrap();
        assert_eq!(report, RefundReport::default(), "second drive is a no-op");
        assert_eq!(
            payment.pay_calls(),
            1,
            "never double-pays the legacy refund"
        );
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["sent".to_string()]
        );
    }

    // ---- INV-1 fee-deduction idempotency (spec §3.1 / §5) -------------------

    /// A fee-aware backend for the INV-1 cap/idempotency tests: `refund_net_sat` returns a configurable
    /// net cap and COUNTS its calls (so a test can prove a re-await never re-quotes), and
    /// `pay_refund_capped` records `key -> paid amount`, dedups on the key, and counts pay calls.
    #[derive(Default)]
    struct FeeQuotePayment {
        inner: Mutex<FeeQuoteState>,
    }
    #[derive(Default)]
    struct FeeQuoteState {
        net: u64,
        net_calls: usize,
        key_status: HashMap<String, PayStatus>,
        paid: HashMap<String, u64>, // gen key -> amount actually paid
        pay_calls: usize,
        seq: u64,
    }
    impl FeeQuotePayment {
        fn new(net: u64) -> Self {
            Self {
                inner: Mutex::new(FeeQuoteState {
                    net,
                    ..Default::default()
                }),
            }
        }
        fn set_net(&self, net: u64) {
            self.inner.lock().unwrap().net = net;
        }
        fn set_key_status(&self, key: &str, s: PayStatus) {
            self.inner
                .lock()
                .unwrap()
                .key_status
                .insert(key.to_string(), s);
        }
        fn net_calls(&self) -> usize {
            self.inner.lock().unwrap().net_calls
        }
        fn paid_amount(&self, key: &str) -> Option<u64> {
            self.inner.lock().unwrap().paid.get(key).copied()
        }
    }
    #[async_trait]
    impl PaymentBackend for FeeQuotePayment {
        async fn refund_net_sat(&self, _gross_sat: u64) -> Result<u64> {
            let mut st = self.inner.lock().unwrap();
            st.net_calls += 1;
            Ok(st.net)
        }
        async fn pay_refund_capped(
            &self,
            _bolt11: &str,
            amount_sat: u64,
            _gross_sat: u64,
            idempotency_key: &str,
        ) -> Result<String> {
            let mut st = self.inner.lock().unwrap();
            st.pay_calls += 1;
            if st.paid.contains_key(idempotency_key) {
                return Ok(format!("fee-pay-{idempotency_key}")); // idempotent on the key
            }
            let n = st.seq;
            st.seq += 1;
            st.paid.insert(idempotency_key.to_string(), amount_sat);
            Ok(format!("fee-pay-{n}"))
        }
        async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
            let st = self.inner.lock().unwrap();
            Ok(if st.paid.contains_key(idempotency_key) {
                PayStatus::Succeeded
            } else if let Some(s) = st.key_status.get(idempotency_key) {
                *s
            } else {
                PayStatus::Unknown
            })
        }
        async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String> {
            self.pay_refund_capped(dest, amount_sat, amount_sat, idempotency_key)
                .await
        }
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("refunder never receives")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!()
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            unimplemented!()
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!()
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!()
        }
    }

    // A PENDING/Unknown generation is re-awaited with its PERSISTED pay amount and NEVER re-quotes the
    // gateway, so a fee change between drives can neither re-price nor double-pay it. The persisted
    // gen-1 invoice is 500 sat; the fee then "rises" so a fresh quote would cap at 400, but the
    // in-flight gen-1 reuses 500 with no re-quote and no resolve.
    #[tokio::test]
    async fn inv1_pending_generation_reuses_persisted_amount_no_requote() {
        let store = mem_store();
        let payment = Arc::new(FeeQuotePayment::new(500));
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Pending); // gen-1 in flight
        let clock = TestClock::new(200);
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let b = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400); // 500 sat
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, &b, 10_000, 1).await;

        payment.set_net(400); // a FRESH quote would now cap lower; a re-await must ignore it.
        let report = Refunder::with_resolver(
            store.clone(),
            payment.clone(),
            Arc::new(clock.clone()),
            resolver.clone(),
        )
        .drive()
        .await
        .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(payment.net_calls(), 0, "a re-await never re-quotes the fee");
        assert_eq!(resolver.calls(), 0, "and never re-resolves");
        assert_eq!(
            payment.paid_amount("refund:order:sub-1:g1"),
            Some(500),
            "reused the persisted 500-sat invoice, not the new 400 cap"
        );
    }

    // Only a DEFINITE Failed+expired generation re-resolves, and THAT is the one place a fresh cap is
    // quoted — so a lower post-change fee yields a new lower-net invoice at the next generation.
    #[tokio::test]
    async fn inv1_failed_expired_generation_requotes_new_cap() {
        let store = mem_store();
        let payment = Arc::new(FeeQuotePayment::new(400)); // the new (lower) cap
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Failed);
        let clock = TestClock::new(200); // past the seeded expiry 100 -> expired
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let b = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400);
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, &b, 100, 1).await;

        let report = Refunder::with_resolver(
            store.clone(),
            payment.clone(),
            Arc::new(clock.clone()),
            resolver.clone(),
        )
        .drive()
        .await
        .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            payment.net_calls(),
            1,
            "a Failed+expired gen quotes a fresh cap"
        );
        assert_eq!(resolver.calls(), 1, "and re-resolves a new invoice");
        let (_, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(gen, 2, "generation bumped");
        assert_eq!(
            payment.paid_amount("refund:order:sub-1:g2"),
            Some(400),
            "paid the NEW 400-sat cap, not the old 500"
        );
    }

    /// The gateway operator RAISED the (otherwise static) fee, so the persisted invoice — quoted at the
    /// old fee (500 sat) — now exceeds the current 400-sat cap. Even though the invoice is NOT expired,
    /// retrying it would fail the INV-1 cap preflight forever; so re-query the current fee and re-resolve
    /// at the new 400-sat net, rather than parking the refund (codex final-gate P2 / operator guidance).
    #[tokio::test]
    async fn gen_failed_over_cap_reresolves_at_current_fee_even_unexpired() {
        let store = mem_store();
        let payment = Arc::new(FeeQuotePayment::new(400)); // the current (raised-fee) cap
        payment.set_key_status("refund:order:sub-1:g1", PayStatus::Failed);
        let clock = TestClock::new(200); // now=200 < the seeded expiry 10_000 -> NOT expired
        let resolver = Arc::new(TestResolver::new(10_000));
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let b = crate::refund_resolver::mint_bolt11(500_000, "meta", 1_000, 86_400); // 500 sat @ old fee
        seed_refund_resolved(&store, "sub-1", LN_ADDR, 500, &b, 10_000, 1).await;

        let report = Refunder::with_resolver(
            store.clone(),
            payment.clone(),
            Arc::new(clock.clone()),
            resolver.clone(),
        )
        .drive()
        .await
        .unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(
            resolver.calls(),
            1,
            "re-resolved at the new fee despite the invoice not being expired"
        );
        let (_, gen) = resolution_of(&store, "ref-order:sub-1").await;
        assert_eq!(gen, 2, "generation bumped to the new, cheaper invoice");
        assert_eq!(
            payment.paid_amount("refund:order:sub-1:g2"),
            Some(400),
            "paid the new 400-sat cap, not the stale over-cap 500"
        );
    }

    // Review P2: a below-cap direct bolt11 (400 sat) for a 500-sat gross refund pays 400, and the
    // "sent" DM reports the NET 400 delivered — NOT the 500 gross. The refund_attempt.amount_sat ledger
    // figure stays the gross 500 (AC-9: gross and net are distinct quantities).
    #[tokio::test]
    async fn sent_dm_reports_net_paid_not_gross() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let bolt11 = crate::refund_resolver::mint_bolt11(400_000, "meta", 1_000, 86_400); // 400 sat
        seed_refund(&store, "sub-1", Some(&bolt11), Some(500)).await; // gross 500
        seed_reservation(&store, "sub-1").await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.sent, 1);
        assert_eq!(payment.pay_calls(), 1);
        assert_eq!(
            refund_outbox(&store).await,
            vec![("sent".to_string(), 400)],
            "the DM reports the net 400 paid, not the 500 gross"
        );
        assert_eq!(
            refund_amount(&store, "ref-order:sub-1").await,
            Some(500),
            "the gross liability ledger figure is unchanged by the fee deduction"
        );
    }

    /// A gen-0 direct-bolt11 refund whose status is Unknown but whose op ACTUALLY STARTED (fedimint
    /// crash window) must be RE-AWAITED on the same key, NOT re-quoted. Even with the net cap now
    /// BELOW the fixed bolt11 — which a re-quote path would park FAILED — the in-flight payment is
    /// recovered and reaches SENT (codex P2: don't strand an in-flight refund on a gateway/fee change).
    #[tokio::test]
    async fn gen0_unknown_started_reawaits_without_requote() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        let bolt11 = crate::refund_resolver::mint_bolt11(400_000, "meta", 1_000, 86_400); // 400 sat
        seed_refund(&store, "sub-1", Some(&bolt11), Some(500)).await; // gross 500
        seed_reservation(&store, "sub-1").await;
        // Unknown but the op actually started; the cap has since dropped to 100 (< the 400 bolt11), so a
        // re-quote path would park FAILED for exceeding the cap.
        payment.set_key_status("refund:order:sub-1", PayStatus::Unknown);
        payment.set_started("refund:order:sub-1");
        payment.set_net_cap(100);

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(
            report.sent, 1,
            "the in-flight gen-0 Unknown was re-awaited, not re-quoted/parked"
        );
        assert!(
            payment.was_paid("refund:order:sub-1"),
            "paid on the same gen-0 key"
        );
    }

    // ---- INV-3 provenance guard (spec §3.3 / §5) ----------------------------

    /// Seed ONLY the PENDING refund_attempt row (NO provenance) — for the INV-3 guard tests.
    async fn seed_refund_no_provenance(
        store: &Store,
        sub_id: &str,
        dest: Option<&str>,
        amount: Option<i64>,
    ) {
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

    // A refund with NO received-payment provenance is parked FAILED, never paid.
    #[tokio::test]
    async fn inv3_no_provenance_parks_failed() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_no_provenance(&store, "sub-1", Some(LN_ADDR), Some(500)).await;

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "no provenance never pays");
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
        assert_eq!(
            refund_outbox_statuses(&store).await,
            vec!["failed".to_string()]
        );
    }

    // A refund whose row amount mismatches the received amount is parked FAILED (never pay a guessed
    // gross): the order received 500 but the refund row claims 400.
    #[tokio::test]
    async fn inv3_amount_mismatch_parks_failed() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_no_provenance(&store, "sub-1", Some(LN_ADDR), Some(400)).await;
        store
            .transaction(move |tx| {
                seed_paid_invoice_txn(tx, "order:sub-1", "sub-1", "PAID", Some(30), Some(500))?;
                Ok(())
            })
            .await
            .unwrap();

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(report.failed, 1);
        assert_eq!(payment.pay_calls(), 0, "an amount mismatch never pays");
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "FAILED");
    }

    // Provenance via settled_at on an already-terminal (EXPIRED) invoice — the LATE-settlement case
    // (capture stamps settled_at without flipping the invoice back to PAID). The refund IS paid.
    #[tokio::test]
    async fn inv3_late_terminal_settled_provenance_is_accepted() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_no_provenance(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        store
            .transaction(move |tx| {
                // status EXPIRED (not PAID) but settled_at set -> still received-payment provenance.
                seed_paid_invoice_txn(tx, "order:sub-1", "sub-1", "EXPIRED", Some(30), Some(500))?;
                Ok(())
            })
            .await
            .unwrap();

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(
            report.sent, 1,
            "settled_at provenance is accepted even when status != PAID"
        );
        assert_eq!(payment.pay_calls(), 1);
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
    }

    // Provenance via a settle-refund event_log entry for an unmatched/orphan settlement (no invoice
    // row). The refund IS paid against the journal's recorded amount.
    #[tokio::test]
    async fn inv3_settle_refund_journal_provenance_is_accepted() {
        let store = mem_store();
        let payment = Arc::new(TestPayment::new());
        let clock = TestClock::new(1_000);
        seed_sub(&store, "sub-1", "REFUND_DUE", "buyer-hex").await;
        seed_refund_no_provenance(&store, "sub-1", Some(LN_ADDR), Some(500)).await;
        store
            .transaction(move |tx| {
                let detail = serde_json::json!({
                    "external_id": "order:sub-1",
                    "amount_sat": 500,
                    "settled_at": 30,
                })
                .to_string();
                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                     VALUES (?1, 'settle_unmatched_refund', ?2, 0)",
                    params!["sub-1", detail],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let report = refunder(&store, &payment, &clock).drive().await.unwrap();

        assert_eq!(
            report.sent, 1,
            "a settle-refund journal entry is valid provenance"
        );
        let (status, _, _) = refund_row(&store, "ref-order:sub-1").await;
        assert_eq!(status, "SENT");
    }

    // INV-3 SOURCE AUDIT (spec §3.3): the ONLY production writers of refund_attempt are
    // capture.rs::refund_intent and provision.rs::RefundDueWrite. Any other production
    // `INTO refund_attempt` would let a refund row exist without going through the received-payment
    // path; this fails the build so a future writer must reuse a central site or update the spec + the
    // provenance guard intentionally. Test code (trailing `#[cfg(test)]` modules) and the schema /
    // migrations (CREATE/ALTER TABLE — no `INTO`) are excluded.
    #[test]
    fn only_known_sites_insert_refund_attempt() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
        let allowed = ["capture.rs", "provision.rs"];
        let mut offenders = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let src = std::fs::read_to_string(&path).unwrap();
            // Production code only — drop everything from the first `#[cfg(test)]` onward (every module
            // keeps its tests in a trailing `#[cfg(test)]` mod). `INTO refund_attempt` matches every
            // INSERT form but not CREATE/ALTER TABLE (no `INTO`).
            let production = src.split("#[cfg(test)]").next().unwrap_or("");
            let fname = path.file_name().unwrap().to_str().unwrap().to_string();
            if production.contains("INTO refund_attempt") && !allowed.contains(&fname.as_str()) {
                offenders.push(fname);
            }
        }
        assert!(
            offenders.is_empty(),
            "unexpected production `INTO refund_attempt` writer(s): {offenders:?}; only \
             capture.rs::refund_intent and provision.rs::RefundDueWrite may create refund rows (INV-3)"
        );
    }
}
