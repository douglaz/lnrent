//! Real **phoenixd** `PaymentBackend` (lnrent-xk3, ADR-0018) — the co-equal, non-federation money
//! path the backend `payment_backend=phoenixd` constructs. phoenixd is an ACINQ service the OPERATOR
//! runs and owns; lnrent is only an HTTP client of it (`reqwest`) and never supervises the process,
//! never holds its seed, and never assumes it is co-located. Compiles in BOTH cargo feature
//! configurations (it depends on nothing fedimint-gated), unlike [`crate::lnv2_backend`].
//!
//! ## The verified API contract (phoenixd 0.9.0, measured on a live mainnet node 2026-07-25)
//! Auth is HTTP BASIC with an EMPTY username and the api password as the secret (`curl -u ":$PW"`).
//! Only these endpoints exist here; anything not measured is a TODO, never a guess:
//!  - `POST /createinvoice` (form `amountSat`, `description`, `externalId`, `expirySeconds`) ->
//!    `{amountSat, paymentHash, serialized}`; `serialized` IS the bolt11 and `externalId` is
//!    lnrent's ADR-0009 correlation token verbatim. `expirySeconds` IS honoured (measured
//!    2026-07-25: a 60 s request produced `expiresAt - createdAt == 60_000` ms; omitting it gives
//!    phoenixd's 24 h default), so lnrent's own window is pushed down to the bolt11.
//!  - `POST /payinvoice` (form `invoice`) -> success `{recipientAmountSat, routingFeeSat, paymentId,
//!    paymentHash, paymentPreimage}`, or — for a bolt11 phoenixd ALREADY paid — HTTP **200** with
//!    `{paymentHash, reason:"this invoice has already been paid"}` and NO `paymentId`/preimage.
//!  - `GET /payments/outgoingbyhash/{hash}` -> the outgoing record, or **404 `Not found`** when the
//!    hash is unknown to phoenixd.
//!  - `GET /payments/incoming?externalId=<urlencoded>&all=true` -> an **ARRAY** of incoming records.
//!    (The `/payments/incomingbyexternalid` path in the b2f design does NOT exist — 404 Unknown
//!    endpoint.) **`all=true` is REQUIRED and load-bearing**: measured 2026-07-25, the same query
//!    WITHOUT it returns only RECEIVED payments — a just-created unpaid invoice comes back as `[]`,
//!    and so does an expired unpaid one. Since every OPEN-invoice answer here (`lookup`,
//!    `create_invoice`'s reuse check, the settlement poll's expiry retirement) is read from this
//!    array, omitting the flag makes each of them see "no such payment" for the entire life of an
//!    unpaid invoice. With the flag, unpaid records are returned with `isPaid:false` and `isExpired`
//!    flipping `true` once the window elapses (both measured).
//!  - `GET /getbalance` -> `{balanceSat, feeCreditSat, ...}`; `GET /getinfo` -> node info.
//!
//! ## UNITS — the live footgun
//! `requestedSat` / `receivedSat` / `sent` / `balanceSat` / `feeCreditSat` / `recipientAmountSat` /
//! `routingFeeSat` are **SAT**. `fees` is **MSAT** (measured: `fees=4480` msat = 4.48 sat on a
//! 120-sat pay). `routingFeeSat` is the true msat fee ROUNDED DOWN (4 vs 4.48), so it is not even
//! DESERIALIZED here — every exact fee figure comes from `fees` (msat), and there is no field a call
//! site could reach for by mistake. Every conversion goes through [`sat_to_msat`] and every binding is
//! `_sat`/`_msat` named.
//!
//! ## Money-safety facts this design is built on (all live-measured)
//! 1. **Receive is NET of fee, and the gap can be enormous.** Measured: `requestedSat=25000` but
//!    `receivedSat=2723` (`fees=22_277_000` msat — an ACINQ first-channel open). The wallet credit is
//!    `receivedSat`, NOT the invoice face value, so [`PaymentBackend::received_amount_msat`] reports
//!    `receivedSat * 1000` and NEVER the invoice amount. lnrent's money core already threads
//!    `received_msat` end to end (lnrent-3d5), so this needs no ledger work — but it does mean this
//!    backend must never answer `Ok(None)` there (that would make the caller book the GROSS, i.e.
//!    ~9x what actually arrived); it fails CLOSED with `Err` when the exact credit is unknown.
//!    `feeCreditSat` is EXCLUDED from the balance: it is an LSP fee credit, not spendable funds.
//!    ADR-0019's phoenixd caveat lives here too: a receive small enough that the LSP will not open a
//!    channel for it lands in `Part.FeeCredit`, which `receivedSat` still counts but `balanceSat`
//!    does not — booking it as liability would make an order refundable with no spendable credit
//!    behind it. phoenixd 0.9.0 offers NO per-payment attribution for that (the FULL live incoming
//!    record was re-measured 2026-07-25: `type, subType, paymentHash, preimage, externalId,
//!    description, invoice, isPaid, isExpired, requestedSat, receivedSat, fees, expiresAt,
//!    completedAt, createdAt` — no parts/fee-credit field), so the exclusion is implemented in the
//!    ADR's other permitted form: [`spendable_credit_msat`] REFUSES to book a receipt the wallet's
//!    own `feeCreditSat` could account for in FULL **while the spendable `balanceSat` cannot even
//!    cover it once**, and warns whenever a fee credit exists at all. Both conditions are load
//!    bearing — see that function for what the wallet-level signal can and cannot prove, and for why
//!    refusing on the fee-credit figure alone strands paid orders it has no evidence against.
//! 2. **phoenixd itself dedups a repeated same-bolt11 `payinvoice`** (the 200+`reason` shape above;
//!    the live probe's balance dropped exactly once). That is DEFENSE IN DEPTH, not a replacement
//!    for the local `phoenixd_pay` map: the map is what survives *lnrent's own* crash window and what
//!    recovery reads. A dedup answer for a key we already own is treated as SUCCESS-equivalent, but
//!    only after re-reading `outgoingbyhash` — the record, never the English `reason` string, is the
//!    authority on whether money moved.
//! 3. **Crash recovery works**: `outgoingbyhash` is queryable immediately after `payinvoice` returns,
//!    and an unknown hash is a clean 404. So a `PREPARED` key whose hash 404s provably never paid and
//!    MAY be retried; ANY other answer must never be retried blindly. That proof is about ONE
//!    wallet's history, so the `PREPARED` witness records `getinfo.nodeId` and the retry is gated on
//!    the same node still answering ([`PhoenixdPayment::require_prepared_node`]) — repointing
//!    `phoenixd_url` at another wallet, or restoring phoenixd onto a fresh payments DB, would
//!    otherwise turn "that node never paid it" into a second payment of a live destination.
//! 4. The outbound fee is a single-tier trampoline constant (4 sat + 0.4%), measured consistent at
//!    120 sat -> 4480 msat. phoenixd offers NO pre-quote and no max-fee parameter, so INV-1 is
//!    enforced by RESERVING that fee out of the receipt before paying (see [`FeeSchedule`]).
//!    Per ADR-0019 that schedule is **operator config with a version-verified default**, not a bare
//!    literal: the money path refuses to pay while the running phoenixd is not the release the
//!    schedule was verified against, and the operator clears that refusal by configuring a schedule
//!    they verified for their release (`[phoenixd] fee_schedule_version/fee_base_msat/fee_ppm`).
//!
//! ## Idempotency + crash story (the `phoenixd_pay` map)
//! `payinvoice` takes no idempotency parameter, so the durable local map is lnrent's own dedup:
//!  - The destination bolt11's **payment hash is derivable BEFORE the call** (it is encoded in the
//!    invoice), so — exactly like lnv2's deterministic attempt-0 operation id — we commit a
//!    `PREPARED` row containing `(bolt11, payment_hash)` BEFORE `POST /payinvoice`, and recovery
//!    resolves it through `outgoingbyhash` (fact 3).
//!  - **Crash before the POST lands** -> 404 -> the retry re-runs the whole preflight (INV-1
//!    included) and pays. **Crash after it landed** -> the record is present -> adopt it, never a
//!    second POST. **A transport error** is NOT proof the payment did not start (unlike lnv2's
//!    pre-funding `Retryable`), so the `PREPARED` row is NEVER deleted on one — deleting it is what
//!    would permit a double pay.
//!  - `FAILED` is recorded ONLY for a refusal that provably never POSTed (an expired destination,
//!    structural preflight, or the INV-1 cap). **Invariant: a `FAILED` row means no outbound payment
//!    was ever started under that key** — which is why
//!    [`PaymentBackend::failed_refund_can_reuse_invoice`] can be `true` here and a retry of a
//!    `FAILED` key simply re-runs the preflight.
//!
//! ## Cross-order same-invoice guard (ported [8A], lnrent-85t)
//! phoenixd dedups by payment hash across the WHOLE node, so if some other idempotency key already
//! owns a bolt11, adopting phoenixd's dedup answer as OUR key's success would silently under-refund
//! (two lnrent refunds credited to one real payment). Before funding we check the local map for a
//! DIFFERENT key holding this payment hash and refuse without paying. ADR-0018 already dissolves the
//! common source of this (raw bolt11 refund destinations are rejected at intake, resolver
//! destinations re-resolve per generation), so this is defense-in-depth for a residual — e.g. an
//! LNURL server handing back a reused invoice. Residual, deliberately accepted: a foreign payment of
//! the same bolt11 that is ABSENT from the local map is adopted as ours, because that is exactly the
//! shape a legitimate restore-from-backup takes and refusing it would strand a completed refund.
//! Second residual, also accepted: this refusal is deterministic in the colliding bolt11, and the
//! Refunder reuses a persisted invoice across a `Failed` retry (that is what
//! [`PaymentBackend::failed_refund_can_reuse_invoice`] `== true` buys), so a collided refund refuses
//! identically each drive until it parks `FAILED` with a `RefundStuck` alert instead of re-resolving
//! at a fresh generation. Nothing is paid and nothing is lost — this is the fail-closed direction —
//! and the operator is told; the alternative would be a per-key "this destination needs re-resolving"
//! signal, which the `PaymentBackend` trait does not carry and which belongs to the refund core, not
//! to a backend.
//!
//! ## Settlement observation (no websocket, by scope)
//! [`PaymentBackend::watch`] polls the locally indexed invoices through the verified
//! `incoming?externalId=&all=true` endpoint. It keeps polling an invoice past lnrent's own expiry
//! instant, because a late settlement must still reach capture so it can be refunded. What it does
//! NOT do is poll a row FOREVER — see [`poll_settlements_once`] for the two phoenixd-evidence exits
//! and the clock backstop that bound the poll to LIVE invoices, and for what still recovers a
//! settlement whose capture failed. The supervisor's `settlement_catch_up` remains a second,
//! independent poll for OPEN rows. No timestamp fidelity is lost: phoenixd reports the TRUE
//! settlement instant in `completedAt`. TODO (follow-up bead): a verified websocket listener could
//! replace this polling load, but no unverified endpoint is invented here.
//!
//! ## Deliberately NOT here (scoped to follow-up beads)
//! The `lnrent phoenixd-seed` helper + the phoenixd wallet's own backup/restore story (lnrent-kr1),
//! the phoenixd doctor/preflight probe (lnrent-5mi), the exact per-receipt fee-credit exclusion
//! (lnrent-itw), and the staging dogfood that gates operator-facing exposure and the go-live
//! runbook (lnrent-tof) are separate beads — all filed, none silently dropped. Index GC is also deferred: `phoenixd_invoice` grows one
//! row per created invoice, the same unpaid-order flood surface lnv2 grew a throttled reaper for
//! (lnrent-y4m.15/y4m.19). The poll reads only rows whose bolt11 can still be paid (an indexed
//! `expires_at` range, not a table scan) and retires within that set, so the steady-state poll cost
//! tracks LIVE invoices rather than the table; what GC still buys is disk. It is an operational
//! bound, not a money-safety one — no row here is authoritative
//! over money (phoenixd is), and the reaper's money-safety rules (never reap a row that can still
//! move funds) are already worked out there to port.
//!
use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::mpsc;

use crate::backends::{Invoice, PayStatus, PaymentBackend, PaymentStatus, Settlement};
use crate::clock::Clock;

/// The lnrent-owned sqlite index beside the operator state DB: the create-once
/// `external_id -> invoice` receive map plus the `idempotency_key -> (bolt11, payment_hash)` pay map.
/// phoenixd owns all the MONEY state (it is the wallet); this file holds only the correlations
/// phoenixd cannot answer for us.
const INDEX_DB_FILE: &str = "phoenixd_index.db";

/// Bound on an ordinary phoenixd API round-trip (reads, invoice creation).
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// `POST /payinvoice` is SYNCHRONOUS — it blocks until the payment resolves — so it gets a much
/// larger bound than the read endpoints. A timeout here is AMBIGUOUS, never a failure: the row stays
/// `PREPARED` and the next drive resolves it through `outgoingbyhash` (fact 3).
const PAY_TIMEOUT: Duration = Duration::from_secs(120);

/// Poll cadence for the push-compatible `watch()` seam. The poll keeps watching a locally expired
/// row for a while: a settlement that lands late must still reach capture (which refunds it).
const SETTLEMENT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How far past lnrent's OWN expiry instant the settlement poll keeps watching a row before retiring
/// it on the clock alone ([`poll_settlements_once`]). lnrent now pushes its window down to phoenixd
/// as `expirySeconds`, so a row is unpayable at phoenixd the moment its local window ends; this
/// margin is deliberately larger than phoenixd's 24 h DEFAULT window (measured) so that a row created
/// before that push-down — or under any clock skew between the two hosts — is still watched until
/// after the bolt11 it names can possibly be paid.
const SETTLEMENT_POLL_GRACE_SECS: i64 = 25 * 60 * 60;

/// phoenixd's outbound trampoline fee schedule: what INV-1 reserves against, since phoenixd exposes
/// NO pre-quote and NO max-fee parameter. ADR-0019 requires this to be **operator config with a
/// version-verified default** rather than a bare literal, and requires the money path to fail closed
/// — refusing automated refunds until the operator explicitly configures a schedule they verified —
/// when the running node is not that release. [`Self::default`] is the live-MEASURED phoenixd 0.9.0
/// schedule (120 sat payout -> `fees` 4480 msat = 4000 msat base + 480 msat proportional), and the
/// `[phoenixd] fee_schedule_version/fee_base_msat/fee_ppm` config block replaces all three together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeSchedule {
    /// The phoenixd release this schedule was verified against; compared with `getinfo.version`
    /// before every new payment and in the readiness seams.
    pub verified_version: String,
    pub base_msat: u64,
    pub ppm: u64,
}

impl Default for FeeSchedule {
    fn default() -> Self {
        Self {
            verified_version: "0.9.0".to_string(),
            base_msat: 4_000,
            ppm: 4_000, // 0.4%
        }
    }
}

impl FeeSchedule {
    /// phoenixd's outbound trampoline fee for a payout of `pay_msat`, in MSAT: `base + pay*ppm/1e6`.
    /// The proportional part rounds UP because this figure is a RESERVE — reserving more is the
    /// INV-1-safe direction — while the measured schedule is exact at the anchor (120 sat -> 4480
    /// msat).
    fn fee_msat(&self, pay_msat: u128) -> u128 {
        let prop = pay_msat
            .saturating_mul(u128::from(self.ppm))
            .div_ceil(1_000_000);
        prop.saturating_add(u128::from(self.base_msat))
    }

    /// The largest whole-sat payout whose payout PLUS the reserved trampoline fee still fits inside
    /// `gross_sat` — the INV-1 net cap (`docs/specs/refund-money-path-hardening.md` §3.1). Monotone
    /// in the payout, so a binary search finds the exact maximum. `0` means true dust: no positive
    /// whole-sat payout plus its fee fits.
    fn net_payout_sat(&self, gross_sat: u64) -> u64 {
        let cap_msat = sat_to_msat(gross_sat);
        let fits = |pay_sat: u64| -> bool {
            if pay_sat == 0 {
                return true;
            }
            let pay_msat = sat_to_msat(pay_sat);
            pay_msat.saturating_add(self.fee_msat(pay_msat)) <= cap_msat
        };
        let mut lo = 0u64;
        let mut hi = gross_sat;
        while lo < hi {
            // Upper mid (lo < mid <= hi): guarantees progress and avoids overflow at u64::MAX.
            let mid = hi - (hi - lo) / 2;
            if fits(mid) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        lo
    }

    /// Does the RUNNING phoenixd version match the release this schedule was verified against?
    ///
    /// `getinfo.version` carries a build suffix on a real node — the live 0.9.0 staging node reports
    /// `"0.9.0-b072567"` (MEASURED 2026-07-25) — so the comparison is on the release component
    /// (everything before the first `-`), with a whole-string match also accepted so an operator can
    /// pin one exact build. Anything else fails closed at the call sites.
    fn matches_running_version(&self, running: &str) -> bool {
        let release = running.split('-').next().unwrap_or(running);
        running == self.verified_version || release == self.verified_version
    }
}

const STATUS_PREPARED: &str = "PREPARED";
const STATUS_SUCCEEDED: &str = "SUCCEEDED";
const STATUS_FAILED: &str = "FAILED";

const INDEX_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS phoenixd_invoice (
    external_id   TEXT NOT NULL,
    invoice_id    TEXT PRIMARY KEY,
    bolt11        TEXT NOT NULL,
    payment_hash  TEXT NOT NULL,
    amount_sat    INTEGER NOT NULL,
    -- lnrent's OWN expiry window (see `create_invoice`), also pushed down to phoenixd as
    -- `expirySeconds`. lnrent judges the invoice by this local instant, never by a remote flag.
    expires_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS phoenixd_invoice_by_external_id ON phoenixd_invoice (external_id);
-- The settlement poll reads only rows whose bolt11 can still be paid, so its cost tracks LIVE
-- invoices rather than the never-GC'd table (`idx_pollable_invoices`).
CREATE INDEX IF NOT EXISTS phoenixd_invoice_by_expires_at ON phoenixd_invoice (expires_at);
CREATE TABLE IF NOT EXISTS phoenixd_pay (
    idempotency_key TEXT PRIMARY KEY,
    bolt11          TEXT NOT NULL,
    -- Derived from the bolt11 BEFORE the POST: the durable recovery witness `outgoingbyhash` reads.
    payment_hash    TEXT NOT NULL,
    -- The phoenixd WALLET the attempt was started against (`getinfo.nodeId`). Recovery reasons that
    -- a 404 proves this hash never paid, which only holds against that same wallet, so the identity
    -- is recorded with the witness. NULL = unknown identity, which recovery treats as unproven.
    node_id         TEXT,
    -- phoenixd's own uuid; known only once a payment record exists (NULL while PREPARED/FAILED).
    payment_id      TEXT,
    status          TEXT NOT NULL DEFAULT 'PREPARED', -- PREPARED | SUCCEEDED | FAILED
    terminal_at     INTEGER
);
CREATE INDEX IF NOT EXISTS phoenixd_pay_by_hash ON phoenixd_pay (payment_hash);
";

// ---------------------------------------------------------------------------------------------------
// Normalized phoenixd seam (`PhoenixdOps`)
//
// The money-critical logic (idempotency, the cross-key guard, INV-1 caps, crash recovery, the
// net-of-fee credit) lives in `PhoenixdPayment` and is unit-tested against a fake. ALL HTTP lives in
// `real::RealPhoenixdOps`, so the seam's types are plain data and no phoenixd service is needed to
// test the behaviors this bead mandates.
// ---------------------------------------------------------------------------------------------------

/// A freshly created phoenixd invoice (`POST /createinvoice`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhoenixdNewInvoice {
    pub(crate) amount_sat: u64,
    pub(crate) payment_hash: String,
    /// phoenixd's `serialized` field: the bolt11.
    pub(crate) bolt11: String,
}

/// One `GET /payments/incoming` record. `requested_sat` is the invoice face value; `received_sat` is
/// the WALLET CREDIT after phoenixd's receive-side fee (fact 1) — they differ, sometimes by ~9x.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhoenixdIncoming {
    pub(crate) payment_hash: String,
    pub(crate) bolt11: String,
    pub(crate) is_paid: bool,
    pub(crate) is_expired: bool,
    pub(crate) requested_sat: u64,
    pub(crate) received_sat: u64,
    /// Epoch MILLISECONDS (phoenixd's unit); lnrent uses SECONDS everywhere else.
    pub(crate) completed_at_ms: Option<i64>,
    /// The bolt11's OWN expiry instant, epoch MILLISECONDS, when phoenixd reports one (`expiresAt`;
    /// measured present on the live 0.9.0 incoming record — module header). Optional so a node that
    /// omits it degrades to the local fallback instead of failing to decode. Only used where lnrent
    /// has NO local window to preserve (`create_invoice`'s crash-window reuse of a paid record).
    pub(crate) expires_at_ms: Option<i64>,
}

/// One `GET /payments/outgoingbyhash/{hash}` record. `fees_msat` is phoenixd's `fees` field, which is
/// MSAT (`routingFeeSat` is the same figure rounded DOWN and is never used for accounting).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhoenixdOutgoing {
    pub(crate) payment_id: String,
    pub(crate) payment_hash: String,
    pub(crate) is_paid: bool,
    pub(crate) fees_msat: u64,
}

/// `GET /getbalance`. `fee_credit_sat` is tracked so it can be explicitly EXCLUDED from every
/// spendable figure — it is an LSP fee credit, not funds lnrent can pay out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhoenixdBalance {
    pub(crate) balance_sat: u64,
    pub(crate) fee_credit_sat: u64,
}

/// The fields `GET /getinfo` carries that the money path depends on. The fee reserve is pinned to one
/// live-verified phoenixd release, so callers must compare `version` before trusting that schedule;
/// `node_id` is the WALLET IDENTITY a `PREPARED` payment was started against (see the 404 recovery
/// branch in [`PhoenixdPayment::pay_inner`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhoenixdNodeInfo {
    pub(crate) node_id: String,
    pub(crate) version: String,
}

/// Normalized outcome of `POST /payinvoice` when phoenixd answered with a 2xx body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PayAttempt {
    /// A payment receipt: `paymentId` AND `paymentPreimage` present — money moved.
    Paid {
        payment_id: String,
        payment_hash: String,
    },
    /// A 2xx WITHOUT a receipt (no `paymentId`/`paymentPreimage`). The live-verified instance is the
    /// DUPLICATE shape (`reason: "this invoice has already been paid"`), but the English string is
    /// never the decision — the caller re-reads `outgoingbyhash`, which is the authority on whether
    /// money moved.
    NoReceipt { reason: Option<String> },
}

/// The exact operations `PhoenixdPayment` needs from a phoenixd node. Production impl:
/// [`real::RealPhoenixdOps`]; test impl: a scripted fake (see the tests module).
#[async_trait]
pub(crate) trait PhoenixdOps: Send + Sync {
    /// `POST /createinvoice`, with lnrent's own window pushed down as `expirySeconds` (measured
    /// honoured; see [`PhoenixdPayment::create_invoice`]).
    async fn create_invoice(
        &self,
        amount_sat: u64,
        description: &str,
        external_id: &str,
        expiry_s: u32,
    ) -> Result<PhoenixdNewInvoice>;
    /// `GET /payments/incoming?externalId=…&all=true` — an ARRAY (possibly empty). MUST include
    /// unpaid and expired records: `all=true` is what makes phoenixd return them (module header).
    async fn incoming_by_external_id(&self, external_id: &str) -> Result<Vec<PhoenixdIncoming>>;
    /// `POST /payinvoice`. `Err` = transport/HTTP failure: AMBIGUOUS, never proof of non-payment.
    async fn pay_invoice(&self, bolt11: &str) -> Result<PayAttempt>;
    /// `GET /payments/outgoingbyhash/{hash}`. `Ok(None)` is phoenixd's clean 404 — the live-verified
    /// proof that this hash never paid (fact 3).
    async fn outgoing_by_hash(&self, payment_hash: &str) -> Result<Option<PhoenixdOutgoing>>;
    /// `GET /getbalance`.
    async fn balance(&self) -> Result<PhoenixdBalance>;
    /// `GET /getinfo` — an authenticated liveness and fee-schedule compatibility round-trip.
    async fn node_info(&self) -> Result<PhoenixdNodeInfo>;
}

// ---------------------------------------------------------------------------------------------------
// Pure money helpers (unit-tested without a phoenixd)
// ---------------------------------------------------------------------------------------------------

/// The ONE sat -> msat conversion in this module (widened to `u128` so a gross receipt can never
/// overflow the arithmetic below). Named so no call site can silently mix the two units.
fn sat_to_msat(sat: u64) -> u128 {
    u128::from(sat) * 1000
}

/// SAT -> MSAT for trait DTOs whose historical field type is `u64`. Saturating is fail-safe for a
/// nonsensically large remote value: it cannot wrap into a small spendable/liability figure.
fn sat_to_msat_u64(sat: u64) -> u64 {
    sat.saturating_mul(1000)
}

/// By how much a payout's ACTUAL total outlay exceeded its ceiling, or `None` when it fit. Used after
/// the fact (the money already moved) to detect a phoenixd fee schedule that no longer matches the
/// configured/pinned [`FeeSchedule`]. `actual_fees_msat` is phoenixd's `fees` field — MSAT, never
/// `routingFeeSat`.
fn inv1_overrun_msat(
    payout_msat: u128,
    actual_fees_msat: u64,
    ceiling_msat: Option<u128>,
) -> Option<u128> {
    let ceiling_msat = ceiling_msat?;
    let outlay_msat = payout_msat.saturating_add(u128::from(actual_fees_msat));
    outlay_msat
        .checked_sub(ceiling_msat)
        .filter(|over| *over > 0)
}

/// How much of a PAID receipt the wallet's own fee credit could account for. phoenixd's fee credit
/// is the LSP crediting a receive it would not open a channel for: `receivedSat` counts it, but
/// `balanceSat` does not, so it is NOT spendable (ADR-0019's phoenixd caveat).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreditBacking {
    /// The wallet reports no fee credit at all, so nothing of this receipt can be sitting in it.
    FullyBacked,
    /// Fee credit exists, so up to `fee_credit_sat` of this receipt might be sitting in it — but the
    /// wallet's SPENDABLE balance covers the receipt anyway, so a refund of it can actually be paid.
    /// Booked with a warning: the over-book is bounded by `fee_credit_sat`.
    UnattributedButPayable { fee_credit_sat: u64 },
    /// The fee credit could account for this receipt IN FULL and the spendable balance cannot even
    /// cover it once: booking it would create refundable liability the wallet demonstrably cannot
    /// pay. This is ADR-0019's "reject a fee-credit-only receipt" case.
    UnbackedAndUnpayable {
        fee_credit_sat: u64,
        balance_sat: u64,
    },
}

/// Classify a receipt against the WALLET-level `getbalance` snapshot (both figures come from the one
/// response, so they are consistent with each other).
///
/// Both conditions are required to refuse, and each is doing distinct work. `fee_credit_sat >=
/// received_sat` is only a NECESSARY condition for "this receive landed entirely in fee credit" —
/// phoenixd 0.9.0 exposes no per-payment attribution, so a standing fee credit larger than a small
/// order's receipt says nothing about whether THAT receipt reached the channel. Refusing on it alone
/// would wedge every paid order smaller than the operator's standing fee credit (no capture, no
/// refund, and reconcile cannot expire a `Paid` invoice either), which loses the buyer's money in the
/// far more common false-positive case. `balance_sat < received_sat` is what makes the refusal earn
/// its cost: there, booking the receipt would promise a refund the wallet has no funds for at all.
fn credit_backing(received_sat: u64, fee_credit_sat: u64, balance_sat: u64) -> CreditBacking {
    if fee_credit_sat == 0 {
        CreditBacking::FullyBacked
    } else if fee_credit_sat >= received_sat && balance_sat < received_sat {
        CreditBacking::UnbackedAndUnpayable {
            fee_credit_sat,
            balance_sat,
        }
    } else {
        CreditBacking::UnattributedButPayable { fee_credit_sat }
    }
}

/// The msat credit a PAID incoming record may be booked as refundable liability — `receivedSat` in
/// msat — or `Err` when the verified API cannot show that credit is spendable.
///
/// This is ADR-0019's phoenixd rule ("the liability baseline must exclude fee credit, or reject a
/// fee-credit-only receipt") in the only form phoenixd 0.9.0 supports. The FULL live incoming record
/// was re-measured 2026-07-25 and carries no parts/fee-credit breakdown, so there is nothing to
/// subtract per receipt; the only fee-credit signal the node exposes is the WALLET-level
/// `getbalance`. So the rejection form is used, on BOTH figures of that one snapshot:
///  - `feeCreditSat == 0`: nothing is unbacked; book `receivedSat`.
///  - fee credit exists, but `balanceSat >= receivedSat`: whatever this receipt is made of, the
///    wallet holds at least that much SPENDABLE, so a refund of it can be paid. Book it and WARN —
///    the over-book is bounded by `feeCreditSat`.
///  - `feeCreditSat >= receivedSat` AND `balanceSat < receivedSat`: this receipt could be fee credit
///    in full and there are not even spendable funds to refund it. Refused (the fail-closed
///    direction the rest of this module takes). The caller retries, so the operator sees a
///    repeating, explicit reason rather than a silent liability with no funds behind it; the remedy
///    is to fund the wallet or let phoenixd convert the fee credit into liquidity.
///
/// Why the refusal is NOT taken on the fee-credit test alone, which is what an "exclude fee credit"
/// reading would suggest: a refusal here has no escape. `received_amount_msat` and the settlement
/// poll both hold the receipt back, `settlement_catch_up` leaves the invoice OPEN, and reconcile
/// will not expire an invoice the backend reports `Paid` — so a receipt refused here means the buyer
/// paid and gets neither service nor refund until the wallet-level condition clears. Since the
/// wallet-level fee-credit test cannot ATTRIBUTE credit, it fires for receipts that provably reached
/// the channel too (any paid order smaller than a standing fee credit), and that false positive is
/// buyer money stuck. Requiring the balance condition as well confines the refusal to the case where
/// booking would promise a refund the wallet cannot pay at all — and in that case the refusal is not
/// the only guard either: a refund that outruns the spendable balance fails at `payinvoice` and
/// raises the existing `RefundStuck` operator alert rather than quietly overdrawing.
///
/// LIMIT, stated rather than papered over: this is a wallet-level test, so it cannot ATTRIBUTE
/// credit in either direction. A fee-credit receive whose credit was already consumed by a channel
/// open before lnrent observed the settlement reads as `FullyBacked` and is booked; a funded wallet
/// books a receipt that may have been fee credit, over-booking by at most that receipt. Closing that
/// needs a per-payment attribution field phoenixd does not have — tracked on lnrent-itw (measure a
/// live fee-credit receive, then exclude it exactly).
async fn spendable_credit_msat(
    ops: &Arc<dyn PhoenixdOps>,
    invoice_id: &str,
    record: &PhoenixdIncoming,
) -> Result<u64> {
    let balance = ops.balance().await.with_context(|| {
        format!(
            "reading the phoenixd balance to check invoice {invoice_id}'s credit against the \
             wallet's fee credit"
        )
    })?;
    match credit_backing(
        record.received_sat,
        balance.fee_credit_sat,
        balance.balance_sat,
    ) {
        CreditBacking::FullyBacked => {}
        CreditBacking::UnattributedButPayable { fee_credit_sat } => tracing::warn!(
            invoice_id,
            received_sat = record.received_sat,
            fee_credit_sat,
            balance_sat = balance.balance_sat,
            "phoenixd holds a non-spendable LSP fee credit; up to that much of lnrent's booked \
             receipts may not be backed by spendable funds (ADR-0019). Booking this receipt because \
             the spendable balance covers it, so a refund of it can still be paid."
        ),
        CreditBacking::UnbackedAndUnpayable {
            fee_credit_sat,
            balance_sat,
        } => {
            tracing::error!(
                invoice_id,
                received_sat = record.received_sat,
                fee_credit_sat,
                balance_sat,
                "phoenixd's fee credit is at least this receipt and the spendable balance is below \
                 it, so the receipt may have landed ENTIRELY in fee credit — which `receivedSat` \
                 counts but `balanceSat` does not — and there are no spendable funds to refund it. \
                 Refusing to book it as refundable liability (ADR-0019). Fund the wallet (or let \
                 phoenixd convert the fee credit) and the retry books it."
            );
            bail!(
                "phoenixd invoice {invoice_id} received {} sat while the wallet holds {} sat of \
                 non-spendable fee credit and only {} sat spendable, so its spendable credit is \
                 UNKNOWN and could not be refunded; refusing to book it",
                record.received_sat,
                fee_credit_sat,
                balance_sat
            );
        }
    }
    Ok(sat_to_msat_u64(record.received_sat))
}

/// Which incoming record a repeated `create_invoice(external_id)` must REUSE (ADR-0009 create-once):
/// a PAID one first — money already arrived, so a second invoice for that order must never be minted
/// — then any unexpired unpaid one. Expired unpaid records are skipped so a lapsed order can be
/// re-invoiced.
fn select_reusable_incoming(records: &[PhoenixdIncoming]) -> Option<&PhoenixdIncoming> {
    records
        .iter()
        .find(|r| r.is_paid)
        .or_else(|| records.iter().find(|r| !r.is_expired))
}

/// The record for the EXACT invoice lnrent bound to an external id. phoenixd returns an array per
/// `externalId` (a lapsed-then-reissued order legitimately has several), so status is always read
/// from the payment hash in the local index — never from "the first element".
fn find_incoming_by_hash<'a>(
    records: &'a [PhoenixdIncoming],
    payment_hash: &str,
) -> Option<&'a PhoenixdIncoming> {
    records.iter().find(|r| r.payment_hash == payment_hash)
}

/// phoenixd reports its instants (`completedAt`, `expiresAt`) in epoch MILLISECONDS; every lnrent
/// timestamp is unix SECONDS. `None` (or a nonsensical value) yields `None`, which the settlement
/// contract reads as "the true time is unknown" and the supervisor caps conservatively — never a
/// fabricated instant.
fn epoch_secs_from_ms(epoch_ms: Option<i64>) -> Option<i64> {
    epoch_ms.filter(|ms| *ms > 0).map(|ms| ms / 1000)
}

/// lnrent's local invoice id for a phoenixd invoice. Deterministic in the payment hash, so a crash
/// between phoenixd's create and the local row write regenerates the SAME id on the retry.
fn invoice_id_for(payment_hash: &str) -> String {
    format!("phoenixd-{payment_hash}")
}

/// Map a stored `phoenixd_pay.status` to the trait [`PayStatus`]. `PREPARED` is `Pending`: the row is
/// the durable record of an attempt that may or may not have landed — exactly the trait's "can be
/// neither confirmed nor refuted" state, which recovery resolves via `outgoingbyhash`.
fn map_pay_status(status: Option<String>) -> PayStatus {
    match status.as_deref() {
        Some(STATUS_SUCCEEDED) => PayStatus::Succeeded,
        Some(STATUS_FAILED) => PayStatus::Failed,
        Some(STATUS_PREPARED) => PayStatus::Pending,
        _ => PayStatus::Unknown,
    }
}

/// The INV-1 ceiling a pay must respect, in msats (`None` = no cap: the legacy `pay()` path).
#[derive(Debug, Clone, Copy)]
enum PayCap {
    None,
    Gross(u64),
    Outlay(u128),
}

impl PayCap {
    fn ceiling_msat(self) -> Option<u128> {
        match self {
            PayCap::None => None,
            PayCap::Gross(gross_sat) => Some(sat_to_msat(gross_sat)),
            PayCap::Outlay(msat) => Some(msat),
        }
    }
}

/// The destination invoice, parsed ONCE per pay attempt: its payment hash is the durable recovery
/// witness, its encoded amount is what phoenixd will actually send, and its absolute expiry prevents
/// a proven-never-paid recovery attempt from reposting a bolt11 that lapsed during the outage.
struct ParsedDest {
    payment_hash: String,
    amount_msat: Option<u64>,
    expires_at: i64,
}

fn parse_dest(bolt11: &str) -> Result<ParsedDest> {
    let invoice = lightning_invoice::Bolt11Invoice::from_str(bolt11)
        .context("parsing the phoenixd refund destination bolt11")?;
    Ok(ParsedDest {
        payment_hash: invoice.payment_hash().to_string(),
        amount_msat: invoice.amount_milli_satoshis(),
        expires_at: invoice
            .expires_at()
            .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX),
    })
}

// ---------------------------------------------------------------------------------------------------
// The backend
// ---------------------------------------------------------------------------------------------------

/// The phoenixd payment backend: the HTTP operations seam, the lnrent-owned correlation index, the
/// registered settlement sender, and a clock for expiry windows and terminal stamps.
pub struct PhoenixdPayment {
    ops: Arc<dyn PhoenixdOps>,
    index: Arc<Mutex<Connection>>,
    clock: Arc<dyn Clock>,
    /// The trampoline schedule INV-1 reserves against, and the phoenixd release it was verified on
    /// (ADR-0019: a version-verified default the operator can replace for their own release).
    fee_schedule: FeeSchedule,
    /// Serializes `create_invoice`'s check -> create -> insert so two concurrent same-`external_id`
    /// callers cannot both create a phoenixd invoice.
    create_lock: tokio::sync::Mutex<()>,
    /// Serializes the pay check -> POST -> record critical section so two concurrent same-key callers
    /// cannot both reach `payinvoice` before either recorded its `PREPARED` witness. Bounded: every
    /// call it spans carries an explicit HTTP timeout ([`HTTP_TIMEOUT`] / [`PAY_TIMEOUT`]).
    ///
    /// Unlike lnv2 — which funds under the lock and then awaits the terminal WITHOUT it — phoenixd's
    /// `payinvoice` is a single synchronous call that both starts and completes the payment, so the
    /// lock necessarily spans the whole payment. Consequence, accepted: the Refunder's worker pool
    /// (lnrent-26b) parallelizes destination RESOLUTION but its phoenixd pays drain one at a time.
    /// That is throughput, not correctness, and it is what lnrent-26b already assumes the backend's
    /// pay lock provides; a per-key lock registry would only be worth its mechanism if refund volume
    /// ever made serial pays the bottleneck.
    pay_start_lock: tokio::sync::Mutex<()>,
}

impl PhoenixdPayment {
    /// Open the backend against an operator-managed phoenixd at `url` (already transport-validated by
    /// `config::validate_phoenixd_url`: https, or http to loopback only), authenticating with
    /// `api_password`. Creates/opens the lnrent-owned index under the data dir (0600, symlink-refused).
    /// Does NOT contact phoenixd — a node that is momentarily down must not wedge daemon startup; the
    /// readiness seams (`backend_ready` / `refund_gateway_ready`) report reachability instead.
    pub fn open(
        url: &str,
        api_password: &str,
        fee_schedule: FeeSchedule,
        data_dir: &Path,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let ops = real::RealPhoenixdOps::new(url, api_password)?;
        let index_path = data_dir.join(INDEX_DB_FILE);
        crate::fedimint_paths::prepare_private_file(&index_path, "phoenixd lnrent index db")
            .context("preparing the phoenixd index db path")?;
        let conn = Connection::open(&index_path).context("opening phoenixd index db")?;
        conn.execute_batch(INDEX_SCHEMA)
            .context("initialising phoenixd index schema")?;
        Ok(Self::with_ops(Arc::new(ops), conn, clock, fee_schedule))
    }

    /// Assemble a backend around an already-built ops seam + index connection (shared by the real
    /// constructor and the tests).
    fn with_ops(
        ops: Arc<dyn PhoenixdOps>,
        index: Connection,
        clock: Arc<dyn Clock>,
        fee_schedule: FeeSchedule,
    ) -> Self {
        Self {
            ops,
            index: Arc::new(Mutex::new(index)),
            clock,
            fee_schedule,
            create_lock: tokio::sync::Mutex::new(()),
            pay_start_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Fail closed unless the running node is the release whose trampoline fee schedule was verified
    /// (ADR-0019). This is a money-path preflight, independent of the out-of-scope doctor CLI.
    /// Returns the `getinfo` answer so a caller that also needs the node's IDENTITY (the `PREPARED`
    /// witness) does not pay for a second round-trip.
    async fn require_supported_version(&self) -> Result<PhoenixdNodeInfo> {
        let info = self
            .ops
            .node_info()
            .await
            .context("checking the phoenixd version against the verified trampoline fee schedule")?;
        if !self.fee_schedule.matches_running_version(&info.version) {
            bail!(
                "phoenixd version {} is not the {} release its trampoline fee schedule was verified \
                 against, so lnrent cannot bound an outbound fee on it; verify the schedule for this \
                 release and configure it ([phoenixd] fee_schedule_version/fee_base_msat/fee_ppm)",
                info.version,
                self.fee_schedule.verified_version
            );
        }
        Ok(info)
    }

    /// Is the phoenixd answering now the SAME WALLET the `PREPARED` attempt was started against?
    ///
    /// Recovery's central inference — "`outgoingbyhash` 404s, so this hash never paid, so retrying is
    /// a genuinely new attempt" (fact 3) — is a statement about ONE node's payment history. Point
    /// `phoenixd_url` at a different wallet, or restore phoenixd onto a fresh payments DB, and the
    /// 404 becomes "this OTHER node never paid it" while the original node may have paid it already:
    /// re-POSTing then is a double pay of a live refund destination. So the witness records
    /// `getinfo.nodeId` and this check gates the retry on it, failing CLOSED (the key stays PREPARED
    /// -> `Pending`, the Refunder re-awaits, and a permanently unresolvable one surfaces as
    /// `RefundStuck`) whenever the identity differs or was never recorded.
    async fn require_prepared_node(&self, idempotency_key: &str, row: &PayRow) -> Result<()> {
        let info = self.ops.node_info().await.context(
            "identifying the phoenixd node before retrying a payment its history does not show",
        )?;
        match row.node_id.as_deref() {
            Some(prepared) if prepared == info.node_id => Ok(()),
            Some(prepared) => bail!(
                "phoenixd pay key {idempotency_key} was prepared against node {prepared} but node \
                 {} is answering now, so that node's 404 for hash {} is no evidence about the \
                 earlier attempt; refusing to pay again",
                info.node_id,
                row.payment_hash
            ),
            None => bail!(
                "phoenixd pay key {idempotency_key} has no recorded node identity, so a 404 for \
                 hash {} cannot prove the earlier attempt never paid; refusing to pay again",
                row.payment_hash
            ),
        }
    }

    /// The incoming records for the invoice `invoice_id` names, plus the payment hash lnrent bound to
    /// it. `Ok(None)` = we have no such invoice.
    async fn incoming_for_invoice(
        &self,
        invoice_id: &str,
    ) -> Result<Option<(InvoiceRow, Vec<PhoenixdIncoming>)>> {
        let Some(row) = idx_get_by_invoice_id(&self.index, invoice_id)? else {
            return Ok(None);
        };
        let records = self
            .ops
            .incoming_by_external_id(&row.external_id)
            .await
            .context("looking up the phoenixd incoming payment for an lnrent invoice")?;
        Ok(Some((row, records)))
    }

    /// The pay/refund core. Idempotent on `idempotency_key` via the durable `phoenixd_pay` map,
    /// INV-1-capped per `cap`, crash-safe through `outgoingbyhash` (module header).
    async fn pay_inner(
        &self,
        bolt11: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
    ) -> Result<String> {
        // The whole critical section is serialized: unlike lnv2 there is no second dedup layer we
        // control, so two concurrent callers must never both reach `payinvoice` for one key.
        let _guard = self.pay_start_lock.lock().await;
        match pay_get(&self.index, idempotency_key)? {
            Some(row) if row.status == STATUS_SUCCEEDED => {
                // Idempotent: never re-pay. The stored phoenixd payment id IS the backend payment id
                // every caller keys on — `payment_status` looks a row up `WHERE payment_id = ?`, so
                // substituting the payment hash for a missing id would report `Unknown` for a
                // completed payment. Unreachable by construction (`pay_mark_succeeded` always writes
                // one), so fail closed rather than hand back an id nothing can resolve.
                row.payment_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "phoenixd pay key {idempotency_key} is SUCCEEDED with no payment id; its \
                         payment cannot be identified"
                    )
                })
            }
            Some(row) if row.status == STATUS_PREPARED => {
                // Crash / ambiguity recovery. `outgoingbyhash` is the sole authority (fact 3).
                match self.ops.outgoing_by_hash(&row.payment_hash).await? {
                    Some(record) if record.is_paid => {
                        self.adopt_paid(idempotency_key, &record, amount_sat, cap)
                    }
                    // MEASUREMENT GAP, stated honestly: the live probe measured the 404 and the
                    // paid/dedup shapes, NOT what `outgoingbyhash` returns for an outgoing payment
                    // that definitively FAILED (route/liquidity). So an unpaid record is only
                    // provably "not paid YET" — it may equally be a terminal failure. Classifying it
                    // as failed on an unmeasured field would be a guess whose cost is a DOUBLE PAY
                    // (re-paying a payment that was still in flight); lnrent's ambiguous-pay
                    // discipline (lnrent-y4m.16) is the opposite — stay pending, never a second pay,
                    // surface it. The key therefore reports `Pending`, the Refunder re-awaits it
                    // without ever re-quoting or minting a new payment hash, and a permanently
                    // ambiguous one becomes a `RefundStuck` operator alert rather than a silent
                    // livelock. TODO (lnrent-tof, needs the live node): measure the failed-outgoing
                    // shape and, IF it is unambiguous, resolve it to `FAILED` here — which is also
                    // what would make `failed_refund_can_reuse_invoice`'s retry apply to a real
                    // payment failure and not only to the preflight refusals below.
                    Some(_) => bail!(
                        "phoenixd pay for key {idempotency_key} has an outgoing record for hash {} \
                         that is not (yet) paid; leaving it in flight rather than paying again \
                         (phoenixd's failed-payment record shape is not yet measured, so this can \
                         be neither confirmed nor refuted here)",
                        row.payment_hash
                    ),
                    // A clean 404 PROVES this hash never paid AT THE NODE THAT ANSWERED — which is
                    // proof about OUR attempt only while that is still the same wallet
                    // (`require_prepared_node`). Once it is, the retry is a genuinely new attempt and
                    // must re-run the full preflight, INV-1 included.
                    None => {
                        self.require_prepared_node(idempotency_key, &row).await?;
                        self.start_pay(bolt11, amount_sat, idempotency_key, cap)
                            .await
                    }
                }
            }
            Some(row) if row.status == STATUS_FAILED => {
                // FAILED is only ever a refusal that provably never POSTed (module header invariant),
                // and phoenixd can re-pay the same bolt11, so a retry re-runs the whole preflight.
                // This is what makes `failed_refund_can_reuse_invoice() == true` actually progress.
                self.start_pay(bolt11, amount_sat, idempotency_key, cap)
                    .await
            }
            Some(row) => bail!(
                "phoenixd pay key {idempotency_key} has invalid persisted status {:?}",
                row.status
            ),
            None => {
                self.start_pay(bolt11, amount_sat, idempotency_key, cap)
                    .await
            }
        }
    }

    /// Fund a NEW phoenixd payment for `idempotency_key`. The caller has PROVEN no live attempt exists
    /// for this key ([7A]): either there is no row, or recovery just saw a clean 404 for its hash, or
    /// the row is a FAILED refusal that never POSTed. Every refusal below is therefore safe to record
    /// as terminal `FAILED` — it can never race a live payment.
    async fn start_pay(
        &self,
        bolt11: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
    ) -> Result<String> {
        // An unparseable destination cannot yield a payment hash, so there is no recovery witness to
        // record and nothing was sent. Return a plain error WITHOUT a row (the key stays `Unknown`,
        // which the Refunder re-awaits). Practically unreachable: the resolver parses every bolt11
        // before persisting it (lnrent-ug8) and intake rejects raw bolt11 destinations.
        let dest = parse_dest(bolt11)?;
        let payout_msat = sat_to_msat(amount_sat);

        // ORDERING (money-safety): EVERY refusal below runs BEFORE the `PREPARED` witness is
        // committed, and the witness goes in immediately before the POST. A refusal path must never
        // leave a PREPARED row naming this hash even transiently: `PREPARED` means "a POST may have
        // landed", so recovery resolves it by ADOPTING whatever `outgoingbyhash` reports — and if the
        // hash belongs to another key's payment (the [8A] case below), a crash in that window would
        // make recovery credit that payment to this key, which is exactly what the guard forbids.

        // An invoice can expire while a PREPARED attempt is ambiguous. Once recovery has obtained a
        // clean outgoingbyhash 404, retrying that expired bolt11 cannot pay and a transport/HTTP
        // rejection after PREPARED would become ambiguous again. Refuse it definitively before
        // writing another recovery witness or issuing a second POST.
        if self.clock.now() >= dest.expires_at {
            return self.refuse(
                idempotency_key,
                bolt11,
                &dest.payment_hash,
                format!(
                    "phoenixd refund pay refused for key {idempotency_key}: destination invoice \
                     expired at {} (now {})",
                    dest.expires_at,
                    self.clock.now()
                ),
            );
        }

        // Structural amount preflight. `payinvoice` sends the invoice's ENCODED amount, but the cap
        // below is computed from `amount_sat`; a resolver/LNURL endpoint that returned an invoice for
        // a DIFFERENT (larger) amount would otherwise pass the cap and overspend. An AMOUNTLESS
        // invoice is refused for the same reason: the verified `payinvoice` form carries no
        // `amountSat`, so lnrent cannot bound what such a payment would send.
        match dest.amount_msat {
            Some(invoice_msat) if u128::from(invoice_msat) == payout_msat => {}
            Some(invoice_msat) => {
                return self.refuse(
                    idempotency_key,
                    bolt11,
                    &dest.payment_hash,
                    format!(
                        "phoenixd refund pay refused for key {idempotency_key}: invoice amount \
                         {invoice_msat} msat != owed {amount_sat} sat ({payout_msat} msat)"
                    ),
                )
            }
            None => {
                return self.refuse(
                    idempotency_key,
                    bolt11,
                    &dest.payment_hash,
                    format!(
                        "phoenixd refund pay refused for key {idempotency_key}: the destination \
                         invoice encodes no amount, and the verified payinvoice form cannot bound \
                         what an amountless payment would send"
                    ),
                )
            }
        }

        // [8A] cross-order guard: phoenixd dedups by payment hash node-wide, so paying a bolt11 that
        // ANOTHER key already owns would let us adopt someone else's payment as this refund's success
        // — a silent under-refund. Refuse before spending anything.
        if let Some(other_key) =
            pay_other_key_for_hash(&self.index, &dest.payment_hash, idempotency_key)?
        {
            tracing::error!(
                key = idempotency_key,
                other_key,
                "phoenixd: refund destination bolt11 is already owned by a DIFFERENT idempotency key \
                 ([8A] cross-order same-invoice collision) — refusing to pay so we can never credit \
                 one real payment to two lnrent refunds"
            );
            return self.refuse(
                idempotency_key,
                bolt11,
                &dest.payment_hash,
                format!(
                    "phoenixd refund pay refused for key {idempotency_key}: its destination invoice \
                     is already owned by idempotency key {other_key}"
                ),
            );
        }

        // INV-1: the total outlay (payout + the RESERVED trampoline fee, since phoenixd offers no
        // pre-quote) must fit the ceiling. Refusing here is what keeps a refund from ever costing
        // more than the receipt it refunds.
        if let Some(ceiling_msat) = cap.ceiling_msat() {
            let reserve_msat = self.fee_schedule.fee_msat(payout_msat);
            let outlay_msat = payout_msat.saturating_add(reserve_msat);
            if outlay_msat > ceiling_msat {
                return self.refuse(
                    idempotency_key,
                    bolt11,
                    &dest.payment_hash,
                    format!(
                        "phoenixd refund pay refused for key {idempotency_key}: payout {amount_sat} \
                         sat plus the reserved trampoline fee ({reserve_msat} msat) is \
                         {outlay_msat} msat, exceeding the INV-1 cap ({ceiling_msat} msat)"
                    ),
                );
            }
        }

        // The reserve above is safe only for the live-verified 0.9.0 fee schedule. Probe immediately
        // before the durable intent so a mismatch or probe error leaves neither PREPARED nor a
        // payinvoice POST behind. The same answer carries the node IDENTITY the witness is stamped
        // with, so recovery can tell "this node never paid it" from "a different node never paid it".
        let node = self.require_supported_version().await?;

        // Every refusal is behind us: commit the recovery witness, then POST. A crash between the two
        // leaves no row (Unknown -> the next drive re-runs this whole preflight); a crash after it
        // leaves the witness `outgoingbyhash` resolves.
        pay_upsert_prepared(
            &self.index,
            idempotency_key,
            bolt11,
            &dest.payment_hash,
            &node.node_id,
        )?;

        match self.ops.pay_invoice(bolt11).await {
            Ok(PayAttempt::Paid {
                payment_id,
                payment_hash,
            }) => {
                if payment_hash != dest.payment_hash {
                    // phoenixd reported a receipt for a hash we did not send. Do NOT mark a terminal
                    // for a payment we cannot identify; the row stays PREPARED so recovery re-reads
                    // OUR hash rather than binding this key to an unknown payment.
                    bail!(
                        "phoenixd payinvoice for key {idempotency_key} returned payment hash \
                         {payment_hash}, which is not the destination invoice's hash {}",
                        dest.payment_hash
                    );
                }
                pay_mark_succeeded(
                    &self.index,
                    idempotency_key,
                    &dest.payment_hash,
                    Some(&payment_id),
                    self.clock.now(),
                )?;
                self.audit_inv1(idempotency_key, &dest.payment_hash, payout_msat, cap)
                    .await;
                Ok(payment_id)
            }
            Ok(PayAttempt::NoReceipt { reason }) => {
                // The live-verified instance of this shape is phoenixd's own dedup of a bolt11 it
                // ALREADY paid (fact 2). Treat it as success-equivalent ONLY on the evidence of the
                // record — the `reason` string is logged as a hint, never trusted as the signal.
                let deduped_hint = reason
                    .as_deref()
                    .is_some_and(|r| r.to_ascii_lowercase().contains("already been paid"));
                tracing::warn!(
                    key = idempotency_key,
                    reason = reason.as_deref().unwrap_or("<none>"),
                    deduped_hint,
                    "phoenixd payinvoice returned no receipt; consulting outgoingbyhash for the truth"
                );
                match self.ops.outgoing_by_hash(&dest.payment_hash).await {
                    Ok(Some(record)) if record.is_paid => {
                        self.adopt_paid(idempotency_key, &record, amount_sat, cap)
                    }
                    // No paid record: nothing moved for this hash, but phoenixd also gave no proof
                    // that it never will. Stay PREPARED (Pending) so the next drive re-resolves it
                    // through `outgoingbyhash` — a 404 there unlocks a clean retry.
                    Ok(_) => bail!(
                        "phoenixd payinvoice for key {idempotency_key} returned no receipt \
                         (reason: {}) and no paid outgoing record; leaving the attempt in flight",
                        reason.as_deref().unwrap_or("<none>")
                    ),
                    Err(e) => Err(e).context(format!(
                        "re-reading the phoenixd outgoing record for key {idempotency_key} after a \
                         receipt-less payinvoice"
                    )),
                }
            }
            Err(e) => {
                // AMBIGUOUS. An HTTP/transport error is NOT proof the payment did not start (a
                // timeout on the synchronous `payinvoice` is the common case), so the PREPARED row is
                // NEVER deleted here — it is the witness the next drive resolves. Deleting it is
                // exactly what would permit a double pay.
                Err(e).context(format!(
                    "phoenixd payinvoice for key {idempotency_key} failed; the attempt is ambiguous \
                     and stays pending"
                ))
            }
        }
    }

    /// Record a terminal refusal that provably never POSTed ([7A]-safe; see [`Self::start_pay`]) and
    /// return it as an error. `FAILED` — not a silent `Unknown` — is what lets the Refunder advance
    /// to a fresh invoice generation instead of re-awaiting a payment that will never exist. Writes
    /// the terminal row DIRECTLY (never via `PREPARED`), which is what keeps a refused key from ever
    /// naming a hash recovery could adopt (see the ordering note in [`Self::start_pay`]).
    fn refuse(
        &self,
        idempotency_key: &str,
        bolt11: &str,
        payment_hash: &str,
        message: String,
    ) -> Result<String> {
        pay_upsert_failed(
            &self.index,
            idempotency_key,
            bolt11,
            payment_hash,
            self.clock.now(),
        )?;
        bail!(message)
    }

    /// Bind an EXISTING paid phoenixd payment to our key (crash recovery, or phoenixd's own dedup of
    /// a bolt11 we already POSTed). Never starts a payment.
    fn adopt_paid(
        &self,
        idempotency_key: &str,
        record: &PhoenixdOutgoing,
        amount_sat: u64,
        cap: PayCap,
    ) -> Result<String> {
        pay_mark_succeeded(
            &self.index,
            idempotency_key,
            &record.payment_hash,
            Some(&record.payment_id),
            self.clock.now(),
        )?;
        log_inv1_overrun(
            idempotency_key,
            sat_to_msat(amount_sat),
            record.fees_msat,
            cap,
        );
        Ok(record.payment_id.clone())
    }

    /// Best-effort post-pay INV-1 audit: phoenixd charges its fee AFTER the fact, so the only way to
    /// learn the real cost is to re-read the record's `fees` (MSAT). A fee above what was reserved
    /// means the node's schedule no longer matches the configured [`FeeSchedule`] and INV-1 was
    /// breached; the money has already moved, so this LOGS loudly and never re-drives the payment.
    async fn audit_inv1(
        &self,
        idempotency_key: &str,
        payment_hash: &str,
        payout_msat: u128,
        cap: PayCap,
    ) {
        match self.ops.outgoing_by_hash(payment_hash).await {
            Ok(Some(record)) => {
                log_inv1_overrun(idempotency_key, payout_msat, record.fees_msat, cap)
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(
                key = idempotency_key,
                error = %format!("{e:#}"),
                "phoenixd: could not re-read the outgoing record to audit the actual fee"
            ),
        }
    }
}

fn log_inv1_overrun(idempotency_key: &str, payout_msat: u128, fees_msat: u64, cap: PayCap) {
    if let Some(excess_msat) = inv1_overrun_msat(payout_msat, fees_msat, cap.ceiling_msat()) {
        tracing::error!(
            key = idempotency_key,
            fees_msat,
            excess_msat = %excess_msat,
            "phoenixd: the ACTUAL trampoline fee exceeded the reserved INV-1 cap — this refund cost \
             more than the receipt it refunds. The configured fee schedule no longer matches the \
             running phoenixd; stop automated refunds and reconcile."
        );
    }
}

#[async_trait]
impl PaymentBackend for PhoenixdPayment {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        // Serialize check -> create -> insert so two concurrent same-external_id callers can't both
        // create an invoice.
        let _guard = self.create_lock.lock().await;
        let cached = idx_get_invoice(&self.index, external_id)?;

        // Crash-window idempotency, and the reason this backend needs no receive oplog scan: phoenixd
        // INDEXES incoming payments by `externalId`, so an invoice created by a previous attempt that
        // died before the local row was written is recoverable — unlike lnv2, where each receive
        // draws a fresh tweak and cannot be looked up by our key at all.
        let existing = self
            .ops
            .incoming_by_external_id(external_id)
            .await
            .context("checking phoenixd for an existing invoice for this external id")?;
        let now = self.clock.now();
        let expires_at = now + i64::from(expiry_s);
        if let Some(inv) = cached {
            let Some(record) = find_incoming_by_hash(&existing, &inv.payment_hash) else {
                bail!(
                    "phoenixd no longer returns the cached invoice for external id {external_id} \
                     (hash {}); refusing to create a second invoice while the first payment state \
                     is unknown",
                    inv.payment_hash
                );
            };
            if record.requested_sat != amount_sat {
                bail!(
                    "phoenixd cached invoice for external id {external_id} requests {} sat, but \
                     {amount_sat} sat was asked for; refusing to reuse or replace it",
                    record.requested_sat
                );
            }
            if record.is_paid {
                // Preserve the ORIGINAL local expiry: completedAt determines whether capture treats
                // this payment as timely or late, and extending the window would change that fact.
                return Ok(inv);
            }
            if !record.is_expired {
                // phoenixd still reports the bolt11 payable, so a replay may safely reopen lnrent's
                // local window around this SAME invoice; creating a second live invoice would make a
                // payment racing this check ambiguous. Since `expirySeconds` now carries lnrent's
                // window down, the two windows end at the same instant (phoenixd's a hair later — it
                // counts from ITS create time), so this branch is the narrow skew case rather than
                // the routine one it was when phoenixd kept its own 24 h default. The reopened local
                // window can therefore outlast the bolt11: the cost is a reservation held until it
                // lapses, never a payment lnrent fails to see, and it is preferable to minting a
                // second invoice for an id phoenixd still says is payable.
                if now < inv.expires_at {
                    return Ok(inv);
                }
                let refreshed = Invoice { expires_at, ..inv };
                idx_upsert(&self.index, &refreshed)?;
                return Ok(refreshed);
            }
            // The exact cached invoice is definitively expired and unpaid at phoenixd. It cannot
            // move money now, so a successor may be created. The old invoice-id row remains in the
            // index so any still-OPEN state-store row can continue to resolve and expire normally.
        }
        if let Some(record) = select_reusable_incoming(&existing) {
            if record.requested_sat != amount_sat {
                // The external id is deterministic per invoice class (§6.6), so a stored invoice for a
                // DIFFERENT amount means lnrent and phoenixd disagree about this order. Minting a
                // second invoice under one external id would make settlement ambiguous, and reusing
                // one for the wrong amount would misbill — fail closed for the operator instead.
                bail!(
                    "phoenixd already holds an invoice for external id {external_id} requesting \
                     {} sat, but {amount_sat} sat was asked for; refusing to reuse or duplicate it",
                    record.requested_sat
                );
            }
            // A PAID record's window is HISTORY, not a window to reopen: the cached arm above
            // preserves the original precisely because the instant capture judges the settlement
            // against (timely vs late) must not move. There is no local row to preserve on THIS path
            // (it is the crash-window recovery one), so the original window is recovered from
            // phoenixd's own `expiresAt` when the node reports one, and only a node that does not
            // falls back to a fresh window. An UNPAID record keeps the fresh window — see the cached
            // arm's note on why reopening lnrent's window around a still-payable bolt11 is the safe
            // direction there.
            let expires_at = match (record.is_paid, epoch_secs_from_ms(record.expires_at_ms)) {
                (true, Some(original)) => original,
                _ => expires_at,
            };
            let inv = Invoice {
                id: invoice_id_for(&record.payment_hash),
                external_id: external_id.to_string(),
                backend_invoice_id: record.payment_hash.clone(),
                payment_hash: record.payment_hash.clone(),
                bolt11: record.bolt11.clone(),
                amount_sat: record.requested_sat,
                expires_at,
            };
            idx_upsert(&self.index, &inv)?;
            return Ok(inv);
        }

        // lnrent's own window is pushed DOWN to phoenixd as `expirySeconds` (MEASURED honoured on the
        // live 0.9.0 node; omitting it would leave the bolt11 payable for phoenixd's 24 h default,
        // far past the reservation lnrent released). `expires_at` still stores that window locally
        // because it is what `lookup` and reconcile/reservation release key on, and because lnrent
        // must keep judging the invoice by its OWN clock rather than by a remote flag.
        let created = self
            .ops
            .create_invoice(amount_sat, memo, external_id, expiry_s)
            .await
            .context("creating a phoenixd invoice")?;
        if created.amount_sat != amount_sat {
            bail!(
                "phoenixd createinvoice returned amountSat {}, but lnrent requested {amount_sat}; \
                 refusing to persist or bill the mismatched invoice",
                created.amount_sat
            );
        }
        let inv = Invoice {
            id: invoice_id_for(&created.payment_hash),
            external_id: external_id.to_string(),
            // phoenixd has no invoice id distinct from the payment hash.
            backend_invoice_id: created.payment_hash.clone(),
            payment_hash: created.payment_hash,
            bolt11: created.bolt11,
            amount_sat: created.amount_sat,
            expires_at,
        };
        // The create-once anchor: persist BEFORE returning, so the buyer never sees a bolt11 whose
        // correlation row is not durable.
        idx_upsert(&self.index, &inv)?;
        Ok(inv)
    }

    async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
        Ok(self.lookup_settlement(id).await?.0)
    }

    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        let Some((row, records)) = self.incoming_for_invoice(id).await? else {
            // FAIL CLOSED, not `Expired`. No path DELETES a `phoenixd_invoice` row (there is no
            // index GC — module header), so a missing row for an id lnrent's state DB still holds
            // is DIVERGENCE between the two files, never a lapsed invoice: the index is the only
            // thing that can map this id to its `externalId`, so the payment's true state is
            // UNKNOWN. Answering `Expired` would let reconcile expire an invoice phoenixd may have
            // been PAID — stranding the buyer's money with no capture and no refund. `Err` instead
            // makes every caller defer and retry (reconcile.rs:617/660/1240 and the supervisor's
            // settlement catch-up all treat it as "retry next tick"), so the divergence surfaces in
            // operator logs instead of silently expiring paid money. Same discipline, and the same
            // reason, as lnv2's `PAID_UNRECOVERED` bail (`lnv2_backend.rs:1074`) and
            // `received_amount_msat` below. NOTE for the deferred index-GC bead: a reaper that
            // deletes rows must keep any invoice reconcile can still `lookup` (y4m.15's rule), or
            // it turns this fail-closed arm into a permanent retry.
            bail!(
                "phoenixd has no index row for invoice {id}; its payment state is UNKNOWN (the \
                 lnrent index and state DB have diverged), so it must not be treated as expired"
            );
        };
        let Some(record) = find_incoming_by_hash(&records, &row.payment_hash) else {
            bail!(
                "phoenixd no longer returns invoice {id} (hash {}); its payment state is UNKNOWN, \
                 so it must not be treated as expired",
                row.payment_hash
            );
        };
        // phoenixd reports the TRUE settlement instant, so this is a genuine LIVE `Some(ts)` per
        // the trait contract (lnrent-zwk) — no conservative capping needed. A paid invoice stays
        // Paid regardless of lnrent's own expiry window.
        if record.is_paid {
            let settled_at = epoch_secs_from_ms(record.completed_at_ms).with_context(|| {
                format!("phoenixd reports invoice {id} paid without a valid completedAt timestamp")
            })?;
            Ok((PaymentStatus::Paid, Some(settled_at)))
        } else if self.clock.now() >= row.expires_at {
            Ok((PaymentStatus::Expired, None))
        } else {
            Ok((PaymentStatus::Open, None))
        }
    }

    async fn received_amount_msat(&self, invoice_id: &str) -> Result<Option<u64>> {
        // NEVER `Ok(None)` for phoenixd: the caller reads `None` as "the backend credited the gross
        // invoice amount", which on a receive that cost a channel open would over-credit by ~9x
        // (fact 1). When the exact credit cannot be read we fail CLOSED with `Err`, which settlement
        // catch-up treats as "skip and retry" rather than booking a guess.
        let Some((row, records)) = self.incoming_for_invoice(invoice_id).await? else {
            bail!("phoenixd has no invoice row for {invoice_id}; refusing to guess its credit");
        };
        match find_incoming_by_hash(&records, &row.payment_hash) {
            Some(record) if record.is_paid => {
                // The WALLET CREDIT is `receivedSat` — net of phoenixd's receive-side fee — not the
                // invoice's face value. This is the headline money-safety property of this backend.
                // `spendable_credit_msat` additionally refuses a receipt the wallet's fee credit
                // could account for in full (ADR-0019).
                Ok(Some(
                    spendable_credit_msat(&self.ops, invoice_id, record).await?,
                ))
            }
            _ => bail!(
                "phoenixd reports no paid incoming payment for invoice {invoice_id} (hash {}); \
                 refusing to book a credit that has not been observed",
                row.payment_hash
            ),
        }
    }

    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String> {
        // Legacy/no-context callers: no INV-1 ceiling. The Refunder/Sweeper use the capped variants.
        self.pay_inner(dest, amount_sat, idempotency_key, PayCap::None)
            .await
    }

    async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
        // Pure arithmetic against phoenixd's single-tier trampoline schedule — no node round-trip, so
        // this can never fail transiently the way an lnv2 gateway quote can.
        Ok(self.fee_schedule.net_payout_sat(gross_sat))
    }

    async fn refund_required_outlay_msat(
        &self,
        gross_sat: u64,
        pay_sat: Option<u64>,
    ) -> Result<u128> {
        let pay_sat = match pay_sat {
            Some(pay_sat) => pay_sat,
            None => self.fee_schedule.net_payout_sat(gross_sat),
        };
        if pay_sat == 0 {
            return Ok(0);
        }
        let payout_msat = sat_to_msat(pay_sat);
        Ok(payout_msat.saturating_add(self.fee_schedule.fee_msat(payout_msat)))
    }

    async fn pay_refund_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
    ) -> Result<String> {
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Gross(gross_sat),
        )
        .await
    }

    async fn pay_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        idempotency_key: &str,
    ) -> Result<String> {
        // The operator sweep passes its just-quoted outlay as the ceiling (urw.3).
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Outlay(max_outlay_msat),
        )
        .await
    }

    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus> {
        Ok(map_pay_status(pay_status_by_payment_id(
            &self.index,
            payment_id,
        )?))
    }

    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
        Ok(map_pay_status(pay_status_by_key(
            &self.index,
            idempotency_key,
        )?))
    }

    async fn payment_started_by_key(&self, idempotency_key: &str) -> Result<bool> {
        Ok(pay_status_by_key(&self.index, idempotency_key)?.is_some())
    }

    async fn available_balance_msat(&self) -> Result<Option<u64>> {
        let balance = self.ops.balance().await?;
        // `feeCreditSat` is DELIBERATELY excluded: it is an LSP fee credit, not spendable funds, and
        // counting it would authorize refunds/sweeps against money lnrent cannot actually send.
        Ok(Some(sat_to_msat_u64(balance.balance_sat)))
    }

    fn failed_refund_can_reuse_invoice(&self) -> bool {
        // TRUE, unlike lnv2. lnv2's `send` derives a deterministic attempt-0 operation from the
        // invoice, so once that reaches a terminal the SAME bolt11 can never be re-sent. phoenixd has
        // no such binding: it refuses only a bolt11 it has ALREADY PAID (fact 2), so re-attempting
        // the same invoice is legitimate. Scope note, so this answer is not read as more than it is:
        // every `FAILED` row this backend writes today is a PREFLIGHT REFUSAL that provably never
        // POSTed (module-header invariant), and `pay_inner` re-runs the full preflight for such a key
        // rather than bailing terminally — that is the retry this `true` unlocks. A payment that
        // actually failed AT phoenixd stays `Pending` instead (see the measurement gap in
        // `pay_inner`), so it never reaches this branch of the Refunder at all.
        true
    }

    async fn refund_gateway_ready(&self) -> Result<bool> {
        // phoenixd has no gateway concept; "can we price and pay a refund" reduces to "is the node
        // reachable, authenticating, and running the fee-compatible release". Fails CLOSED (`Err`),
        // which readiness treats as not-ready.
        self.require_supported_version().await.map(|_info| true)
    }

    async fn backend_ready(&self) -> Result<bool> {
        // An authenticated `getinfo` round-trip plus the money-path version compatibility check.
        self.require_supported_version().await.map(|_info| true)
    }

    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        let ops = self.ops.clone();
        let index = self.index.clone();
        let clock = self.clock.clone();
        tokio::spawn(async move {
            // Rows this task is done with (see `poll_settlements_once`). Process-local ON PURPOSE:
            // a restart re-arms every row, which is what re-delivers a settlement whose capture
            // never committed.
            let mut retired: HashSet<String> = HashSet::new();
            loop {
                if !poll_settlements_once(&ops, &index, &clock, &tx, &mut retired).await {
                    break;
                }
                tokio::select! {
                    () = tokio::time::sleep(SETTLEMENT_POLL_INTERVAL) => {}
                    () = tx.closed() => break,
                }
            }
        });
        Ok(rx)
    }
}

// ---------------------------------------------------------------------------------------------------
// sqlite index helpers (std::sync::Mutex; the lock never crosses an `.await`)
// ---------------------------------------------------------------------------------------------------

/// A `phoenixd_invoice` row: the correlation phoenixd cannot answer for us.
struct InvoiceRow {
    external_id: String,
    payment_hash: String,
    expires_at: i64,
}

fn idx_get_invoice(index: &Mutex<Connection>, external_id: &str) -> Result<Option<Invoice>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT external_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at
           FROM phoenixd_invoice
          WHERE external_id = ?1
          ORDER BY rowid DESC
          LIMIT 1",
        params![external_id],
        |r| {
            let payment_hash: String = r.get(3)?;
            Ok(Invoice {
                external_id: r.get(0)?,
                id: r.get(1)?,
                bolt11: r.get(2)?,
                backend_invoice_id: payment_hash.clone(),
                payment_hash,
                amount_sat: r.get::<_, i64>(4)? as u64,
                expires_at: r.get(5)?,
            })
        },
    )
    .optional()
    .context("reading phoenixd_invoice by external_id")
}

fn idx_get_by_invoice_id(
    index: &Mutex<Connection>,
    invoice_id: &str,
) -> Result<Option<InvoiceRow>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT external_id, payment_hash, expires_at FROM phoenixd_invoice WHERE invoice_id = ?1",
        params![invoice_id],
        |r| {
            Ok(InvoiceRow {
                external_id: r.get(0)?,
                payment_hash: r.get(1)?,
                expires_at: r.get(2)?,
            })
        },
    )
    .optional()
    .context("reading phoenixd_invoice by invoice_id")
}

fn idx_upsert(index: &Mutex<Connection>, inv: &Invoice) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO phoenixd_invoice
            (external_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(invoice_id) DO UPDATE SET
             external_id=excluded.external_id,
             bolt11=excluded.bolt11,
             payment_hash=excluded.payment_hash,
             amount_sat=excluded.amount_sat,
             expires_at=excluded.expires_at",
        params![
            inv.external_id,
            inv.id,
            inv.bolt11,
            inv.payment_hash,
            inv.amount_sat as i64,
            inv.expires_at,
        ],
    )
    .context("upserting phoenixd_invoice")?;
    Ok(())
}

/// A row needed by the polling `watch()` task. A row is NOT dropped the moment lnrent's own window
/// ends — a settlement that lands late must still reach capture — only once its bolt11 can no longer
/// be paid at all; see [`idx_pollable_invoices`] and [`poll_settlements_once`].
struct SettlementPollRow {
    external_id: String,
    invoice_id: String,
    payment_hash: String,
    amount_sat: u64,
}

/// The invoices whose bolt11 can still be paid: `expires_at >= watch_floor`, where the caller's floor
/// is `now - SETTLEMENT_POLL_GRACE_SECS` (the poll's clock backstop).
///
/// The predicate is IN THE QUERY, not in the loop, and that is what keeps the poll's per-cycle cost
/// proportional to LIVE invoices rather than to the whole never-GC'd table (`phoenixd_invoice` has no
/// reaper — module header). Filtering in the loop would still read, allocate and discard every
/// historical row every 5 s, forever, while holding the index lock the money path also takes.
/// `phoenixd_invoice_by_expires_at` makes it a range scan.
fn idx_pollable_invoices(
    index: &Mutex<Connection>,
    watch_floor: i64,
) -> Result<Vec<SettlementPollRow>> {
    let conn = index.lock().unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT external_id, invoice_id, payment_hash, amount_sat
               FROM phoenixd_invoice
              WHERE expires_at >= ?1",
        )
        .context("preparing the phoenixd settlement poll")?;
    let rows = stmt
        .query_map(params![watch_floor], |r| {
            Ok(SettlementPollRow {
                external_id: r.get(0)?,
                invoice_id: r.get(1)?,
                payment_hash: r.get(2)?,
                amount_sat: r.get::<_, i64>(3)? as u64,
            })
        })
        .context("querying phoenixd invoices for settlement polling")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("reading phoenixd invoices for settlement polling")?;
    Ok(rows)
}

/// Run one watch poll. Returns `false` only when the receiver has closed and the task should exit.
///
/// Two things bound the work. The QUERY ([`idx_pollable_invoices`]) only returns rows whose bolt11
/// can still be paid: past `expires_at + SETTLEMENT_POLL_GRACE_SECS` the invoice is unpayable at
/// phoenixd (lnrent pushes its window down as `expirySeconds`, and the grace exceeds phoenixd's 24 h
/// default window for any row created before that push-down), so there is no settlement left to
/// observe and the row is not read at all — which is what stops the never-GC'd index from growing the
/// per-cycle cost forever, even while phoenixd is unreachable or has stopped returning a record.
///
/// `retired` then holds the rows this task will not poll again inside that live set, on phoenixd's
/// own evidence:
///  - it was PAID and its settlement went into the channel — a paid incoming payment is terminal at
///    phoenixd, so every later poll of it could only re-send the same settlement; or
///  - phoenixd reports `isExpired` with no payment — the bolt11 can no longer move money, which is
///    the same fact `create_invoice` already relies on to mint a successor.
///
/// That matters because each cycle's cost is one SEQUENTIAL round-trip per live row — an unreachable
/// node costs `HTTP_TIMEOUT` apiece.
///
/// Retirement is IN MEMORY, so it costs nothing durable and heals by restart. The delivery it drops
/// is a redelivery, not the settlement: `tx.send` awaits channel capacity, so the settlement is
/// handed to the settlement loop before the row retires, and that loop logs loudly if `capture`
/// fails. Anything left uncaptured is picked up by the supervisor's `settlement_catch_up` (every
/// maintenance tick, for still-OPEN invoices) or by the full re-arm the next daemon start performs.
/// The alternative — re-emitting every paid row every 5s forever — grew one HTTP round-trip AND one
/// sole-writer store transaction per indexed invoice per cycle, which starves the money path long
/// before it heals anything.
async fn poll_settlements_once(
    ops: &Arc<dyn PhoenixdOps>,
    index: &Arc<Mutex<Connection>>,
    clock: &Arc<dyn Clock>,
    tx: &mpsc::Sender<Settlement>,
    retired: &mut HashSet<String>,
) -> bool {
    // The clock backstop, applied as the query's floor (see above): a row whose bolt11 lapsed this
    // long ago can never settle again, so it is never read, let alone round-tripped.
    let watch_floor = clock.now().saturating_sub(SETTLEMENT_POLL_GRACE_SECS);
    let rows = match idx_pollable_invoices(index, watch_floor) {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                error = %format!("{e:#}"),
                "phoenixd settlement poll could not read the local invoice index"
            );
            return true;
        }
    };
    for row in rows {
        if retired.contains(&row.invoice_id) {
            continue;
        }
        let records = match ops.incoming_by_external_id(&row.external_id).await {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(
                    external = %row.external_id,
                    error = %format!("{e:#}"),
                    "phoenixd settlement poll failed"
                );
                continue;
            }
        };
        let Some(record) = find_incoming_by_hash(&records, &row.payment_hash) else {
            continue;
        };
        if !record.is_paid {
            // phoenixd's OWN expiry flag (not lnrent's window): this bolt11 can never be paid now,
            // so there is no settlement left to observe for it.
            if record.is_expired {
                retired.insert(row.invoice_id);
            }
            continue;
        }
        let Some(settled_at) = epoch_secs_from_ms(record.completed_at_ms) else {
            tracing::warn!(
                external = %row.external_id,
                "phoenixd settlement poll found a paid invoice without a valid completedAt; \
                 refusing to invent a settlement timestamp"
            );
            continue;
        };
        // Identical fee-credit rule to `received_amount_msat` (ADR-0019): a receipt the wallet's
        // fee credit could account for in full is NOT emitted, so it never becomes liability with
        // no spendable funds behind it. The row stays un-retired, so the next cycle re-checks it.
        let received_msat = match spendable_credit_msat(ops, &row.invoice_id, record).await {
            Ok(received_msat) => received_msat,
            Err(e) => {
                tracing::warn!(
                    external = %row.external_id,
                    error = %format!("{e:#}"),
                    "phoenixd settlement poll is holding back a paid invoice whose spendable credit \
                     could not be established"
                );
                continue;
            }
        };
        let settlement = Settlement {
            invoice_id: row.invoice_id.clone(),
            external_id: row.external_id,
            amount_sat: row.amount_sat,
            received_msat,
            settled_at,
        };
        if tx.send(settlement).await.is_err() {
            return false;
        }
        // Handed off (the send awaited capacity), and phoenixd can never un-pay it.
        retired.insert(row.invoice_id);
    }
    true
}

/// A `phoenixd_pay` row.
struct PayRow {
    payment_hash: String,
    payment_id: Option<String>,
    status: String,
    /// The phoenixd wallet this attempt was started against ([`PhoenixdPayment::require_prepared_node`]).
    node_id: Option<String>,
}

fn pay_get(index: &Mutex<Connection>, key: &str) -> Result<Option<PayRow>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT payment_hash, payment_id, status, node_id
           FROM phoenixd_pay WHERE idempotency_key = ?1",
        params![key],
        |r| {
            Ok(PayRow {
                payment_hash: r.get(0)?,
                payment_id: r.get(1)?,
                status: r.get(2)?,
                node_id: r.get(3)?,
            })
        },
    )
    .optional()
    .context("reading phoenixd_pay by key")
}

fn pay_status_by_key(index: &Mutex<Connection>, key: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT status FROM phoenixd_pay WHERE idempotency_key = ?1",
        params![key],
        |r| r.get(0),
    )
    .optional()
    .context("reading phoenixd_pay status by key")
}

fn pay_status_by_payment_id(index: &Mutex<Connection>, payment_id: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT status FROM phoenixd_pay WHERE payment_id = ?1",
        params![payment_id],
        |r| r.get(0),
    )
    .optional()
    .context("reading phoenixd_pay status by payment id")
}

/// The FIRST other idempotency key holding this payment hash in a non-`FAILED` state ([8A]). A
/// `FAILED` row is excluded because the module-header invariant makes it "never POSTed" — it owns no
/// payment and must not block a legitimate one.
fn pay_other_key_for_hash(
    index: &Mutex<Connection>,
    payment_hash: &str,
    our_key: &str,
) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT idempotency_key FROM phoenixd_pay
          WHERE payment_hash = ?1 AND idempotency_key <> ?2 AND status <> 'FAILED'
          LIMIT 1",
        params![payment_hash, our_key],
        |r| r.get(0),
    )
    .optional()
    .context("checking phoenixd_pay for a cross-key payment-hash collision")
}

/// Commit the `PREPARED` witness before `payinvoice`. Re-preparing a `FAILED` key is allowed (that
/// row provably never POSTed); a `SUCCEEDED` row is CAS-protected so no path can ever walk a
/// completed payment back to in-flight.
fn pay_upsert_prepared(
    index: &Mutex<Connection>,
    key: &str,
    bolt11: &str,
    payment_hash: &str,
    node_id: &str,
) -> Result<()> {
    let conn = index.lock().unwrap();
    let changed = conn
        .execute(
            "INSERT INTO phoenixd_pay (idempotency_key, bolt11, payment_hash, node_id, status)
             VALUES (?1, ?2, ?3, ?4, 'PREPARED')
             ON CONFLICT(idempotency_key) DO UPDATE
                SET bolt11 = ?2, payment_hash = ?3, node_id = ?4, status = 'PREPARED',
                    payment_id = NULL, terminal_at = NULL
              WHERE phoenixd_pay.status <> 'SUCCEEDED'",
            params![key, bolt11, payment_hash, node_id],
        )
        .context("committing the phoenixd_pay PREPARED intent")?;
    if changed != 1 {
        bail!("phoenixd pay key {key} is already SUCCEEDED; refusing to re-prepare it");
    }
    Ok(())
}

/// Terminal SUCCESS, CAS-guarded on the payment hash so a stale caller cannot overwrite a row that
/// was rebound to a different destination.
fn pay_mark_succeeded(
    index: &Mutex<Connection>,
    key: &str,
    payment_hash: &str,
    payment_id: Option<&str>,
    terminal_at: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    let changed = conn
        .execute(
            "UPDATE phoenixd_pay SET status = 'SUCCEEDED', payment_id = ?3, terminal_at = ?4
              WHERE idempotency_key = ?1 AND payment_hash = ?2",
            params![key, payment_hash, payment_id, terminal_at],
        )
        .context("marking phoenixd_pay SUCCEEDED")?;
    if changed != 1 {
        // The CAS miss deliberately leaves the durable state untouched. In the expected recovery
        // mismatch case that is PREPARED -> Pending, so the caller cannot report success and no
        // later drive blindly issues a second POST.
        bail!(
            "phoenixd pay key {key} could not be marked SUCCEEDED for payment hash {payment_hash}; \
             the persisted recovery witness was not changed"
        );
    }
    Ok(())
}

/// Terminal refusal, written in ONE step so a refused key never passes through `PREPARED` (which
/// would invite recovery to adopt whatever `outgoingbyhash` says about that hash). Only ever called
/// where no payment was started ([7A]); CAS-guarded on the row NOT being `SUCCEEDED`, so a refusal
/// can never demote a completed payment.
fn pay_upsert_failed(
    index: &Mutex<Connection>,
    key: &str,
    bolt11: &str,
    payment_hash: &str,
    terminal_at: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO phoenixd_pay (idempotency_key, bolt11, payment_hash, status, terminal_at)
         VALUES (?1, ?2, ?3, 'FAILED', ?4)
         ON CONFLICT(idempotency_key) DO UPDATE
            SET bolt11 = ?2, payment_hash = ?3, status = 'FAILED', payment_id = NULL,
                terminal_at = ?4
          WHERE phoenixd_pay.status <> 'SUCCEEDED'",
        params![key, bolt11, payment_hash, terminal_at],
    )
    .context("recording a phoenixd_pay refusal")?;
    // The rows-changed count is deliberately NOT asserted here, unlike `pay_upsert_prepared`. The one
    // way this writes nothing is the CAS clause declining to demote a `SUCCEEDED` row — the money-safe
    // outcome — and the sole caller (`refuse`) returns its refusal error regardless, so there is no
    // path where a zero count is reported as success.
    Ok(())
}

// ---------------------------------------------------------------------------------------------------
// Production ops seam (the only place that speaks HTTP to phoenixd)
// ---------------------------------------------------------------------------------------------------

mod real {
    use std::fmt;

    use anyhow::{anyhow, Context, Result};
    use async_trait::async_trait;
    use serde::Deserialize;
    use url::Url;
    use zeroize::Zeroizing;

    use super::{
        PayAttempt, PhoenixdBalance, PhoenixdIncoming, PhoenixdNewInvoice, PhoenixdNodeInfo,
        PhoenixdOps, PhoenixdOutgoing, HTTP_TIMEOUT, PAY_TIMEOUT,
    };

    /// The live phoenixd HTTP client. Auth is HTTP BASIC with an EMPTY username and the api password
    /// as the secret, exactly as the verified contract (`curl -u ":$PW"`).
    pub(super) struct RealPhoenixdOps {
        client: reqwest::Client,
        base: Url,
        /// §13 secret: zeroized on drop, redacted in `Debug`, and only ever placed in an
        /// `Authorization` header — never in a URL, so a reqwest error's URL echo cannot leak it.
        api_password: Zeroizing<String>,
    }

    /// Manual `Debug` so the api password can never reach a log line or a panic message through a
    /// derived formatter.
    impl fmt::Debug for RealPhoenixdOps {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RealPhoenixdOps")
                .field("base", &self.base.as_str())
                .field("api_password", &"<redacted>")
                .finish()
        }
    }

    impl RealPhoenixdOps {
        pub(super) fn new(url: &str, api_password: &str) -> Result<Self> {
            let base = Url::parse(url).context("parsing the phoenixd base url")?;
            let client = reqwest::Client::builder()
                .timeout(HTTP_TIMEOUT)
                // The configured plaintext deployment is loopback-only. Ignore ambient proxy
                // variables so Basic credentials for that local service can never leave the host.
                .no_proxy()
                // Never follow a redirect: it would replay the `Authorization` header (the api
                // password) at whatever host the redirect names.
                .redirect(reqwest::redirect::Policy::none())
                // Force rustls/ring even if feature unification also enabled native-tls elsewhere.
                .use_rustls_tls()
                .build()
                .context("building the phoenixd HTTP client")?;
            Ok(Self {
                client,
                base,
                api_password: Zeroizing::new(api_password.to_string()),
            })
        }

        fn endpoint(&self, path: &str) -> Result<Url> {
            self.base
                .join(path)
                .with_context(|| format!("building the phoenixd {path} url"))
        }

        fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
            req.basic_auth("", Some(self.api_password.as_str()))
        }

        /// Send a request and decode its JSON body, mapping a non-2xx status to an error that carries
        /// the STATUS ONLY. The body is not folded into the error (it is persisted and surfaced as a
        /// refund's `last_error`), but it IS logged — see [`log_error_body`].
        async fn json<T: serde::de::DeserializeOwned>(
            &self,
            req: reqwest::RequestBuilder,
            what: &str,
        ) -> Result<T> {
            let resp = req
                .send()
                .await
                .with_context(|| format!("phoenixd {what} request failed"))?;
            let status = resp.status();
            if !status.is_success() {
                log_error_body(what, status, resp, self.api_password.as_str()).await;
                return Err(anyhow!("phoenixd {what} returned HTTP {status}"));
            }
            resp.json::<T>()
                .await
                .with_context(|| format!("decoding the phoenixd {what} response"))
        }
    }

    /// The `GET /payments/incoming` query for one external id.
    ///
    /// `all=true` is REQUIRED, not cosmetic (MEASURED 2026-07-25 on the live 0.9.0 node): without it
    /// phoenixd returns only RECEIVED payments, so a just-created unpaid invoice — and an expired
    /// unpaid one — come back as `[]`. Every OPEN-invoice answer in this backend is read from this
    /// array, so dropping the flag would make `lookup` report the payment state UNKNOWN for the whole
    /// life of an unpaid invoice. `query_pairs_mut` percent-encodes the external id, which is an
    /// opaque ADR-0009 token (colons and all).
    pub(super) fn incoming_by_external_id_url(base: &Url, external_id: &str) -> Url {
        let mut url = base.clone();
        url.query_pairs_mut()
            .append_pair("externalId", external_id)
            .append_pair("all", "true");
        url
    }

    /// How much of a phoenixd error body reaches the log. Bounded because it is remote-controlled
    /// text.
    const ERROR_BODY_LOG_LIMIT: usize = 512;

    /// What of a failed phoenixd call's response body may be logged, or `None` for "log nothing".
    ///
    /// lnrent puts the api password ONLY in an `Authorization` header, never in a body, URL, or
    /// query — but the body is written by whatever answered (phoenixd, or the operator's TLS
    /// terminator/proxy), and a debug-ish error page that echoes the request's headers would hand
    /// that credential straight to the operator's logs. §13 says the password never reaches a log
    /// line, so this does not rely on the upstream's good manners:
    ///  - any occurrence of the password itself is replaced (covers a decoded echo), and
    ///  - a body that looks like it carries an auth header at all — the encoded `Basic <base64>`
    ///    form, which no plaintext match could catch — is dropped WHOLE rather than truncated,
    ///    because a truncated credential is still a credential.
    ///
    /// Everything else is passed through (bounded), which is what keeps the operator's only signal
    /// for WHY a refund will not go out (no route vs insufficient balance vs offline channel).
    pub(super) fn loggable_error_body(body: &str, api_password: &str) -> Option<String> {
        let trimmed = body.trim();
        if trimmed.is_empty() {
            return None;
        }
        let lowered = trimmed.to_ascii_lowercase();
        if lowered.contains("authorization") || lowered.contains("basic ") {
            return None;
        }
        let redacted = if api_password.is_empty() {
            trimmed.to_string()
        } else {
            trimmed.replace(api_password, "<redacted>")
        };
        let mut snippet: String = redacted.chars().take(ERROR_BODY_LOG_LIMIT).collect();
        if snippet.len() < redacted.len() {
            snippet.push('…');
        }
        Some(snippet)
    }

    /// Log a failed phoenixd call's response body at `warn`, after [`loggable_error_body`] has
    /// removed anything that could be the api password. The body stays OUT of the returned error,
    /// which is persisted on the refund row; this is a log line only.
    async fn log_error_body(
        what: &str,
        status: reqwest::StatusCode,
        resp: reqwest::Response,
        api_password: &str,
    ) {
        let body = resp.text().await.unwrap_or_default();
        let Some(snippet) = loggable_error_body(&body, api_password) else {
            return;
        };
        tracing::warn!(
            endpoint = what,
            status = %status,
            body = %snippet,
            "phoenixd returned an error response"
        );
    }

    #[derive(Deserialize)]
    struct CreateInvoiceResponse {
        #[serde(rename = "amountSat")]
        amount_sat: u64,
        #[serde(rename = "paymentHash")]
        payment_hash: String,
        serialized: String,
    }

    #[derive(Deserialize)]
    struct IncomingResponse {
        #[serde(rename = "paymentHash")]
        payment_hash: String,
        invoice: String,
        #[serde(rename = "isPaid")]
        is_paid: bool,
        #[serde(rename = "isExpired")]
        is_expired: bool,
        #[serde(rename = "requestedSat")]
        requested_sat: u64,
        /// SAT — the actual wallet credit, net of phoenixd's receive fee.
        #[serde(rename = "receivedSat")]
        received_sat: u64,
        #[serde(rename = "completedAt")]
        completed_at: Option<i64>,
        /// The bolt11's own expiry, epoch MILLIS. `default` (not required): it is only used to
        /// recover a window lnrent lost, and a node that omits it must still decode.
        #[serde(rename = "expiresAt", default)]
        expires_at: Option<i64>,
    }

    #[derive(Deserialize)]
    struct OutgoingResponse {
        #[serde(rename = "paymentId")]
        payment_id: String,
        #[serde(rename = "paymentHash")]
        payment_hash: String,
        #[serde(rename = "isPaid")]
        is_paid: bool,
        /// MSAT (phoenixd's `fees`), NOT sat — `routingFeeSat` is the same figure rounded down.
        #[serde(rename = "fees")]
        fees: u64,
    }

    #[derive(Deserialize)]
    struct PayInvoiceResponse {
        #[serde(rename = "paymentId", default)]
        payment_id: Option<String>,
        #[serde(rename = "paymentHash", default)]
        payment_hash: Option<String>,
        #[serde(rename = "paymentPreimage", default)]
        payment_preimage: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    }

    #[derive(Deserialize)]
    struct BalanceResponse {
        #[serde(rename = "balanceSat")]
        balance_sat: u64,
        #[serde(rename = "feeCreditSat")]
        fee_credit_sat: u64,
    }

    #[derive(Deserialize)]
    struct NodeInfoResponse {
        /// phoenixd's own node id — the WALLET IDENTITY recovery pins a `PREPARED` attempt to.
        #[serde(rename = "nodeId")]
        node_id: String,
        version: String,
    }

    #[async_trait]
    impl PhoenixdOps for RealPhoenixdOps {
        async fn create_invoice(
            &self,
            amount_sat: u64,
            description: &str,
            external_id: &str,
            expiry_s: u32,
        ) -> Result<PhoenixdNewInvoice> {
            let url = self.endpoint("createinvoice")?;
            let form = [
                ("amountSat", amount_sat.to_string()),
                ("description", description.to_string()),
                ("externalId", external_id.to_string()),
                // MEASURED 2026-07-25 against the live 0.9.0 node: `expirySeconds` is accepted and
                // honoured EXACTLY (a 60 s request produced `expiresAt - createdAt == 60_000` ms).
                // Without it the bolt11 gets phoenixd's 24 h default, which would outlive lnrent's
                // reservation window; sending it makes the two windows the same instant.
                ("expirySeconds", expiry_s.to_string()),
            ];
            let resp: CreateInvoiceResponse = self
                .json(
                    self.authed(self.client.post(url)).form(&form),
                    "createinvoice",
                )
                .await?;
            Ok(PhoenixdNewInvoice {
                amount_sat: resp.amount_sat,
                payment_hash: resp.payment_hash,
                bolt11: resp.serialized,
            })
        }

        async fn incoming_by_external_id(
            &self,
            external_id: &str,
        ) -> Result<Vec<PhoenixdIncoming>> {
            let url = incoming_by_external_id_url(&self.endpoint("payments/incoming")?, external_id);
            let resp: Vec<IncomingResponse> = self
                .json(self.authed(self.client.get(url)), "payments/incoming")
                .await?;
            Ok(resp
                .into_iter()
                .map(|r| PhoenixdIncoming {
                    payment_hash: r.payment_hash,
                    bolt11: r.invoice,
                    is_paid: r.is_paid,
                    is_expired: r.is_expired,
                    requested_sat: r.requested_sat,
                    received_sat: r.received_sat,
                    completed_at_ms: r.completed_at,
                    expires_at_ms: r.expires_at,
                })
                .collect())
        }

        async fn pay_invoice(&self, bolt11: &str) -> Result<PayAttempt> {
            let url = self.endpoint("payinvoice")?;
            let form = [("invoice", bolt11.to_string())];
            let resp: PayInvoiceResponse = self
                .json(
                    self.authed(self.client.post(url))
                        // `payinvoice` blocks until the payment resolves.
                        .timeout(PAY_TIMEOUT)
                        .form(&form),
                    "payinvoice",
                )
                .await?;
            // A RECEIPT is `paymentId` AND `paymentPreimage`; their ABSENCE is the dedup/no-receipt
            // shape. The `reason` string is carried for the caller to log, never to decide on.
            match (resp.payment_id, resp.payment_preimage, resp.payment_hash) {
                (Some(payment_id), Some(_preimage), Some(payment_hash)) => Ok(PayAttempt::Paid {
                    payment_id,
                    payment_hash,
                }),
                _ => Ok(PayAttempt::NoReceipt {
                    reason: resp.reason,
                }),
            }
        }

        async fn outgoing_by_hash(&self, payment_hash: &str) -> Result<Option<PhoenixdOutgoing>> {
            let url = self.endpoint(&format!("payments/outgoingbyhash/{payment_hash}"))?;
            let resp = self
                .authed(self.client.get(url))
                .send()
                .await
                .context("phoenixd payments/outgoingbyhash request failed")?;
            // The live-verified "this hash never paid" answer: a 404 with a plain-text `Not found`
            // body (so the status must be checked BEFORE any JSON decode).
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let status = resp.status();
            if !status.is_success() {
                log_error_body(
                    "payments/outgoingbyhash",
                    status,
                    resp,
                    self.api_password.as_str(),
                )
                .await;
                return Err(anyhow!(
                    "phoenixd payments/outgoingbyhash returned HTTP {status}"
                ));
            }
            let record: OutgoingResponse = resp
                .json()
                .await
                .context("decoding the phoenixd payments/outgoingbyhash response")?;
            if record.payment_id.trim().is_empty() {
                return Err(anyhow!(
                    "phoenixd payments/outgoingbyhash returned an empty paymentId"
                ));
            }
            Ok(Some(PhoenixdOutgoing {
                payment_id: record.payment_id,
                payment_hash: record.payment_hash,
                is_paid: record.is_paid,
                fees_msat: record.fees,
            }))
        }

        async fn balance(&self) -> Result<PhoenixdBalance> {
            let url = self.endpoint("getbalance")?;
            let resp: BalanceResponse = self
                .json(self.authed(self.client.get(url)), "getbalance")
                .await?;
            Ok(PhoenixdBalance {
                balance_sat: resp.balance_sat,
                fee_credit_sat: resp.fee_credit_sat,
            })
        }

        async fn node_info(&self) -> Result<PhoenixdNodeInfo> {
            let url = self.endpoint("getinfo")?;
            let resp: NodeInfoResponse =
                self.json(self.authed(self.client.get(url)), "getinfo").await?;
            Ok(PhoenixdNodeInfo {
                node_id: resp.node_id,
                version: resp.version,
            })
        }
    }

    #[cfg(test)]
    mod decode_tests {
        use super::{IncomingResponse, OutgoingResponse};

        #[test]
        fn incoming_money_and_status_fields_are_required() {
            let valid = r#"{
                "paymentHash":"00",
                "invoice":"lnbc...",
                "isPaid":true,
                "isExpired":false,
                "requestedSat":25000,
                "receivedSat":2723,
                "completedAt":1753400000000
            }"#;
            let decoded: IncomingResponse = serde_json::from_str(valid).expect("verified shape");
            assert!(decoded.is_paid);
            assert_eq!(decoded.received_sat, 2_723);

            for malformed in [
                r#"{
                    "paymentHash":"00","invoice":"lnbc...","isExpired":false,
                    "requestedSat":25000,"receivedSat":2723,"completedAt":1753400000000
                }"#,
                r#"{
                    "paymentHash":"00","invoice":"lnbc...","isPaid":true,"isExpired":false,
                    "requestedSat":25000,"completedAt":1753400000000
                }"#,
            ] {
                assert!(
                    serde_json::from_str::<IncomingResponse>(malformed).is_err(),
                    "missing isPaid/receivedSat must not decode as unpaid or zero credit"
                );
            }
        }

        #[test]
        fn outgoing_payment_id_status_and_msat_fee_are_required() {
            let valid = r#"{
                "paymentId":"pay-1",
                "paymentHash":"00",
                "isPaid":true,
                "fees":4480
            }"#;
            let decoded: OutgoingResponse = serde_json::from_str(valid).expect("verified shape");
            assert_eq!(decoded.fees, 4_480, "fees is exact MSAT");

            for missing in [
                r#"{"paymentHash":"00","isPaid":true,"fees":4480}"#,
                r#"{"paymentId":"pay-1","paymentHash":"00","fees":4480}"#,
                r#"{"paymentId":"pay-1","paymentHash":"00","isPaid":true}"#,
            ] {
                assert!(serde_json::from_str::<OutgoingResponse>(missing).is_err());
            }
        }
    }
}

#[cfg(test)]
mod tests;
