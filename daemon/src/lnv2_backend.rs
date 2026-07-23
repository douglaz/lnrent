//! Real **lnv2** Fedimint `PaymentBackend` (lnrent-3d5, ADR-0018) — ecash receive + refund via the
//! `fedimint-lnv2-client` module, the backend `payment_backend=fedimint` constructs. This is the live
//! money path; the retired lnv1 backend was deleted by lnrent-8ym (ADR-0018). Feature-gated behind the
//! `fedimint` cargo feature (default ON). Daemon-only (rocksdb C++, glibc-dynamic); never compiled into
//! the wasm buyer.
//!
//! ## Why lnv2 needs far less machinery than lnv1 (ADR-0018)
//! lnv1 forced ~2k lines of crash-recovery into the daemon (oplog scan, the kum dead-op ledger, pay
//! stream classification) because its client leaked ambiguous non-final states and a wall-clock
//! restart trap. lnv2 removes those by DESIGN, so this backend is FINAL-STATE-DRIVEN:
//!  - **Send (pay/refund).** `LightningClientModule::send` derives a DETERMINISTIC operation id from
//!    the invoice, and `await_final_send_operation_state` returns a TRUTHFUL terminal —
//!    `Success` / `Refunded` / `Failure`. Crucially, both `Refunded` and `Failure` mean the
//!    DESTINATION WAS NOT PAID: the `Refunding -> Success` recheck inside the client (lib.rs:733-742)
//!    already re-derives `Success` for the sole misbehaving-gateway case where the gateway claimed the
//!    contract. So there is NO ambiguous-park machinery (that was lnv1's disease): a definitive
//!    failure maps straight to [`PayStatus::Failed`], and the Refunder re-resolves a FRESH invoice at
//!    the next generation (never the same bolt11 — see NO-RETRY below).
//!  - **Receive.** `receive` mints the invoice and `await_final_receive_operation_state` yields
//!    `Claimed` / `Expired` / `Failure`. A `Claimed` observed live stamps the true `settled_at`
//!    (lnrent-zwk); a `Claimed` first seen on a boot re-subscribe (settled while the daemon was down)
//!    stamps NULL, so the supervisor catch-up caps it conservatively. `Failure` is NOT expiry: upstream
//!    reaches it only after Lightning confirmation entered `Claiming` and mint output issuance failed,
//!    so lnrent persists `PAID_UNRECOVERED`, alerts, and fails lookup closed for manual recovery.
//!
//! ## Idempotency / index decision (bead INDEX DECISION, audit DURABLE IDEMPOTENCY DESIGN)
//! lnv2 op ids are deterministic from the invoice, so the lnv1-style op-mapping shrinks — but it does
//! not disappear, because two lnrent-owned facts are NOT reconstructible from the federation alone:
//!  - **Receive: a create-once `external_id -> invoice` map** (`lnv2_invoice`). Each `receive()` call
//!    draws a FRESH ephemeral tweak, so it is NOT idempotent on any external key — calling twice mints
//!    two invoices. lnrent's `create_invoice` idempotency (same `external_id` -> same invoice) is
//!    enforced by this local row, written BEFORE `create_invoice` returns. The crash window (receive
//!    committed the contract, the row not yet persisted) needs **no oplog scan**: `order_intake` only
//!    persists (and only ever shows the buyer) a bolt11 AFTER `create_invoice` returns, so a crash
//!    inside `create_invoice` leaves an orphaned, never-transmitted incoming contract that no one can
//!    pay — it simply expires. On the deterministic-`external_id` retry we mint a fresh invoice; the
//!    orphan is harmless. This is the lnv2 simplification the ADR promises.
//!  - **Pay: a durable `idempotency_key -> (bolt11, operation)` map** (`lnv2_pay`) with the commit
//!    ordering + crash story documented on [`Lnv2Payment::pay_inner`]. It is what makes `pay(key)`
//!    idempotent AND prevents lnv2's own `send()` from silently advancing to a fresh payment attempt on
//!    the same invoice after a definitive failure (`get_next_operation_id`, client lib.rs:667-698).
//!
//! There is deliberately **no dead-op ledger and no oplog recovery scan** (both were lnv1-only): lnv2
//! terminals are truthful, and the receive crash window is money-safe without a scan (above).
//!
//! ## [8A] cross-order same-invoice guard (ported lnrent-85t, bead AC)
//! On a `send()` dedup answer (`PaymentInProgress(op)` / `InvoiceAlreadyPaid(op)`) we VERIFY the op's
//! `custom_meta.lnrent_key` equals OUR key before adopting it. A different key on the same invoice
//! (a foreign order that resolved to the same bolt11) is a cross-order collision: adopting its outcome
//! would silently under-refund, so we NEVER record the foreign op as ours. Instead we park OUR key
//! FAILED under a sentinel op and return `Err` + warn: because lnv2 op ids are deterministic from the
//! invoice, our key can never own this bolt11's attempt-0, so the bolt11 is permanently unusable to us —
//! leaving the key `Unknown` would make the Refunder re-await the same dead invoice forever (stranding
//! the liability PENDING), whereas `Failed` unlocks a fresh-invoice re-resolution at the next generation.
//! ADR-0018 already dissolves the common source of this — raw bolt11 refund destinations are rejected
//! at intake (lnrent-hyg) and resolver destinations re-resolve per generation — so this guard is
//! defense-in-depth for a residual (e.g. an LNURL server returning a reused invoice).
//!
//! ## NO-RETRY on the same bolt11 (bead NO-RETRY note)
//! A definitively-failed lnv2 send is NEVER re-sent on the same bolt11 by this backend: once the
//! `lnv2_pay` row is `FAILED`, `pay(key)` returns the terminal failure without calling `send()` again.
//! The Refunder re-resolves a FRESH invoice (LN-address/LNURL -> a new bolt11) under the next
//! generation key (lnrent-ug8), so retries always target a new payment hash. Per-generation
//! re-resolution is what structurally dissolves lnv2's "cannot retry the same invoice" gap.
//!
//! ## Gateway selection / INV-1 (bead: map y4m.8 or document the lnv2 equivalent)
//! For the REFUND PAY we do lnrent-side ordered failover (the y4m.18 mapping) rather than delegating to
//! lnv2's `send(None)`: the INV-1 cap must be measured against the ACTUAL paying gateway's advertised
//! fee (lnv2's `SEND_FEE_LIMIT` of 1.5%+100sat is far looser than "payout+fee <= gross"). So the pay
//! path pins ONE gateway: `reachable_gateway_preferring` iterates the registered gateways in order and
//! returns the first that answers `routing_info` AND passes lnrent's componentwise send-fee/expiration
//! guard (`lnv2_send_usable`). That guard is stricter than the client's lexicographic fee gate: it skips
//! both gateways `send()` would refuse and low-base/high-ppm gateways `send()` would accept but whose fee
//! could exceed lnrent's INV-1 cap. Selecting a gateway lnv2 will refuse is NOT free: `send()` returns a
//! retryable error before funding, PREPARED is removed, and the deterministic-order selection would
//! re-pick the SAME refused endpoint on every Refunder drive — the refund would stay PENDING forever even
//! when a compliant gateway is registered (and the doctor probe, which shares this selection, would
//! falsely report Healthy). Skipping refused gateways during selection restores the failover lnv1's
//! y4m.8 gave.
//! `refund_quote` selects such a gateway and prices the total ecash debit — gateway fee + lnv2 consensus
//! output fee + the mint inputs/change outputs chosen by Fedimint's own funding algorithm — under BOTH
//! possible gateway schedules, then reserves the larger outlay and returns the API url as the opaque
//! `gateway_hint`. Dry-running both matters because mint note-selection cost is non-monotone: the larger
//! gateway fee need not produce the larger wallet debit. `pay_refund_capped_via` prefers that same gateway,
//! re-runs the SAME total-outlay preflight against its current fee and wallet snapshot (the quote/pay
//! fee-rise refusal), and passes it explicitly to `send()`. The mint calculation is a read-only dry run in
//! an uncommitted client-DB transaction, not a duplicated coin selector. Residual, bounded: `send()` has
//! no max-fee parameter and re-fetches `routing_info`/funds after our check, so a gateway fee or wallet
//! note-set change in that sub-second window can change the final debit up to the client's own fee
//! limits; the API offers no atomic quote+send seam.
//!
//! RECEIVE is the deliberate exception: `create_invoice` passes `None` and lets lnv2's `select_gateway`
//! pick the receive gateway (first responsive, then its own `RECEIVE_FEE_LIMIT` check). lnv2 does NOT
//! fail over past a responsive-but-over-`RECEIVE_FEE_LIMIT` gateway — a residual, upstream-inherited
//! availability gap (a misconfigured guardian-vetted gateway advertising receive fees above lnv2's
//! generous 0.5%+50sat limit can make `create_invoice` fail even when a compliant gateway exists). This
//! is availability-only (no money moves, nothing is misrouted), matches upstream lnv2's accepted "the
//! gateway is guardian-vetted, trust its fee" stance, and is remediated by removing the misbehaving
//! gateway; we do NOT replicate receive-gateway selection client-side (lnrent-3d5 rejected finding).
//!
//! The configured `[fedimint] gateway` (an lnv1-era secp256k1 pubkey) is NOT an lnv2 selector (lnv2
//! selects by gateway API `SafeUrl`), so this backend does not consult it. Doctor/preflight performs
//! the functional gateway check without making daemon startup depend on a remote diagnostic call.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::backends::{
    Invoice, Lnv2Probe, PayStatus, PaymentBackend, PaymentStatus, RefundQuote, Settlement,
};
use crate::clock::Clock;

/// HKDF salt wrapping lnrent's deterministic 32-byte Fedimint root secret (`identity.rs`, already
/// domain-separated `lnrent:fedimint:v1`) into a fedimint `DerivableSecret`. Intentionally IDENTICAL
/// to the lnv1 backend's salt: the ecash MINT position is derived from the root secret + salt, and lnv1
/// and lnv2 are never both live, so sharing the salt keeps ONE canonical ecash wallet (backup/restore
/// semantics unchanged) whether reached through the lnv1 or lnv2 client.
const ROOT_SECRET_SALT: &[u8] = b"lnrent:fedimint:client:v1";

/// The lnv2-owned sqlite index (per federation data-dir): the create-once `external_id -> invoice`
/// receive map + the `idempotency_key -> operation` pay map. Distinct filename from lnv1's
/// `lnrent_index.db` so a stale lnv1 index can never be mistaken for the lnv2 one.
const INDEX_DB_FILE: &str = "lnv2_index.db";
const CLIENT_DB_DIR: &str = "client.db";

/// Bound the terminal-send await so a wedged federation surfaces as an ambiguous (recoverable) refund
/// rather than hanging the Refunder tick. A timeout leaves the `lnv2_pay` row PENDING with its op, so
/// the next `pay(key)` re-awaits the SAME operation (never a second send).
const PAY_AWAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Ordered gateway failover is only real if an endpoint that accepts a connection but never answers
/// cannot hold the entire pay-start critical section forever. This bounds one routing-info probe; a
/// timeout is that gateway's reachability failure and selection continues in the registered order.
const GATEWAY_PROBE_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(25)
} else {
    Duration::from_secs(10)
};

/// Retain canceled incoming invoices long enough for normal store/audit reconciliation, then reap them
/// so a public distinct-`external_id` unpaid-order flood cannot grow the lnv2 index without bound. PAID
/// and PAID_UNRECOVERED rows are never reaped: both are money evidence, not free flood traffic.
const INVOICE_INDEX_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

/// A definitively failed/refused pay mapping can be re-derived safely from the deterministic lnv2 op,
/// but keep it for a long window so ordinary refund retries see `Failed` immediately. SUCCEEDED and
/// in-flight rows are never reaped because they are the durable same-key-never-pays-twice record.
const PAY_INDEX_RETENTION_SECS: i64 = 180 * 24 * 60 * 60;

/// A create burst schedules at most one best-effort terminal-row reap per hour.
const INDEX_GC_INTERVAL_SECS: i64 = 60 * 60;

/// Backoff between receive-subscription re-attempts after a transient stream error. `watch()` is called
/// only ONCE (at boot) and `lookup()` reads the local index (a still-OPEN row can't be recovered by the
/// supervisor catch-up), so a receive task that gave up on the first error would strand a later-paid
/// invoice until the daemon restarts; instead it re-subscribes to the SAME op until it reaches a terminal
/// or the watcher is dropped. Short in tests so the resubscribe path runs fast.
const RECEIVE_RESUBSCRIBE_BACKOFF: Duration = if cfg!(test) {
    Duration::from_millis(5)
} else {
    Duration::from_secs(5)
};

const INDEX_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS lnv2_invoice (
    external_id   TEXT PRIMARY KEY,
    operation_id  TEXT NOT NULL,
    invoice_id    TEXT NOT NULL,
    bolt11        TEXT NOT NULL,
    payment_hash  TEXT NOT NULL,
    amount_sat    INTEGER NOT NULL,
    credited_msat INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'OPEN', -- OPEN | CANCELED | PAID | PAID_UNRECOVERED
    settled_at    INTEGER
);
CREATE INDEX IF NOT EXISTS lnv2_invoice_by_invoice_id ON lnv2_invoice (invoice_id);
CREATE INDEX IF NOT EXISTS lnv2_invoice_gc_idx ON lnv2_invoice (status, expires_at);
CREATE TABLE IF NOT EXISTS lnv2_pay (
    idempotency_key  TEXT PRIMARY KEY,
    bolt11           TEXT NOT NULL,
    -- The deterministic attempt-0 op is stored BEFORE send(). PREPARED means the backend call may or
    -- may not have committed it; recovery checks the operation log before deciding whether to await or
    -- re-run the cap + send. This closes both send crash windows without an oplog scan.
    operation_id     TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PREPARED', -- PREPARED | PENDING | SUCCEEDED | FAILED
    terminal_at      INTEGER
);
CREATE INDEX IF NOT EXISTS lnv2_pay_gc_idx ON lnv2_pay (status, terminal_at);
";

// ---------------------------------------------------------------------------------------------------
// Normalized fedimint seam (`Lnv2Ops`)
//
// The money-critical logic (idempotency, [8A] verification, INV-1 caps, crash-window recovery, the
// settlement live/recovery contract) lives in `Lnv2Payment` and is unit-tested against a fake. ALL
// direct `fedimint-lnv2-client` API usage is confined to `RealLnv2Ops`, a thin production wrapper, so
// the seam's types are plain data (no fedimint types leak) and a federation is not needed to test the
// behaviors the bead mandates. This is the minimum abstraction that makes the mandated tests
// executable under `cargo test --workspace` (no live federation), NOT speculative layering.
// ---------------------------------------------------------------------------------------------------

/// A freshly minted lnv2 receive invoice (normalized out of `(Bolt11Invoice, OperationId)`).
#[derive(Debug, Clone)]
struct Lnv2NewInvoice {
    bolt11: String,
    payment_hash: String,
    op: String, // OperationId hex (`fmt_full`)
}

/// Normalized outcome of an lnv2 `send()` attempt — the fedimint `SendPaymentError` dedup answers made
/// explicit so the money logic never matches on fedimint error types.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SendAttempt {
    /// A fresh outgoing contract was funded (a NEW send for this invoice).
    Started(String),
    /// An attempt for this invoice is already in flight (`PaymentInProgress(op)`); NO new contract.
    InProgress(String),
    /// An attempt for this invoice already reached `Success` (`InvoiceAlreadyPaid(op)`); NO new pay.
    AlreadyPaid(String),
    /// The invoice itself can never succeed (expired, malformed, missing amount, wrong currency).
    /// Safe to park FAILED only because the PREPARED op check proves this fresh call did not race an
    /// already-committed operation ([7A]).
    Rejected(String),
    /// A definite pre-funding environmental error (gateway/routing/block-count). No operation was
    /// committed, so the PREPARED row is removed and a retry must re-run the INV-1 cap.
    Retryable(String),
}

/// Operation-log result for a deterministic send op. `Present(None)` is a foreign/legacy op with no
/// lnrent key; callers must fail closed exactly like a different key ([8A]).
enum SendOpLookup {
    Missing,
    Present(Option<String>),
}

/// Truthful terminal of an lnv2 send (`FinalSendOperationState`). Both `Refunded` and `Failure` mean
/// the destination was NOT paid (see module header).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SendFinal {
    Success,
    Refunded,
    Failure,
}

/// Terminal of an lnv2 receive (`FinalReceiveOperationState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiveFinal {
    Claimed,
    Expired,
    Failure,
}

/// A gateway's advertised send-fee schedule, as the two `PaymentFee`s `RoutingInfo::send_parameters`
/// chooses between: `default` (lightning swap) vs `minimum` (direct swap). We do not know at quote time
/// which the (not-yet-minted) destination invoice will hit, so the INV-1 cap reserves the WORSE of the
/// two (see `lnv2_gateway_net_payout_sat`). Plain-data mirror of
/// `PaymentFee { base, parts_per_million }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GatewaySendFee {
    default_base_msat: u64,
    default_ppm: u64,
    minimum_base_msat: u64,
    minimum_ppm: u64,
    /// The block-height expiration deltas the two schedules require (`RoutingInfo::expiration_delta_*`).
    /// lnv2 `send()` REFUSES a gateway whose applicable delta exceeds `EXPIRATION_DELTA_LIMIT` before
    /// funding (client lib.rs:580), so `lnv2_send_usable` checks these alongside the fee.
    default_expiration_delta: u64,
    minimum_expiration_delta: u64,
}

/// The exact operations `Lnv2Payment` needs from the fedimint lnv2 client. Production impl:
/// [`RealLnv2Ops`]; test impl: a scripted fake (see the tests module). Op ids are hex strings.
#[async_trait]
trait Lnv2Ops: Send + Sync {
    /// Mint an incoming-contract bolt11 for `amount_msat`, embedding `custom_meta`.
    async fn receive(
        &self,
        amount_msat: u64,
        expiry_s: u32,
        memo: &str,
        custom_meta: Value,
    ) -> Result<Lnv2NewInvoice>;
    /// Block until the receive operation reaches its terminal state.
    async fn await_receive_final(&self, op: &str) -> Result<ReceiveFinal>;
    /// Ecash actually added by the claimed transaction, after every Fedimint consensus fee.
    async fn claimed_credit_msat(&self, op: &str) -> Result<u64>;
    /// Fund an outgoing contract for `bolt11` (idempotent on the invoice at the module), embedding
    /// `custom_meta`. `gateway` = an explicit gateway API url to pin, or `None` to let lnv2 auto-select.
    async fn send(&self, bolt11: &str, gateway: Option<&str>, custom_meta: Value) -> SendAttempt;
    /// Deterministic attempt-0 operation id for `bolt11`, persisted before `send()`.
    fn send_operation_id(&self, bolt11: &str) -> Result<String>;
    /// The msat amount ENCODED in `bolt11` (`Some`), or `None` for an amountless (or, defensively,
    /// unparseable) invoice. lnv2 `send()` funds the invoice's encoded amount, so the pay path compares
    /// this to what we owe BEFORE the cap — a smaller declared `amount_sat` paired with a larger invoice
    /// would otherwise pass the cap yet overspend. `None` needs no separate reject here: `send()` itself
    /// rejects an amountless/unparseable invoice (`InvoiceMissingAmount`), which parks FAILED downstream.
    fn invoice_amount_msat(&self, bolt11: &str) -> Result<Option<u64>>;
    /// Block (bounded) until the send op reaches a final state; `Err` = ambiguous (timeout / stream).
    async fn await_send_final(&self, op: &str) -> Result<SendFinal>;
    /// The `lnrent_key` embedded in a send op's `custom_meta` ([8A]); `None` if unset/foreign.
    async fn send_op_lnrent_key(&self, op: &str) -> Result<SendOpLookup>;
    /// The federation's registered lnv2 gateway API endpoints (urls).
    async fn list_gateways(&self) -> Result<Vec<String>>;
    /// A gateway's send-fee schedule; `Ok(None)` = does not support this federation, while `Err` keeps a
    /// concrete reachability/API failure for doctor and refund diagnostics.
    async fn gateway_send_fee(&self, gateway: &str) -> Result<Option<GatewaySendFee>>;
    /// Total ecash debit for this outgoing-contract amount, including all consensus fees.
    async fn outlay_for_contract_msat(&self, contract_msat: u128) -> Result<u128>;
    /// Spendable ecash balance in msats (the shared mint module — identical across lnv1/lnv2).
    async fn balance_msat(&self) -> Result<u64>;
    /// A cheap authenticated guardian round-trip (`session_count`); `Err` carries the reason.
    async fn guardians_reachable(&self) -> Result<()>;
    /// Whether the joined federation exposes the lnv2 module.
    async fn lnv2_module_present(&self) -> bool;
}

// ---------------------------------------------------------------------------------------------------
// Pure money helpers (unit-tested without a federation)
// ---------------------------------------------------------------------------------------------------

/// The lnv2 gateway send fee in MSATS for a payout of `pay_msat`, computed EXACTLY as
/// `PaymentFee::absolute_fee` (fedimint-lnv2-common `gateway_api.rs`): `pay_msat*ppm/1_000_000 + base`,
/// integer floor on the proportional step — the fee `send_fee.add_to(amount)` folds into the outgoing
/// contract. Widened to u128 so `pay_msat` (up to gross) never overflows; for in-range amounts this
/// equals fedimint's u64 arithmetic, and in the (unreachable-in-practice) overflow regime it
/// over-estimates, which only ever RESERVES more fee — the INV-1-safe direction.
fn lnv2_fee_msat(base_msat: u64, ppm: u64, pay_msat: u128) -> u128 {
    let prop = pay_msat.saturating_mul(u128::from(ppm)) / 1_000_000;
    prop.saturating_add(u128::from(base_msat))
}

/// The WORSE (larger) absolute fee of a gateway's `default` and `minimum` schedules at `pay_msat` — the
/// fee the INV-1 cap must reserve, because `RoutingInfo::send_parameters` picks `minimum` for a direct
/// swap and `default` otherwise and we cannot know which the destination invoice will hit at quote time.
fn lnv2_worst_fee_msat(fee: &GatewaySendFee, pay_msat: u128) -> u128 {
    let d = lnv2_fee_msat(fee.default_base_msat, fee.default_ppm, pay_msat);
    let m = lnv2_fee_msat(fee.minimum_base_msat, fee.minimum_ppm, pay_msat);
    d.max(m)
}

/// Largest whole-sat payout whose payout plus WORST gateway fee fits `gross_sat`. This is an upper
/// bound for the real refund because Fedimint consensus fees are non-negative. The total-outlay path
/// starts here and checks candidates downward, so it never assumes mint note-selection fees are
/// monotone. Binary search is valid here because gateway `PaymentFee::absolute_fee` is monotone.
fn lnv2_gateway_net_payout_sat(fee: &GatewaySendFee, gross_sat: u64) -> u64 {
    let r_msat = u128::from(gross_sat) * 1000;
    let valid = |n: u64| -> bool {
        if n == 0 {
            return true;
        }
        let pay_msat = u128::from(n) * 1000;
        pay_msat.saturating_add(lnv2_worst_fee_msat(fee, pay_msat)) <= r_msat
    };
    let mut lo = 0u64;
    let mut hi = gross_sat;
    while lo < hi {
        // Upper mid (lo < mid <= hi): guarantees progress and avoids `hi - lo + 1` overflow at u64::MAX.
        let mid = hi - (hi - lo) / 2;
        if valid(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// lnv2 send-policy limits, mirrored as PLAIN DATA from the pinned fedimint source so the pure gateway
/// selection can reject a gateway that lnv2 `send()` would refuse BEFORE funding (client lib.rs:576-582):
///  - `SEND_FEE_LIMIT` = 100 sat + 1.5% (`PaymentFee::SEND_FEE_LIMIT`, lnv2-common gateway_api.rs:209).
///  - `EXPIRATION_DELTA_LIMIT` = 1440 blocks (client lib.rs:74; that const is private, hence mirrored).
const LNV2_SEND_FEE_LIMIT_BASE_MSAT: u64 = 100_000;
const LNV2_SEND_FEE_LIMIT_PPM: u64 = 15_000;
const LNV2_EXPIRATION_DELTA_LIMIT: u64 = 1440;

/// Whether lnrent will SELECT this gateway for an lnv2 send (fee + expiration within limit). lnrent's
/// guard is COMPONENTWISE (`base <= LIMIT_BASE && ppm <= LIMIT_PPM`) and is therefore deliberately
/// STRICTER than upstream `send()`'s gate `send_fee.le(&SEND_FEE_LIMIT)`, which — because `PaymentFee`
/// derives a LEXICOGRAPHIC order (base, then ppm) — ACCEPTS a low-base/high-ppm schedule (e.g. `99_999`
/// base + an arbitrarily large ppm sorts below the limit on the base field alone). Rejecting that case
/// is intentional (lnrent-z2v): its proportional fee could exceed the ~1.5%+100sat ceiling and lnrent's
/// reserved INV-1 cap.
///
/// The anti-stranding rationale still holds in ONE direction: the guard stays AT LEAST as strict as
/// `send()`, so it never pins a gateway `send()` would refuse — the stranding mode the original comment
/// guarded against (a refused endpoint the deterministic-order selection re-picks every drive, leaving
/// the refund PENDING forever). The new, ACCEPTED failure mode — skipping a gateway `send()` would have
/// used — is fail-closed and money-safe: the refund stays PENDING (a transient no-gateway quote failure)
/// rather than selecting an advertised schedule that could exceed its reserved INV-1 cap. This guard is
/// selection-time defense-in-depth, NOT a send-time overpay backstop: the INV-1 preflight and
/// `net_payout_sat` constrain the selected routing-info snapshot, but upstream `send()` refetches that
/// info and its lexicographic gate can still admit a low-base/high-ppm reprice in the documented race
/// window. Within the selected snapshot, the guard avoids an over-ceiling gateway the cap would
/// otherwise reject at pay time (a FAILED-then-requote generation burn) or that would shrink the refund
/// payout — the correct trade.
///
/// Both the direct-swap (`minimum`) and lightning-swap (`default`) schedules must be within limit (fee
/// and expiration delta) because the not-yet-known destination invoice decides which `send_parameters`
/// applies — the same worst-of-both the INV-1 cap reserves.
fn lnv2_send_usable(fee: &GatewaySendFee) -> bool {
    let fee_within = |base: u64, ppm: u64| {
        base <= LNV2_SEND_FEE_LIMIT_BASE_MSAT && ppm <= LNV2_SEND_FEE_LIMIT_PPM
    };
    fee_within(fee.default_base_msat, fee.default_ppm)
        && fee_within(fee.minimum_base_msat, fee.minimum_ppm)
        && fee.default_expiration_delta <= LNV2_EXPIRATION_DELTA_LIMIT
        && fee.minimum_expiration_delta <= LNV2_EXPIRATION_DELTA_LIMIT
}

/// PURE preference-ordering for gateway selection (the lnv2 analogue of lnv1's `ordered_with_preference`,
/// y4m.18): the `preferred` gateway url first (if present and among the registered set — an unknown hint
/// is dropped, since a gateway that left the federation cannot pay), then the rest in registration order,
/// de-duplicated. Empty result iff `gateways` is empty.
fn lnv2_ordered_with_preference(preferred: Option<&str>, gateways: &[String]) -> Vec<String> {
    let mut ordered = Vec::with_capacity(gateways.len());
    if let Some(p) = preferred {
        if gateways.iter().any(|g| g == p) {
            ordered.push(p.to_string());
        }
    }
    for g in gateways {
        if Some(g.as_str()) != ordered.first().map(String::as_str) {
            ordered.push(g.clone());
        }
    }
    ordered
}

/// Extract the lnrent idempotency key embedded in a send op's `custom_meta` ([8A]).
fn extract_lnrent_key(custom_meta: &Value) -> Option<String> {
    custom_meta
        .get("lnrent_key")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The `custom_meta` value stamped into every lnv2 send so the op is self-identifying for [8A].
fn send_custom_meta(idempotency_key: &str) -> Value {
    json!({ "lnrent_key": idempotency_key })
}

/// Map a stored `lnv2_pay.status` to the trait `PayStatus`.
fn map_pay_status(s: Option<String>) -> PayStatus {
    match s.as_deref() {
        Some("SUCCEEDED") => PayStatus::Succeeded,
        Some("FAILED") => PayStatus::Failed,
        Some("PREPARED" | "PENDING") => PayStatus::Pending,
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
            PayCap::Gross(g) => Some(u128::from(g) * 1000),
            PayCap::Outlay(m) => Some(m),
        }
    }
}

// ---------------------------------------------------------------------------------------------------
// The backend
// ---------------------------------------------------------------------------------------------------

/// The lnv2 payment backend: the fedimint operations seam, the lnv2-owned idempotency index, the
/// registered settlement sender, and a clock for observed-settlement timestamps.
pub struct Lnv2Payment {
    ops: Arc<dyn Lnv2Ops>,
    index: Arc<Mutex<Connection>>,
    settle_tx: Mutex<Option<mpsc::Sender<Settlement>>>,
    clock: Arc<dyn Clock>,
    /// Serializes `create_invoice`'s check->mint->insert so two concurrent same-`external_id` callers
    /// cannot both mint a gateway invoice (the loser would be stranded — absent from the index).
    create_lock: tokio::sync::Mutex<()>,
    /// Serializes the pay check->send->record critical section so two concurrent same-key callers
    /// cannot both fund a contract before either recorded the operation. A wedged federation cannot hold
    /// this lock indefinitely: the two federation round-trips it spans — `list_gateways()` (inside
    /// `reachable_gateway_preferring`) and `send()` — are guardian jsonrpc calls over fedimint's ws
    /// client, which is built with jsonrpsee's default per-request timeout (~60s; the builder in
    /// fedimint-connectors ws.rs does not override it), so a silent-but-connected guardian surfaces as a
    /// per-peer error rather than an unbounded await. The gateway routing-info probe (a direct gateway
    /// HTTP call, not a guardian one) and the terminal await carry their own explicit bounds
    /// (`GATEWAY_PROBE_TIMEOUT`, `PAY_AWAIT_TIMEOUT`).
    pay_start_lock: tokio::sync::Mutex<()>,
    /// Throttles best-effort terminal-row GC on the public invoice-create path.
    last_index_gc_at: Mutex<i64>,
}

impl Lnv2Payment {
    /// Conservative current ecash debit for one payout across BOTH schedules the pinned gateway may use.
    /// Upstream selects `minimum` for a direct swap and `default` otherwise. Dry-run each distinct
    /// schedule-specific contract and reserve the larger resulting outlay: mint note selection is
    /// non-monotone, so the contract with the larger gateway fee need not produce the larger wallet debit.
    /// The ops seam includes the lnv2 output fee plus the mint inputs/change outputs Fedimint will add.
    async fn outgoing_outlay_msat(&self, fee: &GatewaySendFee, pay_sat: u64) -> Result<u128> {
        if pay_sat == 0 {
            return Ok(0);
        }
        let pay_msat = u128::from(pay_sat) * 1000;
        let contract_msat = |base_msat, ppm| {
            pay_msat
                .checked_add(lnv2_fee_msat(base_msat, ppm, pay_msat))
                .context("lnv2 payout plus gateway fee overflow")
        };
        let default_contract = contract_msat(fee.default_base_msat, fee.default_ppm)?;
        let minimum_contract = contract_msat(fee.minimum_base_msat, fee.minimum_ppm)?;
        let default_outlay = self.ops.outlay_for_contract_msat(default_contract).await?;
        if minimum_contract == default_contract {
            return Ok(default_outlay);
        }
        let minimum_outlay = self.ops.outlay_for_contract_msat(minimum_contract).await?;
        Ok(default_outlay.max(minimum_outlay))
    }

    /// Largest whole-sat payout whose TOTAL ecash debit fits `gross_sat`. Payout plus gateway fee is
    /// a mathematical upper bound; check every candidate downward from there because mint note
    /// selection can make consensus fees non-monotone. The first fit is therefore the exact maximum,
    /// not merely a safe value that can silently under-refund by jumping over a viable candidate.
    async fn net_payout_sat(&self, fee: &GatewaySendFee, gross_sat: u64) -> Result<u64> {
        let cap_msat = u128::from(gross_sat) * 1000;
        let mut pay_sat = lnv2_gateway_net_payout_sat(fee, gross_sat);
        loop {
            let outlay = self.outgoing_outlay_msat(fee, pay_sat).await?;
            if outlay <= cap_msat {
                return Ok(pay_sat);
            }
            if pay_sat == 0 {
                return Ok(0);
            }
            pay_sat -= 1;
        }
    }

    /// Join (first run) or open (subsequent runs) the lnv2-enabled federation named by `invite_code`,
    /// building a fedimint client with the lnv2 lightning module (+ mint + wallet), and opening the
    /// lnv2-owned sqlite index alongside its rocksdb under `data_dir/fedimint/<federation_id>/`.
    /// `root_secret` is lnrent's deterministic 32-byte seed (`identity.rs`), wrapped as a fedimint
    /// `DerivableSecret` under `StandardDoubleDerive`. There is NO oplog recovery pass (module header).
    pub async fn join_or_open(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let (ops, index) = real::build(invite_code, data_dir, root_secret).await?;
        Ok(Self::with_ops(Arc::new(ops), index, clock))
    }

    /// Assemble a backend around an already-built ops seam + index connection (shared by the real
    /// constructor and the tests).
    fn with_ops(ops: Arc<dyn Lnv2Ops>, index: Connection, clock: Arc<dyn Clock>) -> Self {
        Self {
            ops,
            index: Arc::new(Mutex::new(index)),
            settle_tx: Mutex::new(None),
            clock,
            create_lock: tokio::sync::Mutex::new(()),
            pay_start_lock: tokio::sync::Mutex::new(()),
            last_index_gc_at: Mutex::new(0),
        }
    }

    /// Schedule bounded, best-effort terminal index cleanup after a successful invoice create. The
    /// already-minted invoice result never depends on maintenance succeeding.
    fn gc_index_if_due(&self) {
        let now = self.clock.now();
        if !index_gc_due_and_stamp(&self.last_index_gc_at, now, INDEX_GC_INTERVAL_SECS) {
            return;
        }
        let index = self.index.clone();
        drop(tokio::task::spawn_blocking(move || {
            match gc_lnv2_invoice_index(&index, now, INVOICE_INDEX_RETENTION_SECS) {
                Ok(0) => {}
                Ok(reaped) => tracing::info!(
                    reaped,
                    "lnv2: reaped canceled invoice index rows past retention"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    "lnv2: best-effort invoice index GC failed; ignoring"
                ),
            }
            match gc_lnv2_pay_index(&index, now, PAY_INDEX_RETENTION_SECS) {
                Ok(0) => {}
                Ok(reaped) => {
                    tracing::info!(reaped, "lnv2: reaped failed pay index rows past retention")
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    "lnv2: best-effort pay index GC failed; ignoring"
                ),
            }
        }));
    }

    /// Select a reachable gateway + its fee schedule, preferring `preferred` (an advisory quote-time
    /// hint) then the registered set in order — the FIRST that answers `routing_info` wins (lnv2's own
    /// failover, mapped onto y4m.18's ordered-with-preference). Fails CLOSED (Err, never a silent
    /// wrong-gateway) when none is reachable — a TRANSIENT quote failure, never dust.
    async fn reachable_gateway_preferring(
        &self,
        preferred: Option<&str>,
    ) -> Result<(String, GatewaySendFee)> {
        let gateways = self
            .ops
            .list_gateways()
            .await
            .context("listing lnv2 gateways")?;
        self.reachable_gateway_preferring_from(preferred, &gateways)
            .await
    }

    async fn reachable_gateway_preferring_from(
        &self,
        preferred: Option<&str>,
        gateways: &[String],
    ) -> Result<(String, GatewaySendFee)> {
        let ordered = lnv2_ordered_with_preference(preferred, gateways);
        if ordered.is_empty() {
            bail!("no lnv2 gateway is registered on the federation");
        }
        let mut last_err: Option<anyhow::Error> = None;
        for gw in ordered {
            match tokio::time::timeout(GATEWAY_PROBE_TIMEOUT, self.ops.gateway_send_fee(&gw)).await
            {
                Ok(Ok(Some(fee))) if lnv2_send_usable(&fee) => return Ok((gw, fee)),
                Ok(Ok(Some(_))) => {
                    // Reachable but outside lnrent's componentwise fee/expiration guard. This includes
                    // gateways `send()` would refuse and low-base/high-ppm gateways it would accept
                    // lexicographically but which could exceed INV-1. Skip either; pinning the former
                    // strands the refund PENDING, while selecting the latter risks overpaying.
                    last_err = Some(anyhow::anyhow!(
                        "lnv2 gateway {gw} advertises a send fee/expiration above the lnv2 client \
                         limit in at least one component; lnrent enforces these limits componentwise \
                         (100_000 msat base / 15_000 ppm / 1440 blocks); skipping"
                    ));
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => last_err = Some(e),
                Err(_) => {
                    last_err = Some(anyhow::anyhow!(
                        "lnv2 gateway {gw} routing-info timed out after {GATEWAY_PROBE_TIMEOUT:?}"
                    ));
                }
            }
        }
        match last_err {
            Some(e) => Err(e).context("no reachable lnv2 gateway"),
            None => bail!("no reachable lnv2 gateway"),
        }
    }

    /// The pay/refund core. Idempotent on `idempotency_key` via the durable `lnv2_pay` map, INV-1-capped
    /// per `cap`, crash-safe across both windows.
    ///
    /// ## Commit ordering + crash story (audit DURABLE IDEMPOTENCY DESIGN)
    /// For a FRESH key we (1) enforce INV-1 against the chosen gateway, then (2) commit a PREPARED row
    /// containing `(bolt11, deterministic attempt-0 op)` BEFORE calling `send()`, then (3) `send()`, then
    /// (4) mark the op PENDING. Recovery of PREPARED first checks that exact op in the local operation
    /// log (and verifies its [8A] key):
    ///  - **Crash before `send()` commits** (PREPARED op absent): recovery re-checks INV-1 and sends.
    ///  - **Crash after `send()` commits, before PENDING recorded** (PREPARED op present under our key):
    ///    recovery marks it PENDING and awaits it directly — no second send and no cap-bypass window.
    ///  - **Crash after PENDING recorded**: recovery re-awaits the SAME op; never a second send.
    ///
    /// Once the row is SUCCEEDED or FAILED it is terminal: `pay(key)` returns without ever calling
    /// `send()` again (the NO-RETRY guarantee — a fresh generation re-resolves a new invoice instead).
    async fn pay_inner(
        &self,
        bolt11: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
        gateway_hint: Option<&str>,
    ) -> Result<String> {
        // Phase 1 (locked): pick the operation to await — re-await an existing op, or fund a new send.
        let op = {
            let _guard = self.pay_start_lock.lock().await;
            match pay_get(&self.index, idempotency_key)? {
                Some(row) if row.status == "SUCCEEDED" => {
                    // Idempotent: never re-pay. The stored op is the backend payment id.
                    return Ok(row.operation_id);
                }
                Some(row) if row.status == "FAILED" => {
                    // Definitive prior failure OR proven no-send cap refusal. Do NOT re-send the same
                    // bolt11 (NO-RETRY); the Refunder re-resolves a fresh invoice at the next generation.
                    // Surface a terminal Err so its error path sees `payment_status_by_key -> Failed`.
                    bail!(
                        "lnv2 refund key {idempotency_key} previously failed definitively \
                         or was refused before send; a fresh generation invoice is required"
                    );
                }
                Some(row) if row.status == "PENDING" => {
                    // Confirmed operation: re-await it. NO new send.
                    row.operation_id
                }
                Some(row) if row.status == "PREPARED" => {
                    // The process died inside send(). The
                    // deterministic op is the commit witness: present+ours => await; absent => no prior
                    // funding, so the retry is a genuinely new send and MUST re-run the cap.
                    match self.ops.send_op_lnrent_key(&row.operation_id).await? {
                        SendOpLookup::Missing => {
                            self.start_send(
                                bolt11,
                                amount_sat,
                                idempotency_key,
                                cap,
                                gateway_hint,
                                Some(&row.operation_id),
                            )
                            .await?
                        }
                        SendOpLookup::Present(op_key)
                            if op_key.as_deref() == Some(idempotency_key) =>
                        {
                            pay_set_op(&self.index, idempotency_key, &row.operation_id, "PENDING")?;
                            row.operation_id
                        }
                        SendOpLookup::Present(op_key) => {
                            self.fail_dedup_key(idempotency_key, bolt11, &row.operation_id, op_key)?
                        }
                    }
                }
                Some(row) => bail!(
                    "lnv2 refund key {idempotency_key} has invalid persisted status {:?}",
                    row.status
                ),
                None => {
                    self.start_send(bolt11, amount_sat, idempotency_key, cap, gateway_hint, None)
                        .await?
                }
            }
        };

        // Phase 2 (unlocked): await the truthful terminal and settle the row.
        match self.ops.await_send_final(&op).await {
            Ok(SendFinal::Success) => {
                pay_mark(
                    &self.index,
                    idempotency_key,
                    &op,
                    "SUCCEEDED",
                    self.clock.now(),
                )?;
                Ok(op)
            }
            Ok(SendFinal::Refunded) | Ok(SendFinal::Failure) => {
                // Truthful definitive failure: the destination was NOT paid. Park FAILED (so a fresh
                // generation re-resolves) and return Err — the Refunder reads `payment_status_by_key`
                // -> Failed and treats it as definite.
                pay_mark(
                    &self.index,
                    idempotency_key,
                    &op,
                    "FAILED",
                    self.clock.now(),
                )?;
                bail!("lnv2 send {op} for key {idempotency_key} reached a definitive failure (not paid)")
            }
            Err(e) => {
                // Ambiguous (timeout / stream error): leave the row PENDING with its op so the next
                // pay(key) re-awaits the SAME op. Return Err -> the Refunder reads PENDING and retries.
                Err(e).context(format!(
                    "awaiting lnv2 send terminal for key {idempotency_key} (op {op}) — still pending"
                ))
            }
        }
    }

    /// Fund (or adopt an existing) send for `idempotency_key`, returning the operation id to await.
    /// `prepared_op` is `Some` only when PREPARED recovery previously proved that op absent. We still
    /// recheck immediately before funding: upstream advances a terminally-failed invoice to attempt 1,
    /// so calling `send()` while attempt 0 already belongs to another key would bypass [8A] entirely.
    /// Holds no lock itself — the caller serializes via `pay_start_lock`.
    async fn start_send(
        &self,
        bolt11: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
        gateway_hint: Option<&str>,
        prepared_op: Option<&str>,
    ) -> Result<String> {
        let op = self.ops.send_operation_id(bolt11)?;
        if let Some(prepared_op) = prepared_op {
            if prepared_op != op {
                bail!(
                    "lnv2 prepared operation mismatch for key {idempotency_key}: stored {prepared_op}, derived {op}"
                );
            }
        }

        // [8A] must run BEFORE send, not only on its dedup errors. `get_next_operation_id` advances a
        // terminally-failed attempt-0 to attempt 1; without this operation-log check, a different key
        // using the same invoice could fund that second attempt before any dedup answer existed.
        match self.ops.send_op_lnrent_key(&op).await? {
            SendOpLookup::Present(op_key) if op_key.as_deref() == Some(idempotency_key) => {
                // Backend committed but the local mapping did not (fresh crash recovery), or a PREPARED
                // recovery raced the durable op becoming visible. Persist/adopt it and await directly.
                if prepared_op.is_none() {
                    pay_insert_prepared(&self.index, idempotency_key, bolt11, &op)?;
                }
                pay_set_op(&self.index, idempotency_key, &op, "PENDING")?;
                return Ok(op);
            }
            SendOpLookup::Present(op_key) => {
                return self.fail_dedup_key(idempotency_key, bolt11, &op, op_key);
            }
            SendOpLookup::Missing => {}
        }

        // Structural amount preflight (parity with the lnv1 backend's `inv_msat != pay_msat` guard).
        // lnv2 `send()` funds the invoice's ENCODED amount, but the cap below is computed from
        // `amount_sat`; if a resolver/LNURL endpoint returned an invoice for a DIFFERENT (larger) amount,
        // the cap would pass on the small declared figure while `send()` overspends past the INV-1 cap
        // (and the received gross). Reject BEFORE any funding. [7A]-safe: the operation-log check above
        // proved OUR attempt-0 absent, so no live send can race this FAILED park; recording FAILED (like
        // the over-cap/rejected arms) unlocks the Refunder to re-resolve a FRESH invoice at the next
        // generation. An amountless/unparseable invoice (`None`) is not parked here — `send()` is the
        // authority on it and rejects it as `InvoiceMissingAmount`.
        if let Some(inv_msat) = self.ops.invoice_amount_msat(bolt11)? {
            // Checked msat conversion (spec §3.1 overflow discipline): an owed amount whose msat form
            // overflows u64 can never equal a real invoice amount, so it also lands here as a mismatch.
            if amount_sat.checked_mul(1000) != Some(inv_msat) {
                if prepared_op.is_none() {
                    pay_insert_prepared(&self.index, idempotency_key, bolt11, &op)?;
                }
                pay_mark(
                    &self.index,
                    idempotency_key,
                    &op,
                    "FAILED",
                    self.clock.now(),
                )?;
                bail!(
                    "lnv2 refund pay refused for key {idempotency_key}: invoice amount {inv_msat} msat \
                     != owed {amount_sat} sat ({} msat)",
                    u128::from(amount_sat) * 1000
                );
            }
        }

        // Attempt 0 is absent, so this is a genuinely NEW funding attempt. Enforce the cap every time;
        // recovery of an existing op returned above without re-funding.
        let gateway: Option<String> = match cap.ceiling_msat() {
            Some(ceiling) => {
                let (gw, fee) = self.reachable_gateway_preferring(gateway_hint).await?;
                let outlay_msat = self.outgoing_outlay_msat(&fee, amount_sat).await?;
                if outlay_msat > ceiling {
                    // Over cap AFTER the deterministic operation-log check above proved OUR attempt-0
                    // absent: no send exists to race, so recording a definitive no-operation `Failed` is
                    // [7A]-safe. This is load-bearing liveness: the Refunder advances to a fresh invoice
                    // generation and re-quotes; leaving Unknown/PREPARED would re-drive this same now-
                    // over-cap invoice forever. Persist the key->invoice/op mapping even though no backend
                    // call starts, preserving the durable idempotency contract on both paths.
                    if prepared_op.is_none() {
                        pay_insert_prepared(&self.index, idempotency_key, bolt11, &op)?;
                    }
                    pay_mark(
                        &self.index,
                        idempotency_key,
                        &op,
                        "FAILED",
                        self.clock.now(),
                    )?;
                    bail!(
                        "lnv2 refund pay refused: payout {amount_sat} sat via gateway {gw} has total \
                         ecash outlay {outlay_msat} msat (gateway + Fedimint consensus fees), exceeding \
                         the INV-1 cap ({ceiling} msat)"
                    );
                }
                Some(gw)
            }
            // No cap (legacy pay()): let lnv2 auto-select.
            None => None,
        };

        if prepared_op.is_none() {
            // Commit the deterministic op BEFORE send() so both crash windows are recoverable.
            pay_insert_prepared(&self.index, idempotency_key, bolt11, &op)?;
        }

        match self
            .ops
            .send(
                bolt11,
                gateway.as_deref(),
                send_custom_meta(idempotency_key),
            )
            .await
        {
            SendAttempt::Started(op) => {
                pay_set_op(&self.index, idempotency_key, &op, "PENDING")?;
                Ok(op)
            }
            SendAttempt::InProgress(op) => {
                self.adopt_deduped(idempotency_key, bolt11, &op, "PENDING")
                    .await?;
                Ok(op)
            }
            SendAttempt::AlreadyPaid(op) => {
                // Already succeeded under this op — adopt as SUCCEEDED, but only after the [8A] check.
                self.adopt_deduped(idempotency_key, bolt11, &op, "SUCCEEDED")
                    .await?;
                Ok(op)
            }
            SendAttempt::Rejected(e) => {
                // The exact PREPARED op was absent before this call and the client rejected the invoice
                // before funding. It cannot race a live attempt, so FAILED is truthful and lets the
                // Refunder re-resolve a fresh invoice. This is the [7A]-safe terminal preflight case.
                pay_mark(
                    &self.index,
                    idempotency_key,
                    &op,
                    "FAILED",
                    self.clock.now(),
                )?;
                bail!("lnv2 send for key {idempotency_key} rejected before funding: {e}")
            }
            SendAttempt::Retryable(e) => {
                // The client proved it failed before funding (gateway/routing/guardian preflight). Remove
                // PREPARED so the retry is NEW and must re-run INV-1; never park FAILED ([7A]).
                pay_delete_prepared(&self.index, idempotency_key, &op)?;
                bail!("lnv2 send for key {idempotency_key} errored: {e}")
            }
        }
    }

    /// [8A] guard: before adopting a deduped operation as OUR key's outcome, verify its embedded
    /// `lnrent_key` matches. A mismatch is a cross-order same-invoice collision (a foreign order paid
    /// this bolt11): fail CLOSED (Err + warn) and record NOTHING, so we never silently under-refund by
    /// crediting someone else's payment. `record_status` = the status to persist on a match.
    async fn adopt_deduped(
        &self,
        idempotency_key: &str,
        bolt11: &str,
        op: &str,
        record_status: &str,
    ) -> Result<()> {
        let op_key = match self.ops.send_op_lnrent_key(op).await? {
            SendOpLookup::Missing => None,
            SendOpLookup::Present(key) => key,
        };
        if op_key.as_deref() != Some(idempotency_key) {
            return self
                .fail_dedup_key(idempotency_key, bolt11, op, op_key)
                .map(|_| ());
        }
        pay_set_op(&self.index, idempotency_key, op, record_status)?;
        Ok(())
    }

    fn fail_dedup_key(
        &self,
        idempotency_key: &str,
        bolt11: &str,
        op: &str,
        op_key: Option<String>,
    ) -> Result<String> {
        tracing::error!(
            key = idempotency_key,
            op,
            found_key = ?op_key,
            "lnv2 send dedup returned an operation for a DIFFERENT idempotency key ([8A] cross-order \
             same-invoice collision) — refusing to adopt; recording OUR key FAILED under a sentinel op \
             (never the foreign op) so the Refunder re-resolves a fresh invoice"
        );
        // Park OUR key FAILED WITHOUT adopting the foreign op. This is load-bearing liveness: with no
        // terminal status the key stays Unknown, which the generation gate RE-AWAITS on the SAME
        // persisted (unusable) invoice every drive — never re-resolving — stranding this refund liability
        // PENDING indefinitely. FAILED unlocks a fresh-invoice re-resolution at the next generation and
        // is [7A]-safe: the collision proves OUR key funded no operation, so there is no live attempt to
        // race. The sentinel op keeps us from ever binding to (or reporting) the foreign operation.
        pay_park_collision_failed(&self.index, idempotency_key, bolt11, self.clock.now())?;
        bail!(
            "lnv2 [8A] guard: send() deduped key {idempotency_key} onto op {op} whose lnrent_key is \
             {op_key:?} (not ours) — failing closed to avoid a silent under-refund"
        )
    }
}

#[async_trait]
impl PaymentBackend for Lnv2Payment {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        // Serialize check->mint->insert so two concurrent same-external_id callers can't both mint.
        let create_guard = self.create_lock.lock().await;
        // Idempotent on external_id: a repeat (or crash-retry) returns the stored invoice.
        if let Some(inv) = idx_get_by_external(&self.index, external_id)? {
            return Ok(inv);
        }

        let minted = self
            .ops
            .receive(
                amount_sat.saturating_mul(1000),
                expiry_s,
                memo,
                json!({ "lnrent_external_id": external_id }),
            )
            .await
            .context("minting lnv2 receive invoice")?;

        let inv = Invoice {
            id: format!("lnv2-{}", minted.op),
            external_id: external_id.to_string(),
            backend_invoice_id: minted.op.clone(),
            payment_hash: minted.payment_hash,
            bolt11: minted.bolt11,
            amount_sat,
            // Absolute expiry from our clock at creation (matches the field's contract + MockPayment).
            expires_at: self.clock.now() + i64::from(expiry_s),
        };
        // The create-once anchor: persist BEFORE returning, so the buyer never sees a bolt11 whose row
        // is not durable (module header: this is why no oplog scan is needed).
        idx_insert(&self.index, &inv, &minted.op)?;

        // If a watcher is registered, drive this fresh (live) invoice's settlement now; otherwise the
        // next watch() re-subscribes it from the index (status OPEN).
        if let Some(tx) = self.settle_tx.lock().unwrap().clone() {
            spawn_receive_task(
                self.ops.clone(),
                self.index.clone(),
                tx,
                self.clock.clone(),
                OpenRow {
                    external_id: inv.external_id.clone(),
                    operation_id: minted.op,
                    invoice_id: inv.id.clone(),
                    amount_sat,
                },
                true, // live: a freshly-created invoice pushes Settlement on Claimed
            );
        }
        // The GC is best-effort background work and must never run under the create serialization lock.
        drop(create_guard);
        self.gc_index_if_due();
        Ok(inv)
    }

    async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
        Ok(self.lookup_settlement(id).await?.0)
    }

    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        match idx_get_settlement(&self.index, id)? {
            // `settled_at` is Some ONLY for a LIVE Claimed (spawn_receive_task live=true); a recovery
            // Claimed left it NULL -> None, so the supervisor catch-up caps conservatively (lnrent-zwk).
            Some((status, expires_at, settled_at)) => {
                if status == "PAID" {
                    Ok((PaymentStatus::Paid, settled_at))
                } else if status == "PAID_UNRECOVERED" {
                    // Upstream reaches FinalReceiveOperationState::Failure only after Pending->Claiming:
                    // Lightning payment was confirmed, but mint output issuance failed. Returning an
                    // error keeps reconcile from expiring the local invoice as unpaid and repeatedly
                    // surfaces the durable manual-recovery liability to operator logs.
                    bail!(
                        "lnv2 invoice {id} received its Lightning payment but ecash minting failed; \
                         manual wallet/federation recovery is required"
                    )
                } else if self.clock.now() >= expires_at {
                    Ok((PaymentStatus::Expired, None))
                } else {
                    Ok((PaymentStatus::Open, None))
                }
            }
            None => Ok((PaymentStatus::Expired, None)),
        }
    }

    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String> {
        // Legacy/no-context callers: no INV-1 cap, lnv2 auto-selects the gateway. The Refunder/Sweeper
        // use the capped variants when they know the ceiling.
        self.pay_inner(dest, amount_sat, idempotency_key, PayCap::None, None)
            .await
    }

    async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
        Ok(self.refund_quote(gross_sat).await?.net_sat)
    }

    async fn refund_quote(&self, gross_sat: u64) -> Result<RefundQuote> {
        // ONE gateway decision for the quote (y4m.18): select once, derive BOTH the net cap and the hint
        // (that gateway's api url) from it, so the pay of the SAME attempt prefers this exact gateway and
        // the INV-1 cap is measured against one fee schedule. A selection failure stays a TRANSIENT Err.
        let (gw, fee) = self
            .reachable_gateway_preferring(None)
            .await
            .context("selecting a reachable lnv2 gateway for the refund fee quote")?;
        Ok(RefundQuote {
            net_sat: self.net_payout_sat(&fee, gross_sat).await?,
            gateway_hint: Some(gw), // opaque, never persisted (backends.rs contract)
        })
    }

    async fn refund_required_outlay_msat(
        &self,
        gross_sat: u64,
        pay_sat: Option<u64>,
    ) -> Result<u128> {
        let (_gw, fee) = self
            .reachable_gateway_preferring(None)
            .await
            .context("selecting a reachable lnv2 gateway for the refund liquidity check")?;
        let pay_sat = match pay_sat {
            Some(pay_sat) => pay_sat,
            None => self.net_payout_sat(&fee, gross_sat).await?,
        };
        if pay_sat == 0 {
            return Ok(0);
        }
        self.outgoing_outlay_msat(&fee, pay_sat).await
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
            None,
        )
        .await
    }

    async fn pay_refund_capped_via(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
        gateway_hint: Option<&str>,
    ) -> Result<String> {
        // The advisory quote-time hint (y4m.18) is an lnv2 gateway api url; an unknown hint is dropped by
        // the ordered selection (it's advisory, and the INV-1 cap is enforced against whatever pays).
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Gross(gross_sat),
            gateway_hint,
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
        // The operator sweep passes the just-quoted outlay as the ceiling: a fee that rose since the
        // quote makes payout+fee exceed it, so the preflight refuses rather than overspends (urw.3).
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Outlay(max_outlay_msat),
            None,
        )
        .await
    }

    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus> {
        Ok(map_pay_status(pay_status_by_op(&self.index, payment_id)?))
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
        Ok(Some(self.ops.balance_msat().await?))
    }

    async fn received_amount_msat(&self, invoice_id: &str) -> Result<Option<u64>> {
        idx_received_msat(&self.index, invoice_id)
    }

    async fn refund_gateway_ready(&self) -> Result<bool> {
        // Ready iff a reachable lnv2 gateway exists; fails CLOSED (Err) when none does — the preflight
        // gateway_check treats Err and Ok(false) identically as not-ready.
        self.reachable_gateway_preferring(None).await.map(|_| true)
    }

    fn failed_refund_can_reuse_invoice(&self) -> bool {
        // lnv2 NO-RETRY (module header): a definitively-failed send can NEVER re-pay the same bolt11
        // (deterministic attempt-0 op reached a terminal). The Refunder MUST re-resolve a fresh invoice
        // at the next generation instead of re-driving the terminal key, or the refund loops until the
        // retry cap parks it FAILED. This is what makes the generation gate advance a generation on a
        // definite lnv2 failure even before the invoice expires.
        false
    }

    async fn backend_ready(&self) -> Result<bool> {
        // Federation LIVENESS (lnrent-urw.4): a `session_count` guardian round-trip, NOT a local read.
        self.ops.guardians_reachable().await.map(|_| true)
    }

    async fn lnv2_functional_probe(&self) -> Result<Lnv2Probe> {
        // Ordered so each failure state gets a specific diagnostic (bead DOCTOR NEGATIVE MATRIX):
        // guardians -> module present -> gateway present -> gateway reachable.
        if let Err(e) = self.ops.guardians_reachable().await {
            return Ok(Lnv2Probe::GuardiansUnreachable(format!("{e:#}")));
        }
        if !self.ops.lnv2_module_present().await {
            return Ok(Lnv2Probe::ModuleAbsent);
        }
        let gateways = self
            .ops
            .list_gateways()
            .await
            .context("listing lnv2 gateways")?;
        if gateways.is_empty() {
            return Ok(Lnv2Probe::GatewayAbsent);
        }
        match self
            .reachable_gateway_preferring_from(None, &gateways)
            .await
        {
            Ok(_) => Ok(Lnv2Probe::Healthy),
            Err(e) => Ok(Lnv2Probe::GatewayUnreachable(format!("{e:#}"))),
        }
    }

    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        *self.settle_tx.lock().unwrap() = Some(tx.clone());

        // Boot/restart re-subscribe is RECOVERY-only (live=false): the fedimint notifier replays this
        // op's states from the earliest (`Pending`) on every subscribe, so the first observed state can
        // never prove the settlement is live rather than replayed (see `await_receive_final`). A later
        // Claimed here stamps NULL and pushes nothing; settlement catch-up recovers it with a
        // conservative in-window timestamp (lnrent-zwk). Only freshly created invoices are live.
        for row in idx_list_open(&self.index)? {
            spawn_receive_task(
                self.ops.clone(),
                self.index.clone(),
                tx.clone(),
                self.clock.clone(),
                row,
                false,
            );
        }
        Ok(rx)
    }
}

/// A still-OPEN index row to (re-)subscribe to on `watch()`.
struct OpenRow {
    external_id: String,
    operation_id: String,
    invoice_id: String,
    amount_sat: u64,
}

/// Drive one invoice operation to its receive terminal. `live=true` means a freshly-created operation
/// (`create_invoice`) whose settlement we therefore observe in real time — its Claimed stamps the true
/// `clock.now()` and pushes a `Settlement`. A boot/restart re-subscribe is `live=false` (recovery): the
/// fedimint notifier replays this op's states from the earliest (`Pending`) on every subscribe, so the
/// first observed state cannot prove liveness — a recovered Claimed stamps NULL and pushes nothing, and
/// settlement catch-up supplies a conservative in-window timestamp (lnrent-zwk). A live task whose
/// stream errors downgrades to recovery too, because settlement could have happened during the blind gap.
fn spawn_receive_task(
    ops: Arc<dyn Lnv2Ops>,
    index: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<Settlement>,
    clock: Arc<dyn Clock>,
    row: OpenRow,
    mut live: bool,
) {
    tokio::spawn(async move {
        let OpenRow {
            external_id,
            operation_id: op,
            invoice_id,
            amount_sat,
        } = row;
        // Re-subscribe on a transient error until the op reaches a terminal (or the watcher shuts down).
        // A single stream blip must NOT strand a later-paid invoice: `watch()` runs once and `lookup()`
        // reads the local index, so no catch-up recovers a row left OPEN here (reviewer P2).
        loop {
            match ops.await_receive_final(&op).await {
                Ok(final_state) => {
                    match final_state {
                        ReceiveFinal::Claimed => {
                            // `contract.commitment.amount` is only net of the gateway receive fee. The claim
                            // transaction also pays an lnv2 input fee and mint-output fees (and may consolidate
                            // old notes). Read the ACTUAL mint-output minus mint-input delta from that accepted
                            // transaction before exposing settlement; otherwise holdings and refunds can spend
                            // unrelated receipts. If local transaction introspection fails, persist the
                            // paid-but-unrecovered liability rather than inventing a credit or leaving it OPEN.
                            let credited_msat = match ops.claimed_credit_msat(&op).await {
                                Ok(credited_msat) => credited_msat,
                                Err(e) => {
                                    tracing::error!(
                                        op,
                                        error = %e,
                                        "lnv2: Lightning payment claimed but exact wallet credit is unavailable; manual recovery required"
                                    );
                                    // Claimed is already a paid terminal. Exact-credit decoding errors are
                                    // deterministic for its immutable transaction, so retrying forever would
                                    // leave a paid invoice OPEN until it appeared expired. Persist the existing
                                    // fail-closed liability state instead; never invent a spendable amount.
                                    let persisted = persist_receive_terminal(
                                        &tx,
                                        &op,
                                        "marking exact-credit failure PAID_UNRECOVERED",
                                        || idx_mark_paid_unrecovered(&index, &op),
                                    )
                                    .await;
                                    if persisted {
                                        tracing::error!(
                                            op,
                                            invoice = %invoice_id,
                                            external = %external_id,
                                            amount_sat,
                                            "lnv2: paid receive credit cannot be decoded; manual recovery required"
                                        );
                                    }
                                    return;
                                }
                            };
                            let settled_at = if live { Some(clock.now()) } else { None };
                            if !persist_receive_terminal(&tx, &op, "marking invoice PAID", || {
                                idx_mark_paid(&index, &op, credited_msat, settled_at)
                            })
                            .await
                            {
                                return;
                            }
                            if let Some(at) = settled_at {
                                let _ = tx
                                    .send(Settlement {
                                        invoice_id,
                                        external_id,
                                        amount_sat,
                                        received_msat: credited_msat,
                                        settled_at: at,
                                    })
                                    .await;
                            }
                            return;
                        }
                        ReceiveFinal::Expired => {
                            // Take the row out of the OPEN re-subscribe set; a CAS guard keeps a late terminal
                            // from demoting an already-PAID row.
                            persist_receive_terminal(&tx, &op, "marking invoice CANCELED", || {
                                idx_mark_canceled(&index, &op)
                            })
                            .await;
                            return;
                        }
                        ReceiveFinal::Failure => {
                            // This is NOT expiry. In upstream lnv2 the only path is Pending -> Claiming after the
                            // Lightning payment is confirmed, then failure while awaiting the mint outputs. Keep
                            // a distinct durable liability, fail lookup closed, and alert loudly; pretending it
                            // was unpaid would let reconcile expire a buyer's confirmed payment silently.
                            if !persist_receive_terminal(
                                &tx,
                                &op,
                                "marking paid-but-unrecovered invoice",
                                || idx_mark_paid_unrecovered(&index, &op),
                            )
                            .await
                            {
                                return;
                            }
                            tracing::error!(
                                op,
                                invoice = %invoice_id,
                                external = %external_id,
                                amount_sat,
                                "lnv2: Lightning payment confirmed but ecash minting failed; manual recovery required"
                            );
                            return;
                        }
                    }
                }
                Err(e) => {
                    // The watcher's receiver was dropped (supervisor shutdown): stop, don't spin. On a
                    // real federation the terminal (at worst `Expired` once the invoice lapses) ends this
                    // loop; a permanent op error backs off, it never hot-loops.
                    if tx.is_closed() {
                        return;
                    }
                    // We cannot know whether Claiming/Claimed occurred while the subscription was blind.
                    // A later terminal is recovery, never a trustworthy live timestamp or push.
                    live = false;
                    tracing::warn!(op, error = %e, "lnv2: receive subscription errored; re-subscribing");
                    tokio::time::sleep(RECEIVE_RESUBSCRIBE_BACKOFF).await;
                }
            }
        }
    });
}

/// Retry one local receive-terminal transition until it is durable or the watcher is shutting down.
/// A terminal observation is money evidence; logging a transient sqlite failure and abandoning its only
/// task would leave a paid liability OPEN and later make it appear merely expired.
async fn persist_receive_terminal<F>(
    tx: &mpsc::Sender<Settlement>,
    op: &str,
    action: &str,
    mut persist: F,
) -> bool
where
    F: FnMut() -> Result<()> + Send,
{
    loop {
        match persist() {
            Ok(()) => return true,
            Err(e) => {
                tracing::error!(op, error = %e, action, "lnv2: receive terminal persistence failed; retrying")
            }
        }
        if tx.is_closed() {
            return false;
        }
        tokio::time::sleep(RECEIVE_RESUBSCRIBE_BACKOFF).await;
    }
}

// ---------------------------------------------------------------------------------------------------
// sqlite index helpers (std::sync::Mutex; the lock never crosses an `.await`)
// ---------------------------------------------------------------------------------------------------

fn idx_get_by_external(index: &Mutex<Connection>, ext: &str) -> Result<Option<Invoice>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at
         FROM lnv2_invoice WHERE external_id = ?1",
        params![ext],
        |r| {
            Ok(Invoice {
                external_id: r.get(0)?,
                backend_invoice_id: r.get(1)?,
                id: r.get(2)?,
                bolt11: r.get(3)?,
                payment_hash: r.get(4)?,
                amount_sat: r.get::<_, i64>(5)? as u64,
                expires_at: r.get(6)?,
            })
        },
    )
    .optional()
    .context("reading lnv2_invoice by external_id")
}

fn idx_insert(index: &Mutex<Connection>, inv: &Invoice, op: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    // OPEN rows have no trustworthy wallet credit yet: the contract face value excludes claim-time
    // consensus fees. Claimed atomically replaces this placeholder with the decoded wallet delta.
    conn.execute(
        "INSERT INTO lnv2_invoice
            (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, credited_msat,
             expires_at, status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'OPEN')
         ON CONFLICT(external_id) DO NOTHING",
        params![
            inv.external_id,
            op,
            inv.id,
            inv.bolt11,
            inv.payment_hash,
            inv.amount_sat as i64,
            0i64,
            inv.expires_at,
        ],
    )
    .context("inserting lnv2_invoice")?;
    Ok(())
}

fn idx_get_settlement(
    index: &Mutex<Connection>,
    invoice_id: &str,
) -> Result<Option<(String, i64, Option<i64>)>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT status, expires_at, settled_at FROM lnv2_invoice WHERE invoice_id = ?1",
        params![invoice_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .optional()
    .context("reading lnv2_invoice settlement")
}

fn idx_received_msat(index: &Mutex<Connection>, invoice_id: &str) -> Result<Option<u64>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT credited_msat FROM lnv2_invoice WHERE invoice_id = ?1",
        params![invoice_id],
        |r| Ok(r.get::<_, i64>(0)? as u64),
    )
    .optional()
    .context("reading lnv2_invoice credited amount")
}

fn idx_mark_paid(
    index: &Mutex<Connection>,
    op: &str,
    credited_msat: u64,
    settled_at: Option<i64>,
) -> Result<()> {
    let conn = index.lock().unwrap();
    // COALESCE so a NULL (recovery) never clobbers an existing live timestamp. CAS on OPEN, exactly like
    // idx_mark_canceled / idx_mark_paid_unrecovered: a late terminal must never demote a settled row —
    // in particular it must never flip a PAID_UNRECOVERED liability (Lightning-paid, mint-failed, lookup
    // fails closed for manual recovery) to PAID, which would silently mask that recovery obligation.
    conn.execute(
        "UPDATE lnv2_invoice
         SET status='PAID', credited_msat=?2, settled_at = COALESCE(?3, settled_at)
         WHERE operation_id = ?1 AND status='OPEN'",
        params![op, credited_msat as i64, settled_at],
    )
    .context("marking lnv2_invoice PAID")?;
    Ok(())
}

fn idx_mark_canceled(index: &Mutex<Connection>, op: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    // CAS on OPEN so a late Expired/Failure cannot demote a PAID row.
    conn.execute(
        "UPDATE lnv2_invoice SET status='CANCELED' WHERE operation_id = ?1 AND status='OPEN'",
        params![op],
    )
    .context("marking lnv2_invoice CANCELED")?;
    Ok(())
}

fn idx_mark_paid_unrecovered(index: &Mutex<Connection>, op: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE lnv2_invoice SET status='PAID_UNRECOVERED'
          WHERE operation_id = ?1 AND status='OPEN'",
        params![op],
    )
    .context("marking lnv2_invoice PAID_UNRECOVERED")?;
    Ok(())
}

fn idx_list_open(index: &Mutex<Connection>) -> Result<Vec<OpenRow>> {
    let conn = index.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT external_id, operation_id, invoice_id, amount_sat
           FROM lnv2_invoice WHERE status='OPEN'",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(OpenRow {
                external_id: r.get(0)?,
                operation_id: r.get(1)?,
                invoice_id: r.get(2)?,
                amount_sat: r.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("listing OPEN lnv2 invoices")?;
    Ok(rows)
}

/// A `lnv2_pay` row.
struct PayRow {
    operation_id: String,
    status: String,
}

fn pay_get(index: &Mutex<Connection>, key: &str) -> Result<Option<PayRow>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT operation_id, status FROM lnv2_pay WHERE idempotency_key = ?1",
        params![key],
        |r| {
            Ok(PayRow {
                operation_id: r.get(0)?,
                status: r.get(1)?,
            })
        },
    )
    .optional()
    .context("reading lnv2_pay by key")
}

fn pay_status_by_key(index: &Mutex<Connection>, key: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT status FROM lnv2_pay WHERE idempotency_key = ?1",
        params![key],
        |r| r.get(0),
    )
    .optional()
    .context("reading lnv2_pay status by key")
}

fn pay_status_by_op(index: &Mutex<Connection>, op: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    conn.query_row(
        "SELECT status FROM lnv2_pay WHERE operation_id = ?1",
        params![op],
        |r| r.get(0),
    )
    .optional()
    .context("reading lnv2_pay status by op")
}

/// Commit PREPARED with the deterministic attempt-0 op before `send()` — the crash-window witness.
fn pay_insert_prepared(index: &Mutex<Connection>, key: &str, bolt11: &str, op: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO lnv2_pay (idempotency_key, bolt11, operation_id, status)
         VALUES (?1, ?2, ?3, 'PREPARED')
         ON CONFLICT(idempotency_key) DO NOTHING",
        params![key, bolt11, op],
    )
    .context("inserting lnv2_pay PREPARED intent")?;
    Ok(())
}

/// Remove a PREPARED row only after the client proved the send failed before funding. The CAS keeps a
/// stale caller from deleting an op another path already confirmed.
fn pay_delete_prepared(index: &Mutex<Connection>, key: &str, op: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "DELETE FROM lnv2_pay
          WHERE idempotency_key = ?1 AND operation_id = ?2 AND status = 'PREPARED'",
        params![key, op],
    )
    .context("deleting retryable lnv2_pay PREPARED intent")?;
    Ok(())
}

/// Bind the operation to the PREPARED key, setting `status`. Every caller must already have committed
/// that durable intent before `send()`; fabricating a partial row here would break the crash story.
fn pay_set_op(index: &Mutex<Connection>, key: &str, op: &str, status: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    let changed = conn
        .execute(
            "UPDATE lnv2_pay SET operation_id = ?2, status = ?3 WHERE idempotency_key = ?1",
            params![key, op, status],
        )
        .context("binding lnv2_pay op")?;
    if changed != 1 {
        bail!("binding lnv2_pay op for key {key} found no durable PREPARED intent");
    }
    Ok(())
}

/// Terminal mark, CAS-guarded on the operation so a stale awaiter can't overwrite a row already rebound
/// to a different op.
fn pay_mark(
    index: &Mutex<Connection>,
    key: &str,
    op: &str,
    status: &str,
    terminal_at: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE lnv2_pay SET status = ?3, terminal_at = ?4
          WHERE idempotency_key = ?1 AND operation_id = ?2",
        params![key, op, status, terminal_at],
    )
    .context("marking lnv2_pay terminal")?;
    Ok(())
}

/// Synthetic operation id stamped on a key parked FAILED by the [8A] collision guard. It is never a real
/// lnv2 operation id (those are hex `fmt_full` strings), so `pay_status_by_op` can never confuse it with
/// — or bind us to — the foreign operation the collision was about.
const COLLISION_SENTINEL_OP: &str = "(8a-collision)";

/// Park OUR `idempotency_key` FAILED after an [8A] cross-order collision WITHOUT adopting the foreign
/// operation (see [`Lnv2Payment::fail_dedup_key`] for why this liveness matters). Upsert: a fresh-key
/// collision (caught by the pre-send operation-log check) has no PREPARED row yet, while a post-send or
/// PREPARED-recovery collision already has one whose `operation_id` is the foreign op — either way OUR
/// row ends FAILED with the SENTINEL op, never the foreign one.
fn pay_park_collision_failed(
    index: &Mutex<Connection>,
    key: &str,
    bolt11: &str,
    terminal_at: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO lnv2_pay (idempotency_key, bolt11, operation_id, status, terminal_at)
         VALUES (?1, ?2, ?3, 'FAILED', ?4)
         ON CONFLICT(idempotency_key)
           DO UPDATE SET operation_id = ?3, status = 'FAILED', terminal_at = ?4",
        params![key, bolt11, COLLISION_SENTINEL_OP, terminal_at],
    )
    .context("parking lnv2_pay FAILED after an [8A] collision")?;
    Ok(())
}

/// Reap only old CANCELED invoices. OPEN may still settle; PAID and PAID_UNRECOVERED are durable money
/// evidence. Chunking releases the sole index mutex between batches on a flooded DB.
fn gc_lnv2_invoice_index(
    index: &Mutex<Connection>,
    now: i64,
    retention_secs: i64,
) -> Result<usize> {
    const BATCH: usize = 512;
    let mut total = 0;
    loop {
        let deleted = {
            let conn = index.lock().unwrap();
            conn.execute(
                "DELETE FROM lnv2_invoice WHERE rowid IN (
                     SELECT rowid FROM lnv2_invoice
                      WHERE status='CANCELED'
                        AND expires_at > 0
                        AND expires_at < MIN(unixepoch(), ?1) - ?2
                      LIMIT ?3)",
                params![now, retention_secs, BATCH],
            )?
        };
        total += deleted;
        if deleted < BATCH {
            return Ok(total);
        }
    }
}

/// Reap only old definitive/no-send FAILED mappings. PREPARED/PENDING may still move money and
/// SUCCEEDED is the durable idempotency proof, so age can never make those rows disposable.
fn gc_lnv2_pay_index(index: &Mutex<Connection>, now: i64, retention_secs: i64) -> Result<usize> {
    const BATCH: usize = 512;
    let mut total = 0;
    loop {
        let deleted = {
            let conn = index.lock().unwrap();
            conn.execute(
                "DELETE FROM lnv2_pay WHERE rowid IN (
                     SELECT rowid FROM lnv2_pay
                      WHERE status='FAILED'
                        AND terminal_at IS NOT NULL
                        AND terminal_at < MIN(unixepoch(), ?1) - ?2
                      LIMIT ?3)",
                params![now, retention_secs, BATCH],
            )?
        };
        total += deleted;
        if deleted < BATCH {
            return Ok(total);
        }
    }
}

fn index_gc_due_and_stamp(last: &Mutex<i64>, now: i64, interval_secs: i64) -> bool {
    let mut last = last.lock().unwrap();
    if now >= *last && now - *last < interval_secs {
        return false;
    }
    *last = now;
    true
}

// ---------------------------------------------------------------------------------------------------
// Production ops seam (the only place that touches `fedimint-lnv2-client`)
// ---------------------------------------------------------------------------------------------------

mod real {
    use std::path::Path;
    use std::str::FromStr;
    use std::sync::Arc;

    use anyhow::{anyhow, Context, Result};
    use async_trait::async_trait;
    use rusqlite::Connection;
    use serde_json::Value;

    use fedimint_client::module::transaction::TxSubmissionStates;
    use fedimint_client::{
        Client, ClientHandleArc, ClientModule, ClientModuleInstance, OperationId, RootSecret,
    };
    use fedimint_connectors::ConnectorRegistry;
    use fedimint_core::db::Database;
    use fedimint_core::invite_code::InviteCode;
    use fedimint_core::module::AmountUnit;
    use fedimint_core::util::SafeUrl;
    use fedimint_core::Amount;
    use fedimint_derive_secret::DerivableSecret;
    use fedimint_lnv2_client::{
        FinalSendOperationState, LightningClientInit, LightningClientModule,
        LightningOperationMeta, ReceiveOperationState, SendPaymentError,
    };
    use fedimint_lnv2_common::config::LightningClientConfig;
    use fedimint_lnv2_common::Bolt11InvoiceDescription;
    use fedimint_mint_client::{MintClientInit, MintClientModule};
    use fedimint_mint_common::{MintInput, MintOutput};
    use fedimint_rocksdb::RocksDb;
    use fedimint_wallet_client::WalletClientInit;
    use futures_util::StreamExt;
    use lightning_invoice::Bolt11Invoice;

    use super::{
        extract_lnrent_key, GatewaySendFee, Lnv2NewInvoice, Lnv2Ops, ReceiveFinal, SendAttempt,
        SendFinal, SendOpLookup, CLIENT_DB_DIR, INDEX_DB_FILE, INDEX_SCHEMA, PAY_AWAIT_TIMEOUT,
        ROOT_SECRET_SALT,
    };
    use crate::fedimint_paths::prepare_fedimint_paths;

    /// Build the real lnv2 client + open the lnv2 index. Returns the ops seam + index connection.
    pub(super) async fn build(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
    ) -> Result<(RealLnv2Ops, Connection)> {
        let invite: InviteCode = invite_code
            .parse()
            .context("parsing federation invite code")?;
        let federation_id = invite.federation_id().to_string();
        let paths = prepare_fedimint_paths(data_dir, &federation_id, CLIENT_DB_DIR, INDEX_DB_FILE)
            .context("preparing lnv2 data paths")?;

        let db: Database = RocksDb::build(paths.client_db)
            .open()
            .await
            .context("opening fedimint client rocksdb")?
            .into();

        // The lnv2 lightning module + mint + wallet. 0.11.1 auto-selects the primary (mint) module by
        // priority, so there is no `with_primary_module_kind` call.
        let mut builder = Client::builder().await.context("fedimint client builder")?;
        builder.with_module(LightningClientInit::default());
        builder.with_module(MintClientInit);
        builder.with_module(WalletClientInit::default());

        let secret = RootSecret::StandardDoubleDerive(DerivableSecret::new_root(
            &root_secret[..],
            ROOT_SECRET_SALT,
        ));

        let endpoints = ConnectorRegistry::build_from_client_defaults()
            .bind()
            .await
            .context("binding fedimint connectors")?;

        let client: ClientHandleArc = if Client::is_initialized(&db).await {
            builder
                .open(endpoints, db, secret)
                .await
                .map(Arc::new)
                .context("opening existing fedimint client")?
        } else {
            builder
                .preview(endpoints, &invite)
                .await
                .context("previewing federation from invite")?
                .join(db, secret)
                .await
                .map(Arc::new)
                .context("joining federation")?
        };

        let conn = Connection::open(paths.index_db).context("opening lnv2 index db")?;
        conn.execute_batch(INDEX_SCHEMA)
            .context("initialising lnv2 index schema")?;

        Ok((RealLnv2Ops { client }, conn))
    }

    /// Thin production wrapper mapping `fedimint-lnv2-client` onto the plain-data `Lnv2Ops` seam. The
    /// lnv2 module instance is fetched per call (`get_first_module` borrows the client), so there is no
    /// held module state.
    pub(super) struct RealLnv2Ops {
        client: ClientHandleArc,
    }

    impl RealLnv2Ops {
        /// Fetch the lnv2 module instance for one client call; the returned handle borrows `self`.
        fn lnv2(&self) -> Result<ClientModuleInstance<'_, LightningClientModule>> {
            self.client
                .get_first_module::<LightningClientModule>()
                .context("fedimint: no lnv2 lightning module on this federation")
        }
    }

    pub(super) fn classify_send_error(error: SendPaymentError) -> SendAttempt {
        match error {
            SendPaymentError::PaymentInProgress(op) => {
                SendAttempt::InProgress(op.fmt_full().to_string())
            }
            SendPaymentError::InvoiceAlreadyPaid(op) => {
                SendAttempt::AlreadyPaid(op.fmt_full().to_string())
            }
            e @ (SendPaymentError::InvoiceMissingAmount
            | SendPaymentError::InvoiceExpired
            | SendPaymentError::WrongCurrency { .. }) => SendAttempt::Rejected(e.to_string()),
            // `finalize_and_submit_transaction` creates the operation log entry and transaction
            // submission state in ONE autocommit DB transaction. Every returned error rolls that
            // transaction back (commit exhaustion panics instead), so FailedToFundPayment is a
            // definite no-operation outcome. Remove PREPARED and retry the cap; keeping it would expose
            // a nonexistent send as Pending and suppress the liability indefinitely.
            SendPaymentError::FailedToFundPayment(e) => SendAttempt::Retryable(e),
            e => SendAttempt::Retryable(e.to_string()),
        }
    }

    #[async_trait]
    impl Lnv2Ops for RealLnv2Ops {
        async fn receive(
            &self,
            amount_msat: u64,
            expiry_s: u32,
            memo: &str,
            custom_meta: Value,
        ) -> Result<Lnv2NewInvoice> {
            let (invoice, op) = self
                .lnv2()?
                .receive(
                    Amount::from_msats(amount_msat),
                    expiry_s,
                    Bolt11InvoiceDescription::Direct(memo.to_string()),
                    None, // lnv2 auto-selects the receive gateway
                    custom_meta,
                )
                .await
                .map_err(|e| anyhow!("lnv2 receive failed: {e}"))?;
            Ok(Lnv2NewInvoice {
                bolt11: invoice.to_string(),
                payment_hash: invoice.payment_hash().to_string(),
                op: op.fmt_full().to_string(),
            })
        }

        async fn await_receive_final(&self, op: &str) -> Result<ReceiveFinal> {
            let op = OperationId::from_str(op).map_err(|e| anyhow!("bad op id: {e}"))?;
            let mut updates = self
                .lnv2()?
                .subscribe_receive_operation_state_updates(op)
                .await
                .context("subscribing to lnv2 receive state")?
                .into_stream();
            // We do NOT infer live-vs-recovery provenance from the first observed state: fedimint's
            // `ModuleNotifier::subscribe` REPLAYS every persisted state for the operation, sorted by
            // `created_at` (fedimint-client-module notifier.rs:54-138), so the earliest — always
            // `Pending`, the receive SM's initial state — is yielded first on EVERY subscribe, including
            // a boot re-subscribe of an already-settled op. `spawn_receive_task` therefore treats only a
            // freshly-created invoice as live (see there); this loop just reports the terminal.
            while let Some(state) = updates.next().await {
                let final_state = match state {
                    ReceiveOperationState::Claimed => Some(ReceiveFinal::Claimed),
                    ReceiveOperationState::Expired => Some(ReceiveFinal::Expired),
                    ReceiveOperationState::Failure => Some(ReceiveFinal::Failure),
                    ReceiveOperationState::Pending | ReceiveOperationState::Claiming => None,
                };
                if let Some(final_state) = final_state {
                    return Ok(final_state);
                }
            }
            Err(anyhow!(
                "lnv2 receive state stream ended without a terminal"
            ))
        }

        async fn claimed_credit_msat(&self, op: &str) -> Result<u64> {
            let op = OperationId::from_str(op).map_err(|e| anyhow!("bad op id: {e}"))?;
            let mint = self
                .client
                .get_first_module::<MintClientModule>()
                .context("fedimint: no mint module while reading lnv2 receive credit")?;
            let mint_id = mint.id;
            let mut updates = self.client.transaction_updates(op).await.update_stream;
            while let Some(update) = updates.next().await {
                let TxSubmissionStates::Created(transaction) = update.state else {
                    continue;
                };
                let mut mint_inputs_msat = 0u64;
                for input in &transaction.inputs {
                    if input.module_instance_id() != mint_id {
                        continue;
                    }
                    let input = input
                        .as_any()
                        .downcast_ref::<MintInput>()
                        .context("decoding mint input in lnv2 receive claim")?
                        .ensure_v0_ref()
                        .context("unsupported mint input in lnv2 receive claim")?;
                    mint_inputs_msat = mint_inputs_msat
                        .checked_add(input.amount.msats)
                        .context("mint input sum overflow in lnv2 receive claim")?;
                }
                let mut mint_outputs_msat = 0u64;
                for output in &transaction.outputs {
                    if output.module_instance_id() != mint_id {
                        continue;
                    }
                    let output = output
                        .as_any()
                        .downcast_ref::<MintOutput>()
                        .context("decoding mint output in lnv2 receive claim")?
                        .ensure_v0_ref()
                        .context("unsupported mint output in lnv2 receive claim")?;
                    mint_outputs_msat = mint_outputs_msat
                        .checked_add(output.amount.msats)
                        .context("mint output sum overflow in lnv2 receive claim")?;
                }
                // A claim may consolidate pre-existing notes in the SAME transaction. Subtract those
                // mint inputs from the new mint outputs, yielding the exact wallet-balance increase
                // attributable to this receive after lnv2 input + mint input/output fees and dust.
                return mint_outputs_msat.checked_sub(mint_inputs_msat).context(
                    "lnv2 receive claim spent more pre-existing mint value than it issued",
                );
            }
            Err(anyhow!(
                "lnv2 receive op {op:?} has no persisted claim transaction"
            ))
        }

        async fn send(
            &self,
            bolt11: &str,
            gateway: Option<&str>,
            custom_meta: Value,
        ) -> SendAttempt {
            let ln = match self.lnv2() {
                Ok(l) => l,
                Err(e) => return SendAttempt::Retryable(e.to_string()),
            };
            let invoice = match Bolt11Invoice::from_str(bolt11) {
                Ok(i) => i,
                Err(e) => return SendAttempt::Rejected(format!("unparseable bolt11: {e}")),
            };
            let gw = match gateway {
                Some(s) => match SafeUrl::parse(s) {
                    Ok(u) => Some(u),
                    Err(e) => return SendAttempt::Retryable(format!("bad gateway url {s}: {e}")),
                },
                None => None,
            };
            match ln.send(invoice, gw, custom_meta).await {
                Ok(op) => SendAttempt::Started(op.fmt_full().to_string()),
                Err(e) => classify_send_error(e),
            }
        }

        fn send_operation_id(&self, bolt11: &str) -> Result<String> {
            let invoice = Bolt11Invoice::from_str(bolt11)
                .map_err(|e| anyhow!("unparseable bolt11 while deriving lnv2 operation id: {e}"))?;
            Ok(OperationId::from_encodable(&(invoice, 0u64))
                .fmt_full()
                .to_string())
        }

        fn invoice_amount_msat(&self, bolt11: &str) -> Result<Option<u64>> {
            // Parseability is already established (send_operation_id parses first); on the off chance of
            // an unparseable string here, report None and let send() be the authority (InvoiceMissingAmount
            // / rejection), rather than turning a malformed dest into a spurious hard error.
            Ok(match Bolt11Invoice::from_str(bolt11) {
                Ok(inv) => inv.amount_milli_satoshis(),
                Err(_) => None,
            })
        }

        async fn await_send_final(&self, op: &str) -> Result<SendFinal> {
            let opid = OperationId::from_str(op).map_err(|e| anyhow!("bad op id: {e}"))?;
            let ln = self.lnv2()?;
            match tokio::time::timeout(PAY_AWAIT_TIMEOUT, ln.await_final_send_operation_state(opid))
                .await
            {
                Ok(Ok(FinalSendOperationState::Success)) => Ok(SendFinal::Success),
                Ok(Ok(FinalSendOperationState::Refunded)) => Ok(SendFinal::Refunded),
                Ok(Ok(FinalSendOperationState::Failure)) => Ok(SendFinal::Failure),
                Ok(Err(e)) => Err(e).context("awaiting lnv2 send final state"),
                Err(_) => Err(anyhow!(
                    "timed out after {PAY_AWAIT_TIMEOUT:?} awaiting lnv2 send {op}"
                )),
            }
        }

        async fn send_op_lnrent_key(&self, op: &str) -> Result<SendOpLookup> {
            let opid = OperationId::from_str(op).map_err(|e| anyhow!("bad op id: {e}"))?;
            match self.client.operation_log().get_operation(opid).await {
                // Our stored send ops are always lnv2 sends, so `meta::<LightningOperationMeta>()` is safe.
                Some(entry) => match entry.meta::<LightningOperationMeta>() {
                    LightningOperationMeta::Send(m) => {
                        Ok(SendOpLookup::Present(extract_lnrent_key(&m.custom_meta)))
                    }
                    _ => Ok(SendOpLookup::Present(None)),
                },
                None => Ok(SendOpLookup::Missing),
            }
        }

        async fn list_gateways(&self) -> Result<Vec<String>> {
            let urls = self
                .lnv2()?
                .list_gateways(None)
                .await
                .map_err(|e| anyhow!("listing lnv2 gateways: {e}"))?;
            Ok(urls.into_iter().map(|u| u.to_string()).collect())
        }

        async fn gateway_send_fee(&self, gateway: &str) -> Result<Option<GatewaySendFee>> {
            let gw = SafeUrl::parse(gateway).context("parsing lnv2 gateway url")?;
            match self.lnv2()?.routing_info(&gw).await {
                Ok(Some(ri)) => Ok(Some(GatewaySendFee {
                    default_base_msat: ri.send_fee_default.base.msats,
                    default_ppm: ri.send_fee_default.parts_per_million,
                    minimum_base_msat: ri.send_fee_minimum.base.msats,
                    minimum_ppm: ri.send_fee_minimum.parts_per_million,
                    default_expiration_delta: ri.expiration_delta_default,
                    minimum_expiration_delta: ri.expiration_delta_minimum,
                })),
                // `None` means this gateway does not support the federation and is safe to skip. A
                // transport/API error is materially different: preserve it so doctor/refund diagnostics
                // name the outage instead of collapsing every failure to "no reachable gateway".
                Ok(None) => Ok(None),
                Err(e) => Err(anyhow!("lnv2 gateway {gateway} routing-info failed: {e}")),
            }
        }

        async fn outlay_for_contract_msat(&self, contract_msat: u128) -> Result<u128> {
            if contract_msat == 0 {
                return Ok(0);
            }
            let contract_msat = u64::try_from(contract_msat)
                .context("lnv2 outgoing contract exceeds the client's u64 amount range")?;

            // `send()`'s partial transaction contains one lnv2 output for `contract_msat`. Fedimint's
            // finalizer first adds this module's consensus output fee, then asks the mint primary module
            // to select ecash inputs and create change (both of which carry their own consensus fees).
            let lnv2_id = self.lnv2()?.id;
            let client_config = self.client.config().await;
            let lnv2_config = client_config
                .modules
                .get(&lnv2_id)
                .context("lnv2 module config missing while quoting consensus fees")?
                .cast::<LightningClientConfig>()
                .context("decoding lnv2 client config while quoting consensus fees")?;
            let lnv2_output_fee_msat = lnv2_config
                .fee_consensus
                .fee(Amount::from_msats(contract_msat))
                .msats;
            let required_msat = contract_msat
                .checked_add(lnv2_output_fee_msat)
                .context("lnv2 contract plus consensus output fee overflow")?;

            let mint = self
                .client
                .get_first_module::<MintClientModule>()
                .context("fedimint: no mint module while quoting lnv2 send outlay")?;
            let mut dbtx = mint.db.begin_transaction_nc().await;
            let dry_run_op =
                OperationId::from_encodable(&("lnrent-lnv2-consensus-fee-dry-run", required_msat));
            let (inputs, outputs) = mint
                .inner()
                .create_final_inputs_and_outputs(
                    &mut dbtx,
                    dry_run_op,
                    AmountUnit::BITCOIN,
                    Amount::ZERO,
                    Amount::from_msats(required_msat),
                )
                .await
                .context("dry-running mint funding for lnv2 send fee quote")?;
            let mint_inputs_msat = inputs.inputs().iter().try_fold(0u128, |sum, input| {
                sum.checked_add(u128::from(input.amounts.get_bitcoin().msats))
                    .context("mint input sum overflow while quoting lnv2 send")
            })?;
            let mint_outputs_msat = outputs.outputs().iter().try_fold(0u128, |sum, output| {
                sum.checked_add(u128::from(output.amounts.get_bitcoin().msats))
                    .context("mint output sum overflow while quoting lnv2 send")
            })?;
            // Dropping the uncommitted module transaction rolls back note deletion, consolidation and
            // issuance-index changes. The returned input-minus-change delta is exactly what the real
            // finalizer would debit for the same lnv2 output on this wallet snapshot.
            drop(dbtx);
            mint_inputs_msat
                .checked_sub(mint_outputs_msat)
                .context("mint dry-run produced more change than selected inputs")
        }

        async fn balance_msat(&self) -> Result<u64> {
            Ok(self.client.get_balance_for_btc().await?.msats)
        }

        async fn guardians_reachable(&self) -> Result<()> {
            self.client
                .api()
                .session_count()
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("fedimint federation unreachable (session_count): {e}"))
        }

        async fn lnv2_module_present(&self) -> bool {
            self.client
                .get_first_module::<LightningClientModule>()
                .is_ok()
        }
    }
}

#[cfg(test)]
mod tests;
