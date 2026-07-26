//! The `PaymentBackend` money seam (SPEC.md §6.1): its DTOs (`Invoice`, `PaymentStatus`,
//! `PayStatus`, `Settlement`) plus the in-memory `MockPayment` fixture. The real Fedimint
//! backend lives in `lnv2_backend.rs` (ADR-0018); provisioning is hook-driven (`runner.rs`
//! + recipes), not a trait here.

use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tokio::sync::mpsc;

use async_trait::async_trait;

pub const DEV_SETTLE_UNSUPPORTED: &str = "dev settle is only supported on the mock payment backend";

/// Receiving and refunding Lightning. SPEC.md §6.1. No hold invoices on the v1
/// backends, so `pay` exists for capture-then-refund (ADR-0003).
///
/// Async because the real backend (Fedimint, lnrent-7fp.4) is async to the core
/// (`fedimint-client`); the in-memory `MockPayment` satisfies it trivially. Every call site is
/// already inside an `async fn`, so it just `.await`s here (no runtime-nesting bridge).
#[async_trait]
pub trait PaymentBackend: Send + Sync {
    /// Create (or return the existing) invoice. **Idempotent on `external_id`**: a repeated
    /// call with the same `external_id` MUST return the same invoice, not a duplicate — so a
    /// retry after a crash regenerates the same `external_id` and reuses the invoice (§6.6).
    async fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str, // binds settlement -> order (ADR-0009); deterministic per invoice class (§6.6)
    ) -> Result<Invoice>;
    async fn lookup(&self, id: &str) -> Result<PaymentStatus>;
    /// Invoice status PLUS the backend's observed-LIVE settled_at. `Some(ts)` is returned ONLY for a
    /// settlement the backend observed live (its true time is known); `None` for a not-paid invoice
    /// OR a RECOVERY settlement (settled while the daemon was down, so the true time is unknown). The
    /// supervisor's settlement catch-up uses `Some(ts)` EXACTLY and caps only on `None` — so a late
    /// LIVE payment refunds (capture's g5p gate) instead of being stamped just-in-window and wrongly
    /// provisioned (lnrent-zwk). `lookup()` stays the status-only seam reconcile uses (unchanged).
    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)>;
    /// Outbound payment, used for refunds. **Idempotent on `idempotency_key`**: calling twice
    /// with the same key never pays twice (ADR-0009, SPEC §6.6). Returns a backend payment id.
    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String>;
    /// The maximum NET whole-sat amount this backend can auto-send for a refund of `gross_sat`, after
    /// reserving the outbound fee so total outlay never exceeds what was received (INV-1, anti-drain;
    /// `docs/specs/refund-money-path-hardening.md` §3.1). Returns `Ok(0)` ONLY for true dust — no
    /// positive whole-sat payout plus its fee fits inside `gross_sat`. Quote/operability failures (for
    /// example no reachable gateway) are `Err`/transient, NOT `Ok(0)`. Read-only; never mints or pays.
    /// Default: no fee (returns `gross_sat`) — correct for `MockPayment` and any internal-only backend.
    async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
        Ok(gross_sat)
    }
    /// Exact backend outlay needed NOW to start an automated refund payment. `pay_sat=Some(_)` is for
    /// a fixed/direct or persisted resolved invoice; `None` asks the backend to price a fresh net cap.
    /// Default backends charge no outbound fee, so the outlay is just the payout in msats.
    async fn refund_required_outlay_msat(
        &self,
        gross_sat: u64,
        pay_sat: Option<u64>,
    ) -> Result<u128> {
        let pay_sat = match pay_sat {
            Some(pay_sat) => pay_sat,
            None => self.refund_net_sat(gross_sat).await?,
        };
        Ok(u128::from(pay_sat) * 1000)
    }
    /// Idempotent refund pay with a final INV-1 cap check before any NEW backend operation. Existing
    /// `SUCCEEDED`/`PENDING` operations for `idempotency_key` are re-awaited exactly as [`pay`](Self::pay).
    /// For a NEW operation the backend MUST refuse to start if
    /// `amount_sat*1000 + fee(amount_sat*1000) > gross_sat*1000` (spec §3.1). Default: delegate to
    /// `pay` — mock/internal backends charge no fee, so the cap holds with `amount_sat == gross_sat`.
    async fn pay_refund_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
    ) -> Result<String> {
        let _ = gross_sat; // the default backend charges no fee; the cap is the caller's quote
        self.pay(bolt11, amount_sat, idempotency_key).await
    }
    /// The INV-1 fee-adjusted net cap for `gross_sat` PLUS an advisory hint identifying the gateway that
    /// priced it (lnrent-y4m.18). With ordered gateway failover (lnrent-y4m.8) the quote and the pay
    /// each select a gateway independently; a failover between them lets one gateway price a refund the
    /// other pays, so a resolved invoice minted for the quoted gateway's net can fail the pay-side cap
    /// preflight against a different gateway. `gateway_hint` lets the caller carry the QUOTE-time
    /// gateway into [`pay_refund_capped_via`](Self::pay_refund_capped_via) so both use ONE decision.
    ///
    /// QUOTE/PAY-CONSISTENCY CONTRACT: `gateway_hint` is an OPAQUE, backend-specific gateway identity,
    /// valid ONLY within the caller's current attempt (one Refunder drive); it is advisory and MUST
    /// NEVER be persisted. It MUST NOT weaken the INV-1 cap: whatever gateway actually pays is the
    /// gateway the cap preflight is enforced against, hint or no hint. Default: no failover concept, so
    /// the quote is just [`refund_net_sat`](Self::refund_net_sat) with no hint.
    async fn refund_quote(&self, gross_sat: u64) -> Result<RefundQuote> {
        Ok(RefundQuote {
            net_sat: self.refund_net_sat(gross_sat).await?,
            gateway_hint: None,
        })
    }
    /// Idempotent refund pay exactly like [`pay_refund_capped`](Self::pay_refund_capped), plus a
    /// best-effort `gateway_hint` (lnrent-y4m.18): the opaque gateway identity a preceding
    /// [`refund_quote`](Self::refund_quote) selected in the SAME attempt. The backend SHOULD prefer that
    /// gateway so the quote and the pay use one decision, but MAY ignore it or fall back (e.g. the
    /// hinted gateway is no longer reachable). The hint is advisory ONLY and MUST NOT weaken the INV-1
    /// cap: the cap preflight is always enforced against the gateway that ACTUALLY pays. Default: ignore
    /// the hint and delegate to `pay_refund_capped` (no gateway concept).
    async fn pay_refund_capped_via(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
        gateway_hint: Option<&str>,
    ) -> Result<String> {
        let _ = gateway_hint; // the default backend has no gateway to prefer; ignore it
        self.pay_refund_capped(bolt11, amount_sat, gross_sat, idempotency_key)
            .await
    }
    /// Whether a definitively-`Failed` refund pay for a key can be RE-ATTEMPTED against the SAME
    /// persisted invoice, or is TERMINAL PER INVOICE — so a retry needs a FRESH invoice at the next
    /// generation. This is the ONE money-semantic the refund generation gate (`refund.rs`) needs to
    /// know per backend:
    ///  - **mock / default (`true`).** A definite `Failed` re-attempts the same bolt11 in place: the
    ///    default backend re-drives the same persisted invoice, so a transient-cause failure on a
    ///    still-unexpired, still-in-cap invoice is retried as-is.
    ///  - **lnv2 (`false`).** `send` derives a DETERMINISTIC attempt-0 operation id from the invoice;
    ///    once it reaches `Refunded`/`Failure` the SAME bolt11 can NEVER be re-sent (NO-RETRY,
    ///    lnrent-3d5 / ADR-0018). The gate MUST re-resolve a fresh invoice at the next generation, or
    ///    re-driving the dead key loops until the retry cap parks the refund `FAILED` — stranded money.
    ///
    /// A pure per-backend constant (not per-key state); the gate reads it to decide reuse-vs-re-resolve
    /// on a `Failed`. It NEVER weakens any cap: whichever invoice is paid, the INV-1 preflight still
    /// binds. Does not affect `Pending`/`Unknown` (those always re-await the same invoice, both backends).
    fn failed_refund_can_reuse_invoice(&self) -> bool {
        true
    }
    /// Idempotent OUTLAY-capped pay for the operator sweep (gate1-operator-sweep, urw.3). Like
    /// [`pay_refund_capped`](Self::pay_refund_capped) it re-awaits an existing `SUCCEEDED`/`PENDING`
    /// operation for `idempotency_key`, but a NEW operation MUST refuse to start if
    /// `amount_sat*1000 + fee(amount_sat*1000) > max_outlay_msat` — the just-quoted outlay ceiling, so
    /// a gateway fee that rose between the quote and the send refuses rather than overspends. Default:
    /// delegate to `pay` — mock/internal backends charge no fee, so `amount_sat*1000 == max_outlay_msat`
    /// and the cap always holds.
    async fn pay_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        idempotency_key: &str,
    ) -> Result<String> {
        let _ = max_outlay_msat; // the default backend charges no fee; the caller's quote is the cap
        self.pay(bolt11, amount_sat, idempotency_key).await
    }
    /// Status of an outbound payment by its backend id (ADR-0009 refund ledger).
    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus>;
    /// Check an in-flight refund by its idempotency key after a crash (SPEC §6.6). An
    /// optimization only — retrying `pay(key)` is always safe (the key dedups).
    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus>;
    /// Whether this key has durable evidence of a started outbound operation. This disambiguates
    /// `PayStatus::Unknown` for readiness: no record still needs liquidity; an unqueryable started
    /// operation has already committed funds.
    async fn payment_started_by_key(&self, _idempotency_key: &str) -> Result<bool> {
        Ok(false)
    }
    /// Spendable balance in msats, or `None` for backends without an observable balance.
    async fn available_balance_msat(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    /// Actual wallet credit for a received invoice, after any backend-side receive fee. `None` means
    /// the backend credits the gross invoice amount (the mock and lnv1). Recovery settlement catch-up
    /// uses this local metadata so it never books an lnv2 invoice fee as spendable holdings.
    async fn received_amount_msat(&self, _invoice_id: &str) -> Result<Option<u64>> {
        Ok(None)
    }
    /// Whether the backend can currently price and pay refunds.
    async fn refund_gateway_ready(&self) -> Result<bool> {
        Ok(true)
    }
    /// LIVENESS: whether the backend's ROOT money dependency is usable — a cheap authenticated
    /// round-trip that actually hits the remote service (NOT a local-DB read), so a dead root is
    /// distinguishable from a down refund-pay seam or a low balance (lnrent-urw.4). On Fedimint that
    /// root is the guardians; on phoenixd it is the operator's node (authenticated `getinfo` on a
    /// fee-verified release), and an `Err` there carries a sanitized [`PhoenixdReadinessError`] so
    /// shared reporting can speak the configured backend's vocabulary (lnrent-p2e). Default
    /// `Ok(true)` for backends with no such dependency (the mock).
    async fn backend_ready(&self) -> Result<bool> {
        Ok(true)
    }
    /// DEV-ONLY test/operator affordance: force-settle an inbound invoice by backend external id.
    /// Real money backends must leave this unsupported.
    async fn dev_settle(&self, _external_id: &str, _settled_at: i64) -> Result<()> {
        Err(anyhow::anyhow!(DEV_SETTLE_UNSUPPORTED))
    }
    /// FUNCTIONAL lnv2 doctor probe (ADR-0018, lnrent-3d5): is the configured backend's lnv2 money path
    /// actually usable — the lnv2 module present on the federation AND an lnv2-capable gateway attached
    /// and reachable? Config-presence is insufficient (ADR-0018), so this reaches the guardians/gateways.
    /// A backend with NO lnv2 path (the mock, or a future phoenixd) returns [`Lnv2Probe::NotApplicable`],
    /// which `lnrent preflight` renders as a SKIPPED (passing) check — exactly like the provider-token
    /// skip. The distinct failure variants let the doctor emit a specific human diagnostic per state
    /// (module absent vs gateway absent vs gateway unreachable vs guardians unreachable). Read-only.
    async fn lnv2_functional_probe(&self) -> Result<Lnv2Probe> {
        Ok(Lnv2Probe::NotApplicable)
    }
    /// FUNCTIONAL phoenixd doctor probe (ADR-0019, lnrent-5mi): is the operator's EXTERNAL phoenixd
    /// node answering an AUTHENTICATED `getinfo` right now, and is the release it reports the one its
    /// trampoline fee schedule was verified against? Those are the two conditions the phoenixd money
    /// path already fails CLOSED on, so without this probe an operator first learns about them from a
    /// stalled refund. A backend with NO phoenixd money path (the mock, lnv2) returns
    /// [`PhoenixdProbe::NotApplicable`], which makes `lnrent preflight` omit the backend-conditional
    /// check. Read-only: no invoice, no pay.
    async fn phoenixd_probe(&self) -> Result<PhoenixdProbe> {
        Ok(PhoenixdProbe::NotApplicable)
    }
    /// The configured backend's IDENTITY — cheap, synchronous, and network-free ON PURPOSE.
    ///
    /// Readiness reporting needs to label its output for the operator's actual backend in EVERY
    /// state, including the healthy one; deriving that from an error's content only works when
    /// something FAILED (codex on PR #66: a healthy phoenixd fell back to printing "Federation:").
    /// It must not be answered by [`Self::phoenixd_probe`], which is a network doctor probe that
    /// readiness is forbidden to call (ADR-0016 — the ledger authorizes, and only explicit
    /// `reconcile` reads the wallet).
    fn backend_kind(&self) -> BackendKind {
        BackendKind::Federation
    }
    /// Stream of settled payments (push). `Settlement.external_id` carries the order id
    /// (SPEC §6.1). M1a wires this to the Fedimint client settlement stream.
    async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>>;
}

#[derive(Debug, Clone)]
pub struct Invoice {
    pub id: String,                 // our local invoice id (§11 invoice.id)
    pub external_id: String,        // unique per-invoice token binding settlement->order (ADR-0009)
    pub backend_invoice_id: String, // the backend's own invoice id (§11 invoice.backend_invoice_id)
    pub payment_hash: String,       // bolt11 payment hash (§11 invoice.payment_hash)
    pub bolt11: String,
    pub amount_sat: u64,
    pub expires_at: i64, // bolt11 expiry (unix secs); the order reservation is released at this (§9.3)
}

/// A refund fee QUOTE (lnrent-y4m.18): the INV-1 fee-adjusted net cap PLUS an advisory hint naming the
/// gateway that priced it. `gateway_hint` is an OPAQUE, backend-specific gateway identity carried
/// through to [`PaymentBackend::pay_refund_capped_via`] within ONE attempt so the quote and the pay use
/// one gateway decision under failover; it is `None` when the backend has no gateway concept and MUST
/// NEVER be persisted. See [`PaymentBackend::refund_quote`] for the full quote/pay-consistency contract.
#[derive(Debug, Clone)]
pub struct RefundQuote {
    pub net_sat: u64,
    pub gateway_hint: Option<String>,
}

/// Result of the FUNCTIONAL lnv2 doctor probe ([`PaymentBackend::lnv2_functional_probe`], ADR-0018,
/// lnrent-3d5). The lnv2 money path is usable ONLY when the lnv2 module is present on the joined
/// federation AND at least one lnv2-capable gateway is attached and reachable — so the probe reaches
/// the guardians and the gateway, never a mere config read. Each failure variant carries enough for the
/// doctor to print a state-specific human diagnostic; the reachability variants carry the underlying
/// error string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lnv2Probe {
    /// lnv2 module present AND a reachable lnv2 gateway — the money path is functional.
    Healthy,
    /// The backend has no lnv2 money path at all (mock / non-fedimint). The doctor renders this as a
    /// skipped, passing check — the check simply does not apply to this backend.
    NotApplicable,
    /// The federation guardians are unreachable, so module/gateway presence cannot even be determined.
    GuardiansUnreachable(String),
    /// The guardians are reachable but the joined federation exposes no lnv2 module (join an
    /// lnv2-enabled federation).
    ModuleAbsent,
    /// The lnv2 module is present but the federation advertises no lnv2 gateway.
    GatewayAbsent,
    /// An lnv2 gateway is advertised but none is reachable (routing-info round-trip failed).
    GatewayUnreachable(String),
}

/// Result of the FUNCTIONAL phoenixd doctor probe ([`PaymentBackend::phoenixd_probe`], ADR-0019,
/// lnrent-5mi). phoenixd is an operator-managed EXTERNAL service reached over authenticated HTTP, so
/// "configured" says nothing about usable: the node can be down, the api password can be wrong, and
/// the running release can be one whose trampoline fee schedule nobody verified (on which the money
/// path refuses every automated refund). Each variant carries what the doctor needs to print a
/// state-specific diagnostic AND its remedy.
///
/// SECRETS (§13): no variant carries the api password, and none may ever be constructed from one.
/// [`Unreachable`](Self::Unreachable) and [`BalanceUnavailable`](Self::BalanceUnavailable) carry
/// transport/HTTP diagnostics, which the phoenixd ops layer already keeps credential-free (the
/// password travels only in an `Authorization` header, and `phoenixd_url` may not embed credentials).
/// Which money backend the daemon is configured with, for OPERATOR-FACING labelling only. Never a
/// control-flow switch on the money path — the trait's own methods carry backend behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// Fedimint/lnv2 and the mock: report in the federation terms these paths have always used.
    Federation,
    /// The operator runs their own phoenixd node — there are no guardians to name.
    Phoenixd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhoenixdProbe {
    /// The backend has no phoenixd money path at all (mock / lnv2). The doctor omits the check.
    NotApplicable,
    /// The node answered an AUTHENTICATED `getinfo` and runs a release whose fee schedule is verified.
    /// `fee_credit_sat` is phoenixd's `feeCreditSat`: money `receivedSat` counted that `balanceSat`
    /// cannot spend. Non-zero is reported prominently but is NOT a failure — a funded node can
    /// legitimately hold fee credit.
    Ready {
        /// The operator-safe `getinfo.version`, including a valid build suffix (a live node reports
        /// e.g. `0.9.0-b072567`). Remote-controlled or credential-bearing values are redacted before
        /// this variant is constructed.
        version: String,
        /// The release the ACTIVE trampoline fee schedule was verified against.
        verified_version: String,
        /// `getbalance.balanceSat` — the SPENDABLE balance, reported alongside `fee_credit_sat`
        /// because the fee credit is only interpretable next to the balance it is excluded from
        /// ("2723 sat unspendable" reads very differently at balance 0 than at balance 500k). The
        /// doctor prints both and judges NEITHER: whether the node holds enough to fund refunds is
        /// lnrent-itw / lnrent-tof, not this probe.
        balance_sat: u64,
        fee_credit_sat: u64,
    },
    /// The node did not answer a usable `getinfo` (connect refused, TLS failure, timeout, a proxy's
    /// 5xx …). Carries the underlying diagnostic.
    Unreachable(String),
    /// The node answered, but REJECTED the credentials (HTTP 401/403) — a distinct operator remedy
    /// from an unreachable node, so it is a distinct variant.
    AuthRejected { status: u16 },
    /// `getinfo` succeeded and the version is compatible, but `getbalance` failed, so the doctor
    /// could not read `feeCreditSat`. The node has already proved reachable; this needs a distinct
    /// diagnostic from [`Unreachable`](Self::Unreachable).
    BalanceUnavailable(String),
    /// Reachable and authenticated, but the running release is not the one the active trampoline fee
    /// schedule was verified against, so the money path cannot bound an outbound fee on it (ADR-0019)
    /// and refuses automated refunds.
    VersionMismatch { running: String, verified: String },
}

/// A sanitized phoenixd readiness failure carried through the existing
/// [`PaymentBackend::backend_ready`] / [`PaymentBackend::refund_gateway_ready`] seams.
///
/// This wraps only probe states that can fail those seams. It lets shared reporting reuse the
/// operator wording from the phoenixd doctor without re-running the full doctor probe (whose
/// successful path reads `getbalance`, forbidden on automatic readiness paths by ADR-0016).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhoenixdReadinessError {
    probe: PhoenixdProbe,
}

impl PhoenixdReadinessError {
    pub(crate) fn new(probe: PhoenixdProbe) -> Self {
        assert!(
            matches!(
                probe,
                PhoenixdProbe::Unreachable(_)
                    | PhoenixdProbe::AuthRejected { .. }
                    | PhoenixdProbe::VersionMismatch { .. }
            ),
            "a phoenixd readiness error must carry a getinfo/version failure"
        );
        Self { probe }
    }

    pub(crate) fn probe(&self) -> &PhoenixdProbe {
        &self.probe
    }
}

impl std::fmt::Display for PhoenixdReadinessError {
    /// ONE operator text per condition. This error escapes the readiness path — the money path logs
    /// it as `error = %e` when a refund pay hits the same failing seam (`refund.rs`) — so rendering
    /// it here by hand gave the SAME condition two stories, and they had already drifted (the local
    /// copy dropped "every automated refund fails closed" and applied neither the remote-version nor
    /// the configured-version report hygiene). Delegating to the one probe→text function keeps
    /// `lnrent preflight`, the readiness report and the money-path log in agreement by construction.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `NotApplicable` is the only probe state with no rendering, and the constructor rejects it
        // along with every other non-failure state.
        let check = crate::preflight::phoenixd_check_from_probe(self.probe.clone())
            .expect("constructor rejects non-readiness phoenixd probe states");
        f.write_str(&check.detail)
    }
}

impl std::error::Error for PhoenixdReadinessError {}

/// Constant replacement for a remote-controlled phoenixd version that is unsafe to paste into an
/// operator report.
pub(crate) const REDACTED_PHOENIXD_VERSION: &str = "<redacted untrusted version>";

/// Accept only the measured phoenixd release/build shapes for operator-facing output:
/// `MAJOR.MINOR.PATCH` or `MAJOR.MINOR.PATCH-<hex>`. The authenticated endpoint is still
/// remote-controlled and sees the Basic header, so arbitrary text (including whitespace/control
/// injection or an echoed Authorization header) must never be rendered as a version. The real ops
/// layer additionally supplies `api_password`, closing the valid-looking-suffix case too.
///
/// This is DISPLAY sanitization only. The money-path compatibility predicate continues to evaluate
/// the original `getinfo.version`.
pub(crate) fn safe_phoenixd_version<'a>(
    version: &'a str,
    api_password: Option<&str>,
) -> Option<&'a str> {
    if version.is_empty()
        || version.len() > 64
        || api_password.is_some_and(|secret| !secret.is_empty() && version.contains(secret))
    {
        return None;
    }

    let (release, build) = match version.split_once('-') {
        Some((release, build)) => (release, Some(build)),
        None => (version, None),
    };
    let mut release_parts = release.split('.');
    let valid_release = (0..3).all(|_| {
        release_parts.next().is_some_and(|part| {
            !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())
        })
    }) && release_parts.next().is_none();
    let valid_build = build.is_none_or(|hash| {
        (6..=40).contains(&hash.len()) && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    });

    (valid_release && valid_build).then_some(version)
}

/// Status of one of OUR inbound invoices (receiving). SPEC.md §6.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentStatus {
    Open,
    Paid,
    Expired,
}

/// Status of an OUTBOUND payment (a refund), distinct from invoice status (§6.6). `Unknown`
/// is the honest answer when an in-flight refund can be neither confirmed nor refuted;
/// recovery retries `pay(key)` regardless (the key dedups), so `Unknown` never strands a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayStatus {
    Unknown,
    Pending,
    Succeeded,
    Failed,
}

/// A settled incoming payment for one of OUR invoices. The backend filters out any
/// non-lnrent payments, so `external_id` is always the invoice's correlation token
/// (ADR-0009); capture binds it to the order.
#[derive(Debug, Clone)]
pub struct Settlement {
    pub invoice_id: String,
    pub external_id: String,
    pub amount_sat: u64,
    /// Actual amount credited to the backend wallet after receiver-side fees. Billing still uses the
    /// gross bolt11 `amount_sat`; refunds and holdings accounting use this money value.
    pub received_msat: u64,
    pub settled_at: i64, // when the backend observed settlement (unix secs); capture sets
                         // paid_through = settled_at + period (§6.3), so it must come from here
}

/// Deterministic in-memory PaymentBackend — the M1a money-path FIXTURE (the operator chose a
/// mock over a live federation for now). It issues fake bolt11s (idempotent on `external_id`),
/// treats every refund `pay` as an immediate success (idempotent on the key), and lets a
/// driver/test push settlements via `settle()` (also delivered on the `watch()` stream). This is
/// NOT the real Fedimint backend (lnrent-7fp.4) — that stays a deferred follow-up; this lets the
/// capture/refund/reconcile money path be built and proven without a federation.
#[derive(Default)]
pub struct MockPayment {
    state: Mutex<MockState>,
}

#[derive(Default)]
struct MockState {
    invoices: HashMap<String, Invoice>, // external_id -> Invoice (idempotency anchor)
    paid: HashSet<String>,              // external_ids observed settled
    settled_at: HashMap<String, i64>,   // external_id -> LIVE settled ts (None on recovery)
    payments: HashMap<String, String>,  // refund idempotency_key -> backend payment id
    seq: u64,
    now: i64, // mock wall clock; create_invoice stamps absolute expiry, lookup honors it
    settle_tx: Option<mpsc::Sender<Settlement>>,
}

impl MockPayment {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the mock's clock (unix secs). `create_invoice` stamps `expires_at = now + expiry`,
    /// and `lookup` returns `Expired` once `now` passes it — so expiry/reconcile tests exercise a
    /// realistic backend instead of an invoice that never expires.
    pub fn set_now(&self, now: i64) {
        self.state.lock().unwrap().now = now;
    }

    /// Drive a LIVE settlement for a previously-created invoice (by `external_id`): mark it paid,
    /// record the real `settled_at` (so `lookup_settlement` can surface it), and, if a `watch()`
    /// stream is open, push the `Settlement`. Returns the Settlement so a caller can also hand it
    /// straight to capture. Errors if no such invoice was created.
    pub fn settle(&self, external_id: &str, settled_at: i64) -> Result<Settlement> {
        let mut st = self.state.lock().unwrap();
        settle_mock_invoice(&mut st, external_id, settled_at)
    }

    /// Mark a previously-created invoice paid as a RECOVERY settlement: it was settled while the
    /// daemon was DOWN, so the backend has NO live timestamp (`lookup_settlement` reports `None` and
    /// the supervisor catch-up must cap conservatively). Unlike [`settle`](Self::settle) it records
    /// no live ts and does NOT push on the `watch()` stream (there was no watcher). Errors if no such
    /// invoice was created.
    pub fn settle_recovered(&self, external_id: &str) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.invoices.contains_key(external_id) {
            bail!("mock: no invoice for external_id {external_id}");
        }
        st.paid.insert(external_id.to_string());
        Ok(())
    }
}

fn settle_mock_invoice(
    st: &mut MockState,
    external_id: &str,
    settled_at: i64,
) -> Result<Settlement> {
    let inv = st
        .invoices
        .get(external_id)
        .ok_or_else(|| anyhow::anyhow!("mock: no invoice for external_id {external_id}"))?
        .clone();
    st.paid.insert(external_id.to_string());
    st.settled_at.insert(external_id.to_string(), settled_at);
    let s = Settlement {
        invoice_id: inv.id,
        external_id: external_id.to_string(),
        amount_sat: inv.amount_sat,
        received_msat: inv.amount_sat.saturating_mul(1000),
        settled_at,
    };
    if let Some(tx) = &st.settle_tx {
        let _ = tx.try_send(s.clone());
    }
    Ok(s)
}

#[async_trait]
impl PaymentBackend for MockPayment {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        _memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        let mut st = self.state.lock().unwrap();
        if let Some(inv) = st.invoices.get(external_id) {
            return Ok(inv.clone()); // idempotent on external_id (a crash-retry reuses the invoice)
        }
        let n = st.seq;
        st.seq += 1;
        let inv = Invoice {
            id: format!("mock-inv-{n}"),
            external_id: external_id.to_string(),
            backend_invoice_id: format!("mock-bk-{n}"),
            payment_hash: format!("{n:064x}"),
            bolt11: format!("lnbcmock1{external_id}"),
            amount_sat,
            expires_at: st.now + i64::from(expiry_s), // absolute unix secs (matches the field's doc)
        };
        st.invoices.insert(external_id.to_string(), inv.clone());
        Ok(inv)
    }
    async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
        let st = self.state.lock().unwrap();
        match st.invoices.values().find(|inv| inv.id == id) {
            Some(inv) if st.paid.contains(&inv.external_id) => Ok(PaymentStatus::Paid),
            Some(inv) if st.now >= inv.expires_at => Ok(PaymentStatus::Expired), // past its expiry
            Some(_) => Ok(PaymentStatus::Open),
            None => Ok(PaymentStatus::Expired), // unknown id -> gone
        }
    }
    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        let st = self.state.lock().unwrap();
        match st.invoices.values().find(|inv| inv.id == id) {
            // Paid: surface the LIVE ts (`settle`) — `None` for a recovery settle (`settle_recovered`).
            Some(inv) if st.paid.contains(&inv.external_id) => Ok((
                PaymentStatus::Paid,
                st.settled_at.get(&inv.external_id).copied(),
            )),
            Some(inv) if st.now >= inv.expires_at => Ok((PaymentStatus::Expired, None)),
            Some(_) => Ok((PaymentStatus::Open, None)),
            None => Ok((PaymentStatus::Expired, None)),
        }
    }
    async fn pay(&self, _dest: &str, _amount_sat: u64, idempotency_key: &str) -> Result<String> {
        let mut st = self.state.lock().unwrap();
        if let Some(pid) = st.payments.get(idempotency_key) {
            return Ok(pid.clone()); // idempotent on key -> never pays twice
        }
        let n = st.seq;
        st.seq += 1;
        let pid = format!("mock-pay-{n}");
        st.payments.insert(idempotency_key.to_string(), pid.clone());
        Ok(pid)
    }
    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus> {
        let st = self.state.lock().unwrap();
        Ok(if st.payments.values().any(|p| p == payment_id) {
            PayStatus::Succeeded
        } else {
            PayStatus::Unknown
        })
    }
    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
        let st = self.state.lock().unwrap();
        Ok(if st.payments.contains_key(idempotency_key) {
            PayStatus::Succeeded
        } else {
            PayStatus::Unknown
        })
    }
    async fn dev_settle(&self, external_id: &str, settled_at: i64) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let inv = st.invoices.get(external_id).ok_or_else(|| {
            anyhow::anyhow!("mock: no OPEN invoice for external_id {external_id}")
        })?;
        if st.paid.contains(external_id) || st.now >= inv.expires_at {
            bail!("mock: no OPEN invoice for external_id {external_id}");
        }
        settle_mock_invoice(&mut st, external_id, settled_at)?;
        Ok(())
    }
    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        self.state.lock().unwrap().settle_tx = Some(tx);
        Ok(rx)
    }
}

#[cfg(test)]
mod mock_payment_tests {
    use super::*;

    #[tokio::test]
    async fn create_invoice_is_idempotent_on_external_id() {
        let m = MockPayment::new();
        let a = m.create_invoice(1000, "memo", 3600, "ext1").await.unwrap();
        let b = m.create_invoice(9999, "other", 60, "ext1").await.unwrap();
        assert_eq!(
            a.id, b.id,
            "same external_id -> same invoice, never a duplicate"
        );
        assert_eq!(
            b.amount_sat, 1000,
            "the original invoice is returned unchanged"
        );
    }

    #[tokio::test]
    async fn lookup_honors_absolute_expiry() {
        let m = MockPayment::new();
        m.set_now(1_000);
        let inv = m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        assert_eq!(inv.expires_at, 1_060, "absolute expiry = now + expiry_s");
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Open);
        m.set_now(1_100); // past expiry
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Expired);
    }

    #[tokio::test]
    async fn settle_flips_lookup_to_paid_even_past_expiry() {
        let m = MockPayment::new();
        let inv = m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        m.settle("ext1", 30).unwrap();
        m.set_now(10_000); // a paid invoice stays Paid regardless of the clock
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Paid);
    }

    #[tokio::test]
    async fn pay_is_idempotent_on_key() {
        let m = MockPayment::new();
        let p1 = m.pay("dest", 500, "refund:x").await.unwrap();
        let p2 = m.pay("dest", 500, "refund:x").await.unwrap();
        assert_eq!(p1, p2, "same key -> same payment id, never pays twice");
        assert_eq!(
            m.payment_status_by_key("refund:x").await.unwrap(),
            PayStatus::Succeeded
        );
        assert_eq!(
            m.payment_status_by_key("refund:never").await.unwrap(),
            PayStatus::Unknown
        );
    }

    #[tokio::test]
    async fn settle_pushes_on_the_watch_stream() {
        let m = MockPayment::new();
        m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        let mut rx = m.watch().await.unwrap();
        let pushed = m.settle("ext1", 42).unwrap();
        let got = rx.recv().await.expect("a settlement arrives on watch()");
        assert_eq!(got.external_id, "ext1");
        assert_eq!(got.amount_sat, 1000);
        assert_eq!(got.settled_at, 42);
        assert_eq!(pushed.external_id, got.external_id);
    }
}
