//! `lnrent preflight` / `doctor` (lnrent-y4m.9, PR-14): probe the EXTERNAL dependencies bootstrap
//! validation never touches — the configured payment backend (Fedimint's refund gateway, guardians,
//! and lnv2 money path, or the operator's own phoenixd node), the provider API token, and the loaded
//! recipe's provisioning params — and report per-check `{name, ok, detail}` plus an aggregate `ok`.
//! Durable-state validation is strong at bootstrap, but a well-formed-but-WRONG gateway or
//! federation invite passes it and only fails at runtime; `DO_TOKEN` validity used to be a
//! hand-run curl in go-live.md §4 a stranger-operator can skip. This is the machine-readable
//! replacement an operator agent can use to gate subsequent launch promotion (the CLI exits
//! nonzero on any failed check). The daemon publishes its listing before starting IPC, so this is a
//! post-start health gate, not a publication interlock.
//!
//! REUSED seams only — no new probes: on a non-phoenixd backend, gateway =
//! [`PaymentBackend::refund_gateway_ready`] (the
//! money-path gateway-readiness probe; fails closed, the Err carries the
//! diagnostic), federation = [`PaymentBackend::backend_ready`] (the
//! urw.4 `session_count()` guardian round-trip, distinct from gateway/balance — it proves the
//! JOINED federation answers now; a bad invite fails `join_or_open` at daemon startup, so
//! preflight surfaces it as a daemon-unreachable nonzero exit, not as this labeled check),
//! phoenixd = [`PaymentBackend::phoenixd_probe`] (lnrent-5mi: the SAME authenticated `getinfo` +
//! verified-release predicate the phoenixd money path already fails closed on, classified into the
//! distinct operator remedies), provider token = the authenticated
//! `GET /v2/account` go-live.md §4 did by hand, with the token resolved from the daemon env
//! exactly as `runner::hook_env` forwards it to the do-vps hooks. Read-only end to end: the only
//! network I/O is the probes themselves; no store write, no money call.

use crate::backends::{
    safe_phoenixd_version, Lnv2Probe, PaymentBackend, PhoenixdProbe, REDACTED_PHOENIXD_VERSION,
};
use crate::recipe::Recipe;
use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

/// The env var the do-vps recipe declares in `provisioning.env` and its hooks read for the
/// DigitalOcean API token (`: "${DO_TOKEN:?}"`). Preflight keys the provider-token check on the
/// same declaration, so "the DO recipe is configured" and "the hooks will demand the token" can
/// never disagree.
pub const DO_TOKEN_ENV: &str = "DO_TOKEN";

/// The real DigitalOcean API base; `/v2/account` is the cheapest authenticated read — exactly the
/// endpoint the go-live.md §4 manual check curled.
const DO_API_BASE: &str = "https://api.digitalocean.com";

/// Bounded token-probe timeout, mirroring the refund resolver's LNURL fetch bound — a black-holed
/// provider API fails the check with a diagnostic instead of hanging the command.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound BOTH reused readiness seams at the operator-command layer: `backend_ready` is a guardian
/// round-trip directly, and the gateway probe's selection refreshes the federation's gateway
/// registrations — itself a guardian round-trip. Some supported federation transports have no
/// request timeout, so relying on upstream defaults can leave preflight without the failure
/// report and exit status it exists to provide.
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(15);

/// One preflight check result — the machine-readable `{name, ok, detail}` contract. A SKIPPED
/// (genuinely not-configured) check reports `ok: true` with the reasoning in `detail`, so the
/// aggregate stays a plain all-of; a dependency that IS configured but unusable is always a
/// failure, never a skip.
#[derive(Debug, Clone, Serialize)]
pub struct PreflightCheck {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

impl PreflightCheck {
    fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            ok: true,
            detail: detail.into(),
        }
    }
    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            ok: false,
            detail: detail.into(),
        }
    }
}

/// The provider-token HTTP round-trip, factored behind a trait so every unit test stubs it — NO
/// test may hit the real DigitalOcean API. Returns the HTTP status code; a transport failure
/// (DNS, connect, timeout) is `Err`.
#[async_trait]
pub trait ProviderTokenProbe: Send + Sync {
    async fn account_status(&self, token: &str) -> Result<u16>;
}

/// The production probe: an authenticated GET to the DigitalOcean account endpoint over the same
/// reqwest/rustls stack the refund resolver already depends on (no new HTTP dependency). The
/// token travels ONLY in the Authorization header — never in the URL, a log line, or a
/// diagnostic string.
pub struct DoTokenProbe {
    base_url: String,
}

impl DoTokenProbe {
    pub fn new() -> Self {
        Self {
            base_url: DO_API_BASE.to_string(),
        }
    }
}

impl Default for DoTokenProbe {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProviderTokenProbe for DoTokenProbe {
    async fn account_status(&self, token: &str) -> Result<u16> {
        // A per-call client is fine: preflight is a rare operator command. Redirects are refused
        // (the account endpoint answers directly; following one could replay the Authorization
        // header elsewhere) and rustls is forced for the same feature-unification reason as the
        // refund resolver's client.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(PROBE_TIMEOUT)
            .use_rustls_tls()
            .build()?;
        let resp = client
            .get(format!("{}/v2/account", self.base_url))
            .bearer_auth(token)
            .send()
            .await?;
        Ok(resp.status().as_u16())
    }
}

/// Resolve the operator's provider token the SAME way a hook receives it: from the daemon
/// environment (`runner::hook_env` forwards `DO_TOKEN` to a declaring recipe's hooks via
/// `std::env::var`). Wrapped in a zeroizing guard so preflight's copy wipes on drop; the value is
/// never logged or echoed — only its status is reported.
pub fn read_token_env() -> Option<Zeroizing<String>> {
    std::env::var(DO_TOKEN_ENV).ok().map(Zeroizing::new)
}

/// Run the checks in a stable order and fold their aggregate. Pure over the injected seams except
/// for optional recipe hooks, which use the bounded [`crate::runner::run_hook`] path.
///
/// SERIALIZED process-wide (adversarial y4m.9 review): each report holds several bounded network
/// probes plus up to [`crate::runner::DEFAULT_TIMEOUT`] per recipe hook, and the IPC loop spawns one
/// task per connection, so unserialized concurrent preflights would amplify load onto the shared
/// fedimint client and provider APIs (contending with money-path operations). Queued callers still
/// each get a fresh report; every probe inside is individually time-bounded, so the queue drains.
pub async fn preflight_report(
    payment: &Arc<dyn PaymentBackend>,
    recipes: &[Recipe],
    token: Option<Zeroizing<String>>,
    probe: &dyn ProviderTokenProbe,
) -> Value {
    static SERIALIZE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _one_at_a_time = SERIALIZE.lock().await;
    // Ask the backend-specific seam first. `NotApplicable` is immediate for mock/Fedimint. For
    // phoenixd, its classified result is also the authority that the Fedimint-only probes do not
    // apply; render those required structural slots as explicit skips instead of making two more
    // identical getinfo calls and claiming nonexistent gateways/guardians are reachable.
    let phoenixd = phoenixd_check(payment).await;
    let mut checks = if let Some(phoenixd) = phoenixd {
        vec![
            PreflightCheck::pass("gateway", "skipped (phoenixd backend has no refund gateway)"),
            PreflightCheck::pass(
                "federation",
                "skipped (phoenixd backend has no federation guardians)",
            ),
            PreflightCheck::pass("lnv2", "skipped (phoenixd backend has no lnv2 money path)"),
            phoenixd,
        ]
    } else {
        vec![
            gateway_check(payment).await,
            federation_check(payment).await,
            lnv2_check(payment).await,
        ]
    };
    checks.push(provider_token_check(recipes, token, probe).await);
    if let Some(check) = recipe_preflight_check(recipes).await {
        checks.push(check);
    }
    json!({
        "ok": aggregate_ok(&checks),
        "checks": checks,
    })
}

/// The aggregate verdict the CLI's nonzero-exit gate rests on: every check `ok`. Pure. A skipped
/// check counts ok only when the dependency is genuinely not configured or the loaded recipe does
/// not declare the optional hook (see [`provider_token_check`] and [`recipe_preflight_check`]).
pub fn aggregate_ok(checks: &[PreflightCheck]) -> bool {
    checks.iter().all(|c| c.ok)
}

/// Gateway reachability through the existing y4m.8 failover-aware money-path selection seam.
/// `Ok(false)` and `Err` are BOTH failures (the same fails-closed folding as
/// `RefundReadinessProbe::query`), each with its own diagnostic — the `Err` carries the backend's
/// selection diagnostic. `Ok(true)` from a backend without a gateway concept (the mock's trait
/// default) is a trivial pass: the seam cannot distinguish it, and it is aggregate-equivalent to a
/// not-configured skip. Bounded exactly like the federation check: the selection probe refreshes
/// the federation's gateway registrations — itself a guardian round-trip with no upstream
/// timeout — and this check runs FIRST, so an unbounded stall here would hang the whole command
/// before any check reports.
async fn gateway_check(payment: &Arc<dyn PaymentBackend>) -> PreflightCheck {
    gateway_check_with_timeout(payment, READINESS_PROBE_TIMEOUT).await
}

async fn gateway_check_with_timeout(
    payment: &Arc<dyn PaymentBackend>,
    timeout: Duration,
) -> PreflightCheck {
    const NAME: &str = "gateway";
    match tokio::time::timeout(timeout, payment.refund_gateway_ready()).await {
        Ok(Ok(true)) => PreflightCheck::pass(NAME, "refund gateway reachable"),
        Ok(Ok(false)) => PreflightCheck::fail(
            NAME,
            "no configured gateway is reachable (backend reports not-ready)",
        ),
        Ok(Err(e)) => PreflightCheck::fail(NAME, format!("gateway probe failed: {e:#}")),
        Err(_) => PreflightCheck::fail(NAME, format!("gateway probe timed out after {timeout:?}")),
    }
}

/// Federation reachability via the urw.4 guardian round-trip (`session_count()`), distinct from
/// the gateway and balance reads. This proves the federation the daemon ACTUALLY JOINED answers a
/// guardian round-trip NOW — it does NOT re-validate the configured invite: an unusable invite
/// fails `join_or_open` at daemon startup, BEFORE the IPC socket exists (main.rs builds the
/// backend before the supervisor serves IPC), so preflight surfaces that as the CLI's
/// daemon-unreachable failure (exit 4 — the command still fails nonzero, just without this check's
/// label), and a live-but-unintended federation the daemon already joined passes.
/// `Ok(true)` from a backend with no federation (the mock) is a trivial pass, exactly as for the
/// gateway. The whole readiness seam is bounded here because not every supported guardian
/// transport supplies its own request timeout.
async fn federation_check(payment: &Arc<dyn PaymentBackend>) -> PreflightCheck {
    federation_check_with_timeout(payment, READINESS_PROBE_TIMEOUT).await
}

async fn federation_check_with_timeout(
    payment: &Arc<dyn PaymentBackend>,
    timeout: Duration,
) -> PreflightCheck {
    const NAME: &str = "federation";
    match tokio::time::timeout(timeout, payment.backend_ready()).await {
        Ok(Ok(true)) => PreflightCheck::pass(NAME, "federation guardians reachable"),
        Ok(Ok(false)) => PreflightCheck::fail(
            NAME,
            "federation guardians unreachable (backend reports not-ready)",
        ),
        Ok(Err(e)) => PreflightCheck::fail(NAME, format!("federation probe failed: {e:#}")),
        Err(_) => PreflightCheck::fail(
            NAME,
            format!("federation probe timed out after {timeout:?}"),
        ),
    }
}

/// FUNCTIONAL lnv2 money-path probe (ADR-0018, lnrent-3d5): is the lnv2 module present on the joined
/// federation AND an lnv2-capable gateway attached and reachable? Config-presence is insufficient
/// (ADR-0018), so the backend reaches the guardians and the gateway. Each failure state gets its own
/// human diagnostic (module absent vs gateway absent vs gateway unreachable vs guardians unreachable).
/// A backend with no lnv2 money path (the mock) reports `NotApplicable`, rendered as a SKIPPED
/// (passing) check exactly like the provider-token skip — the check simply does not apply. Bounded
/// like the other readiness probes.
async fn lnv2_check(payment: &Arc<dyn PaymentBackend>) -> PreflightCheck {
    lnv2_check_with_timeout(payment, READINESS_PROBE_TIMEOUT).await
}

async fn lnv2_check_with_timeout(
    payment: &Arc<dyn PaymentBackend>,
    timeout: Duration,
) -> PreflightCheck {
    const NAME: &str = "lnv2";
    match tokio::time::timeout(timeout, payment.lnv2_functional_probe()).await {
        Ok(Ok(Lnv2Probe::Healthy)) => {
            PreflightCheck::pass(NAME, "lnv2 module present and an lnv2 gateway is reachable")
        }
        Ok(Ok(Lnv2Probe::NotApplicable)) => {
            PreflightCheck::pass(NAME, "skipped (backend has no lnv2 money path)")
        }
        Ok(Ok(Lnv2Probe::GuardiansUnreachable(e))) => {
            PreflightCheck::fail(NAME, format!("federation guardians unreachable: {e}"))
        }
        Ok(Ok(Lnv2Probe::ModuleAbsent)) => PreflightCheck::fail(
            NAME,
            "federation has no lnv2 module (join an lnv2-enabled federation)",
        ),
        Ok(Ok(Lnv2Probe::GatewayAbsent)) => PreflightCheck::fail(
            NAME,
            "lnv2 module present but no lnv2 gateway is attached to the federation",
        ),
        Ok(Ok(Lnv2Probe::GatewayUnreachable(e))) => {
            PreflightCheck::fail(NAME, format!("lnv2 gateway attached but unreachable: {e}"))
        }
        Ok(Err(e)) => PreflightCheck::fail(NAME, format!("lnv2 probe failed: {e:#}")),
        Err(_) => PreflightCheck::fail(NAME, format!("lnv2 probe timed out after {timeout:?}")),
    }
}

const PHOENIXD_CHECK: &str = "phoenixd";

/// FUNCTIONAL phoenixd probe (ADR-0019, lnrent-5mi): for an operator whose `payment_backend` is
/// phoenixd, is that EXTERNAL node reachable, is the api password accepted, and is the running
/// release the one its trampoline fee schedule was verified against? Those are exactly the two
/// conditions the phoenixd money path fails CLOSED on, so until this check existed an operator first
/// met them as a stalled refund. A backend with no phoenixd money path reports `NotApplicable`, and
/// the check is OMITTED so a mock/Fedimint operator's report remains unchanged. Consequently
/// `PREFLIGHT_REQUIRED_CHECKS` cannot require this backend-conditional name; when it is present, the
/// CLI's every-check gate still requires it to pass.
///
/// Bounded by the same [`READINESS_PROBE_TIMEOUT`] as the other readiness probes, and that ONE bound
/// deliberately covers BOTH of the probe's round-trips (`getinfo` then `getbalance`): the bound
/// exists to keep the operator's command from hanging, not to give each HTTP call its own budget. A
/// phoenixd node that cannot answer two trivial reads inside it is not a node an operator should be
/// promoting to launch.
///
/// That bound is SHORTER than the phoenixd client's own per-call HTTP timeout (30s), so a node whose
/// packets are DROPped — the ordinary firewall/wedged case — trips THIS timeout before reqwest can
/// produce a transport error, i.e. it lands in the timeout verdict rather than in
/// [`PhoenixdProbe::Unreachable`]. The timeout verdict therefore has to carry its own remedy, not
/// just the elapsed bound.
async fn phoenixd_check(payment: &Arc<dyn PaymentBackend>) -> Option<PreflightCheck> {
    phoenixd_check_with_timeout(payment, READINESS_PROBE_TIMEOUT).await
}

async fn phoenixd_check_with_timeout(
    payment: &Arc<dyn PaymentBackend>,
    timeout: Duration,
) -> Option<PreflightCheck> {
    match tokio::time::timeout(timeout, payment.phoenixd_probe()).await {
        Ok(Ok(probe)) => phoenixd_check_from_probe(probe),
        Ok(Err(e)) => Some(PreflightCheck::fail(
            PHOENIXD_CHECK,
            format!("phoenixd probe failed: {e:#}"),
        )),
        Err(_) => Some(PreflightCheck::fail(
            PHOENIXD_CHECK,
            format!(
                "phoenixd probe timed out after {timeout:?} — a node that is unroutable, firewalled \
                 (packets DROPped) or wedged lands here instead of in a transport error, so check \
                 that `[phoenixd] url` reaches the node, that the node is running and responsive, \
                 and retry"
            ),
        )),
    }
}

/// PURE probe -> operator verdict rendering for [`phoenixd_check`]. Every failure names its own
/// REMEDY, because the operator reading this is a stranger to the money path.
///
/// A non-zero `feeCreditSat` is reported PROMINENTLY but does NOT fail the check: it is money
/// phoenixd's `receivedSat` counted that `balanceSat` cannot spend (ADR-0019, lnrent-itw), and a
/// perfectly healthy funded node can legitimately hold some. Failing on it would make preflight
/// unpassable for a node that is working exactly as designed; the operator still needs to SEE it,
/// because that much of the receipts cannot fund a refund. The SPENDABLE `balanceSat` is printed
/// beside it for the same reason — the credit is only interpretable next to the balance it is
/// excluded from — but neither figure is judged here: a "is the wallet funded enough" rule is
/// lnrent-tof, and this check deliberately does not invent one.
///
/// §13: the RUNNING version is remote-controlled operator output, so even the backend's
/// credential-aware sanitization is enforced again here with the strict public version shape before
/// rendering. The VERIFIED version is local config and is rendered through
/// [`configured_version`] instead — see there for why the remote grammar must not be applied to it.
pub(crate) fn phoenixd_check_from_probe(probe: PhoenixdProbe) -> Option<PreflightCheck> {
    const NAME: &str = PHOENIXD_CHECK;
    let check = match probe {
        // `NotApplicable` has NO rendering: a backend with no phoenixd money path must produce no
        // phoenixd line AT ALL, not a passing "skipped" one — unlike `lnv2`, which does report itself
        // skipped, so that a mock/Fedimint operator's report is left exactly as this bead found it.
        // The omit decision lives here, in the one function that turns a probe into report text; a
        // "skipped" arm in this match would be unreachable text describing behaviour no report has.
        PhoenixdProbe::NotApplicable => return None,
        PhoenixdProbe::Ready {
            version,
            verified_version,
            balance_sat,
            fee_credit_sat,
        } => {
            let version =
                safe_phoenixd_version(&version, None).unwrap_or(REDACTED_PHOENIXD_VERSION);
            let verified_version = configured_version(&verified_version);
            let mut detail = format!(
                "phoenixd {version} reachable and the api password was accepted; its trampoline fee \
                 schedule is verified for {verified_version}; spendable balance {balance_sat} sat"
            );
            if fee_credit_sat > 0 {
                detail.push_str(&format!(
                    "; NOTE fee credit {fee_credit_sat} sat is NOT spendable — phoenixd counted it \
                     in receivedSat but balanceSat cannot pay it out, so that much of what was \
                     received cannot fund a refund (not a failure)"
                ));
            }
            PreflightCheck::pass(NAME, detail)
        }
        PhoenixdProbe::AuthRejected { status } => PreflightCheck::fail(
            NAME,
            format!(
                "phoenixd REJECTED the api password (HTTP {status}) — set `[phoenixd] api_password` \
                 to that node's `http-password` (phoenixd.conf) and restart the daemon"
            ),
        ),
        // Every non-401/403 failure lands here, INCLUDING an HTTP answer the probe cannot use (a
        // proxy's 502, a 404 from a wrong sub-path), so the lead sentence says "no usable answer"
        // rather than "did not answer" — the appended diagnostic carries the status when there was
        // one, and naming the url's PATH is what a 404 actually needs.
        PhoenixdProbe::Unreachable(e) => PreflightCheck::fail(
            NAME,
            format!(
                "phoenixd returned no usable answer to an authenticated getinfo — check that the \
                 node is running and that `[phoenixd] url` (including its path) points at it: {e}"
            ),
        ),
        PhoenixdProbe::BalanceUnavailable(e) => PreflightCheck::fail(
            NAME,
            format!(
                "phoenixd answered authenticated getinfo, but getbalance failed while reading \
                 feeCreditSat — check the node or its fronting proxy and retry: {e}"
            ),
        ),
        PhoenixdProbe::VersionMismatch { running, verified } => {
            let running =
                safe_phoenixd_version(&running, None).unwrap_or(REDACTED_PHOENIXD_VERSION);
            let verified = configured_version(&verified);
            PreflightCheck::fail(
                NAME,
                format!(
                    "phoenixd {running} is not the {verified} release its trampoline fee schedule was \
                     verified against, so lnrent cannot bound an outbound fee on it and every automated \
                     refund fails closed — verify this release's schedule and configure it ([phoenixd] \
                     fee_schedule_version/fee_base_msat/fee_ppm)"
                ),
            )
        }
    };
    Some(check)
}

/// Replacement for operator-configured version text that cannot be printed into a report as-is.
const UNPRINTABLE_CONFIGURED_VERSION: &str = "<unprintable configured version>";

/// Render the VERIFIED version — `[phoenixd] fee_schedule_version`, or the build's default schedule.
///
/// This is LOCAL operator config, not remote input, so it deliberately does NOT go through
/// [`safe_phoenixd_version`]'s measured `MAJOR.MINOR.PATCH[-<hex>]` grammar. `matches_running_version`
/// accepts a WHOLE-STRING pin, so an operator can legitimately be verified against a build whose id
/// is not that shape (`0.9.0-rc1`, a self-built `-SNAPSHOT`); redacting it would make the passing
/// detail name no release at all, and — worse — would leave the MISMATCH remedy unable to say which
/// release the schedule was configured for, which is the one sentence that operator needs.
///
/// Only report hygiene is enforced (printable single-token ASCII, length-bounded), so config text can
/// neither inject control characters into a pasted report nor swamp it. There is no credential to
/// redact here: this string is the fee schedule's, and the api password is never copied into it
/// ([`PhoenixdProbe`]). Filtering it for password CONTAINMENT would also invert the property it
/// claims to protect — the schedule version is otherwise the same public constant for every operator
/// on the default schedule, so a `<redacted>` in its place would itself signal what the password is.
fn configured_version(version: &str) -> &str {
    let printable =
        !version.is_empty() && version.len() <= 64 && version.bytes().all(|b| b.is_ascii_graphic());
    if printable {
        version
    } else {
        UNPRINTABLE_CONFIGURED_VERSION
    }
}

/// Provider-token validity — the go-live.md §4 manual curl, automated. Required iff a loaded
/// recipe declares [`DO_TOKEN_ENV`] in its `provisioning.env` passthrough (the same declaration
/// `runner::hook_env` resolves at hook-spawn time): then an ABSENT token is a FAILURE (the first
/// provision would die on the hooks' `: "${DO_TOKEN:?}"` guard), and a REJECTED token (401/403),
/// an unexpected status, and an unreachable API each fail with a distinct diagnostic. No recipe
/// declaring it ⇒ genuinely not configured ⇒ skipped-ok. The token value NEVER appears in a
/// detail string — only its status.
///
/// A pass proves the token AUTHENTICATES; it cannot prove the droplet WRITE scopes the recipe's
/// provision/lifecycle hooks need. The provider offers no side-effect-free scope introspection,
/// and any write probe would mutate billable provider state, so a read-only token is first
/// caught by the provision hooks' own 403 — the pass detail states that limit explicitly.
async fn provider_token_check(
    recipes: &[Recipe],
    token: Option<Zeroizing<String>>,
    probe: &dyn ProviderTokenProbe,
) -> PreflightCheck {
    const NAME: &str = "provider_token";
    let required = recipes
        .iter()
        .any(|r| r.provisioning.env.iter().any(|name| name == DO_TOKEN_ENV));
    if !required {
        return PreflightCheck::pass(
            NAME,
            format!(
                "skipped: no loaded recipe declares {DO_TOKEN_ENV} (no provider API configured)"
            ),
        );
    }
    // Blank/whitespace counts as unusable. The hooks reject an empty value immediately; whitespace
    // would reach the provider and be rejected later, so preflight fails both before making a request.
    let Some(token) = token.filter(|t| !t.trim().is_empty()) else {
        return PreflightCheck::fail(
            NAME,
            format!(
                "{DO_TOKEN_ENV} is not set (or is blank) in the daemon environment, but the loaded recipe \
                 declares it — provision hooks will fail"
            ),
        );
    };
    // A present-but-malformed token (control chars, inner whitespace, non-ASCII — e.g. a stray
    // newline from a careless paste) can never form a valid Authorization header; without this
    // arm the header-construction error would be mislabeled "provider API unreachable"
    // (adversarial y4m.9 review) — keep the absent/malformed/rejected/unreachable diagnostics
    // distinct. The token VALUE still never appears in the detail.
    if token.chars().any(|c| !c.is_ascii_graphic()) {
        return PreflightCheck::fail(
            NAME,
            format!(
                "{DO_TOKEN_ENV} is set but MALFORMED (contains whitespace, control, or non-ASCII \
                 characters) — re-copy the token"
            ),
        );
    }
    match probe.account_status(&token).await {
        Ok(status) if (200..300).contains(&status) => PreflightCheck::pass(
            NAME,
            format!(
                "token accepted by the provider API (HTTP {status}); proves authentication only — \
                 provisioning WRITE scopes are first exercised by a real provision"
            ),
        ),
        Ok(status @ (401 | 403)) => PreflightCheck::fail(
            NAME,
            format!(
                "token REJECTED by the provider API (HTTP {status}) — invalid, revoked, or \
                 under-scoped"
            ),
        ),
        Ok(status) => PreflightCheck::fail(
            NAME,
            format!("unexpected provider API response (HTTP {status})"),
        ),
        Err(e) => PreflightCheck::fail(NAME, format!("provider API unreachable: {e:#}")),
    }
}

/// Fold recipe-owned provisioning validation into one provider-agnostic check. Runner failures
/// retain structured hook stderr in the detail; a loaded recipe without the optional hook skips.
async fn recipe_preflight_check(recipes: &[Recipe]) -> Option<PreflightCheck> {
    const NAME: &str = "recipe_preflight";
    if recipes.is_empty() {
        return None;
    }
    let mut ran_hook = false;
    for recipe in recipes {
        let hook = recipe.preflight_hook();
        if !hook.exists() {
            continue;
        }
        ran_hook = true;
        if let Err(e) = crate::runner::run_hook(
            &hook,
            &json!({}),
            crate::runner::DEFAULT_TIMEOUT,
            &recipe.provisioning.env,
        )
        .await
        {
            return Some(PreflightCheck::fail(
                NAME,
                format!(
                    "recipe `{}` preflight hook rejected the provisioning params: {e:#}",
                    recipe.service.id
                ),
            ));
        }
    }
    Some(if ran_hook {
        PreflightCheck::pass(
            NAME,
            "recipe preflight hook validated provisioning parameters",
        )
    } else {
        PreflightCheck::pass(NAME, "skipped: recipe declares no preflight hook")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{Invoice, PayStatus, PaymentStatus, Settlement};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    /// What one of the two reused readiness seams reports in a test.
    #[derive(Clone, Copy)]
    enum Seam {
        Ready,
        NotReady,
        Broken,
        Pending,
    }

    /// A backend exposing ONLY the reused readiness seams; every money method PANICS, so a passing
    /// test also proves preflight is read-only on the payment backend.
    struct SeamPayment {
        gateway: Seam,
        federation: Seam,
        lnv2: Lnv2Probe,
        phoenixd: PhoenixdProbe,
    }

    /// The common two-seam backend: the lnv2 and phoenixd probes report `NotApplicable`, as a backend
    /// with neither money path does — so the existing gateway/federation tests are unaffected.
    fn seam_payment(gateway: Seam, federation: Seam) -> Arc<dyn PaymentBackend> {
        Arc::new(SeamPayment {
            gateway,
            federation,
            lnv2: Lnv2Probe::NotApplicable,
            phoenixd: PhoenixdProbe::NotApplicable,
        })
    }

    /// A backend whose lnv2 functional probe reports `lnv2` (for the doctor negative matrix), with
    /// guardians + gateway seams held Ready so the lnv2 check is the one under test.
    fn seam_payment_lnv2(lnv2: Lnv2Probe) -> Arc<dyn PaymentBackend> {
        Arc::new(SeamPayment {
            gateway: Seam::Ready,
            federation: Seam::Ready,
            lnv2,
            phoenixd: PhoenixdProbe::NotApplicable,
        })
    }

    /// A phoenixd operator's backend: the Fedimint-only readiness seams are deliberately BROKEN, so a
    /// passing phoenixd test also proves preflight did not call or render them as real probes.
    fn seam_payment_phoenixd(phoenixd: PhoenixdProbe) -> Arc<dyn PaymentBackend> {
        Arc::new(SeamPayment {
            gateway: Seam::Broken,
            federation: Seam::Broken,
            lnv2: Lnv2Probe::NotApplicable,
            phoenixd,
        })
    }

    #[async_trait]
    impl PaymentBackend for SeamPayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            panic!("preflight must not create invoices")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            panic!("preflight must not look up invoices")
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            panic!("preflight must not look up settlements")
        }
        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            panic!("preflight must not pay")
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            panic!("preflight must not check payment status")
        }
        async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
            panic!("preflight must not check payment status by key")
        }
        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            panic!("preflight must not read the wallet balance")
        }
        async fn refund_gateway_ready(&self) -> Result<bool> {
            match self.gateway {
                Seam::Ready => Ok(true),
                Seam::NotReady => Ok(false),
                Seam::Broken => anyhow::bail!("gateway socket refused"),
                Seam::Pending => std::future::pending().await,
            }
        }
        async fn backend_ready(&self) -> Result<bool> {
            match self.federation {
                Seam::Ready => Ok(true),
                Seam::NotReady => Ok(false),
                Seam::Broken => anyhow::bail!("guardians timed out"),
                Seam::Pending => std::future::pending().await,
            }
        }
        async fn lnv2_functional_probe(&self) -> Result<Lnv2Probe> {
            Ok(self.lnv2.clone())
        }
        async fn phoenixd_probe(&self) -> Result<PhoenixdProbe> {
            Ok(self.phoenixd.clone())
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            panic!("preflight must not watch settlements")
        }
    }

    /// A stub probe answering a fixed HTTP status.
    struct StatusProbe(u16);
    #[async_trait]
    impl ProviderTokenProbe for StatusProbe {
        async fn account_status(&self, token: &str) -> Result<u16> {
            assert!(
                !token.trim().is_empty(),
                "the probe never sees a blank token"
            );
            Ok(self.0)
        }
    }

    /// A stub probe failing at the transport layer.
    struct FailProbe;
    #[async_trait]
    impl ProviderTokenProbe for FailProbe {
        async fn account_status(&self, _: &str) -> Result<u16> {
            anyhow::bail!("dns lookup failed")
        }
    }

    /// A probe that must never run (the skip / absent-token arms decide BEFORE any network call).
    struct PanicProbe;
    #[async_trait]
    impl ProviderTokenProbe for PanicProbe {
        async fn account_status(&self, _: &str) -> Result<u16> {
            panic!("the provider API must not be probed on this path")
        }
    }

    fn load_recipe(name: &str) -> Recipe {
        let dir = format!("{}/../recipes/{name}", env!("CARGO_MANIFEST_DIR"));
        Recipe::load(&dir).expect("load recipe")
    }

    /// Require the token probe without loading do-vps's real networked preflight hook.
    fn do_token_recipe() -> Recipe {
        let mut r = load_recipe("dummy");
        r.provisioning.env = vec![DO_TOKEN_ENV.to_string()];
        r
    }

    /// Point a valid recipe at an optional fake executable hook.
    fn temp_recipe_with_preflight(name: &str, script: Option<&str>) -> Recipe {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-preflight-1sr-{name}-{}-{nanos}-{seq}",
            std::process::id()
        ));
        std::fs::create_dir(&dir).unwrap();
        if let Some(body) = script {
            let path = dir.join("preflight");
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = load_recipe("dummy");
        r.dir = dir;
        r
    }

    /// Look a check up BY NAME, so a test asserting on one check does not encode the position of
    /// every other check in the report.
    fn named<'a>(v: &'a Value, name: &str) -> &'a Value {
        checks(v)
            .iter()
            .find(|c| c["name"] == json!(name))
            .unwrap_or_else(|| panic!("{name} check present"))
    }

    fn recipe_preflight(v: &Value) -> &Value {
        named(v, "recipe_preflight")
    }

    fn provider_token(v: &Value) -> &Value {
        named(v, "provider_token")
    }

    fn phoenixd(v: &Value) -> &Value {
        named(v, "phoenixd")
    }

    fn token(s: &str) -> Option<Zeroizing<String>> {
        Some(Zeroizing::new(s.to_string()))
    }

    fn checks(v: &Value) -> &Vec<Value> {
        v["checks"].as_array().expect("checks array")
    }

    // The happy path on a deployment with no provider recipe (mock/dummy): both seams pass, the
    // token check is SKIPPED-ok (PanicProbe proves no network attempt even with a token set),
    // and the aggregate is ok. The check order is a stable contract.
    #[tokio::test]
    async fn all_ready_without_provider_recipe_is_ok_and_never_probes_the_api() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let v = preflight_report(
            &payment,
            &[load_recipe("dummy")],
            token("dop_v1_x"),
            &PanicProbe,
        )
        .await;

        assert_eq!(v["ok"], json!(true));
        let names: Vec<&str> = checks(&v)
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec![
                "gateway",
                "federation",
                "lnv2",
                "provider_token",
                "recipe_preflight"
            ]
        );
        assert!(checks(&v).iter().all(|c| c["ok"] == json!(true)));
        assert!(
            named(&v, "lnv2")["detail"]
                .as_str()
                .unwrap()
                .contains("skipped"),
            "a non-lnv2 backend reports the lnv2 check skipped"
        );
        assert!(
            checks(&v).iter().all(|check| check["name"] != "phoenixd"),
            "a non-phoenixd backend omits the backend-conditional phoenixd check"
        );
        assert!(
            provider_token(&v)["detail"]
                .as_str()
                .unwrap()
                .contains("skipped"),
            "an undeclared provider token reports skipped, not validated"
        );
    }

    // Doctor negative matrix (ADR-0018, lnrent-3d5): each lnv2 federation state renders a distinct
    // `lnv2` check verdict + human diagnostic. Exit-code + full --json round-trip live in
    // tests/preflight_cli.rs; here we assert the per-state classification the CLI renders.
    #[tokio::test]
    async fn lnv2_probe_matrix_renders_each_state() {
        // Healthy -> pass.
        let v = preflight_report(&seam_payment_lnv2(Lnv2Probe::Healthy), &[], None, &PanicProbe).await;
        assert_eq!(checks(&v)[2]["name"], json!("lnv2"));
        assert_eq!(checks(&v)[2]["ok"], json!(true));
        assert!(checks(&v)[2]["detail"].as_str().unwrap().contains("module present"));

        // Guardians unreachable -> fail with the guardian diagnostic.
        let v = preflight_report(
            &seam_payment_lnv2(Lnv2Probe::GuardiansUnreachable("no consensus".into())),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        assert_eq!(v["ok"], json!(false));
        assert_eq!(checks(&v)[2]["ok"], json!(false));
        assert!(checks(&v)[2]["detail"]
            .as_str()
            .unwrap()
            .contains("guardians unreachable"));

        // Module absent -> fail with the module diagnostic.
        let v = preflight_report(&seam_payment_lnv2(Lnv2Probe::ModuleAbsent), &[], None, &PanicProbe)
            .await;
        assert_eq!(checks(&v)[2]["ok"], json!(false));
        assert!(checks(&v)[2]["detail"].as_str().unwrap().contains("no lnv2 module"));

        // Gateway absent -> fail with the gateway-absent diagnostic (distinct from unreachable).
        let v = preflight_report(&seam_payment_lnv2(Lnv2Probe::GatewayAbsent), &[], None, &PanicProbe)
            .await;
        assert_eq!(checks(&v)[2]["ok"], json!(false));
        assert!(checks(&v)[2]["detail"]
            .as_str()
            .unwrap()
            .contains("no lnv2 gateway is attached"));

        // Gateway attached but unreachable -> fail with the unreachable diagnostic.
        let v = preflight_report(
            &seam_payment_lnv2(Lnv2Probe::GatewayUnreachable("connection refused".into())),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        assert_eq!(checks(&v)[2]["ok"], json!(false));
        assert!(checks(&v)[2]["detail"]
            .as_str()
            .unwrap()
            .contains("gateway attached but unreachable"));
    }

    // The phoenixd operator's happy path (lnrent-5mi): the check PASSES and its detail NAMES the
    // running version, so the report itself records which release the fee schedule was accepted for.
    #[tokio::test]
    async fn phoenixd_ready_passes_and_names_the_version() {
        let payment = seam_payment_phoenixd(PhoenixdProbe::Ready {
            version: "0.9.0-a1b2c3d".into(),
            verified_version: "0.9.0".into(),
            balance_sat: 50_000,
            fee_credit_sat: 0,
        });
        let v = preflight_report(&payment, &[], None, &PanicProbe).await;

        assert_eq!(v["ok"], json!(true));
        let check = phoenixd(&v);
        assert_eq!(check["ok"], json!(true));
        let detail = check["detail"].as_str().unwrap();
        assert!(
            detail.contains("0.9.0-a1b2c3d"),
            "names the version: {detail}"
        );
        assert!(
            detail.contains("50000"),
            "the spendable balance is reported: {detail}"
        );
        assert!(
            !detail.contains("fee credit"),
            "no fee credit, no note: {detail}"
        );
    }

    // A non-zero `feeCreditSat` is money `receivedSat` counted that `balanceSat` cannot spend
    // (ADR-0019 / lnrent-itw). It is surfaced PROMINENTLY but does NOT fail the check: a funded node
    // can legitimately hold fee credit, and failing on it would make preflight unpassable for a node
    // working as designed.
    #[tokio::test]
    async fn non_zero_fee_credit_is_surfaced_without_failing() {
        let payment = seam_payment_phoenixd(PhoenixdProbe::Ready {
            version: "0.9.0-b072567".into(),
            verified_version: "0.9.0".into(),
            balance_sat: 411,
            fee_credit_sat: 2_277,
        });
        let v = preflight_report(&payment, &[], None, &PanicProbe).await;

        assert_eq!(v["ok"], json!(true), "fee credit is not a failure");
        let check = phoenixd(&v);
        assert_eq!(check["ok"], json!(true));
        let detail = check["detail"].as_str().unwrap();
        assert!(detail.contains("2277"), "the amount is surfaced: {detail}");
        assert!(
            detail.contains("NOT spendable"),
            "what it MEANS is surfaced, not just a number: {detail}"
        );
        // The credit is only interpretable NEXT TO the balance it is excluded from: 2277 sat of
        // unspendable credit reads very differently against 411 sat than against 500k. Reported,
        // never judged — a thin balance is not a failure here (lnrent-tof owns that).
        assert!(
            detail.contains("411"),
            "the spendable balance is reported beside it: {detail}"
        );
    }

    // A successful getinfo response is still remote-controlled and the endpoint sees the Basic
    // header. Credential/header echoes and control characters must be replaced before the report is
    // pasted into an issue, on both the compatible and mismatch render paths.
    #[test]
    fn remote_controlled_version_text_is_redacted() {
        for probe in [
            PhoenixdProbe::Ready {
                version: "0.9.0-Basic OnNlY3JldA==\r\nX-Echo: credential".into(),
                verified_version: "0.9.0".into(),
                balance_sat: 50_000,
                fee_credit_sat: 0,
            },
            PhoenixdProbe::VersionMismatch {
                running: "0.10.0-Basic OnNlY3JldA==\r\nX-Echo: credential".into(),
                verified: "0.9.0".into(),
            },
        ] {
            let detail = rendered(probe).detail;
            assert!(
                detail.contains(REDACTED_PHOENIXD_VERSION),
                "unsafe remote version must be visibly redacted: {detail}"
            );
            for secret_fragment in ["Basic", "OnNlY3JldA", "X-Echo", "\r", "\n"] {
                assert!(
                    !detail.contains(secret_fragment),
                    "remote version fragment leaked into detail: {detail}"
                );
            }
        }
    }

    /// Render an APPLICABLE probe. `NotApplicable` is the one state with no rendering at all (the
    /// check is omitted), so a test that reaches the renderer is asserting on a real verdict.
    fn rendered(probe: PhoenixdProbe) -> PreflightCheck {
        phoenixd_check_from_probe(probe).expect("an applicable probe renders a check")
    }

    // `verified_version` is the operator's OWN config (`[phoenixd] fee_schedule_version`, or the
    // build's default schedule), NOT remote input, and `matches_running_version` accepts a
    // whole-string pin — so an operator can be verified against a build id that is not the measured
    // `MAJOR.MINOR.PATCH[-<hex>]` shape. Applying the remote grammar to it would redact the
    // operator's own pin, leaving the passing detail naming no release and the MISMATCH remedy unable
    // to say which release the schedule was configured for. It is rendered as configured.
    #[test]
    fn a_configured_version_is_rendered_as_configured_not_as_remote_input() {
        let detail = rendered(PhoenixdProbe::Ready {
            version: "0.9.0-b072567".into(),
            verified_version: "0.9.0-rc1".into(),
            balance_sat: 50_000,
            fee_credit_sat: 0,
        })
        .detail;
        assert!(
            detail.contains("verified for 0.9.0-rc1"),
            "a pre-release pin names itself: {detail}"
        );

        let detail = rendered(PhoenixdProbe::VersionMismatch {
            running: "0.9.1-a1b2c3d".into(),
            verified: "0.9.0-SNAPSHOT".into(),
        })
        .detail;
        assert!(
            detail.contains("0.9.1-a1b2c3d") && detail.contains("0.9.0-SNAPSHOT"),
            "the remedy names BOTH releases — which one runs and which one was verified: {detail}"
        );

        // Report hygiene is still enforced: config text may not inject control characters into a
        // report that gets pasted into an issue, nor swamp it.
        for junk in ["0.9.0\r\nX-Injected: 1", "", &"9".repeat(65)] {
            let detail = rendered(PhoenixdProbe::VersionMismatch {
                running: "0.9.1-b072567".into(),
                verified: junk.into(),
            })
            .detail;
            assert!(
                detail.contains(UNPRINTABLE_CONFIGURED_VERSION),
                "unprintable config text is replaced: {detail}"
            );
            assert!(
                !detail.contains("X-Injected") && !detail.contains('\r'),
                "no control characters survive: {detail}"
            );
        }
    }

    // The three phoenixd failure states are DISTINCT verdicts, each naming its own remedy — an
    // unreachable node, a rejected api password, and a release whose fee schedule nobody verified are
    // three different things for the operator to go fix.
    #[tokio::test]
    async fn phoenixd_failures_are_distinct_and_actionable() {
        // Unreachable: an actionable sentence FIRST, with the transport diagnostic after it.
        let v = preflight_report(
            &seam_payment_phoenixd(PhoenixdProbe::Unreachable(
                "phoenixd getinfo request failed: connection refused".into(),
            )),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        assert_eq!(v["ok"], json!(false));
        let detail = phoenixd(&v)["detail"].as_str().unwrap();
        assert_eq!(phoenixd(&v)["ok"], json!(false));
        assert!(
            detail.contains("no usable answer to an authenticated getinfo")
                && detail.contains("`[phoenixd] url`"),
            "an actionable diagnostic, not a bare transport dump: {detail}"
        );
        assert!(
            detail.contains("connection refused"),
            "the underlying cause is still carried: {detail}"
        );

        // The same variant also carries a node that DID answer with an unusable status (a wrong
        // reverse-proxy sub-path 404s), so the lead sentence must not claim nothing answered — and
        // the remedy must point at the url's PATH, which is what a 404 needs.
        let v = preflight_report(
            &seam_payment_phoenixd(PhoenixdProbe::Unreachable(
                "phoenixd getinfo returned HTTP 404 Not Found".into(),
            )),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        let detail = phoenixd(&v)["detail"].as_str().unwrap();
        assert!(
            !detail.contains("did not answer") && detail.contains("including its path"),
            "a 404 answered — the remedy is the url's path, not 'nothing answered': {detail}"
        );

        // A later getbalance failure must not claim the successful getinfo call was unreachable.
        let v = preflight_report(
            &seam_payment_phoenixd(PhoenixdProbe::BalanceUnavailable(
                "phoenixd getbalance returned HTTP 502 Bad Gateway".into(),
            )),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        assert_eq!(v["ok"], json!(false));
        let detail = phoenixd(&v)["detail"].as_str().unwrap();
        assert!(
            detail.contains("answered authenticated getinfo")
                && detail.contains("getbalance failed")
                && detail.contains("fronting proxy"),
            "the failing endpoint and remedy are accurate: {detail}"
        );
        assert!(
            !detail.contains("did not answer"),
            "a successful getinfo must not be rendered as unreachable: {detail}"
        );

        // Auth rejected: a DIFFERENT remedy (the api password), never conflated with unreachable.
        for status in [401u16, 403] {
            let v = preflight_report(
                &seam_payment_phoenixd(PhoenixdProbe::AuthRejected { status }),
                &[],
                None,
                &PanicProbe,
            )
            .await;
            assert_eq!(v["ok"], json!(false));
            let detail = phoenixd(&v)["detail"].as_str().unwrap();
            assert!(detail.contains("REJECTED the api password"), "{detail}");
            assert!(detail.contains(&status.to_string()), "{detail}");
            assert!(
                !detail.contains("did not answer"),
                "an authenticated rejection is not an unreachable node: {detail}"
            );
        }

        // Version mismatch: the detail states the REMEDY (configure a verified fee schedule), which
        // is the only way an operator clears the money path's fail-closed refusal.
        let v = preflight_report(
            &seam_payment_phoenixd(PhoenixdProbe::VersionMismatch {
                running: "0.10.0-bdeadbee".into(),
                verified: "0.9.0".into(),
            }),
            &[],
            None,
            &PanicProbe,
        )
        .await;
        assert_eq!(v["ok"], json!(false));
        let detail = phoenixd(&v)["detail"].as_str().unwrap();
        assert!(
            detail.contains("0.10.0-bdeadbee") && detail.contains("0.9.0"),
            "{detail}"
        );
        assert!(
            detail.contains("fee_schedule_version")
                && detail.contains("fee_base_msat")
                && detail.contains("fee_ppm"),
            "the remedy is named: {detail}"
        );
    }

    // A probe that never answers must become a labeled failure, exactly like the other readiness
    // probes: phoenixd is reached over the network, so a black-holed node cannot be allowed to hang
    // the operator's command. This bound is SHORTER than the phoenixd client's own 30s HTTP timeout,
    // so a DROP-firewalled node lands HERE rather than in the remedy-bearing `Unreachable` render —
    // which is why this verdict has to name a remedy too, not just the elapsed bound.
    #[tokio::test]
    async fn phoenixd_timeout_fails_with_a_diagnostic() {
        struct PendingPhoenixd;
        #[async_trait]
        impl PaymentBackend for PendingPhoenixd {
            async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
                panic!("preflight must not create invoices")
            }
            async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
                panic!("preflight must not look up invoices")
            }
            async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
                panic!("preflight must not look up settlements")
            }
            async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
                panic!("preflight must not pay")
            }
            async fn payment_status(&self, _: &str) -> Result<PayStatus> {
                panic!("preflight must not check payment status")
            }
            async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
                panic!("preflight must not check payment status by key")
            }
            async fn phoenixd_probe(&self) -> Result<PhoenixdProbe> {
                std::future::pending().await
            }
            async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
                panic!("preflight must not watch settlements")
            }
        }

        let payment: Arc<dyn PaymentBackend> = Arc::new(PendingPhoenixd);
        let check = phoenixd_check_with_timeout(&payment, Duration::from_millis(10))
            .await
            .expect("a backend whose phoenixd probe times out is applicable");

        assert!(!check.ok);
        assert_eq!(check.name, "phoenixd");
        assert!(check.detail.contains("timed out after 10ms"));
        assert!(
            check.detail.contains("`[phoenixd] url`") && check.detail.contains("firewalled"),
            "the verdict a black-holed node actually lands in names its remedy: {}",
            check.detail
        );
    }

    // Gateway `Ok(false)` fails closed with the no-reachable-gateway diagnostic.
    #[tokio::test]
    async fn gateway_not_ready_fails_the_gateway_check() {
        let payment = seam_payment(Seam::NotReady, Seam::Ready);
        let v = preflight_report(&payment, &[], None, &PanicProbe,
        ).await;

        assert_eq!(v["ok"], json!(false));
        assert_eq!(checks(&v)[0]["ok"], json!(false));
        assert!(checks(&v)[0]["detail"]
            .as_str()
            .unwrap()
            .contains("no configured gateway is reachable"));
        assert_eq!(
            checks(&v)[1]["ok"],
            json!(true),
            "federation is independent"
        );
    }

    // Gateway `Err` fails with the backend's own diagnostic (distinct from the Ok(false) arm).
    #[tokio::test]
    async fn gateway_error_carries_the_backend_diagnostic() {
        let payment = seam_payment(Seam::Broken, Seam::Ready);
        let v = preflight_report(&payment, &[], None, &PanicProbe,
        ).await;

        assert_eq!(v["ok"], json!(false));
        let detail = checks(&v)[0]["detail"].as_str().unwrap();
        assert!(detail.contains("gateway probe failed"));
        assert!(detail.contains("gateway socket refused"));
    }

    // Federation `Ok(false)` / `Err` both fail the federation check, each with its own detail.
    #[tokio::test]
    async fn federation_not_ready_and_error_both_fail() {
        let not_ready = seam_payment(Seam::Ready, Seam::NotReady);
        let v = preflight_report(&not_ready, &[], None, &PanicProbe,
        ).await;
        assert_eq!(v["ok"], json!(false));
        assert_eq!(checks(&v)[1]["ok"], json!(false));
        assert!(checks(&v)[1]["detail"]
            .as_str()
            .unwrap()
            .contains("federation guardians unreachable"));

        let broken = seam_payment(Seam::Ready, Seam::Broken);
        let v = preflight_report(&broken, &[], None, &PanicProbe,
        ).await;
        assert_eq!(v["ok"], json!(false));
        let detail = checks(&v)[1]["detail"].as_str().unwrap();
        assert!(detail.contains("federation probe failed"));
        assert!(detail.contains("guardians timed out"));
    }

    // The gateway probe stalls the same way (its selection refresh is a guardian round-trip), and
    // it runs FIRST — unbounded, it would hang the whole command before any check reports.
    #[tokio::test]
    async fn gateway_timeout_fails_with_a_diagnostic() {
        let payment = seam_payment(Seam::Pending, Seam::Ready);
        let check = gateway_check_with_timeout(&payment, Duration::from_millis(10)).await;

        assert!(!check.ok);
        assert_eq!(check.name, "gateway");
        assert!(check.detail.contains("timed out after 10ms"));
    }

    // A supported guardian transport may stay connected but never answer. The lnrent-owned bound
    // must turn that stalled readiness future into a labeled failure rather than hanging the IPC
    // request forever.
    #[tokio::test]
    async fn federation_timeout_fails_with_a_diagnostic() {
        let payment = seam_payment(Seam::Ready, Seam::Pending);
        let check = federation_check_with_timeout(&payment, Duration::from_millis(10)).await;

        assert!(!check.ok);
        assert_eq!(check.name, "federation");
        assert!(check.detail.contains("timed out after 10ms"));
    }

    // A recipe declaring DO_TOKEN makes an ABSENT token a FAILURE (not a skip) and no network probe
    // is attempted. A blank/whitespace token is likewise unusable and fails before reaching the
    // provider.
    #[tokio::test]
    async fn required_token_absent_or_blank_fails_without_probing() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipes = [do_token_recipe()];
        for tok in [None, token(""), token("  \n")] {
            let v = preflight_report(&payment, &recipes, tok, &PanicProbe).await;
            assert_eq!(v["ok"], json!(false));
            assert_eq!(provider_token(&v)["ok"], json!(false));
            assert!(provider_token(&v)["detail"]
                .as_str()
                .unwrap()
                .contains("not set"));
        }
    }

    // A PRESENT-but-malformed token (a stray newline / control char from a careless paste) fails
    // with its own MALFORMED diagnostic BEFORE any network call — distinct from absent, rejected,
    // and unreachable (adversarial y4m.9 review: the header-construction error was previously
    // mislabeled "provider API unreachable"). No token material appears in the detail.
    #[tokio::test]
    async fn malformed_token_fails_distinctly_without_probing() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipes = [do_token_recipe()];
        for tok in [token("dop_v1\nsecret"), token("dop v1 secret"), token("dop_v1_\u{7f}")] {
            let v = preflight_report(&payment, &recipes, tok, &PanicProbe).await;
            assert_eq!(v["ok"], json!(false));
            let detail = provider_token(&v)["detail"].as_str().unwrap();
            assert!(detail.contains("MALFORMED"), "distinct diagnostic: {detail}");
            assert!(
                !detail.contains("dop") && !detail.contains("secret"),
                "no token material in the detail"
            );
        }
    }

    // A 401/403 is the REJECTED-token failure — distinct from absent and from unreachable — and
    // the token value never appears in the report.
    #[tokio::test]
    async fn rejected_token_fails_distinctly_and_never_leaks() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipes = [do_token_recipe()];
        for status in [401u16, 403] {
            let v = preflight_report(
                &payment,
                &recipes,
                token("dop_v1_secret"),
                &StatusProbe(status),
            )
            .await;
            assert_eq!(v["ok"], json!(false));
            let detail = provider_token(&v)["detail"].as_str().unwrap();
            assert!(detail.contains("REJECTED"));
            assert!(detail.contains(&status.to_string()));
            assert!(
                !serde_json::to_string(&v).unwrap().contains("dop_v1_secret"),
                "the token value must never appear in the report"
            );
        }
    }

    // A non-2xx/401/403 status (a provider outage / proxy) is its own failure mode.
    #[tokio::test]
    async fn unexpected_provider_status_fails() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let v = preflight_report(
            &payment,
            &[do_token_recipe()],
            token("dop_v1_x"),
            &StatusProbe(500),
        )
        .await;

        assert_eq!(v["ok"], json!(false));
        let detail = provider_token(&v)["detail"].as_str().unwrap();
        assert!(detail.contains("unexpected provider API response"));
        assert!(detail.contains("500"));
    }

    // A transport failure (DNS/connect/timeout) is the UNREACHABLE failure mode, carrying the
    // transport diagnostic.
    #[tokio::test]
    async fn unreachable_provider_api_fails() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let v = preflight_report(
            &payment,
            &[do_token_recipe()],
            token("dop_v1_x"),
            &FailProbe,
        )
        .await;

        assert_eq!(v["ok"], json!(false));
        let detail = provider_token(&v)["detail"].as_str().unwrap();
        assert!(detail.contains("provider API unreachable"));
        assert!(detail.contains("dns lookup failed"));
    }

    // An accepted token passes; the full report never echoes the token.
    #[tokio::test]
    async fn accepted_token_passes_and_never_leaks() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let v = preflight_report(
            &payment,
            &[do_token_recipe()],
            token("dop_v1_secret"),
            &StatusProbe(200),
        )
        .await;

        assert_eq!(v["ok"], json!(true));
        assert_eq!(provider_token(&v)["ok"], json!(true));
        assert!(provider_token(&v)["detail"]
            .as_str()
            .unwrap()
            .contains("accepted"));
        assert!(!serde_json::to_string(&v).unwrap().contains("dop_v1_secret"));
    }

    // Regression anchor: retired `debian-12-x64` reached a paid buyer as a 422 "You specified an
    // invalid image for Droplet creation"; this hook failure must stop orders before payment.
    #[tokio::test]
    async fn invalid_image_fails_recipe_preflight() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipe = temp_recipe_with_preflight(
            "invalid-image",
            Some("#!/bin/sh\necho '{\"error\":\"DO_IMAGE=debian-12-x64 invalid\"}' >&2\nexit 1\n"),
        );
        let v = preflight_report(&payment, &[recipe], None, &PanicProbe).await;

        assert_eq!(v["ok"], json!(false));
        let check = recipe_preflight(&v);
        assert_eq!(check["ok"], json!(false));
        assert!(check["detail"].as_str().unwrap().contains("debian-12-x64"));
    }

    #[tokio::test]
    async fn valid_config_passes_recipe_preflight() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipe = temp_recipe_with_preflight(
            "valid",
            Some("#!/usr/bin/env bash\necho '{\"ok\":true,\"image\":\"debian-13-x64\"}'\n"),
        );
        let v = preflight_report(&payment, &[recipe], None, &PanicProbe).await;

        assert_eq!(v["ok"], json!(true), "a valid config passes preflight");
        assert_eq!(recipe_preflight(&v)["ok"], json!(true));
    }

    #[tokio::test]
    async fn recipe_without_preflight_hook_skips() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipe = temp_recipe_with_preflight("no-hook", None);
        let v = preflight_report(&payment, &[recipe], None, &PanicProbe).await;

        assert_eq!(v["ok"], json!(true));
        let check = recipe_preflight(&v);
        assert_eq!(check["ok"], json!(true));
        assert!(check["detail"].as_str().unwrap().contains("skipped"));
    }

    // The aggregate is a pure all-of over the per-check verdicts.
    #[test]
    fn aggregate_ok_is_all_of() {
        let pass = PreflightCheck::pass("a", "x");
        let fail = PreflightCheck::fail("b", "y");
        assert!(aggregate_ok(&[]));
        assert!(aggregate_ok(&[pass.clone(), pass.clone()]));
        assert!(!aggregate_ok(&[pass.clone(), fail.clone()]));
        assert!(!aggregate_ok(&[fail, pass]));
    }

    // The REAL reqwest probe against a local stub server (injected base URL — never the real
    // API): the request is `GET /v2/account` with the token in a Bearer Authorization header
    // (never the URL), and the response status maps through verbatim.
    #[tokio::test]
    async fn do_token_probe_sends_a_bearer_get_and_maps_the_status() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut head = String::new();
            let mut buf = [0u8; 4096];
            while !head.contains("\r\n\r\n") {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                head.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            sock.write_all(
                b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .await
            .unwrap();
            head
        });

        let probe = DoTokenProbe {
            base_url: format!("http://{addr}"),
        };
        let status = probe.account_status("tok-secret-123").await.unwrap();
        let head = server.await.unwrap();

        assert_eq!(status, 401);
        // NEVER print the raw head on failure — it carries the (synthetic) Authorization header,
        // and a hygiene test must not model the leak it polices (adversarial y4m.9 review).
        let redacted: String = head
            .lines()
            .map(|l| {
                if l.to_ascii_lowercase().starts_with("authorization:") {
                    "authorization: <redacted>"
                } else {
                    l
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            head.starts_with("GET /v2/account HTTP/1.1\r\n"),
            "unexpected request head: {redacted}"
        );
        assert!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer tok-secret-123"),
            "the token must travel in the Authorization header (head redacted): {redacted}"
        );
    }

    // The REAL reqwest probe REFUSES redirects (adversarial y4m.9 review): a 3xx from the
    // provider API maps through as its own status (-> the "unexpected provider API response"
    // failure arm) and the Location target NEVER receives a request — the Authorization header
    // cannot be replayed to a second origin.
    #[tokio::test]
    async fn do_token_probe_refuses_redirects_and_never_replays_the_token() {
        // The would-be redirect TARGET: any connection here is a failure.
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        let target_hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits = target_hits.clone();
        tokio::spawn(async move {
            while target.accept().await.is_ok() {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let mut head = String::new();
            while !head.contains("\r\n\r\n") {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                head.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            let resp = format!(
                "HTTP/1.1 302 Found\r\nlocation: http://{target_addr}/v2/account\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        });

        let probe = DoTokenProbe {
            base_url: format!("http://{addr}"),
        };
        let status = probe.account_status("tok-secret-123").await.unwrap();

        assert_eq!(status, 302, "the 3xx maps through instead of being followed");
        // Give any (buggy) follow-up request a moment to land before asserting none did.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            target_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the redirect target must never be contacted (no Authorization replay)"
        );
    }
}
