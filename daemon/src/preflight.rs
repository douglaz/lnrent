//! `lnrent preflight` / `doctor` (lnrent-y4m.9, PR-14): probe the three EXTERNAL dependencies
//! bootstrap validation never touches — the refund gateway, the federation guardians, and the
//! provider API token — and report per-check `{name, ok, detail}` plus an aggregate `ok`.
//! Durable-state validation is strong at bootstrap, but a well-formed-but-WRONG gateway or
//! federation invite passes it and only fails at runtime; `DO_TOKEN` validity used to be a
//! hand-run curl in go-live.md §4 a stranger-operator can skip. This is the machine-readable
//! replacement an operator agent can use to gate subsequent launch promotion (the CLI exits
//! nonzero on any failed check). The daemon publishes its listing before starting IPC, so this is a
//! post-start health gate, not a publication interlock.
//!
//! REUSED seams only — no new probes: gateway = [`PaymentBackend::refund_gateway_ready`] (the
//! y4m.8 failover-aware money-path gateway-selection probe; fails closed, the Err carries the
//! diagnostic), federation = [`PaymentBackend::backend_ready`] (the
//! urw.4 `session_count()` guardian round-trip, distinct from gateway/balance — it proves the
//! JOINED federation answers now; a bad invite fails `join_or_open` at daemon startup, so
//! preflight surfaces it as a daemon-unreachable nonzero exit, not as this labeled check),
//! provider token = the authenticated
//! `GET /v2/account` go-live.md §4 did by hand, with the token resolved from the daemon env
//! exactly as `runner::hook_env` forwards it to the do-vps hooks. Read-only end to end: the only
//! network I/O is the three probes themselves; no store write, no money call.

use crate::backends::{Lnv2Probe, PaymentBackend};
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

/// The full preflight report: run the three checks in a stable order (gateway, federation,
/// provider_token) and fold the aggregate. Pure over its inputs — the payment seams, the loaded
/// recipes, the pre-read token, the injected probe — so every branch is unit-testable with no
/// network.
///
/// SERIALIZED process-wide (adversarial y4m.9 review): each report holds up to ~40s of guardian
/// round-trips + a provider API call, and the IPC loop spawns one task per connection, so
/// unserialized concurrent preflights would amplify load onto the shared fedimint client and the
/// provider API (contending with money-path operations). Queued callers still each get a fresh
/// report; every probe inside is individually time-bounded, so the queue drains.
pub async fn preflight_report(
    payment: &Arc<dyn PaymentBackend>,
    recipes: &[Recipe],
    token: Option<Zeroizing<String>>,
    probe: &dyn ProviderTokenProbe,
) -> Value {
    static SERIALIZE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _one_at_a_time = SERIALIZE.lock().await;
    let checks = [
        gateway_check(payment).await,
        federation_check(payment).await,
        lnv2_check(payment).await,
        provider_token_check(recipes, token, probe).await,
    ];
    json!({
        "ok": aggregate_ok(&checks),
        "checks": checks,
    })
}

/// The aggregate verdict the CLI's nonzero-exit gate rests on: every check `ok`. Pure. A skipped
/// check counts ok only because a skip is emitted solely when the dependency is genuinely not
/// configured (see [`provider_token_check`]).
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
    }

    /// The common two-seam backend: the lnv2 probe reports `NotApplicable` (a skipped, passing check),
    /// as a non-lnv2 backend does — so the existing gateway/federation tests are unaffected by the added
    /// check.
    fn seam_payment(gateway: Seam, federation: Seam) -> Arc<dyn PaymentBackend> {
        Arc::new(SeamPayment {
            gateway,
            federation,
            lnv2: Lnv2Probe::NotApplicable,
        })
    }

    /// A backend whose lnv2 functional probe reports `lnv2` (for the doctor negative matrix), with
    /// guardians + gateway seams held Ready so the lnv2 check is the one under test.
    fn seam_payment_lnv2(lnv2: Lnv2Probe) -> Arc<dyn PaymentBackend> {
        Arc::new(SeamPayment {
            gateway: Seam::Ready,
            federation: Seam::Ready,
            lnv2,
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
        assert_eq!(names, vec!["gateway", "federation", "lnv2", "provider_token"]);
        assert!(checks(&v).iter().all(|c| c["ok"] == json!(true)));
        assert!(
            checks(&v)[2]["detail"].as_str().unwrap().contains("skipped"),
            "a non-lnv2 backend reports the lnv2 check skipped"
        );
        assert!(
            checks(&v)[3]["detail"]
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

    // The do-vps recipe declares DO_TOKEN, so an ABSENT token is a FAILURE (not a skip) and no
    // network probe is attempted. A blank/whitespace token is likewise unusable and fails before
    // reaching the provider.
    #[tokio::test]
    async fn required_token_absent_or_blank_fails_without_probing() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let recipes = [load_recipe("do-vps")];
        for tok in [None, token(""), token("  \n")] {
            let v = preflight_report(&payment, &recipes, tok, &PanicProbe).await;
            assert_eq!(v["ok"], json!(false));
            assert_eq!(checks(&v)[3]["ok"], json!(false));
            assert!(checks(&v)[3]["detail"]
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
        let recipes = [load_recipe("do-vps")];
        for tok in [token("dop_v1\nsecret"), token("dop v1 secret"), token("dop_v1_\u{7f}")] {
            let v = preflight_report(&payment, &recipes, tok, &PanicProbe).await;
            assert_eq!(v["ok"], json!(false));
            let detail = checks(&v)[3]["detail"].as_str().unwrap();
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
        let recipes = [load_recipe("do-vps")];
        for status in [401u16, 403] {
            let v = preflight_report(
                &payment,
                &recipes,
                token("dop_v1_secret"),
                &StatusProbe(status),
            )
            .await;
            assert_eq!(v["ok"], json!(false));
            let detail = checks(&v)[3]["detail"].as_str().unwrap();
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
            &[load_recipe("do-vps")],
            token("dop_v1_x"),
            &StatusProbe(500),
        )
        .await;

        assert_eq!(v["ok"], json!(false));
        let detail = checks(&v)[3]["detail"].as_str().unwrap();
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
            &[load_recipe("do-vps")],
            token("dop_v1_x"),
            &FailProbe,
        )
        .await;

        assert_eq!(v["ok"], json!(false));
        let detail = checks(&v)[3]["detail"].as_str().unwrap();
        assert!(detail.contains("provider API unreachable"));
        assert!(detail.contains("dns lookup failed"));
    }

    // An accepted token passes; the full report never echoes the token.
    #[tokio::test]
    async fn accepted_token_passes_and_never_leaks() {
        let payment = seam_payment(Seam::Ready, Seam::Ready);
        let v = preflight_report(
            &payment,
            &[load_recipe("do-vps")],
            token("dop_v1_secret"),
            &StatusProbe(200),
        )
        .await;

        assert_eq!(v["ok"], json!(true));
        assert_eq!(checks(&v)[3]["ok"], json!(true));
        assert!(checks(&v)[3]["detail"]
            .as_str()
            .unwrap()
            .contains("accepted"));
        assert!(!serde_json::to_string(&v).unwrap().contains("dop_v1_secret"));
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
