//! Refund-dest RESOLVER (lnrent-ug8, SPEC.md §6.6): turn a stored `refund_dest` into a concrete,
//! payable bolt11 AT REFUND TIME, so refunds reach real buyers.
//!
//! A buyer is OFFLINE when a refund fires (days later) and a bolt11 is single-use + expires, so
//! `refund_dest` is a BOLT12 offer or a Lightning address / LNURL (SPEC §6.6). [`PaymentBackend::pay`]
//! takes a bolt11, so the [`crate::refund::Refunder`] resolves the dest just BEFORE paying. This
//! module is BACKEND-AGNOSTIC (phoenixd needs the identical step) and has no payment-backend
//! dependency.
//!
//! Form detection ([`detect_form`], no I/O — also the order-time format gate):
//! - a bolt11 (`lnbc`/`lntb`/`lnbcrt`...) -> PASS-THROUGH (no HTTP);
//! - a Lightning address `user@domain` -> the LNURL-pay URL `https://domain/.well-known/lnurlp/user`;
//! - an LNURL bech32 `lnurl1...` -> its bech32-decoded https URL;
//! - a BOLT12 offer `lno1...` -> STRUCTURAL failure (unsupported in v1; deferred);
//! - anything else -> STRUCTURAL failure (malformed).
//!
//! LNURL-pay flow ([`Resolver`]): GET the lnurlp URL -> `{callback, minSendable, maxSendable,
//! metadata}`; assert `owed_msat ∈ [min, max]`; GET `callback?amount=<owed_msat>` -> `{pr}`; parse
//! `pr` as a bolt11 and verify its amount == `owed_msat` AND its `description_hash` ==
//! `sha256(the EXACT raw metadata string)` AND it has enough remaining TTL. Security: HTTPS-only on
//! every URL, an SSRF guard rejecting private/loopback/link-local targets, a body-size cap and a
//! bounded per-request timeout.
//!
//! Failures are typed ([`ResolveError`]): a STRUCTURAL failure can never succeed (the
//! [`crate::refund::Refunder`] parks the refund FAILED immediately, NOT via the retry cap); a
//! TRANSIENT failure (DNS/connect/TLS/timeout/5xx) leaves the row PENDING for the next drive.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescriptionRef};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::{Host, Url};

/// Cap on an LNURL response body we will read (bytes). A malicious endpoint can't OOM us.
const MAX_BODY_BYTES: u64 = 64 * 1024;
/// Bounded per-request timeout for an LNURL fetch (and for the pre-connect SSRF DNS lookup, which
/// runs outside reqwest's own timeout).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound on the redirect hops an LNURL fetch will follow, EACH re-validated through the SSRF/HTTPS
/// guard. Enough for host / trailing-slash / CDN canonicalization, far short of a redirect loop.
const MAX_REDIRECTS: u32 = 4;
/// Minimum remaining TTL (secs) a resolved bolt11 must have before we'll pay it — never return an
/// about-to-expire invoice the refund executor can't settle in time.
const MIN_BOLT11_TTL_S: i64 = 600;

/// A resolved, payable refund destination: the bolt11 to pay + its absolute expiry (unix secs). The
/// expiry is persisted as `refund_attempt.resolved_expiry`; the generation gate only re-resolves a
/// CURRENT-gen invoice once it is BOTH Failed AND past this expiry (§6.6).
#[derive(Debug, Clone)]
pub struct Resolved {
    pub bolt11: String,
    pub expiry: i64,
}

/// Why a resolution failed. STRUCTURAL is permanent (BOLT12; malformed dest; amount out of
/// `[min,max]`; description-hash / amount mismatch; HTTPS/SSRF violation) — the refund parks FAILED
/// immediately, never burning the retry cap. TRANSIENT is recoverable (DNS/connect/TLS error, 5xx,
/// timeout, oversize/garbled response) — the refund stays PENDING and is retried next drive.
#[derive(Debug)]
pub enum ResolveError {
    Structural(String),
    Transient(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::Structural(m) | ResolveError::Transient(m) => write!(f, "{m}"),
        }
    }
}

/// The detected shape of a `refund_dest` (the result of [`detect_form`], no network I/O).
#[derive(Debug, Clone)]
pub enum DestForm {
    /// A bolt11 invoice — pay it directly (no resolution, generation stays 0).
    Bolt11,
    /// A Lightning address `user@domain` (LUD-16).
    LnAddress { user: String, domain: String },
    /// An LNURL bech32 (`lnurl1...`) decoded to its target https URL (LUD-01).
    Lnurl(String),
}

/// The seam the [`crate::refund::Refunder`] resolves through, just before `pay()`. Production injects
/// [`Resolver`] for real LNURL-pay HTTP; tests inject fakes or [`PassThroughResolver`].
#[async_trait]
pub trait RefundResolver: Send + Sync {
    /// Resolve `dest` to a payable bolt11 for `owed_msat`, where `now` is the wall clock (unix secs)
    /// used for the bolt11 TTL check. Returns a typed [`ResolveError`] so the caller can park
    /// structural failures immediately and retry transient ones.
    async fn resolve(&self, dest: &str, owed_msat: u64, now: i64)
        -> Result<Resolved, ResolveError>;
}

/// Detect the form of `dest` with NO network I/O. Used at order time (the format gate in
/// `order_intake`) and as the first step of refund-time resolution. Only ever returns
/// [`ResolveError::Structural`] (it is pure): a BOLT12 offer or a malformed dest.
pub fn detect_form(dest: &str) -> Result<DestForm, ResolveError> {
    let d = dest.trim();
    if d.is_empty() {
        return Err(ResolveError::Structural("empty refund destination".into()));
    }
    // A Lightning address is the only form with an '@'; bolt11 / LNURL / BOLT12 never contain one.
    if d.contains('@') {
        return parse_ln_address(d);
    }
    let lower = d.to_ascii_lowercase();
    if lower.starts_with("lnurl") {
        return Ok(DestForm::Lnurl(decode_lnurl(d)?));
    }
    // BOLT12 offer: explicitly unsupported in v1 (deferred — gateway onion-message support).
    if lower.starts_with("lno1") {
        return Err(ResolveError::Structural(
            "BOLT12 offers are unsupported — use a Lightning address or a bolt11".into(),
        ));
    }
    // bolt11: must actually parse as a BOLT11 invoice (lnbc/lntb/lntbs/lnbcrt...).
    if Bolt11Invoice::from_str(d).is_ok() {
        return Ok(DestForm::Bolt11);
    }
    Err(ResolveError::Structural(
        "unsupported or malformed refund destination".into(),
    ))
}

/// Order-time FORMAT validation of a `refund_dest` (lnrent-ug8): [`detect_form`] PLUS, for the two
/// HTTP-fetched forms, a check that the URL the resolver will GET is a syntactically valid HTTPS URL
/// with a host. Bare [`detect_form`] only shape-checks (it bech32-decodes the LNURL and accepts a
/// `:port` in a LN-address domain, since its decode/parse path is shared with refund-time resolution
/// where the test bypass relaxes the scheme), so it would wave through an `lnurl1` decoding to
/// `http://…` / non-URL bytes OR a LN-address whose `domain[:port]` can't form a URL (e.g.
/// `alice@host:abc`) — only for the refund to park FAILED days later (review P2). This gate is
/// order-time ONLY (the live SSRF + redirect checks stay at refund time) and has NO test bypass, so
/// HTTPS is required UNCONDITIONALLY: a real buyer's LNURL endpoint is always an HTTPS URL.
pub fn validate_dest_format(dest: &str) -> Result<(), ResolveError> {
    match detect_form(dest)? {
        DestForm::Bolt11 => Ok(()),
        // Build the would-be lnurlp URL and validate it NOW (same shape check as the LNURL branch),
        // so a bad `domain[:port]` is rejected at order time instead of at refund time (review P2).
        DestForm::LnAddress { user, domain } => {
            validate_https_url(&format!("https://{domain}/.well-known/lnurlp/{user}"))
        }
        DestForm::Lnurl(url) => validate_https_url(&url),
    }
}

/// Order-time shape check on a URL a `refund_dest` will be fetched from: it must PARSE, be HTTPS, and
/// carry a host. HTTPS is required unconditionally (a real buyer's LNURL endpoint is always HTTPS; the
/// refund-time test bypass does not apply at order time). No DNS / SSRF here — that is refund-time.
fn validate_https_url(raw: &str) -> Result<(), ResolveError> {
    let parsed = Url::parse(raw).map_err(|e| {
        ResolveError::Structural(format!(
            "refund destination yields an invalid URL '{raw}': {e}"
        ))
    })?;
    if parsed.scheme() != "https" {
        return Err(ResolveError::Structural(format!(
            "refund destination must use HTTPS, got '{}' ({raw})",
            parsed.scheme()
        )));
    }
    if parsed.host().is_none() {
        return Err(ResolveError::Structural(format!(
            "refund destination URL has no host ({raw})"
        )));
    }
    Ok(())
}

/// Parse + shape-validate a Lightning address `user@domain` (LUD-16). `domain` may carry a `:port`
/// (used by the test mock). No DNS / HTTP here — that is the SSRF-guarded resolution step.
fn parse_ln_address(d: &str) -> Result<DestForm, ResolveError> {
    let mut parts = d.splitn(2, '@');
    let user = parts.next().unwrap_or("");
    let domain = parts.next().unwrap_or("");
    let malformed = || ResolveError::Structural(format!("malformed Lightning address '{d}'"));
    if user.is_empty() || domain.is_empty() || domain.contains('@') {
        return Err(malformed());
    }
    let user_ok = user
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+'));
    let domain_ok = domain
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'));
    if !user_ok || !domain_ok {
        return Err(malformed());
    }
    Ok(DestForm::LnAddress {
        user: user.to_string(),
        domain: domain.to_string(),
    })
}

/// bech32-decode an `lnurl1...` (LUD-01) to its target URL. The decoded data bytes ARE the URL.
fn decode_lnurl(d: &str) -> Result<String, ResolveError> {
    let (hrp, data) = bech32::decode(d)
        .map_err(|e| ResolveError::Structural(format!("invalid LNURL bech32: {e}")))?;
    if !hrp.to_string().eq_ignore_ascii_case("lnurl") {
        return Err(ResolveError::Structural(format!(
            "LNURL has an unexpected hrp '{hrp}'"
        )));
    }
    String::from_utf8(data)
        .map_err(|e| ResolveError::Structural(format!("LNURL is not valid UTF-8: {e}")))
}

/// A no-resolution resolver: returns the raw `dest` verbatim as the bolt11, with no expiry. The
/// DEFAULT for [`crate::refund::Refunder::new`] because mock payment backends accept any
/// `pay(dest)` string. Production daemon wiring injects the real [`Resolver`] via
/// [`crate::refund::Refunder::with_resolver`]; passthrough stays available for focused tests and
/// mock harnesses.
pub struct PassThroughResolver;

#[async_trait]
impl RefundResolver for PassThroughResolver {
    async fn resolve(
        &self,
        dest: &str,
        _owed_msat: u64,
        _now: i64,
    ) -> Result<Resolved, ResolveError> {
        Ok(Resolved {
            bolt11: dest.to_string(),
            expiry: i64::MAX,
        })
    }
}

/// LUD-06 `payRequest` metadata response from the lnurlp URL.
#[derive(Deserialize)]
struct LnurlPayResponse {
    /// LUD-06 mandates `tag: "payRequest"`. Asserted in [`Resolver::resolve`] so a wrong-shape (or
    /// LUD-21 withdraw/etc.) response is rejected early and clearly, not only by the downstream
    /// amount/description-hash checks. Optional in the wire struct so a missing tag yields our own
    /// structured error rather than an opaque deserialize failure.
    #[serde(default)]
    tag: Option<String>,
    callback: String,
    #[serde(rename = "minSendable")]
    min_sendable: u64,
    #[serde(rename = "maxSendable")]
    max_sendable: u64,
    /// The raw metadata STRING (a JSON-encoded array). The callback invoice's `description_hash` MUST
    /// equal `sha256` of THIS exact string content (LUD-06), so it is kept as a `String`, never
    /// reparsed/reserialized.
    metadata: String,
}

/// LUD-06 callback response carrying the payable bolt11.
#[derive(Deserialize)]
struct LnurlCallbackResponse {
    pr: String,
}

/// LUD-06 protocol-level error envelope: an HTTP 200 body `{"status":"ERROR","reason":...}` (e.g. a
/// deleted/typo'd Lightning address — a PERMANENT condition). Detected in [`Resolver::get_json`] and
/// surfaced as a STRUCTURAL failure: otherwise it fails the strict `payRequest`/callback deserialize,
/// is misread as TRANSIENT, and is retried EVERY drive forever — never parking FAILED, never enqueuing
/// the failed `billing.refund` DM, never alerting the operator (codex P2). Both fields are optional so
/// a normal success body (`status` absent) parses here as a non-error and falls through unchanged.
#[derive(Deserialize)]
struct LnurlErrorEnvelope {
    status: Option<String>,
    reason: Option<String>,
}

/// The production refund-dest resolver: real LNURL-pay HTTP with the full security envelope
/// (HTTPS-only, SSRF guard, body-size cap, bounded timeout, no redirects).
pub struct Resolver {
    client: reqwest::Client,
    /// When true, the SSRF private-IP guard and the HTTPS-only requirement are RELAXED (and a
    /// Lightning address resolves over http) so a local 127.0.0.1 mock server is reachable in tests.
    /// ALWAYS false in production.
    allow_private_hosts: bool,
}

impl Resolver {
    /// Production resolver: HTTPS-only, SSRF guard ON. The daemon injects this through
    /// [`crate::refund::Refunder::with_resolver`].
    pub fn new() -> Self {
        Self::build(false)
    }

    /// Test-only constructor that relaxes the SSRF / HTTPS guard so the in-process mock LNURL server
    /// on 127.0.0.1 is reachable (the design's testability seam).
    #[cfg(test)]
    fn with_allow_private(allow_private_hosts: bool) -> Self {
        Self::build(allow_private_hosts)
    }

    fn build(allow_private_hosts: bool) -> Self {
        let client = Self::client_builder()
            .build()
            .expect("reqwest client (rustls/ring) builds unless the TLS backend is broken");
        Self {
            client,
            allow_private_hosts,
        }
    }

    /// The shared reqwest configuration: no automatic redirect-following (a 3xx could point at a
    /// non-https or private target, defeating the SSRF guard — we reject any 3xx explicitly), a
    /// bounded timeout, and a forced rustls/ring backend (even if feature-unification also enabled
    /// native-tls elsewhere). Used both for the long-lived [`Self::client`] and for the per-request
    /// IP-pinned client in [`Self::get_json`] (the DNS-rebind TOCTOU fix).
    fn client_builder() -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .use_rustls_tls()
    }

    /// Build the lnurlp URL for a Lightning address (LUD-16). https in production; http under the
    /// test bypass so a local plain-HTTP mock is reachable.
    fn ln_address_url(&self, user: &str, domain: &str) -> String {
        let scheme = if self.allow_private_hosts {
            "http"
        } else {
            "https"
        };
        format!("{scheme}://{domain}/.well-known/lnurlp/{user}")
    }

    /// Parse + security-validate a URL we are about to fetch: scheme (HTTPS-only, unless bypassed)
    /// and a localhost-name reject. IP-literal / DNS private-target checks happen in
    /// [`Self::validated_addrs`].
    fn validate_url(&self, raw: &str) -> Result<Url, ResolveError> {
        let url = Url::parse(raw)
            .map_err(|e| ResolveError::Structural(format!("invalid LNURL URL '{raw}': {e}")))?;
        let scheme = url.scheme();
        let scheme_ok = scheme == "https" || (self.allow_private_hosts && scheme == "http");
        if !scheme_ok {
            return Err(ResolveError::Structural(format!(
                "LNURL endpoint must be HTTPS, got '{scheme}' ({url})"
            )));
        }
        match url.host() {
            Some(Host::Domain(name)) if !self.allow_private_hosts => {
                let lname = name.to_ascii_lowercase();
                if lname == "localhost" || lname.ends_with(".localhost") {
                    return Err(ResolveError::Structural(
                        "LNURL host is localhost (SSRF guard)".into(),
                    ));
                }
            }
            Some(_) => {}
            None => return Err(ResolveError::Structural("LNURL URL has no host".into())),
        }
        Ok(url)
    }

    /// SSRF guard: reject a target that resolves to a private/loopback/link-local/unspecified IP,
    /// and — for a domain host — RETURN the validated socket addresses so the caller can PIN them
    /// into the connection. Pinning closes the DNS-rebind TOCTOU: without it a host that passes this
    /// check could re-resolve to a private/loopback IP at reqwest's own connect time. Returns `None`
    /// when there is nothing to pin — an IP-literal host (already checked here) or the test bypass.
    /// Skipped entirely under the bypass.
    async fn validated_addrs(
        &self,
        url: &Url,
    ) -> Result<Option<(String, Vec<SocketAddr>)>, ResolveError> {
        if self.allow_private_hosts {
            return Ok(None);
        }
        match url.host() {
            Some(Host::Ipv4(ip)) => {
                check_ip(IpAddr::V4(ip))?;
                Ok(None)
            }
            Some(Host::Ipv6(ip)) => {
                check_ip(IpAddr::V6(ip))?;
                Ok(None)
            }
            Some(Host::Domain(name)) => {
                let port = url.port_or_known_default().unwrap_or(443);
                // Bound the lookup itself: it runs OUTSIDE reqwest's request timeout, and the refund
                // drive resolves rows serially, so an unbounded slow-resolving buyer domain would
                // stall the entire drive (review P3).
                let addrs: Vec<SocketAddr> = match tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    tokio::net::lookup_host((name, port)),
                )
                .await
                {
                    Ok(Ok(it)) => it.collect(),
                    Ok(Err(e)) => {
                        return Err(ResolveError::Transient(format!(
                            "DNS lookup for '{name}' failed: {e}"
                        )))
                    }
                    Err(_) => {
                        return Err(ResolveError::Transient(format!(
                            "DNS lookup for '{name}' timed out after {REQUEST_TIMEOUT:?}"
                        )))
                    }
                };
                if addrs.is_empty() {
                    return Err(ResolveError::Transient(format!(
                        "DNS lookup for '{name}' returned no addresses"
                    )));
                }
                for sa in &addrs {
                    check_ip(sa.ip())?;
                }
                Ok(Some((name.to_string(), addrs)))
            }
            None => Err(ResolveError::Structural("LNURL URL has no host".into())),
        }
    }

    /// GET `raw_url` (after URL + SSRF validation), following up to [`MAX_REDIRECTS`] redirects,
    /// rejecting a permanent non-success, reading the body under the size cap, and deserializing it as
    /// JSON. Redirects are followed MANUALLY (reqwest's own following is disabled) so EVERY hop —
    /// including the redirect target — re-runs the full security envelope below; this keeps a legit
    /// host / trailing-slash / CDN canonicalization redirect from permanently parking a refund, while
    /// a hop to a non-https / private target still can't pass the guard (review P2).
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        raw_url: &str,
    ) -> Result<T, ResolveError> {
        let mut current = raw_url.to_string();
        let mut hops = 0u32;
        let (body, url) = loop {
            let url = self.validate_url(&current)?;
            // PIN the validated IP(s) so reqwest does NOT re-resolve the host at connect time. For a
            // domain host `validated_addrs` returns the addresses it already vetted; a per-request
            // client pinned to exactly those addresses can only connect to a vetted IP, closing the
            // DNS-rebind TOCTOU. An IP-literal host (or the test bypass) has nothing to pin and uses
            // the shared client. TLS SNI / certificate validation still uses the original hostname.
            let resp = match self.validated_addrs(&url).await? {
                Some((host, addrs)) => {
                    let client = Self::client_builder()
                        .resolve_to_addrs(&host, &addrs)
                        .build()
                        .map_err(|e| {
                            ResolveError::Transient(format!("building IP-pinned LNURL client: {e}"))
                        })?;
                    client.get(url.clone()).send().await
                }
                None => self.client.get(url.clone()).send().await,
            }
            .map_err(|e| ResolveError::Transient(format!("LNURL request to {url} failed: {e}")))?;
            let status = resp.status();
            if status.is_redirection() {
                hops += 1;
                if hops > MAX_REDIRECTS {
                    return Err(ResolveError::Structural(format!(
                        "LNURL fetch exceeded {MAX_REDIRECTS} redirects (last {url})"
                    )));
                }
                // Re-validated by the next loop pass before any connection is made.
                current = redirect_target(&url, &resp)?;
                continue;
            }
            if !status.is_success() {
                return Err(classify_http_status(status, &url));
            }
            break (read_body_capped(resp).await?, url);
        };
        // LUD-06 protocol error: an HTTP 200 carrying {"status":"ERROR","reason":...} (e.g. a
        // deleted/typo'd Lightning address — permanent). It would otherwise fail the strict
        // payRequest/callback deserialize below and be misclassified TRANSIENT (retried forever, never
        // parked FAILED, no operator alert). Surface it as STRUCTURAL so the refund parks FAILED and is
        // escalated (codex P2). A normal success body has no `status` field and falls through.
        if let Ok(env) = serde_json::from_slice::<LnurlErrorEnvelope>(&body) {
            if env.status.as_deref() == Some("ERROR") {
                return Err(ResolveError::Structural(format!(
                    "LNURL endpoint {url} returned a protocol error: {}",
                    env.reason.as_deref().unwrap_or("(no reason given)")
                )));
            }
        }
        serde_json::from_slice(&body)
            .map_err(|e| ResolveError::Transient(format!("invalid LNURL JSON from {url}: {e}")))
    }

    /// Parse + validate the callback's bolt11 against `owed_msat`, the raw `metadata`, and the TTL.
    fn validate_invoice(
        &self,
        pr: &str,
        owed_msat: u64,
        metadata: &str,
        now: i64,
    ) -> Result<Resolved, ResolveError> {
        let inv = Bolt11Invoice::from_str(pr).map_err(|e| {
            // A 200 callback carrying a non-bolt11 `pr` is a protocol violation, not a blip — like the
            // ERROR-envelope and permanent-4xx cases, treat it as STRUCTURAL so the refund parks
            // FAILED and escalates rather than retrying a permanently-broken endpoint forever (review
            // P3).
            ResolveError::Structural(format!(
                "LNURL callback returned an unparseable bolt11: {e}"
            ))
        })?;
        // Amount must match the owed refund EXACTLY.
        match inv.amount_milli_satoshis() {
            Some(a) if a == owed_msat => {}
            other => {
                return Err(ResolveError::Structural(format!(
                    "bolt11 amount {other:?} msat != owed {owed_msat} msat"
                )))
            }
        }
        // description_hash must be sha256 of the EXACT raw metadata string (LUD-06), not a
        // reserialized value.
        let want = hex::encode(Sha256::digest(metadata.as_bytes()));
        match inv.description() {
            Bolt11InvoiceDescriptionRef::Hash(h) => {
                let got = h.0.to_string();
                if got != want {
                    return Err(ResolveError::Structural(format!(
                        "bolt11 description_hash {got} != sha256(metadata) {want}"
                    )));
                }
            }
            Bolt11InvoiceDescriptionRef::Direct(_) => {
                return Err(ResolveError::Structural(
                    "bolt11 carries a direct description, not the required description_hash".into(),
                ))
            }
        }
        // TTL: never return an about-to-expire invoice.
        let expiry = bolt11_expiry_unix(&inv);
        if expiry < now + MIN_BOLT11_TTL_S {
            return Err(ResolveError::Transient(format!(
                "bolt11 expires at {expiry}, within {MIN_BOLT11_TTL_S}s of now {now}"
            )));
        }
        Ok(Resolved {
            bolt11: pr.to_string(),
            expiry,
        })
    }
}

impl Default for Resolver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RefundResolver for Resolver {
    async fn resolve(
        &self,
        dest: &str,
        owed_msat: u64,
        now: i64,
    ) -> Result<Resolved, ResolveError> {
        let lnurlp_url = match detect_form(dest)? {
            // A bolt11 dest is normally handled by the Refunder before resolve() is reached; pass it
            // through defensively if resolve() is called directly.
            DestForm::Bolt11 => {
                let inv = Bolt11Invoice::from_str(dest).map_err(|e| {
                    ResolveError::Structural(format!("malformed bolt11 refund destination: {e}"))
                })?;
                return Ok(Resolved {
                    bolt11: dest.to_string(),
                    expiry: bolt11_expiry_unix(&inv),
                });
            }
            DestForm::LnAddress { user, domain } => self.ln_address_url(&user, &domain),
            DestForm::Lnurl(u) => u,
        };

        let meta: LnurlPayResponse = self.get_json(&lnurlp_url).await?;
        if meta.tag.as_deref() != Some("payRequest") {
            return Err(ResolveError::Structural(format!(
                "LNURL response tag is not 'payRequest' (got {:?})",
                meta.tag
            )));
        }
        if owed_msat < meta.min_sendable || owed_msat > meta.max_sendable {
            return Err(ResolveError::Structural(format!(
                "owed {owed_msat} msat is outside the LNURL range [{}, {}]",
                meta.min_sendable, meta.max_sendable
            )));
        }

        let mut callback = Url::parse(&meta.callback).map_err(|e| {
            ResolveError::Structural(format!("invalid LNURL callback '{}': {e}", meta.callback))
        })?;
        callback
            .query_pairs_mut()
            .append_pair("amount", &owed_msat.to_string());
        let cb: LnurlCallbackResponse = self.get_json(callback.as_str()).await?;

        self.validate_invoice(&cb.pr, owed_msat, &meta.metadata, now)
    }
}

/// A bolt11's absolute expiry as unix secs (saturating to `i64::MAX` if it would overflow).
fn bolt11_expiry_unix(inv: &Bolt11Invoice) -> i64 {
    inv.expires_at()
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX)
}

/// Read a response body, bounding memory at [`MAX_BODY_BYTES`] (a `Content-Length` pre-check plus a
/// streaming cap so a lying/chunked server can't blow past it).
async fn read_body_capped(resp: reqwest::Response) -> Result<Vec<u8>, ResolveError> {
    if let Some(len) = resp.content_length() {
        if len > MAX_BODY_BYTES {
            return Err(ResolveError::Transient(format!(
                "LNURL response too large: {len} bytes"
            )));
        }
    }
    let mut resp = resp;
    let mut buf = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| ResolveError::Transient(format!("reading LNURL body: {e}")))?
    {
        if buf.len() + chunk.len() > MAX_BODY_BYTES as usize {
            return Err(ResolveError::Transient(
                "LNURL response exceeded the size cap".into(),
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// The next URL to fetch for a 3xx, resolved (relative `Location`s included) against the CURRENT hop
/// and returned absolute so the next loop pass re-validates it through the SSRF/HTTPS guard. A
/// missing or unparseable `Location` is STRUCTURAL — a broken redirect can never resolve.
fn redirect_target(current: &Url, resp: &reqwest::Response) -> Result<String, ResolveError> {
    let loc = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ResolveError::Structural(format!(
                "LNURL redirect from {current} has no Location header"
            ))
        })?;
    current.join(loc).map(|u| u.to_string()).map_err(|e| {
        ResolveError::Structural(format!("invalid LNURL redirect target '{loc}': {e}"))
    })
}

/// Map a non-2xx (and non-3xx) HTTP status to a typed failure. 5xx and the retryable 4xx (408 Request
/// Timeout, 429 Too Many Requests) are TRANSIENT — the endpoint may recover, so keep retrying. Every
/// other 4xx is a PERMANENT client error (e.g. a 404 for a deleted / mistyped Lightning address):
/// STRUCTURAL, so the refund parks FAILED + alerts the operator instead of being retried forever with
/// no escalation (review P2).
fn classify_http_status(status: reqwest::StatusCode, url: &Url) -> ResolveError {
    let retryable_4xx = matches!(status.as_u16(), 408 | 429);
    let msg = format!("LNURL endpoint {url} returned status {status}");
    if status.is_server_error() || retryable_4xx {
        ResolveError::Transient(msg)
    } else {
        ResolveError::Structural(msg)
    }
}

/// Reject an IP that is private / loopback / link-local / unspecified (the SSRF blocklist, §6.6).
fn check_ip(ip: IpAddr) -> Result<(), ResolveError> {
    let blocked = match ip {
        IpAddr::V4(v4) => ipv4_blocked(v4),
        IpAddr::V6(v6) => ipv6_blocked(v6),
    };
    if blocked {
        return Err(ResolveError::Structural(format!(
            "LNURL target resolves to a private/loopback address: {ip} (SSRF guard)"
        )));
    }
    Ok(())
}

/// Reject every non-globally-routable IPv4 (`Ipv4Addr::is_global` is still unstable, so the ranges
/// are spelled out): 10/8, 172.16/12, 192.168/16 (private); 127/8 (loopback); 169.254/16
/// (link-local); 0/8 (unspecified / "this network"); 255.255.255.255 (broadcast);
/// 192.0.2/24 + 198.51.100/24 + 203.0.113/24 (documentation); 224/4 (multicast). Plus the ranges the
/// stdlib predicates miss but a buyer-controlled host could still point at internal infra (review
/// P2/P3): 100.64/10 (CGNAT shared, RFC 6598 — routable inside cloud/k8s), 198.18/15 (benchmarking,
/// RFC 2544), 192.0.0/24 (IETF protocol assignments, RFC 6890), and 240/4 (reserved / future use).
fn ipv4_blocked(v4: Ipv4Addr) -> bool {
    let o = v4.octets();
    v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_multicast()
        || o[0] == 0 // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 CGNAT shared (RFC 6598)
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol (RFC 6890)
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking (RFC 2544)
        || o[0] >= 240 // 240.0.0.0/4 reserved / future use (RFC 1112)
}

/// ::1 (loopback); :: (unspecified); ff00::/8 (multicast); fc00::/7 (ULA); fe80::/10 (link-local);
/// fec0::/10 (deprecated site-local, RFC 3879 — still non-global, still reachable internally); and any
/// embedded-IPv4 transition form whose carried v4 is itself blocked (so an attacker can't smuggle a
/// private v4 target — e.g. 169.254.169.254 — past the guard inside an IPv6 literal). The site-local
/// and multicast ranges close the gap a buyer-controlled host could otherwise use to reach internal
/// IPv6 services (review P2).
fn ipv6_blocked(v6: Ipv6Addr) -> bool {
    if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
        return true;
    }
    let seg0 = v6.segments()[0];
    // ULA fc00::/7, link-local fe80::/10, and deprecated site-local fec0::/10 — all non-globally
    // routable internal targets.
    if (seg0 & 0xfe00) == 0xfc00 || (seg0 & 0xffc0) == 0xfe80 || (seg0 & 0xffc0) == 0xfec0 {
        return true;
    }
    if let Some(v4) = embedded_ipv4(v6) {
        return ipv4_blocked(v4);
    }
    false
}

/// Extract the IPv4 carried by an IPv6 transition form that could otherwise tunnel a private v4
/// target past [`ipv4_blocked`]: IPv4-mapped `::ffff:0:0/96`, deprecated IPv4-compatible `::/96`,
/// NAT64 `64:ff9b::/96`, 6to4 `2002::/16`, and Teredo `2001:0::/32` (client v4 in the low 32 bits,
/// bit-inverted). Returns `None` for a native IPv6 address.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    let seg = v6.segments();
    let v4 = |hi: u16, lo: u16| Ipv4Addr::new((hi >> 8) as u8, hi as u8, (lo >> 8) as u8, lo as u8);
    // IPv4-compatible ::/96 (deprecated): the first 96 bits are zero (but it isn't :: or ::1).
    if seg[..6].iter().all(|&s| s == 0) && (seg[6] != 0 || seg[7] != 0) {
        return Some(v4(seg[6], seg[7]));
    }
    // NAT64 well-known prefix 64:ff9b::/96.
    if seg[..6] == [0x0064, 0xff9b, 0, 0, 0, 0] {
        return Some(v4(seg[6], seg[7]));
    }
    // 6to4 2002::/16: the v4 is segments 1-2.
    if seg[0] == 0x2002 {
        return Some(v4(seg[1], seg[2]));
    }
    // Teredo 2001:0::/32: the client v4 is the low 32 bits, bit-inverted.
    if seg[0] == 0x2001 && seg[1] == 0x0000 {
        return Some(v4(seg[6] ^ 0xffff, seg[7] ^ 0xffff));
    }
    None
}

/// Mint a valid SIGNED bolt11 for the resolver tests (and the refund-executor's bolt11-passthrough
/// test). `description_hash = sha256(metadata)`, absolute expiry `ts_secs + expiry_s`.
#[cfg(test)]
pub(crate) fn mint_bolt11(amount_msat: u64, metadata: &str, ts_secs: u64, expiry_s: u64) -> String {
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
    use std::time::{Duration, SystemTime};

    let sk = SecretKey::from_slice(&[0x11u8; 32]).unwrap();
    let payment_hash = sha256::Hash::hash(&[0u8; 32]);
    let desc_hash = sha256::Hash::hash(metadata.as_bytes());
    InvoiceBuilder::new(Currency::Regtest)
        .amount_milli_satoshis(amount_msat)
        .description_hash(desc_hash)
        .payment_hash(payment_hash)
        .payment_secret(PaymentSecret([42u8; 32]))
        .timestamp(SystemTime::UNIX_EPOCH + Duration::from_secs(ts_secs))
        .min_final_cltv_expiry_delta(144)
        .expiry_time(Duration::from_secs(expiry_s))
        .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &sk))
        .expect("test bolt11 builds")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const METADATA: &str = r#"[["text/plain","lnrent refund"]]"#;

    fn structural(e: &ResolveError) -> bool {
        matches!(e, ResolveError::Structural(_))
    }

    // ---- form detection -----------------------------------------------------

    #[test]
    fn detects_bolt11_pass_through() {
        let b = mint_bolt11(1000, METADATA, 1000, 3600);
        assert!(matches!(detect_form(&b), Ok(DestForm::Bolt11)));
    }

    #[test]
    fn detects_lightning_address() {
        match detect_form("alice@example.com") {
            Ok(DestForm::LnAddress { user, domain }) => {
                assert_eq!(user, "alice");
                assert_eq!(domain, "example.com");
            }
            other => panic!(
                "expected LnAddress, got a different form: {:?}",
                other.is_ok()
            ),
        }
    }

    #[test]
    fn detects_lnurl_bech32() {
        let encoded = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("lnurl").unwrap(),
            "https://example.com/lnurl/pay".as_bytes(),
        )
        .unwrap();
        match detect_form(&encoded) {
            Ok(DestForm::Lnurl(url)) => assert_eq!(url, "https://example.com/lnurl/pay"),
            _ => panic!("expected an Lnurl form"),
        }
    }

    #[test]
    fn bolt12_offer_is_structural() {
        let e = detect_form(
            "lno1qcp4256ypqpq86q2pucnq42ngssx2an9wfujqerp0y2pqun4wd68jtn00fkxzcnn9ehhyec6",
        )
        .unwrap_err();
        assert!(structural(&e), "BOLT12 must be a structural failure");
        assert!(e.to_string().contains("BOLT12"));
    }

    #[test]
    fn garbage_and_empty_are_structural() {
        assert!(structural(&detect_form("not-a-destination").unwrap_err()));
        assert!(structural(&detect_form("").unwrap_err()));
        assert!(structural(&detect_form("  ").unwrap_err()));
        assert!(structural(&detect_form("bad@@addr").unwrap_err()));
    }

    /// bech32-encode `payload` under the `lnurl` hrp (the order-time LNURL-validation tests).
    fn lnurl_of(payload: &str) -> String {
        bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("lnurl").unwrap(), payload.as_bytes())
            .unwrap()
    }

    // The ORDER-TIME gate ([`validate_dest_format`]) rejects up front an `lnurl1` that bare
    // `detect_form` would accept but that can never resolve: one decoding to `http://…` (not HTTPS)
    // or to non-URL bytes (review P2). A bolt11 / LN-address / valid-HTTPS-lnurl1 still passes; a
    // BOLT12 / malformed dest is still structural.
    #[test]
    fn order_time_validation_rejects_unresolvable_lnurl() {
        // bare detect_form WAVES these through (it only bech32-decodes), the order-time gate does not.
        let http_lnurl = lnurl_of("http://example.com/lnurlp/u");
        assert!(matches!(detect_form(&http_lnurl), Ok(DestForm::Lnurl(_))));
        assert!(
            structural(&validate_dest_format(&http_lnurl).unwrap_err()),
            "an lnurl1 decoding to http:// is rejected at order time"
        );

        let junk_lnurl = lnurl_of("not a url at all");
        assert!(matches!(detect_form(&junk_lnurl), Ok(DestForm::Lnurl(_))));
        assert!(
            structural(&validate_dest_format(&junk_lnurl).unwrap_err()),
            "an lnurl1 decoding to non-URL bytes is rejected at order time"
        );

        // Supported forms still pass the order-time gate.
        let https_lnurl = lnurl_of("https://example.com/lnurlp/u");
        assert!(validate_dest_format(&https_lnurl).is_ok());
        assert!(validate_dest_format("alice@example.com").is_ok());
        assert!(validate_dest_format(&mint_bolt11(1000, METADATA, 1000, 3600)).is_ok());

        // BOLT12 / malformed are still structural at the gate.
        assert!(structural(
            &validate_dest_format("lno1pqps7sjqpgz").unwrap_err()
        ));
        assert!(structural(
            &validate_dest_format("not-a-destination").unwrap_err()
        ));
    }

    // The order-time gate also rejects a Lightning address whose `domain[:port]` can't form a URL —
    // bare `parse_ln_address` permits a `:` in the domain (for the test mock's :port), so
    // `alice@host:abc` passes detection but its lnurlp URL fails to parse; rejecting it NOW avoids
    // parking the refund FAILED at refund time (review P2).
    #[test]
    fn order_time_validation_rejects_bad_ln_address_port() {
        assert!(matches!(
            detect_form("alice@host:abc"),
            Ok(DestForm::LnAddress { .. })
        ));
        assert!(
            structural(&validate_dest_format("alice@host:abc").unwrap_err()),
            "a LN-address with a non-numeric port is rejected at order time"
        );
        // A valid explicit numeric port still passes the order-time gate.
        assert!(validate_dest_format("alice@host.example:8443").is_ok());
    }

    // ---- mock LNURL server --------------------------------------------------

    /// A tiny HTTP/1.1 server for one route closure. `make` receives the bound addr so the lnurlp
    /// response can advertise a callback on the SAME server. Serves one response per connection and
    /// closes (the resolver's client sets no keep-alive expectations).
    async fn spawn_mock<H, MK>(make: MK) -> SocketAddr
    where
        H: Fn(&str) -> (u16, String) + Send + Sync + 'static,
        MK: FnOnce(SocketAddr) -> H,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler = Arc::new(make(addr));
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let target = req
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    let (status, body) = handler(&target);
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        addr
    }

    /// Like [`spawn_mock`] but the handler returns the FULL raw HTTP/1.1 response, so a test can set
    /// arbitrary headers (e.g. a redirect's `Location`). Same one-response-per-connection shape.
    async fn spawn_raw_mock<H, MK>(make: MK) -> SocketAddr
    where
        H: Fn(&str) -> String + Send + Sync + 'static,
        MK: FnOnce(SocketAddr) -> H,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler = Arc::new(make(addr));
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let target = req
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("/")
                        .to_string();
                    let _ = sock.write_all(handler(&target).as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        addr
    }

    /// A 200 JSON response in the raw-mock format.
    fn raw_json(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// A mock that serves a valid lnurlp + callback. `bolt11` is the invoice the callback returns;
    /// `(min, max)` the advertised sendable range; `metadata` the payRequest metadata.
    fn ok_handler(
        addr: SocketAddr,
        bolt11: String,
        min: u64,
        max: u64,
        metadata: String,
    ) -> impl Fn(&str) -> (u16, String) + Send + Sync + 'static {
        let cb = format!("http://{addr}/cb");
        move |target: &str| {
            if target.starts_with("/.well-known/lnurlp/") {
                let body = serde_json::json!({
                    "tag": "payRequest",
                    "callback": cb,
                    "minSendable": min,
                    "maxSendable": max,
                    "metadata": metadata,
                })
                .to_string();
                (200, body)
            } else if target.starts_with("/cb") {
                (200, serde_json::json!({ "pr": bolt11 }).to_string())
            } else {
                (404, "{}".to_string())
            }
        }
    }

    // ---- resolution ---------------------------------------------------------

    #[tokio::test]
    async fn lnurl_pay_happy_path_returns_bolt11() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 86_400);
            ok_handler(addr, bolt11, 1, 1_000_000, METADATA.to_string())
        })
        .await;

        let resolver = Resolver::with_allow_private(true);
        let dest = format!("refunduser@{addr}");
        let resolved = resolver.resolve(&dest, owed, 1_000).await.unwrap();

        assert_eq!(resolved.expiry, 1_000 + 86_400);
        let inv = Bolt11Invoice::from_str(&resolved.bolt11).unwrap();
        assert_eq!(inv.amount_milli_satoshis(), Some(owed));
    }

    #[tokio::test]
    async fn lnurl_via_bech32_resolves() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 86_400);
            ok_handler(addr, bolt11, 1, 1_000_000, METADATA.to_string())
        })
        .await;
        let lnurl = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("lnurl").unwrap(),
            format!("http://{addr}/.well-known/lnurlp/u").as_bytes(),
        )
        .unwrap();

        let resolver = Resolver::with_allow_private(true);
        let resolved = resolver.resolve(&lnurl, owed, 1_000).await.unwrap();
        assert_eq!(resolved.expiry, 1_000 + 86_400);
    }

    #[tokio::test]
    async fn amount_out_of_range_is_structural() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 86_400);
            // owed is ABOVE maxSendable.
            ok_handler(addr, bolt11, 1, 1_000, METADATA.to_string())
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "amount out of range must be structural: {e}"
        );
    }

    #[tokio::test]
    async fn description_hash_mismatch_is_structural() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            // The invoice's description_hash is over METADATA, but the server advertises DIFFERENT
            // metadata, so sha256(metadata) won't match the bolt11's hash.
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 86_400);
            ok_handler(
                addr,
                bolt11,
                1,
                1_000_000,
                r#"[["text/plain","tampered"]]"#.to_string(),
            )
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap_err();
        assert!(structural(&e), "desc-hash mismatch must be structural: {e}");
    }

    #[tokio::test]
    async fn invoice_amount_mismatch_is_structural() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            // The bolt11 is for a DIFFERENT amount than owed.
            let bolt11 = mint_bolt11(999_000, METADATA, 1_000, 86_400);
            ok_handler(addr, bolt11, 1, 1_000_000, METADATA.to_string())
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "invoice amount mismatch must be structural: {e}"
        );
    }

    // A 200 callback whose `pr` is not a parseable bolt11 is a protocol violation (permanent), so it
    // is STRUCTURAL — parks FAILED and escalates rather than retrying a broken endpoint forever
    // (review P3).
    #[tokio::test]
    async fn unparseable_callback_pr_is_structural() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            // ok_handler returns `{"pr": <this>}` from the callback — a non-bolt11 string here.
            ok_handler(
                addr,
                "not-a-bolt11".to_string(),
                1,
                1_000_000,
                METADATA.to_string(),
            )
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "an unparseable callback pr must be structural: {e}"
        );
    }

    #[tokio::test]
    async fn ssrf_private_host_is_rejected_without_http() {
        // allow_private OFF: a Lightning address pointing at loopback is rejected at the guard.
        let resolver = Resolver::with_allow_private(false);
        let e = resolver
            .resolve("u@127.0.0.1", 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(structural(&e), "loopback target must be structural: {e}");
    }

    #[tokio::test]
    async fn non_https_endpoint_is_rejected() {
        // An lnurl1 decoding to an http:// URL is rejected (HTTPS-only) when the guard is on.
        let lnurl = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("lnurl").unwrap(),
            "http://example.com/lnurl/pay".as_bytes(),
        )
        .unwrap();
        let resolver = Resolver::with_allow_private(false);
        let e = resolver.resolve(&lnurl, 500_000, 1_000).await.unwrap_err();
        assert!(structural(&e), "non-https must be structural: {e}");
        assert!(e.to_string().contains("HTTPS"));
    }

    #[tokio::test]
    async fn http_5xx_is_transient() {
        let addr = spawn_mock(|_addr| |_target: &str| (503, "{}".to_string())).await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            matches!(e, ResolveError::Transient(_)),
            "a 5xx must be transient (retryable): {e}"
        );
    }

    // A permanent 4xx (here a 404 — a deleted / mistyped Lightning address) is STRUCTURAL, so the
    // refund parks FAILED + alerts rather than being retried forever with no escalation (review P2).
    #[tokio::test]
    async fn http_4xx_is_structural() {
        let addr = spawn_mock(|_addr| |_target: &str| (404, "{}".to_string())).await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("ghost@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "a permanent 404 must be structural (parks FAILED, not retried forever): {e}"
        );
    }

    // A 429 (rate-limited) stays TRANSIENT — the endpoint may recover, so the refund keeps retrying
    // rather than parking FAILED on a temporary throttle.
    #[tokio::test]
    async fn http_429_is_transient() {
        let addr = spawn_mock(|_addr| |_target: &str| (429, "{}".to_string())).await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            matches!(e, ResolveError::Transient(_)),
            "a 429 must stay transient (retryable): {e}"
        );
    }

    // A 3xx to a valid (re-validated) target is FOLLOWED, not rejected, so a legit
    // host/trailing-slash/CDN canonicalization redirect doesn't permanently park the refund (review
    // P2). Here the lnurlp endpoint 302s to a second path that serves the real payRequest.
    #[tokio::test]
    async fn redirect_to_valid_target_is_followed() {
        let owed = 500_000u64;
        let addr = spawn_raw_mock(move |addr| {
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 86_400);
            move |target: &str| {
                if target == "/.well-known/lnurlp/u" {
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{addr}/lnurlp2\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                } else if target == "/lnurlp2" {
                    raw_json(
                        &serde_json::json!({
                            "tag": "payRequest",
                            "callback": format!("http://{addr}/cb"),
                            "minSendable": 1,
                            "maxSendable": 1_000_000,
                            "metadata": METADATA,
                        })
                        .to_string(),
                    )
                } else if target.starts_with("/cb") {
                    raw_json(&serde_json::json!({ "pr": bolt11 }).to_string())
                } else {
                    raw_json("{}")
                }
            }
        })
        .await;

        let resolver = Resolver::with_allow_private(true);
        let resolved = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap();
        assert_eq!(resolved.expiry, 1_000 + 86_400);
        let inv = Bolt11Invoice::from_str(&resolved.bolt11).unwrap();
        assert_eq!(inv.amount_milli_satoshis(), Some(owed));
    }

    // A redirect LOOP is bounded and ends STRUCTURAL — it can never resolve, so park rather than spin.
    #[tokio::test]
    async fn redirect_loop_is_structural() {
        let addr = spawn_raw_mock(move |addr| {
            move |_target: &str| {
                format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://{addr}/loop\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            }
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "an unbounded redirect loop must be structural: {e}"
        );
    }

    #[tokio::test]
    async fn about_to_expire_bolt11_is_transient() {
        let owed = 500_000u64;
        let addr = spawn_mock(move |addr| {
            // The invoice expires at ts(1000) + 100 = 1100, well within MIN_BOLT11_TTL_S of now.
            let bolt11 = mint_bolt11(owed, METADATA, 1_000, 100);
            ok_handler(addr, bolt11, 1, 1_000_000, METADATA.to_string())
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), owed, 1_000)
            .await
            .unwrap_err();
        assert!(
            matches!(e, ResolveError::Transient(_)),
            "an about-to-expire bolt11 is transient (a fresh resolve next drive may help): {e}"
        );
    }

    #[tokio::test]
    async fn bolt11_pass_through_resolves_to_itself() {
        let b = mint_bolt11(500_000, METADATA, 1_000, 86_400);
        let resolver = Resolver::with_allow_private(true);
        let resolved = resolver.resolve(&b, 500_000, 1_000).await.unwrap();
        assert_eq!(resolved.bolt11, b, "a bolt11 dest passes through unchanged");
        assert_eq!(resolved.expiry, 1_000 + 86_400);
    }

    // A lnurlp response with a tag other than "payRequest" (e.g. a withdraw endpoint) is rejected as
    // structural, before the amount/callback steps.
    #[tokio::test]
    async fn wrong_tag_is_structural() {
        let addr = spawn_mock(move |_addr| {
            |target: &str| {
                if target.starts_with("/.well-known/lnurlp/") {
                    let body = serde_json::json!({
                        "tag": "withdrawRequest",
                        "callback": "https://example.com/cb",
                        "minSendable": 1,
                        "maxSendable": 1_000_000,
                        "metadata": METADATA,
                    })
                    .to_string();
                    (200, body)
                } else {
                    (404, "{}".to_string())
                }
            }
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("u@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "a non-payRequest tag must be structural: {e}"
        );
        assert!(e.to_string().contains("payRequest"));
    }

    // An LUD-06 endpoint returning HTTP 200 `{"status":"ERROR","reason":...}` (a deleted/typo'd
    // Lightning address — a PERMANENT condition) is a STRUCTURAL failure, so the refund parks FAILED
    // and is escalated — not misread as transient and retried forever with no alert (codex P2).
    #[tokio::test]
    async fn lnurl_error_response_is_structural() {
        let addr = spawn_mock(move |_addr| {
            |target: &str| {
                if target.starts_with("/.well-known/lnurlp/") {
                    let body = serde_json::json!({
                        "status": "ERROR",
                        "reason": "unknown user",
                    })
                    .to_string();
                    (200, body)
                } else {
                    (404, "{}".to_string())
                }
            }
        })
        .await;
        let resolver = Resolver::with_allow_private(true);
        let e = resolver
            .resolve(&format!("ghost@{addr}"), 500_000, 1_000)
            .await
            .unwrap_err();
        assert!(
            structural(&e),
            "an LNURL ERROR response must be structural (parks FAILED, not retried forever): {e}"
        );
        assert!(e.to_string().contains("unknown user"));
    }

    // ---- SSRF: IPv4 blocklist -----------------------------------------------

    // The shared/reserved IPv4 ranges the stdlib predicates miss are blocked (review P2/P3), while a
    // genuinely globally-routable address (including the public IPs just OUTSIDE each blocked range)
    // is allowed.
    #[test]
    fn ssrf_blocks_shared_and_reserved_ipv4_ranges() {
        let blocked = [
            "0.0.0.0",         // 0/8 this-network
            "100.64.0.1",      // 100.64/10 CGNAT (low edge)
            "100.127.255.254", // 100.64/10 CGNAT (high edge)
            "192.0.0.1",       // 192.0.0/24 IETF protocol
            "198.18.0.1",      // 198.18/15 benchmarking (low edge)
            "198.19.255.254",  // 198.18/15 benchmarking (high edge)
            "224.0.0.1",       // multicast
            "240.0.0.1",       // 240/4 reserved/future
            "255.255.255.255", // broadcast
        ];
        for s in blocked {
            let ip: Ipv4Addr = s.parse().unwrap();
            assert!(ipv4_blocked(ip), "{s} must be blocked");
        }

        let allowed = [
            "8.8.8.8",
            "1.1.1.1",
            "100.63.255.255", // just below CGNAT
            "100.128.0.0",    // just above CGNAT
            "198.17.255.255", // just below benchmarking
            "198.20.0.0",     // just above benchmarking
            "192.0.1.1",      // just outside 192.0.0/24
        ];
        for s in allowed {
            let ip: Ipv4Addr = s.parse().unwrap();
            assert!(!ipv4_blocked(ip), "{s} must be allowed (globally routable)");
        }
    }

    // ---- SSRF: IPv6 embedded-IPv4 transition forms --------------------------

    // A private/loopback v4 smuggled inside an IPv4-mapped / IPv4-compatible / NAT64 / 6to4 / Teredo
    // IPv6 literal is still blocked; a genuinely public v4 inside the same forms is allowed.
    #[test]
    fn ssrf_blocks_embedded_ipv4_v6_forms() {
        let blocked = [
            "::ffff:127.0.0.1",       // IPv4-mapped loopback
            "::ffff:169.254.169.254", // IPv4-mapped metadata endpoint
            "::7f00:1",               // IPv4-compatible (deprecated) loopback
            "64:ff9b::7f00:1",        // NAT64 loopback
            "64:ff9b::a9fe:a9fe",     // NAT64 link-local metadata endpoint
            "2002:7f00:1::",          // 6to4 loopback
            "2002:c0a8:1::",          // 6to4 192.168.0.1 (private)
        ];
        for s in blocked {
            let ip: Ipv6Addr = s.parse().unwrap();
            assert!(ipv6_blocked(ip), "{s} must be blocked");
        }

        // Teredo carries the client v4 bit-inverted in the low 32 bits: !127.0.0.1 = 0x80fffffe.
        let teredo_loopback: Ipv6Addr = "2001::ffff:80ff:fffe".parse().unwrap();
        assert!(
            ipv6_blocked(teredo_loopback),
            "Teredo-embedded loopback must be blocked"
        );

        let allowed = [
            "2606:4700:4700::1111", // a native public IPv6 (Cloudflare)
            "2002:0808:0808::",     // 6to4 carrying public 8.8.8.8
            "64:ff9b::0808:0808",   // NAT64 carrying public 8.8.8.8
        ];
        for s in allowed {
            let ip: Ipv6Addr = s.parse().unwrap();
            assert!(!ipv6_blocked(ip), "{s} must be allowed");
        }
    }

    // ---- SSRF: IPv6 site-local + multicast ----------------------------------

    // Deprecated site-local (fec0::/10) and any multicast (ff00::/8) IPv6 are non-global internal
    // targets and must be blocked, while a genuinely public IPv6 stays allowed (review P2).
    #[test]
    fn ssrf_blocks_site_local_and_multicast_ipv6() {
        let blocked = [
            "fec0::1",       // site-local (low edge)
            "fec0:0:0:1::1", // site-local
            "feff:ffff::1",  // site-local (high edge)
            "ff02::1",       // link-local all-nodes multicast
            "ff05::1:3",     // site-local multicast
            "ff0e::1",       // global-scope multicast
        ];
        for s in blocked {
            let ip: Ipv6Addr = s.parse().unwrap();
            assert!(ipv6_blocked(ip), "{s} must be blocked");
        }
        let ok: Ipv6Addr = "2606:4700:4700::1111".parse().unwrap();
        assert!(!ipv6_blocked(ok), "a public IPv6 must stay allowed");
    }
}
