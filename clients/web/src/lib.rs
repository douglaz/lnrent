mod clock;
mod relay;
mod signer;

#[cfg(target_arch = "wasm32")]
use std::time::Duration;

use lnrent_buyer_core::lnrent_wire::PublicKey;
#[cfg(target_arch = "wasm32")]
use lnrent_buyer_core::{
    lnrent_wire::{OperationDecl, ParsedListing},
    BuyerError,
};
use qrcode::{render::svg, QrCode};
use serde::Serialize;
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

pub use clock::BrowserClock;
pub use relay::BrowserRelay;
pub use signer::WebSigner;

#[cfg(target_arch = "wasm32")]
use lnrent_buyer_core::{BuyerClient, RenewReply};
#[cfg(target_arch = "wasm32")]
pub use signer::Nip07Signer;

#[cfg(target_arch = "wasm32")]
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn bolt11_qr_svg(bolt11: &str) -> String {
    render_bolt11_qr_svg(bolt11)
}

fn render_bolt11_qr_svg(bolt11: &str) -> String {
    let Ok(code) = QrCode::new(bolt11.as_bytes()) else {
        return String::new();
    };
    let rendered = code
        .render::<svg::Color<'_>>()
        .min_dimensions(256, 256)
        .dark_color(svg::Color("#111111"))
        .light_color(svg::Color("#ffffff"))
        .build();
    rendered
        .find("<svg")
        .map(|idx| rendered[idx..].to_owned())
        .unwrap_or(rendered)
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub struct WebBuyer {
    config: WebBuyerConfig,
    signer: WebSigner,
    resolved_signer_mode: ResolvedSignerMode,
    clock: BrowserClock,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
impl WebBuyer {
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen(constructor))]
    pub fn new(
        relay_url: String,
        operator_npub: String,
        signer_mode: String,
    ) -> Result<WebBuyer, JsValue> {
        #[cfg(all(target_arch = "wasm32", feature = "console_error_panic_hook"))]
        console_error_panic_hook::set_once();

        // M1a is single-relay (spec §"Scope"); multi-relay is an explicit non-goal, so the public
        // JS surface takes one `relay_url`, not a list. Error codes mirror buyer-core's ErrEnvelope
        // taxonomy (clients/core/src/error.rs) — `bad_request`, not a forked `bad_config` — so the web
        // and CLI hosts don't drift; once the flows are wired they marshal `BuyerError::envelope()`.
        let config = WebBuyerConfig::parse(relay_url, operator_npub, signer_mode)
            .map_err(|err| js_error("bad_request", err, Value::Object(serde_json::Map::new())))?;
        let (signer, resolved_signer_mode) = resolve_signer(config.signer_mode)
            .map_err(|err| js_error("bad_request", err, Value::Object(serde_json::Map::new())))?;

        Ok(WebBuyer {
            config,
            signer,
            resolved_signer_mode,
            clock: BrowserClock,
        })
    }

    pub async fn discover(&self) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let relay = self.relay();
            let buyer = self.client(&relay);
            let listings = buyer.discover_listings().await.map_err(buyer_error_js)?;
            ok_js(json!({
                "listings": listings.iter().map(listing_json).collect::<Vec<_>>(),
            }))
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            Err(self.not_implemented("discover"))
        }
    }

    pub async fn list_ops(&self) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let relay = self.relay();
            let buyer = self.client(&relay);
            let ops = buyer.list_ops().await.map_err(buyer_error_js)?;
            ok_js(json!({ "operations": operations_json(&ops) }))
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            Err(self.not_implemented("list_ops"))
        }
    }

    pub async fn create_order(
        &self,
        listing_id: String,
        params: JsValue,
        refund_dest: String,
    ) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let params = parse_params_js(params)?;
            let relay = self.relay();
            let buyer = self.client(&relay);
            let invoice = buyer
                .create_order(&listing_id, params, Some(refund_dest))
                .await
                .map_err(buyer_error_js)?;
            ok_serialize(&invoice)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (listing_id, params, refund_dest);
            Err(self.not_implemented("create_order"))
        }
    }

    pub async fn wait_provision(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let relay = self.relay();
            let buyer = self.client(&relay);
            let ready = buyer
                .wait_provision(&subscription_id)
                .await
                .map_err(buyer_error_js)?;
            ok_serialize(&ready)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = subscription_id;
            Err(self.not_implemented("wait_provision"))
        }
    }

    pub async fn renew(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let relay = self.relay();
            let buyer = self.client(&relay);
            // lnrent-zs2: a RESUMING sub answers with a "retry in a moment" notice rather than an
            // invoice; serialize whichever the operator sent so the web UI can render it. Tag the
            // retry with `"retry": true` to mirror the CLI (main.rs) so the JS consumer reads one
            // explicit discriminator instead of field-sniffing invoice-vs-notice shapes.
            match buyer.renew(&subscription_id).await.map_err(buyer_error_js)? {
                RenewReply::Invoice(invoice) => ok_serialize(&invoice),
                RenewReply::Retry(notice) => ok_serialize(&json!({
                    "retry": true,
                    "subscription_id": notice.subscription_id,
                    "state": notice.state,
                    "message": notice.message,
                })),
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = subscription_id;
            Err(self.not_implemented("renew"))
        }
    }

    pub async fn invoke_op(
        &self,
        subscription_id: String,
        op: String,
        op_kind: Option<String>,
        params: JsValue,
    ) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let params = parse_params_js(params)?;
            let relay = self.relay();
            let buyer = self.client(&relay);
            let result = buyer
                .invoke_op(&subscription_id, &op, op_kind.as_deref(), params)
                .await
                .map_err(buyer_error_js)?;
            ok_serialize(&result)
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (subscription_id, op, op_kind, params);
            Err(self.not_implemented("invoke_op"))
        }
    }

    pub async fn cancel(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        #[cfg(target_arch = "wasm32")]
        {
            let relay = self.relay();
            let buyer = self.client(&relay);
            buyer
                .cancel(&subscription_id)
                .await
                .map_err(buyer_error_js)?;
            ok_js(json!({
                "subscription_id": subscription_id,
                "sent": true,
                "note": "sub.cancel sent; confirmation arrives later as billing.notice",
            }))
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = subscription_id;
            Err(self.not_implemented("cancel"))
        }
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = resolvedSignerMode))]
    pub fn resolved_signer_mode(&self) -> String {
        self.resolved_signer_mode.as_str().to_string()
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = buyerNpub))]
    pub async fn buyer_npub(&self) -> Result<String, JsValue> {
        self.signer
            .buyer_npub()
            .await
            .map_err(|e| js_error("internal", format!("buyer npub: {e}"), json!({})))
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen(js_name = embeddedNsec))]
    pub fn embedded_nsec(&self) -> Result<Option<String>, JsValue> {
        self.signer
            .embedded_nsec()
            .map_err(|e| js_error("internal", format!("embedded nsec: {e}"), json!({})))
    }
}

impl WebBuyer {
    #[cfg(target_arch = "wasm32")]
    fn relay(&self) -> BrowserRelay {
        BrowserRelay::new(self.config.relay_url.clone())
    }

    #[cfg(target_arch = "wasm32")]
    fn client<'a>(
        &'a self,
        relay: &'a BrowserRelay,
    ) -> BuyerClient<'a, BrowserRelay, WebSigner, BrowserClock> {
        BuyerClient::new(
            relay,
            &self.signer,
            &self.clock,
            self.config.operator,
            DEFAULT_TIMEOUT,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn not_implemented(&self, method: &str) -> JsValue {
        let _ = self.clock;
        js_error(
            "not_implemented",
            format!("WebBuyer.{method} is not implemented in this scaffold step"),
            json!({
                "method": method,
                "relay_url": self.config.relay_url,
                "operator_pubkey": self.config.operator.to_hex(),
                "signer_mode": self.config.signer_mode,
                "resolved_signer_mode": self.resolved_signer_mode,
            }),
        )
    }
}

#[derive(Debug, Clone)]
struct WebBuyerConfig {
    relay_url: String,
    operator: PublicKey,
    signer_mode: SignerMode,
}

impl WebBuyerConfig {
    fn parse(
        relay_url: String,
        operator_npub: String,
        signer_mode: String,
    ) -> Result<Self, String> {
        let relay_url = parse_relay_url(&relay_url)?;
        let operator = parse_operator_npub(&operator_npub)?;
        let signer_mode = SignerMode::parse(&signer_mode)?;

        Ok(Self {
            relay_url,
            operator,
            signer_mode,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum SignerMode {
    Auto,
    Nip07,
    Embedded,
}

impl SignerMode {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "nip07" | "nip-07" => Ok(Self::Nip07),
            "embedded" => Ok(Self::Embedded),
            "" => Err("signer_mode is required".into()),
            other => Err(format!(
                "unsupported signer_mode `{other}`; expected auto, nip07, or embedded"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResolvedSignerMode {
    #[cfg(target_arch = "wasm32")]
    Nip07,
    Embedded,
}

impl ResolvedSignerMode {
    fn as_str(self) -> &'static str {
        match self {
            #[cfg(target_arch = "wasm32")]
            Self::Nip07 => "nip07",
            Self::Embedded => "embedded",
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn resolve_signer(mode: SignerMode) -> Result<(WebSigner, ResolvedSignerMode), String> {
    match mode {
        SignerMode::Nip07 => Ok((
            WebSigner::Nip07(Nip07Signer::new()?),
            ResolvedSignerMode::Nip07,
        )),
        SignerMode::Embedded => Ok((
            WebSigner::embedded_from_session_or_generate()?,
            ResolvedSignerMode::Embedded,
        )),
        SignerMode::Auto => match Nip07Signer::new() {
            Ok(signer) => Ok((WebSigner::Nip07(signer), ResolvedSignerMode::Nip07)),
            Err(_) => Ok((
                WebSigner::embedded_from_session_or_generate()?,
                ResolvedSignerMode::Embedded,
            )),
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn resolve_signer(mode: SignerMode) -> Result<(WebSigner, ResolvedSignerMode), String> {
    match mode {
        SignerMode::Nip07 => Err("NIP-07 is only available in a browser wasm build".into()),
        SignerMode::Auto | SignerMode::Embedded => {
            Ok((WebSigner::embedded_random(), ResolvedSignerMode::Embedded))
        }
    }
}

fn parse_operator_npub(raw: &str) -> Result<PublicKey, String> {
    let trimmed = raw.trim();
    if !trimmed.starts_with("npub1") {
        return Err("operator_npub must be a NIP-19 npub".into());
    }
    PublicKey::parse(trimmed).map_err(|e| format!("invalid operator_npub `{trimmed}`: {e}"))
}

fn parse_relay_url(raw: &str) -> Result<String, String> {
    let relays: Vec<&str> = raw
        .split(|ch: char| ch == ',' || ch.is_ascii_whitespace())
        .map(str::trim)
        .filter(|relay| !relay.is_empty())
        .collect();

    match relays.as_slice() {
        [] => Err("one relay URL is required".into()),
        [relay] => {
            validate_relay_url(relay)?;
            Ok((*relay).to_owned())
        }
        _ => Err("exactly one relay URL is supported in this scaffold step".into()),
    }
}

fn validate_relay_url(relay: &str) -> Result<(), String> {
    let Some(after_scheme) = relay
        .strip_prefix("wss://")
        .or_else(|| relay.strip_prefix("ws://"))
    else {
        return Err(format!(
            "relay URL `{relay}` must start with ws:// or wss://"
        ));
    };
    let host = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    if host.is_empty() {
        return Err(format!("relay URL `{relay}` must include a host"));
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn parse_params_js(params: JsValue) -> Result<Value, JsValue> {
    let params: Value = serde_wasm_bindgen::from_value(params).map_err(|e| {
        buyer_error_js(BuyerError::BadRequest(format!(
            "params must be a JSON object: {e}"
        )))
    })?;
    if params.is_object() {
        Ok(params)
    } else {
        Err(buyer_error_js(BuyerError::BadRequest(
            "params must be a JSON object".into(),
        )))
    }
}

#[cfg(target_arch = "wasm32")]
fn listing_json(parsed: &ParsedListing) -> Value {
    let listing = &parsed.listing;
    json!({
        "listing_id": parsed.listing_id,
        "d": listing.d,
        "operator": listing.operator,
        "recipe_id": listing.recipe_id,
        "recipe_version": listing.recipe_version,
        "title": listing.title,
        "summary": listing.summary,
        "amount_sat": listing.amount_sat,
        "period": listing.period,
        "tier": listing.tier,
        "params": listing.params,
        "operations": listing.operations,
        "version": listing.version,
    })
}

#[cfg(target_arch = "wasm32")]
fn operations_json(ops: &[OperationDecl]) -> Value {
    json!(ops
        .iter()
        .map(|op| json!({
            "name": op.name,
            "label": op.label,
            "kind": op.kind,
            "params": op.params,
        }))
        .collect::<Vec<_>>())
}

#[cfg(target_arch = "wasm32")]
fn ok_serialize<T: serde::Serialize>(data: &T) -> Result<JsValue, JsValue> {
    serde_json::to_value(data)
        .map_err(|e| js_error("internal", format!("rendering result: {e}"), json!({})))
        .and_then(ok_js)
}

#[cfg(target_arch = "wasm32")]
fn ok_js(data: Value) -> Result<JsValue, JsValue> {
    to_json_compatible_js(&json!({ "ok": true, "data": data })).map_err(|e| {
        js_error(
            "internal",
            format!("marshalling JS result envelope: {e}"),
            json!({}),
        )
    })
}

#[cfg(target_arch = "wasm32")]
fn buyer_error_js(err: BuyerError) -> JsValue {
    let env = err.envelope();
    js_error_with_retryable(
        &env.code,
        env.message,
        env.retryable,
        json!({
            "exit_code": err.exit_code(),
        }),
    )
}

#[cfg(target_arch = "wasm32")]
fn js_error(code: &str, message: impl Into<String>, details: Value) -> JsValue {
    js_error_with_retryable(code, message, false, details)
}

#[cfg(target_arch = "wasm32")]
fn js_error_with_retryable(
    code: &str,
    message: impl Into<String>,
    retryable: bool,
    details: Value,
) -> JsValue {
    to_json_compatible_js(&json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message.into(),
            "retryable": retryable,
            "details": details,
        }
    }))
    .unwrap_or_else(|_| JsValue::from_str(code))
}

#[cfg(target_arch = "wasm32")]
fn to_json_compatible_js(value: &Value) -> Result<JsValue, serde_wasm_bindgen::Error> {
    value.serialize(&serde_wasm_bindgen::Serializer::json_compatible())
}

#[cfg(not(target_arch = "wasm32"))]
fn js_error(code: &str, message: impl Into<String>, details: Value) -> JsValue {
    let _ = (code, message.into(), details);
    JsValue::UNDEFINED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_rejects_bad_config_on_native_without_js_runtime() {
        assert!(WebBuyer::new(
            "https://relay.example".into(),
            "not-an-npub".into(),
            "auto".into()
        )
        .is_err());
    }

    #[test]
    fn relay_parser_requires_exactly_one_websocket_url() {
        assert!(parse_relay_url("").is_err());
        assert!(parse_relay_url("https://relay.example").is_err());
        assert!(parse_relay_url("wss://relay.example, ws://localhost:8080").is_err());
        assert_eq!(
            parse_relay_url(" wss://relay.example ").unwrap(),
            "wss://relay.example"
        );
    }

    #[test]
    fn signer_mode_parser_accepts_planned_modes() {
        assert!(matches!(SignerMode::parse("auto"), Ok(SignerMode::Auto)));
        assert!(matches!(SignerMode::parse("nip-07"), Ok(SignerMode::Nip07)));
        assert!(matches!(
            SignerMode::parse("embedded"),
            Ok(SignerMode::Embedded)
        ));
        assert!(SignerMode::parse("hardware").is_err());
    }

    #[test]
    fn bolt11_qr_svg_is_non_empty_svg() {
        let svg = render_bolt11_qr_svg("lnbc1p5qqqexamplebolt11invoice");

        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("</svg>"));
        assert!(svg.contains("path"));
    }
}
