mod clock;
mod relay;

use lnrent_buyer_core::lnrent_wire::PublicKey;
use serde::Serialize;
use serde_json::{json, Value};
use wasm_bindgen::prelude::*;

pub use clock::BrowserClock;
pub use relay::BrowserRelay;

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub struct WebBuyer {
    config: WebBuyerConfig,
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

        Ok(WebBuyer {
            config,
            clock: BrowserClock,
        })
    }

    pub async fn discover(&self) -> Result<JsValue, JsValue> {
        Err(self.not_implemented("discover"))
    }

    pub async fn list_ops(&self) -> Result<JsValue, JsValue> {
        Err(self.not_implemented("list_ops"))
    }

    pub async fn create_order(
        &self,
        listing_id: String,
        params: JsValue,
        refund_dest: String,
    ) -> Result<JsValue, JsValue> {
        let _ = (listing_id, params, refund_dest);
        Err(self.not_implemented("create_order"))
    }

    pub async fn wait_provision(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        let _ = subscription_id;
        Err(self.not_implemented("wait_provision"))
    }

    pub async fn renew(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        let _ = subscription_id;
        Err(self.not_implemented("renew"))
    }

    pub async fn invoke_op(
        &self,
        subscription_id: String,
        op: String,
        op_kind: Option<String>,
        params: JsValue,
    ) -> Result<JsValue, JsValue> {
        let _ = (subscription_id, op, op_kind, params);
        Err(self.not_implemented("invoke_op"))
    }

    pub async fn cancel(&self, subscription_id: String) -> Result<JsValue, JsValue> {
        let _ = subscription_id;
        Err(self.not_implemented("cancel"))
    }
}

impl WebBuyer {
    fn not_implemented(&self, method: &str) -> JsValue {
        let _ = self.clock;
        js_error(
            "not_implemented",
            format!("WebBuyer.{method} is not implemented in this scaffold step"),
            json!({
                "method": method,
                "relay_url": self.config.relay_url,
                "operator_npub": self.config.operator_npub,
                "operator_pubkey": self.config.operator.to_hex(),
                "signer_mode": self.config.signer_mode,
            }),
        )
    }
}

#[derive(Debug, Clone)]
struct WebBuyerConfig {
    relay_url: String,
    operator_npub: String,
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
        let operator_npub = operator_npub.trim().to_owned();
        let operator = parse_operator_npub(&operator_npub)?;
        let signer_mode = SignerMode::parse(&signer_mode)?;

        Ok(Self {
            relay_url,
            operator_npub,
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
fn js_error(code: &str, message: impl Into<String>, details: Value) -> JsValue {
    serde_wasm_bindgen::to_value(&json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message.into(),
            "retryable": false,
            "details": details,
        }
    }))
    .unwrap_or_else(|_| JsValue::from_str(code))
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
}
