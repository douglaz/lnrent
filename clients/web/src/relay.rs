#![cfg_attr(not(any(test, target_arch = "wasm32")), allow(dead_code))]

use lnrent_buyer_core::{
    lnrent_wire::{Event, PublicKey, LISTING_KIND},
    RelayError,
};
use serde_json::{json, Value};

#[cfg(target_arch = "wasm32")]
use {
    futures_util::{
        future::{select, Either},
        pin_mut, FutureExt, SinkExt, StreamExt,
    },
    gloo_net::websocket::{futures::WebSocket, Message, WebSocketError},
    gloo_timers::future::TimeoutFuture,
    lnrent_buyer_core::{GiftWrapStream, Relay},
    std::collections::HashSet,
    std::time::Duration,
};

const GIFT_WRAP_KIND: u16 = 1059;

#[cfg(target_arch = "wasm32")]
const SUBSCRIBE_SETTLE_MS: u32 = 500;

#[derive(Debug, Clone)]
pub struct BrowserRelay {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    url: String,
}

impl BrowserRelay {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum RelayFrame {
    Event {
        sub_id: String,
        event: Event,
    },
    Eose {
        sub_id: String,
    },
    Ok {
        event_id: String,
        accepted: bool,
        message: String,
    },
    Closed {
        sub_id: String,
        message: String,
    },
    Ignored,
}

fn build_event_frame(event: &Event) -> Result<String, RelayError> {
    serde_json::to_string(&json!(["EVENT", event]))
        .map_err(|e| RelayError(format!("encode EVENT frame: {e}")))
}

fn build_req_frame(sub_id: &str, filter: Value) -> Result<String, RelayError> {
    serde_json::to_string(&json!(["REQ", sub_id, filter]))
        .map_err(|e| RelayError(format!("encode REQ frame: {e}")))
}

fn build_close_frame(sub_id: &str) -> Result<String, RelayError> {
    serde_json::to_string(&json!(["CLOSE", sub_id]))
        .map_err(|e| RelayError(format!("encode CLOSE frame: {e}")))
}

fn listings_filter(operator: &PublicKey) -> Value {
    json!({
        "kinds": [LISTING_KIND],
        "authors": [operator.to_hex()],
    })
}

fn giftwrap_filter(recipient: &PublicKey) -> Value {
    json!({
        "kinds": [GIFT_WRAP_KIND],
        "#p": [recipient.to_hex()],
    })
}

fn parse_frame(raw: &str) -> Result<RelayFrame, String> {
    let value: Value = serde_json::from_str(raw).map_err(|e| format!("invalid JSON: {e}"))?;
    let frame = value
        .as_array()
        .ok_or_else(|| "relay frame must be a JSON array".to_string())?;
    let kind = frame
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| "relay frame kind must be a string".to_string())?;

    match kind {
        "EVENT" => {
            let sub_id = string_at(frame, 1, "EVENT subscription id")?;
            let event_value = frame
                .get(2)
                .ok_or_else(|| "EVENT frame missing event".to_string())?;
            let event: Event = serde_json::from_value(event_value.clone())
                .map_err(|e| format!("EVENT frame has invalid event: {e}"))?;
            Ok(RelayFrame::Event { sub_id, event })
        }
        "EOSE" => Ok(RelayFrame::Eose {
            sub_id: string_at(frame, 1, "EOSE subscription id")?,
        }),
        "OK" => Ok(RelayFrame::Ok {
            event_id: string_at(frame, 1, "OK event id")?,
            accepted: bool_at(frame, 2, "OK accepted flag")?,
            message: optional_string_at(frame, 3, "OK message")?,
        }),
        "CLOSED" => Ok(RelayFrame::Closed {
            sub_id: string_at(frame, 1, "CLOSED subscription id")?,
            message: optional_string_at(frame, 2, "CLOSED message")?,
        }),
        // NIP-01 NOTICE, NIP-42 AUTH, NIP-45 COUNT, and future relay-to-client frames are valid
        // traffic even when this buyer does not act on them.
        _ => Ok(RelayFrame::Ignored),
    }
}

fn string_at(frame: &[Value], idx: usize, what: &str) -> Result<String, String> {
    frame
        .get(idx)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("{what} must be a string"))
}

fn optional_string_at(frame: &[Value], idx: usize, what: &str) -> Result<String, String> {
    match frame.get(idx) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(value) => value
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("{what} must be a string")),
    }
}

fn bool_at(frame: &[Value], idx: usize, what: &str) -> Result<bool, String> {
    frame
        .get(idx)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("{what} must be a boolean"))
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
impl Relay for BrowserRelay {
    async fn publish(&self, event: &Event) -> Result<(), RelayError> {
        let mut ws = open_websocket(&self.url)?;
        let deadline = deadline_ms(Duration::from_secs(30));
        let frame = build_event_frame(event)?;
        let want_id = event.id.to_hex();

        send_text_until(&mut ws, frame, deadline, "publish EVENT").await?;

        loop {
            match read_text_until(&mut ws, deadline).await? {
                WsRead::Text(raw) => match parse_runtime_frame(&raw) {
                    RelayFrame::Ok {
                        event_id,
                        accepted,
                        message,
                    } if event_id == want_id => {
                        return if accepted {
                            Ok(())
                        } else {
                            Err(RelayError(format!(
                                "publish rejected for event {want_id}: {message}"
                            )))
                        };
                    }
                    _ => {}
                },
                WsRead::Ignored => {}
                WsRead::Timeout => {
                    return Err(RelayError(format!(
                        "publish: no OK for event {want_id} before timeout"
                    )))
                }
                WsRead::Closed => {
                    return Err(RelayError(format!(
                        "publish: socket closed before OK for event {want_id}"
                    )))
                }
            }
        }
    }

    async fn fetch_listings(
        &self,
        operator: &PublicKey,
        timeout: Duration,
    ) -> Result<Vec<Event>, RelayError> {
        let mut ws = open_websocket(&self.url)?;
        let sub_id = new_sub_id("listings")?;
        let deadline = deadline_ms(timeout);
        let req = build_req_frame(&sub_id, listings_filter(operator))?;
        let mut events = Vec::new();

        send_text_until(&mut ws, req, deadline, "send listings REQ").await?;

        loop {
            match read_text_until(&mut ws, deadline).await? {
                WsRead::Text(raw) => match parse_runtime_frame(&raw) {
                    RelayFrame::Event {
                        sub_id: frame_sub,
                        event,
                    } if frame_sub == sub_id => events.push(event),
                    RelayFrame::Eose { sub_id: frame_sub } if frame_sub == sub_id => break,
                    RelayFrame::Closed {
                        sub_id: frame_sub,
                        message,
                    } if frame_sub == sub_id => {
                        return Err(RelayError(format!(
                            "listings subscription closed by relay: {message}"
                        )))
                    }
                    _ => {}
                },
                WsRead::Ignored => {}
                WsRead::Timeout => break,
                WsRead::Closed => {
                    return Err(RelayError(
                        "listings subscription socket closed before EOSE or timeout".into(),
                    ))
                }
            }
        }

        send_close_best_effort(&mut ws, &sub_id).await;
        Ok(events)
    }

    async fn subscribe_giftwraps(
        &self,
        recipient: &PublicKey,
        timeout: Duration,
    ) -> Result<Box<dyn GiftWrapStream>, RelayError> {
        let mut ws = open_websocket(&self.url)?;
        let sub_id = new_sub_id("giftwraps")?;
        let deadline = deadline_ms(timeout);
        let req = build_req_frame(&sub_id, giftwrap_filter(recipient))?;

        send_text_until(&mut ws, req, deadline, "send giftwrap REQ").await?;
        TimeoutFuture::new(SUBSCRIBE_SETTLE_MS).await;

        Ok(Box::new(BrowserGiftWrapStream {
            ws: Some(ws),
            sub_id,
            deadline,
            seen: HashSet::new(),
            sent_close: false,
        }))
    }
}

#[cfg(target_arch = "wasm32")]
struct BrowserGiftWrapStream {
    ws: Option<WebSocket>,
    sub_id: String,
    deadline: f64,
    seen: HashSet<String>,
    sent_close: bool,
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
impl GiftWrapStream for BrowserGiftWrapStream {
    async fn next(&mut self) -> Result<Option<Event>, RelayError> {
        loop {
            let read = {
                let Some(ws) = self.ws.as_mut() else {
                    return Ok(None);
                };
                read_text_until(ws, self.deadline).await?
            };

            match read {
                WsRead::Text(raw) => match parse_runtime_frame(&raw) {
                    RelayFrame::Event { sub_id, event } if sub_id == self.sub_id => {
                        if self.seen.insert(event.id.to_hex()) {
                            return Ok(Some(event));
                        }
                    }
                    RelayFrame::Eose { sub_id } if sub_id == self.sub_id => {}
                    RelayFrame::Closed { sub_id, .. } if sub_id == self.sub_id => {
                        self.close().await;
                        return Ok(None);
                    }
                    _ => {}
                },
                WsRead::Ignored => {}
                WsRead::Timeout | WsRead::Closed => {
                    self.close().await;
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl BrowserGiftWrapStream {
    async fn close(&mut self) {
        if self.sent_close {
            return;
        }
        self.sent_close = true;
        if let Some(ws) = self.ws.as_mut() {
            send_close_best_effort(ws, &self.sub_id).await;
        }
        self.ws = None;
    }
}

#[cfg(target_arch = "wasm32")]
enum WsRead {
    Text(String),
    Ignored,
    Timeout,
    Closed,
}

#[cfg(target_arch = "wasm32")]
fn open_websocket(url: &str) -> Result<WebSocket, RelayError> {
    WebSocket::open(url).map_err(|e| RelayError(format!("open relay websocket {url}: {e}")))
}

#[cfg(target_arch = "wasm32")]
async fn send_text_until(
    ws: &mut WebSocket,
    text: String,
    deadline: f64,
    action: &str,
) -> Result<(), RelayError> {
    let Some(ms) = remaining_ms(deadline) else {
        return Err(RelayError(format!(
            "{action}: timeout before websocket send"
        )));
    };

    let send = ws.send(Message::Text(text)).fuse();
    let timer = TimeoutFuture::new(ms).fuse();
    pin_mut!(send, timer);

    match select(send, timer).await {
        Either::Left((Ok(()), _)) => Ok(()),
        Either::Left((Err(e), _)) => Err(RelayError(format!("{action}: {e}"))),
        Either::Right(((), _)) => Err(RelayError(format!("{action}: websocket send timed out"))),
    }
}

#[cfg(target_arch = "wasm32")]
async fn read_text_until(ws: &mut WebSocket, deadline: f64) -> Result<WsRead, RelayError> {
    let Some(ms) = remaining_ms(deadline) else {
        return Ok(WsRead::Timeout);
    };

    let next = ws.next().fuse();
    let timer = TimeoutFuture::new(ms).fuse();
    pin_mut!(next, timer);

    match select(next, timer).await {
        Either::Left((Some(Ok(Message::Text(raw))), _)) => Ok(WsRead::Text(raw)),
        Either::Left((Some(Ok(Message::Bytes(_))), _)) => Ok(WsRead::Ignored),
        Either::Left((Some(Err(WebSocketError::ConnectionClose(_))), _)) => Ok(WsRead::Closed),
        Either::Left((Some(Err(e)), _)) => Err(RelayError(format!("relay websocket read: {e}"))),
        Either::Left((None, _)) => Ok(WsRead::Closed),
        Either::Right(((), _)) => Ok(WsRead::Timeout),
    }
}

#[cfg(target_arch = "wasm32")]
fn parse_runtime_frame(raw: &str) -> RelayFrame {
    parse_frame(raw).unwrap_or(RelayFrame::Ignored)
}

#[cfg(target_arch = "wasm32")]
async fn send_close_best_effort(ws: &mut WebSocket, sub_id: &str) {
    if let Ok(frame) = build_close_frame(sub_id) {
        let _ = send_text_until(ws, frame, js_sys::Date::now() + 1_000.0, "send CLOSE").await;
    }
}

#[cfg(target_arch = "wasm32")]
fn deadline_ms(timeout: Duration) -> f64 {
    let timeout_ms = timeout.as_millis().min(u128::from(u32::MAX)) as f64;
    js_sys::Date::now() + timeout_ms
}

#[cfg(target_arch = "wasm32")]
fn remaining_ms(deadline: f64) -> Option<u32> {
    let remaining = deadline - js_sys::Date::now();
    if remaining <= 0.0 {
        None
    } else {
        Some(remaining.ceil().min(f64::from(u32::MAX)) as u32)
    }
}

#[cfg(target_arch = "wasm32")]
fn new_sub_id(prefix: &str) -> Result<String, RelayError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| RelayError(format!("generate relay subscription id: {e}")))?;
    Ok(format!("{prefix}-{}", hex(&bytes)))
}

#[cfg(target_arch = "wasm32")]
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use lnrent_buyer_core::lnrent_wire::{build_listing, Keys, Listing, SCHEMA_VERSION};

    #[test]
    fn builds_event_req_and_close_frames() {
        let event = sample_event();

        let event_frame: Value =
            serde_json::from_str(&build_event_frame(&event).expect("EVENT frame")).unwrap();
        assert_eq!(event_frame[0], "EVENT");
        assert_eq!(event_frame[1]["id"], event.id.to_hex());

        let listings_req: Value = serde_json::from_str(
            &build_req_frame("sub-list", listings_filter(&event.pubkey)).expect("REQ frame"),
        )
        .unwrap();
        assert_eq!(
            listings_req,
            json!([
                "REQ",
                "sub-list",
                {"kinds": [LISTING_KIND], "authors": [event.pubkey.to_hex()]}
            ])
        );

        let giftwrap_req: Value = serde_json::from_str(
            &build_req_frame("sub-gift", giftwrap_filter(&event.pubkey)).expect("REQ frame"),
        )
        .unwrap();
        assert_eq!(
            giftwrap_req,
            json!([
                "REQ",
                "sub-gift",
                {"kinds": [GIFT_WRAP_KIND], "#p": [event.pubkey.to_hex()]}
            ])
        );

        let close: Value =
            serde_json::from_str(&build_close_frame("sub-list").expect("CLOSE frame")).unwrap();
        assert_eq!(close, json!(["CLOSE", "sub-list"]));
    }

    #[test]
    fn parses_event_frame() {
        let event = sample_event();
        let raw = json!(["EVENT", "sub-1", event]).to_string();

        match parse_frame(&raw).expect("EVENT parses") {
            RelayFrame::Event { sub_id, event } => {
                assert_eq!(sub_id, "sub-1");
                assert_eq!(event.id.to_hex().len(), 64);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[test]
    fn parses_eose_ok_rejected_and_closed_frames() {
        assert_eq!(
            parse_frame(r#"["EOSE","sub-1"]"#).unwrap(),
            RelayFrame::Eose {
                sub_id: "sub-1".into()
            }
        );
        assert_eq!(
            parse_frame(r#"["OK","abc123",true,""]"#).unwrap(),
            RelayFrame::Ok {
                event_id: "abc123".into(),
                accepted: true,
                message: "".into()
            }
        );
        assert_eq!(
            parse_frame(r#"["OK","abc123",true]"#).unwrap(),
            RelayFrame::Ok {
                event_id: "abc123".into(),
                accepted: true,
                message: "".into()
            }
        );
        assert_eq!(
            parse_frame(r#"["OK","abc123",false,"blocked"]"#).unwrap(),
            RelayFrame::Ok {
                event_id: "abc123".into(),
                accepted: false,
                message: "blocked".into()
            }
        );
        assert_eq!(
            parse_frame(r#"["CLOSED","sub-1","restricted"]"#).unwrap(),
            RelayFrame::Closed {
                sub_id: "sub-1".into(),
                message: "restricted".into()
            }
        );
        assert_eq!(
            parse_frame(r#"["CLOSED","sub-1"]"#).unwrap(),
            RelayFrame::Closed {
                sub_id: "sub-1".into(),
                message: "".into()
            }
        );
    }

    #[test]
    fn ignores_valid_but_unhandled_relay_frames() {
        assert_eq!(
            parse_frame(r#"["NOTICE","relay is restarting"]"#).unwrap(),
            RelayFrame::Ignored
        );
        assert_eq!(
            parse_frame(r#"["AUTH","challenge"]"#).unwrap(),
            RelayFrame::Ignored
        );
        assert_eq!(
            parse_frame(r#"["COUNT","sub-1",{"count":3}]"#).unwrap(),
            RelayFrame::Ignored
        );
        assert_eq!(
            parse_frame(r#"["FUTURE","shape","does","not","matter"]"#).unwrap(),
            RelayFrame::Ignored
        );
    }

    #[cfg(target_arch = "wasm32")]
    #[test]
    fn runtime_parser_ignores_malformed_frames() {
        assert_eq!(parse_runtime_frame("not-json"), RelayFrame::Ignored);
        assert_eq!(
            parse_runtime_frame(r#"["EVENT","sub"]"#),
            RelayFrame::Ignored
        );
        assert_eq!(
            parse_runtime_frame(r#"["OK","abc","yes"]"#),
            RelayFrame::Ignored
        );
    }

    fn sample_event() -> Event {
        let keys = Keys::generate();
        build_listing(&Listing {
            d: "web-test".into(),
            operator: keys.public_key().to_hex(),
            recipe_id: "dummy".into(),
            recipe_version: "0.1.0".into(),
            title: "Dummy".into(),
            summary: "test".into(),
            amount_sat: 1,
            period: "30d".into(),
            params: Vec::new(),
            operations: Vec::new(),
            tier: None,
            version: SCHEMA_VERSION,
        })
        .unwrap()
        .sign_with_keys(&keys)
        .unwrap()
    }
}
