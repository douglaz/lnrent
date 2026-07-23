//! The lnrent DM protocol (SPEC.md §5.1): the JSON message types carried inside NIP-17
//! private DMs between a buyer and an operator. Each message is a JSON object with a `type`
//! discriminator; [`Msg`] is the tagged union over all of them.
//!
//! Structs ignore unknown fields on decode (no `deny_unknown_fields`) so a newer peer can add
//! fields without breaking an older one (forward-compat).

use nostr::PublicKey;
use serde::de::{self, Deserializer};
use serde::ser::{self, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// The nested error shape shared by `order.error` and `op.result` (SPEC.md §5.1, ADR-0014):
/// always `{ code, message, retryable }` — never a top-level `code` — so a buyer agent
/// branches on errors uniformly regardless of which message carried them. `code` is an open
/// string: `order.error` uses `capacity_full` / `params_invalid` / `price_changed` /
/// `unavailable` / `refund_dest_invalid` / `rejected` (reserved; not currently emitted); `op.result` uses `unauthorized` / `unknown_op` /
/// `invalid_params` / `not_active` / `timeout` / `hook_failed` / `interrupted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

/// `order.request` — buyer → operator. Opens an order against a listing (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    /// Client-chosen unique request id; the operator dedupes on `(sender, id)` (§5.1).
    pub id: String,
    /// The listing's addressable coordinate `30402:<pubkey>:<d>` (§5.4).
    pub listing_id: String,
    /// Validated order params (the listing's `params` schema, §7.1).
    pub params: Value,
    /// Refund destination — a re-resolvable Lightning address or HTTPS LNURL (ADR-0003). REQUIRED for
    /// new orders: the daemon rejects a missing/empty value, a raw BOLT11, or a BOLT12 offer at intake
    /// (spec F3/F6). Kept `Option` on the wire only so legacy rows / non-order messages deserialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refund_dest: Option<String>,
}

/// `order.invoice` — operator → buyer. The first invoice for an order (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderInvoice {
    /// Correlates to the originating `order.request` `id`.
    pub request_id: String,
    pub order_id: String,
    pub bolt11: String,
    pub amount_sat: u64,
    pub period: String,
    /// Unix seconds after which the invoice (and order) expires.
    pub expires_at: i64,
}

/// `order.error` — operator → buyer. A pre-order or order-time failure (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderError {
    /// Correlates to the originating `order.request` `id`.
    pub request_id: String,
    /// Absent for a pre-order validation failure (no order was created).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// The shared nested error shape (§5.1, ADR-0014).
    pub error: WireError,
}

/// `provision.ready` — operator → buyer. Delivers the credentials after provisioning (§5.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionReady {
    pub subscription_id: String,
    /// The delivery payload (e.g. a WireGuard config); opaque to the codec.
    pub payload: Value,
}

/// `delivery.resend.request` — buyer → operator. Re-send the latest `provision.ready`
/// (dropped-DM resync). Naturally idempotent, so it carries no request id (§5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryResendRequest {
    pub subscription_id: String,
}

/// `billing.invoice` — operator → buyer. A renewal invoice (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingInvoice {
    pub subscription_id: String,
    /// Correlates to the originating `renew.request` `id` *when answering one* (SPEC.md §5.1).
    /// Absent for an operator-initiated renewal invoice — the daemon proactively makes one
    /// available at the soft date (§6.2) with no originating request to correlate to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub bolt11: String,
    pub amount_sat: u64,
    pub due_at: i64,
    pub expires_at: i64,
}

/// `billing.notice` — operator → buyer. A renewal reminder / suspend / terminate notice (§5.1).
///
/// `request_id` correlates the notice to an originating buyer request when there is one — set ONLY
/// for the transient-RESUMING answer to a `renew.request` (lnrent-z4u/zs2), so the buyer's `renew()`
/// can match it exactly like a `billing.invoice` and NOT confuse a relay-replayed stale notice (or an
/// unsolicited reminder/suspend/terminate notice, which carry `None`) for its live reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingNotice {
    pub subscription_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub state: String,
    pub message: String,
}

/// `billing.refund` — operator → buyer. The outcome of a refund (SPEC.md §5.1, ADR-0003).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingRefund {
    pub subscription_id: String,
    pub amount_sat: u64,
    /// `sent` | `failed`.
    pub status: String,
}

/// `renew.request` — buyer → operator. Requests a renewal invoice on demand (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRequest {
    /// Client-chosen unique request id; the operator dedupes on `(sender, id)` (§5.1).
    pub id: String,
    pub subscription_id: String,
}

/// `operator.alert` — operator → operator's OWN chosen peer (lnrent-urw.1 / GATE-1 PR-5). NOT a
/// buyer-facing message: it is a NIP-17 DM the daemon sends to the operator's personal npub (or
/// itself) to surface a condition the money/provisioning path detected, riding the same durable
/// outbox as every other DM. Buyers never receive or decode it. `kind` is one of the daemon's
/// closed `AlertKind` wire spellings; `subject`/`detail` are human-readable context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorAlert {
    pub kind: String,
    pub subject: String,
    pub detail: String,
}

/// `sub.cancel` — buyer → operator. Cancels a subscription. Naturally idempotent, so it
/// carries no request id (SPEC.md §5.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubCancel {
    pub subscription_id: String,
}

/// `op.request` — buyer → operator. Invokes a recipe-declared management operation
/// (SPEC.md §5.1, §7.4, ADR-0013).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpRequest {
    /// Client-chosen unique request id; the operator dedupes on `(sender, id)` (§5.1).
    pub id: String,
    pub subscription_id: String,
    /// The operation name (matches a listing/recipe `[[operation]]` name).
    pub op: String,
    /// The operation's params object (the op's `params` schema).
    pub params: Value,
}

/// `op.request` / `op.result` status discriminator (SPEC.md §5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpStatus {
    Ok,
    Error,
}

/// `op.result` — operator → buyer. The result of an `op.request` (SPEC.md §5.1, §7.4).
/// On `ok`, `data` carries the hook output; on `error`, `error` carries the shared nested
/// shape (the same one as `order.error`).
///
/// Both ser and de enforce the `status`/payload pairing via [`OpResult::validate`] (a custom
/// `Serialize` runs it too) so the encode path is symmetric with decode: a caller that bypasses
/// the [`OpResult::ok`] / [`OpResult::err`] constructors and hand-builds an inconsistent value
/// (e.g. `status: Error` with no `error`) fails to encode rather than emitting a malformed
/// `op.result` the peer would then reject.
#[derive(Debug, Clone, PartialEq)]
pub struct OpResult {
    /// Correlates to the originating `op.request` `id`.
    pub request_id: String,
    pub subscription_id: String,
    pub op: String,
    pub status: OpStatus,
    /// Present on `ok`: the operation's output (config / `url` / status fields). The custom
    /// `Serialize` omits it when `None` (and requires it on `ok`); see [`OpResult::validate`].
    pub data: Option<Value>,
    /// Present on `error`: the shared nested error shape (§5.1, ADR-0014). The custom
    /// `Serialize` omits it when `None` (and requires it on `error`).
    pub error: Option<WireError>,
}

impl Serialize for OpResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Encode is symmetric with decode: reject an inconsistent status/payload pairing here
        // rather than emit a malformed `op.result` (§5.1). A valid value carries exactly one of
        // `data` / `error`, matching the `skip_serializing_if = "Option::is_none"` of the
        // previously derived impl.
        self.validate().map_err(ser::Error::custom)?;
        let fields = 4 + self.data.is_some() as usize + self.error.is_some() as usize;
        let mut s = serializer.serialize_struct("OpResult", fields)?;
        s.serialize_field("request_id", &self.request_id)?;
        s.serialize_field("subscription_id", &self.subscription_id)?;
        s.serialize_field("op", &self.op)?;
        s.serialize_field("status", &self.status)?;
        if let Some(data) = &self.data {
            s.serialize_field("data", data)?;
        }
        if let Some(error) = &self.error {
            s.serialize_field("error", error)?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for OpResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireOpResult {
            request_id: String,
            subscription_id: String,
            op: String,
            status: OpStatus,
            #[serde(default)]
            data: Option<Value>,
            #[serde(default)]
            error: Option<WireError>,
        }

        let wire = WireOpResult::deserialize(deserializer)?;
        let result = OpResult {
            request_id: wire.request_id,
            subscription_id: wire.subscription_id,
            op: wire.op,
            status: wire.status,
            data: wire.data,
            error: wire.error,
        };
        result.validate().map_err(de::Error::custom)?;
        Ok(result)
    }
}

impl OpResult {
    /// An `ok` result carrying the hook's output `data`.
    pub fn ok(
        request_id: impl Into<String>,
        subscription_id: impl Into<String>,
        op: impl Into<String>,
        data: Value,
    ) -> Self {
        OpResult {
            request_id: request_id.into(),
            subscription_id: subscription_id.into(),
            op: op.into(),
            status: OpStatus::Ok,
            data: Some(data),
            error: None,
        }
    }

    /// An `error` result carrying the shared nested error shape.
    pub fn err(
        request_id: impl Into<String>,
        subscription_id: impl Into<String>,
        op: impl Into<String>,
        error: WireError,
    ) -> Self {
        OpResult {
            request_id: request_id.into(),
            subscription_id: subscription_id.into(),
            op: op.into(),
            status: OpStatus::Error,
            data: None,
            error: Some(error),
        }
    }

    /// Validate the `status`/payload pairing required by SPEC.md §5.1.
    pub fn validate(&self) -> Result<(), &'static str> {
        match self.status {
            OpStatus::Ok => {
                let Some(data) = &self.data else {
                    return Err("op.result status `ok` requires object `data`");
                };
                if !data.is_object() {
                    return Err("op.result status `ok` requires object `data`");
                }
                if self.error.is_some() {
                    return Err("op.result status `ok` must not include `error`");
                }
            }
            OpStatus::Error => {
                if self.error.is_none() {
                    return Err("op.result status `error` requires `error`");
                }
                if self.data.is_some() {
                    return Err("op.result status `error` must not include `data`");
                }
            }
        }
        Ok(())
    }
}

/// The tagged union of every lnrent DM message (SPEC.md §5.1). The wire form is a JSON object
/// whose `type` field selects the variant, e.g. `{"type":"order.request", ...}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Msg {
    #[serde(rename = "order.request")]
    OrderRequest(OrderRequest),
    #[serde(rename = "order.invoice")]
    OrderInvoice(OrderInvoice),
    #[serde(rename = "order.error")]
    OrderError(OrderError),
    #[serde(rename = "provision.ready")]
    ProvisionReady(ProvisionReady),
    #[serde(rename = "delivery.resend.request")]
    DeliveryResendRequest(DeliveryResendRequest),
    #[serde(rename = "billing.invoice")]
    BillingInvoice(BillingInvoice),
    #[serde(rename = "billing.notice")]
    BillingNotice(BillingNotice),
    #[serde(rename = "billing.refund")]
    BillingRefund(BillingRefund),
    #[serde(rename = "renew.request")]
    RenewRequest(RenewRequest),
    #[serde(rename = "sub.cancel")]
    SubCancel(SubCancel),
    #[serde(rename = "op.request")]
    OpRequest(OpRequest),
    #[serde(rename = "op.result")]
    OpResult(OpResult),
    #[serde(rename = "operator.alert")]
    OperatorAlert(OperatorAlert),
}

impl Msg {
    /// The wire `type` discriminator, e.g. `"order.request"`.
    pub fn type_str(&self) -> &'static str {
        match self {
            Msg::OrderRequest(_) => "order.request",
            Msg::OrderInvoice(_) => "order.invoice",
            Msg::OrderError(_) => "order.error",
            Msg::ProvisionReady(_) => "provision.ready",
            Msg::DeliveryResendRequest(_) => "delivery.resend.request",
            Msg::BillingInvoice(_) => "billing.invoice",
            Msg::BillingNotice(_) => "billing.notice",
            Msg::BillingRefund(_) => "billing.refund",
            Msg::RenewRequest(_) => "renew.request",
            Msg::SubCancel(_) => "sub.cancel",
            Msg::OpRequest(_) => "op.request",
            Msg::OpResult(_) => "op.result",
            Msg::OperatorAlert(_) => "operator.alert",
        }
    }

    /// The client-chosen request `id` of a request message that carries one — `order.request`,
    /// `renew.request`, `op.request` (SPEC.md §5.1). `None` for every other message.
    pub fn id(&self) -> Option<&str> {
        match self {
            Msg::OrderRequest(m) => Some(&m.id),
            Msg::RenewRequest(m) => Some(&m.id),
            Msg::OpRequest(m) => Some(&m.id),
            _ => None,
        }
    }

    /// The request `id` a response correlates back to — `order.invoice`, `order.error`,
    /// `billing.invoice`, and `op.result` (SPEC.md §5.1). In particular `op.result`'s
    /// `request_id` correlates to the `op.request` `id`. `None` for every other message.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Msg::OrderInvoice(m) => Some(&m.request_id),
            Msg::OrderError(m) => Some(&m.request_id),
            Msg::OpResult(m) => Some(&m.request_id),
            // `billing.invoice` carries a `request_id` only when answering a `renew.request`;
            // an operator-initiated soft-date invoice has none (§5.1, §6.2).
            Msg::BillingInvoice(m) => m.request_id.as_deref(),
            // `billing.notice` carries a `request_id` only for the transient-RESUMING answer to a
            // `renew.request` (lnrent-zs2); unsolicited notices (reminder/suspend/terminate/cancel)
            // have none. Exposing it here lets `dedupe_id` suppress a relay-replayed duplicate of
            // that specific reply and lets consumers correlate it like a `billing.invoice`.
            Msg::BillingNotice(m) => m.request_id.as_deref(),
            _ => None,
        }
    }

    /// A stable, deterministic key for idempotent dedupe of a request/response message by one
    /// receiving peer (SPEC.md §5.1) — or `None` when the message carries no such key.
    ///
    /// `Some` only for the messages §5.1 makes idempotent on a client-chosen id: the request
    /// messages (`order.request`, `renew.request`, `op.request`, keyed by their `id`) and the
    /// correlated responses (`order.invoice`, `order.error`, `op.result`, and a
    /// `renew.request`-answering `billing.invoice`, keyed by their `request_id`). The sender
    /// pubkey is part of the key because the durable operator-side keys in §5.1/§11 are
    /// `(sender_pubkey, request_id)`; the wire `type` is also folded in so two message classes
    /// that happen to reuse the same client-chosen id (e.g. an `order.request` and an
    /// `op.request`) cannot collide in a single consumer dedupe map. Consumers that instead key
    /// the spec's composite DB rows (`inbound_request` / `op_invocation`, both
    /// `(sender_pubkey, request_id)` with no `type`) should build those from `sender` plus
    /// [`Msg::id`] / [`Msg::request_id`] directly rather than parsing this string.
    ///
    /// `None` for the id-less, naturally-idempotent messages (`sub.cancel`,
    /// `delivery.resend.request`, `provision.ready`, `billing.notice`, `billing.refund`, and an
    /// operator-initiated `billing.invoice` with no `request_id`): they act on existing state
    /// and are meant to be re-runnable, so they have no protocol-level dedupe key. In
    /// particular a content hash would be wrong here — it would give every
    /// `delivery.resend.request` for one subscription the same key and so suppress the buyer's
    /// retry, defeating the dropped-DM recovery path. A consumer that needs to drop only true
    /// relay-level duplicate deliveries should fall back to the unique outer Nostr event id.
    pub fn dedupe_id(&self, sender: &PublicKey) -> Option<String> {
        let msg_type = self.type_str();
        let key = self.id().or_else(|| self.request_id())?;
        Some(format!("{}:{msg_type}:{key}", sender.to_hex()))
    }
}
