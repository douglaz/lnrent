//! lnrent wire-protocol codec — the on-wire schema shared by the operator Nostr engine and
//! buyer-core (SPEC.md §5). Pure codec, no network/relay/signer/payment I/O: it owns the typed
//! messages and the encode/decode functions, and the consumers inject the keys and the relay.
//! Target-agnostic — compiles native AND wasm32 (the web buyer reuses it).
//!
//! Three layers:
//! - [`Msg`] and friends — the lnrent DM protocol (SPEC.md §5.1), JSON-encoded.
//! - [`gift_wrap`] / [`gift_unwrap`] — the NIP-17 gift-wrap transport for those messages.
//! - [`Listing`] with [`build_listing`] / [`parse_listing`] — NIP-99 30402 listings (SPEC.md §5.4).

pub mod dm;
pub mod error;
pub mod listing;
pub mod wrap;

pub use dm::{
    BillingInvoice, BillingNotice, BillingRefund, DeliveryResendRequest, Msg, OpRequest, OpResult,
    OpStatus, OrderError, OrderInvoice, OrderRequest, ProvisionReady, RenewRequest, SubCancel,
    WireError,
};
pub use error::Error;
pub use listing::{
    build_listing, listing_coordinate, parse_listing, Listing, OperationDecl, ParamDecl,
    ParsedListing, LISTING_KIND, MAX_OPERATIONS, MAX_PARAMS, SCHEMA_VERSION,
};
pub use wrap::{gift_unwrap, gift_wrap, Unwrapped, MAX_INBOUND_CONTENT_BYTES};

// Re-export the rust-nostr types that appear in this crate's signatures, so a consumer can
// drive the codec without depending on `nostr` directly.
pub use nostr::{Event, Keys, NostrSigner, PublicKey};

#[cfg(test)]
mod tests {
    use super::*;

    // The `type` discriminator strings are the wire contract (SPEC.md §5.1); guard them so a
    // serde rename can't silently drift. The two error carriers share the nested shape.
    #[test]
    fn message_type_discriminators_match_spec() {
        let cases = [
            (
                Msg::RenewRequest(RenewRequest {
                    id: "r1".into(),
                    subscription_id: "s1".into(),
                }),
                "renew.request",
            ),
            (
                Msg::SubCancel(SubCancel {
                    subscription_id: "s1".into(),
                }),
                "sub.cancel",
            ),
            (
                Msg::DeliveryResendRequest(DeliveryResendRequest {
                    subscription_id: "s1".into(),
                }),
                "delivery.resend.request",
            ),
        ];
        for (msg, want) in cases {
            assert_eq!(msg.type_str(), want);
            let json: serde_json::Value =
                serde_json::from_str(&serde_json::to_string(&msg).unwrap()).unwrap();
            assert_eq!(json["type"], want);
        }
    }

    // op.result `status` serializes lowercase, and the nested error shape is `{code,message,
    // retryable}` with no top-level `code` (SPEC.md §5.1, ADR-0014).
    #[test]
    fn op_result_error_uses_nested_shape() {
        let r = OpResult::err(
            "req1",
            "sub1",
            "restart",
            WireError {
                code: "hook_failed".into(),
                message: "boom".into(),
                retryable: false,
            },
        );
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(json["status"], "error");
        assert_eq!(json["error"]["code"], "hook_failed");
        assert_eq!(json["error"]["retryable"], false);
        // No top-level `code`: agents branch on the nested `error` uniformly.
        assert!(json.get("code").is_none());
        // `ok` results omit `error` entirely.
        let ok = OpResult::ok("req2", "sub1", "status", serde_json::json!({"up": true}));
        let ok_json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&ok).unwrap()).unwrap();
        assert_eq!(ok_json["status"], "ok");
        assert!(ok_json.get("error").is_none());
    }
}
