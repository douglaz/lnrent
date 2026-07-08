//! Acceptance round-trips for the lnrent wire codec (bead lnrent-7fp.19):
//! every DM message type encode->decode == identity through JSON AND through NIP-17 gift-wrap,
//! and a 30402 listing build->parse == identity including the operation declarations.

use lnrent_wire::dm::OpStatus;
use lnrent_wire::*;
use nostr::Keys;
use serde_json::json;
use std::sync::Arc;

/// One sample of every DM message type (SPEC.md §5.1), including the `id`-carrying requests
/// and the two nested-error carriers (`order.error`, `op.result`).
fn sample_messages() -> Vec<Msg> {
    vec![
        Msg::OrderRequest(OrderRequest {
            id: "ord-req-1".into(),
            listing_id: "30402:abc:wg-1".into(),
            params: json!({ "pubkey": "Wg/PeerKey=" }),
            refund_dest: Some("lnurl1refund".into()),
        }),
        // refund_dest omitted exercises the skip_serializing_if path.
        Msg::OrderRequest(OrderRequest {
            id: "ord-req-2".into(),
            listing_id: "30402:abc:wg-1".into(),
            params: json!({}),
            refund_dest: None,
        }),
        Msg::OrderInvoice(OrderInvoice {
            request_id: "ord-req-1".into(),
            order_id: "order-1".into(),
            bolt11: "lnbc500n1...".into(),
            amount_sat: 5000,
            period: "30d".into(),
            expires_at: 1_700_000_900,
        }),
        Msg::OrderError(OrderError {
            request_id: "ord-req-1".into(),
            order_id: None,
            error: WireError {
                code: "params_invalid".into(),
                message: "pubkey is not a valid WireGuard key".into(),
                retryable: false,
            },
        }),
        Msg::ProvisionReady(ProvisionReady {
            subscription_id: "sub-1".into(),
            payload: json!({ "config": "[Interface]\n..." }),
        }),
        Msg::DeliveryResendRequest(DeliveryResendRequest {
            subscription_id: "sub-1".into(),
        }),
        // Answering a renew.request: carries the correlating request_id.
        Msg::BillingInvoice(BillingInvoice {
            subscription_id: "sub-1".into(),
            request_id: Some("renew-1".into()),
            bolt11: "lnbc500n1renew...".into(),
            amount_sat: 5000,
            due_at: 1_702_000_000,
            expires_at: 1_702_000_900,
        }),
        // Operator-initiated at the soft date (§6.2): no request_id — exercises the skip path.
        Msg::BillingInvoice(BillingInvoice {
            subscription_id: "sub-1".into(),
            request_id: None,
            bolt11: "lnbc500n1soft...".into(),
            amount_sat: 5000,
            due_at: 1_702_500_000,
            expires_at: 1_702_500_900,
        }),
        Msg::BillingNotice(BillingNotice {
            subscription_id: "sub-1".into(),
            state: "SUSPENDED".into(),
            message: "Your subscription expired and is suspended.".into(),
        }),
        Msg::BillingRefund(BillingRefund {
            subscription_id: "sub-1".into(),
            amount_sat: 5000,
            status: "sent".into(),
        }),
        Msg::RenewRequest(RenewRequest {
            id: "renew-1".into(),
            subscription_id: "sub-1".into(),
        }),
        Msg::SubCancel(SubCancel {
            subscription_id: "sub-1".into(),
        }),
        Msg::OpRequest(OpRequest {
            id: "op-req-1".into(),
            subscription_id: "sub-1".into(),
            op: "get-config".into(),
            params: json!({ "format": "qr" }),
        }),
        Msg::OpResult(OpResult::ok(
            "op-req-1",
            "sub-1",
            "get-config",
            json!({ "config": "[Interface]\n..." }),
        )),
        Msg::OpResult(OpResult::err(
            "op-req-2",
            "sub-1",
            "restart",
            WireError {
                code: "hook_failed".into(),
                message: "exit 1".into(),
                retryable: true,
            },
        )),
    ]
}

#[test]
fn every_message_round_trips_through_json() {
    for msg in sample_messages() {
        let encoded = serde_json::to_string(&msg).expect("encode");
        let decoded: Msg = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(
            decoded,
            msg,
            "JSON round-trip differs for {}",
            msg.type_str()
        );
    }
}

#[tokio::test]
async fn every_message_round_trips_through_gift_wrap() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    for msg in sample_messages() {
        let wrapped = gift_wrap(&sender, &recipient.public_key(), &msg)
            .await
            .expect("gift_wrap");
        // It is a NIP-59 gift wrap (kind 1059), not the plaintext message.
        assert_eq!(wrapped.kind.as_u16(), 1059);
        let out = gift_unwrap(&recipient, &wrapped)
            .await
            .expect("gift_unwrap");
        assert_eq!(
            out.msg,
            msg,
            "gift-wrap round-trip differs for {}",
            msg.type_str()
        );
        // The seal authenticates the sender (the operator authorizes by this, §7.4).
        assert_eq!(out.sender, sender.public_key());
    }
}

#[tokio::test]
async fn gift_wrap_accepts_injected_nostr_signers() {
    let sender: Arc<dyn NostrSigner> = Arc::new(Keys::generate());
    let recipient: Arc<dyn NostrSigner> = Arc::new(Keys::generate());
    let recipient_pubkey = recipient.get_public_key().await.expect("recipient pubkey");
    let msg = Msg::RenewRequest(RenewRequest {
        id: "renew-injected".into(),
        subscription_id: "sub-1".into(),
    });

    let wrapped = gift_wrap(&sender, &recipient_pubkey, &msg)
        .await
        .expect("gift_wrap with signer trait object");
    let out = gift_unwrap(&recipient, &wrapped)
        .await
        .expect("gift_unwrap with signer trait object");

    assert_eq!(out.msg, msg);
    assert_eq!(
        out.sender,
        sender.get_public_key().await.expect("sender pubkey")
    );
}

#[tokio::test]
async fn gift_unwrap_rejects_a_plain_event() {
    let recipient = Keys::generate();
    let sender = Keys::generate();
    // A non-gift-wrap event must be rejected, not mis-decoded.
    let plain = nostr::EventBuilder::text_note("hi")
        .sign_with_keys(&sender)
        .expect("sign");
    let err = gift_unwrap(&recipient, &plain).await.unwrap_err();
    assert!(
        matches!(err, Error::NotGiftWrap),
        "expected NotGiftWrap, got {err:?}"
    );
}

#[tokio::test]
async fn gift_unwrap_rejects_a_non_dm_rumor() {
    let recipient = Keys::generate();
    let sender = Keys::generate();
    // Valid lnrent JSON, but carried in a non-NIP-17 rumor (a kind-1 text note rather than the
    // kind-14 private DM). NIP-59 unseals rumors of any kind, so gift_unwrap must reject this on
    // the rumor kind before it would otherwise be processed as a protocol message.
    let lnrent_json = serde_json::to_string(&Msg::SubCancel(SubCancel {
        subscription_id: "sub-1".into(),
    }))
    .expect("encode");
    let rumor = nostr::EventBuilder::text_note(lnrent_json).build(sender.public_key());
    let wrap = nostr::EventBuilder::gift_wrap(&sender, &recipient.public_key(), rumor, [])
        .await
        .expect("gift_wrap a non-DM rumor");
    let err = gift_unwrap(&recipient, &wrap).await.unwrap_err();
    assert!(
        matches!(err, Error::NotPrivateDm),
        "expected NotPrivateDm, got {err:?}"
    );
}

#[tokio::test]
async fn gift_unwrap_rejects_a_tampered_outer_wrap() {
    let sender = Keys::generate();
    let recipient = Keys::generate();
    let msg = Msg::SubCancel(SubCancel {
        subscription_id: "sub-1".into(),
    });
    let mut wrapped = gift_wrap(&sender, &recipient.public_key(), &msg)
        .await
        .expect("gift_wrap");
    // Tamper with the outer envelope so its id no longer matches its content: gift_unwrap must
    // verify the outer kind-1059 event and reject it, not decrypt a forged/copied wrap.
    wrapped.content.push('x');
    let err = gift_unwrap(&recipient, &wrapped).await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidEvent(_)),
        "a tampered outer wrap must be rejected, got {err:?}"
    );
}

#[test]
fn op_result_request_id_correlates_to_op_request_id() {
    let req = Msg::OpRequest(OpRequest {
        id: "op-req-42".into(),
        subscription_id: "sub-1".into(),
        op: "status".into(),
        params: json!({}),
    });
    let res = Msg::OpResult(OpResult::ok(
        "op-req-42",
        "sub-1",
        "status",
        json!({ "up": true }),
    ));
    // The op.result correlates back to the op.request by carrying the request id.
    assert_eq!(req.id(), Some("op-req-42"));
    assert_eq!(res.request_id(), Some("op-req-42"));
}

#[test]
fn dedupe_id_is_stable_and_distinguishes_messages() {
    let sender = Keys::generate();
    let sender_pubkey = sender.public_key();
    let other_sender = Keys::generate();
    let other_sender_pubkey = other_sender.public_key();

    // Id-less, naturally-idempotent messages carry no dedupe key: they are meant to be
    // re-runnable, so suppressing a repeat would break the dropped-DM recovery path (SPEC.md
    // §5.1). The consumer falls back to the unique outer Nostr event id for true relay dupes.
    let cancel = Msg::SubCancel(SubCancel {
        subscription_id: "sub-1".into(),
    });
    let resend = Msg::DeliveryResendRequest(DeliveryResendRequest {
        subscription_id: "sub-1".into(),
    });
    assert_eq!(cancel.dedupe_id(&sender_pubkey), None);
    assert_eq!(
        resend.dedupe_id(&sender_pubkey),
        None,
        "a delivery.resend.request must stay re-runnable, not collapse to one key"
    );

    // Client ids are scoped by message type; a single map keyed by dedupe_id cannot drop a
    // distinct request just because the buyer reused the same local id in another protocol flow.
    let order = Msg::OrderRequest(OrderRequest {
        id: "same-id".into(),
        listing_id: "30402:abc:wg-1".into(),
        params: json!({}),
        refund_dest: None,
    });
    let renew = Msg::RenewRequest(RenewRequest {
        id: "same-id".into(),
        subscription_id: "sub-1".into(),
    });
    let op = Msg::OpRequest(OpRequest {
        id: "same-id".into(),
        subscription_id: "sub-1".into(),
        op: "status".into(),
        params: json!({}),
    });
    let op_result = Msg::OpResult(OpResult::ok(
        "same-id",
        "sub-1",
        "status",
        json!({ "up": true }),
    ));
    let sender_hex = sender_pubkey.to_hex();
    assert_eq!(
        order.dedupe_id(&sender_pubkey),
        Some(format!("{sender_hex}:order.request:same-id"))
    );
    assert_eq!(
        renew.dedupe_id(&sender_pubkey),
        Some(format!("{sender_hex}:renew.request:same-id"))
    );
    assert_eq!(
        op.dedupe_id(&sender_pubkey),
        Some(format!("{sender_hex}:op.request:same-id"))
    );
    assert_eq!(
        op_result.dedupe_id(&sender_pubkey),
        Some(format!("{sender_hex}:op.result:same-id"))
    );
    assert_ne!(
        order.dedupe_id(&sender_pubkey),
        order.dedupe_id(&other_sender_pubkey),
        "same client id from different senders must not collide"
    );
    assert_ne!(
        order.dedupe_id(&sender_pubkey),
        renew.dedupe_id(&sender_pubkey)
    );
    assert_ne!(
        renew.dedupe_id(&sender_pubkey),
        op.dedupe_id(&sender_pubkey)
    );
    assert_ne!(
        op.dedupe_id(&sender_pubkey),
        op_result.dedupe_id(&sender_pubkey)
    );
}

#[test]
fn billing_invoice_request_id_is_optional() {
    // A `billing.invoice` with no `request_id` is an operator-initiated soft-date renewal
    // invoice (SPEC.md §5.1 "when answering a renew.request", §6.2): it must parse, and its
    // correlation id is `None` rather than a decode error.
    let json = r#"{
        "type": "billing.invoice",
        "subscription_id": "sub-1",
        "bolt11": "lnbc500n1soft...",
        "amount_sat": 5000,
        "due_at": 1702000000,
        "expires_at": 1702000900
    }"#;
    let msg: Msg = serde_json::from_str(json).expect("operator-initiated billing.invoice parses");
    assert_eq!(msg.request_id(), None);
    match msg {
        Msg::BillingInvoice(b) => assert_eq!(b.request_id, None),
        other => panic!("expected billing.invoice, got {}", other.type_str()),
    }
}

#[test]
fn malformed_op_result_payloads_are_rejected() {
    let cases = [
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "ok"
        }"#,
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "ok",
            "data": null
        }"#,
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "ok",
            "data": []
        }"#,
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "ok",
            "data": {},
            "error": { "code": "hook_failed", "message": "boom", "retryable": false }
        }"#,
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "error"
        }"#,
        r#"{
            "type": "op.result",
            "request_id": "op-1",
            "subscription_id": "sub-1",
            "op": "status",
            "status": "error",
            "data": {},
            "error": { "code": "hook_failed", "message": "boom", "retryable": false }
        }"#,
    ];

    for case in cases {
        assert!(
            serde_json::from_str::<Msg>(case).is_err(),
            "malformed op.result should fail: {case}"
        );
    }
}

#[test]
fn malformed_op_result_is_rejected_on_encode() {
    // Built past the ok()/err() constructors with public fields, an inconsistent status/payload
    // pairing must fail to ENCODE too — the encode path is symmetric with decode so the codec
    // never emits a malformed `op.result` the peer would reject (SPEC.md §5.1).
    let cases = [
        // status `error` but no `error`.
        OpResult {
            request_id: "r1".into(),
            subscription_id: "s1".into(),
            op: "status".into(),
            status: OpStatus::Error,
            data: None,
            error: None,
        },
        // status `ok` but no `data`.
        OpResult {
            request_id: "r1".into(),
            subscription_id: "s1".into(),
            op: "status".into(),
            status: OpStatus::Ok,
            data: None,
            error: None,
        },
        // status `ok` carrying both `data` and `error`.
        OpResult {
            request_id: "r1".into(),
            subscription_id: "s1".into(),
            op: "status".into(),
            status: OpStatus::Ok,
            data: Some(json!({})),
            error: Some(WireError {
                code: "hook_failed".into(),
                message: "boom".into(),
                retryable: false,
            }),
        },
        // status `ok` but `data` is JSON null, which serde would decode as absent through
        // `Option<Value>`; reject it before emitting asymmetric wire JSON.
        OpResult::ok("r1", "s1", "status", serde_json::Value::Null),
        // status `ok` but `data` is not the object SPEC.md §5.1 requires.
        OpResult::ok("r1", "s1", "status", json!([])),
    ];
    for bad in cases {
        assert!(
            serde_json::to_string(&bad).is_err(),
            "malformed op.result should fail to encode directly"
        );
        // ...and also through the `Msg` tagged-enum wrapper.
        assert!(
            serde_json::to_string(&Msg::OpResult(bad)).is_err(),
            "malformed op.result should fail to encode through Msg"
        );
    }
}

/// A listing carrying the order params and two op declarations (one with its own params).
fn sample_listing(operator_hex: &str) -> Listing {
    Listing {
        d: "wg-1".into(),
        operator: operator_hex.into(),
        recipe_id: "wireguard".into(),
        recipe_version: "0.1.0".into(),
        title: "WireGuard VPN — 1 device".into(),
        summary: "Private WireGuard peer, 1 device, unmetered.".into(),
        amount_sat: 5000,
        period: "30d".into(),
        params: vec![ParamDecl {
            key: "pubkey".into(),
            label: "Your WireGuard public key".into(),
            ty: "string".into(),
            required: true,
        }],
        operations: vec![
            OperationDecl {
                name: "status".into(),
                label: "Service status".into(),
                kind: "request".into(),
                params: vec![],
            },
            OperationDecl {
                name: "get-config".into(),
                label: "Download WireGuard config".into(),
                kind: "request".into(),
                params: vec![ParamDecl {
                    key: "format".into(),
                    label: "Output format".into(),
                    ty: "string".into(),
                    required: false,
                }],
            },
        ],
        // The honest security tier (§9.1) buyer-core reads before renting.
        tier: Some("0".into()),
        version: SCHEMA_VERSION,
    }
}

#[test]
fn listing_builds_and_parses_to_identity() {
    let keys = Keys::generate();
    let listing = sample_listing(&keys.public_key().to_hex());

    let event = build_listing(&listing)
        .expect("build")
        .sign_with_keys(&keys)
        .expect("sign");
    assert_eq!(event.kind.as_u16(), LISTING_KIND);

    let parsed = parse_listing(&event).expect("parse");
    // Build -> parse == identity, including the op declarations and the schema version.
    assert_eq!(parsed.listing, listing);
    // listing_id is the addressable coordinate 30402:<pubkey>:<d> (§5.4).
    assert_eq!(
        parsed.listing_id,
        format!("30402:{}:wg-1", keys.public_key().to_hex())
    );
}

#[test]
fn listing_round_trips_the_security_tier() {
    let keys = Keys::generate();
    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.tier = Some("1.5".into());

    let event = build_listing(&listing)
        .expect("build")
        .sign_with_keys(&keys)
        .expect("sign");
    // The honest tier rides in the `lnrent` content as a structured field (§9.1), not prose, so
    // buyer-core/web can branch on it directly.
    let content: serde_json::Value = serde_json::from_str(&event.content).expect("content json");
    assert_eq!(content["lnrent"]["tier"], "1.5");

    let parsed = parse_listing(&event).expect("parse");
    assert_eq!(parsed.listing.tier, Some("1.5".into()));
    assert_eq!(parsed.listing, listing);
}

#[test]
fn listing_without_a_tier_omits_the_field_and_round_trips() {
    // A service that declares no tier omits the field entirely (not `null`); parse maps the
    // absent field back to `None` (forward/backward-tolerant, §5.4).
    let keys = Keys::generate();
    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.tier = None;

    let event = build_listing(&listing)
        .expect("build")
        .sign_with_keys(&keys)
        .expect("sign");
    let content: serde_json::Value = serde_json::from_str(&event.content).expect("content json");
    assert!(
        content["lnrent"].get("tier").is_none(),
        "a None tier must be omitted, not serialized as null"
    );

    let parsed = parse_listing(&event).expect("parse");
    assert_eq!(parsed.listing.tier, None);
    assert_eq!(parsed.listing, listing);
}

#[test]
fn listing_parse_rejects_invalid_event_signature() {
    let keys = Keys::generate();
    let listing = sample_listing(&keys.public_key().to_hex());
    let mut event = build_listing(&listing)
        .expect("build")
        .sign_with_keys(&keys)
        .expect("sign");

    event.content = event.content.replace("wireguard", "evilguard");

    assert!(
        matches!(parse_listing(&event), Err(Error::InvalidEvent(_))),
        "a mutated signed listing must be rejected before fields are trusted"
    );
}

#[test]
fn listing_parse_tolerates_unknown_tags_and_content_fields() {
    let keys = Keys::generate();
    let listing = sample_listing(&keys.public_key().to_hex());

    // A future operator adds an unknown event tag — parsing must not choke.
    let event = build_listing(&listing)
        .expect("build")
        .tag(nostr::Tag::custom(
            nostr::TagKind::custom("future"),
            ["whatever".to_string()],
        ))
        .sign_with_keys(&keys)
        .expect("sign");
    let parsed = parse_listing(&event).expect("parse with unknown tag");
    assert_eq!(parsed.listing, listing);
}

#[test]
fn listing_build_rejects_values_beyond_parser_bounds() {
    let keys = Keys::generate();
    let mut listing = sample_listing(&keys.public_key().to_hex());

    listing.operations = (0..=MAX_OPERATIONS)
        .map(|i| OperationDecl {
            name: format!("op{i}"),
            label: "x".into(),
            kind: "request".into(),
            params: vec![],
        })
        .collect();
    assert!(
        matches!(
            build_listing(&listing),
            Err(Error::TooMany {
                field: "operations",
                max: MAX_OPERATIONS,
            })
        ),
        "builder must reject operations arrays the parser rejects"
    );

    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.params = (0..=MAX_PARAMS)
        .map(|i| ParamDecl {
            key: format!("k{i}"),
            label: "x".into(),
            ty: "string".into(),
            required: false,
        })
        .collect();
    assert!(
        matches!(
            build_listing(&listing),
            Err(Error::TooMany {
                field: "params",
                max: MAX_PARAMS,
            })
        ),
        "builder must reject top-level params arrays the parser rejects"
    );

    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.operations[0].params = (0..=MAX_PARAMS)
        .map(|i| ParamDecl {
            key: format!("k{i}"),
            label: "x".into(),
            ty: "string".into(),
            required: false,
        })
        .collect();
    assert!(
        matches!(
            build_listing(&listing),
            Err(Error::TooMany {
                field: "operation.params",
                max: MAX_PARAMS,
            })
        ),
        "builder must reject operation params arrays the parser rejects"
    );
}

#[test]
fn listing_build_and_parse_reject_unsupported_schema_version() {
    let keys = Keys::generate();
    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.version = SCHEMA_VERSION + 1;

    assert!(
        matches!(
            build_listing(&listing),
            Err(Error::UnsupportedSchemaVersion {
                found,
                supported: SCHEMA_VERSION,
            }) if found == SCHEMA_VERSION + 1
        ),
        "builder must reject stale or future schema versions"
    );

    let content = json!({
        "lnrent": {
            "version": SCHEMA_VERSION + 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "params": [],
            "operations": []
        }
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    assert!(
        matches!(
            parse_listing(&event),
            Err(Error::UnsupportedSchemaVersion {
                found,
                supported: SCHEMA_VERSION,
            }) if found == SCHEMA_VERSION + 1
        ),
        "parser must reject stale or future schema versions"
    );
}

#[test]
fn listing_content_tolerates_unknown_keys() {
    // A content blob from a newer schema carries extra keys inside `lnrent`; we parse what we
    // know and ignore the rest (forward-compat, §5.4).
    let keys = Keys::generate();
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0", "future_field": 7 },
            "params": [],
            "operations": [],
            "extra_unknown": { "anything": true }
        },
        "also_unknown": "tolerated"
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    let parsed = parse_listing(&event).expect("parse with unknown content keys");
    assert_eq!(parsed.listing.recipe_id, "wireguard");
    assert_eq!(parsed.listing.version, 1);
}

#[test]
fn listing_parse_uses_first_d_tag_for_coordinate() {
    let keys = Keys::generate();
    let listing = sample_listing(&keys.public_key().to_hex());

    let event = build_listing(&listing)
        .expect("build")
        .tag(nostr::Tag::identifier("other-d".to_string()))
        .sign_with_keys(&keys)
        .expect("sign");
    let parsed = parse_listing(&event).expect("parse duplicate d tags");

    assert_eq!(parsed.listing.d, "wg-1");
    assert_eq!(
        parsed.listing_id,
        format!("30402:{}:wg-1", keys.public_key().to_hex())
    );
}

#[test]
fn listing_parse_rejects_non_sat_price_currency() {
    let keys = Keys::generate();
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "params": [],
            "operations": []
        }
    })
    .to_string();
    let event = nostr::EventBuilder::new(nostr::Kind::Custom(LISTING_KIND), content)
        .tags([
            nostr::Tag::identifier("wg-1".to_string()),
            nostr::Tag::custom(nostr::TagKind::custom("title"), ["t".to_string()]),
            nostr::Tag::custom(nostr::TagKind::custom("summary"), ["s".to_string()]),
            nostr::Tag::custom(
                nostr::TagKind::custom("price"),
                ["5000".to_string(), "USD".to_string(), "30d".to_string()],
            ),
            nostr::Tag::custom(
                nostr::TagKind::custom("operator"),
                [keys.public_key().to_hex()],
            ),
        ])
        .sign_with_keys(&keys)
        .expect("sign");

    let err = parse_listing(&event).unwrap_err();
    assert!(
        matches!(err, Error::InvalidPriceCurrency { ref found } if found == "USD"),
        "expected InvalidPriceCurrency, got {err:?}"
    );
}

/// Sign a kind-30402 event with the standard listing tags around an arbitrary `content` JSON
/// — used to feed hostile / oversized content blobs to [`parse_listing`].
fn listing_event_with_content(keys: &Keys, content: String) -> nostr::Event {
    nostr::EventBuilder::new(nostr::Kind::Custom(LISTING_KIND), content)
        .tags([
            nostr::Tag::identifier("wg-1".to_string()),
            nostr::Tag::custom(nostr::TagKind::custom("title"), ["t".to_string()]),
            nostr::Tag::custom(nostr::TagKind::custom("summary"), ["s".to_string()]),
            nostr::Tag::custom(
                nostr::TagKind::custom("price"),
                ["5000".to_string(), "SAT".to_string(), "30d".to_string()],
            ),
            nostr::Tag::custom(
                nostr::TagKind::custom("operator"),
                [keys.public_key().to_hex()],
            ),
        ])
        .sign_with_keys(keys)
        .expect("sign")
}

#[test]
fn listing_parse_bounds_the_operations_array() {
    // One past the bound must be rejected as the array is read (forward-compat DoS guard,
    // §5.4) — not silently accepted after a hostile array is fully materialized.
    let keys = Keys::generate();
    let ops: Vec<_> = (0..=MAX_OPERATIONS)
        .map(|i| json!({ "name": format!("op{i}"), "label": "x", "kind": "request" }))
        .collect();
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "operations": ops
        }
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    assert!(
        matches!(parse_listing(&event), Err(Error::Json(_))),
        "an over-bound operations array must be rejected"
    );

    // Exactly at the bound is accepted.
    let ops: Vec<_> = (0..MAX_OPERATIONS)
        .map(|i| json!({ "name": format!("op{i}"), "label": "x", "kind": "request" }))
        .collect();
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "operations": ops
        }
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    let parsed = parse_listing(&event).expect("a listing at the operations bound parses");
    assert_eq!(parsed.listing.operations.len(), MAX_OPERATIONS);
}

#[test]
fn listing_parse_bounds_the_params_arrays() {
    let keys = Keys::generate();
    let params: Vec<_> = (0..=MAX_PARAMS)
        .map(|i| json!({ "key": format!("k{i}"), "label": "x", "type": "string" }))
        .collect();
    // Top-level order params over the bound: rejected.
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "params": params.clone()
        }
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    assert!(
        matches!(parse_listing(&event), Err(Error::Json(_))),
        "an over-bound top-level params array must be rejected"
    );

    // A single operation's params over the bound: also rejected.
    let content = json!({
        "lnrent": {
            "version": 1,
            "recipe": { "id": "wireguard", "version": "0.1.0" },
            "operations": [{ "name": "op", "label": "x", "kind": "request", "params": params }]
        }
    })
    .to_string();
    let event = listing_event_with_content(&keys, content);
    assert!(
        matches!(parse_listing(&event), Err(Error::Json(_))),
        "an over-bound operation params array must be rejected"
    );
}

#[test]
fn listing_build_rejects_an_empty_d() {
    let keys = Keys::generate();
    let mut listing = sample_listing(&keys.public_key().to_hex());
    listing.d = String::new();
    assert!(
        matches!(build_listing(&listing), Err(Error::Missing("d"))),
        "an empty d yields a malformed coordinate and must be rejected"
    );
}

#[test]
fn listing_build_rejects_a_non_pubkey_operator() {
    let mut listing = sample_listing("not-a-real-pubkey");
    listing.d = "wg-1".into();
    assert!(
        matches!(build_listing(&listing), Err(Error::InvalidOperator { ref found }) if found == "not-a-real-pubkey"),
        "operator must be a valid public key"
    );
}

#[test]
fn non_listing_event_is_rejected() {
    let keys = Keys::generate();
    let plain = nostr::EventBuilder::text_note("not a listing")
        .sign_with_keys(&keys)
        .expect("sign");
    assert!(matches!(parse_listing(&plain), Err(Error::NotListing)));
}

// op.status round-trips as the lowercase wire form.
#[test]
fn op_status_round_trips() {
    for s in [OpStatus::Ok, OpStatus::Error] {
        let j = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<OpStatus>(&j).unwrap(), s);
    }
}

/// An `order.request` with a `filler` param of `n` 'x' bytes (1 byte each in JSON — no escapes).
fn order_with_filler(n: usize) -> Msg {
    Msg::OrderRequest(OrderRequest {
        id: "sized-1".into(),
        listing_id: "30402:abc:wg-1".into(),
        params: json!({ "filler": "x".repeat(n) }),
        refund_dest: None,
    })
}

// gdu.4: a large-but-legitimate message near the top of the wrappable range round-trips — the
// inbound content cap (MAX_INBOUND_CONTENT_BYTES) does not falsely reject anything the transport
// can carry. The cap's exact boundary is unit-tested in wire/src/wrap.rs instead, because the
// NIP-59 layering (NIP-44's 65,535-byte plaintext ceiling applied to the seal's base64-expanded
// ciphertext) caps rumor content well BELOW our bound: `gift_wrap` cannot produce an at-cap
// rumor, which this test pins as the second assertion (the transport itself already refuses
// over-cap content, making the unwrap-side guard defense-in-depth).
#[tokio::test]
async fn large_wrappable_content_round_trips_and_transport_refuses_over_cap() {
    let sender = Keys::generate();
    let recipient = Keys::generate();

    // 32 KiB of filler: comfortably above every real lnrent message, still under the transport
    // ceiling — the wrap layer NIP-44-encrypts the SEAL event, whose content is the base64 (4/3×)
    // ciphertext of the padded rumor, so rumor content tops out just under 40 KiB (measured:
    // 40,572 bytes with this fixture), not 65,535.
    let big = order_with_filler(32 * 1024);
    let wrapped = gift_wrap(&sender, &recipient.public_key(), &big)
        .await
        .expect("gift_wrap a 32 KiB message");
    let out = gift_unwrap(&recipient, &wrapped)
        .await
        .expect("a large legitimate message decodes — no false reject from the content cap");
    assert_eq!(out.msg, big);

    // Content past MAX_INBOUND_CONTENT_BYTES cannot even be produced: the NIP-44 encrypt step
    // refuses the plaintext, so the unwrap-side cap is currently unreachable via gift_wrap.
    let over = order_with_filler(MAX_INBOUND_CONTENT_BYTES + 1);
    assert!(
        gift_wrap(&sender, &recipient.public_key(), &over)
            .await
            .is_err(),
        "the NIP-44 plaintext ceiling refuses over-cap content at wrap time"
    );
}
