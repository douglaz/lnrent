//! Language-neutral conformance vectors (docs/protocol/README.md "Test vectors").
//!
//! `tests/vectors/*.json` is the byte-level contract a second implementation loads. This file
//! proves the Rust codec agrees with every fixture in both directions: decode the fixture, then
//! re-encode, and require the SAME JSON value — so a field the codec adds and the fixture lacks
//! fails (re-encode gains a key), and a field the fixture carries and the codec drops fails
//! (re-encode loses a key). A rename fails both ways. Every DM `type` must have a fixture and
//! every fixture must decode, so a new message type without a vector is a red test too.
//!
//! `listing.event.json` is a SIGNED kind-30402 event. It is parsed (signature verified) and
//! rebuilt; tags and content must match the fixture. Regenerate it with the ignored test at the
//! bottom when the listing layout changes on purpose.

use lnrent_wire::*;
use nostr::{Event, JsonUtil, Keys, Timestamp};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors")
}

fn read_json(name: &str) -> Value {
    let path = vectors_dir().join(name);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{}: not JSON: {e}", path.display()))
}

/// The closed set of DM `type` discriminators (docs/protocol/dm-protocol.md §2). Every fixture's
/// type must be in it and every entry must have a fixture. It is tied to the enum by
/// [`expected_type`], an exhaustive `match` over `Msg`: adding a variant without touching this
/// file is a compile error there, and the arm you add sits next to this list.
const DM_TYPES: [&str; 13] = [
    "order.request",
    "order.invoice",
    "order.error",
    "provision.ready",
    "delivery.resend.request",
    "billing.invoice",
    "billing.notice",
    "billing.refund",
    "renew.request",
    "sub.cancel",
    "op.request",
    "op.result",
    "operator.alert",
];

/// The wire `type` each variant MUST carry, spelled out here independently of `Msg::type_str`
/// so a serde rename in the crate and a stale fixture cannot agree with each other behind this
/// test's back. Exhaustive on purpose (no `_` arm): a new `Msg` variant fails to compile until it
/// is added here, and adding it here without a fixture fails the set assertion below.
fn expected_type(msg: &Msg) -> &'static str {
    match msg {
        Msg::OrderRequest(_) => DM_TYPES[0],
        Msg::OrderInvoice(_) => DM_TYPES[1],
        Msg::OrderError(_) => DM_TYPES[2],
        Msg::ProvisionReady(_) => DM_TYPES[3],
        Msg::DeliveryResendRequest(_) => DM_TYPES[4],
        Msg::BillingInvoice(_) => DM_TYPES[5],
        Msg::BillingNotice(_) => DM_TYPES[6],
        Msg::BillingRefund(_) => DM_TYPES[7],
        Msg::RenewRequest(_) => DM_TYPES[8],
        Msg::SubCancel(_) => DM_TYPES[9],
        Msg::OpRequest(_) => DM_TYPES[10],
        Msg::OpResult(_) => DM_TYPES[11],
        Msg::OperatorAlert(_) => DM_TYPES[12],
    }
}

#[test]
fn every_dm_fixture_round_trips_to_the_same_json_value() {
    let mut seen_types = BTreeSet::new();
    let mut files = 0;
    for entry in std::fs::read_dir(vectors_dir()).expect("vectors dir") {
        let path = entry.expect("dir entry").path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".json") || name == "listing.event.json" {
            continue;
        }
        files += 1;
        let fixture = read_json(&name);
        let msg: Msg = serde_json::from_value(fixture.clone())
            .unwrap_or_else(|e| panic!("{name}: codec rejects the fixture: {e}"));
        assert_eq!(
            fixture["type"].as_str(),
            Some(expected_type(&msg)),
            "{name}: `type` discriminator vs the variant it decoded to"
        );
        assert_eq!(
            msg.type_str(),
            expected_type(&msg),
            "{name}: Msg::type_str drifted from the protocol spelling"
        );
        let reencoded = serde_json::to_value(&msg).expect("encode");
        assert_eq!(
            reencoded, fixture,
            "{name}: decode->encode must reproduce the fixture exactly (no field gained, lost or renamed)"
        );
        seen_types.insert(msg.type_str());
    }
    assert!(
        files >= DM_TYPES.len(),
        "expected at least one fixture per type, found {files} files"
    );
    let expected: BTreeSet<&str> = DM_TYPES.into_iter().collect();
    assert_eq!(
        seen_types, expected,
        "every DM type needs a fixture, and no fixture may carry an unlisted type"
    );
}

/// The optional-field variants the spec calls out (dm-protocol.md §3.6, §3.7): a request-correlated
/// and an unsolicited `billing.invoice` / `billing.notice`. Pinned separately so the fixture set
/// cannot silently drop one of the two shapes.
#[test]
fn optional_request_id_variants_are_both_pinned() {
    assert!(read_json("billing.invoice.json")
        .get("request_id")
        .is_some());
    assert!(read_json("billing.invoice.auto.json")
        .get("request_id")
        .is_none());
    assert!(read_json("billing.notice.resuming.json")
        .get("request_id")
        .is_some());
    assert!(read_json("billing.notice.json").get("request_id").is_none());
    assert_eq!(read_json("op.result.ok.json")["status"], "ok");
    assert_eq!(read_json("op.result.error.json")["status"], "error");
}

/// The fixed operational key the listing vector is signed with. Test-only, publicly known.
const VECTOR_SECRET_HEX: &str = "7f3a1c5e9b2d4f60817a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70";
const VECTOR_CREATED_AT: u64 = 1_757_376_000;

fn vector_listing() -> Listing {
    let keys = Keys::parse(VECTOR_SECRET_HEX).expect("vector key");
    Listing {
        d: "do-vps".into(),
        operator: keys.public_key().to_hex(),
        recipe_id: "do-vps".into(),
        recipe_version: "0.1.0".into(),
        title: "Cloud VPS (DigitalOcean)".into(),
        summary: "A dedicated cloud VPS with root SSH. 1 vCPU / 1 GB / 25 GB.".into(),
        amount_sat: 30000,
        period: "30d".into(),
        params: vec![ParamDecl {
            key: "ssh_pubkey".into(),
            label: "Your SSH public key".into(),
            ty: "string".into(),
            required: true,
        }],
        operations: vec![
            OperationDecl {
                name: "status".into(),
                label: "VPS status".into(),
                kind: "request".into(),
                params: vec![],
            },
            OperationDecl {
                name: "restart".into(),
                label: "Reboot the VPS".into(),
                kind: "request".into(),
                params: vec![],
            },
        ],
        tier: Some("0".into()),
        version: SCHEMA_VERSION,
    }
}

#[test]
fn listing_vector_parses_and_rebuilds_identically() {
    let raw = read_json("listing.event.json");
    let event = Event::from_json(raw.to_string()).expect("fixture is a nostr event");
    let parsed = parse_listing(&event).expect("signed 30402 fixture parses (signature verified)");

    let expected = vector_listing();
    assert_eq!(parsed.listing, expected, "parsed listing fields");
    assert_eq!(
        parsed.listing_id,
        listing_coordinate(&expected.operator, "do-vps"),
        "coordinate is 30402:<signer>:<d>"
    );

    // Rebuild from the parsed listing and require the SAME tags and content the fixture carries.
    // id/sig are not compared (Schnorr signatures are randomized), so the rebuilt event is signed
    // only to obtain a concrete Event to read tags from.
    let keys = Keys::parse(VECTOR_SECRET_HEX).unwrap();
    let rebuilt = build_listing(&parsed.listing)
        .expect("build")
        .custom_created_at(Timestamp::from(VECTOR_CREATED_AT))
        .sign_with_keys(&keys)
        .expect("sign");
    let fixture_tags: Vec<Vec<String>> = event.tags.iter().map(|t| t.clone().to_vec()).collect();
    let rebuilt_tags: Vec<Vec<String>> = rebuilt.tags.iter().map(|t| t.clone().to_vec()).collect();
    assert_eq!(rebuilt_tags, fixture_tags, "tag layout and order");
    let fixture_content: Value = serde_json::from_str(&event.content).unwrap();
    let rebuilt_content: Value = serde_json::from_str(&rebuilt.content).unwrap();
    assert_eq!(rebuilt_content, fixture_content, "content JSON");
}

/// Regenerator: `cargo test -p lnrent-wire --test vectors -- --ignored --nocapture
/// generate_listing_vector` prints the signed event; paste it into `tests/vectors/listing.event.json`.
/// Run it ONLY when the listing layout changes on purpose — that is a protocol change
/// (docs/protocol/listing.md).
#[test]
#[ignore]
fn generate_listing_vector() {
    let keys = Keys::parse(VECTOR_SECRET_HEX).unwrap();
    let event = build_listing(&vector_listing())
        .unwrap()
        .custom_created_at(Timestamp::from(VECTOR_CREATED_AT))
        .sign_with_keys(&keys)
        .unwrap();
    println!("{}", event.as_pretty_json());
}
