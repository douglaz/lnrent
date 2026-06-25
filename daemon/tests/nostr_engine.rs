//! Integration tests for the operator Nostr engine (lnrent-7fp.5), driven over an in-process
//! `nostr-relay-builder` relay — no public relays. They prove the transport acceptance criteria:
//! a 30402 listing publishes + is fetchable carrying the operator tag AND the recipe's declared
//! ops; an order.request -> order.invoice -> provision.ready round-trips over NIP-17; and a
//! duplicate inbound DM (same outer event id) is processed exactly once, durably across a restart.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nostr_relay_builder::builder::RateLimit;
use nostr_relay_builder::{LocalRelay, MockRelay, RelayBuilder};
use nostr_sdk::prelude::*;
use tokio::time::timeout;

use lnrent_wire::{
    gift_unwrap, gift_wrap, parse_listing, Msg, OrderInvoice, OrderRequest, ProvisionReady,
    LISTING_KIND,
};
use lnrentd::nostr_engine::{listing_from_recipe, NostrEngine, OpHandler, OrderHandler, Outbound};
use lnrentd::recipe::Recipe;
use lnrentd::store::Store;

/// A 20s ceiling on every relay round-trip so a wiring bug fails the test instead of hanging.
const DEADLINE: Duration = Duration::from_secs(20);
static HEAVY_RELAY_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn dummy_recipe() -> Recipe {
    let dir = format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
    Recipe::load(&dir).expect("load dummy recipe")
}

async fn store() -> Store {
    Store::open_spawn(":memory:").expect("open in-memory store")
}

/// Start a local `MockRelay`, retrying the upstream port race. `nostr-relay-builder` 0.44 picks a
/// random port, drops its probe listener, then re-binds — a TOCTOU window where another relay in
/// the parallel test run can grab that port first and the bind fails with `AddrInUse`. Retrying
/// picks a fresh random port, so the suite stays green under load.
async fn mock_relay() -> MockRelay {
    let mut last_err = None;
    for _ in 0..10 {
        match MockRelay::run().await {
            Ok(relay) => return relay,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!("local relay failed after retries: {}", last_err.unwrap());
}

async fn high_throughput_relay() -> (LocalRelay, String) {
    let mut last_err = None;
    for _ in 0..10 {
        let relay = LocalRelay::new(RelayBuilder::default().rate_limit(RateLimit {
            max_reqs: 2_000,
            notes_per_minute: 1_000_000,
        }));
        match relay.run().await {
            Ok(()) => {
                let url = relay.url().await.to_string();
                return (relay, url);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    panic!(
        "local high-throughput relay failed after retries: {}",
        last_err.unwrap()
    );
}

fn temp_db_path(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lnrent-{name}-{}-{nonce}.sqlite",
        std::process::id()
    ))
}

/// A buyer-side relay client connected to `url`, signing with `keys`.
async fn buyer_client(url: &str, keys: Keys) -> Client {
    let client = Client::new(keys);
    client.add_relay(url).await.expect("buyer add relay");
    client.connect().await;
    client.wait_for_connection(DEADLINE).await;
    client
}

/// Stub [`OrderHandler`] (the real one is lnrent-7fp.17): on an `order.request` it emits the
/// `order.invoice` then the `provision.ready` through the reply primitive — the flow the
/// round-trip exercises — and counts its invocations so the dedupe test can assert exactly-once.
struct StubOrderHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl OrderHandler for StubOrderHandler {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Msg::OrderRequest(req) = msg {
            out.reply(
                &sender,
                &Msg::OrderInvoice(OrderInvoice {
                    request_id: req.id.clone(),
                    order_id: "order-1".into(),
                    bolt11: "lnbcstub".into(),
                    amount_sat: 100,
                    period: "30d".into(),
                    expires_at: 0,
                }),
            )
            .await?;
            out.reply(
                &sender,
                &Msg::ProvisionReady(ProvisionReady {
                    subscription_id: "sub-1".into(),
                    payload: serde_json::json!({ "creds": "ok" }),
                }),
            )
            .await?;
        }
        Ok(())
    }
}

/// Minimal order handler for process-inbound tests that should not publish replies.
struct CountingOrderHandler {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

#[async_trait]
impl OrderHandler for CountingOrderHandler {
    async fn handle(
        &self,
        _sender: PublicKey,
        _msg: Msg,
        _out: &dyn Outbound,
    ) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            anyhow::bail!("transient handler failure");
        }
        Ok(())
    }
}

/// Order handler that sleeps inside `handle`, widening the window so the concurrency test exercises
/// the in-memory in-flight guard (the second simultaneous delivery is dropped while the first is
/// still running) rather than the durable seen-row path.
struct SlowOrderHandler {
    calls: Arc<AtomicUsize>,
    delay: Duration,
}

#[async_trait]
impl OrderHandler for SlowOrderHandler {
    async fn handle(
        &self,
        _sender: PublicKey,
        _msg: Msg,
        _out: &dyn Outbound,
    ) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}

/// Stub [`OpHandler`] (the real dispatch is lnrent-7fp.20): this bead only routes to it; the stub
/// just records that it was reached.
struct StubOpHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl OpHandler for StubOpHandler {
    async fn handle(
        &self,
        _sender: PublicKey,
        _msg: Msg,
        _out: &dyn Outbound,
    ) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Acceptance: a NIP-99 30402 listing publishes and is fetchable from a (local) relay, carrying the
// operator tag AND the recipe's declared management operations (§5.4, §7.4).
#[tokio::test]
async fn listing_publishes_and_is_fetchable_with_operator_tag_and_ops() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let listing = listing_from_recipe(&dummy_recipe(), "dummy-1", op_keys.public_key().to_hex());
    engine
        .publish_listing(&listing)
        .await
        .expect("publish listing");

    // A reader fetches the 30402 back from the relay and parses it with the shared codec.
    let reader = buyer_client(&url, Keys::generate()).await;
    let filter = Filter::new()
        .kind(Kind::Custom(LISTING_KIND))
        .author(op_keys.public_key());
    let events = reader
        .fetch_events(filter, DEADLINE)
        .await
        .expect("fetch listing");
    let event = events.first_owned().expect("a listing was published");

    let parsed = parse_listing(&event).expect("parse fetched listing");
    assert_eq!(
        parsed.listing.operator,
        op_keys.public_key().to_hex(),
        "the listing carries the operator tag"
    );
    let ops: Vec<&str> = parsed
        .listing
        .operations
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert!(
        ops.contains(&"status") && ops.contains(&"restart"),
        "the listing carries the recipe's declared ops, got {ops:?}"
    );
    assert_eq!(
        parsed.listing_id,
        format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
        "listing_id is the addressable coordinate"
    );
}

// Acceptance: order.request -> order.invoice -> provision.ready round-trips over NIP-17 on a local
// relay, driven by a stub order handler that emits the invoice then the ready.
#[tokio::test]
async fn order_request_invoice_ready_round_trips_over_nip17() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();

    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let order_calls = Arc::new(AtomicUsize::new(0));
    let op_calls = Arc::new(AtomicUsize::new(0));
    let order_handler: Arc<dyn OrderHandler> = Arc::new(StubOrderHandler {
        calls: order_calls.clone(),
    });
    let op_handler: Arc<dyn OpHandler> = Arc::new(StubOpHandler { calls: op_calls });

    // The operator's inbound loop subscribes + decodes + routes on its own task.
    let inbound = engine.clone();
    tokio::spawn(async move {
        let _ = inbound.run_inbound(order_handler, op_handler).await;
    });

    let buyer = buyer_client(&url, buyer_keys.clone()).await;
    // Capture the notification stream BEFORE any reply can exist, then subscribe to replies
    // addressed to the buyer.
    let mut notifications = buyer.notifications();
    buyer
        .subscribe(
            Filter::new()
                .kind(Kind::GiftWrap)
                .pubkey(buyer_keys.public_key()),
            None,
        )
        .await
        .expect("buyer subscribes for replies");
    // Let both subscriptions register on the relay before the order is sent.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let order = Msg::OrderRequest(OrderRequest {
        id: "req-1".into(),
        listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("buyer gift-wraps the order.request");
    buyer
        .send_event(&wrap)
        .await
        .expect("buyer publishes the order.request");

    // Collect the two replies (in either delivery order) by unwrapping each inbound gift wrap.
    let collect = async {
        let mut got_invoice = false;
        let mut got_ready = false;
        while !(got_invoice && got_ready) {
            match notifications.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => {
                    if event.kind != Kind::GiftWrap {
                        continue;
                    }
                    if let Ok(unwrapped) = gift_unwrap(&buyer_keys, &event).await {
                        match unwrapped.msg {
                            Msg::OrderInvoice(inv) => {
                                assert_eq!(
                                    inv.request_id, "req-1",
                                    "invoice correlates to the request"
                                );
                                assert_eq!(
                                    unwrapped.sender,
                                    op_keys.public_key(),
                                    "reply is from the operator"
                                );
                                got_invoice = true;
                            }
                            Msg::ProvisionReady(_) => got_ready = true,
                            _ => {}
                        }
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        (got_invoice, got_ready)
    };

    let (got_invoice, got_ready) = timeout(DEADLINE, collect)
        .await
        .expect("order.request -> order.invoice -> provision.ready completes in time");
    assert!(got_invoice, "the buyer received the order.invoice");
    assert!(got_ready, "the buyer received the provision.ready");
    assert_eq!(
        order_calls.load(Ordering::SeqCst),
        1,
        "the order handler ran once for one request"
    );
}

#[tokio::test]
async fn inbound_startup_backfill_pages_past_relay_limit_to_old_valid_wrap() {
    const GARBAGE: usize = 600;
    const RELAY_DEFAULT_FILTER_LIMIT: usize = 500;

    let _heavy = HEAVY_RELAY_TEST.lock().await;
    let (_relay, url) = high_throughput_relay().await;

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");
    let buyer = buyer_client(&url, buyer_keys.clone()).await;

    let order = Msg::OrderRequest(OrderRequest {
        id: "startup-buried-valid-order".into(),
        listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let valid = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("gift-wrap the valid order.request");
    buyer
        .send_event(&valid)
        .await
        .expect("buyer publishes the valid order.request");

    let garbage_signer = Keys::generate();
    let garbage_created_at = valid.created_at + 1;
    for i in 0..GARBAGE {
        let wrap = EventBuilder::new(Kind::GiftWrap, format!("garbage-not-a-nip44-payload-{i}"))
            .tag(Tag::public_key(op_keys.public_key()))
            .custom_created_at(garbage_created_at)
            .sign_with_keys(&garbage_signer)
            .expect("sign garbage gift wrap");
        let output = buyer
            .send_event(&wrap)
            .await
            .expect("buyer publishes garbage gift wrap");
        assert!(
            !output.success.is_empty(),
            "garbage wrap {i} was not accepted by the relay: {:?}",
            output.failed
        );
    }

    let retained = buyer
        .fetch_events(
            Filter::new()
                .kind(Kind::GiftWrap)
                .pubkey(op_keys.public_key())
                .limit(GARBAGE * 2),
            DEADLINE,
        )
        .await
        .expect("count retained inbound wraps");
    assert!(
        retained.len() > RELAY_DEFAULT_FILTER_LIMIT,
        "test setup must retain more than the relay default limit"
    );

    let calls = Arc::new(AtomicUsize::new(0));
    let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
        calls: Arc::clone(&calls),
        fail: false,
    });
    let op_handler: Arc<dyn OpHandler> = Arc::new(StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    });

    let inbound = engine.clone();
    let handle = tokio::spawn(async move {
        let _ = inbound.run_inbound(order_handler, op_handler).await;
    });

    timeout(DEADLINE, async {
        loop {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("startup paged backfill routed the old valid wrap buried behind > limit garbage");

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly the buried valid wrap routes; same-timestamp garbage is ignored"
    );

    handle.abort();
    let _ = handle.await;
}

// Acceptance: a duplicate inbound DM (same outer event id) is processed EXACTLY once — and the
// dedupe is durable, so a fresh engine on the same store still drops the duplicate after a restart.
#[tokio::test]
async fn duplicate_inbound_dm_processed_exactly_once_durably() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let path = temp_db_path("nostr-dedupe");
    let store = Store::open_spawn(&path).expect("open file-backed store");
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store.clone())
        .await
        .expect("operator engine connects");

    let order_calls = Arc::new(AtomicUsize::new(0));
    let order_handler = StubOrderHandler {
        calls: order_calls.clone(),
    };
    let op_handler = StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };

    // One inbound order.request, gift-wrapped to the operator. The SAME wrap re-delivered shares
    // the outer event id, which is the transport dedupe key.
    let order = Msg::OrderRequest(OrderRequest {
        id: "req-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("gift-wrap the order.request");

    // First delivery is new and routes; the second (same event id) is deduped.
    let first = engine
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await
        .expect("first delivery");
    assert!(first, "the first delivery is new and routes");
    let second = engine
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await
        .expect("second delivery");
    assert!(!second, "a re-delivery of the same wrap is deduped");
    assert_eq!(
        order_calls.load(Ordering::SeqCst),
        1,
        "the handler ran exactly once"
    );

    // Durable across a restart: re-open the same sqlite file with a fresh store actor/engine.
    drop(engine);
    drop(store);
    let store2 = Store::open_spawn(&path).expect("re-open file-backed store");
    let engine2 = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store2)
        .await
        .expect("a restarted engine connects on the same store");
    let after_restart = engine2
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await
        .expect("post-restart delivery");
    assert!(
        !after_restart,
        "the durable seen_message row survives a restart"
    );
    assert_eq!(
        order_calls.load(Ordering::SeqCst),
        1,
        "still exactly once after restart"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

// A handler error writes NO `seen_message` row, so the same wrap re-routes on the next delivery
// (relay redelivery or buyer resend) — the wrap is "burned" only after a handler durably commits.
#[tokio::test]
async fn handler_error_leaves_wrap_unmarked_for_retry() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let calls = Arc::new(AtomicUsize::new(0));
    let failing = CountingOrderHandler {
        calls: calls.clone(),
        fail: true,
    };
    let succeeding = CountingOrderHandler {
        calls: calls.clone(),
        fail: false,
    };
    let op_handler = StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };

    let order = Msg::OrderRequest(OrderRequest {
        id: "retry-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("gift-wrap the order.request");

    let first = engine.process_inbound(&wrap, &failing, &op_handler).await;
    assert!(first.is_err(), "the first handler attempt fails");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the failing handler was called once"
    );

    let retry = engine
        .process_inbound(&wrap, &succeeding, &op_handler)
        .await
        .expect("retry after transient handler failure");
    assert!(
        retry,
        "the same wrap re-routes because the failed attempt left it unmarked"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the retry reached the handler"
    );
}

// P2 (lnrent-7fp.5): a wrap the operator can't decode (forged/garbage, or sealed to someone else)
// fails to decode ONCE; the bounded in-memory negative cache then short-circuits the redelivery
// before re-running the NIP-44 decrypt — so a reconnect/lag backfill that re-fetches the retained
// garbage set doesn't re-decrypt it every time. The Err -> Ok(false) transition is the observable
// proof the decode was skipped on the second pass.
#[tokio::test]
async fn undecodable_wrap_is_decoded_once_then_negative_cached() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let stranger_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let calls = Arc::new(AtomicUsize::new(0));
    let order_handler = CountingOrderHandler {
        calls: calls.clone(),
        fail: false,
    };
    let op_handler = StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };

    // A kind-1059 gift wrap the operator key cannot NIP-44 decrypt: it is sealed to a third party,
    // not the operator, so `gift_unwrap` errors regardless of the inner payload.
    let order = Msg::OrderRequest(OrderRequest {
        id: "garbage-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = gift_wrap(&buyer_keys, &stranger_keys.public_key(), &order)
        .await
        .expect("gift-wrap to a third party");

    // First delivery: the decode is attempted and fails (surfaced as an error, logged once).
    let first = engine
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await;
    assert!(
        first.is_err(),
        "an undecodable wrap fails to decode on first sight"
    );

    // Redelivery: the negative cache short-circuits BEFORE re-decoding — no error this time, just a
    // routed=false drop. The Err -> Ok(false) difference proves the NIP-44 decrypt was skipped.
    let second = engine
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await
        .expect("the cached non-routable wrap short-circuits without re-decoding");
    assert!(
        !second,
        "the redelivery is dropped as non-routable, not re-decoded"
    );

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an undecodable wrap never reaches a handler"
    );
}

// P2 (review round 7): a malformed relay copy can carry the same event fields/id with a bogus
// signature, because Nostr ids do not commit to `sig`. The engine must reject it before using the
// id for in-flight or negative-cache suppression, so the valid copy from another relay still routes.
#[tokio::test]
async fn malformed_duplicate_signature_does_not_poison_valid_wrap_id() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let stranger_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let calls = Arc::new(AtomicUsize::new(0));
    let order_handler = CountingOrderHandler {
        calls: calls.clone(),
        fail: false,
    };
    let op_handler = StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };

    let order = Msg::OrderRequest(OrderRequest {
        id: "sig-poison-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let valid = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("gift-wrap the order.request");
    let other = gift_wrap(&stranger_keys, &op_keys.public_key(), &order)
        .await
        .expect("make an unrelated signature");
    let malformed = Event::new(
        valid.id,
        valid.pubkey,
        valid.created_at,
        valid.kind,
        valid.tags.clone().to_vec(),
        valid.content.clone(),
        other.sig,
    );
    assert_eq!(malformed.id, valid.id);
    assert!(
        malformed.verify().is_err(),
        "the malformed copy has the same id but an invalid signature"
    );

    let first = engine
        .process_inbound(&malformed, &order_handler, &op_handler)
        .await;
    assert!(
        first.is_err(),
        "the malformed copy is rejected before suppression"
    );

    let second = engine
        .process_inbound(&valid, &order_handler, &op_handler)
        .await
        .expect("valid copy after malformed duplicate");
    assert!(second, "the valid same-id wrap still routes");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the valid copy reached the handler"
    );
}

// Pruning rides the post-success seen-write: routing a fresh DM marks it seen AND drops rows past
// the retention window in the same transaction, so the dedupe table stays bounded.
#[tokio::test]
async fn seen_message_prunes_old_rows_on_mark() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let store = store().await;
    store
        .transaction(|tx| {
            tx.execute(
                "INSERT INTO seen_message (event_id, sender, msg_type, seen_at)
                 VALUES ('old-wrap', 'sender', 'order.request', 0)",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("insert old seen row");

    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store.clone())
        .await
        .expect("operator engine connects");
    let calls = Arc::new(AtomicUsize::new(0));
    let order_handler = CountingOrderHandler {
        calls: calls.clone(),
        fail: false,
    };
    let op_handler = StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    };

    let order = Msg::OrderRequest(OrderRequest {
        id: "prune-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("gift-wrap the order.request");

    let routed = engine
        .process_inbound(&wrap, &order_handler, &op_handler)
        .await
        .expect("new delivery routes");
    assert!(routed);

    let old_rows: i64 = store
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT count(*) FROM seen_message WHERE event_id = 'old-wrap'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .expect("count old seen rows");
    assert_eq!(old_rows, 0, "stale transport dedupe rows are pruned");
}

// The multi-relay fan-out delivers the SAME wrap once per relay, near-simultaneously. The
// in-memory in-flight guard must collapse those concurrent copies to a single handler run (the
// handler may be non-idempotent), with exactly one delivery routing and the others dropped.
#[tokio::test]
async fn concurrent_duplicate_deliveries_run_handler_once() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();

    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store().await)
        .await
        .expect("operator engine connects");

    let calls = Arc::new(AtomicUsize::new(0));
    let order_handler: Arc<dyn OrderHandler> = Arc::new(SlowOrderHandler {
        calls: calls.clone(),
        delay: Duration::from_millis(300),
    });
    let op_handler: Arc<dyn OpHandler> = Arc::new(StubOpHandler {
        calls: Arc::new(AtomicUsize::new(0)),
    });

    let order = Msg::OrderRequest(OrderRequest {
        id: "concurrent-1".into(),
        listing_id: "30402:op:dummy-1".into(),
        params: serde_json::json!({}),
        refund_dest: None,
    });
    let wrap = Arc::new(
        gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
            .await
            .expect("gift-wrap the order.request"),
    );

    // Two tasks process the identical wrap (same outer event id) at the same time.
    let mut handles = Vec::new();
    for _ in 0..2 {
        let engine = engine.clone();
        let wrap = Arc::clone(&wrap);
        let order_handler = Arc::clone(&order_handler);
        let op_handler = Arc::clone(&op_handler);
        handles.push(tokio::spawn(async move {
            engine
                .process_inbound(wrap.as_ref(), order_handler.as_ref(), op_handler.as_ref())
                .await
                .expect("process inbound")
        }));
    }
    let mut routed = 0;
    for h in handles {
        if h.await.unwrap() {
            routed += 1;
        }
    }

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the handler ran exactly once for concurrent duplicate deliveries"
    );
    assert_eq!(
        routed, 1,
        "exactly one delivery routed; the concurrent duplicate was dropped"
    );
}
