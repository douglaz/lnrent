//! Smoke integration test for the production daemon runtime (lnrent-7fp.21): the wired
//! [`Supervisor`] actually RUNS the M1a money path. Driven over an in-process
//! `nostr-relay-builder` relay + an in-memory sqlite store + a `MockPayment` + a `TestClock` + the
//! dummy echo-creds recipe, with FAST loop intervals so every assertion completes in milliseconds.
//!
//! It proves the four acceptance criteria of the bead:
//! 1. Boot — the supervisor serves IPC AND the Nostr engine concurrently and publishes its listing
//!    (durable `listing` row + a fetchable NIP-99 30402).
//! 2. Handshake — `order.request` -> `order.invoice`; `mock.settle(...)` -> the settlement loop
//!    captures and the maintenance loop drives provisioning so the buyer receives `provision.ready`.
//! 3. Supervision — a panic in a supervised loop is restarted (the loop keeps progressing) and does
//!    NOT take down the process / IPC.
//! 4. Recovery — a crash (DROP) mid-handshake leaves durable state that a fresh supervisor on the
//!    SAME store re-drives to completion, with NO lost or duplicated effect.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nostr_relay_builder::MockRelay;
use nostr_sdk::prelude::*;
use rusqlite::OptionalExtension;
use tokio::sync::watch;
use tokio::time::timeout;

use lnrent_wire::{
    gift_unwrap, gift_wrap, listing_coordinate, parse_listing, Msg, OrderRequest, ProvisionReady,
    LISTING_KIND,
};
use lnrentd::backends::{
    Invoice, MockPayment, PayStatus, PaymentBackend, PaymentStatus, Settlement,
    DEV_SETTLE_UNSUPPORTED,
};
use lnrentd::capture::{capture, Capture};
use lnrentd::clock::{Clock, TestClock};
use lnrentd::ipc::{self, Reply};
use lnrentd::nostr_engine::{NostrEngine, OrderHandler, Outbound};
use lnrentd::order_intake::OrderIntake;
use lnrentd::recipe::Recipe;
use lnrentd::refund_resolver::PassThroughResolver;
use lnrentd::reservation::Budget;
use lnrentd::store::Store;
use lnrentd::supervisor::{supervise, Backoff, Intervals, RunningSupervisor, Supervisor};

/// A 20s ceiling on every relay/loop round-trip so a wiring bug fails the test instead of hanging.
const DEADLINE: Duration = Duration::from_secs(20);
/// The TestClock start (unix secs). The mock invoice expiry is stamped at `now + 1h`, so settling
/// at `now` is always well inside the window.
const START: i64 = 1_000;

fn dummy_recipe() -> Recipe {
    let dir = format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
    Recipe::load(&dir).expect("load dummy recipe")
}

/// FAST cadences so loops fire in milliseconds and the test is deterministic.
fn fast_intervals() -> Intervals {
    Intervals {
        reconcile: Duration::from_millis(15),
        maintenance: Duration::from_millis(10),
    }
}

/// A unique IPC socket path (all tests share one PID), so concurrent tests don't clobber it.
fn temp_sock() -> PathBuf {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-sup-{}-{n}.sock", std::process::id()))
}

/// Start a local `MockRelay`, retrying the upstream random-port race (see tests/nostr_engine.rs).
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

async fn buyer_client(url: &str, keys: Keys) -> Client {
    let client = Client::new(keys);
    client.add_relay(url).await.expect("buyer add relay");
    client.connect().await;
    client.wait_for_connection(DEADLINE).await;
    client
}

struct NoopOutbound;

#[async_trait]
impl Outbound for NoopOutbound {
    async fn reply(&self, _recipient: &PublicKey, _msg: &Msg) -> Result<EventId> {
        Ok(EventId::all_zeros())
    }
}

struct EnvGuard {
    key: &'static str,
    old: Option<String>,
}

impl EnvGuard {
    fn unset(key: &'static str) -> Self {
        let old = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, old }
    }

    fn set(&self, value: &str) {
        std::env::set_var(self.key, value);
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.old {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

/// Connect a fresh operator engine to `url`.
async fn engine_for(op_keys: &Keys, url: &str, store: Store) -> NostrEngine {
    let relays = [url.to_string()];
    NostrEngine::connect(op_keys.clone(), &relays, store)
        .await
        .expect("operator engine connects")
}

/// Build + start a supervisor with the dummy recipe and FAST intervals.
async fn start_supervisor(
    op_keys: &Keys,
    url: &str,
    store: Store,
    payment: Arc<MockPayment>,
    clock: Arc<TestClock>,
) -> (RunningSupervisor, PathBuf) {
    let engine = engine_for(op_keys, url, store.clone()).await;
    let sock = temp_sock();
    let payment_for_sync = payment.clone();
    let alerts = Arc::new(lnrentd::alerts::AlertDispatcher::disabled(
        store.clone(),
        clock.clone(),
    ));
    let sup = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        Arc::new(PassThroughResolver),
        alerts,
        dummy_recipe(),
        sock.clone(),
        fast_intervals(),
        u32::MAX,
    )
    .await
    .expect("build supervisor");
    let sup = sup.with_payment_clock_sync(move |now| payment_for_sync.set_now(now));
    let running = sup.start().await.expect("start supervisor");
    (running, sock)
}

/// Poll the IPC `Status` until it answers `ok` (the accept loop binds on a spawned task).
async fn wait_for_ipc_ok(sock: &Path) -> Reply {
    timeout(DEADLINE, async {
        loop {
            if let Ok(r) = ipc::call(sock, ipc::Request::Status).await {
                if r.ok {
                    return r;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("IPC never came up")
}

// ---- store query helpers ---------------------------------------------------------------------

async fn sub_state(store: &Store, id: &str) -> Option<String> {
    let id = id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT state FROM subscription WHERE id=?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

async fn listing_row_state(store: &Store, id: &str) -> Option<String> {
    let id = id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT state FROM listing WHERE id=?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

async fn instance_count(store: &Store, sub_id: &str) -> i64 {
    let id = sub_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM instance WHERE subscription_id=?1",
                rusqlite::params![id],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

/// `(count, state)` of the durable provision.ready outbox row for a subscription.
async fn provision_outbox(store: &Store, sub_id: &str) -> (i64, Option<String>) {
    let id = sub_id.to_string();
    store
        .read(move |c| {
            let n: i64 = c.query_row(
                "SELECT count(*) FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready'",
                rusqlite::params![id],
                |r| r.get(0),
            )?;
            let st: Option<String> = c
                .query_row(
                    "SELECT state FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready' LIMIT 1",
                    rusqlite::params![id],
                    |r| r.get(0),
                )
                .optional()?;
            Ok((n, st))
        })
        .await
        .unwrap()
}

async fn invoice_status(store: &Store, external_id: &str) -> Option<String> {
    let id = external_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT status FROM invoice WHERE external_id=?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

/// Seed a durable, ACTIVE `listing` row priced to match the dummy recipe (so order intake matches).
async fn seed_listing_row(store: &Store, id: &str, recipe_id: &str, amount_sat: i64, now: i64) {
    let (id, recipe_id) = (id.to_string(), recipe_id.to_string());
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO listing
                    (id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, state, updated_at)
                 VALUES (?1, ?2, 'dummy', ?3, 2592000, 604800, 604800, 'ACTIVE', ?4)",
                rusqlite::params![id, recipe_id, amount_sat, now],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Poll until `cond` (driven by an async store read) holds, within [`DEADLINE`].
async fn wait_until<F, Fut>(label: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    timeout(DEADLINE, async {
        loop {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("condition `{label}` not reached in time"));
}

// ==============================================================================================
// 1. Boot: serves IPC AND Nostr concurrently AND publishes the listing.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boots_serving_ipc_and_nostr_and_publishes_listing() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let (running, sock) = start_supervisor(&op_keys, &url, store.clone(), payment, clock).await;

    // IPC is up (the accept loop runs concurrently with the Nostr loops).
    let status = wait_for_ipc_ok(&sock).await;
    assert!(status.ok, "IPC Status answered ok");
    assert_eq!(status.data.unwrap()["recipes"], serde_json::json!(1));

    // The durable listing row exists + is ACTIVE (order intake matches against it).
    let coord = listing_coordinate(&op_keys.public_key().to_hex(), "dummy");
    assert_eq!(
        listing_row_state(&store, &coord).await.as_deref(),
        Some("ACTIVE"),
        "durable listing row published + ACTIVE"
    );

    // The NIP-99 30402 event is fetchable from the relay and parses to the same coordinate.
    let reader = buyer_client(&url, Keys::generate()).await;
    let events = reader
        .fetch_events(
            Filter::new()
                .kind(Kind::Custom(LISTING_KIND))
                .author(op_keys.public_key()),
            DEADLINE,
        )
        .await
        .expect("fetch listing");
    let event = events.first_owned().expect("a 30402 listing was published");
    let parsed = parse_listing(&event).expect("parse fetched listing");
    assert_eq!(
        parsed.listing_id, coord,
        "the published listing coordinate matches the durable row"
    );

    running.shutdown().await.unwrap();
}

// ==============================================================================================
// 2. Full handshake: order.request -> order.invoice -> settle -> provision.ready.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_handshake_drives_settlement_through_to_provision_ready() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let (running, _sock) = start_supervisor(
        &op_keys,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
    )
    .await;

    // Buyer subscribes for replies addressed to it BEFORE sending the order.
    let buyer = buyer_client(&url, buyer_keys.clone()).await;
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
    // Let the buyer subscription AND the operator's inbound REQ register on the relay.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let coord = listing_coordinate(&op_keys.public_key().to_hex(), "dummy");
    let order = Msg::OrderRequest(OrderRequest {
        id: "req-1".into(),
        listing_id: coord,
        params: serde_json::json!({}),
        refund_dest: Some("refunds@example.com".to_string()),
    });
    let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
        .await
        .expect("buyer gift-wraps the order.request");
    buyer
        .send_event(&wrap)
        .await
        .expect("buyer publishes order.request");

    // Receive order.invoice; once seen, settle the order. Then receive provision.ready, driven by the
    // settlement loop (capture) + the maintenance loop (provision + outbox).
    let external_id = format!("order:{}:req-1", buyer_keys.public_key().to_hex());
    let collect = async {
        let mut got_invoice = false;
        let mut settled = false;
        let mut ready: Option<ProvisionReady> = None;
        while ready.is_none() {
            match notifications.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => {
                    if event.kind != Kind::GiftWrap {
                        continue;
                    }
                    if let Ok(u) = gift_unwrap(&buyer_keys, &event).await {
                        assert_eq!(
                            u.sender,
                            op_keys.public_key(),
                            "replies come from the operator"
                        );
                        match u.msg {
                            Msg::OrderInvoice(inv) => {
                                assert_eq!(inv.request_id, "req-1");
                                got_invoice = true;
                                if !settled {
                                    payment
                                        .settle(&external_id, clock.now())
                                        .expect("settle the order invoice");
                                    settled = true;
                                }
                            }
                            Msg::ProvisionReady(pr) => ready = Some(pr),
                            _ => {}
                        }
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        (got_invoice, ready)
    };

    let (got_invoice, ready) = timeout(DEADLINE, collect)
        .await
        .expect("order -> invoice -> settle -> provision.ready completed in time");
    assert!(got_invoice, "buyer received order.invoice");
    let ready = ready.expect("buyer received provision.ready");
    assert_eq!(
        ready.payload["credential"], "dummy-secret-token",
        "provision.ready carries the dummy recipe's delivery payload"
    );

    // The subscription reached ACTIVE with exactly one instance + one (sent) provision outbox row.
    let sub_id = format!("ord:{}:req-1", buyer_keys.public_key().to_hex());
    wait_until("subscription ACTIVE", || async {
        sub_state(&store, &sub_id).await.as_deref() == Some("ACTIVE")
    })
    .await;
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "exactly one instance"
    );
    wait_until("provision.ready SENT", || async {
        provision_outbox(&store, &sub_id).await.1.as_deref() == Some("SENT")
    })
    .await;
    let (n, state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n, 1, "exactly one provision.ready outbox row");
    assert_eq!(
        state.as_deref(),
        Some("SENT"),
        "the provision.ready was delivered"
    );

    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_settle_ipc_is_env_gated_and_provisions_mock_invoice() {
    let env = EnvGuard::unset("LNRENT_DEV");
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let (running, sock) = start_supervisor(
        &op_keys,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
    )
    .await;
    assert!(wait_for_ipc_ok(&sock).await.ok);

    let coord = listing_coordinate(&op_keys.public_key().to_hex(), "dummy");
    let intake = OrderIntake::new(
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
        Budget {
            cpu: 4,
            mem_mb: 4096,
            disk_gb: 100,
            ports: 16,
        },
        u32::MAX,
    );
    let order = Msg::OrderRequest(OrderRequest {
        id: "dev-1".into(),
        listing_id: coord,
        params: serde_json::json!({}),
        refund_dest: Some("refunds@example.com".to_string()),
    });
    intake
        .handle(buyer_keys.public_key(), order, &NoopOutbound)
        .await
        .expect("order intake commits an open invoice");

    let buyer_hex = buyer_keys.public_key().to_hex();
    let sub_id = format!("ord:{buyer_hex}:dev-1");
    let external_id = format!("order:{buyer_hex}:dev-1");
    assert_eq!(
        invoice_status(&store, &external_id).await.as_deref(),
        Some("OPEN")
    );

    let disabled = ipc::call(
        &sock,
        ipc::Request::DevSettle {
            subscription_id: sub_id.clone(),
        },
    )
    .await
    .expect("dev settle disabled reply");
    assert!(!disabled.ok);
    assert_eq!(
        disabled.error.as_ref().map(|e| e.code.as_str()),
        Some("dev_disabled")
    );
    assert_eq!(
        invoice_status(&store, &external_id).await.as_deref(),
        Some("OPEN"),
        "the disabled command must not settle anything"
    );

    env.set("1");
    let settled = ipc::call(
        &sock,
        ipc::Request::DevSettle {
            subscription_id: sub_id.clone(),
        },
    )
    .await
    .expect("dev settle enabled reply");
    assert!(settled.ok, "dev settle failed: {:?}", settled.error);
    let data = settled.data.expect("dev settle returns data");
    assert_eq!(data["subscription_id"], serde_json::json!(&sub_id));
    assert_eq!(data["external_id"], serde_json::json!(&external_id));
    assert_eq!(data["settled_at"], serde_json::json!(START));

    wait_until("dev settle provisions subscription", || async {
        sub_state(&store, &sub_id).await.as_deref() == Some("ACTIVE")
    })
    .await;
    assert_eq!(
        invoice_status(&store, &external_id).await.as_deref(),
        Some("PAID")
    );
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "dev-settled order provisions exactly once"
    );
    wait_until("dev settle sends provision.ready", || async {
        provision_outbox(&store, &sub_id).await.1.as_deref() == Some("SENT")
    })
    .await;
    let (n, state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n, 1, "exactly one provision.ready outbox row");
    assert_eq!(state.as_deref(), Some("SENT"));

    running.shutdown().await.unwrap();
}

/// A non-mock `PaymentBackend` that overrides none of the defaulted methods, so `dev_settle`
/// falls through to the trait default. Replaces the deleted `backends::FedimintPayment` M0 stub
/// (CUT-2): this test only ever needed *a* non-mock backend, not the real Fedimint one, and
/// `MockPayment` can't stand in (it overrides `dev_settle` to succeed).
struct NonMockBackend;

#[async_trait]
impl PaymentBackend for NonMockBackend {
    async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
        unimplemented!("NonMockBackend is a dev_settle-default fixture only")
    }
    async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
        unimplemented!()
    }
    async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
        unimplemented!()
    }
    async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
        unimplemented!()
    }
    async fn payment_status(&self, _: &str) -> Result<PayStatus> {
        unimplemented!()
    }
    async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
        unimplemented!()
    }
    async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
        unimplemented!()
    }
}

#[tokio::test]
async fn dev_settle_default_is_unsupported_for_non_mock_backend() {
    let err = NonMockBackend
        .dev_settle("external", START)
        .await
        .expect_err("non-mock backend must not support dev settle");
    assert_eq!(err.to_string(), DEV_SETTLE_UNSUPPORTED);
}

// ==============================================================================================
// 3. Supervision: a panic in a supervised loop is restarted, and the process / IPC survives.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_supervised_loop_restarts_without_killing_ipc() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // A real supervisor (its IPC loop runs under the SAME supervise() primitive as the faulty loop).
    let (running, sock) = start_supervisor(&op_keys, &url, store, payment, clock).await;
    assert!(wait_for_ipc_ok(&sock).await.ok);

    // Run a faulty long-lived loop under the supervision primitive: it PANICS on the first run, then
    // makes progress on every restart. A capped backoff keeps the restart fast.
    let runs = Arc::new(AtomicUsize::new(0));
    let (fault_tx, fault_rx) = watch::channel(false);
    let runs2 = runs.clone();
    let faulty = tokio::spawn(supervise(
        "test-faulty",
        fault_rx,
        Backoff {
            base: Duration::from_millis(10),
            max: Duration::from_millis(50),
        },
        move |mut sd| {
            let runs2 = runs2.clone();
            async move {
                let n = runs2.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    panic!("injected fault into a supervised loop");
                }
                // Progressed past the panic — idle until shutdown so we don't restart-storm.
                let _ = sd.wait_for(|s| *s).await;
                Ok(())
            }
        },
    ));

    // The loop restarted after the panic (a second run happened): the panic did NOT end supervision.
    wait_until("faulty loop restarted past its panic", || async {
        runs.load(Ordering::SeqCst) >= 2
    })
    .await;

    // And the process is still alive: the (separately supervised) IPC loop keeps answering.
    assert!(
        wait_for_ipc_ok(&sock).await.ok,
        "a panic in one supervised loop did not take down the process / IPC"
    );

    // Stop the faulty loop and the supervisor.
    let _ = fault_tx.send(true);
    let _ = faulty.await;
    running.shutdown().await.unwrap();
}

// ==============================================================================================
// 4. Recovery: a crash mid-handshake leaves durable state a fresh supervisor re-drives to
//    completion, with no lost or duplicated effect.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_recovery_redrives_provisioning_without_duplication() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let buyer_keys = Keys::generate();
    // ONE store handle shared across the simulated crash + restarts (same sole-writer actor).
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let coord = listing_coordinate(&op_keys.public_key().to_hex(), "dummy");
    seed_listing_row(&store, &coord, "dummy", 100, clock.now()).await;
    let sub_id = format!("ord:{}:rec-1", buyer_keys.public_key().to_hex());
    let external_id = format!("order:{}:rec-1", buyer_keys.public_key().to_hex());

    // --- Reproduce the durable state a crash leaves MID-HANDSHAKE: order placed + paid + captured
    //     (sub PROVISIONING) but NOT yet provisioned. Done via the public seams with no supervisor
    //     running, so there is no loop race and the interrupted point is exact. ---
    {
        let engine = engine_for(&op_keys, &url, store.clone()).await;
        let intake = OrderIntake::new(
            store.clone(),
            payment.clone(),
            clock.clone(),
            dummy_recipe(),
            Budget {
                cpu: 4,
                mem_mb: 4096,
                disk_gb: 100,
                ports: 16,
            },
            u32::MAX,
        );
        let order = Msg::OrderRequest(OrderRequest {
            id: "rec-1".into(),
            listing_id: coord.clone(),
            params: serde_json::json!({}),
            refund_dest: Some("refunds@example.com".to_string()),
        });
        // Commits the PENDING sub + OPEN invoice (the order.invoice reply publishes via the engine).
        intake
            .handle(buyer_keys.public_key(), order, &engine)
            .await
            .expect("order intake");
        // Pay + capture directly -> sub PROVISIONING (the crash point: provision.ready NOT yet sent).
        let settlement = payment.settle(&external_id, clock.now()).expect("settle");
        let cap = capture(&store, settlement, clock.now())
            .await
            .expect("capture");
        assert_eq!(cap, Capture::Captured);
        assert_eq!(
            sub_state(&store, &sub_id).await.as_deref(),
            Some("PROVISIONING")
        );
        assert_eq!(
            instance_count(&store, &sub_id).await,
            0,
            "not provisioned yet"
        );
    }

    // --- A fresh supervisor on the SAME store boots and RE-DRIVES the interrupted work to completion
    //     (boot recovery runs synchronously inside start()). ---
    let (running_a, _sock_a) = start_supervisor(
        &op_keys,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
    )
    .await;
    wait_until("recovery completes provisioning", || async {
        sub_state(&store, &sub_id).await.as_deref() == Some("ACTIVE")
    })
    .await;
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "recovery provisioned exactly once"
    );
    let (n, state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n, 1, "exactly one provision.ready outbox row");
    assert_eq!(
        state.as_deref(),
        Some("SENT"),
        "recovery delivered provision.ready"
    );

    // --- CRASH the running supervisor (drop -> abort its tasks), then a SECOND fresh supervisor on
    //     the same store must NOT duplicate any effect (idempotent boot recovery). ---
    drop(running_a);
    tokio::time::sleep(Duration::from_millis(150)).await; // let the aborts settle

    let (running_b, _sock_b) = start_supervisor(
        &op_keys,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
    )
    .await;
    // Give boot recovery + a maintenance pass time to (idempotently) run again.
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(sub_state(&store, &sub_id).await.as_deref(), Some("ACTIVE"));
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "no DOUBLE provision after the crash + restart"
    );
    let (n2, _) = provision_outbox(&store, &sub_id).await;
    assert_eq!(
        n2, 1,
        "no DOUBLE provision.ready outbox row after the crash + restart"
    );

    running_b.shutdown().await.unwrap();
}

// ==============================================================================================
// 5. GATE-1 alert sink (lnrent-urw.1): a refund that parks FAILED under a real running Supervisor
//    enqueues a durable `operator.alert` DM to the configured recipient. We assert the persisted
//    outbox row, NOT relay delivery (the in-process test relay rate-limits / clamps filter limits,
//    which can make a DM-delivery assertion silently vacuous).
// ==============================================================================================

/// Seed a PENDING `refund_attempt` with NO provenance invoice: the refunder's INV-3 execution-time
/// guard finds no matching PAID `order` invoice, so it parks the row FAILED on the first drive —
/// the simplest way to force a park-FAILED end to end without a full order→pay→provision-fail flow.
async fn seed_unprovenanced_refund(store: &Store, sub_id: &str) {
    let external_id = format!("order:{sub_id}");
    let sub_id = sub_id.to_string();
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO refund_attempt
                    (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts,
                     created_at, updated_at)
                 VALUES (?1, ?2, ?3, 500, ?4, 'PENDING', 0, 0, 0)",
                rusqlite::params![
                    format!("ref-{external_id}"),
                    sub_id,
                    "lnaddr@buyer",
                    format!("refund:{external_id}"),
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

/// Count `operator.alert` outbox rows addressed to `recipient_hex`.
async fn operator_alert_rows(store: &Store, recipient_hex: &str) -> i64 {
    let recipient = recipient_hex.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM outbox WHERE msg_type='operator.alert' AND recipient=?1",
                rusqlite::params![recipient],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refund_park_failed_enqueues_operator_alert_via_running_supervisor() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    seed_unprovenanced_refund(&store, "sub-1").await;

    // Build the Supervisor with the alert sink ENABLED, self-DM to the operator key. (The shared
    // `start_supervisor` helper disables alerts; this test needs them on.)
    let recipient_hex = op_keys.public_key().to_hex();
    let engine = engine_for(&op_keys, &url, store.clone()).await;
    let sock = temp_sock();
    let payment_for_sync = payment.clone();
    let alerts = Arc::new(lnrentd::alerts::AlertDispatcher::new(
        store.clone(),
        clock.clone(),
        recipient_hex.clone(),
    ));
    let sup = Supervisor::build(
        store.clone(),
        engine,
        payment,
        clock,
        Arc::new(PassThroughResolver),
        alerts,
        dummy_recipe(),
        sock,
        fast_intervals(),
        u32::MAX,
    )
    .await
    .expect("build supervisor");
    let sup = sup.with_payment_clock_sync(move |now| payment_for_sync.set_now(now));
    let running = sup.start().await.expect("start supervisor");

    // The maintenance loop drives the refunder, which parks the unprovenanced refund FAILED and
    // fires the alert. Poll the durable outbox row.
    let store_for_poll = store.clone();
    let recipient_for_poll = recipient_hex.clone();
    wait_until("operator.alert enqueued", || {
        let store = store_for_poll.clone();
        let recipient = recipient_for_poll.clone();
        async move { operator_alert_rows(&store, &recipient).await >= 1 }
    })
    .await;

    // Exactly one, and the refund did park FAILED.
    assert_eq!(operator_alert_rows(&store, &recipient_hex).await, 1);
    assert_eq!(
        scalar_status(&store, "SELECT status FROM refund_attempt WHERE subscription_id='sub-1'")
            .await
            .as_deref(),
        Some("FAILED"),
        "the refund parked FAILED"
    );

    running.shutdown().await.unwrap();
}

/// Read a single optional `TEXT` status column.
async fn scalar_status(store: &Store, sql: &'static str) -> Option<String> {
    store
        .read(move |c| Ok(c.query_row(sql, [], |r| r.get::<_, String>(0)).optional()?))
        .await
        .unwrap()
}

// ==============================================================================================
// 6. urw.2 teardown dead-letter: the real maintenance loop retries an owed teardown and resolves
//    it when the (dummy) destroy hook succeeds — proving the supervisor wires the reconciler's
//    retry_teardowns end to end.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_retries_and_resolves_a_teardown_dead_letter() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // Seed a dead-letter as if a prior `destroy` failed. last_attempt_at=0, so by START the backoff
    // (attempts=1 → 120s) has long elapsed → it is due on the first maintenance tick.
    lnrentd::teardown::record_failure(&store, "sub-1", "destroy", None, "boom", 0)
        .await
        .unwrap();
    assert_eq!(lnrentd::teardown::open_count(&store).await.unwrap(), 1);

    // The dummy recipe's `destroy` hook SUCCEEDS, so retry_teardowns resolves the row.
    let (running, _sock) =
        start_supervisor(&op_keys, &url, store.clone(), payment, clock).await;

    let store_for_poll = store.clone();
    wait_until("teardown dead-letter resolved", || {
        let store = store_for_poll.clone();
        async move { lnrentd::teardown::open_count(&store).await.unwrap() == 0 }
    })
    .await;

    running.shutdown().await.unwrap();
}

// ==============================================================================================
// 7. urw.5 refund actuator: `lnrent refunds` lists a parked refund, and `refund-retry` re-drives
//    it through the real refunder (resolver + capped pay) to SENT under a live Supervisor.
// ==============================================================================================

/// Seed a parked (FAILED) refund WITH INV-3 provenance (a PAID `order` invoice) and a dest the
/// PassThroughResolver + MockPayment will pay, so a retry can reach SENT.
async fn seed_parked_refund_with_provenance(store: &Store) {
    store
        .transaction(|tx| {
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, settled_at, issued_at)
                 VALUES ('inv-order:sub-x', 'sub-x', 'order:sub-x', 'order', 500, 'PAID', 10, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                 VALUES ('ref-order:sub-x', 'sub-x', 'lnaddr@buyer', 500, 'refund:order:sub-x', 'FAILED', 5, 10, 20)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn refund_status(store: &Store, id: &str) -> Option<String> {
    let id = id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT status FROM refund_attempt WHERE id=?1",
                rusqlite::params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refund_retry_redrives_a_parked_refund_to_sent() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    seed_parked_refund_with_provenance(&store).await;
    let (running, sock) = start_supervisor(&op_keys, &url, store.clone(), payment, clock).await;
    let _ = wait_for_ipc_ok(&sock).await;

    // It shows up parked.
    let list = ipc::call(&sock, ipc::Request::Refunds).await.unwrap();
    let arr = list.data.unwrap();
    assert!(
        arr.as_array().unwrap().iter().any(|r| r["id"] == "ref-order:sub-x" && r["status"] == "FAILED"),
        "the parked refund is listed"
    );

    // Retry it → the refunder re-drives it to SENT.
    let retry = ipc::call(&sock, ipc::Request::RefundRetry { id: "ref-order:sub-x".into() })
        .await
        .unwrap();
    assert!(retry.ok, "retry accepted: {:?}", retry.error);

    let store_for_poll = store.clone();
    wait_until("refund reaches SENT", || {
        let store = store_for_poll.clone();
        async move { refund_status(&store, "ref-order:sub-x").await.as_deref() == Some("SENT") }
    })
    .await;

    running.shutdown().await.unwrap();
}
