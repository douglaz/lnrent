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

use nostr_relay_builder::MockRelay;
use nostr_sdk::prelude::*;
use rusqlite::OptionalExtension;
use tokio::sync::watch;
use tokio::time::timeout;

use lnrent_wire::{
    gift_unwrap, gift_wrap, listing_coordinate, parse_listing, Msg, OrderRequest, ProvisionReady,
    LISTING_KIND,
};
use lnrentd::backends::MockPayment;
use lnrentd::capture::{capture, Capture};
use lnrentd::clock::{Clock, TestClock};
use lnrentd::ipc::{self, Reply};
use lnrentd::nostr_engine::{NostrEngine, OrderHandler};
use lnrentd::order_intake::OrderIntake;
use lnrentd::recipe::Recipe;
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
    let sup = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        dummy_recipe(),
        sock.clone(),
        fast_intervals(),
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
        refund_dest: None,
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
    let (n, state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n, 1, "exactly one provision.ready outbox row");
    assert_eq!(
        state.as_deref(),
        Some("SENT"),
        "the provision.ready was delivered"
    );

    running.shutdown().await.unwrap();
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
        );
        let order = Msg::OrderRequest(OrderRequest {
            id: "rec-1".into(),
            listing_id: coord.clone(),
            params: serde_json::json!({}),
            refund_dest: None,
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
