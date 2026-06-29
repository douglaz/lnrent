//! M1a end-to-end money-path TEST SUITE (lnrent-7fp.15): the ship gate.
//!
//! This proves the ALREADY-MERGED MockPayment money path runs correctly end-to-end against the
//! WIRED [`Supervisor`] (lnrent-7fp.21) — not the individual modules (those are unit-tested). It
//! adds TESTS ONLY; it must not (and does not) change any product behaviour.
//!
//! ## How the cases are driven (determinism, no hangs)
//! Each scenario boots a real [`Supervisor`] over an in-process `nostr-relay-builder` relay + a
//! [`MockPayment`] + a [`TestClock`] with FAST loop intervals (~10–15ms), mirroring
//! daemon/tests/supervisor.rs. Money-path INPUTS are injected deterministically rather than over
//! flaky relay round-trips:
//! - orders/renewals are committed by calling the SAME [`OrderIntake`] handler the supervisor wires,
//!   with a recording [`Outbound`] (the durable rows commit before the reply DM, which we ignore);
//! - settlements are pushed with [`MockPayment::settle`] onto the supervisor's `watch()` stream;
//! - time is driven with [`TestClock`]; a crash is simulated by DROPPING the [`RunningSupervisor`]
//!   and building a fresh one on the SAME [`Store`].
//!
//! Every wait is a bounded [`wait_until`] (short [`DEADLINE`], polls a store row) that FAILS FAST —
//! no unbounded awaits, no real sleeps in the hot path. Assertions read exact rows AND make the
//! negative assertions (no dup instance/outbox/refund, no resurrection, …).
//!
//! If a scenario could only pass with a product change, it is left with a `// BUG(.15): …` note and
//! reported as a SEPARATE bead — .15 is the gate, not the fix. (None were needed: the suite is green.)

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nostr_relay_builder::MockRelay;
use nostr_sdk::prelude::*;
use rusqlite::{params, OptionalExtension};
use tokio::time::timeout;

use lnrent_wire::{
    listing_coordinate, Msg, OpRequest, OpResult, OpStatus, OrderRequest, RenewRequest,
};
use lnrentd::backends::{MockPayment, PaymentBackend};
use lnrentd::clock::{Clock, TestClock};
use lnrentd::nostr_engine::{NostrEngine, OpHandler, OrderHandler, Outbound};
use lnrentd::op_dispatch::OpDispatch;
use lnrentd::order_intake::OrderIntake;
use lnrentd::recipe::Recipe;
use lnrentd::reservation::Budget;
use lnrentd::store::Store;
use lnrentd::supervisor::{Intervals, RunningSupervisor, Supervisor};

// ---- constants -------------------------------------------------------------------------------

/// A SHORT ceiling on every store-state wait. Far below the default 120s so a wiring bug fails the
/// test instead of hanging; generous enough for the provision-failure retry path (~0.5s).
const DEADLINE: Duration = Duration::from_secs(8);
/// The TestClock start (unix secs). The mock order invoice expiry is `now + 1h`, so a settlement at
/// `now` is well inside the window.
const START: i64 = 1_000;

// The dummy recipe's pricing timers, in seconds (recipes/dummy/recipe.toml: 30d / 7d / 7d).
const PERIOD: i64 = 30 * 86_400; // 2_592_000
const RENEW_LEAD: i64 = 7 * 86_400; // 604_800
const RETENTION: i64 = 7 * 86_400; // 604_800

// Derived timers for a sub provisioned from a settlement at START.
const PAID_THROUGH: i64 = START + PERIOD; // 2_593_000
const SOFT_DATE: i64 = PAID_THROUGH - RENEW_LEAD; // 1_988_200
const RETENTION_END: i64 = PAID_THROUGH + RETENTION; // 3_197_800

// ==============================================================================================
// Harness: relay, supervisor, injectors, store readers, bounded wait.
// ==============================================================================================

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

/// The injected order-intake host budget (the dummy recipe reserves 0 resources, so any room works).
fn big_budget() -> Budget {
    Budget {
        cpu: 8,
        mem_mb: 16_384,
        disk_gb: 200,
        ports: 16,
    }
}

/// A unique IPC socket path (all tests share one PID), so concurrent tests don't clobber it.
fn temp_sock() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-e2e-{}-{n}.sock", std::process::id()))
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

async fn engine_for(op_keys: &Keys, url: &str, store: Store) -> NostrEngine {
    let relays = [url.to_string()];
    NostrEngine::connect(op_keys.clone(), &relays, store)
        .await
        .expect("operator engine connects")
}

/// Build + start a supervisor over `store` with `recipe` and FAST intervals (mirrors
/// daemon/tests/supervisor.rs). The supervisor publishes its listing + runs boot recovery inside
/// `start()`, so on return the durable listing row exists and any seeded recovery has completed.
async fn start_supervisor(
    op_keys: &Keys,
    url: &str,
    store: Store,
    payment: Arc<MockPayment>,
    clock: Arc<TestClock>,
    recipe: Recipe,
) -> RunningSupervisor {
    start_supervisor_with_sock(op_keys, url, store, payment, clock, recipe)
        .await
        .0
}

async fn start_supervisor_with_sock(
    op_keys: &Keys,
    url: &str,
    store: Store,
    payment: Arc<MockPayment>,
    clock: Arc<TestClock>,
    recipe: Recipe,
) -> (RunningSupervisor, PathBuf) {
    let engine = engine_for(op_keys, url, store.clone()).await;
    let sock = temp_sock();
    let payment_for_sync = payment.clone();
    let sup = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        recipe,
        sock.clone(),
        fast_intervals(),
    )
    .await
    .expect("build supervisor");
    let sup = sup.with_payment_clock_sync(move |now| payment_for_sync.set_now(now));
    (sup.start().await.expect("start supervisor"), sock)
}

/// A stub [`Outbound`] that records every reply instead of touching a relay — the order/op handlers
/// commit their durable rows BEFORE the reply DM, so injecting work through them needs no relay.
#[derive(Default)]
struct RecordingOutbound {
    sent: Mutex<Vec<(PublicKey, Msg)>>,
}

#[async_trait]
impl Outbound for RecordingOutbound {
    async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
        self.sent.lock().unwrap().push((*recipient, msg.clone()));
        Ok(EventId::all_zeros())
    }
}

impl RecordingOutbound {
    fn messages(&self) -> Vec<(PublicKey, Msg)> {
        self.sent.lock().unwrap().clone()
    }
}

/// The operator's `30402:<op>:dummy` coordinate (the supervisor publishes the durable listing row at
/// this id on boot, and order intake matches against it).
fn coord(op_keys: &Keys) -> String {
    listing_coordinate(&op_keys.public_key().to_hex(), "dummy")
}

/// Commit a buyer order through the SAME [`OrderIntake`] the supervisor wires (PENDING sub + OPEN
/// order invoice + HELD reservation, all in one txn). Returns `(sub_id, external_id)`.
#[allow(clippy::too_many_arguments)]
async fn place_order(
    store: &Store,
    payment: &Arc<MockPayment>,
    clock: &Arc<TestClock>,
    recipe: &Recipe,
    buyer: &Keys,
    listing_id: &str,
    req_id: &str,
    refund_dest: Option<&str>,
) -> (String, String) {
    let intake = OrderIntake::new(
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
        big_budget(),
    );
    let order = Msg::OrderRequest(OrderRequest {
        id: req_id.into(),
        listing_id: listing_id.into(),
        params: serde_json::json!({}),
        refund_dest: refund_dest.map(|s| s.to_string()),
    });
    intake
        .handle(buyer.public_key(), order, &RecordingOutbound::default())
        .await
        .expect("order intake commits");
    let hex = buyer.public_key().to_hex();
    (
        format!("ord:{hex}:{req_id}"),
        format!("order:{hex}:{req_id}"),
    )
}

/// Issue a buyer renewal invoice through [`OrderIntake`] (OPEN renewal invoice committed). Returns
/// the renewal external_id `renew:req:<buyer>:<req_id>`.
async fn issue_renew(
    store: &Store,
    payment: &Arc<MockPayment>,
    clock: &Arc<TestClock>,
    recipe: &Recipe,
    buyer: &Keys,
    sub_id: &str,
    req_id: &str,
) -> String {
    let intake = OrderIntake::new(
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
        big_budget(),
    );
    let renew = Msg::RenewRequest(RenewRequest {
        id: req_id.into(),
        subscription_id: sub_id.into(),
    });
    intake
        .handle(buyer.public_key(), renew, &RecordingOutbound::default())
        .await
        .expect("renew intake commits");
    format!("renew:req:{}:{}", buyer.public_key().to_hex(), req_id)
}

/// Drive a full order -> settle -> provision against a running supervisor and return the ACTIVE
/// subscription id (the order id). The settlement is pushed onto the supervisor's `watch()` stream.
#[allow(clippy::too_many_arguments)]
async fn provision_active(
    store: &Store,
    payment: &Arc<MockPayment>,
    clock: &Arc<TestClock>,
    recipe: &Recipe,
    op_keys: &Keys,
    buyer: &Keys,
    req_id: &str,
    refund_dest: Option<&str>,
) -> String {
    let (sub_id, external_id) = place_order(
        store,
        payment,
        clock,
        recipe,
        buyer,
        &coord(op_keys),
        req_id,
        refund_dest,
    )
    .await;
    payment
        .settle(&external_id, clock.now())
        .expect("settle the order invoice");
    let sid = sub_id.clone();
    wait_until(
        store,
        &format!("{sub_id} ACTIVE"),
        |s: &Option<String>| s.as_deref() == Some("ACTIVE"),
        move |c| sub_state_blocking(c, &sid),
    )
    .await;
    sub_id
}

// ---- store readers ---------------------------------------------------------------------------

fn sub_state_blocking(c: &rusqlite::Connection, id: &str) -> Option<String> {
    c.query_row(
        "SELECT state FROM subscription WHERE id=?1",
        params![id],
        |r| r.get(0),
    )
    .optional()
    .unwrap()
}

async fn sub_state(store: &Store, id: &str) -> Option<String> {
    let id = id.to_string();
    store
        .read(move |c| Ok(sub_state_blocking(c, &id)))
        .await
        .unwrap()
}

/// `(paid_through, soft_date, next_deadline, suspend_not_before)` for a subscription.
async fn sub_times(
    store: &Store,
    id: &str,
) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
    let id = id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT paid_through, soft_date, next_deadline, suspend_not_before
                 FROM subscription WHERE id=?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?)
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
                params![id],
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
                params![id],
                |r| r.get(0),
            )?;
            let st: Option<String> = c
                .query_row(
                    "SELECT state FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready' LIMIT 1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?;
            Ok((n, st))
        })
        .await
        .unwrap()
}

async fn reservation_state(store: &Store, order_id: &str) -> Option<String> {
    let id = order_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT state FROM reservation WHERE order_id=?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

/// `(status, applied_at)` of an invoice by its external_id.
async fn invoice_status(store: &Store, external_id: &str) -> Option<(String, Option<i64>)> {
    let ext = external_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT status, applied_at FROM invoice WHERE external_id=?1",
                params![ext],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
        })
        .await
        .unwrap()
}

/// Count invoice rows for an exact external_id.
async fn invoice_count(store: &Store, external_id: &str) -> i64 {
    let ext = external_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM invoice WHERE external_id=?1",
                params![ext],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn scalar_i64(store: &Store, sql: &'static str) -> i64 {
    store
        .read(move |c| Ok(c.query_row(sql, [], |r| r.get(0))?))
        .await
        .unwrap()
}

/// Count `event_log` rows of `kind` for a subscription.
async fn event_count(store: &Store, sub_id: &str, kind: &str) -> i64 {
    let (id, kind) = (sub_id.to_string(), kind.to_string());
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM event_log WHERE subscription_id=?1 AND kind=?2",
                params![id, kind],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

/// Count `outbox` rows of `msg_type` for a subscription.
async fn outbox_count(store: &Store, sub_id: &str, msg_type: &str) -> i64 {
    let (id, mt) = (sub_id.to_string(), msg_type.to_string());
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*) FROM outbox WHERE subscription_id=?1 AND msg_type=?2",
                params![id, mt],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
}

/// `(status, count)` of the refund_attempt rows for a subscription (there is at most one per order).
async fn refund_row(store: &Store, sub_id: &str) -> (Option<String>, i64) {
    let id = sub_id.to_string();
    store
        .read(move |c| {
            let n: i64 = c.query_row(
                "SELECT count(*) FROM refund_attempt WHERE subscription_id=?1",
                params![id],
                |r| r.get(0),
            )?;
            let st: Option<String> = c
                .query_row(
                    "SELECT status FROM refund_attempt WHERE subscription_id=?1 LIMIT 1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?;
            Ok((st, n))
        })
        .await
        .unwrap()
}

/// Poll a store read until `pred` holds (or [`DEADLINE`] elapses, which FAILS the test). `project`
/// runs on the connection; `pred` decides. No real sleep in the hot path beyond the 10ms poll tick.
async fn wait_until<T, P, F>(store: &Store, label: &str, pred: P, project: F) -> T
where
    T: Send + 'static,
    P: Fn(&T) -> bool,
    F: Fn(&rusqlite::Connection) -> T + Clone + Send + 'static,
{
    let res = timeout(DEADLINE, async {
        loop {
            let project = project.clone();
            let v = store.read(move |c| Ok(project(c))).await.unwrap();
            if pred(&v) {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    res.unwrap_or_else(|_| panic!("condition `{label}` not reached within {DEADLINE:?}"))
}

/// Crash-sim stop: dropping [`RunningSupervisor`] aborts every supervised loop. Poll the IPC socket
/// until its listener is gone so the test does not advance [`TestClock`] while the old supervisor is
/// still accepting work. The other loops are aborted by the same drop path.
async fn drop_and_wait_for_ipc_down(running: RunningSupervisor, sock: PathBuf) {
    drop(running);
    timeout(DEADLINE, async {
        loop {
            match tokio::net::UnixStream::connect(&sock).await {
                Ok(_) => tokio::task::yield_now().await,
                Err(_) => return,
            }
        }
    })
    .await
    .expect("crashed supervisor IPC listener stayed up");
}

// ---- raw seed helpers (durable state for the crash/restart matrix) ----------------------------

/// Create the MockPayment invoice for `external_id` (so a later `lookup`/`settle` resolves) and
/// return the local invoice id + absolute expiry.
async fn mint_invoice(payment: &Arc<MockPayment>, external_id: &str) -> (String, i64) {
    let inv = payment
        .create_invoice(100, &format!("lnrent {external_id}"), 3600, external_id)
        .await
        .expect("mint mock invoice");
    (inv.id, inv.expires_at)
}

#[allow(clippy::too_many_arguments)]
async fn seed_subscription(
    store: &Store,
    id: &str,
    state: &str,
    buyer_hex: &str,
    paid_through: Option<i64>,
    next_deadline: Option<i64>,
    refund_dest: Option<&str>,
) {
    let (id, state, buyer, refund) = (
        id.to_string(),
        state.to_string(),
        buyer_hex.to_string(),
        refund_dest.map(|s| s.to_string()),
    );
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO subscription
                    (id, recipe_id, listing_id, buyer_pubkey, state, params_json, refund_dest,
                     period_s, renew_lead_s, retention_s, paid_through, soft_date, next_deadline,
                     created_at, updated_at)
                 VALUES (?1, 'dummy', '30402:op:dummy', ?2, ?3, '{}', ?4, ?5, ?6, ?7, ?8, NULL, ?9, 0, 0)",
                params![id, buyer, state, refund, PERIOD, RENEW_LEAD, RETENTION, paid_through, next_deadline],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn seed_invoice(
    store: &Store,
    inv_id: &str,
    sub_id: &str,
    external_id: &str,
    kind: &str,
    status: &str,
    expires_at: Option<i64>,
    settled_at: Option<i64>,
) {
    let (inv_id, sub_id, ext, kind, status) = (
        inv_id.to_string(),
        sub_id.to_string(),
        external_id.to_string(),
        kind.to_string(),
        status.to_string(),
    );
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO invoice
                    (id, subscription_id, external_id, kind, amount_sat, status, expires_at, settled_at, applied_at, issued_at)
                 VALUES (?1, ?2, ?3, ?4, 100, ?5, ?6, ?7, ?7, 0)",
                params![inv_id, sub_id, ext, kind, status, expires_at, settled_at],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_reservation(store: &Store, order_id: &str, state: &str) {
    let (order_id, state) = (order_id.to_string(), state.to_string());
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO reservation (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                 VALUES (?1, ?2, '{\"cpu\":0,\"mem_mb\":0,\"disk_gb\":0}', '{\"count\":0}', ?3, 0, 0)",
                params![format!("res-{order_id}"), order_id, state],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_instance(store: &Store, sub_id: &str) {
    let id = sub_id.to_string();
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO instance (id, subscription_id, box_id, kind, handles_json, state, created_at, updated_at)
                 VALUES (?1, ?2, 'box-0', 'dummy', '{}', 'RUNNING', 0, 0)",
                params![format!("inst:{id}"), id],
            )?;
            tx.execute(
                "UPDATE subscription SET instance_id=?2 WHERE id=?1",
                params![id, format!("inst:{id}")],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_pending_outbox(
    store: &Store,
    sub_id: &str,
    buyer_hex: &str,
    msg_type: &str,
    payload: &Msg,
) {
    let (id, buyer, mt, json) = (
        sub_id.to_string(),
        buyer_hex.to_string(),
        msg_type.to_string(),
        serde_json::to_string(payload).unwrap(),
    );
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO outbox (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, 0)",
                params![format!("outbox:{mt}:{id}"), buyer, id, mt, json],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

// ==============================================================================================
// 1. Full order -> invoice -> settle -> provision.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_order_settle_provision_reaches_active_once() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "o1",
        None,
    )
    .await;
    let external_id = format!("order:{}:o1", buyer.public_key().to_hex());

    // sub ACTIVE; order invoice PAID; reservation CONSUMED; exactly ONE instance; exactly ONE
    // provision.ready outbox row, delivered (SENT).
    assert_eq!(sub_state(&store, &sub_id).await.as_deref(), Some("ACTIVE"));
    assert_eq!(
        invoice_status(&store, &external_id)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("PAID")
    );
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("CONSUMED")
    );
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "exactly one instance"
    );
    let (n, state) = wait_until(
        &store,
        "provision.ready SENT",
        |(_, st): &(i64, Option<String>)| st.as_deref() == Some("SENT"),
        {
            let id = sub_id.clone();
            move |c| {
                let n: i64 = c
                    .query_row(
                        "SELECT count(*) FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready'",
                        params![id],
                        |r| r.get(0),
                    )
                    .unwrap();
                let st: Option<String> = c
                    .query_row(
                        "SELECT state FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready' LIMIT 1",
                        params![id],
                        |r| r.get(0),
                    )
                    .optional()
                    .unwrap();
                (n, st)
            }
        },
    )
    .await;
    assert_eq!(n, 1, "exactly one provision.ready outbox row");
    assert_eq!(state.as_deref(), Some("SENT"));

    // NEGATIVE: no refund, exactly one provision_active journal event, one order capture.
    assert_eq!(
        refund_row(&store, &sub_id).await.1,
        0,
        "no refund on the happy path"
    );
    assert_eq!(event_count(&store, &sub_id, "provision_active").await, 1);
    assert_eq!(event_count(&store, &sub_id, "capture_order").await, 1);
}

// ==============================================================================================
// 2. Renewal extend: settle renew:req:<buyer>:<id> -> paid_through += one period.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewal_extends_paid_through_by_one_period() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "r1",
        None,
    )
    .await;
    let ext = issue_renew(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &buyer,
        &sub_id,
        "rr1",
    )
    .await;
    payment.settle(&ext, clock.now()).expect("settle renewal");

    // paid_through advances by exactly one period; next_deadline == new soft_date; invoice PAID.
    let new_pt = PAID_THROUGH + PERIOD;
    let new_soft = new_pt - RENEW_LEAD;
    wait_until(
        &store,
        "paid_through extended",
        |pt: &Option<i64>| *pt == Some(new_pt),
        {
            let id = sub_id.clone();
            move |c| {
                c.query_row(
                    "SELECT paid_through FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
                .flatten()
            }
        },
    )
    .await;
    let (pt, _soft, nd, _snb) = sub_times(&store, &sub_id).await;
    assert_eq!(pt, Some(new_pt), "paid_through += one period");
    assert_eq!(nd, Some(new_soft), "next_deadline == the new soft_date");
    assert_eq!(
        invoice_status(&store, &ext)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("PAID")
    );
    assert_eq!(sub_state(&store, &sub_id).await.as_deref(), Some("ACTIVE"));

    // exactly ONE renew_extend, NO resume, NO refund.
    assert_eq!(event_count(&store, &sub_id, "renew_extend").await, 1);
    assert_eq!(event_count(&store, &sub_id, "renew_resume").await, 0);
    assert_eq!(
        refund_row(&store, &sub_id).await.1,
        0,
        "a timely renewal never refunds"
    );
}

// ==============================================================================================
// 3. Soft-date reminder: advance to soft_date -> ONE renew:auto invoice + billing DMs.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soft_date_reminder_issues_one_auto_renewal() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "s1",
        None,
    )
    .await;

    // Cross the soft_date: the reconcile loop issues exactly one renew:auto invoice and enqueues the
    // billing DMs. paid_through is UNCHANGED (only capture extends it); the cursor advances to
    // effective_suspend_at (== paid_through, uncredited).
    clock.set(SOFT_DATE);
    let auto_ext = format!("renew:auto:{sub_id}:{PAID_THROUGH}");
    wait_until(
        &store,
        "auto-renewal invoice issued",
        |present: &bool| *present,
        {
            let ext = auto_ext.clone();
            move |c| {
                let n: i64 = c
                    .query_row(
                        "SELECT count(*) FROM invoice WHERE external_id=?1",
                        params![ext],
                        |r| r.get(0),
                    )
                    .unwrap();
                n == 1
            }
        },
    )
    .await;

    let (pt, _soft, nd, _snb) = sub_times(&store, &sub_id).await;
    assert_eq!(
        pt,
        Some(PAID_THROUGH),
        "soft reminder does NOT extend paid_through"
    );
    assert_eq!(
        nd,
        Some(PAID_THROUGH),
        "cursor advanced from soft_date to paid_through"
    );
    assert_eq!(sub_state(&store, &sub_id).await.as_deref(), Some("ACTIVE"));

    // exactly ONE renew:auto invoice; billing.invoice + billing.notice enqueued; NO suspension yet.
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM invoice WHERE kind='renewal' AND external_id LIKE 'renew:auto:%'"
        )
        .await,
        1,
        "exactly one auto-renewal invoice for the cycle"
    );
    assert_eq!(outbox_count(&store, &sub_id, "billing.invoice").await, 1);
    assert_eq!(outbox_count(&store, &sub_id, "billing.notice").await, 1);
    assert_eq!(
        event_count(&store, &sub_id, "reconcile_soft_renewal").await,
        1
    );
    assert_eq!(
        event_count(&store, &sub_id, "reconcile_suspend").await,
        0,
        "no suspend at soft_date"
    );
}

// ==============================================================================================
// 4. Suspend + destroy: paid_through -> SUSPENDED; retention end -> TERMINATED + reservation freed.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpaid_sub_suspends_then_terminates() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "d1",
        None,
    )
    .await;

    // Past paid_through (unpaid): ACTIVE -> SUSPENDED (after the soft reminder fires first).
    clock.set(PAID_THROUGH);
    wait_until(
        &store,
        "SUSPENDED",
        |s: &Option<String>| s.as_deref() == Some("SUSPENDED"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;
    assert_eq!(event_count(&store, &sub_id, "reconcile_suspend").await, 1);
    assert_eq!(
        outbox_count(&store, &sub_id, "billing.notice").await,
        2,
        "soft-reminder notice + suspend notice"
    );
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "still has its instance while suspended"
    );

    // Past retention end: SUSPENDED -> TERMINATED + reservation RELEASED.
    clock.set(RETENTION_END);
    wait_until(
        &store,
        "TERMINATED",
        |s: &Option<String>| s.as_deref() == Some("TERMINATED"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;
    assert_eq!(event_count(&store, &sub_id, "reconcile_terminate").await, 1);
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("RELEASED"),
        "the capacity hold is freed on terminate"
    );
    // NEGATIVE: a non-payment lifecycle never mints a refund.
    assert_eq!(refund_row(&store, &sub_id).await.1, 0);
}

// ==============================================================================================
// 5. Refund on provision failure: failing provision hook -> REFUNDED, reservation freed, cleanup ran.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provision_failure_refunds_and_releases() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // A dummy-id recipe whose `provision` ALWAYS fails and whose `destroy` touches a marker (so we
    // can assert the best-effort cleanup ran). Service id stays "dummy" so the listing/sub match.
    let (recipe, destroy_marker) = failing_provision_recipe();
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
    )
    .await;

    let (sub_id, external_id) = place_order(
        &store,
        &payment,
        &clock,
        &recipe,
        &buyer,
        &coord(&op),
        "f1",
        Some("lnaddr@buyer"),
    )
    .await;
    payment
        .settle(&external_id, clock.now())
        .expect("settle order");

    // The maintenance loop drives provisioning (it fails 3x), marks REFUND_DUE + cleanup, then the
    // refunder pays -> REFUNDED + reservation RELEASED + billing.refund(sent).
    wait_until(
        &store,
        "REFUNDED",
        |s: &Option<String>| s.as_deref() == Some("REFUNDED"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;

    assert_eq!(
        instance_count(&store, &sub_id).await,
        0,
        "a failed provision records NO instance"
    );
    let (status, n) = refund_row(&store, &sub_id).await;
    assert_eq!(n, 1, "exactly one refund attempt");
    assert_eq!(status.as_deref(), Some("SENT"), "the refund was paid");
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("RELEASED")
    );
    assert_eq!(
        outbox_count(&store, &sub_id, "billing.refund").await,
        1,
        "exactly one billing.refund DM"
    );
    assert_eq!(
        provision_outbox(&store, &sub_id).await.0,
        0,
        "no provision.ready for a failed order"
    );
    assert!(
        destroy_marker.exists(),
        "the best-effort destroy cleanup ran"
    );
    // The order invoice is PAID (funds arrived) but no service was granted -> refunded.
    assert_eq!(
        invoice_status(&store, &external_id)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("PAID")
    );
}

// ==============================================================================================
// 6. Settled-but-terminal auto-refund: a renewal on a TERMINATED sub -> refund, no resurrection.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settlement_on_terminal_sub_refunds_without_resurrection() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    // A TERMINATED sub with an OPEN renewal invoice (minted on the backend so settle resolves).
    let sub_id = "ord:terminal:1";
    seed_subscription(
        &store,
        sub_id,
        "TERMINATED",
        &buyer_hex,
        Some(PAID_THROUGH),
        None,
        Some("lnaddr@buyer"),
    )
    .await;
    let ext = format!("renew:auto:{sub_id}:{PAID_THROUGH}");
    let (inv_id, exp) = mint_invoice(&payment, &ext).await;
    seed_invoice(
        &store,
        &inv_id,
        sub_id,
        &ext,
        "renewal",
        "OPEN",
        Some(exp),
        None,
    )
    .await;

    payment
        .settle(&ext, clock.now())
        .expect("settle the terminal renewal");

    // The settled renewal on a terminal sub refunds; the sub is NEVER resurrected.
    wait_until(
        &store,
        "refund SENT",
        |st: &Option<String>| st.as_deref() == Some("SENT"),
        {
            let id = sub_id.to_string();
            move |c| {
                c.query_row(
                    "SELECT status FROM refund_attempt WHERE subscription_id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
            }
        },
    )
    .await;
    assert_eq!(
        sub_state(&store, sub_id).await.as_deref(),
        Some("TERMINATED"),
        "no resurrection"
    );
    assert_eq!(
        refund_row(&store, sub_id).await.1,
        1,
        "exactly one refund attempt"
    );
    assert_eq!(
        instance_count(&store, sub_id).await,
        0,
        "no instance materialized"
    );
    assert_eq!(
        outbox_count(&store, sub_id, "provision.ready").await,
        0,
        "no provision for a terminal sub"
    );
    assert_eq!(
        invoice_status(&store, &ext)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("PAID")
    );
}

// ==============================================================================================
// 7. Duplicate settlement idempotency: settle a renewal TWICE -> one extension, no extra effect.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_renewal_settlement_extends_once() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "i1",
        None,
    )
    .await;
    let ext = issue_renew(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &buyer,
        &sub_id,
        "rr1",
    )
    .await;

    // Settle the SAME renewal twice (a redelivery). Capture's OPEN->PAID CAS makes the second a no-op.
    payment.settle(&ext, clock.now()).expect("first settle");
    let new_pt = PAID_THROUGH + PERIOD;
    wait_until(
        &store,
        "renewed once",
        |pt: &Option<i64>| *pt == Some(new_pt),
        {
            let id = sub_id.clone();
            move |c| {
                c.query_row(
                    "SELECT paid_through FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
                .flatten()
            }
        },
    )
    .await;
    payment
        .settle(&ext, clock.now())
        .expect("second (duplicate) settle");
    // Drive an unrelated successful order after the duplicate. Because the supervisor's settlement
    // loop is serialized, once this later settlement provisions, the duplicate settlement has already
    // had its chance to mutate the original subscription.
    let barrier_buyer = Keys::generate();
    let _barrier_sub = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &barrier_buyer,
        "dup-barrier",
        None,
    )
    .await;

    let (pt, _soft, _nd, _snb) = sub_times(&store, &sub_id).await;
    assert_eq!(
        pt,
        Some(new_pt),
        "the duplicate did not add an extra extension before the barrier order"
    );
    assert_eq!(
        invoice_count(&store, &ext).await,
        1,
        "one renewal invoice row"
    );
    let applied = invoice_status(&store, &ext).await.unwrap();
    assert_eq!(applied.0, "PAID");
    assert!(applied.1.is_some(), "exactly one applied_at, stamped once");
    assert_eq!(refund_row(&store, &sub_id).await.1, 0, "no refund");
    assert_eq!(
        event_count(&store, &sub_id, "renew_extend").await,
        1,
        "one renew_extend event; the duplicate did not re-extend"
    );
    // No second provision/outbox from the duplicate.
    assert_eq!(instance_count(&store, &sub_id).await, 1);
    assert_eq!(provision_outbox(&store, &sub_id).await.0, 1);
}

// ==============================================================================================
// 8. Late settlement (g5p): an order settlement at/after expiry -> refund, NO PROVISIONING.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_order_settlement_refunds_without_provisioning() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let (sub_id, external_id) = place_order(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &buyer,
        &coord(&op),
        "g1",
        Some("lnaddr@buyer"),
    )
    .await;
    // Read the order invoice's bolt11 expiry, then push a live watched settlement AT that expiry.
    // This exercises the supervisor's wired settlement loop and the g5p INCLUSIVE expiry gate.
    let (inv_id, expires_at) = mint_invoice(&payment, &external_id).await; // idempotent: returns the existing invoice
    assert_eq!(
        invoice_status(&store, &external_id)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("OPEN")
    );
    payment
        .settle(&external_id, expires_at)
        .expect("late watched settlement");

    // The supervisor's refunder drains the PENDING refund -> SENT. NO provisioning ever happens.
    wait_until(
        &store,
        "late refund SENT",
        |st: &Option<String>| st.as_deref() == Some("SENT"),
        {
            let id = sub_id.clone();
            move |c| {
                c.query_row(
                    "SELECT status FROM refund_attempt WHERE subscription_id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
            }
        },
    )
    .await;
    assert_ne!(
        sub_state(&store, &sub_id).await.as_deref(),
        Some("ACTIVE"),
        "the late order never activates"
    );
    assert_eq!(
        instance_count(&store, &sub_id).await,
        0,
        "no instance for a late order"
    );
    assert_eq!(
        provision_outbox(&store, &sub_id).await.0,
        0,
        "no provision.ready for a late order"
    );
    assert_eq!(refund_row(&store, &sub_id).await.1, 1, "exactly one refund");
    let (status, _applied_at) = invoice_status(&store, &external_id).await.unwrap();
    assert_eq!(status, "PAID");
    let local_inv_id: String = store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT id FROM invoice WHERE external_id=?1",
                params![external_id],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        local_inv_id, inv_id,
        "settlement matched the order invoice row created by intake"
    );
}

// ==============================================================================================
// 9. Crash/restart matrix: seed exact durable state, boot a FRESH supervisor on the same Store.
// ==============================================================================================

/// 9a. An order paid while the daemon was DOWN: settlement_catch_up captures on boot -> provisioned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_catches_up_a_settlement_missed_while_down() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // Durable state with NO daemon running: PENDING sub + OPEN order invoice + HELD reservation, and
    // the backend has marked the invoice paid (the watch() push was lost — there was no watcher).
    let sub_id = format!("ord:{buyer_hex}:c9a");
    let ext = format!("order:{buyer_hex}:c9a");
    let (inv_id, exp) = mint_invoice(&payment, &ext).await;
    seed_subscription(
        &store,
        &sub_id,
        "PENDING",
        &buyer_hex,
        None,
        Some(exp),
        None,
    )
    .await;
    seed_invoice(
        &store,
        &inv_id,
        &sub_id,
        &ext,
        "order",
        "OPEN",
        Some(exp),
        None,
    )
    .await;
    seed_reservation(&store, &sub_id, "HELD").await;
    payment
        .settle_recovered(&ext)
        .expect("mark paid at backend (RECOVERY: settled-while-down, no watcher/live ts)");

    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    wait_until(
        &store,
        "caught-up sub ACTIVE",
        |s: &Option<String>| s.as_deref() == Some("ACTIVE"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "caught up + provisioned exactly once"
    );
    assert_eq!(
        invoice_status(&store, &ext)
            .await
            .map(|(s, _)| s)
            .as_deref(),
        Some("PAID")
    );
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("CONSUMED")
    );
}

/// 9b. Crashed at PROVISIONING: boot re-drives to ACTIVE, with no duplicate effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_redrives_a_crashed_provisioning_sub() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer_hex = Keys::generate().public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // The exact durable state a crash leaves mid-handshake: a PAID order invoice (settled_at stamped)
    // + PROVISIONING sub + HELD reservation, but provision.ready NOT yet recorded.
    let sub_id = format!("ord:{buyer_hex}:c9b");
    let ext = format!("order:{buyer_hex}:c9b");
    seed_subscription(
        &store,
        &sub_id,
        "PROVISIONING",
        &buyer_hex,
        None,
        None,
        None,
    )
    .await;
    seed_invoice(
        &store,
        &format!("inv-{ext}"),
        &sub_id,
        &ext,
        "order",
        "PAID",
        Some(START + 3600),
        Some(START),
    )
    .await;
    seed_reservation(&store, &sub_id, "HELD").await;

    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    wait_until(
        &store,
        "re-driven ACTIVE",
        |s: &Option<String>| s.as_deref() == Some("ACTIVE"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "recovery provisioned exactly once"
    );
    assert_eq!(
        provision_outbox(&store, &sub_id).await.0,
        1,
        "exactly one provision.ready"
    );
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("CONSUMED")
    );
}

/// 9c. ACTIVE before the outbox was delivered: boot drains the PENDING provision.ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_drains_an_undelivered_provision_ready() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let sub_id = format!("ord:{buyer_hex}:c9c");
    seed_subscription(
        &store,
        &sub_id,
        "ACTIVE",
        &buyer_hex,
        Some(PAID_THROUGH),
        Some(SOFT_DATE),
        None,
    )
    .await;
    seed_instance(&store, &sub_id).await;
    seed_reservation(&store, &sub_id, "CONSUMED").await;
    let ready = Msg::ProvisionReady(lnrent_wire::ProvisionReady {
        subscription_id: sub_id.clone(),
        payload: serde_json::json!({"credential": "dummy-secret-token"}),
    });
    seed_pending_outbox(&store, &sub_id, &buyer_hex, "provision.ready", &ready).await;

    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let (_n, st) = wait_until(
        &store,
        "provision.ready drained to SENT",
        |(_, st): &(i64, Option<String>)| st.as_deref() == Some("SENT"),
        {
            let id = sub_id.clone();
            move |c| {
                let n: i64 = c
                    .query_row(
                        "SELECT count(*) FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready'",
                        params![id],
                        |r| r.get(0),
                    )
                    .unwrap();
                let st: Option<String> = c
                    .query_row(
                        "SELECT state FROM outbox WHERE subscription_id=?1 AND msg_type='provision.ready' LIMIT 1",
                        params![id],
                        |r| r.get(0),
                    )
                    .optional()
                    .unwrap();
                (n, st)
            }
        },
    )
    .await;
    assert_eq!(
        st.as_deref(),
        Some("SENT"),
        "boot drain delivered the undelivered DM"
    );
    assert_eq!(
        provision_outbox(&store, &sub_id).await.0,
        1,
        "no duplicate outbox row"
    );
    assert_eq!(
        instance_count(&store, &sub_id).await,
        1,
        "no duplicate instance"
    );
}

/// 9d. A PENDING refund whose key the backend already paid: recorded SENT, no double-pay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_records_an_already_paid_refund_without_double_pay() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let sub_id = format!("ord:{buyer_hex}:c9d");
    let ext = format!("order:{buyer_hex}:c9d");
    seed_subscription(
        &store,
        &sub_id,
        "REFUND_DUE",
        &buyer_hex,
        None,
        None,
        Some("lnaddr@buyer"),
    )
    .await;
    seed_reservation(&store, &sub_id, "HELD").await;
    let idem_key = format!("refund:{ext}");
    {
        let (id, sub, dest, key) = (
            format!("ref-{ext}"),
            sub_id.clone(),
            "lnaddr@buyer".to_string(),
            idem_key.clone(),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO refund_attempt
                        (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 100, ?4, 'PENDING', 0, 0, 0)",
                    params![id, sub, dest, key],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }
    // INV-3 provenance: the PAID order invoice this refund derives from (capture/provision write a
    // refund row only strictly downstream of a received payment; the seeded amount 100 matches).
    seed_invoice(
        &store,
        &format!("inv-{ext}"),
        &sub_id,
        &ext,
        "order",
        "PAID",
        None,
        Some(START),
    )
    .await;
    // The backend already paid this key before the crash (the fast-skip path must NOT pay again).
    let pre_pay_id = payment
        .pay("lnaddr@buyer", 100, &idem_key)
        .await
        .expect("pre-pay the refund key");

    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    wait_until(
        &store,
        "refund recorded SENT",
        |s: &Option<String>| s.as_deref() == Some("SENT"),
        {
            let id = sub_id.clone();
            move |c| {
                c.query_row(
                    "SELECT status FROM refund_attempt WHERE subscription_id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
            }
        },
    )
    .await;
    assert_eq!(
        sub_state(&store, &sub_id).await.as_deref(),
        Some("REFUNDED"),
        "REFUND_DUE -> REFUNDED"
    );
    assert_eq!(
        reservation_state(&store, &sub_id).await.as_deref(),
        Some("RELEASED")
    );
    // The key still maps to the SAME backend payment id (idempotent) — no second payment was made.
    assert_eq!(
        payment.pay("lnaddr@buyer", 100, &idem_key).await.unwrap(),
        pre_pay_id,
        "the refund key is idempotent: the fast-skip never double-paid"
    );
    assert_eq!(outbox_count(&store, &sub_id, "billing.refund").await, 1);
}

/// 9e. An orphaned RUNNING op_invocation: boot flips it to ERROR{interrupted}, no hook re-run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_interrupts_an_orphaned_running_op() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let sub_id = format!("ord:{buyer_hex}:c9e");
    seed_subscription(
        &store,
        &sub_id,
        "ACTIVE",
        &buyer_hex,
        Some(PAID_THROUGH),
        Some(SOFT_DATE),
        None,
    )
    .await;
    {
        let (s, sub) = (buyer_hex.clone(), sub_id.clone());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO op_invocation (sender_pubkey, request_id, subscription_id, op, state, created_at)
                     VALUES (?1, 'op-orphan', ?2, 'restart', 'RUNNING', 0)",
                    params![s, sub],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    let (state, err_code) = wait_until(
        &store,
        "orphaned op -> ERROR{interrupted}",
        |(st, _): &(Option<String>, Option<String>)| st.as_deref() == Some("ERROR"),
        move |c| {
            let row: Option<(String, Option<String>)> = c
                .query_row(
                    "SELECT state, error_json FROM op_invocation WHERE request_id='op-orphan'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .unwrap();
            match row {
                Some((st, err)) => (Some(st), err),
                None => (None, None),
            }
        },
    )
    .await;
    assert_eq!(state.as_deref(), Some("ERROR"));
    let err: lnrent_wire::WireError = serde_json::from_str(&err_code.expect("error_json")).unwrap();
    assert_eq!(err.code, "interrupted");
    assert!(!err.retryable, "an interrupted op is not retryable");
    // No DONE row was ever created (the hook never ran).
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM op_invocation WHERE state='DONE'"
        )
        .await,
        0
    );
}

// ==============================================================================================
// 10. Op path: correlated result, duplicate idempotency, auth + no existence leak.
//     Driven through the SAME OpDispatch handler the supervisor wires (a recording Outbound makes
//     the op.result + idempotency assertions deterministic without flaky relay round-trips).
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn op_path_correlates_dedups_and_guards_authorization() {
    let store = Store::open_spawn(":memory:").unwrap();
    let clock = Arc::new(TestClock::new(START));
    let buyer = Keys::generate();
    let buyer_hex = buyer.public_key().to_hex();

    // A dummy-id recipe whose `restart` op appends to a marker (so we can count hook runs) and whose
    // `status` op echoes. service.id stays "dummy" (the declared ops come from recipe.toml).
    let (recipe, restart_marker) = restart_marker_recipe();
    let sub_id = "ord:op:1";
    seed_subscription(
        &store,
        sub_id,
        "ACTIVE",
        &buyer_hex,
        Some(PAID_THROUGH),
        Some(SOFT_DATE),
        None,
    )
    .await;
    seed_instance(&store, sub_id).await;

    let dispatch = OpDispatch::new(store.clone(), clock.clone(), recipe);

    // (a) authorized `status` -> exactly one correlated op.result.ok carrying the hook data.
    let out = RecordingOutbound::default();
    dispatch
        .handle(
            buyer.public_key(),
            op_request("st-1", sub_id, "status"),
            &out,
        )
        .await
        .unwrap();
    let res = only_op_result(&out);
    assert_eq!(res.status, OpStatus::Ok);
    assert_eq!(res.request_id, "st-1");
    assert_eq!(res.subscription_id, sub_id);
    assert_eq!(res.op, "status");
    assert_eq!(
        res.data.as_ref().unwrap()["state"],
        serde_json::json!("running")
    );

    // (b) duplicate `restart` with the SAME request_id TWICE -> ONE op_invocation DONE, the SAME
    //     cached result both times, and the marker shows the hook ran EXACTLY once.
    let out = RecordingOutbound::default();
    dispatch
        .handle(
            buyer.public_key(),
            op_request("rs-dup", sub_id, "restart"),
            &out,
        )
        .await
        .unwrap();
    clock.set(START + 1000); // a re-run would stamp a new finished_at / append the marker again
    dispatch
        .handle(
            buyer.public_key(),
            op_request("rs-dup", sub_id, "restart"),
            &out,
        )
        .await
        .unwrap();
    let msgs = out.messages();
    assert_eq!(msgs.len(), 2, "both duplicate calls replied");
    assert_eq!(
        msgs[0].1, msgs[1].1,
        "the duplicate resends the identical cached op.result"
    );
    let restart_done = scalar_i64(
        &store,
        "SELECT count(*) FROM op_invocation WHERE request_id='rs-dup' AND state='DONE'",
    )
    .await;
    assert_eq!(
        restart_done, 1,
        "one DONE op_invocation for the duplicate pair"
    );
    let marker_runs = std::fs::read_to_string(&restart_marker)
        .unwrap_or_default()
        .lines()
        .count();
    assert_eq!(
        marker_runs, 1,
        "the restart hook ran exactly once (idempotent on request_id)"
    );

    // (c) the SAME op from ANOTHER buyer is `unauthorized` with NO hook run, AND a nonexistent vs an
    //     unowned subscription produce the IDENTICAL error shape (no existence leak).
    let stranger = Keys::generate();
    let out_unowned = RecordingOutbound::default();
    dispatch
        .handle(
            stranger.public_key(),
            op_request("us-1", sub_id, "status"),
            &out_unowned,
        )
        .await
        .unwrap();
    let unowned = only_op_result(&out_unowned);

    let out_missing = RecordingOutbound::default();
    dispatch
        .handle(
            stranger.public_key(),
            op_request("us-2", "ord:does-not-exist", "status"),
            &out_missing,
        )
        .await
        .unwrap();
    let missing = only_op_result(&out_missing);

    assert_eq!(unowned.status, OpStatus::Error);
    assert_eq!(missing.status, OpStatus::Error);
    assert_eq!(unowned.error.as_ref().unwrap().code, "unauthorized");
    assert_eq!(
        unowned.error, missing.error,
        "an unowned and a nonexistent sub give the IDENTICAL error (no existence leak)"
    );
    // The stranger's requests ran NO hook (no new DONE rows beyond the owner's restart).
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM op_invocation WHERE state='DONE'"
        )
        .await,
        2,
        "only the owner's status + restart are DONE; the stranger's two are ERROR"
    );
    assert_eq!(
        std::fs::read_to_string(&restart_marker)
            .unwrap_or_default()
            .lines()
            .count(),
        1,
        "no extra hook run from the unauthorized attempts"
    );
}

// ==============================================================================================
// 11. Downtime credit (.22): a credited outage raises the suspend FLOOR; the sub stays ACTIVE past
//     the old paid_through, the missed soft reminder still fires, suspension waits for the floor.
// ==============================================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downtime_credit_defers_suspension_past_the_old_paid_through() {
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    // Supervisor A provisions an ACTIVE sub and writes liveness heartbeats at START (the clock does
    // not move while A is alive).
    let (sup_a, sock_a) = start_supervisor_with_sock(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;
    let sub_id = provision_active(
        &store,
        &payment,
        &clock,
        &dummy_recipe(),
        &op,
        &buyer,
        "dc1",
        None,
    )
    .await;
    wait_until(
        &store,
        "heartbeat written",
        |hb: &Option<i64>| *hb == Some(START),
        |c| {
            c.query_row(
                "SELECT last_heartbeat FROM daemon_state WHERE rowid=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
            .flatten()
        },
    )
    .await;

    // Crash A (drop), wait until its IPC listener is gone, THEN advance the clock across the
    // renewal/suspend window (past paid_through). The outage is [START, now].
    drop_and_wait_for_ipc_down(sup_a, sock_a).await;
    let booted_at = PAID_THROUGH + 100;
    clock.set(booted_at);

    // Boot a fresh supervisor B on the SAME store: boot recovery credits the outage (raises the
    // suspend floor, leaves the pre-reminder cursor) and the catch-up reconcile fires the missed soft
    // reminder — all synchronously inside start().
    let _sup_b = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        dummy_recipe(),
    )
    .await;

    // The floor (suspend_not_before) was raised above the old paid_through.
    let snb = wait_until(
        &store,
        "suspend floor raised",
        |snb: &Option<i64>| matches!(snb, Some(v) if *v > PAID_THROUGH),
        {
            let id = sub_id.clone();
            move |c| {
                c.query_row(
                    "SELECT suspend_not_before FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap()
                .flatten()
            }
        },
    )
    .await
    .expect("a credited floor");
    assert!(
        snb > booted_at,
        "the credited floor is in the future (the sub stays serviceable)"
    );

    // The sub is STILL ACTIVE past the old paid_through, with NO reconcile_suspend, and the missed
    // soft reminder DID fire (one auto-renewal invoice + downtime_credit journal).
    assert_eq!(
        sub_state(&store, &sub_id).await.as_deref(),
        Some("ACTIVE"),
        "credited sub stays ACTIVE"
    );
    assert_eq!(
        event_count(&store, &sub_id, "reconcile_suspend").await,
        0,
        "the operator's downtime never suspends the buyer at the old paid_through"
    );
    assert_eq!(
        event_count(&store, &sub_id, "downtime_credit").await,
        1,
        "the outage was credited once"
    );
    assert_eq!(
        scalar_i64(
            &store,
            "SELECT count(*) FROM invoice WHERE kind='renewal' AND external_id LIKE 'renew:auto:%'"
        )
        .await,
        1,
        "the missed soft reminder still fired exactly one auto-renewal invoice"
    );

    // Suspension only happens once the clock reaches the CREDITED floor.
    clock.set(snb);
    wait_until(
        &store,
        "SUSPENDED after the credited floor",
        |s: &Option<String>| s.as_deref() == Some("SUSPENDED"),
        {
            let id = sub_id.clone();
            move |c| sub_state_blocking(c, &id)
        },
    )
    .await;
    assert_eq!(
        event_count(&store, &sub_id, "reconcile_suspend").await,
        1,
        "suspension fires only after the floor"
    );
}

// ==============================================================================================
// Marker-recipe + op-request test helpers.
// ==============================================================================================

/// A dummy-id recipe whose `provision` hook ALWAYS fails and whose `destroy` touches a marker. Only
/// the hook dir changes (service.id stays "dummy"), so the listing/order/sub all still match. The
/// marker proves the best-effort cleanup `destroy` ran.
fn failing_provision_recipe() -> (Recipe, PathBuf) {
    let dir = fresh_dir("lnrent-e2e-failprov");
    let marker = dir.join("destroyed");
    write_hook(
        &dir,
        "provision",
        "#!/usr/bin/env bash\ncat >/dev/null; echo boom >&2; exit 1\n",
    );
    write_hook(
        &dir,
        "destroy",
        &format!(
            "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
            marker.display()
        ),
    );
    let mut r = dummy_recipe();
    r.dir = dir;
    (r, marker)
}

/// A dummy-id recipe whose `ops/restart` hook APPENDS a line to a marker (so hook runs are
/// countable) and whose `ops/status` echoes a running state. The recipe's declared operations come
/// from the dummy recipe.toml; only the `ops/` hook bodies change.
fn restart_marker_recipe() -> (Recipe, PathBuf) {
    let dir = fresh_dir("lnrent-e2e-restartmarker");
    let ops = dir.join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    let marker = dir.join("restart-runs");
    write_op_hook(
        &ops,
        "restart",
        &format!(
            "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho run >> '{}'\necho '{{\"restarted\":true,\"state\":\"running\"}}'\n",
            marker.display()
        ),
    );
    write_op_hook(
        &ops,
        "status",
        "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho '{\"state\":\"running\"}'\n",
    );
    let mut r = dummy_recipe();
    r.dir = dir;
    (r, marker)
}

/// A fresh, EMPTY temp dir (wiping any same-PID leftovers so a stale marker can't skew assertions).
fn fresh_dir(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("{name}-{}-{n}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_hook(dir: &Path, name: &str, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn write_op_hook(ops_dir: &Path, name: &str, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    let path = ops_dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn op_request(id: &str, sub_id: &str, op: &str) -> Msg {
    Msg::OpRequest(OpRequest {
        id: id.into(),
        subscription_id: sub_id.into(),
        op: op.into(),
        params: serde_json::json!({}),
    })
}

fn only_op_result(out: &RecordingOutbound) -> OpResult {
    let mut msgs = out.messages();
    assert_eq!(msgs.len(), 1, "expected exactly one reply, got {msgs:?}");
    match msgs.pop().unwrap().1 {
        Msg::OpResult(r) => r,
        other => panic!("expected op.result, got {other:?}"),
    }
}

// ==============================================================================================
// do-vps: the WIRED supervisor provisions a REAL DigitalOcean VM end-to-end.
// #[ignore]: needs /tmp/dotoken and creates a real (billed) droplet — torn down by a Drop reaper.
// ==============================================================================================

/// `instance.handles_json` for a subscription, parsed (the provision hook's returned handles).
async fn instance_handles(store: &Store, sub_id: &str) -> serde_json::Value {
    let id = sub_id.to_string();
    let hj: Option<String> = store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT handles_json FROM instance WHERE subscription_id=?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?)
        })
        .await
        .unwrap();
    hj.and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(serde_json::Value::Null)
}

/// Destroys the droplet tagged `sub:<id>` on drop (incl. panic) so a failed test never leaks a
/// billing droplet — belt to the explicit destroy at the end of the happy path.
struct DropletReaper {
    tag: String,
    token: String,
}
impl Drop for DropletReaper {
    fn drop(&mut self) {
        let script = format!(
            r#"id=$(curl -fsS -H "Authorization: Bearer {t}" "https://api.digitalocean.com/v2/droplets?tag_name={g}" | jq -r '.droplets[0].id // empty'); [ -n "$id" ] && curl -sS -o /dev/null -X DELETE -H "Authorization: Bearer {t}" "https://api.digitalocean.com/v2/droplets/$id" || true"#,
            t = self.token,
            g = self.tag
        );
        let _ = std::process::Command::new("bash")
            .arg("-c")
            .arg(script)
            .status();
    }
}

/// `(count, id)` of droplets on DO tagged `tag`, via the API (a Command, like the recipe hooks).
fn do_droplets_by_tag(token: &str, tag: &str) -> (i64, String) {
    let script = format!(
        r#"curl -fsS -H "Authorization: Bearer {t}" "https://api.digitalocean.com/v2/droplets?tag_name={g}" | jq -r '"\(.droplets|length) \(.droplets[0].id // "")"'"#,
        t = token,
        g = tag
    );
    let out = std::process::Command::new("bash")
        .arg("-c")
        .arg(script)
        .output()
        .expect("query DO");
    let s = String::from_utf8_lossy(&out.stdout);
    let mut it = s.split_whitespace();
    let n: i64 = it.next().unwrap_or("0").parse().unwrap_or(0);
    (n, it.next().unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "creates a real DigitalOcean droplet; needs /tmp/dotoken on the env"]
async fn do_vps_full_order_provisions_a_real_droplet() {
    let token = std::fs::read_to_string("/tmp/dotoken")
        .expect("/tmp/dotoken (operator DO API token)")
        .trim()
        .to_string();
    std::env::set_var("DO_TOKEN", &token); // the supervisor-spawned provision hook inherits this
    std::env::set_var("DO_REGION", "nyc3");
    std::env::set_var("DO_SIZE", "s-1vcpu-1gb");
    std::env::set_var("DO_IMAGE", "debian-12-x64");

    let recipe = Recipe::load(format!("{}/../recipes/do-vps", env!("CARGO_MANIFEST_DIR")))
        .expect("load do-vps recipe");
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let _sup = start_supervisor(
        &op,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
    )
    .await;

    // A full order for the do-vps listing, carrying the buyer's SSH key as a param.
    let pubkey =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGPCj7pYy3w66ym0kNqJ8N5+zQg7gGpClH37rBGlVIK9 e2e";
    let req_id = "dovps-e2e";
    let intake = OrderIntake::new(
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
        big_budget(),
    );
    let order = Msg::OrderRequest(OrderRequest {
        id: req_id.into(),
        listing_id: listing_coordinate(&op.public_key().to_hex(), "do-vps"),
        params: serde_json::json!({ "ssh_pubkey": pubkey }),
        refund_dest: None,
    });
    intake
        .handle(buyer.public_key(), order, &RecordingOutbound::default())
        .await
        .expect("order intake commits");
    let hex = buyer.public_key().to_hex();
    let sub_id = format!("ord:{hex}:{req_id}");
    let external_id = format!("order:{hex}:{req_id}");
    let _reaper = DropletReaper {
        tag: format!("sub:{sub_id}"),
        token: token.clone(),
    };

    // Settle the order -> the supervisor's reconcile loop runs the do-vps provision hook, which
    // creates a REAL droplet (real time, ~60-100s). Wait up to ~180s for ACTIVE.
    payment.settle(&external_id, clock.now()).expect("settle");
    let mut active = false;
    for _ in 0..120 {
        if sub_state(&store, &sub_id).await.as_deref() == Some("ACTIVE") {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    assert!(
        active,
        "sub did not reach ACTIVE — the real DO provision failed"
    );

    // Exactly one instance, recorded with a REAL droplet_id handle.
    assert_eq!(instance_count(&store, &sub_id).await, 1, "one instance");
    let handles = instance_handles(&store, &sub_id).await;
    let droplet_id = handles
        .get("droplet_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert!(
        droplet_id.is_number() || droplet_id.is_string(),
        "instance carries a droplet_id handle, got {handles}"
    );

    // A provision.ready was delivered to the buyer (the access-details DM).
    let (n_ready, ready_state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n_ready, 1, "exactly one provision.ready");
    assert_eq!(ready_state.as_deref(), Some("SENT"), "delivery SENT");

    // The droplet REALLY exists on DigitalOcean (by tag), matching the recorded handle.
    let (n_do, do_id) = do_droplets_by_tag(&token, &format!("sub:{sub_id}"));
    assert_eq!(n_do, 1, "exactly one droplet on DO for this subscription");
    assert_eq!(
        do_id,
        droplet_id
            .as_i64()
            .map(|x| x.to_string())
            .or_else(|| droplet_id.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        "the DO droplet id matches the recorded instance handle"
    );

    println!("DO-VPS E2E PASSED: order -> settle -> supervisor provisioned real droplet {do_id}, access delivered");
    // (the DropletReaper destroys it on drop)
}

/// The stored bolt11 for an order/renewal invoice, by `external_id`.
#[cfg(feature = "fedimint")]
async fn invoice_bolt11(store: &Store, external_id: &str) -> Option<String> {
    let id = external_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT bolt11 FROM invoice WHERE external_id=?1",
                params![id],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
        })
        .await
        .unwrap()
}

// ==============================================================================================
// REAL MONEY end-to-end: a 1-sat do-vps order paid with REAL ecash from a REAL wallet on a LIVE
// federation -> the wired Supervisor provisions a REAL DigitalOcean VM. The o6p go-live, proven.
// #[ignore] + --features fedimint; needs LNRENT_REAL_INVITE + LNRENT_PAYER_WALLET + /tmp/dotoken.
// ==============================================================================================
#[cfg(feature = "fedimint")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "REAL Fedimint payment (LNRENT_REAL_INVITE + LNRENT_PAYER_WALLET) + a real DO droplet (/tmp/dotoken)"]
async fn do_vps_real_payment_provisions_a_real_vm() {
    let invite = std::env::var("LNRENT_REAL_INVITE").expect("LNRENT_REAL_INVITE");
    let wallet = std::env::var("LNRENT_PAYER_WALLET").expect("LNRENT_PAYER_WALLET");
    let cli = std::env::var("LNRENT_FEDIMINT_CLI")
        .unwrap_or_else(|_| format!("{}/bin/fedimint-cli", std::env::var("HOME").unwrap()));
    let gateway = std::env::var("LNRENT_GATEWAY").ok();
    let token = std::fs::read_to_string("/tmp/dotoken")
        .expect("/tmp/dotoken")
        .trim()
        .to_string();
    std::env::set_var("DO_TOKEN", &token);
    std::env::set_var("DO_REGION", "nyc3");
    std::env::set_var("DO_SIZE", "s-1vcpu-1gb");
    std::env::set_var("DO_IMAGE", "debian-12-x64");

    // A 1-sat do-vps (override the price so the cheap order is payable from a small wallet).
    let mut recipe = Recipe::load(format!("{}/../recipes/do-vps", env!("CARGO_MANIFEST_DIR")))
        .expect("load do-vps");
    recipe.pricing.amount_sat = 1;

    // The REAL Fedimint backend, joined to the live federation (fresh daemon client).
    let data_dir = std::env::temp_dir().join(format!("lnrent-realflow-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let fedi_secret = [43u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(lnrentd::clock::SystemClock);
    let payment: Arc<dyn PaymentBackend> = Arc::new(
        lnrentd::fedimint_backend::FedimintPayment::join_or_open(
            &invite,
            &data_dir,
            &fedi_secret,
            gateway.as_deref(),
            clock.clone(),
        )
        .await
        .expect("join the live federation"),
    );

    // The wired supervisor with the REAL backend (real clock, no mock clock-sync).
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op = Keys::generate();
    let buyer = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let engine = engine_for(&op, &url, store.clone()).await;
    let _sup = Supervisor::build(
        store.clone(),
        engine,
        payment.clone(),
        clock.clone(),
        recipe.clone(),
        temp_sock(),
        fast_intervals(),
    )
    .await
    .expect("build supervisor")
    .start()
    .await
    .expect("start supervisor");

    // Place a 1-sat do-vps order (with the ssh_pubkey param) -> a REAL Fedimint invoice is minted.
    let pubkey =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGPCj7pYy3w66ym0kNqJ8N5+zQg7gGpClH37rBGlVIK9 realflow";
    let req_id = "realflow";
    let intake = OrderIntake::new(
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe.clone(),
        big_budget(),
    );
    let order = Msg::OrderRequest(OrderRequest {
        id: req_id.into(),
        listing_id: listing_coordinate(&op.public_key().to_hex(), "do-vps"),
        params: serde_json::json!({ "ssh_pubkey": pubkey }),
        refund_dest: None,
    });
    intake
        .handle(buyer.public_key(), order, &RecordingOutbound::default())
        .await
        .expect("order intake commits");
    let hex = buyer.public_key().to_hex();
    let sub_id = format!("ord:{hex}:{req_id}");
    let external_id = format!("order:{hex}:{req_id}");
    let _reaper = DropletReaper {
        tag: format!("sub:{sub_id}"),
        token: token.clone(),
    };

    // Read the order's REAL bolt11 and pay it with the funded wallet.
    let bolt11 = invoice_bolt11(&store, &external_id)
        .await
        .expect("order invoice has a bolt11");
    eprintln!("paying the order's 1-sat invoice with the wallet: {bolt11}");
    let out = std::process::Command::new(&cli)
        .env("FM_CLIENT_DIR", &wallet)
        .args(["ln-pay", &bolt11])
        .output()
        .expect("spawn fedimint-cli ln-pay");
    assert!(
        out.status.success(),
        "wallet ln-pay failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // The daemon settles the REAL payment -> captures -> provisions a real DO VM -> delivers.
    let mut active = false;
    for _ in 0..150 {
        if sub_state(&store, &sub_id).await.as_deref() == Some("ACTIVE") {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
    assert!(active, "sub did not reach ACTIVE after the real payment");
    assert_eq!(instance_count(&store, &sub_id).await, 1, "one instance");
    let handles = instance_handles(&store, &sub_id).await;
    let droplet_id = handles
        .get("droplet_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert!(
        droplet_id.is_number() || droplet_id.is_string(),
        "instance carries a droplet_id handle: {handles}"
    );
    let (n_ready, ready_state) = provision_outbox(&store, &sub_id).await;
    assert_eq!(n_ready, 1, "one provision.ready");
    assert_eq!(ready_state.as_deref(), Some("SENT"), "delivery SENT");
    let (n_do, do_id) = do_droplets_by_tag(&token, &format!("sub:{sub_id}"));
    assert_eq!(n_do, 1, "the droplet exists on DO");

    eprintln!("REAL-MONEY E2E PASSED: 1-sat order -> wallet paid a real Fedimint invoice -> supervisor provisioned real droplet {do_id} -> access delivered");
}
