//! End-to-end integration test for the buyer CLI (lnrent-7fp.13): the real `lnrent-buyer` binary
//! drives the full money-free buyer path against a REAL operator `Supervisor` over an in-process
//! relay + `MockPayment` + the dummy echo-creds recipe.
//!
//! It proves the bead's DONE criteria:
//!   `listings` finds the published listing -> `order create` returns a bolt11 -> the harness calls
//!   `mock.settle(...)` -> `order wait` receives provision.ready -> `ops <sub> status` round-trips,
//! and asserts the `--json` envelope + exit code on a SUCCESS and on a FAILURE (an unauthorized op
//! against an unowned subscription -> exit 6, `error.code == "unauthorized"`).
//!
//! The operator side is wired exactly as daemon/tests/supervisor.rs wires it; only the buyer half is
//! under test here, exercised as an agent would: a subprocess reading `--json` off stdout/stderr.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use nostr_relay_builder::MockRelay;
use nostr_sdk::prelude::*;
use tokio::time::timeout;

use lnrent_wire::listing_coordinate;
use lnrentd::backends::MockPayment;
use lnrentd::clock::{Clock, TestClock};
use lnrentd::ipc::{self, Reply};
use lnrentd::nostr_engine::NostrEngine;
use lnrentd::recipe::Recipe;
use lnrentd::refund_resolver::PassThroughResolver;
use lnrentd::store::Store;
use lnrentd::supervisor::{Intervals, RunningSupervisor, Supervisor};

use serde_json::Value;

/// A ceiling on the whole flow so a wiring bug fails the test instead of hanging.
const DEADLINE: Duration = Duration::from_secs(40);
/// The TestClock start (unix secs). The mock invoice expiry is `now + 1h`, so settling at `now` is
/// well inside the window.
const START: i64 = 1_000;

fn dummy_recipe() -> Recipe {
    let dir = format!("{}/../../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
    Recipe::load(&dir).expect("load dummy recipe")
}

/// FAST cadences so the operator's loops fire in milliseconds.
fn fast_intervals() -> Intervals {
    Intervals {
        reconcile: Duration::from_millis(15),
        maintenance: Duration::from_millis(10),
    }
}

fn temp_sock() -> PathBuf {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-buyer-it-{}-{n}.sock", std::process::id()))
}

fn temp_key_file() -> PathBuf {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-buyer-it-{}-{n}.nsec", std::process::id()))
}

/// Start a local `MockRelay`, retrying the upstream random-port race (see daemon's engine tests).
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

/// Build + start a supervisor with the dummy recipe and FAST intervals (mirrors supervisor.rs).
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
        Arc::new(PassThroughResolver),
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
    .expect("operator IPC never came up")
}

/// Run the `lnrent-buyer` binary with the shared flags + `args`, returning `(exit_code, envelope)`.
/// The envelope is parsed from stdout on success and stderr on failure (the `--json` contract).
async fn run_buyer(relay_url: &str, op_hex: &str, key_file: &Path, args: &[&str]) -> (i32, Value) {
    let bin = env!("CARGO_BIN_EXE_lnrent-buyer");
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--json")
        .arg("--relay")
        .arg(relay_url)
        .arg("--operator")
        .arg(op_hex)
        .arg("--key-file")
        .arg(key_file)
        .arg("--timeout")
        .arg("30");
    for a in args {
        cmd.arg(a);
    }
    let out = timeout(DEADLINE, cmd.output())
        .await
        .expect("buyer subprocess did not finish in time")
        .expect("spawn buyer binary");
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let body = if !stdout.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    let env: Value = serde_json::from_str(body).unwrap_or_else(|e| {
        panic!("buyer output is not JSON ({e}): out={stdout:?} err={stderr:?}")
    });
    (code, env)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buyer_cli_drives_full_flow_over_a_real_operator() {
    // --- operator side: relay + supervisor + mock payment + dummy recipe ---
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));

    let (running, sock) =
        start_supervisor(&op_keys, &url, store, payment.clone(), clock.clone()).await;
    wait_for_ipc_ok(&sock).await;

    // --- buyer identity: a key file the CLI loads (we know its pubkey to derive the settle key) ---
    let buyer_keys = Keys::generate();
    let buyer_hex = buyer_keys.public_key().to_hex();
    let key_file = temp_key_file();
    let nsec = buyer_keys
        .secret_key()
        .to_bech32()
        .expect("encode buyer nsec");
    std::fs::write(&key_file, format!("{nsec}\n")).expect("write buyer key file");

    let op_hex = op_keys.public_key().to_hex();
    let coord = listing_coordinate(&op_hex, "dummy");

    // 1. `listings` finds the published listing (success envelope + exit 0).
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["listings"]).await;
    assert_eq!(code, 0, "listings exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    let found = env["data"]["listings"]
        .as_array()
        .expect("listings array")
        .iter()
        .any(|l| l["listing_id"] == serde_json::json!(coord));
    assert!(found, "the published dummy listing is discovered: {env}");

    // 2. `order create` returns a bolt11 invoice (the buyer NEVER pays).
    let (code, env) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &[
            "order",
            "create",
            &coord,
            "--refund-dest",
            "refunds@example.com",
        ],
    )
    .await;
    assert_eq!(code, 0, "order create exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    let data = &env["data"];
    assert!(
        data["bolt11"]
            .as_str()
            .is_some_and(|s| s.starts_with("lnbc")),
        "order create returns a bolt11: {env}"
    );
    let order_id = data["order_id"].as_str().expect("order_id").to_string();
    let request_id = data["request_id"].as_str().expect("request_id").to_string();

    // 3. The harness settles the order out-of-band (the operator's payment concern, SPEC §4.7).
    let external_id = format!("order:{buyer_hex}:{request_id}");
    payment
        .settle(&external_id, clock.now())
        .expect("settle the order invoice");

    // 4. `order wait` receives provision.ready with the dummy recipe's delivered credential.
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["order", "wait", &order_id]).await;
    assert_eq!(code, 0, "order wait exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    assert_eq!(
        env["data"]["subscription_id"],
        serde_json::json!(order_id),
        "provision.ready is for our subscription: {env}"
    );
    assert_eq!(
        env["data"]["payload"]["credential"],
        serde_json::json!("dummy-secret-token"),
        "provision.ready carries the dummy recipe payload: {env}"
    );

    // 5. SUCCESS assertion: `ops <sub> status` round-trips (exit 0, ok envelope, op data).
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["ops", &order_id, "status"]).await;
    assert_eq!(code, 0, "ops status exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    assert_eq!(env["data"]["status"], serde_json::json!("ok"), "{env}");
    assert_eq!(
        env["data"]["data"]["state"],
        serde_json::json!("running"),
        "the status hook output round-trips: {env}"
    );

    // 6. FAILURE assertion: an op against an unowned subscription -> remote error, exit 6.
    let unowned = format!("ord:{op_hex}:not-a-real-order");
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["ops", &unowned, "status"]).await;
    assert_eq!(code, 6, "unauthorized op exits 6 (remote_error): {env}");
    assert_eq!(env["ok"], serde_json::json!(false));
    assert_eq!(
        env["error"]["code"],
        serde_json::json!("unauthorized"),
        "the operator's structured error reaches the agent unchanged: {env}"
    );

    let _ = std::fs::remove_file(&key_file);
    running.shutdown().await.unwrap();
}
