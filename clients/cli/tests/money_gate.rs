//! M1a money-gate CLI golden path (lnrent-7fp.15, scenario 12): the REAL `lnrent-buyer` subprocess
//! drives the whole agent flow against a live operator [`Supervisor`] over an in-process relay +
//! [`MockPayment`] + a restart-marker dummy recipe.
//!
//! ONE flow (the CLI is slow — the exhaustive cases live in daemon/tests/e2e_money_path.rs against
//! the daemon seams):
//!   listings -> order create --request-id -> (harness mock.settle) -> order wait (provision.ready)
//!   -> renew --request-id -> (harness mock.settle the renewal) -> ops status -> duplicate
//!   `ops restart --request-id` TWICE (proven ONE effect via the op hook's marker) -> an unauthorized
//!   op (exit 6, `error.code == "unauthorized"`).
//!
//! The operator is wired exactly as daemon/tests/supervisor.rs wires it; only the buyer half is under
//! test, exercised as an agent would — a subprocess reading the `--json` envelope off stdout/stderr.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
use lnrentd::store::Store;
use lnrentd::supervisor::{Intervals, RunningSupervisor, Supervisor};

use serde_json::Value;

/// A ceiling on the whole flow so a wiring bug fails the test instead of hanging.
const DEADLINE: Duration = Duration::from_secs(40);
/// The operator TestClock start (unix secs). The mock order invoice expiry is `now + 1h`, so settling
/// at `now` is well inside the window.
const START: i64 = 1_000;

/// A dummy-id recipe whose `ops/restart` hook APPENDS a line to a marker file (so we can prove the
/// hook ran exactly once across a duplicate request) and which otherwise provisions instantly like
/// the shipped dummy recipe. Only the hook dir changes — the metadata (service id "dummy", pricing,
/// declared ops) is loaded from the real recipe, so the published listing/coordinate are unchanged.
fn restart_marker_recipe() -> (Recipe, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("lnrent-money-gate-{}-{n}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(dir.join("ops")).unwrap();
    let marker = dir.join("restart-runs");

    let write = |rel: &str, body: String| {
        let path = dir.join(rel);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    // provision returns the dummy credential payload (so `order wait` sees it).
    write(
        "provision",
        "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho '{\"payload\":{\"credential\":\"dummy-secret-token\"},\"handles\":{\"instance\":\"dummy-1\"}}'\n".into(),
    );
    for h in ["destroy", "suspend", "resume"] {
        write(
            h,
            "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho '{\"ok\":true}'\n".into(),
        );
    }
    write("healthcheck", "#!/usr/bin/env bash\nexit 0\n".into());
    write(
        "ops/status",
        "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho '{\"state\":\"running\"}'\n"
            .into(),
    );
    // restart counts its runs in the marker, so a duplicate request_id that re-ran the hook would show 2.
    write(
        "ops/restart",
        format!(
            "#!/usr/bin/env bash\nset -euo pipefail\ncat >/dev/null\necho run >> '{}'\necho '{{\"restarted\":true,\"state\":\"running\"}}'\n",
            marker.display()
        ),
    );

    let mut recipe = Recipe::load(format!(
        "{}/../../recipes/dummy",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("load dummy recipe metadata");
    recipe.dir = dir;
    (recipe, marker)
}

fn fast_intervals() -> Intervals {
    Intervals {
        reconcile: Duration::from_millis(15),
        maintenance: Duration::from_millis(10),
    }
}

fn temp_sock() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-money-gate-{}-{n}.sock", std::process::id()))
}

fn temp_key_file() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("lnrent-money-gate-{}-{n}.nsec", std::process::id()))
}

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

async fn start_supervisor(
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

async fn wait_for_op_done(store: &Store, sender_hex: &str, request_id: &str) -> Value {
    let sender = sender_hex.to_string();
    let request = request_id.to_string();
    timeout(DEADLINE, async {
        loop {
            let s = sender.clone();
            let r = request.clone();
            if let Some(result_json) = store
                .read(move |c| {
                    let mut stmt = c.prepare(
                        "SELECT result_json FROM op_invocation
                          WHERE sender_pubkey=?1 AND request_id=?2 AND state='DONE'",
                    )?;
                    let mut rows = stmt.query([s.as_str(), r.as_str()])?;
                    Ok(match rows.next()? {
                        Some(row) => row.get::<_, Option<String>>(0)?,
                        None => None,
                    })
                })
                .await
                .unwrap()
            {
                return serde_json::from_str(&result_json).expect("op result_json is valid JSON");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("op {request_id} did not reach DONE within {DEADLINE:?}"))
}

/// Run `lnrent-buyer` with the shared flags + `args`, returning `(exit_code, envelope)`. The envelope
/// is parsed from stdout on success and stderr on failure (the `--json` contract).
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
async fn buyer_cli_money_gate_golden_path() {
    // --- operator side: relay + supervisor + mock payment + restart-marker dummy recipe ---
    let relay = mock_relay().await;
    let url = relay.url().await.to_string();
    let op_keys = Keys::generate();
    let store = Store::open_spawn(":memory:").unwrap();
    let payment = Arc::new(MockPayment::new());
    payment.set_now(START);
    let clock = Arc::new(TestClock::new(START));
    let (recipe, restart_marker) = restart_marker_recipe();

    let (running, sock) = start_supervisor(
        &op_keys,
        &url,
        store.clone(),
        payment.clone(),
        clock.clone(),
        recipe,
    )
    .await;
    wait_for_ipc_ok(&sock).await;

    // --- buyer identity: a key file the CLI loads (we know its pubkey to derive the settle keys) ---
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

    // 1. `listings` discovers the published dummy listing.
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["listings"]).await;
    assert_eq!(code, 0, "listings exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    let found = env["data"]["listings"]
        .as_array()
        .expect("listings array")
        .iter()
        .any(|l| l["listing_id"] == serde_json::json!(coord));
    assert!(found, "the published dummy listing is discovered: {env}");

    // 2. `order create --request-id ord1` returns a bolt11 (the buyer NEVER pays).
    let (code, env) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &["order", "create", &coord, "--request-id", "ord1"],
    )
    .await;
    assert_eq!(code, 0, "order create exit 0: {env}");
    assert_eq!(env["ok"], serde_json::json!(true));
    assert_eq!(
        env["data"]["request_id"],
        serde_json::json!("ord1"),
        "the fixed request id is used: {env}"
    );
    assert!(
        env["data"]["bolt11"]
            .as_str()
            .is_some_and(|s| s.starts_with("lnbc")),
        "order create returns a bolt11: {env}"
    );
    let order_id = env["data"]["order_id"]
        .as_str()
        .expect("order_id")
        .to_string();

    // 3. The harness settles the order out-of-band (the operator's payment concern, SPEC §4.7).
    let order_ext = format!("order:{buyer_hex}:ord1");
    payment
        .settle(&order_ext, clock.now())
        .expect("settle the order invoice");

    // 4. `order wait` receives provision.ready with the dummy recipe's delivered credential.
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["order", "wait", &order_id]).await;
    assert_eq!(code, 0, "order wait exit 0: {env}");
    assert_eq!(
        env["data"]["subscription_id"],
        serde_json::json!(order_id),
        "provision.ready is ours: {env}"
    );
    assert_eq!(
        env["data"]["payload"]["credential"],
        serde_json::json!("dummy-secret-token"),
        "provision.ready carries the dummy recipe payload: {env}"
    );

    // 5. `renew --request-id rnw1` returns a renewal billing.invoice; the harness settles it. The
    //    settle SUCCEEDING proves the operator really committed the renewal invoice on the backend.
    let (code, env) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &["renew", &order_id, "--request-id", "rnw1"],
    )
    .await;
    assert_eq!(code, 0, "renew exit 0: {env}");
    assert_eq!(
        env["data"]["subscription_id"],
        serde_json::json!(order_id),
        "renewal for our sub: {env}"
    );
    assert!(
        env["data"]["bolt11"]
            .as_str()
            .is_some_and(|s| s.starts_with("lnbc")),
        "renew returns a payable bolt11: {env}"
    );
    let renew_ext = format!("renew:req:{buyer_hex}:rnw1");
    payment
        .settle(&renew_ext, clock.now())
        .expect("the operator committed the renewal invoice, so settling it succeeds");

    // 6. `ops <sub> status` round-trips (exit 0, ok envelope, op data) — the sub is still serviceable
    //    after the renewal.
    let (code, env) = run_buyer(&url, &op_hex, &key_file, &["ops", &order_id, "status"]).await;
    assert_eq!(code, 0, "ops status exit 0: {env}");
    assert_eq!(env["data"]["status"], serde_json::json!("ok"), "{env}");
    assert_eq!(
        env["data"]["data"]["state"],
        serde_json::json!("running"),
        "the status hook round-trips: {env}"
    );

    // 7. Duplicate `ops <sub> restart --request-id rst1` TWICE -> both succeed, and the op hook ran
    //    EXACTLY ONCE (idempotent on (sender, request_id), §5.1): the marker has one line.
    let (code_a, env_a) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &["ops", &order_id, "restart", "--request-id", "rst1"],
    )
    .await;
    let (code_b, env_b) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &["ops", &order_id, "restart", "--request-id", "rst1"],
    )
    .await;
    assert_eq!(code_a, 0, "first restart exit 0: {env_a}");
    assert_eq!(code_b, 0, "duplicate restart exit 0: {env_b}");
    assert_eq!(
        env_a["data"], env_b["data"],
        "the duplicate resends the identical cached op.result"
    );
    assert_eq!(
        wait_for_op_done(&store, &buyer_hex, "rst1").await,
        env_a["data"]["data"],
        "the cached restart result is durably DONE"
    );
    // The second buyer may have consumed a replayed prior op.result before the operator handled its
    // duplicate request. Force one later operator round-trip through the same relay before counting
    // hook effects, so any duplicate that would incorrectly re-run the hook has had its chance.
    let (code_after, env_after) = run_buyer(
        &url,
        &op_hex,
        &key_file,
        &["ops", &order_id, "status", "--request-id", "after-dup"],
    )
    .await;
    assert_eq!(code_after, 0, "post-duplicate status exit 0: {env_after}");
    assert_eq!(
        wait_for_op_done(&store, &buyer_hex, "after-dup").await,
        env_after["data"]["data"],
        "post-duplicate status was processed by the operator"
    );
    let runs = std::fs::read_to_string(&restart_marker)
        .unwrap_or_default()
        .lines()
        .count();
    assert_eq!(
        runs, 1,
        "the restart hook ran exactly ONCE despite two identical requests"
    );

    // 8. An op against an UNOWNED subscription -> remote error, exit 6, `unauthorized`.
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
