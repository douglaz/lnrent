use lnrentd::backends::{MockPayment, PaymentBackend};
use lnrentd::clock::{Clock, TestClock};
use lnrentd::ipc;
use lnrentd::recipe::Recipe;
use lnrentd::store::{migrate, Store};
use rusqlite::Connection;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::watch;

fn temp_data_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lnrent-operator-cli-{name}-{}-{nanos}",
        std::process::id()
    ))
}

fn mem_store() -> Store {
    let conn = Connection::open_in_memory().unwrap();
    migrate(&conn).unwrap();
    Store::spawn(conn)
}

async fn wait_for_socket(sock: &Path) {
    for _ in 0..100 {
        if sock.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("IPC socket was not created at {}", sock.display());
}

#[tokio::test]
async fn json_money_returns_reply_envelope() {
    let data_dir = temp_data_dir("money-json");
    fs::create_dir_all(&data_dir).unwrap();
    let sock = data_dir.join("lnrent.sock");
    let store = mem_store();
    let recipes = Arc::new(Vec::<Recipe>::new());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));
    let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let sock_for_server = sock.clone();
    let server = tokio::spawn(async move {
        ipc::serve_with_shutdown(
            store,
            recipes,
            clock,
            payment,
            lnrentd::relay_status::RelayStatusCell::new(),
            &sock_for_server,
            shutdown_rx,
        )
        .await
    });
    wait_for_socket(&sock).await;

    let out = Command::new(env!("CARGO_BIN_EXE_lnrent"))
        .args(["--json", "--data-dir", data_dir.to_str().unwrap(), "money"])
        .output()
        .await
        .unwrap();

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("IPC server stopped")
        .unwrap()
        .unwrap();

    assert!(
        out.status.success(),
        "lnrent --json money failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "successful --json output must stay on stdout"
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], serde_json::json!(true));
    let data = v["data"].as_object().expect("reply carries data object");
    let mut keys = data.keys().map(|s| s.as_str()).collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            // lnrent-y4m.3: the degraded/read-only latch is surfaced so a status poll (not just the
            // daemon log) reveals a money daemon that is refusing writes after a fatal DB error.
            "degraded_read_only",
            // gate1-operator-sweep (urw.3): `money` also folds the operator-sweep surplus breakdown
            // (earned/reserved/paid_out/surplus) + last_sweep — all pure LOCAL ledger reads.
            "earned_msat",
            "expected_msat",
            "federation_ok",
            "gateway_ok",
            "gross_liability_sat",
            "last_sweep",
            "liability_count",
            "paid_out_msat",
            "parked_count",
            "ready",
            "required_msat",
            "reserved_msat",
            "surplus_msat",
            "warning",
        ]
    );
    // §E (lnrent-urw.10): the balance operand is now the ledger `expected_msat` (0 on a fresh store),
    // NOT a live wallet read — plain `money` makes no `available_balance_msat` call.
    assert_eq!(data["expected_msat"], serde_json::json!(0));
    assert_eq!(data["degraded_read_only"], serde_json::json!(false));
    assert_eq!(data["gateway_ok"], serde_json::json!(true));
    assert_eq!(data["liability_count"], 0);
    assert_eq!(data["ready"], serde_json::json!(true));
    assert_eq!(data["warning"], Value::Null);
    // The sweep surplus breakdown is 0 on a fresh store and no sweep has run yet.
    assert_eq!(data["surplus_msat"], serde_json::json!(0));
    assert_eq!(data["last_sweep"], Value::Null);

    let _ = fs::remove_dir_all(&data_dir);
}
