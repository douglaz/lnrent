//! `lnrent preflight`/`doctor` smoke tests (lnrent-y4m.9), mirroring the `lnrent money` pattern
//! (`operator_money_cli.rs`): a real IPC server + the real CLI binary, proving the op round-trips,
//! the JSON shape is stable, and the exit code is gated on the AGGREGATE verdict. MockPayment (no
//! gateway/federation concept) passes both seams trivially; no recipe declares DO_TOKEN here, so
//! the provider-token check is SKIPPED — no test touches the network or the real DigitalOcean API.

use anyhow::Result;
use async_trait::async_trait;
use lnrentd::backends::{
    Invoice, MockPayment, PayStatus, PaymentBackend, PaymentStatus, Settlement,
};
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
use tokio::sync::{mpsc, watch};

fn temp_data_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lnrent-preflight-cli-{name}-{}-{nanos}",
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

/// A backend whose refund gateway is down (`Ok(false)`) while the federation is fine — the
/// failure the preflight exit gate must surface. Money methods are never reached by preflight.
struct GatewayDownPayment;

#[async_trait]
impl PaymentBackend for GatewayDownPayment {
    async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
        panic!("preflight must not create invoices")
    }
    async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
        panic!("preflight must not look up invoices")
    }
    async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
        panic!("preflight must not look up settlements")
    }
    async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
        panic!("preflight must not pay")
    }
    async fn payment_status(&self, _: &str) -> Result<PayStatus> {
        panic!("preflight must not check payment status")
    }
    async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
        panic!("preflight must not check payment status by key")
    }
    async fn refund_gateway_ready(&self) -> Result<bool> {
        Ok(false)
    }
    async fn backend_ready(&self) -> Result<bool> {
        Ok(true)
    }
    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        panic!("preflight must not watch settlements")
    }
}

/// Serve a real IPC socket over `payment`, run `lnrent <args...>` against it, and return the
/// process output (the money-test pattern, parameterized on the backend).
async fn run_cli(
    name: &str,
    payment: Arc<dyn PaymentBackend>,
    args: &[&str],
) -> std::process::Output {
    let data_dir = temp_data_dir(name);
    fs::create_dir_all(&data_dir).unwrap();
    let sock = data_dir.join("lnrent.sock");
    let store = mem_store();
    let recipes = Arc::new(Vec::<Recipe>::new());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));
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
        .arg("--data-dir")
        .arg(data_dir.to_str().unwrap())
        .args(args)
        .output()
        .await
        .unwrap();

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("IPC server stopped")
        .unwrap()
        .unwrap();
    let _ = fs::remove_dir_all(&data_dir);
    out
}

// The op round-trips and the JSON shape is stable: `{ok, checks:[{name, ok, detail}×3]}` in a
// fixed order, aggregate ok=true on a healthy mock backend, provider-token SKIPPED (no recipe
// declares DO_TOKEN), exit 0.
#[tokio::test]
async fn json_preflight_all_ok_round_trips_with_stable_shape() {
    let out = run_cli("ok", Arc::new(MockPayment::new()), &["--json", "preflight"]).await;

    assert!(
        out.status.success(),
        "lnrent --json preflight failed: stderr={}",
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
    assert_eq!(keys, vec!["checks", "ok"]);
    assert_eq!(data["ok"], serde_json::json!(true));

    let checks = data["checks"].as_array().expect("checks array");
    let names: Vec<&str> = checks.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["gateway", "federation", "provider_token"]);
    for c in checks {
        let mut check_keys = c
            .as_object()
            .unwrap()
            .keys()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        check_keys.sort_unstable();
        assert_eq!(check_keys, vec!["detail", "name", "ok"]);
        assert_eq!(c["ok"], serde_json::json!(true));
    }
    assert!(
        checks[2]["detail"].as_str().unwrap().contains("skipped"),
        "no recipe declares DO_TOKEN here, so the provider-token check is SKIPPED"
    );
}

// A failed check exits nonzero (exit 1 — the post-start launch gate) while the IPC envelope itself is
// still a healthy `ok:true` on stdout carrying the per-check diagnostics. Driven via the
// `doctor` alias to prove it routes to the same command.
#[tokio::test]
async fn doctor_alias_failed_check_exits_nonzero_with_diagnostics() {
    let out = run_cli(
        "gw-down",
        Arc::new(GatewayDownPayment),
        &["--json", "doctor"],
    )
    .await;

    assert_eq!(
        out.status.code(),
        Some(1),
        "a failed preflight check must exit 1: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        v["ok"],
        serde_json::json!(true),
        "the IPC round-trip itself succeeded"
    );
    let data = &v["data"];
    assert_eq!(data["ok"], serde_json::json!(false));
    let checks = data["checks"].as_array().unwrap();
    assert_eq!(checks[0]["name"], "gateway");
    assert_eq!(checks[0]["ok"], serde_json::json!(false));
    assert!(checks[0]["detail"]
        .as_str()
        .unwrap()
        .contains("no configured gateway is reachable"));
    assert_eq!(
        checks[1]["ok"],
        serde_json::json!(true),
        "federation is independent"
    );
}
