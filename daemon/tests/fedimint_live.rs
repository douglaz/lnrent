//! Live regtest integration test for the real Fedimint backend (lnrent-7fp.4d). Compiled ONLY with
//! `--features fedimint` and `#[ignore]`d by default — it requires a running devimint federation and
//! is never part of the normal `cargo test` (CI has no federation). It is the FUNCTIONAL proof that
//! `FedimintPayment` actually issues, receives, and pays real (regtest) ecash against fedimint 0.11.1.
//!
//! Confirmed GREEN against a live v0.11.1 dev-fed (received 1000 sat, paid 500 sat out, idempotent).
//! Run it via the PRE-BUILT test binary so there's no nested nix-shell PATH problem (devimint's
//! `--exec` injects `FM_INVITE_CODE` + the live `fedimint-cli` alias onto PATH):
//!
//!   # one-time: a v0.11.1 worktree + the fedimint workspace (devimint + fedimintd/gatewayd/cli):
//!   git -C ~/p/fedimint worktree add /tmp/fedimint-0.11.1 v0.11.1
//!   cd /tmp/fedimint-0.11.1 && nix develop --command cargo build
//!   # build this test binary in the lnrent devshell:
//!   cd ~/projects/lnrent && nix develop . --command \
//!     cargo test -p lnrentd --features fedimint --test fedimint_live --no-run
//!   # run it under a fresh dev-fed:
//!   TESTBIN=$(ls -t ~/projects/lnrent/target/debug/deps/fedimint_live-* | grep -vE '\.d$' | head -1)
//!   cd /tmp/fedimint-0.11.1 && nix develop --command bash -c \
//!     "export PATH=\"\$PWD/target-nix/debug:\$PATH\"; \
//!      devimint dev-fed --exec $TESTBIN --ignored --nocapture"
#![cfg(feature = "fedimint")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lnrentd::backends::{PayStatus, PaymentBackend, PaymentStatus};
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::fedimint_backend::FedimintPayment;

/// Run `fedimint-cli <args>` (the devimint internal, pegged-in client) and parse its JSON stdout.
/// A non-object response is wrapped as a JSON string so single-value outputs still parse.
fn fedimint_cli(args: &[&str]) -> serde_json::Value {
    let out = Command::new("fedimint-cli")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn fedimint-cli {args:?}: {e}"));
    assert!(
        out.status.success(),
        "fedimint-cli {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(&stdout)
        .unwrap_or_else(|_| serde_json::Value::String(stdout.trim().to_string()))
}

/// Full money-path proof against a live regtest federation: join, idempotent invoice, receive ecash
/// (settlement stream), then pay ecash out (idempotently). Each wait is bounded so a stuck federation
/// fails the test rather than hanging.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation (FM_INVITE_CODE on the env)"]
async fn fedimint_receive_and_pay_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let data_dir =
        std::env::temp_dir().join(format!("lnrent-fedimint-live-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let root_secret = [7u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let backend = FedimintPayment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation");

    // --- watch FIRST so a freshly-created invoice gets a LIVE settlement task (pushes on Claimed) --
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // --- 1. create_invoice is idempotent on external_id ------------------------------------------
    let inv = backend
        .create_invoice(1000, "lnrent live receive", 3600, "ext-live-1")
        .await
        .expect("create gateway bolt11 invoice");
    let inv_again = backend
        .create_invoice(1000, "lnrent live receive", 3600, "ext-live-1")
        .await
        .expect("create_invoice idempotent");
    assert_eq!(inv.id, inv_again.id, "same external_id -> same invoice");
    assert_eq!(inv.bolt11, inv_again.bolt11);
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Open,
        "unpaid invoice is Open"
    );

    // --- 2. receive: the devimint client pays our invoice -> ecash claimed -> Settlement pushed ---
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    let settlement = tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("a settlement arrives within 90s")
        .expect("settlement channel open");
    assert_eq!(settlement.external_id, "ext-live-1");
    assert_eq!(settlement.amount_sat, 1000);
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Paid,
        "invoice is Paid after settlement"
    );

    // --- 3. pay out: our client now holds ~1000 sat ecash; pay a 500-sat invoice, idempotently ----
    let invoice_json = fedimint_cli(&["ln-invoice", "--amount", "500000"]); // Amount defaults to msat -> 500 sat
    let dest_bolt11 = invoice_json
        .get("invoice")
        .and_then(|v| v.as_str())
        .or_else(|| invoice_json.as_str())
        .expect("a bolt11 from fedimint-cli ln-invoice")
        .to_string();

    let pay_id = tokio::time::timeout(
        Duration::from_secs(90),
        backend.pay(&dest_bolt11, 500, "refund-live-1"),
    )
    .await
    .expect("pay completes within 90s")
    .expect("pay 500 sat out");
    assert_eq!(
        backend
            .payment_status_by_key("refund-live-1")
            .await
            .unwrap(),
        PayStatus::Succeeded,
        "the refund key is Succeeded"
    );
    // Re-pay the same key: idempotent, no double-pay.
    let pay_id_again = backend
        .pay(&dest_bolt11, 500, "refund-live-1")
        .await
        .expect("pay idempotent on key");
    assert_eq!(pay_id, pay_id_again, "same key -> same payment id");

    println!("FEDIMINT LIVE TEST PASSED — received 1000 sat, paid 500 sat out (idempotent)");
}

/// Find the lnrent-owned `lnrent_index.db` under `data_dir/fedimint/<federation_id>/`.
fn find_index_db(data_dir: &std::path::Path) -> std::path::PathBuf {
    let fed_root = data_dir.join("fedimint");
    for entry in std::fs::read_dir(&fed_root).expect("read fedimint dir") {
        let db = entry.expect("dir entry").path().join("lnrent_index.db");
        if db.exists() {
            return db;
        }
    }
    panic!("lnrent_index.db not found under {fed_root:?}");
}

/// PROOF of the pay-side oplog recovery (lnrent-4gt PART 2): a crash in `pay()`'s window (the fedimint
/// op committed but the local `fedimint_pay` row not yet persisted) leaves the refund key reporting
/// `Unknown`; on reopen, `recover_pay_from_oplog` backfills the row from the oplog `extra_meta` so the
/// next `pay(key)` re-awaits the OPERATION (not the maybe-expired bolt11) and resolves to terminal.
/// We simulate the crash by DELETING the persisted pay row, then reopening the backend.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation (FM_INVITE_CODE on the env)"]
async fn fedimint_pay_oplog_recovery_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let data_dir =
        std::env::temp_dir().join(format!("lnrent-fedimint-recover-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let root_secret = [9u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let key = "refund-recover-1";

    // --- fund the client (~1000 sat ecash) so it can pay a refund out -----------------------------
    let backend = FedimintPayment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation");
    let mut settlements = backend.watch().await.expect("open settlement stream");
    let inv = backend
        .create_invoice(1000, "recover-fund", 3600, "ext-recover-1")
        .await
        .expect("create gateway bolt11 invoice");
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("a settlement arrives within 90s")
        .expect("settlement channel open");

    // --- pay a refund out under `key` -------------------------------------------------------------
    let dest = fedimint_cli(&["ln-invoice", "--amount", "400000"]); // 400 sat
    let dest_bolt11 = dest
        .get("invoice")
        .and_then(|v| v.as_str())
        .or_else(|| dest.as_str())
        .expect("a bolt11 from fedimint-cli ln-invoice")
        .to_string();
    tokio::time::timeout(Duration::from_secs(90), backend.pay(&dest_bolt11, 400, key))
        .await
        .expect("pay completes within 90s")
        .expect("pay 400 sat out");
    assert_eq!(
        backend.payment_status_by_key(key).await.unwrap(),
        PayStatus::Succeeded,
        "the refund key is Succeeded before the simulated crash"
    );

    // --- simulate the crash window: close the backend, DELETE the pay row (so the key would report
    //     Unknown), then reopen so recover_pay_from_oplog backfills it from the oplog ---------------
    drop(settlements);
    drop(backend);
    let index_db = find_index_db(&data_dir);
    {
        let conn = rusqlite::Connection::open(&index_db).expect("open lnrent_index.db");
        let n = conn
            .execute(
                "DELETE FROM fedimint_pay WHERE idempotency_key = ?1",
                rusqlite::params![key],
            )
            .expect("delete the pay row");
        assert_eq!(n, 1, "deleted exactly the one pay row (the crash window)");
    }

    let backend2 = FedimintPayment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("reopen + recover from oplog");
    // Without recovery this would be Unknown (no row); recover_pay_from_oplog backfilled it.
    assert_ne!(
        backend2.payment_status_by_key(key).await.unwrap(),
        PayStatus::Unknown,
        "oplog recovery backfilled the pay row (no longer Unknown)"
    );
    // Re-driving pay(key) re-awaits the recovered OP (not the bolt11) and reaches terminal Succeeded.
    backend2
        .pay(&dest_bolt11, 400, key)
        .await
        .expect("re-await via the recovered op");
    assert_eq!(
        backend2.payment_status_by_key(key).await.unwrap(),
        PayStatus::Succeeded,
        "the recovered refund resolves to Succeeded via the op"
    );

    println!("FEDIMINT PAY OPLOG RECOVERY TEST PASSED — crash-window pay row recovered + resolved");
}

/// PROOF that a COLD backup/restore preserves the ecash position across box death (lnrent-7fp.14 PART B
/// — the live half of the test design). Receive ecash + pay a refund, then back up the STOPPED data
/// dir, restore it into a FRESH data dir, reopen `FedimintPayment` there, and assert the prior pay
/// status survived AND the recovered ecash is still SPENDABLE (a second refund pays out). The offline
/// test (tests/backup.rs) proves the file-level round-trip; only a real federation can prove the
/// fedimint POSITION is live after restore.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation (FM_INVITE_CODE on the env)"]
async fn fedimint_backup_restore_preserves_ecash_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let pid = std::process::id();
    let data_dir = std::env::temp_dir().join(format!("lnrent-fedimint-bk-src-{pid}"));
    std::fs::create_dir_all(&data_dir).unwrap();
    let root_secret = [11u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    // --- fund (~1000 sat ecash) + pay a 300-sat refund out under a known key ----------------------
    let backend = FedimintPayment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation");
    let mut settlements = backend.watch().await.expect("open settlement stream");
    let inv = backend
        .create_invoice(1000, "bk-fund", 3600, "ext-bk-1")
        .await
        .expect("create gateway bolt11 invoice");
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("a settlement arrives within 90s")
        .expect("settlement channel open");
    let dest1 = fedimint_cli(&["ln-invoice", "--amount", "300000"]);
    let dest1_bolt11 = dest1
        .get("invoice")
        .and_then(|v| v.as_str())
        .or_else(|| dest1.as_str())
        .expect("a bolt11 from fedimint-cli ln-invoice")
        .to_string();
    tokio::time::timeout(
        Duration::from_secs(90),
        backend.pay(&dest1_bolt11, 300, "refund-bk-1"),
    )
    .await
    .expect("pay completes within 90s")
    .expect("pay 300 sat out");
    assert_eq!(
        backend.payment_status_by_key("refund-bk-1").await.unwrap(),
        PayStatus::Succeeded,
        "the first refund Succeeded before backup"
    );

    // --- BOX DEATH: stop the daemon (drop the backend -> rocksdb closes). backup() needs a state DB
    //     present, so create a minimal valid lnrent.sqlite (this test has no daemon store; the offline
    //     test covers state-DB reproduction — here the focus is the fedimint position). -------------
    drop(settlements);
    drop(backend);
    rusqlite::Connection::open(data_dir.join("lnrent.sqlite"))
        .expect("create a stub state DB so backup() has one to capture");
    let dest = std::env::temp_dir().join(format!("lnrent-fedimint-bk-dest-{pid}"));
    let restored = std::env::temp_dir().join(format!("lnrent-fedimint-bk-restored-{pid}"));
    lnrentd::backup::backup(&data_dir, &dest).expect("cold backup of the stopped data dir");
    lnrentd::backup::restore(&dest, &restored, false).expect("restore into a fresh data dir");

    // --- reopen on the RESTORED dir: prior pay status survives + the ecash is still SPENDABLE -------
    let backend2 = FedimintPayment::join_or_open(&invite, &restored, &root_secret, clock.clone())
        .await
        .expect("reopen the fedimint client from the restored backup");
    assert_eq!(
        backend2.payment_status_by_key("refund-bk-1").await.unwrap(),
        PayStatus::Succeeded,
        "the prior refund's status survived the backup/restore"
    );
    let dest2 = fedimint_cli(&["ln-invoice", "--amount", "200000"]);
    let dest2_bolt11 = dest2
        .get("invoice")
        .and_then(|v| v.as_str())
        .or_else(|| dest2.as_str())
        .expect("a bolt11 from fedimint-cli ln-invoice")
        .to_string();
    tokio::time::timeout(
        Duration::from_secs(90),
        backend2.pay(&dest2_bolt11, 200, "refund-bk-2"),
    )
    .await
    .expect("pay completes within 90s")
    .expect("the restored ecash is spendable");
    assert_eq!(
        backend2.payment_status_by_key("refund-bk-2").await.unwrap(),
        PayStatus::Succeeded,
        "a second refund pays out from the RESTORED ecash position"
    );

    println!(
        "FEDIMINT BACKUP/RESTORE TEST PASSED — pay status preserved + ecash spendable after restore"
    );
}
