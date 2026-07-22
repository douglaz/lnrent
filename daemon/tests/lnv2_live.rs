//! Live regtest integration for the lnv2 Fedimint backend (lnrent-3d5, ADR-0018). Compiled ONLY with
//! `--features fedimint` and `#[ignore]`d by default — it needs
//! a running devimint federation whose lnv2 module + an lnv2-capable gateway are attached, and is never
//! part of the normal `cargo test` (CI has no federation). It is the FUNCTIONAL proof that
//! `Lnv2Payment` issues, receives, and pays real (regtest) ecash via `fedimint-lnv2-client` — and that
//! the send is idempotent on the refund key (the property the whole idempotency design protects).
//!
//! Run it via the pre-built test binary under a fresh dev-fed:
//!
//!   # one-time: a v0.11.1 worktree + the fedimint workspace (devimint + fedimintd/gatewayd/cli):
//!   git -C ~/p/fedimint worktree add /tmp/fedimint-0.11.1 v0.11.1
//!   cd /tmp/fedimint-0.11.1 && nix develop --command cargo build
//!   # build this test binary in the lnrent devshell, then run it under a fresh dev-fed:
//!   cd ~/projects/lnrent && nix develop . --command \
//!     cargo test -p lnrentd --features fedimint --test lnv2_live --no-run
//!   TESTBIN=$(ls -t ~/projects/lnrent/target/debug/deps/lnv2_live-* | grep -vE '\.d$' | head -1)
//!   cd /tmp/fedimint-0.11.1 && nix develop --command bash -c \
//!     "export PATH=\"\$PWD/target-nix/debug:\$PATH\"; \
//!      devimint dev-fed --exec $TESTBIN --ignored --nocapture"
#![cfg(feature = "fedimint")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lnrentd::backends::{PayStatus, PaymentBackend, PaymentStatus};
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::lnv2_backend::Lnv2Payment;

/// Run `fedimint-cli <args>` (the devimint internal client) and parse its JSON stdout. A non-object
/// response is wrapped as a JSON string so single-value outputs still parse.
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

/// Amounts are env-overridable so the same test can run against a live (mainnet) federation at a
/// trivial size. Defaults preserve the regtest/devimint behaviour exactly.
fn amt_sat(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .map(|v| v.parse().unwrap_or_else(|e| panic!("{var}={v:?}: {e}")))
        .unwrap_or(default)
}

/// The client data dir. Ephemeral (pid-scoped) by default, as devimint wants; against a live
/// federation set `LNRENT_LIVE_DATA_DIR` so residual ecash lands in a wallet we can drain later
/// rather than in a temp dir that gets reaped.
fn data_dir(suffix: &str) -> std::path::PathBuf {
    let dir = match std::env::var("LNRENT_LIVE_DATA_DIR") {
        Ok(base) => std::path::PathBuf::from(base).join(suffix),
        Err(_) => std::env::temp_dir().join(format!("lnrent-{suffix}-{}", std::process::id())),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A per-RUN id, used to make invoice `external_id`s and refund keys unique across runs.
///
/// With the default (pid-scoped) data dir every run gets a fresh wallet, so constant ids were fine.
/// With `LNRENT_LIVE_DATA_DIR` the wallet PERSISTS, and constant ids would make a second run a
/// no-op instead of a test: `create_invoice` is idempotent on `external_id` (it would hand back the
/// already-PAID invoice, so no settlement ever arrives) and `pay_refund_capped` is idempotent on its
/// key (it would return the previous op without sending). Stable WITHIN a run — the idempotency
/// assertions still re-use the same id deliberately — and distinct ACROSS runs.
fn run_id() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after the unix epoch")
        .as_nanos()
        .to_string()
}

fn bolt11_of(v: &serde_json::Value) -> String {
    v.get("invoice")
        .and_then(|x| x.as_str())
        .or_else(|| v.as_str())
        .expect("a bolt11 from fedimint-cli")
        .to_string()
}

/// Full lnv2 money-path proof: join, idempotent invoice, receive ecash (live settlement), then pay
/// ecash out idempotently on the refund key. Each wait is bounded so a stuck federation fails the test
/// rather than hanging.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation with lnv2 (FM_INVITE_CODE on the env)"]
async fn lnv2_receive_and_pay_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let data_dir = data_dir("lnv2-live");
    let recv_sat = amt_sat("LNRENT_LIVE_RECV_SAT", 1000);
    // Default HALF the receive, so lowering only RECV_SAT stays coherent (1000 -> 500 unchanged).
    let pay_sat = amt_sat("LNRENT_LIVE_PAY_SAT", recv_sat / 2);
    assert!(
        (1..recv_sat).contains(&pay_sat),
        "LNRENT_LIVE_PAY_SAT ({pay_sat}) must be in 1..{recv_sat}: at least 1 sat so the run actually \
         proves the outbound send, and strictly below the receive since its fee takes a cut"
    );
    let ext = format!("ext-lnv2-{}", run_id());
    let refund_key = format!("refund-lnv2-{}", run_id());
    let root_secret = [17u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let backend = Lnv2Payment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation via lnv2");

    // watch FIRST so a freshly-created invoice gets a LIVE settlement task (pushes on Claimed).
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // 1. create_invoice is idempotent on external_id.
    let inv = backend
        .create_invoice(recv_sat, "lnrent lnv2 receive", 3600, &ext)
        .await
        .expect("mint an lnv2 gateway bolt11");
    let inv_again = backend
        .create_invoice(recv_sat, "lnrent lnv2 receive", 3600, &ext)
        .await
        .expect("create_invoice idempotent");
    assert_eq!(inv.id, inv_again.id, "same external_id -> same invoice");
    assert_eq!(inv.bolt11, inv_again.bolt11);
    assert_eq!(backend.lookup(&inv.id).await.unwrap(), PaymentStatus::Open);

    // 2. receive: the devimint client pays our invoice -> ecash claimed -> Settlement pushed.
    let balance_before_msat = backend
        .available_balance_msat()
        .await
        .expect("read balance before receive")
        .expect("Fedimint exposes an available balance");
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    let settlement = tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("a settlement arrives within 90s")
        .expect("settlement channel open");
    assert_eq!(settlement.external_id, ext);
    assert_eq!(settlement.amount_sat, recv_sat);
    assert!(
        settlement.received_msat < settlement.amount_sat * 1000,
        "the live arm exposes the lnv2 gateway receive fee ([9A])"
    );
    let balance_after_msat = backend
        .available_balance_msat()
        .await
        .expect("read balance after receive")
        .expect("Fedimint exposes an available balance");
    assert_eq!(
        settlement.received_msat,
        balance_after_msat
            .checked_sub(balance_before_msat)
            .expect("a successful receive increases the wallet balance"),
        "the real claim decoder equals the wallet's exact live balance delta ([9A])"
    );
    assert_eq!(
        backend.received_amount_msat(&inv.id).await.unwrap(),
        Some(settlement.received_msat),
        "the exact live credit is also durable for recovery"
    );
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Paid,
        "invoice is Paid after settlement"
    );

    // 3. pay out: our client now holds the received ecash; pay a smaller invoice, idempotently.
    let pay_msat = (pay_sat * 1000).to_string();
    let dest = bolt11_of(&fedimint_cli(&["ln-invoice", "--amount", &pay_msat]));
    let pay_id = tokio::time::timeout(
        Duration::from_secs(90),
        backend.pay_refund_capped(&dest, pay_sat, recv_sat, &refund_key),
    )
    .await
    .expect("pay completes within 90s")
    .expect("pay 500 sat out");
    assert_eq!(
        backend.payment_status_by_key(&refund_key).await.unwrap(),
        PayStatus::Succeeded,
        "the refund key is Succeeded"
    );
    // Re-pay the same key: idempotent, no double-pay.
    let pay_id_again = backend
        .pay_refund_capped(&dest, pay_sat, recv_sat, &refund_key)
        .await
        .expect("pay idempotent on key");
    assert_eq!(
        pay_id, pay_id_again,
        "same key -> same op id, never a second send"
    );

    println!(
        "LNV2 LIVE TEST PASSED — received {recv_sat} sat, paid {pay_sat} sat out (idempotent)"
    );
}

/// PROOF that a DEFINITIVELY-failed lnv2 send parks the key FAILED and never re-sends the same bolt11
/// (the NO-RETRY guarantee): we ask lnv2 to pay an already-EXPIRED invoice, which the client refuses
/// (or refunds), so the key resolves to a non-success terminal.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation with lnv2 (FM_INVITE_CODE on the env)"]
async fn lnv2_send_failure_is_terminal_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let data_dir = data_dir("lnv2-fail");
    let fund_sat = amt_sat("LNRENT_LIVE_RECV_SAT", 1000);
    // Strictly below `fund_sat` so the send is attemptable at all after the receive fee. Defaults to
    // 2/5 of the funding (1000 -> 400 unchanged) so lowering only RECV_SAT cannot strand it above.
    let pay_sat = amt_sat("LNRENT_LIVE_FAIL_PAY_SAT", fund_sat * 2 / 5);
    assert!(
        (1..fund_sat).contains(&pay_sat),
        "LNRENT_LIVE_FAIL_PAY_SAT ({pay_sat}) must be in 1..{fund_sat}: at least 1 sat so a send is \
         actually attempted, and strictly below the funding since its receive fee takes a cut"
    );
    let ext = format!("ext-lnv2-fail-{}", run_id());
    let refund_key = format!("refund-lnv2-fail-{}", run_id());
    let root_secret = [19u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let backend = Lnv2Payment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation via lnv2");
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // Fund the wallet so a send can be attempted at all.
    let inv = backend
        .create_invoice(fund_sat, "fund", 3600, &ext)
        .await
        .expect("mint invoice");
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("settlement within 90s")
        .expect("channel open");

    // A 1-second-expiry destination invoice, then wait it out so the send cannot succeed.
    let pay_msat = (pay_sat * 1000).to_string();
    let dest = bolt11_of(&fedimint_cli(&[
        "ln-invoice",
        "--amount",
        &pay_msat,
        "--expiry-time",
        "1",
    ]));
    tokio::time::sleep(Duration::from_secs(3)).await;

    let first = tokio::time::timeout(
        Duration::from_secs(90),
        backend.pay_refund_capped(&dest, pay_sat, fund_sat, &refund_key),
    )
    .await
    .expect("pay attempt completes within 90s");
    assert!(first.is_err(), "an expired-invoice send must not succeed");
    assert_eq!(
        backend.payment_status_by_key(&refund_key).await.unwrap(),
        PayStatus::Failed,
        "a definitive send failure parks the key FAILED (a fresh generation re-resolves)"
    );

    // NO-RETRY: re-driving the SAME key stays terminal Err — it does not re-send the same bolt11.
    let again = backend
        .pay_refund_capped(&dest, pay_sat, fund_sat, &refund_key)
        .await;
    assert!(
        again.is_err(),
        "a FAILED key never re-sends the same bolt11"
    );

    println!(
        "LNV2 SEND-FAILURE TEST PASSED — definitive failure is terminal + NO-RETRY on the bolt11"
    );
}

/// PROOF that a COLD backup/restore preserves the lnv2 ecash position across box death (lnrent-2ad —
/// the live half; the offline `tests/backup.rs` proves only the file-level byte round-trip). This is
/// the lnv2 port of the `fedimint_backup_restore_preserves_ecash_live` test deleted with lnv1
/// (lnrent-8ym): receive ecash + pay a refund out, back up the STOPPED data dir, restore it into a
/// FRESH dir, reopen `Lnv2Payment` there, and assert the prior pay status survived AND the recovered
/// ecash is still SPENDABLE (a second refund pays out). Only a real federation can prove the fedimint
/// POSITION is live after restore — the offline test cannot reopen the backend (that needs guardians).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running devimint federation with lnv2 (FM_INVITE_CODE on the env)"]
async fn lnv2_backup_restore_preserves_ecash_live() {
    let invite = std::env::var("FM_INVITE_CODE")
        .expect("FM_INVITE_CODE — run under `devimint dev-fed --exec`");
    let src_dir = data_dir("lnv2-bk-src");
    let recv_sat = amt_sat("LNRENT_LIVE_RECV_SAT", 1000);
    // Two payouts out of the funded position: pay1 before backup, pay2 from the RESTORED ecash.
    // Defaults 3/10 + 2/10 keep the historical 1000 -> 300/200 and stay coherent when RECV_SAT is
    // lowered (50 -> 15/10). Both >= 1 and pay1+pay2 < recv so the second send is fundable post-fee.
    let pay1_sat = amt_sat("LNRENT_LIVE_BK_PAY1_SAT", recv_sat * 3 / 10);
    let pay2_sat = amt_sat("LNRENT_LIVE_BK_PAY2_SAT", recv_sat * 2 / 10);
    assert!(
        pay1_sat >= 1 && pay2_sat >= 1 && pay1_sat + pay2_sat < recv_sat,
        "LNRENT_LIVE_BK_PAY1_SAT ({pay1_sat}) + PAY2_SAT ({pay2_sat}) must each be >= 1 and sum below \
         LNRENT_LIVE_RECV_SAT ({recv_sat}): receive + send fees take cuts, so an equal/greater total \
         can never be funded"
    );
    let ext = format!("ext-lnv2-bk-{}", run_id());
    let key1 = format!("refund-lnv2-bk1-{}", run_id());
    let key2 = format!("refund-lnv2-bk2-{}", run_id());
    let root_secret = [23u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    // --- fund + pay the first refund out under key1 -----------------------------------------------
    let backend = Lnv2Payment::join_or_open(&invite, &src_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation via lnv2");
    let mut settlements = backend.watch().await.expect("open settlement stream");
    let inv = backend
        .create_invoice(recv_sat, "lnv2 bk fund", 3600, &ext)
        .await
        .expect("mint an lnv2 gateway bolt11");
    fedimint_cli(&["ln-pay", &inv.bolt11]);
    tokio::time::timeout(Duration::from_secs(90), settlements.recv())
        .await
        .expect("a settlement arrives within 90s")
        .expect("settlement channel open");

    let pay1_msat = (pay1_sat * 1000).to_string();
    let dest1 = bolt11_of(&fedimint_cli(&["ln-invoice", "--amount", &pay1_msat]));
    tokio::time::timeout(
        Duration::from_secs(90),
        backend.pay_refund_capped(&dest1, pay1_sat, recv_sat, &key1),
    )
    .await
    .expect("pay1 completes within 90s")
    .expect("pay the first refund out");
    assert_eq!(
        backend.payment_status_by_key(&key1).await.unwrap(),
        PayStatus::Succeeded,
        "the first refund Succeeded before backup"
    );

    // --- BOX DEATH: drop the backend (rocksdb closes). backup() needs a state DB present, so create a
    //     minimal valid lnrent.sqlite (this test has no daemon store; the offline test covers state-DB
    //     reproduction — here the focus is the fedimint POSITION surviving restore). ---------------
    drop(settlements);
    drop(backend);
    rusqlite::Connection::open(src_dir.join("lnrent.sqlite"))
        .expect("create a stub state DB so backup() has one to capture");
    let bk_dest = std::env::temp_dir().join(format!("lnrent-lnv2-bk-dest-{}", std::process::id()));
    let restored = data_dir("lnv2-bk-restored");
    // restore() refuses a non-empty target; use a fresh dir so a re-run starts clean.
    let _ = std::fs::remove_dir_all(&restored);
    lnrentd::backup::backup(&src_dir, &bk_dest, None).expect("cold backup of the stopped data dir");
    lnrentd::backup::restore(&bk_dest, &restored, false, None)
        .expect("restore into a fresh data dir");

    // --- reopen on the RESTORED dir: prior pay status survives + the ecash is still SPENDABLE -------
    let backend2 = Lnv2Payment::join_or_open(&invite, &restored, &root_secret, clock.clone())
        .await
        .expect("reopen the lnv2 client from the restored backup");
    assert_eq!(
        backend2.payment_status_by_key(&key1).await.unwrap(),
        PayStatus::Succeeded,
        "the prior refund's status survived the backup/restore"
    );
    let pay2_msat = (pay2_sat * 1000).to_string();
    let dest2 = bolt11_of(&fedimint_cli(&["ln-invoice", "--amount", &pay2_msat]));
    tokio::time::timeout(
        Duration::from_secs(90),
        backend2.pay_refund_capped(&dest2, pay2_sat, recv_sat, &key2),
    )
    .await
    .expect("pay2 completes within 90s")
    .expect("the restored ecash is spendable");
    assert_eq!(
        backend2.payment_status_by_key(&key2).await.unwrap(),
        PayStatus::Succeeded,
        "a second refund pays out from the RESTORED ecash position"
    );

    println!(
        "LNV2 BACKUP/RESTORE TEST PASSED — pay status preserved + ecash spendable after restore"
    );
}
