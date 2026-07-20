//! Live regtest integration for the lnv2 Fedimint backend (lnrent-3d5, ADR-0018), mirroring
//! `fedimint_live.rs`. Compiled ONLY with `--features fedimint` and `#[ignore]`d by default — it needs
//! a running devimint federation whose lnv2 module + an lnv2-capable gateway are attached, and is never
//! part of the normal `cargo test` (CI has no federation). It is the FUNCTIONAL proof that
//! `Lnv2Payment` issues, receives, and pays real (regtest) ecash via `fedimint-lnv2-client` — and that
//! the send is idempotent on the refund key (the property the whole idempotency design protects).
//!
//! Run it via the pre-built test binary under a fresh dev-fed (see `fedimint_live.rs` for the one-time
//! worktree setup — the invocation is identical, only the `--test` name changes):
//!
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
    let pay_sat = amt_sat("LNRENT_LIVE_PAY_SAT", 500);
    let root_secret = [17u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let backend = Lnv2Payment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation via lnv2");

    // watch FIRST so a freshly-created invoice gets a LIVE settlement task (pushes on Claimed).
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // 1. create_invoice is idempotent on external_id.
    let inv = backend
        .create_invoice(recv_sat, "lnrent lnv2 receive", 3600, "ext-lnv2-1")
        .await
        .expect("mint an lnv2 gateway bolt11");
    let inv_again = backend
        .create_invoice(recv_sat, "lnrent lnv2 receive", 3600, "ext-lnv2-1")
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
    assert_eq!(settlement.external_id, "ext-lnv2-1");
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
        backend.pay_refund_capped(&dest, pay_sat, recv_sat, "refund-lnv2-1"),
    )
    .await
    .expect("pay completes within 90s")
    .expect("pay 500 sat out");
    assert_eq!(
        backend
            .payment_status_by_key("refund-lnv2-1")
            .await
            .unwrap(),
        PayStatus::Succeeded,
        "the refund key is Succeeded"
    );
    // Re-pay the same key: idempotent, no double-pay.
    let pay_id_again = backend
        .pay_refund_capped(&dest, pay_sat, recv_sat, "refund-lnv2-1")
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
    // Strictly below `fund_sat` so the send is attemptable at all after the receive fee.
    let pay_sat = amt_sat("LNRENT_LIVE_FAIL_PAY_SAT", 400);
    let root_secret = [19u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let backend = Lnv2Payment::join_or_open(&invite, &data_dir, &root_secret, clock.clone())
        .await
        .expect("join the regtest federation via lnv2");
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // Fund the wallet so a send can be attempted at all.
    let inv = backend
        .create_invoice(fund_sat, "fund", 3600, "ext-lnv2-fail")
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
        backend.pay_refund_capped(&dest, pay_sat, fund_sat, "refund-lnv2-fail"),
    )
    .await
    .expect("pay attempt completes within 90s");
    assert!(first.is_err(), "an expired-invoice send must not succeed");
    assert_eq!(
        backend
            .payment_status_by_key("refund-lnv2-fail")
            .await
            .unwrap(),
        PayStatus::Failed,
        "a definitive send failure parks the key FAILED (a fresh generation re-resolves)"
    );

    // NO-RETRY: re-driving the SAME key stays terminal Err — it does not re-send the same bolt11.
    let again = backend
        .pay_refund_capped(&dest, pay_sat, fund_sat, "refund-lnv2-fail")
        .await;
    assert!(
        again.is_err(),
        "a FAILED key never re-sends the same bolt11"
    );

    println!(
        "LNV2 SEND-FAILURE TEST PASSED — definitive failure is terminal + NO-RETRY on the bolt11"
    );
}
