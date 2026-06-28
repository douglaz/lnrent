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
