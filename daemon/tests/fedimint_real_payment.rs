//! REAL-payment integration test against a LIVE federation + a REAL funded wallet (NOT devimint).
//! Proves the daemon's `FedimintPayment` receives an actual payment made by an external fedimint-cli
//! wallet on the same federation. Compiled only with `--features fedimint` and `#[ignore]`d.
//!
//! Drive it with:
//!   LNRENT_REAL_INVITE="$(jq -r .invite_code /tmp/orange-invite.json)" \
//!   LNRENT_PAYER_WALLET="$HOME/p/wallets/orange" \
//!   LNRENT_GATEWAY=039d1e06e6b10f3d18bbb76bb67f38a7088679c9a5e5914f4efe839298cb17e5e1 \
//!   nix develop . --command cargo test -p lnrentd --features fedimint --test fedimint_real_payment \
//!     -- --ignored --nocapture
#![cfg(feature = "fedimint")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lnrentd::backends::{PaymentBackend, PaymentStatus};
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::fedimint_backend::FedimintPayment;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs LNRENT_REAL_INVITE + LNRENT_PAYER_WALLET (a real funded wallet on the same fed)"]
async fn fedimint_receives_a_real_payment_from_a_wallet() {
    let invite = std::env::var("LNRENT_REAL_INVITE").expect("LNRENT_REAL_INVITE");
    let wallet = std::env::var("LNRENT_PAYER_WALLET").expect("LNRENT_PAYER_WALLET");
    let cli = std::env::var("LNRENT_FEDIMINT_CLI")
        .unwrap_or_else(|_| format!("{}/bin/fedimint-cli", std::env::var("HOME").unwrap()));
    let gateway = std::env::var("LNRENT_GATEWAY").ok();

    let data_dir = std::env::temp_dir().join(format!("lnrent-realpay-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let root_secret = [42u8; 32];
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    // Join the LIVE federation with a fresh daemon client (0 balance) and open the settlement stream.
    let backend = FedimintPayment::join_or_open(
        &invite,
        &data_dir,
        &root_secret,
        gateway.as_deref(),
        clock.clone(),
    )
    .await
    .expect("join the live federation");
    let mut settlements = backend.watch().await.expect("open settlement stream");

    // Mint a 1-sat invoice through the federation gateway.
    let inv = backend
        .create_invoice(1, "lnrent real-payment test", 3600, "real-pay-1")
        .await
        .expect("mint a 1-sat gateway invoice");
    eprintln!("minted 1-sat invoice: {}", inv.bolt11);
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Open,
        "unpaid invoice is Open"
    );

    // Pay it with the REAL funded wallet (an external fedimint-cli client on the same federation).
    let out = Command::new(&cli)
        .env("FM_CLIENT_DIR", &wallet)
        .args(["ln-pay", &inv.bolt11])
        .output()
        .expect("spawn fedimint-cli ln-pay");
    assert!(
        out.status.success(),
        "wallet ln-pay failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // The daemon's settlement watcher observes the real payment.
    let s = tokio::time::timeout(Duration::from_secs(120), settlements.recv())
        .await
        .expect("a settlement arrives within 120s")
        .expect("settlement channel open");
    assert_eq!(s.external_id, "real-pay-1");
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Paid,
        "invoice is Paid after the real payment"
    );
    eprintln!(
        "REAL PAYMENT RECEIVED: {} sat for {} (a live federation, paid by a real wallet)",
        s.amount_sat, s.external_id
    );
}
