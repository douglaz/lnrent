//! Standalone local Nostr relay for LIVE cross-process product testing (operator daemon <-> buyer
//! CLI running as separate processes). Prints its `ws://127.0.0.1:PORT` URL on the first stdout line,
//! then serves until the process is killed. Dev/test harness only — never shipped.
use nostr_relay_builder::MockRelay;

#[tokio::main]
async fn main() {
    let relay = MockRelay::run().await.expect("start local relay");
    let url = relay.url().await;
    println!("{url}");
    // Keep the process (and thus the relay) alive until killed.
    std::future::pending::<()>().await;
}
