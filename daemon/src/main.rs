//! lnrentd: the lnrent control plane. AI-free runtime path (SPEC.md §4.1).
//! M0 skeleton: opens state, loads recipes. M1 wires the reconcile loop (§6.5),
//! the Nostr engine, and the payment watch.

use anyhow::Result;
use lnrentd::{recipe::Recipe, store};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let data_dir = std::env::var("LNRENT_DATA_DIR").unwrap_or_else(|_| "./data".into());
    std::fs::create_dir_all(&data_dir)?;
    let db_path = format!("{data_dir}/lnrent.sqlite");
    let _conn = store::open(&db_path)?;
    tracing::info!(db = %db_path, "lnrentd state opened");

    let recipes_dir = std::env::var("LNRENT_RECIPES_DIR").unwrap_or_else(|_| "./recipes".into());
    match Recipe::load_all(&recipes_dir) {
        Ok(recipes) => {
            tracing::info!(count = recipes.len(), dir = %recipes_dir, "recipes loaded");
            for r in &recipes {
                tracing::info!(id = %r.service.id, version = %r.service.version, "recipe");
            }
        }
        Err(e) => tracing::warn!(error = %e, dir = %recipes_dir, "no recipes loaded"),
    }

    // TODO M1: reconcile loop (SPEC.md §6.5), Nostr engine, payment watch, CLI socket.
    tracing::info!("lnrentd skeleton up; M1 wires the reconcile loop and Nostr/payments");
    Ok(())
}
