//! lnrentd: the lnrent control plane. AI-free runtime path (SPEC.md §4.1).
//! Opens state, spawns the sole-writer store actor (ADR-0001), loads recipes, and serves the
//! operator IPC socket (§4.2). M1 adds the reconcile loop (§6.5), the Nostr engine, and the
//! payment watch alongside.

use anyhow::Result;
use lnrentd::{ipc, recipe::Recipe, store::Store};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let data_dir = std::env::var("LNRENT_DATA_DIR").unwrap_or_else(|_| "./data".into());
    std::fs::create_dir_all(&data_dir)?;
    let db_path = format!("{data_dir}/lnrent.sqlite");
    let store = Store::open_spawn(&db_path)?;
    tracing::info!(db = %db_path, "lnrentd state opened; store actor up (sole writer)");

    let recipes_dir = std::env::var("LNRENT_RECIPES_DIR").unwrap_or_else(|_| "./recipes".into());
    let recipes = match Recipe::load_all(&recipes_dir) {
        Ok(rs) => {
            tracing::info!(count = rs.len(), dir = %recipes_dir, "recipes loaded");
            for r in &rs {
                if let Err(e) = r.validate() {
                    tracing::warn!(id = %r.service.id, error = %e, "recipe failed validation");
                }
            }
            rs
        }
        Err(e) => {
            tracing::warn!(error = %e, dir = %recipes_dir, "no recipes loaded");
            Vec::new()
        }
    };

    // TODO M1: reconcile loop (§6.5), Nostr engine, payment watch — spawned alongside serve().
    let sock = format!("{data_dir}/lnrent.sock");
    tracing::info!(socket = %sock, "lnrentd up; serving operator IPC");
    ipc::serve(store, Arc::new(recipes), &sock).await
}
