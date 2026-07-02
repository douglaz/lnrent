//! lnrent control-plane library. Pure Rust, no LLM in the runtime path (SPEC.md §4.1).

pub mod backends;
/// COLD/OFFLINE operator backup + restore of the durable state (lnrent-7fp.14 PART A).
pub mod backup;
pub mod capture;
pub mod clock;
pub mod config;
pub mod domain;
/// Real Fedimint backend (lnrent-7fp.4) — only when the `fedimint` feature is on (default OFF).
#[cfg(feature = "fedimint")]
pub mod fedimint_backend;
pub mod identity;
pub mod ipc;
pub mod nostr_engine;
pub mod op_dispatch;
pub mod order_intake;
pub mod provision;
pub mod recipe;
pub mod reconcile;
pub mod refund;
pub mod refund_resolver;
pub mod reservation;
pub mod resume;
pub mod runner;
pub mod store;
pub mod supervisor;
