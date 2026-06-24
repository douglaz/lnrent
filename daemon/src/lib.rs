//! lnrent control-plane library. Pure Rust, no LLM in the runtime path (SPEC.md §4.1).

pub mod backends;
pub mod capture;
pub mod clock;
pub mod domain;
pub mod ipc;
pub mod nostr_engine;
pub mod recipe;
pub mod reservation;
pub mod runner;
pub mod store;
