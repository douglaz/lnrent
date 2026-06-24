//! lnrent control-plane library. Pure Rust, no LLM in the runtime path (SPEC.md §4.1).

pub mod backends;
pub mod clock;
pub mod domain;
pub mod recipe;
pub mod store;
