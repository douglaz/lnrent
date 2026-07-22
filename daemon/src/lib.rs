//! lnrent control-plane library. Pure Rust, no LLM in the runtime path (SPEC.md §4.1).

/// GATE-1 alert dispatcher (lnrent-urw.1): a thin edge-triggered sink that surfaces
/// money/provisioning conditions as durable NIP-17 operator DMs. NOT a monitoring framework.
pub mod alerts;
pub mod backends;
/// COLD/OFFLINE operator backup + restore of the durable state (lnrent-7fp.14 PART A).
pub mod backup;
pub mod capture;
pub mod clock;
pub mod config;
pub mod domain;
/// Shared hardened data-dir path prep for the fedimint backend (lnrent-3d5).
#[cfg(feature = "fedimint")]
pub mod fedimint_paths;
/// The lnv2 Fedimint backend (lnrent-3d5, ADR-0018): the backend `payment_backend=fedimint`
/// constructs — the live ecash money path. Only when the `fedimint` feature is on (default ON;
/// build `--no-default-features` for the mock-only path). The retired lnv1 backend was deleted by
/// lnrent-8ym (ADR-0018).
#[cfg(feature = "fedimint")]
pub mod lnv2_backend;
pub mod identity;
pub mod ipc;
/// Ledger-authoritative money core (lnrent-urw.10): `expected_msat`, the LOCAL sqlite lower bound on
/// spendable wallet holdings that replaces the live federation balance in every automatic path.
pub mod ledger;
pub mod nostr_engine;
pub mod op_dispatch;
pub mod order_intake;
/// `lnrent preflight`/`doctor` (lnrent-y4m.9): probe the three EXTERNAL go-live dependencies
/// (gateway, federation, provider token) via the existing readiness seams — per-check pass/fail.
pub mod preflight;
pub mod provision;
pub mod recipe;
pub mod reconcile;
pub mod refund;
pub mod refund_resolver;
pub mod relay_status;
pub mod reservation;
pub mod resume;
pub mod runner;
pub mod store;
pub mod supervisor;
/// Operator sweep (gate1-operator-sweep, urw.3): a daemon-safe payout paying the operator's own
/// bolt11 from ledger SURPLUS (never a federation balance read), capped so it can never overspend.
pub mod sweep;
/// Orphaned-instance teardown dead-letter (lnrent-urw.2): surfaces + retries a failed `destroy` hook
/// so a droplet that failed to delete stops billing the operator invisibly.
pub mod teardown;
