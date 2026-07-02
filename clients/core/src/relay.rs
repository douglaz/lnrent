//! The injected I/O seams buyer-core needs from its host (SPEC.md §4.7, lnrent-7fp.13).
//!
//! Core is pure protocol: it builds [`lnrent_wire::Msg`]s, gift-wraps them, and correlates replies,
//! but it does no network I/O itself. The host (the native CLI today, the web client later) injects
//! a [`Relay`] for publish/fetch/subscribe and a [`Clock`] for time + fresh request ids. Keeping
//! these as traits is what lets the same flows run on a wasm32 web target with a NIP-07 signer and a
//! browser WebSocket — no native-only type leaks into core.

use lnrent_wire::{Event, PublicKey};
use std::time::Duration;

/// A transport-level relay failure (publish/fetch/subscribe). Always treated as retryable by the
/// buyer (the daemon/relay may recover); core wraps it into [`crate::BuyerError::Transport`].
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RelayError(pub String);

#[cfg(not(target_arch = "wasm32"))]
pub trait RelayBounds: Send + Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync> RelayBounds for T {}

#[cfg(target_arch = "wasm32")]
pub trait RelayBounds {}

#[cfg(target_arch = "wasm32")]
impl<T> RelayBounds for T {}

/// The relay seam: publish a signed event, fetch the operator's listings, and open a live
/// subscription to the buyer's gift-wrapped replies. The host owns the relay connection and the
/// concrete filters; core only drives this trait.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Relay: RelayBounds {
    /// Publish an already-signed event (a gift wrap or, in principle, any event) to the relay(s).
    async fn publish(&self, event: &Event) -> Result<(), RelayError>;

    /// Fetch the operator's currently-published NIP-99 30402 listing events (kind 30402, authored
    /// by `operator`), giving up after `timeout`. Core verifies + parses each (`parse_listing`).
    async fn fetch_listings(
        &self,
        operator: &PublicKey,
        timeout: Duration,
    ) -> Result<Vec<Event>, RelayError>;

    /// Open a live subscription to kind-1059 gift wraps addressed to `recipient` and return a
    /// stream the caller pulls events from until a deadline of `timeout` from now (then
    /// [`GiftWrapStream::next`] yields `None`). Subscribe BEFORE publishing a request so no reply is
    /// missed; the host is responsible for any registration settle delay.
    async fn subscribe_giftwraps(
        &self,
        recipient: &PublicKey,
        timeout: Duration,
    ) -> Result<Box<dyn GiftWrapStream>, RelayError>;
}

/// A live stream of gift-wrap events from [`Relay::subscribe_giftwraps`]. `next` returns the next
/// event, or `Ok(None)` once the subscription's deadline elapses (or it closes) — so core's
/// correlation loop terminates with a timeout instead of hanging.
// `Send` is a DIRECT supertrait on native (not via a marker trait) so it is reflected onto the
// `Box<dyn GiftWrapStream>` trait object `subscribe_giftwraps` returns — the buyer flow holds that box
// across `.next().await`, so it must stay `Send` to run on a multi-threaded executor. On wasm32 the
// stream owns non-`Send` JS handles (a `WebSocket`), so the trait is `?Send` and single-threaded.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
pub trait GiftWrapStream: Send {
    async fn next(&mut self) -> Result<Option<Event>, RelayError>;
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait GiftWrapStream {
    async fn next(&mut self) -> Result<Option<Event>, RelayError>;
}

/// The time / fresh-id seam. `now_secs` is wall-clock unix seconds; `new_request_id` mints a
/// unique request id for an `order.request` / `renew.request` / `op.request` (the operator dedupes
/// on `(sender, id)`, §5.1, so it MUST be unique — the native host uses real entropy, a test uses a
/// deterministic counter). Injected because both differ per target (native vs wasm32).
pub trait Clock: Send + Sync {
    fn now_secs(&self) -> i64;
    fn new_request_id(&self) -> String;
}
