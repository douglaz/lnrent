//! The native I/O implementations buyer-core injects (lnrent-7fp.13): a [`NostrRelay`] backed by a
//! `nostr-sdk` client, and a [`SysClock`] for wall-clock time + random request ids. These are the
//! ONLY native-coupled pieces — everything protocol lives in `lnrent-buyer-core`.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use lnrent_buyer_core::{Clock, GiftWrapStream, Relay, RelayError};
use lnrent_wire::LISTING_KIND;
use nostr_sdk::prelude::*;

/// How long to let a fresh REQ register on the relay before we publish a request, so a fast reply
/// is not missed (mirrors the settle delay the operator's own engine tests rely on).
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(500);

/// A `nostr-sdk`-backed [`Relay`]: one client connected to the configured relay(s).
pub struct NostrRelay {
    client: Client,
}

impl NostrRelay {
    /// Connect to `url` with `keys` (used by the client for relay auth; gift-wrap sealing uses the
    /// signer buyer-core holds), waiting up to `timeout` for the connection.
    pub async fn connect(keys: Keys, url: &str, timeout: Duration) -> Result<Self, RelayError> {
        let client = Client::new(keys);
        client
            .add_relay(url)
            .await
            .map_err(|e| RelayError(format!("add relay {url}: {e}")))?;
        client.connect().await;
        client.wait_for_connection(timeout).await;
        Ok(Self { client })
    }
}

#[async_trait]
impl Relay for NostrRelay {
    async fn publish(&self, event: &Event) -> Result<(), RelayError> {
        self.client
            .send_event(event)
            .await
            .map_err(|e| RelayError(format!("publish: {e}")))?;
        Ok(())
    }

    async fn fetch_listings(
        &self,
        operator: &PublicKey,
        timeout: Duration,
    ) -> Result<Vec<Event>, RelayError> {
        let filter = Filter::new()
            .kind(Kind::Custom(LISTING_KIND))
            .author(*operator);
        let events = self
            .client
            .fetch_events(filter, timeout)
            .await
            .map_err(|e| RelayError(format!("fetch listings: {e}")))?;
        Ok(events.into_iter().collect())
    }

    async fn subscribe_giftwraps(
        &self,
        recipient: &PublicKey,
        timeout: Duration,
    ) -> Result<Box<dyn GiftWrapStream>, RelayError> {
        // Open the notification receiver BEFORE subscribing so replayed stored events + live events
        // are both delivered to it (a Nostr REQ returns stored matches, then live ones).
        let notifications = self.client.notifications();
        let filter = Filter::new().kind(Kind::GiftWrap).pubkey(*recipient);
        self.client
            .subscribe(filter, None)
            .await
            .map_err(|e| RelayError(format!("subscribe: {e}")))?;
        tokio::time::sleep(SUBSCRIBE_SETTLE).await;
        Ok(Box::new(NostrStream {
            notifications,
            deadline: Instant::now() + timeout,
        }))
    }
}

/// The live gift-wrap stream: drains relay-pool notifications, yielding kind-1059 events until the
/// deadline elapses (or the channel closes/lags), then `None`.
struct NostrStream {
    notifications: tokio::sync::broadcast::Receiver<RelayPoolNotification>,
    deadline: Instant,
}

#[async_trait]
impl GiftWrapStream for NostrStream {
    async fn next(&mut self) -> Result<Option<Event>, RelayError> {
        loop {
            let now = Instant::now();
            if now >= self.deadline {
                return Ok(None);
            }
            match tokio::time::timeout(self.deadline - now, self.notifications.recv()).await {
                // Deadline elapsed (no reply in time): end the stream → buyer-core surfaces a relay
                // timeout.
                Err(_) => return Ok(None),
                Ok(Ok(RelayPoolNotification::Event { event, .. })) => {
                    if event.kind == Kind::GiftWrap {
                        return Ok(Some(Event::clone(&event)));
                    }
                }
                // We fell behind the pool's broadcast buffer and lost some notifications, but the
                // operator's reply may still be buffered ahead — keep reading instead of aborting
                // the exchange as a spurious timeout.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                // The broadcast channel closed (pool shut down): nothing more will arrive.
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return Ok(None),
                Ok(Ok(_)) => {}
            }
        }
    }
}

/// System clock + request ids. The operator dedupes on `(sender, request_id)` (SPEC §5.1), so for a
/// fresh request the id must be unique: derive it from keypair entropy (rust-nostr's CSPRNG — no
/// extra dep). When the caller passes `--request-id` (an idempotency key for safe retries) that
/// fixed id is returned instead, so retrying a timed-out `order create` / `renew` / `ops` reuses the
/// operator's dedup key rather than creating a duplicate. One verb runs per process, so the id is
/// still minted exactly once.
pub struct SysClock {
    fixed_request_id: Option<String>,
}

impl SysClock {
    /// A clock that mints a fresh random request id per request (the default).
    pub fn new() -> Self {
        Self {
            fixed_request_id: None,
        }
    }

    /// A clock that returns `id` from `new_request_id` (the buyer's `--request-id` idempotency key),
    /// or a fresh random id when `id` is `None`.
    pub fn with_request_id(id: Option<String>) -> Self {
        Self {
            fixed_request_id: id,
        }
    }
}

impl Default for SysClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SysClock {
    fn now_secs(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
    fn new_request_id(&self) -> String {
        match &self.fixed_request_id {
            Some(id) => id.clone(),
            None => format!("req-{}", &Keys::generate().public_key().to_hex()[..24]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `--request-id` makes the minted id deterministic, so a retried order/renew/op reuses the
    // operator's `(sender, request_id)` dedup key instead of duplicating (the idempotency fix).
    #[test]
    fn fixed_request_id_is_stable_for_idempotent_retry() {
        let clock = SysClock::with_request_id(Some("req-fixed-123".into()));
        assert_eq!(clock.new_request_id(), "req-fixed-123");
        assert_eq!(clock.new_request_id(), "req-fixed-123");
    }

    // Without `--request-id`, each mint is a fresh unique id — distinct requests must NOT collide on
    // the operator's dedup key (SPEC §5.1).
    #[test]
    fn default_request_ids_are_unique() {
        let clock = SysClock::new();
        let a = clock.new_request_id();
        let b = clock.new_request_id();
        assert_ne!(a, b, "fresh request ids must differ");
        assert!(a.starts_with("req-"));
    }
}
