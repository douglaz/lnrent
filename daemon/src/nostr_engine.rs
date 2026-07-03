//! Operator-side Nostr engine (lnrent-7fp.5): the NIP-99 / NIP-17 **transport** primitives,
//! built ON the shared wire codec [`lnrent_wire`] (lnrent-7fp.19). It owns the relay plumbing,
//! not the message schema: it publishes the operator's 30402 listing, subscribes to inbound
//! gift-wrapped DMs, decodes + dedupes them, routes them to injected business handlers, and
//! offers the reply/publish primitive the durable outbox sender (lnrent-7fp.10) drains.
//!
//! What lives here is only transport (SPEC.md §5.1–§5.4, §7.4 "publish op declarations"):
//! - **Publish** the NIP-99 30402 listing (built via [`lnrent_wire::build_listing`], signed with
//!   the injected operator key) to every relay, carrying the operator tag + the recipe's
//!   `[[operation]]` declarations so buyers discover the ops surface (§7.4).
//! - **Subscribe** to inbound NIP-17 gift wraps (kind 1059) addressed to the operator, decode
//!   each via [`lnrent_wire::gift_unwrap`] (which authenticates the sender), and **dedupe** by the
//!   outer event id. The dedupe is durable (the `seen_message` table) but deliberately
//!   *best-effort*: a wrap is marked seen only AFTER its handler durably commits, so a crash
//!   mid-handling reprocesses it rather than silently dropping a money-path DM. The AUTHORITATIVE
//!   exactly-once guarantee lives one layer down in the business idempotency keys
//!   (`inbound_request` / `op_invocation`, SPEC.md §5.1), which write the cached response in the
//!   same transaction as the order/op effect; this transport dedupe just suppresses the common
//!   relay redelivery / restart replay cheaply (§5.1 "NIP-17 has no delivery guarantee", §5.2).
//! - **Route** decoded DMs to the injected [`OrderHandler`] (lnrent-7fp.17) and [`OpHandler`]
//!   (lnrent-7fp.20) seams; this bead provides the routing, not the order/op business logic.
//! - The **reply** primitive: gift-wrap a [`Msg`] to a buyer and publish it.
//!
//! Everything is injected for testability: the signer ([`Keys`], the operator account-0 key in
//! M1a — lnrent-7fp.16 derives the real one), the relay list, the [`Store`] handle, and the two
//! handler traits. The integration tests drive it over an in-process `nostr-relay-builder` relay.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use nostr_sdk::prelude::*;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use lnrent_wire::{
    build_listing, gift_unwrap, gift_wrap, Listing, Msg, OperationDecl, ParamDecl, Unwrapped,
};

use crate::recipe::{Operation, Param, Recipe};
use crate::store::Store;

/// How long [`NostrEngine::connect`] waits for the relay connections to come up before returning.
/// Bounded so a dead relay never stalls startup — publishing still queues until a relay attaches.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the transport-level outer-event dedupe rows are retained. Business idempotency lives
/// in `inbound_request` / `op_invocation`; this table only suppresses relay redeliveries of the
/// same gift wrap, so old audit rows can be pruned by local receipt time without trusting the
/// randomized gift-wrap `created_at`.
const SEEN_MESSAGE_RETENTION_SECS: i64 = 90 * 24 * 60 * 60;

/// The stable subscription id for the operator's inbound gift-wrap REQ. Reusing one id means a
/// resubscribe (e.g. after a broadcast-channel lag) REPLACES the prior REQ on each relay instead
/// of leaking a new one — NIP-01 overwrites a REQ that reuses a subscription id.
const INBOUND_SUB_ID: &str = "lnrent-inbound";

/// How long lag recovery waits for the relay's retained inbound set. This is an explicit fetch
/// because `nostr-sdk` only emits [`RelayPoolNotification::Event`] once per event id; retained
/// duplicates/backfills still arrive as raw [`RelayPoolNotification::Message`] values, and
/// the backfill streams each relay separately so the pool's cross-relay event-id dedupe cannot
/// hide a valid copy behind a malformed same-id copy from another relay.
const INBOUND_BACKFILL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the same-timestamp overflow fallback waits for a relay to answer the initial
/// negentropy handshake. Unsupported relays should fall back quickly to the plain timestamp walk.
const INBOUND_BACKFILL_NEGENTROPY_TIMEOUT: Duration = Duration::from_secs(2);

/// Page size for the lag/restart inbound backfill REQ. A NIP-17 gift wrap's `created_at` is
/// randomized (§5.1) so the backfill cannot key a `since` cursor on it; instead it pages the
/// retained set newest→oldest with an explicit `limit` and a backward `until` cursor. Relays
/// clamp an ABSENT limit (the in-process `nostr-relay-builder` relay defaults to 500), so a single
/// unpaged fetch would let an attacker bury an older RETAINED money-path DM behind more than this
/// many newer `#p` gift-wrap garbage events — a permanently-lost order/op DM. Paging with an
/// explicit limit and a backward cursor walks past that garbage; the durable `seen_message` dedupe
/// makes the per-page boundary overlap cheap to re-decrypt.
const INBOUND_BACKFILL_PAGE_LIMIT: usize = 500;

/// Exact-id chunk size used only after negentropy identifies a same-timestamp overflow that cannot
/// be paged with a plain `until` cursor. Keep this below common relay max limits so exact-id fetches
/// are not clamped.
const INBOUND_BACKFILL_ID_CHUNK_LIMIT: usize = 100;

/// Upper bound on backfill pages PER RELAY so the paged backfill still terminates against a relay
/// that returns an unbounded stream of full pages (e.g. an attacker minting `#p` gift-wrap garbage
/// faster than we page). [`INBOUND_BACKFILL_PAGE_LIMIT`] events per page × this cap is far above any
/// realistic retained set (bounded by [`SEEN_MESSAGE_RETENTION_SECS`]), so a legitimate backlog is
/// never truncated; hitting the cap is logged, never silent.
const INBOUND_BACKFILL_MAX_PAGES: usize = 256;

/// Upper bound on inbound gift wraps decoded + routed concurrently. The inbound loop acquires a
/// permit before spawning a per-wrap task, so a startup backfill — or a flood of attacker-minted
/// kind-1059 wraps (the `#p` recipient tag is public, the signer ephemeral) — applies backpressure:
/// the loop stops draining the notification stream until a slot frees, instead of spawning one
/// unbounded task per retained event. Overflowing the broadcast buffer while blocked lags us into
/// explicit relay backfill, which the durable dedupe makes cheap.
const MAX_INBOUND_CONCURRENCY: usize = 32;

/// Bounds the in-memory negative cache (see [`NegativeCache`]). Sized to comfortably hold the
/// working set of recently-redelivered wraps; distinct forged ids past this just evict the oldest,
/// so memory stays capped while the common redelivery short-circuits. Backlogs larger than this
/// cache can still re-decrypt the overflow on a later reconnect/lag backfill.
const NEGATIVE_CACHE_CAPACITY: usize = 4096;

/// How many times [`NostrEngine::run_inbound`] retries the *initial* inbound subscribe before
/// giving up. A transient relay hiccup right after `connect` must not permanently stop inbound DM
/// routing; if every attempt fails the call returns an error for the supervisor (lnrent-7fp.10) to
/// restart the task.
const INBOUND_SUBSCRIBE_ATTEMPTS: u32 = 5;

/// Delay between [`INBOUND_SUBSCRIBE_ATTEMPTS`] retries of the inbound subscribe.
const INBOUND_SUBSCRIBE_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Backoff before resubscribing after a relay ENDs our inbound REQ server-side (`CLOSED`).
/// `subscribe_with_id` only enqueues the REQ, so a resubscribe "succeeds" immediately; a relay
/// that keeps rejecting (auth-required, rate-limited, invalid filter) would otherwise busy-loop
/// CLOSED→resubscribe→CLOSED. This dampens that to a slow, logged retry that still recovers a
/// transient closure quickly without trusting the resubscribe's optimistic success.
const INBOUND_CLOSED_BACKOFF: Duration = Duration::from_secs(1);

/// Maximum per-relay backoff after repeated server-side `CLOSED`s of the inbound subscription.
const INBOUND_CLOSED_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// The reply/publish primitive handed to inbound handlers: gift-wrap an lnrent [`Msg`] to a buyer
/// and publish it to the relays. The engine implements it over its relay client; handlers take it
/// as a `&dyn Outbound` so they can answer a buyer (order.invoice, op.result, …) without owning
/// the transport. The durable outbox sender (lnrent-7fp.10) calls the engine's primitive directly.
#[async_trait]
pub trait Outbound: Send + Sync {
    /// Gift-wrap `msg` to `recipient` and publish it; returns the published gift-wrap event id.
    async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId>;
}

/// Injected seam for buyer→operator **order / billing** DMs — `order.request`, `renew.request`,
/// `sub.cancel`, `delivery.resend.request` (SPEC.md §5.1). Implemented by the order-intake bead
/// (lnrent-7fp.17); this engine only decodes/dedupes/routes the transport. `out` is the reply
/// primitive the handler answers through (a `delivery.resend.request` prompts a `provision.ready`
/// redelivery; a `renew.request` is renewal-invoice resync only).
#[async_trait]
pub trait OrderHandler: Send + Sync {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()>;
}

/// Injected seam for buyer→operator `op.request` **management** DMs (SPEC.md §5.1, §7.4).
/// Implemented by the op-dispatch bead (lnrent-7fp.20); the engine routes here after decode +
/// dedupe. The business idempotency on `(sender, request_id)` lives in that bead's `op_invocation`
/// rows — this layer's dedupe only drops true relay-level redeliveries of the same gift wrap.
#[async_trait]
pub trait OpHandler: Send + Sync {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()>;
}

/// The operator's Nostr transport. Cheap to clone — the relay [`Client`], the [`Keys`], the
/// [`Store`] handle, and the three `Arc`-shared coordination primitives are all shareable — so a
/// clone can drive the inbound loop (or one spawned per-wrap task) on its own and still share the
/// SAME in-flight set, concurrency cap, and negative cache.
#[derive(Clone)]
pub struct NostrEngine {
    client: Client,
    keys: Keys,
    store: Store,
    /// Collapses CONCURRENT deliveries of the same outer event id (the N-relay fan-out delivers
    /// each wrap once per relay, near-simultaneously) so only one task decodes + routes it in this
    /// process. Ephemeral on purpose: a crash drops it, and recovery then leans on the durable
    /// `seen_message` row (for a wrap a prior run completed) or the business idempotency layer (for
    /// one it didn't) — exactly the cases an in-memory claim must NOT durably suppress.
    in_flight: Arc<Mutex<HashSet<EventId>>>,
    /// Bounds the inbound decode/route fan-out (see [`MAX_INBOUND_CONCURRENCY`]).
    inbound_sem: Arc<Semaphore>,
    /// Short-circuits recently re-fetched non-routable wraps when a reconnect/lag backfill sees the
    /// relay's retained set again. Bounded + ephemeral, so it never durably suppresses a wrap.
    negative_cache: Arc<Mutex<NegativeCache>>,
    /// Engine-owned ownership/drain state for inbound tasks accepted by the live subscription and
    /// retained-set helpers.
    inbound_tasks: Arc<InboundTaskState>,
}

/// Result of a bounded inbound shutdown drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundDrainResult {
    /// The inbound loop stopped accepting and all accepted per-wrap tasks finished.
    Drained,
    /// The deadline expired; straggler inbound work was aborted so process exit cannot hang.
    TimedOut,
}

impl NostrEngine {
    /// Connect the operator engine to `relays`, signing with `keys` (the operator account-0 key in
    /// M1a; the real key is derived by lnrent-7fp.16 and injected here). Adds every relay and opens
    /// the connections so the engine can immediately publish and subscribe. Waits up to
    /// [`CONNECT_TIMEOUT`] for the connections; a slow/dead relay does not fail the call (the
    /// pool keeps retrying and queues until one attaches), but at least one relay must be supplied.
    pub async fn connect(keys: Keys, relays: &[String], store: Store) -> Result<Self> {
        if relays.is_empty() {
            return Err(anyhow!("nostr engine needs at least one relay"));
        }
        let client = Client::new(keys.clone());
        for url in relays {
            client
                .add_relay(url.as_str())
                .await
                .with_context(|| format!("adding relay {url}"))?;
        }
        client.connect().await;
        client.wait_for_connection(CONNECT_TIMEOUT).await;
        Ok(Self {
            client,
            keys,
            store,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            inbound_sem: Arc::new(Semaphore::new(MAX_INBOUND_CONCURRENCY)),
            negative_cache: Arc::new(Mutex::new(NegativeCache::default())),
            inbound_tasks: Arc::new(InboundTaskState::new()),
        })
    }

    /// The operator's public key (the listing signer + the `#p` recipient inbound DMs address).
    pub fn operator_pubkey(&self) -> PublicKey {
        self.keys.public_key()
    }

    /// Publish (or replace) the operator's NIP-99 30402 listing, signed with the engine's key, to
    /// every relay. A 30402 is a replaceable event keyed on `(30402, pubkey, d)`, so republishing
    /// the same `d` REPLACES the prior listing — price edits keep the coordinate
    /// `30402:<pubkey>:<d>` stable (§5.4). The listing carries the operator tag and the recipe's
    /// published `[[operation]]` declarations (§7.4); the operator-internal `hook` is never in it.
    /// Returns the published event id.
    pub async fn publish_listing(&self, listing: &Listing) -> Result<EventId> {
        let builder = build_listing(listing).map_err(|e| anyhow!("building 30402 listing: {e}"))?;
        // `send_event_builder` signs with the client's signer (= the engine's operator key) and
        // broadcasts to every relay.
        let output = self
            .client
            .send_event_builder(builder)
            .await
            .context("publishing 30402 listing")?;
        require_relay_acceptance(&output, "publishing 30402 listing")?;
        Ok(output.val)
    }

    /// The reply/publish primitive: gift-wrap `msg` to `recipient` and publish it to every relay,
    /// returning the published gift-wrap event id. This is the single send path — the inbound
    /// handlers answer through it (as [`Outbound`]) and the durable outbox sender (lnrent-7fp.10)
    /// calls it directly. Publishing to multiple relays is the engine's drop-tolerance (§5.2).
    pub async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
        self.send_wrap(recipient, msg).await
    }

    /// Subscribe to inbound NIP-17 gift wraps (kind 1059) addressed to the operator, then decode +
    /// dedupe + route each until the relay client shuts down. Decoding authenticates the sender
    /// (the seal); the dedupe is durable (so a restart never re-routes a delivered DM); routing
    /// hands order/billing DMs to `order` and `op.request` to `op`. Spawn this on its own task.
    pub async fn run_inbound(
        &self,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<()> {
        if self.inbound_tasks.is_stopping() {
            return Ok(());
        }
        let mut notifications = self.client.notifications();
        let closed_resubscriptions: ClosedResubscriptions = Arc::new(Mutex::new(HashMap::new()));
        // Retry the initial subscribe: a transient relay hiccup right after `connect` must not
        // permanently silence inbound routing. If it ultimately fails the call returns the error
        // for the caller's supervisor (lnrent-7fp.10) to restart the task — the lag-recovery
        // resubscribe below is only logged because by then the loop is already running and the next
        // lag (or relay reconnect) re-triggers it.
        self.subscribe_inbound_with_retry().await?;
        if self.inbound_tasks.is_stopping() {
            return Ok(());
        }
        // A live subscription replay can be clamped by the relay's default limit because the
        // long-lived REQ intentionally carries no limit, so run the explicit paged retained-set
        // fetch on startup too. SPAWN it rather than await it: the paged backfill can walk up to
        // [`INBOUND_BACKFILL_MAX_PAGES`] sequential pages per relay against a large or adversarial
        // retained set, and awaiting it here would block entering the live recv loop — silencing
        // fresh DM routing behind that fetch. The CLOSED-recovery path already spawns its backfill
        // for exactly this reason, and the in-flight + durable seen-message dedupe collapses the
        // overlap between this backfill and the live subscription replay.
        {
            let engine = self.clone();
            let order = Arc::clone(&order);
            let op = Arc::clone(&op);
            self.spawn_inbound_aux(async move {
                if let Err(e) = engine.fetch_inbound_backlog(order, op).await {
                    tracing::error!(
                        error = %e,
                        "failed to fetch inbound backlog after initial subscribe"
                    );
                }
            });
        }
        loop {
            let notification = tokio::select! {
                biased;
                _ = self.inbound_tasks.stop.cancelled() => break,
                notification = notifications.recv() => notification,
            };
            match notification {
                Ok(RelayPoolNotification::Message { relay_url, message }) => match message {
                    // The raw relay EVENT for our REQ — the source we route off (fires for every
                    // relay copy, including saved duplicates, unlike `Event` below). The guard drops
                    // EVENTs for any other subscription id.
                    RelayMessage::Event {
                        subscription_id,
                        event,
                    } if subscription_id.as_ref().as_str() == INBOUND_SUB_ID => {
                        // An EVENT under our sub id proves this relay's inbound subscription is live
                        // again, so reset its CLOSED-resubscribe backoff (a recovered relay must
                        // return to fast retry, not stay pinned at the max backoff from old
                        // transient closures).
                        reset_closed_resubscribe(&closed_resubscriptions, &relay_url);
                        self.queue_inbound_event(
                            event.into_owned(),
                            Arc::clone(&order),
                            Arc::clone(&op),
                        )
                        .await?;
                    }
                    // A relay can END our long-lived inbound REQ server-side (`CLOSED`):
                    // auth-required, rate-limited, or an invalid filter. `subscribe_with_id` only
                    // enqueued the REQ, so it reported success and never observed this — and without
                    // handling it here that relay silently stops delivering money-path order/op DMs.
                    // Schedule a per-relay resubscribe off the recv loop: reusing the stable
                    // [`INBOUND_SUB_ID`] REPLACES the REQ on that relay rather than leaking one,
                    // while healthy relays keep draining normally. Repeated closures are coalesced
                    // and backed off per relay; a hard retry failure is logged and the next
                    // CLOSED/lag/reconnect re-triggers recovery.
                    RelayMessage::Closed {
                        subscription_id,
                        message,
                    } if subscription_id.as_ref().as_str() == INBOUND_SUB_ID => {
                        if let Some(backoff) =
                            schedule_closed_resubscribe(&closed_resubscriptions, &relay_url)
                        {
                            tracing::warn!(
                                relay = %relay_url,
                                reason = %message,
                                backoff_ms = backoff.as_millis(),
                                "relay closed inbound subscription; scheduling per-relay resubscribe"
                            );
                            let engine = self.clone();
                            let closed_resubscriptions = Arc::clone(&closed_resubscriptions);
                            let order = Arc::clone(&order);
                            let op = Arc::clone(&op);
                            self.spawn_inbound_aux(async move {
                                let mut backoff = backoff;
                                loop {
                                    tokio::select! {
                                        biased;
                                        _ = engine.inbound_tasks.stop.cancelled() => break,
                                        _ = tokio::time::sleep(backoff) => {}
                                    }
                                    if engine.inbound_tasks.is_stopping() {
                                        break;
                                    }
                                    let recovered = match engine
                                        .subscribe_inbound_to_with_retry(relay_url.clone())
                                        .await
                                    {
                                        Ok(()) => {
                                            if let Err(e) = engine
                                                .fetch_inbound_backlog_from_url(
                                                    relay_url.clone(),
                                                    Arc::clone(&order),
                                                    Arc::clone(&op),
                                                )
                                                .await
                                            {
                                                tracing::error!(
                                                    relay = %relay_url,
                                                    error = %e,
                                                    "failed to fetch inbound backlog after CLOSED recovery"
                                                );
                                            }
                                            true
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                relay = %relay_url,
                                                error = %e,
                                                "failed to resubscribe relay after inbound CLOSED"
                                            );
                                            false
                                        }
                                    };

                                    if let Some(next_backoff) = finish_closed_resubscribe(
                                        &closed_resubscriptions,
                                        &relay_url,
                                        recovered,
                                    ) {
                                        tracing::warn!(
                                            relay = %relay_url,
                                            backoff_ms = next_backoff.as_millis(),
                                            "relay closed inbound subscription during pending recovery; scheduling another retry"
                                        );
                                        backoff = next_backoff;
                                    } else {
                                        break;
                                    }
                                }
                            });
                        } else {
                            tracing::debug!(
                                relay = %relay_url,
                                reason = %message,
                                "relay closed inbound subscription; resubscribe already pending"
                            );
                        }
                    }
                    // EOSE for our REQ confirms the relay accepted the (re)subscription and finished
                    // replaying its retained set — the subscription is healthy — so reset the
                    // per-relay CLOSED-resubscribe backoff (same recovery signal as a live EVENT,
                    // and it fires even when the retained set is empty so there is no EVENT to ride).
                    RelayMessage::EndOfStoredEvents(subscription_id)
                        if subscription_id.as_ref().as_str() == INBOUND_SUB_ID =>
                    {
                        reset_closed_resubscribe(&closed_resubscriptions, &relay_url);
                    }
                    // Every other relay message (OK, NOTICE, AUTH, COUNT, messages for other
                    // subscription ids, …) is irrelevant to inbound DM routing.
                    _ => {}
                },
                // `Event` is intentionally ignored: nostr-sdk emits it only the first time the
                // relay pool sees an event id, so it is the wrong source for retained/backfilled
                // money-path DMs after lag or handler retry. The raw `Message` arm above fires for
                // every relay EVENT, including saved duplicates.
                Ok(RelayPoolNotification::Event { .. }) => {}
                Ok(RelayPoolNotification::Shutdown) => break,
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(skipped)) => {
                    // The pool's broadcast buffer overflowed for this slow consumer, so we skipped
                    // `skipped` notifications — which may include money-path order.request /
                    // op.request DMs. NIP-17 has no delivery guarantee, but relays RETAIN gift
                    // wraps, so explicitly fetch the retained inbound set and feed it through the
                    // same decode/dedupe/route path. Re-subscribing keeps the live REQ fresh, but
                    // recovery does NOT rely on `RelayPoolNotification::Event`: nostr-sdk only
                    // emits that variant once per event id, while raw relay messages / fetches
                    // still surface saved duplicates (§5.1, §5.2).
                    tracing::warn!(
                        skipped,
                        "inbound notification stream lagged; fetching retained inbound DMs"
                    );
                    let engine = self.clone();
                    let order = Arc::clone(&order);
                    let op = Arc::clone(&op);
                    self.spawn_inbound_aux(async move {
                        if let Err(e) = engine.fetch_inbound_backlog(order, op).await {
                            tracing::error!(error = %e, "failed to fetch inbound backlog after lag");
                        }
                        if engine.inbound_tasks.is_stopping() {
                            return;
                        }
                        if let Err(e) = engine.subscribe_inbound_with_retry().await {
                            tracing::error!(error = %e, "failed to resubscribe after inbound lag");
                        }
                    });
                }
            }
        }
        Ok(())
    }

    /// Stop the inbound receive loop, wait for its accepted event handoffs to be registered in the
    /// engine-owned tracker, then drain accepted per-wrap work within `deadline`.
    ///
    /// The caller must pass the supervisor-owned `run_inbound` join handle. Awaiting that handle is the
    /// proof that the live recv loop has stopped accepting and no accepted live wrap remains between
    /// relay notification and tracker registration.
    pub async fn drain(
        &self,
        deadline: Duration,
        inbound: &mut JoinHandle<Result<()>>,
    ) -> InboundDrainResult {
        let deadline_at = Instant::now() + deadline;
        self.start_inbound_drain_until(deadline_at).await;

        match tokio::time::timeout_at(deadline_at, &mut *inbound).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => tracing::warn!(
                error = %format!("{e:#}"),
                "inbound loop returned an error during shutdown drain"
            ),
            Ok(Err(join)) if join.is_panic() => {
                tracing::error!("inbound loop panicked during shutdown drain")
            }
            Ok(Err(join)) => tracing::warn!(
                error = %join,
                "inbound loop was cancelled during shutdown drain"
            ),
            Err(_) => {
                inbound.abort();
                // Wait for the aborted accept loop to actually stop BEFORE snapshotting the per-wrap
                // handles: abort() is cooperative, so run_inbound can still finish a synchronous
                // spawn_inbound_wrap (past the semaphore acquire) after the deadline. Joining it first
                // guarantees that last-spawned handler is registered in the tracker and included in the
                // abort snapshot below, not left detached past the final flush.
                let _ = (&mut *inbound).await;
                let handlers = self.inbound_tasks.abort_per_wrap();
                self.inbound_tasks.close_per_wrap();
                tracing::error!(
                    handlers,
                    "inbound drain timed out waiting for the accept loop; aborting accepted handlers"
                );
                return InboundDrainResult::TimedOut;
            }
        }

        self.drain_per_wrap_until(deadline_at).await
    }

    /// Drain accepted per-wrap work after the inbound loop has already returned. This is used only
    /// by the supervisor's shutdown race path where the child completed before the shutdown arm won.
    pub(crate) async fn drain_after_inbound_stopped(
        &self,
        deadline: Duration,
    ) -> InboundDrainResult {
        let deadline_at = Instant::now() + deadline;
        self.start_inbound_drain_until(deadline_at).await;
        self.drain_per_wrap_until(deadline_at).await
    }

    async fn start_inbound_drain_until(&self, deadline_at: Instant) {
        self.inbound_tasks.stop_accepting();

        let aux = self.inbound_tasks.abort_aux();
        if aux > 0 {
            tracing::debug!(
                tasks = aux,
                "aborted auxiliary inbound backlog/resubscribe tasks"
            );
        }
        self.inbound_tasks.close_aux();
        if tokio::time::timeout_at(deadline_at, self.inbound_tasks.wait_aux())
            .await
            .is_err()
        {
            tracing::warn!("inbound auxiliary tasks did not stop within the drain deadline");
        }
    }

    async fn drain_per_wrap_until(&self, deadline_at: Instant) -> InboundDrainResult {
        self.inbound_tasks.close_per_wrap();
        match tokio::time::timeout_at(deadline_at, self.inbound_tasks.wait_per_wrap()).await {
            Ok(()) => InboundDrainResult::Drained,
            Err(_) => {
                let handlers = self.inbound_tasks.abort_per_wrap();
                tracing::error!(
                    handlers,
                    "inbound drain deadline expired; aborting straggler handlers"
                );
                InboundDrainResult::TimedOut
            }
        }
    }

    #[cfg(test)]
    fn inbound_aux_task_count_for_test(&self) -> usize {
        self.inbound_tasks.aux_count()
    }

    #[cfg(test)]
    pub(crate) async fn queue_inbound_event_for_test(
        &self,
        event: Event,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<()> {
        self.queue_inbound_event(event, order, op).await
    }

    /// Decode, dedupe, and route a single inbound gift wrap — the unit [`run_inbound`] runs per
    /// delivered event, exposed so the dedupe is testable deterministically and an alternate driver
    /// (e.g. a durable inbox) can feed events directly. Returns `true` if the DM was routed to a
    /// handler that committed, or `false` if it was a duplicate (concurrent or already-handled) or
    /// an ignored operator→buyer response. The wrap's `created_at` is never trusted for timing — it
    /// is randomized (§5.1); the dedupe key is the stable outer event id, and any expiry lives in
    /// the payload (handled downstream by lnrent-7fp.17 / .20).
    ///
    /// Dedupe layering (SPEC.md §5.1): the durable `seen_message` row is written only AFTER the
    /// handler returns Ok, so the guarantee this transport provides is *at-least-once* delivery to
    /// the handler plus best-effort suppression of redeliveries — NOT exactly-once on its own. A
    /// crash between the handler's business commit and the seen-write reprocesses the wrap; the
    /// handler's `inbound_request` / `op_invocation` idempotency (same-transaction cached response)
    /// is what makes that safe and is the authoritative exactly-once boundary. An in-memory
    /// in-flight claim additionally collapses simultaneous multi-relay copies in one process, and a
    /// bounded in-memory negative cache short-circuits recently-seen wraps that already resolved as
    /// non-routable (decode failure / non-buyer type) — neither is durable, so a restart correctly
    /// re-evaluates.
    pub async fn process_inbound(
        &self,
        wrap: &Event,
        order: &dyn OrderHandler,
        op: &dyn OpHandler,
    ) -> Result<bool> {
        // Verify the outer event BEFORE any id-based suppression. The Nostr event id does not
        // commit to the signature, so a malformed relay copy with the same fields but a bogus
        // signature must not claim/cache the id and suppress a valid copy from another relay.
        verify_inbound_wrap(wrap)?;

        // Collapse CONCURRENT deliveries first (in-memory, instant): hold the claim for the whole
        // call so a simultaneous copy from another relay is dropped instead of re-running a
        // possibly non-idempotent handler. The RAII guard frees the claim on every exit path
        // (decode error, handler error, success). Held BEFORE the durable check so that once we own
        // the claim, no concurrent task can be mid-handling this id — any other copy is either
        // dropped here or already finished (and thus visible to `is_seen` below).
        let _in_flight = match InFlight::claim(&self.in_flight, wrap.id) {
            Some(guard) => guard,
            None => {
                tracing::debug!(event_id = %wrap.id, "duplicate inbound DM ignored (in flight)");
                return Ok(false);
            }
        };

        // In-memory negative cache: a wrap we already resolved as non-routable (failed to decode,
        // or decoded to a non-buyer type we never act on) short-circuits HERE — before the durable
        // read and the NIP-44 decrypt — so a relay reconnect / lag backfill that re-fetches the
        // retained set can skip recently-seen garbage. Bounded + ephemeral by design (see
        // [`NegativeCache`]); handler ERRORS are never cached here, so a transient failure still
        // reprocesses on the buyer's resync (at-least-once delivery).
        if self.is_negative_cached(wrap.id) {
            tracing::debug!(event_id = %wrap.id, "duplicate inbound DM ignored (non-routable, cached)");
            return Ok(false);
        }

        // Durable best-effort dedupe: a wrap whose handler already committed (its `seen_message`
        // row is written only on success, below) never re-routes — cheap read so the common case,
        // a relay redelivering or a restart replaying a finished wrap, short-circuits before
        // decryption.
        if self.is_seen(wrap.id).await? {
            tracing::debug!(event_id = %wrap.id, "duplicate inbound DM ignored (already handled)");
            return Ok(false);
        }

        // `gift_unwrap` verifies the outer kind-1059 envelope AND authenticates the sender from the
        // inner seal before returning the lnrent message. A decode failure is NOT recorded in
        // `seen_message`: that durable table would let a peer flooding distinct forged wraps fill it
        // (one row each), which is worse than re-verifying the bounded set of redelivered bad wraps.
        // Instead it goes in the bounded in-memory negative cache, which caps the re-decrypt a
        // reconnect/lag backfill would otherwise repeat over the whole retained garbage set without
        // the table-filling problem. Still surfaced as an Err so the inbound loop logs the drop once;
        // the cache suppresses the repeats.
        let Unwrapped { sender, msg } = match gift_unwrap(&self.keys, wrap).await {
            Ok(unwrapped) => unwrapped,
            Err(e) => {
                self.note_non_routable(wrap.id);
                return Err(anyhow!("decoding inbound gift wrap {}: {e}", wrap.id));
            }
        };
        // Captured before the handler consumes `msg`/`sender`, for the post-success seen-write.
        let msg_type = msg.type_str().to_string();
        let sender_hex = sender.to_hex();

        let routed = match &msg {
            Msg::OrderRequest(_)
            | Msg::RenewRequest(_)
            | Msg::SubCancel(_)
            | Msg::DeliveryResendRequest(_) => order.handle(sender, msg, self).await.map(|()| true),
            Msg::OpRequest(_) => op.handle(sender, msg, self).await.map(|()| true),
            // Operator→buyer responses never legitimately arrive inbound; never act on a spoofed or
            // confused peer's copy — log and drop without recording it in `seen_message` (same flood
            // reasoning as a decode failure). It IS noted in the bounded in-memory negative cache, so
            // a backfill replay of this copy isn't re-decoded + re-routed every reconnect/lag.
            other => {
                tracing::warn!(sender = %sender, msg = other.type_str(), "ignoring non-buyer inbound DM");
                self.note_non_routable(wrap.id);
                Ok(false)
            }
        };
        match routed {
            // The handler ran and durably committed: record the wrap so a later relay redelivery —
            // or a restart — short-circuits before re-decoding/re-routing it. A handler error (or a
            // deliberately-dropped non-buyer DM) writes NO row, so the wrap is reprocessed on
            // redelivery/resend and the business idempotency layer makes that re-run safe.
            Ok(true) => {
                self.mark_seen(wrap.id, sender_hex, msg_type).await?;
                Ok(true)
            }
            Ok(false) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Subscribe to gift wraps (kind 1059) tagged to the operator's pubkey (`#p`), the NIP-17
    /// recipient tag. A long-lived subscription under the stable [`INBOUND_SUB_ID`] — the inbound
    /// loop reads raw relay `EVENT` messages off the pool's notification stream, and a lag-recovery
    /// resubscribe reuses the same id so it replaces (not leaks) the REQ.
    ///
    /// Deliberately carries no `since`/`limit`: a NIP-17 gift wrap's `created_at` is randomized up
    /// to two days into the past (§5.1) to defeat timing analysis, so a `since` cursor keyed on it
    /// would silently drop legitimately-recent DMs that happen to carry an older timestamp. We
    /// re-fetch the relay's retained backlog instead and lean on the durable `seen_message` dedupe
    /// to make the replay cheap and exactly-once; the same reason we never trust `created_at` for
    /// expiry (that lives in the payload, handled by lnrent-7fp.17 / .20). Because a `limit` is
    /// off the table for the same timing reason, the bounded in-memory negative cache (see
    /// [`NegativeCache`]) is what caps re-decrypt of the backlog's *non-routable* portion (forged /
    /// non-buyer wraps that leave no `seen_message` row) across repeated backfills.
    async fn subscribe_inbound(&self) -> Result<()> {
        let filter = self.inbound_filter();
        self.client
            .subscribe_with_id(SubscriptionId::new(INBOUND_SUB_ID), filter, None)
            .await
            .context("subscribing to inbound DMs")
            .and_then(|output| require_relay_acceptance(&output, "subscribing to inbound DMs"))?;
        Ok(())
    }

    fn inbound_filter(&self) -> Filter {
        Filter::new()
            .kind(Kind::GiftWrap)
            .pubkey(self.operator_pubkey())
    }

    async fn queue_inbound_event(
        &self,
        event: Event,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<()> {
        // Re-enforce the operator's inbound filter on the RAW relay-message path before any
        // verify/decrypt work. The REQ already filters to kind 1059 tagged to the operator, but a
        // malicious or noncompliant relay can push arbitrary events under our subscription id; an
        // unchecked kind-1059 without our `#p` tag would force needless signature-verify + NIP-44
        // decrypt. Match the same [`inbound_filter`](Self::inbound_filter) (kind 1059 AND a `p` tag
        // equal to the operator pubkey) so the engine enforces its own filter boundary. (`#p` is
        // public, so this is not full DoS defense — it just keeps the engine honest about what it
        // accepts under its subscription.)
        if !self
            .inbound_filter()
            .match_event(&event, MatchEventOptions::new())
        {
            tracing::debug!(
                event_id = %event.id,
                kind = %event.kind,
                "dropping inbound event that does not match the operator inbound filter"
            );
            return Ok(());
        }
        // Backpressure: block until a concurrency slot frees BEFORE spawning, so a relay backfill
        // or a flood can't grow memory with one task per retained event. While we wait here the
        // notification stream stops draining; if its buffer then overflows, the lag branch fetches
        // the relay's retained set and feeds those events through this same queue.
        let permit = Arc::clone(&self.inbound_sem)
            .acquire_owned()
            .await
            .context("inbound semaphore closed")?;
        let engine = self.clone();
        self.spawn_inbound_wrap(async move {
            // Hold the permit for the task's lifetime so the slot frees only when this wrap is
            // fully processed.
            let _permit = permit;
            if let Err(e) = engine
                .process_inbound(&event, order.as_ref(), op.as_ref())
                .await
            {
                // A single bad/undecodable DM must not kill the loop; the buyer resyncs.
                tracing::warn!(event_id = %event.id, error = %e, "inbound DM dropped");
            }
        });
        Ok(())
    }

    fn spawn_inbound_wrap<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.inbound_tasks.per_wrap.spawn(task);
        self.inbound_tasks
            .track_per_wrap_abort(handle.abort_handle());
        drop(handle);
    }

    fn spawn_inbound_aux<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inbound_tasks.spawn_aux(task)
    }

    async fn fetch_inbound_backlog(
        &self,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<()> {
        if self.inbound_tasks.is_stopping() {
            return Ok(());
        }
        let relays = self.client.relays().await;
        if relays.is_empty() {
            bail!("streaming retained inbound DMs: no relays configured");
        }

        let mut tasks = Vec::with_capacity(relays.len());
        for (relay_url, relay) in relays {
            if self.inbound_tasks.is_stopping() {
                break;
            }
            let engine = self.clone();
            let order = Arc::clone(&order);
            let op = Arc::clone(&op);
            let filter = self.inbound_filter();
            tasks.push(self.spawn_inbound_aux(async move {
                engine
                    .fetch_inbound_backlog_from_relay(relay_url, relay, filter, order, op)
                    .await
            }));
        }

        let mut count = 0usize;
        let mut failures = 0usize;
        for task in tasks {
            match task.await {
                Ok(Ok(n)) => count += n,
                Ok(Err(e)) => {
                    failures += 1;
                    tracing::error!(error = %e, "failed to stream retained inbound DMs from relay");
                }
                Err(e) => {
                    failures += 1;
                    tracing::error!(error = %e, "retained inbound DM relay stream task failed");
                }
            }
        }
        tracing::info!(events = count, failures, "queued inbound backlog");
        Ok(())
    }

    async fn fetch_inbound_backlog_from_url(
        &self,
        relay_url: RelayUrl,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<usize> {
        if self.inbound_tasks.is_stopping() {
            return Ok(0);
        }
        let mut relays = self.client.relays().await;
        let relay = relays.remove(&relay_url).with_context(|| {
            format!("streaming retained inbound DMs from {relay_url}: relay not configured")
        })?;
        self.fetch_inbound_backlog_from_relay(relay_url, relay, self.inbound_filter(), order, op)
            .await
    }

    async fn fetch_inbound_backlog_from_relay(
        &self,
        relay_url: RelayUrl,
        relay: Relay,
        base_filter: Filter,
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<usize> {
        // Page the retained set newest→oldest with an explicit page limit and a backward `until`
        // cursor. A NIP-17 gift wrap's `created_at` is randomized (§5.1), so the backfill cannot key
        // a `since` cursor on it; it walks `until` backward instead, deduping the inclusive
        // page-boundary overlap by event id (the durable `seen_message` row still covers anything a
        // prior run handled).
        //
        // Termination must NOT assume "a page shorter than the REQUESTED limit ⇒ exhausted". A relay
        // clamps a requested limit to its own maximum (NIP-11 `max_limit`, or the in-process relay's
        // `max_filter_limit`), which is not portably readable and a noncompliant relay need not
        // advertise. If the walk stopped on `page_items < INBOUND_BACKFILL_PAGE_LIMIT`, a relay that
        // clamps below that limit would make EVERY page look short and the walk would stop after the
        // newest page — letting an attacker bury an older RETAINED money-path DM behind clamp-many
        // newer `#p` garbage events (a permanently-lost order/op DM). Instead we keep walking while
        // the `until` cursor advances to a strictly-older second, and stop only when it STALLS on a
        // single `created_at` second. At a stall, timestamp paging cannot cross that second, so we
        // tell a benign short tail apart from a same-second overflow by whether the stalled page is
        // as full as the LARGEST page the relay has handed us (its OBSERVED clamp — not our request):
        // a full pinned page may be hiding more same-second events behind the clamp, so reconcile
        // exact ids via negentropy; a short pinned page is the whole tail at that second, so stop.
        // `INBOUND_BACKFILL_MAX_PAGES` bounds the walk against an infinite relay.
        let mut until: Option<Timestamp> = None;
        let mut seen_ids: HashSet<EventId> = HashSet::new();
        // The (id, created_at) of every wrap queued this backfill — handed to the negentropy
        // reconciliation as OUR local set (see [`fetch_inbound_backlog_via_negentropy`]).
        let mut local_items: Vec<(EventId, Timestamp)> = Vec::new();
        // The largest page the relay has actually returned, i.e. its observed clamp. Compared
        // against a stalled page's size to tell a benign exhausted tail from a same-second overflow.
        let mut observed_page_limit = 0usize;
        let mut count = 0usize;
        for page in 0..INBOUND_BACKFILL_MAX_PAGES {
            if self.inbound_tasks.is_stopping() {
                return Ok(count);
            }
            let mut filter = base_filter.clone().limit(INBOUND_BACKFILL_PAGE_LIMIT);
            if let Some(until) = until {
                filter = filter.until(until);
            }
            let mut events = tokio::select! {
                biased;
                _ = self.inbound_tasks.stop.cancelled() => return Ok(count),
                events = relay.stream_events(filter, INBOUND_BACKFILL_TIMEOUT, ReqExitPolicy::ExitOnEOSE) => {
                    events.with_context(|| format!("streaming retained inbound DMs from {relay_url}"))?
                }
            };

            let mut page_items = 0usize;
            let mut oldest: Option<Timestamp> = None;
            while let Some(event) = tokio::select! {
                biased;
                _ = self.inbound_tasks.stop.cancelled() => return Ok(count),
                event = events.next() => event,
            } {
                page_items += 1;
                match event {
                    Ok(event) => {
                        oldest = Some(oldest.map_or(event.created_at, |t| t.min(event.created_at)));
                        // Dedup the deliberate cursor overlap (and any relay-side duplicate) by id so
                        // the same wrap isn't re-queued across pages; the durable `seen_message`
                        // dedupe still covers wraps a prior backfill/run already handled.
                        if seen_ids.insert(event.id) {
                            local_items.push((event.id, event.created_at));
                            self.queue_inbound_event(event, Arc::clone(&order), Arc::clone(&op))
                                .await?;
                            count += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            relay = %relay_url,
                            error = %e,
                            "relay returned an invalid retained inbound DM"
                        );
                    }
                }
            }
            observed_page_limit = observed_page_limit.max(page_items);

            // An empty page means the relay has nothing at/below the cursor: the walk is exhausted.
            if page_items == 0 {
                return Ok(count);
            }
            // A non-empty page with no valid (deserializable) event leaves no timestamp to move the
            // cursor to; stop rather than re-fetch the identical page forever.
            let Some(oldest) = oldest else {
                return Ok(count);
            };

            // The cursor advanced iff this page reached a strictly-older second than the cursor
            // (always true on the first, cursor-less page). Keep walking regardless of how short the
            // page is — the relay may simply be clamping it below our requested limit.
            let cursor_advanced = match until {
                Some(until) => oldest < until,
                None => true,
            };
            if cursor_advanced {
                until = Some(oldest);
                if page + 1 == INBOUND_BACKFILL_MAX_PAGES {
                    tracing::warn!(
                        relay = %relay_url,
                        pages = INBOUND_BACKFILL_MAX_PAGES,
                        queued = count,
                        "inbound backfill hit the per-relay page cap; stopping (retained set exceeds cap)"
                    );
                }
                continue;
            }

            // Cursor stalled: every event in this page shares `oldest == until`, so timestamp paging
            // cannot move past this second. A page SHORTER than the observed clamp is the relay's
            // whole (short) tail at this second → exhausted.
            if page_items < observed_page_limit {
                return Ok(count);
            }
            // A FULL pinned page may be hiding more same-second events behind the relay's clamp (the
            // buried-money-DM attack), so reconcile the exact missing ids via negentropy.
            tracing::warn!(
                relay = %relay_url,
                queued = count,
                timestamp = %oldest,
                "inbound backfill cursor stalled on a single timestamp; reconciling exact ids"
            );
            match self
                .fetch_inbound_backlog_via_negentropy(
                    &relay_url,
                    &relay,
                    base_filter.clone(),
                    &local_items,
                    Arc::clone(&order),
                    Arc::clone(&op),
                )
                .await
            {
                Ok(n) => count += n,
                Err(e) => {
                    // Negentropy is a best-effort enhancement for same-second overflow; a relay that
                    // does not support it (or times out the handshake) must not discard the events we
                    // already paged. Log and return what we queued rather than failing the backfill.
                    tracing::warn!(
                        relay = %relay_url,
                        error = %e,
                        queued = count,
                        timestamp = %oldest,
                        "same-timestamp inbound backfill reconciliation failed; returning paged events"
                    );
                }
            }
            return Ok(count);
        }
        Ok(count)
    }

    async fn fetch_inbound_backlog_via_negentropy(
        &self,
        relay_url: &RelayUrl,
        relay: &Relay,
        filter: Filter,
        local_items: &[(EventId, Timestamp)],
        order: Arc<dyn OrderHandler>,
        op: Arc<dyn OpHandler>,
    ) -> Result<usize> {
        if self.inbound_tasks.is_stopping() {
            return Ok(0);
        }
        // Use negentropy only as an ID reconciliation step. Letting the SDK perform the "down"
        // fetch internally makes the backfill depend on that helper's subscription bookkeeping; by
        // running it as a dry run and doing our own exact-id fetches below, the transport keeps the
        // same bounded chunking and queue/dedupe path as ordinary backfill pages.
        //
        // Seed reconciliation with OUR queued `(id, created_at)` set (`local_items`), NOT the SDK
        // client's event cache. `relay.sync` derives the local set from `database().negentropy_items`
        // — the client database — which can already hold a wrap the live subscription saved but this
        // engine lagged on and never routed. Such a wrap would then be absent from `remote` and never
        // re-fetched: a permanently-lost money-path DM. `sync_with_items` makes `remote` exactly the
        // relay's retained set minus what we have queued, independent of the client cache.
        let opts = SyncOptions::new()
            .initial_timeout(INBOUND_BACKFILL_NEGENTROPY_TIMEOUT)
            .direction(SyncDirection::Down)
            .dry_run();
        let id_filter = filter.clone();
        let reconciliation = tokio::select! {
            biased;
            _ = self.inbound_tasks.stop.cancelled() => return Ok(0),
            reconciliation = relay.sync_with_items(filter, local_items.to_vec(), &opts) => {
                reconciliation.with_context(|| {
                    format!("negentropy reconciling retained inbound DMs from {relay_url}")
                })?
            }
        };

        if reconciliation.remote.is_empty() {
            return Ok(0);
        }

        // Dedup the exact-id fetches against what paging already queued (the `local_items` ids),
        // so an event the relay also returned via the timestamp walk isn't re-queued here. Owned by
        // this step rather than threaded in from the caller — the caller returns immediately after.
        let mut seen_ids: HashSet<EventId> = local_items.iter().map(|&(id, _)| id).collect();
        let mut count = 0usize;
        let ids: Vec<EventId> = reconciliation.remote.into_iter().collect();
        for chunk in ids.chunks(INBOUND_BACKFILL_ID_CHUNK_LIMIT) {
            if self.inbound_tasks.is_stopping() {
                return Ok(count);
            }
            let mut events = tokio::select! {
                biased;
                _ = self.inbound_tasks.stop.cancelled() => return Ok(count),
                events = relay.stream_events(
                    id_filter.clone().ids(chunk.iter().copied()),
                    INBOUND_BACKFILL_TIMEOUT,
                    ReqExitPolicy::ExitOnEOSE,
                ) => {
                    events.with_context(|| {
                        format!("fetching exact retained inbound DM ids from {relay_url}")
                    })?
                }
            };
            while let Some(event) = tokio::select! {
                biased;
                _ = self.inbound_tasks.stop.cancelled() => return Ok(count),
                event = events.next() => event,
            } {
                match event {
                    Ok(event) => {
                        if seen_ids.insert(event.id) {
                            self.queue_inbound_event(event, Arc::clone(&order), Arc::clone(&op))
                                .await?;
                            count += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            relay = %relay_url,
                            error = %e,
                            "relay returned an invalid exact-id retained inbound DM"
                        );
                    }
                }
            }
        }
        Ok(count)
    }

    /// Gift-wrap `msg` from the operator key to `recipient` and publish it to every relay. The
    /// single send path behind both [`reply`](Self::reply) and the [`Outbound`] impl.
    async fn send_wrap(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
        let wrap = gift_wrap(&self.keys, recipient, msg)
            .await
            .map_err(|e| anyhow!("gift-wrapping {}: {e}", msg.type_str()))?;
        let output = self
            .client
            .send_event(&wrap)
            .await
            .with_context(|| format!("publishing {} DM", msg.type_str()))?;
        require_relay_acceptance(&output, &format!("publishing {} DM", msg.type_str()))?;
        Ok(wrap.id)
    }

    /// Record a fully-handled wrap's outer event id in `seen_message` so a later relay redelivery —
    /// or a restart — short-circuits before re-decoding/re-routing it. Called ONLY after the handler
    /// returned Ok, so a crash mid-handling leaves no row and the wrap is reprocessed (the business
    /// idempotency layer makes that re-run safe). `ON CONFLICT DO NOTHING` keeps it idempotent, and
    /// the same transaction prunes rows past [`SEEN_MESSAGE_RETENTION_SECS`] by local receipt time
    /// (never the randomized gift-wrap `created_at`).
    async fn mark_seen(
        &self,
        event_id: EventId,
        sender_hex: String,
        msg_type: String,
    ) -> Result<()> {
        let event_id = event_id.to_hex();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "DELETE FROM seen_message WHERE seen_at < unixepoch() - ?1",
                    rusqlite::params![SEEN_MESSAGE_RETENTION_SECS],
                )?;
                tx.execute(
                    "INSERT INTO seen_message (event_id, sender, msg_type, seen_at)
                     VALUES (?1, ?2, ?3, unixepoch())
                     ON CONFLICT(event_id) DO NOTHING",
                    rusqlite::params![event_id, sender_hex, msg_type],
                )?;
                Ok(())
            })
            .await
    }

    /// Cheap durable check for already-handled wraps. Authoritative across restarts (the row is
    /// durable); within one process the in-flight claim collapses concurrent copies, so a row only
    /// ever exists for a wrap whose handler already committed.
    async fn is_seen(&self, event_id: EventId) -> Result<bool> {
        let event_id = event_id.to_hex();
        self.store
            .read(move |conn| {
                let n: i64 = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM seen_message WHERE event_id = ?1)",
                    rusqlite::params![event_id],
                    |row| row.get(0),
                )?;
                Ok(n != 0)
            })
            .await
    }

    /// True if `id` already resolved as non-routable (see [`NegativeCache`]); checked before the
    /// NIP-44 decrypt so a backfill replay of forged/garbage or spoofed wraps short-circuits cheaply
    /// in-memory, without a store read.
    fn is_negative_cached(&self, id: EventId) -> bool {
        self.negative_cache
            .lock()
            .expect("negative-cache mutex poisoned")
            .contains(&id)
    }

    /// Record `id` as non-routable so a later backfill/redelivery skips re-decoding it (see
    /// [`NegativeCache`]). Called ONLY for a decode failure or a non-buyer message type — never for
    /// a handler error, which must stay retryable so the buyer's resync still reaches the handler.
    fn note_non_routable(&self, id: EventId) {
        self.negative_cache
            .lock()
            .expect("negative-cache mutex poisoned")
            .insert(id);
    }

    /// Subscribe to inbound DMs, retrying transient failures up to [`INBOUND_SUBSCRIBE_ATTEMPTS`]
    /// times. Used for both the initial subscribe (whose ultimate failure propagates so a
    /// supervisor restarts the task) and the lag-recovery resubscribe (whose ultimate failure is
    /// tolerated by the caller).
    async fn subscribe_inbound_with_retry(&self) -> Result<()> {
        let mut attempt = 1;
        loop {
            match self.subscribe_inbound().await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < INBOUND_SUBSCRIBE_ATTEMPTS => {
                    tracing::warn!(attempt, error = %e, "inbound subscribe failed; retrying");
                    tokio::time::sleep(INBOUND_SUBSCRIBE_RETRY_DELAY).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn subscribe_inbound_to(&self, relay_url: RelayUrl) -> Result<()> {
        let action = format!("subscribing to inbound DMs on {relay_url}");
        let filter = self.inbound_filter();
        let output = self
            .client
            .subscribe_with_id_to(
                std::iter::once(relay_url),
                SubscriptionId::new(INBOUND_SUB_ID),
                filter,
                None,
            )
            .await
            .with_context(|| action.clone())?;
        require_relay_acceptance(&output, &action)?;
        Ok(())
    }

    async fn subscribe_inbound_to_with_retry(&self, relay_url: RelayUrl) -> Result<()> {
        let mut attempt = 1;
        loop {
            match self.subscribe_inbound_to(relay_url.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < INBOUND_SUBSCRIBE_ATTEMPTS => {
                    tracing::warn!(
                        relay = %relay_url,
                        attempt,
                        error = %e,
                        "per-relay inbound subscribe failed; retrying"
                    );
                    tokio::time::sleep(INBOUND_SUBSCRIBE_RETRY_DELAY).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

type ClosedResubscriptions = Arc<Mutex<HashMap<RelayUrl, ClosedResubscribe>>>;

#[derive(Default)]
struct ClosedResubscribe {
    attempts: u32,
    pending: bool,
    retry_requested: bool,
}

fn schedule_closed_resubscribe(
    closed: &ClosedResubscriptions,
    relay_url: &RelayUrl,
) -> Option<Duration> {
    let mut closed = closed
        .lock()
        .expect("closed-resubscribe map mutex poisoned");
    let state = closed.entry(relay_url.clone()).or_default();
    if state.pending {
        state.retry_requested = true;
        return None;
    }

    let delay = inbound_closed_backoff(state.attempts);
    state.attempts = state.attempts.saturating_add(1);
    state.pending = true;
    state.retry_requested = false;
    Some(delay)
}

fn finish_closed_resubscribe(
    closed: &ClosedResubscriptions,
    relay_url: &RelayUrl,
    healthy: bool,
) -> Option<Duration> {
    if let Ok(mut closed) = closed.lock() {
        if let Some(state) = closed.get_mut(relay_url) {
            if state.retry_requested {
                state.retry_requested = false;
                let delay = inbound_closed_backoff(state.attempts);
                state.attempts = state.attempts.saturating_add(1);
                state.pending = true;
                return Some(delay);
            }
            state.pending = false;
            if healthy {
                state.attempts = 0;
            }
        }
    }
    None
}

/// Reset a relay's CLOSED-resubscribe backoff after its inbound subscription proves healthy again
/// (an EVENT or EOSE under [`INBOUND_SUB_ID`]). [`finish_closed_resubscribe`] only clears the
/// in-flight `pending` flag, so without this a relay that recovered from old transient closures
/// would stay pinned at [`INBOUND_CLOSED_MAX_BACKOFF`] forever; zeroing `attempts` returns it to
/// fast retry on the next closure. The `pending` flag is left untouched so an in-flight resubscribe
/// task still clears it on completion. A no-op for a relay that never closed (no map entry).
fn reset_closed_resubscribe(closed: &ClosedResubscriptions, relay_url: &RelayUrl) {
    if let Ok(mut closed) = closed.lock() {
        if let Some(state) = closed.get_mut(relay_url) {
            state.attempts = 0;
            state.retry_requested = false;
        }
    }
}

fn inbound_closed_backoff(attempts: u32) -> Duration {
    let multiplier = 1u64 << attempts.min(16);
    let secs = INBOUND_CLOSED_BACKOFF
        .as_secs()
        .max(1)
        .saturating_mul(multiplier)
        .min(INBOUND_CLOSED_MAX_BACKOFF.as_secs());
    Duration::from_secs(secs)
}

/// RAII claim on an in-flight outer event id. Inserted into the engine's in-memory set on
/// [`claim`](InFlight::claim) and removed on drop, so every exit path of
/// [`NostrEngine::process_inbound`] (decode error, handler error, success) frees the slot. Purely
/// in-memory: a crash drops the whole set, which is correct — post-restart redeliveries must be
/// reprocessed (then deduped by the durable `seen_message` row, or recovered by the business layer).
struct InFlight {
    set: Arc<Mutex<HashSet<EventId>>>,
    id: EventId,
}

impl InFlight {
    /// Try to claim `id`. Returns `Some(guard)` if no other task is currently processing it, or
    /// `None` if a concurrent delivery already holds the claim.
    fn claim(set: &Arc<Mutex<HashSet<EventId>>>, id: EventId) -> Option<InFlight> {
        let mut guard = set.lock().expect("in-flight set mutex poisoned");
        guard.insert(id).then(|| InFlight {
            set: Arc::clone(set),
            id,
        })
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.id);
        }
    }
}

/// Bounded, in-memory set of outer gift-wrap event ids that already resolved as NON-routable — a
/// wrap that failed to decode (forged/garbage kind-1059) or decoded to a message type the operator
/// never acts on inbound (a spoofed operator→buyer response). Those leave NO durable `seen_message`
/// row on purpose — recording them would let a peer flooding distinct wraps fill that table — so
/// without this cache every relay reconnect or broadcast-lag
/// [`fetch_inbound_backlog`](NostrEngine::fetch_inbound_backlog) would re-decrypt and re-route the
/// entire retained garbage set. The cache short-circuits that repeat work before the expensive
/// NIP-44 decrypt for the recent working set; if the retained garbage set grows past
/// [`NEGATIVE_CACHE_CAPACITY`], evicted ids can be re-decrypted on a later backfill. It is
/// deliberately:
/// - **bounded** (FIFO eviction past [`NEGATIVE_CACHE_CAPACITY`]) so a flood of distinct forged ids
///   caps pinned memory rather than growing unbounded;
/// - **ephemeral** (a clone shares it; a restart drops it) so it never durably suppresses a wrap —
///   the durable `seen_message` row and the business idempotency layer own that;
/// - **never populated on a handler error** — only on decode failure or a non-buyer type — so a
///   transient handler failure still reprocesses on the buyer's resync (at-least-once delivery).
#[derive(Default)]
struct NegativeCache {
    members: HashSet<EventId>,
    order: VecDeque<EventId>,
}

impl NegativeCache {
    fn contains(&self, id: &EventId) -> bool {
        self.members.contains(id)
    }

    /// Record `id` as non-routable, evicting the oldest entry once past
    /// [`NEGATIVE_CACHE_CAPACITY`]. Idempotent: re-inserting a present id neither duplicates it nor
    /// reorders eviction.
    fn insert(&mut self, id: EventId) {
        if self.members.insert(id) {
            self.order.push_back(id);
            if self.order.len() > NEGATIVE_CACHE_CAPACITY {
                if let Some(evicted) = self.order.pop_front() {
                    self.members.remove(&evicted);
                }
            }
        }
    }
}

#[async_trait]
impl Outbound for NostrEngine {
    async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
        NostrEngine::reply(self, recipient, msg).await
    }
}

struct InboundTaskState {
    stop: CancellationToken,
    per_wrap: TaskTracker,
    aux: TaskTracker,
    per_wrap_aborts: Mutex<Vec<AbortHandle>>,
    aux_aborts: Mutex<Vec<AbortHandle>>,
}

impl InboundTaskState {
    fn new() -> Self {
        Self {
            stop: CancellationToken::new(),
            per_wrap: TaskTracker::new(),
            aux: TaskTracker::new(),
            per_wrap_aborts: Mutex::new(Vec::new()),
            aux_aborts: Mutex::new(Vec::new()),
        }
    }

    fn is_stopping(&self) -> bool {
        self.stop.is_cancelled()
    }

    fn stop_accepting(&self) {
        self.stop.cancel();
    }

    fn close_per_wrap(&self) {
        self.per_wrap.close();
    }

    async fn wait_per_wrap(&self) {
        self.per_wrap.wait().await;
    }

    fn close_aux(&self) {
        self.aux.close();
    }

    async fn wait_aux(&self) {
        self.aux.wait().await;
    }

    fn track_per_wrap_abort(&self, abort: AbortHandle) {
        track_abort(&self.per_wrap_aborts, abort);
    }

    fn spawn_aux<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.aux.spawn(task);
        track_abort(&self.aux_aborts, handle.abort_handle());
        handle
    }

    fn abort_aux(&self) -> usize {
        abort_tracked(&self.aux_aborts)
    }

    fn abort_per_wrap(&self) -> usize {
        abort_tracked(&self.per_wrap_aborts)
    }

    #[cfg(test)]
    fn aux_count(&self) -> usize {
        live_abort_count(&self.aux_aborts)
    }
}

fn track_abort(handles: &Mutex<Vec<AbortHandle>>, abort: AbortHandle) {
    let mut handles = handles.lock().expect("inbound abort list mutex poisoned");
    handles.retain(|handle| !handle.is_finished());
    handles.push(abort);
}

fn abort_tracked(handles: &Mutex<Vec<AbortHandle>>) -> usize {
    let mut handles = handles.lock().expect("inbound abort list mutex poisoned");
    let live = handles
        .iter()
        .filter(|handle| !handle.is_finished())
        .count();
    for handle in handles.drain(..) {
        handle.abort();
    }
    live
}

#[cfg(test)]
fn live_abort_count(handles: &Mutex<Vec<AbortHandle>>) -> usize {
    let mut handles = handles.lock().expect("inbound abort list mutex poisoned");
    handles.retain(|handle| !handle.is_finished());
    handles.len()
}

fn verify_inbound_wrap(wrap: &Event) -> Result<()> {
    if wrap.kind != Kind::GiftWrap {
        bail!("inbound event {} is not a gift wrap", wrap.id);
    }
    wrap.verify()
        .map_err(|e| anyhow!("verifying inbound gift wrap {}: {e}", wrap.id))
}

fn require_relay_acceptance<T: std::fmt::Debug>(output: &Output<T>, action: &str) -> Result<()> {
    if output.success.is_empty() {
        tracing::warn!(
            action,
            failures = ?output.failed,
            "nostr operation reached no relays"
        );
        bail!("no relays accepted {action}; failures: {:?}", output.failed);
    }
    if !output.failed.is_empty() {
        tracing::warn!(
            action,
            accepted_relays = output.success.len(),
            failed_relays = output.failed.len(),
            failures = ?output.failed,
            "nostr operation failed on some relays"
        );
    }
    Ok(())
}

/// Build a publishable [`Listing`] for `recipe`, priced from its own `[[pricing]]`, for
/// [`NostrEngine::publish_listing`]. Carries the `operator` tag (the master pubkey, hex) and the
/// recipe's published `[[operation]]` declarations — minus the operator-internal `hook`, which is
/// never serialized into a listing (§5.4, §7.4). `d` is the NIP-99 replaceable-event identifier;
/// the listing's coordinate is then `30402:<operator_pubkey>:<d>`.
pub fn listing_from_recipe(
    recipe: &Recipe,
    d: impl Into<String>,
    operator_master_hex: impl Into<String>,
) -> Listing {
    Listing {
        d: d.into(),
        operator: operator_master_hex.into(),
        recipe_id: recipe.service.id.clone(),
        recipe_version: recipe.service.version.clone(),
        title: recipe.service.name.clone(),
        summary: recipe.service.summary.clone(),
        amount_sat: recipe.pricing.amount_sat,
        period: recipe.pricing.period.clone(),
        params: recipe.params.iter().map(param_decl).collect(),
        operations: recipe.operations.iter().map(op_decl).collect(),
        // The honest security tier is a VM-rental concept (ADR-0007, §9.1): only VM listings carry
        // it. A non-VM service (isolation none/container) declares no meaningful tier, so the
        // listing advertises `None` rather than a misleading default "0" (wire `Listing.tier` doc).
        tier: (recipe.provisioning.isolation == "vm").then(|| recipe.provisioning.tier.clone()),
        version: lnrent_wire::SCHEMA_VERSION,
    }
}

/// Map a recipe `[[params]]` entry to the listing's published [`ParamDecl`].
fn param_decl(p: &Param) -> ParamDecl {
    ParamDecl {
        key: p.key.clone(),
        label: p.label.clone(),
        ty: p.ty.clone(),
        required: p.required,
    }
}

/// Map a recipe `[[operation]]` to its published [`OperationDecl`], dropping the internal `hook`.
fn op_decl(op: &Operation) -> OperationDecl {
    OperationDecl {
        name: op.name.clone(),
        label: op.label.clone(),
        kind: op.kind.clone(),
        params: op.params.iter().map(param_decl).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nostr_relay_builder::builder::{PolicyResult, QueryPolicy, RateLimit};
    use nostr_relay_builder::{LocalRelay, MockRelay, RelayBuilder};
    use nostr_sdk::nostr::nips::nip44;
    use tokio::time::timeout;

    const TEST_DEADLINE: Duration = Duration::from_secs(10);
    static RELAY_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Start a local `MockRelay`, retrying the upstream port race (`nostr-relay-builder` 0.44 picks
    /// a random port, drops its probe listener, then re-binds — a TOCTOU window where a concurrent
    /// relay in the parallel test run can grab that port and the bind fails with `AddrInUse`).
    async fn mock_relay() -> MockRelay {
        let mut last_err = None;
        for _ in 0..10 {
            match MockRelay::run().await {
                Ok(relay) => return relay,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!("local relay failed after retries: {}", last_err.unwrap());
    }

    /// A relay [`QueryPolicy`] that rejects the FIRST REQ it sees (the relay answers `CLOSED`) and
    /// accepts every later one — so a test can drive the engine's CLOSED-recovery path: the initial
    /// inbound subscribe is ended server-side, and the engine must observe it, back off, and
    /// resubscribe before any inbound DM is delivered.
    #[derive(Debug, Default)]
    struct RejectFirstQuery {
        seen: AtomicUsize,
    }

    impl QueryPolicy for RejectFirstQuery {
        fn admit_query<'a>(
            &'a self,
            _query: &'a Filter,
            _addr: &'a SocketAddr,
        ) -> BoxedFuture<'a, PolicyResult> {
            let first = self.seen.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                if first {
                    PolicyResult::Reject("test rejects the first REQ".into())
                } else {
                    PolicyResult::Accept
                }
            })
        }
    }

    /// Start a local relay that CLOSEs the first REQ then accepts (see [`RejectFirstQuery`]),
    /// retrying the same upstream port race as [`mock_relay`]. Returns the running relay (keep it
    /// alive for the test's duration) and its url.
    async fn rejecting_relay() -> (LocalRelay, String) {
        let mut last_err = None;
        for _ in 0..10 {
            let relay =
                LocalRelay::new(RelayBuilder::default().query_policy(RejectFirstQuery::default()));
            match relay.run().await {
                Ok(()) => {
                    let url = relay.url().await.to_string();
                    return (relay, url);
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!(
            "local rejecting relay failed after retries: {}",
            last_err.unwrap()
        );
    }

    /// Start a local relay whose per-connection event rate limit is high enough to accept a large
    /// retained set in one test. The default `nostr-relay-builder` limit is 60 notes/minute, which
    /// silently drops most of a > 500-event publish as `OK: false` (and `send_event` does not raise
    /// that as an error) — so without this the paged-backfill test would never retain more than one
    /// page. Retries the same upstream port race as [`mock_relay`].
    async fn high_throughput_relay() -> (LocalRelay, String) {
        let mut last_err = None;
        for _ in 0..10 {
            let relay = LocalRelay::new(RelayBuilder::default().rate_limit(RateLimit {
                max_reqs: 500,
                notes_per_minute: 1_000_000,
            }));
            match relay.run().await {
                Ok(()) => {
                    let url = relay.url().await.to_string();
                    return (relay, url);
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!(
            "local high-throughput relay failed after retries: {}",
            last_err.unwrap()
        );
    }

    /// Start a local relay that CLAMPS any requested filter limit down to `max_filter_limit` (the
    /// load-shedding public relay whose NIP-11 `max_limit` is below the backfill page size) while
    /// still accepting a large retained set. The default `nostr-relay-builder` relay leaves
    /// `max_filter_limit` unset, so it honors our `limit(INBOUND_BACKFILL_PAGE_LIMIT)` and the
    /// clamped-walk path never triggers — this helper forces the clamp. Retries the same upstream
    /// port race as [`mock_relay`].
    async fn clamped_relay(max_filter_limit: usize) -> (LocalRelay, String) {
        let mut last_err = None;
        for _ in 0..10 {
            let relay = LocalRelay::new(
                RelayBuilder::default()
                    .max_filter_limit(max_filter_limit)
                    .rate_limit(RateLimit {
                        max_reqs: 500,
                        notes_per_minute: 1_000_000,
                    }),
            );
            match relay.run().await {
                Ok(()) => {
                    let url = relay.url().await.to_string();
                    return (relay, url);
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        panic!(
            "local clamped relay failed after retries: {}",
            last_err.unwrap()
        );
    }

    async fn buyer_client(url: &str, keys: Keys) -> Client {
        let client = Client::new(keys);
        client.add_relay(url).await.expect("buyer add relay");
        client.connect().await;
        client.wait_for_connection(TEST_DEADLINE).await;
        client
    }

    async fn gift_wrap_at(
        sender: &Keys,
        recipient: &PublicKey,
        msg: &Msg,
        created_at: Timestamp,
    ) -> Event {
        let content = serde_json::to_string(msg).expect("serialize lnrent DM");
        let rumor = EventBuilder::private_msg_rumor(*recipient, content).build(sender.public_key());
        let seal = EventBuilder::seal(sender, recipient, rumor)
            .await
            .expect("build seal")
            .sign(sender)
            .await
            .expect("sign seal");
        let wrap_keys = Keys::generate();
        let content = nip44::encrypt(
            wrap_keys.secret_key(),
            recipient,
            seal.as_json(),
            nip44::Version::default(),
        )
        .expect("encrypt seal");
        EventBuilder::new(Kind::GiftWrap, content)
            .tag(Tag::public_key(*recipient))
            .custom_created_at(created_at)
            .sign_with_keys(&wrap_keys)
            .expect("sign gift wrap")
    }

    struct CountingOrderHandler {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderHandler for CountingOrderHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct NoopOpHandler;

    #[async_trait]
    impl OpHandler for NoopOpHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            Ok(())
        }
    }

    struct GateOrderHandler {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderHandler for GateOrderHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one(); // sticky: retains a permit so the test's notified() can't miss the start signal
            self.release.notified().await;
            Ok(())
        }
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct NeverOrderHandler {
        started: Arc<tokio::sync::Notify>,
        dropped: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderHandler for NeverOrderHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            let _drop = DropCounter(Arc::clone(&self.dropped));
            self.started.notify_one(); // sticky: retains a permit so the test's notified() can't miss the start signal
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    fn order_request(id: &str, op_key: PublicKey) -> Msg {
        Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: id.into(),
            listing_id: format!("30402:{}:dummy-1", op_key.to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        })
    }

    #[test]
    fn all_failed_relay_output_is_an_error() {
        let output = Output::<()>::default();
        let err = require_relay_acceptance(&output, "publishing test")
            .expect_err("zero accepted relays must fail");
        assert!(
            err.to_string().contains("no relays accepted"),
            "unexpected error: {err}"
        );
    }

    // P3b (lnrent-7fp.5, codex xhigh): repeated server-side CLOSEDs climb the per-relay backoff, but
    // a recovered relay must return to fast retry. `finish_closed_resubscribe` only clears `pending`,
    // so without `reset_closed_resubscribe` a relay with old transient closures stays pinned at the
    // max backoff forever. An inbound EVENT/EOSE for our subscription resets the attempt counter.
    #[test]
    fn inbound_activity_resets_closed_resubscribe_backoff() {
        let closed: ClosedResubscriptions = Arc::new(Mutex::new(HashMap::new()));
        let relay = RelayUrl::parse("wss://relay.example").expect("relay url");

        // Three handled closures climb the backoff (each schedule bumps `attempts`, each finish
        // clears `pending` so the next closure can schedule again).
        let first = schedule_closed_resubscribe(&closed, &relay).expect("first schedule");
        finish_closed_resubscribe(&closed, &relay, false);
        schedule_closed_resubscribe(&closed, &relay).expect("second schedule");
        finish_closed_resubscribe(&closed, &relay, false);
        let third = schedule_closed_resubscribe(&closed, &relay).expect("third schedule");
        finish_closed_resubscribe(&closed, &relay, false);
        assert!(
            third > first,
            "backoff must grow across repeated closures: {third:?} !> {first:?}"
        );

        // Recovery: an inbound EVENT/EOSE for this relay's subscription resets the attempt counter,
        // so the next closure backs off from the base again instead of the climbed maximum.
        reset_closed_resubscribe(&closed, &relay);
        let after_recovery =
            schedule_closed_resubscribe(&closed, &relay).expect("schedule after recovery");
        assert_eq!(
            after_recovery, first,
            "a recovered relay returns to the base backoff after EVENT/EOSE"
        );
    }

    // Reviewer 1 P2: if another CLOSED arrives while the previous CLOSED recovery task is still
    // pending, it must not be swallowed. The pending task should schedule one more retry when it
    // finishes, otherwise the relay can remain unsubscribed forever.
    #[test]
    fn closed_during_pending_recovery_schedules_followup_retry() {
        let closed: ClosedResubscriptions = Arc::new(Mutex::new(HashMap::new()));
        let relay = RelayUrl::parse("wss://relay.example").expect("relay url");

        let first = schedule_closed_resubscribe(&closed, &relay).expect("first schedule");
        assert!(
            schedule_closed_resubscribe(&closed, &relay).is_none(),
            "a second CLOSED while recovery is pending is coalesced, not spawned immediately"
        );

        let followup = finish_closed_resubscribe(&closed, &relay, true)
            .expect("pending CLOSED schedules a follow-up retry");
        assert!(
            followup > first,
            "follow-up retry should continue the backoff sequence: {followup:?} !> {first:?}"
        );

        assert!(
            finish_closed_resubscribe(&closed, &relay, true).is_none(),
            "once the follow-up finishes quietly, no extra retry remains"
        );
        let after_quiet =
            schedule_closed_resubscribe(&closed, &relay).expect("schedule after quiet recovery");
        assert_eq!(
            after_quiet, first,
            "a quiet successful recovery resets future CLOSED retries to the base backoff"
        );
    }

    // P3a (lnrent-7fp.5, codex xhigh): the raw relay-message path must enforce the same inbound
    // filter as the REQ before verify/decrypt work. A malicious relay can send arbitrary kind-1059s
    // under our subscription id; a wrap tagged to another pubkey must be dropped before it reaches
    // `process_inbound` (observable here because process_inbound would negative-cache the
    // undecryptable wrap).
    #[tokio::test]
    async fn raw_relay_message_requires_operator_p_tag_before_decode() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let stranger_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys, std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "wrong-p-tag".into(),
            listing_id: "30402:op:dummy-1".into(),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let wrap = gift_wrap(&buyer_keys, &stranger_keys.public_key(), &msg)
            .await
            .expect("gift-wrap to a non-operator recipient");

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        engine
            .queue_inbound_event(wrap.clone(), order_handler, op_handler)
            .await
            .expect("queue wrong-recipient relay event");
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            !engine.is_negative_cached(wrap.id),
            "wrong-#p relay events are dropped before decrypt/negative-cache work"
        );
    }

    #[tokio::test]
    async fn drain_waits_for_slow_inbound_handler_to_finish_and_mark_seen() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");
        let wrap = gift_wrap(
            &buyer_keys,
            &op_keys.public_key(),
            &order_request("drain-slow", op_keys.public_key()),
        )
        .await
        .expect("gift-wrap order.request");

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(GateOrderHandler {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        engine
            .queue_inbound_event(wrap.clone(), order_handler, op_handler)
            .await
            .expect("queue inbound wrap");
        timeout(TEST_DEADLINE, started.notified())
            .await
            .expect("handler started before drain");

        let drain_engine = engine.clone();
        let drain = tokio::spawn(async move {
            let mut inbound = tokio::spawn(async { Ok(()) });
            drain_engine
                .drain(Duration::from_secs(2), &mut inbound)
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !drain.is_finished(),
            "drain waits while the accepted handler is still running"
        );

        release.notify_waiters();
        let result = timeout(TEST_DEADLINE, drain)
            .await
            .expect("drain returned")
            .expect("drain task joined");
        assert_eq!(result, InboundDrainResult::Drained);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            engine.is_seen(wrap.id).await.expect("seen check"),
            "seen_message is written only after the handler returns and drain completes"
        );
    }

    #[tokio::test]
    async fn drain_deadline_aborts_stuck_inbound_handler_without_marking_seen() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");
        let wrap = gift_wrap(
            &buyer_keys,
            &op_keys.public_key(),
            &order_request("drain-stuck", op_keys.public_key()),
        )
        .await
        .expect("gift-wrap order.request");

        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(NeverOrderHandler {
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        engine
            .queue_inbound_event(wrap.clone(), order_handler, op_handler)
            .await
            .expect("queue inbound wrap");
        timeout(TEST_DEADLINE, started.notified())
            .await
            .expect("handler started before drain");

        let mut inbound = tokio::spawn(async { Ok(()) });
        let result = timeout(
            Duration::from_secs(1),
            engine.drain(Duration::from_millis(50), &mut inbound),
        )
        .await
        .expect("bounded drain returned");
        assert_eq!(result, InboundDrainResult::TimedOut);

        timeout(TEST_DEADLINE, async {
            while dropped.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("stuck handler task was aborted");
        assert!(
            !engine.is_seen(wrap.id).await.expect("seen check"),
            "timed-out abort must not write seen_message before handler success"
        );
    }

    #[tokio::test]
    async fn run_inbound_accepts_no_work_after_drain_starts() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys, std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");
        let mut inbound = tokio::spawn(async { Ok(()) });
        assert_eq!(
            engine.drain(Duration::from_millis(100), &mut inbound).await,
            InboundDrainResult::Drained
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);
        timeout(
            Duration::from_millis(200),
            engine.run_inbound(order_handler, op_handler),
        )
        .await
        .expect("run_inbound returns immediately after drain")
        .expect("run_inbound exits cleanly after drain");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn drain_aborts_auxiliary_inbound_tasks() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys, std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        let dropped = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped_for_task = Arc::clone(&dropped);
        let started_for_task = Arc::clone(&started);
        let aux = engine.spawn_inbound_aux(async move {
            let _drop = DropCounter(dropped_for_task);
            started_for_task.notify_one(); // sticky: don't lose the start signal if the test isn't awaiting yet
            std::future::pending::<()>().await;
        });
        timeout(TEST_DEADLINE, started.notified())
            .await
            .expect("aux task started");
        assert_eq!(engine.inbound_aux_task_count_for_test(), 1);

        let mut inbound = tokio::spawn(async { Ok(()) });
        assert_eq!(
            engine.drain(Duration::from_millis(100), &mut inbound).await,
            InboundDrainResult::Drained
        );
        assert_eq!(engine.inbound_aux_task_count_for_test(), 0);
        assert!(aux.await.expect_err("aux task is aborted").is_cancelled());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn queue_inbound_event_backpressures_before_spawning_extra_task() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(GateOrderHandler {
            started,
            release: Arc::clone(&release),
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        for i in 0..MAX_INBOUND_CONCURRENCY {
            let wrap = gift_wrap(
                &buyer_keys,
                &op_keys.public_key(),
                &order_request(&format!("backpressure-{i}"), op_keys.public_key()),
            )
            .await
            .expect("gift-wrap order.request");
            engine
                .queue_inbound_event(wrap, Arc::clone(&order_handler), Arc::clone(&op_handler))
                .await
                .expect("queue inbound wrap");
        }

        timeout(TEST_DEADLINE, async {
            while calls.load(Ordering::SeqCst) != MAX_INBOUND_CONCURRENCY {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all concurrency slots are occupied");

        let extra = gift_wrap(
            &buyer_keys,
            &op_keys.public_key(),
            &order_request("backpressure-extra", op_keys.public_key()),
        )
        .await
        .expect("gift-wrap extra order.request");
        let queue = {
            let engine = engine.clone();
            let order_handler = Arc::clone(&order_handler);
            let op_handler = Arc::clone(&op_handler);
            tokio::spawn(async move {
                engine
                    .queue_inbound_event(extra, order_handler, op_handler)
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !queue.is_finished(),
            "the extra wrap waits for a permit instead of spawning an unbounded pending task"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_INBOUND_CONCURRENCY,
            "no extra handler starts until a concurrency slot frees"
        );

        release.notify_waiters();
        timeout(TEST_DEADLINE, queue)
            .await
            .expect("extra queue call returned after a slot freed")
            .expect("queue task joined")
            .expect("extra wrap queued");
        timeout(TEST_DEADLINE, async {
            while calls.load(Ordering::SeqCst) != MAX_INBOUND_CONCURRENCY + 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("extra handler started after a slot freed");

        release.notify_waiters();
        assert_eq!(
            engine
                .drain_after_inbound_stopped(Duration::from_secs(2))
                .await,
            InboundDrainResult::Drained
        );
    }

    fn dummy_recipe() -> Recipe {
        let dir = format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
        Recipe::load(&dir).expect("load dummy recipe")
    }

    // §5.4/§7.4: a listing built from a recipe carries the recipe's published op declarations
    // (name/label/kind/params) and the operator tag — but NEVER the operator-internal hook.
    #[test]
    fn listing_from_recipe_publishes_op_declarations_without_hook() {
        let recipe = dummy_recipe();
        let operator = Keys::generate().public_key().to_hex();
        let listing = listing_from_recipe(&recipe, "dummy-1", operator.clone());

        assert_eq!(listing.operator, operator);
        assert_eq!(listing.recipe_id, "dummy");
        assert_eq!(listing.amount_sat, recipe.pricing.amount_sat);
        // The dummy recipe declares status + restart; both surface as op declarations.
        let names: Vec<_> = listing.operations.iter().map(|o| o.name.as_str()).collect();
        assert!(
            names.contains(&"status") && names.contains(&"restart"),
            "ops published: {names:?}"
        );
        // The published declaration carries no hook — `OperationDecl` simply has no such field, so
        // the operator-internal hook can never leak into a listing (§5.4).
        assert_eq!(listing.version, lnrent_wire::SCHEMA_VERSION);

        // And it actually builds into a signed 30402 the wire codec round-trips.
        let keys = Keys::generate();
        let listing = listing_from_recipe(&recipe, "dummy-1", keys.public_key().to_hex());
        let event = build_listing(&listing)
            .expect("build")
            .sign_with_keys(&keys)
            .expect("sign");
        let parsed = lnrent_wire::parse_listing(&event).expect("parse");
        assert_eq!(parsed.listing.operations, listing.operations);
        assert_eq!(
            parsed.listing_id,
            format!("30402:{}:dummy-1", keys.public_key().to_hex())
        );
    }

    // ADR-0007/§9.1: the security tier is a VM-rental concept. A non-VM recipe (the dummy is
    // isolation = "none") advertises no tier; a VM recipe carries its declared tier.
    #[test]
    fn listing_tier_is_vm_only() {
        let mut recipe = dummy_recipe();
        let operator = Keys::generate().public_key().to_hex();
        assert_eq!(recipe.provisioning.isolation, "none");
        assert_eq!(
            listing_from_recipe(&recipe, "dummy-1", operator.clone()).tier,
            None,
            "a non-VM service advertises no tier"
        );

        recipe.provisioning.isolation = "vm".into();
        recipe.provisioning.tier = "1".into();
        assert_eq!(
            listing_from_recipe(&recipe, "dummy-1", operator).tier,
            Some("1".to_string()),
            "a VM listing carries its declared tier"
        );
    }

    #[tokio::test]
    async fn inbound_loop_routes_saved_replay_from_raw_relay_message() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        let buyer = Client::new(buyer_keys.clone());
        buyer.add_relay(&url).await.expect("buyer add relay");
        buyer.connect().await;
        buyer.wait_for_connection(TEST_DEADLINE).await;

        let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "raw-message-replay".into(),
            listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &msg)
            .await
            .expect("gift-wrap order.request");
        buyer
            .send_event(&wrap)
            .await
            .expect("buyer publishes order.request");

        // Mark this event as already seen by the SDK relay-pool database before the long-lived
        // inbound REQ starts. On replay, nostr-sdk suppresses `RelayPoolNotification::Event` for
        // this saved id, but it still emits the raw relay `Message` that the engine must consume.
        let fetched = engine
            .client
            .fetch_events(engine.inbound_filter(), TEST_DEADLINE)
            .await
            .expect("operator fetches retained inbound wrap");
        assert!(
            fetched.iter().any(|event| event.id == wrap.id),
            "the operator client marked the wrap saved before run_inbound starts"
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);
        let inbound = engine.clone();
        let handle = tokio::spawn(async move {
            let _ = inbound.run_inbound(order_handler, op_handler).await;
        });

        timeout(TEST_DEADLINE, async {
            loop {
                if calls.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("saved replay routes through raw RelayMessage::Event");

        handle.abort();
        let _ = handle.await;
    }

    // P2 (lnrent-7fp.5): the lag-recovery backfill path. When the broadcast notification stream
    // overflows for this slow consumer, the loop skips notifications that may include money-path
    // order/op DMs; recovery re-fetches the relay's RETAINED inbound set via `fetch_inbound_backlog`
    // (the "never trust `created_at`, re-fetch on lag" design) and feeds every wrap through the same
    // decode/dedupe/route path. Publish N wraps, call the backfill directly, and assert all N route
    // exactly once — then a SECOND backfill re-fetches the same retained set but the durable
    // `seen_message` rows dedupe it, proving the replay is cheap and exactly-once.
    #[tokio::test]
    async fn fetch_inbound_backlog_routes_retained_wraps_then_dedupes_replay() {
        let _relay_test = RELAY_TEST.lock().await;
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();

        let op_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        // A buyer publishes N distinct order.request wraps; the relay RETAINS them for the backfill.
        let buyer_keys = Keys::generate();
        let buyer = Client::new(buyer_keys.clone());
        buyer.add_relay(&url).await.expect("buyer add relay");
        buyer.connect().await;
        buyer.wait_for_connection(TEST_DEADLINE).await;

        const N: usize = 5;
        for i in 0..N {
            let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
                id: format!("backlog-{i}"),
                listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
                params: serde_json::json!({}),
                refund_dest: None,
            });
            let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &msg)
                .await
                .expect("gift-wrap order.request");
            buyer
                .send_event(&wrap)
                .await
                .expect("buyer publishes order.request");
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        // Exactly what the lag branch calls: re-fetch the retained set and queue it through the same
        // path. `queue_inbound_event` spawns per wrap, so poll until all N land.
        engine
            .fetch_inbound_backlog(Arc::clone(&order_handler), Arc::clone(&op_handler))
            .await
            .expect("backfill the retained inbound set");
        timeout(TEST_DEADLINE, async {
            loop {
                if calls.load(Ordering::SeqCst) == N {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("every retained wrap routes through the backfill path exactly once");

        // A second backfill re-fetches the identical retained set; the durable seen rows suppress it.
        engine
            .fetch_inbound_backlog(order_handler, op_handler)
            .await
            .expect("second backfill of the same retained set");
        // Give any (incorrectly) re-routed wrap a chance to land before asserting none did.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            N,
            "a re-fetch of the retained set is deduped, not re-routed"
        );
    }

    // Reviewer 1 P1: a full backfill page pinned to one `created_at` second must not skip the rest
    // of that second. Timestamp-only pagination cannot advance inside that bucket, so the backfill
    // falls back to negentropy and exact-id fetches.
    #[tokio::test]
    async fn fetch_inbound_backlog_recovers_same_timestamp_overflow_with_exact_ids() {
        const SAME_SECOND_GARBAGE_BEFORE_VALID: usize = 600;
        const RELAY_DEFAULT_FILTER_LIMIT: usize = 500;
        const ROUTE_DEADLINE: Duration = Duration::from_secs(30);

        let _relay_test = RELAY_TEST.lock().await;
        let (_relay, url) = high_throughput_relay().await;

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");
        let buyer = buyer_client(&url, buyer_keys.clone()).await;

        let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "same-second-buried-valid-order".into(),
            listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let created_at = Timestamp::now();
        let valid = loop {
            let wrap = gift_wrap_at(&buyer_keys, &op_keys.public_key(), &msg, created_at).await;
            if wrap.id.as_bytes()[0] >= 0xf0 {
                break wrap;
            }
        };
        buyer
            .send_event(&valid)
            .await
            .expect("buyer publishes the valid order.request");

        let garbage_signer = Keys::generate();
        let mut accepted = 0usize;
        let mut attempts = 0usize;
        while accepted < SAME_SECOND_GARBAGE_BEFORE_VALID {
            attempts += 1;
            assert!(
                attempts < SAME_SECOND_GARBAGE_BEFORE_VALID * 4,
                "could not generate enough lower-id garbage to bury the valid wrap"
            );
            let wrap = EventBuilder::new(
                Kind::GiftWrap,
                format!("same-second-garbage-not-a-nip44-payload-{attempts}"),
            )
            .tag(Tag::public_key(op_keys.public_key()))
            .custom_created_at(created_at)
            .sign_with_keys(&garbage_signer)
            .expect("sign same-second garbage gift wrap");
            if wrap.id >= valid.id {
                continue;
            }
            let output = buyer
                .send_event(&wrap)
                .await
                .expect("buyer publishes same-second garbage gift wrap");
            assert!(
                !output.success.is_empty(),
                "garbage wrap {accepted} was not accepted by the relay: {:?}",
                output.failed
            );
            engine.note_non_routable(wrap.id);
            accepted += 1;
        }

        let first_page = buyer
            .fetch_events(
                Filter::new()
                    .kind(Kind::GiftWrap)
                    .pubkey(op_keys.public_key())
                    .since(created_at)
                    .until(created_at)
                    .limit(RELAY_DEFAULT_FILTER_LIMIT),
                TEST_DEADLINE,
            )
            .await
            .expect("fetch same-second first page");
        assert_eq!(
            first_page.len(),
            RELAY_DEFAULT_FILTER_LIMIT,
            "same-second page must be full"
        );
        assert!(
            !first_page.iter().any(|event| event.id == valid.id),
            "valid wrap must be beyond the relay's first same-second page"
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        engine
            .fetch_inbound_backlog(Arc::clone(&order_handler), Arc::clone(&op_handler))
            .await
            .expect("backfill same-second retained set");
        timeout(ROUTE_DEADLINE, async {
            loop {
                if calls.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("same-second exact-id fallback routed the valid wrap beyond the first page");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly the buried valid wrap routes; same-second garbage is ignored"
        );
    }

    // Reviewer 2 P1 (lnrent-7fp.5, codex xhigh): the paged backfill must not treat a page shorter
    // than the REQUESTED limit as "retained set exhausted". A relay whose max filter limit is below
    // INBOUND_BACKFILL_PAGE_LIMIT clamps every page short, so a "short page = done" walk stops after
    // the newest page and silently drops older retained money-path DMs. Here the relay clamps to 50
    // while retaining MORE than its default filter limit (600) DISTINCT-timestamp wraps, with the one
    // valid lnrent DM as the OLDEST — many clamped pages deep. The cursor-advancing walk must page
    // all the way back and still route it (the same-second negentropy fallback is NOT what is under
    // test here: distinct timestamps keep the cursor advancing).
    #[tokio::test]
    async fn fetch_inbound_backlog_pages_past_a_clamped_filter_limit() {
        const CLAMP: usize = 50;
        const GARBAGE_NEWER_THAN_VALID: usize = 600;
        const ROUTE_DEADLINE: Duration = Duration::from_secs(30);

        const {
            assert!(
                CLAMP < INBOUND_BACKFILL_PAGE_LIMIT,
                "the relay must clamp below our requested page size to exercise the bug"
            )
        };

        let _relay_test = RELAY_TEST.lock().await;
        let (_relay, url) = clamped_relay(CLAMP).await;

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");
        let buyer = buyer_client(&url, buyer_keys.clone()).await;

        // The valid order DM is the OLDEST retained wrap, buried behind GARBAGE_NEWER_THAN_VALID
        // newer (distinct-second) #p gift-wrap garbage — so it sits many clamped pages deep.
        let base = Timestamp::now();
        let valid_created_at =
            Timestamp::from_secs(base.as_secs() - GARBAGE_NEWER_THAN_VALID as u64 - 1);
        let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "clamped-buried-valid-order".into(),
            listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let valid = gift_wrap_at(&buyer_keys, &op_keys.public_key(), &msg, valid_created_at).await;
        buyer
            .send_event(&valid)
            .await
            .expect("buyer publishes the buried valid order.request");

        let garbage_signer = Keys::generate();
        for i in 0..GARBAGE_NEWER_THAN_VALID {
            // Distinct, strictly-newer timestamps so the backfill cursor genuinely advances
            // page-by-page rather than pinning on one second.
            let created_at = Timestamp::from_secs(base.as_secs() - i as u64);
            let wrap = EventBuilder::new(
                Kind::GiftWrap,
                format!("clamped-garbage-not-a-nip44-payload-{i}"),
            )
            .tag(Tag::public_key(op_keys.public_key()))
            .custom_created_at(created_at)
            .sign_with_keys(&garbage_signer)
            .expect("sign clamped garbage gift wrap");
            let output = buyer
                .send_event(&wrap)
                .await
                .expect("buyer publishes clamped garbage gift wrap");
            assert!(
                !output.success.is_empty(),
                "garbage wrap {i} was not accepted by the relay: {:?}",
                output.failed
            );
        }

        // The relay clamps our requested page below INBOUND_BACKFILL_PAGE_LIMIT, and the valid wrap
        // is beyond that first page: a "short page = done" walk would stop here and lose it.
        let first_page = buyer
            .fetch_events(
                Filter::new()
                    .kind(Kind::GiftWrap)
                    .pubkey(op_keys.public_key())
                    .limit(INBOUND_BACKFILL_PAGE_LIMIT),
                TEST_DEADLINE,
            )
            .await
            .expect("fetch clamped first page");
        assert_eq!(
            first_page.len(),
            CLAMP,
            "the relay must clamp our requested page below INBOUND_BACKFILL_PAGE_LIMIT"
        );
        assert!(
            !first_page.iter().any(|event| event.id == valid.id),
            "the valid wrap must be beyond the relay's first clamped page"
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);

        engine
            .fetch_inbound_backlog(Arc::clone(&order_handler), Arc::clone(&op_handler))
            .await
            .expect("backfill the clamped retained set");
        timeout(ROUTE_DEADLINE, async {
            loop {
                if calls.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("paged backfill walked past the clamp and routed the buried valid wrap");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly the buried valid wrap routes; clamped garbage is ignored"
        );
    }

    // P2 (lnrent-7fp.5, Reviewer 1): a relay can END the long-lived inbound REQ server-side
    // (`CLOSED`) AFTER `subscribe_with_id` already reported success (it only enqueued the REQ).
    // The engine must OBSERVE that closure and recover — without it that relay silently stops
    // delivering money-path order/op DMs (the SDK removes the closed subscription and does not
    // auto-resubscribe). Drive it with a relay that CLOSEs the first REQ then accepts: the
    // operator's initial subscribe is closed, the engine backs off and resubscribes, and only then
    // is the retained order.request delivered + routed — so the handler firing proves the recovery.
    #[tokio::test]
    async fn relay_closed_inbound_subscription_is_resubscribed() {
        let _relay_test = RELAY_TEST.lock().await;
        let (_relay, url) = rejecting_relay().await;

        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        let calls = Arc::new(AtomicUsize::new(0));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(CountingOrderHandler {
            calls: Arc::clone(&calls),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(NoopOpHandler);
        let inbound = engine.clone();
        let handle = tokio::spawn(async move {
            let _ = inbound.run_inbound(order_handler, op_handler).await;
        });

        // The buyer publishes one order.request; the relay RETAINS it. Because the operator's first
        // REQ is CLOSED, it can only be delivered once the engine resubscribes — so a handler call
        // is unambiguous proof the CLOSED was observed and recovered from.
        let buyer = Client::new(buyer_keys.clone());
        buyer.add_relay(&url).await.expect("buyer add relay");
        buyer.connect().await;
        buyer.wait_for_connection(TEST_DEADLINE).await;
        let msg = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "closed-recovery-1".into(),
            listing_id: format!("30402:{}:dummy-1", op_keys.public_key().to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let wrap = gift_wrap(&buyer_keys, &op_keys.public_key(), &msg)
            .await
            .expect("gift-wrap order.request");
        buyer
            .send_event(&wrap)
            .await
            .expect("buyer publishes order.request");

        timeout(TEST_DEADLINE, async {
            loop {
                if calls.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the engine resubscribed after CLOSED and routed the retained DM");

        handle.abort();
        let _ = handle.await;
    }
}
