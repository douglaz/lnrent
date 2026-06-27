//! The production daemon runtime: wire every M1a subsystem together and SUPERVISE them so the money
//! path actually RUNS (lnrent-7fp.21). This is INTEGRATION GLUE — each subsystem already exists and
//! is unit-tested; here we only construct, boot-recover, and keep them alive.
//!
//! Layout:
//! - [`Supervisor::build`] constructs the handlers/drivers from the injected seams (store, engine,
//!   payment, clock, recipe) — sourcing the order-intake [`Budget`] from the `box` capacity row or a
//!   bounded, clearly-logged M1a fallback.
//! - [`Supervisor::start`] publishes the operator's listing (durable row + NIP-99 event), runs the
//!   ordered boot recovery, then spawns every long-lived loop under [`supervise`] and returns a
//!   [`RunningSupervisor`] handle.
//! - [`supervise`] is the one restart primitive every loop runs under: a panic / `Err` / unexpected
//!   exit is logged and restarted with a capped backoff; a shared shutdown signal stops everything.
//!
//! Supervised loops: the IPC accept loop, the Nostr inbound loop, the settlement→capture loop, the
//! reconcile tick loop, and a SINGLE serialized maintenance loop (provision-drive + refund-drive +
//! outbox-drain) woken on an interval AND on settlement/reconcile nudges.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use lnrent_wire::Msg;
use nostr_sdk::PublicKey;
use rusqlite::OptionalExtension;
use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};

use crate::backends::{PaymentBackend, PaymentStatus, Settlement};
use crate::capture::{capture, Capture};
use crate::clock::Clock;
use crate::ipc;
use crate::nostr_engine::{listing_from_recipe, NostrEngine, OpHandler, OrderHandler, Outbound};
use crate::op_dispatch::OpDispatch;
use crate::order_intake::OrderIntake;
use crate::provision::{DeliveryResendOrderHandler, OutboxSender, Provisioner};
use crate::recipe::Recipe;
use crate::reconcile::Reconciler;
use crate::refund::Refunder;
use crate::reservation::Budget;
use crate::store::Store;

/// The loop cadences (injected so tests run in milliseconds and deterministically). Production
/// defaults: reconcile every 60s (§6.5), maintenance every 5s.
#[derive(Debug, Clone, Copy)]
pub struct Intervals {
    /// How often the reconcile tick scans deadline cursors.
    pub reconcile: Duration,
    /// How often the single maintenance loop drives provision/refund/outbox (also runs on nudges).
    pub maintenance: Duration,
}

impl Intervals {
    /// The production cadence: reconcile 60s, maintenance 5s.
    pub fn production() -> Self {
        Intervals {
            reconcile: Duration::from_secs(60),
            maintenance: Duration::from_secs(5),
        }
    }
}

/// A capped exponential restart backoff for a supervised loop.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
}

/// The restart backoff every daemon loop uses: a tenth of a second growing to 15s.
const RESTART_BACKOFF: Backoff = Backoff {
    base: Duration::from_millis(200),
    max: Duration::from_secs(15),
};

/// Upper bound on how long [`RunningSupervisor::shutdown`] waits for the loops to wind down before
/// aborting the stragglers — a stuck loop must not hang process exit.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Per-loop window [`supervise`] gives a child to finish its in-flight work on shutdown before it is
/// aborted. Every long-lived loop observes the shutdown signal and returns promptly (the IPC loop
/// drains its in-flight handlers first), so this is only a backstop for a loop that ignores it.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(3);

/// M1a fallback host budget, used ONLY when no `box` capacity row exists yet (onboard, a later bead,
/// writes the real one). Deliberately SMALL and BOUNDED — never an unlimited budget — and LOGGED on
/// use, so an order can never reserve more capacity than a modest single host actually has.
const M1A_FALLBACK_BUDGET: Budget = Budget {
    cpu: 2,
    mem_mb: 2_048,
    disk_gb: 50,
    ports: 16,
};

/// The box id recorded on instance rows when no `box` row exists yet (single-box M1a).
const M1A_FALLBACK_BOX_ID: &str = "box-0";

/// Default seconds for a recipe duration string that won't parse — a malformed period must not write
/// a zero/negative timer into the listing row that would wedge capture/reconcile math.
const DEFAULT_DURATION_S: i64 = 30 * 86_400;

/// The wired-but-not-yet-running daemon: all handlers/drivers constructed, ready to [`start`].
///
/// [`start`]: Supervisor::start
pub struct Supervisor {
    store: Store,
    engine: NostrEngine,
    payment: Arc<dyn PaymentBackend>,
    clock: Arc<dyn Clock>,
    recipe: Recipe,
    /// The single-recipe catalog handed to the IPC surface (status/recipes queries).
    recipes: Arc<Vec<Recipe>>,
    /// Order/billing DMs: `OrderIntake` wrapped so `delivery.resend.request` hits the outbox path.
    order_handler: Arc<dyn OrderHandler>,
    /// Management-op DM handler wrapped with the same inbound drain tracker.
    op_handler: Arc<dyn OpHandler>,
    inbound_drain: Arc<InboundDrain>,
    /// Management-op DMs; also owns the orphaned-RUNNING boot recovery.
    op_dispatch: Arc<OpDispatch>,
    provisioner: Arc<Provisioner>,
    refunder: Arc<Refunder>,
    reconciler: Arc<Reconciler>,
    sock_path: PathBuf,
    intervals: Intervals,
    /// Optional hook to keep a mock payment backend's internal clock in step with `clock` (M1a only;
    /// `MockPayment::set_now` is concrete to the mock, not on the [`PaymentBackend`] trait). Seeded
    /// at boot AND re-applied each maintenance tick, so the mock stamps invoice expiry off the live
    /// clock instead of a 1970 epoch (which would make reconcile instantly expire every order).
    /// `None` for the real Fedimint backend (it owns its own clock). Installed via
    /// [`Supervisor::with_payment_clock_sync`].
    payment_clock_sync: Option<Arc<dyn Fn(i64) + Send + Sync>>,
}

impl Supervisor {
    /// Construct the wired supervisor from the injected seams. The engine must already be connected
    /// ([`NostrEngine::connect`]); the store must be opened ONCE and shared. Sources the order-intake
    /// [`Budget`] + box id from the `box` row, or a bounded logged fallback.
    pub async fn build(
        store: Store,
        engine: NostrEngine,
        payment: Arc<dyn PaymentBackend>,
        clock: Arc<dyn Clock>,
        recipe: Recipe,
        sock_path: PathBuf,
        intervals: Intervals,
    ) -> Result<Self> {
        let (budget, box_id) = load_budget_and_box(&store).await;

        // Order/billing handler: OrderIntake, wrapped so delivery.resend.request resends the latest
        // provision.ready from the durable outbox (lnrent-7fp.10) instead of re-running order intake.
        let inbound_drain = Arc::new(InboundDrain::default());
        let intake: Arc<dyn OrderHandler> = Arc::new(OrderIntake::new(
            store.clone(),
            payment.clone(),
            clock.clone(),
            recipe.clone(),
            budget,
        ));
        let order_handler: Arc<dyn OrderHandler> = Arc::new(DrainingOrderHandler::new(
            Arc::new(DeliveryResendOrderHandler::new(
                intake,
                OutboxSender::new(store.clone(), clock.clone()),
            )),
            inbound_drain.clone(),
        ));
        let op_dispatch = Arc::new(OpDispatch::new(
            store.clone(),
            clock.clone(),
            recipe.clone(),
        ));
        let op_handler: Arc<dyn OpHandler> = Arc::new(DrainingOpHandler::new(
            op_dispatch.clone(),
            inbound_drain.clone(),
        ));

        let provisioner = Arc::new(Provisioner::new(
            store.clone(),
            clock.clone(),
            recipe.clone(),
            box_id,
        ));
        let refunder = Arc::new(Refunder::new(store.clone(), payment.clone(), clock.clone()));
        let reconciler = Arc::new(Reconciler::new(
            store.clone(),
            payment.clone(),
            recipe.clone(),
        ));
        let recipes = Arc::new(vec![recipe.clone()]);

        Ok(Supervisor {
            store,
            engine,
            payment,
            clock,
            recipe,
            recipes,
            order_handler,
            op_dispatch,
            op_handler,
            inbound_drain,
            provisioner,
            refunder,
            reconciler,
            sock_path,
            intervals,
            payment_clock_sync: None,
        })
    }

    /// Install a hook that keeps a mock payment backend's clock in step with `clock` (M1a only).
    /// `main` supplies `move |now| mock.set_now(now)` over the concrete [`MockPayment`]; the real
    /// Fedimint backend needs none (it owns its own clock). The hook is seeded at boot and re-applied
    /// each maintenance tick (see [`Supervisor::start`] / [`maintenance_pass`]).
    pub fn with_payment_clock_sync(mut self, sync: impl Fn(i64) + Send + Sync + 'static) -> Self {
        self.payment_clock_sync = Some(Arc::new(sync));
        self
    }

    /// A fresh outbox sender (cheap — clones the store handle + clock). Used for boot recovery and
    /// the graceful final flush; the maintenance loop builds its own.
    fn outbox(&self) -> OutboxSender {
        OutboxSender::new(self.store.clone(), self.clock.clone())
    }

    /// Publish the operator's NIP-99 30402 listing when the durable row is ACTIVE, and upsert the
    /// durable `listing` row. Order intake matches an incoming `order.request` against that row
    /// (price/state, order_intake.rs), so the row is upserted FIRST and is authoritative; the relay
    /// publish is best-effort discovery (a relay outage logs a warning, never fails boot — the next
    /// boot republishes).
    async fn publish_and_upsert_listing(&self) -> Result<()> {
        let operator_hex = self.engine.operator_pubkey().to_hex();
        // The NIP-99 replaceable-event `d` tag; the coordinate is then `30402:<operator>:<d>`. M1a
        // serves one listing per recipe, so key `d` on the recipe id (stable across republishes).
        let d = self.recipe.service.id.clone();
        let listing_id = lnrent_wire::listing_coordinate(&operator_hex, &d);

        let now = self.clock.now();
        let amount_sat = self.recipe.pricing.amount_sat as i64;
        let period_s = duration_secs(&self.recipe.pricing.period);
        let renew_lead_s = duration_secs(&self.recipe.pricing.renew_lead);
        let retention_s = duration_secs(&self.recipe.pricing.retention);
        let recipe_id = self.recipe.service.id.clone();

        // 1. Durable row first (authoritative for order matching).
        {
            let (id, recipe_id, d_tag) = (listing_id.clone(), recipe_id, d.clone());
            self.store
                .transaction(move |tx| {
                    // A FRESH row is born ACTIVE; on CONFLICT only the price/timer columns are
                    // refreshed and `state` is DELIBERATELY left untouched — re-publishing the
                    // listing on every boot must NOT resurrect a deliberately WITHDRAWN listing back
                    // to ACTIVE (review #5).
                    tx.execute(
                        "INSERT INTO listing
                            (id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, state, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'ACTIVE', ?8)
                         ON CONFLICT(id) DO UPDATE SET
                            recipe_id=excluded.recipe_id, d_tag=excluded.d_tag, amount_sat=excluded.amount_sat,
                            period_s=excluded.period_s, renew_lead_s=excluded.renew_lead_s,
                            retention_s=excluded.retention_s, updated_at=excluded.updated_at",
                        rusqlite::params![
                            id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, now
                        ],
                    )?;
                    Ok(())
                })
                .await
                .context("upserting durable listing row")?;
        }

        let listing_state: String = {
            let id = listing_id.clone();
            self.store
                .read(move |c| {
                    Ok(c.query_row(
                        "SELECT state FROM listing WHERE id=?1",
                        rusqlite::params![id],
                        |r| r.get(0),
                    )?)
                })
                .await
                .context("reading durable listing state")?
        };
        if listing_state != "ACTIVE" {
            tracing::info!(
                listing = %listing_id,
                state = %listing_state,
                "durable listing is not ACTIVE; skipping 30402 publish"
            );
            return Ok(());
        }

        // 2. Publish the NIP-99 event (best-effort discovery). Record the event id on the row.
        let listing = listing_from_recipe(&self.recipe, d, operator_hex);
        match self.engine.publish_listing(&listing).await {
            Ok(event_id) => {
                let (id, ev) = (listing_id.clone(), event_id.to_hex());
                let _ = self
                    .store
                    .transaction(move |tx| {
                        tx.execute(
                            "UPDATE listing SET event_id=?2 WHERE id=?1",
                            rusqlite::params![id, ev],
                        )?;
                        Ok(())
                    })
                    .await;
                tracing::info!(listing = %listing_id, event = %event_id.to_hex(), "published 30402 listing");
            }
            Err(e) => tracing::warn!(
                listing = %listing_id,
                error = %format!("{e:#}"),
                "publishing 30402 listing failed (durable row upserted; will republish next boot)"
            ),
        }
        Ok(())
    }

    /// Run-once boot recovery, in the order each subsystem's durable recovery requires (lnrent-7fp.21):
    /// op-dispatch interrupted ops, settlement catch-up, provisioning re-drive (+ failed cleanups),
    /// refunds, a reconcile catch-up tick, then the outbox drain. Each step is idempotent; an error
    /// in one is logged and does not block the rest (the periodic loops will retry).
    async fn boot_recovery(&self) -> Result<()> {
        tracing::info!(
            "boot recovery: op-dispatch -> settlement catch-up -> provision -> refund -> reconcile -> outbox"
        );

        match self.op_dispatch.recover_interrupted_ops().await {
            Ok(n) => tracing::info!(count = n, "boot recovery: interrupted ops recovered"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: op-dispatch recovery failed")
            }
        }
        // Catch any settlement the live watch() stream missed while the daemon was down (a PAID order
        // left stuck PENDING forever otherwise); each caught order moves to PROVISIONING for the
        // re-drive below. Idempotent — capture's OPEN->PAID CAS no-ops an already-applied settlement.
        match settlement_catch_up(&self.store, &self.payment, &self.clock).await {
            Ok(n) => tracing::info!(count = n, "boot recovery: settlements caught up"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: settlement catch-up failed")
            }
        }
        // `recover` re-drives PROVISIONING subs AND finishes failed-provision cleanups.
        match self.provisioner.recover().await {
            Ok(n) => tracing::info!(count = n, "boot recovery: provisioning re-driven"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: provision recovery failed")
            }
        }
        match self.refunder.drive().await {
            Ok(rep) => tracing::info!(?rep, "boot recovery: refunds driven"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: refund recovery failed")
            }
        }
        match self.reconciler.reconcile_tick(self.clock.now()).await {
            Ok(rep) => tracing::info!(?rep, "boot recovery: reconcile catch-up"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: reconcile catch-up failed")
            }
        }
        match self.outbox().drain_once(&self.engine).await {
            Ok(n) => tracing::info!(count = n, "boot recovery: outbox drained"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: outbox drain failed")
            }
        }
        Ok(())
    }

    /// Boot: publish + upsert the listing, run ordered boot recovery, then spawn every long-lived
    /// loop under [`supervise`]. Returns a [`RunningSupervisor`]; the loops run until it is shut down
    /// (graceful) or dropped (abort — the crash simulation in tests).
    pub async fn start(self) -> Result<RunningSupervisor> {
        // Seed the mock payment clock to the live clock BEFORE any expiry-sensitive work (settlement
        // catch-up's `lookup`, reconcile catch-up). Without this the mock's `now` sits at 1970 while
        // the rest of the daemon runs on the real clock, so every fresh invoice looks expired (M1a;
        // no-op for the real Fedimint backend). Kept in step thereafter by the maintenance loop.
        if let Some(sync) = &self.payment_clock_sync {
            sync(self.clock.now());
        }

        // Register the settlement stream BEFORE spawning anything, so a settlement that arrives the
        // instant start() returns is never dropped by a not-yet-registered watcher.
        let settle_rx = self
            .payment
            .watch()
            .context("opening payment settlement stream")?;

        self.publish_and_upsert_listing().await?;
        self.boot_recovery().await?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Wakes the single maintenance loop immediately on settlement/reconcile work.
        let nudge = Arc::new(Notify::new());
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        // -- IPC accept loop (shutdown-aware) --
        {
            let (store, recipes, clock, sock) = (
                self.store.clone(),
                self.recipes.clone(),
                self.clock.clone(),
                self.sock_path.clone(),
            );
            tasks.push(tokio::spawn(supervise(
                "ipc",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (store, recipes, clock, sock) =
                        (store.clone(), recipes.clone(), clock.clone(), sock.clone());
                    async move { ipc::serve_with_shutdown(store, recipes, clock, &sock, sd).await }
                },
            )));
        }

        // -- Nostr inbound loop (decode/dedupe/route order + op DMs) --
        {
            let engine = self.engine.clone();
            let order = self.order_handler.clone();
            let op = self.op_handler.clone();
            tasks.push(tokio::spawn(supervise(
                "nostr-inbound",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (engine, order, op) = (engine.clone(), order.clone(), op.clone());
                    // run_inbound cannot observe the shutdown signal itself, so race it against the
                    // signal here: on shutdown the select drops the inbound future and returns Ok,
                    // letting the loop wind down gracefully instead of relying on a hard abort.
                    async move {
                        let mut sd = sd;
                        tokio::select! {
                            r = engine.run_inbound(order, op) => r,
                            _ = wait_for_shutdown(&mut sd) => Ok(()),
                        }
                    }
                },
            )));
        }

        // -- Settlement -> capture loop --
        {
            // First start consumes the pre-registered stream; a restart re-registers `watch()`.
            let slot = Arc::new(Mutex::new(Some(settle_rx)));
            let (payment, store, clock, nudge2) = (
                self.payment.clone(),
                self.store.clone(),
                self.clock.clone(),
                nudge.clone(),
            );
            tasks.push(tokio::spawn(supervise(
                "settlement",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (slot, payment, store, clock, nudge2) = (
                        slot.clone(),
                        payment.clone(),
                        store.clone(),
                        clock.clone(),
                        nudge2.clone(),
                    );
                    async move {
                        let rx = match slot.lock().await.take() {
                            Some(rx) => rx,
                            None => payment
                                .watch()
                                .context("re-opening settlement stream on restart")?,
                        };
                        run_settlement_loop(rx, store, clock, nudge2, sd).await
                    }
                },
            )));
        }

        // -- Reconcile tick loop --
        {
            let (reconciler, clock, nudge2) =
                (self.reconciler.clone(), self.clock.clone(), nudge.clone());
            let interval = self.intervals.reconcile;
            tasks.push(tokio::spawn(supervise(
                "reconcile",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (reconciler, clock, nudge2) =
                        (reconciler.clone(), clock.clone(), nudge2.clone());
                    async move { run_reconcile_loop(reconciler, clock, interval, nudge2, sd).await }
                },
            )));
        }

        // -- Single serialized maintenance loop (clock sync + periodic settlement catch-up + provision + refund + outbox) --
        {
            let (provisioner, refunder, payment, engine, store, clock, nudge2, sync) = (
                self.provisioner.clone(),
                self.refunder.clone(),
                self.payment.clone(),
                self.engine.clone(),
                self.store.clone(),
                self.clock.clone(),
                nudge.clone(),
                self.payment_clock_sync.clone(),
            );
            let interval = self.intervals.maintenance;
            tasks.push(tokio::spawn(supervise(
                "maintenance",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (provisioner, refunder, payment, engine, store, clock, nudge2, sync) = (
                        provisioner.clone(),
                        refunder.clone(),
                        payment.clone(),
                        engine.clone(),
                        store.clone(),
                        clock.clone(),
                        nudge2.clone(),
                        sync.clone(),
                    );
                    async move {
                        run_maintenance_loop(
                            provisioner,
                            refunder,
                            payment,
                            store,
                            clock,
                            engine,
                            sync,
                            interval,
                            nudge2,
                            sd,
                        )
                        .await
                    }
                },
            )));
        }

        tracing::info!(
            socket = %self.sock_path.display(),
            "supervisor up: IPC + Nostr inbound + settlement + reconcile + maintenance loops running"
        );

        Ok(RunningSupervisor {
            shutdown_tx,
            tasks,
            engine: self.engine,
            store: self.store,
            clock: self.clock,
            inbound_drain: self.inbound_drain,
        })
    }
}

#[derive(Default)]
struct InboundDrain {
    active: AtomicUsize,
    idle: Notify,
}

impl InboundDrain {
    fn enter(self: &Arc<Self>) -> InboundGuard {
        self.active.fetch_add(1, Ordering::SeqCst);
        InboundGuard {
            drain: self.clone(),
        }
    }

    async fn wait_idle(&self) {
        loop {
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            self.idle.notified().await;
        }
    }
}

struct InboundGuard {
    drain: Arc<InboundDrain>,
}

impl Drop for InboundGuard {
    fn drop(&mut self) {
        if self.drain.active.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.drain.idle.notify_waiters();
        }
    }
}

struct DrainingOrderHandler {
    inner: Arc<dyn OrderHandler>,
    drain: Arc<InboundDrain>,
}

impl DrainingOrderHandler {
    fn new(inner: Arc<dyn OrderHandler>, drain: Arc<InboundDrain>) -> Self {
        Self { inner, drain }
    }
}

#[async_trait]
impl OrderHandler for DrainingOrderHandler {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        let _guard = self.drain.enter();
        self.inner.handle(sender, msg, out).await
    }
}

struct DrainingOpHandler {
    inner: Arc<dyn OpHandler>,
    drain: Arc<InboundDrain>,
}

impl DrainingOpHandler {
    fn new(inner: Arc<dyn OpHandler>, drain: Arc<InboundDrain>) -> Self {
        Self { inner, drain }
    }
}

#[async_trait]
impl OpHandler for DrainingOpHandler {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        let _guard = self.drain.enter();
        self.inner.handle(sender, msg, out).await
    }
}

/// A running supervisor: the shutdown switch + the supervised task handles. [`shutdown`] is the
/// graceful path (Ctrl-C / SIGTERM); a bare drop ABORTS every loop (the crash simulation in tests).
///
/// [`shutdown`]: RunningSupervisor::shutdown
pub struct RunningSupervisor {
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    engine: NostrEngine,
    store: Store,
    clock: Arc<dyn Clock>,
    inbound_drain: Arc<InboundDrain>,
}

impl RunningSupervisor {
    /// Graceful shutdown: stop accepting new work, let in-flight transactions commit (the store actor
    /// drains its queue), flush the outbox once, then stop every loop. Bounded by [`SHUTDOWN_GRACE`].
    pub async fn shutdown(mut self) -> Result<()> {
        tracing::info!("supervisor: graceful shutdown starting");
        // 1. Signal every loop. Each supervise() wrapper aborts its in-flight child, so the inbound
        //    loop (which can't observe shutdown itself) stops too.
        let _ = self.shutdown_tx.send(true);
        // 2. Wait for the loops to wind down (take the handles out so Drop is a no-op afterward).
        let tasks = std::mem::take(&mut self.tasks);
        let aborts: Vec<AbortHandle> = tasks.iter().map(|t| t.abort_handle()).collect();
        let join = async move {
            for t in tasks {
                let _ = t.await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_GRACE, join).await.is_err() {
            tracing::warn!("supervisor: loops did not stop within the grace window; aborting");
            for a in &aborts {
                a.abort();
            }
        }
        // 3. run_inbound spawns per-wrap handler tasks internally. We cannot own those join handles
        //    from here, but the injected order/op handlers are wrapped with an active-call tracker;
        //    wait for those commits/replies before the final outbox flush.
        if tokio::time::timeout(SHUTDOWN_DRAIN, self.inbound_drain.wait_idle())
            .await
            .is_err()
        {
            tracing::warn!(
                "supervisor: inbound handlers did not drain within the grace window; flushing anyway"
            );
        }
        // 4. Final outbox flush so a just-committed provision.ready / billing.refund DM goes out.
        if let Err(e) = OutboxSender::new(self.store.clone(), self.clock.clone())
            .drain_once(&self.engine)
            .await
        {
            tracing::warn!(error = %format!("{e:#}"), "supervisor: final outbox drain failed");
        }
        tracing::info!("supervisor: graceful shutdown complete");
        Ok(())
    }
}

impl Drop for RunningSupervisor {
    fn drop(&mut self) {
        // Crash-sim / safety net: if shutdown() wasn't called, stop every loop now. Aborting each
        // supervise() wrapper drops its AbortOnDrop guard, which aborts the wrapper's in-flight child
        // — so no loop (not even the engine inbound loop) survives the drop.
        let _ = self.shutdown_tx.send(true);
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Aborts a child task when this guard is dropped. Dropping a `JoinHandle` only DETACHES the task, so
/// a supervisor that is cancelled (or returns on shutdown) must explicitly abort its child — else a
/// loop that cannot observe the shutdown signal (the engine inbound loop) would survive.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run `make` as a long-lived SUPERVISED task. Each (re)start spawns the future on its own task and
/// awaits it; a panic (`JoinError`), an `Err` return, or an unexpected clean exit is LOGGED with
/// `name` and restarted after a capped backoff. A panic is ISOLATED — the process and the sibling
/// loops keep running. Returns when `shutdown` flips to `true`: the child is given a bounded
/// [`SHUTDOWN_DRAIN`] window to wind down its in-flight work (the IPC loop drains its in-flight admin
/// handlers and replies in that window), and is aborted only if it overruns — so a loop that cannot
/// observe shutdown itself is still stopped. This is the single supervision primitive every daemon
/// loop runs under.
pub async fn supervise<F, Fut>(
    name: &'static str,
    mut shutdown: watch::Receiver<bool>,
    backoff: Backoff,
    make: F,
) where
    F: Fn(watch::Receiver<bool>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let mut delay = backoff.base;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let started = tokio::time::Instant::now();
        let mut child = tokio::spawn(make(shutdown.clone()));
        // If THIS supervisor future is dropped (a crash-sim drop) or a draining child overruns, abort
        // the child — dropping the JoinHandle alone would only detach it.
        let guard = AbortOnDrop(child.abort_handle());

        let outcome = tokio::select! {
            out = &mut child => out,
            _ = wait_for_shutdown(&mut shutdown) => {
                // Graceful stop: the child observes the same shutdown signal and returns promptly
                // (the IPC loop first drains its in-flight admin handlers + replies). Give it a
                // bounded window to finish that in-flight work rather than aborting it mid-commit;
                // abort only if it overruns. This is what lets an in-flight admin txn + reply land.
                match tokio::time::timeout(SHUTDOWN_DRAIN, &mut child).await {
                    Ok(out) => out,
                    Err(_) => {
                        tracing::warn!(task = name, "supervised loop did not stop within the drain window; aborting");
                        return; // `guard` drops here, aborting the child
                    }
                }
            }
        };
        drop(guard); // the child already finished; this abort is a no-op

        // Shutting down: never restart, however the child finished.
        if *shutdown.borrow() {
            return;
        }

        match outcome {
            Ok(Ok(())) => tracing::warn!(
                task = name,
                "supervised loop exited cleanly (unexpected for a long-lived loop); restarting"
            ),
            Ok(Err(e)) => tracing::error!(
                task = name,
                error = %format!("{e:#}"),
                "supervised loop returned an error; restarting"
            ),
            Err(join) if join.is_panic() => tracing::error!(
                task = name,
                "supervised loop PANICKED; restarting (process and sibling loops stay up)"
            ),
            Err(join) => {
                tracing::warn!(task = name, error = %join, "supervised loop cancelled; stopping");
                return;
            }
        }

        // Reset the backoff if the child ran healthily for a while, so an intermittent crash
        // (e.g. once an hour) doesn't pin the loop at the max backoff forever.
        if started.elapsed() >= backoff.max {
            delay = backoff.base;
        }
        // Capped backoff before the restart; wake immediately if shutdown lands meanwhile.
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = wait_for_shutdown(&mut shutdown) => return,
        }
        delay = delay.saturating_mul(2).min(backoff.max);
    }
}

/// Resolve as soon as `shutdown` is observed `true` (or its sender is dropped).
async fn wait_for_shutdown(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// Settlement loop: drain the payment backend's settlement stream and `capture` each; after a
/// `Captured`/`RefundDue` outcome NUDGE the maintenance loop so provision/refund runs promptly.
async fn run_settlement_loop(
    mut rx: mpsc::Receiver<Settlement>,
    store: Store,
    clock: Arc<dyn Clock>,
    nudge: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(settlement) = maybe else {
                    // The settlement stream ended; let the supervisor restart us (which re-registers
                    // watch()) unless we are shutting down.
                    return Ok(());
                };
                let external = settlement.external_id.clone();
                match capture(&store, settlement, clock.now()).await {
                    Ok(Capture::Captured) | Ok(Capture::RefundDue) => {
                        // Provisioning (Captured) or a refund (RefundDue) is now pending — wake the
                        // maintenance loop instead of waiting for its interval.
                        nudge.notify_one();
                    }
                    Ok(other) => tracing::debug!(external = %external, ?other, "settlement captured"),
                    Err(e) => tracing::error!(
                        external = %external,
                        error = %format!("{e:#}"),
                        "capture failed"
                    ),
                }
            }
            _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
        }
    }
}

/// Reconcile loop: fire time-driven deadline transitions every `interval`, then nudge maintenance
/// (a tick can enqueue refunds / suspend & terminate DMs into the outbox).
async fn run_reconcile_loop(
    reconciler: Arc<Reconciler>,
    clock: Arc<dyn Clock>,
    interval: Duration,
    nudge: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match reconciler.reconcile_tick(clock.now()).await {
                    Ok(rep) => tracing::debug!(?rep, "reconcile tick"),
                    Err(e) => tracing::error!(error = %format!("{e:#}"), "reconcile tick failed"),
                }
                nudge.notify_one();
            }
            _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
        }
    }
}

/// The SINGLE serialized maintenance loop: on each `interval` tick AND on every nudge, run one
/// maintenance pass. Single-threaded by construction — the Provisioner is CAS-safe but duplicate
/// concurrent drives could run recipe hooks before one loses the CAS, so provision driving stays on
/// this one loop.
#[allow(clippy::too_many_arguments)]
async fn run_maintenance_loop(
    provisioner: Arc<Provisioner>,
    refunder: Arc<Refunder>,
    payment: Arc<dyn PaymentBackend>,
    store: Store,
    clock: Arc<dyn Clock>,
    engine: NostrEngine,
    sync: Option<Arc<dyn Fn(i64) + Send + Sync>>,
    interval: Duration,
    nudge: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let outbox = OutboxSender::new(store.clone(), clock.clone());
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let run_catch_up = tokio::select! {
            _ = ticker.tick() => true,
            _ = nudge.notified() => false,
            _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
        };
        maintenance_pass(
            &provisioner,
            &refunder,
            &payment,
            &store,
            &clock,
            &outbox,
            &engine,
            sync.as_ref(),
            run_catch_up,
        )
        .await;
    }
}

/// One serialized maintenance pass: keep the mock payment clock in step, periodically catch any
/// missed settlements, drive PROVISIONING subs to ACTIVE/REFUND_DUE (+ finish failed cleanups), pay
/// PENDING refunds, then deliver unsent DMs (provision.ready, billing.refund, suspend/terminate
/// notices). Each step is independently idempotent; an error in one is logged and does not block the
/// others.
#[allow(clippy::too_many_arguments)]
async fn maintenance_pass(
    provisioner: &Provisioner,
    refunder: &Refunder,
    payment: &Arc<dyn PaymentBackend>,
    store: &Store,
    clock: &Arc<dyn Clock>,
    outbox: &OutboxSender,
    engine: &NostrEngine,
    sync: Option<&Arc<dyn Fn(i64) + Send + Sync>>,
    run_catch_up: bool,
) {
    // Keep a mock payment backend's clock in step with ours so freshly-issued invoices stamp a live
    // expiry (M1a only; a no-op for the real Fedimint backend, which owns its own clock).
    if let Some(sync) = sync {
        sync(clock.now());
    }
    // Catch settlements the live watch() stream missed on the interval cadence. Nudges are already
    // caused by work the live stream/reconcile loop observed; rescanning every OPEN invoice on each
    // nudge would turn a single settlement into an O(open invoices) backend lookup burst.
    if run_catch_up {
        if let Err(e) = settlement_catch_up(store, payment, clock).await {
            tracing::error!(error = %format!("{e:#}"), "maintenance: settlement catch-up failed");
        }
    }
    if let Err(e) = provisioner.recover().await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: provision drive failed");
    }
    if let Err(e) = refunder.drive().await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: refund drive failed");
    }
    if let Err(e) = outbox.drain_once(engine).await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: outbox drain failed");
    }
}

/// Settlement catch-up: capture any settlement the live `watch()` stream missed. A settlement can be
/// missed while the daemon is down, dropped in a `watch()` restart gap, or buffered past the bounded
/// channel — leaving a PAID order/renewal stuck OPEN forever. This scans every OPEN settlement-
/// bearing invoice (`order` and `renewal`), `lookup`s it on the backend, and `capture`s the ones
/// reported `Paid` exactly as the settlement loop would. Fully idempotent: an already-applied invoice
/// is no longer `OPEN` (filtered out), and capture's OPEN->PAID CAS no-ops a settlement the watch
/// loop also delivered. Returns the number of invoices it captured/refunded this pass.
/// (Supervisor-level SQL is allowed here — it only reuses the existing capture + lookup seams.)
async fn settlement_catch_up(
    store: &Store,
    payment: &Arc<dyn PaymentBackend>,
    clock: &Arc<dyn Clock>,
) -> Result<usize> {
    // (id, external_id, amount_sat, expires_at, kind, sub.paid_through, sub.retention_s)
    type CatchUpRow = (String, String, i64, Option<i64>, String, Option<i64>, Option<i64>);
    let open: Vec<CatchUpRow> = store
        .read(|c| {
            let mut stmt = c.prepare(
                "SELECT i.id, i.external_id, COALESCE(i.amount_sat, 0), i.expires_at, i.kind,
                        s.paid_through, s.retention_s
                   FROM invoice i
                   LEFT JOIN subscription s ON s.id = i.subscription_id
                  WHERE i.status='OPEN' AND i.kind IN ('order', 'renewal')",
            )?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<i64>>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, Option<i64>>(5)?,
                        r.get::<_, Option<i64>>(6)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await?;

    let mut caught = 0;
    for (inv_id, external_id, amount_sat, expires_at, kind, paid_through, retention_s) in open {
        match payment.lookup(&inv_id) {
            Ok(PaymentStatus::Paid) => {
                let now = clock.now();
                // Latest in-window settle time the recovered payment can carry: the invoice expiry (the
                // order gate, capture refunds at settled_at >= expires_at), and for a renewal ALSO
                // `paid_through + retention_s` (capture refunds a renewal at settled_at >= that boundary).
                // Cap by the binding one, so a renewal paid in time but recovered late isn't stamped past
                // retention and wrongly refunded.
                let renewal_boundary = match (kind.as_str(), paid_through, retention_s) {
                    ("renewal", Some(pt), Some(ret)) => Some(pt + ret),
                    _ => None,
                };
                let settled_at = recovered_settled_at(now, min_opt(expires_at, renewal_boundary));
                let settlement = Settlement {
                    invoice_id: inv_id,
                    external_id: external_id.clone(),
                    amount_sat: amount_sat as u64,
                    settled_at,
                };
                match capture(store, settlement, now).await {
                    Ok(Capture::Captured | Capture::RefundDue) => caught += 1,
                    Ok(_) => {}
                    Err(e) => tracing::error!(
                        external = %external_id,
                        error = %format!("{e:#}"),
                        "settlement catch-up: capture failed"
                    ),
                }
            }
            Ok(_) => {} // still Open / Expired — nothing to capture
            Err(e) => tracing::warn!(
                external = %external_id,
                error = %format!("{e:#}"),
                "settlement catch-up: backend lookup failed"
            ),
        }
    }
    Ok(caught)
}

/// `PaymentBackend::lookup` tells us that an OPEN invoice is already paid but does not expose the
/// backend's paid timestamp. On catch-up after the local invoice expiry, using `now` would fabricate
/// a too-late settlement and refund a payment the backend had already marked paid while reconcile
/// deliberately kept the invoice OPEN for capture. Use the latest in-window timestamp (the binding
/// `cap`: the invoice expiry, and for a renewal also `paid_through + retention_s`) instead; live
/// settlements still carry their exact backend timestamp.
fn recovered_settled_at(now: i64, cap: Option<i64>) -> i64 {
    match cap {
        Some(c) if now >= c => c.saturating_sub(1),
        _ => now,
    }
}

/// The smaller of two optional settle-time bounds (the binding cap), ignoring absent ones.
fn min_opt(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

/// Read the single-box capacity for the order-intake reservation budget + the box id for instance
/// rows. M1a is single-box; onboard (a later bead) writes the row. Until then, fall back to a
/// BOUNDED, clearly-logged budget rather than an unlimited one.
async fn load_budget_and_box(store: &Store) -> (Budget, String) {
    let row: Option<(String, Option<String>)> = match store
        .read(|c| {
            Ok(c.query_row(
                "SELECT id, capacity_json FROM box ORDER BY id LIMIT 1",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
            )
            .optional()?)
        })
        .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "reading box capacity failed; using M1a fallback budget");
            None
        }
    };

    match row {
        Some((box_id, capacity_json)) => {
            let budget = capacity_json
                .as_deref()
                .and_then(parse_budget)
                .unwrap_or_else(|| {
                    tracing::warn!(box_id = %box_id, "box row has no/invalid capacity_json; using bounded M1a fallback budget");
                    M1A_FALLBACK_BUDGET
                });
            (budget, box_id)
        }
        None => {
            tracing::warn!(
                budget = ?M1A_FALLBACK_BUDGET,
                box_id = M1A_FALLBACK_BOX_ID,
                "no box row yet (onboard pending); using BOUNDED M1a fallback budget + box id"
            );
            (M1A_FALLBACK_BUDGET, M1A_FALLBACK_BOX_ID.to_string())
        }
    }
}

/// Parse a `box.capacity_json` (`{cpu, mem_mb, disk_gb, ports}`) into a [`Budget`]. Missing `cpu`/
/// `mem_mb`/`disk_gb` make it `None` (fall back); `ports` defaults to 0.
fn parse_budget(capacity_json: &str) -> Option<Budget> {
    let v: Value = serde_json::from_str(capacity_json).ok()?;
    Some(Budget {
        cpu: v.get("cpu").and_then(Value::as_u64)? as u32,
        mem_mb: v.get("mem_mb").and_then(Value::as_u64)? as u32,
        disk_gb: v.get("disk_gb").and_then(Value::as_u64)? as u32,
        ports: v.get("ports").and_then(Value::as_u64).unwrap_or(0) as u32,
    })
}

/// Convert a recipe pricing duration string (`"30d"`, `"7d"`, `"12h"`, `"3600"`, …) to seconds for
/// the durable `listing` row. A trailing `s`/`m`/`h`/`d`/`w` is the unit; a bare number is seconds.
/// An unparseable value logs and falls back to [`DEFAULT_DURATION_S`] so a malformed recipe cannot
/// write a zero/negative timer that would wedge capture/reconcile math.
fn duration_secs(s: &str) -> i64 {
    let s = s.trim();
    if s.is_empty() {
        return DEFAULT_DURATION_S;
    }
    let (num, unit) = match s.as_bytes()[s.len() - 1] {
        b's' => (&s[..s.len() - 1], 1),
        b'm' => (&s[..s.len() - 1], 60),
        b'h' => (&s[..s.len() - 1], 3_600),
        b'd' => (&s[..s.len() - 1], 86_400),
        b'w' => (&s[..s.len() - 1], 604_800),
        _ => (s, 1), // a bare number (seconds), or something that won't parse below
    };
    match num.trim().parse::<i64>() {
        Ok(n) if n > 0 => n.saturating_mul(unit),
        _ => {
            tracing::warn!(value = %s, "unparseable recipe duration; using default");
            DEFAULT_DURATION_S
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovered_settle_time_is_capped_to_the_binding_in_window_boundary() {
        // Recovered before the cap → keep `now` (exact-enough, in window).
        assert_eq!(recovered_settled_at(900, Some(1000)), 900);
        // Recovered at/after the cap → just inside it, never past the gate.
        assert_eq!(recovered_settled_at(1000, Some(1000)), 999);
        assert_eq!(recovered_settled_at(5000, Some(1000)), 999);
        // No cap (no expiry) → `now`.
        assert_eq!(recovered_settled_at(1234, None), 1234);

        // min_opt picks the binding (smaller) bound; absent bounds are ignored.
        assert_eq!(min_opt(Some(5190), Some(1600)), Some(1600));
        assert_eq!(min_opt(Some(1600), None), Some(1600));
        assert_eq!(min_opt(None, Some(1600)), Some(1600));
        assert_eq!(min_opt(None, None), None);

        // The codex P1 scenario: a renewal (invoice expiry 5190) issued near retention
        // (paid_through 1000 + retention 600 = 1600), recovered late at now=1700. The binding cap is the
        // renewal retention boundary 1600, so settled_at=1599 < 1600 → capture extends, does NOT refund.
        let cap = min_opt(Some(5190), Some(1000 + 600));
        assert_eq!(recovered_settled_at(1700, cap), 1599);
    }

    #[test]
    fn duration_secs_parses_units_and_falls_back() {
        assert_eq!(duration_secs("30d"), 30 * 86_400);
        assert_eq!(duration_secs("7d"), 7 * 86_400);
        assert_eq!(duration_secs("12h"), 12 * 3_600);
        assert_eq!(duration_secs("90m"), 90 * 60);
        assert_eq!(duration_secs("1w"), 604_800);
        assert_eq!(duration_secs("3600"), 3_600); // bare number = seconds
        assert_eq!(duration_secs("nonsense"), DEFAULT_DURATION_S);
        assert_eq!(duration_secs(""), DEFAULT_DURATION_S);
        assert_eq!(duration_secs("0d"), DEFAULT_DURATION_S); // non-positive -> default
    }

    #[test]
    fn parse_budget_reads_capacity_or_none() {
        let b = parse_budget(r#"{"cpu":4,"mem_mb":8192,"disk_gb":100,"ports":32}"#).unwrap();
        assert_eq!(b.cpu, 4);
        assert_eq!(b.mem_mb, 8192);
        assert_eq!(b.disk_gb, 100);
        assert_eq!(b.ports, 32);
        // ports optional (defaults 0); cpu missing -> None.
        assert_eq!(
            parse_budget(r#"{"cpu":2,"mem_mb":1,"disk_gb":1}"#)
                .unwrap()
                .ports,
            0
        );
        assert!(parse_budget(r#"{"mem_mb":1,"disk_gb":1}"#).is_none());
        assert!(parse_budget("not json").is_none());
    }
}
