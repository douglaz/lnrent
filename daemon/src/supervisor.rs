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
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};

use crate::backends::{PayStatus, PaymentBackend, PaymentStatus, Settlement};
use crate::capture::{capture, Capture};
use crate::clock::Clock;
use crate::ipc;
use crate::nostr_engine::{
    listing_from_recipe, InboundDrainResult, NostrEngine, OpHandler, OrderHandler, Outbound,
};
use crate::op_dispatch::OpDispatch;
use crate::order_intake::OrderIntake;
use crate::provision::{DeliveryResendOrderHandler, OutboxSender, Provisioner};
use crate::recipe::Recipe;
use crate::alerts::{Alert, AlertDispatcher, AlertKind};
use crate::relay_status::{
    RelayBlackoutMonitor, RelayStatusCell, RelayStatusRow, RELAY_BLACKOUT_ALERT_S,
};
use crate::reconcile::Reconciler;
use crate::refund::{gen_key, parse_whole_sat, Refunder};
use crate::refund_resolver::RefundResolver;
use crate::sweep::Sweeper;
use crate::reservation::Budget;
use crate::resume::ResumeDriver;
use crate::store::{
    RefundAttemptLiability, RefundReadinessLiability, RefundReadinessSource, Store,
};

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
    resume_driver: Arc<ResumeDriver>,
    refunder: Arc<Refunder>,
    /// Operator sweep (gate1-operator-sweep, urw.3): pays the operator's own bolt11 from ledger
    /// surplus. Driven (crash recovery) at boot and each maintenance pass, right after the refunder.
    sweeper: Arc<Sweeper>,
    reconciler: Arc<Reconciler>,
    /// GATE-1 alert sink, retained for the maintenance loop's relay-blackout check (lnrent-urw.6);
    /// the drivers each hold their own clone.
    alerts: Arc<AlertDispatcher>,
    /// Shared relay-liveness snapshot: the maintenance loop refreshes it each tick from the engine
    /// pool, the IPC `Request::Relays`/`Status` paths read it (lnrent-urw.6).
    relays: RelayStatusCell,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        store: Store,
        engine: NostrEngine,
        payment: Arc<dyn PaymentBackend>,
        clock: Arc<dyn Clock>,
        resolver: Arc<dyn RefundResolver>,
        alerts: Arc<AlertDispatcher>,
        recipe: Recipe,
        sock_path: PathBuf,
        intervals: Intervals,
        max_live_holds_per_buyer: u32,
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
            max_live_holds_per_buyer,
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

        // GATE-1 alert sink: shared into every condition owner (lnrent-urw.1 refunder + lnrent-urw.2
        // reconciler teardown + provisioner cleanup backlog). Cheap Arc clones share one cooldown map.
        let provisioner = Arc::new(
            Provisioner::new(store.clone(), clock.clone(), recipe.clone(), box_id)
                .with_alerts(alerts.clone()),
        );
        let resume_driver = Arc::new(ResumeDriver::new(
            store.clone(),
            clock.clone(),
            recipe.clone(),
        ));
        let refunder = Arc::new(
            Refunder::with_resolver(store.clone(), payment.clone(), clock.clone(), resolver)
                .with_alerts(alerts.clone()),
        );
        // Operator sweep (gate1-operator-sweep, urw.3): same store+payment+clock the money path uses,
        // plus the shared alert sink so a parked FAILED sweep surfaces a durable operator DM.
        let sweeper = Arc::new(
            Sweeper::new(store.clone(), payment.clone(), clock.clone())
                .with_alerts(alerts.clone()),
        );
        let reconciler = Arc::new(
            Reconciler::new(store.clone(), payment.clone(), recipe.clone())
                .with_alerts(alerts.clone()),
        );
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
            resume_driver,
            refunder,
            sweeper,
            reconciler,
            alerts,
            relays: RelayStatusCell::new(),
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
    /// op-dispatch interrupted ops, downtime credit, settlement catch-up, provisioning re-drive
    /// (+ failed cleanups), refunds, sweeps, a reconcile catch-up tick, then the outbox drain. Each step
    /// is idempotent; an error in one is logged and does not block the rest (the periodic loops retry).
    async fn boot_recovery(&self) -> Result<()> {
        tracing::info!(
            "boot recovery: op-dispatch -> downtime-credit -> settlement catch-up -> provision -> resume -> refund -> sweep -> reconcile -> outbox"
        );

        match self.op_dispatch.recover_interrupted_ops().await {
            Ok(n) => tracing::info!(count = n, "boot recovery: interrupted ops recovered"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: op-dispatch recovery failed")
            }
        }
        // Credit the operator's outage BEFORE settlement catch-up and reconcile catch-up, so a buyer
        // whose renewal was paid during the outage is recovered against the credited boundary, and a
        // buyer whose soft-date/paid_through fell inside downtime is not suspended for it (§6.5). The
        // credit raises the suspend floor (+ leaves a pre-reminder cursor) that both later steps honor.
        match self
            .reconciler
            .apply_restart_downtime_credit(self.clock.now())
            .await
        {
            Ok(n) => tracing::info!(count = n, "boot recovery: downtime credit applied"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: downtime credit failed")
            }
        }
        // Catch any settlement the live watch() stream missed while the daemon was down (a PAID order
        // left stuck PENDING forever otherwise); each caught order moves to PROVISIONING for the
        // re-drive below. Idempotent — capture's OPEN->PAID CAS no-ops an already-applied settlement.
        // Runs after downtime credit so renewal catch-up caps recovered settlements at the same
        // credited boundary capture's refund gate uses.
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
        match self.resume_driver.recover().await {
            Ok(n) => tracing::info!(count = n, "boot recovery: resumes re-driven"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: resume recovery failed")
            }
        }
        match self.refunder.drive().await {
            Ok(rep) => tracing::info!(?rep, "boot recovery: refunds driven"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: refund recovery failed")
            }
        }
        // Finish any sweep the daemon crashed mid-flight (gate1-operator-sweep, urw.3): re-await a
        // started key or re-gate an unstarted intent. Idempotent; runs after the refunder so refund
        // liabilities are settled before a sweep re-gates against the current surplus.
        match self.sweeper.drive().await {
            Ok(rep) => tracing::info!(?rep, "boot recovery: sweeps driven"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "boot recovery: sweep recovery failed")
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
            .await
            .context("opening payment settlement stream")?;

        self.publish_and_upsert_listing().await?;
        self.boot_recovery().await?;
        log_refund_readiness(&self.store, &self.payment).await;

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
            let payment = self.payment.clone();
            let relays = self.relays.clone();
            tasks.push(tokio::spawn(supervise(
                "ipc",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (store, recipes, clock, payment, relays, sock) = (
                        store.clone(),
                        recipes.clone(),
                        clock.clone(),
                        payment.clone(),
                        relays.clone(),
                        sock.clone(),
                    );
                    async move {
                        ipc::serve_with_shutdown(store, recipes, clock, payment, relays, &sock, sd)
                            .await
                    }
                },
            )));
        }

        // -- Nostr inbound loop (decode/dedupe/route order + op DMs) --
        let inbound_task = {
            let engine = self.engine.clone();
            let order = self.order_handler.clone();
            let op = self.op_handler.clone();
            tokio::spawn(supervise_inbound(
                "nostr-inbound",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                engine,
                order,
                op,
            ))
        };

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
                                .await
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

        // -- Single serialized maintenance loop (clock sync + periodic settlement catch-up + provision + resume + refund + sweep + relay-blackout check + outbox) --
        {
            #[allow(clippy::type_complexity)]
            let (
                provisioner,
                resume_driver,
                refunder,
                sweeper,
                payment,
                engine,
                store,
                clock,
                nudge2,
                sync,
                alerts,
                relays,
            ) = (
                self.provisioner.clone(),
                self.resume_driver.clone(),
                self.refunder.clone(),
                self.sweeper.clone(),
                self.payment.clone(),
                self.engine.clone(),
                self.store.clone(),
                self.clock.clone(),
                nudge.clone(),
                self.payment_clock_sync.clone(),
                self.alerts.clone(),
                self.relays.clone(),
            );
            let interval = self.intervals.maintenance;
            tasks.push(tokio::spawn(supervise(
                "maintenance",
                shutdown_rx.clone(),
                RESTART_BACKOFF,
                move |sd| {
                    let (
                        provisioner,
                        resume_driver,
                        refunder,
                        sweeper,
                        payment,
                        engine,
                        store,
                        clock,
                        nudge2,
                        sync,
                        alerts,
                        relays,
                    ) = (
                        provisioner.clone(),
                        resume_driver.clone(),
                        refunder.clone(),
                        sweeper.clone(),
                        payment.clone(),
                        engine.clone(),
                        store.clone(),
                        clock.clone(),
                        nudge2.clone(),
                        sync.clone(),
                        alerts.clone(),
                        relays.clone(),
                    );
                    async move {
                        run_maintenance_loop(
                            provisioner,
                            resume_driver,
                            refunder,
                            sweeper,
                            payment,
                            store,
                            clock,
                            engine,
                            sync,
                            alerts,
                            relays,
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
            inbound_task: Some(inbound_task),
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
}

impl InboundDrain {
    fn enter(self: &Arc<Self>) -> InboundGuard {
        self.active.fetch_add(1, Ordering::SeqCst);
        InboundGuard {
            drain: self.clone(),
        }
    }

    fn active_count(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

struct InboundGuard {
    drain: Arc<InboundDrain>,
}

impl Drop for InboundGuard {
    fn drop(&mut self) {
        self.drain.active.fetch_sub(1, Ordering::SeqCst);
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
    inbound_task: Option<JoinHandle<()>>,
    tasks: Vec<JoinHandle<()>>,
    engine: NostrEngine,
    store: Store,
    clock: Arc<dyn Clock>,
    inbound_drain: Arc<InboundDrain>,
}

impl RunningSupervisor {
    /// Graceful shutdown: stop accepting new work, let in-flight transactions commit (the store actor
    /// drains its queue), flush the outbox once, then stop every loop. The wind-down is bounded by
    /// ~2×[`SHUTDOWN_GRACE`] — the inbound loop drains its accepted per-wrap tasks (step 1b) before the
    /// remaining loops are awaited (step 2), each with its own `SHUTDOWN_GRACE` window — plus the final
    /// outbox flush.
    pub async fn shutdown(mut self) -> Result<()> {
        tracing::info!("supervisor: graceful shutdown starting");
        // 1. Signal every loop. The inbound supervisor special-cases run_inbound: it asks the engine
        //    to stop accepting and drains accepted per-wrap tasks before returning.
        let _ = self.shutdown_tx.send(true);

        if let Some(inbound) = self.inbound_task.take() {
            let abort = inbound.abort_handle();
            if tokio::time::timeout(SHUTDOWN_GRACE, inbound).await.is_err() {
                tracing::warn!(
                    "supervisor: inbound drain did not stop within the grace window; aborting wrapper"
                );
                abort.abort();
            }
        }

        // 2. Wait for the remaining loops to wind down (take the handles out so Drop is a no-op afterward).
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
        // 3. InboundDrain is diagnostic-only; the engine-owned TaskTracker above is authoritative.
        let active = self.inbound_drain.active_count();
        if active > 0 {
            tracing::warn!(
                active,
                "supervisor: diagnostic inbound handler counter still active after engine drain"
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
        if let Some(t) = &self.inbound_task {
            t.abort();
        }
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

/// Nostr inbound supervision is separate from [`supervise`] because graceful shutdown must not drop
/// `run_inbound` before the engine has stopped accepting and drained accepted per-wrap work.
async fn supervise_inbound(
    name: &'static str,
    mut shutdown: watch::Receiver<bool>,
    backoff: Backoff,
    engine: NostrEngine,
    order: Arc<dyn OrderHandler>,
    op: Arc<dyn OpHandler>,
) {
    let mut delay = backoff.base;
    loop {
        if *shutdown.borrow() {
            // A prior run_inbound may have exited (relay drop -> restart backoff) leaving accepted
            // per-wrap handlers still committing; drain them before returning so the final outbox flush
            // includes their replies rather than detaching them. No-op on the first iteration.
            drain_stopped_inbound(name, &engine).await;
            return;
        }
        let started = tokio::time::Instant::now();
        let run_engine = engine.clone();
        let run_order = order.clone();
        let run_op = op.clone();
        let mut child =
            tokio::spawn(async move { run_engine.run_inbound(run_order, run_op).await });
        // If this inbound supervisor wrapper is aborted or dropped, abort run_inbound too. Dropping
        // the JoinHandle alone would detach the child and leave the receive loop alive.
        let guard = AbortOnDrop(child.abort_handle());

        let outcome = tokio::select! {
            biased;
            _ = wait_for_shutdown(&mut shutdown) => {
                drain_running_inbound(name, &engine, &mut child).await;
                return;
            }
            out = &mut child => out,
        };
        drop(guard);

        if *shutdown.borrow() {
            drain_stopped_inbound(name, &engine).await;
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

        if started.elapsed() >= backoff.max {
            delay = backoff.base;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = wait_for_shutdown(&mut shutdown) => {
                drain_stopped_inbound(name, &engine).await;
                return;
            }
        }
        delay = delay.saturating_mul(2).min(backoff.max);
    }
}

async fn drain_running_inbound(
    name: &'static str,
    engine: &NostrEngine,
    child: &mut JoinHandle<Result<()>>,
) {
    match engine.drain(SHUTDOWN_DRAIN, child).await {
        InboundDrainResult::Drained => {
            tracing::info!(task = name, "inbound loop drained for shutdown");
        }
        InboundDrainResult::TimedOut => {
            tracing::warn!(task = name, "inbound loop drain timed out during shutdown");
        }
    }
}

async fn drain_stopped_inbound(name: &'static str, engine: &NostrEngine) {
    match engine.drain_after_inbound_stopped(SHUTDOWN_DRAIN).await {
        InboundDrainResult::Drained => {
            tracing::info!(task = name, "stopped inbound loop drained for shutdown");
        }
        InboundDrainResult::TimedOut => {
            tracing::warn!(
                task = name,
                "stopped inbound loop drain timed out during shutdown"
            );
        }
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
/// `Captured`/`Resumed`/`RefundDue` outcome NUDGE the maintenance loop so provision/resume/refund
/// runs promptly.
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
                    Ok(Capture::Captured) | Ok(Capture::Resumed) | Ok(Capture::RefundDue) => {
                        // Provisioning, resume, or refund work is now pending — wake the maintenance
                        // loop instead of waiting for its interval.
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
/// concurrent drives could run recipe hooks before one loses the CAS; resume has the same hook
/// shape, so both drivers stay on this one loop.
#[allow(clippy::too_many_arguments)]
async fn run_maintenance_loop(
    provisioner: Arc<Provisioner>,
    resume_driver: Arc<ResumeDriver>,
    refunder: Arc<Refunder>,
    sweeper: Arc<Sweeper>,
    payment: Arc<dyn PaymentBackend>,
    store: Store,
    clock: Arc<dyn Clock>,
    engine: NostrEngine,
    sync: Option<Arc<dyn Fn(i64) + Send + Sync>>,
    alerts: Arc<AlertDispatcher>,
    relays: RelayStatusCell,
    interval: Duration,
    nudge: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let outbox = OutboxSender::new(store.clone(), clock.clone());
    // The relay-blackout edge-trigger state lives across ticks (lnrent-urw.6). A supervised restart
    // of this loop re-arms it — worst case one duplicate alert for a blackout spanning the restart.
    let mut blackout = RelayBlackoutMonitor::new();
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
            &resume_driver,
            &refunder,
            &sweeper,
            &payment,
            &store,
            &clock,
            &outbox,
            &engine,
            sync.as_ref(),
            &alerts,
            &relays,
            &mut blackout,
            run_catch_up,
        )
        .await;
    }
}

/// One serialized maintenance pass: keep the mock payment clock in step, periodically catch any
/// missed settlements, drive PROVISIONING subs to ACTIVE/REFUND_DUE (+ finish failed cleanups), drive
/// RESUMING subs to ACTIVE or a detached renewal refund, pay PENDING refunds, finish any
/// crash-interrupted operator sweep, then deliver unsent DMs (provision.ready, billing.refund,
/// suspend/terminate notices). Each step is independently idempotent; an error in one is logged and
/// does not block the others.
#[allow(clippy::too_many_arguments)]
async fn maintenance_pass(
    provisioner: &Provisioner,
    resume_driver: &ResumeDriver,
    refunder: &Refunder,
    sweeper: &Sweeper,
    payment: &Arc<dyn PaymentBackend>,
    store: &Store,
    clock: &Arc<dyn Clock>,
    outbox: &OutboxSender,
    engine: &NostrEngine,
    sync: Option<&Arc<dyn Fn(i64) + Send + Sync>>,
    alerts: &Arc<AlertDispatcher>,
    relays: &RelayStatusCell,
    blackout: &mut RelayBlackoutMonitor,
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
    if let Err(e) = resume_driver.recover().await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: resume drive failed");
    }
    if let Err(e) = refunder.drive().await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: refund drive failed");
    }
    // Finish any crash-interrupted operator sweep (gate1-operator-sweep, urw.3), right after the
    // refunder so a sweep re-gates against a surplus that already reflects settled refunds.
    if let Err(e) = sweeper.drive().await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: sweep drive failed");
    }
    log_refund_readiness(store, payment).await;
    let relay_rows = engine.relay_status_snapshot().await;
    refresh_relay_status(relay_rows, relays, blackout, alerts, clock.now()).await;
    if let Err(e) = outbox.drain_once(engine).await {
        tracing::error!(error = %format!("{e:#}"), "maintenance: outbox drain failed");
    }
}

/// Publish the tick's relay-liveness `rows` to the shared snapshot and fire a single edge-triggered
/// `RelayBlackout` alert if the pool has been fully disconnected past the threshold (lnrent-urw.6,
/// GATE-1 PR-9c). Takes the already-projected rows (the caller reads them from the engine pool) so
/// the alert/edge-trigger logic is testable without a live socket. Best-effort: never fails the
/// tick. Honest caveat — this alert is the one that cannot be delivered during the very blackout it
/// reports; it queues in the outbox and drains on reconnect, and `Request::Relays` is the
/// out-of-band read for the meantime.
async fn refresh_relay_status(
    rows: Vec<RelayStatusRow>,
    relays: &RelayStatusCell,
    blackout: &mut RelayBlackoutMonitor,
    alerts: &Arc<AlertDispatcher>,
    now: i64,
) {
    if blackout.onset_due(&rows, now) {
        let onset = blackout.onset().unwrap_or(now);
        tracing::error!(
            relays = rows.len(),
            onset,
            "relay blackout: every relay has been disconnected for over {}min — inbound orders and \
             outbound DMs are not flowing; check relay reachability (`lnrent relays`)",
            RELAY_BLACKOUT_ALERT_S / 60
        );
        let detail = format!(
            "all {} configured relay(s) have been disconnected for over {}min. Inbound orders and \
             outbound refund/billing DMs are not flowing. This alert itself queues until a relay \
             reconnects — check relay reachability (`lnrent relays` reads the pool out-of-band).",
            rows.len(),
            RELAY_BLACKOUT_ALERT_S / 60,
        );
        // Key the alert on the ONSET so a distinct blackout soon after recovery re-alerts despite
        // the dispatcher's per-(kind, subject) 6h cooldown (codex). Confirm `fired` only once the
        // alert is durably enqueued, so a dispatch failure retries next tick (coderabbit).
        let subject = format!("relay-pool@{onset}");
        match alerts
            .dispatch(Alert::new(AlertKind::RelayBlackout, subject, detail))
            .await
        {
            Ok(_) => blackout.mark_fired(),
            Err(e) => tracing::warn!(
                error = %format!("{e:#}"),
                "failed to enqueue RelayBlackout alert; leaving the onset eligible to retry next tick"
            ),
        }
    }
    relays.set(rows);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefundReadinessReport {
    liability_count: usize,
    gross_liability_sat: u128,
    required_msat: u128,
    /// The ledger's conservative LOWER bound on spendable holdings (`ledger::expected_msat`, §D) —
    /// the balance operand the readiness compare now uses instead of a live federation query (§E).
    expected_msat: u128,
    gateway_ok: bool,
    /// LIVENESS: whether the federation guardians are reachable (lnrent-urw.4). Distinct from
    /// `gateway_ok` — a down federation is a different failure than a down gateway.
    federation_ok: bool,
    parked_count: usize,
    /// PENDING liabilities that could not be priced this pass (transient quote/gateway error). Their
    /// cost is absent from `required_msat`, so coverage cannot be confirmed — forces a warning.
    unpriceable_count: usize,
    warning: Option<RefundReadinessWarning>,
}

impl Default for RefundReadinessReport {
    fn default() -> Self {
        Self {
            liability_count: 0,
            gross_liability_sat: 0,
            required_msat: 0,
            expected_msat: 0,
            gateway_ok: true,
            federation_ok: true,
            parked_count: 0,
            unpriceable_count: 0,
            warning: None,
        }
    }
}

impl RefundReadinessReport {
    pub(crate) fn to_money_value(&self, gateway_ok: bool, federation_ok: bool) -> Value {
        json!({
            "expected_msat": self.expected_msat,
            "gateway_ok": gateway_ok,
            "federation_ok": federation_ok,
            "liability_count": self.liability_count,
            "gross_liability_sat": self.gross_liability_sat,
            "required_msat": self.required_msat,
            "parked_count": self.parked_count,
            "ready": self.warning.is_none(),
            "warning": self.warning.map(RefundReadinessWarning::as_str),
        })
    }
}

/// The two LIVENESS probes the readiness path still makes (§E): the refund gateway and the
/// federation guardians. NO balance read — that is retired from every automatic path (ledger
/// `expected_msat` is the balance operand now); the wallet is read solely by `reconcile` (§F).
#[derive(Debug, Clone)]
pub(crate) struct RefundReadinessProbe {
    gateway_ok: bool,
    gateway_error: Option<String>,
    federation_ok: bool,
    federation_error: Option<String>,
}

impl RefundReadinessProbe {
    pub(crate) async fn query(payment: &Arc<dyn PaymentBackend>) -> Self {
        let (gateway_ok, gateway_error) = match payment.refund_gateway_ready().await {
            Ok(ok) => (ok, None),
            Err(e) => (false, Some(format!("{e:#}"))),
        };
        // Federation LIVENESS (lnrent-urw.4): a guardian round-trip, distinct from the gateway read.
        // An error OR an explicit `false` means the federation is not reachable.
        let (federation_ok, federation_error) = match payment.backend_ready().await {
            Ok(ok) => (ok, None),
            Err(e) => (false, Some(format!("{e:#}"))),
        };

        Self {
            gateway_ok,
            gateway_error,
            federation_ok,
            federation_error,
        }
    }

    pub(crate) fn gateway_ok(&self) -> bool {
        self.gateway_ok
    }

    pub(crate) fn federation_ok(&self) -> bool {
        self.federation_ok
    }

    fn log_failures(&self) {
        if let Some(e) = &self.gateway_error {
            tracing::warn!(error = %e, "refund readiness: gateway readiness query failed");
        }
        if let Some(e) = &self.federation_error {
            tracing::warn!(error = %e, "refund readiness: federation liveness probe failed (guardians unreachable)");
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefundReadinessWarning {
    /// The federation guardians are unreachable (lnrent-urw.4): the root infra failure — nothing can
    /// be invoiced, paid, or reconciled. Distinct from (and higher priority than) a down gateway.
    FederationDown,
    GatewayUnavailable,
    /// The ledger's expected holdings (`expected_msat`, §D) are below the required refund outlay: the
    /// books say we cannot cover what is owed. (Was a live balance compare; ledger-derived since §E.)
    InsufficientBalance,
    /// A real PENDING liability could not be priced (its outlay is missing from `required_msat`), so
    /// coverage cannot be confirmed — warn rather than falsely report "covered".
    Unpriceable,
    ParkedManual,
}

impl RefundReadinessWarning {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RefundReadinessWarning::FederationDown => "FederationDown",
            RefundReadinessWarning::GatewayUnavailable => "GatewayUnavailable",
            RefundReadinessWarning::InsufficientBalance => "InsufficientBalance",
            RefundReadinessWarning::Unpriceable => "Unpriceable",
            RefundReadinessWarning::ParkedManual => "ParkedManual",
        }
    }
}

async fn log_refund_readiness(store: &Store, payment: &Arc<dyn PaymentBackend>) {
    let report = match refund_readiness_report(store, payment).await {
        Ok(report) => report,
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "refund readiness check failed");
            return;
        }
    };
    // FederationDown is a ROOT infra failure (guardians unreachable) — it must alarm even at zero
    // liabilities (the idle/pre-go-live case codex flagged); every other warning is a refund-coverage
    // condition that only matters when something is owed.
    if report.liability_count == 0 && report.warning != Some(RefundReadinessWarning::FederationDown) {
        return;
    }

    match report.warning {
        Some(RefundReadinessWarning::FederationDown) => tracing::error!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            federation_ok = report.federation_ok,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            "refund readiness ALARM: the FEDERATION is unreachable (guardians down / no consensus) — no invoices, payments, or refunds can settle; investigate the federation"
        ),
        Some(RefundReadinessWarning::GatewayUnavailable) => tracing::warn!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            expected_msat = %report.expected_msat,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            "refund readiness warning: gateway unreachable: cannot create invoices or pay refunds"
        ),
        Some(RefundReadinessWarning::InsufficientBalance) => tracing::warn!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            expected_msat = %report.expected_msat,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            "refund readiness warning: ledger expected holdings are below required refund outlay"
        ),
        Some(RefundReadinessWarning::ParkedManual) => tracing::warn!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            expected_msat = %report.expected_msat,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            "refund readiness warning: parked refund liabilities require manual handling"
        ),
        Some(RefundReadinessWarning::Unpriceable) => tracing::warn!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            expected_msat = %report.expected_msat,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            unpriceable_count = report.unpriceable_count,
            "refund readiness warning: a pending refund liability could not be priced; coverage unconfirmed"
        ),
        None => tracing::info!(
            liabilities = report.liability_count,
            gross_liability_sat = %report.gross_liability_sat,
            required_outlay_msat = %report.required_msat,
            expected_msat = %report.expected_msat,
            gateway_ok = report.gateway_ok,
            parked_count = report.parked_count,
            "refund liabilities covered"
        ),
    }
}

pub(crate) async fn refund_readiness_report(
    store: &Store,
    payment: &Arc<dyn PaymentBackend>,
) -> Result<RefundReadinessReport> {
    // Always probe — the liveness check must run even at ZERO liabilities so a down federation
    // surfaces to an idle operator (codex). Delegates to the shared builder, which folds the
    // empty-liabilities + federation-down cases correctly. A `session_count()` guardian round-trip
    // per readiness check is the intended cost of liveness monitoring.
    let probe = RefundReadinessProbe::query(payment).await;
    refund_readiness_report_with_probe(store, payment, &probe).await
}

pub(crate) async fn refund_readiness_report_with_probe(
    store: &Store,
    payment: &Arc<dyn PaymentBackend>,
    probe: &RefundReadinessProbe,
) -> Result<RefundReadinessReport> {
    // §D/§E: the ledger's conservative lower bound on spendable holdings is the balance operand now
    // — a pure LOCAL read (no federation balance call). Computed UNCONDITIONALLY so `lnrent money`
    // reports real expected holdings even at zero liabilities (as the old view showed the real
    // balance), and so the money display value is exactly the figure the verdict compares.
    let expected_msat = crate::ledger::expected_msat(store, payment).await?;
    let liabilities = store.refund_readiness_liabilities().await?;
    if liabilities.is_empty() {
        // Even with NO refund liabilities, a down federation must still surface (codex): it is a
        // fundamental infra failure (no invoices/payments can settle), not a refund-coverage
        // question — otherwise an idle/pre-go-live operator sees READY and announces a listing while
        // guardians are unreachable. Refund-coverage warnings (balance/parked/unpriceable) rightly
        // stay silent with nothing owed.
        probe.log_failures();
        return Ok(RefundReadinessReport {
            expected_msat,
            gateway_ok: probe.gateway_ok,
            federation_ok: probe.federation_ok,
            warning: (!probe.federation_ok).then_some(RefundReadinessWarning::FederationDown),
            ..Default::default()
        });
    }

    refund_readiness_report_from_liabilities(liabilities, expected_msat, payment, probe).await
}

async fn refund_readiness_report_from_liabilities(
    liabilities: Vec<RefundReadinessLiability>,
    expected_msat: u128,
    payment: &Arc<dyn PaymentBackend>,
    probe: &RefundReadinessProbe,
) -> Result<RefundReadinessReport> {
    probe.log_failures();

    let mut report = RefundReadinessReport {
        liability_count: liabilities.len(),
        gross_liability_sat: 0,
        required_msat: 0,
        expected_msat,
        gateway_ok: probe.gateway_ok,
        federation_ok: probe.federation_ok,
        parked_count: 0,
        unpriceable_count: 0,
        warning: None,
    };

    for liability in &liabilities {
        report.gross_liability_sat += u128::from(liability.gross_sat);
        match &liability.source {
            RefundReadinessSource::RefundAttempt(refund) => {
                if refund.status == "FAILED" {
                    report.parked_count += 1;
                    continue;
                }
                if refund.status != "PENDING" {
                    continue;
                }
                match pending_refund_required_msat(liability, refund, payment).await {
                    Ok(msat) => report.required_msat = report.required_msat.saturating_add(msat),
                    Err(e) => {
                        report.unpriceable_count += 1;
                        tracing::warn!(
                            external_id = %liability.external_id,
                            error = %format!("{e:#}"),
                            "refund readiness: could not price pending refund liability"
                        );
                    }
                }
            }
            RefundReadinessSource::PaidUndeliveredOrder
            | RefundReadinessSource::UnreconciledSettlement => {
                report.required_msat = report
                    .required_msat
                    .saturating_add(u128::from(liability.gross_sat) * 1000);
            }
        }
    }

    report.warning = if !report.federation_ok {
        // The federation is the root: if it's unreachable, gateway/balance checks are moot. Report
        // it distinctly so the operator fixes the right thing (lnrent-urw.4).
        Some(RefundReadinessWarning::FederationDown)
    } else if !report.gateway_ok {
        Some(RefundReadinessWarning::GatewayUnavailable)
    } else if report.expected_msat < report.required_msat {
        // §E: the ledger's expected holdings (a pure local read) — not a live federation query —
        // are the coverage operand. Below the required outlay ⇒ the books can't cover what's owed.
        Some(RefundReadinessWarning::InsufficientBalance)
    } else if report.unpriceable_count > 0 {
        // A pending liability we could not price (e.g. a transient quote failure while the gateway
        // reported ready) is missing from required_msat, so coverage cannot be confirmed — warn
        // rather than report "covered" and silently suppress a real liability (codex P2).
        Some(RefundReadinessWarning::Unpriceable)
    } else if report.parked_count > 0 {
        Some(RefundReadinessWarning::ParkedManual)
    } else {
        None
    };
    Ok(report)
}

async fn pending_refund_required_msat(
    liability: &RefundReadinessLiability,
    refund: &RefundAttemptLiability,
    payment: &Arc<dyn PaymentBackend>,
) -> Result<u128> {
    if let Some(bolt11) = refund.resolved_bolt11.as_deref() {
        let key = gen_key(&liability.external_id, refund.resolution_gen);
        match payment.payment_status_by_key(&key).await? {
            PayStatus::Succeeded | PayStatus::Pending => return Ok(0),
            PayStatus::Unknown if payment.payment_started_by_key(&key).await? => return Ok(0),
            PayStatus::Unknown | PayStatus::Failed => {}
        }
        let pay_sat = parse_whole_sat(bolt11).unwrap_or(liability.gross_sat);
        return payment
            .refund_required_outlay_msat(liability.gross_sat, Some(pay_sat))
            .await;
    }

    if let Some(pay_sat) = refund.dest.as_deref().and_then(|d| parse_whole_sat(d).ok()) {
        let key = gen_key(&liability.external_id, 0);
        match payment.payment_status_by_key(&key).await? {
            PayStatus::Succeeded | PayStatus::Pending => return Ok(0),
            PayStatus::Unknown if payment.payment_started_by_key(&key).await? => return Ok(0),
            PayStatus::Unknown | PayStatus::Failed => {}
        }
        return payment
            .refund_required_outlay_msat(liability.gross_sat, Some(pay_sat))
            .await;
    }

    payment
        .refund_required_outlay_msat(liability.gross_sat, None)
        .await
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
    // (id, external_id, amount_sat, expires_at, kind, sub.paid_through, sub.retention_s,
    //  sub.suspend_not_before)
    type CatchUpRow = (
        String,
        String,
        i64,
        Option<i64>,
        String,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    );
    let open: Vec<CatchUpRow> = store
        .read(|c| {
            let mut stmt = c.prepare(
                "SELECT i.id, i.external_id, COALESCE(i.amount_sat, 0), i.expires_at, i.kind,
                        s.paid_through, s.retention_s, s.suspend_not_before
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
                        r.get::<_, Option<i64>>(7)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await?;

    let mut caught = 0;
    for (
        inv_id,
        external_id,
        amount_sat,
        expires_at,
        kind,
        paid_through,
        retention_s,
        suspend_not_before,
    ) in open
    {
        match payment.lookup_settlement(&inv_id).await {
            Ok((PaymentStatus::Paid, observed)) => {
                let now = clock.now();
                // A LIVE-observed settlement carries its TRUE time (`Some`): use it EXACTLY, so a late
                // live payment is refunded by capture's g5p gate (settled_at >= expires_at) instead of
                // being stamped just-in-window and wrongly provisioned (lnrent-zwk). A RECOVERY
                // settlement (`None`: settled while the daemon was down, true time unknown) keeps the
                // conservative in-window cap below — we must never over-credit or fabricate a refund.
                let settled_at = match observed {
                    Some(real) => real,
                    None => {
                        // Latest in-window settle time the recovered payment can carry: the invoice
                        // expiry (the order gate, capture refunds at settled_at >= expires_at), and for
                        // a renewal ALSO the CREDITED resumable boundary
                        // `B = max(paid_through, suspend_not_before) + retention_s` (the SAME boundary
                        // capture's renewal refund gate honors, §6.5, lnrent-7fp.22). Cap by the binding
                        // one, so a renewal paid in time but recovered late isn't stamped past the
                        // credited boundary and wrongly capped — capping at the RAW
                        // `paid_through + retention_s` would extend `paid_through` from a too-early
                        // `settled_at` for a credited sub.
                        let renewal_boundary = match (kind.as_str(), paid_through, retention_s) {
                            ("renewal", Some(pt), Some(ret)) => {
                                Some(pt.max(suspend_not_before.unwrap_or(pt)) + ret)
                            }
                            _ => None,
                        };
                        recovered_settled_at(now, min_opt(expires_at, renewal_boundary))
                    }
                };
                let settlement = Settlement {
                    invoice_id: inv_id,
                    external_id: external_id.clone(),
                    amount_sat: amount_sat as u64,
                    settled_at,
                };
                match capture(store, settlement, now).await {
                    Ok(Capture::Captured | Capture::Resumed | Capture::RefundDue) => caught += 1,
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

/// When `PaymentBackend::lookup_settlement` reports an invoice paid WITHOUT a live timestamp
/// (`None` — settled while the daemon was down, so the true time is unknown), catch-up cannot trust
/// `now`: after the local invoice expiry, using `now` would fabricate a too-late settlement and
/// refund a payment the backend had already marked paid while reconcile deliberately kept the invoice
/// OPEN for capture. Use the latest in-window timestamp (the binding `cap`: the invoice expiry, and
/// for a renewal also the effective credited retention boundary) instead. A LIVE-observed settlement
/// (`Some`) bypasses this entirely and carries its exact backend timestamp (lnrent-zwk).
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
    use crate::backends::Invoice;
    use crate::store::migrate;
    use nostr_relay_builder::MockRelay;
    use nostr_sdk::Keys;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    // §E: readiness is ledger-derived — the balance operand is `ledger::expected_msat` (seeded via
    // the store), NOT a backend balance call. So this double has NO balance and its
    // `available_balance_msat` PANICS: every readiness test proves the path never reads the wallet.
    #[derive(Default)]
    struct ReadinessPayment {
        gateway_ok: StdMutex<bool>,
        federation_ok: StdMutex<bool>,
        statuses: StdMutex<HashMap<String, PayStatus>>,
        started: StdMutex<HashSet<String>>,
        required_by_gross: StdMutex<HashMap<u64, u128>>,
        fail_pricing: StdMutex<bool>,
    }

    impl ReadinessPayment {
        fn new(gateway_ok: bool) -> Self {
            Self {
                gateway_ok: StdMutex::new(gateway_ok),
                federation_ok: StdMutex::new(true),
                statuses: StdMutex::new(HashMap::new()),
                started: StdMutex::new(HashSet::new()),
                required_by_gross: StdMutex::new(HashMap::new()),
                fail_pricing: StdMutex::new(false),
            }
        }

        fn set_federation_ok(&self, ok: bool) {
            *self.federation_ok.lock().unwrap() = ok;
        }

        fn set_pricing_fails(&self, fails: bool) {
            *self.fail_pricing.lock().unwrap() = fails;
        }

        fn set_status(&self, key: &str, status: PayStatus) {
            self.statuses
                .lock()
                .unwrap()
                .insert(key.to_string(), status);
        }

        fn set_started(&self, key: &str) {
            self.started.lock().unwrap().insert(key.to_string());
        }

        fn set_required(&self, gross_sat: u64, required_msat: u128) {
            self.required_by_gross
                .lock()
                .unwrap()
                .insert(gross_sat, required_msat);
        }
    }

    #[async_trait]
    impl PaymentBackend for ReadinessPayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("readiness tests do not create invoices")
        }

        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!("readiness tests do not look up inbound invoices")
        }

        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            unimplemented!("readiness tests do not look up settlements")
        }

        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            unimplemented!("readiness tests do not pay")
        }

        async fn refund_required_outlay_msat(
            &self,
            gross_sat: u64,
            pay_sat: Option<u64>,
        ) -> Result<u128> {
            if *self.fail_pricing.lock().unwrap() {
                anyhow::bail!("simulated transient refund quote failure");
            }
            Ok(self
                .required_by_gross
                .lock()
                .unwrap()
                .get(&gross_sat)
                .copied()
                .unwrap_or_else(|| u128::from(pay_sat.unwrap_or(gross_sat)) * 1000))
        }

        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!("readiness tests check by key")
        }

        async fn payment_status_by_key(&self, key: &str) -> Result<PayStatus> {
            Ok(*self
                .statuses
                .lock()
                .unwrap()
                .get(key)
                .unwrap_or(&PayStatus::Unknown))
        }

        async fn payment_started_by_key(&self, key: &str) -> Result<bool> {
            Ok(self.started.lock().unwrap().contains(key))
        }

        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            panic!("readiness is ledger-derived (§E) and must never read the federation balance");
        }

        async fn refund_gateway_ready(&self) -> Result<bool> {
            Ok(*self.gateway_ok.lock().unwrap())
        }

        async fn backend_ready(&self) -> Result<bool> {
            Ok(*self.federation_ok.lock().unwrap())
        }

        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("readiness tests do not watch settlements")
        }
    }

    fn mem_store() -> Store {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        Store::spawn(conn)
    }

    fn readiness_payment(gateway_ok: bool) -> Arc<dyn PaymentBackend> {
        Arc::new(ReadinessPayment::new(gateway_ok))
    }

    /// Create the urw.3-owned `sweep_attempt` table locally and insert an in-flight (SENT/PENDING)
    /// cap, so a readiness test can drive `expected_msat` BELOW `required_msat` the way a real sweep
    /// draining reserved funds would — the genuine ledger-derived InsufficientBalance condition.
    async fn seed_sweep_cap(store: &Store, id: &str, status: &str, max_outlay_msat: i64) {
        let (id, status) = (id.to_string(), status.to_string());
        store
            .transaction(move |tx| {
                tx.execute_batch(
                    "CREATE TABLE IF NOT EXISTS sweep_attempt (
                       id TEXT PRIMARY KEY, status TEXT NOT NULL, max_outlay_msat INTEGER NOT NULL
                     );",
                )?;
                tx.execute(
                    "INSERT INTO sweep_attempt (id, status, max_outlay_msat) VALUES (?1, ?2, ?3)",
                    rusqlite::params![id, status, max_outlay_msat],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn readiness(store: &Store, payment: &Arc<dyn PaymentBackend>) -> RefundReadinessReport {
        refund_readiness_report(store, payment).await.unwrap()
    }

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

    struct SlowOutboxOrderHandler {
        store: Store,
        clock: Arc<dyn Clock>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl OrderHandler for SlowOutboxOrderHandler {
        async fn handle(&self, sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            self.started.notify_waiters();
            self.release.notified().await;

            let payload = Msg::ProvisionReady(lnrent_wire::ProvisionReady {
                subscription_id: "sub-drain".into(),
                payload: serde_json::json!({ "credential": "after-drain" }),
            });
            let recipient = sender.to_hex();
            let payload_json = serde_json::to_string(&payload)?;
            let now = self.clock.now();
            self.store
                .transaction(move |tx| {
                    tx.execute(
                        "INSERT INTO outbox
                            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                         VALUES ('outbox-drain', ?1, 'sub-drain', 'provision.ready', ?2, 'PENDING', 0, ?3)",
                        rusqlite::params![recipient, payload_json, now],
                    )?;
                    Ok(())
                })
                .await
        }
    }

    struct TestNoopOpHandler;

    #[async_trait]
    impl OpHandler for TestNoopOpHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            Ok(())
        }
    }

    async fn outbox_state(store: &Store, id: &str) -> Option<String> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT state FROM outbox WHERE id=?1",
                    rusqlite::params![id],
                    |r| r.get(0),
                )
                .optional()?)
            })
            .await
            .unwrap()
    }

    async fn seed_subscription(store: &Store, id: &str, state: &str) {
        let (id, state) = (id.to_string(), state.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, buyer_pubkey, created_at, updated_at)
                     VALUES (?1, ?2, 'buyer', 0, 0)",
                    rusqlite::params![id, state],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_invoice(
        store: &Store,
        sub_id: &str,
        external_id: &str,
        kind: &str,
        amount_sat: i64,
        status: &str,
        settled_at: Option<i64>,
        applied_at: Option<i64>,
    ) {
        let (sub_id, external_id, kind, status) = (
            sub_id.to_string(),
            external_id.to_string(),
            kind.to_string(),
            status.to_string(),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status,
                         settled_at, applied_at, issued_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0)",
                    rusqlite::params![
                        format!("inv-{external_id}"),
                        sub_id,
                        external_id,
                        kind,
                        amount_sat,
                        status,
                        settled_at,
                        applied_at,
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_refund_attempt(
        store: &Store,
        sub_id: &str,
        external_id: &str,
        amount_sat: i64,
        status: &str,
        resolved_bolt11: Option<&str>,
        resolution_gen: i64,
    ) {
        let (sub_id, external_id, status, resolved_bolt11) = (
            sub_id.to_string(),
            external_id.to_string(),
            status.to_string(),
            resolved_bolt11.map(str::to_string),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO refund_attempt
                        (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts,
                         resolved_bolt11, resolved_expiry, resolution_gen, created_at, updated_at)
                     VALUES (?1, ?2, 'lnaddr@buyer', ?3, ?4, ?5, 0, ?6, 100, ?7, 0, 0)",
                    rusqlite::params![
                        format!("ref-{external_id}"),
                        sub_id,
                        amount_sat,
                        format!("refund:{external_id}"),
                        status,
                        resolved_bolt11,
                        resolution_gen,
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_inbound_handler_before_final_outbox_flush() {
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();
        let op_keys = Keys::generate();
        let buyer_keys = Keys::generate();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let engine =
            NostrEngine::connect(op_keys.clone(), std::slice::from_ref(&url), store.clone())
                .await
                .expect("operator engine connects");

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let order_handler: Arc<dyn OrderHandler> = Arc::new(SlowOutboxOrderHandler {
            store: store.clone(),
            clock: clock.clone(),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        });
        let op_handler: Arc<dyn OpHandler> = Arc::new(TestNoopOpHandler);
        let inbound_drain = Arc::new(InboundDrain::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let inbound_task = tokio::spawn(supervise_inbound(
            "nostr-inbound-test",
            shutdown_rx,
            RESTART_BACKOFF,
            engine.clone(),
            Arc::clone(&order_handler),
            Arc::clone(&op_handler),
        ));
        let running = RunningSupervisor {
            shutdown_tx,
            inbound_task: Some(inbound_task),
            tasks: Vec::new(),
            engine: engine.clone(),
            store: store.clone(),
            clock: clock.clone(),
            inbound_drain,
        };

        let order = Msg::OrderRequest(lnrent_wire::OrderRequest {
            id: "shutdown-drain".into(),
            listing_id: format!("30402:{}:dummy", op_keys.public_key().to_hex()),
            params: serde_json::json!({}),
            refund_dest: None,
        });
        let wrap = lnrent_wire::gift_wrap(&buyer_keys, &op_keys.public_key(), &order)
            .await
            .expect("buyer gift-wraps order.request");
        engine
            .queue_inbound_event_for_test(wrap, Arc::clone(&order_handler), Arc::clone(&op_handler))
            .await
            .expect("queue accepted inbound wrap");
        tokio::time::timeout(Duration::from_secs(10), started.notified())
            .await
            .expect("slow handler started before shutdown");

        let shutdown = tokio::spawn(running.shutdown());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown waits for the accepted inbound handler"
        );

        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .expect("shutdown returned")
            .expect("shutdown task joined")
            .expect("shutdown succeeded");

        assert_eq!(
            outbox_state(&store, "outbox-drain").await.as_deref(),
            Some("SENT"),
            "final shutdown outbox flush sends the DM enqueued by the drained handler"
        );
    }

    /// lnrent-urw.6 (PR-9c): the REAL projection over a REAL nostr-sdk pool. Connect to a live
    /// in-process relay and assert `relay_status_snapshot` reflects a connected relay with a
    /// last-connection stamp. (No shutdown: forcing a client-side disconnect via the mock is not
    /// prompt/deterministic under load — the blackout-alert path is covered separately below with
    /// synthetic rows.)
    #[tokio::test]
    async fn relay_status_snapshot_reflects_a_live_pool() {
        let relay = mock_relay().await;
        let url = relay.url().await.to_string();
        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let engine = NostrEngine::connect(Keys::generate(), std::slice::from_ref(&url), store)
            .await
            .expect("operator engine connects");

        // `connect` already waits for the connection, but poll to stay robust under a loaded run.
        let mut snap = engine.relay_status_snapshot().await;
        for _ in 0..250 {
            if snap.iter().any(|r| r.connected) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            snap = engine.relay_status_snapshot().await;
        }
        assert_eq!(snap.len(), 1, "one configured relay projected");
        assert!(snap[0].connected, "the live relay projects as connected");
        assert!(
            snap[0].last_connected_at.is_some(),
            "a live connection stamps last_connected_at"
        );
        assert_eq!(snap[0].url, url);
    }

    /// lnrent-urw.6 (PR-9c) e2e: the blackout-alert + edge-trigger path through the REAL dispatcher
    /// and durable outbox. Drives `refresh_relay_status` with deterministic all-disconnected rows
    /// (see the live-pool projection test above for the real-socket half) and asserts the PERSISTED
    /// `operator.alert` outbox rows — NOT relay delivery (the alert cannot be delivered during the
    /// very blackout it reports).
    #[tokio::test]
    async fn relay_blackout_alert_is_edge_triggered_and_re_alerts_per_onset() {
        async fn blackout_rows(store: &Store) -> i64 {
            store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT count(*) FROM outbox WHERE msg_type='operator.alert'
                           AND payload_json LIKE '%relay_blackout%'",
                        [],
                        |r| r.get(0),
                    )?)
                })
                .await
                .unwrap()
        }
        fn down() -> Vec<RelayStatusRow> {
            vec![RelayStatusRow {
                url: "wss://relay-a".into(),
                connected: false,
                status: "Disconnected".into(),
                last_connected_at: Some(500),
            }]
        }
        fn up() -> Vec<RelayStatusRow> {
            vec![RelayStatusRow {
                url: "wss://relay-a".into(),
                connected: true,
                status: "Connected".into(),
                last_connected_at: Some(500),
            }]
        }

        let store = Store::open_spawn(":memory:").expect("open in-memory store");
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(0));
        let alerts = Arc::new(AlertDispatcher::new(
            store.clone(),
            clock.clone(),
            Keys::generate().public_key().to_hex(),
        ));
        let cell = RelayStatusCell::new();
        let mut monitor = RelayBlackoutMonitor::new();

        // Onset at T0: records the window + publishes the snapshot to the cell, does NOT alert.
        refresh_relay_status(down(), &cell, &mut monitor, &alerts, 10_000).await;
        assert!(
            crate::relay_status::all_disconnected(&cell.get()),
            "the shared cell reflects the disconnected pool for `Request::Relays`"
        );
        assert_eq!(blackout_rows(&store).await, 0, "no alert before the threshold");

        // Crossing the threshold fires exactly one RelayBlackout alert into the durable outbox.
        refresh_relay_status(down(), &cell, &mut monitor, &alerts, 10_000 + RELAY_BLACKOUT_ALERT_S)
            .await;
        assert_eq!(blackout_rows(&store).await, 1, "one persisted alert past the threshold");

        // A later tick in the SAME onset is edge-triggered off — still exactly one.
        refresh_relay_status(
            down(),
            &cell,
            &mut monitor,
            &alerts,
            10_000 + RELAY_BLACKOUT_ALERT_S + 300,
        )
        .await;
        assert_eq!(blackout_rows(&store).await, 1, "edge-triggered — one per onset, not per tick");

        // Reconnect re-arms the monitor.
        refresh_relay_status(up(), &cell, &mut monitor, &alerts, 20_000).await;

        // A SECOND, distinct blackout onset re-alerts — even within the dispatcher's 6h cooldown —
        // because the alert is keyed on the onset (codex). Onset at 20_000, fires at +threshold.
        refresh_relay_status(down(), &cell, &mut monitor, &alerts, 20_000).await;
        refresh_relay_status(down(), &cell, &mut monitor, &alerts, 20_000 + RELAY_BLACKOUT_ALERT_S)
            .await;
        assert_eq!(
            blackout_rows(&store).await,
            2,
            "the next onset re-alerts (distinct onset subject dodges the per-subject cooldown)"
        );
    }

    #[tokio::test]
    async fn readiness_no_warning_at_zero_expected_with_zero_liability() {
        let store = mem_store();
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.liability_count, 0);
        assert_eq!(report.expected_msat, 0);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_warns_when_required_exceeds_expected() {
        // The books hold a 2-sat receipt (expected 2_000), but a SENT sweep locked 1_000 of it out
        // — the genuine ledger-derived InsufficientBalance: expected (1_000) < required (2_000).
        let store = mem_store();
        seed_subscription(&store, "sub-1", "PROVISIONING").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_sweep_cap(&store, "sw-1", "SENT", 1_000).await;
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.required_msat, 2_000);
        assert_eq!(report.expected_msat, 1_000);
        assert_eq!(
            report.warning,
            Some(RefundReadinessWarning::InsufficientBalance)
        );
    }

    #[tokio::test]
    async fn readiness_warns_gateway_down_with_liabilities() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "PROVISIONING").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            1,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        let payment = readiness_payment(false);

        let report = readiness(&store, &payment).await;

        assert_eq!(
            report.warning,
            Some(RefundReadinessWarning::GatewayUnavailable)
        );
    }

    // urw.4: a down FEDERATION is reported DISTINCTLY from a down gateway, and takes priority (it's
    // the root — if guardians are unreachable, gateway/balance checks are moot).
    #[tokio::test]
    async fn readiness_warns_federation_down_distinct_from_gateway() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "PROVISIONING").await;
        seed_invoice(&store, "sub-1", "order:sub-1", "order", 1, "PAID", Some(10), Some(10)).await;

        let with_federation_down = |gateway_ok: bool| -> Arc<dyn PaymentBackend> {
            let p = ReadinessPayment::new(gateway_ok);
            p.set_federation_ok(false);
            Arc::new(p)
        };

        // Federation down, gateway UP → FederationDown (not GatewayUnavailable).
        let report = readiness(&store, &with_federation_down(true)).await;
        assert_eq!(
            report.warning,
            Some(RefundReadinessWarning::FederationDown),
            "a down federation with an OK gateway is FederationDown, not GatewayUnavailable"
        );

        // Federation down AND gateway down → still FederationDown (the root wins).
        let report2 = readiness(&store, &with_federation_down(false)).await;
        assert_eq!(
            report2.warning,
            Some(RefundReadinessWarning::FederationDown),
            "federation-down takes priority over gateway-down"
        );
    }

    // urw.4 (codex): a down federation must surface even with ZERO refund liabilities — otherwise an
    // idle operator sees READY and announces a listing while guardians are unreachable. A HEALTHY
    // federation with zero liabilities stays ready (refund-coverage warnings rightly stay silent).
    #[tokio::test]
    async fn readiness_surfaces_federation_down_even_with_zero_liabilities() {
        let store = mem_store(); // no liabilities seeded

        let p = ReadinessPayment::new(true);
        p.set_federation_ok(false);
        let down: Arc<dyn PaymentBackend> = Arc::new(p);
        let report = readiness(&store, &down).await;
        assert_eq!(
            report.warning,
            Some(RefundReadinessWarning::FederationDown),
            "federation-down surfaces with no liabilities"
        );
        assert!(!report.federation_ok);

        let ok_report = readiness(&store, &readiness_payment(true)).await;
        assert_eq!(ok_report.warning, None, "a healthy idle daemon is READY");
        assert!(ok_report.federation_ok);
    }

    #[tokio::test]
    async fn readiness_ignores_old_paid_renewals_on_later_inactive_subscriptions() {
        let store = mem_store();
        for (sub_id, state) in [("sub-s", "SUSPENDED"), ("sub-t", "TERMINATED")] {
            seed_subscription(&store, sub_id, state).await;
            seed_invoice(
                &store,
                sub_id,
                &format!("renew:auto:{sub_id}:1000"),
                "renewal",
                1,
                "PAID",
                Some(900),
                Some(900),
            )
            .await;
        }
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.liability_count, 0);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_reports_parked_failed_refunds_as_manual_liabilities() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(&store, "sub-1", "order:sub-1", 2, "FAILED", None, 0).await;
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.gross_liability_sat, 2);
        assert_eq!(report.required_msat, 0);
        assert_eq!(report.parked_count, 1);
        assert_eq!(report.warning, Some(RefundReadinessWarning::ParkedManual));
    }

    #[tokio::test]
    async fn readiness_covers_when_expected_equals_required_exactly() {
        // The compare is `expected < required`, in msats: expected == required is COVERED (not
        // InsufficientBalance), proving the boundary is `<` not `<=`. A 2-sat receipt with a 500-msat
        // SENT sweep leaves expected 1_500, exactly the 1_500-msat required outlay.
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(&store, "sub-1", "order:sub-1", 2, "PENDING", None, 0).await;
        seed_sweep_cap(&store, "sw-1", "SENT", 500).await;
        let payment = Arc::new(ReadinessPayment::new(true));
        payment.set_required(2, 1_500);
        let payment: Arc<dyn PaymentBackend> = payment;

        let report = readiness(&store, &payment).await;

        assert_eq!(report.required_msat, 1_500);
        assert_eq!(report.expected_msat, 1_500);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_in_flight_pending_generation_does_not_inflate_required() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(
            &store,
            "sub-1",
            "order:sub-1",
            2,
            "PENDING",
            Some("persisted-bolt11"),
            1,
        )
        .await;
        let payment = Arc::new(ReadinessPayment::new(true));
        payment.set_status("refund:order:sub-1:g1", PayStatus::Pending);
        payment.set_required(2, 2_000);
        let payment: Arc<dyn PaymentBackend> = payment;

        let report = readiness(&store, &payment).await;

        assert_eq!(report.gross_liability_sat, 2);
        assert_eq!(report.required_msat, 0);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_unpriceable_pending_liability_warns_not_covered() {
        // Gateway reports ready and the books are ample, but pricing this PENDING refund fails
        // transiently. Its cost is then absent from required_msat, so coverage cannot be confirmed —
        // the report MUST warn (Unpriceable) rather than fall through to "covered" and silently
        // suppress a real liability (codex P2).
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(
            &store,
            "sub-1",
            "order:sub-1",
            2,
            "PENDING",
            Some("persisted-bolt11"),
            1,
        )
        .await;
        let payment = Arc::new(ReadinessPayment::new(true));
        // The prior pay op returned funds (Failed) so a NEW pay must be priced — and pricing fails.
        payment.set_status("refund:order:sub-1:g1", PayStatus::Failed);
        payment.set_pricing_fails(true);
        let payment: Arc<dyn PaymentBackend> = payment;

        let report = readiness(&store, &payment).await;

        assert_eq!(report.unpriceable_count, 1);
        assert_eq!(report.required_msat, 0);
        assert_eq!(report.warning, Some(RefundReadinessWarning::Unpriceable));
    }

    #[tokio::test]
    async fn readiness_unknown_without_started_attempt_still_requires_liquidity() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(
            &store,
            "sub-1",
            "order:sub-1",
            2,
            "PENDING",
            Some("persisted-bolt11"),
            1,
        )
        .await;
        // An Unknown pay with NO started evidence still requires FULL liquidity (required 2_000), and
        // is NOT subtracted from expected. A 1-msat sweep drops expected below required → the books
        // can't cover it (InsufficientBalance), distinguishing this from the started shape below.
        seed_sweep_cap(&store, "sw-1", "PENDING", 1).await;
        let payment = Arc::new(ReadinessPayment::new(true));
        payment.set_status("refund:order:sub-1:g1", PayStatus::Unknown);
        payment.set_required(2, 2_000);
        let payment: Arc<dyn PaymentBackend> = payment;

        let report = readiness(&store, &payment).await;

        assert_eq!(report.required_msat, 2_000);
        assert_eq!(report.expected_msat, 1_999);
        assert_eq!(
            report.warning,
            Some(RefundReadinessWarning::InsufficientBalance)
        );
    }

    #[tokio::test]
    async fn readiness_in_flight_unknown_started_key_does_not_inflate_required() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "REFUND_DUE").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "PAID",
            Some(10),
            Some(10),
        )
        .await;
        seed_refund_attempt(
            &store,
            "sub-1",
            "order:sub-1",
            2,
            "PENDING",
            Some("persisted-bolt11"),
            1,
        )
        .await;
        let payment = Arc::new(ReadinessPayment::new(true));
        payment.set_status("refund:order:sub-1:g1", PayStatus::Unknown);
        payment.set_started("refund:order:sub-1:g1");
        payment.set_required(2, 2_000);
        let payment: Arc<dyn PaymentBackend> = payment;

        let report = readiness(&store, &payment).await;

        assert_eq!(report.gross_liability_sat, 2);
        assert_eq!(report.required_msat, 0);
        // The started pay is subtracted from expected too (its funds are locked), so expected and
        // required both net to 0 — consistently covered, no false InsufficientBalance.
        assert_eq!(report.expected_msat, 0);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_dedups_paid_pending_order_against_unreconciled_settlement() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "PENDING").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            1,
            "PAID",
            Some(10),
            None,
        )
        .await;
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.liability_count, 1);
        assert_eq!(report.gross_liability_sat, 1);
        assert_eq!(report.required_msat, 1_000);
        assert_eq!(report.warning, None);
    }

    #[tokio::test]
    async fn readiness_sent_refund_blocks_terminal_invoice_residual_bucket() {
        let store = mem_store();
        seed_subscription(&store, "sub-1", "EXPIRED").await;
        seed_invoice(
            &store,
            "sub-1",
            "order:sub-1",
            "order",
            2,
            "EXPIRED",
            Some(10),
            None,
        )
        .await;
        seed_refund_attempt(&store, "sub-1", "order:sub-1", 2, "SENT", None, 0).await;
        let payment = readiness_payment(true);

        let report = readiness(&store, &payment).await;

        assert_eq!(report.liability_count, 0);
        assert_eq!(report.warning, None);
    }

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

    // FIX 1 (§6.5, lnrent-7fp.22): settlement catch-up honors the CREDITED resumable boundary
    // B = max(paid_through, suspend_not_before) + retention_s. A missed renewal settlement recovered
    // when `paid_through + retention_s <= now < B` must be capped at B (not the raw
    // paid_through + retention_s), so capture extends paid_through from the correct in-window
    // settled_at instead of a too-early one — consistent with capture's own renewal refund gate.
    #[tokio::test]
    async fn settlement_catch_up_caps_credited_renewal_at_effective_boundary() {
        use crate::backends::MockPayment;
        use crate::clock::TestClock;
        use crate::store::migrate;
        use rusqlite::{params, Connection};

        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let store = Store::spawn(conn);

        // paid_through=1000, retention=500 -> RAW boundary 1500. Credited floor 1300 -> effective
        // boundary B = max(1000,1300)+500 = 1800. Recovery at now=1600 sits in [1500, 1800): the RAW
        // cap would stamp settled_at=1499 (under-extending paid_through to 1599); the CREDITED cap
        // stamps settled_at=now=1600 (extending paid_through to 1700).
        let mock = Arc::new(MockPayment::new());
        mock.set_now(0); // invoice expires_at = 0 + 8000 = 8000 (well past B, so not the binding cap)
        let inv = mock
            .create_invoice(1000, "lnrent renewal s1", 8000, "renew:auto:s1:1000")
            .await
            .unwrap();
        let inv_id = inv.id.clone();
        let expires_at = inv.expires_at;
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, state, period_s, renew_lead_s, retention_s, paid_through,
                         suspend_not_before, next_deadline, created_at, updated_at)
                     VALUES ('s1','ACTIVE',100,10,500,1000,1300,1000,0,0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, expires_at, issued_at)
                     VALUES (?1,'s1','renew:auto:s1:1000','renewal',1000,'OPEN',?2,0)",
                    params![inv_id, expires_at],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        mock.settle_recovered("renew:auto:s1:1000").unwrap(); // RECOVERY: settled-while-down, no live ts

        let payment: Arc<dyn PaymentBackend> = mock.clone();
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1600));
        settlement_catch_up(&store, &payment, &clock).await.unwrap();

        let (state, pt, refunds): (String, i64, i64) = store
            .read(|c| {
                let row = c.query_row(
                    "SELECT state, paid_through FROM subscription WHERE id='s1'",
                    [],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                )?;
                let refunds: i64 =
                    c.query_row("SELECT count(*) FROM refund_attempt", [], |r| r.get(0))?;
                Ok((row.0, row.1, refunds))
            })
            .await
            .unwrap();
        assert_eq!(
            state, "ACTIVE",
            "a credited renewal recovered in-window renews"
        );
        assert_eq!(
            pt, 1700,
            "paid_through extends from settled_at=now(1600) capped at the credited boundary B, not \
             the raw paid_through+retention_s (which would cap at 1499 -> paid_through 1599)"
        );
        assert_eq!(refunds, 0, "no refund for an in-window credited renewal");
    }

    // Boot-order regression for the same invariant as the catch-up cap above: the restart credit must
    // be installed BEFORE settlement catch-up scans missed payments. Otherwise a renewal paid during
    // the outage is recovered with suspend_not_before still NULL, capped at raw paid_through+retention,
    // and paid_through extends from a too-early recovered settled_at.
    #[tokio::test]
    async fn downtime_credit_precedes_settlement_catch_up_for_same_outage_renewal() {
        use crate::backends::MockPayment;
        use crate::clock::TestClock;
        use crate::store::migrate;
        use rusqlite::{params, Connection};

        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let store = Store::spawn(conn);

        // paid_through=1000, retention=500 -> raw boundary 1500. The outage [950,1600]
        // credits a floor at 1650, so the effective boundary B becomes 2150. Catch-up at now=1600
        // must therefore recover the settlement at 1600, not raw-boundary-capped 1499.
        let mock = Arc::new(MockPayment::new());
        mock.set_now(0);
        let inv = mock
            .create_invoice(1000, "lnrent renewal s1", 8000, "renew:auto:s1:1000")
            .await
            .unwrap();
        let inv_id = inv.id.clone();
        let expires_at = inv.expires_at;
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO daemon_state (rowid, last_heartbeat) VALUES (1, 950)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO subscription
                        (id, state, period_s, renew_lead_s, retention_s, paid_through,
                         soft_date, next_deadline, created_at, updated_at)
                     VALUES ('s1','ACTIVE',100,100,500,1000,900,1000,0,0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, expires_at, issued_at)
                     VALUES (?1,'s1','renew:auto:s1:1000','renewal',1000,'OPEN',?2,0)",
                    params![inv_id, expires_at],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        mock.settle_recovered("renew:auto:s1:1000").unwrap(); // RECOVERY: settled-while-down, no live ts

        let payment: Arc<dyn PaymentBackend> = mock.clone();
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1600));
        let recipe = Recipe::load(format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR")))
            .expect("dummy recipe");
        let reconciler = Reconciler::new(store.clone(), payment.clone(), recipe);

        assert_eq!(
            reconciler
                .apply_restart_downtime_credit(clock.now())
                .await
                .unwrap(),
            1,
            "restart installs the credited floor before missed settlements are recovered"
        );
        settlement_catch_up(&store, &payment, &clock).await.unwrap();

        let (state, pt, snb, settled_at, refunds): (String, i64, Option<i64>, i64, i64) = store
            .read(|c| {
                let sub = c.query_row(
                    "SELECT state, paid_through, suspend_not_before FROM subscription WHERE id='s1'",
                    [],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                        ))
                    },
                )?;
                let settled_at =
                    c.query_row("SELECT settled_at FROM invoice WHERE external_id='renew:auto:s1:1000'", [], |r| {
                        r.get::<_, i64>(0)
                    })?;
                let refunds: i64 =
                    c.query_row("SELECT count(*) FROM refund_attempt", [], |r| r.get(0))?;
                Ok((sub.0, sub.1, sub.2, settled_at, refunds))
            })
            .await
            .unwrap();
        assert_eq!(state, "ACTIVE");
        assert_eq!(
            settled_at, 1600,
            "recovered settlement is capped against the credited boundary, not raw paid_through+retention"
        );
        assert_eq!(
            pt, 1700,
            "paid_through extends from the credited in-window recovered settlement"
        );
        assert_eq!(
            snb, None,
            "the renewal consumed the just-installed downtime-credit floor"
        );
        assert_eq!(refunds, 0);
    }

    // lnrent-zwk regression: settlement catch-up must honor a LIVE settled_at. A late LIVE-paid order
    // (true settled_at >= invoice expiry) must be REFUNDED by catch-up via capture's g5p gate — NOT
    // provisioned via a fabricated capped timestamp. A RECOVERY-paid order (settled while down, no
    // live ts) still uses the conservative in-window cap and PROVISIONS. Same store, same clock, same
    // expiry — only the settlement PROVENANCE differs.
    #[tokio::test]
    async fn settlement_catch_up_refunds_late_live_order_but_caps_recovery() {
        use crate::backends::MockPayment;
        use crate::clock::TestClock;
        use crate::store::migrate;
        use rusqlite::{params, Connection};

        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let store = Store::spawn(conn);

        let mock = Arc::new(MockPayment::new());
        mock.set_now(0); // both order invoices expire at 0 + 1000 = 1000
        let live = mock
            .create_invoice(1000, "lnrent order live", 1000, "order:live")
            .await
            .unwrap();
        let rec = mock
            .create_invoice(1000, "lnrent order rec", 1000, "order:rec")
            .await
            .unwrap();
        let (live_inv, live_exp) = (live.id.clone(), live.expires_at);
        let (rec_inv, rec_exp) = (rec.id.clone(), rec.expires_at);
        assert_eq!(live_exp, 1000);
        assert_eq!(rec_exp, 1000);

        store
            .transaction(move |tx| {
                for (sub, inv_id, ext, exp) in [
                    ("s_live", live_inv.as_str(), "order:live", live_exp),
                    ("s_rec", rec_inv.as_str(), "order:rec", rec_exp),
                ] {
                    tx.execute(
                        "INSERT INTO subscription
                            (id, state, period_s, renew_lead_s, retention_s, next_deadline, created_at, updated_at)
                         VALUES (?1,'PENDING',100,10,500,1000,0,0)",
                        params![sub],
                    )?;
                    tx.execute(
                        "INSERT INTO invoice
                            (id, subscription_id, external_id, kind, amount_sat, status, expires_at, issued_at)
                         VALUES (?1,?2,?3,'order',1000,'OPEN',?4,0)",
                        params![inv_id, sub, ext, exp],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();

        // s_live: a LIVE settlement records the real ts (here AT the expiry boundary -> the g5p gate).
        mock.settle("order:live", 1000).unwrap();
        // s_rec: a RECOVERY settlement — paid at the backend, but the true time is unknown.
        mock.settle_recovered("order:rec").unwrap();

        // Catch-up runs well after the expiry. The recovered order is stamped expires_at-1 (=999) by the
        // conservative cap and provisions; the live order must use its real ts (1000) and refund.
        let payment: Arc<dyn PaymentBackend> = mock.clone();
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1500));
        settlement_catch_up(&store, &payment, &clock).await.unwrap();

        #[allow(clippy::type_complexity)]
        let (live_state, live_refunds, live_settled, rec_state, rec_refunds, rec_settled): (
            String,
            i64,
            i64,
            String,
            i64,
            i64,
        ) = store
            .read(|c| {
                let live_state = c.query_row(
                    "SELECT state FROM subscription WHERE id='s_live'",
                    [],
                    |r| r.get(0),
                )?;
                let live_refunds: i64 = c.query_row(
                    "SELECT count(*) FROM refund_attempt WHERE subscription_id='s_live'",
                    [],
                    |r| r.get(0),
                )?;
                let live_settled: i64 = c.query_row(
                    "SELECT settled_at FROM invoice WHERE external_id='order:live'",
                    [],
                    |r| r.get(0),
                )?;
                let rec_state = c.query_row(
                    "SELECT state FROM subscription WHERE id='s_rec'",
                    [],
                    |r| r.get(0),
                )?;
                let rec_refunds: i64 = c.query_row(
                    "SELECT count(*) FROM refund_attempt WHERE subscription_id='s_rec'",
                    [],
                    |r| r.get(0),
                )?;
                let rec_settled: i64 = c.query_row(
                    "SELECT settled_at FROM invoice WHERE external_id='order:rec'",
                    [],
                    |r| r.get(0),
                )?;
                Ok((
                    live_state,
                    live_refunds,
                    live_settled,
                    rec_state,
                    rec_refunds,
                    rec_settled,
                ))
            })
            .await
            .unwrap();

        // LIVE late payment: stamped with the REAL ts (1000), refunded via g5p, NEVER provisioned.
        assert_eq!(
            live_settled, 1000,
            "the late LIVE order keeps its real settled_at exactly, not a capped one"
        );
        assert_eq!(
            live_state, "PENDING",
            "a late LIVE order is refunded (state untouched), not moved toward provisioning"
        );
        assert_eq!(
            live_refunds, 1,
            "the late LIVE order has exactly one refund intent"
        );

        // RECOVERY payment: conservative in-window cap (999) -> provisioned, no refund.
        assert_eq!(
            rec_settled, 999,
            "a recovered order is capped to expires_at-1 (true time unknown)"
        );
        assert_eq!(
            rec_state, "PROVISIONING",
            "a recovered order keeps the conservative cap and provisions"
        );
        assert_eq!(rec_refunds, 0, "no refund for an in-window recovered order");
    }
}
