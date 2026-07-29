//! The operator's PUBLICATION GATE (lnrent-i23): who decides that this daemon is publicly listed,
//! and when.
//!
//! Before this bead the answer was "the process", and it was not really a decision at all: the
//! durable `listing` row was born `ACTIVE` on the first boot and the NIP-99 `30402` went out
//! unconditionally, so a fresh install was taking orders from strangers before its operator had
//! looked at a single log line. There was also no way back — `WITHDRAWN` existed in the schema and
//! in the boot guard, but no code path ever wrote it.
//!
//! The model here instead:
//!
//! - **Quiet by default.** [`upsert_row`] (the boot path) creates the row [`UNPUBLISHED`] and NEVER
//!   writes [`ACTIVE`]. Order intake already refuses anything that is not `ACTIVE`
//!   (`order_intake.rs`), so a fresh daemon takes no orders.
//! - **Publication is an explicit operator act** — [`publish`], reached over IPC as
//!   `lnrent listing publish`.
//! - **One-time, not per-boot.** The durable row is the authority: once `ACTIVE`, every later boot
//!   republishes the event from it with no operator action ([`republish_on_boot`]), and the boot
//!   upsert leaves `state` alone so a `WITHDRAWN` listing is never resurrected.
//! - **[`withdraw`] is the symmetric counterpart** and is the only thing that writes `WITHDRAWN`.
//!
//! ## Why publish is gated, and why the gate has two tiers
//!
//! [`publish`] runs the SAME [`crate::preflight`] checks `lnrent preflight` runs, and splits their
//! failures on the [`FailureClass`] each check mints:
//!
//! - [`FailureClass::Structural`] — the operator's own misconfiguration (invalid provisioning
//!   params, a rejected credential, a federation without the money module). It does not fix itself,
//!   so publish HARD BLOCKS and names it. There is no override.
//! - [`FailureClass::Reachability`] — someone else is down right now. Publish WARNS and refuses,
//!   but `--accept-unverified` overrides it, because a third party's outage must not be able to
//!   block an operator's launch: down at publish time is not down at order time.
//!
//! What the override actually costs is NOT uniform across those dependencies, and the honest
//! statement of it is per-dependency rather than a blanket "the money path fails closed":
//!
//! - **Payment backend down** — genuinely fails closed. `order_intake` cannot mint an invoice, so
//!   the order is refused before any money moves. Overriding this one risks nothing but a Buyer
//!   seeing a failed order.
//! - **Provider (compute) down** — does NOT fail closed. `order_intake` runs a price check, a
//!   capacity reservation and `create_invoice`, and consults the PROVIDER nowhere; provisioning
//!   happens after settlement. So a Buyer who orders while the provider is still broken PAYS, fails
//!   to provision, and is refunded net of fees (ADR-0019) — a real cost to them, on the ordinary
//!   failed-provision path rather than a new hazard, but a cost the operator is choosing on their
//!   behalf when they override. That is why the refusal names the check: overriding a provider
//!   reachability failure is a different decision from overriding a payment-backend one.
//!
//! Gating invoice issuance on a live provider probe would close that, and is deliberately NOT done:
//! it puts third-party network I/O on the money path (ADR-0016 keeps probes off it — the ledger
//! authorizes, and only explicit `reconcile` reads the wallet) and would make every order depend on
//! the provider's uptime rather than only the ones that reach provisioning.
//!
//! ## Why nothing here ever auto-withdraws
//!
//! There is deliberately NO liveness check, probe result, or backend outage that retracts a
//! published listing — only the operator does, through [`withdraw`]. **The listing is not the safety
//! mechanism; the money path is.** Order intake already refuses when it cannot mint, so a listing
//! left up during an outage costs a buyer one clean rejection. Auto-retraction would instead make
//! marketplace presence flap on every hiccup, and — because relays hold the replaceable `30402`
//! whether or not we asked them to drop it — different buyers would see different answers. That is
//! strictly worse than staying listed and failing closed at order time.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::OptionalExtension;

use crate::backends::PaymentBackend;
use crate::clock::Clock;
use crate::preflight::{FailureClass, PreflightCheck};
use crate::recipe::Recipe;
use crate::store::Store;

/// A row that exists but has never been published: born here, and the state a fresh daemon sits in
/// until its operator runs `lnrent listing publish`. Order intake treats it exactly like
/// `WITHDRAWN` — anything that is not [`ACTIVE`] is refused.
pub const UNPUBLISHED: &str = "UNPUBLISHED";
/// Published: the operator asked for it, and every later boot republishes the `30402` from this row.
pub const ACTIVE: &str = "ACTIVE";
/// Withdrawn by the operator. The boot path must never bring this back to [`ACTIVE`]; only an
/// explicit [`publish`] can.
pub const WITHDRAWN: &str = "WITHDRAWN";

/// How long [`withdraw`] waits on the best-effort relay retraction before giving up on it. The
/// durable withdrawal is already committed by then, so this bound only keeps the operator's command
/// from hanging on an unreachable relay — it can never reverse or block the withdrawal itself.
const RETRACT_TIMEOUT: Duration = Duration::from_secs(10);

/// The same bound for the outbound 30402 broadcast. Like the retraction it cannot change what the
/// durable row says; it exists so a wedged relay cannot pin [`TRANSITION`] (and with it the
/// operator's ability to WITHDRAW) for however long the Nostr client would otherwise wait.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

/// Serializes every listing TRANSITION — the durable state read, the state write, the relay side
/// effect, and the outcome the operator is told — into one order.
///
/// Without it, a `listing publish` and a `listing withdraw` arriving on two IPC connections
/// interleave: publish commits `ACTIVE` and stalls in its broadcast while withdraw commits
/// `WITHDRAWN` and retracts, after which publish's 30402 lands ON TOP of the deletion and publish
/// still answers `published: true` — a reply that contradicts the durable row, which is precisely
/// the "the operator believes they are live and is not (or the reverse)" failure this bead exists to
/// remove. Both relay legs are bounded ([`PUBLISH_TIMEOUT`], [`RETRACT_TIMEOUT`]), so the queued verb
/// waits a bounded time and then reads the state the winner actually left.
///
/// Process-wide (the daemon owns one store) and held only across these operator verbs plus the boot
/// republish — deliberately NOT across preflight's probes. Those are 10-15s of third-party network
/// I/O, and `withdraw` is the operator's emergency stop: queueing a withdrawal behind a publish's
/// probes would be the worse bug. The window that opens instead — a withdrawal arriving while a
/// publish is still probing — is closed by the CAS in [`publish_gated`], not by widening this lock.
static TRANSITION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The relay side of publication, injected so the gate is testable without a relay and so IPC does
/// not depend on the whole [`crate::nostr_engine::NostrEngine`]. Implemented over the engine in
/// `nostr_engine.rs`.
#[async_trait]
pub trait ListingRelay: Send + Sync {
    /// The operator pubkey (hex) that authors the listing — half of its `30402:<pubkey>:<d>`
    /// coordinate.
    fn operator_pubkey_hex(&self) -> String;
    /// Publish (or replace) the 30402 for `recipe` at `d`; returns the published event id in hex.
    async fn publish_listing_for(
        &self,
        recipe: &Recipe,
        d: &str,
        operator_hex: &str,
    ) -> Result<String>;
    /// Ask the relays to drop the 30402 at `d`. Best-effort by contract — see
    /// [`crate::nostr_engine::NostrEngine::retract_listing`].
    async fn retract_listing_at(&self, d: &str) -> Result<()>;
}

/// The relay seam as the IPC surface holds it: `None` when no Nostr engine is wired (the standalone
/// [`crate::ipc::serve`] some unit tests use). Publish/withdraw are REFUSED under `None` rather than
/// silently writing a durable state whose event could never reach a relay.
pub type RelayHandle = Option<Arc<dyn ListingRelay>>;

/// What [`publish`] did, or why it would not.
#[derive(Debug)]
pub enum PublishOutcome {
    /// The row is now `ACTIVE`. `event_id` is `None` when the durable state committed but the relay
    /// publish failed — the next boot republishes it (`warnings` carries the overridden checks).
    Published {
        listing_id: String,
        event_id: Option<String>,
        relay_error: Option<String>,
        warnings: Vec<PreflightCheck>,
    },
    /// Already `ACTIVE`; publish is idempotent and re-broadcast the event.
    AlreadyPublished {
        listing_id: String,
        event_id: Option<String>,
        relay_error: Option<String>,
        warnings: Vec<PreflightCheck>,
    },
    /// Refused: the operator's own misconfiguration. No override.
    ///
    /// `still_published` is the durable state at the moment of the refusal, and it is NOT
    /// redundant: an operator whose listing has been `ACTIVE` for a week and who re-runs publish
    /// after a dependency broke gets this refusal while the row is still `ACTIVE` and order intake
    /// is still accepting orders. Refusing without saying so would report "not published" for a
    /// live listing — the exact confusion the gate exists to remove. A refusal NEVER withdraws.
    BlockedStructural {
        failed: Vec<PreflightCheck>,
        still_published: bool,
    },
    /// Refused: a dependency is unreachable. `--accept-unverified` overrides this.
    /// `still_published` carries the same meaning as on [`PublishOutcome::BlockedStructural`].
    BlockedUnverified {
        failed: Vec<PreflightCheck>,
        still_published: bool,
    },
    /// Abandoned: the world this publish was decided against CHANGED while preflight was running —
    /// in practice an operator's `listing withdraw` landing while this publish was still probing its
    /// dependencies. Nothing was written and nothing was broadcast; `state` is what the row says now
    /// (which is NOT always different from what the publish saw — a withdrawal that wrote nothing
    /// durable supersedes it too, see [`PublishSnapshot`]).
    Superseded { listing_id: String, state: String },
}

/// The world a [`publish`] was decided against, captured BEFORE its (unlocked) preflight probes and
/// re-checked under [`TRANSITION`] before anything is committed — [`publish_gated`]'s
/// compare-and-swap.
///
/// Two fields, because the durable state alone is not enough. `withdraw` writes nothing when the row
/// is already `WITHDRAWN` (it only re-asks the relays) or was never published, so an operator who
/// relists and then changes their mind mid-preflight would see their `listing withdraw` answer
/// truthfully — and the publish they can no longer see would still bring the listing up behind them.
/// The epoch counts withdrawal ATTEMPTS, so the operator's later command wins in every branch.
#[derive(Debug)]
pub(crate) struct PublishSnapshot {
    state: String,
    withdraw_epoch: u64,
}

impl PublishSnapshot {
    /// Snapshot the state and epoch a publish is about to be decided against.
    ///
    /// Taken under [`TRANSITION`] so the pair is ATOMIC with respect to `withdraw`. Reading them
    /// unlocked is not enough in either direction, and the second one is not obvious: a withdrawal
    /// that lands entirely BEFORE the epoch read writes nothing durable when the row is
    /// `UNPUBLISHED` or already `WITHDRAWN`, so the snapshot would absorb that withdrawal into its
    /// own baseline — new epoch, unchanged state — and the later compare-and-swap would see nothing
    /// moved and let an already-started publish bring the listing up after the operator had been
    /// told it was not live. The lock makes the snapshot a single ordering point, so a withdrawal is
    /// unambiguously either before this publish (and reflected in what it decided against) or after
    /// it (and caught by the CAS). Held only for the two reads; the seconds of preflight probes that
    /// follow still run UNLOCKED.
    async fn take(store: &Store, listing_id: &str) -> Result<Self> {
        let _one_at_a_time = TRANSITION.lock().await;
        let withdraw_epoch = store.listing_withdraw_epoch();
        let state = state(store, listing_id).await?.unwrap_or_default();
        Ok(Self {
            state,
            withdraw_epoch,
        })
    }
}

/// What [`withdraw`] did.
#[derive(Debug)]
pub enum WithdrawOutcome {
    /// The row is now `WITHDRAWN`. `retract_error` is the relay diagnostic when the best-effort
    /// retraction failed — the withdrawal still stands.
    Withdrawn {
        listing_id: String,
        retract_error: Option<String>,
    },
    /// Already `WITHDRAWN` — the durable state is untouched, but the best-effort relay retraction
    /// is RE-ATTEMPTED (see [`withdraw`]), so `retract_error` carries this attempt's diagnostic.
    AlreadyWithdrawn {
        listing_id: String,
        retract_error: Option<String>,
    },
    /// Never published *by this database*, so there is nothing durable to withdraw — but the relays
    /// were still asked to drop the coordinate, and `retract_error` carries that attempt's
    /// diagnostic. See [`withdraw`] for why a row that was never published still retracts.
    NotPublished {
        listing_id: String,
        retract_error: Option<String>,
    },
}

/// The NIP-99 replaceable-event `d` for `recipe`. M1a serves one listing per recipe, so `d` is the
/// recipe id — stable across republishes, which keeps the coordinate stable across price edits.
pub fn d_tag(recipe: &Recipe) -> String {
    recipe.service.id.clone()
}

/// The addressable coordinate `30402:<operator>:<d>` — the id of the durable row AND what a buyer's
/// `order.request.listing_id` references.
pub fn coordinate(recipe: &Recipe, operator_hex: &str) -> String {
    lnrent_wire::listing_coordinate(operator_hex, &d_tag(recipe))
}

/// Upsert the durable row for `recipe` at boot, refreshing the price/timer columns from the recipe.
///
/// A FRESH row is born [`UNPUBLISHED`] (lnrent-i23) — the daemon starts QUIET, and only
/// [`publish`] ever writes [`ACTIVE`]. On CONFLICT the price/timer columns are refreshed and
/// `state` is DELIBERATELY left untouched, so re-running this on every boot neither publishes a
/// listing the operator never published NOR resurrects one they deliberately withdrew.
pub async fn upsert_row(
    store: &Store,
    clock: &Arc<dyn Clock>,
    recipe: &Recipe,
    listing_id: &str,
) -> Result<()> {
    let now = clock.now();
    let amount_sat = recipe.pricing.amount_sat as i64;
    let period_s = crate::supervisor::duration_secs(&recipe.pricing.period);
    let renew_lead_s = crate::supervisor::duration_secs(&recipe.pricing.renew_lead);
    let retention_s = crate::supervisor::duration_secs(&recipe.pricing.retention);
    let (id, recipe_id, d) = (
        listing_id.to_string(),
        recipe.service.id.clone(),
        d_tag(recipe),
    );
    store
        .transaction(move |tx| {
            tx.execute(
                "INSERT INTO listing
                    (id, recipe_id, d_tag, amount_sat, period_s, renew_lead_s, retention_s, state, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(id) DO UPDATE SET
                    recipe_id=excluded.recipe_id, d_tag=excluded.d_tag, amount_sat=excluded.amount_sat,
                    period_s=excluded.period_s, renew_lead_s=excluded.renew_lead_s,
                    retention_s=excluded.retention_s, updated_at=excluded.updated_at",
                rusqlite::params![
                    id, recipe_id, d, amount_sat, period_s, renew_lead_s, retention_s, UNPUBLISHED, now
                ],
            )?;
            Ok(())
        })
        .await
        .context("upserting durable listing row")
}

/// The durable publication state for `listing_id` — [`UNPUBLISHED`], [`ACTIVE`], [`WITHDRAWN`], or
/// `None` when no row exists yet. THIS, not what a relay still serves, is what decides an order.
pub async fn state(store: &Store, listing_id: &str) -> Result<Option<String>> {
    let id = listing_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT state FROM listing WHERE id=?1",
                rusqlite::params![id],
                |r| Ok(r.get::<_, Option<String>>(0)?.unwrap_or_default()),
            )
            .optional()?)
        })
        .await
        .context("reading durable listing state")
}

/// Whether buyers can order right now: the one bit an operator must not get wrong.
pub async fn is_published(store: &Store, listing_id: &str) -> Result<bool> {
    Ok(state(store, listing_id).await?.as_deref() == Some(ACTIVE))
}

/// Boot path: upsert the row, then republish the `30402` IFF the durable row already says `ACTIVE`.
///
/// This is what makes publication one-time rather than per-boot — the operator publishes once and
/// every restart thereafter re-broadcasts from the row with no operator action. An `UNPUBLISHED` or
/// `WITHDRAWN` row publishes NOTHING.
pub async fn republish_on_boot(
    store: &Store,
    clock: &Arc<dyn Clock>,
    relay: &dyn ListingRelay,
    recipe: &Recipe,
) -> Result<()> {
    // Under the same lock as the operator verbs: the read below decides whether to broadcast, and a
    // `withdraw` that lands between that read and the broadcast would otherwise be undone on the
    // relays by this boot.
    let _one_at_a_time = TRANSITION.lock().await;
    let operator_hex = relay.operator_pubkey_hex();
    let listing_id = coordinate(recipe, &operator_hex);
    upsert_row(store, clock, recipe, &listing_id).await?;

    let current = state(store, &listing_id).await?.unwrap_or_default();
    if current != ACTIVE {
        tracing::info!(
            listing = %listing_id,
            state = %current,
            "durable listing is not ACTIVE; publishing nothing (run `lnrent listing publish` to go live)"
        );
        return Ok(());
    }
    if let Err(e) = publish_event(store, relay, recipe, &listing_id, &operator_hex).await {
        tracing::warn!(
            listing = %listing_id,
            error = %format!("{e:#}"),
            "publishing 30402 listing failed (durable row stays ACTIVE; will republish next boot)"
        );
    }
    Ok(())
}

/// Publish the `30402` for `recipe` and record the returned event id on the durable row.
///
/// Relay-side only: the caller owns the `state` column. A relay failure is returned to the caller to
/// report/log — it never changes the durable state, because the row (not the relay) is what order
/// intake matches against.
async fn publish_event(
    store: &Store,
    relay: &dyn ListingRelay,
    recipe: &Recipe,
    listing_id: &str,
    operator_hex: &str,
) -> Result<(String, Option<String>)> {
    let event_id = match tokio::time::timeout(
        PUBLISH_TIMEOUT,
        relay.publish_listing_for(recipe, &d_tag(recipe), operator_hex),
    )
    .await
    {
        Ok(r) => r?,
        Err(_) => anyhow::bail!("relay publish timed out after {PUBLISH_TIMEOUT:?}"),
    };
    let (id, ev) = (listing_id.to_string(), event_id.clone());
    // The relay has ACCEPTED at this point, and that is the fact the operator is told about. A
    // failure to record the event id locally must NOT be reported as a relay failure: the caller
    // renders any `Err` from here as `relay_error` ("the 30402 did not reach a relay"), which would
    // be false — buyers can already discover the listing — and would send the operator chasing
    // relays over what is really a local storage fault (a degraded store, a full disk). The id is a
    // diagnostic, so losing it costs `lnrent status` its "discoverable" evidence and nothing more;
    // it is returned as a separate warning instead of collapsing into the same error.
    let persist_warning = match store
        .transaction(move |tx| {
            tx.execute(
                "UPDATE listing SET event_id=?2 WHERE id=?1",
                rusqlite::params![id, ev],
            )?;
            Ok(())
        })
        .await
        .context("recording the published listing event id")
    {
        Ok(()) => None,
        Err(e) => {
            let detail = format!("{e:#}");
            tracing::warn!(
                listing = %listing_id,
                event = %event_id,
                error = %detail,
                "a relay ACCEPTED the 30402 but its event id could not be recorded; the listing is \
                 published and discoverable, and the next publish re-records the id"
            );
            Some(detail)
        }
    };
    tracing::info!(listing = %listing_id, event = %event_id, "published 30402 listing");
    Ok((event_id, persist_warning))
}

/// `lnrent listing publish`: run preflight, gate on it, then mark the row `ACTIVE` and broadcast.
///
/// `accept_unverified` overrides REACHABILITY failures only — a structural failure is refused with
/// or without it. The durable state is written BEFORE the relay broadcast, mirroring the boot path:
/// the row is the authority order intake reads, so a relay outage must not leave "published" meaning
/// two different things.
///
/// Publishing after a [`withdraw`] is allowed and is the intended way back — the no-resurrect rule
/// binds the BOOT path ([`upsert_row`]), not an operator who explicitly asks again.
///
/// KNOWN RESIDUAL, deliberately not built here: an operator who publishes with `--accept-unverified`
/// is never re-checked or nagged afterwards. A dependency that never recovers surfaces instead as
/// failing orders and `lnrent money` / `lnrent preflight`. A standing re-check is a scheduler, which
/// is exactly the auto-publication policy this bead exists to remove.
pub async fn publish(
    store: &Store,
    clock: &Arc<dyn Clock>,
    payment: &Arc<dyn PaymentBackend>,
    recipes: &[Recipe],
    relay: &dyn ListingRelay,
    accept_unverified: bool,
) -> Result<PublishOutcome> {
    let recipe = recipes
        .first()
        .context("no recipe is loaded, so there is nothing to publish")?;
    let listing_id = coordinate(recipe, &relay.operator_pubkey_hex());
    // Snapshot the world BEFORE the probes and hand it to the gate, which re-reads under
    // [`TRANSITION`] and abandons the publish if it moved. The probes below are seconds of
    // third-party network I/O and run UNLOCKED (see [`TRANSITION`]), so an operator's
    // `listing withdraw` can complete inside that window; without this compare-and-swap the publish
    // would then take the lock and quietly resurrect the listing they just took down — a listing
    // taking orders while its operator has been told it is withdrawn.
    let observed = PublishSnapshot::take(store, &listing_id).await?;
    // The provider token resolves from the daemon env exactly as `Request::Preflight` resolves it;
    // the probe is the real DigitalOcean client. The gate's own classification tests drive
    // [`publish_gated`] with crafted checks and never reach this function; the tests that DO call it
    // stay network-free because `provider_token_check` short-circuits to a skip unless a loaded
    // recipe declares `DO_TOKEN` in `provisioning.env` — which the `dummy` recipe they use does not.
    let checks = crate::preflight::preflight_checks(
        payment,
        recipes,
        crate::preflight::read_token_env(),
        &crate::preflight::DoTokenProbe::new(),
    )
    .await;
    publish_gated(
        store,
        clock,
        recipes,
        relay,
        accept_unverified,
        checks,
        Some(observed),
    )
    .await
}

/// [`publish`] with the preflight verdicts already in hand — the seam the gate's own tests drive, so
/// the structural-vs-reachability split can be asserted without a live probe.
///
/// `pub(crate)` deliberately: it accepts ARBITRARY verdicts, so it IS the gate's own bypass. Only
/// [`publish`], which runs the real checks, is reachable from outside this crate.
///
/// `observed_before_preflight` is [`publish`]'s compare-and-swap: the [`PublishSnapshot`] it took
/// before the (unlocked) probes. If the world has moved since, this publish is acting on one that no
/// longer exists and is ABANDONED rather than applied — see [`PublishOutcome::Superseded`]. `None`
/// means "there was no window to guard" and skips the compare, which is what the tests that supply
/// their own verdicts pass.
pub(crate) async fn publish_gated(
    store: &Store,
    clock: &Arc<dyn Clock>,
    recipes: &[Recipe],
    relay: &dyn ListingRelay,
    accept_unverified: bool,
    checks: Vec<PreflightCheck>,
    observed_before_preflight: Option<PublishSnapshot>,
) -> Result<PublishOutcome> {
    let recipe = recipes
        .first()
        .context("no recipe is loaded, so there is nothing to publish")?;
    let _one_at_a_time = TRANSITION.lock().await;
    let operator_hex = relay.operator_pubkey_hex();
    let listing_id = coordinate(recipe, &operator_hex);

    // Read the DURABLE state before the gates, not after: a refusal has to be able to say whether
    // the listing it is refusing to publish is nevertheless still live and taking orders. (Reading
    // it here rather than after the upsert is equivalent — the boot upsert never touches `state`.)
    let current = state(store, &listing_id).await?.unwrap_or_default();
    // The CAS. Any change counts, not just a withdrawal: the safe rule is that a transition applies
    // only to the world it was decided against. The benign directions (someone else published the
    // same listing meanwhile) cost the operator one re-run and a reply naming the state that won;
    // the dangerous one — a withdrawal underneath us — is refused instead of being undone. The
    // epoch is the half of that the state cannot see: `withdraw` writes nothing when the row is
    // already WITHDRAWN or was never published, so without it a relist would sail past a withdrawal
    // the operator issued (and was truthfully answered) while this publish was probing.
    if let Some(observed) = observed_before_preflight {
        let withdraw_epoch = store.listing_withdraw_epoch();
        if observed.state != current || observed.withdraw_epoch != withdraw_epoch {
            tracing::warn!(
                listing = %listing_id,
                observed = %observed.state,
                now = %current,
                withdrawals_during_preflight = withdraw_epoch.saturating_sub(observed.withdraw_epoch),
                "abandoning `listing publish`: the listing changed while preflight was running"
            );
            return Ok(PublishOutcome::Superseded {
                listing_id,
                state: current,
            });
        }
    }
    let was_active = current == ACTIVE;

    // The two tiers PARTITION the failures, rather than each matching its own class: `class` is an
    // `Option` (a PASSING check carries none), so a `!ok` check with no class must land on the side
    // that REFUSES, not fall through both filters and publish. Every failure constructor in
    // `preflight.rs` sets a class today, so this changes no verdict — it makes the gate fail CLOSED
    // by construction, keeping `aggregate_ok(&checks) == false` ⟹ publication is gated, which is
    // exactly the property `lnrent preflight`'s own exit code already promises the operator.
    let failed = |reachability: bool| -> Vec<PreflightCheck> {
        checks
            .iter()
            .filter(|c| !c.ok && (c.class == Some(FailureClass::Reachability)) == reachability)
            .cloned()
            .collect()
    };
    let structural = failed(false);
    if !structural.is_empty() {
        return Ok(PublishOutcome::BlockedStructural {
            failed: structural,
            still_published: was_active,
        });
    }
    let unverified = failed(true);
    if !unverified.is_empty() && !accept_unverified {
        return Ok(PublishOutcome::BlockedUnverified {
            failed: unverified,
            still_published: was_active,
        });
    }

    // Refresh price/timers from the recipe first, so an operator who edited the price and then
    // published cannot publish a 30402 that disagrees with the row order intake prices against.
    upsert_row(store, clock, recipe, &listing_id).await?;
    if !was_active {
        // Clearing `event_id` is PART of this transition, not housekeeping. A row coming back from
        // `WITHDRAWN` still carries the id of the event the relays were asked to DELETE, and if the
        // broadcast below then fails, `lnrent status` would answer `published: true` alongside an
        // event id no relay serves — "live and discoverable" and "live but nobody has it" have to
        // stay distinguishable. `publish_event` writes the real id back the moment one lands.
        set_state(store, clock, &listing_id, ACTIVE, true).await?;
        tracing::info!(listing = %listing_id, "operator published the listing");
    }

    let broadcast = publish_event(store, relay, recipe, &listing_id, &operator_hex).await;
    let (event_id, relay_error) = match broadcast {
        // A relay took it. `persist_warning` (the event id could not be recorded locally) is NOT a
        // relay error: the listing is discoverable either way, so reporting it here would tell the
        // operator the opposite of what happened. It is already logged at the failure site, and the
        // absent `event_id` is what `lnrent status` renders.
        Ok((id, _persist_warning)) => (Some(id), None),
        Err(e) => {
            let detail = format!("{e:#}");
            tracing::warn!(
                listing = %listing_id,
                error = %detail,
                "listing is ACTIVE but the 30402 did not reach a relay; the next boot republishes"
            );
            (None, Some(detail))
        }
    };
    Ok(if was_active {
        PublishOutcome::AlreadyPublished {
            listing_id,
            event_id,
            relay_error,
            warnings: unverified,
        }
    } else {
        PublishOutcome::Published {
            listing_id,
            event_id,
            relay_error,
            warnings: unverified,
        }
    })
}

/// `lnrent listing withdraw`: write `WITHDRAWN`, then ask the relays to drop the `30402`.
///
/// Order matters and is the whole design: the DURABLE write lands first and unconditionally, because
/// it is what actually stops orders (`order_intake.rs` refuses a non-`ACTIVE` row). The relay
/// retraction is BEST-EFFORT buyer-UX afterwards — bounded by [`RETRACT_TIMEOUT`], reported, and
/// never able to block or reverse the local withdrawal.
///
/// Re-running it on an ALREADY-withdrawn listing re-attempts that retraction (the durable state is
/// left exactly as it is) — see [`WithdrawOutcome::AlreadyWithdrawn`].
pub async fn withdraw(
    store: &Store,
    clock: &Arc<dyn Clock>,
    recipes: &[Recipe],
    relay: &dyn ListingRelay,
) -> Result<WithdrawOutcome> {
    let recipe = recipes
        .first()
        .context("no recipe is loaded, so there is nothing to withdraw")?;
    let _one_at_a_time = TRANSITION.lock().await;
    // Count the ATTEMPT, before knowing whether it will write anything: a publish that is out
    // probing its dependencies right now must be superseded by the operator asking to come down,
    // including on the branches below that leave the row untouched.
    //
    // UNDER the lock, not before it. Incrementing outside made the epoch itself racy on a
    // multi-threaded runtime: a withdrawal could `fetch_add` and then, before its `lock().await`
    // enqueued, a publish's `PublishSnapshot::take` could acquire TRANSITION and read the ALREADY
    // INCREMENTED epoch together with the still-unchanged state — absorbing a withdrawal that had
    // executed nothing into its own baseline. If that withdrawal then took one of the branches
    // below that write nothing durable (`AlreadyWithdrawn`, `NotPublished`), the publish's CAS saw
    // neither the state nor the epoch move and republished on top of the retraction, which is the
    // precise case the epoch exists to close.
    //
    // Inside the lock every interleaving resolves by lock order instead: a withdrawal that gets the
    // lock first is fully reflected in the snapshot that follows it, and one that gets it second
    // increments while the publish is probing, so the CAS catches it. The pre-lock increment's
    // stated purpose — covering a withdrawal queued behind a publish's relay leg — is unaffected:
    // that publish has already committed, and this increment still lands before any later
    // snapshot, which cannot read the epoch without first taking this lock.
    store.note_listing_withdraw();
    let listing_id = coordinate(recipe, &relay.operator_pubkey_hex());
    match state(store, &listing_id).await?.as_deref() {
        // Already withdrawn: the durable state is final, but the RELAY leg is best-effort and may
        // never have landed — the relays were unreachable, or the process died between the commit
        // and the send. Re-running the command is then the operator's only affordance for asking
        // them again; returning early here would leave the stale 30402 discoverable forever with no
        // way to retry short of publishing and withdrawing a second time. This mirrors publish,
        // which likewise re-broadcasts on an already-`ACTIVE` row. Nothing durable is rewritten.
        Some(WITHDRAWN) => {
            let retract_error = retract_from_relays(relay, recipe, &listing_id).await;
            return Ok(WithdrawOutcome::AlreadyWithdrawn {
                listing_id,
                retract_error,
            });
        }
        Some(ACTIVE) => {}
        // No row, or one that was never published. The durable state is left alone — writing
        // WITHDRAWN over UNPUBLISHED would silently take away the operator's ability to tell the two
        // apart — but the RELAYS are still asked to drop the coordinate.
        //
        // That is not belt-and-braces: this database having no record is not evidence that no event
        // exists. A 30402 is replaceable on `(kind, pubkey, d)`, and both halves of that key survive
        // a data-dir reset — the pubkey comes from the operator seed, the `d` from the recipe id. So
        // an operator who wipes and re-bootstraps on the same seed (or restores an older backup)
        // gets an UNPUBLISHED row while relays still serve the PREVIOUS incarnation's listing.
        // Before this bead that healed itself, because the row was born ACTIVE and the next boot
        // republished over the stale event; quiet boot removes that, so without retracting here the
        // operator has NO way to take it down — buyers keep discovering a listing whose orders all
        // bounce (`unavailable`), and `withdraw`, the documented stop command, would answer "nothing
        // to withdraw" while the thing they want gone is still up.
        _ => {
            let retract_error = retract_from_relays(relay, recipe, &listing_id).await;
            return Ok(WithdrawOutcome::NotPublished {
                listing_id,
                retract_error,
            });
        }
    }
    // `event_id` is deliberately LEFT on the row (hence `false`): it names the event the retraction
    // below targets, and the one relays may keep serving if that retraction fails.
    // `state`/`published` already say this listing is not live, so keeping the id is a diagnostic,
    // not a second answer — and a later `publish` clears it as part of going back to ACTIVE.
    set_state(store, clock, &listing_id, WITHDRAWN, false).await?;
    tracing::info!(listing = %listing_id, "operator withdrew the listing; order intake now refuses");

    let retract_error = retract_from_relays(relay, recipe, &listing_id).await;
    Ok(WithdrawOutcome::Withdrawn {
        listing_id,
        retract_error,
    })
}

/// The best-effort relay leg of [`withdraw`], bounded by [`RETRACT_TIMEOUT`]. Returns the operator's
/// diagnostic on failure and NEVER an `Err`: by the time it runs the durable withdrawal is already
/// committed, and no relay outcome may reverse or block it.
async fn retract_from_relays(
    relay: &dyn ListingRelay,
    recipe: &Recipe,
    listing_id: &str,
) -> Option<String> {
    let d = d_tag(recipe);
    let retract_error =
        match tokio::time::timeout(RETRACT_TIMEOUT, relay.retract_listing_at(&d)).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("{e:#}")),
            Err(_) => Some(format!(
                "relay retraction timed out after {RETRACT_TIMEOUT:?}"
            )),
        };
    if let Some(detail) = &retract_error {
        tracing::warn!(
            listing = %listing_id,
            error = %detail,
            "the listing is WITHDRAWN locally (orders refused) but the relay retraction failed; \
             relays may keep serving the stale 30402 — re-run `lnrent listing withdraw` to ask again"
        );
    }
    retract_error
}

/// Set the row's `state`. The ONLY writer of `ACTIVE`/`WITHDRAWN` — both reached exclusively from an
/// operator verb, never from a probe, a loop, or a boot path.
///
/// An UPDATE that matched no row is an ERROR, not a silent no-op: both callers hold [`TRANSITION`]
/// and have just read (publish additionally upserts) the row, so a zero rowcount means the row is
/// gone underneath them — and reporting `published: true` for a write that landed nowhere is the one
/// thing this module must never do.
///
/// `clear_event_id` drops the recorded 30402 id in the SAME write as the state change, so no window
/// exists in which the row claims a publication state and an event id that belong to different
/// publications. Each call site documents its choice.
async fn set_state(
    store: &Store,
    clock: &Arc<dyn Clock>,
    listing_id: &str,
    state: &'static str,
    clear_event_id: bool,
) -> Result<()> {
    let (id, now) = (listing_id.to_string(), clock.now());
    let sql = if clear_event_id {
        "UPDATE listing SET state=?2, updated_at=?3, event_id=NULL WHERE id=?1"
    } else {
        "UPDATE listing SET state=?2, updated_at=?3 WHERE id=?1"
    };
    store
        .transaction(move |tx| {
            let rows = tx.execute(sql, rusqlite::params![id, state, now])?;
            if rows == 0 {
                anyhow::bail!("listing row `{id}` does not exist");
            }
            Ok(())
        })
        .await
        .with_context(|| format!("marking the durable listing {state}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MockPayment;
    use crate::clock::TestClock;
    use std::sync::Mutex;

    /// A [`ListingRelay`] that records what it was asked to do instead of touching a relay, and can
    /// be told to fail — so the durable-state rules can be asserted independently of any relay.
    struct FakeRelay {
        operator_hex: String,
        published: Mutex<Vec<String>>,
        retracted: Mutex<Vec<String>>,
        /// Every relay side effect in the order it happened, so a test can assert the ORDER two
        /// concurrent verbs left the relays in, not just the counts.
        ops: Mutex<Vec<&'static str>>,
        /// `Mutex` rather than plain `bool`s so a test can flip either leg BETWEEN two commands:
        /// the relays recovering is the retry case [`withdraw`] has to serve, and the relays going
        /// down after a successful publish is how a relist's broadcast fails.
        publish_fails: Mutex<bool>,
        retract_fails: Mutex<bool>,
        /// Widens the publish window so a concurrent verb reliably arrives inside it.
        publish_delay: Duration,
    }

    impl FakeRelay {
        fn new() -> Self {
            Self {
                // A well-formed 32-byte hex pubkey; the gate never parses it, but keeping it real
                // means the coordinate looks like a production one.
                operator_hex: "a".repeat(64),
                published: Mutex::new(Vec::new()),
                retracted: Mutex::new(Vec::new()),
                ops: Mutex::new(Vec::new()),
                publish_fails: Mutex::new(false),
                retract_fails: Mutex::new(false),
                publish_delay: Duration::ZERO,
            }
        }
        fn publish_count(&self) -> usize {
            self.published.lock().unwrap().len()
        }
        fn retract_count(&self) -> usize {
            self.retracted.lock().unwrap().len()
        }
        fn ops(&self) -> Vec<&'static str> {
            self.ops.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ListingRelay for FakeRelay {
        fn operator_pubkey_hex(&self) -> String {
            self.operator_hex.clone()
        }
        async fn publish_listing_for(&self, _: &Recipe, d: &str, _: &str) -> Result<String> {
            if !self.publish_delay.is_zero() {
                tokio::time::sleep(self.publish_delay).await;
            }
            self.published.lock().unwrap().push(d.to_string());
            self.ops.lock().unwrap().push("publish");
            if *self.publish_fails.lock().unwrap() {
                anyhow::bail!("no relays accepted publishing 30402 listing");
            }
            Ok(format!("event-{}", self.published.lock().unwrap().len()))
        }
        async fn retract_listing_at(&self, d: &str) -> Result<()> {
            self.retracted.lock().unwrap().push(d.to_string());
            self.ops.lock().unwrap().push("retract");
            if *self.retract_fails.lock().unwrap() {
                anyhow::bail!("no relays accepted retracting 30402 listing");
            }
            Ok(())
        }
    }

    fn dummy_recipe() -> Recipe {
        let dir = format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
        Recipe::load(&dir).expect("load dummy recipe")
    }

    fn fixture() -> (Store, Arc<dyn Clock>, Arc<dyn PaymentBackend>, Vec<Recipe>) {
        (
            Store::open_spawn(":memory:").unwrap(),
            Arc::new(TestClock::new(1_000)),
            Arc::new(MockPayment::new()),
            vec![dummy_recipe()],
        )
    }

    // A fresh boot must NOT publish: the row is born UNPUBLISHED and no event goes out.
    #[tokio::test]
    async fn fresh_boot_does_not_publish() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();

        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();

        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(UNPUBLISHED)
        );
        assert!(
            !is_published(&store, &id).await.unwrap(),
            "a fresh daemon is not published"
        );
        assert_eq!(relay.publish_count(), 0, "no 30402 reached a relay");
    }

    // Publish, then restart: the durable row republishes with NO operator action.
    #[tokio::test]
    async fn publish_then_restart_republishes() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();

        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        assert!(matches!(out, PublishOutcome::Published { .. }), "{out:?}");
        assert_eq!(relay.publish_count(), 1);

        // A second boot over the SAME store — the operator does nothing.
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        assert_eq!(relay.publish_count(), 2, "boot republished from the row");
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(is_published(&store, &id).await.unwrap());
    }

    // Withdraw, then restart: the no-resurrect guard still holds.
    #[tokio::test]
    async fn withdraw_then_restart_does_not_republish() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();

        let out = withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        assert!(
            matches!(
                out,
                WithdrawOutcome::Withdrawn {
                    retract_error: None,
                    ..
                }
            ),
            "{out:?}"
        );
        assert_eq!(relay.retract_count(), 1, "retraction was attempted");
        let published_before_boot = relay.publish_count();

        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();

        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(WITHDRAWN),
            "boot must not resurrect a WITHDRAWN listing"
        );
        assert_eq!(
            relay.publish_count(),
            published_before_boot,
            "no 30402 was republished for a WITHDRAWN listing"
        );
    }

    // A failed relay retraction must NOT block or reverse the local withdrawal.
    #[tokio::test]
    async fn failed_retraction_still_withdraws_locally() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        *relay.retract_fails.lock().unwrap() = true;
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();

        let out = withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        match out {
            WithdrawOutcome::Withdrawn { retract_error, .. } => {
                assert!(retract_error.is_some(), "the relay failure is reported")
            }
            other => panic!("expected a local withdrawal despite the relay failure: {other:?}"),
        }
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(WITHDRAWN)
        );
    }

    // ...and re-running withdraw once the relays are back RETRIES that retraction. Without this the
    // operator has no affordance at all for the (entirely ordinary) case where the first withdrawal's
    // relay leg failed: the durable state is already final, so the command would return "already
    // withdrawn" and the stale 30402 would stay discoverable indefinitely.
    #[tokio::test]
    async fn re_running_withdraw_retries_a_failed_retraction() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        *relay.retract_fails.lock().unwrap() = true;
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        assert_eq!(relay.retract_count(), 1, "the first attempt failed");

        // The relays recover, and the operator runs the same command again.
        *relay.retract_fails.lock().unwrap() = false;
        let out = withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        match out {
            WithdrawOutcome::AlreadyWithdrawn { retract_error, .. } => assert!(
                retract_error.is_none(),
                "the retry succeeded: {retract_error:?}"
            ),
            other => panic!("expected an idempotent withdrawal that retried: {other:?}"),
        }
        assert_eq!(relay.retract_count(), 2, "the retraction was asked again");
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(WITHDRAWN),
            "the retry rewrites nothing durable"
        );
    }

    // Publishing an UNPUBLISHED listing with a dead relay still commits the durable ACTIVE state —
    // the row is the authority, and the next boot republishes.
    #[tokio::test]
    async fn relay_failure_does_not_block_the_durable_publish() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        *relay.publish_fails.lock().unwrap() = true;
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();

        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        match out {
            PublishOutcome::Published {
                event_id,
                relay_error,
                ..
            } => {
                assert!(event_id.is_none());
                assert!(relay_error.is_some(), "the relay failure is reported");
            }
            other => panic!("expected a durable publish despite the relay failure: {other:?}"),
        }
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(is_published(&store, &id).await.unwrap());
    }

    // Withdraw does not write WITHDRAWN over a listing that was never published, so UNPUBLISHED and
    // WITHDRAWN stay distinguishable — but it DOES ask the relays to drop the coordinate.
    //
    // The relay leg is the half that is not obvious. This database having no record of publishing is
    // not evidence that no event exists: a 30402 is replaceable on `(kind, pubkey, d)`, and both
    // halves survive a data-dir reset (pubkey from the operator seed, `d` from the recipe id). An
    // operator who wipes and re-bootstraps on the same seed therefore has an UNPUBLISHED row while
    // relays still serve the previous incarnation's listing. Quiet boot removed the accident that
    // used to heal this (a row born ACTIVE republished over the stale event), so if `withdraw` did
    // not retract here, nothing could.
    #[tokio::test]
    async fn withdraw_leaves_an_unpublished_row_alone_but_still_retracts() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());

        let out = withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        assert!(
            matches!(out, WithdrawOutcome::NotPublished { .. }),
            "{out:?}"
        );
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(UNPUBLISHED),
            "the durable state must stay UNPUBLISHED, not become WITHDRAWN"
        );
        assert_eq!(
            relay.retract_count(),
            1,
            "the relays are still asked to drop the coordinate a previous data dir may have published"
        );
    }

    // Publish is idempotent: a second publish re-broadcasts without churning the state.
    #[tokio::test]
    async fn publishing_twice_is_idempotent() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();

        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        assert!(
            matches!(out, PublishOutcome::AlreadyPublished { .. }),
            "{out:?}"
        );
    }

    fn check(name: &'static str, class: Option<FailureClass>) -> PreflightCheck {
        PreflightCheck {
            name,
            ok: class.is_none(),
            detail: "fixture".into(),
            class,
        }
    }

    // A STRUCTURAL failure hard-blocks publication, names the failing check, and writes nothing.
    #[tokio::test]
    async fn structural_failure_refuses_publication() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();

        let out = publish_gated(
            &store,
            &clock,
            &recipes,
            &relay,
            // Even the override must not admit a structural failure.
            true,
            vec![
                check("gateway", None),
                check("recipe_preflight", Some(FailureClass::Structural)),
            ],
            None,
        )
        .await
        .unwrap();

        match out {
            PublishOutcome::BlockedStructural {
                failed,
                still_published,
            } => {
                assert_eq!(failed.len(), 1);
                assert_eq!(failed[0].name, "recipe_preflight", "the failure is named");
                assert!(!still_published, "it was never live to begin with");
            }
            other => panic!("a structural failure must hard-block: {other:?}"),
        }
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(!is_published(&store, &id).await.unwrap());
        assert_eq!(relay.publish_count(), 0);
    }

    // The gate is FAIL-CLOSED on the wire contract's optional `class`: a failing check that carries
    // no class must be refused, not fall between the two tiers and publish. Every constructor in
    // `preflight.rs` classifies its failures today, so this asserts the gate's shape rather than a
    // reachable verdict — the property being pinned is `aggregate_ok == false` ⟹ publication is
    // gated, which is what `lnrent preflight`'s nonzero exit already promises the operator.
    #[tokio::test]
    async fn an_unclassified_failure_is_refused_rather_than_published() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        let unclassified = PreflightCheck {
            name: "gateway",
            ok: false,
            detail: "fixture".into(),
            class: None,
        };
        assert!(!crate::preflight::aggregate_ok(std::slice::from_ref(
            &unclassified
        )));

        // Even with the override, which only ever admits a REACHABILITY failure.
        let out = publish_gated(&store, &clock, &recipes, &relay, true, vec![unclassified], None)
            .await
            .unwrap();
        assert!(
            matches!(out, PublishOutcome::BlockedStructural { .. }),
            "an unclassified failure must be refused: {out:?}"
        );
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(!is_published(&store, &id).await.unwrap());
        assert_eq!(relay.publish_count(), 0);
    }

    // A REACHABILITY failure refuses WITHOUT the override and succeeds (carrying the warning) WITH it.
    #[tokio::test]
    async fn reachability_failure_is_overridable() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        let checks = || {
            vec![
                check("federation", Some(FailureClass::Reachability)),
                check("recipe_preflight", None),
            ]
        };
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());

        let refused = publish_gated(&store, &clock, &recipes, &relay, false, checks(), None)
            .await
            .unwrap();
        match refused {
            PublishOutcome::BlockedUnverified {
                failed,
                still_published,
            } => {
                assert_eq!(failed[0].name, "federation", "the failure is named");
                assert!(!still_published);
            }
            other => panic!("expected a refusal without the override: {other:?}"),
        }
        assert!(!is_published(&store, &id).await.unwrap());
        assert_eq!(relay.publish_count(), 0);

        let accepted = publish_gated(&store, &clock, &recipes, &relay, true, checks(), None)
            .await
            .unwrap();
        match accepted {
            PublishOutcome::Published { warnings, .. } => {
                assert_eq!(warnings.len(), 1, "the overridden check is reported back");
                assert_eq!(warnings[0].name, "federation");
            }
            other => panic!("--accept-unverified must publish: {other:?}"),
        }
        assert!(is_published(&store, &id).await.unwrap());
        assert_eq!(relay.publish_count(), 1);
    }

    // Nothing but the operator withdraws: a backend/probe failure AFTER publication leaves the
    // listing ACTIVE. `republish_on_boot` is the one automatic path that touches the row, and even a
    // fully-failing preflight has no say in it.
    #[tokio::test]
    async fn a_probe_failure_never_withdraws_a_published_listing() {
        let (store, clock, _payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish_gated(&store, &clock, &recipes, &relay, false, vec![], None)
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());

        // Every dependency is now broken, and the daemon restarts into that.
        let broken = FakeRelay::new();
        *broken.publish_fails.lock().unwrap() = true;
        republish_on_boot(&store, &clock, &broken, &recipes[0])
            .await
            .unwrap();

        assert!(
            is_published(&store, &id).await.unwrap(),
            "an outage must never retract the operator's listing"
        );
    }

    // A publish and a withdraw arriving at once (two IPC connections) must not interleave: whoever
    // goes second sees the first one's durable state, and the relay side effects land in the SAME
    // order as the durable writes — never a 30402 broadcast on top of a completed retraction, and
    // never a reply that contradicts the row.
    #[tokio::test]
    async fn a_concurrent_publish_and_withdraw_do_not_interleave() {
        let (store, clock, _payment, recipes) = fixture();
        let mut relay = FakeRelay::new();
        // Hold the publish inside its relay leg so the withdraw reliably arrives mid-transition.
        relay.publish_delay = Duration::from_millis(50);
        let relay = Arc::new(relay);
        republish_on_boot(&store, &clock, relay.as_ref(), &recipes[0])
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());

        let publishing = {
            let (store, clock, recipes, relay) =
                (store.clone(), clock.clone(), recipes.clone(), relay.clone());
            tokio::spawn(async move {
                publish_gated(&store, &clock, &recipes, relay.as_ref(), false, vec![], None).await
            })
        };
        // Let the publish take the transition and reach its (slow) broadcast.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let withdrawing = {
            let (store, clock, recipes, relay) =
                (store.clone(), clock.clone(), recipes.clone(), relay.clone());
            tokio::spawn(async move { withdraw(&store, &clock, &recipes, relay.as_ref()).await })
        };

        let published = publishing.await.unwrap().unwrap();
        let withdrawn = withdrawing.await.unwrap().unwrap();

        // The publish finished first and was TRUE when it answered; the withdraw then observed the
        // ACTIVE row it left (rather than an UNPUBLISHED one it raced past) and withdrew it.
        assert!(
            matches!(published, PublishOutcome::Published { .. }),
            "{published:?}"
        );
        assert!(
            matches!(withdrawn, WithdrawOutcome::Withdrawn { .. }),
            "{withdrawn:?}"
        );
        assert_eq!(
            state(&store, &id).await.unwrap().as_deref(),
            Some(WITHDRAWN),
            "the last operator act is what the durable row says"
        );
        assert_eq!(
            relay.ops(),
            vec!["publish", "retract"],
            "the relay side effects follow the durable writes; a 30402 must never land after the \
             retraction that was meant to remove it"
        );
    }

    // A refusal on an ALREADY-LIVE listing must report that it is still live, and must leave it
    // alone. This is the reachable one: an operator published last week, a dependency broke, and
    // they re-run publish. Refusing while silently implying "not published" would be the very
    // confusion the gate exists to remove — and a refusal must never withdraw.
    #[tokio::test]
    async fn a_refusal_reports_a_listing_that_is_still_live() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        let published_events = relay.publish_count();

        for (class, expect_structural) in [
            (FailureClass::Reachability, false),
            (FailureClass::Structural, true),
        ] {
            let out = publish_gated(
                &store,
                &clock,
                &recipes,
                &relay,
                false,
                vec![check("gateway", Some(class))],
                None,
            )
            .await
            .unwrap();
            let still = match (&out, expect_structural) {
                (PublishOutcome::BlockedStructural { still_published, .. }, true) => *still_published,
                (PublishOutcome::BlockedUnverified { still_published, .. }, false) => {
                    *still_published
                }
                _ => panic!("wrong refusal for {class:?}: {out:?}"),
            };
            assert!(still, "the refusal reports the listing is still live: {out:?}");
        }

        assert!(
            is_published(&store, &id).await.unwrap(),
            "a refusal must never withdraw a live listing"
        );
        assert_eq!(
            relay.publish_count(),
            published_events,
            "a refusal broadcasts nothing"
        );
    }

    /// The recorded 30402 id for a row — the `event_id` `lnrent status` shows the operator.
    async fn event_id_of(store: &Store, listing_id: &str) -> Option<String> {
        let id = listing_id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT event_id FROM listing WHERE id=?1",
                    rusqlite::params![id],
                    |r| r.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten())
            })
            .await
            .unwrap()
    }

    // `publish` runs its probes UNLOCKED (seconds of third-party I/O), so a `listing withdraw` can
    // complete inside that window. The publish must then be ABANDONED, not applied: applying it
    // would resurrect a listing the operator has already been told is down, and it would start
    // taking orders again with nobody having asked for it. Driven through `publish_gated` with the
    // snapshot `publish` takes, because that is the compare — no relay or clock timing involved.
    #[tokio::test]
    async fn a_withdrawal_during_preflight_supersedes_the_publish() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());

        // The operator's publish reads ACTIVE and goes off to probe...
        let observed = PublishSnapshot::take(&store, &id).await.unwrap();
        assert_eq!(observed.state, ACTIVE);
        // ...their withdraw lands while it is still probing...
        withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        let published_events = relay.publish_count();
        // ...and only now do the (all-passing) verdicts come back.
        let out = publish_gated(
            &store,
            &clock,
            &recipes,
            &relay,
            false,
            vec![],
            Some(observed),
        )
        .await
        .unwrap();

        match out {
            PublishOutcome::Superseded { state, .. } => {
                assert_eq!(state, WITHDRAWN, "the reply names the state that won")
            }
            other => panic!("a publish decided against a stale state must abandon: {other:?}"),
        }
        assert_eq!(
            super::state(&store, &id).await.unwrap().as_deref(),
            Some(WITHDRAWN),
            "the operator's withdrawal is final"
        );
        assert_eq!(
            relay.publish_count(),
            published_events,
            "nothing was broadcast on top of the retraction"
        );

        // ...and the operator can still relist deliberately, which is the whole difference: the
        // command they run NOW is decided against the state they are actually in.
        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        assert!(matches!(out, PublishOutcome::Published { .. }), "{out:?}");
    }

    // A relist whose broadcast fails must not leave the WITHDRAWN row's `event_id` behind: that id
    // names the event the relays were just asked to DELETE, and `lnrent status` would otherwise
    // answer "published, and here is your live event" for a publication no relay ever saw.
    #[tokio::test]
    async fn a_relist_that_never_reaches_a_relay_reports_no_event_id() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(
            event_id_of(&store, &id).await.is_some(),
            "the first publication recorded its event"
        );
        withdraw(&store, &clock, &recipes, &relay).await.unwrap();
        assert!(
            event_id_of(&store, &id).await.is_some(),
            "withdraw keeps it: it names the event the retraction targeted"
        );

        // The relays go away, and the operator relists.
        *relay.publish_fails.lock().unwrap() = true;
        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        match out {
            PublishOutcome::Published {
                event_id,
                relay_error,
                ..
            } => {
                assert!(event_id.is_none());
                assert!(relay_error.is_some());
            }
            other => panic!("expected a durable publish despite the relay failure: {other:?}"),
        }
        assert!(
            is_published(&store, &id).await.unwrap(),
            "the durable publication stands — the row is the authority"
        );
        assert_eq!(
            event_id_of(&store, &id).await,
            None,
            "but no event id is claimed for a publication no relay took"
        );
    }

    // ...and the same must hold for the withdrawals that change NOTHING durable, which is where a
    // state-only comparison is blind: re-asking the relays about an already-WITHDRAWN listing, and
    // refusing one that was never published, both leave the row exactly as the publish saw it. The
    // operator relists, changes their mind, runs `listing withdraw` in another terminal, is told
    // truthfully that the listing is not up — and the publish they can no longer see must NOT bring
    // it up behind them.
    #[tokio::test]
    async fn a_withdrawal_that_writes_nothing_still_supersedes_an_in_flight_publish() {
        for already_published_once in [false, true] {
            let (store, clock, payment, recipes) = fixture();
            let relay = FakeRelay::new();
            republish_on_boot(&store, &clock, &relay, &recipes[0])
                .await
                .unwrap();
            let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
            // Either a fresh install (UNPUBLISHED → withdraw refuses) or a relist after a
            // withdrawal (WITHDRAWN → withdraw only re-asks the relays). Neither writes `state`.
            if already_published_once {
                publish(&store, &clock, &payment, &recipes, &relay, false)
                    .await
                    .unwrap();
                withdraw(&store, &clock, &recipes, &relay).await.unwrap();
            }
            let before = state(&store, &id).await.unwrap().unwrap();
            let published_events = relay.publish_count();

            // The operator's publish snapshots that state and goes off to probe...
            let observed = PublishSnapshot::take(&store, &id).await.unwrap();
            // ...their withdraw lands while it is still probing, and writes nothing...
            let out = withdraw(&store, &clock, &recipes, &relay).await.unwrap();
            assert!(
                matches!(
                    out,
                    WithdrawOutcome::NotPublished { .. } | WithdrawOutcome::AlreadyWithdrawn { .. }
                ),
                "this case is the one that leaves the row untouched: {out:?}"
            );
            assert_eq!(
                state(&store, &id).await.unwrap().as_deref(),
                Some(before.as_str()),
                "the durable state really is unchanged, so only the epoch can catch this"
            );

            // ...and only now do the (all-passing) verdicts come back.
            let out = publish_gated(
                &store,
                &clock,
                &recipes,
                &relay,
                false,
                vec![],
                Some(observed),
            )
            .await
            .unwrap();
            assert!(
                matches!(out, PublishOutcome::Superseded { .. }),
                "a withdrawal during preflight supersedes the publish even when it wrote \
                 nothing (already published once: {already_published_once}): {out:?}"
            );
            assert!(!is_published(&store, &id).await.unwrap());
            assert_eq!(
                relay.publish_count(),
                published_events,
                "nothing was broadcast"
            );

            // The operator can still relist deliberately: the command they run NOW is decided
            // against the world they are actually in.
            let out = publish(&store, &clock, &payment, &recipes, &relay, false)
                .await
                .unwrap();
            assert!(matches!(out, PublishOutcome::Published { .. }), "{out:?}");
        }
    }

    // An operator may publish again after withdrawing — the no-resurrect rule binds BOOT, not an
    // explicit operator act.
    #[tokio::test]
    async fn publish_after_withdraw_relists() {
        let (store, clock, payment, recipes) = fixture();
        let relay = FakeRelay::new();
        republish_on_boot(&store, &clock, &relay, &recipes[0])
            .await
            .unwrap();
        publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        withdraw(&store, &clock, &recipes, &relay).await.unwrap();

        let out = publish(&store, &clock, &payment, &recipes, &relay, false)
            .await
            .unwrap();
        assert!(matches!(out, PublishOutcome::Published { .. }), "{out:?}");
        let id = coordinate(&recipes[0], &relay.operator_pubkey_hex());
        assert!(is_published(&store, &id).await.unwrap());
    }

    // ORDERING AUDIT: the withdrawal epoch must be incremented UNDER `TRANSITION`.
    //
    // This is a source audit rather than a behavioral test on purpose. The defect it guards is a
    // cross-thread window — a `fetch_add` that lands before its own `lock().await` enqueues, letting
    // a concurrent `PublishSnapshot::take` read the incremented epoch beside the not-yet-changed
    // state and absorb a withdrawal that has executed nothing. Reproducing that window
    // deterministically is not possible from a test: the "did not increment yet" assertion passes
    // vacuously whenever the spawned task has not reached the lock, so the test would be flaky in the
    // direction that reports success. `a_withdrawal_that_writes_nothing_still_supersedes_an_in_flight_publish`
    // pins the sequential behaviour; this pins the ordering the concurrent case depends on, the way
    // `refund.rs`'s `only_known_sites_insert_refund_attempt` pins its writer set.
    #[test]
    fn the_withdraw_epoch_is_counted_under_the_transition_lock() {
        let src = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/listing.rs"
        ))
        .unwrap();
        // Production half only — the tests below may mention either symbol freely.
        let production = src.split("#[cfg(test)]").next().unwrap_or("");
        let body = production
            .split_once("pub async fn withdraw(")
            .expect("withdraw exists")
            .1
            .split_once("\n}\n")
            .expect("withdraw has a body")
            .0;

        assert_eq!(
            production.matches("note_listing_withdraw()").count(),
            1,
            "one production call site is what makes this audit sufficient; a second one needs its \
             own ordering argument"
        );
        let lock_at = body
            .find("TRANSITION.lock()")
            .expect("withdraw takes the transition lock");
        let bump_at = body
            .find("note_listing_withdraw()")
            .expect("withdraw counts the attempt");
        assert!(
            lock_at < bump_at,
            "`note_listing_withdraw()` must come AFTER `TRANSITION.lock()` in `withdraw`: counting \
             the attempt outside the lock lets a publish snapshot absorb a withdrawal that has not \
             run yet, and the compare-and-swap then misses it on the branches that write nothing \
             durable (AlreadyWithdrawn / NotPublished)"
        );
    }
}
