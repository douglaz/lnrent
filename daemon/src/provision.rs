//! Provision + delivery outbox (lnrent-7fp.10, SPEC.md §6.2/§7.2, ADR-0009 §6.6).
//!
//! The settle → capture → **provision** → deliver step. Capture (lnrent-7fp.8) already moved the
//! subscription `PENDING -> PROVISIONING` and stamped the order invoice's `settled_at`; this module
//! runs the recipe `provision` hook and, on success, in ONE store transaction (the capture.rs /
//! order_intake.rs atomic-multi-row style): moves the sub `PROVISIONING -> ACTIVE`, sets the timers
//! off `settled_at` (never the wall clock), CONSUMES the capacity reservation (lnrent-7fp.7),
//! records the `instance` row, and writes a PENDING `outbox` row carrying the `provision.ready` DM.
//! The DM is published only by the outbox sender, AFTER that commit — so a crash between ACTIVE and
//! send just leaves the row PENDING for a restart drain (durable, at-least-once).
//!
//! It only consumes existing seams (the store actor, the recipe runner, `reservation::consume_txn`,
//! `lnrent_wire`, the injected [`Outbound`] publish primitive); it rebuilds nothing, and it does
//! NOT execute refunds (lnrent-7fp.11), fire deadlines (reconcile, lnrent-7fp.9), or own the daemon
//! task supervisor that drives it on an interval (lnrent-7fp.21).

use std::{error::Error as StdError, fmt, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};

use lnrent_wire::{Msg, ProvisionReady, PublicKey};

use crate::alerts::{Alert, AlertDispatcher, AlertKind};
use crate::clock::Clock;
use crate::nostr_engine::{OrderHandler, Outbound};
use crate::recipe::Recipe;
use crate::reservation;
use crate::runner::{run_hook, HookOutput, DEFAULT_TIMEOUT};
use crate::store::Store;

/// How long a provision-failure cleanup may stay open (its idempotent `destroy` failing every
/// maintenance retry) before the operator is alerted (lnrent-urw.2). The backlog itself is surfaced
/// immediately in `lnrent teardowns`; this bounds the DM to genuinely-stuck cleanups.
const CLEANUP_STUCK_ALERT_S: i64 = 3600;

/// Bounded retry for the `provision` hook within one drive: the hook is idempotent (§7.2), so a
/// transient failure is safe to re-run a few times before declaring a PERMANENT failure. A simple
/// count — not a backoff framework (the steady-state cadence belongs to the supervisor, .21).
const PROVISION_ATTEMPTS: u32 = 3;
const PROVISION_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const CLEANUP_PENDING: &str = "provision_cleanup_pending";
const CLEANUP_DONE: &str = "provision_cleanup_done";

/// Outcome of driving one subscription through provisioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provisioned {
    /// Hook succeeded: sub `-> ACTIVE`, reservation CONSUMED, `instance` recorded, `provision.ready`
    /// queued in the outbox.
    Active,
    /// Hook failed permanently: best-effort `destroy` ran, sub `-> REFUND_DUE` (the refund itself is
    /// lnrent-7fp.11).
    RefundDue,
    /// The sub was not in PROVISIONING (already activated / handled by a racer) — an idempotent
    /// no-op that writes no second `instance`/`outbox` row.
    Skipped,
}

/// Drives a PROVISIONING subscription to ACTIVE (or REFUND_DUE). Holds the injected seams; the
/// daemon wiring (lnrent-7fp.21) constructs it and calls [`Provisioner::drive`] /
/// [`Provisioner::recover`].
pub struct Provisioner {
    store: Store,
    clock: Arc<dyn Clock>,
    /// The recipe this operator serves (M1a is single-recipe): the `provision`/`destroy` hooks and
    /// the service id recorded as the instance `kind`.
    recipe: Recipe,
    /// The box this operator provisions onto (M1a is single-box). Recorded on the `instance` row.
    box_id: String,
    /// Optional GATE-1 alert sink (lnrent-urw.2): a provision-failure cleanup backlog stuck past a
    /// threshold fires a `TeardownFailed` operator DM. `None` in focused tests.
    alerts: Option<Arc<AlertDispatcher>>,
}

/// The subscription fields the provision step reads up front (before the hook), plus the SETTLED
/// order invoice's `settled_at` (set by capture, lnrent-7fp.8) the timers are computed from.
struct ProvTarget {
    state: String,
    buyer_hex: String,
    recipe_id: Option<String>,
    params_json: Option<String>,
    refund_dest: Option<String>,
    period_s: i64,
    renew_lead_s: i64,
    /// `settled_at` of the PAID order invoice, or `None` if there is no settled order invoice yet.
    settled_at: Option<i64>,
    order_external_id: Option<String>,
    order_amount_sat: Option<i64>,
}

impl Provisioner {
    pub fn new(store: Store, clock: Arc<dyn Clock>, recipe: Recipe, box_id: String) -> Self {
        Self {
            store,
            clock,
            recipe,
            box_id,
            alerts: None,
        }
    }

    /// Inject the GATE-1 alert sink (lnrent-urw.2) so a stuck provision-failure cleanup backlog
    /// surfaces a `TeardownFailed` operator DM.
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// `(count, oldest_at)` of OPEN provision-failure cleanups for this provisioner's recipe.
    pub async fn open_cleanups_summary(&self) -> Result<(i64, Option<i64>)> {
        open_cleanups_summary_for(&self.store, &self.recipe.service.id).await
    }

    /// Provision one subscription (step A). Idempotent: safe to re-run on restart because the
    /// hook is idempotent (§7.2) and the ACTIVE move is a compare-and-swap on `state='PROVISIONING'`
    /// — a second drive of an already-ACTIVE sub is a [`Provisioned::Skipped`] no-op.
    pub async fn drive(&self, subscription_id: &str) -> Result<Provisioned> {
        let Some(t) = self.load_target(subscription_id).await? else {
            return Ok(Provisioned::Skipped); // unknown sub
        };
        if t.state != "PROVISIONING" {
            // Already activated / refunded / handled by a racer — nothing to do.
            return Ok(Provisioned::Skipped);
        }
        if t.recipe_id.as_deref() != Some(self.recipe.service.id.as_str()) {
            tracing::warn!(
                sub = %subscription_id,
                row_recipe = t.recipe_id.as_deref().unwrap_or(""),
                provisioner_recipe = %self.recipe.service.id,
                "skipping PROVISIONING subscription for a different recipe"
            );
            return Ok(Provisioned::Skipped);
        }
        // Timers key on the SETTLED order invoice's settled_at (capture .8), NEVER the wall clock.
        let Some(settled_at) = t.settled_at else {
            bail!("provision: PROVISIONING sub `{subscription_id}` has no settled order invoice");
        };

        let instance_id = format!("inst:{subscription_id}");
        let params: Value = t
            .params_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);
        let input = json!({
            "subscription": {
                "id": subscription_id,
                "buyer_pubkey": t.buyer_hex,
                "recipe_id": t.recipe_id,
                "box_id": self.box_id,
                "params": params,
            },
            "instance": {
                "id": instance_id,
                "subscription_id": subscription_id,
                "box_id": self.box_id,
                "kind": self.recipe.service.id,
            },
            "params": params,
            "host": {
                "box_id": self.box_id,
                "backend": self.recipe.provisioning.backend,
                "isolation": self.recipe.provisioning.isolation,
                "tier": self.recipe.provisioning.tier,
                "os": self.recipe.os.supports,
                "resources": self.recipe.provisioning.resources,
            },
        });

        // Run provision with a simple bounded retry (idempotent hook). On PERMANENT failure run a
        // best-effort `destroy` to purge partial resources, then move the sub to REFUND_DUE — we
        // only SET REFUND_DUE here; executing the refund is lnrent-7fp.11.
        let hook_out = match self.run_provision(&input).await {
            Ok(out) => out,
            Err(e) => {
                tracing::error!(sub = %subscription_id, error = %e.error, "provision failed permanently; destroy + REFUND_DUE");
                return self
                    .fail_to_refund_due(subscription_id, &t, &e.destroy_input)
                    .await;
            }
        };

        let payload = delivery_payload(&hook_out.stdout_json)?;
        let handles = hook_out
            .stdout_json
            .get("handles")
            .cloned()
            .unwrap_or(Value::Null);
        let handles_json = if handles.is_null() {
            "{}".to_string()
        } else {
            handles.to_string()
        };
        // The hook SUCCEEDED, so any post-success cleanup (lost capacity hold / lost activation CAS)
        // must destroy the resources it just created — and a generic `destroy` hook needs the
        // returned `handles` (a container id, peer index, ...) to find them (codex P2). The plain
        // `input` above is right only for a FAILED hook, which produced no handles.
        let destroy_input = destroy_input_with_handles(&input, &handles);
        // The wire DM the buyer receives. A STABLE outbox id (below) makes redelivery a buyer-side
        // dedupe (provision.ready is naturally idempotent — identical payload on every resend).
        let payload_json = serde_json::to_string(&Msg::ProvisionReady(ProvisionReady {
            subscription_id: subscription_id.to_string(),
            payload,
        }))?;

        // Sample the clock AFTER the provision hook (the bounded retry can run up to
        // PROVISION_ATTEMPTS * DEFAULT_TIMEOUT), so the audit timestamps (updated_at, instance
        // created_at, journal `at`, outbox created_at) reflect commit time, not pre-hook time. The
        // money/timer fields below key on `settled_at`, never the wall clock.
        let now = self.clock.now();
        let paid_through = settled_at + t.period_s;
        let soft_date = paid_through - t.renew_lead_s;
        let write = ActiveWrite {
            subscription_id: subscription_id.to_string(),
            // The reservation key IS the order id, which IS the subscription id (order_intake .17).
            order_id: subscription_id.to_string(),
            instance_id,
            box_id: self.box_id.clone(),
            kind: self.recipe.service.id.clone(),
            handles_json,
            destroy_input_json: destroy_input.to_string(),
            // STABLE per sub, so a re-drive can never queue a second provision.ready.
            outbox_id: format!("outbox:provision:{subscription_id}"),
            recipient_hex: t.buyer_hex.clone(),
            payload_json,
            paid_through,
            soft_date,
            now,
        };
        let outcome = match self.store.transaction(move |tx| write.write(tx)).await {
            Ok(outcome) => outcome,
            Err(e) if e.downcast_ref::<CapacityHoldLost>().is_some() => {
                tracing::error!(sub = %subscription_id, error = %e, "provision succeeded but capacity hold is gone; destroy + REFUND_DUE");
                return self
                    .fail_to_refund_due(subscription_id, &t, &destroy_input)
                    .await;
            }
            Err(e) => return Err(e),
        };
        match outcome {
            ActiveWriteOutcome::Activated => Ok(Provisioned::Active),
            ActiveWriteOutcome::Lost(intent) => {
                self.finish_lost_hook_cleanup(subscription_id, &destroy_input, intent)
                    .await
            }
        }
    }

    /// Restart recovery (driven by lnrent-7fp.21 at startup): re-drive every subscription stuck in
    /// PROVISIONING — idempotent, since [`drive`] re-runs the hook and CAS-guards the ACTIVE move.
    /// Returns how many subs reached a terminal outcome (ACTIVE / REFUND_DUE), plus any recovered
    /// failed-provision cleanups.
    pub async fn recover(&self) -> Result<usize> {
        let ids = self.provisioning_ids().await?;
        let mut driven = 0;
        for id in ids {
            match self.drive(&id).await {
                Ok(Provisioned::Skipped) => {}
                Ok(_) => driven += 1,
                Err(e) => {
                    tracing::error!(sub = %id, error = %e, "re-drive of PROVISIONING sub failed")
                }
            }
        }
        driven += self.recover_failed_cleanups().await?;
        Ok(driven)
    }

    /// Re-drive failed-provision cleanups (best-effort `destroy`) that were durably claimed before a
    /// crash, so a partial resource left by a crashed or failed `destroy` is purged on restart. The
    /// capacity hold is left HELD for the refund executor (lnrent-7fp.11) — see
    /// [`Provisioner::finish_failed_cleanup`].
    pub async fn recover_failed_cleanups(&self) -> Result<usize> {
        let rows = self.pending_failed_cleanups().await?;
        let mut finished = 0;
        for row in rows {
            if self
                .complete_failed_cleanup(
                    row.cleanup_event_id,
                    &row.subscription_id,
                    &row.destroy_input,
                )
                .await?
            {
                finished += 1;
            }
        }
        // Surface a STUCK provision-cleanup backlog (lnrent-urw.2): whatever this pass couldn't
        // finish stays open; alert once the oldest has been owed past the threshold. Cooldown-
        // collapsed on a fixed subject; BEST-EFFORT — a summary-read error must not fail recovery.
        match self.open_cleanups_summary().await {
            Ok((open, Some(oldest_at))) => {
                let now = self.clock.now();
                if open > 0 && now - oldest_at >= CLEANUP_STUCK_ALERT_S {
                    self.alert_cleanup_backlog(open, now - oldest_at).await;
                }
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "provision cleanup backlog summary failed (non-fatal)")
            }
        }
        Ok(finished)
    }

    /// Best-effort `TeardownFailed` for a stuck provision-cleanup backlog. Fixed subject so repeats
    /// collapse to one DM per cooldown; never fails the caller.
    async fn alert_cleanup_backlog(&self, open: i64, oldest_age_s: i64) {
        let Some(alerts) = &self.alerts else { return };
        let detail = format!(
            "{open} provision-failure cleanup(s) still owed (idempotent destroy failing on retry); \
             oldest has been open {oldest_age_s}s — a resource may still be billing. See `lnrent teardowns`."
        );
        if let Err(e) = alerts
            .dispatch(Alert::new(
                AlertKind::TeardownFailed,
                "provision-cleanup",
                detail,
            ))
            .await
        {
            tracing::warn!(error = %format!("{e:#}"), "failed to enqueue provision-cleanup TeardownFailed alert");
        }
    }

    /// Run the `provision` hook up to [`PROVISION_ATTEMPTS`] times, returning the first success or
    /// the last error.
    async fn run_provision(
        &self,
        input: &Value,
    ) -> std::result::Result<HookOutput, ProvisionError> {
        let hook = self.recipe.hook("provision");
        let mut last_err = None;
        let mut destroy_input = input.clone();
        for attempt in 1..=PROVISION_ATTEMPTS {
            match run_hook(&hook, input, DEFAULT_TIMEOUT, &self.recipe.provisioning.env).await {
                Ok(out) => match delivery_payload(&out.stdout_json) {
                    Ok(_) => return Ok(out),
                    Err(e) => {
                        tracing::warn!(attempt, error = %e, "provision hook output is invalid");
                        if let Some(handles) = out.stdout_json.get("handles") {
                            destroy_input = destroy_input_with_handles(input, handles);
                        }
                        last_err = Some(e);
                    }
                },
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "provision hook attempt failed");
                    last_err = Some(e);
                }
            }
            if attempt < PROVISION_ATTEMPTS {
                tokio::time::sleep(PROVISION_RETRY_BACKOFF).await;
            }
        }
        Err(ProvisionError {
            error: last_err.unwrap_or_else(|| anyhow!("provision hook failed")),
            destroy_input,
        })
    }

    /// Best-effort `destroy` to purge partial resources after a permanent provision failure — a
    /// destroy failure is logged, NOT fatal (§7.2).
    async fn best_effort_destroy(&self, input: &Value) -> bool {
        match run_hook(
            &self.recipe.hook("destroy"),
            input,
            DEFAULT_TIMEOUT,
            &self.recipe.provisioning.env,
        )
        .await
        {
            Ok(_) => {
                tracing::info!("destroy purged partial resources after provision failure");
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "destroy hook failed after provision failure (non-fatal)");
                false
            }
        }
    }

    /// Permanent provision failure (or a successful hook whose capacity hold vanished): claim the
    /// sub `PROVISIONING -> REFUND_DUE` (CAS) FIRST, then run best-effort `destroy` only if THIS
    /// call won that claim.
    ///
    /// Ordering is the safety property. Destroying *before* the CAS (the naive order) leaves a
    /// window where a concurrent drive can commit `PROVISIONING -> ACTIVE` and record a live
    /// instance, which our `destroy` — keyed on the same sub id — then tears down before our CAS
    /// loses (Reviewer 1 P1 / Reviewer 2 P2). Mirroring the success path's
    /// [`handle_activation_cas_lost`], we never invoke `destroy` until we hold an exclusive claim:
    /// once REFUND_DUE is committed no racer can re-activate from PROVISIONING, so the resources are
    /// unambiguously ours to purge. If the CAS loses, a racer already owns the sub — leave it alone.
    async fn fail_to_refund_due(
        &self,
        subscription_id: &str,
        t: &ProvTarget,
        destroy_input: &Value,
    ) -> Result<Provisioned> {
        let refund = RefundDueWrite::new(subscription_id, t, destroy_input, self.clock.now())?;
        match self.mark_refund_due(refund).await? {
            // Lost the CAS: a concurrent drive already moved the sub off PROVISIONING. Our hook may
            // still have (re)created resources that no `instance` row tracks — the idempotent hook
            // (§7.2) can re-materialise them even after the winner's cleanup ran — so clean them up
            // crash-safely, UNLESS the racer activated (its instance is then the live owner).
            RefundDueWriteOutcome::Lost(intent) => {
                return self
                    .finish_lost_hook_cleanup(subscription_id, destroy_input, intent)
                    .await;
            }
            RefundDueWriteOutcome::Claimed(cleanup_event_id) => {
                // We hold the terminal REFUND_DUE claim and wrote a durable cleanup intent in that
                // same txn. Finish the best-effort cleanup; the capacity hold is left for the refund
                // executor (lnrent-7fp.11), as on capture's refund paths.
                self.complete_failed_cleanup(cleanup_event_id, subscription_id, destroy_input)
                    .await?;
                Ok(Provisioned::RefundDue)
            }
        }
    }

    async fn complete_failed_cleanup(
        &self,
        cleanup_event_id: i64,
        subscription_id: &str,
        destroy_input: &Value,
    ) -> Result<bool> {
        if !self.best_effort_destroy(destroy_input).await {
            return Ok(false);
        }
        self.finish_failed_cleanup(cleanup_event_id, subscription_id, self.clock.now())
            .await?;
        Ok(true)
    }

    /// A drive's hook produced resources but the drive then lost its terminal CAS (the
    /// `PROVISIONING -> ACTIVE` success CAS, or the `PROVISIONING -> REFUND_DUE` failure CAS) to a
    /// concurrent racer, so no `instance` row of ours tracks them. A tracked live instance means the
    /// racer already owns these same (idempotent §7.2) resources — leave them untouched, even if the
    /// subscription moved on from ACTIVE to SUSPENDED before we observed it. Any untracked outcome
    /// means the resources are ours to purge.
    ///
    /// A failed drive that loses the REFUND_DUE CAS must STILL purge the output its idempotent hook
    /// may have re-materialised after the winner's destroy already ran, or it leaks. The cleanup is
    /// crash-safe: a durable CLEANUP_PENDING intent is recorded BEFORE `destroy`, so a crash
    /// mid-`destroy` is re-driven by [`recover_failed_cleanups`] on restart.
    #[cfg(test)]
    async fn cleanup_lost_hook_output(
        &self,
        subscription_id: &str,
        destroy_input: &Value,
    ) -> Result<Provisioned> {
        let intent = self
            .claim_cleanup_if_unowned(subscription_id, destroy_input)
            .await?;
        self.finish_lost_hook_cleanup(subscription_id, destroy_input, intent)
            .await
    }

    async fn finish_lost_hook_cleanup(
        &self,
        subscription_id: &str,
        destroy_input: &Value,
        intent: CleanupIntent,
    ) -> Result<Provisioned> {
        match intent {
            CleanupIntent::LiveOwner => {
                // The racer recorded a live instance (ACTIVE or already SUSPENDED): leave it live.
                Ok(Provisioned::Skipped)
            }
            CleanupIntent::Pending {
                cleanup_event_id,
                owner_state,
            } => {
                self.complete_failed_cleanup(cleanup_event_id, subscription_id, destroy_input)
                    .await?;
                Ok(match owner_state.as_deref() {
                    Some("REFUND_DUE") | None => Provisioned::RefundDue,
                    Some(other) => {
                        tracing::warn!(sub = %subscription_id, state = other, "provision CAS lost to unexpected state; destroyed untracked hook output");
                        Provisioned::Skipped
                    }
                })
            }
        }
    }

    #[cfg(test)]
    async fn claim_cleanup_if_unowned(
        &self,
        subscription_id: &str,
        destroy_input: &Value,
    ) -> Result<CleanupIntent> {
        let sub_id = subscription_id.to_string();
        let detail = destroy_input.to_string();
        let now = self.clock.now();
        self.store
            .transaction(move |tx| claim_cleanup_if_unowned_txn(tx, &sub_id, &detail, now))
            .await
    }

    /// Durably record a recoverable cleanup intent (CLEANUP_PENDING) for a lost-CAS drive whose
    /// hook output no `instance` row tracks, so a crash before/during `destroy` is re-driven by
    /// [`recover_failed_cleanups`] on restart (the REFUND_DUE winner instead records this intent
    /// atomically inside its state-transition txn, see [`RefundDueWrite::write`]).
    #[cfg(test)]
    async fn record_cleanup_intent(
        &self,
        subscription_id: &str,
        destroy_input: &Value,
    ) -> Result<i64> {
        let sub_id = subscription_id.to_string();
        let detail = destroy_input.to_string();
        let now = self.clock.now();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
                        params![sub_id, CLEANUP_PENDING, detail, now],
                )?;
                Ok(tx.last_insert_rowid())
            })
            .await
    }

    /// Move the sub `PROVISIONING -> REFUND_DUE` (CAS) + durable refund intent + cleanup intent +
    /// journal, all in ONE txn. The refund executor is lnrent-7fp.11; we only record the
    /// intent/state. Returns whether THIS call won the CAS or, on a lost CAS, the cleanup intent
    /// already recorded for this drive's untracked hook output.
    async fn mark_refund_due(&self, refund: RefundDueWrite) -> Result<RefundDueWriteOutcome> {
        self.store.transaction(move |tx| refund.write(tx)).await
    }

    /// Record CLEANUP_DONE for a completed best-effort `destroy`, idempotently (only for a
    /// CLEANUP_PENDING that is not already done). Capacity is deliberately NOT released here: a
    /// failed provision leaves the sub REFUND_DUE with its hold still HELD, exactly as capture
    /// (lnrent-7fp.8) leaves the hold on its own refund paths — the refund executor (lnrent-7fp.11)
    /// releases it when it settles the refund. Releasing here would be a premature reuse hazard: a
    /// winner finishing its cleanup cannot see a slower concurrent drive whose own CLEANUP_PENDING
    /// is not yet written, so it could hand the slot to a new order while that drive's idempotent
    /// hook (§7.2) is still materialising resources (Reviewer 1 P2). The held hold still counts as
    /// live usage (its order invoice is PAID), so the slot is never double-allocated meanwhile.
    async fn finish_failed_cleanup(
        &self,
        cleanup_event_id: i64,
        subscription_id: &str,
        now: i64,
    ) -> Result<()> {
        let sub_id = subscription_id.to_string();
        let done_detail = cleanup_done_detail(cleanup_event_id);
        self.store
            .transaction(move |tx| {
                let pending_id: Option<i64> = tx
                    .query_row(
                        "SELECT p.id
                           FROM event_log p
                          WHERE p.id=?1 AND p.subscription_id=?2 AND p.kind=?3
                            AND NOT EXISTS (
                              SELECT 1 FROM event_log d
                               WHERE d.subscription_id=p.subscription_id
                                 AND d.kind=?4
                                 AND d.detail_json=?5
                            )
                          LIMIT 1",
                        params![
                            cleanup_event_id,
                            sub_id,
                            CLEANUP_PENDING,
                            CLEANUP_DONE,
                            done_detail,
                        ],
                        |r| r.get(0),
                    )
                    .optional()?;
                if pending_id.is_none() {
                    return Ok(());
                }
                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![sub_id, CLEANUP_DONE, done_detail, now],
                )?;
                Ok(())
            })
            .await
    }

    async fn load_target(&self, sub_id: &str) -> Result<Option<ProvTarget>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                // One LEFT JOIN to the PAID order invoice (not three separate correlated subqueries)
                // so settled_at / external_id / amount_sat are guaranteed to come from the SAME row
                // even if a sub ever has >1 PAID order invoice (Reviewer 2 P2). ORDER BY ... LIMIT 1
                // picks one deterministically; under the one-order-invoice invariant there is only
                // ever one anyway.
                Ok(c.query_row(
                    "SELECT s.state, s.buyer_pubkey, s.recipe_id, s.params_json, s.refund_dest,
                            s.period_s, s.renew_lead_s,
                            oi.settled_at, oi.external_id,
                            COALESCE(oi.received_msat / 1000, oi.amount_sat)
                     FROM subscription s
                     LEFT JOIN invoice oi
                       ON oi.subscription_id = s.id AND oi.kind='order' AND oi.status='PAID'
                     WHERE s.id = ?1
                     ORDER BY oi.settled_at DESC
                     LIMIT 1",
                    params![id],
                    |r| {
                        Ok(ProvTarget {
                            state: r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            buyer_hex: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            recipe_id: r.get(2)?,
                            params_json: r.get(3)?,
                            refund_dest: r.get(4)?,
                            period_s: r.get::<_, Option<i64>>(5)?.unwrap_or(0),
                            renew_lead_s: r.get::<_, Option<i64>>(6)?.unwrap_or(0),
                            settled_at: r.get(7)?,
                            order_external_id: r.get(8)?,
                            order_amount_sat: r.get(9)?,
                        })
                    },
                )
                .optional()?)
            })
            .await
    }

    async fn provisioning_ids(&self) -> Result<Vec<String>> {
        let recipe_id = self.recipe.service.id.clone();
        self.store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id FROM subscription WHERE state='PROVISIONING' AND recipe_id=?1",
                )?;
                let ids = stmt
                    .query_map(params![recipe_id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(ids)
            })
            .await
    }

    async fn pending_failed_cleanups(&self) -> Result<Vec<CleanupRow>> {
        let recipe_id = self.recipe.service.id.clone();
        self.store
            .read(move |c| {
                // Only THIS provisioner's recipe (joined to the subscription, mirroring
                // `provisioning_ids`). A cleanup's `destroy` hook is recipe-specific, so in a
                // multi-recipe store one recipe's provisioner must never run its destroy against
                // another recipe's cleanup input and then mark it done — that purges nothing real and
                // robs the owning recipe of ever cleaning up its own resources (Reviewer 1 P2).
                let mut stmt = c.prepare(
                    "SELECT p.id, p.subscription_id, p.detail_json
                       FROM event_log p
                       JOIN subscription s ON s.id=p.subscription_id
                      WHERE p.kind=?1
                        AND s.recipe_id=?3
                        AND NOT EXISTS (
                          SELECT 1 FROM event_log d
                           WHERE d.subscription_id=p.subscription_id
                             AND d.kind=?2
                             AND d.detail_json=('{\"pending_event_id\":' || p.id || '}')
                        )
                      ORDER BY p.id",
                )?;
                let rows = stmt
                    .query_map(params![CLEANUP_PENDING, CLEANUP_DONE, recipe_id], |r| {
                        let cleanup_event_id: i64 = r.get(0)?;
                        let subscription_id: Option<String> = r.get(1)?;
                        let detail_json: Option<String> = r.get(2)?;
                        let destroy_input = detail_json
                            .as_deref()
                            .and_then(|s| serde_json::from_str(s).ok())
                            .unwrap_or(Value::Null);
                        Ok(CleanupRow {
                            cleanup_event_id,
                            subscription_id: subscription_id.unwrap_or_default(),
                            destroy_input,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }
}

/// `(count, oldest_at)` of OPEN provision-failure cleanups for `recipe_id` — a
/// `provision_cleanup_pending` journal row with no matching `_done`. Shared by the provisioner's
/// stuck-backlog alert and the IPC `teardowns`/`status` fold (lnrent-urw.2).
pub async fn open_cleanups_summary_for(
    store: &Store,
    recipe_id: &str,
) -> Result<(i64, Option<i64>)> {
    let recipe_id = recipe_id.to_string();
    store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT count(*), MIN(p.at)
                   FROM event_log p
                   JOIN subscription s ON s.id=p.subscription_id
                  WHERE p.kind=?1
                    AND s.recipe_id=?3
                    AND NOT EXISTS (
                      SELECT 1 FROM event_log d
                       WHERE d.subscription_id=p.subscription_id
                         AND d.kind=?2
                         AND d.detail_json=('{\"pending_event_id\":' || p.id || '}')
                    )",
                params![CLEANUP_PENDING, CLEANUP_DONE, recipe_id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?)),
            )?)
        })
        .await
}

fn delivery_payload(stdout_json: &Value) -> Result<Value> {
    match stdout_json.get("payload") {
        Some(payload) if !payload.is_null() => Ok(payload.clone()),
        _ => bail!("provision hook output must include a non-null `payload`"),
    }
}

struct ProvisionError {
    error: anyhow::Error,
    destroy_input: Value,
}

struct CleanupRow {
    cleanup_event_id: i64,
    subscription_id: String,
    destroy_input: Value,
}

struct CleanupOwner {
    state: Option<String>,
    has_live_instance: bool,
}

enum CleanupIntent {
    LiveOwner,
    Pending {
        cleanup_event_id: i64,
        owner_state: Option<String>,
    },
}

enum ActiveWriteOutcome {
    Activated,
    Lost(CleanupIntent),
}

enum RefundDueWriteOutcome {
    Claimed(i64),
    Lost(CleanupIntent),
}

fn cleanup_done_detail(pending_event_id: i64) -> String {
    json!({"pending_event_id": pending_event_id}).to_string()
}

fn cleanup_owner_txn(tx: &rusqlite::Transaction, sub_id: &str) -> Result<CleanupOwner> {
    let row: Option<(Option<String>, bool)> = tx
        .query_row(
            "SELECT s.state,
                    EXISTS (
                      SELECT 1 FROM instance i
                       WHERE i.id=s.instance_id
                         AND i.subscription_id=s.id
                         AND i.state IN ('CREATING','RUNNING','STOPPED')
                    )
               FROM subscription s
              WHERE s.id=?1",
            params![sub_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(match row {
        Some((state, has_live_instance)) => CleanupOwner {
            state,
            has_live_instance,
        },
        None => CleanupOwner {
            state: None,
            has_live_instance: false,
        },
    })
}

fn claim_cleanup_if_unowned_txn(
    tx: &rusqlite::Transaction,
    sub_id: &str,
    destroy_input_json: &str,
    now: i64,
) -> Result<CleanupIntent> {
    let owner = cleanup_owner_txn(tx, sub_id)?;
    if owner.has_live_instance {
        return Ok(CleanupIntent::LiveOwner);
    }
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
        params![sub_id, CLEANUP_PENDING, destroy_input_json, now],
    )?;
    Ok(CleanupIntent::Pending {
        cleanup_event_id: tx.last_insert_rowid(),
        owner_state: owner.state,
    })
}

/// The `destroy` input for cleaning up a SUCCESSFUL provision whose activation failed: the original
/// provision `input` plus the backend `handles` the hook returned (mirrored under both top-level
/// `handles` and `instance.handles`, where the durable instance row carries them), so a generic
/// `destroy` hook can find the resources just created (codex P2).
fn destroy_input_with_handles(input: &Value, handles: &Value) -> Value {
    let mut di = input.clone();
    if let Some(obj) = di.as_object_mut() {
        obj.insert("handles".to_string(), handles.clone());
        if let Some(inst) = obj.get_mut("instance").and_then(Value::as_object_mut) {
            inst.insert("handles".to_string(), handles.clone());
        }
    }
    di
}

#[derive(Debug)]
struct CapacityHoldLost(anyhow::Error);

impl fmt::Display for CapacityHoldLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "capacity hold lost during provision activation: {}",
            self.0
        )
    }
}

impl StdError for CapacityHoldLost {}

/// Owned inputs for the atomic ACTIVE write, so the transaction closure is `move + 'static`
/// (the order_intake.rs `OrderWrite` pattern).
struct ActiveWrite {
    subscription_id: String,
    order_id: String,
    instance_id: String,
    box_id: String,
    kind: String,
    handles_json: String,
    destroy_input_json: String,
    outbox_id: String,
    recipient_hex: String,
    payload_json: String,
    paid_through: i64,
    soft_date: i64,
    now: i64,
}

impl ActiveWrite {
    /// PROVISIONING -> ACTIVE + reservation CONSUMED + `instance` + PENDING `outbox` in one txn.
    /// Returns `Activated` if THIS call activated the sub; on a lost CAS, records any needed cleanup
    /// intent in the same transaction before returning the result (no duplicate instance/outbox).
    fn write(self, tx: &rusqlite::Transaction) -> Result<ActiveWriteOutcome> {
        // CAS on state='PROVISIONING' is the idempotency gate: at most one drive activates the sub.
        let n = tx.execute(
            "UPDATE subscription
                SET state='ACTIVE', paid_through=?2, soft_date=?3, next_deadline=?3,
                    instance_id=?4, updated_at=?5
              WHERE id=?1 AND state='PROVISIONING'",
            params![
                self.subscription_id,
                self.paid_through,
                self.soft_date,
                self.instance_id,
                self.now,
            ],
        )?;
        if n == 0 {
            return Ok(ActiveWriteOutcome::Lost(claim_cleanup_if_unowned_txn(
                tx,
                &self.subscription_id,
                &self.destroy_input_json,
                self.now,
            )?));
        }
        // Same txn: consume the live HELD reservation, so an ACTIVE sub always atomically holds its
        // capacity (lnrent-7fp.7). Only a genuine `NoHeldReservation` (a concurrent refund/terminate
        // RELEASED the hold) is a capacity loss: the txn rolls back and the caller destroys the hook
        // output before handing the sub to refunds. A transient DB error is NOT a capacity loss —
        // propagate it so the drive aborts and retries, never refunding a provisioned buyer on a
        // blip (Reviewer 2 P3).
        reservation::consume_txn(tx, &self.order_id, self.now).map_err(|e| {
            if e.downcast_ref::<reservation::NoHeldReservation>().is_some() {
                anyhow::Error::new(CapacityHoldLost(e))
            } else {
                e
            }
        })?;
        tx.execute(
            "INSERT INTO instance
                (id, subscription_id, box_id, kind, handles_json, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'RUNNING', ?6, ?6)",
            params![
                self.instance_id,
                self.subscription_id,
                self.box_id,
                self.kind,
                self.handles_json,
                self.now,
            ],
        )?;
        // The provision.ready DM, durable as a PENDING outbox row — published only by the outbox
        // sender AFTER this commit (so a crash between ACTIVE and send just leaves it PENDING).
        tx.execute(
            "INSERT INTO outbox
                (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
             VALUES (?1, ?2, ?3, 'provision.ready', ?4, 'PENDING', 0, ?5)",
            params![
                self.outbox_id,
                self.recipient_hex,
                self.subscription_id,
                self.payload_json,
                self.now,
            ],
        )?;
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, 'provision_active', '{}', ?2)",
            params![self.subscription_id, self.now],
        )?;
        Ok(ActiveWriteOutcome::Activated)
    }
}

/// Owned inputs for the atomic REFUND_DUE write. The refund row is durable work for lnrent-7fp.11;
/// this module does not execute it.
struct RefundDueWrite {
    subscription_id: String,
    dest: Option<String>,
    /// `None` when the PAID order invoice has no `amount_sat` (the column is nullable). Recorded as a
    /// NULL refund amount rather than failing the drive — see [`RefundDueWrite::new`].
    amount_sat: Option<i64>,
    refund_id: String,
    idempotency_key: String,
    destroy_input_json: String,
    now: i64,
}

impl RefundDueWrite {
    fn new(subscription_id: &str, t: &ProvTarget, destroy_input: &Value, now: i64) -> Result<Self> {
        let external_id = t.order_external_id.as_deref().ok_or_else(|| {
            anyhow!("provision: paid order invoice for `{subscription_id}` has no external_id")
        })?;
        // `amount_sat` is nullable in the schema. A missing amount must NOT abort the drive and wedge
        // the sub in PROVISIONING — the supervisor would then re-run the (failed) provision + destroy
        // hooks forever (Reviewer 2 P3). Record REFUND_DUE with a NULL amount and let the refund
        // executor (lnrent-7fp.11) resolve the figure.
        if t.order_amount_sat.is_none() {
            tracing::warn!(sub = %subscription_id, "paid order invoice has no amount_sat; recording REFUND_DUE with a NULL refund amount");
        }
        Ok(Self {
            subscription_id: subscription_id.to_string(),
            dest: t.refund_dest.clone(),
            amount_sat: t.order_amount_sat,
            refund_id: format!("ref-{external_id}"),
            idempotency_key: format!("refund:{external_id}"),
            destroy_input_json: destroy_input.to_string(),
            now,
        })
    }

    /// Returns the cleanup intent id when THIS call won the PROVISIONING -> REFUND_DUE CAS. If the
    /// CAS loses, a cleanup intent for this drive's untracked hook output is recorded in the same
    /// transaction unless a racer already owns a live instance.
    fn write(self, tx: &rusqlite::Transaction) -> Result<RefundDueWriteOutcome> {
        let n = tx.execute(
            // Clear the deadline cursor on the REFUND_DUE transition (lnrent-y4m.4): the refund is
            // driven by the Refunder's `refund_attempt`-ledger scan, not by `subscription.next_deadline`,
            // and reconcile has no REFUND_DUE arm (falls to `noops`) — so a stale cursor only re-selects
            // the row each tick to no-op. CAS guard (`state='PROVISIONING'`) unchanged.
            "UPDATE subscription SET state='REFUND_DUE', next_deadline=NULL, updated_at=?2 WHERE id=?1 AND state='PROVISIONING'",
            params![self.subscription_id, self.now],
        )?;
        if n == 0 {
            return Ok(RefundDueWriteOutcome::Lost(claim_cleanup_if_unowned_txn(
                tx,
                &self.subscription_id,
                &self.destroy_input_json,
                self.now,
            )?));
        }
        tx.execute(
            "INSERT INTO refund_attempt
                (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6, ?6)
             ON CONFLICT(idempotency_key) DO NOTHING",
            params![
                self.refund_id,
                self.subscription_id,
                self.dest,
                self.amount_sat,
                self.idempotency_key,
                self.now,
            ],
        )?;
        // Crash-safety: write a recoverable cleanup intent for the best-effort `destroy`. The caller
        // runs `destroy`, then `finish_failed_cleanup` records CLEANUP_DONE; if the daemon dies in
        // between, startup recovery re-runs the idempotent destroy from this row. The capacity hold
        // is left HELD for the refund executor (lnrent-7fp.11).
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                self.subscription_id,
                CLEANUP_PENDING,
                self.destroy_input_json,
                self.now,
            ],
        )?;
        let cleanup_event_id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, 'provision_failed', '{}', ?2)",
            params![self.subscription_id, self.now],
        )?;
        Ok(RefundDueWriteOutcome::Claimed(cleanup_event_id))
    }
}

/// Durable, at-least-once delivery of queued operator→buyer DMs (step B). A one-shot drain over the
/// PENDING `outbox` rows; the daemon supervisor (lnrent-7fp.21) calls it at startup and on an
/// interval (that interval IS the retry backoff). Restart recovery for unsent DMs is just a drain.
pub struct OutboxSender {
    store: Store,
    clock: Arc<dyn Clock>,
}

/// Order-handler adapter that wires `delivery.resend.request` to the durable provision outbox while
/// leaving `order.request` / `renew.request` with the existing order-intake handler.
pub struct DeliveryResendOrderHandler {
    inner: Arc<dyn OrderHandler>,
    outbox: OutboxSender,
}

impl DeliveryResendOrderHandler {
    pub fn new(inner: Arc<dyn OrderHandler>, outbox: OutboxSender) -> Self {
        Self { inner, outbox }
    }
}

#[async_trait]
impl OrderHandler for DeliveryResendOrderHandler {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        match msg {
            Msg::DeliveryResendRequest(req) => {
                self.outbox
                    .resend_provision_ready(&sender, &req.subscription_id, out)
                    .await?;
                Ok(())
            }
            other => self.inner.handle(sender, other, out).await,
        }
    }
}

/// One durable outbox row selected for publish or resend.
struct OutboxRow {
    id: String,
    recipient_hex: String,
    payload_json: String,
}

impl OutboxSender {
    pub fn new(store: Store, clock: Arc<dyn Clock>) -> Self {
        Self { store, clock }
    }

    /// Publish every PENDING outbox row via the injected [`Outbound`] seam, then mark each SENT
    /// (`state='SENT'`, `sent_at`, `attempts++`). A *transient* publish failure leaves the row
    /// PENDING for the next drain (at-least-once). A *structurally undeliverable* row — an
    /// unparseable recipient or payload, a deterministic failure that can NEVER succeed — is moved
    /// to a terminal `FAILED` state instead, so it is not re-selected and re-logged on every drain
    /// forever (codex P2). Returns the number of rows published this pass.
    pub async fn drain_once(&self, out: &dyn Outbound) -> Result<usize> {
        let rows = self.pending_rows().await?;
        let now = self.clock.now();
        let mut sent = 0;
        for row in rows {
            let recipient = match PublicKey::from_hex(&row.recipient_hex) {
                Ok(pk) => pk,
                Err(e) => {
                    // Deterministic: the stored hex will never parse — quarantine, don't re-select.
                    tracing::error!(outbox = %row.id, error = %e, "outbox row has an unparseable recipient; marking FAILED");
                    self.mark_failed(&row.id).await?;
                    continue;
                }
            };
            let msg: Msg = match serde_json::from_str(&row.payload_json) {
                Ok(msg) => msg,
                Err(e) => {
                    // Deterministic: the stored payload will never deserialize — quarantine it.
                    tracing::error!(outbox = %row.id, error = %e, "outbox row payload is not a Msg; marking FAILED");
                    self.mark_failed(&row.id).await?;
                    continue;
                }
            };
            match out.reply(&recipient, &msg).await {
                Ok(_) => {
                    self.mark_sent(&row.id, now).await?;
                    sent += 1;
                }
                // Transient: leave the row PENDING for the next drain (the supervisor interval is
                // the backoff), but record the attempt so `attempts` reflects the real retry count
                // rather than only ever counting the terminal SENT/FAILED transition (Reviewer 2 P3).
                Err(e) => {
                    let attempts = self.bump_attempt(&row.id).await?;
                    tracing::warn!(outbox = %row.id, attempts, error = %e, "outbox publish failed; will retry")
                }
            }
        }
        Ok(sent)
    }

    /// On-demand delivery resync for `delivery.resend.request`: re-publish the latest durable
    /// `provision.ready` row for this subscription, even if the background drain already marked it
    /// SENT. Returns `false` when there is no row for that subscription, or when the requester is
    /// not the recorded buyer.
    pub async fn resend_provision_ready(
        &self,
        requester: &PublicKey,
        subscription_id: &str,
        out: &dyn Outbound,
    ) -> Result<bool> {
        let Some(row) = self.latest_provision_ready(subscription_id).await? else {
            return Ok(false);
        };
        if row.recipient_hex != requester.to_hex() {
            tracing::warn!(
                sub = %subscription_id,
                requester = %requester.to_hex(),
                "ignoring delivery resend request from non-buyer"
            );
            return Ok(false);
        }
        let recipient = PublicKey::from_hex(&row.recipient_hex)?;
        let msg: Msg = serde_json::from_str(&row.payload_json)?;
        out.reply(&recipient, &msg).await?;
        self.mark_delivered(&row.id, self.clock.now()).await?;
        Ok(true)
    }

    async fn pending_rows(&self) -> Result<Vec<OutboxRow>> {
        self.store
            .read(|c| {
                let mut stmt = c.prepare(
                    "SELECT id, recipient, payload_json FROM outbox WHERE state='PENDING' ORDER BY created_at",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(OutboxRow {
                            id: r.get(0)?,
                            recipient_hex: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            payload_json: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }

    async fn latest_provision_ready(&self, subscription_id: &str) -> Result<Option<OutboxRow>> {
        let sub_id = subscription_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT id, recipient, payload_json
                       FROM outbox
                      WHERE subscription_id=?1
                        AND msg_type='provision.ready'
                        AND state IN ('PENDING','SENT')
                      ORDER BY created_at DESC
                      LIMIT 1",
                    params![sub_id],
                    |r| {
                        Ok(OutboxRow {
                            id: r.get(0)?,
                            recipient_hex: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            payload_json: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        })
                    },
                )
                .optional()?)
            })
            .await
    }

    /// Mark a row SENT. The `AND state='PENDING'` guard makes it idempotent: a SENT row is never
    /// re-marked (and the drain never re-publishes it).
    async fn mark_sent(&self, id: &str, now: i64) -> Result<()> {
        let id = id.to_string();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE outbox SET state='SENT', sent_at=?2, attempts=attempts+1 WHERE id=?1 AND state='PENDING'",
                    params![id, now],
                )?;
                Ok(())
            })
            .await
    }

    async fn mark_delivered(&self, id: &str, now: i64) -> Result<()> {
        let id = id.to_string();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE outbox
                        SET state='SENT', sent_at=?2, attempts=attempts+1
                      WHERE id=?1 AND state IN ('PENDING','SENT')",
                    params![id, now],
                )?;
                Ok(())
            })
            .await
    }

    /// Record a failed delivery attempt on a still-PENDING row (it stays PENDING for the next
    /// drain), returning the new attempt count so a retry can be logged with its count. Guarded on
    /// `state='PENDING'` so it never bumps a row a concurrent drain already finalised (Reviewer 2
    /// P3: keep `attempts` honest even on the transient-retry path).
    async fn bump_attempt(&self, id: &str) -> Result<i64> {
        let id = id.to_string();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE outbox SET attempts=attempts+1 WHERE id=?1 AND state='PENDING'",
                    params![id],
                )?;
                let attempts: i64 = tx
                    .query_row(
                        "SELECT attempts FROM outbox WHERE id=?1",
                        params![id],
                        |r| r.get(0),
                    )
                    .optional()?
                    .unwrap_or(0);
                Ok(attempts)
            })
            .await
    }

    /// Quarantine a structurally-undeliverable row: PENDING -> FAILED (terminal). Guarded on
    /// `state='PENDING'` so it never overwrites a SENT row, and so the row is no longer re-selected
    /// by [`pending_rows`] (no unbounded re-log). `attempts++` records that it was processed once.
    async fn mark_failed(&self, id: &str) -> Result<()> {
        let id = id.to_string();
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE outbox SET state='FAILED', attempts=attempts+1 WHERE id=?1 AND state='PENDING'",
                    params![id],
                )?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::store::{Store, SCHEMA};
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use lnrent_wire::{DeliveryResendRequest, Keys, SubCancel};
    use nostr::EventId;
    use rusqlite::Connection;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    fn dummy_recipe() -> Recipe {
        Recipe::load(format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR")))
            .expect("dummy recipe")
    }

    /// A fresh, EMPTY temp dir for a marker-based hook recipe. Wiping any prior contents first means
    /// a stale `destroyed` marker from a same-PID earlier run (containers/CI reuse PIDs) can never
    /// make a marker assertion pass or fail spuriously (Reviewer 1 P3).
    fn fresh_recipe_dir(name: &str, seq: u64) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{name}-{}-{seq}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A dummy recipe whose `provision` hook always fails (and whose `destroy` records that it ran).
    /// Returns the recipe and the marker path the destroy hook touches.
    fn failing_provision_recipe() -> (Recipe, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov", seq);
        let marker = dir.join("destroyed");
        std::fs::write(
            dir.join("provision"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo boom >&2; exit 1\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                marker.display()
            ),
        )
        .unwrap();
        for h in ["provision", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe(); // keep service.id = "dummy"; only the hook dir changes
        r.dir = dir;
        (r, marker)
    }

    /// A recipe whose provision and destroy hooks both fail; used to prove cleanup remains pending
    /// and capacity remains held until a future recovery can actually purge the partial resource.
    fn failing_destroy_recipe() -> (Recipe, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov-destroy-fails", seq);
        let marker = dir.join("destroy-attempted");
        std::fs::write(
            dir.join("provision"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo boom >&2; exit 1\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo destroy failed >&2; exit 1\n",
                marker.display()
            ),
        )
        .unwrap();
        for h in ["provision", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe();
        r.dir = dir;
        (r, marker)
    }

    /// A recipe whose provision hook exits 0 but forgets the delivery payload.
    fn missing_payload_recipe() -> (Recipe, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov-no-payload", seq);
        let marker = dir.join("destroyed");
        std::fs::write(
            dir.join("provision"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo '{\"handles\":{\"instance\":\"partial\"}}'\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\nset -euo pipefail\ninput=$(cat)\n[[ \"$input\" == *'partial'* ]] || {{ echo \"destroy missing handles: $input\" >&2; exit 1; }}\ntouch '{}'; echo '{{\"ok\":true}}'\n",
                marker.display()
            ),
        )
        .unwrap();
        for h in ["provision", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe();
        r.dir = dir;
        (r, marker)
    }

    /// A recipe whose provision hook succeeds and whose destroy hook records that cleanup ran.
    fn successful_provision_with_destroy_marker_recipe() -> (Recipe, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov-success-marker", seq);
        let marker = dir.join("destroyed");
        std::fs::write(
            dir.join("provision"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo '{\"payload\":{\"credential\":\"ok\"},\"handles\":{\"instance\":\"partial\"}}'\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\ncat >/dev/null; touch '{}'; echo '{{\"ok\":true}}'\n",
                marker.display()
            ),
        )
        .unwrap();
        for h in ["provision", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe();
        r.dir = dir;
        (r, marker)
    }

    /// A recipe whose provision hook succeeds (returning a distinctive `handles` value) and whose
    /// destroy hook FAILS unless it receives those handles on stdin — so a passing `destroy` proves
    /// the post-success cleanup forwarded the hook's handles (codex P2).
    fn handle_checking_destroy_recipe() -> (Recipe, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov-handle-destroy", seq);
        let marker = dir.join("destroyed");
        std::fs::write(
            dir.join("provision"),
            "#!/usr/bin/env bash\ncat >/dev/null; echo '{\"payload\":{\"credential\":\"ok\"},\"handles\":{\"instance\":\"handle-xyz\"}}'\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("destroy"),
            format!(
                "#!/usr/bin/env bash\nset -euo pipefail\ninput=$(cat)\n[[ \"$input\" == *'handle-xyz'* ]] || {{ echo \"destroy missing handles: $input\" >&2; exit 1; }}\ntouch '{}'; echo '{{\"ok\":true}}'\n",
                marker.display()
            ),
        )
        .unwrap();
        for h in ["provision", "destroy"] {
            std::fs::set_permissions(dir.join(h), std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mut r = dummy_recipe();
        r.dir = dir;
        (r, marker)
    }

    fn input_checking_recipe() -> Recipe {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = fresh_recipe_dir("lnrent-prov-input", seq);
        std::fs::write(
            dir.join("provision"),
            r#"#!/usr/bin/env bash
set -euo pipefail
input=$(cat)
[[ "$input" == *'"instance"'* ]] || { echo "missing instance" >&2; exit 1; }
[[ "$input" == *'"id":"inst:sub-1"'* ]] || { echo "missing instance id: $input" >&2; exit 1; }
[[ "$input" == *'"subscription_id":"sub-1"'* ]] || { echo "missing subscription id: $input" >&2; exit 1; }
[[ "$input" == *'"host"'* ]] || { echo "missing host facts" >&2; exit 1; }
[[ "$input" == *'"backend":"host"'* ]] || { echo "missing backend: $input" >&2; exit 1; }
[[ "$input" == *'"os":["nixos","debian"]'* ]] || { echo "missing os facts: $input" >&2; exit 1; }
echo '{"payload":{"credential":"checked"},"handles":{"instance":"checked"}}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(
            dir.join("provision"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let mut r = dummy_recipe();
        r.dir = dir;
        r
    }

    /// A stub [`Outbound`] that records every `(recipient, msg)` instead of touching a relay
    /// (the order_intake.rs / lnrent-7fp.5 test harness).
    #[derive(Default)]
    struct RecordingOutbound {
        sent: Mutex<Vec<(PublicKey, Msg)>>,
    }
    #[async_trait]
    impl Outbound for RecordingOutbound {
        async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
            self.sent.lock().unwrap().push((*recipient, msg.clone()));
            Ok(EventId::all_zeros())
        }
    }
    impl RecordingOutbound {
        fn messages(&self) -> Vec<(PublicKey, Msg)> {
            self.sent.lock().unwrap().clone()
        }
        fn only(&self) -> (PublicKey, Msg) {
            let mut m = self.messages();
            assert_eq!(m.len(), 1, "expected exactly one sent message, got {m:?}");
            m.pop().unwrap()
        }
    }

    #[derive(Default)]
    struct CountingOrderHandler {
        calls: Mutex<usize>,
    }
    #[async_trait]
    impl OrderHandler for CountingOrderHandler {
        async fn handle(&self, _sender: PublicKey, _msg: Msg, _out: &dyn Outbound) -> Result<()> {
            *self.calls.lock().unwrap() += 1;
            Ok(())
        }
    }
    impl CountingOrderHandler {
        fn calls(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    /// A stub [`Outbound`] whose first `fail_until` publishes fail transiently (a relay blip), then
    /// succeed — to exercise the at-least-once retry path.
    struct FlakyOutbound {
        fail_until: usize,
        calls: Mutex<usize>,
        sent: Mutex<Vec<(PublicKey, Msg)>>,
    }
    impl FlakyOutbound {
        fn new(fail_until: usize) -> Self {
            Self {
                fail_until,
                calls: Mutex::new(0),
                sent: Mutex::new(Vec::new()),
            }
        }
    }
    #[async_trait]
    impl Outbound for FlakyOutbound {
        async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
            let n = {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                *calls
            };
            if n <= self.fail_until {
                bail!("relay unreachable (transient)");
            }
            self.sent.lock().unwrap().push((*recipient, msg.clone()));
            Ok(EventId::all_zeros())
        }
    }

    /// Seed a PROVISIONING sub + its SETTLED (PAID, `settled_at`) order invoice + a HELD reservation
    /// — exactly the state capture (lnrent-7fp.8) leaves for provisioning.
    async fn seed_provisioning(
        store: &Store,
        sub_id: &str,
        buyer_hex: &str,
        period_s: i64,
        renew_lead_s: i64,
        settled_at: i64,
    ) {
        let (sub, buyer) = (sub_id.to_string(), buyer_hex.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, params_json, refund_dest, period_s, renew_lead_s, retention_s, created_at, updated_at)
                     VALUES (?1, 'dummy', ?2, 'PROVISIONING', '{}', 'refund-dest', ?3, ?4, 604800, 0, 0)",
                    params![sub, buyer, period_s, renew_lead_s],
                )?;
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, settled_at, applied_at, issued_at)
                     VALUES (?1, ?2, ?3, 'order', 100, 'PAID', ?4, ?4, 0)",
                    params![format!("inv-{sub}"), sub, format!("order:{sub}"), settled_at],
                )?;
                tx.execute(
                    "INSERT INTO reservation
                        (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                     VALUES (?1, ?2, '{\"cpu\":0,\"mem_mb\":0,\"disk_gb\":0}', '{\"count\":0}', 'HELD', 9999999, 0)",
                    params![format!("res-{sub}"), sub],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    async fn scalar_str(store: &Store, sql: &str) -> String {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    async fn cleanup_done_count(store: &Store, pending_event_id: i64) -> i64 {
        let detail = cleanup_done_detail(pending_event_id);
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT count(*) FROM event_log WHERE kind=?1 AND detail_json=?2",
                    params![CLEANUP_DONE, detail],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
    }

    // Test 1: a PROVISIONING sub with a settled order invoice -> provision hook runs -> ONE txn
    // makes it ACTIVE with paid_through = invoice.settled_at + period (NOT the wall clock), the
    // reservation CONSUMED, an instance row (RUNNING), and a PENDING outbox provision.ready row.
    #[tokio::test]
    async fn provision_activates_and_queues_provision_ready_in_one_txn() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        // settled_at = 5000, period = 2_592_000, renew_lead = 604_800. Clock is at 9000 to prove the
        // timers key on settled_at, not now.
        seed_provisioning(&store, "sub-1", &buyer_hex, 2_592_000, 604_800, 5000).await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::Active);

        let (state, paid_through, soft, nd, instance_id): (String, i64, i64, i64, Option<String>) =
            store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT state, paid_through, soft_date, next_deadline, instance_id
                         FROM subscription WHERE id='sub-1'",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                    )?)
                })
                .await
                .unwrap();
        assert_eq!(state, "ACTIVE");
        assert_eq!(
            paid_through,
            5000 + 2_592_000,
            "paid_through = settled_at + period"
        );
        assert_eq!(
            soft,
            paid_through - 604_800,
            "soft_date = paid_through - renew_lead"
        );
        assert_eq!(nd, soft, "next_deadline = soft_date");
        assert_eq!(instance_id.as_deref(), Some("inst:sub-1"));

        // Reservation CONSUMED (the ACTIVE sub's capacity hold).
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "CONSUMED"
        );
        // Exactly one RUNNING instance carrying the recipe service id + the hook's handles.
        let (kind, handles, istate): (String, String, String) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT kind, handles_json, state FROM instance WHERE subscription_id='sub-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(kind, "dummy");
        assert_eq!(istate, "RUNNING");
        assert!(
            handles.contains("dummy-1"),
            "handles from the hook: {handles}"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 1);

        // Exactly one PENDING provision.ready outbox row (not yet published).
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM outbox WHERE state='PENDING' AND msg_type='provision.ready'"
            )
            .await,
            1
        );
        let (recipient, payload_json): (String, String) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT recipient, payload_json FROM outbox WHERE subscription_id='sub-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(recipient, buyer_hex, "DM is addressed to the buyer");
        let msg: Msg = serde_json::from_str(&payload_json).unwrap();
        match msg {
            Msg::ProvisionReady(p) => {
                assert_eq!(p.subscription_id, "sub-1");
                assert_eq!(p.payload["credential"], "dummy-secret-token");
            }
            other => panic!("expected provision.ready, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn provision_hook_input_includes_instance_and_host_facts() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            input_checking_recipe(),
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::Active);
        let handles = scalar_str(
            &store,
            "SELECT handles_json FROM instance WHERE subscription_id='sub-1'",
        )
        .await;
        assert!(handles.contains("checked"), "handles from hook: {handles}");
    }

    // Test 2: crash between ACTIVE and send leaves the outbox row PENDING; running the sender
    // (restart) publishes it and marks it SENT. Running the sender AGAIN does not re-publish a SENT
    // row (at-least-once; a stable id lets the buyer dedupe).
    #[tokio::test]
    async fn outbox_sender_publishes_pending_then_never_resends_sent() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );
        prov.drive("sub-1").await.unwrap();
        // The DM has NOT been published yet — it is only a PENDING outbox row (the "crash" window).
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox WHERE state='PENDING'").await,
            1
        );

        let sender = OutboxSender::new(store.clone(), Arc::new(TestClock::new(9100)));
        let out = RecordingOutbound::default();

        // First drain (restart) publishes the provision.ready to the buyer and marks it SENT.
        assert_eq!(sender.drain_once(&out).await.unwrap(), 1);
        let (recipient, msg) = out.only();
        assert_eq!(recipient, buyer.public_key());
        assert!(matches!(msg, Msg::ProvisionReady(_)));
        let (sstate, sent_at): (String, Option<i64>) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state, sent_at FROM outbox WHERE subscription_id='sub-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(sstate, "SENT");
        assert_eq!(sent_at, Some(9100));

        // A second drain publishes NOTHING (a SENT row is never re-sent).
        assert_eq!(sender.drain_once(&out).await.unwrap(), 0);
        assert_eq!(out.messages().len(), 1, "no re-publish of a SENT row");
    }

    #[tokio::test]
    async fn resend_provision_ready_republishes_latest_sent_row_on_demand() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );
        prov.drive("sub-1").await.unwrap();

        let sender = OutboxSender::new(store.clone(), Arc::new(TestClock::new(9100)));
        let out = RecordingOutbound::default();
        assert_eq!(sender.drain_once(&out).await.unwrap(), 1);
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM outbox WHERE subscription_id='sub-1'"
            )
            .await,
            "SENT"
        );

        assert!(sender
            .resend_provision_ready(&buyer.public_key(), "sub-1", &out)
            .await
            .unwrap());
        assert_eq!(
            out.messages().len(),
            2,
            "on-demand resend republishes a SENT row"
        );
        let wrong_buyer = Keys::generate();
        assert!(!sender
            .resend_provision_ready(&wrong_buyer.public_key(), "sub-1", &out)
            .await
            .unwrap());
        assert_eq!(out.messages().len(), 2, "non-buyer cannot trigger resend");
    }

    #[tokio::test]
    async fn delivery_resend_order_handler_routes_resend_to_outbox_sender() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );
        prov.drive("sub-1").await.unwrap();

        let sender = OutboxSender::new(store.clone(), Arc::new(TestClock::new(9100)));
        let out = RecordingOutbound::default();
        assert_eq!(sender.drain_once(&out).await.unwrap(), 1);

        let inner = Arc::new(CountingOrderHandler::default());
        let handler = DeliveryResendOrderHandler::new(
            inner.clone(),
            OutboxSender::new(store.clone(), Arc::new(TestClock::new(9200))),
        );
        handler
            .handle(
                buyer.public_key(),
                Msg::DeliveryResendRequest(DeliveryResendRequest {
                    subscription_id: "sub-1".to_string(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            out.messages().len(),
            2,
            "delivery.resend.request republishes provision.ready"
        );
        assert_eq!(inner.calls(), 0, "resend is handled by the wrapper");

        handler
            .handle(
                buyer.public_key(),
                Msg::SubCancel(SubCancel {
                    subscription_id: "sub-1".to_string(),
                }),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(inner.calls(), 1, "non-resend messages are delegated");
    }

    // A structurally-undeliverable row (unparseable payload or recipient) is quarantined to a
    // terminal FAILED state — not left PENDING to be re-selected and re-logged forever — while
    // good rows still send (codex P2).
    #[tokio::test]
    async fn outbox_sender_quarantines_poison_rows_and_sends_good_ones() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        let good_msg = serde_json::to_string(&Msg::ProvisionReady(ProvisionReady {
            subscription_id: "sub-good".to_string(),
            payload: json!({"credential": "ok"}),
        }))
        .unwrap();
        store
            .transaction(move |tx| {
                // Poison #1: payload is not a Msg (deterministic deserialize failure).
                tx.execute(
                    "INSERT INTO outbox
                        (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                     VALUES ('outbox-bad-payload', ?1, 'sub-bad', 'provision.ready', 'not-json', 'PENDING', 0, 1)",
                    params![buyer_hex],
                )?;
                // Poison #2: recipient hex will not parse to a PublicKey.
                tx.execute(
                    "INSERT INTO outbox
                        (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                     VALUES ('outbox-bad-recipient', 'not-a-pubkey', 'sub-bad', 'provision.ready', ?1, 'PENDING', 0, 2)",
                    params![good_msg],
                )?;
                tx.execute(
                    "INSERT INTO outbox
                        (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                     VALUES ('outbox-good', ?1, 'sub-good', 'provision.ready', ?2, 'PENDING', 0, 3)",
                    params![buyer_hex, good_msg],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let sender = OutboxSender::new(store.clone(), Arc::new(TestClock::new(9100)));
        let out = RecordingOutbound::default();
        assert_eq!(sender.drain_once(&out).await.unwrap(), 1);
        let (recipient, msg) = out.only();
        assert_eq!(recipient, buyer.public_key());
        assert!(matches!(msg, Msg::ProvisionReady(_)));
        // Both poison rows are quarantined FAILED (never re-selected), not left PENDING forever.
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM outbox WHERE id='outbox-bad-payload'"
            )
            .await,
            "FAILED"
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM outbox WHERE id='outbox-bad-recipient'"
            )
            .await,
            "FAILED"
        );
        assert_eq!(
            scalar_str(&store, "SELECT state FROM outbox WHERE id='outbox-good'").await,
            "SENT"
        );
        // A second drain has nothing left to do: the poison rows are no longer PENDING.
        assert_eq!(sender.drain_once(&out).await.unwrap(), 0);
    }

    // Reviewer 2 P3: a transient publish failure leaves the row PENDING for the next drain (at-
    // least-once), but each failed attempt is still recorded, so `attempts` reflects the real retry
    // count instead of only ever counting the terminal SENT transition.
    #[tokio::test]
    async fn transient_publish_failure_records_attempts_then_retries_to_sent() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );
        prov.drive("sub-1").await.unwrap();

        let sender = OutboxSender::new(store.clone(), Arc::new(TestClock::new(9100)));
        let out = FlakyOutbound::new(2); // first two drains fail transiently, the third succeeds

        // Two transient failures: nothing published, the row stays PENDING, attempts climb.
        assert_eq!(sender.drain_once(&out).await.unwrap(), 0);
        assert_eq!(sender.drain_once(&out).await.unwrap(), 0);
        let (state, attempts): (String, i64) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state, attempts FROM outbox WHERE subscription_id='sub-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(state, "PENDING", "still pending after transient failures");
        assert_eq!(attempts, 2, "each transient failure records an attempt");

        // The third drain succeeds: published once and marked SENT (attempts bumped once more).
        assert_eq!(sender.drain_once(&out).await.unwrap(), 1);
        let (state, attempts): (String, i64) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state, attempts FROM outbox WHERE subscription_id='sub-1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(state, "SENT");
        assert_eq!(
            attempts, 3,
            "transient attempts + the terminal SENT transition"
        );
    }

    // Test 3: a sub stuck in PROVISIONING is re-driven on restart (recover -> ACTIVE), idempotently
    // — re-driving the now-ACTIVE sub writes no second instance/outbox row.
    #[tokio::test]
    async fn recover_redrives_provisioning_idempotently() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );

        // Startup recovery drives the one stuck PROVISIONING sub to ACTIVE.
        assert_eq!(prov.recover().await.unwrap(), 1);
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "ACTIVE"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 1);

        // Re-driving the now-ACTIVE sub is an idempotent no-op: no second instance/outbox row.
        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::Skipped);
        assert_eq!(
            prov.recover().await.unwrap(),
            0,
            "no subs left in PROVISIONING"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 1);
    }

    #[tokio::test]
    async fn recover_and_drive_skip_subscriptions_for_other_recipes() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_provisioning(&store, "sub-1", &buyer_hex, 2_592_000, 604_800, 5000).await;
        seed_provisioning(&store, "sub-other", &buyer_hex, 2_592_000, 604_800, 5000).await;
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET recipe_id='other' WHERE id='sub-other'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );

        assert_eq!(prov.recover().await.unwrap(), 1);
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "ACTIVE"
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM subscription WHERE id='sub-other'"
            )
            .await,
            "PROVISIONING"
        );

        assert_eq!(prov.drive("sub-other").await.unwrap(), Provisioned::Skipped);
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM subscription WHERE id='sub-other'"
            )
            .await,
            "PROVISIONING"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 1);
    }

    // Reviewer 1 P2: a cleanup's `destroy` hook is recipe-specific, so a provisioner must only
    // re-drive failed cleanups for ITS OWN recipe. Running this recipe's destroy against another
    // recipe's cleanup input (and marking it done) would purge nothing real and rob the owning
    // recipe of ever cleaning up — a multi-recipe resource leak.
    #[tokio::test]
    async fn recover_failed_cleanups_only_purges_this_recipes_subscriptions() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_provisioning(&store, "sub-1", &buyer_hex, 2_592_000, 604_800, 5000).await;
        seed_provisioning(&store, "sub-other", &buyer_hex, 2_592_000, 604_800, 5000).await;
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET recipe_id='other' WHERE id='sub-other'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        // A 'dummy' provisioner whose destroy hook succeeds (so any cleanup it DOES claim is marked
        // done) — the point is which cleanups it claims, not whether destroy works.
        let (recipe, _marker) = successful_provision_with_destroy_marker_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        // A durable CLEANUP_PENDING for each recipe's sub (each had a failed provision pre-crash).
        let mine = prov
            .record_cleanup_intent("sub-1", &json!({"cleanup": "mine"}))
            .await
            .unwrap();
        let theirs = prov
            .record_cleanup_intent("sub-other", &json!({"cleanup": "theirs"}))
            .await
            .unwrap();

        // Recovery purges only the 'dummy' sub's cleanup; the 'other' recipe's cleanup is untouched.
        assert_eq!(prov.recover_failed_cleanups().await.unwrap(), 1);
        assert_eq!(cleanup_done_count(&store, mine).await, 1);
        assert_eq!(
            cleanup_done_count(&store, theirs).await,
            0,
            "another recipe's cleanup is left for its own provisioner"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log
                   WHERE kind='provision_cleanup_pending' AND subscription_id='sub-other'"
            )
            .await,
            1,
            "the other recipe's cleanup intent stays pending"
        );
    }

    // Test 4: a PERMANENT provision failure runs a best-effort destroy and moves the sub to
    // REFUND_DUE — no ACTIVE, no instance, no provision.ready (refund execution is lnrent-7fp.11).
    #[tokio::test]
    async fn permanent_provision_failure_destroys_and_sets_refund_due() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        // Seed a stale deadline cursor so the REFUND_DUE transition provably clears it (lnrent-y4m.4).
        store
            .transaction(|tx| {
                tx.execute("UPDATE subscription SET next_deadline=1 WHERE id='sub-1'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::RefundDue);
        assert!(marker.exists(), "best-effort destroy hook ran");

        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        // lnrent-y4m.4: the REFUND_DUE transition cleared the stale cursor (the Refunder drives off the
        // refund_attempt ledger, not next_deadline), so reconcile stops re-selecting it each tick.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM subscription WHERE id='sub-1' AND next_deadline IS NULL"
            )
            .await,
            1,
            "REFUND_DUE clears next_deadline (lnrent-y4m.4)"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM instance").await,
            0,
            "no instance on failure"
        );
        assert_eq!(
            count(&store, "SELECT count(*) FROM outbox").await,
            0,
            "no provision.ready on failure"
        );
        let (dest, amount_sat, idempotency_key, status): (Option<String>, i64, String, String) =
            store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT dest, amount_sat, idempotency_key, status
                         FROM refund_attempt WHERE subscription_id='sub-1'",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )?)
                })
                .await
                .unwrap();
        assert_eq!(dest.as_deref(), Some("refund-dest"));
        assert_eq!(amount_sat, 100);
        assert_eq!(idempotency_key, "refund:order:sub-1");
        assert_eq!(status, "PENDING");
        // The capacity hold is never CONSUMED on the failure path, and it is left HELD for the
        // refund executor (lnrent-7fp.11) — capture (lnrent-7fp.8) leaves the hold on its own refund
        // paths the same way. Releasing it here would risk handing the slot to a new order while a
        // slower concurrent drive is still provisioning (Reviewer 1 P2). A HELD hold for a PAID
        // order still counts as live usage, so the slot stays reserved meanwhile.
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD"
        );
    }

    #[tokio::test]
    async fn failed_destroy_leaves_cleanup_pending_and_capacity_held() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let (recipe, marker) = failing_destroy_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::RefundDue);
        assert!(marker.exists(), "destroy was attempted");
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD",
            "capacity is not released when destroy fails"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_done'"
            )
            .await,
            0
        );
        assert_eq!(
            prov.recover_failed_cleanups().await.unwrap(),
            0,
            "failed destroy stays pending for a later recovery pass"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );
    }

    // A cleanup intent durably claimed before a crash (the destroy never ran) is re-driven on
    // restart: recovery runs the idempotent `destroy` and records CLEANUP_DONE. Throughout, the
    // capacity hold is left HELD for the refund executor (lnrent-7fp.11) — never released here.
    #[tokio::test]
    async fn failed_provision_cleanup_is_recovered_on_restart() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );
        let target = prov
            .load_target("sub-1")
            .await
            .unwrap()
            .expect("sub exists");

        let cleanup_event_id = prov
            .mark_refund_due(
                RefundDueWrite::new("sub-1", &target, &json!({"cleanup": "recover-me"}), 9000)
                    .unwrap(),
            )
            .await
            .unwrap();
        let RefundDueWriteOutcome::Claimed(cleanup_event_id) = cleanup_event_id else {
            panic!("refund CAS should win");
        };
        assert!(cleanup_event_id > 0);
        assert!(!marker.exists(), "simulated crash happened before destroy");
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD",
            "capacity is held for the refund executor (lnrent-7fp.11)"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );

        assert_eq!(prov.recover_failed_cleanups().await.unwrap(), 1);
        assert!(marker.exists(), "startup recovery ran destroy");
        // Recovery purges the partial resource (CLEANUP_DONE) but does NOT touch capacity — the hold
        // stays HELD for the refund executor (lnrent-7fp.11).
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_done'"
            )
            .await,
            1
        );
        assert_eq!(prov.recover_failed_cleanups().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cleanup_completion_marks_only_the_claimed_intent() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            dummy_recipe(),
            "box-1".into(),
        );

        let first = prov
            .record_cleanup_intent("sub-1", &json!({"cleanup": "first"}))
            .await
            .unwrap();
        let second = prov
            .record_cleanup_intent("sub-1", &json!({"cleanup": "second"}))
            .await
            .unwrap();

        prov.complete_failed_cleanup(first, "sub-1", &json!({"cleanup": "first"}))
            .await
            .unwrap();
        assert_eq!(cleanup_done_count(&store, first).await, 1);
        assert_eq!(
            cleanup_done_count(&store, second).await,
            0,
            "newer cleanup intent is not marked done by the older cleanup"
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD",
            "completing a cleanup never releases capacity (it is left for lnrent-7fp.11)"
        );

        assert_eq!(prov.recover_failed_cleanups().await.unwrap(), 1);
        assert_eq!(cleanup_done_count(&store, second).await, 1);
        // Even with every cleanup intent finished, the hold is left HELD for the refund executor
        // (lnrent-7fp.11) — finishing cleanups purges resources, it does not free capacity.
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "HELD"
        );
    }

    // codex P2: a successful provision hook whose capacity hold was RELEASED out from under it (a
    // concurrent refund / terminate) must NOT activate — `consume` finds no HELD row, so the hook
    // output is destroyed and the sub is handed to refunds rather than re-driven forever. The
    // handle-checking destroy hook passing also proves the cleanup forwarded the hook's `handles`.
    #[tokio::test]
    async fn successful_provision_with_released_reservation_destroys_and_refunds() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE reservation SET state='RELEASED' WHERE order_id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let (recipe, marker) = handle_checking_destroy_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::RefundDue);
        assert!(
            marker.exists(),
            "destroy ran AND received the hook's handles"
        );
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        // The reservation was already RELEASED; the cleanup release is an idempotent no-op.
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "RELEASED"
        );
    }

    // Reviewer 1 P2: if a successful hook loses the ACTIVE CAS to a refund handoff, the created
    // resources are not live/tracked by an instance row, so they must be destroyed.
    #[tokio::test]
    async fn successful_provision_losing_activation_to_refund_destroys_output() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='REFUND_DUE' WHERE id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let (recipe, marker) = successful_provision_with_destroy_marker_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(
            prov.cleanup_lost_hook_output("sub-1", &json!({}))
                .await
                .unwrap(),
            Provisioned::RefundDue
        );
        assert!(
            marker.exists(),
            "untracked successful hook output was destroyed"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        // Crash-safe (Reviewer 1 P2): a durable CLEANUP_PENDING was recorded before destroy and
        // then completed — so a crash mid-destroy is recoverable, not a silent leak.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_done'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn active_write_lost_cas_records_cleanup_intent_in_same_txn() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_provisioning(&store, "sub-1", &buyer_hex, 2_592_000, 604_800, 5000).await;
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='REFUND_DUE' WHERE id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payload_json = serde_json::to_string(&Msg::ProvisionReady(ProvisionReady {
            subscription_id: "sub-1".to_string(),
            payload: json!({"credential": "ok"}),
        }))
        .unwrap();
        let write = ActiveWrite {
            subscription_id: "sub-1".to_string(),
            order_id: "sub-1".to_string(),
            instance_id: "inst:sub-1".to_string(),
            box_id: "box-1".to_string(),
            kind: "dummy".to_string(),
            handles_json: "{}".to_string(),
            destroy_input_json: json!({"handles": {"instance": "leaked"}}).to_string(),
            outbox_id: "outbox:provision:sub-1".to_string(),
            recipient_hex: buyer_hex,
            payload_json,
            paid_through: 10,
            soft_date: 9,
            now: 9000,
        };

        let outcome = store.transaction(move |tx| write.write(tx)).await.unwrap();
        let ActiveWriteOutcome::Lost(CleanupIntent::Pending {
            cleanup_event_id,
            owner_state,
        }) = outcome
        else {
            panic!("lost active CAS should record cleanup intent");
        };
        assert!(cleanup_event_id > 0);
        assert_eq!(owner_state.as_deref(), Some("REFUND_DUE"));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
    }

    // Reviewer 1 P2: a FAILED drive that loses the PROVISIONING -> REFUND_DUE CAS to a concurrent
    // racer (already REFUND_DUE, and the hold released out from under it by a concurrent
    // refund/terminate) must STILL destroy the output its idempotent hook (§7.2) may have
    // re-materialised AFTER the winner's destroy ran — otherwise it leaks. It does so crash-safely
    // (a durable CLEANUP_PENDING recorded before destroy), and writes no second refund row. Only an
    // ACTIVE racer skips destroy.
    #[tokio::test]
    async fn permanent_failure_losing_refund_cas_still_destroys_its_output() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        // Simulate the hostile precondition: the sub is already REFUND_DUE and its hold was RELEASED
        // by a concurrent refund/terminate. Our re-created output is no longer referenced by
        // anything, so our destroy must still run (the leak window from P2 #1).
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='REFUND_DUE' WHERE id='sub-1'",
                    [],
                )?;
                tx.execute(
                    "UPDATE reservation SET state='RELEASED' WHERE order_id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        let t = prov
            .load_target("sub-1")
            .await
            .unwrap()
            .expect("sub exists");
        assert_eq!(
            prov.fail_to_refund_due("sub-1", &t, &json!({"handles": {"instance": "leaked"}}))
                .await
                .unwrap(),
            Provisioned::RefundDue
        );
        // Our destroy ran against our own re-created output (the resource P2 #1 would have leaked)...
        assert!(
            marker.exists(),
            "loser destroyed its own re-created hook output"
        );
        // ...crash-safely: a fresh CLEANUP_PENDING was recorded and then completed.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            1
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_done'"
            )
            .await,
            1
        );
        // The CAS was lost, so we wrote no second refund row.
        assert_eq!(
            count(&store, "SELECT count(*) FROM refund_attempt").await,
            0
        );
    }

    // Reviewer 1 P1: if a concurrent drive already moved the sub PROVISIONING -> ACTIVE (recording
    // its live instance under the same stable id), a slow failing drive must NOT destroy that
    // instance. It reports Skipped, leaving the racer's instance + CONSUMED reservation intact.
    #[tokio::test]
    async fn permanent_failure_skips_destroy_when_a_racing_drive_already_activated() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        // Simulate the racing winner: the sub is now ACTIVE with a live RUNNING instance and a
        // CONSUMED reservation (exactly what a successful concurrent drive would have committed).
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='ACTIVE', instance_id='inst:sub-1' WHERE id='sub-1'",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO instance
                        (id, subscription_id, box_id, kind, handles_json, state, created_at, updated_at)
                     VALUES ('inst:sub-1','sub-1','box-1','dummy','{}','RUNNING',0,0)",
                    [],
                )?;
                tx.execute(
                    "UPDATE reservation SET state='CONSUMED' WHERE order_id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let t = prov
            .load_target("sub-1")
            .await
            .unwrap()
            .expect("sub exists");
        assert_eq!(
            prov.fail_to_refund_due("sub-1", &t, &json!({}))
                .await
                .unwrap(),
            Provisioned::Skipped
        );
        assert!(
            !marker.exists(),
            "destroy must NOT run against a concurrently-activated instance"
        );
        // The racer's live instance + ACTIVE state are untouched; no refund row was written.
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "ACTIVE"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 1);
        assert_eq!(
            count(&store, "SELECT count(*) FROM refund_attempt").await,
            0
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "CONSUMED"
        );
    }

    #[tokio::test]
    async fn cleanup_loser_skips_destroy_when_live_instance_is_already_suspended() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        // The activation winner has already recorded the live instance, then an admin/reconcile
        // transition moved the subscription on to SUSPENDED before the loser observed it.
        store
            .transaction(|tx| {
                tx.execute(
                    "UPDATE subscription SET state='SUSPENDED', instance_id='inst:sub-1' WHERE id='sub-1'",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO instance
                        (id, subscription_id, box_id, kind, handles_json, state, created_at, updated_at)
                     VALUES ('inst:sub-1','sub-1','box-1','dummy','{}','RUNNING',0,0)",
                    [],
                )?;
                tx.execute(
                    "UPDATE reservation SET state='CONSUMED' WHERE order_id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let t = prov
            .load_target("sub-1")
            .await
            .unwrap()
            .expect("sub exists");
        assert_eq!(
            prov.fail_to_refund_due("sub-1", &t, &json!({}))
                .await
                .unwrap(),
            Provisioned::Skipped
        );
        assert!(
            !marker.exists(),
            "destroy must not run when a live instance is already tracked"
        );
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "SUSPENDED"
        );
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM event_log WHERE kind='provision_cleanup_pending'"
            )
            .await,
            0
        );
        assert_eq!(
            scalar_str(
                &store,
                "SELECT state FROM reservation WHERE order_id='sub-1'"
            )
            .await,
            "CONSUMED"
        );
    }

    // Reviewer 2 P3: a PAID order invoice with a NULL amount_sat must still reach REFUND_DUE (with a
    // NULL refund amount for the executor) — it must NOT wedge the sub in PROVISIONING and re-run the
    // failed hooks forever.
    #[tokio::test]
    async fn permanent_failure_with_null_amount_still_reaches_refund_due() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, params_json, refund_dest, period_s, renew_lead_s, retention_s, created_at, updated_at)
                     VALUES ('sub-1', 'dummy', ?1, 'PROVISIONING', '{}', 'refund-dest', 2592000, 604800, 604800, 0, 0)",
                    params![buyer_hex],
                )?;
                // PAID order invoice with settled_at set but amount_sat NULL.
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status, settled_at, applied_at, issued_at)
                     VALUES ('inv-sub-1', 'sub-1', 'order:sub-1', 'order', NULL, 'PAID', 5000, 5000, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO reservation
                        (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                     VALUES ('res-sub-1', 'sub-1', '{\"cpu\":0,\"mem_mb\":0,\"disk_gb\":0}', '{\"count\":0}', 'HELD', 9999999, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let (recipe, marker) = failing_provision_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::RefundDue);
        assert!(marker.exists(), "best-effort destroy hook ran");
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        // The refund row exists with a NULL amount preserved for the executor (lnrent-7fp.11).
        let amount: Option<i64> = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT amount_sat FROM refund_attempt WHERE subscription_id='sub-1'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(amount, None, "NULL amount preserved, not fabricated");
    }

    #[tokio::test]
    async fn provision_success_without_payload_is_refund_due_not_active() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_provisioning(
            &store,
            "sub-1",
            &buyer.public_key().to_hex(),
            2_592_000,
            604_800,
            5000,
        )
        .await;
        let (recipe, marker) = missing_payload_recipe();
        let prov = Provisioner::new(
            store.clone(),
            Arc::new(TestClock::new(9000)),
            recipe,
            "box-1".into(),
        );

        assert_eq!(prov.drive("sub-1").await.unwrap(), Provisioned::RefundDue);
        assert!(marker.exists(), "best-effort destroy hook ran");
        assert_eq!(
            scalar_str(&store, "SELECT state FROM subscription WHERE id='sub-1'").await,
            "REFUND_DUE"
        );
        assert_eq!(count(&store, "SELECT count(*) FROM instance").await, 0);
        assert_eq!(count(&store, "SELECT count(*) FROM outbox").await, 0);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM refund_attempt WHERE status='PENDING'"
            )
            .await,
            1
        );
    }
}
