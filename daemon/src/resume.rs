//! Resume suspended subscriptions after a paid renewal.
//!
//! Capture moves a paid renewal of a SUSPENDED subscription to RESUMING because the store
//! transaction cannot run the recipe `resume` hook. This driver owns that side effect: run the
//! idempotent hook with bounded retries, then CAS RESUMING -> ACTIVE. Permanent failure restores the
//! pre-renewal SUSPENDED timers and records a detached renewal refund; it never uses REFUND_DUE,
//! destroys the instance, releases the reservation, or re-delivers provision.ready.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::capture::insert_refund_attempt_txn;
use crate::clock::Clock;
use crate::recipe::Recipe;
use crate::runner::{run_hook, DEFAULT_TIMEOUT};
use crate::store::Store;

/// Same bounded retry shape as provisioning: resume hooks are idempotent, so retry transient
/// failures a few times within one serialized maintenance pass before declaring permanent failure.
const RESUME_ATTEMPTS: u32 = 3;
const RESUME_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// Outcome of driving one RESUMING subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// Hook succeeded and the subscription reached ACTIVE.
    Active,
    /// Hook failed permanently; the subscription returned to SUSPENDED and a renewal refund is due.
    SuspendedRefundPending,
    /// The subscription was not RESUMING, belonged to another recipe, or a racer already resolved it.
    Skipped,
}

/// Drives RESUMING subscriptions to ACTIVE or back to SUSPENDED with a detached renewal refund.
pub struct ResumeDriver {
    store: Store,
    clock: Arc<dyn Clock>,
    recipe: Recipe,
}

struct ResumeTarget {
    state: String,
    buyer_hex: String,
    recipe_id: Option<String>,
    refund_dest: Option<String>,
    renew_lead_s: i64,
    retention_s: i64,
    instance: Option<ResumeInstance>,
}

struct ResumeInstance {
    id: String,
    box_id: Option<String>,
    kind: Option<String>,
    handles_json: Option<String>,
    state: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RenewResumeBaseline {
    external_id: String,
    amount_sat: Option<i64>,
    #[serde(default)]
    previous_paid_through: Option<i64>,
    #[serde(default)]
    previous_suspend_not_before: Option<i64>,
}

impl ResumeDriver {
    pub fn new(store: Store, clock: Arc<dyn Clock>, recipe: Recipe) -> Self {
        Self {
            store,
            clock,
            recipe,
        }
    }

    /// Drive one subscription. Safe to re-run after a crash: the hook is idempotent, success/failure
    /// writes are guarded on state='RESUMING', and the refund row is keyed by refund:<external_id>.
    pub async fn drive(&self, subscription_id: &str) -> Result<ResumeOutcome> {
        let Some(t) = self.load_target(subscription_id).await? else {
            return Ok(ResumeOutcome::Skipped);
        };
        if t.state != "RESUMING" {
            return Ok(ResumeOutcome::Skipped);
        }
        if t.recipe_id.as_deref() != Some(self.recipe.service.id.as_str()) {
            tracing::warn!(
                sub = %subscription_id,
                row_recipe = t.recipe_id.as_deref().unwrap_or(""),
                driver_recipe = %self.recipe.service.id,
                "skipping RESUMING subscription for a different recipe"
            );
            return Ok(ResumeOutcome::Skipped);
        }

        let Some(instance) = t.instance.as_ref() else {
            tracing::error!(sub = %subscription_id, "resume failed permanently: missing instance");
            return self.fail_to_suspended_refund(subscription_id, &t).await;
        };

        let input = lifecycle_input(subscription_id, &t.buyer_hex, instance);
        match self.run_resume(&input).await {
            Ok(()) => self.mark_active(subscription_id).await,
            Err(e) => {
                tracing::error!(sub = %subscription_id, error = %e, "resume failed permanently; restoring SUSPENDED + refunding renewal");
                self.fail_to_suspended_refund(subscription_id, &t).await
            }
        }
    }

    /// Restart/maintenance recovery: re-drive every subscription left in RESUMING for this recipe.
    pub async fn recover(&self) -> Result<usize> {
        let ids = self.resuming_ids().await?;
        let mut driven = 0;
        for id in ids {
            match self.drive(&id).await {
                Ok(ResumeOutcome::Skipped) => {}
                Ok(_) => driven += 1,
                Err(e) => tracing::error!(sub = %id, error = %e, "re-drive of RESUMING sub failed"),
            }
        }
        Ok(driven)
    }

    async fn run_resume(&self, input: &Value) -> Result<()> {
        let hook = self.recipe.hook("resume");
        let mut last_err = None;
        for attempt in 1..=RESUME_ATTEMPTS {
            match run_hook(&hook, input, DEFAULT_TIMEOUT).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "resume hook attempt failed");
                    last_err = Some(e);
                }
            }
            if attempt < RESUME_ATTEMPTS {
                tokio::time::sleep(RESUME_RETRY_BACKOFF).await;
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("resume hook failed")))
    }

    async fn mark_active(&self, subscription_id: &str) -> Result<ResumeOutcome> {
        let sub_id = subscription_id.to_string();
        let now = self.clock.now();
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "UPDATE subscription SET state='ACTIVE', updated_at=?2
                     WHERE id=?1 AND state='RESUMING'",
                    params![sub_id, now],
                )?;
                if n == 0 {
                    return Ok(ResumeOutcome::Skipped);
                }
                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                     VALUES (?1, 'resume_active', '{}', ?2)",
                    params![sub_id, now],
                )?;
                Ok(ResumeOutcome::Active)
            })
            .await
    }

    async fn fail_to_suspended_refund(
        &self,
        subscription_id: &str,
        t: &ResumeTarget,
    ) -> Result<ResumeOutcome> {
        let write = ResumeFailureWrite::new(subscription_id, t, self.clock.now());
        let claimed = self.store.transaction(move |tx| write.write(tx)).await?;
        Ok(if claimed {
            ResumeOutcome::SuspendedRefundPending
        } else {
            ResumeOutcome::Skipped
        })
    }

    async fn load_target(&self, sub_id: &str) -> Result<Option<ResumeTarget>> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                let row = c
                    .query_row(
                        "SELECT s.state, s.buyer_pubkey, s.recipe_id, s.refund_dest,
                                s.renew_lead_s, s.retention_s,
                                i.id, i.box_id, i.kind, i.handles_json, i.state
                           FROM subscription s
                           LEFT JOIN instance i ON i.id=s.instance_id
                          WHERE s.id=?1",
                        params![id],
                        |r| {
                            Ok((
                                r.get::<_, Option<String>>(0)?.unwrap_or_default(),
                                r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                                r.get::<_, Option<String>>(2)?,
                                r.get::<_, Option<String>>(3)?,
                                r.get::<_, Option<i64>>(4)?.unwrap_or(0),
                                r.get::<_, Option<i64>>(5)?.unwrap_or(0),
                                r.get::<_, Option<String>>(6)?,
                                r.get::<_, Option<String>>(7)?,
                                r.get::<_, Option<String>>(8)?,
                                r.get::<_, Option<String>>(9)?,
                                r.get::<_, Option<String>>(10)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((
                    state,
                    buyer_hex,
                    recipe_id,
                    refund_dest,
                    renew_lead_s,
                    retention_s,
                    instance_id,
                    box_id,
                    kind,
                    handles_json,
                    instance_state,
                )) = row
                else {
                    return Ok(None);
                };
                let instance = instance_id.map(|id| ResumeInstance {
                    id,
                    box_id,
                    kind,
                    handles_json,
                    state: instance_state,
                });
                Ok(Some(ResumeTarget {
                    state,
                    buyer_hex,
                    recipe_id,
                    refund_dest,
                    renew_lead_s,
                    retention_s,
                    instance,
                }))
            })
            .await
    }

    async fn resuming_ids(&self) -> Result<Vec<String>> {
        let recipe_id = self.recipe.service.id.clone();
        self.store
            .read(move |c| {
                let mut stmt = c.prepare(
                    "SELECT id FROM subscription WHERE state='RESUMING' AND recipe_id=?1",
                )?;
                let ids = stmt
                    .query_map(params![recipe_id], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(ids)
            })
            .await
    }
}

struct ResumeFailureWrite {
    subscription_id: String,
    dest: Option<String>,
    renew_lead_s: i64,
    retention_s: i64,
    now: i64,
}

impl ResumeFailureWrite {
    fn new(subscription_id: &str, t: &ResumeTarget, now: i64) -> Self {
        Self {
            subscription_id: subscription_id.to_string(),
            dest: t.refund_dest.clone(),
            renew_lead_s: t.renew_lead_s,
            retention_s: t.retention_s,
            now,
        }
    }

    /// RESUMING -> SUSPENDED + restore pre-renewal timers + detached renewal refund in one CAS txn.
    fn write(self, tx: &Transaction) -> Result<bool> {
        let renewals = pending_renew_resumes(tx, &self.subscription_id)?;
        validate_renewals(&self.subscription_id, &renewals)?;
        let first = renewals
            .first()
            .ok_or_else(|| anyhow!("resume: missing renewal baseline"))?;
        let previous_paid_through = first.previous_paid_through;
        let previous_suspend_not_before = first.previous_suspend_not_before;
        let soft_date = previous_paid_through.map(|pt| pt - self.renew_lead_s);
        let next_deadline = previous_paid_through
            .map(|pt| pt.max(previous_suspend_not_before.unwrap_or(pt)) + self.retention_s);

        let n = tx.execute(
            "UPDATE subscription
                SET state='SUSPENDED', paid_through=?2, soft_date=?3, next_deadline=?4,
                    suspend_not_before=?5, updated_at=?6
              WHERE id=?1 AND state='RESUMING'",
            params![
                &self.subscription_id,
                previous_paid_through,
                soft_date,
                next_deadline,
                previous_suspend_not_before,
                self.now,
            ],
        )?;
        if n == 0 {
            return Ok(false);
        }

        let mut refund_ids = Vec::with_capacity(renewals.len());
        let mut external_ids = Vec::with_capacity(renewals.len());
        for renewal in &renewals {
            let refund_id = format!("ref-{}", renewal.external_id);
            insert_refund_attempt_txn(
                tx,
                Some(&self.subscription_id),
                self.dest.as_deref(),
                &renewal.external_id,
                renewal.amount_sat,
                self.now,
            )?;
            refund_ids.push(refund_id);
            external_ids.push(renewal.external_id.clone());
        }
        let detail = json!({
            "external_ids": external_ids,
            "refund_ids": refund_ids,
        })
        .to_string();
        tx.execute(
            "INSERT INTO event_log (subscription_id, kind, detail_json, at)
             VALUES (?1, 'resume_failed', ?2, ?3)",
            params![&self.subscription_id, detail, self.now],
        )?;
        Ok(true)
    }
}

fn pending_renew_resumes(
    conn: &rusqlite::Connection,
    sub_id: &str,
) -> Result<Vec<RenewResumeBaseline>> {
    let last_resolution_id: i64 = conn.query_row(
        "SELECT COALESCE(MAX(id), 0)
           FROM event_log
          WHERE subscription_id=?1 AND kind IN ('resume_active', 'resume_failed')",
        params![sub_id],
        |r| r.get(0),
    )?;
    let mut stmt = conn.prepare(
        "SELECT detail_json
           FROM event_log
          WHERE subscription_id=?1 AND kind='renew_resume' AND id > ?2
          ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(params![sub_id, last_resolution_id], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .filter_map(|raw| match parse_renew_resume_baseline(&raw) {
            Ok(Some(baseline)) => Some(Ok(baseline)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
}

fn parse_renew_resume_baseline(raw: &str) -> Result<Option<RenewResumeBaseline>> {
    let value: Value = serde_json::from_str(raw)?;
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };
    if !obj.contains_key("previous_paid_through")
        || !obj.contains_key("previous_suspend_not_before")
    {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(value)?))
}

fn validate_renewals(sub_id: &str, renewals: &[RenewResumeBaseline]) -> Result<()> {
    if renewals.is_empty() {
        return Err(anyhow!(
            "resume: RESUMING sub `{sub_id}` has no renew_resume journal baseline"
        ));
    }
    for renewal in renewals {
        if renewal.external_id.trim().is_empty() {
            return Err(anyhow!(
                "resume: RESUMING sub `{sub_id}` has an empty renewal external_id"
            ));
        }
    }
    Ok(())
}

fn lifecycle_input(sub_id: &str, buyer_hex: &str, instance: &ResumeInstance) -> Value {
    let handles = match instance.handles_json.as_deref() {
        Some(raw) => serde_json::from_str::<Value>(raw).unwrap_or_else(|e| {
            tracing::warn!(
                sub = %sub_id,
                instance = %instance.id,
                error = %e,
                "resume: instance handles_json is invalid; hook gets null handles"
            );
            Value::Null
        }),
        None => Value::Null,
    };
    json!({
        "subscription": {
            "id": sub_id,
            "buyer_pubkey": buyer_hex,
        },
        "instance": {
            "id": instance.id,
            "subscription_id": sub_id,
            "box_id": instance.box_id,
            "kind": instance.kind,
            "state": instance.state,
            "handles": handles.clone(),
        },
        "handles": handles,
    })
}
