//! Buyer management-op dispatch (lnrent-7fp.20, SPEC.md §5.1/§7.4, ADR-0013).
//!
//! The concrete [`OpHandler`] the Nostr engine (lnrent-7fp.5) routes buyer→operator `op.request`
//! DMs to. It turns one `op.request` into one recipe op-hook run and one `op.result`, with
//! **crash-safe durable idempotency** as its spine. Like its sibling [`crate::order_intake`] it
//! only *consumes* existing seams — it does not rebuild transport, the recipe loader, or the hook
//! runner:
//! - durable idempotency on `(sender_pubkey, request_id)` in the `op_invocation` table (§5.1):
//!   the `RUNNING` claim is taken BEFORE the hook and the terminal `DONE`/`ERROR` is committed
//!   AFTER it, both outside the (async) hook, so a duplicate resends the cached `op.result` and
//!   NEVER re-runs the hook;
//! - authorization against the `subscription` row (owner + `ACTIVE`), folded into the claim txn and
//!   run BEFORE the durable insert so the three auth rejects persist no row (gdu.3); op resolution via
//!   [`Recipe::operation`] (the load-time [`Recipe::validate`] already enforces hook-name safety /
//!   `ops/`-containment — not re-checked here), and op-param validation;
//! - the hook itself via [`runner::run_hook`].
//!
//! It is deliberately the SIMPLEST correct dispatcher: there is NO in-flight task registry. A
//! concurrent duplicate that finds someone else's `RUNNING` row defers by returning `Err` so the
//! transport reprocesses the wrap later — by then the first attempt has committed a terminal state.
//! A hook failure is a committed, cached `op.result` error, never a handler `Err` (which would
//! re-run the hook on transport retry) and never a daemon wedge.
//!
//! Production wiring (into `main.rs` / `run_inbound`, plus calling [`OpDispatch::recover_interrupted_ops`]
//! at startup) is lnrent-7fp.21's job; this bead only exposes the handler and the recovery method.

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

use lnrent_wire::{Msg, OpRequest, OpResult, PublicKey, WireError};

use crate::clock::Clock;
use crate::nostr_engine::{OpHandler, Outbound};
use crate::recipe::{Operation, Recipe};
use crate::runner::{self, run_hook, HookOutput};
use crate::store::Store;

/// Cap on buyer-facing messages derived from local detail so they cannot bloat the cached
/// `op_invocation.error_json` (and the resent `op.result`).
const MAX_ERROR_MESSAGE_CHARS: usize = 256;
const HOOK_FAILED_MESSAGE: &str = "operation hook failed";

/// The op-dispatch integrator: implements [`OpHandler`] over the injected store, clock, and recipe.
/// Cheap to share behind an `Arc` (the engine holds it as `Arc<dyn OpHandler>`). M1a serves ONE
/// operator recipe, so `recipe` IS the subscription's recipe — there is no per-sub recipe lookup.
pub struct OpDispatch {
    store: Store,
    clock: Arc<dyn Clock>,
    recipe: Recipe,
}

/// The outcome of the lookup → auth → claim transaction (steps 1–3 in [`OpDispatch::claim`]):
/// whether WE took the `RUNNING` row, a prior attempt already left a terminal/in-flight state for
/// this `(sender, request_id)`, or the request failed authorization (row-free — nothing persisted).
#[derive(Debug)]
enum Claim {
    /// We inserted the `RUNNING` row — proceed to run the op. Carries the authorized subscription's
    /// state (always `"ACTIVE"`), read in the SAME txn that claimed, for the hook input.
    Claimed { subscription_state: String },
    /// A prior attempt committed `DONE` — resend its cached result `data` JSON, labeled with the
    /// STORED subscription_id/op (the authorized request's), not the duplicate's. No hook re-run.
    Done {
        result_json: String,
        subscription_id: String,
        op: String,
    },
    /// A prior attempt committed `ERROR` — resend its cached [`WireError`] JSON with the STORED
    /// subscription_id/op. No hook re-run.
    Errored {
        error_json: String,
        subscription_id: String,
        op: String,
    },
    /// A concurrent duplicate is mid-flight (`RUNNING`, not ours). Defer-and-retry: do NOT re-run.
    Running,
    /// Auth reject (no row existed): unknown sub OR not the owner. Reply `unauthorized`, persist
    /// NOTHING — an unauthenticated stranger leaves no durable artifact (gdu.3).
    Unauthorized,
    /// Auth reject (no row existed): the owner's sub is not `ACTIVE`. Reply `not_active`, persist
    /// NOTHING (gdu.3).
    NotActive,
}

impl OpDispatch {
    pub fn new(store: Store, clock: Arc<dyn Clock>, recipe: Recipe) -> Self {
        Self {
            store,
            clock,
            recipe,
        }
    }

    /// The `op.request` flow (SPEC.md §5.1/§7.4, ADR-0013, gdu.3): lookup → authorize → durable
    /// claim (one txn) → resolve → validate → run the hook outside any txn → commit terminal state →
    /// reply. Every AUTHORIZED business outcome (unknown/invalid/hook failure) is a committed, cached
    /// `op.result` — never a handler `Err`, which would re-run the hook on transport retry. The three
    /// AUTH rejects (unknown sub / not-owner / not-ACTIVE) reply the same error but persist NOTHING.
    async fn dispatch(&self, sender: PublicKey, req: OpRequest, out: &dyn Outbound) -> Result<()> {
        let now = self.clock.now();
        let sender_hex = sender.to_hex();

        // 1. LOOKUP → AUTHORIZE → CLAIM in ONE serialized txn (see `claim`). A cached
        //    terminal/in-flight state wins BEFORE auth (cached-resend must survive a later sub state
        //    change); the three auth rejects persist NOTHING; only an authorized request inserts the
        //    RUNNING claim, atomically with the ACTIVE read (TOCTOU-safe). Claim carries the
        //    authorized sub's state for the hook input.
        let subscription_state = match self.claim(&sender_hex, &req, now).await? {
            Claim::Claimed { subscription_state } => subscription_state,
            Claim::Done {
                result_json,
                subscription_id,
                op,
            } => {
                // Cached success: a DONE row always holds an object `data` (we reject non-object
                // hook output before committing), so the decode is safe; fall back defensively.
                // Echo the STORED subscription_id/op so a reused id can't relabel the cached reply.
                let data = serde_json::from_str::<Value>(&result_json)
                    .ok()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| json!({}));
                out.reply(
                    &sender,
                    &Msg::OpResult(OpResult::ok(req.id, subscription_id, op, data)),
                )
                .await?;
                return Ok(());
            }
            Claim::Errored {
                error_json,
                subscription_id,
                op,
            } => {
                let error = serde_json::from_str::<WireError>(&error_json)
                    .unwrap_or_else(|_| interrupted());
                out.reply(
                    &sender,
                    &Msg::OpResult(OpResult::err(req.id, subscription_id, op, error)),
                )
                .await?;
                return Ok(());
            }
            Claim::Running => {
                // A concurrent duplicate owns the RUNNING row. Don't re-run and don't reply with a
                // half-baked state — return Err so the transport does NOT mark the wrap seen and
                // reprocesses it later, by which point the first attempt has committed DONE/ERROR
                // (run_hook caps at DEFAULT_TIMEOUT, so it terminates). Defer-and-retry is the
                // minimal correct M1a behavior — no in-flight task registry.
                return Err(anyhow!(
                    "op.request {} from {sender_hex} is already RUNNING (concurrent duplicate); deferring",
                    req.id
                ));
            }
            // Row-free AUTH rejects (gdu.3, spec §C). The reply is IDENTICAL for a nonexistent sub
            // and someone else's sub (both `unauthorized`) — no existence leak. The reply itself
            // stays: silence would break a legitimate buyer with a stale sub id and leaks nothing
            // more; PR-2's per-pubkey bucket bounds the reply amplification. Nothing is persisted.
            Claim::Unauthorized => {
                return self.reply_error(&sender, &req, unauthorized(), out).await;
            }
            Claim::NotActive => {
                return self.reply_error(&sender, &req, not_active(), out).await;
            }
        };

        // From here the RUNNING claim is OURS and the sender is AUTHORIZED. Every BUSINESS outcome
        // below (unknown/invalid/hook result) commits a terminal DONE/ERROR and replies. A
        // store-layer error (a failed read or terminal commit) instead propagates `Err`, leaving the
        // row RUNNING — never re-run inline; the next startup's `recover_interrupted_ops` flips it to
        // the cached `interrupted` error, and the buyer's later retry resends that. So the hook still
        // runs at most once.

        // 2. RESOLVE the op. Unknown, or a non-`request` kind (interactive is out of scope here), is
        //    `unknown_op`. Past auth → STILL commits a terminal ERROR row (cached-resend), unchanged.
        //    Hook-name safety / `ops/`-containment was enforced at load by Recipe::validate
        //    (lnrent-7fp.6) — not re-checked here.
        let Some(op) = self.recipe.operation(&req.op) else {
            return self.fail(&sender, &req, unknown_op(), now, out).await;
        };
        if op.kind != "request" {
            return self.fail(&sender, &req, unknown_op(), now, out).await;
        }

        // 3. VALIDATE params against the op schema (reject unknown/missing/mistyped). Past auth →
        //    STILL commits a terminal ERROR row (cached-resend), unchanged.
        if let Err(e) = validate_op_params(op, &req.params) {
            return self
                .fail(
                    &sender,
                    &req,
                    invalid_params(cap_message(e.to_string())),
                    now,
                    out,
                )
                .await;
        }

        // 4. RUN the hook OUTSIDE any sqlite txn (run_hook is async). The stdin mirrors the
        //    lifecycle-hook I/O contract (provision/suspend/destroy): subscription + instance +
        //    params + host facts, so a real recipe op can target the provisioned service via
        //    `instance.handles`. The instance is `null` before provisioning (dummy ops ignore it).
        let hook_path = self.recipe.op_hook(op);
        let instance = self.load_instance(&req.subscription_id).await?;
        let input = json!({
            "subscription": {
                "id": req.subscription_id.clone(),
                "buyer_pubkey": sender_hex,
                "state": subscription_state,
            },
            "instance": instance,
            "op": req.op.clone(),
            "params": req.params.clone(),
            "host": {
                "backend": self.recipe.provisioning.backend,
                "isolation": self.recipe.provisioning.isolation,
                "tier": self.recipe.provisioning.tier,
                "os": self.recipe.os.supports,
                "resources": self.recipe.provisioning.resources,
            },
            "now": now,
        });
        match run_hook(&hook_path, &input, runner::DEFAULT_TIMEOUT).await {
            Ok(HookOutput { stdout_json }) => {
                // `op.result` ok requires an OBJECT `data`; valid-but-non-object hook output would
                // otherwise wedge the cached-resend path on a duplicate. Treat it as a hook failure.
                if !stdout_json.is_object() {
                    return self
                        .fail(
                            &sender,
                            &req,
                            hook_failed("operation hook did not return a JSON object".into()),
                            now,
                            out,
                        )
                        .await;
                }
                self.done(&sender, &req, stdout_json, now, out).await
            }
            Err(e) => {
                // A timeout vs any other failure (nonzero exit / cap breach / non-JSON). Neither is
                // a daemon wedge nor a handler Err — both commit a cached op.result error.
                let error = if is_timeout(&e) {
                    timeout_err()
                } else {
                    tracing::warn!(
                        request_id = %req.id,
                        subscription_id = %req.subscription_id,
                        op = %req.op,
                        hook = %hook_path.display(),
                        error = %e,
                        "operation hook failed"
                    );
                    hook_failed(HOOK_FAILED_MESSAGE.into())
                };
                self.fail(&sender, &req, error, now, out).await
            }
        }
    }

    /// Steps 1–3 (lookup → authorize → claim) in ONE serialized transaction, keyed
    /// `(sender, request_id)`. The store actor serializes transactions, so reading any existing
    /// invocation, reading the subscription for authorization, and (only if authorized) inserting
    /// the `RUNNING` claim all commit atomically — no other transaction interleaves.
    ///
    /// Ordering is load-bearing (gdu.3, spec §C):
    /// - A pre-existing row wins BEFORE auth: cached-resend must survive a later sub state change, so
    ///   a previously-authorized op whose sub since left ACTIVE still resends its cached DONE/ERROR
    ///   rather than a fresh reject.
    /// - With no row, the three auth rejects (unknown sub / not-owner / not-ACTIVE) return WITHOUT
    ///   inserting — an unauthenticated stranger persists nothing.
    /// - Only an authorized request inserts the claim, atomically with the ACTIVE read. This is the
    ///   TOCTOU guard: a sub cannot pass the ACTIVE check and then be suspended before the insert.
    ///   The concurrent-duplicate race (two fresh authorized requests) is caught by the top lookup —
    ///   or, defensively, by the `ON CONFLICT DO NOTHING` re-read — and the loser defers/resends.
    async fn claim(&self, sender_hex: &str, req: &OpRequest, now: i64) -> Result<Claim> {
        let (s, r, sub, op) = (
            sender_hex.to_string(),
            req.id.clone(),
            req.subscription_id.clone(),
            req.op.clone(),
        );
        self.store
            .transaction(move |tx| {
                // 1. LOOKUP existing — a cached terminal/in-flight state wins before auth.
                if let Some(claim) = lookup_existing(tx, &s, &r)? {
                    return Ok(claim);
                }
                // 2. AUTHORIZE against the subscription row WITHOUT inserting — the three auth
                //    rejects are row-free. The reply is IDENTICAL for a nonexistent sub and someone
                //    else's sub (both `Unauthorized`) — no existence leak.
                let sub_row: Option<(String, String)> = tx
                    .query_row(
                        "SELECT buyer_pubkey, state FROM subscription WHERE id=?1",
                        params![sub],
                        |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                                row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            ))
                        },
                    )
                    .optional()?;
                let Some((buyer, state)) = sub_row else {
                    return Ok(Claim::Unauthorized);
                };
                if buyer != s {
                    return Ok(Claim::Unauthorized);
                }
                if state != "ACTIVE" {
                    return Ok(Claim::NotActive);
                }
                // 3. AUTHORIZED → insert the RUNNING claim, atomically with the auth read above.
                let inserted = tx.execute(
                    "INSERT INTO op_invocation
                        (sender_pubkey, request_id, subscription_id, op, state, created_at)
                     VALUES (?1, ?2, ?3, ?4, 'RUNNING', ?5)
                     ON CONFLICT(sender_pubkey, request_id) DO NOTHING",
                    params![s, r, sub, op, now],
                )?;
                if inserted > 0 {
                    return Ok(Claim::Claimed {
                        subscription_state: state,
                    });
                }
                // A row appeared under the conflict guard (a concurrent authorized duplicate). The
                // serialized store makes the top lookup catch this first; ON CONFLICT keeps it
                // correct regardless. Re-read and classify exactly as the lookup does.
                lookup_existing(tx, &s, &r)?.ok_or_else(|| {
                    anyhow!("op_invocation conflict for ({s}, {r}) vanished on re-read")
                })
            })
            .await
    }

    /// Reply an `op.result` error WITHOUT committing a row — the row-free auth-reject path
    /// (gdu.3, spec §C). Distinct from [`Self::fail`], which ALSO commits a terminal ERROR row for
    /// the post-auth `unknown_op` / `invalid_params` / hook failures that need cached-resend.
    async fn reply_error(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        error: WireError,
        out: &dyn Outbound,
    ) -> Result<()> {
        out.reply(
            sender,
            &Msg::OpResult(OpResult::err(
                req.id.clone(),
                req.subscription_id.clone(),
                req.op.clone(),
                error,
            )),
        )
        .await?;
        Ok(())
    }

    /// The provisioned instance for `sub_id` as the hook's `instance` context (id, box_id, kind, the
    /// decoded backend `handles`, state), or `Value::Null` before provisioning. A real op targets
    /// the service via `instance.handles`. A read (no txn) — the claim already committed RUNNING.
    async fn load_instance(&self, sub_id: &str) -> Result<Value> {
        let id = sub_id.to_string();
        self.store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT id, box_id, kind, handles_json, state FROM instance
                      WHERE subscription_id=?1 LIMIT 1",
                    params![id],
                    |r| {
                        let handles_json: Option<String> = r.get(3)?;
                        Ok(json!({
                            "id": r.get::<_, String>(0)?,
                            "box_id": r.get::<_, Option<String>>(1)?,
                            "kind": r.get::<_, Option<String>>(2)?,
                            "handles": handles_json
                                .and_then(|h| serde_json::from_str::<Value>(&h).ok())
                                .unwrap_or(Value::Null),
                            "state": r.get::<_, Option<String>>(4)?,
                        }))
                    },
                )
                .optional()?
                .unwrap_or(Value::Null))
            })
            .await
    }

    /// Commit `DONE` (result_json = the hook's stdout JSON) and reply `op.result` ok.
    async fn done(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        data: Value,
        now: i64,
        out: &dyn Outbound,
    ) -> Result<()> {
        let result_json = serde_json::to_string(&data)?;
        self.commit_terminal(sender, &req.id, "DONE", Some(result_json), None, now)
            .await?;
        out.reply(
            sender,
            &Msg::OpResult(OpResult::ok(
                req.id.clone(),
                req.subscription_id.clone(),
                req.op.clone(),
                data,
            )),
        )
        .await?;
        Ok(())
    }

    /// Commit `ERROR` (error_json = `error`) and reply `op.result` err. A business failure is ALWAYS
    /// this path — a committed, cached error — never a handler `Err`.
    async fn fail(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        error: WireError,
        now: i64,
        out: &dyn Outbound,
    ) -> Result<()> {
        let error_json = serde_json::to_string(&error)?;
        self.commit_terminal(sender, &req.id, "ERROR", None, Some(error_json), now)
            .await?;
        self.reply_error(sender, req, error, out).await
    }

    /// The terminal commit (SPEC.md §7.4): flip OUR `RUNNING` row to `DONE`/`ERROR` with the cached
    /// payload + `finished_at`, in one transaction. Guarded on `state='RUNNING'` so it only ever
    /// finalizes the claim we hold (a concurrent recovery that already swept the row is a no-op).
    async fn commit_terminal(
        &self,
        sender: &PublicKey,
        request_id: &str,
        state: &str,
        result_json: Option<String>,
        error_json: Option<String>,
        now: i64,
    ) -> Result<()> {
        let (s, r, st) = (sender.to_hex(), request_id.to_string(), state.to_string());
        self.store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE op_invocation
                        SET state=?3, result_json=?4, error_json=?5, finished_at=?6
                      WHERE sender_pubkey=?1 AND request_id=?2 AND state='RUNNING'",
                    params![s, r, st, result_json, error_json, now],
                )?;
                Ok(())
            })
            .await
    }

    /// Startup recovery (SPEC.md §5.1, lnrent-7fp.20): a crash mid-op leaves an orphaned `RUNNING`
    /// row with no live task, so flip EVERY `RUNNING` row to `ERROR` with a cached
    /// `{code:"interrupted", retryable:false}` (neither re-run nor reported as success). Returns the
    /// count swept. lnrent-7fp.21 calls this at startup; this bead only exposes it.
    pub async fn recover_interrupted_ops(&self) -> Result<usize> {
        let now = self.clock.now();
        let error_json = serde_json::to_string(&interrupted())?;
        self.store
            .transaction(move |tx| {
                let n = tx.execute(
                    "UPDATE op_invocation
                        SET state='ERROR', error_json=?1, finished_at=?2
                      WHERE state='RUNNING'",
                    params![error_json, now],
                )?;
                Ok(n)
            })
            .await
    }
}

#[async_trait]
impl OpHandler for OpDispatch {
    async fn handle(&self, sender: PublicKey, msg: Msg, out: &dyn Outbound) -> Result<()> {
        // The engine only routes `op.request` here; an unexpected variant is a routing bug, so Err
        // (it won't be marked seen / cached).
        let Msg::OpRequest(req) = msg else {
            return Err(anyhow!(
                "op dispatch received {} (expected op.request)",
                msg.type_str()
            ));
        };
        self.dispatch(sender, req, out).await
    }
}

/// Read any existing `op_invocation` for `(sender_hex, request_id)` and classify it into the
/// resend/defer [`Claim`] variants — `Done`/`Errored`, else `Running` for a live (`RUNNING`, not
/// ours) or any unexpected non-terminal state. `None` = no row yet (proceed to auth + claim).
/// Shared by [`OpDispatch::claim`]'s top-of-txn lookup and its post-`INSERT` conflict re-read.
fn lookup_existing(tx: &Transaction, sender_hex: &str, request_id: &str) -> Result<Option<Claim>> {
    let row = tx
        .query_row(
            "SELECT state, result_json, error_json, subscription_id, op FROM op_invocation
                  WHERE sender_pubkey=?1 AND request_id=?2",
            params![sender_hex, request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()?;
    Ok(row.map(|(state, result_json, error_json, sub_db, op_db)| {
        match state.as_str() {
            "DONE" => Claim::Done {
                result_json: result_json.unwrap_or_default(),
                subscription_id: sub_db.unwrap_or_default(),
                op: op_db.unwrap_or_default(),
            },
            "ERROR" => Claim::Errored {
                error_json: error_json.unwrap_or_default(),
                subscription_id: sub_db.unwrap_or_default(),
                op: op_db.unwrap_or_default(),
            },
            // RUNNING (or any unexpected non-terminal): not ours, do not re-run.
            _ => Claim::Running,
        }
    }))
}

/// Validate `params` against the op's declared schema (§7.1), mirroring
/// [`crate::reservation::validate_params`]: params must be a JSON object, every `required` param
/// present with the right JSON type — and, since an op schema is closed, every supplied key must be
/// declared (reject unknown).
fn validate_op_params(op: &Operation, params: &Value) -> Result<()> {
    let Some(obj) = params.as_object() else {
        bail!("op params must be a JSON object");
    };
    for p in &op.params {
        match obj.get(&p.key) {
            None => {
                if p.required {
                    bail!("missing required param `{}`", p.key);
                }
            }
            Some(v) => {
                let ok = match p.ty.as_str() {
                    "string" => v.is_string(),
                    "number" | "int" | "integer" => v.is_number(),
                    "bool" | "boolean" => v.is_boolean(),
                    _ => true, // unknown declared type: accept (recipe's own concern)
                };
                if !ok {
                    bail!("param `{}` has the wrong type (expected {})", p.key, p.ty);
                }
            }
        }
    }
    for key in obj.keys() {
        if !op.params.iter().any(|p| &p.key == key) {
            bail!("unknown param `{key}`");
        }
    }
    Ok(())
}

/// True if `err` is a [`runner::run_hook`] timeout (vs a nonzero exit / cap breach / non-JSON). The
/// runner's timeout error uses the specific phrase "timed out after"; don't match the looser
/// "timed out" because a failing hook can print arbitrary stderr.
fn is_timeout(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains(" timed out after ") && !msg.contains(" failed (exit ")
}

/// Cap a buyer-facing failure message to [`MAX_ERROR_MESSAGE_CHARS`] before caching it.
fn cap_message(message: String) -> String {
    if message.chars().count() <= MAX_ERROR_MESSAGE_CHARS {
        message
    } else {
        let mut capped: String = message.chars().take(MAX_ERROR_MESSAGE_CHARS).collect();
        capped.push('…');
        capped
    }
}

// The `op.result` error codes this handler emits (SPEC.md §5.1, ADR-0014). `retryable` follows the
// nature of the failure: a client/auth/shape error is permanent; only a hook timeout is transient.
fn unauthorized() -> WireError {
    WireError {
        code: "unauthorized".into(),
        message: "not authorized for this subscription".into(),
        retryable: false,
    }
}
fn not_active() -> WireError {
    WireError {
        code: "not_active".into(),
        message: "subscription is not active".into(),
        retryable: false,
    }
}
fn unknown_op() -> WireError {
    WireError {
        code: "unknown_op".into(),
        message: "unknown management operation".into(),
        retryable: false,
    }
}
fn invalid_params(message: String) -> WireError {
    WireError {
        code: "invalid_params".into(),
        message,
        retryable: false,
    }
}
fn timeout_err() -> WireError {
    WireError {
        code: "timeout".into(),
        message: "operation hook timed out".into(),
        retryable: true,
    }
}
fn hook_failed(message: String) -> WireError {
    WireError {
        code: "hook_failed".into(),
        message,
        retryable: false,
    }
}
fn interrupted() -> WireError {
    WireError {
        code: "interrupted".into(),
        message: "operation interrupted by daemon restart".into(),
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::store::{Store, SCHEMA};
    use std::sync::Mutex;

    use lnrent_wire::{Keys, OpStatus};
    use nostr::EventId;
    use rusqlite::Connection;
    use serde_json::json;

    #[test]
    fn is_timeout_matches_only_the_runner_timeout_phrase() {
        // The runner's timeout error carries the specific phrase " timed out after ".
        assert!(super::is_timeout(&anyhow!(
            "operation hook timed out after 120s"
        )));
        // A nonzero-exit failure whose captured stderr merely contains "timed out after" must NOT
        // be misclassified as a (retryable) timeout — the " failed (exit " guard catches it.
        assert!(!super::is_timeout(&anyhow!(
            "operation hook failed (exit 3): upstream timed out after 5s"
        )));
        // A plain nonzero-exit failure.
        assert!(!super::is_timeout(&anyhow!(
            "operation hook failed (exit 1)"
        )));
    }

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    fn dummy_recipe() -> Recipe {
        Recipe::load(format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR")))
            .expect("dummy recipe")
    }

    fn dispatcher(store: Store, clock: TestClock, recipe: Recipe) -> OpDispatch {
        OpDispatch::new(store, Arc::new(clock), recipe)
    }

    /// A stub [`Outbound`] that records every `(recipient, msg)` instead of touching a relay.
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

    async fn seed_sub(store: &Store, id: &str, buyer_hex: &str, state: &str) {
        let (id, buyer, state) = (id.to_string(), buyer_hex.to_string(), state.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription
                        (id, recipe_id, buyer_pubkey, state, period_s, renew_lead_s, retention_s, created_at, updated_at)
                     VALUES (?1, 'dummy', ?2, ?3, 2592000, 604800, 604800, 0, 0)",
                    params![id, buyer, state],
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

    fn op_req(id: &str, sub: &str, op: &str, params: Value) -> Msg {
        Msg::OpRequest(OpRequest {
            id: id.into(),
            subscription_id: sub.into(),
            op: op.into(),
            params,
        })
    }

    fn expect_op_result(out: &RecordingOutbound) -> OpResult {
        match out.only().1 {
            Msg::OpResult(r) => r,
            other => panic!("expected op.result, got {other:?}"),
        }
    }

    // Test 1: an authorized `status` op runs the hook and replies exactly one op.result.ok carrying
    // the hook's data; a DONE op_invocation row is recorded.
    #[tokio::test]
    async fn authorized_status_runs_hook_and_records_done() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-1", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();

        let res = expect_op_result(&out);
        assert_eq!(res.status, OpStatus::Ok);
        assert_eq!(res.request_id, "op-1");
        assert_eq!(res.subscription_id, "sub-1");
        assert_eq!(res.op, "status");
        // The dummy `status` hook emits {"state":"running","uptime_s":42}.
        assert_eq!(res.data.as_ref().unwrap()["state"], json!("running"));

        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            1
        );
    }

    // Test 2: a DUPLICATE (sender, id) for a NON-idempotent op (`restart`) returns the SAME cached
    // op.result both times and runs the hook EXACTLY ONCE. Proof: the single op_invocation row's
    // finished_at stays the first call's time even though the clock advanced before the second call,
    // so the second call took the cached path and never re-ran the hook.
    #[tokio::test]
    async fn duplicate_nonidempotent_op_resends_cache_and_runs_hook_once() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let clock = TestClock::new(1000);
        let handler = dispatcher(store.clone(), clock.clone(), dummy_recipe());

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("dup", "sub-1", "restart", json!({})),
                &out,
            )
            .await
            .unwrap();
        // Advance time: a re-run would stamp a NEW finished_at; the cached path must not.
        clock.set(2000);
        handler
            .handle(
                buyer.public_key(),
                op_req("dup", "sub-1", "restart", json!({})),
                &out,
            )
            .await
            .unwrap();

        // Exactly one row, finalized at the FIRST call's time (the hook ran exactly once).
        let (rows, finished): (i64, i64) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT count(*), max(finished_at) FROM op_invocation WHERE state='DONE'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(rows, 1, "a duplicate must not open a second op_invocation");
        assert_eq!(
            finished, 1000,
            "the duplicate took the cache, never re-ran the hook"
        );

        // Both replies are the identical cached op.result.
        let msgs = out.messages();
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            msgs[0].1, msgs[1].1,
            "the duplicate resends the cached op.result"
        );
    }

    // Test 3: recover_interrupted_ops flips an orphaned RUNNING row to ERROR{interrupted,
    // retryable:false} and caches it; a later duplicate handle() resends that cached error and runs
    // NO hook.
    #[tokio::test]
    async fn recover_interrupted_caches_error_and_blocks_rerun() {
        let store = mem_store();
        let buyer = Keys::generate();
        let sender_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &sender_hex, "ACTIVE").await;

        // Seed an orphaned RUNNING op_invocation (a crash mid-op left no live task).
        {
            let s = sender_hex.clone();
            store
                .transaction(move |tx| {
                    tx.execute(
                        "INSERT INTO op_invocation
                            (sender_pubkey, request_id, subscription_id, op, state, created_at)
                         VALUES (?1, 'op-9', 'sub-1', 'restart', 'RUNNING', 500)",
                        params![s],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());
        let n = handler.recover_interrupted_ops().await.unwrap();
        assert_eq!(n, 1);

        let (state, error_json): (String, String) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state, error_json FROM op_invocation WHERE request_id='op-9'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(state, "ERROR");
        let err: WireError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(err.code, "interrupted");
        assert!(!err.retryable);

        // A duplicate of the recovered op resends the cached interrupted error and runs NO hook (a
        // re-run of `restart` would have produced a DONE row).
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-9", "sub-1", "restart", json!({})),
                &out,
            )
            .await
            .unwrap();
        let res = expect_op_result(&out);
        assert_eq!(res.status, OpStatus::Error);
        assert_eq!(res.error.as_ref().unwrap().code, "interrupted");
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 1);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            0
        );
    }

    // Test 4: an op from NOT-the-buyer for an EXISTING sub and for a NONEXISTENT sub BOTH reply
    // op.result.err "unauthorized" with NO hook, and the two error payloads are identical (no
    // existence leak).
    #[tokio::test]
    async fn unauthorized_is_identical_for_foreign_and_missing_sub() {
        let store = mem_store();
        let owner = Keys::generate();
        seed_sub(&store, "sub-1", &owner.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        let stranger = Keys::generate();

        // Existing sub, sender is not the buyer.
        let out_foreign = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                op_req("a", "sub-1", "status", json!({})),
                &out_foreign,
            )
            .await
            .unwrap();
        let foreign = expect_op_result(&out_foreign);

        // Nonexistent sub.
        let out_missing = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                op_req("b", "ghost", "status", json!({})),
                &out_missing,
            )
            .await
            .unwrap();
        let missing = expect_op_result(&out_missing);

        assert_eq!(foreign.status, OpStatus::Error);
        assert_eq!(missing.status, OpStatus::Error);
        assert_eq!(foreign.error.as_ref().unwrap().code, "unauthorized");
        assert_eq!(missing.error.as_ref().unwrap().code, "unauthorized");
        // No existence leak: the error payload is identical whether the sub is foreign or absent.
        assert_eq!(foreign.error, missing.error);

        // gdu.3: the auth rejects are ROW-FREE — neither request persisted an op_invocation row
        // (no hook ran, and nothing terminal was committed). A stranger leaves no durable artifact.
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 0);
    }

    // Test 5: an undeclared op name replies op.result.err "unknown_op" with no hook.
    #[tokio::test]
    async fn unknown_op_is_rejected_without_running_a_hook() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-x", "sub-1", "frobnicate", json!({})),
                &out,
            )
            .await
            .unwrap();
        let res = expect_op_result(&out);
        assert_eq!(res.status, OpStatus::Error);
        assert_eq!(res.error.as_ref().unwrap().code, "unknown_op");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            0
        );
    }

    // Test 6: an op on a non-ACTIVE (SUSPENDED) sub replies op.result.err "not_active" with no hook.
    #[tokio::test]
    async fn op_on_suspended_subscription_is_not_active() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "SUSPENDED").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-s", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        let res = expect_op_result(&out);
        assert_eq!(res.status, OpStatus::Error);
        assert_eq!(res.error.as_ref().unwrap().code, "not_active");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            0
        );
    }

    // Test 7: a hook that exits nonzero is a committed, cached op.result.err "hook_failed" — NOT a
    // handler Err / daemon wedge. The failing op is built ad-hoc in a temp recipe dir so no shared
    // recipe fixture is touched (file constraint).
    #[tokio::test]
    async fn hook_failure_is_a_cached_op_error_not_a_wedge() {
        use std::os::unix::fs::PermissionsExt;

        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;

        // An ad-hoc recipe whose `boom` op hook exits nonzero.
        let mut recipe = dummy_recipe();
        let dir = std::env::temp_dir().join(format!("lnrent-op-dispatch-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("ops")).unwrap();
        let boom = dir.join("ops").join("boom");
        std::fs::write(&boom, "#!/usr/bin/env bash\necho 'kaboom' >&2\nexit 3\n").unwrap();
        std::fs::set_permissions(&boom, std::fs::Permissions::from_mode(0o755)).unwrap();
        recipe.dir = dir;
        recipe.operations.push(Operation {
            name: "boom".into(),
            label: "Boom".into(),
            kind: "request".into(),
            hook: "boom".into(),
            params: vec![],
        });

        let handler = dispatcher(store.clone(), TestClock::new(1000), recipe);
        let out = RecordingOutbound::default();
        // handle() returns Ok despite the hook failing — never an Err (no daemon wedge).
        handler
            .handle(
                buyer.public_key(),
                op_req("op-b", "sub-1", "boom", json!({})),
                &out,
            )
            .await
            .unwrap();

        let res = expect_op_result(&out);
        assert_eq!(res.status, OpStatus::Error);
        let err = res.error.as_ref().unwrap();
        assert_eq!(err.code, "hook_failed");
        assert_eq!(err.message, HOOK_FAILED_MESSAGE);
        assert!(!err.message.contains("kaboom"));
        assert!(!err.message.contains("lnrent-op-dispatch"));
        // Committed as a terminal ERROR row (cached for a duplicate resend).
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='ERROR'"
            )
            .await,
            1
        );
    }

    // gdu.3 Test: each of the three AUTH rejects (unknown sub / non-owner / non-ACTIVE) replies the
    // same reject as before AND writes NO op_invocation row — an unauthenticated stranger's
    // op.request leaves no durable artifact (row-free auth gate, before the claim).
    #[tokio::test]
    async fn auth_rejects_are_row_free() {
        let store = mem_store();
        let owner = Keys::generate();
        seed_sub(&store, "sub-1", &owner.public_key().to_hex(), "ACTIVE").await;
        seed_sub(
            &store,
            "sub-susp",
            &owner.public_key().to_hex(),
            "SUSPENDED",
        )
        .await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());
        let stranger = Keys::generate();

        // (a) unknown sub → unauthorized.
        let out = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                op_req("a", "ghost", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            expect_op_result(&out).error.as_ref().unwrap().code,
            "unauthorized"
        );

        // (b) existing sub, sender is not the owner → unauthorized.
        let out = RecordingOutbound::default();
        handler
            .handle(
                stranger.public_key(),
                op_req("b", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            expect_op_result(&out).error.as_ref().unwrap().code,
            "unauthorized"
        );

        // (c) owner's own sub, but not ACTIVE → not_active.
        let out = RecordingOutbound::default();
        handler
            .handle(
                owner.public_key(),
                op_req("c", "sub-susp", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            expect_op_result(&out).error.as_ref().unwrap().code,
            "not_active"
        );

        // None of the three persisted an op_invocation row.
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 0);
    }

    // gdu.3 Test: PAST-AUTH rejects still commit a terminal ERROR row (cached-resend, unchanged).
    // An authorized sender whose op is unknown (`unknown_op`) or whose params are invalid
    // (`invalid_params`) is past the row-free auth gate, so each persists an ERROR op_invocation.
    #[tokio::test]
    async fn authorized_unknown_op_and_invalid_params_still_commit_error_row() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        // Authorized, unknown op name → unknown_op.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("u", "sub-1", "frobnicate", json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            expect_op_result(&out).error.as_ref().unwrap().code,
            "unknown_op"
        );

        // Authorized, unknown param on a real op (closed schema) → invalid_params.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("i", "sub-1", "status", json!({"nope": 1})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(
            expect_op_result(&out).error.as_ref().unwrap().code,
            "invalid_params"
        );

        // Both past-auth rejects persisted a terminal ERROR row (cached for a duplicate resend).
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='ERROR'"
            )
            .await,
            2
        );
    }

    // gdu.3 Test: cached-resend survives a sub state change. An authorized op runs to DONE; the sub
    // is THEN suspended; a retry of the SAME (sender, request_id) resends the cached DONE result —
    // NOT a fresh not_active reject (the lookup wins BEFORE the auth gate).
    #[tokio::test]
    async fn cached_done_resends_after_sub_suspended() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        // First call: authorized `status` → DONE + cached result.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-1", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        let first = expect_op_result(&out);
        assert_eq!(first.status, OpStatus::Ok);

        // Suspend the sub AFTER the op completed.
        store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE subscription SET state='SUSPENDED' WHERE id='sub-1'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        // Retry the SAME op id: the cached DONE is resent (status Ok, same data), not not_active.
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("op-1", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        let retry = expect_op_result(&out);
        assert_eq!(
            retry.status,
            OpStatus::Ok,
            "cached DONE must resend despite the sub leaving ACTIVE"
        );
        assert_eq!(retry.data, first.data);

        // Still exactly one op_invocation row, DONE — no fresh reject row was written.
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 1);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            1
        );
    }

    // gdu.3 Test: auth+claim TOCTOU contract. `claim()` folds the ACTIVE read and the RUNNING insert
    // into ONE serialized txn, so authorization gates the durable claim atomically. Asserted
    // directly: an ACTIVE sub yields `Claimed` AND a persisted RUNNING row; a non-ACTIVE sub yields
    // `NotActive` AND NO row. Under the OLD order (claim-then-auth) the non-ACTIVE case would leave a
    // RUNNING row — it must not, so a sub that leaves ACTIVE before the insert never gets a hooked run.
    #[tokio::test]
    async fn claim_folds_auth_into_the_insert_txn() {
        let store = mem_store();
        let buyer = Keys::generate();
        let sender_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-active", &sender_hex, "ACTIVE").await;
        seed_sub(&store, "sub-susp", &sender_hex, "SUSPENDED").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        // ACTIVE → Claimed AND a RUNNING row persisted (the claim committed inside the same txn).
        let req_a = OpRequest {
            id: "op-a".into(),
            subscription_id: "sub-active".into(),
            op: "status".into(),
            params: json!({}),
        };
        match handler.claim(&sender_hex, &req_a, 1000).await.unwrap() {
            Claim::Claimed { subscription_state } => assert_eq!(subscription_state, "ACTIVE"),
            other => panic!("expected Claimed, got {other:?}"),
        }
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='RUNNING'"
            )
            .await,
            1
        );

        // SUSPENDED → NotActive AND NO row — auth gated the insert within the one txn.
        let req_s = OpRequest {
            id: "op-s".into(),
            subscription_id: "sub-susp".into(),
            op: "status".into(),
            params: json!({}),
        };
        assert!(matches!(
            handler.claim(&sender_hex, &req_s, 1000).await.unwrap(),
            Claim::NotActive
        ));
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE request_id='op-s'"
            )
            .await,
            0
        );
    }
}
