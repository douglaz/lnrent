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
//! A concurrent duplicate of an op that is STILL RUNNING in THIS process ATTACHES to the in-flight
//! invocation (a small registry INTERNAL to [`OpDispatch`], lnrent-c95) and is served that
//! invocation's terminal result the moment it commits — the hook still runs AT MOST ONCE (only the
//! claim owner runs it; duplicates only await). A duplicate that finds a `RUNNING` row but NO
//! in-process handle still defers by returning `Err` so the transport reprocesses the wrap later
//! and re-reads the authoritative row (see [`OpDispatch::attach_or_defer`] for the causes that
//! actually reach that arm). An owner that exits WITHOUT a committed terminal (early error,
//! panic, task cancellation) wakes its attached duplicates into that SAME defer rather than a
//! fabricated terminal — only the durable row is authoritative. A hook failure is a committed,
//! cached `op.result` error, never a handler `Err` (which would re-run the hook on transport retry)
//! and never a daemon wedge.
//!
//! Production wiring is lnrent-7fp.21's and already shipped: the supervisor constructs this handler
//! and awaits [`OpDispatch::recover_interrupted_ops`] during boot recovery.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use tokio::sync::watch;

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
    /// lnrent-c95 in-flight registry (see [`InflightRegistry`]). `Default`-initialized so it stays
    /// OUT of the public `new`/supervisor wiring — a single shared `OpDispatch` whose `handle` is
    /// called concurrently is all this needs.
    inflight: InflightRegistry,
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
    /// A concurrent duplicate found a `RUNNING` row (not a fresh terminal). ATTACH to the in-flight
    /// owner if it lives in THIS process (lnrent-c95), else defer-and-retry. NEVER re-runs.
    Running,
    /// Auth reject (no row existed): unknown sub OR not the owner. Reply `unauthorized`, persist
    /// NOTHING — an unauthenticated stranger leaves no durable artifact (gdu.3).
    Unauthorized,
    /// Auth reject (no row existed): the owner's sub is not `ACTIVE`. Reply `not_active`, persist
    /// NOTHING (gdu.3).
    NotActive,
}

/// A CLONE-able snapshot of an owner invocation's outcome, published on the in-flight registry's
/// `watch` so a concurrent IN-PROCESS duplicate (lnrent-c95) can build the SAME `op.result` the
/// owner committed — WITHOUT re-running the hook. `Done`/`Errored` mirror the cached-resend
/// [`Claim`] variants (STORED subscription_id/op, carrying the SAME result/error JSON the owner
/// cached so the duplicate's reply is byte-identical) and are published ONLY after the durable
/// terminal commit returned `Ok`. Those labels come from the owner's in-memory `req`, not from a
/// re-read of the row, and that IS the stored pair: ONE `req` feeds both the claim `INSERT` (see
/// [`OpDispatch::claim`], which writes `req.subscription_id`/`req.op` verbatim) and this publish.
///
/// `NoTerminal` is what the owner's [`InflightGuard`] publishes if it exits WITHOUT having OBSERVED
/// a committed terminal (store error / early return / panic / TASK CANCELLATION), and it wakes the
/// duplicate into the same `Err`-DEFER the orphan path uses — deliberately NOT a fabricated
/// `interrupted()` reply. **A cancelled owner can still commit**: the store actor runs a queued job
/// even when its caller was cancelled, so an owner aborted inside `commit_terminal` may yet make
/// `DONE`/`ERROR` durable. Fabricating a terminal there would CONTRADICT the durable cache;
/// deferring instead makes the next redelivery read the authoritative row. This is the reason
/// `NoTerminal` never becomes a reply — referenced, not restated, at its other use sites.
#[derive(Clone)]
enum InflightOutcome {
    Done {
        result_json: String,
        subscription_id: String,
        op: String,
    },
    Errored {
        error_json: String,
        subscription_id: String,
        op: String,
    },
    NoTerminal,
}

/// In-flight op registry (lnrent-c95), INTERNAL to [`OpDispatch`] (never threaded through its public
/// `new`/supervisor wiring): `(sender_hex, request_id)` → a `watch` broadcasting the owner's
/// terminal [`InflightOutcome`]. A `watch` (not a oneshot) so a LATE subscriber still reads the
/// final value. The `std::sync::Mutex` is held ONLY for map insert/lookup/remove — NEVER across an
/// `.await` (see [`OpDispatch::attach_or_defer`]).
type InflightRegistry =
    Arc<StdMutex<HashMap<(String, String), watch::Sender<Option<InflightOutcome>>>>>;

/// Publishes the owner invocation's [`InflightOutcome`] to any attached duplicate AND removes the
/// registry entry on EVERY owner exit path (lnrent-c95 concurrency invariants):
/// - a normal terminal calls [`InflightGuard::publish`] with the COMMITTED outcome;
/// - ANY other exit (store-layer `Err`, `?`-propagation, panic, task cancellation) runs `Drop`,
///   which publishes `NoTerminal` — so an awaiting duplicate can NEVER hang; it defers instead —
///   and removes the entry, so the registry never leaks.
///
/// The entry is inserted by [`InflightGuard::register`], never by a caller: the map insert and the
/// guard that owns its removal are created TOGETHER, so both invariants hold BY CONSTRUCTION.
struct InflightGuard {
    registry: InflightRegistry,
    key: (String, String),
    tx: watch::Sender<Option<InflightOutcome>>,
    published: bool,
}

impl InflightGuard {
    /// Insert a fresh in-flight entry for `key` AND return the guard that owns its cleanup — the
    /// ONLY way an entry enters the registry (lnrent-c95). The two steps live in one infallible,
    /// non-yielding constructor rather than in separate caller statements so no future `.await`,
    /// `?`, or panic can ever land BETWEEN them: an entry inserted without its guard would strand a
    /// live `watch::Sender` in the map forever with nothing left to publish `NoTerminal`, hanging
    /// every duplicate that later subscribes. Same standard as [`Self::publish`]'s flag-first order:
    /// hold the invariant by construction, not by auditing the call site.
    fn register(registry: &InflightRegistry, key: (String, String)) -> Self {
        let (tx, _) = watch::channel::<Option<InflightOutcome>>(None);
        registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone(), tx.clone());
        Self {
            registry: registry.clone(),
            key,
            tx,
            published: false,
        }
    }

    /// Publish the owner's committed terminal outcome, then drop the entry. Marks the guard before
    /// the owner's fallible reply so cancellation or relay failure cannot overwrite the durable
    /// terminal with a spurious `NoTerminal` defer.
    fn publish(&mut self, outcome: InflightOutcome) {
        // Marked FIRST, before either side effect: `published` means "this guard owns a committed
        // terminal, so `Drop` must not overwrite it with `NoTerminal`". Setting it after the send
        // would leave a window where an unwind between the two runs `Drop`, replacing a committed
        // `Done`/`Errored` with a defer. Unreachable today (neither statement below can panic), but
        // the flag-first order makes the invariant hold by construction rather than by audit.
        self.published = true;
        // `send_replace` (NOT `send`): store the outcome UNCONDITIONALLY, even when no duplicate has
        // subscribed yet. `send` drops the value and errs when `receiver_count() == 0`, which opens a
        // race — a duplicate that subscribes in the window between this store and the `remove` below
        // would then read the stale `None` and, on the channel closing, needlessly defer an op that
        // actually committed a terminal it could have been served. `send_replace` closes that: the
        // value is durable in the `watch` until the entry is removed, so any pre-remove subscriber
        // reads the real outcome; a post-remove lookup finds no handle and defers (the SAFE window).
        let _ = self.tx.send_replace(Some(outcome));
        remove_inflight(&self.registry, &self.key);
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if !self.published {
            // The owner exited WITHOUT observing a committed terminal (store error / early return /
            // panic / task cancellation): wake any attached duplicate with `NoTerminal` (never leave
            // it hanging) so it DEFERS, then remove the entry. It must not fabricate a terminal: a
            // cancelled owner's store job is already queued and can still commit `DONE`/`ERROR`, so
            // only the durable row is authoritative (see [`InflightOutcome`]). Otherwise the row
            // stays `RUNNING` for `recover_interrupted_ops` — unchanged.
            // `send_replace` (not `send`) for the same reason as `publish`: store the outcome even
            // with zero current subscribers so a duplicate racing the remove reads it, not a hang.
            let _ = self.tx.send_replace(Some(InflightOutcome::NoTerminal));
            remove_inflight(&self.registry, &self.key);
        }
    }
}

/// Remove an in-flight registry entry, recovering from a poisoned lock: `Drop` must never
/// double-panic, and a poisoned map must still drop the entry.
fn remove_inflight(registry: &InflightRegistry, key: &(String, String)) {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(key);
}

impl OpDispatch {
    pub fn new(store: Store, clock: Arc<dyn Clock>, recipe: Recipe) -> Self {
        Self {
            store,
            clock,
            recipe,
            inflight: InflightRegistry::default(),
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

        // 0. Structural id gate (mi9.2/DRIFT-3, gate0-abuse-resistance §D): the same 1..=128 /
        //    [A-Za-z0-9_-] bound the order path enforces, BEFORE the durable claim — a malformed
        //    buyer-chosen id never persists an op_invocation row. A DISTINCT pre-lookup code (NOT
        //    the post-auth `invalid_params`): nothing was looked up, let alone authorized.
        if crate::order_intake::validate_buyer_request_id_tail(&req.id).is_err() {
            return self
                .reply_error(&sender, &req, invalid_request_id(), out)
                .await;
        }

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
                // Cached success: resend the STORED result (see `resend_done`). Echo the STORED
                // subscription_id/op so a reused id can't relabel the cached reply.
                return self
                    .resend_done(&sender, &req.id, subscription_id, op, &result_json, out)
                    .await;
            }
            Claim::Errored {
                error_json,
                subscription_id,
                op,
            } => {
                return self
                    .resend_errored(&sender, &req.id, subscription_id, op, &error_json, out)
                    .await;
            }
            Claim::Running => {
                // lnrent-c95: a duplicate found a RUNNING row (not a fresh terminal). Attach to the
                // in-flight owner if it lives in THIS process (serve its result the moment it
                // commits), else defer so a redelivery re-reads the durable row. NEVER re-runs the
                // hook and NEVER commits terminal state — see `attach_or_defer`.
                return self.attach_or_defer(&sender, &req, &sender_hex, out).await;
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

        // From here the RUNNING claim is OURS and the sender is AUTHORIZED (gdu.3). lnrent-c95:
        // register this invocation in the in-flight registry BEFORE running the hook, so a concurrent
        // IN-PROCESS duplicate (a `Claim::Running` that finds this handle) ATTACHES to our result
        // instead of Err-deferring. `guard` publishes our terminal `InflightOutcome` and removes the
        // entry on EVERY owner exit path — the normal terminal via `publish`, and any other exit
        // (store `Err`, `?`, panic, task cancellation) via its `Drop`, which publishes `NoTerminal`
        // so an attached duplicate can NEVER hang (it defers instead).
        //
        // INVARIANTS (lnrent-c95): the hook runs AT MOST ONCE — only this owner path runs it
        // (`run_owned`); duplicates only await. No lock is held across an `.await` (the map mutex is
        // insert/lookup/remove-only). The registry never leaks (removed on every exit) — `register`
        // inserts the entry and builds its guard in ONE step, so neither can exist without the other.
        // A store-layer error still propagates `Err`, leaving the row RUNNING for the next startup's
        // `recover_interrupted_ops` — unchanged. NARROW, SAFE window (no locking added to close it): a
        // duplicate that looks up between this claim-commit and the `register` below — or after the
        // owner's publish+remove — finds no handle and defers, getting the committed terminal on the
        // next redelivery.
        let mut guard = InflightGuard::register(&self.inflight, (sender_hex, req.id.clone()));
        // `run_owned` resolves/validates/runs the hook and commits+replies the OWNER's terminal. Its
        // terminal helpers publish through `guard` immediately AFTER the durable commit and BEFORE
        // awaiting the owner's fallible relay reply. On `?` before a commit, `guard` drops without a
        // terminal and publishes `NoTerminal`, deferring any attached duplicate.
        self.run_owned(&sender, &req, subscription_state, now, out, &mut guard)
            .await
    }

    /// The owner-only tail of [`Self::dispatch`] once WE hold the `RUNNING` claim (lnrent-c95
    /// extracted it so the in-flight registry can wrap it): resolve → validate → run the hook outside
    /// any txn → commit the terminal `DONE`/`ERROR`, publish it to attached duplicates, and reply to
    /// the OWNER. Publication happens after the commit but before fallible owner reply I/O. A
    /// store-layer error (a failed read or terminal commit) instead propagates `Err`, leaving the row
    /// `RUNNING`; `recover_interrupted_ops` flips it on the next startup, and `guard` publishes
    /// `NoTerminal` on drop (deferring any attached duplicate). The hook runs AT MOST ONCE — only
    /// this path runs it.
    async fn run_owned(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        subscription_state: String,
        now: i64,
        out: &dyn Outbound,
        guard: &mut InflightGuard,
    ) -> Result<()> {
        let sender_hex = sender.to_hex();
        // 2. RESOLVE the op. Unknown, or a non-`request` kind (interactive is out of scope here), is
        //    `unknown_op`. Past auth → STILL commits a terminal ERROR row (cached-resend), unchanged.
        //    Hook-name safety / `ops/`-containment was enforced at load by Recipe::validate
        //    (lnrent-7fp.6) — not re-checked here.
        let Some(op) = self.recipe.operation(&req.op) else {
            return self.fail(sender, req, unknown_op(), now, out, guard).await;
        };
        if op.kind != "request" {
            return self.fail(sender, req, unknown_op(), now, out, guard).await;
        }

        // 3. VALIDATE params against the op schema (reject unknown/missing/mistyped). Past auth →
        //    STILL commits a terminal ERROR row (cached-resend), unchanged.
        if let Err(e) = validate_op_params(op, &req.params) {
            return self
                .fail(
                    sender,
                    req,
                    invalid_params(cap_message(e.to_string())),
                    now,
                    out,
                    guard,
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
        match run_hook(
            &hook_path,
            &input,
            runner::DEFAULT_TIMEOUT,
            &self.recipe.provisioning.env,
        )
        .await
        {
            Ok(HookOutput { stdout_json }) => {
                // `op.result` ok requires an OBJECT `data`; valid-but-non-object hook output would
                // otherwise wedge the cached-resend path on a duplicate. Treat it as a hook failure.
                if !stdout_json.is_object() {
                    return self
                        .fail(
                            sender,
                            req,
                            hook_failed("operation hook did not return a JSON object".into()),
                            now,
                            out,
                            guard,
                        )
                        .await;
                }
                self.done(sender, req, stdout_json, now, out, guard).await
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
                self.fail(sender, req, error, now, out, guard).await
            }
        }
    }

    /// Build + send the cached `op.result` OK for a `DONE` invocation — shared by the [`Claim::Done`]
    /// cached-resend arm and the lnrent-c95 in-flight attach path, so an attached duplicate's reply
    /// is byte-identical to the owner's committed result. `subscription_id`/`op` are the STORED ones
    /// (an echoed reused id can't relabel the cached reply). A `DONE` `result_json` always holds an
    /// object `data` (non-object hook output is rejected before commit), so the decode is safe; fall
    /// back defensively.
    async fn resend_done(
        &self,
        sender: &PublicKey,
        request_id: &str,
        subscription_id: String,
        op: String,
        result_json: &str,
        out: &dyn Outbound,
    ) -> Result<()> {
        let data = serde_json::from_str::<Value>(result_json)
            .ok()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        out.reply(
            sender,
            &Msg::OpResult(OpResult::ok(
                request_id.to_string(),
                subscription_id,
                op,
                data,
            )),
        )
        .await?;
        Ok(())
    }

    /// Build + send the cached `op.result` err for an `ERROR` invocation — shared by the
    /// [`Claim::Errored`] cached-resend arm and the lnrent-c95 attach path (STORED subscription_id/op,
    /// same `error_json`).
    async fn resend_errored(
        &self,
        sender: &PublicKey,
        request_id: &str,
        subscription_id: String,
        op: String,
        error_json: &str,
        out: &dyn Outbound,
    ) -> Result<()> {
        let error = serde_json::from_str::<WireError>(error_json).unwrap_or_else(|_| interrupted());
        out.reply(
            sender,
            &Msg::OpResult(OpResult::err(
                request_id.to_string(),
                subscription_id,
                op,
                error,
            )),
        )
        .await?;
        Ok(())
    }

    /// [`Claim::Running`] resolution (lnrent-c95). Look the `(sender_hex, request_id)` key up in the
    /// in-flight registry:
    /// - PRESENT → the first attempt is STILL RUNNING in THIS process. Subscribe to its terminal
    ///   `watch` — dropping the map lock BEFORE awaiting, so the lock is NEVER held across an
    ///   `.await` — await the owner's [`InflightOutcome`], then reply to the DUPLICATE's sender with
    ///   the SAME `op.result` the owner committed (STORED subscription_id/op, not the duplicate's).
    ///   The hook is NOT re-run and NO terminal state is committed — only the owner commits. If the
    ///   owner exits WITHOUT a committed terminal it publishes [`InflightOutcome::NoTerminal`] and
    ///   we fall back to the same `Err`-defer as below (never a fabricated terminal).
    ///   COST: parking holds this wrap's inbound concurrency permit for the REST of the owner's run,
    ///   where the pre-c95 defer released it after one store round-trip. Deliberate — that wait is
    ///   exactly what buys a served buyer instead of silence until redelivery — and it is bounded by
    ///   the owner's OWN occupancy of its slot: the wait ends at the owner's terminal (which the hook
    ///   timeout bounds) and the drop guard covers every non-terminal exit. The full permit-budget
    ///   trade-off is recorded in the lnrent-c95 bead/PR, not frozen here.
    /// - ABSENT → re-read the authoritative row, because the owner may have committed and removed its
    ///   handle since [`Self::claim`] returned [`Claim::Running`]. Resend a terminal immediately; if
    ///   the row is still `RUNNING` — a true orphan, or the narrow claim-commit → registry-insert
    ///   window — keep the pre-c95 `Err`-defer (no reply, wrap NOT marked seen). Deferring rather
    ///   than replying or committing is what preserves crash recovery and must not regress: a stale
    ///   `RUNNING` row is flipped to a cached `interrupted` error by the next startup's
    ///   `recover_interrupted_ops`.
    async fn attach_or_defer(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        sender_hex: &str,
        out: &dyn Outbound,
    ) -> Result<()> {
        let key = (sender_hex.to_string(), req.id.clone());
        // Lock ONLY to subscribe; drop it (end of block) BEFORE the await below.
        let maybe_rx = {
            let map = self
                .inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.get(&key).map(watch::Sender::subscribe)
        };
        let Some(mut rx) = maybe_rx else {
            // The claim can be stale: the owner may have committed and publish+removed between the
            // claim transaction and this registry lookup. Re-read the durable row and serve such a
            // terminal immediately through the SAME cached-resend helpers. This read never claims,
            // runs, or commits anything, so hook-at-most-once remains owner-only.
            match self.reread_existing(sender_hex, &req.id).await? {
                Some(Claim::Done {
                    result_json,
                    subscription_id,
                    op,
                }) => {
                    return self
                        .resend_done(sender, &req.id, subscription_id, op, &result_json, out)
                        .await;
                }
                Some(Claim::Errored {
                    error_json,
                    subscription_id,
                    op,
                }) => {
                    return self
                        .resend_errored(sender, &req.id, subscription_id, op, &error_json, out)
                        .await;
                }
                // Still RUNNING (or defensively absent/non-terminal): preserve the pre-c95 defer.
                // This includes a true orphan and the explicitly acceptable claim-commit →
                // registry-insert window. No lock is held across this store await.
                _ => {}
            }
            return Err(anyhow!(
                "op.request {} from {sender_hex} is already RUNNING with no in-process handle \
                 (owner not registered here, or exited without committing a terminal); deferring",
                req.id
            ));
        };
        // PRESENT → attach. `info`, not `debug`: this is the only silent registry path (both defer
        // arms already warn), and it can park a wrap for the owner's remaining run — log it while it
        // is parked, not after. The `duplicate_*` labels are the DUPLICATE's own; the reply below
        // uses the owner's STORED subscription_id/op (see [`Claim::Done`]), which for a reused
        // request_id can legitimately differ.
        tracing::info!(
            request_id = %req.id,
            duplicate_subscription_id = %req.subscription_id,
            duplicate_op = %req.op,
            "duplicate op.request attached to the in-flight invocation"
        );
        // Await the owner's outcome. `borrow_and_update` reads-and-marks the current value, so a
        // terminal published before we subscribed is seen immediately; otherwise `changed()` blocks
        // until the owner publishes (or its drop guard publishes `NoTerminal`).
        let outcome = loop {
            if let Some(outcome) = rx.borrow_and_update().clone() {
                break outcome;
            }
            if rx.changed().await.is_err() {
                // The owner dropped its sender without publishing. The drop guard always publishes
                // `NoTerminal` FIRST, so this is only reachable defensively — treat it the same.
                break InflightOutcome::NoTerminal;
            }
        };
        match outcome {
            InflightOutcome::Done {
                result_json,
                subscription_id,
                op,
            } => {
                self.resend_done(sender, &req.id, subscription_id, op, &result_json, out)
                    .await
            }
            InflightOutcome::Errored {
                error_json,
                subscription_id,
                op,
            } => {
                self.resend_errored(sender, &req.id, subscription_id, op, &error_json, out)
                    .await
            }
            // The owner exited WITHOUT observing a committed terminal. There is nothing authoritative
            // to echo AND we must not invent one: a cancelled owner's queued store job can still
            // commit `DONE`/`ERROR` after this point, so replying a permanent `interrupted` could
            // contradict the durable cache. DEFER exactly like the orphan arm above — no reply, not
            // marked seen — and let the next redelivery read the row (cached terminal, or another
            // defer until `recover_interrupted_ops` resolves it). This is also precisely the pre-c95
            // behavior for this case, so it regresses nothing.
            InflightOutcome::NoTerminal => Err(anyhow!(
                "op.request {} from {sender_hex} is still RUNNING (in-flight owner exited without a committed terminal); deferring",
                req.id
            )),
        }
    }

    /// Re-read an invocation after a registry miss without claiming or authorizing anything. This
    /// closes the commit+publish/remove race in [`Self::attach_or_defer`]: only a durable terminal is
    /// served; a still-`RUNNING` or absent row remains a safe defer.
    async fn reread_existing(&self, sender_hex: &str, request_id: &str) -> Result<Option<Claim>> {
        let sender_hex = sender_hex.to_string();
        let request_id = request_id.to_string();
        self.store
            .read(move |connection| lookup_existing(connection, &sender_hex, &request_id))
            .await
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

    /// Commit `DONE` (result_json = the hook's stdout JSON) and reply `op.result` ok to the OWNER
    /// after publishing [`InflightOutcome::Done`] to attached duplicates. The outcome carries the
    /// SAME `result_json` so the duplicate rebuilds a byte-identical reply via
    /// [`Self::resend_done`]. Publishing before the fallible owner reply prevents a committed result
    /// from being replaced by the guard's fallback `NoTerminal` defer.
    async fn done(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        data: Value,
        now: i64,
        out: &dyn Outbound,
        guard: &mut InflightGuard,
    ) -> Result<()> {
        let result_json = serde_json::to_string(&data)?;
        self.commit_terminal(
            sender,
            &req.id,
            "DONE",
            Some(result_json.clone()),
            None,
            now,
        )
        .await?;
        guard.publish(InflightOutcome::Done {
            result_json,
            subscription_id: req.subscription_id.clone(),
            op: req.op.clone(),
        });
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

    /// Commit `ERROR` (error_json = `error`) and reply `op.result` err to the OWNER. A business
    /// failure is ALWAYS this path — a committed, cached error — never a handler `Err`. Publishes
    /// [`InflightOutcome::Errored`] immediately after the commit and before the fallible owner reply,
    /// carrying the SAME `error_json` so the duplicate rebuilds a byte-identical reply.
    async fn fail(
        &self,
        sender: &PublicKey,
        req: &OpRequest,
        error: WireError,
        now: i64,
        out: &dyn Outbound,
        guard: &mut InflightGuard,
    ) -> Result<()> {
        let error_json = serde_json::to_string(&error)?;
        self.commit_terminal(
            sender,
            &req.id,
            "ERROR",
            None,
            Some(error_json.clone()),
            now,
        )
        .await?;
        guard.publish(InflightOutcome::Errored {
            error_json,
            subscription_id: req.subscription_id.clone(),
            op: req.op.clone(),
        });
        self.reply_error(sender, req, error, out).await
    }

    /// The terminal commit (SPEC.md §7.4): flip OUR `RUNNING` row to `DONE`/`ERROR` with the cached
    /// payload + `finished_at`, in one transaction. Guarded on `state='RUNNING'` so it only ever
    /// finalizes the claim we hold — a row that already left `RUNNING` matches nothing, and that is
    /// caught below rather than passed off as a successful commit.
    ///
    /// `Ok` here means the row DID leave `RUNNING`, which is what lets [`InflightGuard::publish`]
    /// treat the outcome as durable — and lnrent-c95 widened that guarantee from the owner's single
    /// reply to a broadcast at every attached duplicate. So the affected-row count is CHECKED: on
    /// every live path it is exactly `1`, and a `0` would mean the row we own is no longer ours to
    /// finalize, so nothing we could publish is authoritative. `Err` instead, which drops `guard`
    /// unpublished → `NoTerminal` → owner and duplicates both DEFER to the durable row. A cheap
    /// assertion of the standing rule: only the row is authoritative, never a fabricated terminal.
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
                let updated = tx.execute(
                    "UPDATE op_invocation
                        SET state=?3, result_json=?4, error_json=?5, finished_at=?6
                      WHERE sender_pubkey=?1 AND request_id=?2 AND state='RUNNING'",
                    params![s, r, st, result_json, error_json, now],
                )?;
                if updated == 0 {
                    bail!("op_invocation ({s}, {r}) left RUNNING before its {st} commit");
                }
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
/// Shared by [`OpDispatch::claim`]'s two transaction lookups and the registry-miss terminal re-read
/// in [`OpDispatch::reread_existing`].
fn lookup_existing(
    connection: &rusqlite::Connection,
    sender_hex: &str,
    request_id: &str,
) -> Result<Option<Claim>> {
    let row = connection
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
            // RUNNING (or any unexpected non-terminal): a live/orphaned claim — dispatch will
            // attach-or-defer (lnrent-c95), never re-run.
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
fn invalid_request_id() -> WireError {
    WireError {
        code: "invalid_request_id".into(),
        message: "request id must be 1..=128 chars using only [A-Za-z0-9_-]".into(),
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
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

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

    /// Fails the owner's first reply after its terminal commit, then records later replies. This
    /// models fallible relay I/O while an in-process duplicate is already attached.
    #[derive(Default)]
    struct FailFirstOutbound {
        attempts: AtomicUsize,
        sent: Mutex<Vec<(PublicKey, Msg)>>,
    }
    #[async_trait]
    impl Outbound for FailFirstOutbound {
        async fn reply(&self, recipient: &PublicKey, msg: &Msg) -> Result<EventId> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                bail!("simulated owner reply failure");
            }
            self.sent.lock().unwrap().push((*recipient, msg.clone()));
            Ok(EventId::all_zeros())
        }
    }
    impl FailFirstOutbound {
        fn messages(&self) -> Vec<(PublicKey, Msg)> {
            self.sent.lock().unwrap().clone()
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

    // mi9.2/DRIFT-3: a malformed buyer-chosen id is rejected BEFORE the durable claim with the
    // DISTINCT pre-lookup `invalid_request_id` code — row-free, like the auth rejects — while a
    // valid id still dispatches normally.
    #[tokio::test]
    async fn malformed_request_id_is_rejected_row_free_before_the_claim() {
        let store = mem_store();
        let buyer = Keys::generate();
        seed_sub(&store, "sub-1", &buyer.public_key().to_hex(), "ACTIVE").await;
        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());

        // Empty, over-long, and out-of-alphabet ids — even from the AUTHORIZED owner of an ACTIVE
        // sub, so the reject provably fires before lookup/auth, not because of them.
        let long = "a".repeat(129);
        for bad in ["", long.as_str(), "sp ace", "semi;colon"] {
            let out = RecordingOutbound::default();
            handler
                .handle(
                    buyer.public_key(),
                    op_req(bad, "sub-1", "status", json!({})),
                    &out,
                )
                .await
                .unwrap();
            let res = expect_op_result(&out);
            assert_eq!(res.status, OpStatus::Error);
            assert_eq!(res.error.as_ref().unwrap().code, "invalid_request_id");
        }
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 0);

        // A valid id on the same sub still claims + runs the hook (the gate does not over-drop).
        let out = RecordingOutbound::default();
        handler
            .handle(
                buyer.public_key(),
                op_req("ok-1", "sub-1", "status", json!({})),
                &out,
            )
            .await
            .unwrap();
        assert_eq!(expect_op_result(&out).status, OpStatus::Ok);
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 1);
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

    // ---- lnrent-c95: in-flight registry (concurrent in-process duplicate attach) ----

    /// A `dummy`-based recipe with one extra `request` op whose hook this test controls (a temp
    /// `ops/<op>` script). Returns the recipe plus the temp dir and the `counter`/`release` file
    /// paths the caller bakes into the hook body (see [`write_hook`]). The counter file lets a test
    /// observe when the hook has STARTED (⇒ owner's RUNNING claim + registry entry are in place) and
    /// prove it ran EXACTLY ONCE; the release file gates when the hook finishes.
    fn blocking_recipe(
        label: &str,
        op: &str,
    ) -> (
        Recipe,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let mut recipe = dummy_recipe();
        let dir = std::env::temp_dir().join(format!("lnrent-c95-{label}-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("ops")).unwrap();
        let counter = dir.join("count");
        let release = dir.join("release");
        // Fresh state even if a same-pid prior run left files behind.
        let _ = std::fs::remove_file(&counter);
        let _ = std::fs::remove_file(&release);
        recipe.dir = dir.clone();
        recipe.operations.push(Operation {
            name: op.into(),
            label: op.into(),
            kind: "request".into(),
            hook: op.into(),
            params: vec![],
        });
        (recipe, dir, counter, release)
    }

    fn write_hook(dir: &std::path::Path, op: &str, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        let hook = dir.join("ops").join(op);
        std::fs::write(&hook, body).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// A hook that records ONE byte to `counter` on start, blocks until `release` exists, then either
    /// emits `{"ran":true}` (ok) or exits nonzero (fail). Absolute paths are baked in — the hook
    /// gets no arbitrary env.
    fn blocking_hook_body(
        counter: &std::path::Path,
        release: &std::path::Path,
        ok: bool,
    ) -> String {
        let tail = if ok {
            "echo '{\"ran\":true}'".to_string()
        } else {
            "echo boom >&2\nexit 3".to_string()
        };
        format!(
            "#!/usr/bin/env bash\nprintf x >> '{}'\nwhile [ ! -e '{}' ]; do sleep 0.02; done\n{}\n",
            counter.display(),
            release.display(),
            tail
        )
    }

    /// Poll `f` every 10ms up to ~5s; panic with `what` if it never becomes true. Used to observe
    /// filesystem / registry state without a fixed sleep.
    async fn poll_until<F: FnMut() -> bool>(mut f: F, what: &str) {
        for _ in 0..500 {
            if f() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    fn counter_len(counter: &std::path::Path) -> u64 {
        std::fs::metadata(counter).map(|m| m.len()).unwrap_or(0)
    }

    // Test 8 (c95 core): a duplicate `(sender, request_id)` arriving while the FIRST attempt's hook
    // is STILL RUNNING in this process ATTACHES to the in-flight invocation and receives the SAME
    // op.result — and the hook runs EXACTLY ONCE (only the owner runs it). Proof of "attached while
    // RUNNING": we release the hook only AFTER observing the duplicate has subscribed to the owner's
    // watch (receiver_count ≥ 1) while the owner is still blocked (no terminal committed).
    // TWO duplicates attach, not one: the `watch` is a fan-out primitive and every subscriber must be
    // served, so a regression to a single-consumer channel (oneshot) would hang or drop one of them.
    // The SECOND duplicate carries DIFFERENT labels (`sub-2`/`other_op`) so the assertions below pin
    // "echo the STORED subscription_id/op, not the duplicate's" — the attach-path twin of
    // `cached_done_resends_after_sub_suspended`. With every duplicate sent as `sub-1`/`block_ok` the
    // label assertions would hold even if the attach arm echoed `req`, proving nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_duplicate_attaches_to_running_owner_and_hook_runs_once() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &buyer_hex, "ACTIVE").await;

        let (recipe, dir, counter, release) = blocking_recipe("attach-ok", "block_ok");
        write_hook(
            &dir,
            "block_ok",
            &blocking_hook_body(&counter, &release, true),
        );
        let handler = Arc::new(dispatcher(store.clone(), TestClock::new(1000), recipe));
        let out = Arc::new(RecordingOutbound::default());
        let key = (buyer_hex.clone(), "dup".to_string());

        // Owner: claims RUNNING, inserts the registry entry, then blocks in the hook.
        let owner = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_ok", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        // The hook has started ⇒ the owner's RUNNING row is committed and the registry entry inserted.
        poll_until(|| counter_len(&counter) >= 1, "owner hook to start").await;

        // Duplicates: same (sender, request_id) ⇒ Claim::Running ⇒ attach to the in-flight owner.
        // The second one is labeled `sub-2`/`other_op` (a reused request id with different labels);
        // it must still be served the OWNER's stored `sub-1`/`block_ok` result.
        let spawn_dup = |sub: &'static str, op: &'static str| {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(pk, op_req("dup", sub, op, json!({})), o.as_ref())
                    .await
            })
        };
        let dup = spawn_dup("sub-1", "block_ok");
        let dup2 = spawn_dup("sub-2", "other_op");
        // BOTH duplicates have SUBSCRIBED (attached) to the owner's watch while the owner is still
        // blocked — this is the "duplicate arrives while RUNNING" moment.
        poll_until(
            || {
                handler
                    .inflight
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(watch::Sender::receiver_count)
                    .unwrap_or(0)
                    >= 2
            },
            "both duplicates to attach to the in-flight watch",
        )
        .await;
        // Owner has not committed a terminal yet (still blocked on the hook).
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='RUNNING'"
            )
            .await,
            1,
            "the owner is still RUNNING when the duplicate attaches"
        );

        // Release: the hook finishes ONCE, the owner commits DONE + publishes, the duplicate resends.
        std::fs::write(&release, b"go").unwrap();
        let r_owner = tokio::time::timeout(std::time::Duration::from_secs(10), owner)
            .await
            .expect("owner did not finish")
            .unwrap();
        let r_dup = tokio::time::timeout(std::time::Duration::from_secs(10), dup)
            .await
            .expect("attached duplicate hung (it must never hang)")
            .unwrap();
        let r_dup2 = tokio::time::timeout(std::time::Duration::from_secs(10), dup2)
            .await
            .expect("second attached duplicate hung (the watch must fan out to every subscriber)")
            .unwrap();
        r_owner.unwrap();
        r_dup.unwrap();
        r_dup2.unwrap();

        // All three got an op.result.ok with the SAME data + STORED labels.
        let msgs = out.messages();
        assert_eq!(
            msgs.len(),
            3,
            "owner + BOTH attached duplicates each reply exactly once"
        );
        let results: Vec<OpResult> = msgs
            .iter()
            .map(|(_, m)| match m {
                Msg::OpResult(r) => r.clone(),
                other => panic!("expected op.result, got {other:?}"),
            })
            .collect();
        assert!(results.iter().all(|r| r.status == OpStatus::Ok));
        assert!(
            results.iter().all(|r| r.data == results[0].data),
            "every attached duplicate gets the SAME result as the owner"
        );
        assert_eq!(results[0].data.as_ref().unwrap()["ran"], json!(true));
        for r in &results {
            assert_eq!(r.request_id, "dup");
            // STORED labels — `sub-2`/`other_op` (the second duplicate's own) must NOT appear.
            assert_eq!(r.subscription_id, "sub-1");
            assert_eq!(r.op, "block_ok");
        }

        // The hook ran EXACTLY ONCE (only the owner), and exactly one DONE row was committed.
        assert_eq!(counter_len(&counter), 1, "the hook ran exactly once");
        assert_eq!(count(&store, "SELECT count(*) FROM op_invocation").await, 1);
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            1
        );
        // The registry entry was removed on the terminal — no leak.
        assert!(
            handler.inflight.lock().unwrap().is_empty(),
            "registry must not leak after a terminal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Test 9 (c95): an owner whose hook ERRORS publishes the error to the attached duplicate — both
    // get the SAME op.result.err ("hook_failed") and the hook still runs exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_duplicate_attaches_to_erroring_owner() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &buyer_hex, "ACTIVE").await;

        let (recipe, dir, counter, release) = blocking_recipe("attach-err", "block_err");
        write_hook(
            &dir,
            "block_err",
            &blocking_hook_body(&counter, &release, false),
        );
        let handler = Arc::new(dispatcher(store.clone(), TestClock::new(1000), recipe));
        let out = Arc::new(RecordingOutbound::default());
        let key = (buyer_hex.clone(), "dup".to_string());

        let owner = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_err", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(|| counter_len(&counter) >= 1, "owner hook to start").await;

        let dup = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_err", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(
            || {
                handler
                    .inflight
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(watch::Sender::receiver_count)
                    .unwrap_or(0)
                    >= 1
            },
            "duplicate to attach to the in-flight watch",
        )
        .await;

        std::fs::write(&release, b"go").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), owner)
            .await
            .expect("owner did not finish")
            .unwrap()
            // A hook failure is a cached op.result error, NOT a handler Err.
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), dup)
            .await
            .expect("attached duplicate hung (it must never hang)")
            .unwrap()
            .unwrap();

        let msgs = out.messages();
        assert_eq!(msgs.len(), 2);
        for (_, m) in &msgs {
            match m {
                Msg::OpResult(r) => {
                    assert_eq!(r.status, OpStatus::Error);
                    assert_eq!(r.error.as_ref().unwrap().code, "hook_failed");
                    assert_eq!(r.subscription_id, "sub-1");
                    assert_eq!(r.op, "block_err");
                }
                other => panic!("expected op.result, got {other:?}"),
            }
        }
        assert_eq!(counter_len(&counter), 1, "the hook ran exactly once");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='ERROR'"
            )
            .await,
            1
        );
        assert!(
            handler.inflight.lock().unwrap().is_empty(),
            "registry must not leak after a terminal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Test 10 (c95): a RUNNING op_invocation with NO registry entry — the shape left by an owner that
    // exited without committing a terminal, and by a crashed prior process whose boot recovery has
    // not run — still Err-DEFERS (the pre-c95 crash-recovery behavior) and does NOT hang
    // (attach_or_defer returns immediately, never awaiting). No reply is sent and the row is
    // untouched, so the next startup's recover_interrupted_ops can flip it to `interrupted`.
    #[tokio::test]
    async fn orphaned_running_row_defers_and_does_not_hang() {
        let store = mem_store();
        let buyer = Keys::generate();
        let sender_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &sender_hex, "ACTIVE").await;

        // Pre-seed a RUNNING row with NO in-process registry entry (a crash mid-op).
        {
            let s = sender_hex.clone();
            store
                .transaction(move |tx| {
                    tx.execute(
                        "INSERT INTO op_invocation
                            (sender_pubkey, request_id, subscription_id, op, state, created_at)
                         VALUES (?1, 'op-orphan', 'sub-1', 'restart', 'RUNNING', 500)",
                        params![s],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        let handler = dispatcher(store.clone(), TestClock::new(1000), dummy_recipe());
        let out = RecordingOutbound::default();
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handler.handle(
                buyer.public_key(),
                op_req("op-orphan", "sub-1", "restart", json!({})),
                &out,
            ),
        )
        .await
        .expect("orphaned-RUNNING defer must not hang");
        assert!(
            res.is_err(),
            "an orphaned RUNNING row Err-defers (pre-c95 behavior)"
        );
        assert!(out.messages().is_empty(), "a defer sends no reply");
        // The row is untouched (still RUNNING) and no registry entry was created.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='RUNNING'"
            )
            .await,
            1
        );
        assert!(handler.inflight.lock().unwrap().is_empty());
    }

    // Test 11 (c95 race regression): `claim()` can read RUNNING, then the owner can commit and
    // publish+remove before `attach_or_defer` looks in the registry. A registry miss must therefore
    // re-read the durable row and immediately resend a terminal DONE/ERROR rather than needlessly
    // Err-deferring it to another relay redelivery. Calling `attach_or_defer` directly models the
    // stale `Claim::Running` already held by the duplicate while keeping the interleaving
    // deterministic.
    #[tokio::test]
    async fn registry_miss_rereads_and_resends_terminal_row() {
        let store = mem_store();
        let buyer = Keys::generate();
        let sender_hex = buyer.public_key().to_hex();
        let seeded_sender = sender_hex.clone();
        let error_json = serde_json::to_string(&hook_failed("boom".into())).unwrap();
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO op_invocation
                        (sender_pubkey, request_id, subscription_id, op, state, result_json,
                         created_at, finished_at)
                     VALUES (?1, 'just-done', 'stored-sub', 'stored-op', 'DONE',
                             '{\"fresh\":true}', 500, 600)",
                    params![seeded_sender],
                )?;
                tx.execute(
                    "INSERT INTO op_invocation
                        (sender_pubkey, request_id, subscription_id, op, state, error_json,
                         created_at, finished_at)
                     VALUES (?1, 'just-errored', 'stored-sub', 'stored-op', 'ERROR',
                             ?2, 500, 600)",
                    params![seeded_sender, error_json],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let handler = dispatcher(store, TestClock::new(1000), dummy_recipe());
        let out = RecordingOutbound::default();
        for request_id in ["just-done", "just-errored"] {
            let req = OpRequest {
                id: request_id.into(),
                subscription_id: "duplicate-sub".into(),
                op: "duplicate-op".into(),
                params: json!({}),
            };
            handler
                .attach_or_defer(&buyer.public_key(), &req, &sender_hex, &out)
                .await
                .expect("a terminal row must be resent immediately after a registry miss");
        }

        let messages = out.messages();
        assert_eq!(messages.len(), 2);
        let Msg::OpResult(done) = &messages[0].1 else {
            panic!("expected DONE op.result");
        };
        assert_eq!(done.status, OpStatus::Ok);
        assert_eq!(done.data.as_ref().unwrap()["fresh"], json!(true));
        assert_eq!(done.subscription_id, "stored-sub");
        assert_eq!(done.op, "stored-op");
        let Msg::OpResult(errored) = &messages[1].1 else {
            panic!("expected ERROR op.result");
        };
        assert_eq!(errored.status, OpStatus::Error);
        assert_eq!(errored.error.as_ref().unwrap().code, "hook_failed");
        assert_eq!(errored.subscription_id, "stored-sub");
        assert_eq!(errored.op, "stored-op");
        assert!(handler.inflight.lock().unwrap().is_empty());
    }

    // Test 14 (c95 regression for the publish race): the owner may publish its terminal through
    // `InflightGuard::publish` BEFORE any duplicate has subscribed (`receiver_count() == 0`). The
    // production method MUST use `send_replace`, not `send`: `send` drops the value and errs when
    // there are no receivers, so a duplicate that subscribes an instant later — still in the window
    // before the registry entry is removed — would read a stale `None` and needlessly defer an op
    // that actually committed a terminal it could have been served. Keep a sender clone so this test
    // can model that late subscriber after `publish` removes the registry entry. (Under plain `send`
    // this reads `None` and panics.)
    #[tokio::test]
    async fn zero_receiver_publish_is_visible_to_a_late_subscriber() {
        let registry = InflightRegistry::default();
        let key = ("buyer".to_string(), "request".to_string());
        // Through the production constructor, so this exercises the real insert+guard pairing.
        let mut guard = InflightGuard::register(&registry, key.clone());
        let late_subscriber = registry.lock().unwrap().get(&key).unwrap().clone();

        // No receiver is live now — mirrors "no duplicate has subscribed yet". Exercise the
        // production publish method, including its registry removal and `published` bookkeeping.
        guard.publish(InflightOutcome::Done {
            result_json: "{\"ran\":true}".into(),
            subscription_id: "sub-1".into(),
            op: "block_ok".into(),
        });
        assert!(registry.lock().unwrap().is_empty());

        // A duplicate subscribes only now, after the zero-receiver publish. Bind the clone before
        // the match so the `watch::Ref` borrow is released immediately (not held across arms).
        let mut rx = late_subscriber.subscribe();
        let current = rx.borrow_and_update().clone();
        match current {
            Some(InflightOutcome::Done {
                subscription_id,
                op,
                ..
            }) => {
                assert_eq!(subscription_id, "sub-1");
                assert_eq!(op, "block_ok");
            }
            Some(_) => panic!("late subscriber read a non-Done outcome"),
            None => panic!(
                "late subscriber read a stale None (publish used `send`, not `send_replace`)"
            ),
        }
    }

    // Test 15 (c95 regression): a terminal commit is authoritative even if the owner's subsequent
    // relay reply fails. The attached duplicate must receive the committed DONE result, never the
    // guard's fallback `NoTerminal` defer; publishing before awaiting owner I/O also prevents a stalled or
    // cancelled owner reply from withholding that terminal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_reply_failure_after_commit_still_publishes_done_to_duplicate() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &buyer_hex, "ACTIVE").await;

        let (recipe, dir, counter, release) = blocking_recipe("owner-reply-fail", "block_ok");
        write_hook(
            &dir,
            "block_ok",
            &blocking_hook_body(&counter, &release, true),
        );
        let handler = Arc::new(dispatcher(store.clone(), TestClock::new(1000), recipe));
        let out = Arc::new(FailFirstOutbound::default());
        let key = (buyer_hex, "dup".to_string());

        let owner = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_ok", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(|| counter_len(&counter) >= 1, "owner hook to start").await;

        let duplicate = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_ok", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(
            || {
                handler
                    .inflight
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(watch::Sender::receiver_count)
                    .unwrap_or(0)
                    >= 1
            },
            "duplicate to attach to the in-flight watch",
        )
        .await;

        std::fs::write(&release, b"go").unwrap();
        let owner_result = tokio::time::timeout(std::time::Duration::from_secs(10), owner)
            .await
            .expect("owner did not finish")
            .unwrap();
        let duplicate_result = tokio::time::timeout(std::time::Duration::from_secs(10), duplicate)
            .await
            .expect("attached duplicate hung")
            .unwrap();

        // WHICH task consumes `FailFirstOutbound`'s attempt-0 failure is deliberately NOT asserted:
        // `guard.publish` wakes the attached duplicate BEFORE the owner calls `out.reply`, so on a
        // multi-thread runtime either task can reach the outbound first. Assert what c95 actually
        // guarantees, order-independently: BOTH tasks attempted a reply — which is what proves the
        // duplicate was served the COMMITTED terminal and not the guard's `NoTerminal` defer, since a
        // defer replies nothing — exactly one hit the simulated failure, and the single message that
        // did get through is that committed DONE (the owner's and the duplicate's are byte-identical
        // by construction: same `result_json`, same STORED subscription_id/op).
        assert_eq!(
            out.attempts.load(Ordering::SeqCst),
            2,
            "owner AND attached duplicate must each attempt a reply (a deferred duplicate would not)"
        );
        assert_eq!(
            usize::from(owner_result.is_err()) + usize::from(duplicate_result.is_err()),
            1,
            "exactly one reply hits the simulated failure, the other succeeds \
             (owner={owner_result:?}, duplicate={duplicate_result:?})"
        );
        let messages = out.messages();
        assert_eq!(messages.len(), 1, "only the second reply attempt succeeds");
        match &messages[0].1 {
            Msg::OpResult(result) => {
                assert_eq!(result.status, OpStatus::Ok);
                assert_eq!(result.data.as_ref().unwrap()["ran"], json!(true));
                assert_eq!(result.subscription_id, "sub-1");
                assert_eq!(result.op, "block_ok");
            }
            other => panic!("expected op.result, got {other:?}"),
        }
        assert_eq!(counter_len(&counter), 1, "the hook ran exactly once");
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='DONE'"
            )
            .await,
            1
        );
        assert!(
            handler.inflight.lock().unwrap().is_empty(),
            "registry must not leak after owner reply failure"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Test 16 (c95): an owner whose TASK IS CANCELLED mid-invocation (the inbound drain deadline's
    // `abort_per_wrap` does exactly this) must NOT hand its attached duplicate a fabricated terminal.
    // The store actor runs an already-queued job even when its caller was cancelled, so a cancelled
    // owner can still commit DONE/ERROR durably; replying a permanent `interrupted` here would
    // contradict the durable cache. The duplicate must instead DEFER (Err, no reply, wrap not marked
    // seen) — the pre-c95 behavior — and must NOT hang. Cancelling inside the blocking hook is the
    // cleanest reachable proxy: no terminal is committed, the row stays RUNNING, and the drop guard
    // is what wakes the duplicate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_owner_defers_the_attached_duplicate_instead_of_faking_a_terminal() {
        let store = mem_store();
        let buyer = Keys::generate();
        let buyer_hex = buyer.public_key().to_hex();
        seed_sub(&store, "sub-1", &buyer_hex, "ACTIVE").await;

        let (recipe, dir, counter, release) = blocking_recipe("owner-cancelled", "block_ok");
        write_hook(
            &dir,
            "block_ok",
            &blocking_hook_body(&counter, &release, true),
        );
        let handler = Arc::new(dispatcher(store.clone(), TestClock::new(1000), recipe));
        let out = Arc::new(RecordingOutbound::default());
        let key = (buyer_hex, "dup".to_string());

        let owner = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_ok", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(|| counter_len(&counter) >= 1, "owner hook to start").await;

        let duplicate = {
            let (h, o, pk) = (handler.clone(), out.clone(), buyer.public_key());
            tokio::spawn(async move {
                h.handle(
                    pk,
                    op_req("dup", "sub-1", "block_ok", json!({})),
                    o.as_ref(),
                )
                .await
            })
        };
        poll_until(
            || {
                handler
                    .inflight
                    .lock()
                    .unwrap()
                    .get(&key)
                    .map(watch::Sender::receiver_count)
                    .unwrap_or(0)
                    >= 1
            },
            "duplicate to attach to the in-flight watch",
        )
        .await;

        // Cancel the owner while it is still in the hook (the `release` file is never written): its
        // `InflightGuard` drops WITHOUT a published terminal.
        owner.abort();
        // Await cancellation so the owner's future (and therefore `InflightGuard`) has finished
        // dropping before the registry-cleanup assertion below. The duplicate is woken by the
        // guard's publish before its subsequent remove, so awaiting only the duplicate would race
        // that remove.
        let owner_error = tokio::time::timeout(std::time::Duration::from_secs(10), owner)
            .await
            .expect("aborted owner did not finish")
            .expect_err("aborted owner unexpectedly completed");
        assert!(
            owner_error.is_cancelled(),
            "owner abort must cancel its task"
        );
        let duplicate_result = tokio::time::timeout(std::time::Duration::from_secs(10), duplicate)
            .await
            .expect("attached duplicate hung after the owner was cancelled")
            .unwrap();

        assert!(
            duplicate_result.is_err(),
            "a duplicate attached to a cancelled owner must Err-defer, never fabricate a terminal"
        );
        assert!(
            out.messages().is_empty(),
            "a defer sends no reply (nothing may contradict the durable row)"
        );
        // Nothing terminal was committed and the registry did not leak.
        assert_eq!(
            count(
                &store,
                "SELECT count(*) FROM op_invocation WHERE state='RUNNING'"
            )
            .await,
            1,
            "the cancelled owner's row stays RUNNING for recover_interrupted_ops"
        );
        assert!(
            handler.inflight.lock().unwrap().is_empty(),
            "the drop guard must remove the registry entry on cancellation"
        );
        // No `release` write is needed: dropping the owner's future drops `run_hook`, whose
        // process-group guard (lnrent-y4m.12) kills the still-blocked hook — no orphan spins on it.
        let _ = std::fs::remove_dir_all(&dir);
    }
}
