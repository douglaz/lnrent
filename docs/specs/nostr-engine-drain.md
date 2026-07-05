# Spec: NostrEngine graceful shutdown drain (lnrent-k82)

**Status:** **Implemented** (master `02917b5`; nostr_engine.rs `InboundTaskState`/`drain`, supervisor.rs inbound-special-cased shutdown). Landed slightly stronger than specified: aux backlog/resubscribe tasks are aborted *and awaited* within the drain deadline, and the timed-out path awaits the aborted accept loop before snapshotting per-wrap handles.
**Bead:** lnrent-k82 (P2, money-path). Confirmed by reading `daemon/src/supervisor.rs` + `daemon/src/nostr_engine.rs`.

## Problem (verified)

`NostrEngine::run_inbound` spawns inbound work with dropped `JoinHandle`s: the per-wrap handler tasks
(`queue_inbound_event`), the initial/lag backlog fetch tasks, and the per-relay CLOSED-resubscribe
recovery tasks. On graceful shutdown the supervisor currently aborts/drops the `run_inbound` future via
its shutdown `select!`, then waits on `InboundDrain::wait_idle()`.

`InboundDrain` only counts calls after a spawned per-wrap task has finished verify/decrypt/dedupe and
entered the injected business handler. A task that is spawned but still in `process_inbound` before the
handler call, or a wrap that `run_inbound` has accepted but is waiting for the concurrency permit before
spawn, is invisible to `InboundDrain`. `wait_idle()` can therefore return while money-path work is still
in flight, and the final outbox flush can run before a just-committed handler has enqueued its reply.

## Design — engine-owned inbound drain

Add one engine-owned inbound task/drain seam. Do not try to share `Arc<JoinSet>`; `JoinSet` needs
exclusive mutable access. Use a cloneable `tokio_util::task::TaskTracker` for per-wrap work plus stored
`AbortHandle`s for deadline aborts (add a direct `tokio-util` dependency with the `rt` feature if the
daemon does not already have one). Put the state behind a new `Arc<InboundTaskState>` field on
`NostrEngine` so every engine clone and every spawned task sees the same stop flag, task tracker, and
abort-handle lists.

Minimal shape:

- `stop_accepting`: set by `NostrEngine::drain`; checked by `run_inbound` and by backlog/resubscribe
  helpers before accepting/queuing new wraps. It must also wake the live receive loop so it can return.
- `per_wrap: TaskTracker` plus per-wrap `AbortHandle`s: authoritative ownership for every accepted
  wrap from handoff through `process_inbound` and the post-success `seen_message` write.
- `aux_aborts`: best-effort abort handles for backlog/resubscribe helper tasks. These tasks are not
  part of the correctness drain and do not need a clean-stop protocol.
- The supervisor owns the `run_inbound` loop `JoinHandle`. Awaiting that handle is the accept-loop
  quiescence proof; do not add a second authoritative signal/counter unless the implementation detaches
  the recv→spawn handoff.

`InboundDrain` becomes diagnostic/legacy plumbing only. It may stay around to log active handler calls,
but `RunningSupervisor::shutdown` must not use `InboundDrain::wait_idle()` as an authoritative drain.

## Accepted-to-tracked handoff

No separate `AcceptedWrapGuard` / `pending_handoffs` counter is required for the live subscription
path. The graceful path must stop accepting, then await the supervisor-owned `run_inbound` `JoinHandle`
instead of dropping/aborting it; once that handle has returned, any wrap `run_inbound` accepted has
already finished its in-progress `queue_inbound_event` handoff and been spawned through the
`TaskTracker`. If an implementation detaches that handoff, or lets an auxiliary producer be aborted
while holding an already-accepted event before `TaskTracker` registration, add the tiny guard/counter
there as an implementation detail.

`run_inbound` and backlog/resubscribe producers must check `stop_accepting` before accepting the next
notification/backfill item (for example with a stop-biased `select!`). Do not check the flag after an
event has already been received and then silently drop that event: once a producer has an event in hand,
it is accepted work and must route through the tracked per-wrap helper. Keeping the current pre-spawn
semaphore wait is safe for the live loop because the loop handle is awaited, not canceled; alternatively
the helper may register the tracked task before waiting for the permit.

## Inbound drain sequence

This can be exposed as one `NostrEngine::drain(deadline, inbound_handle)` helper or as supervisor
orchestration around smaller engine methods; keep this order:

1. Set `stop_accepting` and wake the live inbound receive loop.
2. Abort auxiliary backlog/resubscribe helpers best-effort. If `fetch_inbound_backlog` keeps spawning
   per-relay children, route those children through the same aux-abort wrapper or make them non-detached;
   aborting only the parent while dropping inner `JoinHandle`s would just detach them.
3. Wait, within the same `deadline`, for the supervisor-owned `run_inbound` `JoinHandle` to return (or
   timeout). Its return proves no more live-loop per-wrap registrations can occur; if it times out,
   abort that loop handle, abort stored per-wrap handles, log loudly, and return a timed-out result.
4. Only after the loop has returned, close/wait the per-wrap `TaskTracker` until all accepted wraps
   finish. If the remaining deadline expires here, abort the stored per-wrap handles, log loudly, and
   return a timed-out result. A stuck handler must not hang process exit.

Do not rely on `TaskTracker::close()` as the stop signal; the stop flag plus returned inbound loop
handle is what prevents late registrations. `close()` is only for the “no more per-wrap tasks can be
registered” phase after the inbound loop handle has returned.

Abort safety depends on the existing invariant only: the business handler commits in its own sqlite
transaction, and `seen_message` is written only after handler success. Abort before commit leaves no
business row and no seen row; abort after commit but before the seen write reprocesses on restart and is
made safe by the handler's `inbound_request` / `op_invocation` idempotency. Do not move the seen write
earlier.

## Supervisor wiring

`RunningSupervisor::shutdown` must special-case the inbound loop so the supervisor never drops or aborts
`run_inbound` before the inbound drain sequence has completed or timed out. The inbound order is:

1. stop accepting (the engine stop/drain API does this),
2. await the `run_inbound` loop `JoinHandle` so the recv→tracked-task handoff is complete,
3. if that await times out, abort the inbound loop handle and per-wrap handles and report timeout,
4. otherwise drain accepted per-wrap work through the engine-owned `TaskTracker`, aborting stragglers
   only if the remaining deadline expires,
5. then run the final outbox flush.

Other supervised loops may be signaled before or around this, but the current inbound wrapper pattern
`select! { engine.run_inbound(..) => ..., _ = wait_for_shutdown(..) => Ok(()) }` must go away or be
changed so the shutdown arm cannot drop the `run_inbound` future pre-drain. Keep the final outbox flush
after inbound drain so a handler that just committed and enqueued a `provision.ready` / `billing.refund`
DM is included.

## Non-goals / cuts

- Do not change `process_inbound`, dedupe, in-flight claims, relay transport, or message types.
- Do not make drain unbounded.
- Do not make backlog/resubscribe tasks part of the money-safety proof. Aborting them is enough as long
  as they check `stop_accepting` before accepting another retained item and any already-accepted item
  goes through the same tracked per-wrap helper (or the optional guard if that helper has a detached
  abortable gap).
- Do not add a second authoritative drain beside the engine-owned tracker plus inbound-loop handle.
  `InboundDrain` is diagnostic-only.

## Acceptance

- Slow handler test: send a wrap, begin graceful shutdown/drain while the handler is running, unblock it,
  and assert the business commit lands and the subsequent final outbox flush sends its queued DM.
- Pre-handler test: a spawned wrap still in verify/decrypt/dedupe is awaited by `engine.drain`; this is
  proven by the per-wrap tracker, not by `InboundDrain`.
- Pre-permit handoff test: a wrap received while all `MAX_INBOUND_CONCURRENCY` permits are occupied is
  not missed; shutdown awaits the `run_inbound` handle, and that handle cannot return until the
  in-progress handoff has spawned the tracked task.
- Deadline test: a never-finishing handler causes `drain(deadline)` to return timed out, aborts the
  straggler, writes no early `seen_message` row, and restart/backfill can safely reprocess or replay the
  idempotent committed result.
- Stop-accepting test: after drain starts, new relay/backfill events are not accepted; an auxiliary
  backlog/resubscribe task may drop/abort only before acceptance, while any already-accepted item routes
  through the tracked helper.
- Supervisor test: shutdown does not abort/drop `run_inbound` before the inbound drain sequence
  completes or times out; final outbox flush happens after inbound drain.
- Existing `nostr_engine` + `supervisor` smoke/integration tests stay green.

## Suggested implementation order

1. Add `InboundTaskState` to `NostrEngine` (`TaskTracker`, per-wrap abort handles, aux abort handles,
   stop flag/waker).
2. Store/special-case the inbound `run_inbound` `JoinHandle` in `RunningSupervisor` instead of hiding it
   behind the shutdown `select!`.
3. Route `queue_inbound_event` through the tracked helper; route backlog/resubscribe spawns through
   best-effort aux abort storage. Add an `AcceptedWrapGuard` only if an abortable detached producer can
   hold an already-accepted event before tracker registration.
4. Teach `run_inbound` to observe `stop_accepting` and return only after its current handoff is closed.
5. Implement the engine drain helper(s).
6. Rewire `RunningSupervisor::shutdown` for the inbound-specific order: stop, await inbound handle,
   drain tracker or abort timed-out stragglers, final outbox flush after drain; make `InboundDrain`
   diagnostic-only.
7. Add the acceptance tests above.
