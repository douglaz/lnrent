# Spec: buyer-initiated subscription cancellation (`sub.cancel`)

Status: **Proposed** — closes the one genuine buyer-lifecycle gap (`sub.cancel` is currently a no-op).
Scope: `daemon/src/{order_intake.rs, store.rs}` + a CLI comment/behaviour touch in `clients/cli/src/main.rs`.
Audience: the rb-lite implementer. This spec is the contract; the tests below are mandatory ship gates.

## 1. Motivation

The buyer CLI has a `cancel` command that sends `Msg::SubCancel { subscription_id }`, and the Nostr engine
routes it to `OrderIntake::handle` (`nostr_engine.rs:519`). But the handler's dispatch drops it on the
floor — `order_intake.rs:645` `_ => Ok(())` — so **a buyer cannot actually cancel a subscription.** The
CLI even documents this ("the operator does not yet act on it — not confirmed end-to-end").

Everything downstream already exists: `reconcile.rs::fire_destroy` (`:803`) terminates a `CANCELLED`
subscription after its retention window (runs the `destroy` hook to tear down the VM, releases the
reservation, `-> TERMINATED`), and `handle_renew` (`order_intake.rs:322`) only issues renewals for
`ACTIVE`/`SUSPENDED`, so a `CANCELLED` sub already stops being billed. The missing piece is the **entry
point**: a handler that transitions the sub to `CANCELLED` with the correct termination deadline.

## 2. Behaviour contract

A `sub.cancel` from the subscription's owner MUST:

- **CANCEL-1 (authorize).** Act only if `sender.to_hex() == subscription.buyer_pubkey`. A cancel for a
  non-existent subscription, or from a non-owner, is silently dropped (logged at WARN) — no reply, no
  existence leak. (`sub.cancel` has no `request_id`; it is fire-and-forget and naturally idempotent.)
- **CANCEL-2 (gate on cancellable states).** Transition to `CANCELLED` only from `ACTIVE` or `SUSPENDED`
  (the states with a real `paid_through` and a live/lapsing service). Any other state is an idempotent
  no-op:
  - `CANCELLED`/`TERMINATED`/`EXPIRED`/`REFUNDED` — already terminal; drop.
  - `REFUND_DUE` — a refund is already owed; cancellation must not interfere; drop.
  - `PENDING`/`PROVISIONING` — OUT OF SCOPE for this cut (see §6): a `PENDING` order expires on its own
    and a `PROVISIONING` sub is mid-hook (racy); drop with a WARN.
- **CANCEL-3 (terminate at the end of the already-paid window; compute the deadline INSIDE the txn).**
  Cancel is NOT a lapse-suspend: the buyer paid through `paid_through`, so an `ACTIVE` sub keeps running the
  full paid period and is then DESTROYED — there is no unpaid retention/suspend phase (that grace exists to
  let a *lapsed* payer recover; an explicit canceller needs none, and giving one would be free extra
  service). The termination deadline MUST be read + computed from the CURRENT row INSIDE the same
  transaction as the state write, never from a stale pre-read — a renewal capture or downtime-credit
  committing between a pre-read and the write would otherwise let the CAS succeed with a stale deadline and
  schedule destroy before a just-paid boundary, tearing down a renewed box:
  ```
  -- all inside ONE store transaction:
  SELECT state, paid_through, next_deadline FROM subscription WHERE id=?sub_id;
  --   ACTIVE    -> term_deadline = paid_through   (destroy at the paid-period end; NO retention window)
  --   SUSPENDED -> term_deadline = next_deadline  (already the retention_end the suspend set; keep it)
  UPDATE subscription SET state='CANCELLED', next_deadline=?term_deadline, updated_at=?now
   WHERE id=?sub_id AND state IN ('ACTIVE','SUSPENDED');
  ```
  If `term_deadline` is NULL (an `ACTIVE` row with `paid_through IS NULL`, or a `SUSPENDED` row with
  `next_deadline IS NULL`), the handler is a NO-OP and MUST NOT run the `UPDATE` — a NULL `next_deadline`
  would strand the row (`due_subscriptions`/`fire_destroy` never select a NULL deadline). Otherwise run the
  CAS `UPDATE`; if it affects 0 rows (raced to a terminal state), the handler is a no-op.
  `reconcile.rs::fire_destroy` (`:803`, CAS on `state IN ('SUSPENDED','CANCELLED')`) then runs
  the `destroy` hook + releases the reservation + `-> TERMINATED` at `term_deadline`. A `CANCELLED` sub gets
  no other reconcile action before then (no reminder/suspend — those are `ACTIVE`-only), so the box simply
  runs until `term_deadline` and is destroyed.
- **CANCEL-4 (confirm to the buyer).** In the same transaction, enqueue a `billing.notice` to the buyer
  via the outbox (`reconcile.rs::enqueue`, `:1010`), so the buyer's CLI can confirm the cancel landed:
  ```rust
  Msg::BillingNotice(BillingNotice {
      subscription_id,
      state: "CANCELLED".into(),
      message: "subscription cancelled; service runs until the paid period ends, then terminates".into(),
  })
  ```
  Use an idempotent outbox id keyed to the sub + deadline (e.g. `outbox:cancel-notice:{sub_id}:{retention_end}`)
  so a duplicate `sub.cancel` cannot enqueue a second notice.
- **CANCEL-5 (journal).** Journal an `order_intake_cancel` event to `event_log` in the same transaction
  (every mutation is journaled, ADR-0001/§6.5).

Cancel does NOT touch renewal invoices or the billing paths — the existing machinery already makes a
cancelled sub MONEY-SAFE, and cancel-side invoice surgery would only add settlement gaps (a locally
`EXPIRED` bolt11 can still be paid, yet `settlement_catch_up` only scans `OPEN` invoices). Instead, rely on
what already holds:
- `handle_renew` drops a renew for a non-`ACTIVE`/`SUSPENDED` sub at its pre-read (`order_intake.rs:322`),
  so no NEW renewal is issued once the cancel is visible;
- capture REFUNDS (never resurrects) any renewal that settles on a terminal/`CANCELLED` sub
  (`capture.rs:683`) — covering a renewal invoice that was already `OPEN`/queued at cancel time, a
  `renew.request` that was mid-flight when the cancel committed, or a payment landing in a concurrent
  window;
- `reconcile.rs::fire_destroy` terminates the sub at `term_deadline`, resolving any still-`OPEN` renewal
  invoice by a final backend lookup before it terminates.

So NO cancel-side billing code is added: a cancelled sub is never double-charged, any stray renewal
settlement is refunded, and it is never resurrected. **Perfectly suppressing every stray renewal invoice
(cached / queued / mid-flight) is explicitly OUT OF SCOPE (§6) — money-safety, not zero stray invoices, is
the contract.** Tests assert: a `renew.request` after cancel is dropped by `handle_renew`, and a renewal
that settles on a `CANCELLED` sub is refunded (not applied).

## 3. Wiring

Add `handle_cancel(sender, cancel, out)` to `OrderIntake` (parallel to `handle_order`/`handle_renew`) and
dispatch it from the `handle` match (`order_intake.rs:640`):
```rust
Msg::SubCancel(req) => self.handle_cancel(sender, req, out).await,
```
`DeliveryResendRequest` stays on the `_ => Ok(())` arm (owned by the supervisor's delivery wrapper,
`7fp.10`) — do NOT change its handling.

Authorization (CANCEL-1) may use a pre-read of the sub's IMMUTABLE `buyer_pubkey` (a sub's owner never
changes, so that pre-read is race-free) plus a rough `state` to decide whether to proceed at all. The
`term_deadline` selection, however, MUST happen INSIDE the cancel transaction (CANCEL-3) — never from a
pre-read — because `paid_through`/`next_deadline` can move under a concurrent renewal/credit: the same txn
re-`SELECT`s `state, paid_through, next_deadline`, computes `term_deadline` (`paid_through` for `ACTIVE`, the
existing `next_deadline` for `SUSPENDED`), and runs the CAS `UPDATE` (+ CANCEL-4/5). Load only those
columns; do NOT read `retention_s`/`suspend_not_before` or recompute a
`max(paid_through, suspend_not_before)+retention_s` deadline.

## 4. CLI

`clients/cli/src/main.rs`: the `cancel` command already sends `sub.cancel`. Update its help text (drop
"the operator does not yet act on it — not confirmed end-to-end"). `sub.cancel` is fire-and-forget with
no correlated reply, so the command stays send-and-exit-0 on successful publish; do NOT block waiting for
the `billing.notice` (it is unsolicited and best-effort). A follow-up `subs status` / `delivery resend`
already lets the buyer observe state. Keep the change minimal.

## 5. Tests (mandatory)

Unit (in `order_intake.rs` tests, mirroring the `handle_renew` + `reconcile.rs::cancelled_terminates_
after_retention` setups):
- CANCEL-owner-active: owner cancels an `ACTIVE` sub → state `CANCELLED`, `next_deadline == paid_through`
  (the paid-period end; NO retention window), exactly one `billing.notice` (state `CANCELLED`) enqueued to
  the buyer, an `order_intake_cancel` journal row.
- CANCEL-owner-suspended: owner cancels a `SUSPENDED` sub → `CANCELLED`, `next_deadline` unchanged from
  the suspend value.
- CANCEL-nonowner: a different sender → NO state change, NO outbox row (silent drop).
- CANCEL-terminal-noop: cancel on `TERMINATED`/`CANCELLED`/`REFUND_DUE`/`EXPIRED`/`PENDING`/`PROVISIONING`
  → no state change, no outbox row.
- CANCEL-idempotent: two identical `sub.cancel`s → one state change, exactly one `billing.notice` (the
  keyed outbox id dedups).
- CANCEL-then-terminate: after cancelling an `ACTIVE` sub, a `reconcile_tick(retention_end)` terminates it
  (destroy hook runs, reservation RELEASED, `-> TERMINATED`) — proving the cancel feeds `fire_destroy`.
- CANCEL-stops-billing (money-safe, no cancel-side billing code): after cancel, a `renew.request` for the
  sub is dropped by `handle_renew` (no `billing.invoice`); and a renewal that settles on the now-`CANCELLED`
  sub is REFUNDED, not applied (the existing capture terminal-renewal path) — no resurrection.

## 6. Non-goals

- No pro-rated refund on cancel (M1a has none); the paid period stands, the VM runs until it ends.
- No immediate teardown; an `ACTIVE` cancel terminates at `paid_through` (the paid-period end), a
  `SUSPENDED` cancel at its existing `retention_end` — via the existing `fire_destroy` path. There is no
  extra unpaid-retention window for a cancel.
- `PENDING`/`PROVISIONING` cancel is deferred (PENDING expires on its own; PROVISIONING is mid-hook and
  racy). Revisit if buyers need pre-payment/mid-provision cancellation.
- No read-only `subs status` (M1a's `subs status` remains the side-effecting re-delivery).
- Perfectly suppressing every stray renewal invoice (one already issued/cached/queued/mid-flight at cancel
  time) is out of scope. Cancel is money-safe by construction — a stray renewal that settles is refunded,
  never resurrected (existing capture path) — so it does not chase down in-flight billing artifacts.

## 7. Resolved decisions

- Cancel is supported ONLY from `ACTIVE` and `SUSPENDED`. `PENDING` is left to expire on its own (its order
  invoice expires -> `EXPIRED`); `PROVISIONING` cancel is deferred (mid-hook, racy). Both are silent no-ops
  for now — revisit only if buyers report needing pre-payment / mid-provision cancellation.
- `SUSPENDED` cancel IS supported: it relabels to `CANCELLED` keeping the existing `next_deadline`, so the
  buyer's explicit cancel is recorded + confirmed even though termination timing is unchanged.
- Cancel DOES confirm to the buyer via a `billing.notice` (state `CANCELLED`) so a CLI/agent can observe
  that the cancel landed. It is best-effort + unsolicited (the CLI does not block on it — §4).
