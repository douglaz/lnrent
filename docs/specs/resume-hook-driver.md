# Spec: run the recipe `resume` hook on a paid suspended-renewal (lnrent-18v)

**Status:** draft for codex-review-loop → rb-lite
**Bead:** lnrent-18v (P1; the resume-hook strand split from g5p, was blocked on `.21` daemon wiring — now
unblocked). Confirmed by the 2026-07-02 codex review (a real paid-but-no-service money bug).

## Problem (verified)

A paid renewal of a **SUSPENDED** subscription is captured by `apply_paid` (daemon/src/capture.rs:230-250):
in one sqlite txn it extends `paid_through`, clears `suspend_not_before`, flips the sub straight to
**ACTIVE**, and returns `Capture::Resumed`. But capture is a pure store txn — it does NOT (cannot) run the
recipe **`resume`** hook (the actual power-on; e.g. recipes/do-vps/resume boots the droplet). Nothing in
the daemon acts on `Capture::Resumed` (supervisor.rs only nudges maintenance for captured/refund-due). So
the buyer pays to un-suspend, the DB says ACTIVE, but **the VM stays powered off** — paid, no service, and
no refund. This mirrors exactly the provisioning gap that `provision.rs` already closes for first-order
provisioning; resume needs the same driver.

## Design — a RESUMING state + a resume driver (mirror the provision driver)

The invariant: **a subscription must not read ACTIVE until its service is actually running.** A paid
suspended-renewal therefore cannot be marked `ACTIVE` inside capture; it must be durably marked as
in-flight, the recipe `resume` hook must run, and only then can the row move to `ACTIVE`.

`RESUMING` is the smallest durable marker that preserves that invariant. Keeping the row `SUSPENDED`
would let reconcile/destroy treat a paid renewal as an unpaid suspended rental, and an event-log marker
beside `SUSPENDED` would force every time-driven path to consult side state. `REFUND_DUE` is also wrong
for this path: today's refunder interprets it as failed first-order provisioning, then moves the sub to
`REFUNDED` and releases the reservation, orphaning the existing instance. `RESUMING` is the resume
analogue of `PROVISIONING`, but failure restores `SUSPENDED` and refunds only the renewal.

1. **Capture (capture.rs).** On a paid renewal where the current state is `SUSPENDED`, first keep the
   existing inclusive-terminal retention gate exactly as-is (a settlement at/after the credited retention
   end still refunds instead of resuming). If it is inside the resumable window, the capture txn does all of
   the following:

   - compute the fresh paid timers exactly as today:
     `new_paid_through = max(previous_paid_through, settled_at) + period_s`,
     `new_soft_date = new_paid_through - renew_lead_s`;
   - update the sub to
     `state='RESUMING', paid_through=new_paid_through, soft_date=new_soft_date,
     next_deadline=new_soft_date, suspend_not_before=NULL`;
   - return `Capture::Resumed`;
   - journal `renew_resume` with the renewal `external_id`, received `amount_sat`, and the minimal
     pre-renewal restore baseline: `previous_paid_through` and `previous_suspend_not_before`.

   On permanent resume failure, the driver restores the old suspended lifecycle from that baseline:
   `paid_through=previous_paid_through`,
   `soft_date=previous_paid_through - renew_lead_s`,
   `next_deadline=max(previous_paid_through, previous_suspend_not_before.unwrap_or(previous_paid_through))
   + retention_s`, and `suspend_not_before=previous_suspend_not_before`. Copying the full four-column
   preimage (`soft_date`/`next_deadline` too) into the journal is optional defensive/audit data, not a
   required part of the fix. A renewal of an already-`ACTIVE` sub is unchanged (`renew_extend` → stays
   `ACTIVE`; the service is already running). Once capture accepts a timely renewal into `RESUMING`, the
   old credited-retention boundary is not re-checked by the driver; a delayed driver resumes or refunds
   based on hook outcome, not because wall clock passed the old boundary.

2. **ResumeDriver (new, mirror Provisioner).** A driver that takes a `RESUMING` subscription, loads its
   instance + recipe, and runs the recipe **`resume`** hook with the same bounded retry shape as
   `Provisioner` (3 attempts, 250ms backoff, `DEFAULT_TIMEOUT`, JSON stdout, idempotent hook contract).
   Use the existing lifecycle-hook input shape (`subscription`, `instance`, and top-level `handles`) that
   `suspend`/`destroy` receive; do **not** use the provision delivery payload contract. Resume stdout only
   needs to be valid JSON to prove hook success and is not delivered to the buyer.

   On success: CAS `RESUMING → ACTIVE`, leaving the paid timers capture already set. Do not enqueue a new
   `provision.ready`; the instance and credentials already exist and a power-on must not re-deliver them.

   On permanent failure (including a missing/non-executable resume hook, non-JSON stdout, timeout after the
   bounded attempts, or a missing instance target): in one txn guarded by `WHERE state='RESUMING'`, restore
   the old suspended timers, move `RESUMING → SUSPENDED`, and insert exactly one renewal refund row:
   `id='ref-<renewal external_id>'`, `idempotency_key='refund:<renewal external_id>'`,
   `subscription_id=<sub id>`, `dest=<sub refund_dest>`, `amount_sat=<renewal amount>`,
   `status='PENDING'`, `attempts=0`, `ON CONFLICT(idempotency_key) DO NOTHING`. The existing refunder
   drains every pending `refund_attempt`; because the subscription is `SUSPENDED` rather than `REFUND_DUE`,
   `commit_sent` will leave the sub and reservation alone instead of moving it to `REFUNDED` or releasing
   capacity. Do not destroy the instance and do not release the reservation; the restored suspended
   retention cursor owns eventual destroy. If the CAS loses, do nothing further.

   A permanently broken hook therefore resolves in one driver pass (after the bounded attempts) and cannot
   pin `RESUMING` forever. A daemon crash only re-drives the same idempotent hook/CAS on the next pass.

3. **Wiring (supervisor.rs).** Drive `RESUMING` subscriptions from the same single serialized maintenance
   loop that drives `PROVISIONING`: after settlement catch-up/provision recovery and before
   `refunder.drive()`, so a resume failure's renewal refund can be paid in the same pass. Boot recovery gets
   the same step after settlement catch-up/provision recovery and before refunds/reconcile:
   `... settlement_catch_up → provisioner.recover → resume_driver.recover → refunder.drive → reconcile ...`.
   A live `Capture::Resumed` result must nudge the maintenance loop, and settlement catch-up must count
   `Resumed` as work so recovered paid renewals are immediately followed by the resume recovery step.

4. **Reconcile (reconcile.rs).** `RESUMING` is an in-flight, paid state: reconcile must NOT issue renewal
   reminders, suspend, destroy, or expire the already-PAID renewal's subscription while the resume is in
   progress, even if `next_deadline` is now due. A stale `RESUMING` row is not the backstop; the driver is.
   On resume failure the driver restores `SUSPENDED` with the old retention cursor, and only then can the
   normal destroy path run later. Confirm no reconcile arm treats `RESUMING` as ACTIVE/SUSPENDED in a way
   that double-acts.

## Non-goals

Do not change first-order provisioning (`provision.rs`) or the provision hook. Do not re-deliver
credentials on resume (the instance + creds persist across a suspend/resume; only wire re-delivery if a
future recipe needs changed access material — out of scope). Do not add downtime-credit/d6n changes. Keep
the change minimal + patterned on the existing provision driver; no new message types.

## Acceptance

- A paid renewal of a SUSPENDED sub leaves it **RESUMING** (not ACTIVE) after capture with the new paid
  timers set, then the ResumeDriver runs the recipe `resume` hook and moves it to **ACTIVE** (assert the
  hook saw the lifecycle input, the state path is SUSPENDED→RESUMING→ACTIVE, and no duplicate
  `provision.ready` is enqueued).
- A `resume` hook that fails permanently creates exactly one detached renewal refund, restores the pre-renewal
  **SUSPENDED** timers, and leaves the instance/reservation intact (assert the refunder pays the renewal back
  without moving the sub to `REFUNDED` or releasing the reservation). A transient failure is retried within the
  bounded attempt budget and is not destroyed.
- **Boot recovery** and the maintenance pass re-drive `RESUMING` subs left by a crash; the hook is idempotent
  and both success/failure writes are CAS-guarded, so restart does not duplicate refunds or deliveries.
- Reconcile leaves a `RESUMING` sub alone (no reminder/suspend/destroy/expire) until it resolves; a renewal
  captured one second before the credited retention boundary still resumes even if the driver runs after that
  old boundary.
- A renewal of an ACTIVE sub is unchanged (stays ACTIVE, no resume hook). Existing capture/reconcile/
  provision tests stay green.

## Suggested implementation order

1. Add the `RESUMING` state + capture change (Capture::Resumed → RESUMING) with the capture tests updated.
2. The ResumeDriver (copy the Provisioner structure into a new module; run `resume`, bounded retry, ACTIVE
   or restored SUSPENDED + detached renewal refund).
3. Wire it into the maintenance loop + boot recovery + the reconcile guard.
4. Integration test: SUSPENDED → paid renewal → RESUMING → resume hook → ACTIVE; and the permanent-failure
   refund/restore path.
