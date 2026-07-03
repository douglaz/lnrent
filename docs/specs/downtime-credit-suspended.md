# Spec: downtime credit for SUSPENDED subscriptions (lnrent-d6n)

**Status:** draft for codex-review-loop → rb-lite
**Bead:** lnrent-d6n (P2; confirmed by the 2026-07-02 codex review). Extends the 7fp.22 downtime credit.

## Problem (verified)

`Reconciler::apply_restart_downtime_credit` (daemon/src/reconcile.rs) credits an operator outage only to
**ACTIVE** subscriptions (it raises a `suspend_not_before` floor so a buyer still gets their full
`renew_lead` window before suspension). It SKIPS subs that were already **SUSPENDED** when the outage
began. A SUSPENDED sub is inside its retention window `[effective_suspend_at, B]` where
`effective_suspend_at = max(paid_through, suspend_not_before)` and the destroy deadline
`B = effective_suspend_at + retention_s` — the window during which the buyer can still renew/resume. If
the daemon is DOWN for part of that window, the buyer cannot renew during the outage, yet on restart the
destroy catch-up (reconcile.rs, the SUSPENDED/CANCELLED → TERMINATED arm) will destroy the sub as soon as
`now >= B`, consuming retention the buyer never got to use. This violates the §6.5/ADR-0005 principle that
an operator outage must never penalize a buyer for the operator's own downtime.

## Design — credit SUSPENDED subs' retention window (mirror the ACTIVE credit)

Extend `apply_restart_downtime_credit` (same boot txn, same boot-recovery position: BEFORE settlement
catch-up and BEFORE the boot destroy catch-up) to also consider **SUSPENDED** subs. For an ACTIVE sub the
credit gives back the lost `renew_lead`; for a SUSPENDED sub it gives back the lost **retention**
(renew-opportunity) window — by raising the same
`suspend_not_before` floor so the destroy boundary `B` moves forward. Reconcile's destroy arm already
destroys SUSPENDED rows at their `next_deadline`, and the SUSPENDED cursor is the credited boundary
`B = max(paid_through, suspend_not_before) + retention_s`; so NO reconcile-arm change is needed — only the
credit must keep the floor and cursor in sync.

For each SUSPENDED candidate, with the outage window `[last, now]` (`last` = the last liveness heartbeat):

- `E_old = max(paid_through, suspend_not_before ?? paid_through)` (the effective suspend/retention start).
- `B_old = E_old + retention_s` (its current destroy deadline).
- Select only retention windows that overlap the outage: `E_old <= now` and `B_old > last`. The `B_old > last`
  half is the anti-resurrection gate: if `B_old <= last`, the retention had already ended before the outage
  and the sub should destroy normally on catch-up.
- `remaining = clamp(B_old - last, 0, retention_s)` — how much retention was un-consumed when the outage
  started. Equivalently, `pre_available = retention_s - remaining = clamp(last - E_old, 0, retention_s)`.
- `target_B = now + remaining` — give back that remaining retention FROM RESTART (mirrors the ACTIVE
  credit's `target = now + (lead - pre_available)`).
- `new_floor = max(E_old, paid_through, target_B - retention_s)` — a
  MONOTONIC floor (never regresses, never precedes the prepaid window).
  This makes the new destroy deadline `new_B = max(paid_through, new_floor) + retention_s` (`target_B` when
  the floor binds). Move/keep the reconcile cursor consistent with the existing SUSPENDED credited-destroy
  cursor by setting `suspend_not_before = new_floor` and `next_deadline = new_B` in the same UPDATE.
- Never mint an invoice; never move `paid_through`. Preserve the reconcile CAS shape of the UPDATE.

Implementation shape (keep ACTIVE unchanged): run a separate SUSPENDED candidate branch/query so the ACTIVE
selection math is not disturbed. The SUSPENDED query is:

```sql
SELECT id, paid_through, retention_s, suspend_not_before, next_deadline
  FROM subscription
 WHERE state='SUSPENDED'
   AND paid_through IS NOT NULL
   AND retention_s IS NOT NULL
   AND next_deadline IS NOT NULL
   AND MAX(paid_through, COALESCE(suspend_not_before, paid_through)) <= ?2
   AND MAX(paid_through, COALESCE(suspend_not_before, paid_through)) + retention_s > ?1
```

where `?1 = last` and `?2 = now`; the `<= ?2` predicate filters outages wholly before `E_old`, and the final
predicate is the required `B_old > last` gate. Read the current `next_deadline` as `old_cursor`, compute
`new_B`, and if `new_B <= old_cursor`, skip the
UPDATE/journal/count (on normal rows `old_cursor == B_old`, so this is exactly "the effective floor did not
rise"). Otherwise update with:

```sql
UPDATE subscription
   SET suspend_not_before=?2, next_deadline=?3, updated_at=?4
 WHERE id=?1 AND state='SUSPENDED' AND next_deadline=?5
```

with params `(id, new_floor, new_B, now, old_cursor)`. Write the heartbeat after both ACTIVE and SUSPENDED
credits in the same transaction, as today, so the outage is consumed exactly once.

Skip **RESUMING** subs (a transient paid-renewal-mid-resume state from lnrent-18v; it self-resolves and is
not in a retention-consuming state). A subsequent paid renewal still clears the floor exactly as it does
today for ACTIVE-credited subs (the renewal capture sets `suspend_not_before=NULL`).

## Non-goals

Do not change the ACTIVE renew_lead credit formula, the heartbeat mechanism, or reconcile's destroy/suspend
arms (they already honor `B`). Do not mint invoices or move `paid_through`. Do not credit CANCELLED subs (a
cancelled sub's terminal deadline is the buyer's own choice, not consumed by operator downtime — its
`next_deadline` is the agreed termination, unrelated to renew opportunity). Keep the change minimal and
symmetric with the existing ACTIVE credit.

Do not change renewal-invoice expiry semantics; expired auto-invoices are invoice-only, while `renew.request`,
capture, and settlement catch-up already use the credited boundary
`max(paid_through, suspend_not_before) + retention_s`.

## Acceptance

- A SUSPENDED sub whose retention window overlapped an outage gets its destroy deadline extended by the
  retention lost to downtime (assert: outage of D seconds inside retention → `B` moves so the buyer still
  has the un-consumed retention from restart; the sub is NOT destroyed immediately on the boot catch-up).
- Boundary test: an outage entirely before `E_old` is a no-op; an outage starting before `E_old` and ending
  inside retention gives the buyer a full retention window from restart.
- A SUSPENDED sub whose retention had ALREADY ended before the outage (`B_old <= last`) is still destroyed
  on catch-up (no spurious credit / resurrection).
- The credit is monotonic + idempotent: re-running it (or a second outage) never lowers the floor; a sub
  already credited past `now` is not double-credited for the same outage; a row credited as ACTIVE on a
  restart is not also SUSPENDED-credited in that same pass.
- A subsequent paid renewal of a credited SUSPENDED sub clears the floor (existing behavior).
- The ACTIVE-sub credit is unchanged (regression), and RESUMING/CANCELLED subs are not credited. A missed
  settlement recovered during boot uses the newly credited SUSPENDED boundary before capture/refund logic.

## Suggested implementation order

1. Add a separate SUSPENDED candidate struct/query/loop to `apply_restart_downtime_credit`; leave the existing
   ACTIVE SELECT and formula unchanged.
2. Tests: SUSPENDED-in-retention outage → extended `B`; boundary no-op; retention-already-ended →
   destroyed; CAS/idempotent skip; renewal clears the floor; settlement catch-up honors the credited
   boundary; ACTIVE unchanged.
