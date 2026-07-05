# 0005 — Prepaid expiry subscriptions with a reconcile-loop enforcer

A subscription is prepaid to a hard expiry date (`paid_through`). The buyer renews
before that date; each payment extends `paid_through` by one `period`, and early
renewals stack so renewing early is never wasted. A soft date (`paid_through -
renew_lead`) is a recommendation only: from it the daemon nudges the buyer to renew so
the service is never interrupted. At the hard date, if unpaid, the service is
suspended and later destroyed after `retention`; there is no post-expiry grace,
because the soft-date recommendation is the buffer. We chose renew-before-the-date
(prepaid) over bill-at-period-end-then-grace because it is simpler to reason about and
does not penalize a buyer for operator downtime: the buyer can renew at any time, the
operator only nudges.

A single periodic reconcile loop enforces this: it scans subscriptions whose
`next_deadline <= now`, fires the due transition (remind at soft date, suspend at hard
date, destroy at retention end), and recomputes `next_deadline`. Transitions are
idempotent and journaled to `event_log`, and all dates are absolute wall-clock
timestamps, so the loop is downtime-safe: a transition missed while the Box was off
simply fires on restart.

## Consequences

- Subscription states drop `DUE`/`GRACE`; the relevant dates are `paid_through` and
  `soft_date`. The buyer can pull a renewal invoice on demand (`renew.request`).
- NWC pull (v2) auto-renews before the soft date, making the subscription hands-off.
- Reminders are best-effort; correctness does not depend on them, because the buyer
  can always request an invoice and renew.

## Revision — credit operator downtime (v0.15)

Wall-clock suspension is downtime-safe for the operator but unfair to the buyer: if the
operator was down during a subscription's renewal window (`soft_date -> paid_through`),
the buyer could not renew (no invoice issued, no reminders) yet would be suspended the
moment the operator returns. Home-lab boxes reboot, lose power, and sleep, so this is a
real churn/dispute generator. Fix: the daemon persists a heartbeat, and on restart it
**credits its own downtime**. Rather than move `paid_through` (the prepaid-money and the
`renew:auto:<sub>:<paid_through>` invoice anchor), it records a per-subscription
`suspend_not_before` floor for any ACTIVE sub whose renewal window overlapped the outage
`[last_heartbeat, now]`, computed so the buyer gets their full `renew_lead` window of actual
operator availability before suspension; the missed reminder fires normally on the restart
tick. Every consumer of the "resumable until" boundary honors the credited
`B = max(paid_through, suspend_not_before) + retention_s`: the suspend transition
(`effective_suspend_at = max(paid_through, suspend_not_before)`) and the destroy that runs
`retention_s` after it, capture's renewal refund gate, the buyer's `renew.request`, and the
restart settlement catch-up. So a buyer is never suspended — nor destroyed, nor refused a
renewal — for the operator's outage. The floor never moves a deadline *earlier*, self-expires
once a renewal pushes `paid_through` past it, and is cleared on renewal. (Crediting an
*already-SUSPENDED* sub's retention/destroy landed too — lnrent-d6n,
docs/specs/downtime-credit-suspended.md.)
