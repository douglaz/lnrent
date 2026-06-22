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
