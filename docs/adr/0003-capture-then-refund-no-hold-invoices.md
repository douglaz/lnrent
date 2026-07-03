# 0003 — Capture-then-refund in v1; no hold invoices

The trust model is pay-then-provision, and the obvious primitive for atomic "do not
capture unless provisioning succeeds" is a Lightning hold invoice. But our two v1
receiving backends cannot hold: phoenixd auto-settles and exposes no preimage/cancel
control (verified against its API source), and Fedimint resolves incoming Lightning
into ecash before any app-level capture point. So v1 captures the first payment on
settlement, runs a pre-flight check before issuing the invoice to make failure rare,
and refunds (push to a buyer-supplied BOLT12 offer or Lightning address) if
provisioning still fails.

## Considered options

- **Hold invoices (provision-then-capture).** The clean primitive, but unsupported by
  phoenixd and Fedimint. Available only on LND (native) or CLN (plugin).
- **Capture-then-refund (chosen).** Works on phoenixd/Fedimint. The refund is a fresh
  outbound payment, so it can itself fail and needs retries plus an operator alert.

## Consequences

- `order.request` collects a refund destination up front (BOLT12 offer preferred,
  Lightning address fallback); the daemon pushes refunds via phoenixd
  `payoffer`/`paylnaddress`.
  *Revision (2026-07, post-ADR-0012 + SPEC §6.4 F3/F6):* the landed contract is stricter —
  `refund_dest` is REQUIRED and must be a **Lightning address or HTTPS LNURL** (re-resolvable
  at refund time); raw BOLT11 is rejected and BOLT12 is unsupported. Refunds resolve via
  LNURL and pay through the Fedimint `PaymentBackend` (phoenixd was never built). The
  capture-then-refund decision itself is unchanged.
- Subscriptions gain `PROVISIONING`, `REFUND_DUE`, and `REFUNDED` states.
- Pre-flight (capacity and param checks before the invoice) is the main defense; the
  refund path is exceptional.
- Operators who require true atomicity can later run an LND payment backend (native
  hold invoices). Not built in v1.
