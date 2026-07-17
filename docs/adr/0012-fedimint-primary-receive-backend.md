# 0012 — Fedimint ecash is the primary receive backend for low-value rentals

> Partially superseded by ADR-0018 (2026-07-17): phoenixd is promoted to a co-equal
> alternative (the economics concern below is accepted as tolerable), and the fedimint
> path moves to the lnv2 module with lnv1 retired. The capture-then-refund and
> PaymentBackend-trait conclusions here stand.

To RECEIVE a Lightning payment you need inbound liquidity. A fresh phoenixd operator has
none, so the first payment triggers an on-the-fly channel open (service + on-chain fee)
that can exceed a small-sat rental — the very low-value services lnrent leads with. Raw
self-custodial Lightning is a poor economic fit for the core product.

Fedimint sidesteps it: an operator who is a member of an existing federation receives
through the federation's gateway into ecash, with no per-payment inbound-liquidity cost
(the gateway eats the LN side). So:

- **Fedimint ecash is the default/primary receive backend** for low-value rentals (via an
  existing federation + gatewayd; the operator does not run a guardian).
- **phoenixd is secondary**, for standalone operators with their own inbound liquidity and
  larger / longer-prepay payments — not 5k-sat one-offs.
- Pricing guidance: price periods above the receive overhead, offer longer prepay, and
  treat very-small amounts as Fedimint territory.

Both still cannot hold invoices, so capture-then-refund (ADR-0003) applies to both; the
PaymentBackend trait abstracts the difference.

## Considered options

- **phoenixd-primary (raw self-custodial LN).** Self-custody, but per-payment liquidity
  cost kills small-sat unit economics. Now secondary.
- **Pre-provision liquidity / price up.** Helps phoenixd but does not make tiny payments
  economic.
- **Fedimint ecash primary (chosen).** No per-payment liquidity cost; fits the low-value
  core. Trade-off: depends on an existing federation + gateway (the operator trusts the
  federation's guardians for the ecash they hold) rather than pure self-custody.

## Consequences

- The first real PaymentBackend implementation is **Fedimint** (the M1a real receive path,
  against a test federation); phoenixd + the optional LND-hold backend move to a later
  secondary-backends milestone (M3).
- The capture/correlation/refund design (ADR-0009) is backend-agnostic via the trait; the
  Fedimint impl maps ecash-received -> Settlement(external_id) and pays refunds out via the
  gateway.
- Operators who insist on pure self-custody can run phoenixd, accepting the liquidity
  economics. The choice is per control node.
