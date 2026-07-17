# 0018 — Payment backends: fedimint-lnv2 + phoenixd; lnv1 retired

Partially supersedes ADR-0012 (its phoenixd framing and M3 deferral; its capture-then-refund
and PaymentBackend-trait conclusions stand).

The lnv1 fedimint client forced ~2k lines of crash-recovery machinery into the daemon
(extra_meta key embedding, oplog recovery, the dead-op ledger, stream classification —
ADR-0017). The lnv2 module eliminates those client gaps by design (deterministic
invoice-derived operation ids, truthful refund-acceptance-aware final states, no
wall-clock restart trap — 2026-07-16 source survey). lnv2 is opt-in per federation and
absent from federations created before it — but a federation is *picked, not inherited*:
the operator chooses one at onboarding via an invite code, so once public lnv2-enabled
federations exist, any new operator can use lnv2. Operator decision (2026-07-17): commit
to the simplification; cover the rest with backend choice, not with lnv1 maintenance.

## Decision

- **Fedimint ecash stays a primary receive path, via lnv2.** Operators join an
  lnv2-enabled federation of their choosing. lnv2 backend work gates on a released,
  API-stable lnv2 (verify against the adopted release tag, never moving master).
- **phoenixd is promoted to a co-equal alternative** for any operator who doesn't want a
  federation — no longer 0012's "secondary, larger-value only". 0012's economics concern
  (per-payment inbound-liquidity cost on small rentals) is accepted as tolerable:
  recurring subscription billing amortizes channel costs, and the operator sees the
  trade-off. Pricing guidance stays advisory, never gating.
- **lnv1 is retired, not maintained.** Frozen now (hardened through y4m/kum, ADR-0017);
  removed once the adopted lnv2 release ships and the backends above cover onboarding.
  At lnv1 removal the ADR-0017 recovery machinery becomes deletable — and not before.
  No dual-fedimint-module support, ever.
- **Onboarding posture:** exactly one payment backend per Control node (0012's
  per-control-node choice stands). The doctor probes the configured backend
  FUNCTIONALLY: fedimint — lnv2 module present AND an lnv2-capable gateway attached and
  reachable (module-present/gateway-absent fails); phoenixd — API reachable + liquidity
  sanity. Config-presence checks alone are insufficient.

## Considered options

- **Keep lnv1 as the permanent compatibility backend (rejected).** Serves operators on
  pre-lnv2 federations indefinitely, but locks in the ADR-0017 machinery forever and
  adds a third live backend once lnv2 lands — maximum surface, no exit.
- **Defer everything until the lnv2 ecosystem matures (rejected).** The 2026-07-16
  review's outside voices recommended this; overruled by the federation-is-picked
  insight and the phoenixd alternative — waiting bought nothing that backend choice
  doesn't already provide.
- **lnv2 + phoenixd, retire lnv1 (chosen).** Two simple backends, each without the lnv1
  workaround class; the recovery machinery has an end date.

## Consequences

- The dep pins (douglaz/fedimint `v0.11.1-pay-idempotency`) are returned upstream only as
  part of the lnv2 version bump — never as mechanical cleanup (the bump IS the lnv2
  adoption event).
- `docs/specs/backend-strategy.md` (lnrent-bi8) carries the execution detail: phoenixd
  trait mapping, per-backend backup/reconcile stories, the lnv2 no-same-invoice-retry
  gap across all bolt11 surfaces, retirement sequencing.
- Existing lnv1 deployments migrate by moving funds (a federation cannot be upgraded in
  place); the retirement plan must include that operator runbook.
- The Operator-seed backup story ("one backup covers identity AND the wallet") must be
  re-examined for phoenixd, which has its own seed.
