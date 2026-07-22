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
  lnv2-enabled federation of their choosing. lnv2 is RELEASED — the client, common, and
  server crates ship in v0.11.1, the version already pinned — so the lnv2 backend is
  buildable now, with no version bump and no waiting (verified 2026-07-17 against the
  pinned checkout: `send`, `FinalSendOperationState`, `await_final_send_operation_state`,
  and the Refunding→Success refund-claim-failed recheck are all present).
- **phoenixd is promoted to a co-equal alternative** for any operator who doesn't want a
  federation — no longer 0012's "secondary, larger-value only". 0012's economics concern
  (per-payment inbound-liquidity cost on small rentals) is accepted as tolerable:
  recurring subscription billing amortizes channel costs, and the operator sees the
  trade-off. Pricing guidance stays advisory, never gating.
- **lnv1 is deleted ASAP** (operator directive 2026-07-17). It never ships to a
  third-party operator; it remains the dogfood backend only until the lnv2 backend
  lands, then the lnv1 paths, the ADR-0017 recovery machinery, and the
  fedimint-ln-client/-ln-common deps are removed together. No dual-fedimint-module
  support, ever.
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

- The dep pins are on the douglaz/fedimint `v0.11.1-pay-idempotency` fork. That fork is
  upstream `v0.11.1` + two commits — #8818 pay-idempotency (on `fedimint-ln-client`, the
  lnv1 crate) and the tpe 1-of-1 fix (on `crypto/tpe`, whose panicking fn only
  `fedimint-gwv2-client` calls, not in lnrent's tree). At lnv1 deletion (lnrent-8ym) the
  two lnv1 crates are dropped, so NEITHER fork commit touches anything lnrent still
  compiles: the built lnv2 output is byte-identical to the plain upstream `v0.11.1` tag.
  **The pins are KEPT on the fork anyway** (operator decision, lnrent-8ym 2026-07-22), as
  standing patch infrastructure — lnrent carries fedimint patches regularly and the
  gateway image (lnrent-e96) builds from the same fork. Returning to the tag was the
  original plan; it was reversed because it is churn for zero functional change. No
  version bump is involved (lnv2 ships in v0.11.1). Upstream PR #8818 remains a valid
  contribution for lnv1 users.
- `docs/specs/backend-strategy.md` (lnrent-bi8) carries the execution detail: phoenixd
  trait mapping, per-backend backup/reconcile stories, the lnv2 no-same-invoice-retry
  gap across all bolt11 surfaces, retirement sequencing.
- There is no migration: lnrent is greenfield — no operator deployments exist. Retiring
  lnv1 means deleting it once a replacement backend is production-ready; the only lnv1
  wallet in the world is the author's own dogfood, drained by hand.
- Refund destinations must be re-resolvable: order intake rejects raw bolt11 (requires
  LN-address/LNURL), uniformly across backends — lnv2 cannot retry a failed send on the
  same invoice, and per-generation re-resolution dissolves that gap structurally (it also
  removes the cross-order same-invoice collision surface). Operator sweeps are unaffected
  (interactive: a fresh invoice per attempt).
- The Operator-seed backup story ("one backup covers identity AND the wallet") must be
  re-examined for phoenixd, which has its own seed.
