# 0017 — Pay-path crash recovery stays oplog-based; re-paying is not an outcome oracle

> **Status (lnrent-8ym): recovery machinery deleted with lnv1, per ADR-0018.** The lnv1 backend
> this ADR describes — its oplog scan (`recover_pay_from_oplog`), the `fedimint_pay_dead_op`
> dead-op ledger, and the `PayStateClass` update-stream classification — was removed when lnv1 was
> retired. The lnv2 backend (`lnv2_backend.rs`) needs none of it: deterministic invoice-derived
> operation ids and truthful refund-acceptance-aware final states replace the recovery stack by
> design (see ADR-0018). This ADR is kept as history and as the method reference for *why* re-paying
> is not an outcome oracle — the fedimint-internals analysis below remains valid.

The daemon's outbound-pay crash recovery (`recover_pay_from_oplog`, lnrent-4gt/kum) scans
fedimint's operation log on startup and backfills the lnrent `fedimint_pay` idempotency row
for any committed pay operation the crash window (op committed, row not yet upserted) left
unrecorded. When upstream PR [fedimint/fedimint#8818](https://github.com/fedimint/fedimint/pull/8818)
reordered `pay_bolt11_invoice`'s checks (per-payment-hash idempotency **before** invoice
expiry — carried by our pin, lnrent-7y1), the question arose whether the whole recovery stack
could be replaced by *lazy re-pay*: re-submit the persisted invoice and read the answer —
`Ok(completed_payment)` = paid; `Err(PreviousPaymentAttemptStillInProgress{op})` = in flight;
fall-through (`Invoice has expired`) = **neither**, so safe to resolve a fresh invoice and pay.

## Decision

**No.** The fall-through does NOT prove the recipient was never paid, so the oplog scan, the
dead-op ledger (`fedimint_pay_dead_op`, lnrent-kum), and the update-stream classification
(`PayStateClass`, lnrent-y4m.16) all stay. The pin's fix is defense-in-depth *under* that
machinery (a completed prior attempt on a since-expired invoice now heals through the normal
re-pay path instead of erroring), not a replacement for it. The one sound patch-enabled
improvement — adopting `PreviousPaymentAttemptStillInProgress` in `start_new_pay` so an
intra-run crash window heals without a restart — is tracked as lnrent-85t (additive only:
adoption re-awaits the existing op, never starts a second one).

## Why the oracle is unsound (verified against fedimint v0.11.1, commit 2620789)

`pay_bolt11_invoice`'s answers derive from the module-DB `PaymentResult` record plus
`has_active_states`. Three invariants were checked at source level:

- **Proven:** `completed_payment` is written atomically with the pay state machine's
  `Success` transition (same db transaction as the terminal-state write), *including* the
  paid-but-change-errored case — change settlement happens at the operation-log layer after
  the SM already recorded success (`pay.rs` success transition; `incoming.rs` for internal).
- **Proven:** attempt indices are monotonic and only ever advanced by `pay_bolt11_invoice`
  itself after the previous attempt is inactive; the index-bump-before-state-machine crash
  window moves zero funds. So "no attempt still live" is trustworthy.
- **Refuted:** "inactive + no completed_payment ⇒ recipient not paid". The pay SM's `Refund`
  terminal only **submits** the refund transaction — it never observes acceptance. Concrete
  shape: the gateway pays the Lightning recipient and claims the outgoing contract
  pre-timelock while the daemon is down longer than the client's ~3-minute pay-transition
  window (after which the preimage can never be delivered to the SM); on restart the SM can
  only exit via timeout into `Refund`; the refund transaction is **rejected**
  (`InsufficientFunds` — the contract was already emptied by the gateway's claim); the
  tx-submission state machine (same operation id) terminalizes. Result: no active states, no
  completed payment, and the recipient **was paid from operator funds**. Re-paying a fresh
  invoice there is an operator double-pay.

The only signal that distinguishes an ACCEPTED refund (funds provably back — safe to retry)
from that REJECTED refund is the refund transaction's outcome, which fedimint surfaces only
through the per-operation update stream: `subscribe_ln_pay` maps it to `Refunded` vs
`UnexpectedError`. That is exactly what `classify_ln_pay_state` consumes — `Refunded` →
`DefinitiveFailure` (row FAILED, fresh pay allowed) vs `UnexpectedError` → `Ambiguous` (row
pinned PENDING forever, operator resolution required). The stream classification is therefore
the **sole guard** for this shape; `PaymentResult` alone cannot see it.

## Considered options

- **Keep oplog-based recovery (chosen).** Startup scan re-attaches every committed-but-
  unrecorded op to its idempotency row as PENDING, so the next drive re-awaits the operation's
  update stream and the y4m.16 classification lands the truthful terminal — including the
  rejected-refund shape, which parks PENDING instead of triggering a fresh pay.
- **Lazy re-pay-as-oracle recovery (rejected).** Smaller code, no scan, no `extra_meta`
  embedding — but unsound per the refuted invariant above: the inactive-not-completed
  fall-through conflates "definitively failed, funds back" with "recipient paid, refund
  rejected", and it cannot yield the op id needed to re-await the stream in that case.
- **Hybrid (rejected for now).** Re-pay for the completed/active answers + scan only for the
  fall-through would keep both mechanisms alive for a marginal saving; the scan already covers
  all three cases and is fail-closed, so the hybrid only adds surface.

## Consequences

- `recover_pay_from_oplog`, `fedimint_pay_dead_op`, `pay_recovery_action`, and the
  `PayStateClass` classification are load-bearing by proof, not history; do not remove or
  weaken them on the strength of the #8818 ordering fix (or any future fedimint change) without
  re-verifying the refuted invariant above against the fedimint source in use.
- The `lnrent_idempotency_key` embedded in each pay op's `extra_meta` remains required (it is
  what the scan keys on).
- Residuals recorded for completeness: `LightningPayStates::Failure` is unreachable with the
  stock `RealGatewayConnection` (every error is retried, never surfaced as
  `OutgoingContractError`); internal-pay terminals are all safe (success sets the result;
  refund-submitted/funding-failed provably never paid the recipient).
