# Spec: GATE-1 operator sweep / payout (PR-8) — ledger-authoritative

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md PR-8 (verified, 2026-07-03). Money-moving — kept as
its own tight spec. **Revised 2026-07-04:** authorization is computed from the LEDGER ONLY; the
federation balance is never read in this path. (The earlier draft authorized off
`available_balance_msat` and accreted per-race patches — catch-up-first, balance-before-reserve
ordering, open-invoice reserves, quiescence checks — all papering over one mistake: the balance is
an eventually-consistent aggregate on a different clock than the ledger. Transactions are the
truth; the balance is at most a reconciliation artifact. See §Design principle.)

## Problem (verified)

Operator profit (sales − refunds) has no daemon-safe exit. The CLI is read-only + admin
(bin/lnrent.rs:27+); the only outbound path is the Refunder's `pay_refund_capped`, INV-1-capped to
specific received amounts. The workaround — a second fedimint client against the daemon-owned
RocksDB — risks corrupting the money DB (ADR-0015). Funds are recoverable from seed+invite but not
withdrawable in operation.

## Design principle — the ledger authorizes; the balance never does

The daemon's sqlite ledger records every money event it has committed to: captured receipts
(invoices marked PAID/settled by capture), refund intents/outcomes (`refund_attempt`), and — with
this spec — sweep intents/outcomes (`sweep_attempt`). The Fedimint balance is a derived cache of
the same history on the federation's clock; reading it to authorize a payout reintroduces a
two-clock reconciliation problem with an unbounded race surface.

So the sweep authorizes from a single ledger-derived quantity:

```
receipts_msat = Σ gross of ALL captured receipts, over BOTH provenance classes INV-3
                recognizes (docs/specs/refund-money-path-hardening.md §3.3), de-duped by
                external payment id:
                • every invoice row the ledger marked PAID/settled — final, at-risk, and
                  later-refunded alike, AND
                • every settle-refund event_log settlement entry (`settle_unmatched_refund`
                  / `settle_orphan_refund`, capture.rs) — real money received whose only
                  ledger record is the journal entry + its detached refund_attempt
reserved_msat = Σ gross, counted ONCE per external_id (the same de-dup rule INV-2 uses), of:
                • captured receipts still AT RISK (their sub is PENDING/PROVISIONING/
                  RESUMING/REFUND_DUE — a state-machine refund path still exists), and
                • every non-terminal refund_attempt (PENDING or otherwise unresolved,
                  INCLUDING unpriceable ones — gross always bounds the INV-1-capped outlay)
paid_out_msat = Σ gross of refund_attempt rows SENT (gross ≥ actual outlay, by INV-1)
              + Σ max_outlay_msat of sweep_attempt rows SENT or PENDING

surplus_msat  = receipts_msat − reserved_msat − paid_out_msat

ALLOW iff surplus_msat >= outlay_msat(sweep)
```

The base is ALL receipts, not just "final" ones — the arithmetic must net to the right value per
receipt: a FINAL receipt contributes +gross (sweepable); an at-risk receipt +gross −gross = 0; a
refunded receipt +gross −gross(SENT) = 0. **Receipts, reserves, and paid-out MUST all draw from the
same provenance set** — subtracting a refund whose receipt exists only as an event_log settlement
entry, while the base counted only invoice rows, would understate surplus by that gross and strand
the operator's netted-out funds (an orphan settlement nets to 0 like any refunded receipt, but only
if both sides are counted). (Starting the base at final-only and *also* subtracting
reserves/refunds would double-count every non-final receipt and systematically over-refuse —
e.g. one ACTIVE 100k order + one PROVISIONING 100k order must leave 100k sweepable, not 0.) A
receipt is FINAL (contributes its full gross with no offsetting reserve) exactly when no
state-machine path can still route it to a refund:

- an order receipt is final once its subscription reached `ACTIVE` (service delivered; cancel does
  not refund prepaid time). A renewal receipt is final once applied to an `ACTIVE` sub, or once a
  `RESUMING` resume succeeded. Anything ambiguous — the implementer cannot prove no refund path
  remains — is reserved, at gross. **Fail closed: when in doubt, a receipt is reserved, never
  sweepable.**
- `outlay_msat(sweep) = refund_required_outlay_msat(amount_sat, Some(amount_sat))` — a gateway
  QUOTE for the payment about to be made. This is a pricing call for execution, not a balance
  read; if it fails, refuse (`sweep_unpriceable`).
- Fees make the accounting conservative by construction: refunds are subtracted at gross though
  their real outlay ≤ gross (INV-1), sweeps at their capped max outlay — so `surplus_msat`
  UNDER-estimates real holdings and can never over-authorize.

**Why the races are gone, not patched:** an uncaptured settlement is not a captured receipt, so it
contributes nothing to `earned` — it cannot be swept, no matter when it lands (the money sits in
the wallet, invisible to authorization, until capture books it *and* its at-risk reservation in
the same act). An open unpaid invoice is not a receipt at all — no reserve needed. A late terminal
settlement is captured atomically WITH its detached `refund_attempt` (capture writes both in one
txn, §6.4/§6.6), so receipt and liability appear together — no window. There is no balance
snapshot, so there is no read-ordering hazard. The earlier draft's catch-up-first, still-payable
reserves, quiescence refusal, and balance-before-reserve rules are all deleted, not relocated.

**If the wallet somehow holds less than the ledger-authorized surplus** (a fedimint-level loss or
a ledger bug), the sweep's `pay` simply fails to assemble notes and errors cleanly — before
anything moves. The pay itself is the fail-safe; no pre-read needed. Detecting such book-vs-wallet
drift is a *reconciliation* concern, owned by the explicit operator command in
docs/specs/gate1-alerting-operability.md §F — never by this authorization path.

## Command surface

- IPC `Request::Sweep { bolt11: String }` (LNRENT operator socket only, like every admin verb).
- CLI `lnrent sweep <bolt11> [--json]`. The CLI first performs a dry-run quote
  (`Request::SweepQuote { bolt11 }` → amount, quoted outlay, earned/reserved/paid-out/surplus
  breakdown, verdict) and prints it; executing requires the explicit flag `--yes` (mirrors the
  lnd-payments pattern: quote by default, pay only on `--yes`).
- The bolt11 MUST carry an amount (zero-amount invoices rejected: `sweep_invalid`). The amount is
  the invoice's, not a CLI argument — no amount/dest mismatch class. The operator mints the
  invoice in their own wallet.
- Run the gate + the ledger write in the same serialized store/maintenance context the Refunder
  uses, so the surplus computation cannot interleave with a capture or refund commit. (This is
  ordinary single-writer discipline, ADR-0001 — not a race patch: all inputs are now in one
  database under one writer.)

## Idempotency + ledger

- Pay key: `sweep:<payment_hash>` (the bolt11 payment hash — unique per invoice, deterministic on
  retry of the same invoice). **Send with an outlay cap:** extend the backend with
  `pay_capped(bolt11, amount_sat, max_outlay_msat, key)` (or generalize the existing
  `pay_refund_capped`'s check to take an explicit `max_outlay_msat`), passing the just-quoted
  `outlay_msat`. A NEW operation refuses to start if the real `amount*1000 + fee >
  max_outlay_msat` (a fee rise between quote and send must refuse, not overspend); existing
  SUCCEEDED/PENDING ops for the key re-await exactly like `pay`. The backend key dedup + fedimint
  payment-hash dedup make a re-submitted invoice safe (re-awaits, never double-pays).
- Durable intent BEFORE pay, mirroring the refund ledger pattern (§6.6): new table `sweep_attempt`
  (id = `sweep:<payment_hash>`, bolt11, amount_sat, max_outlay_msat, status PENDING|SENT|FAILED,
  attempts, created_at, sent_at, last_error). A PENDING/SENT sweep row subtracts its
  `max_outlay_msat` from the surplus (see gate) the moment it exists — so even mid-flight, the
  committed outlay is already accounted.
- **Crash recovery re-gates unstarted intents.** On boot/maintenance, for each PENDING sweep:
  `payment_started_by_key(key)` (the same disambiguator the refund path uses) →
  - **started** (durable evidence of a backend op): re-await by key (`payment_status_by_key`
    fast-skip on Succeeded) — funds are already committed; finishing is correct and cannot
    double-pay;
  - **not started**: RE-RUN the surplus gate against the current ledger before the capped send
    (new liabilities may have been captured since the intent was written) — **excluding the row
    being recovered from its own `paid_out_msat`** (its cap is already subtracted the moment the
    PENDING row exists; gating it against itself would demand the funds twice and falsely
    supersede a sweep that fit exactly). Other PENDING/SENT sweeps still count. If the gate now
    fails, mark the row FAILED with reason `superseded_by_liability` and alert — never send.
- One in-flight sweep at a time (`WHERE status='PENDING'` count must be 0 to accept a new one):
  keeps the surplus math trivially serializable. `sweep_busy` error otherwise.
- Expired bolt11 at execution → FAILED with the backend error; the operator re-issues and re-runs.
- Do NOT reuse the `refund_attempt` table — sweeps must never enter refund liability/readiness
  math, and INV-3 provenance would (correctly) reject them.

## Observability

- `lnrent money` gains `last_sweep` (status, amount, when) and the surplus breakdown (earned /
  reserved / paid-out / surplus) — all ledger reads, no network.
- Sweep attempts appear in the `event_log` journal (`kind='sweep'`) like other money transitions.
- A FAILED sweep fires the PR-5 alert path if present (kind `SweepFailed`); otherwise a WARN log
  suffices; do not create a dependency between the two specs.

## Non-goals

No balance read anywhere in this path (authorization, pre-checks, or acceptance); no scheduled/
automatic sweeps; no sweep-to-LN-address/LNURL (bolt11 only — the operator controls their own
wallet; the resolver stays refund-only); no partial/split sweeps; no fee-limit knob (the quote is
the fee; the cap enforces it); no on-chain/ecash-note export; no change to refund paths or
INV-1/INV-3; no book-vs-wallet drift detection here (that is the explicit reconcile command,
alerting spec §F).

## Acceptance

- Happy path: with only FINAL receipts in the ledger, quote → `--yes` pays the operator invoice,
  ledger goes PENDING→SENT, `lnrent money` shows it. On a funded regtest backend the fedimint_live
  suite gains a sweep test alongside the existing pay test.
- Surplus gate (pure ledger unit tests, no backend needed): at-risk receipts
  (PENDING/PROVISIONING/RESUMING/REFUND_DUE) reserve at gross; non-terminal refunds — including an
  unpriceable one — reserve at gross with per-external_id de-dup; SENT refunds and PENDING/SENT
  sweeps subtract; a receipt becomes sweepable exactly when its sub reaches ACTIVE (and a
  suspended-renewal receipt when RESUMING→ACTIVE lands).
- An uncaptured settlement is inert: settle an invoice at the (mock) backend WITHOUT running
  capture → surplus unchanged, sweep of those funds refused; after capture the receipt appears as
  at-risk, and only at ACTIVE does it become sweepable.
- Fee-rise safety: quote at fee F, raise the gateway fee before send → the capped send refuses;
  nothing paid; row FAILED with the cap error.
- Idempotency/crash: kill between ledger-PENDING and pay-confirm → restart re-drives by key, funds
  sent exactly once (mirror the refund crash tests); the not-started branch re-gates and refuses
  (`superseded_by_liability`) when a new liability consumed the surplus; re-submitting the same
  bolt11 after success returns the cached success, no second payment.
- Zero-amount bolt11, expired bolt11, quote failure (`sweep_unpriceable`), and a second concurrent
  sweep (`sweep_busy`) are structured refusals; nothing is written to `refund_attempt`; a sweep
  never enters the refund LIABILITY set (`required_msat` unchanged) — but it DOES reduce
  ledger-expected holdings (`expected_msat` subtracts SENT/PENDING sweep caps, per the alerting
  spec §D), so readiness correctly reflects that a committed payout shrinks coverage.
- Works identically on `MockPayment` (no balance concept needed — the gate never asks for one).

## Suggested implementation order

1. `sweep_attempt` table + the ledger surplus computation (pure store/unit-testable — this is most
   of the spec).
2. `pay_capped` (or the generalized cap) on the backend trait + Fedimint impl.
3. IPC SweepQuote/Sweep + CLI with `--yes`.
4. Recovery drive + crash tests; fedimint_live sweep test.
