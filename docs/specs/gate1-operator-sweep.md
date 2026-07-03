# Spec: GATE-1 operator sweep / payout (PR-8)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md PR-8 (verified, 2026-07-03). Money-moving — kept as
its own tight spec. This is the ONLY new outbound-payment path besides refunds; everything below
exists to make it impossible for a sweep to eat funds owed to buyers or to double-pay.

## Problem (verified)

Operator profit (sales − refunds) has no daemon-safe exit. The CLI is read-only + admin
(bin/lnrent.rs:27+); the only outbound path is the Refunder's `pay_refund_capped`, INV-1-capped to
specific received amounts. The workaround — a second fedimint client against the daemon-owned
RocksDB — risks corrupting the money DB (ADR-0015). Funds are recoverable from seed+invite but not
withdrawable in operation.

## Design

One IPC command driving one idempotent backend pay, gated by the existing liability math.

### Command surface

- IPC `Request::Sweep { bolt11: String }` (LNRENT operator socket only, like every admin verb).
- CLI `lnrent sweep <bolt11> [--json]`. The CLI first performs a dry-run quote
  (`Request::SweepQuote { bolt11 }` → amount, fee outlay, balance, required liability reserve,
  verdict) and prints it; executing requires the explicit flag `--yes` (mirrors the lnd-payments
  pattern: quote by default, pay only on `--yes`).
- The bolt11 MUST carry an amount (zero-amount invoices rejected: `sweep_invalid`). The amount is
  the invoice's, not a CLI argument — no amount/dest mismatch class. The operator mints the invoice
  in their own wallet.

### Safety gate (the whole point) — FAIL CLOSED

The gate reconciles an external, asynchronously-moving federation balance against the local
liability ledger. Rather than enumerate every read-interleaving, the gate obeys ONE dominant
principle: **if total owed funds cannot be bounded to a concrete msat amount right now, REFUSE the
sweep.** A refused sweep is a minor operator inconvenience; a sweep that guesses low is a drain of
buyer funds. Concretely, the reserve counts owed funds at GROSS and never omits an unbounded one:

- **Unpriceable refund liabilities are reserved at gross, never dropped.** The INV-2 readiness sum
  omits an unpriceable PENDING refund from `required_msat` (it increments `unpriceable_count`); the
  sweep must NOT reuse that omission. If any liability is unpriceable at gate time, either reserve
  its full gross OR refuse the sweep outright with `sweep_unpriceable_liability` — do not let a
  transient quote failure become sweepable headroom.
- **Any invoice whose funds could still be owed is reserved at gross**, covering the
  late-terminal-settlement window: an invoice can be locally EXPIRED/terminal while a late
  settlement is still capturable (SPEC §6.3 — a late settlement on a terminal sub becomes a
  detached refund). So reserve the gross of every invoice row that is NOT provably resolved —
  i.e. every invoice that is neither (a) delivered as ACTIVE service nor (b) already represented by
  a counted refund liability. In practice: all OPEN rows (as above) PLUS any terminal invoice
  inside the late-settlement window that has no `refund_attempt` yet.
- **Quiescence check:** if catch-up or the settlement stream shows any unreconciled settlement in
  flight at gate time, refuse (`sweep_reconciling`) and let the operator retry once the daemon has
  caught up. This closes the residual "settled at the backend, not yet a local row" window without
  chasing its exact timing.

The numeric gate below is evaluated only after the above fail-closed checks pass.

**Reserve every still-payable invoice, not just captured liabilities.** The backend balance can
include an order invoice the daemon RECEIVED but has not yet CAPTURED (settlement/catch-up lag), and
— because the backend keeps receiving external payments asynchronously — a fresh payment can also
land *between* any catch-up scan and the pay. A one-shot catch-up cannot close that race. So the
reserve must be conservative about POTENTIAL liabilities, not just realized ones:

1. Run the supervisor's settlement catch-up first (scan OPEN invoices, `lookup`, capture-if-Paid) so
   already-received orders become visible liabilities.
2. Then include in `reserve_msat` the **gross of every OPEN (non-terminal, un-captured)
   order/renewal invoice in the store — with NO expiry filter.** An invoice row is OPEN only until
   capture settles, refunds, or expires it; while it is OPEN, capture can still accept a settlement
   whose `settled_at < expires_at` and owe/provision against it. Reserving ALL open rows (not just
   "unexpired" ones) is the simplest rule that closes the entire class of expiry-window races: a
   sweep can never withdraw funds an open invoice might still be captured against, regardless of
   the exact moment balance vs reserve are read. It is intentionally conservative (an open invoice
   that ultimately expires unpaid briefly holds sweepable headroom) — the correct bias for an
   operator payout.

Run the catch-up and the reserve read in the same serialized maintenance context. **The read order
is a hard requirement: snapshot `balance_msat = available_balance_msat()` FIRST (call it T0), then
read `reserve_msat` (liabilities + all OPEN rows) at T1 ≥ T0.** With this order the two rules
compose into the full guarantee: any external payment reflected in the T0 balance was paid before
T0, so its invoice row was OPEN (or captured) before T0 and is therefore still counted by the
reserve read at T1. A NEW invoice created after T0 cannot inflate the T0 balance, so it can't make
`ALLOW` pass on funds it will owe. Reserve-before-balance is NOT permitted (a new invoice could
commit after the reserve snapshot and be paid before the balance snapshot, escaping the reserve).

Computed inside the daemon at execution time, atomically with the pay decision (same serialized
maintenance/store context as the Refunder so liability state cannot race):

```
outlay_msat   = payment.refund_required_outlay_msat(amount_sat, Some(amount_sat))   // payout + fee, real gateway quote
reserve_msat  = gross of ALL outstanding refund liabilities INCLUDING unpriceable + parked/manual
                  (fail-closed: unpriceable counted at gross, never omitted)
                + gross_msat of EVERY OPEN (non-terminal, un-captured) order/renewal invoice
                + gross_msat of every terminal invoice still inside the late-settlement window
                  with no refund_attempt yet
                // and only after the unpriceable / quiescence fail-closed checks above pass
balance_msat  = payment.available_balance_msat()  (None or query error → REFUSE, sweep_unavailable)
ALLOW iff balance_msat >= outlay_msat + reserve_msat
```

- Refusal is a structured error naming the shortfall — never a partial sweep. No "force" override:
  if the operator wants to drain past the liability reserve, that is manual seed-level action, not
  a daemon verb.
- Parked/manual refunds count at GROSS in the reserve (they are owed until resolved).

### Idempotency + ledger

- Pay key: `sweep:<payment_hash>` (the bolt11 payment hash — unique per invoice, deterministic on
  retry of the same invoice). **Send with an outlay cap, not bare `pay`:** the gateway fee can move
  between the quote and the send, and bare `PaymentBackend::pay` has no outlay ceiling — a fee rise
  could push the real outlay past the quoted `outlay_msat` and eat into the refund reserve. Reuse
  the capped-send guarantee: extend the backend with `pay_capped(bolt11, amount_sat,
  max_outlay_msat, key)` (or generalize the existing `pay_refund_capped`'s cap check to take an
  explicit `max_outlay_msat` instead of a gross-sat) and pass the just-quoted `outlay_msat`. A
  NEW operation refuses to start if the real `amount*1000 + fee > max_outlay_msat`; existing
  SUCCEEDED/PENDING ops for the key re-await exactly like `pay`. This keeps INV-1's *refund* gross
  semantics separate while giving the sweep its own explicit outlay ceiling. The backend key dedup +
  fedimint payment-hash dedup make a re-submitted invoice safe (re-awaits, never double-pays).
- Durable intent BEFORE pay, mirroring the refund ledger pattern (§6.6): new table `sweep_attempt`
  (id = `sweep:<payment_hash>`, bolt11, amount_sat, max_outlay_msat, status PENDING|SENT|FAILED,
  attempts, created_at, sent_at, last_error). **Recovery must distinguish a started backend payment
  from an unstarted intent, and re-gate the latter** — a crash can leave a PENDING sweep whose pay
  never started while restart catch-up discovers new refund liabilities; blindly re-driving by key
  could spend funds now reserved for refunds. On boot/maintenance, for each PENDING sweep:
  `payment_started_by_key(key)` (the same disambiguator the refund path uses) →
  - **started** (durable evidence of a backend op): re-await by key exactly like a refund
    (`payment_status_by_key` fast-skip on Succeeded) — the funds are already committed, finishing
    is correct and cannot double-pay;
  - **not started**: RE-RUN the full balance/reserve gate against current liabilities before the
    capped send. If the gate now fails, leave the row PENDING (or mark it FAILED with a clear
    "superseded by refund liability" reason) and alert — never send.
  Do NOT reuse the `refund_attempt` table — sweeps must never enter refund liability/readiness
  math, and INV-3 provenance would (correctly) reject them.
- One in-flight sweep at a time (`WHERE status='PENDING'` count must be 0 to accept a new one):
  keeps the gate math simple and honest. `sweep_busy` error otherwise.
- Expired bolt11 at execution → FAILED with the backend error; the operator re-issues and re-runs.

### Observability

- `lnrent money` gains `last_sweep` (status, amount, when) — one row lookup, no new probe.
- Sweep attempts appear in the event_log journal (`kind='sweep'`) like other money transitions.
- A FAILED sweep fires the PR-5 alert path if present (kind `SweepFailed` — add to the enum in the
  alerting spec's build if both land; otherwise a WARN log suffices; do not create a dependency
  between the two specs).

## Non-goals

No scheduled/automatic sweeps; no sweep-to-LN-address/LNURL (bolt11 only — the operator controls
their own wallet; the resolver stays refund-only); no partial/split sweeps; no fee-limit knob
(the gateway quote is the fee; refusal math already accounts for it); no on-chain/ecash-note
export; no change to refund paths or INV-1/2/3.

## Acceptance

- Quote → pay happy path on a funded regtest backend: `sweep` with `--yes` pays the operator
  invoice, ledger goes PENDING→SENT, `lnrent money` shows it, balance drops by outlay.
- Liability gate: with an outstanding refund liability of R and balance < outlay + R, the sweep is
  refused with the shortfall; paying down the liability (refund completes) then allows it.
  Parked/manual liabilities gate at gross.
- Fail-closed: an unpriceable PENDING refund refuses the sweep (`sweep_unpriceable_liability`) or is
  reserved at gross (never omitted); an in-flight unreconciled settlement refuses (`sweep_reconciling`)
  until catch-up completes; a terminal invoice inside the late-settlement window with no
  `refund_attempt` is reserved at gross. In every ambiguous case the sweep refuses rather than
  guessing low.
- Idempotency/crash: kill between ledger-PENDING and pay-confirm → restart re-drives by key, funds
  sent exactly once (mirror the refund crash tests); re-submitting the same bolt11 after success
  returns the cached success, no second payment.
- Zero-amount bolt11, expired bolt11, balance-query failure, and a second concurrent sweep are all
  structured refusals; nothing is written to `refund_attempt`; refund readiness (`lnrent money`
  ready/warning) is byte-identical before/after a sweep with no liabilities.
- Mock backend: `MockPayment` uses the trait-default `available_balance_msat() -> None`, so a
  sweep against the plain mock is refused `sweep_unavailable` — assert that. To exercise the happy
  path/gate math on the mock, the test injects a mock with a configured balance (add a
  `MockPayment::set_balance`/override in the test support, mirroring `set_now`); `pay` then succeeds
  normally (sweep is not dev-gated). The real balance-backed path is exercised in the fedimint_live
  suite (a sweep test alongside the existing pay test).

## Implementation note (money-safety surface)

The sweep gate reconciles an async external balance against the local ledger — an inherently
concurrent surface. This spec pins the money-safety INVARIANT (fail closed: never sweep unless total
owed funds are bounded and covered; balance snapshot before reserve read; reserve every unresolved
row at gross) rather than a single canonical interleaving. The implementer owns choosing the
concrete serialization (e.g. gate + pay under the maintenance lock, quiescence check before quote)
and MUST cover the fail-closed cases with tests. If the chosen mechanism cannot cheaply prove the
invariant, prefer refusing more sweeps over risking one drain.

## Suggested implementation order

1. `sweep_attempt` table + gate math incl. the fail-closed reserve (pure store/unit-testable).
2. IPC SweepQuote/Sweep + CLI with `--yes`.
3. Recovery drive + crash tests; the fail-closed cases (unpriceable, reconciling, late-settlement);
   fedimint_live sweep test.
