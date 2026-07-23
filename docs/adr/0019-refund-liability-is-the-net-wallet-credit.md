# 0019 — A refund is capped at the net wallet credit; the Buyer bears the Lightning fees

A Buyer pays an Order invoice. Something then makes the Subscription undeliverable — provisioning
fails permanently, or the settlement lands on an already-expired invoice — and the Operator owes the
money back. The obvious answer is "refund what they paid." It is the wrong answer, because "what
they paid" is not what the Operator holds.

Two fees sit between the two numbers, and neither is the Operator's to avoid:

- **The receive cost.** What lands is materially less than the invoice. The gateway's receive fee is
  only part of it: the claim transaction *also* pays an lnv2 input fee and mint-output fees, which is
  why `lnv2_backend.rs` reads the ACTUAL mint-output-minus-input delta from the accepted claim
  transaction rather than trusting `contract.commitment.amount`. Measured live (7qc, 2026-07-20):
  a 50 sat invoice credited 43,550 msat (43 sat after the whole-sat floor), of which only ~2.25 sat
  was ostracoda's advertised receive fee
  (2000 msat + 5000 ppm) — **more than half the gap was fedimint consensus fees**. phoenixd has an
  analogous receive-side gap (ADR-0018). The Operator never held the difference either way.
- **The send cost.** Paying the refund out over Lightning costs again, and it is more than the
  gateway routing fee: the outbound ecash debit is `payout + gateway fee + the send transaction's own
  mint input/change consensus fees` (`outgoing_outlay_msat`). On lnv2 the *whole* debit IS bounded
  before sending — `net_payout_sat` searches the payout downward so the total outlay (gateway
  worst-schedule fee *and* consensus fees, which are non-monotone in note selection) fits
  `≤ gross` — but it is still a real cost that comes out of what was received.

So "refund the invoice amount" would have the Operator pay out strictly more than it took in, on a
transaction that earned it nothing. Repeat that and the float bleeds. Worse, it is *addressable*: a
hostile Buyer who can reliably force refunds (order against a recipe they know will fail to
provision, pay, collect, repeat) turns every cycle into a withdrawal from the Operator's working
balance. The cost per cycle is the Buyer's own routing fee; the damage is the Operator's whole
float. Any policy where the Operator tops up refund fees is a drain vector, not a courtesy.

## Decision

**The refund liability is the actual net wallet credit, floored to whole sats, and the send fee is
paid out of that same liability.** The Operator is made whole; the Buyer bears both fees.

The *credited amount* is recorded at capture from the observed credit, never from the invoice face
value; each refund **liability** is then created at its own site (a failed provision's
`RefundDueWrite`, a resume failure's `ResumeFailureWrite`, or capture's expired/terminal-settlement
gate) and copies that recorded credit as its ceiling (`daemon/src/capture.rs`):

```rust
// lnv2 credits `invoice - receive_fee`. Refund only whole sats from that ACTUAL wallet credit;
// flooring a sub-sat remainder is conservative and matches the whole-sat payout trait.
let refundable_sat = s.received_msat / 1000;
```

(The comment simplifies: `received_msat` is not literally `invoice − receive_fee` but the actual
wallet increase after *all* receive-side fees — the gateway receive fee plus the lnv2 input and mint
input/output consensus fees, per the mint-delta the receive-cost bullet describes. A future backend
must baseline the liability on that measured credit, not the invoice-minus-one-fee shorthand.)

This is why `received_msat` is threaded through the entire money core rather than being a
backend-local detail (lnrent-3d5): capture, ledger, refund, provision, resume, sweep, store and
supervisor all need the *credited* amount, because the credited amount is the ceiling on everything
the Operator can ever owe.

The consequence, INV-1: **the Operator's refund outlay is capped at the whole-sat liability that
Order credited** — `floor(received_msat / 1000)` sat, i.e. `PayCap::Gross(gross_sat)` enforced as a
`gross_sat * 1000` msat ceiling. The cap is a *preflight* (`lnv2_worst_fee_msat` prices the worst
advertised schedule; the payout is searched so `payout + worst_fee ≤ gross`), not an atomic
guarantee: lnv2's `send` takes no max-fee parameter and re-fetches routing info **and funds** after
our check (`daemon/src/lnv2_backend.rs`). Two residual exposures remain. (1) A gateway that
*re-prices* between our gateway selection and upstream `send`'s routing-info refetch. lnrent's own
selection now applies a **componentwise** fee guard (`lnv2_send_usable`: `base ≤ 100_000 msat && ppm
≤ 15_000`, z2v Part 1), strictly tighter than upstream `send`'s **lexicographic** `PaymentFee` gate
(base, then ppm) — componentwise-accept implies lexicographic-accept, so the guard never *selects* a
low-base/high-ppm gateway visible at preflight (its proportional fee could exceed the reserved cap)
while never pinning a gateway `send` would refuse. What remains is a genuine send-time TOCTOU: a
gateway that advertises a componentwise-safe schedule at selection, then re-prices to
low-base/high-ppm before upstream `send` re-fetches routing info; `send`'s lexicographic gate accepts
that reprice (a `99_999`-base schedule sorts below the limit regardless of ppm) and lnrent's
selection-time guard, having already run, cannot see it. **Recorded decision (z2v Part 2): RETAIN
this residual; do not patch the fork.** The attack requires a guardian-vetted gateway to maliciously
re-price within the sub-second window between selection and `send`'s refetch, to skim one refund's
fee — below the risk bar, and the componentwise selection guard already closes the preflight-visible
hole. A componentwise check at `send`'s own use boundary inside the douglaz/fedimint fork (kept
indefinitely as standing patch infrastructure, lnrent-8ym/e96) is the only place that truly prevents
the overrun and remains available as option 3 if the bar changes. (2) A concurrent **incoming
claim** (a receive settling)
that changes the ecash note set between our dry-run and the send: `pay_inner`'s `pay_start_lock`
serializes *outbound* pays (so no refund or sweep can interleave), but not inbound claims, and mint
consensus fees are non-monotone in note selection, so the finalized debit can differ slightly from
the priced dry-run. This second residual is small (a few consensus-fee units), unlike (1). So the honest claim is "capped at the floored liability at
preflight, modulo a narrow re-price TOCTOU," not "can never" — a forced-refund loop is at worst
fee-neutral for the Operator, save that residual, instead of a bleed.

## Consequences

**The Buyer gets back less than they paid, and at small prices the gap is stark.** From the
lnrent-7qc live run (2026-07-20, mainnet): Buyer paid 50 sat, the wallet was credited 43,550 msat
(`received_msat`), which **floors to a 43 sat liability** — so the refund outlay is capped at
43,000 msat, and the refund paid a 36 sat invoice plus its send fee (gateway + consensus) under that
cap. The Buyer recovered ~72% of a failed 50 sat order. (It first sat PENDING on an under-funded
float, then self-funded from the next sale — see "temporarily unpayable" below.) At the shipped
default price scale (tens of thousands of sats) the same absolute fees are noise — but an Operator
who sets a very low price is also setting a bad refund experience, and should know it — so it is
also stated operator-facing in the go-live runbook (`docs/go-live.md`, "Operate"), not only here.

**A refund can be temporarily unpayable, and that is correct.** The liability is fixed at the net
credit, but the send fee is quoted at drive time. What actually holds a refund `PENDING` (rather than
parking it FAILED or paying a partial amount) is the Refunder's own transient-error path: when the
outbound fee **quote cannot be priced** — e.g. an under-funded float where the lnv2 mint-funding
dry-run can't yet fund the send — `Refunder::drive` leaves the row `PENDING` and retries
(`daemon/src/refund.rs`). It does **not** consult the ledger's expected holdings; the Refunder just
tries to pay, and an unfundable send surfaces as that quote failure. The daemon's separate
*readiness report* is **report-only** (ADR-0016) and runs after the drive — it surfaces
`a pending refund liability could not be priced` (`Unpriceable`) for that quote failure, and
independently `ledger expected holdings are below required refund outlay` (`InsufficientBalance`)
when the books are short — but neither warning *gates* payment; they inform the Operator, they don't
defer the refund. Observed live in the 7qc run: a refund sat PENDING for ~4 minutes on an
under-funded float — the fee quote could not be priced — and self-healed the moment the next sale
landed, the documented "refunds self-fund from sales" behaviour. The Operator's mitigation is a small
float, not a policy change.

**Refund amounts are whole sats.** The sub-sat remainder is floored, and the flooring is the
Operator's. This matches the whole-sat payout trait and keeps the ledger free of msat dust; it is
conservative in the safe direction (never over-refund).

**Backend-specific: phoenixd deducts a MAX fee, not a quoted one.** lnv2 can price the outbound fee
before sending (`routing_info` publishes the schedule), so the Buyer is charged close to the actual
cost. phoenixd reports `routingFeeSat` only *after* the payment, and accepts **no** caller-supplied
fee bound — `payinvoice` takes only the invoice and an optional amount. lnrent therefore cannot
*impose* a ceiling; it can only *know* one, because phoenixd's outbound fee is a single hardcoded
trampoline tier (below). lnrent computes `max_fee` from that known schedule and deducts it:
`payout = liability − max_fee`, with `actual_fee ≤ max_fee` guaranteed by phoenixd's own
construction rather than by any parameter lnrent passes, so `payout + actual_fee ≤ liability` holds
pre-payment. **INV-1 on phoenixd therefore rests on an external constant lnrent cannot set** — which
is exactly why it is pinned and version-checked rather than assumed.

One phoenixd-specific liability-baseline caveat for xk3: a small inbound payment can land entirely in
`Part.FeeCredit`, and phoenixd's `receivedSat` still counts it, while `/getbalance` excludes it from
spendable `balanceSat` (reporting it as `feeCreditSat`). Mapping `receivedSat` straight into
`received_msat` would make such an order *refundable with no spendable credit behind it*, so a forced
refund would draw down existing operator balance — reopening the drain. The phoenixd liability
baseline must therefore **exclude fee credit** (or reject a fee-credit-only receipt), the same
"baseline on the measured *spendable* credit" rule the Decision states for lnv2 (tracked on the
phoenixd design, lnrent-b2f).

Two consequences lnv2 does not have. The trampoline fee is **exact in msat, not a ceiling**: a
successful trampoline-routed payment pays the full msat tier fee regardless of the real route cost, so
`actual_fee == max_fee` in msat. `max_fee` is computed in msat (below), but the **payout is floored to
whole sats**, so the deduction leaves a sub-sat msat remainder the Operator keeps (e.g. a 43-sat /
43,000 msat liability → a 38-sat payout at a 4,152-msat fee = 42,152 msat spent, 848 msat left).
The larger exception is a payment routed directly to the LSP, which
costs **zero** — there the Buyer is under-refunded by the entire `max_fee`. And
`routingFeeSat` is reported **floored to whole sats**, so the true msat cost can exceed the reported
figure by up to 999 msat; `max_fee` must be computed from the schedule in **msat with a ceiling**,
never reconstructed from the reported field.

`max_fee` is not a guess: phoenixd's outbound fee is a **single-tier trampoline schedule hardcoded
in its own source** (`conf/Lsp.kt` @ v0.9.0: `feeBase = 4.sat, feeProportional = 4_000` — 4 sat +
0.4%, one entry, no escalating retry ladder). That is the fee budget the payment is sent with, so the
actual routing fee cannot exceed it — an attempt needing more fails rather than costing more. This
makes phoenixd's fee *quotable from a constant*, structurally the same as lnv2 quoting from
`routing_info`, and it is why INV-1 can stay a hard pre-payment refusal on both backends rather than
degrading to best-effort on one.

Because that constant lives in phoenixd rather than here, lnrent treats it as **operator-config with
a version-verified default**, not a bare literal — preflight asserts the running phoenixd matches the
version the schedule was read from, and on a mismatch it must **fail closed: refuse automated refunds
until the operator explicitly configures a verified schedule**, not merely warn. A warn-and-continue
would let a phoenixd upgraded to a higher trampoline schedule keep deducting the stale (too-small)
`max_fee` — and since `payinvoice` accepts no caller-supplied fee cap, the larger actual fee would
violate INV-1 silently, the same failure shape as a provider retiring an image slug that passes every
launch gate and only fails after a Buyer has paid (lnrent-1sr). (Design intent, not yet built: no
phoenixd backend exists until lnrent-xk3.)

A reserve-and-check-afterwards scheme was rejected outright: the check runs after the money has left,
and the Buyer chooses the destination (and therefore influences the route and the fee), which would
hand the drain vector back through a side door.

**This is fee incidence, not a service credit.** A Buyer harmed by a failed provision is made
*nearly* whole, not whole. If lnrent ever wants true make-whole semantics, that is a product
decision requiring a separate funding source (an Operator-declared reserve priced into the listing),
and it must not be implemented by letting refunds outspend receipts — that reopens the drain vector
this ADR exists to close.

## Alternatives considered

**Refund the full invoice face value.** Rejected: pays out more than was received on every refund,
and is directly exploitable as described above.

**Operator absorbs only the send fee, Buyer absorbs the receive fee.** Rejected: it is bounded (the
lnv2 worst-schedule quote makes the exposure quantifiable, so this is not an unbounded bleed), but it
is still the Operator paying out more than the Order credited on every refund, which is the drain
vector above with a smaller constant. Bounded-per-refund is not bounded-in-aggregate when a Buyer
can force refunds repeatedly.

**Quote the send fee and gross it up into the refund invoice.** Rejected: the grossed-up payout
would exceed the net credit, violating INV-1 by construction.
