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
  a 50 sat invoice credited 43 sat, of which only ~2.25 sat was ostracoda's advertised receive fee
  (2000 msat + 5000 ppm) — **more than half the gap was fedimint consensus fees**. phoenixd has an
  analogous receive-side gap (ADR-0018). The Operator never held the difference either way.
- **The send fee.** Paying the refund out over Lightning costs again. On lnv2 this fee IS bounded
  before sending — `lnv2_worst_fee_msat` takes the worst of the gateway's advertised default/minimum
  schedules and the payout is searched so `payout + worst_fee ≤ gross` — but it is still a real cost
  that comes out of what was received.

So "refund the invoice amount" would have the Operator pay out strictly more than it took in, on a
transaction that earned it nothing. Repeat that and the float bleeds. Worse, it is *addressable*: a
hostile Buyer who can reliably force refunds (order against a recipe they know will fail to
provision, pay, collect, repeat) turns every cycle into a withdrawal from the Operator's working
balance. The cost per cycle is the Buyer's own routing fee; the damage is the Operator's whole
float. Any policy where the Operator tops up refund fees is a drain vector, not a courtesy.

## Decision

**The refund liability is the actual net wallet credit, floored to whole sats, and the send fee is
paid out of that same liability.** The Operator is made whole; the Buyer bears both fees.

The liability is recorded at capture time from the observed credit, never from the invoice face
value (`daemon/src/capture.rs`):

```rust
// lnv2 credits `invoice - receive_fee`. Refund only whole sats from that ACTUAL wallet credit;
// flooring a sub-sat remainder is conservative and matches the whole-sat payout trait.
let refundable_sat = s.received_msat / 1000;
```

This is why `received_msat` is threaded through the entire money core rather than being a
backend-local detail (lnrent-3d5): capture, ledger, refund, provision, resume, sweep, store and
supervisor all need the *credited* amount, because the credited amount is the ceiling on everything
the Operator can ever owe.

The consequence, INV-1: **the Operator can never pay out more on a refund than that Order actually
credited.** A refund cannot overdraw, and a forced-refund loop is at worst fee-neutral for the
Operator instead of a bleed.

## Consequences

**The Buyer gets back less than they paid, and at small prices the gap is stark.** From the
lnrent-7qc live run (2026-07-20, mainnet): Buyer paid 50 sat → 43 sat credited → a 36 sat refund
invoice plus ~7.4 sat send fee. The Buyer recovered 72% of a failed 50 sat order. At the shipped
default price scale (tens of thousands of sats) the same absolute fees are noise — but an Operator
who sets a very low price is also setting a bad refund experience, and should know it. This belongs
in the go-live runbook, not only here.

**A refund can be temporarily unpayable, and that is correct.** The liability is fixed at the net
credit, but the send fee is quoted later, and coverage is judged against the LEDGER's expected
holdings, never by reading the wallet balance (ADR-0016). If the books cannot
cover `refund + fee`, the refund stays `PENDING` and the daemon reports
`refund readiness warning: a pending refund liability could not be priced`, rather than parking
FAILED or paying a partial amount. Observed live in the 7qc run: a refund sat PENDING for ~4 minutes
on an under-funded float and self-healed the moment the next sale landed — the documented
"refunds self-fund from sales" behaviour. The Operator's mitigation is a small float, not a policy
change.

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

Two consequences lnv2 does not have. The trampoline fee is **exact, not a ceiling**: a successful
trampoline-routed payment pays the full tier fee regardless of the real route cost, so normally
`actual_fee == max_fee` and there is no residual. The exception is a payment routed directly to the
LSP, which costs **zero** — there the Buyer is under-refunded by the entire `max_fee`. And
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
a version-verified default**, not a bare literal — preflight is to assert the running phoenixd
matches the version the schedule was read from and warn on mismatch. (Design intent, not yet built:
no phoenixd backend exists until lnrent-xk3.) A future phoenixd that raises the schedule
would otherwise break INV-1 silently — the same failure shape as a provider retiring an image slug
that passes every launch gate and only fails after a Buyer has paid (lnrent-1sr).

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
