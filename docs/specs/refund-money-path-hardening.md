# Spec: refund fee-deduction, liability-gated readiness, and refund provenance

Status: **Proposed** — money-path hardening for the Fedimint payment backend wired in lnrent-o6p.
Scope: `daemon/src/{backends.rs, fedimint_backend.rs, refund.rs, capture.rs, provision.rs, supervisor.rs, store.rs}`.
Audience: the rb-lite implementer. This spec is the contract; tests below are mandatory ship gates.

## 1. Motivation

Three money-path defects exist now that the daemon can move real ecash (o6p, commit 87e2253):

1. **Refunds pay the full received amount; the operator eats the gateway fee.** Both refund-creation
   paths record the *gross* received amount as the refund figure (`capture.rs::refund_intent` →
   `amount_sat = s.amount_sat`; `provision.rs::RefundDueWrite::new` → `amount_sat = t.order_amount_sat`).
   `refund.rs::process` resolves a bolt11 for `owed_msat = amount*1000` and calls
   `payment.pay(bolt11, amount, key)`. `fedimint_backend.rs::pay` asserts `inv_msat == amount*1000`
   then calls `LightningClientModule::pay_bolt11_invoice`, which charges the selected gateway's
   advertised `RoutingFees` **on top of** the invoice amount. Net effect: operator outlay =
   `received + fee`. The buyer chooses the refund destination, but in Fedimint the gateway absorbs
   route-finding/routing-risk behind its advertised schedule; the buyer cannot make the operator pay
   arbitrary route fees. The drain is still real: each forced refund costs the operator the selected
   gateway's advertised outbound fee unless the refund amount is reduced by that fee.

2. **The readiness warning is a pure balance check, not a liability check.**
   `fedimint_backend.rs::log_readiness` warns when `balance_sat == 0 || !gateway_ok` with the message
   "refunds will fail until the operator funds ecash". A *fresh* operator legitimately has zero balance
   and owes nothing, yet sees a scary warning. (The code comment even claims a per-pending-refund
   exposure check is "wired in the supervisor" — it is not; only a stale comment exists.)

3. **No explicit guard ties a refund to a received payment.** Refunds are unpayable before a sale only
   *by construction* (refund rows are created exclusively from a `Settlement` in `capture.rs` or a PAID
   order in `provision.rs`). There is no invariant or assertion preventing a refund from executing
   without provenance, and nothing documents the self-funding relationship.

## 2. Invariants (the contract)

- **INV-1 — capped, fee-bearing refund.** For every refund, the operator's *total outlay* (the amount
  delivered to the buyer **plus** the actual gateway/routing fee the backend incurs) MUST be ≤ the
  amount the operator received for that order. The **buyer bears the fee**. A refund whose received
  amount is too small to cover any positive whole-sat payout plus the fee (a "dust refund") MUST NOT be
  auto-paid; it parks for operator/manual handling.

- **INV-2 — liability-gated readiness warning.** A refund-readiness WARNING ("we may be unable to honor
  what we owe") MUST be emitted only when an actual **liability** exists that the operator cannot cover.
  A *liability* is value received but not yet delivered or refunded. With **no liabilities, there are no
  readiness warnings**, regardless of balance.

- **INV-3 — refund provenance.** A refund MUST NOT be executed unless it corresponds to a payment
  actually received for that order. Refunding before/without a received payment is forbidden and is a
  hard error (park, never pay).

These compose: INV-1 makes each refund self-funded (net payout + gateway fee ≤ received, and the
received ecash is held by the client), so INV-3's "no refund before first sale" is a structural
consequence — there is no received ecash, therefore no refund row, therefore nothing to pay.

## 3. Design

### 3.1 INV-1 — fee-bearing, capped refunds

**Fee model.** The selected gateway advertises `RoutingFees { base_msat: u32, proportional_millionths:
u32 }` (`LightningGateway.fees`, reachable via `LightningClientModule::get_gateway(self.gateway,
false) -> Result<Option<LightningGateway>>`). For an outgoing payment of `x_msat` the gateway charges
`fee(x) = base_msat + floor(x_msat * ppm / 1_000_000)`. Fedimint's gateway contract makes this
advertised schedule the operator's fee exposure; route-specific over/under-performance is the gateway's
risk, not something the buyer can amplify by choosing a hard destination.

**Normative net algorithm.** Refund payouts are whole sats because `PaymentBackend::pay` accepts sats
and the Fedimint implementation rejects bolt11 amounts that are not exactly `amount_sat * 1000` msat.
Therefore the calculation MUST choose the largest whole-sat payout `n_sat` whose total outlay fits the
received amount:

```
R_msat      = gross_sat * 1000
pay_msat(n) = n * 1000
fee_msat(n) = if n == 0 { 0 } else { base_msat + floor(pay_msat(n) * ppm / 1_000_000) }
valid(n)    = pay_msat(n) + fee_msat(n) <= R_msat   // valid(0) is ALWAYS true: n==0 is NO payment,
                                                    // hence zero payout and zero fee
net_sat     = max n in [0, gross_sat] such that valid(n)   // always well-defined; net_sat == 0 == dust
```

Do **not** implement the refund amount by computing
`floor(max(0, R_msat - base_msat) * 1_000_000 / (1_000_000 + ppm) / 1000)` as the final sat value. That
closed-form inverse is a safe lower bound in msats, but it is not the largest whole-sat payout once the
fee itself floors: for example, with `base=0, ppm=1`, a 1-sat refund has zero proportional fee and is
payable, while the closed form floors to `0` sats. The implementer may use the closed form only as a
starting bound, then adjust against `valid(n)`, or simply binary-search the monotone predicate above.

**Overflow discipline.** The implementation MUST do all msat and fee arithmetic in `u128` (or a checked
big-enough integer) before converting the final `net_sat` back to `u64`. This is required even though
`base_msat` and `ppm` are `u32`: `gross_sat * 1000 * ppm` can overflow `u64` for large test values.
The denominator `1_000_000 + ppm` also MUST be widened before addition. The binary-search bounds remain
`u64` (`0..=gross_sat`), but every call to `valid(n)` uses widened arithmetic. The chosen `pay_sat`
MUST be representable: use `pay_sat.checked_mul(1000)` (never `saturating_mul`) wherever an msat value
crosses into the resolver / bolt11 / Fedimint amount-equality preflight; an auto-pay amount whose msat
form would overflow `u64` is parked for manual handling. (Unreachable in practice — it would need a
gross exceeding the entire bitcoin supply — but the downcast must be checked, never wrapped/saturated.)

**Dust boundary.** `net_sat == 0` means **no positive whole-sat payout** satisfies `valid(n)`. It is not
equivalent to `R_msat <= base_msat`, and it is not equivalent to the closed-form lower bound flooring to
zero. Examples:

- `base=0, ppm=1, gross=1 sat` is **not dust** (`fee(1 sat)=0`, outlay = 1 sat).
- `base=999 msat, ppm=0, gross=1 sat` is dust (`1 sat + 999 msat > 1 sat received`).
- `base=2000 msat, ppm=0, gross=1 sat` is dust: only `n==0` (no payment) satisfies `valid`, so
  `net_sat=0` and no payment starts. The `n==0 ⇒ no fee` rule keeps `net_sat` well-defined even when
  `base_msat > R_msat` (the empty-set case the naive predicate would produce).

A true dust refund parks FAILED via the structural/manual path ("received amount below the network fee;
cannot auto-refund without operator loss"). It must not be retried as a transient payment failure.

**Gateway unavailable is not dust.** If the configured/selected gateway cannot be read, `refund_net_sat`
MUST return an error (or a typed transient quote result), not `0`. The refunder leaves the row PENDING
and the supervisor's liability warning reports the gateway outage. A gateway outage may resolve later;
parking it as a structural dust failure would violate INV-2/INV-3 by suppressing a real liability.

**New trait method** (`backends.rs`, on `PaymentBackend`), with a default so non-fee backends and the
test `MockPayment` are unchanged:

```rust
/// The maximum NET whole-sat amount this backend can auto-send for a refund of `gross_sat`, after
/// reserving the outbound fee so total outlay never exceeds what was received (INV-1, anti-drain).
/// Returns Ok(0) only for true dust: no positive whole-sat payout plus fee fits inside `gross_sat`.
/// Quote/operability failures (for example no gateway) are Err/transient, not Ok(0).
/// Default: no fee (returns `gross_sat`) — correct for MockPayment and any internal-only backend.
async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> { Ok(gross_sat) }
```

`FedimintPayment` overrides it: select the same configured gateway that refund pay will use, read
`fees`, apply the exact whole-sat algorithm above, and return `net_sat`. The query is read-only and MUST
NOT mint, pay, or mutate state.

**Final cap preflight.** Quoting and paying are separate async operations, so the Fedimint refund pay
path MUST repeat the cap check immediately before initiating any new outbound operation, using the same
`LightningGateway` object (and therefore the same advertised fee schedule) that it passes into
`pay_bolt11_invoice`. The refunder should call a refund-specific helper rather than the plain `pay`
when gross context is available:

```rust
/// Idempotent refund pay with a final INV-1 cap check before any new backend operation.
/// Existing SUCCEEDED/PENDING operations for `idempotency_key` are re-awaited as today.
/// For a new operation, the backend MUST refuse to start if `amount_sat*1000 + fee(amount) > gross_sat*1000`.
async fn pay_refund_capped(
    &self,
    bolt11: &str,
    amount_sat: u64,
    gross_sat: u64,
    idempotency_key: &str,
) -> Result<String> {
    self.pay(bolt11, amount_sat, idempotency_key).await
}
```

The default delegates to `pay` for mock/internal backends. `FedimintPayment` overrides it by preserving
today's idempotency behavior (`SUCCEEDED` fast return, `PENDING` re-await, failed/absent key may start a
new operation), parsing the bolt11 and enforcing the existing amount-equality assertion, selecting the
gateway, checking the current advertised fee against `gross_sat`, and only then calling
`pay_bolt11_invoice(Some(the_same_gateway), ...)`. A final-cap failure before any operation exists is a
structural/manual refund failure for fixed bolt11 destinations; for resolvable destinations it may be
handled by clearing the stale resolution and re-resolving at a new generation, but it MUST NOT start an
over-gross payment.

**Refunder change** (`refund.rs::process`). `amount_sat` in `refund_attempt` remains the gross liability,
but the payable amount is generation-specific:

```
received = verify_refund_provenance(row)?       // INV-3; returns the actual received sats
assert row.amount_sat == received               // NULL/negative/mismatch parks as provenance failure

// plan_payment inspects the CURRENT generation FIRST and quotes fees ONLY when a NEW outbound op
// starts, so a gateway-down quote can never strand an already in-flight/settled generation:
//   SUCCEEDED                     -> AlreadySent (record SENT, no pay, no quote)
//   PENDING/Unknown               -> Pay{ persisted bolt11, persisted pay_sat } (re-await; NO new quote)
//   none / current Failed+expired -> NEW payment, and ONLY here:
//        net_cap = refund_net_sat(received)
//          Err  -> transient quote/gateway problem: leave the row PENDING, retry next drive (NOT dust)
//          0    -> park FAILED via the structural/manual dust path
//          else -> resolve/parse a bolt11 for the chosen pay_sat (<= net_cap), persist the new gen
plan = self.plan_payment(row, external_id, dest, received, now).await?
match plan {
    AlreadySent            -> finish SENT,
    Skip                   -> noop,
    Pay{bolt11, pay_sat, key} -> self.payment.pay_refund_capped(&bolt11, pay_sat, received, &key).await,
}
```

`plan_payment` therefore OWNS the quote and takes `received` (gross), not a pre-quoted cap: it calls
`refund_net_sat` only on the new-payment branch. The re-await branches pass the persisted `pay_sat` and
never touch the gateway, so neither a fee change nor a gateway outage can re-price or strand an existing
generation.

Do not clamp `NULL` or negative `refund_attempt.amount_sat` to zero and then mark the refund handled.
INV-1 needs a known positive received amount. If the provenance row has a positive amount but
`refund_attempt.amount_sat` is missing or different, park FAILED as an invariant/provenance violation
(or explicitly backfill in the same transaction before pricing, if the implementation chooses that
migration path). Never pay using a guessed gross amount.

**Resolved/direct invoice amount is the pay amount.** `plan_payment` must return the exact amount to
pass to `pay_refund_capped` along with the bolt11 and generation, e.g.
`PlanOutcome::Pay { bolt11, gen, pay_sat }`. For newly resolved LNURL/LN-address destinations,
`pay_sat == net_cap` and the resolver must verify the returned bolt11 amount is exactly `net_cap*1000`.
For persisted resolutions and direct bolt11 pass-through, `pay_sat` is derived by parsing the persisted
or direct bolt11 amount; it is **not recomputed** from the latest gateway quote.

**bolt11 pass-through (gen 0) — Q1 resolved.** Direct bolt11 refunds remain allowed for compatibility
with existing rows and the gen-0 bare-key dedup path, but the operator cannot rewrite their amount.
Parse the bolt11 before pay:

- if the bolt11 has no amount, or its amount is not a whole number of sats, park FAILED;
- let `bolt11_sat = inv_msat / 1000`;
- if `bolt11_sat <= net_cap`, pay exactly `bolt11_sat` by calling
  `pay_refund_capped(bolt11, bolt11_sat, received, bare_key)`;
- if `bolt11_sat > net_cap`, park FAILED ("buyer's fixed bolt11 exceeds the fee-adjusted refund").

This reconciles the existing Fedimint `pay` amount-equality assertion: for direct bolt11, the argument
is the invoice's own whole-sat amount, not the larger `net_cap`. If the buyer supplies an invoice below
the cap, they receive that smaller amount and the operator's outlay still fits inside the gross.

**Stored liability stays gross.** `refund_attempt.amount_sat` continues to record the *received* amount
(what INV-2 reports as gross exposure and INV-3 checks as provenance). Fee deduction is applied only at
payout. The gross ↔ cap ↔ generation-pay amount distinction must be explicit in names and logs:
`gross/received_sat`, `net_cap_sat`, and `pay_sat` are different quantities.

**Idempotency interaction.** The amount paid for a generation is bound to the bolt11 for that generation.
A PENDING/Unknown payment re-awaits the persisted bolt11 and passes the persisted/direct invoice amount
(`pay_sat`) with the same generation key — no recompute, no amount-mismatch preflight, and no double-pay.
A re-resolution is allowed only when the current generation is definitely `Failed` and expired; that
means the old payment cannot settle, so the implementation may quote the current gateway fees, request a
new `net_cap`, persist a new bolt11/generation, and pay the new amount. If gateway fees changed between
generations, each generation still satisfies INV-1 because both the quote and the final cap preflight are
bounded by the same `received` gross amount.

### 3.2 INV-2 — liability-gated readiness

**Backend.** `fedimint_backend.rs::log_readiness` stops warning on `balance_sat == 0`. It logs balance +
gateway reachability at INFO. It MAY warn only on `!gateway_ok`, reframed as an **operability** condition
("gateway unreachable: cannot create invoices or pay refunds") — never "fund your ecash". The supervisor
is responsible for money-readiness warnings because it can see liabilities.

**Liability set — Q2 resolved.** The readiness check must count value that has been received and is not
yet delivered/refunded, without double-counting historical paid invoices whose service was already
applied. Define two related numbers:

- `gross_liability_sat`: the received amount still owed/deliverable, for operator visibility.
- `required_outlay_msat`: the **msat** balance an as-yet-UNSTARTED automated payment needs now. Summed
  ONLY over liability rows that would start a NEW outbound operation this drive; a row whose current
  generation already has a PENDING/Unknown backend payment contributes 0 (funds already debited/locked —
  counting it would falsely warn that fresh balance is needed). Per contributing row:
    - a capped refund with a fixed/resolved `pay_sat`: `pay_sat*1000 + current_fee(pay_sat*1000)`;
    - a capped refund not yet resolved: `net_cap*1000 + current_fee(net_cap*1000)`;
    - a paid-but-undelivered order with NO refund row yet (a *potential* refund): the full
      `received*1000` (no fee discount — no refund is priced yet). This conservative reserve makes a paid
      PROVISIONING order whose ecash is missing actually warn.
  It is never greater than `gross*1000` when INV-1 holds. The comparison is in **msats**, NOT sat-rounded:
  flooring the balance to sats while ceiling the requirement to sats can falsely warn when the exact msat
  balance covers the liability (1500 msat available vs 1500 msat required must NOT warn, but `1 < 2`
  would). Round to sats only for display.

The liability rows are:

1. **Refund ledger rows:** every `refund_attempt` whose status is not `SENT` and whose provenance shows
   received funds. ALL such rows count in `gross` (visibility). For `required_outlay_msat`: a `PENDING`
   row WITHOUT an in-flight backend payment (none started, or current gen `Failed+expired`) is an
   as-yet-unstarted liability and contributes; a `PENDING` row whose current generation already has a
   PENDING/Unknown backend payment is in-flight (funds committed) and contributes 0. `FAILED` rows are
   parked/manual liabilities (money received, not refunded) — counted in `gross`, surfaced as
   `parked_count`, never retried or hidden. Dust/no-destination/manual failures do not vanish from
   accounting because automation parked them.
2. **Paid order not yet delivered:** an order invoice with received-payment provenance (`invoice.kind =
   'order'` and (`invoice.status = 'PAID'` or `invoice.settled_at IS NOT NULL`)) whose subscription is in
   `PENDING`, `PROVISIONING`, or `REFUND_DUE`, excluding any external_id already represented by a
   `refund_attempt` row in (1). Mainly `PROVISIONING` in normal operation: the buyer paid, the sub is not
   ACTIVE yet. Contributes `received*1000` to `required` (potential-refund reserve). `REFUND_DUE` without
   a refund row is an invariant gap and must still count so the warning is not falsely suppressed.
3. **Unreconciled settlements:** any received settlement (an `invoice` with `settled_at IS NOT NULL`, or a
   settle-refund `event_log` entry) whose external_id is NOT already counted in (1) or (2) — it has NO
   `refund_attempt` row AND is not the paid-undelivered order of bucket (2) — and whose subscription is
   NOT ACTIVE/delivered. This is the residual invariant-gap case: the late/terminal settlement where
   capture stamped `settled_at` and left the sub terminal (so bucket (2)'s PENDING/PROVISIONING/REFUND_DUE
   filter misses it) yet no refund row exists. **De-dup the entire liability set by external_id** — no
   external_id may be counted in more than one bucket (precedence (1) > (2) > (3)). Counts at `received`
   so a missing refund row cannot silently suppress the warning.
4. **Renewals:** a within-grace renewal does **not** count separately once captured, because capture
   extends `paid_through`/resumes service atomically in the same transaction that marks the renewal paid.
   A late/terminal renewal that cannot be delivered is represented by a `refund_attempt` and counted in
   (1). Do not count old PAID renewal invoices by joining them to a later `SUSPENDED`/`TERMINATED`
   subscription; that re-introduces false-positive noise for already-delivered service.

**Supervisor.** Add a liability-aware readiness check (the supervisor has both the store and the
backend). At boot (after recovery) and on each maintenance tick:

```
liabilities = load_liabilities_from_store()
if liabilities.is_empty():
    -> no money-readiness warning (the fresh-operator case; INV-2)
else:
    gateway_ok = payment.refund_gateway_ready() or equivalent Fedimint health check
    bal_msat = payment.available_balance_msat()   // None for backends without a balance concept -> skip balance compare
    required_msat = sum(required_outlay_msat over NOT-in-flight automated liabilities priceable now)
    gross = sum(gross_liability_sat over all liabilities, including parked/manual)

    if !gateway_ok:
        WARN with {gross, required_msat, bal_msat, gateway_ok=false, parked_count}
    else if bal_msat is Some(b) AND b < required_msat:
        WARN with {gross, required_msat, bal_msat=b, gateway_ok=true, parked_count}
    else if parked_count > 0:
        WARN/ERROR manual-liability alert with {gross_parked, parked_count}, not a "fund ecash" warning
    else:
        -> no warning
```

Using `gross` as the balance threshold would be conservative but can be noisy: a capped refund may be
coverable with `required_liquidity_sat < gross_liability_sat`. The warning condition is therefore based
on required liquidity; the log still includes gross liability so the operator can see the full amount
received and not yet delivered/refunded.

**New trait methods** to let the supervisor observe readiness without depending on Fedimint internals:

```rust
/// Spendable balance in MSATS, or None for backends without a balance concept (e.g. MockPayment).
/// Msats (not sats) so the readiness compare is exact — see INV-2. Observability only; never
/// mints/pays. Default: None.
async fn available_balance_msat(&self) -> Result<Option<u64>> { Ok(None) }

/// Whether the backend can currently price/pay refunds. Default true for mock/internal backends.
async fn refund_gateway_ready(&self) -> Result<bool> { Ok(true) }
```

`FedimintPayment` overrides `available_balance_msat` (`get_balance_for_btc().msats`) and
`refund_gateway_ready` using the same configured gateway lookup used by refund quoting/paying.

### 3.3 INV-3 — refund provenance

Add an explicit guard in `refund.rs::process`, before quote/resolution/pay: the refund's `external_id`
MUST have recorded received-payment provenance, and the refund row's gross amount MUST match that
provenance.

Valid provenance is one of:

1. an `invoice` row with `external_id = external_id_of(refund_attempt)` and evidence that funds arrived:
   `status='PAID'` **or** `settled_at IS NOT NULL`. This includes late settlements against invoices that
   were already terminal/EXPIRED: capture stamps `settled_at` and writes a refund without changing the
   invoice back to PAID;
2. a settlement journal entry for unmatched/orphan cases (`event_log.kind` in the settle-refund family)
   whose JSON `external_id` equals the refund external id.

The guard returns `received_sat` from the provenance source (`invoice.amount_sat` or the settlement
journal's `amount_sat`). If no provenance exists, if the received amount is missing/non-positive, or if
`refund_attempt.amount_sat` is missing/different, park FAILED ("refund without matching received payment
— forbidden") and log at ERROR. Do not pay and do not silently use `0`.

The two production creation sites (`capture.rs::refund_intent`, `provision.rs::RefundDueWrite`) are the
only intended writers of `refund_attempt`, and both are strictly downstream of received-payment handling.
That fact is not the primary enforcement mechanism: the execution-time provenance guard is. Add a source
audit test that fails on production `INSERT INTO refund_attempt` statements outside those two sites
(excluding tests and schema/migrations), so future writers must either reuse a central helper or update
this spec and the provenance guard intentionally.

## 4. Acceptance criteria

- AC-1: For a fee-charging gateway (base/ppm > 0), a refund of `R` pays the largest whole-sat `net`
  satisfying `net*1000 + fee(net*1000) <= R*1000`. For `MockPayment`, `refund_net_sat(R) == R`.
- AC-2: Dust is classified by the exact whole-sat predicate. `base=0, ppm=1, R=1 sat` is payable;
  `base=999 msat, ppm=0, R=1 sat` parks FAILED as structural/manual dust.
- AC-3: Gateway quote/readiness failures leave refund rows PENDING and produce liability-aware
  readiness warnings; they are not returned as `net == 0` and not parked as dust.
- AC-4: A direct bolt11 refund pays exactly the invoice's own whole-sat amount when it is ≤ the current
  cap, and parks FAILED when it is amountless, sub-sat, or above the cap. The Fedimint amount-equality
  preflight remains valid.
- AC-5: A persisted resolved invoice is retried with the persisted/direct invoice amount, not a newly
  recomputed net. Re-resolution recomputes net only after the current generation is definite
  `Failed+expired`.
- AC-6: Boot with zero balance and **no** liabilities emits **no** readiness warning. (Today it warns.)
- AC-7: With an outstanding uncovered liability (for example, a PENDING refund whose required liquidity
  exceeds balance, a paid PROVISIONING order and the gateway is down, or a parked FAILED refund), the
  supervisor emits exactly one liability warning naming gross liability, required liquidity, balance,
  gateway state, and parked/manual count.
- AC-8: A refund whose `external_id` has no received-payment provenance, whose provenance amount is
  missing/non-positive, or whose row amount mismatches provenance is parked FAILED and never paid.
- AC-9: The gross liability recorded in `refund_attempt.amount_sat` is unchanged by fee deduction (it
  remains the received amount); logs/tests distinguish gross, net cap, and generation pay amount.

## 5. Test obligations (mandatory)

- INV-1 unit: exact whole-sat net across `{base=0,ppm=0}`, `{base>0,ppm=0}`, `{base=0,ppm>0}`,
  `{base>0,ppm>0}`, u32-max ppm/base, large `gross_sat` values that would overflow `u64` intermediate
  products, and dust. Property: `net*1000 + fee_msat(net*1000) <= gross*1000`, and `net+1`
  violates when `net < gross`.
- INV-1 rounding regression: `base=0, ppm=1, gross=1 sat` returns `1`, proving the closed-form
  msat-inverse was not used as the final sat result.
- INV-1 integration (fedimint feature, `#[ignore]`, against the live test federation): a real refund
  delivers the capped amount and the operator's balance decreases by ≤ gross; repeat with a direct
  bolt11 whose fixed amount is below the cap.
- INV-1 idempotency: after a resolved bolt11 is persisted, change the mocked gateway fee schedule and
  re-drive a PENDING/Unknown generation; the retry uses the persisted invoice amount/key and does not
  double-pay or fail the amount-equality assertion. A definite `Failed+expired` generation may re-resolve
  at the new cap.
- INV-2: supervisor tests for (a) no warning at zero balance / zero liability, (b) warning when required
  liquidity exceeds balance, (c) warning on gateway down with liabilities, (d) no false warning for old
  PAID renewal invoices on a later SUSPENDED/TERMINATED sub, and (e) parked FAILED refund rows reported
  as manual liabilities.
- INV-3: assert production `refund_attempt` writers are only the two known sites; constructed refund
  rows with no provenance, mismatched amount, NULL amount, and late-terminal invoice provenance
  (`settled_at IS NOT NULL` but `status!='PAID'`) take the required paths.
- Regression: the existing capture/provision/refund/`MockPayment` suites stay green (net == gross under
  mock means no behavioral change on the default path except the intentional provenance checks in tests).

## 6. Non-goals

- No change to the receive/invoice path (only refunds + readiness), except provenance reads and any
  minimal schema/test support needed for generation pay amount clarity.
- No operator-configurable fee/markup; the gateway's advertised schedule is the single source of truth.
- No partial-refund or downtime-credit redesign beyond applying INV-1's net to existing refund figures.
- No new ecash-funding/management CLI (separate work).

## 7. Review question resolutions

- Q1: Keep direct bolt11 pass-through for compatibility, but pay the invoice's exact whole-sat amount;
  fail amountless/sub-sat/over-cap invoices. This preserves Fedimint's amount-equality assertion.
- Q2: Count PENDING/FAILED refund rows with provenance plus paid order invoices in PENDING /
  PROVISIONING / REFUND_DUE that are not already represented by refund rows. Do not count historical
  PAID renewals; within-grace renewals are delivered atomically by extending/resuming service.
- Q3: No arbitrary safety margin. Instead, use exact widened integer arithmetic for the quote and a
  final cap preflight with the same gateway object used to start the Fedimint payment. If the final cap
  fails, do not start the payment.
- Q4: True dust (`net_sat == 0` by the exact predicate) is not auto-payable without violating INV-1. It
  parks for operator/manual handling and remains visible as a parked liability; the spec does not add an
  alternate credit mechanism.
