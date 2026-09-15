# Review brief: question 3 of the attempt-state seam — what "committed" means

Repo: /home/master/projects/lnrent (Rust; daemon in `daemon/src`), master 7927bfb. Read-only. Do not modify files.

## Settled so far (do not relitigate; verify against the tree if you doubt it)

- Question 2 (accepted after your previous review): an **Attempt** is one concept in its STATE
  only (status PENDING/SENT/FAILED, the ADR-0022 fence `migration_unverified_at`, and whether the
  wallet has provably started paying it under the kind-supplied pay key). Its MONEY is not shared:
  a Refund's amount is a facet of its Receipt, a Sweep's is its quoted cap `max_outlay_msat`.
- CONTEXT.md § Billing now defines **Attempt**, **Fence** (orthogonal to lifecycle; a fenced
  attempt is neither paid nor retried and its money is treated as committed until a human clears
  it) and **Parked** (failed-parked | fence-parked, always with reason + remedy). Read them.
- Filed as bead lnrent-4br3 (P1 bug): the refund-readiness scan (`daemon/src/store.rs`
  `load_refund_readiness_liabilities`, `daemon/src/supervisor.rs` ~1800-1875) never reads the
  fence; a fenced PENDING refund is priced into `required_msat` AND (since ledger commit 75de826)
  subtracted from `expected_msat` — double counted — or reported "ready" while the driver refuses it.

## The three money readers today (facts from the tree)

| Reader | Question it answers | Refund counted when | Refund priced at | Sweep counted when |
|---|---|---|---|---|
| Surplus `daemon/src/sweep.rs` `read_surplus` (~73-180) | can the Operator take money out | not SENT → *reserved*; SENT → *paid out* | receipt gross (COALESCE(invoice.amount_sat, journal amount, row amount)), de-duped once per external_id with at-risk receipts | SENT, PENDING, or fenced → paid out at cap |
| Expected holdings `daemon/src/ledger.rs` `expected_msat` (~53-84) | what the wallet should hold (reconcile operand, readiness coverage operand) | SENT, or fenced, or backend `payment_status_by_key` ∈ {Succeeded, Pending}, or Unknown ∧ `payment_started_by_key` | row `amount_sat` | SENT, PENDING, or fenced at cap (`sum_sweep_caps_msat`) |
| Readiness `daemon/src/store.rs` `load_refund_readiness_liabilities` + `daemon/src/supervisor.rs` (~1800-1875, `pending_refund_required_msat`, `check_holdings_floor`) | can the daemon still pay what it owes | status <> SENT → *required*; FAILED → *parked* | net wallet credit (COALESCE(received_msat/1000, amount_sat, journal)) then `refund_required_outlay_msat` (fee-inclusive outlay) | never |

Readiness warns `InsufficientBalance` iff `expected_msat < required_msat`.

## Question 3 (under review)

"What does *committed* mean, and is it one predicate or three?"

## The recommended answer under review

Three named predicates over Attempt STATE, each defined once and consumed by every reader:

- **Committed** — the money has left the wallet or is locked out of it. Refund: SENT, or fenced,
  or the wallet has provably started paying it (the started-evidence probe under the current
  pay key). Sweep: SENT, PENDING (its PREPARED backend witness commits in the same txn as the
  PENDING row, post-ADR-0022), or fenced. `expected_msat` subtracts exactly this; surplus's
  "paid out" IS this.
- **Owed** — a Buyer is still due money. Refund: anything not SENT (PENDING, FAILED, fenced,
  started — all still owed until sent). Sweep: never. Surplus reserves Owed at receipt gross,
  once per Receipt (unchanged).
- **Required liquidity** — Owed ∧ ¬Committed: still needs wallet funds to be paid. Readiness
  compares `expected_msat` against Σ required liquidity (fee-inclusive outlay, as today). A
  started or fenced refund is Committed, so it drops out of required liquidity and the double
  count in lnrent-4br3 disappears. Fence-parked and failed-parked are reported as attention
  items (with remedy), not as liquidity.

Pricing bases (gross vs row amount vs net credit) stay per reader for now — a separate
accounting decision (bead lnrent-zhy8 / future ADR-0024).

## What to report

1. Is this the right question at this point, or does it skip a prior one?
2. Is the three-predicate answer RIGHT? Attack it with concrete scenarios and cite file:line:
   - a refund the wallet started paying that later FAILS terminally (funds return): Committed
     flips back to false — is "Committed" then a stable predicate, and who re-reads it?
   - a fenced FAILED refund: Committed (fenced) ∧ Owed (not SENT) ⇒ not required liquidity.
     After `clear-fence` it becomes Owed ∧ ¬Committed ⇒ required. Is that the right operator
     experience, or does the money "appear" as a liability only after clearance?
   - surplus's "reserved" = Owed at gross, "paid out" = Committed: for a started PENDING refund
     both hold — is the surplus then double-subtracting (reserved AND paid out)? Check
     `read_surplus` ~128-138 and say exactly what happens today.
   - a fenced PENDING sweep: Committed; does the one-in-flight slot rule (`gate_and_write`
     ~858-865) belong to any of the three predicates or is it a fourth thing?
   - the started-evidence probe is a live backend call (`ledger.rs` ~69-73) — is a predicate that
     depends on it still "over Attempt state"?
3. Are three the right number? Name any predicate a reader in the tree needs that these three
   cannot express (cite the reader), or any of the three no reader needs.
4. Verdict in one line, then findings as `[P1]/[P2]/[P3] file:line — claim`, then at most 200
   words of reasoning. Mark anything you could not verify as QUESTION.
