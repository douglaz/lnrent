# Review brief: the "attempt-state seam" — question 2 and its recommended answer

Repo: /home/master/projects/lnrent (Rust; daemon in `daemon/src`). Read-only review. Do not modify files.

## Background (facts, verifiable in the tree at master 7927bfb)

PR #91 (ADR-0022, `docs/adr/0022-backend-state-lives-in-the-books.md`) added a durable per-row
state to `refund_attempt` and `sweep_attempt`: `migration_unverified_at` (the "fence" — a legacy
attempt whose pre-send witness may be lost, so its outcome is unknown and it MAY already have paid).
The codex review bot then ran 12 rounds, and nearly every round found ONE MORE reader of attempt
state that had not been taught about the fence:

- `daemon/src/sweep.rs` `gate_and_write` (the "FAILED -> re-attempt" branch ignored the fence)
- `daemon/src/sweep.rs` `read_surplus` (summed SENT/PENDING sweep caps only)
- `daemon/src/ledger.rs` `expected_msat` (a SECOND surplus-like sum: refund commitment keyed on
  status + backend started-evidence; sweep caps SENT/PENDING)
- `daemon/src/legacy_import.rs` pay coverage (FAILED book row over a non-FAILED map row accepted)
- `daemon/src/refund.rs` / `sweep.rs` drivers (`pending_refunds`/`pending_sweeps` select PENDING
  only, so a fenced FAILED row raises no RefundStuck/SweepStuck)
- `daemon/src/ipc.rs` `query_refunds`, `refund_retry`, `migration_clear_fence`, `money_sweep_view`
- `daemon/src/bin/lnrent.rs` renderers, `--help`, the no-`--yes` refusal
- `daemon/src/store.rs` `load_refund_readiness_liabilities` (refunds only)
- docs: go-live runbook, `docs/specs/gate1-operator-sweep.md`, `docs/specs/gate1-alerting-operability.md`

Both tables share: lifecycle PENDING / SENT / FAILED, `backend_payment_id` (the backend witness),
`migration_unverified_at`. They differ: a refund is bounded by the receipt gross
(`refund_attempt.amount_sat`, keyed to a Receipt by `external_id` / `idempotency_key` with a
resolution generation), a sweep by its quoted cap (`sweep_attempt.max_outlay_msat`, one in flight
at a time). Refund rows are driven by `refund.rs` (`Refunder::drive`), sweep rows by
`sweep.rs` (`Sweeper::execute` / `drive`).

The glossary `CONTEXT.md` (§ Billing) defines Receipt, Surplus, Refund, Sweep, Condition, Alert,
but has NO entry for an attempt, for "parked", or for the fence.

## The plan being grilled

"The attempt-state seam": one reader of "what is this refund or sweep attempt's money state"
that the sweep gate, the surplus, the ledger's expected holdings, the drivers, the readiness scan
and the operator views all consume, so a future state (like the fence) cannot be missed by one of
them. Motivation: the two upcoming beads (uxbd: phoenixd audit adopting a wallet record onto a
fenced attempt; gjwy: same for lnv2) add yet another WRITER of attempt state.

## Question 2 (the one under review)

"Is an *attempt* one domain concept with two kinds, or are a refund attempt and a sweep attempt
two different things that happen to share a shape?"

## The recommended answer under review

One concept. An **Attempt** is one intended outbound payment from the daemon's wallet: it has a
lifecycle (not started, in flight, sent, failed), it may carry a fence, and it commits money
against the books the moment it is in flight. A **Refund** and a **Sweep** are its two *kinds*,
differing only in why the money leaves and how the amount is bounded (receipt gross for a refund,
quoted cap for a sweep). Under that reading the seam is "one reader of Attempt money-state,
parameterised by kind", and the glossary gains **Attempt**, **Parked**, and **Fence**.

The alternative (two concepts) keeps the tables separate but still needs a shared money-state
vocabulary, so it buys less and leaves the two-readers-per-invariant shape in place.

## What to report

1. Is the QUESTION the right question at this point of the design, or is there a prior question
   it skips (e.g. whether "committed" means the same thing to the surplus, the expected holdings
   and the readiness scan; whether the fence is a lifecycle state or an orthogonal flag)?
2. Is the recommended answer RIGHT? Where does "one concept, two kinds" break: cite concrete code
   (file:line) where a refund and a sweep would need different answers from the same reader —
   e.g. refund commitment needs the backend's started-evidence probe (`ledger.rs`), sweeps do not;
   refunds have resolution generations and a destination that may be re-resolved; sweeps have the
   one-in-flight rule and a cap.
3. Name the invariants such a reader must answer, as a short list, and any reader in the tree
   that the brief's list above misses.
4. Verdict in one line, then findings as `[P1]/[P2]/[P3] file:line — claim` lines, then at most
   200 words of reasoning. Mark anything you could not verify as QUESTION.
