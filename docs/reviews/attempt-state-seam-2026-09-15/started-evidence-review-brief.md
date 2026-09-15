# Review brief: question 4 of the attempt-state seam — who may read started evidence, through what

Repo: /home/master/projects/lnrent (Rust; daemon in `daemon/src`), master 7927bfb. Read-only. Do not modify files.

## Settled so far (verify against the tree if in doubt; do not relitigate)

- Q2: an **Attempt** (a Refund or a Sweep) is ONE concept in its STATE only: lifecycle status
  (PENDING/SENT/FAILED), the ADR-0022 fence `migration_unverified_at`, and whether the wallet
  has provably started paying it under the kind-supplied pay key. Money is NOT shared: a
  Refund's amount is a facet of its Receipt, a Sweep's is its quoted cap.
- Q3: three money predicates, defined once (CONTEXT.md § Billing — read Attempt, Fence, Parked,
  Committed, Owed, Required liquidity): **Committed** = sent ∨ fenced ∨ started (re-derived per
  read, never stored; conservative exclusion, reversible when a started pay fails terminally);
  **Owed** = refund not SENT (sweeps never), each counted ONCE by the surplus; **Required
  liquidity** = owed ∧ ¬committed ∧ not parked (readiness compares this to expected holdings).
  Corrections accepted from the previous round: the surplus does NOT use Committed for refunds
  (every owed refund counts once; reserved/paid-out is a display partition), failed-parked is
  outside Required liquidity, and the "started" probe is LOCAL (reads the pay tables inside
  lnrent.sqlite), not a live wallet call.
- Filed: lnrent-4br3 (P1 bug: readiness scan never reads the fence). Recorded on beads uxbd /
  gjwy: clearing a fence is not adoption; an audit that finds the payment landed must mark SENT.

## Facts from the tree

- `daemon/src/backends.rs:270-278`: `PaymentBackend::payment_status_by_key(idempotency_key)`
  and `payment_started_by_key(idempotency_key)` (default `false`). Both are async trait methods.
  Post-ADR-0022 the phoenixd and lnv2 impls read `phoenixd_pay` / `lnv2_pay` rows in
  lnrent.sqlite through the Store actor (`daemon/src/phoenixd_backend.rs` ~2218-2230,
  `daemon/src/lnv2_backend.rs` ~1408-1422).
- `backends.rs:279-290`: a separate by-REFERENCE probe `outbound_status_by_ref(hash, bolt11)`
  asks the wallet's own history (phoenixd `outgoingbyhash`, lnv2 deterministic attempt-0 op) —
  evidence a caller needs before declaring a payment dead (lnrent-7wbo). Not a money reader.
- Callers of the two key-shaped reads (non-test): ipc.rs (6), ledger.rs (4: `expected_msat`
  ~69-73), lnv2_backend.rs (3), order_intake.rs (4), phoenixd_backend.rs (1), reconcile.rs (1),
  refund.rs (14: driver recovery / status_after_error), supervisor.rs (7: readiness
  `pending_refund_required_msat` ~1860-1870), sweep.rs (9: driver recovery).
- The surplus `daemon/src/sweep.rs:73` `read_surplus(conn: &Connection, ..)` is SYNC plain SQL,
  called inside the sweep gate's transaction (`gate_and_write` ~868, `read_surplus(tx, None)`)
  and from `Store::read`. It cannot call the async trait. It reads refund rows by status and
  sweep rows by `status IN (SENT,PENDING) OR fenced`.
- A sweep's PENDING row commits the backend's PREPARED witness in the SAME transaction
  (`gate_and_write` ~874-887, `persist(tx)`), so "sweep in flight ⇒ committed" holds by
  construction without a probe. A refund's PENDING row is born at capture with nothing sent
  (`daemon/src/capture.rs` ~322-326), so refunds DO need the probe to tell started from not.

## Question 4 (under review)

"Who is allowed to read started evidence, and through what?"

## The recommended answer under review

Only through the Attempt state reader; the surplus never needs it.

- ONE composed reader (async; takes the attempt row + its kind; returns lifecycle, fence,
  started-evidence under the kind's pay key) is the only place the two key-shaped backend reads
  are called FOR MONEY PURPOSES. Consumers: `ledger::expected_msat`, the readiness scan
  (supervisor + store CTE), and both drivers' fence/eligibility checks. Nine call-site clusters
  become one for the money predicates.
- The surplus keeps its plain-SQL shape and reads NO started evidence: each owed refund counts
  once regardless; a sweep in flight is committed by construction.
- The by-reference wallet probe (`outbound_status_by_ref`) stays a decision-time tool for the
  drivers (declaring a payment dead), OUTSIDE the seam.
- Rejected alternative: expose started evidence as a SQL join over the pay tables so any
  in-transaction reader could use it. Works post-ADR-0022, but key derivation belongs to each
  backend (`refund::gen_key`, sweep id = key) and no in-transaction reader needs the answer today.

## What to report

1. Is this the right question now, or does it skip a prior one (e.g. whether the drivers'
   recovery reads — `status_after_error`, re-await decisions — are "money purposes" at all, or a
   different consumer that must NOT be routed through a money-state reader)?
2. Is the answer RIGHT? Attack with concrete cases, citing file:line:
   - the sweep gate (`gate_and_write`) decides Busy/Insufficient/Fenced INSIDE a transaction with
     the sync surplus; the composed reader is async and outside it — is there a TOCTOU between
     "attempt state read" and "intent written" that the current in-txn design avoids?
   - readiness runs one probe per pending refund (`supervisor.rs` ~1860) and `expected_msat`
     runs another for the same rows (`ledger.rs` ~69) in the same report — two observations of a
     reversible predicate; does "one reader" fix that, or does it need one OBSERVATION shared by
     both (a snapshot passed around)?
   - an lnv2 PREPARED pay row maps to `PayStatus::Pending` before any send
     (`lnv2_backend.rs` ~484; `refund.rs` ~449-468): is "started" then over-inclusive for
     refunds, and does that matter for Committed (conservative) vs for driver eligibility?
   - the audits (uxbd/gjwy) will WRITE evidence: should they write through the same seam, or is
     the seam read-only?
   - which of the nine caller clusters are NOT money readers (order_intake, reconcile, ipc) and
     must keep calling the backend directly?
3. Suggestions: the smallest shape that satisfies the accepted definitions. Is the composed
   reader a function over (&Connection + &dyn PaymentBackend), a method on Store, a value type
   `AttemptState` built in one place, or a SQL view? What is the ONE runnable check that fails
   if a future reader bypasses it (a clippy disallowed-method list on the two backend methods,
   like the existing `clippy.toml` deny of `PaymentBackend::create_invoice`)?
4. Verdict in one line, then findings as `[P1]/[P2]/[P3] file:line — claim`, then suggestions as
   a short list, then at most 200 words of reasoning. Mark anything unverifiable QUESTION.
