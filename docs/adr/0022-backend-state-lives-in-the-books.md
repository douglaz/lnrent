# 0022 — Backend correlation and pay state live in the books, not in a side file

**Status: DECIDED, NOT YET BUILT.** At the time of writing both payment backends keep their
own sqlite file beside the state DB: `phoenixd_index.db` (`daemon/src/phoenixd_backend.rs`)
and `lnv2_index.db` under the federation directory (`daemon/src/lnv2_backend.rs`). Read every
"is" below as "will" until the delivery beads close. The set is derivable, not listed:

```bash
set -o pipefail; br list --limit 0 --json -a | jq -r '.[] | select(.status!="closed")
  | select((.description // "") + (.title // "") | test("ADR-0022")) | "\(.id) \(.title)"'
```

## Context

A payment backend (the wallet adapter inside `lnrentd`: `PhoenixdPayment` talking HTTP to the
operator's phoenixd, `Lnv2Payment` driving the embedded fedimint client) has to remember two
things the wallet cannot answer for lnrent: which backend invoice belongs to which lnrent
`external_id`, and which refund key already produced which outbound payment. Both backends
write those maps into their own sqlite file, in their own transaction, separate from the
`invoice` and `refund_attempt` rows in `lnrent.sqlite` they correlate.

No ADR chose that. The first fedimint backend had its own `lnrent_index.db` because the
`PaymentBackend` trait holds no store handle and the state DB has one writer (ADR-0001); lnv2
kept the shape and scoped it per federation; phoenixd copied it. The split has a cost that is
now measured rather than hypothetical:

- **The two files can disagree.** A v1 backup restored, a hand-copied data directory, a
  mismatched restore: `lnrent.sqlite` holds an OPEN invoice and the index has no row for it.
  The daemon then fails closed forever ("index divergence", `alert_index_divergence`), and the
  runbook's only remedy is to stop the daemon and settle buyers by hand from phoenixd's records
  (`docs/go-live.md`). The same condition exists for lnv2 (`lookup_settlement_by_ref` arm (c)).
  Every one of these incidents is self-inflicted: the daemon lost its own file.
- **The double-pay window on restore** (lnrent-uxbd) exists because the pay map and the refund
  ledger it dedups can be restored from different instants.
- Two condition reasons (`index_diverged`, `lnv2_missing_correlation`) would exist only to
  report the split's failures; ADR-0023's registry omits them on the strength of this ADR.

The `invoice` row already carries `backend_invoice_id` and `payment_hash` columns, nullable and
unrelied on.

## Decision

Everything lnrent decides or remembers about a backend lives in `lnrent.sqlite`, written in
the same transaction as the row it correlates. Concretely:

- The receive map (`phoenixd_invoice`, `lnv2_invoice`) and the pay map (`phoenixd_pay`,
  `lnv2_pay`) become tables in `lnrent.sqlite`, keeping their per-backend columns. Their DDL is
  declared by the backend module and applied by the store, the way `teardown_failure` is today.
- `create_invoice` returns the correlation; the caller commits the `invoice` row AND the
  backend's receive row in one transaction (§6.6 issuance ordering). A crash between the
  backend call and that commit leaves an orphan at the wallet, never a half-written
  correlation.
- **A replacement refreshes the invoice row, in that same transaction.** When the backend
  replaces a provider-terminated invoice under an existing `external_id` (lnv2 does: its
  receive-map upsert rewrites `invoice_id`/`bolt11`/`payment_hash`/`expires_at` in place for
  a CANCELED row, `lnv2_backend.rs:1583-1598`), the `invoice` row's **`id`** (the store's
  invoice id IS the backend-derived one: `settlement_catch_up` passes it back to
  `lookup_settlement_by_ref`, which classifies a retired id as Expired, so a stale id means a
  paid replacement is never captured), `amount_sat`, `bolt11`, `backend_invoice_id`,
  `payment_hash` and `expires_at` are all updated to match, keyed by `external_id`, **and its
  `status` is reset from `EXPIRED` to `OPEN`** (never from `PAID`): capture books only an
  `OPEN` row, so a refreshed-but-EXPIRED invoice would route the replacement's payment down
  the terminal-refund arm instead of provisioning the buyer. (PR #90 CodeRabbit round 12.)
  Today the
  renewal insert is `ON CONFLICT(external_id) DO NOTHING` (`order_intake.rs:1214-1220`), so
  the books keep the retired bolt11 and hash beside a fresh correlation, and a payment of the
  replacement would be read against the old invoice's data. Same-transaction commit alone
  does not make two rows agree; the refresh is what does. This is a pre-existing hazard the
  delivery bead must pin RED first. (PR #90 codex.)
- The pre-send pay row (`lnv2_pay` PREPARED, `phoenixd_pay` PREPARED) commits in the same
  transaction as the `refund_attempt` / `sweep_attempt` transition that authorises the send.
- A backend reaches the store through the store actor, not a private connection. The trait
  grows a store handle; the sole-writer rule (ADR-0001) is kept, not bent.
- One backend per Control node (CONTEXT.md, *Payment backend*), and **switching is not
  supported**: `payment_backend` is fixed at bootstrap and an explicitly different value is a
  `config_conflict` (`config.rs`). Rows of a foreign backend are retained defensively, not
  because a switch procedure exists; `lookup_settlement_by_ref` arm (d) already treats a
  foreign id as Expired. (PR #90 codex round 33.)

## The interface (normative shape; names are the delivery bead's)

Today `PaymentBackend` has no store handle and each backend opens its own
`rusqlite::Connection` on a private file (`INDEX_SCHEMA` in both backend modules). Under
this ADR the obligations below are the whole change to the backend contract; the bullets are
the source of truth, not a count:

- **Schema, static, all of it.** The store opens before any backend exists (`config.rs`
  `bootstrap_headless_with_store` opens it; `main.rs` constructs the backend afterwards), and
  the backend holds a `Store` handle, so DDL cannot be an instance method. Each backend module
  exports a `const SCHEMA: &str`, and `Store::open` applies **every compiled backend's**
  schema after its own baseline, unconditionally. Not "the selected mode's": on a restart
  with `payment_backend` omitted, the raw config resolves to `Mock` and the persisted backend
  is inherited only after the store is open (`config.rs`, pinned by
  `inherited_phoenixd_still_requires_the_opt_in_on_every_start`), so a mode-keyed open would
  skip the inherited backend's tables. Empty tables for an unused backend cost nothing. The
  legacy side-file import (delivery bead) runs as a post-open step keyed by the *resolved*
  mode. A later change to a backend's schema is a migration in the store's existing list,
  not a backend-private `ALTER`. The backend never applies DDL itself. (PR #90 codex rounds
  7 and 8.)
- **Writes ride the caller's transaction.** `create_invoice(external_id, ...)` returns an
  `Issued { invoice, persist, lease, after_commit }`, and `prepare_pay` a `Prepared {
  persist, lease }`. `lease` is the owned guard object itself (an `OwnedMutexGuard` or
  equivalent), a separate field precisely so the caller has something to hand to
  `Store::transaction_then` as its `lease` argument; a value the closure captured would be
  gone by the time the caller could pass it. (PR #90 codex round 32.)
  - `persist: Box<dyn FnOnce(&rusqlite::Transaction) -> Result<()> + Send>` inserts (or, for
    a provider-terminated replacement, upserts) the receive-map row. The caller runs it
    inside the issuance transaction of SPEC §6.6 alongside the `invoice` insert/refresh.
    **`Issued` owns the backend's per-`external_id` create guard as `lease`; `persist`
    never does** (today's `create_lock` section,
    `lnv2_backend.rs` check→mint→insert): the guard is taken before the check and handed to
    the store **as a transaction-scoped lease**, not captured by the closure. A closure
    releases what it captures when it returns, which in `Store::transaction` is *before*
    `txn.commit`, so a captured guard would let a concurrent caller pass the check while the
    row is still invisible. `Store::transaction_then(f, lease, after_commit)` therefore takes
    the lease as its own argument and the actor drops it only after the commit succeeds (or
    after the rollback, on error). So the critical section extends through the commit, and
    a second same-`external_id` caller arriving between `create_invoice` returning and the
    commit waits, then finds the row, rather than minting a second lnv2 tweak. (PR #90 codex
    round 10; CodeRabbit round 25 for the closure-return hole.)
  - `after_commit: Box<dyn FnOnce() + Send>` runs **only after the transaction commits, and
    is run by the store actor, not by the caller**. The caller hands it to the store
    together with the transaction closure (`Store::transaction_then(f, lease, after_commit)`,
    the same three-argument contract as above); the
    actor invokes it on its own task once the commit succeeds **and only if `persist` ran
    inside that transaction**. A transaction can commit a refusal or a no-op without ever
    persisting the invoice or its correlation (the listing-withdrawn branch of
    `OrderWrite::write`, `order_intake.rs`; the lost soft-reminder CAS in
    `SoftReminderWrite::write`, `reconcile.rs`), and starting the lnv2 receive watcher for
    those would leave a terminal task working against an absent row. `persist` therefore
    records that it ran (a flag on the `Issued` value the actor reads after commit), and the
    hook is skipped otherwise; both no-op shapes are pinned. (PR #90 codex round 77.) Not by
    the caller: the actor
    commits an enqueued transaction even when the awaiting caller is cancelled
    (`store.rs` `run`, ADR-0021 shutdown), and a cancelled caller would drop a closure it
    still owned, leaving the row committed and the watcher never started until an unrelated
    restart. For lnv2 it starts the live receive watcher for the fresh operation (today
    `create_invoice` does this itself after persisting; `watch()` enumerates OPEN rows once
    at boot, so an invoice created after startup is otherwise unobserved). It must not start
    before commit: a `Claimed` observed against an uncommitted row would CAS against nothing
    and be lost. A crash between commit and the hook is covered by the boot enumeration.
    (PR #90 codex rounds 10 and 18.)
  Likewise `prepare_pay(key, bolt11, ...)` returns a `persist` for the PREPARED pay-map row,
  which the refund/sweep driver runs inside the transaction that authorises the send;
  `pay(key)` then reads that row through the store and never creates a row of its own
  before the send. **`Prepared` owns the backend's pay guard as `lease` the same way, and
  its `persist` never does** (today
  `pay_start_lock`, held across the cross-key payment-hash check and the row creation,
  `phoenixd_backend.rs`; lnv2 requires its caller to hold the equivalent): the guard is
  taken before the hash check and handed to the store as the same transaction-scoped
  lease, dropped by the actor only after the caller's commit (never captured by the
  closure, for the reason above), so a second key targeting the same bolt11 cannot pass its own check
  until the first PREPARED row is visible, and then sees the hash already owned. Two
  PREPARED rows for one payment hash are impossible by construction, never detected after
  the fact. (PR #90 codex round 19.) **No backend-owned path may create a correlation row** outside a
  caller's `Store::transaction()`, so a correlation row cannot exist without the ledger row
  it correlates, and cannot diverge from it on recovery.
- **Status transitions on existing rows are the backend's own writes.** A receive reaching
  `Claimed` / `Expired` / `Failure` must durably mark its map row `PAID` / `CANCELED` /
  `PAID_UNRECOVERED` when the terminal arrives (`lnv2_backend.rs` `spawn_receive_task`,
  `idx_mark_paid` / `idx_mark_canceled` / `idx_mark_paid_unrecovered`), and a pay reaching
  its terminal likewise; these arrive asynchronously, long after the issuance transaction,
  and the restart re-subscribe enumerates only rows still `OPEN`. The backend performs them
  through its `Store` handle in its own `Store::transaction()`, CAS on the prior status
  exactly as today, **together with whatever ADR-0023 §3 requires in that same transaction**
  (the `PAID_UNRECOVERED` transition also opens its `lnv2_paid_unrecovered` condition row;
  "one row" here means one map row, not one write). They are facts about the wallet, not
  correlations, so the creation rule above does not apply to them. (PR #90 codex round 8;
  CodeRabbit round 25.)
- **Reads go through the store.** The backend holds a cloned `Store` handle for `read()`
  (settlement lookup, pay-status lookup, the settlement poll's row set) and for the status
  transitions above. It is the same actor every other reader uses; the sole-writer rule is
  untouched.

This is the minimum: a static DDL constant per backend, a returned closure for each of the
two row creations, a store handle for reads and for the backend's own status transitions. (PR #90 CodeRabbit round 6 asked for the interface to be named before
implementation.)

## What stays outside

The wallet's own state is the wallet's: phoenixd's database and seed on the phoenixd host,
and the fedimint client's RocksDB under `fedimint/<federation_id>/client.db` (ADR-0015: the
only published fedimint client backend is RocksDB; lnrent never touches it). The daemon
therefore still runs two storage engines, with a clean line: `lnrent.sqlite` is what lnrent
knows, `client.db` is what the fedimint library knows. Backup captures both, and the phoenixd
wallet neither (`backup.rs`), unchanged.

## Consequences

- "lnrent lost its own correlation" is no longer a reachable condition. `index_diverged` and
  `lnv2_missing_correlation` are dropped from the ADR-0023 reason registry; the fail-closed
  arms that raise them stay as assertions (a row that is missing after this ADR is a bug, not an
  incident). The go-live index-divergence runbook shrinks to the phoenixd-side case.
- The restore double-pay class (uxbd) loses its "different instants" leg. The wallet-vs-ledger
  leg remains and is uxbd's.
- **Backups written under this design are manifest format v3.** Today the writer stamps v2
  iff `phoenixd_index.db` was captured, else v1 (`backup.rs`); with the side file gone, an
  unchanged writer would stamp every new phoenixd backup v1, and the refusal below would then
  reject the daemon's own backups. v3 means "self-contained: correlations are inside
  `lnrent.sqlite`"; restore accepts v3 unconditionally, v2 by importing the captured side
  file as the delivery bead describes, and v1 as follows. **The writer stamps v3 only when
  the migration marker is present in the database it snapshots.** `lnrentd backup` is an
  offline path dispatched before any daemon boot (`main.rs` `Command::Backup`; `backup.rs`
  opens the database only for `VACUUM INTO`), so an operator who upgrades the binary and
  takes a precautionary backup before the first migrated boot still has every correlation
  in the side files; a writer that stamped that snapshot v3 and stopped capturing them would
  produce a backup restore accepts as self-contained and that has lost every correlation. Absent
  the marker, the writer keeps today's conditional exactly: v2 iff `phoenixd_index.db` is
  present, else v1 (`backup.rs`); an lnv2-only operator's pre-migration backup is v1, and v1 is
  what carries `lnv2_index.db` inside `fedimint/`, so "v2" here would break its own restore
  validation. (PR #90 codex round 37.) (PR #90
  codex rounds 16 and 30.) The old
  v1/v2 distinction still gates restore: `backup.rs` accepts format-v1 backups, which carry
  no phoenixd correlation at all. Restoring one onto this design yields books that reference phoenixd
  invoices or refund keys with empty correlation tables, precisely the state this ADR
  declares unreachable and ADR-0023 no longer reports. So **`lnrentd restore` refuses a v1
  backup whose `lnrent.sqlite` references phoenixd** (exact predicate: `invoice.id LIKE
  'phoenixd-%'`, the prefix lives on the store's `id` column via `invoice_id_for`, while
  `backend_invoice_id` holds the bare payment hash, `phoenixd_backend.rs`; or any
  `SENT` refund/sweep attempt while the persisted `payment_backend` is phoenixd, since a
  v1 backup carries no phoenixd pay map to join against and refund/sweep keys are not
  backend-specific, or any attempt with a phoenixd `backend_payment_id`; the restore itself
  gives the refusal, rather than producing a directory the next boot refuses;
  **never** the persisted `payment_backend` selection alone, which a fresh bootstrap writes
  before any correlation exists), naming the reason:
  there is no safe reconstruction (the same argument the divergence runbook makes). A v1
  backup of a mock or fedimint-only deployment restores as before; lnv2's index lives under
  `fedimint/` and is captured in v1. The delivery bead pins the refusal. (PR #90 codex round
  9.)
- `phoenixd_unbookable_settlement` is replaced by the condition row (ADR-0023). Its rows are
  **carried across, not dropped**: the legacy import copies them into `lnrent.sqlite`
  verbatim, and ADR-0023's delivery converts each into a condition row (`first_observed_at =
  first_refusal_at`; `late_unbooked` if the invoice is already EXPIRED past the poll grace,
  else `fee_credit`) before dropping the table. A late-paid refusal recorded before the
  upgrade has no observer left, so dropping the timer would erase the only record of that
  receipt. (PR #90 codex round 10.)
- **The import validates coverage before it records completion.** A present, non-empty side
  file can still be the stale or mismatched one this ADR's Context describes, missing only
  some rows. So the import, inside its transaction and before writing the migration marker,
  checks every backend-referencing book row against the imported correlations and
  **refuses the whole import** if any is missing, naming the first uncorrelated row and the
  runbook. What "backend-referencing" can honestly mean per table:
  - `invoice` rows with that backend's id prefix: every one must have a receive-map row,
    **and the two must agree** on invoice id, `bolt11`, `payment_hash`, `expires_at` **and
    `amount_sat`** (both legacy receive tables carry it, and the replacement rule refreshes it;
    an amount-only divergence must trigger the same repair). They
    can already disagree today: an lnv2 replacement rewrites the side-file row in place
    while the renewal write leaves the books untouched (the same hazard the Decision's
    replacement bullet fixes going forward). On a mismatch the import **repairs the books
    from the map only when the backend's current state proves the map row is the effective
    invoice**, and otherwise refuses. Neither file is authoritative on its own: a newer
    `lnrent.sqlite` restored over an older side file (one of the mismatched-restore shapes
    this import exists for) can hold replacement B in the books while the stale map still
    names predecessor A, and "repair from the map" would rewrite B back to A so a payment of
    B no longer matches the local invoice. The newest `rowid` orders records within the file
    and says nothing about the file's freshness relative to the books. So the tiebreaker is
    the wallet, and it must **positively** establish the map row: for phoenixd,
    `incoming?externalId=` returns the map row's record **paid or still payable** (not
    expired unpaid). The book invoice's record being absent proves nothing, because phoenixd
    can lose its own history (that is `phoenixd_forgot_invoice`): in the newer-books/older-file
    restore phoenixd may hold expired predecessor A while temporarily forgetting successor B,
    and "A present, B absent" would repair the books back to A. An expired-unpaid map record,
    or a map record phoenixd cannot return, refuses. For lnv2, the map row's operation is the
    **current** receive for that key, i.e. its final state is `Claimed` (paid), or
    `Failure` (paid, unminted: the row is `PAID_UNRECOVERED`, which only a cancelled or
    expired row can ever be replaced past, so it is still the effective receive and the
    import repairs the books and backfills its condition), or it is still pending and
    payable, **not merely present**: a predecessor A necessarily existed in the
    client and reached `Expired` before `create_invoice` minted its replacement B
    (`lnv2_backend.rs`), so "A's operation exists" is true of every stale map and proves
    nothing. An `Expired` operation for the map row means the map is the stale side and the
    import refuses. Only then does the import repair (the refresh rules of the replacement
    bullet apply, including EXPIRED→OPEN), journal, and continue; if the backend cannot be
    asked, or answers for neither or both, the import refuses naming the row. (PR #90 codex
    rounds 23 and 57.) **For phoenixd the row compared is the effective one, not any agreeing
    one**: `phoenixd_invoice` has no status and its upsert conflicts on `invoice_id`
    (`idx_upsert`), so a replacement under the same `external_id` leaves the old row in place
    and `idx_get_invoice` reads the newest `rowid` as effective. An agreement check that
    accepts the old row matching the stale book row would stamp completion while the buyer
    holds the successor, whose payment would then miss the book row. The import therefore
    compares the book row against the newest-`rowid` map row per `external_id`, repairing
    the books from it under the replacement rule; the status-based replacement test below is
    lnv2's only. (PR #90 codex round 36.) The replacement shape is recognised by **identity and status, never by
    hash history**: same `external_id`, book row `EXPIRED` or `OPEN` with `settled_at` NULL,
    map row with a different invoice id whose status is `OPEN`, `PAID`, or
    `PAID_UNRECOVERED` (the current successor may already be terminal-paid, exactly the
    statuses the tiebreaker above accepts; a `CANCELED` map row is never a replacement). The retired hash is necessarily absent from
    the map, because `idx_insert` overwrites the sole row for the `external_id` including
    `payment_hash` (`lnv2_backend.rs`), so "a hash the map has never seen" would reject the
    very shape this rule exists to repair. The import **refuses** only when the book row is
    `PAID` or has `settled_at` set and disagrees with the map: money was booked against data
    the map no longer describes, and no rule can say which side is right. (PR #90 codex
    rounds 23 and 33.)
  - **every `SENT` `refund_attempt` / `sweep_attempt` row, keyed by its idempotency key**,
    plus any non-terminal row with a `backend_payment_id`: every one must have a pay-map row.
    Not "rows with a `backend_payment_id`": the recovery paths commit `SENT` with `None`
    (`refund.rs`, `sweep.rs`) and the SQL keeps an id only if one was recorded earlier, so a
    successful attempt can legitimately be `SENT` with a NULL id, and keying coverage on the
    id would skip exactly the rows whose pay-map entry is the historical owner of a payment
    hash that the `[8A]` cross-key guard reads. The pay map is keyed by the **pay key**,
    which for a refund is derived from the attempt, not stored on it: `gen_key(external_id,
    resolution_gen)` (`refund.rs`) yields the bare `refund:<external_id>` at generation 0
    and `refund:<external_id>:g<gen>` from generation 1 on, while
    `refund_attempt.idempotency_key` stays the bare ledger key. So the join derives the key
    from `external_id` and `resolution_gen`; joining on the stored `idempotency_key` would
    miss every resolved-LNURL attempt's current generation or match a stale gen-0 row.
    Sweeps have no generations and join on `sweep_attempt.id`. No backend id is needed. A
    stale file missing one of those would let a later key targeting the same bolt11 credit
    one wallet payment to two liabilities. **And the two must agree**: the attempt's
    effective bolt11 (`resolved_bolt11`, or `dest` for a bolt11 pass-through) against the
    map row's bolt11 always, and the attempt's `backend_payment_id` against the map row's
    payment id / operation id **only when the attempt has one**: a `SENT` row committed by
    recovery legitimately has NULL there while the map row carries the id, and a comparison
    against NULL would reject every such valid pair (and never be true in SQL). (PR #90
    codex round 69.) A mismatched
    restore can hold the same deterministic key in both files naming different payments, and
    an existence check would mark completion while the `[8A]` hash owner and the backend
    status reads no longer describe the SENT attempt. On disagreement the import **refuses**;
    there is no "repair from the map" for pay rows, because which of the two payments went
    out is exactly what a stale file cannot say. (PR #90 codex rounds 27 and 29.)
  - `invoice` rows whose correlation the backend's **own reaper** has legitimately removed
    are exempt: lnv2's `gc_lnv2_invoice_index` deletes `CANCELED` rows 30 days past expiry
    (definitively unpaid), while the matching `EXPIRED` book row is removed independently by
    `reap_terminal_rows`, so a data dir can legitimately hold an `lnv2-*` EXPIRED book row
    with no map row. The import inspects the map **first**: a book row is pre-reaped,
    transactionally, only when it is already eligible under the store's own retention **and**
    the map has no row for it or has it `CANCELED`. A book row the map holds as `OPEN` (a
    live lnv2 replacement whose book row is still the old EXPIRED one) is never reaped; it is
    repaired by the replacement rule above, EXPIRED→OPEN included. Reaping first would delete
    the very row the repair needs and send the buyer's payment of the replacement into
    unmatched-settlement handling. Coverage is then required for what remains. (PR #90 codex
    rounds 27 and 29.)
  - every legacy `phoenixd_unbookable_settlement` timer must resolve to a subject: the timer
    stores only the phoenixd `invoice_id` (`phoenixd_backend.rs`), while ADR-0023 keys the
    condition by `external_id`, so the import looks each timer up in the **imported receive
    map** (not the books, which may already have reaped the invoice) and **refuses** if a
    timer's invoice has no map row, naming the timer and the runbook. Otherwise the
    conversion (ADR-0023) would have nowhere to recover the subject after the marker is
    durable, and the only choices left would be dropping or mis-keying money evidence.
    (PR #90 codex round 32.)
  - non-terminal **and retryable `FAILED`** attempts **without** a `backend_payment_id` whose
    pay-map row **is present**
    must agree too (bolt11, and the operation id / payment hash the row carries): the
    ambiguity below is about a row that is *missing*, and a present row that names a
    different payment is exactly the stale-file shape that lets recovery adopt payment B for
    liability A (`pay_get` reads only operation id and status, and an existing PREPARED
    operation is adopted without comparing bolt11, `lnv2_backend.rs`). Disagreement refuses.
    (PR #90 codex round 40.)
  - `FAILED` attempts **without** a `backend_payment_id` and **without** a pay-map row take
    the same fence, because FAILED is not terminal for the operator: `refund_retry`
    (`ipc.rs`) resets such a row to `PENDING`, and `plan_payment` may then re-resolve it to a
    fresh payment hash. If the omitted map row was the witness to a wallet payment that
    succeeded after the row was marked FAILED, the normal retry workflow pays twice. So the
    import stamps `migration_unverified_at` on retryable FAILED rows exactly as on PENDING
    ones, `refund_retry` refuses a stamped row naming the fence, and only the backend's audit
    clears it. (PR #90 codex round 54.)
  - non-terminal attempts **without** a `backend_payment_id` and **without** a pay-map row:
    **not validated, on purpose.**
    The books today carry no pre-send marker: `backend_payment_id` is written only after a
    successful return and `attempts` is bumped only on failure (`refund.rs`), so a PENDING
    row with neither can be a refund whose LNURL resolution is still failing (never started,
    legitimately no pay row) **or** one whose POST went out and whose PREPARED row the stale
    file lost. The import cannot tell them apart and must not pretend to. **Until the
    lnrent-uxbd boot wallet audit ships, such an attempt is parked, not re-prepared**: the
    import stamps it `migration_unverified`, the driver refuses to run `prepare_pay` for a
    non-terminal attempt carrying that stamp (parked at the mint point, so the existing
    `RefundStuck` / `SweepStuck` machinery keeps alerting, exactly uxbd's selected mint-gate
    posture), and only the audit, an existence test against the wallet's own outgoing
    records, clears the stamp. The stamp is a **column**, `migration_unverified_at` on
    `refund_attempt` and `sweep_attempt` (SPEC §11), not a status value or `last_error` text:
    the status vocabulary is closed and `last_error` is mutable retry state, so either would
    lose the fence on an ordinary update. It is written only by the import and cleared only
    by the audit; no driver path may reset it. **The clearing audit is per backend, and only
    phoenixd has one designed**: uxbd's boot wallet audit is phoenixd-only by that bead's
    own declaration, and the lnv2 half belongs to `lnrent-lnv2-restore-fresh-hash-proof-l5kk`,
    which records that no outgoing-history audit exists for a fedimint client. l5kk is a
    *proof* bead and clears nothing itself; the lnv2 clearance is
    `lnrent-lnv2-migration-unverified-clearance-gjwy`, which depends on l5kk's outcome and
    decides each stamped attempt from the federation's own records (the deterministic
    attempt-0 operation for the stored key: present → adopt and re-await, never re-send;
    absent → clear and pay once **only on evidence that survives a client rollback**, i.e.
    the federation's or gateway's own answer, never the local `client.db` alone, which a
    restore can roll back past an operation that happened (`lnv2_backend.rs` says so on the
    recovery arm); undecidable, including "absent from the local client only" → stay parked
    and say so). Until it ships a stamped
    lnv2 attempt stays parked, `RefundStuck` firing; no phoenixd audit may clear it. (PR #90
    codex round 56.) This delivery does not depend on either bead and
    must not assume them: blessing a re-prepare on the strength of a guard that has not
    shipped would be the acknowledged double-pay with a paper fence. (PR #90 codex rounds
    43 and 51.) Once the audit exists, a cleared attempt is
    re-prepared through the leased authorising transaction exactly as a first attempt and
    `pay(key)` still only reads. (PR #90 codex round 43.)
    Data directories written under this ADR never have the ambiguity: the PREPARED row
    commits in the authorising transaction and *is* the pre-send marker.
  A file that is merely present is not proof of coverage; after this ADR the missing-row
  arms are assertions, so this is the last point at which a receive-side gap can be
  reported. (PR #90 codex rounds 17, 21 and 22.)
- **An upgrade with a side file absent is refused the same way as a v1 restore, for either
  backend.** Today `PhoenixdPayment::open` creates an empty `phoenixd_index.db` when the file
  is missing and `prepare_fedimint_paths` does the same for `lnv2_index.db`, and the
  fail-closed arms report the resulting divergence. Under this ADR there is no divergence
  condition, so the first boot on the new schema checks, per backend: **correlation-bearing
  book rows** reference that backend (exact predicate: `invoice.id LIKE 'phoenixd-%'` or
  `invoice.id LIKE 'lnv2-%'`, the prefixes `invoice_id_for` / `INVOICE_ID_PREFIX` put on the
  store's `id` column, never the `backend_invoice_id` column; or a `SENT` attempt, keyed by
  idempotency key, or any attempt with that backend's `backend_payment_id`), that backend's
  new tables
  empty, no migration marker, no side file → refuse to boot, naming the lost file and the
  runbook. The persisted `payment_backend` selection is **not** a reference: a fresh
  bootstrap persists it (`bootstrap_headless_with_store`) before the backend is even
  constructed, so keying on it would refuse every new installation as a lost index. **Before
  deciding "fresh", the absent-file branch applies the same fence as the present-file import
  to non-terminal and retryable `FAILED` attempts**: a legacy PENDING or FAILED refund or
  sweep with no `backend_payment_id`
  is not correlation-bearing (by definition its POST, if any, left no id), so it would not
  trigger the refusal, yet its PREPARED witness, if one ever existed, is gone with the file.
  Every such attempt is stamped `migration_unverified_at` and parked exactly as above; a data
  dir with any of them is not "fresh", it is "no correlation-bearing rows and N parked
  attempts", and only that backend's own audit clears them (uxbd's for phoenixd; for lnv2
  `lnrent-lnv2-migration-unverified-clearance-gjwy`, as above). **It still writes a migration marker**, with
  content `parked`: the marker's meaning is "this database is self-contained from here on",
  which is true (nothing further will ever be imported; the parked attempts are fenced by
  their stamp, not by the absence of a marker), and without it the daemon would run
  indefinitely producing v1 backups that its own restore rule then refuses once it has
  created new correlations. (PR #90 codex rounds 46 and 47.) A data dir with no
  correlation-bearing rows and no such attempts proceeds **and writes the migration marker
  anyway**, with content `fresh` in place of a side-file hash: the marker means "this database is
  self-contained from here on", and the v3 backup writer keys on it. Without it a fresh
  installation would never earn v3, its backups would stay v2/v1 with no side file to
  capture, and the v1 restore rule would then refuse the daemon's own phoenixd-referencing
  snapshots. Every first migrated boot therefore leaves exactly one marker: imported (hash),
  parked, or fresh; the v3 writer accepts any of them. (PR #90 codex rounds 10, 11, 24 and 31.)
- The cost: a backend can no longer be unit-tested with a throwaway file; it needs the
  in-memory store the rest of the daemon's tests already use.

## Considered

- **Keep the side files, add a consistency check at boot.** Detects the split after the fact
  and still leaves the operator with "settle by hand". Rejected: a check on a design that
  cannot disagree with itself is cheaper than a check on one that can.
- **One generic `backend_receive`/`backend_pay` table with a `backend` column.** The two
  backends' columns differ (`node_id`/`payment_hash` vs `operation_id`); forcing them into one
  shape adds nullable columns for no query that needs them. Per-backend tables in one file.
