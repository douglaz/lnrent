# 0023 — A condition ledger: alert history is not condition state

**Status: DECIDED, NOT YET BUILT.** Today the only durable trace of "a human must act on this
money" is the operator DM row in `outbox`, and `lnrent money`/`status` answer "anything
unbooked?" by reading the last 12h of sent DMs back out of it
(`add_unbookable_settlement_alerts_view`, `daemon/src/ipc.rs`). Read every "is" below as
"will" until the delivery beads close:

```bash
set -o pipefail; br list --limit 0 --json -a | jq -r '.[] | select(.status!="closed")
  | select((.description // "") + (.title // "") | test("ADR-0023")) | "\(.id) \(.title)"'
```

Reviewed by a codex (gpt-6-astra) + Opus panel on 2026-09-11 against the candidate model; the
panel's corrections are folded in and named where they changed the design. Glossary terms are
in CONTEXT.md: **Condition** (durable open situation, open → resolved) and **Alert** (one
delivery about a Condition, never its record).

## Context

The daemon detects several situations on the money path that it cannot resolve by itself and
that a human may have to act on. Today each is edge-triggered into a DM with an in-memory 6h
cooldown, and the operator surface reconstructs "state" from delivery history. A set of
beads, each closed as subsumed into this ADR's delivery bead, were the same defect seen from
different sides: the view prints READY once the DMs age out; downtime across the poll window
loses the alert forever; a restart re-alerts for a receipt booked days ago because the
phoenixd poll cannot see the books; several unbookable shapes only log. The set is
derivable, not listed:

```bash
set -o pipefail; br list --limit 0 --json -a | jq -r '.[]
  | select((.close_reason // "") | test("lnrent-adr0023-condition-ledger-wwlf")) | "\(.id) \(.title)"'
```

The one durable marker, a two-column timer in the phoenixd index, was in the file whose loss
was one of the incidents.

The repo already has the right shape one table over: `teardown_failure` (`store.rs`), a row per
open obligation with `first_failed_at`, `last_attempt_at`, `resolved_at NULL = open`, a
resolver, an open-scan, and a LIVE count in `lnrent status`. So `status` prints a live count
for teardowns and a 12-hour history for money in the same JSON object.

## Decision

**A `condition` table in `lnrent.sqlite`, one row per resolvable unit, from which both the
alerts and the operator view derive.** Reporting never writes on the money path; a gate may
read it.

### 1. Shape

```sql
-- Every INTEGER timestamp in both tables, and every `now` in this ADR, is Unix epoch SECONDS
-- read from the daemon's injected `Clock` (`clock.rs`), the unit every other daemon
-- timestamp uses; the 15-minute alert-after and the 6-hour cooldown are compared in it.
CREATE TABLE condition (
  reason            TEXT NOT NULL,   -- closed registry, §2
  subject           TEXT NOT NULL,   -- the resolvable unit: invoice.external_id, or the wallet id
  backend_ref       TEXT,            -- the backend's own id the DM quotes; informational
  first_observed_at INTEGER NOT NULL,
  last_observed_at  INTEGER NOT NULL,
  observations      INTEGER NOT NULL DEFAULT 1,
  last_evidence     TEXT NOT NULL,   -- what the latest sighting rests on: the CURRENT backend invoice
                                     --   id for receive-side reasons; the attempt key for inv1_overrun (§3)
  resolved_at       INTEGER,         -- NULL = open
  resolved_by       TEXT,            -- daemon | operator
  resolution_note   TEXT,            -- operator-supplied on manual clearance
  closed_out_at     INTEGER,         -- the close-out marker (§6): written only by `condition closeout`
  closed_out_outcome TEXT,           --   served | refunded | unpaid
  provider_terminal_at INTEGER,      -- phoenixd_forgot_invoice only: set by the poll when the provider's record reaches
                                     --   a terminal outcome (booked or isExpired); until set, the poll keeps observing (§3)
  last_detail       TEXT,            -- capped human text from the latest sighting
  PRIMARY KEY (reason, subject));
CREATE INDEX condition_open_idx ON condition(resolved_at);

CREATE TABLE alert_group (           -- alert TIMING, one row per thing that gets a DM (§5)
  reason            TEXT NOT NULL,
  group_key         TEXT NOT NULL,   -- the backend kind ('phoenixd'|'lnv2') for a per-reason reason;
                                     --   the subject for a per-row one (§5)
  generation        INTEGER NOT NULL DEFAULT 1,  -- +1 each time the group goes from zero open rows to one
  last_alerted_at   INTEGER,         -- last DM ENQUEUED for this group (not delivered); the persisted cooldown
  PRIMARY KEY (reason, group_key));
```

Condition rows carry no alert timing. Alert timing is a property of the thing that gets a DM
(a wallet-wide group, or one subject), not of a receipt, and it must survive the receipts
turning over. Four review rounds on PR #90 tried to derive it from the open rows (newest
stamp, inherit on insert, inherit on re-key) and each closed one leak and opened another:
a late arrival with no stamp; the stamp vanishing with the first row to resolve; a group
that resolves and reopens in one second colliding on its outbox id. The panel's codex
reviewer proposed this table at the outset; it was declined as a second table and is now
adopted, because a derived value that needs four patches is the wrong primitive.

- **Subject is lnrent's identity, never the backend's.** lnv2 rewrites its invoice id in
  place when it replaces a cancelled invoice; `invoice.external_id` is `NOT NULL UNIQUE` and
  both producers already hold it. (Panel: both reviewers, independently.)
- **Per-unit rows, not per-incident.** The refusal is judged wallet-wide but resolution
  happens per receipt: fund by 3,000 sat and receipt A books while B stays refused. A single
  wallet row cannot know when the last receipt clears. Grouping is a read-time property of the
  reason (§2), so no parent table and no aggregate to keep in step.
- **Payment certainty is not a column.** It is a pure function of the reason in every case
  in the registry. The view derives a subject's certainty across all of that subject's rows,
  so a `paid_confirmed` row is never erased by a later `state_unknown` one on the same subject.
  (Panel: Opus wanted it in the registry, codex wanted it preserved as evidence; both hold.)

### 2. The reason registry (normative; a reason not listed here does not exist)

One reason = one remedy = one resolution-authority **set** (a reason may name `daemon on
booking`, `operator`, or both; the set is the registry attribute, modelled as a set, not an
enum). Where two rows of one reason would need different remedies, that is two reasons.

| reason | backend | subject | certainty | alert grouping | resolution | alert after | delivery kind | remedy (normative gist; the DM and `money` print this text) |
|---|---|---|---|---|---|---|---|---|
| `fee_credit` | phoenixd | external_id | paid_confirmed | per reason | daemon, on booking | 15 min | `settlement_unbookable` | Give phoenixd spendable balance: the DM names the shortfall for the example receipt; lnrent books held-back receipts automatically once spendable covers them. |
| `late_unbooked` | phoenixd | external_id | paid_confirmed | per reason | operator | none | `settlement_unbookable` | lnrent can no longer book this receipt (paid after local expiry, poll window passed): settle the buyer from phoenixd's own records, then run `condition resolve`; the daemon may keep running, because by this reason's own definition nothing can rebook the receipt (the invoice is EXPIRED past the poll grace, the poll has retired it, catch-up scans only OPEN invoices), so clearance cannot be undercut by a later capture. The accounting gap stays visible in `conditions_cleared_books_open` until the close-out ships. |
| `getbalance_outage` | phoenixd | external_id | paid_confirmed | per reason | daemon, on booking | 15 min | `settlement_unbookable` | phoenixd's balance endpoint is failing while payments are observable: check the node and its API; funding does nothing until the read works. |
| `phoenixd_forgot_invoice` | phoenixd | external_id | state_unknown | per reason | daemon **when the provider's record for the invoice reappears in any state** (booking; live and still payable; provider-terminal unpaid; or paid but blocked by a different reason), or operator | none | `settlement_unbookable` | phoenixd no longer knows this invoice (its own history was lost): restore phoenixd's data (the receipt then books, or phoenixd reports it expired unpaid, and the condition clears itself either way), or **stop the daemon**, settle from whatever records remain, and **keep it stopped** until the accounting close-out ships (an offline command, §7): the invoice stays OPEN and a daemon that later regains phoenixd's history would book the original receipt on top of your manual settlement. Do not restart merely to clear the row; boot catch-up runs before you could reach it. Nothing in lnrent's backup repairs it. |
| `lnv2_paid_unrecovered` | lnv2 | external_id | paid_confirmed | per row | none automatic; no running-daemon clearance: the offline close-out only | none | `settlement_unbookable` | Lightning payment confirmed, ecash minting failed, and lnrent will never capture this receipt (`PAID_UNRECOVERED` is terminal; nothing re-observes it): stop the daemon, recover the funds through federation/wallet recovery, settle the buyer out of band, and keep the daemon stopped until the accounting close-out ships (an offline command; the order stays pending meanwhile). Do not fund or expire, and do not restart merely to clear the row. `condition resolve` refuses this reason: the receipt is confirmed paid and terminal, so there is no reporting-only clearance that is not a lie about the books. |
| `phoenixd_unusable_record` | phoenixd | external_id | paid_confirmed | per reason | daemon, on booking (no running-daemon clearance: the offline close-out only) | none | `settlement_unbookable` | phoenixd reports this invoice PAID but its record carries no usable `completedAt`, so lnrent cannot stamp a settlement time and will not book it (today the poll logs and skips, `poll_settlements_once`; catch-up errors): check the phoenixd release against the verified one; if the record stays unusable, stop the daemon and settle the buyer from phoenixd's records, then wait for the offline close-out. `condition resolve` refuses this reason: the receipt stays bookable, so a corrected record after a reporting-only clearance would book it on top of your settlement. |
| `inv1_overrun` | phoenixd | wallet node id | n/a (send side) | per row | operator | none | `refund_overrun` (new `AlertKind`) | A refund cost more than its receipt: automated refunds are stopped; verify the running phoenixd release and configure `[phoenixd] fee_schedule_*` for it, then `condition resolve`. |

The remedy column is normative in substance, not wording: the DM and the operator view carry
one string per reason from this registry, so the two surfaces cannot drift. (PR #90 codex
round 33.)

*Delivery kind* is the `AlertKind` the DM carries. It is a registry attribute so that §5's
split (`AlertKind::ALL` minus the kinds named here) is computed, not hand-maintained, and so
no kind can be delivered both from the ledger and from the in-memory dispatcher. (PR #90
codex round 29.)

Reachability, so the spec is honest about what is an expected operation and what is a guard:

- `fee_credit` is the normal state of an unfunded phoenixd operator's first small sale
  (measured: 25,000 sat in, 2,723 spendable). It is the reason this ledger exists.
- `late_unbooked` is a `fee_credit`, `getbalance_outage` **or** `phoenixd_unusable_record` row whose invoice is past
  `expires_at` plus the poll's grace: the poll was the only observer of a late payment and
  has retired the row (`poll_settlements_once`), catch-up scans only locally OPEN invoices,
  so neither funding nor the endpoint recovering can book it any more. The resolve scan (§4)
  re-keys the row to this reason, preserving `first_observed_at`. One late reason for both,
  because past that point the remedy is the same regardless of what blocked booking:
  settle the buyer from phoenixd's own records. (PR #90 codex round 5 found the
  `getbalance_outage` half missing.)
- `getbalance_outage` is a transient HTTP failure on the phoenixd host. Common, self-healing.
- `phoenixd_forgot_invoice` is phoenixd losing its own history: proven by the kr1 seed-only
  rebuild drill. lnrent's backup cannot repair it; the remedy is on phoenixd's side. It has
  **two** resolution authorities: the daemon, on booking, because the local invoice stays
  OPEN (the failed lookup blocks expiry) and if phoenixd regains its history, catch-up books
  it or writes its late-refund intent and the condition is then false, **and also on
  proven-unpaid expiry**, where "proven" means **provider-terminal evidence**: phoenixd's
  record for the invoice is present again and carries `isExpired` (unpaid, no longer
  payable). Not the local clock: `lookup_settlement` reports `Expired` merely because
  `now >= expires_at` (`phoenixd_backend.rs`), while the poll deliberately keeps such an
  invoice under observation for `SETTLEMENT_POLL_GRACE_SECS` past that because phoenixd may
  still accept payment; a locally EXPIRED invoice that phoenixd then forgot could have
  settled unseen, so local expiry plus a successful lookup proves nothing. Reconcile's normal
  expiry (`order_invoice_may_expire` returning true) may follow. **The resolver for this
  branch is the phoenixd settlement poll, not the scan**: the poll is the only component
  that sees phoenixd's record with `isExpired` (`PaymentBackend` exposes only
  `PaymentStatus`, which maps local-clock expiry and provider expiry to the same value, and
  the poll's provider-expired observation is otherwise an in-memory retirement), so on
  observing a present, unpaid, `isExpired` record for a subject with an open
  `phoenixd_forgot_invoice` row it resolves that row through its store handle
  (`resolved_by='daemon'`, detail `provider-expired unpaid`) in the same way it opens one.
  The same holds when the record reappears **live and still payable** (`isPaid=false`,
  `isExpired=false`): phoenixd has demonstrated it knows the invoice again, the condition it
  reported is false, and the poll resolves the row on that sighting (detail `provider record
  returned, payable`) with nothing else to open. "Back under normal observation" is only true
  inside the poll grace window; a record that returns live **after** `expires_at +
  SETTLEMENT_POLL_GRACE_SECS` is excluded by `idx_pollable_invoices` on age, and resolving
  the row would drop the one durable reason the poll still looks at it, so a buyer paying
  the provider-confirmed-payable invoice afterwards would settle unobserved. The resolved
  row therefore **keeps the polling obligation**: the poll's row set (below) also includes
  the subject of every `phoenixd_forgot_invoice` row, open or resolved by daemon or operator,
  until the poll observes booking or a provider-terminal record, at which point it stamps the
  row `provider_terminal_at` and the obligation ends. The condition may be false or cleared;
  the obligation is not. (PR #90 codex rounds 78 and 79.) And the same holds when the record reappears **paid** but booking is
  then blocked by fee credit, a balance-read failure, or an unusable `completedAt`: the poll cannot emit the
  settlement and opens the actual `fee_credit` / `getbalance_outage` /
  `phoenixd_unusable_record` row, and **in that same transaction resolves the
  `phoenixd_forgot_invoice` row** (detail `provider record returned`), because the fact it
  reported, that phoenixd does not know the invoice, is no longer true, and leaving it open
  would keep telling the operator to restore history that has returned. The condition that
  is now true is the one that stays open. (PR #90 codex round 58.)
  The scan never resolves this reason on a status read. For the poll to be that resolver it
  must still be looking: today `idx_pollable_invoices` selects only rows with `expires_at`
  inside the grace window, so an invoice phoenixd restores after that window would never be
  re-observed and the condition would stay open despite the remedy's promise. **The poll's
  row set therefore also includes, for **every `phoenixd_forgot_invoice` row that has not yet
  reached a provider-terminal outcome, whether open, daemon-resolved, or operator-cleared**
  (an operator who verified the invoice unpaid has not made phoenixd forget it any less, and
  a record restored later as live or paid must still be observed), the one
  invoice the condition's `last_evidence` names** (`NOT NULL`, and for receive-side reasons
  exactly the current backend invoice id, §3; `backend_ref` is nullable and informational
  and must not be the key) (the current book invoice for that
  `external_id`, not every historical map row sharing the subject: phoenixd retains a
  replaced predecessor beside its successor, and a restart clears the in-memory `retired`
  set, so polling by `external_id` alone could observe the expired predecessor's `isExpired`
  and resolve a condition that was opened for a possibly-paid successor), regardless of
  `expires_at`, until it observes booking or the provider-terminal record **for that
  invoice**; the set is bounded by open conditions, which are rare and operator-visible, not
  by the invoice table. (PR #90 codex rounds 42, 43, 45 and 49.) And the operator, for the case where phoenixd's history never returns. Without the daemon half the scan would re-alert
  a booked receipt forever and withhold READY until a redundant manual clearance. (PR #90
  codex round 23.)
- `lnv2_paid_unrecovered` is upstream fedimint reaching `Failure` after Lightning confirmation
  while minting ecash. `idx_mark_paid` deliberately never flips it, so nothing to auto-resolve,
  and no running-daemon clearance either: the buyer is owed until the offline close-out records
  how they were settled. (PR #90 codex round 68.)
- `phoenixd_unusable_record` is a paid record whose `completedAt` is absent, zero, or
  unparseable. Today the poll warns and `continue`s and catch-up returns a generic error,
  so the receipt stays unbooked with no condition and `status` prints READY over a confirmed
  payment. Reachability: unmeasured; it needs a phoenixd release that emits a malformed
  record, which is why the remedy starts with the version check. It auto-resolves if a later
  observation carries a usable time and the receipt books. It is **not** operator-clearable
  while the daemon runs: unlike `late_unbooked` or `lnv2_paid_unrecovered`, whose receipts
  nothing can rebook, a corrected provider record carries the same invoice evidence, would
  leave a reporting-only clearance in place, and would then be booked on top of a manual
  settlement. Its only non-daemon exit is the offline close-out with the daemon stopped.
  (PR #90 codex rounds 60 and 61.)
- `inv1_overrun` is send-side (lnrent-7fx): a refund cost more than the receipt. Its subject
  is the WALLET, not the attempt, because the cause is a fee schedule that differs from the
  configured one and clearing one attempt must not lift the gate. (Panel: Opus.)
- Dropped by ADR-0022: `index_diverged`, `lnv2_missing_correlation`. lnrent losing its own
  correlation is no longer reachable.

"Booking" for the daemon-resolved reasons means the accounting write the refusal was blocking:
`invoice.settled_at IS NOT NULL` **or** a `refund_attempt` exists for the subject. Not
`status='PAID'`: a late settlement keeps the invoice EXPIRED and writes a refund intent, and
that arm stamps `settled_at` but never `applied_at` (`capture.rs`). (Panel: codex.)

**A reason auto-resolves only when a money-path write makes it false and the resolver observes
that write, with one named exception: an authoritative return of the provider's own record
that the registry names.** A producer that stopped observing is silence, not repair, and a
backend *probe* is never a resolver; but a producer that *observes the provider's own
statement* about the subject holds a fact as strong as a booking. Today that covers
`phoenixd_forgot_invoice` only, and any return of its record counts: reappears live and
still payable (resolved, nothing else to open); reappears unpaid with `isExpired`
(provider-terminal); or reappears paid but blocked, in which case the blocker is opened and
the forgotten row resolved together (§2, registry). Those are the only non-booking automatic
resolutions in the registry. (Panel: Opus; PR #90 codex rounds 44, 60 and 76.)

### 3. Observation

**Who produces.** Whoever holds the fact, at the instant it holds it, through a store handle;
after ADR-0022 every backend has one. Concretely: the lnv2 receive watcher opens
`lnv2_paid_unrecovered` **in the same transaction** as its `PAID_UNRECOVERED` map-row
transition (`spawn_receive_task`), and the phoenixd settlement poll opens
`phoenixd_forgot_invoice` from its missing-record arm on the sighting itself, before the
poll's last look, **only when the missing record is the current book invoice for that
`external_id`** (phoenixd keeps a replaced predecessor beside its successor and the poll
enumerates every retained map row; a missing predecessor is not a condition, and letting it
observe would overwrite the condition's `backend_ref` with the wrong invoice and let the
predecessor's later provider-expired record resolve a condition the successor still
warrants), **and `getbalance_outage` and `fee_credit` from its credit-seam error
arm**: today that arm logs every `spendable_credit_msat` error and reports only a typed
`FeeCreditRefusal` (`poll_settlements_once`), so a balance-read failure on a paid invoice
whose local row is already EXPIRED, where the poll is the only observer left, would never
open a condition. The balance error becomes a typed observation there, downcast like the
refusal is, never matched on text. Neither may be left to a supervisor or reconcile caller: those scan only
locally OPEN invoices, and both facts can first arrive after the local invoice has already
EXPIRED, when the watcher or the poll is the last observer there is. Supervisor-side callers
remain producers for the facts they hold (the catch-up's fail-closed errors, typed not
string-matched). (PR #90 codex round 13.)

Producers call `observe(reason, subject, backend_ref, evidence, detail)`. `evidence` names
what the sighting rests on: for receive-side reasons it is the **current backend invoice id**
for the subject (the same value as `backend_ref` at observation time), not the `external_id`,
because a receive invoice can be **replaced** under the same `external_id` (a provider-expired
predecessor gets a successor, ADR-0022) and the successor is a new fact: an operator who
cleared the predecessor as unpaid must be re-alerted if the successor goes missing or fails,
which the §3 evidence rule below does only if the evidence differs. One receipt, one invoice,
one fact. For `inv1_overrun` it is the overrunning attempt's idempotency key. (PR #90 codex
round 53.) It runs in one store transaction and:

1. Refuses to open a row for a subject that is already booked (§2 predicate). This closes
   peri at the write: a restarted poll re-evaluating a receipt booked days ago writes nothing.
   (Panel: codex — a later scan cannot un-send a DM the recorder already enqueued.)
2. Reads the existing row, if any, and **decides before writing anything**, against the
   row's stored values (PR #90 codex round 2: an upsert that stamps `last_evidence` first
   makes the comparison below always equal):
   - no row: insert it open, `first_observed_at = now`, `observations = 1`.
   - row open: bump `last_observed_at`, `observations`, `last_detail`, `last_evidence`.
   - row resolved by the **daemon**: a fresh sighting after an automatic resolution is a
     **new episode**. The prior episode (`first_observed_at`, `observations`, `resolved_at`,
     `resolved_by`) is written to `event_log`, then the row is reset: `first_observed_at =
     now`, `observations = 1`, `resolved_at`/`resolved_by`/`resolution_note` cleared.
     Nothing carries over, so the old episode cannot lend the new one the wrong age.
     (PR #90 CodeRabbit.)
   - row resolved by the **operator** and the observation's `evidence` **equals** the stored
     `last_evidence`: record the sighting only (`observations`, `last_observed_at`,
     `last_detail`) and leave it resolved, so an operator who cleared a
     `phoenixd_forgot_invoice` "settled by hand" is not re-nagged every catch-up tick for a
     fact that never changes.
   - row resolved by the **operator** and the evidence **differs**: a new episode exactly as
     for a daemon-resolved row, and `last_evidence` takes the new value. For `inv1_overrun`
     that means every later overrun on the same wallet re-opens the latch and re-alerts; the
     operator's clearance covered the attempt they saw, not the wallet forever. (PR #90 codex.)
   - **every** branch writes the observation's `backend_ref`: lnv2 replaces a cancelled
     invoice's backend id under the same `external_id`, and the view must show the current
     one. (PR #90 CodeRabbit round 6.)
   - whenever a branch above **opens** a row (insert, or either new-episode branch) and the
     reason's group had **no other open row**, the group's `alert_group.generation` is
     incremented **and its `last_alerted_at` set to NULL** (row created at generation 1 if
     absent). That is the only writer of `generation`; it marks "this group went quiet and
     came back", which is a new incident: it needs a fresh alert id (§5) and it must not
     inherit the previous incident's cooldown, or a later `inv1_overrun` inside six hours of
     the cleared one would be silenced. (PR #90 codex round 7.)
3. Runs regardless of whether alert delivery is enabled. The ledger is state, not delivery;
   today's dispatcher returns without writing when disabled, and that must not carry over.

Observation goes through `Store::transaction()` like every write, so the y4m.3 degraded latch
refuses it. That is correct: a store with a fatal disk error must not take a money-adjacent
write around the guard. The view reports it (§6).

### 4. Resolution and re-alerting: one scan in the supervisor

The supervisor holds the store and the backend; the producers do not. One scan, at boot
**immediately before the outbox drain** (after settlement catch-up and reconcile, or it alerts
about things the same boot is about to clear) and on each maintenance tick:

- resolves every open daemon-resolvable row whose §2 predicate now holds
  (`resolved_by='daemon'`);
- re-keys `fee_credit`, `getbalance_outage` and `phoenixd_unusable_record` → `late_unbooked` when the invoice is locally
  **`EXPIRED`** *and* past the poll's last look. Both conditions, because an invoice paid
  before local expiry stays `OPEN` (reconcile will not expire a backend-Paid invoice) and
  catch-up re-observes every OPEN invoice each tick, so funding the wallet or the endpoint
  recovering still books it; only the poll-only late payment on an already-EXPIRED invoice
  has no observer left. Re-keying an OPEN one would turn a recoverable receipt into a
  needless manual settlement. (PR #90 codex round 9.) If **more than one** source row exists
  for one subject (any of `fee_credit`, `getbalance_outage`, `phoenixd_unusable_record` can
  coexist, since none resolves until booking: an unusable record, then a valid one while
  `getbalance` fails, then a fee-credit refusal is a real sequence) they **reduce
  deterministically to one**: the row with the oldest `first_observed_at` becomes the
  `late_unbooked` row with `observations` summed over all sources and the newest
  `last_detail`; every other source row is closed `resolved_by='daemon'`, note `merged into
  late_unbooked`, and journaled, in the same transaction. One alertable late row, no
  primary-key collision, no obsolete blocker left open, no rolled-back scan; the three-row
  case is pinned. (PR #90 codex rounds 6 and 66.)
  The re-keyed row opens the `late_unbooked` group like any insert (§3 last bullet) and is
  alerted under that group's timing (§5): at once if the group is quiet, otherwise it rides
  the group's next re-alert, which names the open count. It is not exempt from the group
  cooldown; an earlier draft said "alerts at once" and that contradicted §5. (PR #90 codex
  round 6.)
- alerts, level-triggered, every group that is **due** (§5): never alerted and holding an
  open row older than its reason's *alert after*, or last alerted longer ago than the
  cooldown. The first case is what makes a single observation enough: `observe()` cannot
  satisfy a 15-minute *alert after* inside its own transaction, and a producer that reports
  once and stops must still produce the first DM. This is also what fixes bdkh: downtime can
  delay a condition's alert, never lose it.

### 5. Alerting from the ledger

- **Alert after** is a registry attribute per reason (the 15-minute wait moves from the
  deleted phoenixd timer table to the registry); *none* means the first open row makes the
  group due immediately.
- **Cooldown** is `ALERT_COOLDOWN_S` (6h today, `alerts.rs`), the same constant the
  non-ledger alert kinds use; the registry may override it per reason but no reason does.
  One repeat-alert policy for every alert the daemon sends. (PR #90 CodeRabbit.)
- **Timing lives in `alert_group`** (§1), keyed by `(reason, group_key)`: the **backend
  kind** (`phoenixd` / `lnv2`, a registry attribute of the reason) for a *per reason* grouped
  reason, the subject for a *per row* one. Not a wallet node id: `observe()` holds an
  external_id, a backend ref and evidence, none of which names the wallet, and asking the
  node for it is a probe that can itself fail (the `getbalance_outage` case). One backend
  per Control node (CONTEXT.md) makes the kind a stable key; a backend switch leaves the old
  kind's rows in place, which is the intended history. (PR #90 codex round 9.) A group is **due** when it
  has at least one open row and either `last_alerted_at IS NULL` with an open row older
  than *alert after*, or `last_alerted_at` older than the cooldown. Rows arriving late and
  rows resolving early change nothing about the timing; the group emptying and refilling
  resets it (§3), because that is a new incident. (PR #90 CodeRabbit round 6; panel codex;
  codex round 7 for the reset.)
- **One transaction per alert.** The `outbox` insert and the `alert_group.last_alerted_at`
  stamp commit in the same `Store::transaction()`, so a crash leaves either both or neither:
  never a DM with no stamp (a duplicate on the next scan) and never a stamp with no DM
  (silence for a whole cooldown). A rolled-back alert is simply due again on the next scan.
  The outbox id is `(reason, group_key, generation, stamp time)`. A retry of the same firing
  collides on `ON CONFLICT DO NOTHING` rather than enqueueing twice. Two *legitimate*
  firings cannot collide: within one generation the stamp time separates cooldown re-fires
  (the check and the stamp are one transaction, so never two in one second); across a
  quiet-then-refilled group the generation separates them even in the same second. Cooldown
  re-fires are the same episode of the same condition; nothing on the condition row changes
  when a group re-alerts. (PR #90 codex round 6.) This replaces today's stamp-after-commit in
  memory (`alerts.rs`).
- The DM for a grouped reason names one subject as the example and the open count, as today.
- `last_alerted_at` means enqueued into the outbox, not delivered. Delivery retries stay in
  the outbox.
- **First alert from `observe()`**: when the observation opens a row whose group is due at
  that instant (*alert after: none*, or the row is a re-open of something already older than
  it), `observe()` fires it in its own transaction under the same rules. Otherwise the scan
  does.
- The in-memory `(kind, subject)` cooldown map remains for every alert kind that has no
  ledger reason. The split is **derived, not listed**: `AlertKind::ALL` (`alerts.rs`) minus
  the kinds the condition registry (§2) names as its delivery kinds. Today that subtraction
  moves `SettlementUnbookable` and the new send-side kind; the implementation computes it
  from the two sources rather than carrying a second enumeration. (PR #90 codex round 28.)

### 6. The operator surface

`lnrent money` and `lnrent status` read the ledger, not the outbox. The alert-history view
(`recent_alerts`, `ALERT_VIEW_WINDOW_S`, the GLOB scan over outbox ids) is deleted. Keys:

- `conditions_open`: count of open rows.
- `conditions`: open rows grouped by reason: reason, count, oldest `first_observed_at`,
  certainty, remedy, and each row's subject and `backend_ref`.
- **The GC reaper must not remove a condition's books.** Today `reap_terminal_rows`
  (`store.rs`, lnrent-y4m.2) deletes an EXPIRED, never-settled invoice past retention, and
  can then reap its terminal subscription; a `late_unbooked` subject is exactly such an
  invoice. Reaping it strips the condition of the invoice kind and subscription state this
  view reports, leaves the close-out command nothing to mark, and would reap the marker
  itself. Rule: an invoice whose `external_id` is the subject of **any** `condition` row,
  resolved or not, is excluded from terminal reaping, and so is its subscription. Condition
  subjects are rare and the exclusion is permanent by design; a condition row is money
  evidence, not free-flood traffic. (PR #90 codex round 17.)
- `conditions_cleared_books_open`: rows of **receipt-backed reasons** (subject kind
  `external_id` in the registry; `inv1_overrun`'s wallet subject has no booking predicate and
  is excluded) that the operator cleared (`resolved_by='operator'`) and whose subject is still
  **not booked** (§2 predicate false) **and carries no close-out marker** (below). That
  pair is the only retention test: not "order still PENDING". `closeout --outcome unpaid`
  leaves a subject unbooked but marked, so it leaves the list. (PR #90 codex rounds 7 and
  14.) A subject can be a **renewal** invoice on an ACTIVE
  subscription (`settlement_catch_up` scans `order` and `renewal` alike, `supervisor.rs`),
  which holds no reservation, and the subscription can walk to terminal by ordinary
  lifecycle while the invoice stays unbooked and still capturable or refundable. They are
  not open conditions, so they are not in `conditions`, and they must not vanish either.
  Each carries reason, subject, the invoice `kind`, the subscription's state, `resolved_at`
  and `resolution_note`. A close-out's `--outcome` is validated against the reason's
  certainty: `unpaid` is accepted only for a `state_unknown` reason
  (`phoenixd_forgot_invoice`), never for a `paid_confirmed` one (`late_unbooked`,
  `lnv2_paid_unrecovered`), where accepting it would drop a receipt the wallet is known to
  have settled from this list and let READY return without the buyer being served or
  refunded. (PR #90 codex round 56.) The list empties when the subject books, **or carries a close-out
  marker**: the close-out command (`lnrent-condition-closeout`, which must handle renewals
  as well as orders) must leave a durable marker that this retention test reads, set for
  every outcome including `unpaid`, because a terminal subscription state alone never makes
  the §2 booking predicate true and the row would otherwise be listed forever after a
  successful close-out. **The marker lives on the condition row** (`closed_out_at`,
  `closed_out_outcome`), not on the invoice: a legacy timer converted at upgrade can belong
  to an invoice the reaper already deleted before this ADR (today `reap_terminal_rows`
  removes an EXPIRED unsettled invoice after 30 days and nothing reaps the timer), and such
  a row must still be closeable. For those rows the view prints the invoice kind and
  subscription state as `unknown (books reaped before upgrade)` from `last_detail`, which
  the conversion fills in. (PR #90 codex rounds 8 and 19; CodeRabbit round 6 for renewals.)
- `conditions_unknown: true` when the read fails. Never a false zero.
- `condition_recording_unavailable: true` when the store is degraded (today's
  `alerts_recording_unavailable`, renamed): a zero proves nothing while writes are refused.
- `condition_reasons_observed`: the reasons this backend can raise, replacing the per-backend
  boolean `reports_unbookable_settlements()`. "Zero open" and "this backend cannot observe
  this" are different answers.

`Status: READY` is printed only when `conditions_open` is 0 and none of the two unknown flags
is set. A non-empty `conditions_cleared_books_open` does not withhold READY (the operator has
dealt with the money) but is printed under it, every time, until the subject books or carries
the close-out marker (§6): closing the order or subscription alone does not remove it.

### 7. Manual clearance

`lnrent condition resolve <reason> <subject> --evidence <last_evidence> --note "<text>"`
over the existing IPC socket
sets `resolved_at`, `resolved_by='operator'`, `resolution_note`. It is served by the running
daemon over IPC, so **it is not a step in any remedy that says "stop the daemon"**: it is for
rows the operator has verified need no action while the daemon keeps running (`inv1_overrun`
after correcting the fee schedule; a `phoenixd_forgot_invoice` row for an invoice the
operator has established was never paid). Restarting a stopped daemon only to clear a row
runs boot settlement catch-up before the socket is reachable, which is the double booking the
stop exists to prevent. The accounting close-out (`lnrent-condition-closeout`) must therefore
ship as an **offline `lnrentd` subcommand** usable against a stopped daemon, and it clears the
row atomically with the books, **bound to the observed episode exactly as `condition resolve`
is** (a required `--evidence`, conditioned on `last_evidence`; a replacement that landed before
the daemon fully stopped must not be terminalised under a decision made for its predecessor). (PR #90 codex round 44.) Same authority as
`lnrent sweep --yes`: whoever holds the socket. It reports rows affected and exits non-zero on
zero, so a mistyped subject cannot report success. **The clearance is bound to the episode
the operator saw**: `--evidence` is required, `lnrent money` prints each open row's
`last_evidence` next to it, and the update is conditioned on `last_evidence` still equalling
that value; a mismatch refuses with the current value shown. Without it, a new observation
landing between the operator's read and the command (a replacement invoice rewriting
`last_evidence`, or a second `inv1_overrun` arriving while the first is being remediated)
would be cleared blind, hiding the replacement or lifting the refund latch for an overrun
nobody has looked at. (PR #90 codex round 71.) **It accepts only reasons whose registry
resolution authority includes `operator`** (`phoenixd_unusable_record` names none: its receipt
stays bookable, and `lnv2_paid_unrecovered` names none either: its receipt is confirmed paid
and terminal, so a reporting-only clearance would drop an owed buyer from `conditions_open`
and let READY return; both exit only by booking, where possible, or by the offline close-out). A daemon-only reason (`fee_credit`,
`getbalance_outage`) is refused with the resolver named ("clears when the receipt books; fund
the wallet"): manual clearance of one would leave the receipt unbooked, silence every later
sighting (same evidence, §3), and let `status` print READY over it. (PR #90 codex round 2.) `reason` is a wire vocabulary printed by
`money` and typed here; it is pinned in `docs/protocol/` like `AlertKind` is. **The delivery
amends `docs/go-live.md` (its alert section still says edge-triggered and that `money`/`status`
show recent alert history, and that lnv2 cannot raise this alert; the manual-clearance and
stop-daemon procedures must be discoverable there), `docs/protocol/operator-conformance.md`
item 39 and its fixture, and the standing
alerting spec `docs/specs/gate1-alerting-operability.md` (its "edge-triggered, in-memory,
restart resets, do not persist" contract and its acceptance list), in the same PR**, because
that item still promises `settlement_unbookable` is "enqueued in its own transaction when the
condition is observed, edge-triggered with a per-`(kind, subject)` cooldown" and that a restart
may re-alert; under this ADR it is grouped by backend kind, its cooldown persists across
restarts, and it may be enqueued later by the supervisor scan. `docs/protocol/` is the contract a
second implementation builds against, so the change there is part of the deliverable, not a
follow-up. (PR #90 codex round 26.)

**Clearance is reporting-only.** It changes nothing in the books: an order settled out of band
stays PENDING with its reservation held. Be precise about what can close it today: nothing.
Reconcile refuses to expire an invoice whose lookup fails closed (`reconcile.rs`,
`order_invoice_may_expire` returns false on error), and `lnrent listing withdraw` only stops
new intake (`listing.rs`); neither terminalises an existing order or releases its
reservation. Until the close-out command ships (`lnrent-condition-closeout`, filed
separately), such an order stays open and the view keeps listing it
(`conditions_cleared_books_open`, §6). That is the honest state, and it is why the close-out
bead exists rather than a runbook line. (PR #90 codex round 4.) Folding an accounting write into clearance is exactly the
"reporting writes on the money path" mistake this ADR exists to name. The operator-only
reasons are all rare, externally caused, and their runbooks already begin with "stop the
daemon", so a money-path close-out command for them is machinery for a case handled by hand
with the daemon down.

### 8. Reading the ledger as a gate (send side)

A money decision may read an open row. lnrent-7fx's refuse-new-automated-refunds latch is
"no open `inv1_overrun` row". The predicate is **the read succeeded AND it returned no open
row AND the store is not degraded**. A readable-but-unwritable store answers "no open row"
while the write that would have opened one was refused, and a read error mapped to an empty
set answers the same; both are checks that pass without proving anything. Any error on the
condition read refuses the automated refund, and the refusal propagates: no caller may
convert it into "not gated". (Panel: Opus; PR #90 CodeRabbit round 6 for the read-error
half.)
Whether 7fx also needs a pending-audit obligation row (its audit is one-shot today) is 7fx's
design; the ledger can carry it as another reason.

**Operator clearance is the sanctioned way to lift the latch, deliberately.** lnrent-7fx
specifies "a latching refuse-new-automated-refunds flag until the operator clears it" and
forbids adding a probe; the correction (configure `[phoenixd] fee_schedule_*` for the running
release) is a config change the daemon cannot verify without one. So `condition resolve
inv1_overrun <wallet>` lifts the gate on the operator's word, the same authority as
`lnrent refund-retry`, which 7fx keeps unblocked as the escape hatch. What bounds the risk is
§3: the next overrun carries a new attempt key, re-opens the row, re-latches, and re-alerts,
so an uncorrected wallet bleeds at most one refund's excess per clearance, never silently.
A durable "wallet corrected" approval step was considered (PR #90 CodeRabbit round 19) and
declined as a probe by another name.

## Consequences

- The subsumed beads become acceptance criteria of one implementation bead; the set is
  derived from `br` close reasons (query in Context), never listed here.
- lnrent-8scw (enumerate phoenixd's own records, authority inversion) is not subsumed: it
  needs a new observer, not new state, and rests on unmeasured endpoints.
- `poll_retires_at`/`last_look`, `recent_alerts` and its four corruption-refusal tests, and
  `reports_unbookable_settlements()` are deleted. `phoenixd_unbookable_settlement` is deleted
  **only after** its rows (carried into `lnrent.sqlite` by the ADR-0022 import) have been
  converted, in one idempotent transaction that **runs after the ADR-0022 post-open side-file
  import, never as an ordinary schema migration**: an operator can upgrade straight from
  today's side-file layout to a binary carrying both deliveries, and a migration that runs at
  `Store::open` would find no timer rows yet, complete vacuously, and let the table be dropped
  before the import brings the timers in. The order is fixed and tested for the direct
  upgrade: side-file import → timer conversion → table deletion. (PR #90 codex round 35.)
  The same post-import step **backfills a condition for every pre-existing terminal row the
  watcher will never re-observe**: an imported `lnv2_invoice` row already `PAID_UNRECOVERED`
  gets its `lnv2_paid_unrecovered` condition (`first_observed_at = settled_at` if present, else
  the import time), because the §3 producer opens the condition only on the OPEN →
  PAID_UNRECOVERED transition and boot recovery re-subscribes OPEN rows only
  (`lnv2_backend.rs`); without the backfill a confirmed payment would be visible only in the
  logs while `money` prints READY. The same step creates the row's per-row `alert_group`
  entry (generation 1, `last_alerted_at = NULL`), which §3 would otherwise create only on
  observation and the watcher never re-observes a terminal row, so the condition is due at
  once and the operator gets the DM as well as the `money` line. Idempotent: skip rows whose
  condition exists. (PR #90 codex
  round 39.)
  The conversion: a row whose subject already satisfies the §2
  booking predicate is **skipped** (a late-settled receipt books through the terminal-refund
  arm and leaves its timer behind; converting it would tell the operator to settle an
  already-refunded buyer); every other row becomes a condition row with `first_observed_at =
  first_refusal_at`, reason `late_unbooked` if the invoice is already EXPIRED past the poll
  grace (no observer will ever re-raise it) **or if the invoice row no longer exists** (reaped
  before the upgrade, §6; `last_detail` records `books reaped before upgrade`; a missing row
  can never satisfy a daemon-on-booking resolver, so `fee_credit` there would be a row that
  withholds READY forever), else `fee_credit`; and `alert_group` is
  **backfilled for the phoenixd `fee_credit` group only** from the newest legacy
  `settlement_unbookable` outbox row **whose subject is the old `fee_credit` constant** (an
  `index_diverged` delivery says nothing about these receipts and must not suppress their
  first alert), `created_at` → `last_alerted_at` (generation 1), so a migrated row older than
  its alert-after does not DM the operator again about an incident they were already told
  of. **Every converted row also gets its `alert_group` row created** (generation 1), and for
  a converted `late_unbooked` row that entry is created **unseeded** (`last_alerted_at =
  NULL`) rather than omitted: §§4–5 alert by scanning due groups, so a late row with no
  group would show in `money` and never send its immediately due manual-settlement DM.
  **The seed must post-date the earliest surviving converted row's `first_refusal_at`**:
  a legacy DM for receipt A that then booked (its timer skipped, its outbox row still there)
  says nothing about a later receipt B; the group went quiet between them, and the runtime
  rule treats a quiet-then-refilled group as a fresh incident. If the newest qualifying DM is
  older than every surviving converted row, `last_alerted_at` stays NULL and B alerts once
  its alert-after elapses. (PR #90 codex round 54.) **The `late_unbooked` group is never
  seeded from it**: a timer converted straight to
  `late_unbooked` has a different remedy (manual settlement, not funding), so the legacy
  funding DM did not tell the operator what they now need to do, and §4's runtime re-key
  likewise opens a quiet late group as a fresh incident. (PR #90 codex rounds 10, 12 and 49;
  CodeRabbit round 12.)
- Health: `status` reports `degraded_read_only` only after the store has latched. Nothing
  measures disk headroom before that. Filed separately; not this ADR.

## Considered

- **One row per wallet-level incident** (the current DM subjects). Refuted by the
  two-receipts probe.
- **Incident + affected-receipt children.** Same information with a second table and an
  aggregate that must agree with the rows.
- **Widen the alert-history window.** Delays the disappearance; does not remove it.
- **Episode/version discriminator on the row** (codex). Right in principle; the observe rules
  in §3 give the same guarantees with the primary key alone, and the prior resolution is kept
  in `event_log`.
