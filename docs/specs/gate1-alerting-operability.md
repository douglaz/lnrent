# Spec: GATE-1 alerting + operability (PR-5/PR-6/PR-9/PR-16 + the INV-2 ledger revision)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md GATE-1 (verified findings, 2026-07-03). Everything
here is *around* the money core, not in it: surface conditions the daemon already detects, persist
one failure class it currently swallows, and give the operator actuators. Thin dispatcher — NOT a
monitoring framework.

**Revised 2026-07-04 — ledger-authoritative:** no code path reads the federation balance implicitly
anymore. The ledger (sqlite transaction history) is the sole authority for every money decision and
every automatic warning; `available_balance_msat` survives ONLY inside the explicit operator
`reconcile` command (§F), which compares wallet vs books and *reports* — never acts. This revises
the LANDED INV-2 readiness path (§E) and redefines PR-16 (§D). Rationale: the balance is an
eventually-consistent aggregate of the same history the ledger records, on a different clock —
reading it in automatic paths creates reconciliation races and a whole failure class
(`BalanceQueryFailed` handling) that exists only because of the read. Same principle as
docs/specs/gate1-operator-sweep.md.

## Problem (verified)

- Every "alert" is a log line (`tracing::error!`/`warn!` — refund.rs:864/983,
  supervisor.rs:1266-1310); go-live.md's operating posture is "watch the WARN/ERROR logs". No push
  transport exists.
- A failed `destroy` hook is swallowed: `run_lifecycle_hook` (reconcile.rs:622-630) WARNs and
  returns `Ok(())`; `fire_destroy` then transitions TERMINATED + releases the reservation with no
  retry or record. An orphaned DO droplet bills the operator forever, invisibly.
- A sick federation is indistinguishable from a sick gateway or an empty wallet; the stuck-refund
  alert fires after **7 days** (`RESOLUTION_STUCK_ALERT_S`, refund.rs:63).
- `lnrent money` shows `parked_count` but there is no per-refund list and no retry actuator
  (ipc.rs has no Refunds request; CLI bin/lnrent.rs:27+ has no verb).
- Relay connectivity is logged but not queryable; zero-relay = silent order/DM blackout.
- `log_refund_readiness` early-returns at zero liabilities (supervisor.rs:1261): a wallet draining
  to zero with nothing currently owed warns nobody (PR-16).

## Design

### A. PR-5 — alert dispatcher with a self-Nostr-DM sink

One small `alerts` module owned by the supervisor:

- `Alert { kind: AlertKind, subject: String, detail: String }` with a closed `AlertKind` enum:
  `RefundParked`, `RefundStuck`, `TeardownFailed`, `RelayBlackout`, `HoldingsLow`. No free-form
  kinds. (No `BalanceQueryFailed`: with §E there is no automatic balance query left to fail.)
- **Sink = a NIP-17 DM to the operator's own npub via the existing outbox.** This reuses the
  durable drain/retry/FAILED machinery and needs zero new infra; the operator reads alerts in any
  Nostr DM client. **Wire format (required):** `OutboxSender::drain_once` deserializes every
  outbox `payload_json` as `lnrent_wire::Msg` and drops non-`Msg` rows to FAILED (verified,
  provision.rs). An `msg_type='alert'` row is therefore NOT enough — add a real wire variant
  `Msg::OperatorAlert { kind, subject, detail }` to `wire/src/` (operator→operator DM; it is only
  ever sent to self, so no buyer-facing decode path changes) and set `payload_json` to that. The
  alert is a normal gift-wrapped DM to the operator pubkey. Config: `alerts_enabled` (default
  **true** when the payment backend is fedimint, false for mock), and that is the ONLY config — no
  webhook/email/metrics sinks in this cut (they can be added behind the same dispatch seam later).
- **Edge-triggered with a per-(kind,subject) cooldown**, not level-triggered: the dispatcher keeps
  an in-memory last-sent map and re-sends the same (kind,subject) at most once per
  `ALERT_COOLDOWN_S = 6h`. Restart resets the map (worst case: one duplicate alert per condition
  per restart — acceptable, do not persist).
- Wire-in points (all conditions the code already detects or this spec adds): refund parked-FAILED
  (refund.rs:864), refund stuck-PENDING (see retune in §C), teardown dead-letter insert (§B), relay
  blackout (§C), ledger holdings floor (§D). Each call site keeps its existing log line; the alert
  is additive. (The old balance-query ALARM call site at supervisor.rs:1272 is REMOVED by §E, not
  wired.)
- If the relay pool is down, alert DMs queue in the outbox like any DM — self-limiting, and the
  `RelayBlackout` alert is precisely the one that cannot be delivered; that is what the §C status
  query is for. Document this honestly in the module doc.

### B. PR-6 — teardown dead-letter (orphaned-instance record)

New table (append-only migration):

```
CREATE TABLE teardown_failure (
  id INTEGER PRIMARY KEY,
  subscription_id TEXT NOT NULL,
  hook TEXT NOT NULL,               -- 'destroy' (suspend failures stay WARN-only: no money burn)
  handles_json TEXT,                -- provider handles at failure time (droplet_id etc.)
  attempts INTEGER NOT NULL,
  last_error TEXT NOT NULL,         -- capped like MAX_ERROR_MESSAGE_CHARS
  first_failed_at INTEGER NOT NULL,
  last_attempt_at INTEGER NOT NULL,
  resolved_at INTEGER               -- NULL = open
);
```

- **Scope: reconcile `fire_destroy` (the retention/cancel destroy) ONLY** — this is the actual
  orphaned-droplet gap (a failed destroy there WARNs, transitions TERMINATED, and has NO retry
  ledger). The **provision-failure** best-effort destroy is deliberately EXCLUDED: it already has a
  durable `provision_cleanup_pending`/`provision_cleanup_done` journal + `recover_failed_cleanups()`
  retry loop (provision.rs:38-39,253) — adding a second ledger for the same destroy would create
  two out-of-sync retry loops (the existing one could succeed while this row stays open, retrying
  and alerting forever). Instead, **surface** the existing provision-cleanup backlog: add a read
  that counts open `provision_cleanup_pending` (minus matching `_done`) rows and fold that count
  into the same `lnrent teardowns` / `open_teardowns` view (read-only over the existing journal),
  and fire the `TeardownFailed` alert from the existing `recover_failed_cleanups` retry site when a
  cleanup has failed past a threshold. So there is ONE retry ledger per path; the dead-letter table
  below is exclusively reconcile-`fire_destroy`'s.
- On a `fire_destroy` hook failure: insert-or-update the open `teardown_failure` row for
  (subscription_id, hook) — attempts+1, last_error, last_attempt_at — and CALL the dispatcher on
  EVERY failed attempt (not just the first). The dispatcher's per-(kind,subject) cooldown (§A) is
  what suppresses repeats — so a persistently-failing destroy re-alerts every 6h, as §A and the
  acceptance require. (Firing only on the first failure would mean the dispatcher is never called
  again and the cooldown could never re-fire.)
- **The state transition and reservation handling are unchanged** — the dead-letter is purely
  additive. Preserve BOTH existing behaviors exactly: (1) the reconcile retention `fire_destroy`
  releases the reservation in the same txn as `-> TERMINATED` (a fully-paid-out rental; capacity
  frees); (2) the provision-failure `-> REFUND_DUE` path deliberately KEEPS the reservation held
  until the refund is actually SENT (SPEC §9.3; provision.rs — releasing it while the buyer is
  still owed money would let capacity be reused against an open liability). The dead-letter records
  that provider-side cleanup is owed; it must NOT change which path releases the hold.
- **Retry:** the maintenance loop retries open rows with the SAME hook + persisted `handles_json`
  input, capped backoff (next attempt no sooner than `2^min(attempts,6) * 60s` after
  last_attempt_at). Success → `resolved_at = now`. The do-vps destroy hook is by-tag idempotent
  (verified), so re-running is safe; the hook contract already requires idempotent lifecycle hooks
  (SPEC §7.2). No max-attempts park: a monthly-billing orphan is exactly the thing to keep
  retrying; the alert cooldown keeps the noise bounded.
- **Surface:** IPC `Request::Teardowns` → open rows (id, sub, hook, attempts, last_error, ages);
  CLI `lnrent teardowns [--json]`. `lnrent status` gains an `open_teardowns` count.

### C. PR-9 — liveness, refund actuator, relay status, stuck-alert retune

- **Federation liveness probe:** extend the readiness path with
  `PaymentBackend::backend_ready() -> Result<bool>` (default `Ok(true)`; Fedimint impl does one
  cheap authenticated API round-trip to the federation — implementation picks the lightest
  fedimint-client call that actually hits guardians, NOT a local-DB read). Reported as its own
  field in `lnrent money` (`federation_ok`) alongside `gateway_ok`, and folded into the readiness
  warning set as a distinct variant so "federation down" ≠ "gateway down" ≠ "balance low".
- **Refund list + retry actuator:** IPC `Request::Refunds` → all non-terminal + parked-FAILED
  `refund_attempt` rows (id, subscription_id, dest form, amount_sat, status, attempts, ages); CLI
  `lnrent refunds [--json]`. **The `refund_attempt` schema has no error column today** — the last
  failure reason is only logged. To include a `last_error_class` in the response, add one nullable
  `last_error_class TEXT` column (append-only migration) written by the refunder wherever it already
  bumps `attempts`/parks a row (a short bounded enum-ish class string, not free text). If that write
  is deemed out of scope for this bead, DROP `last_error_class` from the response contract rather
  than returning a value that isn't persisted — do not promise an unpersisted field. Actuator: `lnrent refund-retry <id>` → for a
  parked-FAILED row, reset it to PENDING with attempts=0 and nudge the refunder — the ONLY
  mutation, reusing the existing money path end-to-end (resolver → capped pay → ledger). No cancel
  verb: abandoning a refund liability is a policy action that stays manual/deliberate (documented;
  revisit only with a real need).
- **Relay status:** IPC `Request::Relays` → per-relay {url, connected, last_connected_at} from the
  nostr-sdk pool; surfaced in `lnrent status`. The supervisor checks on each maintenance tick:
  ALL relays disconnected for > `RELAY_BLACKOUT_ALERT_S = 15min` → `RelayBlackout` alert (§A note
  about deliverability applies — the queued alert also documents the outage for later).
- **Stuck-alert retune:** `RESOLUTION_STUCK_ALERT_S` 7d → **6h** (refund.rs:63), and it now also
  fires a `RefundStuck` alert (not just a log). 6h distinguishes a real outage from transient
  gateway blips at M1a scale; keep it a const (config knob only if dogfood shows the need).
  Also extend the stuck detection to cover the in-flight-forever pay path (the P3 the refund audit
  found: `commit_pay_failure` bumps attempts without ever reaching the stuck alert): any
  non-terminal refund whose `created_at` is older than the threshold alerts, regardless of which
  loop last touched it.

### D. PR-16 — ledger-holdings floor warning (liability-independent, no network)

- Define **ledger-expected holdings** (pure LOCAL reads — sqlite ledger + the local `fedimint_pay`
  index; no federation call — reused verbatim by §E's readiness compare and §F's reconcile;
  **NOT the sweep's authorization quantity**: the sweep's `surplus_msat` additionally subtracts
  `reserved_msat` — at-risk receipts + open refund liabilities — on top of this holdings bound.
  `expected_msat` answers "what should the wallet HOLD"; the sweep's surplus answers "what may
  the operator KEEP". Never authorize a payout from `expected_msat`):
  `expected_msat = Σ gross of all captured receipts (BOTH INV-3 provenance classes: settled
  invoice rows AND settle-refund event_log entries, de-duped by external payment id — the same
  receipt base as the sweep spec) − Σ gross of refund_attempt rows that are SENT
  **or whose pay has durable started evidence in the local pay index** (the same started-evidence
  disambiguator INV-2/recovery already use) − Σ max_outlay_msat of SENT/PENDING sweep rows`.
  Started-but-not-yet-SENT refunds must be subtracted: once the backend op starts, the outgoing
  contract locks those funds out of the spendable wallet, so a bound that still counts them would
  sit ABOVE the real spendable balance and §F would report false DRIFT (and readiness would
  over-count coverage — consistently, `required_msat` already treats started pays as committed/0).
  Because refunds/sweeps are subtracted at their gross/cap while real outlays are ≤ that (INV-1),
  this is a conservative LOWER BOUND on the spendable wallet.
- Config `min_holdings_warn_msat` (default **0** = disabled; the operator opts in with a floor that
  matches their float). On each maintenance tick, if `expected_msat < floor` → `HoldingsLow` alert
  (cooldown-bounded). No backend call — this warns about the operator's *books* draining (a real
  low-float condition is visible in the books; wallet-vs-books drift is §F's job).
- **Independence note:** this is an operator-float warning, NOT a refund-readiness warning — it is
  computed regardless of whether any liability exists (the readiness report stays liability-gated,
  §E). Annotate docs/specs/refund-money-path-hardening.md accordingly (the roadmap's PR-16
  cross-doc note already reserves this).

### E. INV-2 revision — refund readiness goes ledger-derived (LANDED-CODE change)

The landed readiness path (docs/specs/refund-money-path-hardening.md §3.2, implemented in
supervisor.rs `log_refund_readiness`/`refund_readiness_report` + `lnrent money`) reads
`available_balance_msat()` on every boot/maintenance tick and grew a `BalanceQueryFailed` ALARM
class for when that network read fails. Revise it to the ledger:

- The readiness compare becomes `expected_msat (§D) >= required_msat` — a pure sqlite read. The
  liability-gating, the `required_msat` outlay pricing, `GatewayUnavailable`, `Unpriceable`,
  `ParkedManual`, and the READY/NOT-READY verdict all stay exactly as they are; ONLY the
  balance-side operand changes from a federation query to the ledger lower bound.
- `BalanceQueryFailed` (warning variant + ALARM log + its call sites, supervisor.rs:1223/1272) is
  RETIRED — there is no automatic balance query left to fail. Remove the variant; the 5-variant
  taxonomy in docs/specs/operator-money-cli.md becomes 4.
- `lnrent money` reports the ledger figures (`expected_msat`, earned/reserved/paid-out per the
  sweep spec's breakdown once that lands) instead of `balance_msat`; plain `lnrent money` makes NO
  network calls except the existing gateway probe and §C's federation liveness probe (both are
  liveness/pricing checks, not balance reads).
- Rationale (same as the sweep spec): if the wallet truly holds less than the books say, the refund
  pay itself fails cleanly and parks/alerts — the pay is the fail-safe; the pre-read added only a
  false sense of coverage plus a failure class. Wallet-vs-books drift detection moves to §F.
- Annotate docs/specs/refund-money-path-hardening.md INV-2 and docs/specs/operator-money-cli.md
  with one-line pointers to this revision (do not rewrite their landed history).

### F. Explicit reconcile — the ONLY place the balance is read

- CLI `lnrent reconcile [--json]` (IPC `Request::Reconcile`): operator-invoked, on demand, never on
  a timer. It queries `available_balance_msat()` ONCE, computes `expected_msat` (§D) from the
  ledger, and REPORTS: `{ wallet_msat, expected_msat, verdict }` where verdict is `OK`
  (`wallet >= expected` — the normal case; fee savings make the wallet run above the lower bound)
  or `DRIFT` (`wallet < expected` — the wallet holds less than the books' lower bound: a
  fedimint-level loss, a missed sweep/refund accounting, or a ledger bug — investigate).
- Report-only, always: reconcile never mutates state, never gates a payment, never auto-refuses
  anything. A DRIFT verdict is for a human. A failed balance query here is just the command
  erroring — operator retries; no alert class, no daemon state.
- This is the single sanctioned `available_balance_msat` call site in the daemon after §E lands.
  **That includes the backend's own startup probe:** `FedimintPayment::join_or_open()` currently
  calls `log_readiness()`, which queries `available_balance_msat()` on every Fedimint start
  (fedimint_backend.rs:371,381) — an implicit automatic balance read with its own
  could-not-query failure branch. Remove the balance half of that startup log (keep the
  gateway-reachability half — that is a liveness probe, not a balance read); the operator gets
  wallet-vs-books on demand via `reconcile`.

## Non-goals

No webhook/email/Prometheus sinks; no HTTP server; no persistence for alert cooldowns; no refund
cancel/abandon verb; no gateway failover (PR-13), doctor/preflight (PR-14), or structured-JSON
logging (PR-19); no changes to refund money math, capture, or reconcile transitions beyond the
dead-letter insert and the alert calls. **No implicit balance read anywhere** — §F's
operator-invoked reconcile is the sole `available_balance_msat` call site; no automatic path
(readiness, floor, alerts, sweep) may query the federation balance.

## Acceptance

- Each AlertKind fires exactly one outbox DM to the operator npub on condition onset, is
  cooldown-suppressed on repeats, and re-fires after cooldown; alerts disabled → no outbox rows,
  logs unchanged.
- A failing destroy hook → TERMINATED + reservation released (unchanged) + one open
  teardown_failure row + one alert; maintenance retries with backoff using the persisted handles;
  a later success resolves the row; `lnrent teardowns` lists it open then resolved.
- `lnrent money` gains `federation_ok`; a mock backend reports true; the readiness warning
  distinguishes federation-down from gateway-down (unit-tested via a backend stub).
- `lnrent refunds` lists a parked refund with its persisted fields (id, sub, dest form, amount,
  status, attempts, ages — plus `last_error_class` only if the nullable column was added);
  `refund-retry` re-drives it through the real resolver+capped-pay path; retry of a non-parked id
  is a structured error.
- All-relays-down > threshold → RelayBlackout alert queued + `lnrent status`/`Relays` show
  disconnected; reconnect clears the condition (next onset re-alerts).
- A stuck refund alerts at 6h (was 7d), including the pay-in-flight-forever shape.
- Ledger-expected holdings below the configured floor alerts (`HoldingsLow`); floor=0 never
  alerts; no backend call is made by the floor check (assert via a backend stub that panics on
  `available_balance_msat`); a started-but-PENDING refund reduces `expected_msat` by its gross
  (assert with a row that has started-evidence in the local pay index).
- The Fedimint startup `log_readiness` no longer queries the balance (its gateway half remains);
  grep-level acceptance: after §E+§F land, `available_balance_msat` has exactly one non-test call
  site (the reconcile handler).
- INV-2 revision: readiness READY/NOT-READY verdicts reproduce the existing test matrix with the
  balance operand replaced by `expected_msat` (liability-gating, Unpriceable, GatewayUnavailable,
  ParkedManual unchanged); `BalanceQueryFailed` no longer exists; plain `lnrent money` makes no
  balance query (same panic-stub assertion); existing money-CLI tests updated accordingly.
- `lnrent reconcile` reports OK when the (mock, balance-stubbed) wallet ≥ expected and DRIFT when
  below; it mutates nothing (ledger byte-identical before/after); it is the only test allowed to
  see the balance stub called.

## Suggested implementation order

1. Alert dispatcher + outbox sink + wire the two existing refund conditions (PR-5 core).
2. §E INV-2 ledger revision (touches landed code; do it early so later steps build on
   `expected_msat`) + the doc annotations.
3. Teardown dead-letter table + retry + query/CLI + alert (PR-6).
4. Refunds list/retry + relay status + federation probe + stuck retune (PR-9).
5. §D holdings floor + §F reconcile command (small, both reuse `expected_msat`).
