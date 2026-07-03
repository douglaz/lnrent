# Spec: GATE-1 alerting + operability (PR-5/PR-6/PR-9/PR-16)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md GATE-1 (verified findings, 2026-07-03). Everything
here is *around* the money core, not in it: surface conditions the daemon already detects, persist
one failure class it currently swallows, and give the operator actuators. Thin dispatcher — NOT a
monitoring framework.

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
  `RefundParked`, `RefundStuck`, `BalanceQueryFailed`, `TeardownFailed`, `RelayBlackout`,
  `BalanceLow`. No free-form kinds.
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
- Wire-in points (all conditions the code already detects): refund parked-FAILED (refund.rs:864),
  refund stuck-PENDING (see retune in §C), balance-query ALARM (supervisor.rs:1272), teardown
  dead-letter insert (§B), relay blackout (§C), balance floor (§D). Each call site keeps its
  existing log line; the alert is additive.
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

### D. PR-16 — balance floor warning (liability-independent)

- Config `min_balance_warn_msat` (default **0** = disabled; the operator opts in with a floor that
  matches their float). On each maintenance tick, if the backend reports
  `available_balance_msat = Some(b)` and `b < floor` → `BalanceLow` alert (cooldown-bounded).
- **INV-2 carve-out:** this is an operator-float warning, NOT a refund-readiness warning — it does
  not touch `refund_readiness_report`, `lnrent money`'s `ready` verdict, or the liability math.
  Annotate INV-2 in docs/specs/refund-money-path-hardening.md accordingly (the roadmap's PR-16
  cross-doc note already reserves this).

## Non-goals

No webhook/email/Prometheus sinks; no HTTP server; no persistence for alert cooldowns; no refund
cancel/abandon verb; no gateway failover (PR-13), doctor/preflight (PR-14), or structured-JSON
logging (PR-19); no changes to refund money math, capture, or reconcile transitions beyond the
dead-letter insert and the alert calls.

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
- Balance below the configured floor alerts; floor=0 never alerts; the `ready` verdict and INV-2
  behavior are unchanged (existing money-CLI tests stay green).

## Suggested implementation order

1. Alert dispatcher + outbox sink + wire the two existing refund/balance conditions (PR-5 core).
2. Teardown dead-letter table + retry + query/CLI + alert (PR-6).
3. Refunds list/retry + relay status + federation probe + stuck retune (PR-9).
4. Balance floor + INV-2 annotation (PR-16, smallest).
