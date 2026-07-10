# Spec: lnrent production-readiness roadmap (M1a → public/mainnet)

**Status:** draft for codex-review-loop → beads. Written from a full read-only review (2026-07-03)
of the M1a tree by six focused audits (money-path state machine, refund/payment backend,
security/secrets, Nostr/wire transport, rate-limiting/abuse, wallet-ops/federation) plus
overengineering/prod-gaps. **Every finding below was independently verified against the code**; each
carries a `file:line` anchor. Findings are dispositioned, not just listed.

## What this spec is (and is not)

This is the **plan of record** for taking lnrent from "M1a single-box self-use, hardened money core"
to "a platform a real operator can run unattended against mainnet money and expose to the public
Nostr marketplace." It is a *roadmap*: it groups verified gaps into go-live gates, gives each a
minimal design sketch, and is the source the follow-on focused specs + beads are cut from. It is
**not** an instruction to implement now, and it deliberately **cuts** speculative mechanism (see
§CUT) — overengineering is the top project risk.

**"A real operator" is meant literally.** The project's goal is an ecosystem of independent
third-party service providers (SPEC §1), so the operator this roadmap serves is a stranger who has
never read this code. That framing is why the gates sit where they do: GATE-0 abuse-resistance
protects operators who cannot patch around a griefing gap themselves, and the GATE-1 operability
surface (alerts, sweep, teardown dead-letter, actuators — plus preflight/doctor and go-live.md) is
the product those operators onboard through, not internal tooling. "Single-box operator" scope
discipline (§non-goals) is unchanged: the ecosystem is many independent single boxes, not multi-box
HA.

## Headline (what the review actually found)

**The money-correctness core is strong and must not be cut.** All three money-path/refund audits
converged: the state machine is total and invoice-status-first; capture is idempotent and CAS-guarded;
refunds are INV-1 fee-capped (cannot overspend gross), single-ledgered, generation-bound, and
crash-recoverable via oplog backfill; the SSRF envelope on the LNURL refund resolver is thorough
(HTTPS-only, private-IP rejection with DNS-pin against rebind, per-hop redirect re-validation,
amount/description-hash binding, body+time caps). The feared bead **lnrent-z4u is confirmed *not* a
live bug** — cancel/renew are gated out of `RESUMING` at two layers (`order_intake.rs:312,374,917-928`),
so a captured renewal in the transient resume window is never stranded; only a P3 UX gap remains (no
reply DM in that window). **Do not touch the money core except the narrow durability and observability
gaps called out below.**

**The real gaps are at the edges: abuse-resistance and operability.** The daemon has *no* anti-griefing
control, *no* real alert transport (an "alert" is a log line), *no* operator payout path, and a failed
teardown hook silently bills the operator forever. These are what separate "self-use" from "platform."

Severity legend: **GATE-0** = must fix before unattended or scaled public marketplace exposure (a
stranger can strand or amplify at zero cost). One explicit carve-out: a deliberately **attended,
small-capacity dogfood launch** may precede GATE-0 as the operator's knowing risk call — that posture
and its accepted risk are documented in docs/go-live.md, which is the sanctioned exception to this
gate, not a contradiction of it. **GATE-1** = must fix before unattended mainnet operation (money can
be lost or stranded invisibly, or the operator can't act); **HARDEN** = fix before scale, not strictly
gating; **CUT** = dead/speculative code to remove; **DRIFT** = doc/spec inconsistency to correct.

---

## GATE-0 — Abuse resistance (before unattended/scaled public exposure; attended-dogfood carve-out in the legend)

The operator `#p` recipient tag is public and `gift_unwrap` authenticates *any* sender (an attacker
signs the seal with a throwaway key), so **every business handler is reachable by an unauthenticated
stranger** with a free keypair. Three independent audits confirmed there is no rate limit, PoW, or
deposit anywhere on the inbound path — the only bound is `MAX_INBOUND_CONCURRENCY = 32`
(`nostr_engine.rs:104`), which caps instantaneous work, not cumulative cost. Replay dedup is excellent
but stops *duplicate* work, not *flood* work (distinct `request_id`s bypass it entirely).

### PR-1 (GATE-0) — Per-pubkey cap on outstanding unpaid HELD reservations
- **Evidence:** `reserve()` (`reservation.rs:242-278`) counts every unexpired HELD reservation against
  the host budget with **no per-pubkey and no global cap**; the unpaid-hold TTL equals the 1h invoice
  expiry (`order_intake.rs:40,157`). On a small host (fallback budget `cpu 2 / ports 16`,
  `supervisor.rs:100`) one free keypair can order → hold the last slot → let it expire → re-order,
  keeping the host unsellable indefinitely at zero cost. The oversell-prevention mechanism becomes a
  capacity-denial weapon.
- **Fix (cheap — two audits independently derived it):** `order_id` already embeds the sender
  (`ord:{sender_hex}:{id}`, `order_intake.rs:105`), so a per-pubkey concurrent-HELD cap is a
  `WHERE order_id LIKE 'ord:{sender}:%' AND state='HELD'` guard inside `reserve()` — counting only
  LIVE (unexpired) holds, so stale/crash-left rows don't eat the cap. **Invariant to preserve:** the
  hold TTL must stay coupled to (≥) the order-invoice expiry — capture gates only on
  `invoice.expires_at`, so a hold released while its invoice is still payable lets a settle-after-release
  land on a slot another order has since reserved (oversell). The lever for freeing capacity faster is
  shortening the unpaid ORDER invoice expiry itself, which shortens both together. Cap value is
  operator-tunable; default small (e.g. 1–2 outstanding holds/pubkey).
- **Non-goal:** no global reputation system, no deposits (that is M-later, ADR-0011 attestation).

### PR-2 (GATE-0) — Per-pubkey inbound rate limit / token bucket
- **Evidence:** the only backpressure is the 32-slot concurrency semaphore; the `RateLimit` type in
  `nostr_engine.rs` is `#[cfg(test)]`-only (`nostr_engine.rs:1579,1663,1696`), wired to the test relay,
  **not the daemon**. Each fresh `(sender, request_id)` drives a signature verify + NIP-44 decrypt +
  several DB writes + a real backend `create_invoice` **before any payment** (`order_intake.rs:181`).
- **Fix:** a per-pubkey token bucket applied in `process_inbound` *after* `gift_unwrap` reveals the
  authenticated sender (so the cost gate keys on identity, not on the free outer wrap). Refill/burst
  operator-tunable. This is the single control that converts "open marketplace" from a griefing vector
  into a bounded one.

### PR-3 (GATE-0) — Authorize the `op.request` reject path *before* the durable claim
- **Evidence:** `claim()` inserts the durable `RUNNING` `op_invocation` row (`op_dispatch.rs:96`)
  **before** the owner/`ACTIVE` authorization checks (`op_dispatch.rs:152-160`). A stranger with no
  subscription therefore persists a 120-day-retained row *and* receives a signed `op.result{unauthorized}`
  reply, with no legitimate artifact — an unauthenticated inbound → durable write + signed outbound
  amplifier.
- **Fix:** the durable-idempotency-first ordering only needs to precede the *hook*, not the *auth
  reject*. Move the read-only, deterministic auth reject (unknown-sub / not-owner / not-ACTIVE) ahead of
  the `RUNNING` claim so an unauthorized `op.request` produces no durable row. Keep durable-first for the
  authorized path (a real op must be crash-idempotent). Tighten this once PR-2's rate limit lands so the
  two compose.
- **Note:** the analogous `order.request` path already reserves before invoice but has no equivalent
  pre-auth (there is no "owner" for a first order); PR-1 + PR-2 cover it.

### PR-4 (HARDEN, folds into GATE-0 story) — Cap unpaid `create_invoice` load + request payload size
- **Evidence:** every fresh order calls the backend `create_invoice` before payment
  (`order_intake.rs:181-190,442`) — a downstream-exhaustion vector against the real fedimint mint /
  gateway; partially mitigated because the invoice is created *after* `reserve` (so PR-1 short-circuits
  it once capacity is full), but wide on a multi-slot host. Separately, `params` has **no size or
  key-count cap** (`reservation.rs:43-65`) and is stored verbatim (`order_intake.rs:756`), and there is
  **no engine-level gift-wrap/message-size cap** (`process_inbound` → `serde_json::from_str`).
- **Fix:** a `params` JSON size + key-count cap in `validate_params`, and a message-size bound at the
  engine before decode. PR-1/PR-2 do most of the invoice-load mitigation; these close the residue.

---

## GATE-1 — Operability & money-safety (before unattended mainnet)

### PR-5 (GATE-1) — A real alert sink (not a `tracing::error!`)
- **Evidence:** every "alert"/"ALARM" in the tree is a log line (`refund.rs:864,939,983`,
  `supervisor.rs:1266-1310`); there is no HTTP server, metrics lib, or webhook/email — `Cargo.toml` has
  no axum/hyper/prometheus/lettre, and `go-live.md:87` itself concedes "watch the daemon's WARN/ERROR
  logs." The current posture assumes an operator tailing logs 24/7, which is not production-ready for
  real money.
- **Fix (cheap, reuses what exists):** a minimal alert dispatcher with a configurable sink, fired on
  the money-critical conditions (parked-FAILED refund, stuck-PENDING refund, orphaned-teardown from
  PR-6, zero-relay from PR-9, ledger-holdings floor from PR-16 — the full AlertKind set is defined in
  docs/specs/gate1-alerting-operability.md §A; per the ledger-authoritative revision there is NO
  balance-query alert, that class is retired). **Self-nostr-DM is the cheapest first sink** — the
  engine and operator keys already exist, so the daemon can NIP-17 DM the operator's own pubkey with no
  new infra. Leave webhook/Prometheus as optional additional sinks. Keep it a thin dispatcher, not a
  monitoring framework.

### PR-6 (GATE-1) — Orphaned-droplet dead-letter (a failed `destroy` hook burns real fiat, invisibly)
- **Evidence:** `run_lifecycle_hook` (`reconcile.rs:622-630`) swallows a failed `destroy` hook as a WARN
  and returns `Ok(())`; `fire_destroy` (`reconcile.rs:902-909`) then unconditionally transitions to
  `TERMINATED`, sets `next_deadline=NULL`, and releases the reservation — **no retry, no dead-letter**.
  A DO droplet that failed to delete keeps costing real money and is invisible: not in `lnrent money`,
  not in `subs`/`sub` (reads clean `TERMINATED`), only a WARN. (The do-vps hook is by-tag idempotent, so
  a *manual* re-run could clean it, but nothing surfaces that it's needed.) The same swallow applies to
  the provision-fail `destroy` (§6.4) — and SPEC §6.3 (the destroy-failure bullet) *claimed* that
  failure is "logged + alerted"; DRIFT-2 (now fixed) rewords it until PR-5 lands.
- **Fix:** persist teardown failures to a queryable dead-letter table (sub id, hook, provider handles,
  attempts, last error), retry with backoff on the maintenance loop, surface via a new IPC query +
  `lnrent` subcommand, and fire a PR-5 alert. Do not block the `TERMINATED` transition on teardown (the
  reservation must still release), but record the orphan so it is never silently lost.
  **Scope (per the focused spec, gate1-alerting-operability.md §B):** the dead-letter table covers
  reconcile `fire_destroy` ONLY. The provision-failure destroy already has its own durable retry ledger
  (`provision_cleanup_pending`/`_done` + `recover_failed_cleanups()`, provision.rs) — it gets
  *surfaced* into the same teardowns view and alerted from its existing retry site, never a second
  ledger (two retry loops over one destroy would drift out of sync).

### PR-7 (downgraded to HARDEN after verification) — make money-DB `synchronous` explicit
- **Original claim VERIFIED FALSE:** the review initially flagged WAL-default-`NORMAL` data loss
  (`store.rs:399` sets only `journal_mode=WAL; foreign_keys=ON`, no `PRAGMA synchronous`). But this
  build uses rusqlite `bundled` (libsqlite3-sys 0.28), which compiles the amalgamation with **neither**
  `SQLITE_DEFAULT_SYNCHRONOUS` nor `SQLITE_DEFAULT_WAL_SYNCHRONOUS` overridden, so the WAL default
  falls back to **FULL** (sqlite3.c:17325-17327; the "NORMAL in WAL" folklore applies to distro builds
  that set the flag). No current data-loss gap.
- **Residual fix (cheap):** add an explicit `PRAGMA synchronous=FULL` in `store.rs::open` plus a test
  asserting `PRAGMA synchronous` == 2, so a future switch to a system/distro sqlite (where a
  NORMAL-in-WAL override is common) cannot silently lower money-write durability.

> **Revision note (2026-07-04, ledger-authoritative):** PR-8, PR-16, and the landed INV-2 readiness
> path were re-based on one principle — **the ledger (transaction history) authorizes and warns; the
> federation balance is never read implicitly.** The balance is an eventually-consistent aggregate of
> the same history on another clock; authorizing payouts or warnings off it created an unbounded
> race surface (the sweep spec's first draft accreted catch-up/ordering/reserve patches before the
> root cause was named). Now: the sweep authorizes from ledger surplus; readiness compares
> ledger-expected holdings; the holdings floor is a ledger read; and the ONLY
> `available_balance_msat` call site is the explicit, report-only `lnrent reconcile` command.
> `BalanceQueryFailed` is retired. See docs/specs/gate1-operator-sweep.md +
> docs/specs/gate1-alerting-operability.md §E/§F.

### PR-8 (GATE-1) — Operator payout / sweep path for accumulated ecash
- **Evidence:** the CLI is read-only + admin (`Status/Recipes/Money/Subs/Sub/Suspend/Resume/Dev`,
  `bin/lnrent.rs:27-47`); the only outbound path is `pay`/`pay_refund_capped`, reachable only from the
  internal Refunder and INV-1-capped to a specific *received* amount. Operator profit (sales − refunds)
  has **no daemon-safe exit** — the only workaround is a second `fedimint-cli` against the daemon-owned
  RocksDB (ADR-0015), risking lock/corruption of the money DB. Funds aren't lost (recoverable from
  seed+invite), but there is no first-class withdrawal.
- **Fix:** a `lnrent sweep <bolt11>` IPC command (the amount is the invoice's own — no amount
  argument; surface per docs/specs/gate1-operator-sweep.md) that drives an operator-initiated `pay` outside
  the refund cap, serialized through the same store/backend actor so it can't race the Refunder or
  corrupt RocksDB. Borderline GATE-0-important: without it an operator literally cannot realize revenue.

### PR-9 (GATE-1) — Federation-liveness probe, distinct from gateway/balance; and relay/refund actuators
Three operability blind spots that leave the operator able to *see* trouble but not diagnose or act:
- **Federation liveness (`abdd29`, `aa3c`):** an offline/no-consensus federation surfaces only as
  failing per-op `Err`s and slowly-accumulating PENDING refunds; `available_balance_msat` reads *local*
  state so it is not a liveness probe, and the loud stuck-alert does not fire for **7 days**
  (`RESOLUTION_STUCK_ALERT_S`, `refund.rs:63`). Add an explicit federation-reachability probe to the
  readiness report, separate from `gateway_ok` and balance. **Retune the 7-day stuck-alert threshold
  down** — a week is far too slow for stranded money.
- **Refund actuator (`abdd29`):** `lnrent money` shows a `parked_count` but there is no per-item list and
  no retry/cancel — the operator sees "3 parked" and cannot inspect destination/amount/attempts or act
  (`ipc.rs:373-405`, no `Refunds` request). Add a `lnrent refunds` list + a retry actuator (a
  cancel/abandon verb is deliberately EXCLUDED — gate1-alerting-operability.md §C keeps abandoning
  a refund liability a manual, deliberate act).
  Note this also gates capacity: REFUND_DUE deliberately holds its reservation until REFUNDED
  (§9.3), so a permanently-parked refund pins host capacity until the operator resolves it.
- **Relay-pool status (`acd5`):** relay churn is logged but not queryable or alerted; if all relays drop,
  the operator silently stops receiving orders and refund/billing DMs. Add relay-pool status to a
  `doctor`/status query + a PR-5 zero-connectivity alert.

---

## HARDEN — fix before scale (not strictly gating)

- **PR-10 — Terminal-row GC.** No `DELETE FROM reservation|subscription|invoice|event_log` exists (only
  `op_invocation`/`inbound_request` are pruned, after 120 days, `store.rs:481-499`; `seen_message` at
  90d). Expiry only flips state (RELEASED/EXPIRED); rows persist forever, so a distinct-request-id flood
  (compounding PR-1/PR-2) grows the DB without bound → disk exhaustion. Add a terminal-row reaper beside
  the existing idempotency prune, with a retention window.
  _LANDED (lnrent-y4m.2): `Store::reap_terminal_rows` (`store.rs`), `TERMINAL_ROW_RETENTION_SECS` (30d),
  called best-effort beside `prune_idempotency_caches` in the reconcile tick, with scan indexes matching
  every reap predicate. Reaps only non-ledger, resolved rows past the window: old audit `event_log`,
  RELEASED reservations, destroyed/orphaned instances (of a terminal past-window sub, or with no sub
  on their own `updated_at`), EXPIRED-unsettled invoices (both
  fully-lapsed orders AND unpaid renewals on live subs — the retention window is itself the
  settlement-can't-arrive proof), and the childless terminal subs behind them (FK-safe, one txn). Kept:
  every ledger receipt (settled invoices + settle-refund journals), unresolved recovery journals
  (`provision_cleanup_pending` / `renew_resume`), rows behind an open refund, and subs with an open
  operational obligation (`teardown_failure` / PENDING `outbox` / ACTIVE `native_connect_session`) — so
  `expected_msat`, cleanup/resume recovery, and open obligations stay intact._
- **PR-11 — Disk-full / corruption handling on the money write path.** ENOSPC / `SQLITE_CORRUPT` /
  `SQLITE_IOERR` on a money commit propagates as a bare `Err` with no integrity-check-on-open or degraded
  mode. Combined with PR-10, a flood can fill disk and fail money writes. Add an integrity check on open
  and a defined degraded/read-only mode.
- **PR-12 — Hook secret hygiene: `.env_clear()` when spawning hooks.** `spawn_hook`
  (`runner.rs:104-110`) spawns with **no `.env_clear()`**, so hooks inherit the daemon's full
  environment. If the operator supplies the seed via `LNRENT_MNEMONIC` — the path `main.rs:358`
  *explicitly recommends* — the BIP39 master seed (controls the identity *and all ecash funds*) sits in
  the daemon env for its lifetime (`config.rs:457` reads it, nothing ever `remove_var`s it) and is copied
  into every provision/suspend/resume/destroy hook's env, exposed to any `set -x`, env-dumping tool, crash
  core, or `/proc/<pid>/environ`. Violates SPEC §13 ("secrets via stdin JSON, not env"). **Fix:**
  `.env_clear()` + an explicit allowlist (PATH, LANG, provider-token vars a recipe needs) when spawning
  hooks; scrub `LNRENT_MNEMONIC`/`LNRENT_FEDIMINT_*` from the daemon env after bootstrap. Secrets already
  ride to hooks via stdin JSON (`op_dispatch.rs:191-208`), so nothing legitimate breaks — with ONE
  documented exception: the go-live §4 run invocation passes `DO_TOKEN=<token>` via the daemon env and
  the do-vps hooks read `$DO_TOKEN`, so the allowlist MUST include it (or the recipe grows a declared
  env-passthrough list) and go-live.md must be re-verified in the same change.
- **PR-13 — Gateway failover.** Config carries one `gateway` pubkey (`config.rs:88`); the backend pins it
  and fails closed when down (`fedimint_backend.rs:653-659`), blocking **both** refunds and receiving
  (invoice creation requires a gateway — no ecash-native receive path). Remedy today is edit-config +
  restart. Add a gateway list with failover, or a hot-swap command. (The receive-side single-point-of-
  failure is inherent to the LN-invoice receive model; document it.)
- **PR-14 — `lnrent preflight`/`doctor` with real reachability checks.** Bootstrap validation is strong on
  durable-state integrity but pings nothing external — a well-formed-but-wrong gateway/invite passes and
  only fails at runtime `join_or_open`; `DO_TOKEN` validity isn't checked at startup (go-live relies on a
  manual `curl`). Add a preflight that actually probes gateway + federation + provider token. (SPEC already
  describes `lnrent doctor`; it is unimplemented.)
- **PR-15 — IPC socket bind→chmod window + no peer-cred.** `ipc.rs:110-113` does `bind` then a *separate*
  `set_permissions(0o600)`; between them the socket carries umask-default perms, and authz is purely FS
  perms (no `SO_PEERCRED`). Narrow (needs a loose umask + a pre-existing world-traversable data dir, which
  `config.rs` deliberately permits), but a local user could issue operator commands in the window. Set a
  tight umask before `bind` (or bind in a private subdir); optionally add an `SO_PEERCRED` check.
- **PR-16 — Draining-holdings warning independent of liabilities (ledger-derived).** `log_refund_readiness`
  early-returns at zero liabilities (`supervisor.rs:1261`) and readiness is liability-gated, so books
  draining toward zero with nothing *currently* owed warn nobody. Per the ledger-authoritative revision
  (note above / ADR-0016), the floor is computed from **ledger-expected holdings** (`expected_msat`,
  a pure local read — gate1-alerting-operability.md §D), NOT from `available_balance_msat` (no
  automatic balance read exists anymore; wallet-vs-books drift is the explicit `lnrent reconcile`'s
  job). **Cross-doc note:** this deliberately extends INV-2's "warn ONLY when an actual liability
  exists" (docs/specs/refund-money-path-hardening.md); the carve-out is annotated there (a holdings-floor
  operator warning is not a refund-readiness warning).
- **PR-17 — Stale past-due `next_deadline` on terminal states.** `capture` order arm (PROVISIONING),
  `provision` (REFUND_DUE), and `refund` (REFUNDED) never clear/reset the reconcile cursor
  (`capture.rs:187-190`, `provision.rs:884-887`, `refund.rs:792-793`), unlike EXPIRED/TERMINATED which set
  `next_deadline=NULL`. So terminal `REFUNDED` rows stay `next_deadline <= now` and are re-selected every
  tick into the totality no-op arm — unbounded per-tick rescans that grow with accumulated rows (compounds
  PR-10). Not a correctness bug (every transition is CAS-guarded). Clear the cursor on entering `REFUNDED`
  (and reset on `PROVISIONING`).
- **PR-18 — Migration + `user_version` bump in one transaction.** `store.rs:313,331` — a future
  multi-statement migration could half-apply. Wrap each migration+bump in one `BEGIN…COMMIT`.
- **PR-19 — Optional structured (JSON) logging.** `main.rs:160` uses `fmt::init()` (unstructured plaintext)
  despite rich structured fields on money events — awkward to ship to Loki/ELK for log-based alerting. Add
  an opt-in `.json()` subscriber. (Secondary to PR-5's push alerts.)
- **PR-20 — Backup passphrase-encryption option.** `backup.rs` writes the plaintext seed + `fedimint.json`
  + ecash-bearing `client.db` unencrypted (hardened 0600/0700, symlink-refused, but not encrypted). Fine
  for a cold operator-controlled dir, but a copy to USB/cloud is a fund-controlling seed at rest. Offer an
  optional passphrase-encrypted backup.

---

## CUT — dead code / overengineering to remove

Confirmed dead by grep (no imports, no construction, no dispatch); removing them is pure risk reduction.

- **CUT-1 — `backends.rs` stub traits + impls (~90 LOC).** `ComputeBackend`+`HostCompute`
  (`backends.rs:16-23,172-190`), `NetworkBackend`+`WireguardNetwork` (`:26-31,193-208`), `StorageBackend`
  (`:422-426`), `Observability` (`:429-432`) — all methods `bail!("M0 stub")`, zero call sites.
  Provisioning is 100% hook-driven (`run_hook`); `recipe.provisioning.backend` is a JSON string, never a
  trait. **CUT** all four. The stale header comment (`backends.rs:1-3`, claims Compute/Network are
  implemented) goes with them.
- **CUT-2 — Duplicate `backends.rs::FedimintPayment` stub** (`backends.rs:212-243`). The real one is
  `fedimint_backend.rs::FedimintPayment`; this stub is used by exactly one test
  (`tests/supervisor.rs:594`, `dev_settle_default_is_unsupported_for_non_mock_backend`). Replace that
  fixture with a tiny local **non-mock** `PaymentBackend` test impl that does NOT override
  `dev_settle` — the test asserts the trait's default `DEV_SETTLE_UNSUPPORTED` error, so
  `MockPayment` (which overrides `dev_settle` to succeed) cannot stand in — then delete the stub.
  **Keep** `PaymentBackend` + its DTOs + `MockPayment` — the one genuinely polymorphic seam
  (`Arc<dyn PaymentBackend>` everywhere).
- **CUT-3 — `compute_backend` operator-config knob** (`config.rs:113,587-601`; `store.rs:68`). Validated
  and persisted but **never read at runtime**; redundant with `recipe.provisioning.backend` (the value
  actually used). Confirmed dead by two audit paths. Remove, or explicitly mark as reserved-for-forward-
  compat — but its validation currently guards nothing. **Coupling:** go-live.md §3 instructs setting
  `LNRENT_COMPUTE_BACKEND=cloud-do` at bootstrap — edit that step in the same change that executes
  this cut.
- **CUT-4 — `domain::SubState` / `domain::Subscription`** (`domain.rs:8-30,53`). Defined,
  `Serialize/Deserialize`-derived, unit-tested for exact string values — and **never used** by any live
  path (every module reads/writes `state` as SQL text; no `state.parse::<SubState>()`, no `Subscription`
  construction from a row). It is also *already wrong*: it has **no `Resuming` variant** even though the
  live machine uses `RESUMING` everywhere. A latent landmine: any future code that parses a
  `state='RESUMING'` row into `SubState` gets a serde error on exactly the in-flight paid state. **Decide:
  delete the dead types, or wire them as the single source of truth for state strings and add `Resuming`.**
  Deletion is the lower-risk default given the overengineering constraint. (Independently found by two
  audits + my own read.)

**Reviewed and explicitly *not* cut (load-bearing, keep):** the SSRF envelope, oplog crash-recovery,
INV-1 fee math + `net_payout_sat`, generation-bound refund keys, CAS-guarded transitions, atomic
multi-row txns, WAL, versioned migrations, the cold backup covering seed+rocksdb+DB, and the
filesystem/seed hardening (0600/O_NOFOLLOW/symlink-TOCTOU vetting). These read "heavy" but are crash/
money safety, not gold-plating. The inbound backfill/negentropy + three-layer dedupe machinery
(`nostr_engine.rs:854-1102,1243-1318`) is the strongest *later* simplification candidate, but each layer
defends a real attack ("buried money-DM", table-fill) — **do not cut it as part of production-hardening.**

---

## DRIFT — spec/doc inconsistencies to correct

- **DRIFT-1 (FIXED this pass) — SPEC §6.3 omitted `RESUMING`.** The canonical state machine's state list
  and the `SUSPENDED -> ACTIVE` bullet never reflected bead lnrent-18v's `SUSPENDED -> RESUMING -> ACTIVE`
  path. Corrected in SPEC.md §6.3 (state list + resume/resume-fail transitions).
- **DRIFT-2 (FIXED 2026-07-05) — SPEC claimed operator "alerts" that don't exist.** The §6.3
  destroy-failure bullet said "logged + alerted" with no alert sink built (PR-5), and the second
  review pass found four more phantom-alert anchors (§6.3 stuck-refund, §6.4 refund-failure, §6.6
  refund ledger, §7.2 hook failure). All five now read "logged (alert once PR-5 lands)".
- **DRIFT-3 — `renew.request` / `op.request` ids are not charset/length-validated; `order.request` is.**
  `validate_buyer_request_id_tail` (`order_intake.rs:978`, `[A-Za-z0-9_-]`, len 1..=128) is applied only to
  the order path (`:99`). `renew.request.id` flows unvalidated into `external_id = renew:req:<sender>:<id>`
  and the `inbound_request.request_id` column; `op.request.id` flows unvalidated into
  `op_invocation.request_id` and is echoed in `op.result`. Neither reaches a hook or a DO tag (so no
  injection), but the missing length bound is a real inconsistency — the F4 request-id hardening covered
  only the order path. Apply the same validator to renew + op ids. **Cross-doc note:** F4
  (docs/specs/refund-provisioning-hardening.md) explicitly scoped renew/op ids out as not-required;
  when this lands, annotate F4 there so the two docs don't point opposite ways.
- **DRIFT-4 — stale "BOLT12 offer or Lightning address" comments in code.** SPEC §6.4 F3/F6 *reject*
  BOLT12 and raw bolt11 at intake (LN-address/LNURL only). Two locations: `domain.rs:60` and
  `store.rs:86` (same wording on the schema column). Also in this class: the `daemon/Cargo.toml:75`
  section comment still says the fedimint feature is "(default OFF)" twenty lines above
  `default = ["fedimint"]`. All comment-only code fixes; bundle with CUT-4's cleanup bead.
- **DRIFT-5 (FIXED this pass) — the hardening spec's §3.1 fee formula was the money-losing one.**
  docs/specs/refund-money-path-hardening.md §3.1 prescribed `base_msat + floor(x_msat*ppm/1e6)` as
  normative, but the code (`fedimint_backend.rs:1283-1293`) correctly mirrors Fedimint's *actual*
  (larger) `RoutingFees::to_amount` — `base + pay/(1_000_000/ppm)`, integer division both steps —
  because the naive form under-quotes the cap (an INV-1 drain, regression-tested). An implementer
  following the doc would have reintroduced a real drain. The doc's fee model, normative algorithm
  block, §3.2 trait list (missing `refund_required_outlay_msat`), and warning taxonomy
  (missing `BalanceQueryFailed`/`Unpriceable`) are now corrected.
- **DRIFT-6 (FIXED this pass) — SPEC §6.5 claimed the SUSPENDED downtime credit was unbuilt.** The
  parenthetical "(Crediting an already-SUSPENDED sub's retention is tracked separately.)" survived after
  lnrent-d6n landed it (`17e3f6a`, reconcile.rs:416-463); §6.5 also said the credit applies to "any
  ACTIVE sub" only. Corrected to cover the SUSPENDED retention extension.
- **DRIFT-7 (FIXED this pass) — four landed spec docs still carried "Status: draft".**
  nostr-engine-drain (`02917b5`), downtime-credit-suspended (`17e3f6a`), resume-hook-driver
  (`86815fc`), web-wasm-buyer (`2ae0e1a`…`96cdd9f`) — all implemented, beads closed, headers stale.
  Flipped to Implemented with landed-divergence notes (multi-renewal RESUMING stacking; WebLN
  explicit-click P1; dev-gated `lnrent dev settle`; RESUMING added to sub-cancel's CANCEL-2 list).
- **DRIFT-8 (FIXED this pass) — six SPEC.md-vs-code drifts from the full SPEC audit.** In every case
  the code was right and SPEC was corrected: (a) **§6.3 buyer-cancel bullets said "run `suspend`" +
  "post-cancel retention"** — the landed contract (docs/specs/sub-cancel.md, `2f45dc5`) runs no hook,
  keeps an ACTIVE sub running until `paid_through`, then destroys with NO retention (materially
  different buyer-facing behavior); (b) §6.6/§11 omitted the outbox **`FAILED`**
  (structurally-undeliverable, quarantined) terminal state (`store.rs:166`, `provision.rs:1156-1170`);
  (c) §9.3 claimed REFUND_DUE releases the reservation — it deliberately KEEPS the hold until
  `REFUND_DUE -> REFUNDED` (`refund.rs:754-800`), so a parked refund holds capacity (see PR-9 note);
  (d) §8 claimed Compute/Network are "implemented fully" — they are dead M0 stubs (CUT-1), provisioning
  is hook-driven; (e) §6.1's PaymentBackend sketch omitted the landed money-hardening surface (most
  critically `pay_refund_capped`) — now points at refund-money-path-hardening.md §3 as source of truth;
  (f) §11 omitted the `seen_message` transport-dedup table; plus a §5.1 footnote that
  `order.error.order_id` is currently always absent.
- **DRIFT-9 (FIXED this pass) — ADR/go-live/README/CONTEXT staleness.** go-live.md: the §Safety-gates
  line falsely said "a default build (mock backend)" (the default build IS fedimint; safety is the
  runtime `payment_backend=mock` default), the preflight checklist told the operator to watch for a
  log line (`NOT fully ready`) that exists nowhere in the tree (real markers:
  `refund readiness warning:`/`ALARM:`), the boot-log order was wrong (npub logs before fedimint
  join), and a dead `scratchpad/live-product-proof.sh` pointer (now `scripts/live-fed-e2e.sh`).
  ADR-0003 got a revision note (BOLT12/phoenixd refund contract superseded by LN-address/LNURL +
  Fedimint); ADR-0005 (d6n landed), ADR-0010/0011 (phoenixd/BOLT12 one-liners) touched up.
  README: "Not yet: a browser/GUI buyer" was false (clients/web landed; added to Layout), CLI list
  gained suspend/resume. CONTEXT.md glossary state list gained `resuming`.

---

## Second review pass (2026-07-05) — fresh-eyes full corpus + code

Six independent fresh-eyes reviews (spec corpus, ADRs/security/runbook, SPEC/CONTEXT/README, and
three code sweeps: money path, transport/daemon shell, clients/recipes) re-swept the tree after
this roadmap landed, deliberately skipping everything already dispositioned above. Everything they
found was either **fixed in the same pass** or queued below as a new PR item.

**Fixed in the pass (code):** recipe-hook failure diagnostics moved to stderr (the runner captures
ONLY stderr on a non-zero exit — every `err()`'s stdout JSON was being discarded, do-vps AND the
wireguard stubs); do-vps `DO_TOKEN` moved off curl argv into a 0600 header file (argv is
world-readable via `/proc/<pid>/cmdline`) plus curl connect/max-time bounds and a multi-line
`ssh_pubkey` reject; do-vps `recipe.toml` tier corrected **2 → 0** (a stock DO droplet has no
attestation; ADR-0007 forbids claiming above the real tier and the value is published into the
signed listing); `order.error` no longer echoes raw backend/store error text to unauthenticated
strangers (fixed generic message + local warn, mirroring op_dispatch); relay URLs are
scheme-validated at bootstrap and the engine now skips (not fails on) an unusable stored relay URL;
M6 adds the missing `event_log` indexes (the ≤5s maintenance scans were unbounded full scans
serialized ahead of money writes); the fedimint receive index marks Canceled invoices out of the
`watch()` respawn set (previously every restart re-subscribed every historical unpaid invoice);
the web buyer's refund-dest validation now matches the daemon's gate (bech32 `lnurl1…` accepted,
`@`-first so `lnbc…@`/`lno…@` addresses are no longer falsely rejected); the supervisor's private
duplicates of `gen_key`/`parse_whole_sat` were deleted in favor of `pub(crate)` in refund.rs (the
lnrent-4gt lockstep-edit hazard). **Fixed in the pass (docs):** DRIFT-2's reword executed across
all five phantom-alert SPEC anchors; README's real-payments example split into bootstrap-then-run
(it violated go-live §3's mnemonic-is-bootstrap-only rule) and its buyer golden path gained the
required `--refund-dest`; SPEC §6.6 "no money-moving backend has shipped", §5.4 codec-pending, §10
skills-as-present, §14 layout, §6.1 duplicated pay-idempotency paragraph, §6.3 RESUMING
"refused"-vs-no-op; CONTEXT Sweep/Reconcile marked target-not-landed + tenant/provider avoid-list
carve-outs; ADR-0001 reload claim, ADR-0002 DO_TOKEN exception, ADR-0004/0009 phoenixd-as-fact,
ADR-0008 M1a/M1b revision note, ADR-0016 consequences marked target-state; go-live §2 now builds
`lnrent-buyer-cli` (§4 preflight needs it), §1 tier honesty + `name` field, §5 announce-vs-order
wording; gate-spec cross-refs (PR-8 bolt11-only, PR-9 no-cancel, §E→§F pointers, SweepFailed enum
note, `max_live_holds_per_buyer` rename, both refund-park alert anchors).

**Verified clean by the code sweeps** (beyond the first pass): capture.rs, refund_resolver.rs,
reservation.rs, backup.rs (fee math, generation gates, oplog backfill, staged restore); the e2e
suite is not vacuous against the test relay's 60/min rate limit and 500 filter clamp.

### PR-21 (GATE-1) — suspend/destroy lifecycle hooks race a concurrently-captured renewal
- **Evidence:** `fire_suspend` (`reconcile.rs:853-869`) runs the suspend hook OUTSIDE the CAS: if
  the buyer's renewal settles during the hook's up-to-120s window, capture commits first (sub stays
  ACTIVE, fresh deadline), the post-hook CAS matches 0 rows and no-ops — leaving the instance
  powered off while the row reads ACTIVE, and the resume driver only drives RESUMING. The buyer
  pays for a down service until the next suspend/renew cycle. `fire_destroy` (`reconcile.rs:899-911`)
  has the same shape: a renewal captured mid-destroy-hook (inside the credited resumable window)
  flips SUSPENDED→RESUMING while the droplet is being irreversibly deleted; money-safe (ends in a
  refund) but a timely-paid service is destroyed and it surfaces only as a confusing refund.
- **Fix sketch:** on a lost suspend-CAS, re-read the sub and best-effort run the `resume` hook as a
  compensating action (mirror provision.rs's lost-CAS cleanup), logged loudly; on destroy, re-check
  `renewal_settlement_pending` after the hook before the CAS and fire a PR-5 alert on a lost CAS to
  RESUMING. Money-core adjacent: needs its own focused spec + acceptance tests before code.

### PR-22 (GATE-1) — single-instance lock on the data dir
- **Evidence:** `run_daemon` takes no lock and `ipc.rs` silently removes + rebinds an existing
  socket, so a systemd restart racing a manually-started daemon yields two maintenance loops
  driving the same PROVISIONING sub (duplicate concurrent drives are exactly the hazard
  `supervisor.rs:1043-1046` documents) — two real droplets, one orphaned — and the second daemon
  steals the first's IPC socket. Mock mode has no accidental RocksDB-lock protection at all.
- **Fix:** an exclusive `flock` on `{data_dir}/lnrentd.lock` at startup; exit with a structured
  "daemon already running" error.

### PR-23 (HARDEN, feeds PR-6) — process-group kill for timed-out hooks
- **Evidence:** `reap()` (`runner.rs:127-130`) SIGKILLs only the immediate hook process; a
  timed-out do-vps provision's in-flight `curl` survives and can complete the droplet create AFTER
  the daemon declared failure, ran the (empty-by-tag) cleanup, and refunded — an invisible billed
  droplet that even PR-6's dead-letter never sees because no hook ever reported it.
- **Fix:** `.process_group(0)` at spawn + kill the group in `reap()`.

### PR-24 (HARDEN, extends PR-15) — IPC connection read deadline
- **Evidence:** `handle_conn` has no read timeout; one idle connected client pins the graceful
  IPC drain at shutdown until the 3s abort kills the whole task set — which can abort a concurrent
  in-flight handler after its txn committed but before its reply was written.
- **Fix:** wrap the request read in a short `tokio::time::timeout`; reply `bad_request`/close.

### PR-25 (HARDEN) — buyer CLI `--json` contract on argv parse errors
- **Evidence:** `Cli::parse()` renders clap failures as plaintext usage on stderr with exit 2 —
  breaking the machine-readable contract and colliding with the taxonomy's exit 2 = not_found.
- **Fix:** `try_parse` + render a `bad_request` JSON envelope (exit 3) when `--json` is present in
  raw argv.

---

## Dispositions for existing open beads (informed by this review)

- **lnrent-z4u (P2) — downgrade/close.** The money-path audit confirmed the feared cancel/renew-during-
  RESUMING bug **cannot occur** (gated at `order_intake.rs:312,374,917-928`). Residual is a P3 UX gap only
  (no reply DM in the transient RESUMING window). Re-file as a small P3 UX bead or fold into PR-9.
- **lnrent-26b (P3) — subsumed by PR-9 + PR-10.** The serial-drive head-of-line starvation (bounded
  refunder concurrency) is real but low-severity; keep as the concrete manifestation under the operability
  workstream.
- **lnrent-7qc (P2, fedimint refund staging dogfood)** — remains the release gate; unaffected.

## Suggested sequencing (for the follow-on focused specs / beads)

> **Status (2026-07-08).** GATE-0 is 3/4 landed on master: PR-1 hold cap (#6), PR-2 inbound
> rate limit (#8), PR-3 auth-before-claim (#10); the mi9.1 cleanup (CUT-1..4, DRIFT-2/4) also
> landed (#5). The critical path below is now **encoded in the bead graph** — `br ready` surfaces
> it, and `lnrent-7qc` (the final go-live gate) is dependency-blocked on exactly the three gates
> still open:
>
> 1. **lnrent-gdu.4** (PR-4: cap unpaid invoice load + params/message size) and **lnrent-mi9.2**
>    (DRIFT-3: request-id validator on renew/op paths) — the two beads that close the GATE-0 epic.
> 2. **lnrent-urw.1** (PR-5: real alert sink) — also unblocks urw.2/urw.4/urw.7.
> 3. **lnrent-y4m.7** (PR-12: hook-env hygiene).
> 4. **lnrent-7qc** (fedimint refund staging dogfood) becomes ready when 1–3 close → attended
>    go-live per docs/go-live.md.
>
> Steps 1–3 are parallel workstreams, not a hard sequence — per the operator decision below, the
> rest of GATE-1 may also proceed in parallel. Deliberately, only 7qc is dependency-blocked (the
> convergence point); GATE-1 child beads stay individually ready, and the epic-level
> GATE-0→GATE-1 edge only orders epic *closure*, not the work.
>
> After the path: the rest of GATE-1 in ready-queue order (urw.9 flock, urw.5 refund actuator,
> urw.8 suspend/renewal race), with **urw.10** (ledger-authoritative money core) as the
> centerpiece that unblocks urw.3 (sweep) and urw.7 (draining-holdings). The GATE-1 epic is also
> graph-ordered after GATE-0. HARDEN (y4m) batches by theme after the gates.

> **Operator decision (2026-07-04):** go-live is gated on **GATE-0 (PR-1..4) + PR-5 alerting +
> PR-12 hook-env hygiene** landing, plus the existing lnrent-7qc refund staging dogfood. The
> attended-dogfood carve-out in the legend remains valid policy but will NOT be exercised before
> those land. Critical path: GATE-0 → PR-5 → PR-12 → 7qc dogfood → go-live; everything else (rest
> of GATE-1, cleanup, HARDEN) may land during or after the attended phase. (PR-12 gates by
> operator choice: the documented runbook never exposes the seed to hooks — bootstrap is one-shot
> and hookless, run reads the seed from disk — but the operator wants the misuse path closed in
> code, not just in prose, before real money.)

1. **GATE-0 first (PR-1, PR-2, PR-3, PR-4):** the abuse cluster is what blocks unattended/scaled
   public exposure (attended dogfood excepted, per the legend) and PR-1's fix is nearly free (sender
   already in `order_id`). One focused spec.
2. **GATE-1 (PR-5, PR-6, PR-8, PR-9):** alert sink first (PR-5 unblocks the "surface it" half of
   PR-6/PR-9), then the orphaned-droplet dead-letter (PR-6), sweep (PR-8), and the liveness/actuator
   trio (PR-9). One or two focused specs (alerting+actuators; sweep). (PR-7 was verified-downgraded
   to HARDEN — the bundled build already runs `synchronous=FULL`.)
3. **CUT-1…CUT-4 + DRIFT-2 + DRIFT-4:** a single low-risk cleanup bead — pure deletion + comment/doc
   fixes, no behavior change, do it early to shrink the surface the above are built on. (DRIFT-3 is a
   small CODE change — applying the id validator to renew/op requests — schedule it as its own tiny
   bead beside PR-4, not in the cleanup bead; DRIFT-1/5/6/7/8/9 are already fixed in the docs.)
4. **HARDEN (PR-10…PR-20):** batch by theme after the gates, tuned to scale needs.

## Scope discipline (non-goals)

- No reputation/deposit/attestation system (that is ADR-0011 / M-later); PR-1/PR-2 are bounded
  anti-griefing, not a trust economy.
- No monitoring framework; PR-5 is a thin dispatcher with self-nostr-DM as the first sink.
- No multi-box / HA / horizontal scale; this is still a single-box operator.
- Do not expand the money core; the gates are durability + observability *around* it, not changes *to* it.
- Each PR above should become a **tightly-scoped** bead with concrete acceptance tests before any code —
  the standing project rule is that overengineering, not under-building, is the top risk.
