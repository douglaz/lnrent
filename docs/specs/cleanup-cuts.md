# Spec: dead-code cuts + comment drift + explicit synchronous (CUT-1..4, DRIFT-2/4, PR-7 residual)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md CUT/DRIFT sections (verified, 2026-07-03). Pure risk
reduction: deletions, comment fixes, and one behavior-identical explicit PRAGMA. Everything here is
verified dead or verified stale; nothing changes runtime behavior (the PRAGMA pins the value the
build already uses).

## Changes

### CUT-1 — delete the dead backend traits (~90 LOC)

`daemon/src/backends.rs`: delete `ComputeBackend` + `HostCompute` (16-23, 172-190),
`NetworkBackend` + `WireguardNetwork` (26-31, 193-208), `StorageBackend` (422-426),
`Observability` (429-432) — all methods `bail!("M0 stub")`, zero call sites outside the file
(grep-verified twice). Fix the stale header comment (backends.rs:1-3, claims Compute/Network are
implemented). Provisioning stays 100% hook-driven; SPEC §8 already reflects this (d612eda).

### CUT-2 — delete the duplicate FedimintPayment stub

`backends.rs:212-243` (all-`bail!` stub shadowing the real `fedimint_backend::FedimintPayment`).
Its ONE consumer is `daemon/tests/supervisor.rs:594`
(`dev_settle_default_is_unsupported_for_non_mock_backend`), which asserts the TRAIT DEFAULT
`DEV_SETTLE_UNSUPPORTED` error — so the replacement fixture MUST be non-mock: a minimal local
`struct NonMockBackend;` in that test file implementing only the required `PaymentBackend` methods
(as `unimplemented!()`/`bail!`) and NOT overriding `dev_settle`. `MockPayment` cannot stand in (it
overrides `dev_settle` to succeed).

### CUT-3 — remove the dead `compute_backend` operator knob

Verified: parsed, validated (config.rs:596), persisted (store.rs:68 operator row), displayed —
never read to drive behavior (runtime dispatch uses the recipe's `provisioning.backend`). Cut the
operator-facing knob, keep the storage:

- Remove the CLI arg (main.rs:68/369) and the reconcile-on-boot update path for the knob
  (config.rs:1628-1771 keeps `relays` handling; the compute column is written once as the fixed
  default `'host'` for new operators). **KEEP the `ENV_COMPUTE_BACKEND` env probe** (config.rs:371)
  — it is the read that lets the ignored-knob WARN fire for env-supplied values; deleting it would
  make the env compatibility case impossible. Keep the probe, stop threading its value into the
  resolved config, and WARN once when it is non-empty (same treatment as the config-file field
  below).
- **KEEP the `RawConfig.compute_backend` field parseable** — `RawConfig` has
  `#[serde(default, deny_unknown_fields)]` (config.rs:135), so a deployed config-file or stdin
  config that still contains `compute_backend` would FAIL to start if the field were deleted. Keep
  the field on the struct (so it parses), stop threading it into the resolved config, and emit the
  one-time "ignored" WARN whenever a non-empty value is supplied (file, stdin, OR env). This
  satisfies the compatibility promise for all config sources, not just env.
- KEEP the `operator.compute_backend` sqlite column (migrations are append-only; dropping a column
  is not worth a migration) — document it as reserved/unused at the schema comment.
- `recipe::is_known_compute_backend` stays (it validates the RECIPE's `provisioning.backend`,
  which is live — recipe.rs:199).
- go-live.md §3: drop `LNRENT_COMPUTE_BACKEND=cloud-do` from the bootstrap example (the roadmap's
  CUT-3 coupling note). Bootstrap with the var set must NOT error (ignore + one-time WARN
  "LNRENT_COMPUTE_BACKEND is ignored") so existing operator scripts don't break.

### CUT-4 — delete the vestigial `domain::SubState` / `domain::Subscription`

domain.rs:8-30/53: `Serialize/Deserialize`-derived, unit-tested, never constructed from a DB row
or parsed anywhere (every module reads `state` as SQL text) — and already wrong (no `Resuming`
variant; a future `state='RESUMING'` parse would serde-fail on exactly the in-flight paid state).
Delete the types + their tests. Deletion over repair per the overengineering rule: wire a typed
state enum only when a consumer actually needs it, and then generate it from §6.3 with a
completeness test.

### DRIFT-4 — stale comments (bundled here)

- domain.rs:60 `refund_dest` doc ("BOLT12 offer or Lightning address") — dies with CUT-4's
  deletion; if any doc comment survives on a remaining type, align it to §6.4 (LN-address/HTTPS
  LNURL only).
- store.rs:86 same stale wording on the schema column comment → "LN address or HTTPS LNURL
  (§6.4 F3/F6; BOLT12/raw bolt11 rejected at intake)".
- daemon/Cargo.toml:75 section comment says the fedimint feature is "(default OFF)" 20 lines above
  `default = ["fedimint"]` → "(DEFAULT ON; --no-default-features for the mock-only build)".

### DRIFT-2 — SPEC §6.3 "logged + alerted" overstatement

SPEC.md (§6.3 PROVISIONING->REFUND_DUE bullet) claims a failed pre-refund destroy is "logged +
alerted"; no alert sink exists yet. Reword to "logged (and recorded/alerted once the GATE-1
teardown dead-letter + alert sink land — docs/specs/gate1-alerting-operability.md)". If the
alerting spec lands FIRST, this line instead becomes plainly true — whichever lands second
reconciles the wording.

### PR-7 residual — explicit `PRAGMA synchronous=FULL`

store.rs `open()`: add `PRAGMA synchronous=FULL;` to the existing `execute_batch` and one test
asserting `PRAGMA synchronous` returns 2. Behavior-identical today (the bundled amalgamation
already defaults to FULL in WAL — verified against libsqlite3-sys 0.28's sqlite3.c:17325-17327);
this pins it against a future switch to a system sqlite where NORMAL-in-WAL overrides are common.

## Non-goals

No new abstractions; no schema migrations; no behavior changes beyond the ignored-knob WARN; no
touching `PaymentBackend`/`MockPayment` (the live seam); DRIFT-3 (renew/op id validation) is NOT
here — it is a behavior change owned by the GATE-0 spec.

## Acceptance

- Workspace builds + clippy clean, default AND `--no-default-features`; full test suite green with
  the CUT-2 fixture swapped (the dev-settle default test still asserts `DEV_SETTLE_UNSUPPORTED`).
- Grep proves no references remain to the deleted traits/types (`ComputeBackend|NetworkBackend|
  StorageBackend|Observability|HostCompute|WireguardNetwork|SubState`, and
  `domain::Subscription`).
- Bootstrap with `LNRENT_COMPUTE_BACKEND` set succeeds with a WARN; without it, unchanged; the
  operator row still round-trips (column intact).
- `PRAGMA synchronous` == 2 asserted by a store test.
- go-live.md §3 no longer sets the knob; SPEC §6.3 destroy-failure wording matches reality.

## Suggested implementation order

One rb-lite pass (or direct edit) — the whole spec is a single small bead; steps in the order
above, tests last.
