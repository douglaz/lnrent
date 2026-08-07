# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `gc7`, `yg0`, `7fx`, `4kg`,
`3zt`, `799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`. Explicitly OUT of scope: `nfj` (stop-list —
cluster deploy + real sats), the live-verification beads (`cnf`, `rwv`, `kr1`, `u43`, `tof`,
`e96`), upstream fedimint (`y32`, `7y1`, both legitimately in_progress), `ea1` (release),
`5h4` (product design), `xov` (user deferred). `br ready` counts the whole repo — filter it
through this line.

**Phase:** HARDEN · **Bead:** lnrent-gc7 · **Branch:** feat/gc7-settlement-unbookable-alert
**Pending:** —
**Gate:** `nix develop -c bash -c 'cargo clippy --all-targets -- -D warnings && cargo test'`
· last green 2026-08-07 at 3f1fd5b (EXIT=0, 883 tests, 0 failed)

## Done (this session)
- lnrent-m7g nix packages + container image — merged #80
- lnrent-ole record the measured phoenixd completedAt shape — merged #81 (scope REDUCED;
  the terminal-FAILED resolution was cut after 11 P1 double-pay findings)

## Now
lnrent-gc7 BUILD closed, entering HARDEN.

BUILD did NOT end on rb-lite converging — I stopped it at round 9 under Guard 2. Evidence
that closes the phase instead:
- gate `nix develop -c bash -c 'cargo clippy --all-targets -- -D warnings && cargo test'`
  → EXIT=0, 883 tests, 0 failed, run unpiped with the exit code captured.
- break-tests, both directions: disabling the alert sink fails 11 tests; making the two
  reasons indistinguishable fails the suite (EXIT=101). Restored byte-identical each time.

Budget post-mortem (skills#31): my ~250 LOC figure was wrong, not the implementation — 545
of ~1470 added lines are a test file, for a bead with five acceptance criteria. The genuine
overrun was two docs files outside the file-lock, reverted. My first cut-back removed an
acceptance criterion (the reason+remedy detail in money/status) and broke the build; both
reverted at 3f1fd5b.

## Next
Panel on this branch, then LAND. Then re-assess the scoped set — `yg0` is the natural pair,
it also touches the alert surface and needs a `refund_attempt.last_error` migration.

## Open questions for the human
- `lnrent-nfj` is the sole P1 and the only gate on the nightly-run epic, but needs a cluster
  deploy and real sats. Stop-listed until you decide the funding amount and policy.
