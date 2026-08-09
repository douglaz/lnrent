# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `gc7`, `yg0`, `7fx`, `4kg`,
`3zt`, `799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`. Explicitly OUT of scope: `nfj` (stop-list —
cluster deploy + real sats), the live-verification beads (`cnf`, `rwv`, `kr1`, `u43`, `tof`,
`e96`), upstream fedimint (`y32`, `7y1`), `ea1` (release), `5h4` (product design), `xov` (user
deferred). `br ready` counts the whole repo — filter it through this line.

**Phase:** HARDEN · **Bead:** lnrent-gc7 · **Branch:** feat/gc7-settlement-unbookable-alert
**Pending:** —
**Gate:** the full CI matrix in AGENTS.md "Building and testing" — the workspace clippy+test pair
is the inner loop only; it omits both `--no-default-features` legs, wasm, and the two web E2E runs
· last green on the tree this commit records (EXIT=0). Deliberately no SHA and no test count:
each names something that the act of writing it changes or dates, and both drifted when this
file carried them.

## Done (this session)
- lnrent-m7g nix packages + container image — merged #80
- lnrent-ole record the measured phoenixd `completedAt` shape — merged #81 (scope REDUCED: the
  terminal-FAILED resolution was cut after 11 P1 double-pay findings)

## Now
lnrent-gc7 — the `SettlementUnbookable` operator alert, plus the index-divergence repair runbook
in `docs/go-live.md`. The code converged early; the review passes since have been almost entirely
about the runbook, which AGENTS.md makes product surface — a stranger operator follows it
mid-incident, so a command that cannot run as written is a defect, not a typo.

Follow-ups filed rather than folded in: `lnrent-bdkh` (a deferred refusal lost across a restart
window), `lnrent-kwr` (12-hour view window), `lnrent-yjtd` (getbalance-only outage),
`lnrent-8scw` (no repair tooling for a lost index), `lnrent-ie4p`.

## Panel is DEGRADED — read before trusting the clearance
Fable ran out of credits during pass 29 (it HANGS rather than erroring: a one-token probe returned
RC=124 with `is_error`, no stderr — so exit-code guards do not catch it). The user decided to
continue codex-only.

This means the HARDEN gate was NOT met as written: `multi-reviewer-loop` reaches CLEAN only when
BOTH reviewers are clean on the same tree, and a one-reviewer pass is CLEAN_DEGRADED. The clearance
recorded for this branch is therefore a CODEX-ONLY clearance and must not be read as a two-reviewer
result. It is written down here, in the PR body, and nowhere is it claimed otherwise.

Worth weighing if this comes back: fable found the last three P1s on this branch — the degraded
store silencing its own alert sink, the correlated-failure hole in the withhold guard, and the
restart double-pay — while codex's final passes were 1 finding, then 3 already-filed. The reviewer
that was dropped is the one that had been finding the money-path defects.

## Next
LAND on a codex-only clearance. Then `yg0` — it also touches the alert surface and needs a
`refund_attempt.last_error` migration.

## Open questions for the human
- `lnrent-nfj` is the sole P1 and the only gate on the nightly-run epic, but needs a cluster
  deploy and real sats. Stop-listed until you decide the funding amount and policy.
