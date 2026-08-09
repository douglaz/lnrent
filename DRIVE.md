# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `gc7`, `yg0`, `7fx`, `4kg`,
`3zt`, `799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`. Explicitly OUT of scope: `nfj` (stop-list —
cluster deploy + real sats), the live-verification beads (`cnf`, `rwv`, `kr1`, `u43`, `tof`,
`e96`), upstream fedimint (`y32`, `7y1`), `ea1` (release), `5h4` (product design), `xov` (user
deferred). `br ready` counts the whole repo — filter it through this line.

**Phase:** BUILD · **Bead:** (next — see Now) · **Branch:** —
**Pending:** —
**Gate:** the full CI matrix in AGENTS.md "Building and testing" — the workspace clippy+test pair
is the inner loop only; it omits both `--no-default-features` legs, wasm, and the two web E2E runs
· last green on the tree this commit records (EXIT=0). Deliberately no SHA and no test count:
each names something that the act of writing it changes or dates, and both drifted when this
file carried them.

## Done (this session)
- lnrent-gc7 SettlementUnbookable operator alert — merged #82, CLOSED. 47 commits. Two operator
  procedures DELETED on panel advice (482 lines); nine follow-ups filed rather than folded in.
- lnrent-m7g nix packages + container image — merged #80
- lnrent-ole record the measured phoenixd `completedAt` shape — merged #81 (scope REDUCED: the
  terminal-FAILED resolution was cut after 11 P1 double-pay findings)

## Now
gc7 is merged and closed. Next scoped bead: **lnrent-yg0** — stuck-payment alerts carry no
diagnosis, so the operator DM cannot say terminal vs in-flight. It is the natural pair (same alert
surface) and needs a `refund_attempt.last_error` migration, which is why it was carved out of
lnrent-ole rather than done there.

Follow-ups gc7 filed, none blocking: **qvjz (P1)** restart re-pays PENDING refunds after an index
loss — exists on master today, gc7 makes it visible; **8scw** the repair tool that replaces the cut
runbook, with a seven-item acceptance list; **3p71** lnv2 PAID_UNRECOVERED unwired; **hh4q**
late settlement on an EXPIRED invoice is owed but invisible; **peri**, **bdkh**, **kwr**, **yjtd**,
**ie4p**.

## Panel status — READ THIS BEFORE THE NEXT HARDEN
Fable ran out of credits during gc7's pass 29 and HANGS rather than erroring (a one-token probe
returned RC=124 with `is_error`, empty stderr — exit-code guards do NOT catch it; absent output is
the only reliable signal). gc7 was landed on a CODEX-ONLY clearance with the user's explicit
decision, labelled as such in the PR body and in the merged DRIVE.md.

Check fable before the next panel. If it is still down, either restore it or say plainly in the PR
that the panel is degraded — do not let a one-reviewer pass be recorded as CLEAN. On gc7, fable
found the last three P1s while codex's final passes went 4 -> 1 -> 1 -> 0.

## Open questions for the human
- `lnrent-nfj` is the sole P1 and the only gate on the nightly-run epic, but needs a cluster
  deploy and real sats. Stop-listed until you decide the funding amount and policy.
