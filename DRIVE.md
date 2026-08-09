# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `gc7`, `qvjz`, `yg0`, `7fx`, `4kg`,
`3zt`, `799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`, plus the beads this drive files itself:
`unbooked-settlement-condition-ledger-hwni` (design), `stale-failed-restore-double-pay-uxbd`,
`sweep-failed-ledger-lie-7wbo`. Explicitly OUT of scope: `nfj` (stop-list —
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
- lnrent-qvjz probe outgoingbyhash before a no-row phoenixd payment — merged #83, CLOSED.
  Six panel rounds (round 4 INVERTED, cut 93 net lines). Six beads filed rather than folded in.
- lnrent-gc7 SettlementUnbookable operator alert — merged #82, CLOSED. 47 commits. Two operator
  procedures DELETED on panel advice (482 lines); nine follow-ups filed rather than folded in.
- lnrent-m7g nix packages + container image — merged #80
- lnrent-ole record the measured phoenixd `completedAt` shape — merged #81 (scope REDUCED: the
  terminal-FAILED resolution was cut after 11 P1 double-pay findings)

## Now
qvjz is merged and closed. **Next: `lnrent-unbooked-settlement-condition-ledger-hwni` (P1, design/ADR)**
— not another patch. lnrent has no durable record of an OPEN condition on the money-RECEIVE side:
alerts are edge-triggered with an in-memory cooldown, the only durable artifact is the outbox DM row,
and `money`/`status` reconstruct "unbookable" from a 12h delivery window. The repo already has the
right pattern one table over (`teardown_failure`: `resolved_at NULL = still open`, plus a LIVE
operator count). Both reviewers, independently: **do not ship bdkh/peri/kwr separately** — each
hardens one symptom while preserving the architecture that produces them. The blocking edges are in
`br` (`br dep tree`), so the drive cannot take the parts in the wrong order.

The four questions that ADR must settle are in the bead. Answer them and bdkh/peri/kwr collapse into
one implementation bead with 3p71/yjtd/ie4p as `reason` variants behind it.

Money-path residuals qvjz surfaced, none of them regressions — all pre-existing, now named:
**uxbd (P1)** a restore reinstates a STALE `FAILED`, which no probe in `pay_inner` can reach;
**7wbo (P1)** the sweeper terminalizes an expired intent unprobed and the next pass pays a NEW hash;
**sll4 (P2)** adoption skips the amount/INV-1 preflights and terminalizes at the wrong amount;
**p2bl (P2)** the PREPARED CAS compares phoenixd's echoed hash casing — wants a MEASUREMENT, not a
second hedge; **9yfn (P2)** the runbook explains the hazard with the mechanism qvjz closed, blocked
on uxbd + 7wbo so it is resynced once, with a true story.

Panel disagreements worth keeping: codex would raise **7fx to P1** and give **4kg** P1 consideration.
Both flag **3zt** as a cheap, independent false-green CI gate — it is the defect class this project
keeps producing, sitting in the harness that guards everything else.

## Panel status — READ THIS BEFORE THE NEXT HARDEN
Fable ran out of credits during gc7's pass 29 and HANGS rather than erroring (a one-token probe
returned RC=124 with `is_error`, empty stderr — exit-code guards do NOT catch it; absent output is
the only reliable signal). gc7 was landed on a CODEX-ONLY clearance with the user's explicit
decision, labelled as such in the PR body and in the merged DRIVE.md.

Check fable before the next panel. If it is still down, either restore it or say plainly in the PR
that the panel is degraded — do not let a one-reviewer pass be recorded as CLEAN. On gc7, fable
found the last three P1s while codex's final passes went 4 -> 1 -> 1 -> 0.

**Substitute found (2026-08-09): `claude --model opus` via the Agent tool is a working third seat.**
On the qvjz/next-steps analysis it agreed with codex on the whole graph recut, and beat it on the one
point that mattered: codex concluded "a second settlement is not proven, only an unsafe re-POST",
having reasoned that phoenixd's node-wide dedup catches a re-POST of the SAME bolt11 — true, but it
missed the EXPIRED branch, where a false `FAILED` sends the Refunder through re-resolution to a FRESH
payment hash that hash-based dedup cannot catch. Use opus where fable used to sit until fable is back.

## Open questions for the human
- `lnrent-nfj` is the sole P1 and the only gate on the nightly-run epic, but needs a cluster
  deploy and real sats. Stop-listed until you decide the funding amount and policy.
