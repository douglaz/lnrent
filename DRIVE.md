# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `gc7`, `yg0`, `7fx`, `4kg`,
`3zt`, `799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`. Explicitly OUT of scope: `nfj` (stop-list —
cluster deploy + real sats), the live-verification beads (`cnf`, `rwv`, `kr1`, `u43`, `tof`,
`e96`), upstream fedimint (`y32`, `7y1`), `ea1` (release), `5h4` (product design), `xov` (user
deferred). `br ready` counts the whole repo — filter it through this line.

**Phase:** BUILD · **Bead:** lnrent-qvjz · **Branch:** fix/qvjz-index-loss-double-pay
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
Building **lnrent-qvjz** (P1) — a restart after a phoenixd index loss can pay a refund TWICE. The
bead's original text named the wrong door and its proposed remedy cannot close it; the verified
mechanism and the fix are in the bead's notes. Not yg0: that is DM-text quality (P2), this is money.

An opus+codex panel (2026-08-09) recut the gc7 follow-up graph, and the edges are now in `br` so the
drive cannot pick these up in the wrong order:

- **lnrent-unbooked-settlement-condition-ledger-hwni (P1, design/ADR)** — the missing concept behind
  most of gc7's follow-ups. lnrent has no durable record of an OPEN condition on the money-RECEIVE
  side: alerts are edge-triggered with an in-memory cooldown, the only durable artifact is the outbox
  DM row, and `money`/`status` reconstruct "unbookable" from a 12h delivery window. The repo already
  has the right pattern one table over (`teardown_failure`, with `resolved_at NULL = still open` and a
  LIVE operator count). Both reviewers, independently: **do not ship bdkh/peri/kwr separately** —
  each hardens one symptom while preserving the false architecture.
- **BLOCKED on that design:** bdkh, peri, kwr (subsumed as its acceptance criteria); 3p71, yjtd, ie4p
  (producer adapters — each becomes a `reason` variant, not a feature).
- **hh4q is now blocked on 8scw** — it is 8scw's acceptance item 1, not an independent alert. Both
  need an authoritative phoenixd inventory over UNMEASURED endpoints, so both are out of drive scope
  until a measurement task runs on the staging node.
- **Unrelated to all of the above:** qvjz (outbound side). 7fx should follow the ADR — its latching
  remedy is the same shape as a ledger row with an operator-cleared `resolved_at`.

Panel disagreements worth keeping: codex would raise **7fx to P1** and give **4kg** P1 consideration
(reconcile can mint entitlement past the credited resumable boundary; xr7 compounds it). Opus rates
**kwr** under-priced at P2 — with the store healthy, once the last alert ages out, `lnrent money`
prints a bare `Status: READY` over a diverged index, and the divergence alert's own remedy ("stop the
daemon") is what drains the window. Both flag **3zt** as a cheap, independent false-green CI gate.

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
