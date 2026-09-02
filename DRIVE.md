# DRIVE — drain the money-path operability beads that need no live infra or real money

**Scope:** rb-lite-drainable operability/money-path beads ONLY — `yg0`, `7fx`, `4kg`, `3zt`, `3ma`,
`799`, `x1u`, `epj`, `xr7`, `02t`, `yxg`, `sll4`, plus the beads this drive filed itself:
`lnv2-lost-row-reports-paid-as-expired-l07s`, `sweep-failed-ledger-lie-7wbo`,
`unbooked-settlement-condition-ledger-hwni` (design — an ADR, NOT an rb-lite loop),
`stale-failed-restore-double-pay-uxbd`, `restore-ledger-overauthorizes-sweep-92d3` (behind uxbd),
`divergence-runbook-mechanism-resync-9yfn` (behind uxbd + 7wbo). Explicitly OUT of scope: `nfj`
(cluster deploy + real sats — funding DECIDED, see below), the live-verification beads (`cnf`, `rwv`,
`kr1`, `u43`, `tof`, `e96`), `8scw` and `p2bl` (each wants a staging MEASUREMENT first),
`lnv2-restore-fresh-hash-proof-l5kk` (a devimint proof task), `ea1` (release), `5h4` (product
design), `xov` (user deferred). `br ready` counts the whole repo — filter it through this line.

**Phase:** BUILD · **Bead:** lnrent-meqe (next — see Now) · **Branch:** —
**Pending:** —
**Gate:** the full CI matrix in AGENTS.md "Building and testing" — the workspace clippy+test pair
is the inner loop only; it omits both `--no-default-features` legs, wasm, and the two web E2E runs
· last green on the tree this commit records (EXIT=0). Deliberately no SHA and no test count:
each names something that the act of writing it changes or dates, and both drifted when this
file carried them.

## Done (this drive)
- lnrent-7wbo never park a sweep FAILED without positive backend evidence — merged #85 (squash
  `b32a99b`), CLOSED. rb-lite 6 rounds / 12 iterations (13 accepted, 9 declined, 5 deferred), STOPPED
  by the operator at round 7: three findings were legitimately deferred to beads and the panel
  re-raised each every round, and the 404 arm OSCILLATED (r4 refused it, r6 restored it, r6's panel
  demanded it refused again). Settled by hand — see the memory
  `rb-lite-panel-cannot-settle-safety-vs-liveness`; the panel has no seat accountable for liveness,
  so it converged on a tree where terminalization was unreachable by any shipped backend and called
  it essentially clean (trend 11→7→7→1→9→3).
  A separate codex pass on the settled tree caught two OVERCLAIMS in my own comments (fixed), and the
  GitHub codex bot then caught a third thing neither had: I applied the liveness argument to phoenixd
  and not to lnv2. lnv2 now answers from the federation's oplog. **Merged through the NORMAL gate**
  (`wrapper_on_tip: 1`, `NO_PENDING_EVIDENCE`) — no § 8b. Full 8-leg matrix EXIT=0 four times; five
  mutations, each reddening exactly its own assertion.
  Follow-ups filed and linked to it: `meqe` (P1), `tk34` (P1), `8l8c` (P2), `k0yl` (P2); `9dzu` and
  `7etm` closed.
  **`r4gf` CLOSED as WRONG-PREMISED** — the bot does post a SHA-bearing clean-round comment; #84's
  exit 4 was correct because that comment named the pre-push head. Corrected upstream at
  douglaz/skills#74.
- lnrent-l07s lnv2 fails CLOSED on a lost index row — merged #84 (squash `bdeeb04`), CLOSED.
  rb-lite 5 panel rounds; codex bot round 2 raised two P1s (both accepted: the hand-counted
  call-site scan replaced by a DERIVED `clippy.toml` denylist per AGENTS.md's no-frozen-counts
  rule — landing lnrent-njv's measured mechanism for these two seams — plus `file:line` cites),
  round 3 clean. Full 8-leg matrix EXIT=0 twice, CI 3x pass, `nix build` 0 post-merge, two
  operator mutations each reddening exactly its pinning assertion.
  **Merged under the degraded path with explicit operator authorization**: `bot-gate` returned
  BLOCKED_UNATTRIBUTED (exit 4) on both the initial and the live recheck, because this bot
  version emits NO wrapper on a clean round. GitHub status was "All Systems Operational" — so
  this was NOT a forge incident, and the substitution rests on round 3 completing clean after
  the push plus CI green on the exact SHA. Gate JSON kept at `.git/merge-gate-84.json` and
  `/tmp/merge-evidence-84.json`. The gap itself is now lnrent-botgate-summary-table-unattributable-r4gf
  (P2 dogfood) — EXPECT exit 4 on every clean PR until it is resolved.
  Follow-ups filed rather than folded in: `lookup-seam-collapse-hbye` (P3, the skeptic's
  SIMPLIFY), and a producer note added to `lnrent-3p71` for arm (c)'s missing operator alert.
- lnrent-qvjz probe outgoingbyhash before a no-row phoenixd payment — merged #83, CLOSED.
  Six panel rounds (round 4 INVERTED, cut 93 net lines). Six beads filed rather than folded in.
- lnrent-gc7 SettlementUnbookable operator alert — merged #82, CLOSED. 47 commits. Two operator
  procedures DELETED on panel advice (482 lines); nine follow-ups filed rather than folded in.
- lnrent-m7g nix packages + container image — merged #80
- lnrent-ole record the measured phoenixd `completedAt` shape — merged #81 (scope REDUCED: the
  terminal-FAILED resolution was cut after 11 P1 double-pay findings)
- 2026-08-30 graph recut (no code): l07s, 7wbo, hwni, uxbd, 7fx recut from a two-reviewer panel;
  92d3 + l5kk filed; `7fx -> hwni` and `92d3 -> uxbd` edges added; y32 + 7y1 CLOSED (upstream
  merged); CONTEXT.md "Operator seed" repaired (phoenixd derivation is designed, not built).

## Now — order decided 2026-08-30 (panel fable + codex, independent, reconciled; both agreed)
1. ~~`lnrent-l07s`~~ **DONE — merged #84 (squash `bdeeb04`), CLOSED 2026-08-31.** See Done above.
2. ~~`lnrent-7wbo`~~ **DONE — merged #85 (squash `b32a99b`), CLOSED 2026-09-02.** See Done above.
   **NEXT is `lnrent-meqe` (P1)**, which 7wbo's own panel found and filed: the
   `superseded_by_liability` exit of the SAME `PayStatus::Unknown` arm still terminalizes on zero
   backend evidence (still-valid intent, surplus shrunk since). The seam it needs already exists, so
   it is a small bead — take it before the hwni ADR by the same rule that ordered 7wbo ahead of it.
3. **`lnrent-unbooked-settlement-condition-ledger-hwni`** (P1, design/ADR — not an rb-lite loop).
   Now SEVEN questions: the original four + subject DOMAIN (receive-only vs outbound keys — 7fx,
   sll4, uxbd want in), subject CARDINALITY (phoenixd aggregates many invoices under one subject;
   resolving one must not clear the rest), and MANUAL CLEARANCE authority; plus the Q2 addendum on
   the y4m.3 degraded latch. phoenixd_backend.rs cites refreshed after #83. Answer them and
   bdkh/peri/kwr collapse into one implementation bead with 3p71/yjtd/ie4p as `reason` variants.
4. **`lnrent-7fx`** (kept P2 — reachability needs a custom phoenixd; ADR-0019 pins one fee tier)
   AFTER hwni (edge added). Absorbs a verified finding: `audit_inv1` is one-shot (`Ok(None) => {}`,
   `Err` warns only) so a latch on `log_inv1_overrun` alone can miss the only observation.
5. **`lnrent-uxbd`** (P1). Recut: the "BUILD NOTHING UNTIL `/payments/outgoing` is measured"
   blocker is DISCHARGED — 8scw measured it 2026-08-12 (200, JSON array, 3 records, from/to on
   `completedAt`). Target selected: mint gate at the two choke points + boot wallet audit (per-restore
   existence test) + the terminalize-as-SENT actuator (UNCOSTED — cost it first, else ship refusal +
   "settle by hand"). Two hazards split out with owners: 92d3 (ledger over-authorizes a post-restore
   sweep, behind uxbd) and l5kk (lnv2 rolled-back client.db re-spend — a devimint PROOF, out of
   drive). uxbd is phoenixd-only by declaration now.
In parallel at any point: **`lnrent-3zt`** (P2) — a verified false-green CI gate (try/catch ->
`exit(0)`, predicate true on entry) in the harness that guards everything else; `3ma` is coupled to
it by 3zt's acceptance, so it is in scope too.

Panel priority disagreements kept on record: codex wanted `7fx` and `4kg` at P1; fable's
reachability arguments held and both stay P2, with codex's findings absorbed into their text.

## Panel status — READ THIS BEFORE THE NEXT HARDEN
**2026-08-30: both seats ran clean.** Fable via the Agent tool (`model: fable`, fresh context,
66 tool calls) and codex via `codex exec -m gpt-5.6-sol -c 'model_reasoning_effort="xhigh"'
-s read-only`. Codex needs TWO things or it produces NOTHING (RC=0, empty stdout, no `-o` file):
(1) a prompt preamble "single reviewer, do NOT spawn_agent or load a multipart/coordinating review
skill, answer in this turn" — a global dpc-review skill otherwise hijacks any holistic prompt into
sub-reviewers whose results `exec` never waits for; (2) pre-rendered `br show/ready/status` dumps,
because `br` cannot take its write lock in the read-only sandbox. Never launch it under bare
`setsid` in this harness: setsid forks and returns 0 while codex keeps running — the "failed"
wrapper hides a live run that will clobber the report file.

**2026-08-12: FABLE IS BACK** — a one-token `claude --model fable -p` probe returned output with
RC=0. Re-probe before each panel anyway — the failure mode below is a HANG, not an error, so absence
of output is the only reliable down-signal. History of the outage, kept for the detection recipe:
Fable ran out of credits during gc7's pass 29 and HANGS rather than erroring (a one-token probe
returned RC=124 with `is_error`, empty stderr — exit-code guards do NOT catch it). gc7 was landed on
a CODEX-ONLY clearance with the user's explicit decision, labelled as such in the PR body. If a seat
is down, either restore it or say plainly in the PR that the panel is degraded — do not let a
one-reviewer pass be recorded as CLEAN. On gc7, fable found the last three P1s while codex's final
passes went 4 -> 1 -> 1 -> 0. `claude --model opus` via the Agent tool is a proven fallback seat
(2026-08-09: it beat codex on the qvjz EXPIRED-branch double pay).

## Open questions for the human
- None. `lnrent-nfj` funding was DECIDED 2026-08-12: dedicated phoenixd buyer node; fund once with
  50,000 sat from fenix-ostracoda (~27k spendable after the measured ~22k ACINQ channel-open
  haircut), 1,000 sat/run, manual refill below 3,000 sat — in the nfj bead + ADR-0020's dated
  amendment. nfj stays OUT of this drive (cluster deploy + real sats) but is executable whenever a
  live-infra session picks it up.
