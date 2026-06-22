# 0001 — The daemon is the sole writer of state

`lnrentd` owns the sqlite state and is the only process that mutates it. Claude
skills and the operator never write sqlite directly; they act exclusively through
the `lnrent` CLI, which the daemon executes, and every mutation lands in the
`event_log`. We chose this so the AI-free control-plane invariant is enforceable
rather than aspirational: an LLM-driven skill can request an action through a typed,
audited surface but cannot silently mutate live infrastructure or billing state, and
there are no write races between skills and the daemon.

## Considered options

- **Shared sqlite, skills write directly.** Simplest, no IPC, but creates write
  races against the daemon and makes the LLM a direct writer to live state. Rejected:
  it reduces the AI-free invariant to a slogan.
- **Hybrid: daemon owns runtime tables, skills read-only and own recipe files.**
  Close to the chosen model but leaves the boundary fuzzy. Folded into the
  sole-writer rule instead.

## Consequences

- The daemon must expose a local CLI/IPC surface, which operators need anyway.
- Recipe files on disk are the single thing skills write directly; the daemon
  reloads them on demand.
- Every state change is deterministic, logged daemon code, which makes both the
  audit trail and the AI-free boundary real.
