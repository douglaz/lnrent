# Archived specs — delivered, kept as history

Everything in this folder describes work that **shipped**. These files are historical delivery
plans, not standing contracts: nothing here is pending, and nothing here binds current code.

They are kept rather than deleted because they record the *reasoning* behind decisions the code
only shows the outcome of — why a design was chosen, what was rejected, and what the constraints
were at the time. Git history preserves the text either way; keeping them readable at a stable path
preserves the reasoning.

## References written before the move
Closed beads, old commit messages and superseded specs cite these files at their ORIGINAL path,
`docs/specs/<name>.md`. Those references are historical records and are deliberately NOT rewritten —
the same reason the specs themselves are not edited to match new code. To resolve one, insert
`archive/`: `docs/specs/cleanup-cuts.md` is now `docs/specs/archive/cleanup-cuts.md`. To find them,
`grep 'docs/specs/' .beads/issues.jsonl` — deliberately not listed here, since any such roster is
stale the moment a bead is edited.

## Read them as of their date, not as of today
A spec here was accurate when written. Where later work changed the behaviour it describes, the
current truth is in `SPEC.md`, the code, or `docs/go-live.md` — not here. Do not treat an archived
statement as a description of present behaviour, and do not "fix" one to match new code: they are a
record of what was decided then.

## What stayed live, and why
`docs/specs/` holds only standing contracts — specs whose constraints still bind. Most are cited
from source by name or by numbering; two are kept live for other reasons, noted in the table:

`gate0-abuse-resistance.md`, `gate1-alerting-operability.md`, `gate1-operator-sweep.md`,
`refund-money-path-hardening.md`, `refund-provisioning-hardening.md`, `sub-cancel.md`,
`production-readiness.md`, `web-wasm-buyer.md`.

Each of those carries its own Status header saying why it is still binding — read it there rather
than from a table here, which would have to be co-maintained with eight headers.

The rule: a spec leaves this folder's sibling directory when its work is done AND no code depends
on it by name. If source cites it, it stays live and gets amended instead.
