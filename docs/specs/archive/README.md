# Archived specs — delivered, kept as history

Everything in this folder describes work that **shipped**. These files are historical delivery
plans, not standing contracts: nothing here is pending, and nothing here binds current code.

They are kept rather than deleted because they record the *reasoning* behind decisions the code
only shows the outcome of — why a design was chosen, what was rejected, and what the constraints
were at the time. Git history preserves the text either way; keeping them readable at a stable path
preserves the reasoning.

## Read them as of their date, not as of today
A spec here was accurate when written. Where later work changed the behaviour it describes, the
current truth is in `SPEC.md`, the code, or `docs/go-live.md` — not here. Do not treat an archived
statement as a description of present behaviour, and do not "fix" one to match new code: they are a
record of what was decided then.

## What stayed live, and why
`docs/specs/` holds only standing contracts — specs whose constraints still bind. Most are cited
from source by name or by numbering; two are kept live for other reasons, noted in the table:

| Spec | Why it is still binding |
|---|---|
| `gate0-abuse-resistance.md` | normative constraints cited from `order_intake.rs` / `nostr_engine.rs`; amended 2026-07-31 for lnrent-ml2 |
| `gate1-alerting-operability.md` | §E/§F are the sanctioned-call-site contract, cited from `ipc.rs` and ADR-0016 |
| `gate1-operator-sweep.md` | sweep invariants cited from 10 source files |
| `refund-money-path-hardening.md` | declares itself the money-path contract; INV-1 cited from `backends.rs` |
| `refund-provisioning-hardening.md` | its F-numbering is a citation namespace — `store.rs` and `SPEC.md` cite "F3/F6" by number |
| `sub-cancel.md` | a contract, not a plan: defines authorization, non-enumeration and state gates for the cancel path. No `.rs` cites it — it stays live because `SPEC.md` §6.3 cites its path and this branch had to amend it, so it is still maintained text rather than a historical record |
| `production-readiness.md` | delivered roadmap, but `alerts.rs` cites its PR-5 §A as the CLOSED alertable-condition set |
| `web-wasm-buyer.md` | **not fully delivered** — the CSP it requires is unshipped (lnrent-3ma), and it holds the only copy of the web security contract |

The rule: a spec leaves this folder's sibling directory when its work is done AND no code depends
on it by name. If source cites it, it stays live and gets amended instead.
