# AGENTS.md — orientation for agents working in this repo

Read this first, then the files it points at. It tells you where truth lives, which is not always
where you would guess.

Nothing here is a substitute for reading the code. Where this file and the tree disagree, **the tree
is right** — and please fix this file.

## The one thing that changes how you should judge everything

**The Operator is a stranger, not this project's author.** lnrent exists to create an ecosystem of
independent third-party operators (README, `CONTEXT.md`). So anything Operator-facing — docs, CLI
output, error messages, onboarding — **is product surface, not polish**. A confusing message is a
defect. Judge changes by whether someone who has never read this code could run it.

## Where truth lives

| Question | Read |
|---|---|
| What does this word mean? | **`CONTEXT.md`** — the glossary. Terms first; it does name concrete backends and CLI behaviour where the term needs it. |
| How is the system supposed to behave? | **`SPEC.md`** — the protocol/state-machine spec. It mixes SHIPPED behaviour with target surface; sections mark what is not built yet, so check before assuming something exists. |
| Why was it built this way? | **`docs/adr/`** — numbered decision records. |
| What are the standing engineering contracts? | **`docs/specs/`** — see below; not all of it is live. |
| What work exists, and what is next? | **`.beads/`** via the `br` CLI — see below. |
| How does an operator actually run it? | **`docs/go-live.md`**. |

Where a spec and the code disagree on an enumeration or a count, **the code wins** — `docs/specs/
gate1-alerting-operability.md` says so verbatim, and the refund contract points at the shipped enum
rather than its own list.

### `docs/specs/` — live contracts vs archived history

- **`docs/specs/*.md` are STANDING CONTRACTS.** Most are cited from source by name or by
  section/number — `daemon/src/store.rs` cites "F3/F6"; `daemon/src/alerts.rs` cites "PR-5 §A" as a
  CLOSED set. A couple stay live for other reasons (e.g. `web-wasm-buyer.md` has unmet
  requirements). If you change behaviour a live spec describes, **amend it in the same PR**.
- **`docs/specs/archive/*.md` are DELIVERED HISTORY.** Read them for rationale, never as a
  description of present behaviour. **Do not edit them to match new code** — they are a record of
  what was decided then. Reviewers will occasionally ask you to; decline, and point at
  `docs/specs/archive/README.md`, which holds the rule for when a spec moves.

Every live spec carries a `**Status:**` header. Most say why the file still binds; read it before
assuming a spec is stale.

### `.beads/` — the work graph

The working store is `.beads/beads.db` (sqlite); `.beads/issues.jsonl` is its export, which is what
gets committed. **Go through `br`, never hand-edit either.**

```bash
br ready --json                # what is unblocked right now
br show <id> --json            # full bead, including acceptance criteria
br create "<title>" -t <type> -p <prio> --description "$(cat file.md)"
br close <id> --reason "..."   # `br update -s closed` is refused BY br itself, not by repo config
br dep add <issue> <blocker>
br sync --flush-only           # flush before committing .beads/issues.jsonl
```

**The plan of record is the phased self-dogfood programme in `SPEC.md` §15**, whose beads carry the
detail. Do not plan from `docs/specs/production-readiness.md` — it is the delivered *previous*
roadmap and says so.

**Bead descriptions go stale.** They are written once and rarely revised, so an older bead can
describe a phase that later beads redefined. Before implementing, check the bead's dependency edges,
its comments, and any amendment sections — not just the description you first read.

Current practice: one bead per PR, and when you find a defect outside your bead's scope, **file a
new bead rather than widening the current one** — then say so in the PR. Older PRs sometimes closed
several beads at once; that is history, not licence.

## Building and testing

Everything runs through the Nix devshell. **`.github/workflows/ci.yml` is the source of truth** —
the block below is a convenience copy that WILL drift, so when they disagree, believe the workflow
and fix this file. Across two jobs it runs:

```bash
# job: lint + test
nix develop . --command cargo clippy --workspace --all-targets -- -D warnings
nix develop . --command cargo test   --workspace
nix develop . --command cargo test   --workspace --no-default-features
nix develop . --command cargo clippy --workspace --all-targets --no-default-features -- -D warnings
nix develop . --command cargo clippy -p lnrent-buyer-web --target wasm32-unknown-unknown -- -D warnings
nix develop . --command cargo build  -p lnrent-buyer-web --target wasm32-unknown-unknown

# job: web buyer headless e2e — NOT covered by anything above.
# CI wraps it, so run it the same way: once with WebLN, once with NO_WEBLN=1.
nix develop . --command bash -c 'clients/web/e2e/run.sh'
nix develop . --command bash -c 'NO_WEBLN=1 clients/web/e2e/run.sh'
```

**Warnings are errors.** The host gates do NOT compile the wasm-only code (most of `clients/web` is
`#[cfg(target_arch = "wasm32")]`), and nothing above exercises the browser flow — so a green local
run of the first block can still fail CI on the web surface. Run the e2e if you touched
`clients/web`.

The `--no-default-features` build compiles **without fedimint/rocksdb**. It is a real shipping
configuration, so it is linted and TESTED, not merely compiled. **It is not a "safe" or "mock-only"
build**: `phoenixd_backend` is deliberately not feature-gated (`daemon/src/lib.rs`), so that binary
can still move real money. The safety boundary is the RUNTIME backend configuration, not the feature
flag — see `docs/go-live.md`.

One trap: `cargo test ... -- -D warnings` does NOT work. After `--`, cargo forwards args to the test
binary and libtest rejects it (`Unrecognized option: 'D'`). Lint coverage of test code comes from the
`--all-targets` clippy runs.

### Formatting — read this before you reach for `cargo fmt`

**Do not run `cargo fmt` across the tree, and do not add `cargo fmt --check` to a gate.** CI does not
format-check, and the tree is not rustfmt-clean — run `cargo fmt --check` if you want the current
scale of it. Running rustfmt on a module root
(`lib.rs`/`main.rs`) reformats *every file it declares*, burying a real change in unrelated lines.
`daemon/src/order_intake.rs` also shows spurious diffs from rustfmt version drift. **Format only the
lines you changed.**

## Layout

```text
daemon/     lnrentd — the control plane (money, orders, provisioning, Nostr, IPC)
wire/       the NIP-17 message types shared by daemon and clients
clients/    core (buyer library) · cli (agent-grade buyer) · web (WASM SPA)
recipes/    service definitions: a manifest + lifecycle hooks. do-vps is the live one
docs/       adr/ · specs/ (+ specs/archive/) · go-live.md · security/
```

## Things that will bite you

- **The daemon serves exactly ONE recipe.** `load_operator_recipe` validates all of them and keeps
  the lowest service id, ignoring the rest with a warning (`daemon/src/main.rs`). Multi-recipe
  serving is future work, and several latent bugs only become reachable when it lands.
- **A fresh daemon starts QUIET.** A new listing row is born `UNPUBLISHED`; only an explicit
  `lnrent listing publish` ever makes it `ACTIVE` (`daemon/src/listing.rs`). Do not "helpfully"
  publish on boot.
- **The daemon is the sole sqlite writer** (ADR-0001). Never write the DB from a CLI or a hook.
- **Money paths are ledger-authorized** (ADR-0016): the ledger authorizes, and **no wallet read may
  authorize money**. Do not add a balance read to a money decision. Reads for REPORTING are fine and
  intentional — the explicit `lnrent reconcile` command, and the phoenixd doctor probe in
  `daemon/src/preflight.rs`, which reports spendable balance and applies no funding rule. Note
  `lnrent reconcile` (the command) is a different thing from the timer-driven reconcile loop, which
  has a confusingly similar name; `CONTEXT.md` distinguishes them.
- **Recipe hooks are trusted, high-privilege, unsandboxed code** (ADR-0002) — not a plugin boundary.
  Treat anything that reaches a hook's environment or arguments as a security surface.
- **`recipe_id` ownership is scoped, not blanket.** A **recipe-scoped** path must never apply THIS
  recipe's hooks, pricing, or service behaviour to a subscription explicitly owned by another. But
  recipe-agnostic housekeeping — expiring an OPEN invoice, expiring an unpaid order, crediting
  downtime — deliberately acts on any row, and adding a gate there would break cleanup. The shared
  rule is `Recipe::owns_row`, where **NULL/absent owner counts as OWNED** (gating NULL as foreign was
  a real regression). `provision.rs`/`resume.rs` use a stricter inline rule on purpose because their
  skip is a *deferral*; do not unify them. Read `owns_row`'s doc comment — it says outright that it
  is not a complete map of who skips the check.
- **Live money tests are `#[ignore]`d** and need real infrastructure. `cargo test --workspace` runs
  the mocked path only.
- The repo is **public**, and a settled invoice's preimage is a bearer proof of payment — keep it out
  of fixtures and logs.

## Writing docs and comments

This repo has been bitten repeatedly by confident prose the code contradicts. Two rules:

1. **A comment or spec sentence is a claim.** Before writing one about behaviour outside your diff,
   read that code and cite `file:line`. If you cannot verify it, do not assert it. This applies to
   *fixes* too: a correction is a new claim needing the same rigour.
2. **Do not hand-maintain a count or a list the repo can derive.** Frozen greps ("cited from 10
   files", "five variants", "20 ADRs") rot on the next change. Name the source of truth and point at
   it instead. This file has violated that rule before; if you find one, cut it.
