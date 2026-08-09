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
nix develop . --command bash -c 'bash clients/web/e2e/run.sh'
nix develop . --command bash -c 'NO_WEBLN=1 bash clients/web/e2e/run.sh'
```

**Warnings are errors.** The host gates do NOT compile the wasm-only code (most of `clients/web` is
`#[cfg(target_arch = "wasm32")]`), and nothing above exercises the browser flow — so a green local
run of the first block can still fail CI on the web surface. Run the e2e if you touched
`clients/web`.

The `--no-default-features` build compiles **without fedimint/rocksdb**. It is a real shipping
configuration, so it is linted and TESTED, not merely compiled — and CI asserts the fedimint crates
are genuinely absent (deriving the list from the feature block) before running those gates. That guard exists because the gates were once vacuous:
`--no-default-features` disables defaults for the SELECTED packages, not for defaults re-activated
through a dependency edge, and `clients/cli`'s dev-dependency on `lnrentd` switched `fedimint` back
on. The tell was that both configurations reported an identical test count (they now differ). If you
add a workspace dependency on `lnrentd`, put `default-features = false` on that edge. **It is not a "safe" or "mock-only"
build**: `phoenixd_backend` is deliberately not feature-gated (`daemon/src/lib.rs`), so that binary
can still move real money. The safety boundary is the RUNTIME backend configuration, not the feature
flag — see `docs/go-live.md`.

One trap: `cargo test ... -- -D warnings` does NOT work. After `--`, cargo forwards args to the test
binary and libtest rejects it (`Unrecognized option: 'D'`). Lint coverage of test code comes from the
`--all-targets` clippy runs.

### Nix packages and the container image

`flake.nix` also builds the binaries and a container image, for our own nightly k8s run. That is
INTERNAL infrastructure — the operator-facing release is a separate piece of work.

```bash
nix build .#lnrentd            # likewise .#lnrent and .#lnrent-buyer
nix build .#image              # docker load < result  →  lnrent:<version>
nix flake check                # both halves of the fedimint-presence probe — but see below
```

Those per-binary attributes narrow `result/bin`, and nothing else. They symlink into ONE shared
build, which therefore stays a runtime dependency: `nix build .#lnrent-buyer` gives you a
`result/bin` with just that binary, but its closure still carries all three (the daemon alone is
~83MB). Do not reach for one of these expecting a slim closure — that split does not exist.

And `nix flake check` reports `all checks passed!` after `running 0 flake checks` when the check
derivations are already in the store. In that state it has proved nothing. The probe halves are
built to be broken — point `marker` at a string no build emits, or flip a `expectMarker` — so if you
change either one, break it and watch it fail rather than trusting a cached pass.

**Smoke the image by STARTING the daemon, not with `--version`.** `--version` exits before the
daemon does anything, so it passes on an image that cannot serve a single order. It did: the recipe
tree was first published under `/opt`, where `buildLayeredImage`'s symlink farm made every `ops/`
hook resolve into `/nix/store`, `Recipe::validate`'s containment check rejected them
(`daemon/src/recipe.rs:313-324`), and the daemon died with `no valid recipe found` — while
`--version` stayed green throughout. Nothing in CI runs this, so run it yourself after any change
to `contents`, `Env`, or `recipesDir`:

```bash
nix build .#image && docker load < result
timeout 25 docker run --rm --tmpfs /var/lib:rw,mode=0755 \
  -e LNRENT_PAYMENT_BACKEND=mock -e LNRENT_RELAYS=wss://relay.example \
  -e LNRENT_MNEMONIC="<any valid bip39>" lnrent:<version> lnrentd
# exit 124 (still running) is the PASS. Exit 1 with `no valid recipe found` is the failure.
```

Two things to keep intact. Dependency artifacts (the fedimint tree + bundled RocksDB C++) build in
their own crane derivation, so packaging/image edits that leave its inputs unchanged reuse it — do
not collapse that into a single `buildRustPackage`. And the package build names its packages
explicitly (`-p lnrentd -p lnrent-buyer-cli`) rather than `--workspace`: `clients/web` is
wasm-primary, and while the gates above do compile it for the host, it yields no binary, so building
it here would only add time to the image.

Nothing in CI builds any of this — the cold fedimint/RocksDB build would dominate PR wall-clock —
so it rots silently unless you run it. The one edge that rots on someone ELSE's change: the fedimint
fork's vendor hash in `flake.nix` is keyed on the verbatim `source =` string in `Cargo.lock`, so
repinning that fork breaks `nix build` while every cargo gate stays green. Repin and re-hash together.

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
- **Money paths are ledger-authorized** (ADR-0016). The rule is about DIRECTION, not about avoiding
  the balance: a wallet read must never be what PERMITS an outlay — the ledger decides that. Reads
  that report, or that reduce what is booked as liability, are correct and some are load-bearing:
  `lnrent reconcile` (the command — a different thing from the timer-driven reconcile loop, which
  `CONTEXT.md` distinguishes), the phoenixd doctor probe in `daemon/src/preflight.rs`, and
  `spendable_credit_msat` in `daemon/src/phoenixd_backend.rs`, which reads the balance to decide
  whether a receipt is spendable or unspendable fee credit (ADR-0019) — deleting that one would
  over-book liability. So: never add a balance read that authorizes paying out; do not remove one
  that constrains what is owed.
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

<!-- agent-discipline-v1 -->
## Working agreement
These rules are here because agents get them wrong by default, not because they
are good general advice.
**Edits must be verified, not assumed.** Prefer your harness's edit tool: it
fails loudly when the target text does not match. `sed -i` and `str.replace()`
do the opposite — a pattern that matches nothing changes nothing, exits 0, and
prints whatever success message you wrote. Batching several edits into one
scripted call is the usual reason this happens, and the saved tool calls are not
worth it. If you do script an edit, assert the target exists before replacing and
make the success message conditional on that assert, then grep the file
afterwards for both the new text and the absence of the old. Prose and markdown
are where this bites hardest: nothing compiles a README, so a silently skipped
edit survives and gets reported as done.
The check does not stop at the file. `nothing to commit, working tree clean` reads
exactly the same whether the work was already committed or was reverted underneath
you by another process holding the tree — so never take that message as proof a
commit happened. Take the exit code instead (`git commit` exits **1** on an empty
commit), and for anything you care about also look inside the commit, since a
partial loss commits cleanly at exit 0:
```
git add -- <every path this change touched>   # not `git add -A`: on a dirty tree it
git diff --cached                            # sweeps in unrelated work — and READ this,
                                             # since staging a path that was already
                                             # dirty takes the other agent's hunks too
git commit -m "<msg>" || { echo "commit produced nothing"; exit 1; }
git show --stat --format= HEAD               # all the paths you meant, and only those
```
Then confirm the *content* landed, per file, by the check that fits it — a file that
gained content must contain a distinctive new phrase; a file you removed lines from must
still exist **and** hold the expected remaining count of the deleted phrase; a deleted path
must be absent. A file that BOTH gained and lost content needs both of the first two — the
added phrase passing says nothing about whether the removal survived. One does not
substitute for another, and each has a way to
lie: `grep` defaults to regex (use `-Fq --`), `git show ... | grep` returns 141 under
`pipefail` when grep exits early (capture to a file first), `grep -c` counts lines rather
than occurrences, and demanding *zero* occurrences rejects a correct partial removal.
```bash
_chk=$(mktemp); trap 'rm -f "$_chk"' EXIT
# ...the three loops, using `grep -Fq --` / `grep -Fo | wc -l || true` on a captured file.
```
A clean `git status` is neither check.
The same tools also corrupt without failing. In a `sed` replacement string `&`
means "the whole match", so substituting a value containing `&&` — any shell
command that chains, which is most of them — silently doubles it and reports
success. Substitute with something that treats the replacement as a literal, and
grep for the result afterwards.
**Never pipe a gate through `tail`, `head`, or `grep`.** A pipeline's exit status
is the last command's, and `tail` always succeeds, so a failing build reports
exit 0. Redirect and capture the real code:
```
<gate> > /tmp/gate.log 2>&1; echo "EXIT=$?"
```
Then read the log. Note the `;` — not `|`.
**"Passing", "clean", "working", "verified", and "done" require a command and an
exit code.** If you cannot show one, say what you actually observed instead. This
is the single most common way an agent reports success it did not have.
**Reviewers read code; they do not run it.** A clean review — human, bot, or
model — is not a passing build. Run the gate yourself before calling anything
done.
**A test that has never failed has proven nothing.** When you add one for a bug,
watch it go red against the unfixed code first. A test asserting behaviour that
was already correct is indistinguishable from a test asserting nothing.
Three things decide whether that red run means anything. Break the **production
behaviour**, never the test's expected value or its setup — those redden any
assertion, including one that never reaches the behaviour. **Read the failure**: it
must name the assertion pinning what you broke, not an unrelated panic. And run **one
mutation per property** the test claims, since reddening the first of two leaves the
second untested while looking verified. If the test drives anything live — a real
database, a running service, real money — do the red run in a disposable environment
or not at all: a deliberately broken build can perform the harmful operation before
any assertion notices.
Gate for this repo: `nix develop -c bash -c 'cargo clippy --all-targets -- -D warnings && cargo test'`
<!-- end-agent-discipline -->

<!-- br-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`/`bd`) for issue tracking. Issues are stored in `.beads/` and tracked in git.

### Essential Commands

```bash
# View ready issues (open, unblocked, not deferred)
br ready              # or: bd ready

# List and search
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br search "keyword"   # Full-text search

# Create and update
br create --title="..." --description="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once

# Sync with git
br sync --flush-only  # Export DB to JSONL
br sync --status      # Check sync status
```

### Workflow Pattern

1. **Start**: Run `br ready` to find actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only open, unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

### Best Practices

- Check `br ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `br create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Always sync before ending session

<!-- end-br-agent-instructions -->
