# Spec: hook environment hygiene — seed never reaches hooks (PR-12)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md PR-12 (verified, 2026-07-03; found P1 by the
security audit — "the actually-exploitable path" undercutting the seed-perms hardening).

## Problem (verified)

`spawn_hook` (runner.rs:104-110) spawns hooks with no `.env_clear()`, so every
provision/suspend/resume/destroy/op hook inherits the daemon's full environment. The recommended
seed-supply path is the environment: main.rs:358 steers operators to `LNRENT_MNEMONIC` (to keep
the mnemonic out of argv/ps), config.rs:457 reads it, and nothing ever `remove_var`s it — so the
BIP39 master seed (controls the operator identity AND all ecash) sits in the daemon env for its
lifetime and is copied into every hook process: exposed to any hook's `set -x`, env-dumping tool
failure, crash core, or `/proc/<hook-pid>/environ`. This violates SPEC §13 ("secrets ride stdin
JSON, not argv or env"). `LNRENT_FEDIMINT_INVITE`/`LNRENT_FEDIMINT_GATEWAY` ride along (lower
severity, same class).

One documented dependency on inheritance exists: go-live.md §4 passes `DO_TOKEN` via the daemon
env and the do-vps hooks read `$DO_TOKEN` (verified: recipes/do-vps/* require it).

## Design

### A. Scrub bootstrap secrets from the daemon's own env

Immediately after `raw_config_from_env` has been consumed at startup (both `bootstrap` and `run`
paths), `std::env::remove_var` every `LNRENT_MNEMONIC` / `LNRENT_FEDIMINT_INVITE` /
`LNRENT_FEDIMINT_GATEWAY` var. **Scope of this guarantee (be precise):** `remove_var` updates the
libc `environ` array, so subsequent `std::env::var` lookups AND — combined with §B's `.env_clear()`
— child-hook inheritance no longer see the secret. It does **NOT** overwrite the initial environment
block the kernel placed on the stack at `exec`, so on Linux `/proc/<pid>/environ` of the *daemon
itself* may still show the original `LNRENT_MNEMONIC` for the daemon's lifetime. Fully removing that
requires the launcher not to put the secret in the daemon's env at all (systemd `LoadCredential` /
an `EnvironmentFile` the daemon deletes, or feeding the seed via `--stdin`/config-file, both already
supported and PREFERRED). So: `remove_var` is defense-in-depth against accidental in-process
`env::var` reads and belt-and-suspenders with §B; the LOAD-BEARING protection for hooks is §B's
`.env_clear()` (which does not depend on `remove_var` at all). Document the `/proc` caveat in
go-live.md and recommend the systemd-credential / stdin path for operators who need the daemon's own
environ clean. Keep the existing zeroization exactly as is.

**Ordering constraint (verified):** the daemon uses `#[tokio::main]`, which builds the multi-thread
runtime BEFORE any code in the async `main` body runs — so a scrub call inside `main` already races
worker threads (and `remove_var` is `unsafe`/thread-unsafe in newer Rust). To scrub before any
thread exists, replace `#[tokio::main]` with a synchronous `fn main()` that: (1) reads the needed
env into the config/`Zeroizing` structs, (2) `remove_var`s the `LNRENT_MNEMONIC`/`LNRENT_FEDIMINT_*`
vars while still single-threaded, (3) THEN builds the runtime explicitly
(`tokio::runtime::Builder::new_multi_thread().enable_all().build()`) and `block_on`s the async
entrypoint. Document this as the reason `#[tokio::main]` was removed.

### B. `.env_clear()` + allowlist when spawning hooks

`spawn_hook` builds the child env explicitly:

- `.env_clear()`, then pass through from the daemon env only:
  `PATH`, `HOME`, `LANG`, `LC_ALL`, `TZ`, `TMPDIR` — the base needed for a shell script to run
  tools sanely. Nothing else by default. (`LNRENT_*` never passes through, by construction.)
- **Recipe-declared passthrough:** the recipe manifest gains an optional
  `provisioning.env = ["DO_TOKEN"]` string list — vars the recipe's hooks need from the operator's
  environment. `Recipe::validate` bounds it (≤ 16 entries, each `[A-Z0-9_]{1,64}`, and REJECTS any
  name starting with `LNRENT` — the daemon's own namespace is never forwardable). At spawn, each
  listed var present in the daemon env is passed through; absent vars are skipped silently (the
  hook fails with its own missing-token error, which is clearer than a daemon-side guess).
- Ship `recipes/do-vps/recipe.toml` with `env = ["DO_TOKEN", "DO_REGION", "DO_SIZE", "DO_IMAGE"]` —
  the do-vps provision hook reads all four from the environment (`DO_REGION`/`DO_SIZE`/`DO_IMAGE`
  are optional operator overrides with hook-baked defaults, recipes/do-vps/provision:18-20), so
  `.env_clear()` with only `DO_TOKEN` would silently drop those knobs. Declare all four.
  `recipes/dummy` and `wireguard` declare nothing.
- All hook spawn paths go through this one builder: runner.rs `spawn_hook` is the single choke
  point (verified: provision/reconcile/resume/op_dispatch all call `run_hook` → `spawn_hook`).

### C. Docs

- go-live.md §4: unchanged operationally; add one line noting hooks receive ONLY the base env +
  the recipe's declared `env` list (so `DO_TOKEN` flows because do-vps declares it).
- SPEC §7.2 hook contract: document the child-env contract (base allowlist + recipe `env`).
- SPEC §13: the "secrets via stdin JSON, not argv or env" claim becomes fully true; note the
  enforcement point.

## Non-goals

No secret-manager integration; no per-op env lists (recipe-level is enough); no change to the
stdin-JSON hook input contract or hook I/O; no attempt to scrub `/proc/self/environ` history
beyond `remove_var`; no changes for the config-file/stdin seed paths (already safe).

## Acceptance

- A test hook that dumps `env` proves: no `LNRENT_*` var reaches any hook even when the daemon was
  started with `LNRENT_MNEMONIC` set; base vars present; a recipe-declared var (`DO_TOKEN`) passes
  through when set; an undeclared var does not.
- After startup, in-process `std::env::var("LNRENT_MNEMONIC")` returns `Err` (the libc `environ`
  scrub) — this is the testable guarantee. (Do NOT assert on `/proc/self/environ`: `remove_var`
  cannot overwrite the kernel-placed initial env block, so that path may still show the value; a
  clean daemon environ requires the systemd-credential/stdin launch path, which is an operator
  deployment choice, not something this code change can enforce.)
- `Recipe::validate` rejects `env = ["LNRENT_MNEMONIC"]`, oversize lists, and malformed names.
- do-vps provisioning still works end-to-end with `DO_TOKEN` in the daemon env (existing live/e2e
  flows unchanged); dummy-recipe e2e (which needs no env) stays green.
- Existing runner tests (timeout, output cap, reap) stay green — the builder change touches only
  env construction.

## Suggested implementation order

1. Env-clear + base allowlist in `spawn_hook` + the env-dumping test hook (proves the leak, then
   the fix).
2. Recipe `env` declaration + validation + do-vps manifest.
3. Startup `remove_var` scrub + environ test.
4. Doc updates (go-live §4 note, SPEC §7.2/§13).
