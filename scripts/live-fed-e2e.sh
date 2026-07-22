#!/usr/bin/env bash
# Live regtest Fedimint end-to-end proof: runs daemon/tests/lnv2_live.rs (the #[ignore]d
# real-ecash test — issue, receive over the settlement stream, then pay ecash out, idempotently)
# against a FRESH federation that devimint spins up (fedimintd + gatewayd + bitcoind + lightning).
#
# This is the LOCAL runner. It does NOT run on cloud CI: fedimint's toolchain builds from source in
# ~an hour and its cachix does not serve external flake evaluations of the v0.11.1 tag, so a
# GitHub-hosted runner times out (see .github/workflows/fedimint-live.yml). Run this on a box that has
# a built fedimint v0.11.1 worktree — this dev box, or a self-hosted runner with a warm /nix/store.
#
# Prereq (one-time): a v0.11.1 fedimint worktree with the daemons built —
#   git -C ~/p/fedimint worktree add /tmp/fedimint-0.11.1 v0.11.1
#   (this script builds the daemons on first run if they are missing).
#
# Usage:  scripts/live-fed-e2e.sh
#   env overrides: LNRENT (repo root), FEDI_WT (fedimint v0.11.1 worktree path).
set -euo pipefail

LNRENT="${LNRENT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
FEDI_WT="${FEDI_WT:-/tmp/fedimint-0.11.1}"

echo "== 1/3 build the lnv2_live test binary (current daemon) =="
cd "$LNRENT"
nix develop . --command cargo test -p lnrentd --features fedimint --test lnv2_live --no-run
TESTBIN="$(readlink -f "$(ls -t "$LNRENT"/target/debug/deps/lnv2_live-* | grep -vE '\.d$' | head -1)")"
test -x "$TESTBIN"
echo "   test binary: $TESTBIN"

echo "== 2/3 ensure the fedimint v0.11.1 daemons are built in $FEDI_WT =="
if [ ! -d "$FEDI_WT" ]; then
  echo "   ERROR: no worktree at $FEDI_WT — create it with:" >&2
  echo "     git -C ~/p/fedimint worktree add $FEDI_WT v0.11.1" >&2
  exit 1
fi
if [ ! -x "$FEDI_WT/target-nix/debug/devimint" ]; then
  echo "   building fedimint v0.11.1 workspace (one-time, heavy — devimint + fedimintd/gatewayd/cli)…"
  ( cd "$FEDI_WT" && nix develop --command cargo build )
fi

echo "== 3/3 run the live test under a fresh devimint dev-fed =="
cd "$FEDI_WT"
# devimint's --exec injects FM_INVITE_CODE + a live fedimint-cli alias onto PATH for the test binary.
exec nix develop --command bash -c \
  "export PATH=\"\$PWD/target-nix/debug:\$PATH\"; \
   devimint dev-fed --exec '$TESTBIN' --ignored --nocapture --test-threads=1"
