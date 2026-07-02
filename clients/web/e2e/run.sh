#!/usr/bin/env bash
# Orchestrates the web-buyer headless e2e (lnrent-7fp.18 step 5b). Builds the wasm bundle, spawns a
# mock lnrentd + local relay + a static file server + headless Chrome, then runs the CDP driver which
# walks the SPA through the full loop and settles the mock invoice out-of-band via `lnrent dev settle`.
# Run inside the nix devshell:  nix develop . --command bash clients/web/e2e/run.sh
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."   # repo root
ROOT="$PWD"
WORK="$(mktemp -d)"; PORT="${PORT:-8137}"; DBG="${DBG:-9333}"
DAEMON=""; RELAY_PID=""; SRV=""; CHR=""
cleanup() {
  for p in "$DAEMON" "$RELAY_PID" "$SRV" "$CHR"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  rm -rf "$WORK" "$ROOT/clients/web/static/pkg"
}
trap cleanup EXIT
say() { echo "== $*"; }

say "build wasm bundle -> static/pkg"
wasm-pack build --target web --out-dir static/pkg clients/web >/dev/null 2>&1 || { echo "wasm-pack failed"; exit 1; }

say "build daemon + operator CLI + local relay"
cargo build -q -p lnrentd --bin lnrentd --bin lnrent 2>/dev/null
cargo build -q -p lnrent-buyer-cli --example localrelay 2>/dev/null

# M1a serves a SINGLE recipe; isolate `dummy` (no params, host backend, instant echo-creds) so the
# daemon publishes it (not do-vps, which needs params + a real DO token).
RECIPES="$WORK/recipes"; mkdir -p "$RECIPES/dummy"; cp -r "$ROOT/recipes/dummy/." "$RECIPES/dummy/"

say "start local relay"
./target/debug/examples/localrelay >"$WORK/relay" 2>/dev/null & RELAY_PID=$!
for _ in $(seq 1 20); do [ -s "$WORK/relay" ] && break; sleep 0.5; done
RELAY="$(cat "$WORK/relay")"; echo "   relay: $RELAY"

say "start mock lnrentd (host + dummy recipe + LNRENT_DEV=1)"
RUST_LOG=lnrentd=info NO_COLOR=1 LNRENT_DEV=1 \
  LNRENT_DATA_DIR="$WORK/data" LNRENT_COMPUTE_BACKEND=host \
  LNRENT_RECIPES_DIR="$RECIPES" LNRENT_RELAYS="$RELAY" \
  LNRENT_MNEMONIC="legal winner thank year wave sausage worth useful legal winner thank yellow" \
  ./target/debug/lnrentd >"$WORK/daemon.log" 2>&1 & DAEMON=$!
for _ in $(seq 1 60); do grep -q 'ipc serving' "$WORK/daemon.log" && break; sleep 1; done
NPUB="$(grep -oaE 'npub1[a-z0-9]{50,}' "$WORK/daemon.log" | head -1)"
echo "   operator npub: ${NPUB:0:24}…"
grep -q 'published' "$WORK/daemon.log" || { echo "daemon did not publish a listing"; tail -20 "$WORK/daemon.log"; exit 1; }

say "serve static/ (with pkg)"
python3 -m http.server "$PORT" --directory clients/web/static >/dev/null 2>&1 & SRV=$!

say "start headless chrome"
google-chrome --headless=new --no-sandbox --disable-gpu --use-gl=swiftshader \
  --remote-debugging-port="$DBG" about:blank >/dev/null 2>&1 & CHR=$!
sleep 4

say "drive the SPA (CDP)"
LNRENT_BIN="$ROOT/target/debug/lnrent" LNRENT_DATA_DIR="$WORK/data" \
  PAGEURL="http://127.0.0.1:$PORT/index.html" CDP="http://127.0.0.1:$DBG" \
  RELAY="$RELAY" NPUB="$NPUB" \
  node "$ROOT/clients/web/e2e/web-buyer-e2e.mjs"
RC=$?
echo "E2E_EXIT=$RC"
[ "$RC" -ne 0 ] && { echo "--- daemon.log tail ---"; tail -25 "$WORK/daemon.log"; }
exit "$RC"
