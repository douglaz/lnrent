# Spec: web (WASM) buyer — browser marketplace access (lnrent-7fp.18)

**Status:** draft for codex-review-loop → rb-lite
**Bead:** lnrent-7fp.18 (P1; blockers .13 CLI buyer + .15 ship gate are CLOSED). Grill-converged bead;
this spec turns it into an implementable plan grounded in the current code.

A static browser SPA that reuses `lnrent-buyer-core` compiled to wasm32 so the marketplace is usable
from a website with **no backend and no central server** — the site is a pure buyer front-end over
one configured Nostr relay + the buyer's own wallet. It is the **human** surface (ADR-0014);
agents use the buyer CLI. The SPA is static files only and exposes NO HTTP/agent API.

## Load-bearing constraint — reuse buyer-core, add only browser adapters

`clients/core` (`lnrent-buyer-core`) already exposes the whole protocol as
`BuyerClient<'a, R: Relay, S: NostrSigner, C: Clock>` with `discover_listings` / `get_listing` /
`list_ops` / `create_order` / `wait_provision` / `resend_delivery` / `renew` / `invoke_op` / `cancel`.
The CLI (`clients/cli`) is one host of that seam (native nostr-sdk Relay + `nostr::Keys` signer +
entropy Clock). The web buyer is a SECOND host of the SAME seam. **No protocol logic is
reimplemented** — the web crate provides only three browser adapters + a wasm-bindgen surface + a
static shell. If a change tempts you to add protocol logic to the web crate, it belongs in buyer-core.

## Scope

A new workspace crate **`clients/web`** (`lnrent-buyer-web`, `crate-type = ["cdylib"]`), built to
wasm32 in the Nix devshell (the flake already has `wasm32-unknown-unknown` + `wasm-pack` +
`wasm-bindgen-cli` + `wasm-opt` + `trunk` + the unwrapped-clang secp fix). It contains:

1. **Browser `Relay`** (`impl Relay`) over a single browser `WebSocket` to one configured relay URL
   (gloo-net or web-sys), speaking the Nostr relay wire protocol directly (`["EVENT",ev]` publish +
   `["OK",id,accepted,msg]`; `["REQ",sub,filter]` +
   `["EVENT",sub,ev]`/`["EOSE",sub]`/`["CLOSED",…]`; `["CLOSE",sub]`).
   `fetch_listings` = a bounded REQ for kind-30402 by `authors=[operator]`; `subscribe_giftwraps` = a
   live REQ for kind-1059 `#p=[recipient]` returning a `GiftWrapStream` whose `next()` yields until the
   caller's deadline. buyer-core still verifies/parses/decrypts every event — the Relay only moves JSON
   frames. Do NOT pull in nostr-sdk's relay pool (its wasm story is not guaranteed); a hand-rolled single
   WS client over the (small) wire protocol is the minimal, testable choice.
   - M1a relay model: exactly one configured relay URL (the static shell can expose one relay input; the
     wasm constructor should take `relay_url`). Multi-relay fan-in, relay selection, publish quorum, and
     event-id de-dupe across relays are OPTIONAL future work, not part of `lnrent-7fp.18`.
   - `publish` sends one `EVENT` and succeeds only on that relay's `OK true`; `OK false`, socket
     close, or timeout becomes `RelayError`.
   - `fetch_listings` is a snapshot: collect until the relay sends `EOSE` or the timeout elapses, then
     send `CLOSE` if the socket/subscription is still open.
   - `subscribe_giftwraps` is snapshot-then-live: `EOSE` only ends the initial replay phase for that
     subscription and MUST NOT end the stream. Keep yielding later live `EVENT`s until the fixed deadline
     computed at subscribe time; `GiftWrapStream::next()` returns `Ok(None)` when that deadline elapses
     or the single relay subscription/socket closes.
   - Subscribe-before-publish matters: do not return from `subscribe_giftwraps` until the relay socket
     is open and the REQ has been sent, plus a short registration-settle delay (mirror the
     CLI's 500ms) so a fast operator reply is not missed. Do not implement auto-reconnect/re-REQ for
     M1a; a disconnect surfaces as timeout/transport failure. Once an invoice/order id has been shown,
     the UI keeps that id visible so the buyer can retry `wait_provision` or `resend_delivery`.

2. **Signer with capability degradation** (`impl NostrSigner`):
   - **NIP-07** preferred — a bridge over `window.nostr` (`getPublicKey`, `signEvent`,
     `nip44.encrypt`/`nip44.decrypt`). buyer-core uses `NostrSigner` for gift-wrap seal/unwrap (NIP-44),
     so the bridge must implement `get_public_key`, `sign_event`, `nip44_encrypt`, and
     `nip44_decrypt` against the extension; the trait-required `nip04_*` methods can return
     `unsupported` because buyer-core must not call them. If `window.nostr` lacks nip44, treat NIP-07
     as unavailable and fall back.
   - **Embedded key fallback** — the SPA generates a `nostr::Keys`, keeps the nsec in memory /
     `sessionStorage` for the current tab session, and immediately prompts the user to export/back it up
     (it decrypts delivered credentials). Do NOT persist a raw nsec in `localStorage`. "Remember this
     browser" persistence is OPTIONAL future work, not part of `lnrent-7fp.18`; if it is later added,
     store only a passphrase-encrypted WebCrypto blob in IndexedDB and require unlock on reload. No key
     material is ever in the static bundle. Clearly label which mode is active.
   - **wasm threading/bounds trap** — JS handles (`WebSocket`, `window.nostr`, callbacks) are not
     `Send`. Do not force them through `Arc<Mutex<_>>` or unsafe cross-thread shims. Run the web surface
     on the single browser thread with `wasm-bindgen-futures` / `spawn_local`; keep `Relay`/`Clock`/
     NIP-07 signer structs config-only or stateless where the traits still require `Send + Sync`.
     Before the browser relay stream, make the minimal shared buyer-core cfg tweak needed for wasm:
     `Relay`/`GiftWrapStream` async-trait futures use `?Send` on `wasm32`, and the
     `GiftWrapStream: Send` supertrait is native-only. Native builds keep the current `Send`
     futures/bounds.

3. **Browser `Clock`** (`impl Clock`): `now_secs` = `Date.now()/1000`; `new_request_id` = real browser
   entropy (`crypto.getRandomValues` via getrandom's `js` feature, or `crypto.randomUUID`) — MUST be
   unique (the operator dedupes on `(sender,id)`), and the id tail must satisfy the server's
   `[A-Za-z0-9_-]` 1..=128 gate (spec F4). Use e.g. `req-` + 16 random bytes hex/base58url; a raw
   `crypto.randomUUID()` also passes because `-` is allowed, but do not add `:`/`.`/`%`/`&`/`#`/space
   or `/`.

4. **wasm-bindgen surface** (`#[wasm_bindgen]`): a small `WebBuyer` object that JS constructs with
   `{relay_url, operator_npub, signer_mode}` and whose async methods (`discover`, `create_order`,
   `wait_provision`, `renew`, `invoke_op`, `cancel`, `list_ops`) construct a `BuyerClient` over the
   adapters and return **JSON** (`serde_wasm_bindgen`/`JsValue`) — thin marshalling only. The web
   `create_order` API takes a required `refund_dest: string` and passes `Some(refund_dest)` to
   buyer-core; do not expose a nullable/omitted order path in the SPA.

5. **Static shell** — `clients/web/static/index.html` + a small vanilla `app.js` + minimal CSS (NO SPA
   framework — smallest bundle, most static, easiest to headless-test). Built by `wasm-pack build
   --target web` into `pkg/`, imported by `index.html` as ES modules. Views: (a) listings, (b) order
   form (recipe params, incl. a **required re-resolvable refund_dest** field per spec F3 — LN-address /
   HTTPS LNURL, not bolt11/bolt12, else the order is rejected server-side), (c) invoice view, (d)
   credentials view, (e) a `request`-kind **ops** view (list declared ops + invoke + show
   `op.result.data`).

6. **Invoice hand-off (SPA NEVER pays)** — `create_order` returns a bolt11; present it to the user's
   wallet: prefer `window.webln` (`enable()` + `sendPayment(bolt11)`), else show the bolt11 with a
   copy button **and** render a payment QR. QR rendering stays required for M1a because the bead's
   no-WebLN fallback is a phone-wallet hand-off; keep it minimal (one tiny dependency or local encoder,
   no scanner/deep-link/payment-status feature). The SPA holds no wallet credentials and runs no payer
   (ADR-0014, §4.7). After WebLN `sendPayment` resolves, call `wait_provision` to await
   `provision.ready` and show credentials. On the QR/copy path, show an "I've paid / wait for
   credentials" action; if a wait times out before the user pays or before delivery arrives, keep the
   order id visible and let the user retry `wait_provision` or request `resend_delivery(order_id)`. The
   SPA never checks or settles payment itself.

## Non-goals

Operator/seller UI (operator stays daemon/CLI/skills); ANY hosted backend or HTTP/agent API (must
stay static — agents use the CLI, ADR-0014); an embedded custodial wallet or an autonomous payer;
multi-relay fan-in/selection/quorum; automatic relay reconnect/re-REQ; "remember this browser"
IndexedDB/WebCrypto persistence; QR scanner/deep-link/payment-status features beyond rendering the
bolt11 QR; the VM access/reachability plane; `interactive`-kind ops (Iroh shell/logs, §9.2) — the
buyer-core seam exists but the web shell wires no Iroh transport. No new protocol messages or
buyer-core changes beyond what the adapters strictly need (if buyer-core needs a wasm cfg tweak, keep it
minimal + shared).

## Capability degradation (the grill requirement, must hold)

Detect + degrade at runtime, and a buyer with NEITHER a Nostr extension NOR WebLN — just a phone
Lightning wallet — MUST still complete the loop:
- signing: NIP-07 → else embedded key (memory/`sessionStorage` + export-prompt; no remember-browser
  persistence in M1a);
- invoice: WebLN → else copy-bolt11 + QR.
Either way the user's own wallet pays; the SPA never does. Surface the active modes in the UI.

## Security

- No secrets in the static bundle. Prefer NIP-07 because the page never sees the private key. Embedded
  key mode is explicitly weaker: raw nsec is memory/`sessionStorage` only for the current tab/session
  and export-prompted. M1a does not implement "remember this browser"; any future optional persistence
  must be a passphrase-encrypted IndexedDB/WebCrypto blob — never raw `localStorage`.
- Delivered credentials are decrypted client-side (NIP-44) and shown only in the creds view; never
  logged, never sent anywhere, and never written back to browser storage.
- The browser Relay connects only to the buyer-configured relay URL; no other network egress.
- Keep the static shell XSS-hostile: no third-party scripts, no `innerHTML` for relay/operator content,
  and when served over HTTP ship/advise a restrictive CSP (`default-src 'self'`, `connect-src` limited
  to the configured relay URL, no inline script except whatever wasm bootstrap strictly requires).

## Testing / verification (acceptance)

A **headless-browser e2e** (this box: system `google-chrome` + swiftshader, driven via CDP with node's
global WebSocket — gstack `/browse` is broken here) drives the SPA through the FULL loop against a
local stack: the daemon/supervisor components on the **mock** payment backend + a **host/dummy** recipe
(instant creds, no DO cost, 1-sat) + a local relay (`MockRelay::run()` in-process, or the
`clients/cli/examples/localrelay` process if the harness is cross-process). The test injects a
**mocked `window.webln`** that only records the bolt11 and notifies the host test harness; the page does
not and cannot settle anything. The host-side harness must own a `MockPayment` handle (as the existing
CLI buyer tests do) and, after observing the WebLN call / copy hand-off, call
`MockPayment::settle("order:{buyer_hex}:{request_id}", now)` out-of-band so the SPA's
`wait_provision` resolves. Do not add a shipped daemon API, browser API, or payment backend endpoint
just for this test. It asserts:
- SPA loads as static files served by a throwaway local static server (do not rely on `file://`;
  wasm-pack's module/wasm fetch path is browser-hostile there), connects to the relay over a browser
  WebSocket;
- browse a listing → `order.request` → invoice presented + handed to the mocked WebLN → `provision.ready`
  → credentials displayed;
- the **no-extension/no-WebLN** path completes via embedded key + copy-bolt11 + QR hand-off (assert the
  copy control and QR are present; settle out-of-band);
- a `request`-kind op (e.g. `status`) on the active subscription runs and `op.result.data` is shown;
- buyer-core is shared with the CLI — grep/build proof that no protocol logic is duplicated in
  `clients/web`.

Keep a fast non-browser unit layer too: the browser `Relay` (frame encode/decode) and the Clock/id
generator are unit-testable on native (behind a trait) without a browser.

## Acceptance criteria (from the bead)

- Static SPA (no backend) connects to the configured relay over a browser WebSocket.
- Headless e2e completes the full buyer loop with a MOCKED `window.webln` settling out-of-band.
- Signing via NIP-07 with an embedded-key fallback (export-prompted); invoice via WebLN else
  copy-bolt11/QR; SPA holds no wallet creds + runs no payer.
- A buyer with neither NIP-07 nor WebLN completes the loop (embedded key + copy/QR).
- A `request`-kind op runs and displays `op.result.data`.
- No duplicated protocol logic — buyer-core is the single implementation.

## Suggested build order (rb-lite beads)

1. Minimal buyer-core wasm seam tweak for non-`Send` browser futures/streams, with native behavior
   unchanged.
2. `clients/web` crate scaffold + wasm-bindgen `WebBuyer` skeleton + the browser `Clock` (native-unit-
   testable) — builds to wasm32 in the devshell.
3. Signer: embedded-key first (simplest, deterministic to test), then the NIP-07 bridge.
4. Single-relay browser `Relay` over WebSocket (wire-frame encode/decode unit-tested on native; the WS
   glue is thin).
5. Static shell (index.html + app.js) wiring the views + WebLN/copy+QR hand-off.
6. Headless-browser e2e against the local mock stack (the acceptance gate).

Keep each bead minimal; the whole feature is browser adapters + a static shell over buyer-core, with
only the cfg-wasm seam tweak above if needed for non-`Send` browser streams.
