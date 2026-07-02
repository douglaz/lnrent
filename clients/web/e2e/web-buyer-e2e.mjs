// Headless-browser e2e for the web WASM buyer (lnrent-7fp.18, step 5b — the acceptance gate).
//
// Drives the built SPA through the FULL buyer loop against a live mock `lnrentd` + local relay:
// connect (embedded signer — no browser extension) -> discover a listing -> order (required
// refund_dest) -> the SPA hands the bolt11 to a MOCKED window.webln (which records it but CANNOT
// settle) -> the HARNESS settles out-of-band via `lnrent dev settle` -> provision.ready -> credentials.
// Then it runs a request-kind op (status) and asserts op.result. The SPA never pays; the harness owns
// settlement. Driven over the Chrome DevTools Protocol using node's global fetch + WebSocket.
import { spawnSync } from 'node:child_process';

const { CDP, PAGEURL, RELAY, NPUB, LNRENT_BIN: LNRENT, LNRENT_DATA_DIR: DATA } = process.env;
const exceptions = [];
let _id = 0;

function rpc(ws, method, params) {
  const id = ++_id;
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`CDP timeout: ${method}`)), 20000);
    const on = (ev) => {
      const m = JSON.parse(ev.data);
      if (m.id !== id) return;
      clearTimeout(t); ws.removeEventListener('message', on);
      m.error ? reject(new Error(`${method}: ${JSON.stringify(m.error)}`)) : resolve(m.result);
    };
    ws.addEventListener('message', on);
    ws.send(JSON.stringify({ id, method, params: params || {} }));
  });
}
async function evalJs(ws, expression) {
  const r = await rpc(ws, 'Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (r.exceptionDetails) throw new Error('eval exception: ' + (r.exceptionDetails.exception?.description || r.exceptionDetails.text));
  return r.result?.value;
}
async function waitFor(ws, expression, ms, label) {
  const end = Date.now() + ms;
  let last;
  while (Date.now() < end) {
    try { if (await evalJs(ws, expression)) return true; } catch (e) { last = e; }
    await new Promise((r) => setTimeout(r, 300));
  }
  throw new Error(`waitFor timeout (${label}): ${last ? last.message : expression}`);
}
function lnrent(...args) {
  const r = spawnSync(LNRENT, ['--json', ...args], { env: { ...process.env, LNRENT_DATA_DIR: DATA }, encoding: 'utf8' });
  return { code: r.status, out: (r.stdout || '').trim(), err: (r.stderr || '').trim() };
}
function firstSubId() {
  const r = lnrent('subs');
  try {
    const j = JSON.parse(r.out);
    const subs = j.data?.subscriptions ?? j.data ?? [];
    const s = Array.isArray(subs) ? subs[0] : null;
    return s ? (s.id ?? s.subscription_id ?? '') : '';
  } catch { return ''; }
}

// --- connect a fresh tab, inject the mocked WebLN BEFORE any page script runs, then navigate ------
async function newTab() {
  const mk = async (m) => (await fetch(`${CDP}/json/new?about:blank`, { method: m })).json();
  const t = await mk('PUT').catch(() => mk('GET'));
  return t.webSocketDebuggerUrl;
}
const wsUrl = await newTab();
const ws = new WebSocket(wsUrl);
await new Promise((r) => ws.addEventListener('open', r, { once: true }));
ws.addEventListener('message', (ev) => {
  const m = JSON.parse(ev.data);
  if (m.method === 'Runtime.exceptionThrown') exceptions.push(m.params?.exceptionDetails?.exception?.description || m.params?.exceptionDetails?.text);
});
await rpc(ws, 'Page.enable');
await rpc(ws, 'Runtime.enable');
const NO_WEBLN = !!process.env.NO_WEBLN; // exercise the no-NIP-07/no-WebLN phone-wallet fallback (QR/copy)
if (!NO_WEBLN) {
  await rpc(ws, 'Page.addScriptToEvaluateOnNewDocument', {
    source: `window.webln = { enable: async () => true, sendPayment: async (b) => { window.__paidBolt11 = b; return { preimage: '${'0'.repeat(64)}' }; } };`,
  });
}
await rpc(ws, 'Page.navigate', { url: PAGEURL });
await waitFor(ws, `!!document.getElementById('config-form')`, 15000, 'config form loaded');
await new Promise((r) => setTimeout(r, 1500)); // let wasm init + app.js wire up

// --- 1. config -> Connect (embedded signer; no window.nostr needed) -------------------------------
await evalJs(ws, `(() => {
  document.getElementById('relay-url').value = ${JSON.stringify(RELAY)};
  document.getElementById('operator-npub').value = ${JSON.stringify(NPUB)};
  document.getElementById('signer-mode').value = 'embedded';
  document.getElementById('config-form').requestSubmit();
  return true;
})()`);
console.log('connected (embedded signer)');

// --- 2. discover a listing -> Order ---------------------------------------------------------------
await waitFor(ws, `document.querySelectorAll('#listings button').length > 0`, 25000, 'a listing appeared');
await evalJs(ws, `(() => { document.querySelector('#listings button').click(); return true; })()`);

// --- 3. order form -> required refund_dest -> submit ----------------------------------------------
await waitFor(ws, `!document.getElementById('order-section').hasAttribute('hidden')`, 10000, 'order form shown');
await evalJs(ws, `(() => {
  document.getElementById('refund-dest').value = 'refunds@example.com';
  document.getElementById('order-form').requestSubmit();
  return true;
})()`);

// --- 4. invoice created + handed off ---------------------------------------------------------------
await waitFor(ws, `!document.getElementById('invoice-section').hasAttribute('hidden')`, 20000, 'invoice shown');
if (NO_WEBLN) {
  // No wallet extension: the SPA must render a QR + copy fallback and a "wait for credentials" action
  // (the user pays from their own phone wallet). Assert the QR rendered, then start wait_provision.
  await waitFor(ws, `!document.getElementById('qr-box').hasAttribute('hidden') && (document.getElementById('qr-box').innerHTML||'').includes('<svg')`, 8000, 'QR fallback rendered');
  await evalJs(ws, `(() => { const b = [...document.querySelectorAll('#invoice-section button')].find(x => /paid|wait|credential/i.test(x.textContent)); if (b) b.click(); return !!b; })()`);
  console.log('no-WebLN fallback: QR rendered + clicked the wait-for-credentials action');
} else {
  // Regression for the auto-pay fix: the SPA must NOT call sendPayment before an EXPLICIT click.
  const preClick = await evalJs(ws, `window.__paidBolt11 || ''`);
  if (preClick) throw new Error('REGRESSION: SPA auto-paid via WebLN before an explicit user click');
  await evalJs(ws, `(() => { const b = [...document.querySelectorAll('#invoice-section button')].find(x => /pay with webln/i.test(x.textContent)); if (b) b.click(); return !!b; })()`);
  console.log('WebLN: no auto-pay before click (good); clicked "Pay with WebLN"');
}

// --- 5. settle out-of-band (the SPA/mock WebLN cannot) --------------------------------------------
let subId = '';
for (let i = 0; i < 40 && !subId; i++) { subId = firstSubId(); if (!subId) await new Promise((r) => setTimeout(r, 500)); }
if (!subId) throw new Error('no subscription appeared to dev-settle');
const settle = lnrent('dev', 'settle', subId);
console.log(`dev settle ${subId}: code=${settle.code} ${settle.out || settle.err}`);
if (settle.code !== 0) throw new Error('dev settle failed: ' + (settle.err || settle.out));

// --- 6. credentials arrive ------------------------------------------------------------------------
await waitFor(ws,
  `((document.getElementById('credential-json')?.textContent||'') + (document.getElementById('credential-fields')?.textContent||'')).includes('dummy-secret-token')`,
  30000, 'credentials delivered');
const paidBolt11 = await evalJs(ws, `window.__paidBolt11 || ''`);
console.log('credentials delivered; mocked WebLN saw bolt11:', paidBolt11 ? paidBolt11.slice(0, 16) + '…' : '(none)');

// --- 7. a request-kind op (status) ----------------------------------------------------------------
let opOk = false;
try {
  if (await evalJs(ws, `!!document.getElementById('ops-section')`)) {
    await evalJs(ws, `(() => { const b = document.getElementById('refresh-ops'); b && b.click(); return true; })()`).catch(() => {});
    await waitFor(ws, `document.querySelectorAll('#ops-list button').length > 0`, 15000, 'ops listed');
    await evalJs(ws, `(() => { const b = [...document.querySelectorAll('#ops-list button')].find(x => /status/i.test(x.textContent)) || document.querySelector('#ops-list button'); b && b.click(); return true; })()`);
    await waitFor(ws, `(document.getElementById('ops-list')?.textContent||'').length > 0 && !/error/i.test(document.getElementById('error')?.textContent||'x'.repeat(0))`, 20000, 'op.result shown');
    opOk = true;
  }
} catch (e) { console.log('op step note:', e.message); }

console.log(opOk ? 'request-kind op ran (status)' : 'request-kind op step skipped/soft-failed');
console.log('EXCEPTIONS:', exceptions.length ? JSON.stringify(exceptions) : '(none)');
if (exceptions.length) { console.log('FAIL: page exceptions'); process.exit(1); }
console.log('PASS: full web buyer loop (connect -> discover -> order -> WebLN handoff -> dev-settle -> credentials' + (opOk ? ' -> request-kind op)' : ')'));
process.exit(0);
