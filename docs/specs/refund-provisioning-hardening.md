# Spec: refund + provisioning hardening (codex full-review follow-up)

**Status:** **Implemented** (all on master) — F1 `8e411eb`, F3+F6 `5288ded`, F2 `206a98f`, F4 `6bfe0fe`, F5 `de10217`
**Source:** full-project `codex exec` review 2026-07-01 (`/tmp/codex-review-lnrent-full-20260701T163906Z/final-review.md`),
each finding independently verified against the code before writing this spec.

Closes six verified findings in the live money/provisioning path. This is a HARDENING pass, not a
redesign. It must not touch the already-hardened refund math or invariants.

## Scope / non-goals

- **Do NOT** modify the INV-1 fee-deduction math (`net_payout_sat`), INV-2 readiness accounting, or
  INV-3 provenance guard — codex explicitly cleared them. No changes to `capture`, the store CAS
  transactions, or the fedimint receive-index recovery.
- **No BOLT12.** Where a re-resolvable refund destination is required, that means LN-address or LNURL
  only; BOLT12 is a later bead.
- Keep changes minimal and tightly scoped (overengineering is a standing project risk). Each finding
  below is an independent, small change with its own test; prefer the smallest fix that closes it.
- Do not add new protocol message types, recipe/price policy knobs, refund-destination discovery, or
  a general filesystem-permission framework, or a blanket protocol-ID normalization layer.
  Client/test fixture changes are in scope only where they are necessary to satisfy the server-side
  hardening gates below.
- Preserve existing behavior for rows already persisted with `refund_dest = NULL` — they remain
  explicit manual liabilities (surfaced by `lnrent money`), never silently dropped or auto-failed
  differently than today.

---

## F1 [P0] Wire the real refund resolver into production (DI seam)

**Problem (verified).** `Supervisor::build` (daemon/src/supervisor.rs:190) constructs the refund executor
with `Refunder::new`, which hardcodes `PassThroughResolver` (daemon/src/refund.rs:199) — it returns the
stored destination verbatim. In the now-default fedimint build, `pay_refund_capped` then parses that
string as BOLT11. A buyer whose `refund_dest` is an **LN-address or LNURL** (the common case) gets a
refund that parks/fails instead of resolving. The real, SSRF-guarded `refund_resolver::Resolver`
(daemon/src/refund_resolver.rs:287, `Resolver::new()` — no args) is only ever wired via
`Refunder::with_resolver` **in tests**. The 1,485-line resolver is dead in production.

**Why it needs a seam, not a one-liner.** The e2e suite (daemon/tests/e2e_money_path.rs) deliberately
refunds to a fake `"lnaddr@buyer"` and asserts `payment.pay("lnaddr@buyer", …)` (lines 1731, 1774),
relying on passthrough. Those refunds run through `Supervisor::build`'s refunder. Swapping in the real
resolver unconditionally would make that harness attempt a real HTTP resolve and break.

**Requirement.**
1. Thread an `Arc<dyn RefundResolver>` into `Supervisor::build` (daemon/src/supervisor.rs:146) and use
   it for the internal `Refunder` via `Refunder::with_resolver` instead of `Refunder::new`.
2. Production caller (daemon/src/main.rs:231) passes `Arc::new(refund_resolver::Resolver::new())`.
3. Every existing test harness that builds a `Supervisor` passes
   `Arc::new(refund_resolver::PassThroughResolver)` (it is already `pub`): daemon
   supervisor/e2e tests and the buyer CLI integration tests. This preserves the existing
   `"lnaddr@buyer"` passthrough assertions unchanged.
4. Keep `Refunder::new` (passthrough) only if still used by focused refund unit tests; production must
   not reach it.

**Acceptance.**
- A Supervisor-level test proves the seam: inject a recording resolver that returns a known BOLT11 and
  assert the payment backend is asked to pay the resolved BOLT11, not the raw address. Do not depend on
  public/live network; production still uses `Resolver::new()`.
- Existing e2e + refund unit tests stay green (they inject passthrough / TestResolver).

---

## F2 [P0/P1] Harden Fedimint wallet + index file permissions

**Problem (verified).** Bootstrap forces a *created* data dir to `0700` and the seed / `fedimint.json`
to `0600`, but a **pre-existing** data dir is only rejected when writable/sticky
(`reject_unsafe_preexisting_dir`, daemon/src/config.rs) — a pre-existing **0755** (readable) dir is
accepted. The Fedimint backend then creates `fedimint/<federation>/client.db` (RocksDB) and
`lnrent_index.db` (sqlite) via `create_dir_all` + defaults with **no chmod** (daemon/src/fedimint_backend.rs:140,
and the RocksDB/sqlite opens that follow). On a pre-existing 0755 data dir, the ecash `client.db`
(note/wallet state — bearer material) is locally readable. The seed itself stays 0600, so this exposes
the note state, not the master secret.

**Requirement.**
1. In the Fedimint backend, before any RocksDB/sqlite open, prepare only the lnrent-owned Fedimint
   subtree: `data_dir/fedimint/`, `fedimint/<federation>/`, and `client.db/`. Refuse a symlink or
   non-directory at those three paths; create missing directories with `0700`; tighten existing
   directories to `0700` (or reject them if they cannot be safely tightened). Do this even when the
   parent data dir pre-existed as `0755`. Unlike the top-level data dir, these are app-private
   children, so making them `0700` is the confidentiality boundary that closes the finding.
2. Protect the lnrent-owned sqlite index main path before opening it: create a missing
   `lnrent_index.db` as `0600` with a no-follow/exclusive open, or preflight an existing one as a
   regular non-symlink file and tighten it to `0600`. Checking existing `-wal`/`-shm` sidecars is OK
   if it falls out of the helper, but it is not the core F2 requirement once the federation directory
   is `0700`.
3. Recursive/post-open chmod of RocksDB files is **optional defense in depth**, not an acceptance
   gate. If implemented, it must be best-effort and tolerate RocksDB churn (files appearing or
   disappearing); do not require lstat/refuse-symlink handling for every churned file as part of this
   hardening bead, and do not build a general permission framework.
4. Do not change the default (freshly-created `0700`) path's behavior.

**Acceptance.**
- A no-live-federation unit test for the factored Fedimint path-prep/hardening code starts with a
  pre-existing `0755` data dir and asserts the fedimint root, federation dir, and `client.db/` are
  `0700`, and the `lnrent_index.db` main file is `0600` and not a symlink.
- Symlinked `fedimint`, federation, `client.db`, or index main paths are refused before any DB open.
- Do not require a test that walks existing RocksDB leaf files or plants a symlink inside
  `client.db/`; that is only relevant if an implementation chooses the optional recursive harden.

---

## F3 [P1] Require a resolvable refund destination before issuing a payable invoice

**Problem (verified).** Order intake validates `refund_dest` only when present, stores `None`, and a
later failed provision / late settlement parks as `"no destination"` (manual liability). A buyer can
pay without one (`--refund-dest` is optional; daemon/src/order_intake.rs:120,247).

**Requirement.**
1. For every **new payable** `order.request`, require a present, non-empty, **re-resolvable**
   `refund_dest` (LN-address or HTTPS LNURL) BEFORE any reservation / invoice / subscription write.
   Do not gate this by recipe or price: dummy, do-vps, 1-sat/staging, and marketplace-sim orders can
   all fail after payment and therefore need a refund route.
2. Validate with the resolver's pure form helpers only (no network at order time). `detect_form` /
   `validate_dest_format` may be reused, but the durable-order gate must additionally require
   `DestForm::LnAddress` or `DestForm::Lnurl`; `DestForm::Bolt11` is rejected by F6.
3. On missing/invalid dest, send the existing structured `order.error` (for example the existing
   `refund_dest_invalid` / `params_invalid`, or a new `refund_dest_required` for the missing case)
   with no dangling PENDING subscription and no reservation held — mirror the current early-failure
   path.
4. Existing NULL-dest rows are untouched (still manual liabilities). Renewals have no new
   `refund_dest` field and use the subscription's stored dest; do not block legacy NULL-dest renewals
   in this hardening pass.
5. Update buyer/marketplace/CLI happy-path fixtures that expect an invoice to pass a static valid
   LN-address. The server-side gate remains authoritative; clients may still surface the remote error.

**Acceptance.**
- Order without `refund_dest` → structured error before any invoice is minted; no reservation leak,
  no PENDING sub.
- Order with a valid LN-address → succeeds as today, including dummy/do-vps and 1-sat/staging flows.
- A pre-existing NULL-dest row still parks as a manual liability (regression guard).

---

## F4 [P1] Constrain buyer ids + URL-encode DO tag lookups

**Problem (verified).** `OrderRequest.id` is an unconstrained `String` (wire/src/dm.rs:31) with no
validation anywhere; it flows into `order_id` / `subscription.id`
(daemon/src/order_intake.rs:100) and the DO recipe curls `?tag_name=sub:${sub_id}` **without URL
encoding** in provision/destroy/resume/suspend. A `req.id` like `x&per_page=1` (or containing `#`, `%`,
space) splits the query so destroy/resume look up a different tag → a billed droplet can leak. Tell:
provision builds a sanitized `clean` value (recipes/do-vps/provision:37) but uses the **raw** one for
the tag lookup.

**Requirement.**
1. Validate the buyer-chosen `OrderRequest.id` before constructing any order id, reservation id,
   subscription id, or payment external id from it. Use a bounded DO-tag-safe tail alphabet
   (`[A-Za-z0-9_-]`, length 1..=128). Exclude
   `.`, `%`, `&`, `#`, space, `/`, and `:` from the buyer tail; `:` is reserved for server prefixes
   (`ord:<buyer>:<tail>`, `sub:<id>`) and is within DO's tag charset. Do not apply this tail alphabet
   to `listing_id` (a Nostr coordinate) or to the full generated subscription id. Applying the same
   helper to `RenewRequest.id` / `OpRequest.id` is acceptable if it is a tiny reuse, but it is not
   required to close the DO tag leak.
   > **DRIFT-3 (production-readiness.md) — landed.** The "acceptable if tiny reuse" option was
   > exercised: the SAME `validate_buyer_request_id_tail` (length/charset only) now also gates
   > `RenewRequest.id` (drop + log, no reply) and `OpRequest.id` (pre-claim
   > `op.result invalid_request_id` reject). This is the bounded id-tail check applied for
   > cross-path consistency — NOT the blanket subscription-id shape validator point 2 refuses,
   > which remains refused.
2. Do **not** add a blanket subscription-id shape validator for `RenewRequest`, `SubCancel`,
   `DeliveryResendRequest`, or `OpRequest` as part of F4. Once new order ids have a safe tail and all
   DO tag lookups are URL-encoded, malformed subscription references can continue through the
   existing unknown-sub / non-owner / `op.result` paths; validating all four message types is not
   load-bearing for the leaked-VM finding and risks blocking legacy/manual-liability rows.
3. URL-encode `tag_name` in **every** DO script lookup: provision, destroy, resume, suspend,
   healthcheck, `ops/status`, and `ops/restart` (plus any test cleanup helper that curls by tag). Compute
   `tag="sub:${sub_id}"` once and use that same value for droplet creation and lookup, e.g.
   `curl --get --data-urlencode "tag_name=${tag}" "${API}/droplets"` or a `jq -rn --arg t … '@uri'`
   helper.

**Acceptance.**
- An `order.request` id outside the tail alphabet is rejected before it becomes an order id,
  reservation id, subscription id, or payment external id; no order/reservation is created.
- A script-level legacy/synthetic tag containing a URL-special character is looked up via an encoded
  query (documented shell assertion is enough); a normal safe id create → destroy uses the exact same
  `sub:${sub_id}` tag value.
- No acceptance test is required for delivery-resend or generic malformed subscription-id shape
  rejection; those paths are intentionally left to existing authorization/lookup behavior.

---

## F5 [P2] Cap renewal invoice expiry to the resumable window

**Problem (verified).** Renewals set
`invoice_expiry_s = (resumable_until - now).max(INVOICE_EXPIRY_S)` (daemon/src/order_intake.rs:345) —
a `.max`, so with e.g. 60s of window left it still mints a 3600s invoice; a payment after
`resumable_until` lands in refund instead of renewal.

**Requirement.** Cap, don't floor. After the existing `now < resumable_until` check, compute
`remaining = resumable_until - now`; if `remaining` is below a small floor (e.g. `< 60s`), refuse to
issue the renewal invoice. Otherwise use
`invoice_expiry_s = min(remaining, i64::from(INVOICE_EXPIRY_S)) as u32`. Do not add one: invoice
expiry is the exclusive/unpayable-at-or-after timestamp, and capture already refunds settlements at
`settled_at >= resumable_until`, so `expires_at == resumable_until` is acceptable but
`expires_at > resumable_until` is not.

**Acceptance.**
- Test: with a short remaining window, the renewal invoice `expires_at <= resumable_until` (or the
  request is refused), never past the terminal boundary; exact-floor remaining time is allowed.

---

## F6 [P2] Reject raw BOLT11 as a stored refund destination

**Problem (verified).** Order-time validation accepts raw BOLT11
(daemon/src/refund_resolver.rs:140), but the refunder treats it as an immutable generation-0 invoice.
For a subscription refund days later, that invoice is expired / amount-mismatched and cannot be
re-resolved → parks.

**Requirement.** For new durable/renewable orders (subscriptions), reject a raw BOLT11 as the stored
`refund_dest`; require LN-address / LNURL (per F3). Implement this in the same intake gate as F3, not
as a second resolver path. Keep the refunder's gen-0 BOLT11 pass-through behavior for focused tests
and any pre-existing rows that already contain a raw invoice. Document that BOLT12 is the future
re-resolvable single-string option.

**Acceptance.**
- A new order with a raw BOLT11 `refund_dest` → structured error (re-resolvable dest required).
- Comment/doc notes BOLT12 as the planned addition.

---

## Suggested implementation order (independent beads)

F1 and F3/F6 are related (both about the resolver/dest contract) — do F1 first (wires the resolver),
then F3+F6 (require a re-resolvable dest). F2, F4, F5 are independent and can land in any order. Each
finding is one small change + one test. Keep the diff minimal; do not refactor adjacent code.
