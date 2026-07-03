# Spec: GATE-0 abuse resistance (PR-1/PR-2/PR-3/PR-4 + DRIFT-3)

**Status:** draft for codex-review-loop → rb-lite
**Source:** docs/specs/production-readiness.md GATE-0 (verified findings, 2026-07-03). This is the
cluster that gates unattended/scaled public exposure (attended-dogfood carve-out in the roadmap
legend). Four bounded changes + one tiny validator extension. No reputation system, no deposits,
no PoW — those are ADR-0011 / M-later.

## Problem (verified)

The operator `#p` tag is public and `gift_unwrap` authenticates *any* sender (an attacker signs the
seal with a free throwaway key), so every business handler is reachable by an unauthenticated
stranger. The only inbound bound is `MAX_INBOUND_CONCURRENCY = 32` (nostr_engine.rs:104) — a
concurrency cap, not a volume cap. Replay dedup is strong but only stops *duplicate* work; distinct
`request_id`s bypass it. Concretely:

- `reserve()` (reservation.rs:242-278) counts every unexpired HELD reservation against the host
  budget with no per-pubkey cap; the unpaid-hold TTL equals the 1h order-invoice expiry
  (`INVOICE_EXPIRY_S`, order_intake.rs:40). One keypair can hold the last slot, let it expire,
  re-order — keeping a small host unsellable at zero cost.
- Each fresh `(sender, request_id)` costs a signature verify + NIP-44 decrypt + several DB writes +
  a real backend `create_invoice` (order_intake.rs:181) before any payment.
- `op_dispatch::dispatch` claims the durable `RUNNING` `op_invocation` row (op_dispatch.rs:96)
  BEFORE the owner/ACTIVE auth checks (op_dispatch.rs:152-160): a stranger with no subscription
  persists a 120-day-retained row and receives a signed `op.result{unauthorized}` reply.
- `params` has no size/key-count cap (`validate_params`, reservation.rs:43-65) and is stored
  verbatim (order_intake.rs:756); there is no engine-level message-size bound before JSON decode.
- `renew.request.id` and `op.request.id` are not charset/length-validated
  (`validate_buyer_request_id_tail` is applied only to the order path, order_intake.rs:99/978).

## Design

### A. PR-1 — per-pubkey cap on outstanding unpaid HELD reservations

In `reservation::reserve()`, before the budget check, count the sender's LIVE unpaid holds and
refuse above a cap:

- The sender is already embedded in the order id (`ord:{sender_hex}:{request_id}`,
  order_intake.rs:105), so the count keys on the sender prefix. It must count a hold as live using
  the SAME liveness rule `reserve()` already uses for the budget (a paid/PROVISIONING hold stays
  live PAST its original TTL — the reservation model treats an in-flight paid order as occupying
  the slot regardless of `expires_at`). Do NOT filter on `expires_at > now` alone (that would drop
  a paid-but-provisioning hold and let one pubkey exceed the cap with paid in-flight orders).
  Reuse the exact predicate `live_usage`/`reserve()` uses to decide a hold is live, restricted to
  the sender AND **excluding the order_id currently being reserved** (exactly as `live_usage`
  already excludes its own `oid`, reservation.rs:107): `... WHERE order_id LIKE 'ord:' || ?sender
  || ':%' AND order_id <> ?this_order_id AND <same live-HELD predicate as the budget count>`.
  Excluding self is REQUIRED for idempotent re-reserve: a crash after `reserve()` but before the
  cached `inbound_request`/invoice commit leaves a live HELD row; the buyer's retry of the SAME
  request must continue the original order, not be counted against itself and rejected
  `capacity_full`. `sender_hex` is lowercase hex (no LIKE wildcards); assert/normalize before
  building the pattern. Stale/crash-left expired-and-never-paid rows are excluded by the live
  predicate, so they don't eat the cap.
- Over the cap → the existing `capacity_full` order error (retryable), NOT a new error code — the
  buyer-visible contract is unchanged and leaks nothing about the cap.
- The cap is operator-tunable via config (`max_unpaid_holds_per_buyer`, default **2**), plumbed the
  same way existing operator config reaches the reservation path. `0` disables ordering for
  everyone; do not special-case it — document that.
- The cap counts only **HELD** rows (a paid order's hold stays HELD through PROVISIONING — that is
  a *paying* buyer; still counted, which is correct: a buyer mid-provision has no reason to open
  more than `cap` parallel unpaid orders).

**Invariant to preserve (review-verified):** the hold TTL stays COUPLED to (≥) the order-invoice
expiry. Capture gates only on `invoice.expires_at`, so a hold released while its invoice is still
payable lets a settle-after-release land on a slot another order has reserved (oversell). Do NOT
shorten the hold TTL alone. (Shortening the unpaid order-invoice expiry itself — which shortens
both together — is allowed but NOT in scope here; keep 1h.)

### B. PR-2 — per-pubkey inbound token bucket

A small in-memory token bucket keyed by sender pubkey, applied in `process_inbound` AFTER
`gift_unwrap` reveals the authenticated seal sender and BEFORE the business handler dispatch (the
verify/decrypt cost is unavoidable — the sender is only known after decrypt; the bucket bounds the
expensive part: handlers, DB writes, invoice creation, signed replies).

- Shape: `capacity` tokens, `refill_per_min` (config: `inbound_rate_capacity` default **10**,
  `inbound_rate_refill_per_min` default **6**). One token per accepted wrap. In-memory only
  (`HashMap<PublicKey, Bucket>` behind the existing engine state), bounded to the N most recently
  seen senders (LRU, cap ~4096 like the negative cache) — a restart resets buckets, which is fine.
- On empty bucket: **drop the wrap without writing `seen_message`** and without a reply, log at
  `debug` with the sender. Not-seen means relay redelivery/backfill retries it later — polite
  backpressure, no permanent loss for a bursty-but-honest buyer. The bounded negative cache is NOT
  used for rate-dropped wraps (they are valid messages, not garbage).
- The operator's own pubkey (self-DMs, PR-5 alerts later) is exempt.
- Do NOT rate-limit by outer wrap identity (ephemeral keys make that free to rotate); only the seal
  sender counts.

### C. PR-3 — authorize the op.request reject path before the durable claim

Split `claim()`'s insert-then-classify into validate-id → lookup → auth → claim:

0. **Validate the id** (`validate_buyer_request_id_tail`, per §D/DRIFT-3): a malformed
   `op.request.id` is a structural reject — return `invalid_request_id` with NO row (you cannot
   form the `op_invocation` PK from it). This precedes the lookup and is distinct from the
   authorized-op `invalid_params` below.
1. **Lookup existing** (read-only): if a row exists for `(sender, request_id)` → classify
   Done/Errored/Running exactly as today and resend/defer. This preserves today's cached-resend
   semantics for retries even if the subscription's state has since changed — a previously
   authorized op whose sub later left ACTIVE must still get its cached `op.result`, not a fresh
   `not_active`.
2. **No row** → run the AUTH gate only (load_subscription → owner → ACTIVE) WITHOUT inserting
   anything. An unauthorized / unknown-sub / not-ACTIVE request gets the same
   `op.result{unauthorized}`/`not_active` reply as today but persists **nothing**. (The reply
   itself remains — silence would leak nothing more and would break legitimate buyers with a stale
   sub id; PR-2's bucket bounds the reply amplification.) **Op-resolution (`unknown_op`) and param
   validation (`invalid_params`) are NOT part of this row-free gate** — they run only AFTER the
   claim, on the authorized path (step 3 / step 4), and DO commit a terminal ERROR row for
   cached-resend idempotency, exactly as today. Only the three auth rejects are row-free.
3. **Authorized** → insert the RUNNING claim. **The auth read and the claim must be one serialized
   store transaction (or the ACTIVE check must be re-run INSIDE the claim txn)** — otherwise a sub
   can pass the ACTIVE read at step 2 and be suspended/terminated before the claim inserts, letting
   the hook run on a no-longer-ACTIVE sub (a TOCTOU the current single-txn claim avoids by reading
   state and inserting together). Concretely: fold `load_subscription` + owner/ACTIVE gate into the
   same `store.transaction` that does the `INSERT ... ON CONFLICT DO NOTHING` + classify, so the
   authorization and the durable claim commit atomically. The reject paths (no row / not-owner /
   not-ACTIVE) return from inside that txn WITHOUT inserting. The concurrent-duplicate race (two
   fresh authorized requests) is still handled by the conflict-classify: the loser sees
   Running/Done and follows today's paths. (The hook still runs OUTSIDE any txn, after the claim
   commits — unchanged.)
   **Row-free applies ONLY to the auth rejects** (unknown sub / not-owner / not-ACTIVE). Once a
   sender is authorized, `unknown_op` and `invalid_params` still commit a terminal ERROR row as
   today (step 4) — they are past auth, deterministic, and need the cached-resend so a retry gets
   the same error without re-evaluating. Do not make those row-free.
4. `unknown_op` / `invalid_params` for an AUTHORIZED sender still commit a terminal ERROR row as
   today (cached, idempotent). Only the *auth* rejects (unauthorized, not_active, unknown sub)
   become row-free: they are deterministic on re-delivery and need no cache. Document that a buyer
   whose sub goes ACTIVE between retries will then get a fresh claim — correct behavior.

### D. PR-4 + DRIFT-3 — input size caps and id validation parity

- **Engine message-size bound:** the cap must sit where the raw bytes still exist. By the time
  `process_inbound` has a decoded `Msg`, `gift_unwrap` has ALREADY JSON-decoded the rumor content
  (verified). So enforce the bound INSIDE the unwrap path — in `wire/`'s gift-unwrap (or a thin
  pre-check on `gift.rumor.content` length before `serde_json::from_str`) — rejecting rumor content
  larger than `MAX_INBOUND_CONTENT_BYTES = 64 * 1024` (matches the resolver's body cap; generous
  for every legitimate message type). **Handling of an over-cap wrap mirrors the existing
  undecodable-wrap path exactly** (verified: that path uses the bounded NEGATIVE CACHE, not a
  `seen_message` write) — so an over-cap wrap goes to the negative cache and is NOT written to
  `seen_message`. (The earlier draft's "seen_message write" was wrong; match the real
  undecodable-wrap disposition.)
- **params caps** in `reservation::validate_params`: reject params whose serialized JSON exceeds
  **8 KiB** or with more than **32 top-level keys**, with the existing `params_invalid` error.
  Applied at order intake (the only path that stores params).
- **DRIFT-3:** apply `validate_buyer_request_id_tail` (order_intake.rs:978, `[A-Za-z0-9_-]`,
  len 1..=128) to `renew.request.id` (handle_renew, before building
  `external_id = renew:req:<sender>:<id>`) and `op.request.id`. A malformed id is a **structural
  pre-lookup reject**, NOT the post-auth `invalid_params` param-schema check of §C: you cannot form
  a safe idempotency key / `op_invocation` PK from a malformed id, so it is rejected BEFORE §C's
  lookup and is row-free by construction (there is nothing valid to key a row on). This is
  consistent with §C — §C's row-free rule is about auth rejects on a *well-formed* request; a
  malformed id never reaches the auth gate at all. Add this id-validation as the explicit first step
  of §C ("0. validate the id; malformed → structured error, no row") and use a distinct reason
  (e.g. `invalid_request_id`) so it is not conflated with authorized-op `invalid_params`. **For
  `renew.request` there is NO error-response variant in `wire::Msg`** (only `billing.invoice`
  answers a renew; other renew rejects are already dropped-and-logged) and this spec adds no wire
  types — so a malformed renew id is **dropped + logged**, exactly like renew's other rejects, not
  answered with a structured error. (The validation still runs before building the renew
  `external_id`; it just fails closed by dropping.) Annotate
  docs/specs/refund-provisioning-hardening.md F4 (which scoped these out) with a pointer here.

## Non-goals

No global reputation/deposit/PoW; no persistence for rate buckets; no change to the capacity
budget model, capture, or invoice expiry values; no new wire message types or error codes; no
change to `order.request`'s existing validator; no rate limiting of outbound/reply traffic beyond
what the inbound bucket implies. No new wire message types and no new error codes EXCEPT the one
`invalid_request_id` reject introduced above for a malformed op.request id (a structural
pre-lookup reject that must be distinct from the authorized-op `invalid_params`); reuse existing
codes everywhere else.

## Acceptance

- A sender with `cap` live unpaid HELD reservations gets `capacity_full` on the next order; a
  different sender still reserves; an expired hold frees the cap slot; a paid order (HELD through
  PROVISIONING) still counts. Existing reservation tests stay green.
- Token bucket: burst of `capacity+k` wraps from one sender → exactly `capacity` reach handlers,
  `k` dropped unseen (assert no `seen_message` rows for dropped event ids); refill admits later
  wraps; a second sender is unaffected; the operator's own pubkey bypasses.
- op.request from a stranger (no sub / non-owner / non-ACTIVE sub) → `op.result` error reply and
  **zero** `op_invocation` rows. A retry of a previously-DONE op whose sub has since been suspended
  still resends the cached DONE result. Hook still runs at most once under concurrent duplicates.
- Oversized rumor content (>64 KiB) is rejected in the unwrap path and disposed of like any
  undecodable wrap (negative cache, no `seen_message` row); oversized/too-many-keys params are
  rejected at order intake with `params_invalid`; boundary sizes (exactly 64 KiB / 8 KiB / 32 keys)
  accepted.
- `renew.request` / `op.request` with a 200-char or `../`-style id are rejected; valid ids
  unchanged; existing renew/op tests stay green.

## Suggested implementation order

1. PR-4 caps + DRIFT-3 validators (smallest, independent).
2. PR-1 reservation cap (+ config knob).
3. PR-3 op_dispatch lookup/auth/claim split.
4. PR-2 token bucket (engine change, biggest test surface).
Each step lands with its tests; steps are independently mergeable in this order.
