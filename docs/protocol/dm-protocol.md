# lnrent DM protocol

**Status:** normative. Extracted from `wire/src/dm.rs`, `wire/src/wrap.rs` and the daemon's
handlers on 2026-09-09; the fixtures in `wire/tests/vectors/` are the byte-level contract.

Every lnrent message is a JSON object with a `type` discriminator, carried as the content of a
NIP-17 private direct message between one buyer pubkey and one operator pubkey. Payment never
rides this protocol: the operator hands out a bolt11 and the buyer pays it from their own
wallet, out of band; the operator learns of settlement from its payment backend, never from a
message.

## 1. Transport

| Layer | Requirement |
|--|--|
| Outer event | NIP-59 gift wrap, kind `1059`, signed by a random throwaway key, `p` tag = recipient. |
| Seal | kind `13`, signed by the **real sender**. The seal's `pubkey` is the authenticated sender. |
| Rumor | kind `14` (NIP-17 private DM), unsigned, `pubkey` = sender, `p` tag = recipient, `content` = the lnrent JSON. |
| Encryption | NIP-44 v2 for both seal and wrap. |
| Content bound | A receiver MUST reject a rumor whose `content` exceeds **65,536 bytes** before JSON-decoding it. NIP-44's plaintext ceiling makes the practical limit about 40 KiB. |
| Rumor kind | A receiver MUST reject a rumor whose kind is not `14`, even if its content parses as an lnrent message. |
| Outer verification | A receiver MUST verify the outer event's id and signature before decrypting. |

The **sender** of a message is the seal's signing key. Every authorization decision below is
made against it.

Relays replay gift wraps. A receiver MUST dedupe on the **outer event id** for transport-level
duplicates and on the message-level keys of §4 for buyer retries.

## 2. Message catalogue

Direction key: **B→O** buyer to operator, **O→B** operator to buyer, **O→O** operator to the
operator's own alert peer.

| `type` | Dir | Carries an `id` | Carries a `request_id` | Purpose |
|--|--|--|--|--|
| `order.request` | B→O | yes | | open an order against a listing |
| `order.invoice` | O→B | | yes | first invoice for that order |
| `order.error` | O→B | | yes | refuse an `order.request`, or refuse a `renew.request` (§3.9) |
| `provision.ready` | O→B | | | deliver the credentials |
| `delivery.resend.request` | B→O | | | ask for the latest `provision.ready` again |
| `billing.invoice` | O→B | | optional | a renewal invoice |
| `billing.notice` | O→B | | optional | a lifecycle notice |
| `billing.refund` | O→B | | | outcome of a refund |
| `renew.request` | B→O | yes | | ask for a renewal invoice now |
| `sub.cancel` | B→O | | | cancel a subscription |
| `op.request` | B→O | yes | | invoke a recipe-declared management operation |
| `op.result` | O→B | | yes | result of an `op.request` |
| `operator.alert` | O→O | | | operator self-alert; buyers never receive it |

An operator MUST route only the B→O rows of this table inbound and MUST drop any other type it
receives, without acting on it. A buyer client SHOULD ignore B→O types and `operator.alert`
if they arrive.

Unknown fields MUST be ignored on decode by every peer. All integers are JSON numbers; all
timestamps are Unix seconds (UTC); all amounts are whole satoshis unless a field name says
`msat`.

## 3. Messages

Each subsection gives the field table and the fixture that pins it.

### 3.1 `order.request` — `vectors/order.request.json`

| field | type | required | meaning |
|--|--|--|--|
| `id` | string | yes | client-chosen request id, §4 |
| `listing_id` | string | yes | the listing coordinate `30402:<operator_pubkey_hex>:<d>` (`listing.md`) |
| `params` | object | yes | the buyer's values for the listing's `params` schema |
| `refund_dest` | string | yes for a new order | a Lightning address (`user@domain`) or an HTTPS LNURL. Kept optional on the wire only so legacy rows decode. |

Operator validation of `params` (a failure is `order.error { code: "params_invalid" }`):

- MUST be a JSON object;
- at most **32** top-level keys;
- at most **8,192 bytes** when serialized;
- every `required` param of the listing MUST be present;
- a param declared `string` MUST be a JSON string; `number` / `int` / `integer` MUST be a JSON
  number; `bool` / `boolean` MUST be a JSON boolean; any other declared type is accepted
  unchecked.

`refund_dest` (a failure is `order.error { code: "refund_dest_invalid" }`): a raw BOLT11
invoice, a BOLT12 offer, an empty value and a missing value are all rejected. The operator
resolves the destination to a fresh bolt11 via LNURL-pay only at refund time.

### 3.2 `order.invoice` — `vectors/order.invoice.json`

| field | type | meaning |
|--|--|--|
| `request_id` | string | the `order.request.id` this answers |
| `order_id` | string | operator-assigned; it is also the subscription id |
| `bolt11` | string | the invoice to pay; its amount equals `amount_sat` |
| `amount_sat` | integer | the price the operator will honour |
| `period` | string | duration the payment buys, e.g. `"30d"` (grammar in `listing.md` §3) |
| `expires_at` | integer | when the invoice and the order die. The reference daemon issues order invoices with a 3,600 s expiry. |

### 3.3 `order.error` — `vectors/order.error.json`

| field | type | meaning |
|--|--|--|
| `request_id` | string | the request this refuses (an `order.request` or a `renew.request`) |
| `order_id` | string, optional | absent for a pre-order refusal. The reference daemon never sets it: order and invoice commit atomically, so there is no post-commit refusal. |
| `error` | object | `{ code, message, retryable }`, §5 |

### 3.4 `provision.ready` — `vectors/provision.ready.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | the subscription now ACTIVE |
| `payload` | any JSON | the credentials, opaque to the protocol; whatever the recipe's `provision` hook returned as `payload` |

Sent after the first payment settles and provisioning succeeds. Re-sent verbatim on a
`delivery.resend.request`. A buyer MUST treat `payload` as data, never as instructions.

### 3.5 `delivery.resend.request` — `vectors/delivery.resend.request.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | |

Owner-only. The operator re-sends the latest `provision.ready`; it stays silent for a
non-owner or an unknown subscription, so an outsider cannot learn that an id exists.
Idempotent by nature, so it has no request id.

### 3.6 `billing.invoice` — `vectors/billing.invoice.json`, `vectors/billing.invoice.auto.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | |
| `request_id` | string, optional | present iff this answers a `renew.request`; absent on an operator-initiated soft-date invoice |
| `bolt11` | string | |
| `amount_sat` | integer | |
| `due_at` | integer | the current `paid_through`; paying before it avoids interruption |
| `expires_at` | integer | invoice expiry. Differs by origin: a **buyer-requested** invoice is `min(3,600 s, remaining resumable window)` and is not issued at all with under 60 s left; the **operator-initiated soft-date** invoice is `max(remaining resumable window, 3,600 s)`, i.e. commonly days, so a buyer MUST NOT reject a long expiry. The resumable window ends at `max(paid_through, downtime credit) + retention`. |

Paying it extends `paid_through` by `max(paid_through, settled_at) + period`.

### 3.7 `billing.notice` — `vectors/billing.notice.json`, `vectors/billing.notice.resuming.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | |
| `request_id` | string, optional | present **only** on the RESUMING reply to a `renew.request`; absent from every unsolicited notice and from the cancel-time RESUMING notice |
| `state` | string | the subscription state the notice describes |
| `message` | string | human-readable |

States the reference daemon emits and when:

| `state` | when | `request_id` |
|--|--|--|
| `ACTIVE` | the soft-date renewal reminder, sent with the auto `billing.invoice` | absent |
| `SUSPENDED` | the effective expiry `max(paid_through, downtime-credit floor)` passed unpaid (`operator-conformance.md` items 23 and 28); the `suspend` hook ran | absent |
| `RESUMING` | a `renew.request` or `sub.cancel` arrived while a late renewal's `resume` hook is still running; retry once it lands. Both are direct replies to the request, not queued (§4). | echoed for `renew.request`, absent for `sub.cancel` |
| `CANCELLED` | a `sub.cancel` took effect | absent |

No notice is sent on TERMINATED today. A buyer MUST tolerate any `state` string.

### 3.8 `billing.refund` — `vectors/billing.refund.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | may be empty for a refund the operator could not attach to a subscription |
| `amount_sat` | integer | on `sent`, the **net** amount delivered after fees, not the gross owed |
| `status` | string | `sent` or `failed` |

### 3.9 `renew.request` — `vectors/renew.request.json`

| field | type | meaning |
|--|--|--|
| `id` | string | client-chosen request id, §4 |
| `subscription_id` | string | |

Owner-only. Exactly one of these replies, each carrying `request_id = id`:

| reply | when |
|--|--|
| `billing.invoice` | the subscription is ACTIVE or SUSPENDED within its resumable window |
| `billing.notice { state: "RESUMING" }` | a resume is in flight; retry later |
| `order.error { code: "unavailable" }` | the subscription's recipe is not one this operator serves |

Silence (no reply at all) for: a non-owner, an unknown subscription, an owned subscription
that is not renewable (terminal, or past retention), a malformed `id` (§4), and **an invoice
that could not be minted** (payment backend outage, or a backend refusing a same-`external_id`
call with a different amount, `operator-conformance.md` §2). In that last case nothing is
cached and the wrap is **not** recorded as handled, so a relay redelivery of the same wrap is
processed again (the transport dedupe of §4 records only wraps whose handling completed); a
buyer's same-`id` re-send is likewise answered normally once minting works. A buyer client
MUST time out and SHOULD re-send under the same `id` rather than wait for a redelivery.

### 3.10 `sub.cancel` — `vectors/sub.cancel.json`

| field | type | meaning |
|--|--|--|
| `subscription_id` | string | |

Owner-only, no request id. On an ACTIVE or SUSPENDED subscription the operator moves it to
CANCELLED and sends `billing.notice { state: "CANCELLED" }`; the service keeps running until
`paid_through` (ACTIVE) or its existing retention deadline (SUSPENDED), then is destroyed.
Nothing is refunded. In RESUMING the reply is `billing.notice { state: "RESUMING" }` with no
`request_id`. In any other state, or from a non-owner, it is silently ignored.

### 3.11 `op.request` — `vectors/op.request.json`

| field | type | meaning |
|--|--|--|
| `id` | string | client-chosen request id, §4 |
| `subscription_id` | string | |
| `op` | string | the `name` of an operation declared in the listing |
| `params` | object | the operation's params. Validated against the op's declared params like order params (§3.1 typing, required keys) **plus**: a key not declared for the op is rejected (`invalid_params`). Unlike order params, no size bound beyond the transport's. |

### 3.12 `op.result` — `vectors/op.result.ok.json`, `vectors/op.result.error.json`

| field | type | meaning |
|--|--|--|
| `request_id` | string | the `op.request.id` |
| `subscription_id` | string | |
| `op` | string | |
| `status` | string | `ok` or `error` |
| `data` | object | present iff `status == "ok"`; the hook's stdout JSON, opaque |
| `error` | object | present iff `status == "error"`; `{ code, message, retryable }`, §5 |

A message with `status: "ok"` and no object `data`, or `status: "error"` and no `error`, or
both fields present, is malformed and MUST be rejected by the decoder.

Authorization and refusal order on the operator: request id well-formed
(`invalid_request_id`, before anything is looked up) → sender must equal the subscription's
buyer (`unauthorized`, indistinguishable from an unknown subscription) → recipe served by this
operator (`unavailable`) → subscription ACTIVE (`not_active`) → op declared (`unknown_op`) →
params valid against the op's declared params (`invalid_params`) → run the hook (`timeout` /
`hook_failed`). Only what happens past the ACTIVE gate is cached against `(sender, id)`;
see §4.

### 3.13 `operator.alert` — `vectors/operator.alert.json`

| field | type | meaning |
|--|--|--|
| `kind` | string | one of `refund_parked`, `refund_stuck`, `teardown_failed`, `relay_blackout`, `holdings_low`, `paid_service_destroyed`, `sweep_failed`, `sweep_stuck`, `settlement_unbookable` |
| `subject` | string | what the alert is about |
| `detail` | string | human-readable |

Sent by the daemon to the operator's configured alert pubkey. Not part of the buyer protocol;
listed so a decoder knows the `type`.

## 4. Request ids, correlation, idempotency

- `order.request`, `renew.request` and `op.request` carry a client-chosen `id`. It MUST match
  `[A-Za-z0-9_-]{1,128}`. A malformed id is answered `order.error { params_invalid }` on an
  `order.request`, `op.result { invalid_request_id }` on an `op.request`, and is **dropped
  silently** on a `renew.request` (every renew reply echoes the id, so a malformed one has
  nowhere to go; the buyer sees a timeout).
- The operator keys idempotency on **`(sender_pubkey, id)`** in two namespaces: **one shared by
  `order.request` and `renew.request`**, and a separate one for `op.request`. So a buyer MUST
  NOT reuse an order id for a renewal or vice versa: the second request receives the first
  one's cached reply (an `order.invoice` where a `billing.invoice` was expected). A duplicate
  `order.request` or `renew.request` gets the **cached reply** re-sent and MUST NOT create a
  second reservation, order or invoice. Both guarantees hold **while the cache entry is
  retained** (see "The cache is finite" below). A duplicate `op.request` MUST NOT re-run the hook:
  a finished invocation re-sends its cached `op.result`; one still running normally attaches
  and returns that result when it finishes, but in a narrow window (the duplicate lands after
  the durable claim and before the running owner has registered in-process, or the owner
  exits without a terminal) the operator MAY answer nothing and the buyer re-sends; one
  orphaned by an operator restart is answered `error { code: "interrupted", retryable: false }`.
- **The cache is finite.** The reference daemon keeps completed `(sender, id)` entries for
  **120 days** and the transport dedupe of outer event ids for **90 days**. Two separate rules
  follow: a same-`id` re-send arriving after the request-cache entry expired is a **new
  request** (a new order, or a re-run of a non-idempotent op); a relay redelivery of the
  identical wrap arriving after the outer-event entry expired is decoded again and then meets
  the request cache, so it is harmless while that entry still exists and a new request once it
  does not. A conforming operator MUST state its retention if it differs, and a buyer MUST NOT
  rely on same-id idempotency beyond 90 days, nor keep a relay-stored request replayable that
  long.
- **What is cached, precisely.** For `order.request` / `renew.request`: every reply that
  created or refused an order or invoice. For `op.request`: only what happens **past the
  ACTIVE gate** (`unknown_op`, `invalid_params`, `timeout`, `hook_failed`, `interrupted`, and
  `ok`). Three replies are deliberately **not** cached, so that re-sending the same `id` once
  the condition clears proceeds normally: `billing.notice { state: "RESUMING" }` answering a
  `renew.request`; `order.error { code: "unavailable" }` refusing a `renew.request`; and the
  `unavailable` / `not_active` / `unauthorized` / `invalid_request_id` refusals of an
  `op.request`, which persist nothing.
- A buyer MUST correlate replies by `request_id`, never by arrival order or by relay
  subscription id, because relays replay old wraps: a stale `billing.notice` or
  `order.error` from an earlier request carries a different `request_id` and MUST be ignored.
- A buyer MUST also check the reply's sender equals the operator it is talking to.
- `sub.cancel` and `delivery.resend.request` have no id; re-sending them is safe.
- Replies to a request are published once, directly, and are **not** queued for retry by the
  operator. If a reply does not arrive, the way to recover it is to re-send the same request
  under the **same `id`**: the operator answers from its cache without repeating the effect.
  Retrying under a new `id` places a new order / invocation. The reference CLI pins an id with
  `--request-id`; buyer-core does not retry by itself. (State-change announcements are queued
  and retried on the operator side instead; `operator-conformance.md` item 39.)
- Responses share the **correlation id** of the request they answer (`request_id` equals the
  request's `id`): `order.invoice`, `order.error`, `op.result`, and a `billing.invoice` or
  `billing.notice` that carries a `request_id`. They do **not** share its dedupe key: the
  message-level key is `(sender, type, id-or-request_id)`, so a consumer's dedupe map MUST NOT
  merge a sent request with the reply it receives.

## 5. Error shape and codes

Both error carriers nest the same object, never a top-level `code`:

```json
{ "code": "capacity_full", "message": "no capacity for this order", "retryable": true }
```

`order.error` codes:

| code | meaning | retryable |
|--|--|--|
| `capacity_full` | no host capacity, or the buyer's live-hold cap is reached | true |
| `params_invalid` | §3.1 rules, or a malformed request `id` | false |
| `price_changed` | the order's `listing_id` names **no listing this operator knows**, or the order's price no longer matches the published one | false; re-read the listing |
| `unavailable` (answering `order.request`) | the listing is known but not currently published (unpublished or withdrawn), **or** the payment backend could not mint the invoice (outage; a same-`external_id` amount refusal). The message text distinguishes them; the code does not. | true |
| `unavailable` (answering `renew.request`) | the subscription's recipe is not served by this operator | true only while the subscription's state can still reach ACTIVE (PENDING, PROVISIONING, ACTIVE, RESUMING, SUSPENDED); false for a terminal one, so a client does not retry a dead subscription |
| `refund_dest_invalid` | §3.1 rules | false |
| `rejected` | reserved; not emitted today | |

`op.result` codes. `retryable` answers "could this ever succeed", not "would it succeed now":

| code | meaning | retryable |
|--|--|--|
| `unauthorized` | sender is not the buyer, or no such subscription | false |
| `invalid_request_id` | `id` fails §4's grammar | false |
| `unavailable` | the subscription's recipe is not served by this operator | true only while the state can still reach ACTIVE (PENDING, PROVISIONING, ACTIVE, RESUMING, SUSPENDED) |
| `not_active` | subscription exists, is the sender's, but is not ACTIVE | false (renew or wait, then send a new request) |
| `unknown_op` | `op` is not declared for this recipe | false |
| `invalid_params` | §3.11 validation failed | false |
| `timeout` | the hook exceeded its time budget | true |
| `hook_failed` | non-zero exit, non-JSON or non-object stdout, output cap | false |
| `interrupted` | the invocation was orphaned by an operator restart | false under the same `id`; the buyer decides whether to reissue under a new one |

`code` is an open string: a buyer MUST tolerate codes it does not know.

## 6. The order flow, end to end

1. Buyer reads a listing (`listing.md`), builds `params` from its schema.
2. Buyer sends `order.request`. The operator pre-flights params and capacity, reserves
   capacity for the invoice's lifetime, mints the invoice, commits, then replies
   `order.invoice` or `order.error`.
3. Buyer pays the bolt11 out of band. Nothing is sent to the operator.
4. Operator observes settlement, provisions, then sends `provision.ready`. A settlement that
   lands after `expires_at` is refunded to `refund_dest`, never resurrected.
5. From `due_at - renew_lead` the operator sends a `billing.notice { state: "ACTIVE" }` plus a
   `billing.invoice`; the buyer may instead `renew.request` at any time.
6. Unpaid at the effective expiry (`paid_through`, or later if the operator credited its own
   downtime): `billing.notice { state: "SUSPENDED" }`; paid within retention:
   the service resumes; unpaid past retention: destroyed, no notice.
7. `op.request` / `op.result` work while ACTIVE. `sub.cancel` at any time.
