# Operator daemon conformance

**Status:** normative. The behavioural MUSTs an operator implementation owes buyers, lifted from
`SPEC.md` §5.1, §6.2–§6.6 and the reference daemon on 2026-09-09, with the reference
implementation's storage details removed. Message shapes are in `dm-protocol.md`.

An implementation that satisfies every MUST here is one buyers can rely on regardless of what
it is written in. SHOULDs describe the reference daemon and may be varied.

## 1. Identity and transport

1. MUST sign listings and decrypt buyer DMs with one Nostr key; the coordinate of every
   listing it serves is `30402:<that key>:<d>`.
2. MUST authenticate every inbound message by the NIP-59 seal's signing key and use that key
   as the buyer identity for every authorization decision.
3. MUST route only `order.request`, `renew.request`, `sub.cancel`,
   `delivery.resend.request` and `op.request` inbound, and MUST drop any other message type.
4. MUST enforce the transport bounds of `dm-protocol.md` §1 before decoding.
5. MUST dedupe delivered wraps on the outer event id so a relay replay never re-runs a
   completed handler, and MUST make every handler safe to re-run anyway (a crash between
   handling and recording the dedupe is allowed to replay).
6. SHOULD rate-limit buyer requests per sender pubkey and SHOULD cap a sender's concurrent
   unpaid holds; both are the operator's policy, and a refused hold is `capacity_full`.

## 2. Listings and orders

7. MUST answer every routed `order.request` with exactly one `order.invoice` or
   `order.error`, correlated by `request_id`.
8. MUST validate `params` and `refund_dest` per `dm-protocol.md` §3.1 before reserving
   capacity or minting an invoice.
9. MUST refuse an order whose listing is not currently published (`unavailable`) and one
   whose price no longer matches the published listing (`price_changed`).
10. MUST reserve the recipe's declared resources for the order for the life of the invoice, so
    two concurrent orders cannot both take the last slot, and MUST release the reservation when
    the invoice expires unpaid.
11. MUST make `(sender, id)` idempotent: a duplicate `order.request` re-sends the cached reply
    and never creates a second reservation, order or invoice. The cached reply MUST be committed
    atomically with the order it describes, so no crash leaves an order without a cached reply.
12. MUST issue the invoice for exactly `amount_sat` and MUST honour that amount even if the
    listing price is edited afterwards.
13. MUST NOT trust any message claiming payment. Settlement is learned from the payment backend
    only.

## 3. Settlement and provisioning

14. A settlement MUST be resolved by the invoice it paid, never by subscription state alone,
    with exactly these outcomes: already applied → no-op; open order invoice → capture and
    provision; open renewal invoice → extend or resume; expired or otherwise unmatched →
    exactly one refund. Money is never dropped, never applied twice, never refunded twice.
15. Capture MUST be idempotent under redelivery (a replayed settlement affects nothing).
16. On first capture MUST run `provision` per `hook-contract.md`, retrying transient failures,
    and on success MUST send `provision.ready` and set `paid_through = settled_at + period`.
17. `provision.ready` MUST be durably queued in the same commit that marks the subscription
    ACTIVE, and retried until a relay accepts it, so a crash cannot strand a paid buyer.
18. On permanent provision failure MUST run `destroy` best-effort, then refund, and MUST never
    keep the money.
19. A settlement arriving after the order invoice expired, or on a terminal subscription, MUST
    be refunded and MUST NOT resurrect the order.

## 4. Subscription lifecycle

20. `paid_through` is a hard date. Every renewal settlement sets
    `paid_through = max(paid_through, settled_at) + period`: early renewals stack, late ones
    re-base.
21. From `paid_through - renew_lead` SHOULD send one `billing.notice { state: "ACTIVE" }` with a
    `billing.invoice`; reminders are best-effort and MUST NOT be the only way to renew.
22. MUST answer an owner's `renew.request` per `dm-protocol.md` §3.9 and MUST stay silent to a
    non-owner or for an unknown subscription.
23. At `paid_through`, unpaid, MUST run `suspend` and send `billing.notice { state: "SUSPENDED" }`;
    the buyer's data MUST be kept for `retention`.
24. A late renewal within retention MUST resume the service; if `resume` fails permanently the
    renewal MUST be refunded, the subscription MUST return to SUSPENDED with its prior deadlines,
    and it MUST never be left in an in-flight state.
25. After `retention`, unpaid, MUST run `destroy`. No message is required.
26. `sub.cancel` from the owner on an ACTIVE or SUSPENDED subscription MUST stop billing, MUST NOT
    interrupt the service before `paid_through` (ACTIVE) or the existing retention deadline
    (SUSPENDED), MUST NOT refund, and MUST send `billing.notice { state: "CANCELLED" }`. In any
    other state it is a no-op.
27. Any settlement-free `(state, event)` pair not listed MUST be an inert no-op, never an error
    that wedges the subscription.
28. All deadlines MUST be absolute timestamps so a transition missed during downtime fires on
    restart; an implementation SHOULD credit its own downtime so a buyer is not suspended for
    an outage they could not renew through.

## 5. Refunds

29. A refund MUST go to the order's `refund_dest`, resolved to a fresh bolt11 at refund time via
    LNURL-pay, never to a destination the buyer supplied later over an unauthenticated channel.
30. A refund MUST be persisted as intent before paying and MUST be paid with an idempotency key
    the backend dedupes on, so a crash on either side of the pay never double-pays.
31. The refunded amount MUST NOT exceed the net credit the operator actually received for that
    receipt, and the outbound fee comes out of it; the buyer is told the net delivered amount in
    `billing.refund`.
32. A refund that cannot be paid MUST stay owed and visible to the operator; it MUST never be
    silently dropped.
33. MUST send `billing.refund` with `status: "sent"` or `"failed"` once the outcome is known.

## 6. Management operations

34. MUST authorize `op.request` by sender == buyer, and MUST answer a non-owner exactly as it
    answers an unknown subscription (`unauthorized`), revealing nothing.
35. MUST refuse ops unless the subscription is ACTIVE (`not_active`), except that the recipe
    check (`unavailable`) precedes it as ordered in `dm-protocol.md` §3.12.
36. MUST run only operations declared in the recipe, resolve the hook strictly inside the
    recipe's `ops/` directory, and bound the hook by the timeout and output cap of
    `hook-contract.md` §2.
37. MUST persist each `(sender, id)` invocation so a duplicate never re-runs the hook, and MUST
    answer an invocation orphaned by a restart with `interrupted`.

## 7. Delivery

38. MUST answer an owner's `delivery.resend.request` by re-sending the latest
    `provision.ready`, and MUST stay silent otherwise.
39. Two durability models, by message class, and a buyer MUST be able to rely on both:
    - **Unsolicited messages** (`provision.ready`, every `billing.notice` and `billing.refund`,
      the soft-date `billing.invoice`, `operator.alert`) MUST be committed to a durable, retrying
      outbox in the same transaction as the state change they announce, and retried until a relay
      accepts them; a message that can never be encoded is quarantined, not retried forever.
    - **Request-correlated replies** (`order.invoice`, `order.error`, a `renew.request`'s
      `billing.invoice` / `billing.notice`, `op.result`) are sent directly once the request's
      effect is committed; they are NOT queued. Their durability is the cached reply of items 11
      and 37: a reply lost to a relay failure is recovered by the buyer **re-sending the same
      request with the same `id`**, which MUST return the cached reply without repeating the
      effect. A buyer client that never retries a silent request has no recovery path, so buyer
      clients MUST retry under the same `id` on timeout.

## 8. Things a conforming operator MUST NOT do

- Hold funds in escrow, or claim to.
- Run any model or LLM on the request path.
- Deliver credentials over anything but NIP-17 gift wrap.
- Act on a `refund_dest` that is not a Lightning address or HTTPS LNURL.
- Publish a recipe's `hook` filenames in a listing.
