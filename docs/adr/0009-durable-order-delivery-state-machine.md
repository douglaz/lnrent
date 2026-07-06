# 0009 — Durable order→delivery state machine (M1a)

The payment handshake is the product and the riskiest part, so its persistence and crash
recovery are specified exactly before M1a code. The PENDING subscription **is** the order
(persisted at order time), so there is always a row to correlate a settlement to.

## Correlation and idempotent capture

- Each invoice carries a unique `external_id` (a per-invoice token, e.g. the invoice id)
  that binds the settlement to its order/subscription. phoenixd's `createinvoice` takes this
  `externalId`, and the settlement event / `lookup` returns it, so a settlement maps to
  exactly one invoice — enforced by `UNIQUE(external_id)`. *(Revision 2026-07-05: phoenixd was
  never built — ADR-0003/0012; the landed implementation is `fedimint_backend.rs`, honoring the
  same backend-agnostic `external_id` contract.)*
- Capture is idempotent on the **payment**, not the transition: the daemon runs
  `UPDATE invoice SET status='PAID', settled_at=? WHERE id=? AND status='OPEN'` and, in the
  SAME sqlite transaction, advances the subscription `PENDING -> PROVISIONING` and journals
  the event. A replayed settlement (phoenixd ws reconnect) affects 0 rows and is a no-op,
  so `paid_through` can never be double-extended.

## Crash recovery (settle -> capture -> provision -> deliver)

Every step writes a durable record before its side effect; the reconcile loop re-drives any
in-flight state on restart:

| Step | Durable record (one txn) | On restart |
|--|--|--|
| order placed | subscription PENDING + invoice OPEN (external_id) | expired-invoice PENDING -> EXPIRED |
| settlement | invoice -> PAID + sub -> PROVISIONING | replayed settlement no-ops (status guard) |
| provision ok | sub -> ACTIVE + `outbox` row for `provision.ready` | unsent outbox -> resend |
| provision fail | best-effort `destroy` + sub -> REFUND_DUE + `refund_attempt` PENDING (idempotency_key) | retry `pay(key)` — idempotent, safe before or after a prior call |
| late settle on terminal sub | detached `refund_attempt` PENDING | retry `pay(key)` (order not resurrected) |

Provision hooks are idempotent and re-run if a Box crashes mid-`PROVISIONING`.

## Delivery outbox (a paid buyer always gets credentials)

`provision.ready` (and other operator->buyer DMs) are written to an **outbox** table in the
same transaction that moves the subscription to ACTIVE; a sender task then publishes the
NIP-17 DM and marks it SENT, retrying until sent. A crash after ACTIVE but before the DM is
sent cannot strand a paying buyer — the message is resent on restart. The outbox also
answers dropped-DM resync: it keeps retrying, and the buyer can send `delivery.resend.request`
(§5.1) to prompt redelivery of the latest `provision.ready`.

## Refund ledger

Refunds are a durable `refund_attempt` ledger: dest, amount, a durable `idempotency_key`,
the `backend_payment_id` (once known), status (`PENDING` | `SENT` | `FAILED`), and an attempt
count. The row is persisted `PENDING` (durable intent) **before** `PaymentBackend::pay(dest,
amount, idempotency_key)`, which is idempotent on the key. Recovery is to **retry `pay(key)`**
for any non-terminal refund — safe whether the crash was before or after a prior call, because
the key dedups (no double-pay) and the durable `PENDING` row is always there to retry;
`payment_status_by_key` only lets restart skip a redundant call once a prior attempt
`Succeeded`. After N failed attempts the subscription stays REFUND_DUE and the operator is
alerted (ADR-0003). (Outbound `PayStatus` is distinct from the inbound-invoice `PaymentStatus`
— §6.1.)

## Consequences

- `PaymentBackend::create_invoice` takes an `external_id`; `pay` returns a payment id.
- §11 gains `invoice.external_id`, and `refund_attempt` + `outbox` tables.
- This is the gate codex flagged before M1a code; M1a implements exactly this.
