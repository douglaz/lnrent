# lnrent listing event

**Status:** normative. Extracted from `wire/src/listing.rs` and `daemon/src/listing.rs` on
2026-09-09; `wire/tests/vectors/listing.event.json` is a signed example.

A listing is a NIP-99 classified listing (kind `30402`, parameterized-replaceable) published
by the operator's signing key. It carries the price and the buyer-facing schema of one
recipe. Its **coordinate** `30402:<pubkey_hex>:<d>` is the `listing_id` every
`order.request` references, and it stays stable across price edits because a republish
reuses the same `(kind, pubkey, d)`.

## 1. Tags

| tag | value | required | notes |
|--|--|--|--|
| `d` | identifier | yes, non-empty | the reference daemon uses the recipe's `service.id`; the protocol only requires a non-empty string |
| `title` | string | yes | |
| `summary` | string | yes | |
| `price` | `[<amount>, "SAT", <period>]` | yes | `amount` is a decimal unsigned integer of whole satoshis; currency MUST be `SAT`; `period` is the duration string of §3 |
| `operator` | 64-hex Nostr pubkey | yes | the master (brand) pubkey. Today it equals the signing key. |

A parser MUST use the **first** occurrence of each single-valued tag, MUST tolerate unknown
tags, and MUST reject a `d` that is missing or empty, a non-`SAT` currency, a non-integer
amount, and an `operator` that is not a valid public key.

## 2. Content

`content` is a JSON object:

```json
{
  "lnrent": {
    "version": 1,
    "recipe": { "id": "do-vps", "version": "0.1.0" },
    "tier": "0",
    "params": [
      { "key": "ssh_pubkey", "label": "Your SSH public key", "type": "string", "required": true }
    ],
    "operations": [
      { "name": "status", "label": "VPS status", "kind": "request", "params": [] }
    ]
  }
}
```

| path | type | required | meaning |
|--|--|--|--|
| `lnrent.version` | integer | yes | schema version; MUST be `1`; a parser MUST reject any other value |
| `lnrent.recipe.id` | string | yes | |
| `lnrent.recipe.version` | string | yes | |
| `lnrent.tier` | string | no | the honest VM security tier `"0"`, `"1"`, `"1.5"`, `"2"`; absent for non-VM services; an open string |
| `lnrent.params` | array of param decl | no, default `[]` | the order `params` schema |
| `lnrent.operations` | array of operation decl | no, default `[]` | the management operations a buyer may invoke |

Param declaration: `{ key: string, label: string, type: string, required: bool (default false) }`.
`type` values the reference daemon validates are `string`, `number` / `int` / `integer`,
`bool` / `boolean`; other strings are accepted and left to the recipe.

Operation declaration: `{ name: string, label: string, kind: string, params: [param decl] }`.
`kind` is `request` (invoked over `op.request`) or `interactive` (reserved for a future
streaming transport; not dispatchable today). The recipe's internal `hook` filename is never
published.

Bounds a parser MUST enforce while deserializing, before allocating: at most **64** entries in
`params`, at most **64** in `operations`, at most **64** in any operation's `params`. Unknown
fields inside the content MUST be ignored.

## 3. Duration strings

`period` in the price tag, and every duration in `order.invoice` / recipe pricing, is a
positive integer followed by an optional unit: `s`, `m`, `h`, `d`, `w`. A bare integer is
seconds. Examples: `"30d"`, `"12h"`, `"3600"`.

## 4. Discovery and verification

- A buyer discovers listings with a filter of kind `30402` and `authors` = the operator
  pubkey it intends to buy from.
- A buyer MUST verify the event signature and MUST trust only events whose `pubkey` is the
  operator it queried. The `operator` tag is informational until the operator-manifest
  mechanism ships (SPEC.md §5.3); there is no cross-key brand verification today.
- The buyer computes `listing_id` from the event's `pubkey` and `d` tag, never from the
  `operator` tag.

## 5. Withdrawal

The operator withdraws a listing by publishing a NIP-09 deletion request (kind `5`) whose
`a` tag names the coordinate `30402:<pubkey_hex>:<d>`, with a human-readable reason. A
buyer that sees a deletion for a coordinate SHOULD stop offering it. The operator also
refuses new orders for a withdrawn listing with `order.error { code: "unavailable" }`;
the deletion is best-effort signalling, the refusal is the guarantee.
