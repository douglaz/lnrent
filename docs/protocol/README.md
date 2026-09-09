# lnrent protocol — the interoperability contract

This folder is the **normative** description of every surface a second, independent
implementation has to match. `SPEC.md` explains the design and the rationale; these files
pin the bytes and the behaviour. Where the two disagree, **this folder wins** — see the
precedence rule below.

Three implementer populations, three surfaces:

| You are writing | Read | Reference implementation |
|--|--|--|
| A **buyer client** (CLI, web, agent SDK) | [`dm-protocol.md`](dm-protocol.md), [`listing.md`](listing.md) | `clients/core` over `wire/` |
| An **operator daemon** | the two above plus [`operator-conformance.md`](operator-conformance.md) | `daemon/` |
| A **recipe** (a service the daemon can sell) | [`hook-contract.md`](hook-contract.md) | `recipes/dummy`, `recipes/do-vps` |

## Precedence rule

For the files in this folder, **the spec wins over the code.** A code change that contradicts
one of them is a protocol change: amend the file in the same PR, and if the change breaks an
existing peer, bump the version it lives under (below). This is the opposite of the rule for
`docs/specs/` and `SPEC.md`, where the code wins on enumerations and counts — those describe
*this* implementation; this folder describes what *every* implementation must do.

## Test vectors

`wire/tests/vectors/` holds language-neutral JSON fixtures: one file per DM message type and
one signed listing event. `wire/tests/vectors.rs` proves the Rust codec round-trips every one
of them byte-for-value. A second implementation should load the same files. A fixture is the
contract; when a message changes shape, the fixture changes in the same PR, and the Rust test
going red is how the change is noticed.

## Versioning

- **Listing content** carries `lnrent.version` (currently `1`). A parser MUST reject a version
  it does not understand.
- **DM messages carry no version field.** Compatibility rests on two rules every peer MUST
  follow: ignore unknown fields on decode, and never remove or retype a field. A change that
  cannot be expressed that way is a new message `type`, never a mutation of an existing one.
- **Recipe manifests** carry `service.version`, which is the recipe's own version, not the
  hook contract's. The hook contract is unversioned today; the same additive rule applies to
  the stdin document.

## What is deliberately NOT here

- The daemon's sqlite schema, its `PaymentBackend` trait, its supervisor — implementation
  internals (`SPEC.md` §6.1, §11).
- Fleet, VM tiers, Iroh reachability, the operator manifest and rental attestations — designed
  in `SPEC.md` and the ADRs, not built, so there is nothing to interoperate with yet. When they
  ship, their wire surface lands here.
- Operator policy such as rate-limit numbers and per-buyer caps. A peer must expect them to
  exist (`operator-conformance.md` says where) but the values are the operator's.
