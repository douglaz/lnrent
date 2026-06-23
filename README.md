# lnrent

A VPS manager. Point it at a box (or a fleet) over SSH+sudo and manage everything on
it: VMs, containers, networking, storage, and services. On top of management, rent
any managed service to others on a Lightning-settled subscription discovered over
Nostr. Operator-run, no central server, no AI in the serving path.

First services: WireGuard VPN, VMs, Hermes agent, Fedimint guardian. Services are
recipes, so the set is open-ended.

- **Control plane:** `lnrentd` (Rust) — manages compute/network/storage/services,
  payments, subscriptions, Nostr.
- **Author-time tooling:** Claude skills — onboard, write recipes, manage, list, inspect.

## Status

Spec-stage, M0 skeleton in. Building toward **M1a** — the durable payment/order handshake
(the riskiest part), proven with a trivial recipe, then a **web WASM buyer** for browser
marketplace access. MVP wedge is thin VM rental (M1a handshake -> M1b VM Tier-0 -> M1c
public exposure). **M0 (skeleton)** is in: domain types, subsystem traits
(`ComputeBackend`/`NetworkBackend`/`PaymentBackend`), recipe loader, sqlite schema.

Design: [SPEC.md](./SPEC.md) (v0.19) · glossary: [CONTEXT.md](./CONTEXT.md) · decisions:
[docs/adr/](./docs/adr/) (0001-0009) · M1a work graph: `.beads/` (br, epic lnrent-7fp).

## Build

```sh
cargo build
cargo test
LNRENT_DATA_DIR=./data LNRENT_RECIPES_DIR=./recipes cargo run --bin lnrentd
cargo run --bin lnrent -- recipes
```

## Layout

- `daemon/` — Rust: `lnrentd` (control plane) + `lnrent` (operator CLI)
- `clients/` — `core` (buyer-core lib) + `cli` (native buyer) + `web` (WASM SPA)
- `recipes/` — service recipes (`wireguard/` so far: manifest + lifecycle hook stubs)
- `docs/adr/` — ADRs (0001-0009); `docs/security/` — VM deployment + networking guidelines
- `.beads/` — M1a work graph (br)
