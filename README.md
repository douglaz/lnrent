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

Spec-stage, building toward **M1** (the WireGuard rental loop). **M0 (skeleton)** is in:
core domain types, subsystem traits (`ComputeBackend`/`NetworkBackend`/`PaymentBackend`)
with `host`/WireGuard/phoenixd stubs, the recipe loader, and the sqlite schema.

Design: [SPEC.md](./SPEC.md) · glossary: [CONTEXT.md](./CONTEXT.md) · decisions:
[docs/adr/](./docs/adr/) (0001–0006).

## Build

```sh
cargo build
cargo test
LNRENT_DATA_DIR=./data LNRENT_RECIPES_DIR=./recipes cargo run --bin lnrentd
cargo run --bin lnrent -- recipes
```

## Layout

- `daemon/` — Rust: `lnrentd` (control plane) + `lnrent` (operator CLI)
- `recipes/` — service recipes (`wireguard/` so far: manifest + lifecycle hook stubs)
- `docs/adr/` — architecture decision records
