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

See **[SPEC.md](./SPEC.md)** for the full design. Status: early draft.
