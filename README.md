# lnrent

Rent and install self-hostable services on a VPS or home-lab box, list them on a
Nostr marketplace, and sell them on a Lightning-settled subscription. Operator-run,
no central server, no AI in the serving path.

First services: WireGuard VPN, VM-for-others, Hermes agent, Fedimint instance.
Services are recipes, so the set is open-ended.

- **Control plane:** `lnrentd` (Rust) — payments, provisioning, subscriptions, Nostr.
- **Author-time tooling:** Claude skills — onboard, write recipes, list, inspect.

See **[SPEC.md](./SPEC.md)** for the full design. Status: early draft.
