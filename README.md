# lnrent

Rent a server for sats. An operator runs `lnrentd`, publishes a service listing over Nostr, and
anyone can discover it, order it, pay a Lightning/Fedimint invoice, and get a provisioned box back —
no central server, no accounts, no AI in the serving path. Services are **recipes** (a manifest + a few
lifecycle hooks), so the catalogue is open-ended; the first real one provisions DigitalOcean VPSs.

- **Control plane:** `lnrentd` (Rust) — orders, payments, subscriptions, provisioning, refunds, Nostr.
- **Operator CLI:** `lnrent` — talks to a running daemon over a local IPC socket (status, subs, money).
- **Buyer CLI:** `lnrent-buyer` — agent-grade; discovers listings and places orders over Nostr.

## Status

The full rental path works and has been **proven live end to end**: a real buyer discovers a listing,
orders it, pays a real Fedimint invoice from a real wallet, the daemon provisions a **real DigitalOcean
droplet** and delivers SSH access over Nostr, and the buyer logs in — then can cancel, after which the
box runs out its paid period and is torn down.

It has also been dogfooded as a **multi-seller / multi-buyer marketplace**: three independent seller
daemons each publish a 1-sat listing on a shared federation, five buyers concurrently discover, order,
and pay real ecash, and every order is delivered — with the payer's spend and the sellers' balances
reconciling to the msat.

Built and tested:
- **Money path** — durable order → invoice → settlement → capture, on a **real Fedimint backend**
  (the **default build**; `--no-default-features` selects an in-memory mock). Refunds are hardened: fees
  are deducted so the operator can't be drained, readiness only warns on real uncovered liabilities, and
  every refund requires provenance.
- **Provisioning** — recipe-hook driven; `do-vps` creates/destroys real DigitalOcean droplets end to end.
- **Buyer lifecycle** — discover, order, pay (out of band), await credentials, renew, cancel, and invoke
  recipe-declared management ops — all over NIP-17 DMs.
- **Operator** — `lnrent status/recipes/subs/sub`, and `lnrent money` (ecash balance, gateway, refund
  liability coverage).

Not yet: a browser/GUI buyer, more compute providers (Hetzner, bring-your-own-host), and the mainnet
go-live (real money is opt-in at runtime and gated on the operator finalizing their setup). See
[SPEC.md](./SPEC.md).

## Build

The workspace links a bundled RocksDB, so builds run inside the Nix devshell. The real Fedimint money
path is the **default feature**:

```sh
nix develop . --command cargo build                                   # real Fedimint backend (default)
nix develop . --command cargo test -p lnrentd
nix develop . --command cargo build --no-default-features -p lnrentd  # mock-only (no fedimint/rocksdb tree)
```

## Run

**Operator daemon** (mock payments — no external services, lean build without the fedimint/rocksdb tree):

```sh
LNRENT_DATA_DIR=./data LNRENT_RECIPES_DIR=./recipes LNRENT_RELAYS=wss://relay.example \
  nix develop . --command cargo run --no-default-features -p lnrentd --bin lnrentd
```

For **real Fedimint payments + real VMs** (the default build), configure the federation + DigitalOcean
token and select the fedimint backend at runtime:

```sh
LNRENT_PAYMENT_BACKEND=fedimint LNRENT_FEDIMINT_INVITE=fed1… LNRENT_FEDIMINT_GATEWAY=<gateway_pubkey> \
LNRENT_COMPUTE_BACKEND=cloud-do DO_TOKEN=<digitalocean_token> \
LNRENT_MNEMONIC="…" LNRENT_DATA_DIR=./data LNRENT_RECIPES_DIR=./recipes LNRENT_RELAYS=wss://relay.example \
  nix develop . --command cargo run -p lnrentd --bin lnrentd
```

**Operator CLI** (same `LNRENT_DATA_DIR` as the daemon — connects to its IPC socket):

```sh
LNRENT_DATA_DIR=./data nix develop . --command cargo run -p lnrentd --bin lnrent -- money
#   subcommands: status · recipes · subs · sub <id> · money   (add --json for machine output)
```

**Buyer CLI** (talks to the operator over a relay; the buyer pays the returned invoice from their own
wallet — the CLI never holds funds):

```sh
B="nix develop . --command cargo run -p lnrent-buyer-cli -- --relay wss://relay.example --operator <npub> --key-file buyer.nsec"
$B identity new                       # create a buyer key
$B listings                           # discover the operator's listings
$B order create <30402:…:…> --params-json '{"ssh_pubkey":"ssh-ed25519 …"}'   # -> a bolt11 invoice
#   ...pay the bolt11 from your wallet...
$B order wait <order_id>              # -> access credentials (host/port/user)
#   also: renew <sub> · cancel <sub> · ops <sub> <op> · delivery resend <sub>
```

## Layout

- `daemon/` — `lnrentd` (control plane) + `lnrent` (operator CLI)
- `wire/` — the Nostr wire codec: DM message types, NIP-99 listings, NIP-17 gift-wrap
- `clients/core` — `lnrent-buyer-core` (buyer library); `clients/cli` — `lnrent-buyer` (native buyer)
- `recipes/` — service recipes: `do-vps` (DigitalOcean VPS), `wireguard` (stub), `dummy` (tests)

## Docs

- Spec: [SPEC.md](./SPEC.md) (draft v0.29) · glossary: [CONTEXT.md](./CONTEXT.md)
- Decisions: [docs/adr/](./docs/adr/) (0001-0015) · change specs: [docs/specs/](./docs/specs/)
- Security/deployment notes: [docs/security/](./docs/security/)
