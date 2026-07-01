# Operator go-live runbook — Fedimint mainnet

How an operator takes lnrent from "works on a test federation" to "taking real money on mainnet and
renting real VMs." Every step below is the OPERATOR's own action with the OPERATOR's credentials —
**lnrent moves no money and publishes nothing on its own.** Real payments are opt-in: without a
`--features fedimint` build AND `payment_backend=fedimint` AND a `[fedimint]` config, nothing moves.

The code is go-live-ready (real Fedimint backend wired, refund path hardened, provisioning + the buyer
and operator CLIs proven live end to end on a real federation). What remains is yours: pick a mainnet
federation, back up a seed, fund a DigitalOcean account, set a real price, and flip it on.

## 0. The one irreversible fact — your mnemonic IS your ecash

The daemon derives its Fedimint ecash position deterministically from your BIP-39 mnemonic (HKDF over the
seed with info `lnrent:fedimint:v1` — the FINAL on-funds anchor, ADR-0012 / `daemon/src/identity.rs`).
There is no separate wallet key.

- **Back up the mnemonic offline BEFORE you fund or take anything.** Lose it → lose the ecash. No recovery.
- It must NOT change once real funds exist. Never regenerate the seed on a funded data dir.
- `lnrentd backup` (COLD/OFFLINE, daemon stopped) snapshots the seed + fedimint dir + state DB + config;
  keep a copy off-box.

## 1. Decisions to make (yours to choose)

- **Federation** — a MAINNET Fedimint federation you trust to custody your working balance, with an active
  Lightning gateway. You need its invite code (`fed1…`) and the gateway's pubkey.
- **Mnemonic** — a fresh, backed-up BIP-39 mnemonic (see §0).
- **Compute** — a DigitalOcean account + API token (`DO_TOKEN`) with billing configured. The droplets are a
  real fiat cost you pay DO; the sats you receive are separate — price accordingly.
- **Relays** — the Nostr relays you publish your listing + receive orders on.
- **Recipe + price** — set `recipes/do-vps/recipe.toml` `[pricing] amount_sat` to a real price covering
  your DO cost + margin (the shipped `30000` sat / 30d is a default to review), and set a real `[service]`
  title/summary.

## 2. Build (with real payments)

```sh
nix develop . --command cargo build --release -p lnrentd --features fedimint
```

## 3. Bootstrap the operator identity + config (persists the seed 0600 into the data dir)

```sh
LNRENT_DATA_DIR=/srv/lnrent/data LNRENT_PAYMENT_BACKEND=fedimint \
LNRENT_FEDIMINT_INVITE=fed1… LNRENT_FEDIMINT_GATEWAY=<gateway_pubkey> \
LNRENT_COMPUTE_BACKEND=cloud-do LNRENT_MNEMONIC="…your backed-up mnemonic…" \
LNRENT_RELAYS=wss://relay-a,wss://relay-b \
  ./target/release/lnrentd bootstrap
```

Idempotent (re-reads the persisted seed on a re-run). Note the operator **npub** it prints — that is your
listing author + DM peer. BACK UP the mnemonic now if you haven't (§0).

## 4. Preflight — verify readiness BEFORE you announce it

Run the daemon (the config is now persisted; run only needs the data dir, the recipes dir, and `DO_TOKEN`
for provisioning):

```sh
RUST_LOG=lnrentd=info DO_TOKEN=<token> \
LNRENT_DATA_DIR=/srv/lnrent/data LNRENT_RECIPES_DIR=/srv/lnrent/recipes \
  ./target/release/lnrentd
```

Confirm ALL of these before customers can order:
- Daemon log shows, in order: `fedimint payment backend joined; real ecash money path active` · the
  operator npub · `operator recipe loaded` · `published … listing` · `ipc serving`. No `NOT fully ready`.
- `LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent money` → `Gateway: ok` and `READY`.
- DO token is valid: `curl -fsS -H "Authorization: Bearer $DO_TOKEN" https://api.digitalocean.com/v2/account`.
- ONE real end-to-end order at a SMALL price first: a buyer discovers the listing → orders → pays →
  gets a droplet → SSHes in → cancels. This is exactly the flow validated on the test federation
  (`scratchpad/live-product-proof.sh` shows the shape). Do it before real customers.

## 5. Go live

The daemon publishing its `30402` listing (step 4) IS the go-live — buyers can discover + order it now.
Share the listing coordinate / operator npub.

## 6. Operate

- **Monitor money:** `lnrent money` — balance, gateway, and refund-liability coverage (`READY` /
  `NOT READY (<reason>)`). `NOT READY` means an uncovered liability or an unreachable gateway; also watch
  the daemon's WARN/ERROR logs (`refund readiness ALARM`, gateway warnings).
- **Refunds self-fund from sales** — you do not pre-fund; keep a small float for outbound Lightning fees.
  A refund that can't be paid parks visibly (surfaced by `lnrent money` + the logs), it is never dropped.
- **Run it durably** — under systemd (`Restart=always`); SIGTERM drains in-flight work + flushes the outbox.
- **Back up on a cadence** — stop the daemon → `lnrentd backup --dest <dir>` → copy off-box → restart.
- **Cancellations are automatic** — a buyer `sub.cancel` runs out the paid period, then reconcile destroys
  the VM. Renewals, reminders, and suspensions are automatic per the reconcile loop.

## 7. Rollback / recovery

- Wrong config, no funds yet: safe to wipe the data dir + re-bootstrap.
- After funds exist: NEVER wipe or regenerate the seed. Restore from a cold backup:
  `lnrentd restore --from <backup-dir>`.
- Federation/gateway down: the daemon can't mint invoices or pay refunds until it recovers; existing subs
  keep running, and reconcile catches up when it's back.

## Safety gates

- Start with SMALL prices + a staging dogfood on a TEST federation FIRST — already validated this session
  on a real (non-mainnet-value) federation: real buyer → real ecash → real DO VM → SSH → cancel.
- Keep it opt-in until you're ready: a default build (mock backend) moves no real money.
