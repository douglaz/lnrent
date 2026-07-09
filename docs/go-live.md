# Operator go-live runbook — Fedimint mainnet

How an operator takes lnrent from "works on a test federation" to "taking real money on mainnet and
renting real VMs." Every step below is the OPERATOR's own action with the OPERATOR's credentials —
**lnrent moves no money and publishes nothing on its own.** Real payments are opt-in at runtime: the Fedimint backend
is compiled in by default, but without `payment_backend=fedimint` AND a `[fedimint]` config nothing moves
(and a `--no-default-features` build drops the backend entirely).

The code is go-live-ready for an **attended, operator-watched launch** (real Fedimint backend wired,
refund path hardened, provisioning + the buyer and operator CLIs proven live end to end on a real
federation). Be clear-eyed about one thing: **starting the daemon (step 4) publishes your public
`30402` listing, and that IS public exposure** — any Nostr keypair can send you orders from that
moment, already during the preflight checks (step 5 merely formalizes it). Until the GATE-0 abuse-resistance items in
`docs/specs/production-readiness.md` land (per-buyer reservation caps, inbound rate-limiting), a
stranger can hold your capacity at zero cost, so an attended launch accepts that documented risk
knowingly: keep the price real but small, capacity low, and watch the logs. GATE-1 (alerting,
teardown dead-letter, payout) gates leaving it to run unattended. What remains for the attended
launch is yours: pick a mainnet federation, back up a seed, fund a DigitalOcean account, set a real
price, and flip it on.

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
  name/summary. Leave `[provisioning] tier` honest: a stock DO droplet is Tier **0** — ADR-0007
  forbids claiming above what the host actually guarantees, and `tier` is published into your
  signed listing for buyer agents to branch on.

## 2. Build (with real payments)

Real Fedimint payments are the **default build** — no feature flag needed (use `--no-default-features`
only if you want a mock-only build). `lnrent-buyer-cli` builds the `lnrent-buyer` binary the §4
preflight end-to-end order uses:

```sh
nix develop . --command cargo build --release -p lnrentd -p lnrent-buyer-cli
```

## 3. Bootstrap the operator identity + config (persists the seed 0600 into the data dir)

```sh
LNRENT_DATA_DIR=/srv/lnrent/data LNRENT_PAYMENT_BACKEND=fedimint \
LNRENT_FEDIMINT_INVITE=fed1… LNRENT_FEDIMINT_GATEWAY=<gateway_pubkey> \
LNRENT_MNEMONIC="…your backed-up mnemonic…" \
LNRENT_RELAYS=wss://relay-a,wss://relay-b \
  ./target/release/lnrentd bootstrap
```

Idempotent (re-reads the persisted seed on a re-run). Note the operator **npub** it prints — that is your
listing author + DM peer. BACK UP the mnemonic now if you haven't (§0).

**Never set `LNRENT_MNEMONIC` (or the `LNRENT_FEDIMINT_*` vars) on the RUN invocation or in the
systemd unit/EnvironmentFile** — they are bootstrap-only. The run daemon reads the seed from the
persisted 0600 `operator.seed`. Even if you do supply the seed via the env, the daemon now closes
the misuse path in code (lnrent-y4m.7): every recipe hook is spawned with a **cleared environment** —
it receives ONLY a fixed base env (`PATH, HOME, LANG, LC_ALL, TZ, TMPDIR`) plus the recipe's own
declared `provisioning.env` list, so `LNRENT_*` (the seed) **never reaches a hook**. `DO_TOKEN` (and
`DO_REGION`/`DO_SIZE`/`DO_IMAGE`) still flow to the do-vps hooks because that recipe declares them.
On startup the daemon also `remove_var`s the seed/fedimint secrets from its own process env — but
that is defense-in-depth: it cannot overwrite the kernel-placed initial env block, so
`/proc/<pid>/environ` may still show them. For a truly clean daemon environ, deliver the seed via a
**systemd credential** (`LoadCredential=`) or **stdin** rather than the environment.

**Set `LNRENT_ALERT_NPUB` on the RUN daemon** (GATE-1 PR-5): the daemon DMs operator alerts (a
refund parked/stuck, and later teardown/relay/holdings conditions) to this npub. Use your PERSONAL
Nostr identity — the one you read DMs on — NOT the operator key, so notifications reach a client you
already carry without exposing the daemon's hot key. Alerts are ON by default on the fedimint
backend; unset ⇒ the daemon self-DMs the operator key (still durable in the outbox, but you'd have
to import the operator key into a DM client to read it). `LNRENT_ALERTS_ENABLED=false` turns the
sink off.

## 4. Preflight — verify readiness BEFORE you announce it

Run the daemon (the config is now persisted; run only needs the data dir, the recipes dir, and `DO_TOKEN`
for provisioning):

```sh
RUST_LOG=lnrentd=info DO_TOKEN=<token> \
LNRENT_DATA_DIR=/srv/lnrent/data LNRENT_RECIPES_DIR=/srv/lnrent/recipes \
  ./target/release/lnrentd
```

Confirm ALL of these before you ANNOUNCE the listing (as §intro says, orders are already
technically possible from the moment the daemon started):
- Daemon log shows, in order: the operator npub (`operator identity ready`) ·
  `fedimint payment backend joined; real ecash money path active` · `operator recipe loaded` ·
  `published … listing` · `ipc serving`. No `refund readiness warning:` / `refund readiness ALARM:`
  lines (the daemon's actual not-ready markers).
- `LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent money` → `Gateway: ok` and `READY`.
- DO token is valid: `curl -fsS -H "Authorization: Bearer $DO_TOKEN" https://api.digitalocean.com/v2/account`.
- ONE real end-to-end order at a SMALL price first: a buyer discovers the listing → orders → pays →
  gets a droplet → SSHes in → cancels. Drive it manually with the buyer CLI (`lnrent-buyer`) against
  your live listing — no script covers the full product flow (`scripts/live-fed-e2e.sh` proves only
  the ecash money path against a throwaway regtest federation). Do it before real customers.

## 5. Go live

The daemon publishing its `30402` listing (step 4) IS the go-live — buyers can discover + order it now.
Share the listing coordinate / operator npub.

## 6. Operate

- **Monitor money:** `lnrent money` — balance, gateway, and refund-liability coverage (`READY` /
  `NOT READY (<reason>)`). `NOT READY` means an uncovered liability or an unreachable gateway; also watch
  the daemon's WARN/ERROR logs (`refund readiness ALARM`, gateway warnings).
- **Alert DMs (GATE-1 PR-5):** with `LNRENT_ALERT_NPUB` set, the daemon DMs you when a refund parks
  FAILED or sits stuck — no need to tail logs 24/7. The alert is a NIP-17 DM riding the durable
  outbox (edge-triggered, at most one per condition per 6h). One honest caveat: a total relay
  blackout is the one condition that cannot be delivered (it queues), so a prolonged silence from a
  daemon you know is up still warrants a direct check.
- **Refunds self-fund from sales** — you do not pre-fund; keep a small float for outbound Lightning fees.
  A refund that can't be paid parks visibly (surfaced by `lnrent money` + the logs), it is never dropped.
- **Watch for owed teardowns:** `lnrent teardowns` (and the `open_teardowns` count in `lnrent status`)
  lists provider resources the daemon failed to tear down — a `destroy` hook that failed, or a stuck
  provision-failure cleanup. A droplet that failed to delete keeps billing you until this clears; the
  daemon retries the (idempotent) hook automatically with backoff and DMs a `TeardownFailed` alert,
  but a persistent entry means you should delete the resource by hand (e.g. in the DigitalOcean UI).
- **Run it durably** — under systemd (`Restart=always`); SIGTERM drains in-flight work + flushes the outbox.
  Only one daemon may run per data dir: startup takes an exclusive lock on `{data_dir}/lnrentd.lock`, so a
  restart racing a still-running instance fails fast with "already running" instead of double-provisioning.
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
- Keep it opt-in until you're ready: the default BUILD includes the fedimint backend
  (`default = ["fedimint"]`), but it moves no money until you bootstrap with
  `payment_backend=fedimint` + a `[fedimint]` config (the runtime default is `mock`).
  `--no-default-features` drops the real backend from the binary entirely.
