# Operator go-live runbook — Fedimint mainnet

How an operator takes lnrent from "works on a test federation" to "taking real money on mainnet and
renting real VMs." Every step below is the OPERATOR's own action with the OPERATOR's credentials —
**lnrent moves no money and publishes nothing on its own.** Real payments are opt-in at runtime: the runtime
default is `mock`, and nothing moves until you bootstrap a real backend — `payment_backend=fedimint` AND a
`[fedimint]` config, or `payment_backend=phoenixd` AND a `[phoenixd]` config. That opt-in is a RUNTIME one:
`--no-default-features` drops the Fedimint backend from the binary, but the phoenixd backend is compiled into
every build, so a build flag is not what keeps real money off.

> **phoenixd is NOT a supported go-live option yet.** It is named here only so the sentence above is
> true about what a build can move; this runbook is the Fedimint path. The phoenixd backend has not
> been through its full-daemon staging acceptance (`lnrent-tof`, which carries the full blocker set),
> and the operator obligations it creates are still open beads — among them `lnrent-kr1` (deriving
> phoenixd's wallet seed from your operator seed, and proving a seed-only restore). One risk is not
> open work at all but an ACCEPTED residual: its fee-credit liability is measured only wallet-wide,
> never per receipt — `lnrent-itw` measured a live fee-credit receive on 2026-07-26 and found
> phoenixd 0.9.0 exposes no per-receipt attribution to exclude (ADR-0019), so there is no exclusion
> left for lnrent to build. Those are what the daemon names when it refuses to start; neither this
> list nor that refusal is exhaustive. `lnrent preflight` now probes phoenixd reachability,
> authentication, and fee-schedule compatibility, but **`lnrentd backup` does not back up a phoenixd
> wallet** — those funds live under phoenixd's own `seed.dat` on the phoenixd host, and backing that
> up is yours to do. Do not take real money on phoenixd until `lnrent-tof` closes and this section
> is replaced with a real runbook.
>
> This paragraph is no longer the only guard (`lnrent-9gi`): `payment_backend=phoenixd` REFUSES to
> start — naming those gates and that residual — unless you explicitly opt in with `[phoenixd]
> accept_unsupported = true` (or `LNRENT_PHOENIXD_ACCEPT_UNSUPPORTED=true`), which attests that you
> accept an unsupported backend, and which the daemon warns about on every start.

The code is go-live-ready for an **attended, operator-watched launch** (real Fedimint backend wired,
refund path hardened, provisioning + the buyer and operator CLIs proven live end to end on a real
federation). Be clear-eyed about one thing: **`lnrent listing publish` (step 5) publishes your
public `30402` listing, and that IS public exposure** — from that moment any Nostr keypair can send
you orders. Starting the daemon does NOT: it boots quiet, and until you publish, every order is
refused.

The abuse-resistance and operability gates in `docs/specs/production-readiness.md` have BOTH landed.
GATE-0 shipped a per-buyer cap on outstanding held reservations, a per-pubkey inbound rate limit,
authorization ahead of the durable claim, and caps on unpaid-invoice load, params size and message
size — so a stranger can no longer hold your capacity at zero cost. GATE-1 shipped the operator
alert DMs, the teardown dead-letter, the sweep, the refunds list and retry, relay-connectivity
status, the draining-holdings warning, the single-instance data-dir lock, and the
ledger-authoritative money core.

That removes the specific risks this section used to ask you to accept knowingly. It does not turn
"unattended" into a promise: judgement about price, capacity and how closely you watch a live
marketplace stays yours, and the open beads are the honest list of what is still rough. Start with
the price real but small and the capacity low anyway — not because a gate is missing, but because
that is how you find out what your own box and recipe do under real orders.

What remains for the attended launch is yours: pick a mainnet federation, back up a seed, fund a
DigitalOcean account, set a real price, and flip it on.

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
  Lightning gateway (the daemon selects one natively from the federation — you do not configure a gateway).
  You need its invite code (`fed1…`).
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
LNRENT_FEDIMINT_INVITE=fed1… \
LNRENT_MNEMONIC="…your backed-up mnemonic…" \
LNRENT_RELAYS=wss://relay-a,wss://relay-b \
  ./target/release/lnrentd bootstrap
```

Idempotent (re-reads the persisted seed on a re-run). Note the operator **npub** it prints — that is your
listing author + DM peer. BACK UP the mnemonic now if you haven't (§0).

**Never set `LNRENT_MNEMONIC` or `LNRENT_FEDIMINT_INVITE` on the RUN invocation or in the
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

## 4. Preflight — verify readiness BEFORE you publish

Run the daemon. It starts **quiet**: it upserts your durable listing row, serves IPC, and publishes
nothing. No buyer can discover or order from you yet, so everything below happens with zero public
exposure. (The config is now persisted; run only needs the data dir, the recipes dir, and `DO_TOKEN`
for provisioning.)

```sh
RUST_LOG=lnrentd=info DO_TOKEN=<token> \
LNRENT_DATA_DIR=/srv/lnrent/data LNRENT_RECIPES_DIR=/srv/lnrent/recipes \
  ./target/release/lnrentd
```

Confirm ALL of these before you publish in step 5. Nothing here is time-pressured — until you
publish, you are not taking orders:
- Daemon log shows, in order: the operator npub (`operator identity ready`) ·
  `fedimint payment backend joined; real ecash money path active` · `operator recipe loaded` ·
  `durable listing is not ACTIVE; publishing nothing` (the quiet start — on a re-run of an
  already-published daemon this is `published … listing` instead) · `ipc serving`. No
  `refund readiness warning:` / `refund readiness ALARM:` lines (the daemon's actual not-ready
  markers).
- `LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent status` → `listing.published` is
  `false` and `listing.state` is `UNPUBLISHED`. This is the field to check any time you want to know
  whether you are live; it reads the same durable row order intake gates every order on.
- `LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent money` → `Gateway: ok` and `READY`.
- `LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent preflight` (alias `doctor`) → every
  emitted check passes: the refund gateway, federation guardians, lnv2 money path, DO token (the
  daemon probes `GET /v2/account` itself — the old hand-run curl), and recipe preflight. There is no
  `phoenixd` line on this Fedimint path — unlike the checks above, which report themselves as
  `skipped` when they do not apply, that one is emitted ONLY for `payment_backend=phoenixd`, so do not
  go looking for it here. Exits nonzero on any failure, `--json` for
  machine output, so an agent can gate subsequent launch promotion on it. Running it here is a
  read-only rehearsal — `listing publish` runs these same checks itself and is what actually gates
  publication. Each FAILING check prints what it will do to that gate (and carries the same verdict
  as `class` under `--json`), so you know before step 5 whether a failure hard-blocks or is one you
  can override.

## 5. Go live — publish the listing

This is the go-live, and it is an explicit act:

```sh
LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent listing publish
```

It re-runs the step-4 preflight and splits the failures:

- **Structural** — your own misconfiguration: invalid recipe provisioning params, a `DO_TOKEN` that
  is missing/malformed/rejected, a federation with no lnv2 module, a phoenixd api password or fee
  schedule that does not match the node. Publication is **refused**, the failing check is named with
  its remedy, and there is **no override**. Fix it and run the command again.
- **Reachability** — someone else is down right now: guardians unreachable, no gateway attached, the
  DigitalOcean API not answering the `provider_token` check, phoenixd not responding. Publication is
  refused too, but you can override it with `--accept-unverified` once you have decided a third
  party's outage should not hold your launch — down at publish time is not down at order time.
  **What that override costs is not the same for every dependency, so decide per check.** If the
  payment backend is still down when a buyer orders, the daemon cannot mint an invoice and refuses
  the order before any money moves. If the **compute provider** is still down, it does not: order
  intake prices the order, reserves capacity and mints the invoice without consulting the provider
  at all (provisioning happens after settlement), so the buyer PAYS, provisioning fails, and they
  are refunded net of the Lightning fees (ADR-0019). That is the ordinary failed-provision path
  rather than a new hazard, but it is a real cost to the buyer that you are accepting on their
  behalf. Nothing re-checks it for you afterwards — `lnrent money` and `lnrent preflight` are how
  you follow up.

Do not go by which dependency is at fault — go by the `class` the failing check reports. One case
crosses the line: a recipe's `preflight` hook reports only a single nonzero exit, so when the
`recipe_preflight` check fails because the provider's own **metadata** read failed (`… could not be
validated — DigitalOcean regions metadata read failed`), it is reported **structural** and there is
no override, even though the cause is an outage. Wait for the provider and run `listing publish`
again. Splitting that apart needs a new hook contract across every recipe; until then
`recipe_preflight` is unconditionally structural.

On success `lnrent status` shows `listing.published: true`. **The decision is durable**: every later
restart republishes the `30402` from the row on its own — you never run this again. Share the
listing coordinate / operator npub.

A refusal only ever declines to CHANGE something. If you re-run `listing publish` on a listing that
is already live and a check now fails, the refusal leaves it live and says so — it keeps taking
orders until you `listing withdraw`. Nothing but that command takes a listing down.

Then do ONE real end-to-end order at a SMALL price before real customers: a buyer discovers the
listing → orders → pays → gets a droplet → SSHes in → cancels. Drive it manually with the buyer CLI
(`lnrent-buyer`) against your live listing — no script covers the full product flow
(`scripts/live-fed-e2e.sh` proves only the ecash money path against a throwaway regtest federation).

### Stopping — `lnrent listing withdraw`

```sh
LNRENT_DATA_DIR=/srv/lnrent/data ./target/release/lnrent listing withdraw
```

Use it for maintenance, exhausted capacity, a bad recipe, or an incident. It marks the listing
withdrawn — which is what actually stops orders, immediately — and then asks your relays to drop the
`30402`, best-effort. A relay that cannot be reached may keep serving the stale listing, but every
order against it is refused anyway, so the retraction is buyer-UX and never blocks the withdrawal.
If the command reports that the relays were not told, run it again once they are back: the durable
withdrawal is already final and is not rewritten, but the retraction is asked again.
Like publishing, it is durable: restarts do not bring the listing back. `listing publish` relists.

Withdraw always wins a race with a publish. `listing publish` spends most of its time in the step-4
probes, and a `listing withdraw` that lands while one is still probing is not overtaken: the publish
is ABANDONED (it writes and broadcasts nothing, and reports the state your withdrawal left it in)
rather than applied on top of your withdrawal. That holds for the withdrawals that change nothing
too — one that answers "already withdrawn" or "was never published" still stops the publish behind
it, so the answer you were given stays true. Run `listing publish` again when you want it back.

**Nothing else ever withdraws your listing.** No probe, backend outage, or failed healthcheck
retracts it — only you. The listing is not the safety mechanism; the money path is, and it already
refuses orders it cannot service.

## 6. Operate

- **Monitor money:** `lnrent money` — ledger-expected holdings, the two stable readiness-seam fields,
  and refund-liability coverage (`READY` / `NOT READY (<reason>)`). `<reason>` is one of the two
  LIVENESS failures below, `InsufficientBalance` (the ledger says holdings cannot cover what is
  owed), `Unpriceable` (a real liability could not be priced this pass, so coverage cannot be
  confirmed — treat as not-covered), or `ParkedManual` (a refund is parked and needs you). The
  `federation_ok`, `gateway_ok`, `FederationDown`, and `GatewayUnavailable` wire names are retained
  for compatibility. On Fedimint they keep their literal guardian/gateway meanings. On phoenixd,
  `FederationDown` means the node is unreachable, rejected the api password, or runs a release whose
  trampoline fee schedule was never verified; `GatewayUnavailable` can mean the first phoenixd
  readiness call failed before the second recovered — it does not imply that phoenixd has a gateway.
  Those CLI/`--json` tokens remain stable across backends. On a phoenixd failure, the human
  `lnrent money` view instead names the node/refund-pay seams and prints `lnrent preflight`'s
  diagnostic and remedy. The daemon's liveness-failure WARN/ERROR lines do the same and report
  `node_ok`/`refund_pay_ok` rather than `federation_ok`/`gateway_ok`; other coverage warnings keep
  their existing fields. Gate log-scraping rules on the stable `--json` keys, not on log fields.
- **Alert DMs (GATE-1 PR-5):** with `LNRENT_ALERT_NPUB` set, the daemon DMs you when a refund parks
  FAILED or sits stuck — no need to tail logs 24/7. The alert is a NIP-17 DM riding the durable
  outbox (edge-triggered, at most one per condition per 6h). One honest caveat: a total relay
  blackout is the one condition that cannot be delivered (it queues), so a prolonged silence from a
  daemon you know is up still warrants a direct check.
- **A settlement lnrent cannot book (lnrent-gc7):** lnrent holds an invoice it will not book. One
  alert kind (`SettlementUnbookable`), one shipped cause — the *fee-credit refusal*, after which
  phoenixd HAS reported the invoice paid, so each held-back item is a real receipt and the buyer has
  certainly paid. (The pre-ADR-0022 second cause, "index divergence — a missing `phoenixd_index.db`
  row", no longer exists: the correlation maps live inside `lnrent.sqlite` and commit with the
  invoice they correlate, so lnrent cannot lose its own correlation; see *Upgrading to the
  self-contained state DB* below. The phoenixd-side case — phoenixd forgetting a receipt lnrent still
  holds — is ADR-0023's `phoenixd_forgot_invoice` condition, not yet built.)

  It is judged whole-wallet, so **one alert covers every item it holds back** and names only one as
  an example.

  - *Fee credit (ADR-0019).* phoenixd publishes no per-receipt fee-credit attribution, so the
    judgement is per WALLET. **Remedy: give the node spendable balance.** The DM names the
    SHORTFALL — how much more spendable is needed to clear the named receipt — not the receipt's
    full amount, because the refusal lifts as soon as spendable reaches it. (It also lifts if
    phoenixd converts the fee credit below the receipt: the refusal needs BOTH `credit >= receipt`
    and `balance < receipt`, so either half falling away is enough.) The DM waits `UNBOOKABLE_SETTLEMENT_ALERT_S`
    (`phoenixd_backend.rs`) from lnrent's first local sighting, because lnrent may book it on a
    later retry; that wait is skipped
    when the settlement poll is about to stop watching, since a delay outliving the last observer is
    silence rather than a delay.

    Funding fixes it without further action: the refusal only exists while phoenixd calls the
    invoice PAID, and reconcile will not expire a backend-Paid invoice, so lnrent keeps re-observing
    it — and will not suspend the buyer meanwhile. The exception is a LATE payment, one that landed
    after your local invoice had already expired: that is watched only for a grace window past the
    expiry, and past it lnrent cannot book it at all — settle that buyer from phoenixd's records.


  `lnrent money` and `lnrent status` show deduplicated alert HISTORY — one row per incident,
  carrying its subject, remedy and timestamp — over `ALERT_VIEW_WINDOW_S` (`alerts.rs`, derived as
  twice the alert cooldown; read it there rather than trusting a figure copied here). It is history,
  not live backend state. A repaired incident stays listed until the
  window expires. It is derived from the durable alert RECORDS lnrent enqueued, NOT from proof of
  delivery, which it never checks: a record appears here whether its DM reached a relay or is still
  queued behind a blackout. The number counts distinct *conditions*, not receipts. If the history
  cannot be read, both commands report it as unknown rather than zero; fix the reported storage
  error and retry.

  Disabling the alert sink (`LNRENT_ALERTS_ENABLED`) does NOT blank this view. Records enqueued
  before it was switched off are durable and still listed; what stops is the recording of new ones,
  which the view flags separately ("recording: OFF"). Read that pairing literally — the count is
  real history, and the absence of NEW entries is not evidence that nothing is wrong. The flag is raised only where this
  alert is WIRED — phoenixd today — which is narrower than "can produce the condition". lnv2 can
  also strand a confirmed payment (`PAID_UNRECOVERED`: the Lightning leg settled, minting failed)
  and is NOT wired to this alert, so on a fedimint deployment a disabled sink shows the count with
  no notice AND that condition would not have raised one anyway. Until lnrent-3p71 wires it, treat
  a quiet fedimint daemon as unmeasured for this, not clear.

- **Watch relay connectivity (GATE-1 PR-9c):** `lnrent relays` shows per-relay connected state +
  last-connected time (also summarized as `relays_connected/relays_total` in `lnrent status`). If
  ALL relays sit disconnected past 15min the daemon fires a `RelayBlackout` alert — but that alert
  is precisely the one that cannot be delivered during the blackout (it queues until a relay
  returns), so `lnrent relays` is the **out-of-band read** to reach for when a daemon you know is up
  has gone quiet. This is a transport-liveness view only — no reconnection/failover logic.
- **Refunds self-fund from sales** — you do not pre-fund; keep a small float for outbound Lightning fees.
  A refund that can't be paid parks visibly (`lnrent money`'s parked count + a `RefundParked` alert), never
  dropped. Inspect the per-item list with `lnrent refunds`; once the buyer's endpoint recovers, re-drive one
  with `lnrent refund-retry <id>` (it reruns the real resolver + capped-pay path — there is no cancel verb,
  abandoning a liability stays a manual decision).
- **A refund returns LESS than the buyer paid, and at tiny prices the haircut is stark.** The refund is
  capped at what the order actually credited (net of receive + consensus fees), and the outbound Lightning
  fee comes out of that too, so the buyer never gets the full invoice back (ADR-0019). At the shipped price
  scale this is noise; at a very low price it is not — a live 50-sat order refunded ~36 sat (~72%). If you
  set a low price, know you are also setting a poor refund experience, and price with a margin over your DO
  cost, the round-trip Lightning fees, AND the Fedimint consensus fees on both the receive and the refund
  send (the outbound debit is the payout plus the gateway fee plus the send's own mint consensus fees).
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
  `lnrentd restore --from <backup-dir>`. A **format-3** backup (taken after the first ADR-0022 boot;
  `MANIFEST.json` says `"version": 3`) is self-contained and restores as is. A **format-2** one
  carries the old `phoenixd_index.db`, which the next boot imports (see *Upgrading to the
  self-contained state DB*). A **format-1** backup whose books reference phoenixd is REFUSED at
  restore: it carries no phoenixd correlation, so restoring it would leave refunds already paid
  looking unpaid — there is no safe reconstruction; use a format-2 or format-3 backup. Any restore
  is still a rollback of the books to the backup's instant while phoenixd keeps every payment it
  made since; a PENDING refund the wallet already paid is the double-pay class lnrent-uxbd owns.
- Federation/gateway down: the daemon can't mint invoices or pay refunds until it recovers; existing subs
  keep running, and reconcile catches up when it's back.

## Upgrading to the self-contained state DB (ADR-0022)

Before ADR-0022 each payment backend kept its correlation maps — which backend invoice belongs to
which order, which refund key already paid — in a private sqlite file beside the state DB
(`phoenixd_index.db`, or `fedimint/<federation>/lnv2_index.db`). Those maps are now tables inside
`lnrent.sqlite`, written in the same transaction as the `invoice` / `refund_attempt` /
`sweep_attempt` row they correlate, so the books cannot disagree with their own correlation.

**The first boot on the new binary imports the side file, once.** With the daemon stopped, upgrade
the binary and start it. On that boot lnrentd:

1. reads the side file, validates that EVERY backend-referencing row in the books has a correlation
   that agrees with it (invoice id, bolt11, payment hash, amount; for pay rows the bolt11 and, where
   the attempt carries one, the backend payment id), repairs the books from the map ONLY where the
   backend's own current state proves the map row is the effective invoice, and refuses to boot
   otherwise, naming the first row it could not reconcile;
2. stamps as `migration_unverified` every non-terminal or retryable-FAILED refund/sweep attempt that
   has no backend payment id and no pay-map row — the file cannot tell "never started" from "started,
   witness lost" — and every FAILED attempt whose pay-map row is NOT failed (the map says the payment
   may have gone out), and PARKS them: not paid or retried while fenced — the legacy outcome is
   unknown and MAY already have paid — and a fenced sweep's cap stays subtracted from the surplus. A fenced PENDING row keeps raising `RefundStuck` / `SweepStuck`; a
   fenced FAILED row raises nothing until you look — `lnrent refunds` and `lnrent money` list them;
3. records completion in the `migration` table in the SAME transaction, then renames the side file
   `*.imported`. A crash between the two is recognised on the next boot and finished.

Take a backup BEFORE the upgrade as usual. Note that `lnrentd backup` run on the upgraded binary
before that first boot still produces the old format (2 with a phoenixd side file, else 1) — the
writer stamps format 3 only once the marker exists, so a pre-boot backup keeps its side file and
stays restorable.

**Refusals and what they mean.** Every refusal names the row and points here. None of them can be
fixed by editing the database by hand (ADR-0001: one audited writer).

- *`<side file> is missing but the books reference this backend`*: the correlation file was lost
  (a hand-copied data dir, a format-1 restore). Restore the data dir from a backup that carries the
  file (format 2, or format 1 for an lnv2-only deployment), or settle the named rows by hand from
  the wallet's own records and start from a fresh data dir.
- *`… disagrees with its … row and the backend does not positively report …`*: the books and the
  side file come from different instants (a mismatched restore) and the wallet's current view does
  not settle which is current. Decide which is current from the wallet's own records, put the
  matching pair back, and boot again. lnrent never guesses: rewriting the books to a stale map could
  orphan a paid replacement.
- *`… ambiguous provenance`* / *`… DIFFERENT …`*: the new tables are already populated, or the
  marker names another file, yet a side file is present. Remove whichever copy you know is stale.
- *`… has no … row in the legacy index`* on a pay row: a SENT attempt whose pay-map row — the owner
  of its payment hash — is missing from the file. Same remedy as the mismatched restore above.

**Parked attempts.** `lnrent refunds` lists refunds; a fenced one is refused by `lnrent
refund-retry` with a message naming the fence. `lnrent money` names fenced sweeps by id; resubmitting
a fenced sweep's invoice is refused (`sweep_fenced`), and its cap stays subtracted from the surplus
until the fence is cleared, because the legacy attempt may have paid. The daemon never clears a phoenixd fence on its own:
phoenixd's outgoing list omits an in-flight payment, its clock is unrelated to lnrent's, and its own
database can be wiped and restored with funds surviving by seed, so absence proves nothing. Check
the wallet's own outgoing records for the attempt's destination, decide, then release it:

```sh
lnrent --data-dir /path/to/your/data-dir migration clear-fence <attempt id> \
  --note "checked phoenixd outgoing: nothing for this hash" --yes
```

The note is journaled to `event_log`, and the reply names the next step, because clearing the fence
does not by itself re-drive the attempt: the drivers pick up `PENDING` rows only. A cleared `PENDING`
attempt is prepared and paid on the driver's next pass exactly as a first attempt; a cleared `FAILED`
refund still needs `lnrent refund-retry <attempt id>`, and a cleared `FAILED` sweep must be
resubmitted (`lnrent sweep <bolt11> --yes`; without `--yes` the CLI only quotes). (A backend audit that ADOPTS a matching wallet record onto a parked
attempt — never a re-send — is lnrent-uxbd for phoenixd and lnrent-gjwy for lnv2.)

**Backups after the upgrade** are format 3 and self-contained; restore accepts them as is.

## Safety gates

- Start with SMALL prices + a staging dogfood on a TEST federation FIRST — already validated this session
  on a real (non-mainnet-value) federation: real buyer → real ecash → real DO VM → SSH → cancel.
- Keep it opt-in until you're ready: the default BUILD includes the fedimint backend
  (`default = ["fedimint"]`), but it moves no money until you bootstrap with
  `payment_backend=fedimint` + a `[fedimint]` config (the runtime default is `mock`).
  `--no-default-features` drops the *Fedimint* backend from the binary — it is NOT a "no real money in
  this build" switch: the phoenixd backend (lnrent-xk3) has no cargo feature and ships in every build,
  so `payment_backend` + its config block is the gate that matters. (phoenixd itself is not go-live
  approved — see the caveat at the top; and its wallet is outside `lnrentd backup`.)
