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
- **A settlement lnrent cannot book (lnrent-gc7):** phoenixd reports an invoice PAID and lnrent
  refuses to book it. Two causes, one alert kind (`SettlementUnbookable`), different remedies. Both
  are judged whole-wallet or whole-index, so **one alert covers every receipt it holds back** and
  names only one as an example.

  - *Fee credit (ADR-0019).* phoenixd publishes no per-receipt fee-credit attribution, so the
    judgement is per WALLET. **Remedy: give the node spendable balance.** The DM names the
    SHORTFALL — how much more spendable is needed to clear the named receipt — not the receipt's
    full amount, because the refusal lifts as soon as spendable reaches it. (It also lifts if
    phoenixd converts the fee credit below the receipt: the refusal needs BOTH `credit >= receipt`
    and `balance < receipt`, so either half falling away is enough.) The DM waits 15 minutes from
    lnrent's first local sighting, because lnrent may book it on a later retry; that wait is skipped
    when the settlement poll is about to stop watching, since a delay outliving the last observer is
    silence rather than a delay.

    Funding fixes it without further action: the refusal only exists while phoenixd calls the
    invoice PAID, and reconcile will not expire a backend-Paid invoice, so lnrent keeps re-observing
    it — and will not suspend the buyer meanwhile. The exception is a LATE payment, one that landed
    after your local invoice had already expired: that is watched only for a grace window past the
    expiry, and past it lnrent cannot book it at all — settle that buyer from phoenixd's records.

  - *Missing `phoenixd_index.db` row.* Payment state becomes UNKNOWN: lnrent can neither observe,
    book, nor expire it. **This is the dangerous repair — read all of it before acting.**

    1. **Enumerate the set.** The alert names one invoice however many are affected. With the
       daemon STOPPED, read both databases (read-only — never write them; the daemon is the sole
       writer, ADR-0001):

       ```sh
       DD=/path/to/your/data-dir          # the daemon's LNRENT_DATA_DIR
       WORK=$(mktemp -d)                  # query COPIES, never the live files
       for db in lnrent.sqlite phoenixd_index.db; do
         cp "$DD/$db" "$WORK/"
         cp "$DD/$db-wal" "$WORK/" 2>/dev/null    # may not exist; that is fine
         cp "$DD/$db-shm" "$WORK/" 2>/dev/null
       done
       sqlite3 "$WORK/lnrent.sqlite" \
         "SELECT id FROM invoice WHERE status='OPEN' ORDER BY id;" > /tmp/open.txt
       sqlite3 "$WORK/phoenixd_index.db" \
         "SELECT invoice_id FROM phoenixd_invoice ORDER BY invoice_id;" > /tmp/indexed.txt
       comm -23 /tmp/open.txt /tmp/indexed.txt > /tmp/affected.txt
       cat /tmp/affected.txt                       # the affected set
       ```

       (The state file is `lnrent.sqlite`, not `state.db`.) Copy first, and query the copy WITHOUT
       `mode=ro`: if the daemon did not exit cleanly the databases have a hot WAL, and a read-only
       connection cannot replay it — the query fails outright, mid-incident. Recovering the WAL is a
       write, which must never touch the live files (the daemon is the sole writer, ADR-0001), so it
       happens on the copy. Bring `-wal` and `-shm` along or the copy loses the committed tail.

       `/tmp/affected.txt` is the set the rest of this procedure is about. Every candidate backup is
       judged against it — see step 2.

       **An ENCRYPTED backup dir holds only `backup.age` and `MANIFEST.json`**, so there is nothing
       to query until you decrypt one. Extract ONLY the two databases, into RAM:

       ```sh
       CAND=$(mktemp -d -p /dev/shm lnrent-cand-XXXXXX)   # tmpfs, 0700, unique name
       age -d backup.age | tar -x -C "$CAND" lnrent.sqlite phoenixd_index.db
       ```

       `age` prompts for the same passphrase you would pass `--passphrase-file`; these backups are
       passphrase-encrypted, so no identity file exists and `-i` does not apply.

       Naming the two members matters: the archive also carries `operator.seed`, the plaintext BIP39
       mnemonic that controls the funds, and a blanket extract writes it out. `/dev/shm` matters for
       the same reason — on a disk-backed `/tmp`, `rm` only unlinks, and this repo already treats
       unlinked plaintext as recoverable (`backup.rs:319-322`), so the mnemonic would survive in
       free blocks and in any snapshot taken meanwhile. Remove the directory when you are done
       (`rm -rf "$CAND"`); on tmpfs that genuinely releases it, and a reboot clears it regardless.
    2. **Choose a backup by CONTENTS, never by date.** An older backup never had their rows; a
       newer one still lacks them if the index was already lost when they were paid. Date tells you
       nothing here. A candidate qualifies only if BOTH of these hold:

       ```sh
       sqlite3 "file:$CAND/phoenixd_index.db?mode=ro" \
         "SELECT invoice_id FROM phoenixd_invoice ORDER BY invoice_id;" > /tmp/cand-indexed.txt
       sqlite3 "file:$CAND/lnrent.sqlite?mode=ro" \
         "SELECT id FROM invoice ORDER BY id;" > /tmp/cand-invoices.txt
       comm -23 /tmp/affected.txt /tmp/cand-indexed.txt     # must be EMPTY
       comm -23 /tmp/affected.txt /tmp/cand-invoices.txt    # must ALSO be EMPTY
       ```

       Both compare against `/tmp/affected.txt`, NOT the full OPEN list. Only the affected invoices
       need their rows back; every other open order is either already correlated or newer than the
       backup, and demanding those too makes the test unpassable by construction — one order placed
       after the divergence would disqualify every backup ever taken, including the one that fixes
       the incident.

       The second check is not redundant. `create_invoice` commits the index row and `order_intake`
       commits the invoice and subscription in a *separate* transaction, so a backup taken between
       them holds the correlation but not the buyer's order. Restoring that one brings the index
       back and still leaves capture unable to apply an already-paid receipt — an index-only test
       would call it qualified.
    3. **Disqualify the candidate BEFORE you restore.** Two conditions make a backup unusable no
       matter how well it passed step 2, and both are unrecoverable once you have restored. Run
       these with the daemon still stopped.

       **(a) It predates an outbound payment.** `phoenixd_pay` is lnrent's only defence against
       paying a refund twice — phoenixd's `payinvoice` takes no idempotency parameter, so that
       durable local row IS the dedup, and the module says outright that deleting it "is what would
       permit a double pay". A whole-dir restore rolls it back while the original payment still
       stands at phoenixd, so restarting can re-drive a restored PENDING refund, resolve a fresh
       bolt11, and **pay it again**.

       ```sh
       sqlite3 "file:$DD/phoenixd_index.db?mode=ro" \
         "SELECT idempotency_key, status FROM phoenixd_pay ORDER BY idempotency_key;" \
         > /tmp/pay-now.txt
       sqlite3 "file:$CAND/phoenixd_index.db?mode=ro" \
         "SELECT idempotency_key, status FROM phoenixd_pay ORDER BY idempotency_key;" \
         > /tmp/pay-cand.txt
       comm -23 /tmp/pay-now.txt /tmp/pay-cand.txt      # must be EMPTY
       ```

       If that is non-empty the backup is NOT usable — there is no supported way to re-establish
       the witness. Treat it exactly as "no backup qualifies" (step 7).

       **(b) It predates a paid subscription.** Step 1's enumeration is `status='OPEN'` and so is
       structurally blind to this: a captured, provisioned subscription has a settled invoice, and
       a backup can qualify in step 2 while still predating it. Restoring drops the subscription,
       its instance row and the ledger rows you would refund from, while the VM keeps running and
       keeps billing.

       ```sh
       sqlite3 "file:$DD/lnrent.sqlite?mode=ro" \
         "SELECT id, subscription_id FROM invoice
            WHERE status='PAID' OR settled_at IS NOT NULL ORDER BY id;" \
         > /tmp/paid-now.txt
       ```

       Compare against the candidate's copy the same way. A backup missing any of these is better
       treated as disqualified: the orphaned VM is the *cheaper* loss, and honouring or refunding a
       term whose records you just deleted is the expensive one.
    4. **Inventory the provider resources.** Teardown is driven from the persisted subscription and
       its `instance.handles_json`; rolling the data dir back to an instant before a provision
       deletes those rows, so the daemon can no longer drive deletion and the VM **keeps billing
       indefinitely**. With the daemon still stopped, record what exists now:

       ```sh
       sqlite3 "file:$DD/lnrent.sqlite?mode=ro" \
         "SELECT subscription_id, kind, handles_json FROM instance WHERE state <> 'DESTROYED';" \
         > /tmp/instances-before.txt
       ```
    5. **Restore — and do NOT restart yet.** `lnrentd restore --from <backup-dir>
       --data-dir <data-dir> --force` (`--force` because restore refuses a non-empty target and the
       live data dir is not empty; add `--passphrase-file` for an encrypted backup). It replaces the
       *whole* data dir, state DB included, rolling lnrent back to that backup's instant:
       **everything committed since is dropped** — later orders, captures, refunds, ledger rows —
       while phoenixd still holds the sats. Step 6 needs the daemon down.
    6. **Reconcile what the rollback dropped, then restart.** Re-run step 4's query into
       `/tmp/instances-after.txt` and diff the two. For every resource present before but absent
       after, decide ONE of two things — never delete on sight:

       - **The subscription was paid and its term has not run out.** Deleting it ends service the
         buyer paid for, and the restore has already dropped the rows you would refund from. Do NOT
         delete: either leave it running and honour the term by hand, or settle the buyer from
         phoenixd's records first.
       - **Nothing was paid, or the term is over.** Delete it at the provider yourself (for
         `do-vps`, the `droplet_id` in `handles_json`). lnrent will never do it for you — it no
         longer knows those rows existed, so the VM bills forever otherwise.

       Then confirm phoenixd is still on the wallet lnrent paid from. lnrent records that identity
       per payment (`phoenixd_pay.node_id` = `getinfo.nodeId`) and refuses a recovery that
       disagrees, so a mismatch here means the money path will fail closed rather than misfire:

       ```sh
       curl -sS -u ":$PHOENIXD_PASSWORD" http://127.0.0.1:9740/getinfo | jq -r .nodeId
       sqlite3 "file:$DD/phoenixd_index.db?mode=ro" \
         "SELECT DISTINCT node_id FROM phoenixd_pay WHERE node_id IS NOT NULL;"
       ```

       They must match. If they do not, stop: phoenixd has been re-seeded onto a different wallet
       and its payment history no longer describes your money — restoring lnrent cannot fix that.
       Only once the diff is settled and the identity matches, restart the daemon.
    7. **If no backup qualifies, do NOT restore.** There is no supported repair: `lnrent reconcile`
       is report-only, no command reconstructs the missing rows, and writing the DB by hand is
       forbidden (the daemon is the sole sqlite writer, ADR-0001). Keep the data dir and phoenixd's
       payment history intact, stop taking new orders, and settle affected buyers out of band from
       phoenixd's own records. Recovery tooling is tracked as lnrent-8scw.

    Never recreate or expire an affected invoice.

  `lnrent money` and `lnrent status` show deduplicated alert HISTORY from the last 12 hours
  (subject, remedy, timestamp) — not live backend state. A repaired incident stays listed until the
  window expires, and disabling the alert sink stops new entries without hiding written ones. The
  number counts distinct *conditions*, not receipts. If the history cannot be read, both commands
  report it as unknown rather than zero; fix the reported storage error and retry.

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
  `lnrentd restore --from <backup-dir>`.
- Federation/gateway down: the daemon can't mint invoices or pay refunds until it recovers; existing subs
  keep running, and reconcile catches up when it's back.

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
