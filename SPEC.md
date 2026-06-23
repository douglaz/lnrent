# lnrent — Spec (draft v0.23)

> Working codename: **lnrent** (rename later). Daemon: `lnrentd`. CLI: `lnrent`.
> Status: DRAFT for review. Author-time tooling = Claude skills. Runtime = pure Rust/bash.

## 1. What this is

lnrent is a **VPS manager**. An operator points it at a box (eventually several)
reachable over SSH with sudo and manages everything on it from one control plane:
virtual machines and containers, networking, storage, and the services running on
top. On top of management, lnrent can **rent any managed service to others**,
settled in Bitcoin Lightning and discovered over a Nostr marketplace. No central
marketplace server, no central payment custodian. "Marketplace" means the decentralized
**Nostr** (discovery, listings, ordering) plus **Iroh/relay** (management transport)
fabric, not a service lnrent runs: relays are public or operator-run, and any KMS /
registry referenced by the VM guidelines is operator-run, never a central party.

Renting is one capability, not the whole product. A managed service is either:

- **self-use** — the operator runs it for themselves; no Listing, no billing; or
- **rented** — the operator publishes a Listing and a Buyer pays a Lightning
  subscription to use it.

The management machinery underneath is identical. Renting only adds a Listing, a
Subscription, and Lightning billing on top of a managed Instance.

The product is two things:

1. **An always-on control plane** (`lnrentd`, Rust) that manages compute, network,
   storage, and services on the operator's boxes, watches for payments, enforces
   subscriptions, and speaks Nostr. Pure Rust/bash, no LLM in the path (§4.1).
2. **A set of Claude skills** used by a human operator at author-time and
   setup-time: onboard a box, write recipes, manage resources, publish listings,
   inspect state.

First services: **WireGuard VPN**, **VMs** (for self and for rent), **Hermes agent**
(NousResearch/hermes-agent), **Fedimint guardian**. These are recipes, not
hardcoded. Adding a service means dropping in a new recipe.

## 2. Goals and non-goals

### Goals
- From one control plane, an operator manages compute, networking, storage, and
  services across one or more boxes, for self-use or for rent.
- An operator can go from a NixOS or Debian box (SSH+sudo) to a published, rentable
  service in a few skill-driven steps.
- A buyer with a Nostr key and a Lightning wallet can discover an offer, pay, and
  receive working credentials, with no account and no operator-side AI.
- Subscriptions are enforced automatically: prepaid to an expiry date, renew before
  it (nudged early from a soft date), lapse means suspend at the date then destroy
  after retention.
- New services are added as self-contained recipes without touching daemon code.
- The whole thing runs on NixOS (declarative) and Debian (imperative).

### Non-goals (v1)
- No central marketplace website or hosted directory. Discovery is Nostr-native.
- No custodial escrow or dispute resolution. Trust model is "pay-then-provision,
  reputation over Nostr."
- No fiat. No on-chain-only path (Lightning first; on-chain is a later fallback).
- No AI anywhere in the runtime serving path (hard rule, see §4).
- No multi-tenant bin-packing guarantees or SLAs. Best-effort, operator-owned.

### 2.1 Capability surface ("do everything")

lnrent's job is the full management surface of a box, with renting layered on top.
Capabilities, grouped and phased:

| Subsystem | Manages | Phase |
|--|--|--|
| Box / fleet | onboard over SSH+sudo, inventory, health; later provision the box itself (cloud API / nixos-anywhere) | core, then later |
| Compute | VMs and system containers (Incus default; libvirt/proxmox later) | core |
| Networking | WireGuard, firewall, port allocation, reverse-proxy/ingress, DNS | phased |
| Storage | volumes, snapshots, backups | phased |
| Services | install and run Recipes, for self-use or rent | core |
| Observability | status, logs, metrics, alerts | phased |
| Marketplace | Listings, Subscriptions, Lightning billing, Nostr | the rental layer |

Everything is a Recipe or a managed resource behind the same subsystems and
lifecycle machinery. Renting only adds Listing + Subscription + billing on top.

Prior art to learn from, not reinvent: Coolify, CapRover, Cloudron, YunoHost,
Cockpit, Proxmox. lnrent's differentiators are the AI-free control plane, native
Nostr/Lightning rental, and self-sovereign single-operator ownership.

## 3. Personas

- **Operator (primary, v1):** owns a box, wants recurring sats for running services.
  Technical, comfortable with a CLI and with Claude skills. Runs `lnrentd`.
- **Buyer (v1):** has a Nostr identity and a Lightning wallet. Discovers offers,
  pays invoices, receives credentials over an encrypted Nostr DM. Uses either the
  lnrent **CLI buyer** or the **web client**; both talk straight to relays and the
  buyer's own wallet, with no lnrent backend between buyer and operator.
- **Marketplace (long-term, two-sided):** emerges from many operators publishing to
  shared Nostr relays. No central party owns it. v1 ships the operator half well;
  the buyer half is two reference clients (CLI + web WASM, over a shared buyer-core) plus the published
  listing and DM-protocol spec.

## 4. Architecture

### 4.1 Hard invariant: AI-free control plane

The runtime path is **pure Rust/bash with no LLM in the loop**:

```
payment watch -> invoice issue -> provision -> lifecycle -> Nostr delivery
```

Claude skills are **author-time and operator-time only**: onboard a box, scaffold
and debug a recipe, publish a listing, inspect subscriptions. Nothing the daemon
does at runtime calls an LLM.

Resolution of the obvious irony: one of the services we *sell* is an AI agent
(Hermes). That agent runs as a **tenant workload** inside a provisioned, isolated
VM or container that the buyer controls. It is never part of lnrent's control
plane. The rule is about our serving path, not the buyer's workload.

### 4.2 Components

```
                         Nostr relays
                              |
            listings (NIP-99) / DMs (NIP-17)
                              |
   +--------------------------+----------------------------+
   |                       lnrentd (Rust)                   |
   |                                                        |
   |  RENTAL LAYER:  nostr | billing | subscription cycle   |
   |  ------------------------------------------------------|
   |  MANAGER CORE:  resource manager (Instance inventory)  |
   |                 recipe runner (lifecycle hooks)        |
   |                 event log                              |
   |  ------------------------------------------------------|
   |  SUBSYSTEMS:    Compute | Network | Storage | Payment  |
   |                         | Observability                |
   |  sqlite state                                          |
   +----+---------+---------+---------+---------+-----------+
        |         |         |         |         |
     Compute   Network   Storage   Payment   Observ.
     - host    - wg peer - volumes  - phoenixd  - status
     - incus   - firewall- snapshot - fedimint  - logs
     - libvirt - ports   - backup   (federation - metrics
     - proxmox - ingress             + gatewayd)
     - cloud   - dns
```

Two layers:

- **Manager core** owns the inventory of managed Instances, runs their lifecycle,
  and drives the subsystems. It behaves identically whether an Instance is self-use
  or rented.
- **Rental layer** (billing, subscription cycle, Nostr) sits on top and engages only
  for rented Instances. Strip it away and lnrent is still a working VPS manager.

Subsystems are trait-bounded backends (§8). Recipes orchestrate them via hooks; the
manager can also drive them directly for operator self-use (e.g. "create a VM for
me", "open port 443").

Claude skills sit beside this, not inside it, and run only when a human invokes
them. The daemon is the **sole writer** of state: skills never touch sqlite
directly, they act only through the `lnrent` CLI, and the one thing they author
directly is recipe files on disk (ADR-0001). This keeps the AI-free invariant
enforceable: the LLM can request actions through a typed, audited surface but
cannot silently mutate live state.

Buyers use the lnrent **CLI** or the **web (WASM) client**. Both are thin shells over a
shared Rust **buyer-core** lib (the DM protocol, order flow, gift-wrap), so there is one
protocol implementation, not two. Both connect directly to relays and to the buyer's own
Lightning wallet, with no lnrent server in between. The web client is a static SPA: the
buyer-core compiles to wasm32; it signs with a NIP-07 browser extension (or a locally held
key), reaches relays over browser WebSockets, and pays via WebLN or a copied bolt11. Static
hosting keeps the no-central-server property — the website is just a buyer front-end over
Nostr + the buyer's wallet.

### 4.3 Trust model

- Buyer pays the first period invoice **before** provisioning. Operator provisions
  on confirmed payment. No escrow.
- Operator reputation accrues to the **master operator identity** (§4.6), not to any
  Box. M1a is blind-trust (a mechanics proof); before the marketplace is publicly
  promoted, a minimal Nostr-native reputation primitive ships — buyer-signed **rental
  attestations** (NIP-32 labels) to the master identity, web-of-trust-weighted, surfaced
  in the web buyer (ADR-0011). Bonds/escrow are deferred; reputation rides on top of the
  technical controls (ADR-0007/0008/0010), never replacing them.
- Credentials are delivered only over NIP-17 gift-wrapped DMs (sender and recipient
  hidden from relays).

### 4.4 Resource model

Everything the manager tracks with a lifecycle is an **Instance**: a VM, a container,
a WireGuard peer, a volume, a fedimintd guardian. An Instance records its kind, the
backend handles needed to manage it later, its owner (the Operator for self-use, or
one Subscription for rented), its Box, and its state. Recipes produce Instances;
direct manager operations also produce them. Settings applied to a Box or an Instance
(a firewall rule, a DNS record) are configuration, not Instances.

### 4.5 Topology: value plane vs hosting plane (ADR-0010)

lnrent splits into two planes so the operator's value never sits on a box that hosts
untrusted tenants:

- **Control node (value / identity / marketplace plane):** holds the phoenixd/Fedimint
  wallet, the BIP39 seed + master key, and the marketplace control — the Nostr engine
  (listings + order DMs), the subscription store + billing/reconcile, and manifest
  signing. The operator's brain + wallet.
- **Hosting box (hosting plane):** runs the provisioning backend (Incus) + tenant VMs;
  holds only a revocable per-box **operational key** (ADR-0004) to authenticate to the
  control node and sign its host security profile. No funds, no seed.

The control node drives hosting boxes over the **Iroh** control plane (ADR-0008,
outbound-only from the box). A hosting-box compromise loses only that box's operational
key and its tenants — not the money or the brand; the master revokes the box by
re-issuing the manifest.

- **M1a (and self-use / no-untrusted-tenants):** the control node and the one hosting
  function may be the **same box** (the all-in-one exception). The split is **mandatory
  once a box runs untrusted tenant VMs** (M1b+).
- lnrent manages a fleet of hosting boxes from the control node, but does not cluster
  them under one scheduler (§17).

### 4.6 Identity and key management

All operator keys derive from a single **BIP39 seed** (the one thing to back up).
Derivation follows NIP-06 (`m/44'/1237'/<account>'/0/0`, secp256k1, the Nostr coin
type), so every key is regenerable from the seed:

- **Master identity** — the operator's brand, derived at account 0. Reputation accrues
  here. It signs an **operator manifest**: a replaceable, master-signed event listing
  the operational pubkeys that act on the brand's behalf (app-defined; no NIP fits).
- **Operational key (per Box)** — derived at account = Box index >= 1 (account 0 is the
  master, never a Box; M1's single key uses account 0 directly). Each Box signs its
  Listings and receives/decrypts buyer NIP-17 DMs with its own operational key, so the
  master key need not be hot on every Box. Buyers verify a Listing by checking the
  master manifest covers its signing key.
- **Payment backend secret** — the **Fedimint client root secret** (the primary backend,
  ADR-0012) derives from the same BIP39 seed at a dedicated path, distinct from the NIP-06
  Nostr paths. So **one seed backs up everything**: the brand + per-Box identity keys AND
  the Fedimint client, whose ecash position is recoverable from the federation by the same
  seed. (phoenixd, the secondary backend, keeps its own channel-state seed, backed up
  separately.)

Custody and rotation:
- The **seed and master key stay off the Boxes** where practical (generated on the
  operator's machine; only the derived operational key is deployed to a Box). A Box
  compromise then leaks only that Box's operational key, which the master revokes by
  re-issuing the manifest; the seed and reputation survive.
- The seed + master key + the receiving wallet live on the **control node**, never on a
  box that hosts untrusted tenants (ADR-0010). An all-in-one box is allowed only for
  M1a / self-use / no-untrusted-tenants; there a box compromise is a seed compromise.
  Onboard makes the backup explicit.
- Losing the seed without a backup loses the identity and its reputation. Onboard
  forces a backup step.

The marketplace identity is portable because reputation lives on the seed-rooted
master, not on any Box.

## 5. Marketplace over Nostr

Principle: use a standard NIP where it genuinely fits; where none fits, define an
explicit lnrent application protocol carried over Nostr's encrypted-DM transport.
Do not bend a NIP to a job it was not designed for.

| Concern | Mechanism |
|--|--|
| Storefront / offer listing | **NIP-99** classified listing (kind `30402`) |
| Order, invoice, credential delivery, billing notices, cancel | **lnrent DM protocol** (JSON) inside **NIP-17** gift-wrapped private DMs (kind `1059`) |
| Identity | Nostr pubkeys (operator and buyer) |
| Auto-renew pull payments (v2) | **NIP-47** Nostr Wallet Connect |
| Listing authenticity / operator brand | **Operator manifest** (master-signed, parameterized-replaceable; §5.3) |

NIP-90 (Data Vending Machines) was considered for ordering and dropped: it is
job-shaped (single request, single result) and does not fit ongoing subscriptions.
Forcing it would be a hack. Ordering and the full subscription lifecycle ride
NIP-17 DMs with an explicit message schema instead. This is exactly how NIP-15
carries its orders, so it is idiomatic, not ad hoc.

### 5.1 lnrent DM protocol

Each message is a JSON object with a `type` discriminator, sent as the content of a
NIP-17 private DM between the buyer and operator pubkeys:

| type | direction | payload |
|--|--|--|
| `order.request`   | buyer -> operator | `listing_id`, validated `params`, `refund_dest` (BOLT12 offer or Lightning address) |
| `order.invoice`   | operator -> buyer | `order_id`, `bolt11`, `amount_sat`, `period`, `expires_at` |
| `order.error`     | operator -> buyer | `order_id`, `reason` |
| `provision.ready` | operator -> buyer | `subscription_id`, `payload` (the credentials) |
| `billing.invoice` | operator -> buyer | `subscription_id`, `bolt11`, `amount_sat`, `due_at` |
| `billing.notice`  | operator -> buyer | `subscription_id`, `state`, `message` (renewal reminder / suspend / terminate) |
| `billing.refund`  | operator -> buyer | `subscription_id`, `amount_sat`, `status` (sent / failed) |
| `renew.request`   | buyer -> operator | `subscription_id` (request a renewal invoice on demand) |
| `sub.cancel`      | buyer -> operator | `subscription_id` |

Payment is settled out-of-band with the buyer's own Lightning wallet. lnrentd
confirms settlement through its `PaymentBackend` (phoenixd/Fedimint), never by
trusting a Nostr message that claims payment.

### 5.2 Relays

Default to a set of popular public relays (operator-overridable). Operator-run or
service-specific relays come later, once listing and order-DM delivery reliability
on public relays is understood.

### 5.3 Listing authenticity (operator manifest)

Listings are signed by a Box's **operational key**, so a Box self-publishes and edits
its own Listings while the master identity stays cold (§4.6). A buyer verifies a
Listing belongs to a brand via the master-signed **operator manifest**:

- **Operator manifest** — a parameterized-replaceable event signed by the master
  identity, listing the operational pubkeys it vouches for. App-defined kind in the
  30000 range with a fixed `d` tag (kind pinned when the manifest ships, in **M5**).
  Replaceable, so the master updates it to add or revoke Boxes. **In M1, before the
  manifest exists, Listings are signed by the single account-0 key and brand-authenticated
  by that pubkey alone (no cross-Box manifest); cross-Box authenticity arrives in M5.**
- Each Listing (kind `30402`) carries an `operator` tag naming the master pubkey.

Buyer verification (CLI and web both run this):

1. Read Listing `L`, signed by operational key `K`.
2. Read `L`'s `operator` tag -> master pubkey `M`.
3. Fetch `M`'s operator manifest; verify it is signed by `M` and that `K` is in it.
4. If `K` is not attested, treat `L` as unverified and do not show it as `M`'s.

This binds a Listing to a brand without the master key being hot: an attacker can put
`M` in an `operator` tag, but cannot get their key into `M`'s manifest. Reputation
attaches to `M`, so buyers compare brands, not Boxes. **Revocation:** the master
re-publishes the manifest without a compromised Box's key; that Box's Listings stop
verifying once buyers refetch the manifest — bounded by a manifest TTL/version (§16), not
instantaneous (replaceable events have no push invalidation). A Listing is also
unverifiable while the manifest cannot be fetched (relay gap).

## 6. Payments and subscriptions

### 6.1 Payment backends

A `PaymentBackend` trait abstracts receiving:

```rust
trait PaymentBackend {
    fn create_invoice(&self, amount_sat: u64, memo: &str, expiry_s: u32, external_id: &str) -> Invoice;  // externalId binds settlement -> order (ADR-0009)
    fn watch(&self) -> PaymentStream;            // stream of settled payments
    fn lookup(&self, id: &InvoiceId) -> PaymentStatus;
    fn pay(&self, dest: &RefundDest, amount_sat: u64) -> PayResult;   // outbound, for refunds
    fn payment_status(&self, payment_id: &PayId) -> PaymentStatus;    // outbound refund status (ADR-0009)
}
```

- **fedimint (default for low-value rentals, ADR-0012):** connect to an **existing
  federation** and route through an **existing gatewayd**; payments settle into **ecash**
  with **no per-payment inbound-liquidity cost** (the federation's gateway eats the LN
  side). The economic fit for the small-sat rentals lnrent leads with. Reuses the
  operator's federation membership; does not run a guardian. Runs on the **control node**.
- **phoenixd (secondary, standalone / higher-value):** self-custodial, HTTP API +
  websocket, auto-manages channel liquidity. But **receiving needs inbound liquidity**: a
  fresh operator's first payment triggers an on-the-fly channel open (service + on-chain
  fee) that can exceed a small rental — so phoenixd fits operators with their own liquidity
  and larger / longer-prepay payments, not 5k-sat one-offs. Control node only (ADR-0010).
- **Inbound-liquidity reality:** to *receive*, you need inbound capacity. Fedimint
  sidesteps it; phoenixd pays for it per above. Price periods above the receive overhead,
  offer longer prepay, and treat tiny amounts as Fedimint territory.
- Neither supports **hold invoices**, so v1 captures on settlement and refunds on failure
  (§6.4, ADR-0003); an **LND** backend (native holds) is a later option.

Operator picks one backend per control node. All expose the same trait.

### 6.2 Subscription model: prepaid expiry, renew before the date (v1)

A subscription is **prepaid to a hard expiry date** (`paid_through`):

- Each payment extends `paid_through` by `period`. The buyer renews any time before
  it; renewing early just pushes the date out (early renewals stack, never wasted).
- A **soft date** (`paid_through - renew_lead`) is a recommendation: from there the
  daemon nudges the buyer to renew (NIP-17 reminders) so the service is never
  interrupted. Renewing well before expiry is the encouraged path.
- At the **hard date**, if unpaid, the service is interrupted (`suspend`), then
  destroyed after `retention`. There is no post-expiry grace; the soft-date
  recommendation is the buffer.
- **NWC pull** (NIP-47) is the v2 hands-off upgrade: the buyer grants a budgeted
  wallet connection and the daemon auto-renews before the soft date, so it never
  interrupts.

### 6.3 Subscription state machine

States: `PENDING`, `PROVISIONING`, `ACTIVE`, `SUSPENDED`, `TERMINATED`, `EXPIRED`,
`CANCELLED`, `REFUND_DUE`, `REFUNDED`.

Timers per Listing (operator-tunable): `period` (how much a payment extends
`paid_through`, e.g. 30d), `renew_lead` (how far before expiry renewal is recommended
and reminders start, e.g. 7d), `retention` (after suspend, data kept before destroy,
e.g. 7d).

**Order and first capture** (no hold; capture-then-refund, §6.4):
- **PENDING** — pre-flight passed, first invoice issued, awaiting payment.
- **PENDING -> EXPIRED** — invoice expires unpaid; the order is dead (a later payment
  is not silently resurrected).
- **EXPIRED + late settlement -> REFUND_DUE** — if a payment settles after expiry (settle
  racing expiry), the order is not resurrected and the funds are **auto-refunded** (§6.4),
  never kept. The invoice carries the order id so this is detectable.
- **PENDING -> PROVISIONING** — first payment settles and is captured. Run
  `provision`, retried with backoff.
- **PROVISIONING -> ACTIVE** — provision succeeded; deliver credentials and set
  `paid_through = now + period`.
- **PROVISIONING -> REFUND_DUE** — provision failed permanently after retries.

**Refund path** (§6.4):
- **REFUND_DUE -> REFUNDED** — auto-refund to the buyer's `refund_dest` succeeded
  (terminal).
- **REFUND_DUE (stuck)** — the refund payment itself failed (payer offline, no
  liquidity); operator alerted, manual resolution. Funds never silently vanish.

**Renewal** (prepaid, renew before the date, §6.2):
- While **ACTIVE**, the Instance runs until `paid_through`. A renewal payment extends
  `paid_through` by `period`; early renewals stack, so renewing early never wastes
  time.
- At `soft_date` (= `paid_through - renew_lead`) the daemon starts NIP-17 renewal
  reminders and makes a renewal invoice available. This is a recommendation; the
  service is not interrupted.
- **ACTIVE -> SUSPENDED** — `paid_through` reached unpaid; run `suspend` (service
  interrupted, data kept).
- **SUSPENDED -> ACTIVE** — late renewal within retention; run `resume`; extend
  `paid_through`.
- **SUSPENDED -> TERMINATED** — retention ended; run `destroy` (purge data).

**Buyer-initiated:**
- **ACTIVE/SUSPENDED -> CANCELLED** — buyer cancels; run `suspend`, then `destroy`
  after retention. Remaining prepaid time is not refunded.

### 6.4 Provisioning atomicity and refunds

phoenixd and Fedimint cannot hold an invoice (accept-but-not-settle), so v1 captures
the first payment on settlement and provisions afterward rather than holding until
provision succeeds (ADR-0003). Two consequences:

- **Pre-flight before the first invoice.** The daemon validates params and checks the
  compute/network capacity a Recipe needs before issuing the bolt11, and **reserves** that
  capacity for the order (§9.3) so concurrent orders cannot race the last slot. Most
  provision failures are caught here, before any money moves.
- **Capture-then-refund on failure.** If `provision` still fails after capture, the
  daemon refunds. The buyer supplies a `refund_dest` in `order.request` (a BOLT12
  offer, preferred, or a Lightning address), which the daemon pushes to via phoenixd
  `payoffer` / `paylnaddress`. If the refund payment itself fails, the subscription
  stays `REFUND_DUE` and the operator is alerted.

Operators who require true provision-then-capture atomicity can run an **LND payment
backend** (native hold invoices) instead of phoenixd. That backend is a later option,
not v1.

### 6.5 Enforcement engine

A single periodic **reconcile loop** advances subscriptions: it scans those whose
`next_deadline <= now` and fires the due transition (remind at `soft_date`, `suspend`
at `paid_through`, `destroy` at retention end), recomputing `next_deadline` each time.
Transitions are idempotent and journaled to `event_log`, so a crash mid-hook cannot
double-run or wedge.

Because all dates are absolute wall-clock timestamps, the loop is **downtime-safe**: a
transition missed while the Box was off fires on restart. But suspension is **credited
for operator downtime** (ADR-0005): the daemon persists a heartbeat, and on restart it
shifts any renewal/suspend deadline that fell inside its downtime window forward by the
outage length and re-sends the reminder, so a buyer is never suspended for the operator's
outage. The buyer can also request a renewal invoice on demand (`renew.request`);
reminders are otherwise best-effort.

### 6.6 Durable handshake and crash recovery (M1a)

The money/delivery path is fully persisted so a crash never strands a payment (ADR-0009).
The PENDING subscription **is** the order, so a settlement always has a row to bind to.

- **Correlation:** each invoice carries a unique `external_id` binding it to its
  order/subscription; phoenixd's `createinvoice` takes it as `externalId` and returns it on
  settlement, so a settlement maps to exactly one invoice (`UNIQUE(external_id)`).
- **Idempotent capture:** `UPDATE invoice SET status='PAID' WHERE id=? AND status='OPEN'`
  plus the `PENDING -> PROVISIONING` move in one transaction; a replayed settlement (ws
  reconnect) affects 0 rows and is a no-op, so `paid_through` can't double-extend.
- **Delivery outbox:** `provision.ready` is written to an `outbox` row in the same
  transaction as `-> ACTIVE`; a sender drains it and retries until sent, so a crash after
  ACTIVE but before the DM cannot strand a paid buyer (also the dropped-DM resync answer).
- **Refund ledger:** a `refund_attempt` row records dest, amount, the `backend_payment_id`
  from `pay`, status, and attempts, so refunds never double-pay and stuck ones alert.

Crash-recovery (step -> durable record in one txn -> restart action):

| Step | Durable record | On restart |
|--|--|--|
| order placed | sub PENDING + invoice OPEN (external_id) | expired-invoice PENDING -> EXPIRED |
| settlement | invoice PAID + sub PROVISIONING | replay no-ops (status guard) |
| provision ok | sub ACTIVE + outbox row | unsent outbox -> resend |
| provision fail | sub REFUND_DUE + refund_attempt | retry `pay()` |
| late settle on EXPIRED | refund_attempt | retry `pay()` (order not resurrected) |

Provision hooks are idempotent and re-run if the Box crashes mid-`PROVISIONING`.

## 7. Service recipe spec

A recipe is a self-contained directory. The daemon never special-cases a service;
it only runs hooks and reads the manifest.

Recipes are **trusted code**: built into lnrent or authored by the Operator, run
with daemon privilege. There is no third-party recipe installation in v1 (ADR-0002).

```
recipes/wireguard/
  recipe.toml          # manifest: metadata, pricing, params, OS support
  provision            # executable: create Instance resources, print JSON result
  suspend              # executable: stop, keep data
  resume               # executable: start again
  destroy              # executable: purge Instance resources
  healthcheck          # executable: exit 0 if healthy
  nixos/               # optional: declarative module fragments for NixOS hosts
  debian/              # optional: imperative install scripts for Debian hosts
```

### 7.1 Manifest (`recipe.toml`)

```toml
[service]
id = "wireguard"
name = "WireGuard VPN"
summary = "Private WireGuard peer, 1 device, unmetered."
version = "0.1.0"
category = ["vpn", "privacy"]

[pricing]
amount_sat = 5000
period = "30d"
renew_lead = "7d"
retention = "7d"

[provisioning]
backend = "host"            # host | incus | libvirt | proxmox | cloud-hetzner ...
isolation = "none"          # none | container | vm
tier = "0"                  # honest security tier: 0 | 1 | 1.5 | 2 (ADR-0007, §9.1)
resources = { cpu = 0, mem_mb = 0, disk_gb = 0 }

[os]
supports = ["nixos", "debian"]

# Buyer-supplied parameters collected in the order, validated by the daemon.
[[params]]
key = "pubkey"
label = "Your WireGuard public key"
type = "string"
required = true
```

### 7.2 Hook contract

- Hooks are plain executables (bash, or any language). No daemon coupling.
- Input: environment variables + a JSON document on stdin describing the
  subscription (if the Instance is rented), the Instance, validated params, and
  host facts (OS, backend handles).
- Output: JSON on stdout. `provision` returns the **delivery payload** (the object
  DM'd to the buyer, e.g. a WireGuard config) plus internal handles the daemon
  records (container id, peer index) for later hooks.
- Exit non-zero = failure; the daemon does not advance state and alerts the operator.
- Hooks must be idempotent where possible (re-run safe).

### 7.3 OS awareness

- **NixOS host:** prefer declarative. A recipe's `nixos/` fragment is rendered into
  the host config (the operator's existing `/etc/nixos` workflow), or applied via a
  recipe-scoped flake/profile. No imperative package installs.
- **Debian host:** imperative. `debian/` scripts use apt + systemd units.
- The daemon exposes host facts so a single hook can branch, or the recipe can ship
  separate `nixos/` and `debian/` paths.

## 8. Subsystems and backends

The manager core drives a Box through trait-bounded subsystems. Recipes declare which
subsystems they use; the manager wires them. The same subsystems serve self-use and
rented Instances; only the rental layer differs.

v1 implements **Compute** (`host` + `incus`) and **Network** (WireGuard, firewall,
port allocation) fully. **Storage** and **Observability** ship as trait stubs and
fill in at M7.

### 8.1 Compute (`ComputeBackend`)

Where a workload runs. (Called `ProvisionBackend` in earlier drafts; recipes still
select it via the manifest's `provisioning.backend` field.)

```rust
trait ComputeBackend {
    fn create(&self, spec: &InstanceSpec) -> InstanceHandle;   // container/VM
    fn stop(&self, h: &InstanceHandle);
    fn start(&self, h: &InstanceHandle);
    fn destroy(&self, h: &InstanceHandle);
    fn exec(&self, h: &InstanceHandle, cmd: &[&str]) -> ExecResult;
}
```

- **host:** no isolation; runs directly on the Box (WireGuard peer, simple daemons).
- **incus (default for isolated Instances):** system containers and KVM VMs from one
  CLI/API, packaged in nixpkgs and Debian. Good single-box default.
- **libvirt/kvm, proxmox:** later adapters for operators already on those.
- **cloud (hetzner/DO/vultr):** thin API adapters for reselling a VPS, and (M7) for
  provisioning the Operator's own Box.

The **VM** service is a recipe whose `provision` hook calls the selected backend to
create a VM, injects the buyer's SSH key, and returns access details.

### 8.2 Network (`NetworkBackend`)

```rust
trait NetworkBackend {
    fn add_wireguard_peer(&self, cfg: &PeerSpec) -> PeerConfig;
    fn remove_wireguard_peer(&self, peer: &PeerId);
    fn open_port(&self, spec: &PortSpec) -> PortHandle;        // firewall + allocation
    fn close_port(&self, h: &PortHandle);
    // phased: ingress/reverse-proxy routes, DNS records
}
```

- v1: WireGuard peer management, firewall rules, per-Instance port allocation.
- phased: reverse-proxy/ingress (route a hostname to an Instance), DNS records.

### 8.3 Storage (`StorageBackend`) — phased (M7)

Volumes attached to Instances, snapshots, and backups (local + offsite). Trait stub
in v1.

### 8.4 Observability — phased (M7)

Read-only: Instance and Box status, logs, metrics, alerts. It never mutates state, so
it sits cleanly inside the AI-free invariant. Trait stub in v1.

## 9. Example services (v1 recipes)

| Recipe | Isolation | What provision does | Delivery payload |
|--|--|--|--|
| **wireguard** | host | add a peer, allocate IP | `.conf` / QR |
| **vm** (flagship) | vm (incus) | create VM, inject SSH key | host, port, user (+ security `tier`, §9.1) |
| **hermes** (>= Tier 1) | vm or container | create instance, run hermes install script, buyer brings LLM keys | SSH + `hermes` usage note |
| **fedimint** (>= Tier 1) | vm | run a `fedimintd` **guardian**, ready for the DKG setup ceremony with peer guardians | guardian admin URL + setup/connection code |

Notes:
- **hermes** = NousResearch/hermes-agent: Python 3.11+/Node, installed via its
  `install.sh`, config in `~/.hermes/`, buyer supplies their own LLM provider keys.
  Runs fully inside the Instance's sandbox. Confirms the AI-free-control-plane rule.
- **fedimint** = a single `fedimintd` **guardian** instance, provisioned ready to
  run the distributed key generation (DKG) setup ceremony and coordinate with other
  guardians to form a federation. Delivery payload is the guardian admin endpoint
  and the setup/connection code the buyer shares with peer guardians. Not a
  pre-formed federation, and not just a client.

### 9.1 VM rental security tiers

VM rental follows the security model in
[docs/security/vm-deployment-guidelines.md](docs/security/vm-deployment-guidelines.md)
(ADR-0007). A normal VM is not a cryptographic boundary against its host, so every VM
Listing advertises an **honest tier** and never claims more:

| Tier | Label | Guarantee |
|--|--|--|
| 0 | Basic VPS | No privacy guarantee against the host operator. |
| 1 | Encrypted VPS | Tenant owns the disk key; host stores ciphertext. Runtime still trusts the host. |
| 1.5 | Hardened VPS | Provider-encrypted, Secure Boot + TPM, per-VM keys, sVirt, audit logs, quarantine. Tenant still trusts the host. |
| 2 | Confidential VPS | Attested confidential VM (SEV-SNP / TDX); secrets released only after attestation. |

**M1 ships Tier 0**, labeled honestly. The tier ladder is the security roadmap (§15).
Governing rules from the guidelines, load-bearing for the design:

- **Never overclaim** (the guidelines' final rule: weaker claim, stronger implementation).
- The control-plane **node agent exposes only narrow VM ops** (create/start/stop/
  reboot/snapshot/rotate-key/health), never arbitrary shell or libvirt/QEMU args.
  This matches the AI-free control plane and ADR-0001.
- **Tenant-provided images are hostile**; their hooks never run on the host.
- Each host publishes a **signed security profile** (guidelines §25; `host_id` is the
  operator's Nostr key — secp256k1; the deployment doc's `ed25519` profile example is
  illustrative, lnrent standardizes on the Nostr key) that buyers read before renting.

The full guidelines govern host onboarding, encryption, isolation, attestation, and
the pre-launch test plan; they are the source of truth for VM security.

### 9.2 VM rental networking and reachability

Per [docs/security/vm-networking-reachability-guidelines.md](docs/security/vm-networking-reachability-guidelines.md)
(ADR-0008). This **supersedes the WireGuard-default stance of earlier drafts.** Principle:
VMs are private by default, host control is outbound-only, and public exposure is explicit,
per-service, and reversible. Reachability is **pluggable**, not one tunnel.

**Three planes** (never collapsed into one tunnel):

| Plane | Who | Default | M1 primitive |
|--|--|--|--|
| Host control (marketplace <-> host agent) | operator / agent | private, outbound-only | **Iroh** (Tor fallback) |
| Tenant management (tenant <-> VM: SSH, console, unlock) | buyer | private | **Iroh** session (Tor fallback); WireGuard advanced-optional |
| Public service (internet <-> tenant app) | public | opt-in per service | shared IPv4 published ports (frp / rathole); public IPv6 when the host has it |

**Per-VM baseline:** per-VM tap, private VM IP, default-deny inbound, anti-spoofing, no
VM-to-VM by default, no host-management access from the VM, **no metadata service**.
Policy is generated from a declarative per-VM network spec.

**Host control plane = outbound-only.** The host agent never needs a public inbound port:
no public SSH, no public libvirt/admin API. It opens an outbound authenticated connection
to the marketplace (Iroh-first; OpenZiti a serious alternative; Tor onion fallback for
recovery). This is what makes home / CGNAT / NAT'd hosts first-class.

**Tenant management = private by default.** The buyer reaches their VM (SSH, console,
unlock, file copy) over a marketplace-native **Iroh** session, with **Tor onion** fallback
for SSH / rescue / unlock. Raw **WireGuard** is an advanced-optional L3 mode, not the
default user-facing concept. So a VM's delivery payload is an Iroh connection ticket
(+ Tor fallback), not a WireGuard config.

**Public exposure = tenant-declared, per service.** None by default. The tenant declares
published services; each maps via an exposure adapter: shared IPv4 published ports
(frp / rathole) for the MVP, public IPv6 / dedicated IPv4 when the host has them, HTTP
ingress with TLS passthrough for web, Tor onion for privacy mode, Cloudflare-Tunnel-like
only as an optional adapter. Published ports / services are scarce host resources, so they
count toward capacity and are reserved on a PENDING order (§6.4).

**Tenant-facing question** is "how should this VM be reachable?" (private admin / public
web / public BTC-LN-Fedimint service / advanced network), not "WireGuard or public IP?".

**Hosts advertise network capabilities** (Iroh, Tor, public IPv6, dedicated IPv4, shared
ports, ingress, restrictions like blocked SMTP, max ports/VM) in their signed profile
(§9.1, guidelines §23), so a buyer picks a Listing whose reachability fits.

**Reachability != isolation != confidentiality.** Network reachability controls who can
connect; VM isolation controls what a tenant can affect; disk/memory protection controls
what the host can see. Separate domains; never conflate them in claims.

### 9.3 VM rental: capacity, images, lifecycle (Tier 0, M1)

**Capacity and reservation.** The Operator configures the host's rentable budget at
onboard (total RAM, disk, vCPU for rentals; the public-port range). Scarce resources are
RAM, disk, and published public ports (vCPU is oversubscribed). To kill the concurrent-
order race, capacity is **reserved at order time**, not at payment:

- Pre-flight (before issuing the invoice) checks `available >= requested` and, atomically
  via the store actor (ADR-0001), creates a **reservation** held for the order with a TTL
  = invoice expiry.
- Invoice expires unpaid -> reservation released. Paid -> reservation stays **HELD**
  through `PROVISIONING` and becomes **CONSUMED** only when the Instance reaches `ACTIVE`,
  so a concurrent order cannot reuse the slot mid-provision.
- SUSPENDED keeps the reservation (disk + ports held through retention); TERMINATED and
  REFUND_DUE release it.

`available = host budget - (active Instances + live reservations)`.

**Images and sizing.** M1 offers a small **curated** image set (guidelines §19: signed,
no default passwords, no embedded keys): Debian (default), Ubuntu LTS, and a NixOS cloud
image. Sizes are **fixed tiers**, each a separate Listing: e.g. `s` (1 vCPU / 1 GB /
20 GB), `m` (2 / 4 / 40), `l` (4 / 8 / 80). The order picks an offered image and supplies
an SSH **public** key. Cloud-init injects only that key — never secrets, no metadata
service (guidelines §20-21). Tenant-provided images are later (treated as hostile, need
sandboxed conversion).

**Lifecycle hooks (`vm` recipe, Incus backend):**
- `provision` — create the VM from the chosen image at the size tier; inject the SSH
  pubkey via cloud-init; attach the per-VM tap + generated firewall policy; bring up the
  **Iroh** management endpoint (+ Tor fallback); map any declared published ports
  (frp / rathole). Delivery payload: Iroh connection ticket + Tor fallback (+ published-
  port mappings if any).
- `suspend` — stop the VM; keep its disk and held reservation.
- `resume` — start the VM; re-establish the management endpoint.
- `destroy` — delete the VM + disk; release the reservation; tear down tap, firewall,
  Iroh endpoint, and any Tor onion / published ports.
- `healthcheck` — VM running and reachable over the management plane.

## 10. Claude skills (author-time and operator-time)

These never run in the serving path. They are how a human drives lnrent.

- **lnrent-onboard** — given an existing box reachable over **SSH with sudo**,
  connect, install `lnrentd` (Nix on NixOS, apt+systemd on Debian), pick payment
  backend (phoenixd or fedimint), pick compute backend, create or restore the
  operator **BIP39 seed** (deriving the master identity + this Box's operational key
  per NIP-06, §4.6) with a forced backup step, and set default relays.
- **lnrent-recipe** — scaffold, edit, and test a service recipe; dry-run the
  lifecycle hooks against a throwaway tenant.
- **lnrent-list** — compose and publish a NIP-99 listing for a recipe; price it.
- **lnrent-subs** — inspect subscriptions, payments, and lifecycle state; force a
  transition (manual suspend/resume) when needed.
- **lnrent-doctor** — diagnose: relay connectivity, payment backend health,
  compute backend health, stuck subscriptions.

## 11. Data model (sqlite)

```sql
CREATE TABLE operator (              -- single row, this Box's identity (seed lives in the data dir, not here)
  master_pubkey TEXT,                 -- brand identity (NIP-06 account 0)
  box_index INTEGER,                  -- this Box's derivation account
  op_pubkey TEXT,                     -- this Box's operational pubkey
  payment_backend TEXT, compute_backend TEXT, relays TEXT);

CREATE TABLE recipe (                -- mirror of on-disk recipes for fast lookup
  id TEXT PRIMARY KEY, version TEXT, manifest_json TEXT, listing_event_id TEXT);

CREATE TABLE subscription (
  id TEXT PRIMARY KEY,
  recipe_id TEXT, buyer_pubkey TEXT,
  state TEXT,                        -- see §6.3 (PENDING|PROVISIONING|ACTIVE|SUSPENDED|TERMINATED|EXPIRED|CANCELLED|REFUND_DUE|REFUNDED)
  params_json TEXT,                  -- validated buyer params
  refund_dest TEXT,                  -- BOLT12 offer or Lightning address, for refunds
  instance_handle_json TEXT,         -- backend handles for later hooks
  period_s INTEGER, renew_lead_s INTEGER, retention_s INTEGER,
  paid_through INTEGER,              -- hard expiry; service interrupted after this
  soft_date INTEGER,                 -- paid_through - renew_lead_s; renewal recommended from here
  next_deadline INTEGER,             -- reconcile-loop cursor
  created_at INTEGER, updated_at INTEGER);

CREATE TABLE invoice (
  id TEXT PRIMARY KEY, subscription_id TEXT,
  external_id TEXT UNIQUE,            -- unique per-invoice token; phoenixd externalId (ADR-0009)
  bolt11 TEXT, amount_sat INTEGER, status TEXT,   -- OPEN|PAID|EXPIRED
  issued_at INTEGER, settled_at INTEGER);

CREATE TABLE event_log (             -- audit trail of every transition + payment
  id INTEGER PRIMARY KEY, subscription_id TEXT, kind TEXT, detail_json TEXT, at INTEGER);

CREATE TABLE reservation (            -- capacity held for a PENDING order (§9.3)
  id TEXT PRIMARY KEY, order_id TEXT,
  resources_json TEXT,               -- {cpu, mem_mb, disk_gb}
  ports_json TEXT,                   -- requested published ports
  state TEXT,                        -- HELD|CONSUMED|RELEASED
  expires_at INTEGER, created_at INTEGER);

CREATE TABLE daemon_state (          -- single row; heartbeat for downtime credit (§6.5)
  last_heartbeat INTEGER);

CREATE TABLE refund_attempt (        -- durable refund ledger (ADR-0009)
  id TEXT PRIMARY KEY, subscription_id TEXT, dest TEXT, amount_sat INTEGER,
  backend_payment_id TEXT,           -- from PaymentBackend::pay, for status/dedup
  status TEXT,                       -- PENDING|SENT|FAILED
  attempts INTEGER, created_at INTEGER, updated_at INTEGER);

CREATE TABLE outbox (                -- pending operator->buyer NIP-17 DMs (ADR-0009)
  id TEXT PRIMARY KEY, recipient TEXT, subscription_id TEXT,
  msg_type TEXT, payload_json TEXT,
  state TEXT,                        -- PENDING|SENT
  attempts INTEGER, created_at INTEGER, sent_at INTEGER);
```

## 12. Deployment

- **NixOS:** ship a flake exposing a `lnrentd` package and a NixOS module
  (`services.lnrentd.enable = true` with options for backends, relays, key path).
  Recipes' `nixos/` fragments compose declaratively.
- **Debian:** ship a static-ish Rust binary + a systemd unit + an install script.
- Both store state under a single data dir (sqlite, operator key, recipe checkout).

## 13. Security

- The operator **BIP39 seed** and derived keys, plus payment-backend credentials,
  live in the data dir with tight perms; never in recipe output or logs.
- **Value plane is separated from the hosting plane (ADR-0010):** the wallet, seed, and
  master key live on the control node, never on a box hosting untrusted tenant VMs, so a
  tenant escape or box compromise cannot drain funds or steal the seed.
- Tenant isolation is the provisioning backend's job; the `host` backend (no
  isolation) is only for services that are safe to run unsandboxed (WireGuard).
- Hooks run with least privilege; the daemon passes secrets via stdin JSON, not
  argv or env where avoidable.
- Recipes are trusted code (built-in or operator-authored). v1 does not install or
  run untrusted/third-party recipes. See ADR-0002.
- Credential delivery is NIP-17 only (metadata-private). No plaintext creds on
  public Nostr events.
- Payment verification is settlement-based (backend confirms settled), never
  trusting a claimed payment.

## 14. Repo layout (proposed)

```
lnrent/
  SPEC.md                 # this file
  README.md
  daemon/                 # Rust: lnrentd + lnrent CLI
    src/
  recipes/                # built-in recipes
    wireguard/ vm/ hermes/ fedimint/
  skills/                 # Claude skills (author/operator-time)
    lnrent-onboard/ lnrent-recipe/ lnrent-list/ lnrent-subs/ lnrent-doctor/
  clients/
    core/                 # Rust buyer-core lib (DM protocol, order flow, gift-wrap; native + wasm32)
    cli/                  # thin native CLI over buyer-core
    web/                  # static WASM SPA over buyer-core (NIP-07 + WebLN + browser WS)
  nix/                    # flake + NixOS module
  packaging/debian/       # systemd unit + install script
  docs/                   # protocol notes, NIP mapping, ADRs
```

## 15. Milestones

- **M0 — Skeleton.** Repo, `lnrentd` skeleton, sqlite, recipe loader, and the
  subsystem traits (`ComputeBackend`, `NetworkBackend`, `PaymentBackend`) with
  `host` compute, WireGuard network, and `phoenixd` payment stubs.
- **M1a — Loop mechanics (handshake core).** Prove the order/payment/lifecycle handshake
  end to end with a trivial, instant recipe (a WireGuard peer or a dummy service), because
  the handshake is the product and the riskiest part, independent of any VM complexity:
  publish NIP-99 listing -> NIP-17 `order.request` -> pre-flight + reservation -> phoenixd
  invoice (`externalId` = order) -> settlement **watch** -> idempotent capture -> provision
  -> NIP-17 `provision.ready` -> reconcile-loop renew/suspend/destroy, with capture-then-
  refund, the settled-but-expired auto-refund, and crash recovery for the
  settle->capture->provision sequence. Single key (account 0). Minimal CLI buyer, then a **web WASM buyer** (shared buyer-core)
  proving the marketplace is browser-accessible via a headless-browser loop test. Pin the
  lnrent DM schema.
- **M1b — VM Tier-0 core.** Swap in the real `vm` recipe: Incus VM provisioning (curated
  images, fixed sizes, §9.3), per-VM tap/firewall + no metadata, **private** reachability
  (Iroh management + Tor fallback, §9.2), and capacity/reservation. Honest **Tier 0**
  Listing, private-only.
- **M1c — Public exposure.** Tenant-declared published services: shared IPv4 ports via
  frp/rathole, with the capacity accounting that ports imply (§9.2/§9.3).
- **M2 — More recipes + Tier 1.** Hermes and Fedimint-guardian recipes, **gated behind
  >= Tier 1** (tenant-managed LUKS) since they are sensitive workloads (guidelines §26);
  recipe-authoring skill polish.
- **M3 — Secondary payment backends.** **phoenixd** (self-custodial, for standalone
  operators with their own liquidity / higher-value payments) and an optional **LND**
  backend (native hold invoices) for true provision-then-capture atomicity (§6.4).
  Fedimint ecash is the primary backend, implemented in M1a (ADR-0012).
- **M4 — NixOS module + Debian packaging.** (The web WASM buyer is proven earlier, in M1a.)
- **M5 — Pre-fleet hardening.** Per-box key split + operator manifest (ADR-0004/0006);
  NWC (NIP-47) pull subscriptions; the **reputation primitive** (buyer-signed rental
  attestations to the master identity, ADR-0011) — which gates promoting the marketplace
  publicly (the web buyer is built/proven in M1a but not publicly launched until then).
- **M6 — Tier 1.5.** The guidelines' "minimum viable secure launch": Secure Boot + TPM +
  per-VM encryption + KMS-style key release + sVirt + remote audit logs + quarantine.
- **M7 — Manager breadth + Tier 2.** Storage (volumes/snapshots/backups), observability,
  multi-box fleet, box self-provisioning (cloud API / nixos-anywhere), public IPv6 /
  dedicated IPv4 / HTTP ingress / zrok / OpenZiti adapters, and **Tier 2** attested
  confidential VMs (SEV-SNP/TDX). Capabilities ship incrementally, not as one block.

The **security tier ladder** (Tier 0 -> 1 -> 1.5 -> 2; ADR-0007, §9.1) and the
**reachability ladder** (Iroh+Tor private planes -> shared ports -> IPv6 -> ingress ->
WireGuard advanced -> dedicated IPv4; ADR-0008, §9.2, guidelines §24) span the milestones
above rather than landing in one.

## 16. Open questions

Resolved in v0.2: Fedimint = single guardian for DKG (§9); NIP-90 dropped in favor
of an lnrent DM protocol over NIP-17 (§5); buyer clients = CLI + web (§3, §14);
operator box assumed to exist with SSH+sudo (§10); default to popular relays (§5.2).
Resolved in v0.5: daemon is the sole writer of state; skills act only through the
`lnrent` CLI (ADR-0001). Resolved in v0.7: capture-then-refund, no hold invoices
(ADR-0003, §6.4). Resolved in v0.8: operator identity is a single BIP39 seed deriving
a master identity + per-Box operational keys, NIP-06 (ADR-0004, §4.6). Resolved in
v0.9: prepaid-expiry subscriptions, renew before the date (ADR-0005, §6.2). Resolved
in v0.10: listing authenticity via a master-signed operator manifest; Listings signed
by operational keys (ADR-0006, §5.3). Resolved in v0.11-0.14: M1 wedge = VM rental;
honest security-tier model with M1 = Tier 0 (ADR-0007, §9.1); three-plane Iroh-first
reachability, WireGuard demoted (ADR-0008, §9.2); capacity reserved at order time (§9.3),
settling resource allocation and the concurrent-order race (runtime per-Instance limits
are enforced by the compute backend).

Still open:

1. **DKG coordination UX:** how do peer guardians discover and authenticate each
   other for the federation-creation ceremony? Out-of-band exchange of setup codes
   in v1, or a Nostr-mediated rendezvous?
2. **Web client trust surface:** require a NIP-07 browser signer + WebLN wallet, or
   ship an embedded key + manual bolt11 copy as a fallback? Affects how "no backend"
   the web client truly is.
3. **Listing updates:** price or availability changes mean re-publishing the
   `30402` event. Define update/withdraw semantics (replaceable-event `d` tag
   handling, sold-out signaling).
4. **Manifest revocation latency:** the operator manifest is cacheable (§5.3) but
   replaceable events have no push invalidation or TTL, so a revoked operational key
   keeps verifying against cached manifests until each buyer refetches. Define a
   manifest TTL / version so "immediate" revocation has a bound.
5. **Fleet topology (RESOLVED v0.20):** control node (value/identity/marketplace) +
   hosting boxes (disposable compute), driven over Iroh — ADR-0010, §4.5.
6. **Unified seed for payments (RESOLVED v0.23):** the Fedimint client root secret (the
   primary backend) derives from the operator BIP39 seed at a dedicated path, so one seed
   backs up identity + ecash funds (§4.6, ADR-0004/0012). phoenixd (secondary) keeps a
   separate channel-state seed.

## 17. Out of scope (v1)

- Central marketplace site, hosted directory, custodial escrow, disputes.
- Installing third-party/community recipes. Recipes are trusted code (ADR-0002); a
  signed recipe registry is a possible later direction, not v1.
- Fiat, on-chain-only settlement.
- Kubernetes-style clustering / HA across boxes. lnrent manages a fleet of boxes
  but does not cluster them under one scheduler.
- Any LLM/AI in the control plane (permanent rule, not just v1).
- SLAs, autoscaling, bin-packing guarantees.
