# lnrent — Spec (draft v0.10)

> Working codename: **lnrent** (rename later). Daemon: `lnrentd`. CLI: `lnrent`.
> Status: DRAFT for review. Author-time tooling = Claude skills. Runtime = pure Rust/bash.

## 1. What this is

lnrent is a **VPS manager**. An operator points it at a box (eventually several)
reachable over SSH with sudo and manages everything on it from one control plane:
virtual machines and containers, networking, storage, and the services running on
top. On top of management, lnrent can **rent any managed service to others**,
settled in Bitcoin Lightning and discovered over a Nostr marketplace. No central
marketplace server, no central payment custodian.

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
  the buyer half is two reference clients (CLI + static web) plus the published
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

Buyers use the lnrent **CLI** or **static web client**. Both connect directly to
relays and to the buyer's own Lightning wallet, with no lnrent server in between.
The web client is a static SPA: it signs with a NIP-07 browser extension (or a
locally held key) and pays via WebLN or a copied bolt11.

### 4.3 Trust model

- Buyer pays the first period invoice **before** provisioning. Operator provisions
  on confirmed payment. No escrow.
- Operator reputation accrues to the **master operator identity** (§4.6), not to any
  Box. v1 does not build a reputation system; it leaves the identity hook so one can
  be layered on (e.g. NIP-32 labels, web-of-trust).
- Credentials are delivered only over NIP-17 gift-wrapped DMs (sender and recipient
  hidden from relays).

### 4.4 Resource model

Everything the manager tracks with a lifecycle is an **Instance**: a VM, a container,
a WireGuard peer, a volume, a fedimintd guardian. An Instance records its kind, the
backend handles needed to manage it later, its owner (the Operator for self-use, or
one Subscription for rented), its Box, and its state. Recipes produce Instances;
direct manager operations also produce them. Settings applied to a Box or an Instance
(a firewall rule, a DNS record) are configuration, not Instances.

### 4.5 Fleet topology

- **v1:** one `lnrentd` runs locally on the single Box it manages. Skills and the
  `lnrent` CLI target that daemon.
- **Fleet (M7):** an Operator manages several Boxes. The aggregation topology (a
  central control node driving boxes over SSH, vs one `lnrentd` per Box plus a
  coordinator, vs a per-Box agent) is an open decision, see §16. Whatever wins,
  lnrent manages a fleet but does not cluster boxes under one scheduler (§17).

### 4.6 Identity and key management

All operator keys derive from a single **BIP39 seed** (the one thing to back up).
Derivation follows NIP-06 (`m/44'/1237'/<account>'/0/0`, secp256k1, the Nostr coin
type), so every key is regenerable from the seed:

- **Master identity** — the operator's brand, derived at account 0. Reputation accrues
  here. It signs an **operator manifest**: a replaceable, master-signed event listing
  the operational pubkeys that act on the brand's behalf (app-defined; no NIP fits).
- **Operational key (per Box)** — derived at account = Box index. Each Box signs its
  Listings and receives/decrypts buyer NIP-17 DMs with its own operational key, so the
  master key need not be hot on every Box. Buyers verify a Listing by checking the
  master manifest covers its signing key.
- **Payment backend seed** — ideally also derived from the same BIP39 seed so one
  backup covers funds. Whether phoenixd accepts an imported seed is unverified (§16);
  if not, its seed is backed up separately in v1.

Custody and rotation:
- The **seed and master key stay off the Boxes** where practical (generated on the
  operator's machine; only the derived operational key is deployed to a Box). A Box
  compromise then leaks only that Box's operational key, which the master revokes by
  re-issuing the manifest; the seed and reputation survive.
- v1 single-Box operators may keep the seed on the Box for convenience, accepting that
  a Box compromise is then a seed compromise. Onboard makes the backup explicit.
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
  30000 range with a fixed `d` tag (exact kind pinned in M1). Replaceable, so the
  master updates it to add or revoke Boxes.
- Each Listing (kind `30402`) carries an `operator` tag naming the master pubkey.

Buyer verification (CLI and web both run this):

1. Read Listing `L`, signed by operational key `K`.
2. Read `L`'s `operator` tag -> master pubkey `M`.
3. Fetch `M`'s operator manifest; verify it is signed by `M` and that `K` is in it.
4. If `K` is not attested, treat `L` as unverified and do not show it as `M`'s.

This binds a Listing to a brand without the master key being hot: an attacker can put
`M` in an `operator` tag, but cannot get their key into `M`'s manifest. Reputation
attaches to `M`, so buyers compare brands, not Boxes. **Revocation:** the master
re-publishes the manifest without a compromised Box's key, and that Box's Listings
immediately stop verifying. The manifest is cacheable per operator; a Listing is
unverifiable only while the manifest cannot be fetched (relay gap).

## 6. Payments and subscriptions

### 6.1 Payment backends

A `PaymentBackend` trait abstracts receiving:

```rust
trait PaymentBackend {
    fn create_invoice(&self, amount_sat: u64, memo: &str, expiry_s: u32) -> Invoice;
    fn watch(&self) -> PaymentStream;            // stream of settled payments
    fn lookup(&self, id: &InvoiceId) -> PaymentStatus;
    fn pay(&self, dest: &RefundDest, amount_sat: u64) -> PayResult;   // outbound, for refunds
}
```

- **phoenixd (default):** self-custodial, HTTP API + websocket for settled events,
  auto-manages channel liquidity. Best fit for a solo operator.
- **fedimint:** connect to an **existing federation** and route through an
  **existing gatewayd**. Receives to ecash; invoices are gateway-issued bolt11.
  Reuses the operator's federation membership rather than running a guardian.
- Neither phoenixd nor Fedimint supports **hold invoices**, so v1 captures on
  settlement and refunds on failure (§6.4, ADR-0003). An **LND** backend (native
  holds) is a later option for operators who want provision-then-capture.

Operator picks one backend per box in config. All expose the same trait.

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
  compute/network capacity a Recipe needs before issuing the bolt11. Most provision
  failures are caught here, before any money moves.
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

Because all dates are absolute wall-clock timestamps, the loop is **downtime-safe**:
if the Box was off across a deadline, the transition fires on restart. The buyer is
expected to renew before `paid_through` (nudged early from `soft_date`), so suspension
at the hard date is the agreed outcome regardless of operator uptime. Reminders are
best-effort; the buyer can also request a renewal invoice on demand (`renew.request`).

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
| **vm** (flagship) | vm (incus) | create VM, inject SSH key | host, port, user |
| **hermes** | vm or container | create instance, run hermes install script, buyer brings LLM keys | SSH + `hermes` usage note |
| **fedimint** | vm | run a `fedimintd` **guardian**, ready for the DKG setup ceremony with peer guardians | guardian admin URL + setup/connection code |

Notes:
- **hermes** = NousResearch/hermes-agent: Python 3.11+/Node, installed via its
  `install.sh`, config in `~/.hermes/`, buyer supplies their own LLM provider keys.
  Runs fully inside the Instance's sandbox. Confirms the AI-free-control-plane rule.
- **fedimint** = a single `fedimintd` **guardian** instance, provisioned ready to
  run the distributed key generation (DKG) setup ceremony and coordinate with other
  guardians to form a federation. Delivery payload is the guardian admin endpoint
  and the setup/connection code the buyer shares with peer guardians. Not a
  pre-formed federation, and not just a client.

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
  bolt11 TEXT, amount_sat INTEGER, status TEXT,   -- OPEN|PAID|EXPIRED
  issued_at INTEGER, settled_at INTEGER);

CREATE TABLE event_log (             -- audit trail of every transition + payment
  id INTEGER PRIMARY KEY, subscription_id TEXT, kind TEXT, detail_json TEXT, at INTEGER);
```

## 12. Deployment

- **NixOS:** ship a flake exposing a `lnrentd` package and a NixOS module
  (`services.lnrentd.enable = true` with options for backends, relays, key path).
  Recipes' `nixos/` fragments compose declaratively.
- **Debian:** ship a static-ish Rust binary + a systemd unit + an install script.
- Both store state under a single data dir (sqlite, operator key, recipe checkout).

## 13. Security

- The operator **BIP39 seed** and derived keys, plus payment-backend credentials,
  live in the data dir with tight perms; never in recipe output or logs. The seed and
  master key stay off Boxes where practical (§4.6).
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
    cli/                  # Rust: lnrent buyer CLI
    web/                  # static web client (SPA over relays + NIP-07 + WebLN)
  nix/                    # flake + NixOS module
  packaging/debian/       # systemd unit + install script
  docs/                   # protocol notes, NIP mapping, ADRs
```

## 15. Milestones

- **M0 — Skeleton.** Repo, `lnrentd` skeleton, sqlite, recipe loader, and the
  subsystem traits (`ComputeBackend`, `NetworkBackend`, `PaymentBackend`) with
  `host` compute, WireGuard network, and `phoenixd` payment stubs.
- **M1 — WireGuard proof-of-life (MVP).** Full loop on one box: publish NIP-99
  listing -> NIP-17 `order.request` -> pre-flight -> phoenixd invoice -> pay
  (capture) -> `provision` peer -> NIP-17 `provision.ready` -> renew/suspend/destroy
  via the state machine, with capture-then-refund on provision failure (§6.4). Pin
  the lnrent DM protocol schema here. Includes a minimal CLI buyer to drive the loop.
- **M2 — Compute management.** Incus backend: create/start/stop/destroy VMs and
  system containers, for the operator's own use and for rent (`vm` recipe, SSH-key
  injection, delivery).
- **M3 — More recipes.** Hermes and Fedimint guardian recipes; recipe-authoring
  skill polish.
- **M4 — Fedimint payment backend.** Receive via existing federation + gatewayd as
  an alternative to phoenixd. Optional **LND backend** (native hold invoices) for
  operators wanting true provision-then-capture atomicity (§6.4).
- **M5 — Web buyer client + NixOS module + Debian packaging.**
- **M6 — v2 hands-off:** NWC (NIP-47) pull subscriptions; reputation hooks.
- **M7 — Manager breadth.** Networking (firewall, port allocation, ingress/reverse
  proxy, DNS), storage (volumes, snapshots, backups), observability, multi-box
  fleet, and provisioning the box itself (cloud API / nixos-anywhere). This is where
  "do everything" lands; capabilities ship incrementally, not as one block.

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
by operational keys (ADR-0006, §5.3).

Still open:

1. **DKG coordination UX:** how do peer guardians discover and authenticate each
   other for the federation-creation ceremony? Out-of-band exchange of setup codes
   in v1, or a Nostr-mediated rendezvous?
2. **Web client trust surface:** require a NIP-07 browser signer + WebLN wallet, or
   ship an embedded key + manual bolt11 copy as a fallback? Affects how "no backend"
   the web client truly is.
3. **Resource enforcement:** per-Instance CPU/mem/disk and port allocation are in
   the manifest but not yet enforced. Which limits does the daemon enforce vs
   delegate to the compute backend?
4. **Listing updates:** price or availability changes mean re-publishing the
   `30402` event. Define update/withdraw semantics (replaceable-event `d` tag
   handling, sold-out signaling).
5. **Fleet topology:** central control node over SSH, one `lnrentd` per Box plus a
   coordinator, or a per-Box agent? (M7.)
6. **Unified seed for payments:** can phoenixd (and the Fedimint client) be
   initialized from the operator's BIP39 seed so one backup covers funds too? If not,
   payment seeds are backed up separately in v1. (Verify, §4.6.)

## 17. Out of scope (v1)

- Central marketplace site, hosted directory, custodial escrow, disputes.
- Installing third-party/community recipes. Recipes are trusted code (ADR-0002); a
  signed recipe registry is a possible later direction, not v1.
- Fiat, on-chain-only settlement.
- Kubernetes-style clustering / HA across boxes. lnrent manages a fleet of boxes
  but does not cluster them under one scheduler.
- Any LLM/AI in the control plane (permanent rule, not just v1).
- SLAs, autoscaling, bin-packing guarantees.
