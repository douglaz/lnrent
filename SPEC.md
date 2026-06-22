# lnrent — Spec (draft v0.2)

> Working codename: **lnrent** (rename later). Daemon: `lnrentd`. CLI: `lnrent`.
> Status: DRAFT for review. Author-time tooling = Claude skills. Runtime = pure Rust/bash.

## 1. What this is

lnrent lets a person with a box (a rented VPS or a home-lab machine) install a
self-hostable service, list it on a Nostr-based marketplace, and rent it to others
on a Lightning-settled subscription. Operators run everything themselves. There is
no central marketplace server and no central payment custodian.

The product is two things:

1. **A small always-on control plane** (`lnrentd`, Rust) that watches for payments,
   provisions and tears down services, enforces subscriptions, and speaks Nostr.
2. **A set of Claude skills** used by a human operator at author-time and setup-time
   to onboard a box, write service recipes, publish listings, and inspect state.

First services: **WireGuard VPN**, **VM-for-others** (provisioning as a service),
**Hermes agent** (NousResearch/hermes-agent), **Fedimint instance**. These are
recipes, not hardcoded. Adding a service means dropping in a new recipe.

## 2. Goals and non-goals

### Goals
- An operator can go from a bare NixOS or Debian box to a published, rentable
  service in a few skill-driven steps.
- A buyer with a Nostr key and a Lightning wallet can discover an offer, pay, and
  receive working credentials, with no account and no operator-side AI.
- Subscriptions are enforced automatically: pay to start, keep paying to stay up,
  lapse means grace then suspend then destroy.
- New services are added as self-contained recipes without touching daemon code.
- The whole thing runs on NixOS (declarative) and Debian (imperative).

### Non-goals (v1)
- No central marketplace website or hosted directory. Discovery is Nostr-native.
- No custodial escrow or dispute resolution. Trust model is "pay-then-provision,
  reputation over Nostr."
- No fiat. No on-chain-only path (Lightning first; on-chain is a later fallback).
- No AI anywhere in the runtime serving path (hard rule, see §4).
- No multi-tenant bin-packing guarantees or SLAs. Best-effort, operator-owned.

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
        listings (30402) / orders (NIP-90) / DMs (NIP-17)
                              |
   +--------------------------+---------------------------+
   |                       lnrentd (Rust)                 |
   |  nostr engine | billing engine | lifecycle engine    |
   |        sqlite state | payment backend trait          |
   +----+----------------+-------------------+------------+
        |                |                   |
   PaymentBackend   ProvisionBackend    Recipe runner
   - phoenixd       - incus (default)   - provision/suspend/
   - fedimint       - libvirt/kvm         resume/destroy/
     (federation+   - proxmox             healthcheck hooks
      gatewayd)     - cloud (hetzner...)
```

Claude skills sit beside this, not inside it. They read and write the same sqlite
state and recipe files, but only when a human runs them.

Buyers use the lnrent **CLI** or **static web client**. Both connect directly to
relays and to the buyer's own Lightning wallet, with no lnrent server in between.
The web client is a static SPA: it signs with a NIP-07 browser extension (or a
locally held key) and pays via WebLN or a copied bolt11.

### 4.3 Trust model

- Buyer pays the first period invoice **before** provisioning. Operator provisions
  on confirmed payment. No escrow.
- Operator reputation accrues to their Nostr pubkey (reviews, longevity). v1 does
  not build a reputation system; it leaves the pubkey-as-identity hook so one can
  be layered on (e.g. NIP-32 labels, web-of-trust).
- Credentials are delivered only over NIP-17 gift-wrapped DMs (sender and recipient
  hidden from relays).

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
| `order.request`   | buyer -> operator | `listing_id`, validated `params` |
| `order.invoice`   | operator -> buyer | `order_id`, `bolt11`, `amount_sat`, `period`, `expires_at` |
| `order.error`     | operator -> buyer | `order_id`, `reason` |
| `provision.ready` | operator -> buyer | `subscription_id`, `payload` (the credentials) |
| `billing.invoice` | operator -> buyer | `subscription_id`, `bolt11`, `amount_sat`, `due_at` |
| `billing.notice`  | operator -> buyer | `subscription_id`, `state`, `message` (grace/suspend/terminate) |
| `sub.cancel`      | buyer -> operator | `subscription_id` |

Payment is settled out-of-band with the buyer's own Lightning wallet. lnrentd
confirms settlement through its `PaymentBackend` (phoenixd/Fedimint), never by
trusting a Nostr message that claims payment.

### 5.2 Relays

Default to a set of popular public relays (operator-overridable). Operator-run or
service-specific relays come later, once listing and order-DM delivery reliability
on public relays is understood.

## 6. Payments and subscriptions

### 6.1 Payment backends

A `PaymentBackend` trait abstracts receiving:

```rust
trait PaymentBackend {
    fn create_invoice(&self, amount_sat: u64, memo: &str, expiry_s: u32) -> Invoice;
    fn watch(&self) -> PaymentStream;            // stream of settled payments
    fn lookup(&self, id: &InvoiceId) -> PaymentStatus;
}
```

- **phoenixd (default):** self-custodial, HTTP API + websocket for settled events,
  auto-manages channel liquidity. Best fit for a solo operator.
- **fedimint:** connect to an **existing federation** and route through an
  **existing gatewayd**. Receives to ecash; invoices are gateway-issued bolt11.
  Reuses the operator's federation membership rather than running a guardian.

Operator picks one backend per box in config. Both expose the same trait.

### 6.2 Subscription model: push billing (v1)

Push model, agreed:

- Service runs while paid. Each period the operator issues a fresh invoice.
- Non-payment moves the subscription through a state machine (below), ending in
  suspend and then destroy.
- **NWC pull** (NIP-47), where the buyer grants a budgeted wallet connection and the
  operator auto-charges, is the v2 hands-off upgrade. The state machine is designed
  so pull is a drop-in trigger for the renewal step.

### 6.3 Subscription state machine

```
            first payment            period elapses, invoice issued
  PENDING --------------------> ACTIVE -----------------------------> DUE
     |  (no pay before expiry)    ^                                    |
     v                            | payment received                  | grace window
  EXPIRED                         +------------------------------------+  elapses
                                  |              (renew)               |
                                  |                                    v
   buyer cancels: ACTIVE/DUE -> CANCELLED -> (suspend) -> SUSPENDED -> GRACE
                                                              |          |
                                            retention elapses |          | payment in grace
                                                              v          v
                                                         TERMINATED    ACTIVE
```

Timers per recipe/listing (operator-tunable): `period` (e.g. 30d), `grace`
(e.g. 3d, service still up), `retention` (e.g. 7d after suspend, data kept before
destroy).

- **ACTIVE -> DUE:** invoice issued, NIP-17 reminder sent.
- **DUE -> GRACE:** period ended unpaid; service stays up, sterner notice.
- **GRACE -> ACTIVE:** payment lands; `resume` not needed (never stopped).
- **GRACE -> SUSPENDED:** grace ended unpaid; run `suspend` hook (stop, keep data).
- **SUSPENDED -> ACTIVE:** late payment; run `resume` hook.
- **SUSPENDED -> TERMINATED:** retention ended; run `destroy` hook (purge data).

## 7. Service recipe spec

A recipe is a self-contained directory. The daemon never special-cases a service;
it only runs hooks and reads the manifest.

```
recipes/wireguard/
  recipe.toml          # manifest: metadata, pricing, params, OS support
  provision            # executable: create tenant resources, print JSON result
  suspend              # executable: stop, keep data
  resume               # executable: start again
  destroy              # executable: purge tenant resources
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
grace = "3d"
retention = "7d"

[provisioning]
backend = "host"            # host | incus | libvirt | proxmox | cloud-hetzner ...
isolation = "none"          # none | container | vm
resources = { cpu = 0, mem_mb = 0, disk_gb = 0 }

[os]
supports = ["nixos", "debian"]

# Buyer-supplied parameters collected in the NIP-90 order, validated by the daemon.
[[params]]
key = "pubkey"
label = "Your WireGuard public key"
type = "string"
required = true
```

### 7.2 Hook contract

- Hooks are plain executables (bash, or any language). No daemon coupling.
- Input: environment variables + a JSON document on stdin describing the
  subscription, tenant, validated params, and host facts (OS, backend handles).
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

## 8. Provisioning backends

A `ProvisionBackend` trait abstracts where a tenant workload runs:

```rust
trait ProvisionBackend {
    fn create(&self, spec: &TenantSpec) -> TenantHandle;   // container/VM
    fn stop(&self, h: &TenantHandle);
    fn start(&self, h: &TenantHandle);
    fn destroy(&self, h: &TenantHandle);
    fn exec(&self, h: &TenantHandle, cmd: &[&str]) -> ExecResult;
}
```

- **host:** no isolation; the service runs directly on the operator box (WireGuard).
- **incus (default for isolated tenants):** system containers and KVM VMs from one
  CLI/API, packaged in nixpkgs and Debian. Good single-box default.
- **libvirt/kvm, proxmox:** later adapters for operators already on those.
- **cloud (hetzner/DO/vultr):** thin API adapters for "resell a VPS" later.

The **VM-for-others** flagship service is a recipe whose `provision` hook calls the
selected backend to create a VM, injects the buyer's SSH key, and DMs back the IP +
access details.

## 9. Example services (v1 recipes)

| Recipe | Isolation | What provision does | Delivery payload |
|--|--|--|--|
| **wireguard** | host | add a peer, allocate IP | `.conf` / QR |
| **vm** (flagship) | vm (incus) | create VM, inject SSH key | host, port, user |
| **hermes** | vm or container | create tenant, run hermes install script, buyer brings LLM keys | SSH + `hermes` usage note |
| **fedimint** | vm | run a `fedimintd` **guardian**, ready for the DKG setup ceremony with peer guardians | guardian admin URL + setup/connection code |

Notes:
- **hermes** = NousResearch/hermes-agent: Python 3.11+/Node, installed via its
  `install.sh`, config in `~/.hermes/`, buyer supplies their own LLM provider keys.
  Runs fully inside the tenant sandbox. Confirms the AI-free-control-plane rule.
- **fedimint** = a single `fedimintd` **guardian** instance, provisioned ready to
  run the distributed key generation (DKG) setup ceremony and coordinate with other
  guardians to form a federation. Delivery payload is the guardian admin endpoint
  and the setup/connection code the buyer shares with peer guardians. Not a
  pre-formed federation, and not just a client.

## 10. Claude skills (author-time and operator-time)

These never run in the serving path. They are how a human drives lnrent.

- **lnrent-onboard** — given an existing box reachable over **SSH with sudo**,
  connect, install `lnrentd` (Nix on NixOS, apt+systemd on Debian), pick payment
  backend (phoenixd or fedimint), pick provisioning backend, generate the operator
  Nostr key, and set default relays.
- **lnrent-recipe** — scaffold, edit, and test a service recipe; dry-run the
  lifecycle hooks against a throwaway tenant.
- **lnrent-list** — compose and publish a NIP-99 listing for a recipe; price it.
- **lnrent-subs** — inspect subscriptions, payments, and lifecycle state; force a
  transition (manual suspend/resume) when needed.
- **lnrent-doctor** — diagnose: relay connectivity, payment backend health,
  provisioning backend health, stuck subscriptions.

## 11. Data model (sqlite)

```sql
CREATE TABLE operator (              -- single row, this box's identity
  nostr_pubkey TEXT, payment_backend TEXT, provision_backend TEXT, relays TEXT);

CREATE TABLE recipe (                -- mirror of on-disk recipes for fast lookup
  id TEXT PRIMARY KEY, version TEXT, manifest_json TEXT, listing_event_id TEXT);

CREATE TABLE subscription (
  id TEXT PRIMARY KEY,
  recipe_id TEXT, buyer_pubkey TEXT,
  state TEXT,                        -- PENDING|ACTIVE|DUE|GRACE|SUSPENDED|TERMINATED|EXPIRED|CANCELLED
  params_json TEXT,                  -- validated buyer params
  tenant_handle_json TEXT,           -- backend handles for later hooks
  period_s INTEGER, grace_s INTEGER, retention_s INTEGER,
  current_period_end INTEGER, next_deadline INTEGER,
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

- Operator Nostr key and payment-backend credentials live in the data dir with
  tight perms; never in recipe output or logs.
- Tenant isolation is the provisioning backend's job; the `host` backend (no
  isolation) is only for services that are safe to run unsandboxed (WireGuard).
- Hooks run with least privilege; the daemon passes secrets via stdin JSON, not
  argv or env where avoidable.
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

- **M0 — Skeleton.** Repo, `lnrentd` skeleton, sqlite, recipe loader, `PaymentBackend`
  + `ProvisionBackend` traits with `host` and `phoenixd` stubs.
- **M1 — WireGuard proof-of-life (MVP).** Full loop on one box: publish NIP-99
  listing -> NIP-17 `order.request` -> phoenixd invoice -> pay -> `provision` peer
  -> NIP-17 `provision.ready` -> renew/suspend/destroy via the state machine. Pin
  the lnrent DM protocol schema here. Includes a minimal CLI buyer to drive the loop.
- **M2 — VM-for-others.** Incus backend, `vm` recipe, SSH-key injection, delivery.
- **M3 — More recipes.** Hermes and Fedimint guardian recipes; recipe-authoring
  skill polish.
- **M4 — Fedimint payment backend.** Receive via existing federation + gatewayd as
  an alternative to phoenixd.
- **M5 — Web buyer client + NixOS module + Debian packaging.**
- **M6 — v2 hands-off:** NWC (NIP-47) pull subscriptions; reputation hooks.

## 16. Open questions

Resolved in v0.2: Fedimint = single guardian for DKG (§9); NIP-90 dropped in favor
of an lnrent DM protocol over NIP-17 (§5); buyer clients = CLI + web (§3, §14);
operator box assumed to exist with SSH+sudo (§10); default to popular relays (§5.2).

Still open:

1. **DKG coordination UX:** how do peer guardians discover and authenticate each
   other for the federation-creation ceremony? Out-of-band exchange of setup codes
   in v1, or a Nostr-mediated rendezvous?
2. **Web client trust surface:** require a NIP-07 browser signer + WebLN wallet, or
   ship an embedded key + manual bolt11 copy as a fallback? Affects how "no backend"
   the web client truly is.
3. **Resource enforcement:** per-tenant CPU/mem/disk and port allocation are in the
   manifest but not yet enforced. Which limits does the daemon enforce vs delegate
   to the provisioning backend?
4. **Listing updates:** price or availability changes mean re-publishing the
   `30402` event. Define update/withdraw semantics (replaceable-event `d` tag
   handling, sold-out signaling).

## 17. Out of scope (v1)

- Central marketplace site, hosted directory, custodial escrow, disputes.
- Fiat, on-chain-only settlement, multi-box orchestration/clustering.
- Any LLM/AI in the control plane (permanent rule, not just v1).
- SLAs, autoscaling, bin-packing guarantees.
