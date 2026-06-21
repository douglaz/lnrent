# lnrent — Spec (draft v0.1)

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
- **Buyer (v1, thin):** has a Nostr identity and a Lightning wallet. Discovers
  offers, pays invoices, receives credentials over an encrypted Nostr DM.
- **Marketplace (long-term, two-sided):** emerges from many operators publishing to
  shared Nostr relays. No central party owns it. v1 ships the operator half well;
  the buyer half is a minimal reference client plus the published event spec.

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

### 4.3 Trust model

- Buyer pays the first period invoice **before** provisioning. Operator provisions
  on confirmed payment. No escrow.
- Operator reputation accrues to their Nostr pubkey (reviews, longevity). v1 does
  not build a reputation system; it leaves the pubkey-as-identity hook so one can
  be layered on (e.g. NIP-32 labels, web-of-trust).
- Credentials are delivered only over NIP-17 gift-wrapped DMs (sender and recipient
  hidden from relays).

## 5. Marketplace over Nostr

Follow Nostr best practices. Mapping:

| Concern | Mechanism | Kind |
|--|--|--|
| Storefront / offer listing | NIP-99 classified listing | `30402` |
| Order / provision request + paid-job loop | NIP-90 Data Vending Machine | request `5xxx`, result `6xxx`, feedback `7000` |
| Invoice delivery, credential delivery, billing notices | NIP-17 private DM (gift-wrapped) | `1059` wrap |
| Auto-renew pull payments (later) | NIP-47 Nostr Wallet Connect | — |

Flow:

1. Operator publishes a **listing** (`30402`) per offered service: title, summary,
   price (`price` tag: amount, `SAT`, `month`), category `t` tags, `d` identifier.
2. Buyer sends a **NIP-90 job request** ("provision service X with these params").
3. `lnrentd` replies with a **NIP-90 feedback** (`7000`, status `payment-required`)
   carrying a bolt11 invoice (or a Fedimint-routable invoice).
4. On payment, `lnrentd` runs the recipe's `provision` hook, then delivers
   credentials as a **NIP-17 DM**, and emits a NIP-90 **job result** (`6xxx`) with
   non-sensitive status only.
5. Renewal notices and suspend/terminate warnings go out as NIP-17 DMs.

Exact kind numbers in the NIP-90 range get pinned in M1 against the current spec.

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
| **fedimint** | vm or container | bring up a Fedimint client/guardian-adjacent instance against config | endpoint + admin creds |

Notes:
- **hermes** = NousResearch/hermes-agent: Python 3.11+/Node, installed via its
  `install.sh`, config in `~/.hermes/`, buyer supplies their own LLM provider keys.
  Runs fully inside the tenant sandbox. Confirms the AI-free-control-plane rule.
- **fedimint** scope (run a guardian vs run a client/gateway-adjacent service) is an
  open question, see §15.

## 10. Claude skills (author-time and operator-time)

These never run in the serving path. They are how a human drives lnrent.

- **lnrent-onboard** — set up a box: install `lnrentd`, pick payment backend
  (phoenixd or fedimint), pick provisioning backend, generate the operator Nostr
  key, configure relays.
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
  nix/                    # flake + NixOS module
  packaging/debian/       # systemd unit + install script
  docs/                   # protocol notes, NIP mapping, ADRs
```

## 15. Milestones

- **M0 — Skeleton.** Repo, `lnrentd` skeleton, sqlite, recipe loader, `PaymentBackend`
  + `ProvisionBackend` traits with `host` and `phoenixd` stubs.
- **M1 — WireGuard proof-of-life (MVP).** Full loop on one box: publish listing ->
  NIP-90 order -> phoenixd invoice -> pay -> `provision` peer -> NIP-17 deliver
  config -> renew/suspend/destroy via the state machine. Pin NIP-90 kinds here.
- **M2 — VM-for-others.** Incus backend, `vm` recipe, SSH-key injection, delivery.
- **M3 — More recipes.** Hermes and Fedimint recipes; recipe-authoring skill polish.
- **M4 — Fedimint payment backend.** Receive via existing federation + gatewayd as
  an alternative to phoenixd.
- **M5 — Buyer reference client + NixOS module + Debian packaging.**
- **M6 — v2 hands-off:** NWC (NIP-47) pull subscriptions; reputation hooks.

## 16. Open questions

1. **Fedimint recipe scope:** does "Fedimint instance" mean run a full guardian, a
   client/gateway-adjacent service, or a single-guardian dev federation? Each is a
   very different recipe.
2. **NIP-90 vs NIP-15:** confirm NIP-90 DVM is the right ordering protocol for
   *ongoing subscriptions* (it is job-shaped). NIP-15 stalls/orders is an alt for
   the order step. Decide during M1.
3. **Buyer client:** how much do we build vs rely on existing Nostr clients that
   speak NIP-90? v1 may ship a minimal CLI buyer only.
4. **Provisioning the box itself:** you mentioned provisioning VMs for others as
   the first external service. Do we also want lnrent to provision the *operator's
   own* box from bare metal (cloud-init/nixos-anywhere), or assume an existing box
   in v1? (Spec currently assumes existing box; "sell VMs" is the M2 service.)
5. **Relay strategy:** which relays, and do operators run their own relay for their
   listings/orders to avoid dropped events?

## 17. Out of scope (v1)

- Central marketplace site, hosted directory, custodial escrow, disputes.
- Fiat, on-chain-only settlement, multi-box orchestration/clustering.
- Any LLM/AI in the control plane (permanent rule, not just v1).
- SLAs, autoscaling, bin-packing guarantees.
