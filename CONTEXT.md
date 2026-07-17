# lnrent

lnrent is an operator-run VPS manager: it manages compute, networking, storage, and
services on one or more boxes, and can rent any managed service to others on a
Lightning-settled subscription discovered over a Nostr marketplace. This file fixes
the domain language. It is a glossary, not a spec; see SPEC.md for design.

## Language

### Actors

**Operator**:
The person who owns one or more Boxes, installs Recipes, publishes Listings, and
receives payment. One Operator has a single brand (Master identity); each Box uses a
derived Operational key (see Identity), all from one Operator seed. An Operator is a
third party, not this project's authors: lnrent exists to create an ecosystem of many
independent Operators, so anything Operator-facing is product surface.
_Avoid_: seller, host, vendor; "provider" only in the fixed framing phrase "ecosystem of
independent service providers"

**Buyer**:
A Nostr identity that rents a service by paying its invoices. Has no account; the
Buyer's Nostr pubkey is the only identifier.
_Avoid_: customer, client, user; "tenant" is reserved for the VM-isolation context (a
Buyer's workload occupying an Instance — "tenant Instances", the §9 planes), never the
marketplace actor

### Infrastructure

**Box**:
A machine lnrent manages, reachable over SSH with sudo (a rented VPS or a home-lab
host). An Operator may manage several Boxes (a fleet). Instances live on a Box. A Box
plays one of two roles (ADR-0010): the Control node or a Hosting box.
_Avoid_: server, host, node, machine

**Control node**:
The Operator's value/identity/marketplace plane: holds the receiving wallet + the hot
marketplace Operational key, and the marketplace control (listings, order DMs, billing). The
Operator seed + Master identity stay cold/offline (used only to issue the manifest) — except
the M1a all-in-one box, where the seed is on the box. Never hosts untrusted tenant Instances.
(ADR-0010)
_Avoid_: coordinator, server.

**Hosting box**:
A Box that runs tenant Instances. Holds only a revocable Operational key (to authenticate
to the Control node and sign its Host security profile) — no funds, no seed. Disposable
compute. (ADR-0010)
_Avoid_: tenant box, worker, node.

### Identity

**Operator seed**:
The single BIP39 mnemonic an Operator backs up. Every operator key derives from it: the
Nostr identity keys (NIP-06) and the Fedimint client root secret (HKDF domain
`lnrent:fedimint:v1`, §4.6). One backup covers identity AND the ecash client — but restoring
the ecash position also needs the federation invite/config (the seed alone is not enough), so
backup must include it. _Avoid_: wallet, private key.

**Master identity**:
The Operator's brand, a Nostr key derived from the Operator seed at account 0.
Reputation accrues here, not to any Box. _Avoid_: root key, operator key.

**Operational key**:
A Nostr key derived from the Operator seed (account >= 1), in one of two roles (ADR-0010):
the **marketplace** operational key (on the Control node) signs Listings and handles buyer
DMs; a **hosting-box** operational key signs that box's Host security profile and
authenticates it to the Control node. Both are linked to the Master identity by a
master-signed operator manifest. (M1a single-box: account-0 plays all roles.) _Avoid_: box
key, device key.

**Operator manifest**:
A master-signed, replaceable Nostr event listing the Operational keys the Master
identity vouches for. Buyers verify a Listing by checking its signing key appears
here. _Avoid_: keylist, attestation.

**Rental attestation**:
A buyer-signed Nostr event (NIP-32 label) vouching that an Operator fulfilled a rental,
accruing to that Operator's Master identity. The web buyer aggregates these,
web-of-trust-weighted, as the reputation signal. (ADR-0011)
_Avoid_: review, rating, feedback.

### Unit of sale

**Service**:
The human-facing category of a thing for rent (WireGuard VPN, a VM, Hermes). A
label used in conversation, not a stored entity. Implemented by a Recipe.
_Avoid_: product, offering

**Recipe**:
The concrete implementation of one Service on a box: its manifest plus lifecycle
hooks. One Service maps to one Recipe (v1). Never sold directly; it is the template
a Listing prices. One Recipe backs many Listings.
_Avoid_: template, package, app, module

**Listing**:
A priced, published offer for one Recipe, signed to Nostr as a classified listing.
Pins concrete pricing and parameter presets. One Recipe -> many Listings.
_Avoid_: offer, ad, post, product

**Order**:
A Buyer's request against a Listing, before payment. Transient: it expires if the
first invoice is not paid. An Order becomes a Subscription on first settlement.
_Avoid_: cart, checkout, request, job

**Subscription**:
The durable paid relationship between a Buyer and a Listing. Prepaid to a Paid-through
date and renewed before it. Carries the lifecycle state (pending, provisioning, active,
resuming, suspended, terminated, plus expired, cancelled, refund-due, refunded; SPEC §6.3).
One Subscription owns one Instance (v1).
_Avoid_: plan, contract, lease, membership; the removed `due`/`grace` states

**Instance**:
The actual provisioned resource lnrent manages: a WireGuard peer, a VM, a container
running Hermes, a fedimintd guardian, a managed network or volume. Owned either by
the Operator directly (self-use) or by one Subscription (rented). One Subscription
-> one Instance (v1).
_Avoid_: tenant, node, deployment, server

### Billing

**Payment backend**:
The wallet the Operator chooses for a Control node: it receives Subscription payments
and pays refunds and Sweeps. Exactly one per Control node, chosen at onboarding.
The real choices: Fedimint ecash (the Operator joins a federation of their choosing —
a federation is picked, not inherited) or phoenixd (self-custodial Lightning, no
federation). Both are reasonable options; ecash for those who want a federation,
phoenixd for those who don't. (`mock` exists for development only.)
_Avoid_: wallet, node, payment provider.

**Paid-through date**:
The hard expiry timestamp of a Subscription. The Instance runs until this date; past
it, unpaid, the service is interrupted. A renewal payment extends it by one period.
_Avoid_: due date, expiry, renewal date.

**Soft date**:
A recommendation timestamp before the Paid-through date (paid-through minus a lead
window) from which the Operator nudges the Buyer to renew, so renewing early avoids
interruption. Not a hard transition.
_Avoid_: grace, reminder date.

**Receipt**:
A payment the Operator's books have recorded as received for a specific Order or
renewal. A receipt is *at risk* while any path could still owe it back to the Buyer,
and *final* once the service outcome makes it irrevocably the Operator's.
_Avoid_: deposit, balance entry, payment (ambiguous with the Buyer-side act).

**Surplus**:
The portion of the Operator's received money that is provably theirs to keep: final
receipts minus everything still owed or at risk. Computed from the books alone — never
from the wallet balance (ADR-0016).
_Avoid_: profit (accounting term), balance, available funds.

**Sweep**:
The Operator withdrawing Surplus out of the daemon's wallet to their own wallet. The
only Operator-initiated outbound payment; refused whenever the owed amount cannot be
bounded. (Target — ADR-0016 / docs/specs/gate1-operator-sweep.md; no sweep command
exists in M1a yet.)
_Avoid_: withdrawal, payout (ambiguous with refunds), cash out.

**Reconcile (operator act)**:
The explicit, on-demand comparison of the wallet against the books, reporting whether
they agree. The single sanctioned reading of the wallet balance; it informs a human and
never authorizes anything. (Target — in M1a `lnrent money` still reads the live balance
and no reconcile command exists yet; ADR-0016 / gate1 specs land this.) Distinct from the *reconcile loop* (the timer-driven
subscription state machine walker, SPEC §6.5) — same word, different thing; say
"reconcile command" vs "reconcile loop" when ambiguous.
_Avoid_: audit, balance check.

### VM security

**Security tier**:
The honestly-advertised privacy guarantee of a VM Listing: Tier 0 (no guarantee vs
the host), 1 (tenant-encrypted disk), 1.5 (hardened provider-encrypted host), 2
(attested confidential VM). A Listing must never claim above its tier. See
docs/security/vm-deployment-guidelines.md.
_Avoid_: security level, privacy mode.

**Host security profile**:
A signed record a Host publishes — signed by its `host_op_pubkey` and naming the
`operator_master_pubkey` it belongs to (§9.1) — declaring its tier, hardware, boot integrity,
encryption, operations posture, and network capabilities, so Buyers can verify what they are
trusting.
_Avoid_: host manifest (that is the operator manifest), attestation.

**Reachability plane**:
One of three independent VM networking planes — host control (marketplace<->agent),
tenant management (tenant<->VM), public service (internet<->app) — each private by
default with its own primitive, never collapsed into one tunnel. See
docs/security/vm-networking-reachability-guidelines.md.
_Avoid_: network mode, tunnel.

**Native connect**:
The marketplace-native private session a Buyer uses to manage their VM (SSH, console,
unlock): Iroh-first, Tor onion fallback. WireGuard is an advanced-optional mode, not
this.
_Avoid_: VPN, the tunnel.

## Example dialogue

**Dev:** A buyer wants the "5-device WireGuard" thing. Is that a Recipe?
**Operator:** No. The Recipe is just "WireGuard" on my box. "5-device, 20k sats/mo"
is a Listing over that Recipe. I also publish a "1-device, 5k" Listing from the
same Recipe.
**Dev:** So when they pay, what gets created?
**Operator:** Their Order turns into a Subscription the moment the first invoice
settles. The Subscription then provisions one Instance: an actual WireGuard peer
with their key. They keep paying to keep that Subscription **current** (paid through the
date); there is no grace period — past the hard date it suspends, then is destroyed after
retention.
**Dev:** And if they stop paying?
**Operator:** The Subscription walks its state machine to suspended, then the
Instance is destroyed at the end of retention. The Listing and Recipe are
untouched; only their Subscription and its Instance go away.
