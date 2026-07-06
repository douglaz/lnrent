# 0008 — VM reachability: three planes, private by default, Iroh-first

Adopts `docs/security/vm-networking-reachability-guidelines.md` (an addendum to the VM
deployment guidelines, ADR-0007). **This supersedes the WireGuard-as-default networking
stance in SPEC §9.2 up to v0.12.**

The reachability system separates three planes and never collapses them into one
tunnel: host control (marketplace <-> host agent), tenant management (tenant <-> VM),
and public service (internet <-> tenant app). VMs are private by default; the host
control plane is outbound-only (no public SSH, no public admin API); public exposure is
explicit, per-service, reversible, and tied to the tenant's payment/reputation state.

Reachability is pluggable, not one fixed primitive:

- **Host control + tenant management:** marketplace-native **Iroh** (P2P QUIC with NAT
  traversal + relay fallback), OpenZiti as a serious alternative, **Tor onion** fallback
  for recovery / SSH / unlock. This makes home / CGNAT / NAT'd hosts first-class.
- **Public service:** shared IPv4 published ports (frp / rathole) for the MVP, public
  IPv6 / dedicated IPv4 when the host has them, HTTP ingress with TLS passthrough for
  web, Tor onion for privacy, Cloudflare-Tunnel-like only as an optional adapter.
- **WireGuard** is demoted to an advanced-optional L3 mode, not the central abstraction.

We chose this over WireGuard-centric networking because WireGuard is a network interface,
not a marketplace reachability system: by itself it gives no service publishing,
NAT-traversal coordination, relay fallback, browser access, or tenant-friendly UX, and
forcing it to serve all three planes is the central mistake the addendum warns against.
Iroh fits the home-lab / NAT'd hosts that are a core lnrent use case, where WireGuard,
public IPs, and port-forwarding each fail or require inbound reachability the host does
not have.

## Consequences

- The VM recipe's delivery payload becomes a marketplace-native connection (an Iroh
  ticket + Tor fallback), not a WireGuard config.
- Hosts advertise pluggable network capabilities in their signed profile (guidelines
  §23); buyers pick a Listing whose reachability fits, asked "how should this VM be
  reachable?" not "WireGuard or public IP?".
- Reachability, isolation, and confidentiality are separate security domains; product
  claims must never conflate them.
- MVP order (guidelines §24): per-VM tap/firewall -> no metadata -> outbound-only agent
  -> Iroh control + management -> Tor fallback -> shared IPv4 publishing. M1 ships the
  private planes (Iroh + Tor); public publishing is the fast-follow.

> **Revision (2026-07-05):** "M1" was later split M1a/M1b. M1a landed with the cloud-DO
> recipe delivering plain public-IPv4 SSH (`{"host":<ipv4>,"port":22,"user":"root"}`) and
> no Iroh/Tor in the tree; the private planes above land with M1b's self-hosted VM recipe
> (SPEC roadmap). The decision itself is unchanged.
