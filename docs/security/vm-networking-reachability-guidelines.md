# VM Networking and Reachability Guidelines

These notes are an addendum to the safe VM deployment guidelines. They focus on how VMs should be reachable in a marketplace where hosts may be home laptops, machines behind CGNAT, dedicated servers, colocated machines, or professional datacenter hosts.

The core conclusion is:

> Do not make WireGuard, public IPs, or port forwarding the single required answer. Make VMs private by default, make host control outbound-only, and support pluggable reachability backends for different use cases.

The recommended default architecture is:

```text
Host control plane:
  outbound-only
  no public host admin ports
  preferably Iroh or OpenZiti style connectivity
  Tor onion fallback for recovery

Tenant management plane:
  private by default
  Iroh session or equivalent app-level tunnel
  Tor onion fallback for SSH, rescue, and unlock
  WireGuard only as an advanced optional mode

Public service plane:
  explicit opt-in per service
  routed IPv6 or dedicated IPv4 when available
  shared IPv4 port publishing when necessary
  HTTP ingress or Cloudflare Tunnel-like adapter for web apps
  Tor onion service for privacy mode
```

## 1. Design goals

The networking system should support these properties:

1. Hosts can live behind NAT, CGNAT, firewalls, or residential routers.
2. The host agent should not require public inbound ports.
3. The host should not expose public SSH or a public admin API.
4. Each VM should have an isolated network interface and generated firewall policy.
5. VMs should be private by default.
6. Public exposure should be explicit, auditable, and service-scoped.
7. Tenant management should not depend on a raw public SSH port.
8. There should be no EC2-style metadata service by default.
9. The marketplace should support classic VPS behavior when the host has routable addresses.
10. The marketplace should also support laptop and home-node hosts that cannot offer public IPs.

## 2. Separate the planes

Do not solve every networking problem with one tunnel.

There are three distinct planes:

```text
1. Host control plane
   marketplace <-> host agent

2. Tenant management plane
   tenant <-> VM SSH, console, unlock, rescue, file copy

3. Public service plane
   internet users <-> tenant service
```

Each plane has different needs.

| Plane | Who uses it | Default exposure | Best primitives |
|---|---|---:|---|
| Host control | Marketplace and host agent | Private only | Iroh, OpenZiti, Tor fallback, maybe WireGuard |
| Tenant management | Tenant and VM | Private only | Iroh session, Tor fallback, optional WireGuard |
| Public service | Public users and tenant apps | Opt-in only | Public IP, port publishing, ingress, Tor onion, Cloudflare Tunnel-like adapter |

The biggest design mistake would be choosing one primitive, such as WireGuard, and forcing it to serve all three planes.

## 3. Why WireGuard should not be the central abstraction

WireGuard is excellent technology. It is simple, fast, and uses cryptokey routing, where public keys are associated with allowed tunnel IPs.

But WireGuard is a network interface, not a full marketplace reachability system.

WireGuard gives you:

```text
L3 tunnel interface
static peer identity
allowed IP routing
strong encryption
simple operation
```

It does not give you by itself:

```text
service publishing
marketplace discovery
relay fallback
NAT traversal coordination
browser access
hostname routing
per-service policy
payment-aware access
tenant-friendly UX
VM console semantics
image upload semantics
LUKS unlock workflow
```

This makes raw WireGuard feel too much like "bring your own VPN topology". It is useful for advanced tenants and some operators, but it should not be the default user-facing concept.

Recommended position:

```text
WireGuard:
  optional advanced networking mode
  useful for L3 private networks
  not the default marketplace control substrate
```

## 4. VM network baseline

Every VM should be created with an isolated virtual interface.

Recommended baseline:

```text
per-VM tap interface
private VM IP
anti-spoofing rules
default-deny inbound firewall
no direct VM-to-VM traffic by default
no host management access from VM
no metadata service
explicit outbound policy
explicit public service publication
```

The host should generate network policy from a declarative VM spec.

Example:

```yaml
vm_network:
  vm_id: vm_01hxyz
  interface: tap-vm-01hxyz
  mac: "02:00:00:7a:12:44"
  ipv4: "10.77.12.8/32"
  ipv6: "fd42:market:12::8/128"

  inbound:
    default: deny

  vm_to_vm:
    default: deny

  outbound:
    default: allow
    rate_limit: true

  host_access:
    default: deny
    allow:
      - dhcp
      - dns_if_platform_provided

  metadata_service:
    enabled: false

  anti_spoofing:
    enforce_mac: true
    enforce_ip: true
    block_rogue_dhcp: true
    block_ipv6_ra: true
```

## 5. Public IP per VM

This is the cleanest classic VPS model.

```text
VM gets:
  public IPv6 address or prefix
  maybe public IPv4 address if available
  default-deny inbound firewall
  tenant explicitly opens services
```

Best fit:

```text
classic VPS behavior
Bitcoin nodes
Lightning nodes
Fedimint gateways
generic internet servers
protocols that expect direct reachability
premium datacenter hosts
```

Pros:

```text
simple mental model
works with arbitrary protocols
avoids weird port remapping
supports tenant-controlled network services
best compatibility with existing VPS expectations
```

Cons:

```text
IPv4 scarcity
DDoS exposure
abuse handling burden
not available on many home nodes
requires correct routing and firewalling by the host
```

Recommendation:

```text
Prefer public IPv6 when available.
Offer dedicated IPv4 as a premium capability.
Never make public inbound open by default.
```

## 6. Shared IPv4 plus published ports

This is the practical MVP fallback for home hosts and low-cost nodes.

```text
host_public_ip:32022 -> vm_a:22
host_public_ip:39735 -> vm_a:9735
host_public_ip:38080 -> vm_b:80
```

Do not allocate exactly one port per VM. Allocate explicit published services per VM.

Example:

```yaml
published_services:
  - name: ssh
    protocol: tcp
    external_port: 32022
    internal_port: 22
    source_acl:
      - tenant_admin_ips

  - name: lightning
    protocol: tcp
    external_port: 39735
    internal_port: 9735
    source_acl:
      - "0.0.0.0/0"
      - "::/0"
```

Best fit:

```text
MVP public TCP exposure
residential hosts
small public services
Bitcoin and Lightning ports
low-cost nodes with one IPv4
```

Pros:

```text
works behind one public IPv4
simple relay model
easy to meter
works with many non-HTTP protocols
```

Cons:

```text
not a full VPS experience
standard ports conflict between tenants
poor UX for HTTP unless paired with hostname ingress
not ideal for protocols that expect fixed ports
abuse attribution is shared by host IP
```

Recommendation:

```text
Support this as a fallback.
Make it explicit in the marketplace listing.
Label it as shared IPv4 port publishing, not dedicated VPS IP.
```

## 7. Host control plane should be outbound-only

The host agent should never need inbound public reachability.

Bad default:

```text
public SSH to host
public libvirt API
public node-agent API
public admin web UI
```

Good default:

```text
host agent opens outbound connection
marketplace authenticates host identity
host publishes capabilities
host receives signed jobs
host streams logs and health data
host never exposes public management ports
```

Possible substrates:

```text
Iroh
OpenZiti
zrok
Tor onion fallback
WireGuard only if operationally justified
```

## 8. Cloudflare Tunnel pattern

Cloudflare Tunnel is useful because it lets a machine expose selected services without requiring a public IP or open inbound ports. The origin runs `cloudflared`, which makes outbound-only connections to Cloudflare. Cloudflare then routes traffic to the configured origin service.

The pattern is attractive:

```text
host or VM behind NAT
  outbound tunnel to edge
  no inbound firewall holes
  edge exposes selected services
```

This is excellent for:

```text
web apps
HTTP APIs
SSH through an access gateway
home-hosted services
CGNAT hosts
users who already trust Cloudflare
```

But for this marketplace, Cloudflare Tunnel should not be the core primitive:

```text
centralized dependency
account and DNS dependency
policy and ToS dependency
Cloudflare sees metadata
TLS termination choices affect tenant privacy
not a generic raw VPS network
```

Recommended position:

```text
Cloudflare Tunnel-like exposure:
  optional adapter for web and convenience use cases
  not the native control substrate
  not the only public exposure mechanism
```

## 9. Open-source Cloudflare Tunnel-like options

The closest open-source options fall into different categories.

| Tool | Best fit | Self-hostable | Role in marketplace |
|---|---|---:|---|
| zrok | Cloudflare Tunnel or ngrok-style sharing | Yes | Higher-level sharing and service exposure |
| OpenZiti | Zero-trust service network | Yes | Serious identity and policy substrate |
| frp | Reverse proxy for TCP, UDP, HTTP, HTTPS | Yes | Simple public port publishing |
| rathole | Minimal Rust reverse proxy | Yes | Lightweight tunnel primitive |
| Pangolin | Identity-aware access and reverse proxy | Yes | Product-like access layer, WireGuard-based |
| boringproxy | Simple web tunnel for self-hosters | Yes | Small web exposure tool |
| chisel | TCP and UDP tunnel over HTTP, secured via SSH | Yes | Rescue and primitive tunnel use |
| Tor onion services | Private anonymous reachability | Yes | Recovery, private admin, privacy mode |
| Iroh | P2P QUIC connectivity with NAT traversal and relays | Yes, as infrastructure and libraries | Native marketplace control and management substrate |

## 10. zrok and OpenZiti

zrok is probably the closest product-level open-source alternative to Cloudflare Tunnel or ngrok-style sharing.

It is useful if you want:

```text
self-hostable sharing platform
public and private service exposure
friendly tunnel UX
service publishing rather than raw VPN routing
OpenZiti foundation underneath
```

OpenZiti is the deeper platform. It is a zero-trust networking system where services are authenticated by identity, authorized by policy, and encrypted end to end.

Interpretation:

```text
zrok:
  higher-level sharing product
  better for MVP tunnel UX

OpenZiti:
  lower-level zero-trust substrate
  better for long-term marketplace identity and policy
```

Marketplace fit:

```text
Use zrok if you want an open-source Cloudflare Tunnel-like product quickly.
Use OpenZiti if you want marketplace identity and service authorization to be core infrastructure.
```

## 11. frp

frp is a practical reverse proxy for exposing services behind NAT or firewalls. It supports TCP, UDP, HTTP, and HTTPS.

Architecture:

```text
marketplace relay:
  frps

host machine:
  frpc

VM service:
  10.77.12.8:9735

public:
  relay.example.com:39735 -> VM:9735
```

Best fit:

```text
Bitcoin node ports
Lightning node ports
Fedimint service ports
SSH rescue ports
generic TCP services
MVP shared port publishing
```

Pros:

```text
simple
mature
self-hostable
supports many protocols
fits one-public-relay architecture
```

Cons:

```text
not a full zero-trust platform
identity and billing must be built around it
abuse controls must be built around it
policy model is less native than OpenZiti
```

Recommendation:

```text
Use frp as a pragmatic MVP for shared public ports.
Do not confuse it with a full marketplace security model.
```

## 12. rathole

rathole is a lightweight Rust reverse proxy for NAT traversal.

Best fit:

```text
small auditable tunnel primitive
Rust-friendly stack
selected TCP services
simple relay architecture
```

Pros:

```text
small surface area
Rust implementation
good fit for explicit service forwarding
```

Cons:

```text
less product-like than zrok
less policy-rich than OpenZiti
requires marketplace layer around it
```

Recommendation:

```text
Consider rathole if you want a smaller tunnel component than frp.
Choose it if Rust ecosystem and minimalism matter more than feature breadth.
```

## 13. Pangolin

Pangolin is an open-source identity-based remote access platform that combines tunneled reverse proxy and VPN-style private access. It is built on WireGuard.

Best fit:

```text
Cloudflare Access-like UX
browser access to web apps
identity-aware private access
teams and internal tools
```

Caveat:

```text
Pangolin is user-friendly, but it is still WireGuard-based internally.
That may be acceptable if users do not need to manually manage WireGuard topology.
```

Recommendation:

```text
Evaluate it as a product-layer option.
Do not adopt it blindly if the goal is to avoid WireGuard as the underlying model.
```

## 14. boringproxy and chisel

boringproxy is a simple tunneling reverse proxy with a web UI and automatic HTTPS. It is good for self-hosted web services.

chisel is a fast TCP and UDP tunnel over HTTP, secured via SSH. It is a useful primitive for passing through restrictive networks.

Marketplace position:

```text
boringproxy:
  useful for small web exposure
  less ideal for multi-tenant VPS networking

chisel:
  useful as rescue tunnel or implementation primitive
  not enough as the main marketplace substrate
```

## 15. Tor onion services

Tor onion services are very interesting for this marketplace.

They provide:

```text
no public IP requirement
no inbound host ports
service location hiding
self-authenticating onion address
TCP service exposure
privacy-friendly admin access
```

Best fit:

```text
SSH fallback
LUKS unlock fallback
host rescue access
private admin endpoints
anonymous service publication
Bitcoin and Lightning privacy mode
```

Limitations:

```text
higher latency
TCP only
mainstream users may not want Tor
not ideal for public web UX
onion addresses are awkward for humans
```

Recommendation:

```text
Implement Tor onion services as a standard fallback and privacy mode.
Do not make Tor the only public reachability model.
```

Example:

```yaml
management_recovery:
  tor_onion:
    enabled: true
    services:
      - name: ssh
        target: "vm:22"
      - name: unlock
        target: "vm:2222"
```

## 16. Iroh

Iroh is likely the best fit for the native marketplace control and tenant-management plane.

Iroh is designed for peer-to-peer connections across NATs and firewalls. It uses QUIC, NAT traversal, direct connections where possible, and relay fallback when direct connectivity does not work.

Iroh is not a generic L3 network interface like WireGuard. That is a feature for this use case. You can build application-level workflows instead of asking users to join a VPN.

Native marketplace commands could look like:

```text
marketctl host enroll
marketctl host health
marketctl vm ssh <vm-id>
marketctl vm console <vm-id>
marketctl vm unlock <vm-id>
marketctl vm upload-image <vm-id>
marketctl vm forward <vm-id> 8080:localhost:80
marketctl vm logs <vm-id>
```

Iroh fit:

```text
host agent identity
tenant identity
VM management sessions
console proxy
SSH proxy
image upload
file transfer
logs
health checks
LUKS unlock
payment-aware control messages
```

Recommendation:

```text
Use Iroh as the native marketplace connectivity substrate.
Use relays controlled by the marketplace or the open ecosystem.
Let direct P2P happen when possible.
Fall back to relays when needed.
Keep public service publishing separate.
```

## 17. Recommended architecture

```text
                          ┌─────────────────────────┐
                          │ Marketplace control     │
                          │ registry, relays, KMS   │
                          │ reputation, payments    │
                          └────────────┬────────────┘
                                       │
                              Iroh or OpenZiti
                               Tor fallback
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│ Host machine                                                                 │
│                                                                               │
│  host-agent                                                                   │
│    - outbound-only native endpoint                                             │
│    - no public SSH by default                                                  │
│    - no public admin API                                                       │
│    - optional Tor onion fallback                                               │
│    - signed capability profile                                                 │
│                                                                               │
│  VM network                                                                   │
│    - per-VM tap                                                               │
│    - default-deny inbound firewall                                             │
│    - anti-spoofing                                                            │
│    - no metadata service                                                       │
│                                                                               │
│  exposure adapters                                                            │
│    - public IPv6 route                                                         │
│    - dedicated IPv4 or 1:1 NAT                                                 │
│    - shared IPv4 published ports                                               │
│    - HTTP ingress                                                              │
│    - zrok or OpenZiti                                                          │
│    - frp or rathole                                                            │
│    - Tor onion services                                                        │
│    - Cloudflare Tunnel optional                                                │
└───────────────────────────────────────────────────────────────────────────────┘
```

## 18. Tenant-facing reachability choices

Do not ask tenants:

```text
Do you want WireGuard or public IP?
```

Ask:

```text
How should this VM be reachable?
```

Then offer these product choices:

```text
Private admin only:
  native marketplace connect
  Tor recovery fallback

Public web app:
  marketplace ingress
  zrok or OpenZiti exposure
  Cloudflare Tunnel optional
  public IP if host supports it

Public Bitcoin, Lightning, or Fedimint service:
  public IPv6 or dedicated IPv4 preferred
  shared public port if necessary
  Tor onion as optional extra

Advanced network:
  WireGuard
  custom routing
  dedicated interface
```

## 19. Declarative service exposure

The VM should not have ad hoc port forwards. Public exposure should be declared.

Example:

```yaml
network:
  default_private: true

  management:
    native_connect:
      backend: iroh
      enabled: true

    tor_recovery:
      enabled: true
      services:
        - ssh
        - unlock

  public_services:
    - name: web
      protocol: tcp
      internal_port: 443
      exposure: ingress_hostname
      hostname: "app.example.com"
      tls: passthrough

    - name: lightning
      protocol: tcp
      internal_port: 9735
      exposure: public_port
      requested_external_port: 9735

    - name: ssh
      protocol: tcp
      internal_port: 22
      exposure: private_only
```

## 20. TLS policy for web ingress

For HTTP and HTTPS ingress, support two modes.

```text
TLS passthrough:
  marketplace routes by SNI
  tenant keeps TLS private key
  better privacy
  less platform-managed convenience

TLS termination:
  marketplace terminates HTTPS
  easier certificates and WAF
  worse privacy claim
  marketplace sees plaintext HTTP
```

Recommended default:

```text
Use TLS passthrough when tenant privacy matters.
Allow TLS termination only with clear labeling.
```

## 21. Metadata service policy

Do not provide an EC2-style metadata service by default.

Problems with metadata services:

```text
SSRF target
secret leakage path
host-controlled trust surface
surprising tenant behavior
requires additional hardening and versioning
```

Recommended policy:

```text
metadata service:
  disabled by default
  never exposed at a magic link-local address by default
  any metadata must be tenant-approved
  secrets must not be injected by host metadata
```

If metadata is needed later, prefer explicit tenant pull:

```text
tenant authenticates to marketplace
tenant fetches config over an end-to-end authenticated session
VM does not blindly trust host-local metadata
```

## 22. Abuse and safety model

Public reachability creates abuse risk.

Every exposure adapter should support:

```text
per-tenant quotas
per-VM rate limits
port allowlists and denylists
DDoS reaction hooks
abuse contact routing
logs of exposure changes
signed exposure declarations
automatic shutdown policy for severe abuse
clear terms for public relays
```

For shared public IPs:

```text
abuse attribution is harder
one tenant can harm host reputation
relay IPs need monitoring
outbound spam needs controls
```

For Tor:

```text
abuse complaints may be different
public discovery is limited
latency and availability vary
```

For Cloudflare-like tunnels:

```text
provider policy matters
edge provider can suspend service
central dependency must be disclosed
```

## 23. Marketplace host capability profile

Hosts should advertise their networking capabilities.

Example:

```yaml
host_network_capabilities:
  native_connect:
    iroh: true
    openziti: false

  recovery:
    tor_onion: true

  public_reachability:
    public_ipv6: true
    dedicated_ipv4: false
    shared_ipv4_ports: true
    http_ingress: true
    cloudflare_tunnel: optional
    zrok: false
    frp: true
    rathole: false

  restrictions:
    blocks_outbound_smtp: true
    blocks_public_udp: false
    max_published_tcp_ports_per_vm: 5
    max_bandwidth_mbps: 100

  security:
    default_deny_inbound: true
    anti_spoofing: true
    no_metadata_service: true
    vm_to_vm_default_deny: true
```

## 24. Recommended MVP

Build the MVP in this order:

```text
1. Per-VM tap and firewall isolation
2. No metadata service
3. Host agent outbound-only
4. Iroh-based host control and tenant management
5. Tor onion fallback for rescue, SSH, and unlock
6. Shared IPv4 public port publishing with frp or rathole
7. Public IPv6 when the host supports it
8. HTTP ingress with TLS passthrough
9. zrok or OpenZiti integration for Cloudflare Tunnel-like workflows
10. WireGuard advanced mode for tenants who want L3 VPN semantics
11. Dedicated IPv4 and richer routing for premium hosts
```

This gives you:

```text
safe defaults
NAT compatibility
home-host support
classic VPS compatibility where possible
privacy fallback
open-source tunnel options
clear upgrade path
```

## 25. Security claims

Safe claim:

> VMs are private by default. The host agent uses outbound-only control connectivity. Public exposure is explicit per service. Management access is available through marketplace-native private sessions and optional Tor recovery. Hosts may offer public IPv6, dedicated IPv4, shared port publishing, or ingress depending on capability.

Unsafe claim:

> Every VM is unreachable by the host operator.

Unsafe claim:

> Tunnels make public services private.

Unsafe claim:

> WireGuard, Tor, Iroh, or zrok solves VM isolation.

Correct framing:

```text
Network reachability controls who can connect.
VM isolation controls what tenants can affect.
Disk and memory protection control what host operators can see.
These are related, but they are separate security domains.
```

## 26. Final recommendation

The best product architecture is hybrid:

```text
Native control and tenant management:
  Iroh-first
  OpenZiti as a serious alternative
  Tor fallback

Public service exposure:
  public IPv6 where available
  dedicated IPv4 when available
  shared port publishing with frp or rathole for MVP
  HTTP ingress with TLS passthrough
  zrok or OpenZiti for open-source Cloudflare Tunnel-like workflows
  Cloudflare Tunnel as optional user/provider adapter

Advanced networking:
  WireGuard as optional L3 mode
```

The design principle:

> Make the VM private by default, make the host reachable only through outbound authenticated channels, and make every public service exposure explicit, reversible, auditable, and tied to the tenant's payment and reputation state.

## References

- [Cloudflare Tunnel documentation](https://developers.cloudflare.com/tunnel/)
- [Cloudflare Tunnel firewall guidance](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-with-firewall/)
- [WireGuard official site](https://www.wireguard.com/)
- [Iroh NAT traversal](https://docs.iroh.computer/concepts/nat-traversal)
- [Iroh relays](https://docs.iroh.computer/concepts/relays)
- [zrok overview](https://netfoundry.io/docs/zrok/intro/)
- [zrok self-hosting documentation](https://netfoundry.io/docs/zrok/category/self-hosting/)
- [OpenZiti overview](https://netfoundry.io/docs/openziti/intro/)
- [OpenZiti GitHub organization](https://github.com/openziti)
- [frp GitHub repository](https://github.com/fatedier/frp)
- [rathole GitHub repository](https://github.com/rathole-org/rathole)
- [Pangolin GitHub repository](https://github.com/fosrl/pangolin)
- [boringproxy GitHub repository](https://github.com/boringproxy/boringproxy)
- [chisel GitHub repository](https://github.com/jpillora/chisel)
- [Tor onion services overview](https://community.torproject.org/onion-services/overview/)
