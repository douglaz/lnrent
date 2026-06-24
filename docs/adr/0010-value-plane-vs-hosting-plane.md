# 0010 — Separate the value plane from the hosting plane

The operator's funds (the phoenixd / Fedimint receiving wallet) and the BIP39 seed +
master key are the operator's most valuable assets. Untrusted tenant VMs run on hosting
boxes. Co-locating them means a tenant VM escape — or any box compromise — drains the
operator's Lightning funds and steals the seed (which is the brand identity and derives
every key). The VM guidelines protect tenants from the host; this protects the operator
from the tenants.

Rule: once a box hosts untrusted tenant VMs, the operator's funds and seed do NOT live on
it. lnrent splits into two planes:

- **Value / identity / marketplace plane (the control node):** holds the phoenixd/Fedimint
  wallet + the hot **marketplace operational key**, and the marketplace control — the Nostr
  engine (publish listings, handle order DMs), the subscription store + billing/reconcile. The
  **BIP39 seed + master key stay cold/offline** (used only to issue/update the manifest), not
  hot on the control node — except the M1a all-in-one box, where the seed lives on the box. The
  operator's brain + wallet.
- **Hosting plane (hosting boxes):** run the provisioning backend (Incus) and tenant VMs.
  A hosting box holds only a per-box **operational key** (revocable via the operator
  manifest, ADR-0004) used to authenticate to the control node and sign its host security
  profile. No funds, no seed.

The control node drives hosting boxes over the Iroh control plane (ADR-0008,
outbound-only from the box). A hosting-box compromise loses only that box's operational
key and its tenants — not the money or the brand; the master revokes the box by
re-issuing the manifest. This also answers fleet topology: the control node is the
aggregation / value / identity point; hosting boxes are disposable compute.

## Considered options

- **All-in-one box** (phoenixd + seed + hosting together). Simplest, but a tenant escape =
  funds + seed gone. Allowed ONLY for M1a / self-use / no-untrusted-tenants.
- **On-box but hardened** (phoenixd in its own VM, master key cold). Reduces but does not
  remove the co-location risk.
- **Value plane separated from hosting plane (chosen).**

## Consequences

- **M1a is exempt** (single box, trivial recipe, no untrusted tenants) — the bead graph is
  unchanged. The split becomes mandatory at M1b+ when real tenant VMs run.
- The control node is the single point of value/identity for the marketplace; back up the
  seed (ADR-0004) and the daemon state (state-backup bead) there.
- Refines ADR-0004: a hosting box's operational key narrows to authenticating the box +
  signing its host security profile; listings, order DMs, and billing live on the control
  node.
- The control node ↔ hosting box link is the Iroh host-control plane (ADR-0008).
