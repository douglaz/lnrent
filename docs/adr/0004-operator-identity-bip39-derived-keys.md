# 0004 — Operator identity: one BIP39 seed, master + per-box derived keys

The operator's Nostr key is the marketplace identity, and reputation built on a key
is stuck to it. To keep reputation portable across a fleet, all operator keys derive
from a single BIP39 seed (NIP-06 derivation, `m/44'/1237'/<account>'/0/0`): a master
identity at account 0 carries the brand and signs an operator manifest, and each Box
gets its own operational key derived at the Box's account index (>= 1; account 0 is the
master, never a Box, so the first Box never collides with it — M1's single-key build uses
account 0 directly). On a fleet (ADR-0010) the control node holds a marketplace operational
key that signs Listings and handles buyer DMs, while each hosting box's operational key
signs its host security profile and authenticates the box; the master need not be hot, and
a compromised box leaks only its revocable operational key, which the master removes by
re-issuing the manifest.

## Considered options

- **One key per box.** Simplest, but welds reputation to a box-resident key; a second
  box is a second identity. Rejected for foreclosing fleet/brand portability.
- **Master + per-box keys, BIP39-rooted (chosen).** One seed to back up; everything
  regenerable; reputation on the master; operational keys hot per box and revocable.

## Consequences

- A single BIP39 seed is the one backup. The seed and master key stay off Boxes where
  practical; only a derived operational key is deployed to each Box.
- Buyers verify Listings against a master-signed **operator manifest**: an app-defined
  replaceable Nostr event listing the operational pubkeys (no NIP fits, so it is an
  explicit app-level event, consistent with the §5 approach).
- The **Fedimint client root secret** (the primary receive backend, ADR-0012) derives
  from the same BIP39 seed via a dedicated HKDF domain (`lnrent:fedimint:v1`, distinct from
  the NIP-06 Nostr paths), so one seed regenerates identity AND the ecash client. The ecash
  position is recoverable from the federation — but **the seed alone is not a full backup**:
  restore also needs the federation invite/config (which federation to rejoin), so onboard's
  backup must include it. (A phoenixd secondary — M3, ADR-0012, never built in v1 — would
  keep its own channel-state seed, backed up separately.)
- v1 single-box operators may keep the seed on the box, accepting that a box
  compromise is then a seed compromise. Onboard forces an explicit backup.
