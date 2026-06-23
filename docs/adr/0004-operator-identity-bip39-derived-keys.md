# 0004 — Operator identity: one BIP39 seed, master + per-box derived keys

The operator's Nostr key is the marketplace identity, and reputation built on a key
is stuck to it. To keep reputation portable across a fleet, all operator keys derive
from a single BIP39 seed (NIP-06 derivation, `m/44'/1237'/<account>'/0/0`): a master
identity at account 0 carries the brand and signs an operator manifest, and each Box
gets its own operational key derived at the Box's account index (>= 1; account 0 is the
master, never a Box, so the first Box never collides with it — M1's single-key build uses
account 0 directly). Boxes sign their own
Listings and handle their own buyer DMs with the operational key, so the master need
not be hot on every Box, and a compromised Box leaks only its operational key, which
the master revokes by re-issuing the manifest.

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
- Goal: derive the payment-backend seed from the same BIP39 seed so one backup covers
  funds too. Whether phoenixd accepts an imported seed is unverified; if not, its seed
  is backed up separately in v1.
- v1 single-box operators may keep the seed on the box, accepting that a box
  compromise is then a seed compromise. Onboard forces an explicit backup.
