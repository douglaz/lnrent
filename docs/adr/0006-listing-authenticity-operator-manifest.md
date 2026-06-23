# 0006 — Listing authenticity via a master-signed operator manifest

Listings are signed by a Box's operational key (so a Box self-publishes and edits its
own Listings while the master identity stays cold, per ADR-0004), and each Listing
carries an `operator` tag naming the master pubkey. A buyer authenticates a Listing
against the master-signed **operator manifest**: a parameterized-replaceable event
(app-defined kind in the 30000 range, fixed `d` tag, pinned in M5 when the manifest ships) that the master
signs to list the operational pubkeys it vouches for. The buyer fetches the manifest
for the master named in the Listing and accepts the Listing only if its signing key is
attested there. We chose operational-key-signed Listings over master-signed Listings
so the master key never has to be hot to publish or edit a Listing.

## Considered options

- **Master-signed Listings.** One signature for the buyer to check, but the master key
  must be hot to publish or edit any Listing, defeating the cold-master design.
- **Operational-key-signed Listings + manifest (chosen).** Master stays cold; the
  manifest binds operational keys to the brand and is revocable.

## Consequences

- Reputation attaches to the master identity; buyers compare brands, not Boxes.
- Revocation is a manifest re-publish: drop a compromised Box's key and its Listings
  stop verifying once buyers refetch the manifest — bounded by a manifest TTL/version
  (§16 open question), not instantaneous (replaceable events have no push invalidation).
- The buyer client (CLI and web) does a second fetch (the manifest) and a
  set-membership check per operator; manifests are cacheable. A Listing is
  unverifiable while its master's manifest cannot be fetched (relay gap).
- An attacker can put a master pubkey in an `operator` tag but cannot enter that
  master's manifest, so the binding holds without the master key being online.
