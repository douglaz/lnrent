# 0011 — Reputation: buyer-signed attestations to the master identity

The marketplace is pay-first with no escrow and no custodian (ADR-0003, §4.3), so buyers
need a reason to trust an anonymous operator before paying. The whole demand thesis rests
on this, yet v1 defers reputation. Public access (the web buyer) makes the gap more
exposed: it lowers the bar to *arrive*, so it must not leave buyers facing pure blind
trust.

Decision:

- **M1a stays blind-trust** — it is a mechanics proof against test operators, no real
  go-to-market. No reputation is built into M1a; the only hook is that reputation accrues
  to the **master identity** (ADR-0004/0006), which already exists.
- **Before the marketplace is publicly promoted to real buyers**, ship a minimal
  Nostr-native reputation primitive: a buyer-signed **rental attestation** (a NIP-32 label
  event vouching that an operator fulfilled a rental) that accrues to the operator's
  master identity, plus master-identity longevity. The web buyer aggregates attestations
  and surfaces the signal next to listings.
- **Sybil resistance** is the hard part: anyone can sign an attestation. Mitigate by
  weighting attestations via web-of-trust (the viewer's follow graph / established
  identities count more) and, as the hardening path, binding an attestation to a real
  settled subscription (the operator counter-signs, or a payment proof) so only genuine
  customers count. The full anti-Sybil design is deferred but the binding hook is reserved.
- **Structurally lower the stakes** regardless: short, cheap first periods (a trial); the
  up-front refund destination (ADR-0003; LN-address/LNURL per SPEC §6.4); and lead with non-sensitive Tier-0 services so
  the trust ask is smaller.

We chose buyer-signed attestations over bonds/staking/slashing or escrow because
attestations are Nostr-native, need no custodian or capital lockup, and accrue to the
portable master identity. The VM guidelines (§24) are explicit that reputation/bonds do
not replace technical controls — so reputation rides on top of the isolation, tiers, and
the value/hosting split (ADR-0007/0008/0010), it does not substitute for them.

## Considered options

- **Blind trust forever.** Fine for a mechanics MVP, untenable for public GTM.
- **Bonds / staking / slashing.** Skin-in-the-game, but needs capital lockup and a
  slashing authority/oracle — heavy and not custodian-free. Deferred.
- **Escrow.** Needs a custodian or a heavy LN/Fedimint construction; contradicts the
  no-custodian stance. Rejected.
- **Buyer-signed attestations to the master identity (chosen).**

## Consequences

- Reputation lives on the master identity (the control node, ADR-0010); the web buyer
  reads attestation events from relays and displays an aggregated, web-of-trust-weighted
  signal.
- An attestation should reference the subscription so payment-binding can harden it later.
- Bonds/escrow remain a possible future layer but are not required for launch.
