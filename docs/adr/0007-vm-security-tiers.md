# 0007 — VM rental uses an honest security-tier model

VM rental adopts the tiered security model in
`docs/security/vm-deployment-guidelines.md`: Tier 0 (basic VPS, no privacy guarantee
against the host), Tier 1 (tenant-encrypted disk), Tier 1.5 (hardened
provider-encrypted host with Secure Boot, TPM, per-VM LUKS, KMS-style key release,
sVirt, remote audit logs, quarantine), and Tier 2 (attested confidential VM via
SEV-SNP/TDX). A normal VM is not a cryptographic boundary against its host, so every
VM Listing MUST advertise its tier honestly and a host MUST NOT claim above its tier.
The guiding rule: make the security claim weaker and the implementation stronger.

M1 ships **Tier 0** ("Basic VPS"), labeled honestly, to prove the rental loop. The
tier ladder is the security roadmap: Tier 1 (tenant LUKS), then Tier 1.5 (the
"minimum viable secure launch", guidelines §28), then Tier 2 (confidential computing).
The recipe manifest and NIP-99 Listing carry a `tier` field; the host publishes a
signed security profile (guidelines §25) that tenants read.

## Consequences

- The tier model is what lets M1 ship a thin VM honestly: Tier 0 overclaims nothing.
- Tier 1.5 (Secure Boot + TPM + per-VM encryption + KMS + quarantine) is a large,
  later milestone, not the MVP bar. Tier 2 (attestation) is later still.
- The control-plane "node agent" exposes only narrow VM operations (create/start/stop/
  reboot/snapshot/rotate-key/health), never arbitrary shell — consistent with the
  AI-free control plane and ADR-0001. Tenant-provided images are treated as hostile
  and never have their hooks run on the host.
- The full guidelines live at `docs/security/vm-deployment-guidelines.md` and govern
  the VM compute backend, host onboarding, encryption, isolation, and the security
  roadmap.
