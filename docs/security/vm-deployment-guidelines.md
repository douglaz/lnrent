# Safe VM Deployment Guidelines for a Sats Marketplace

Status: draft

> Reachability note: this document's host-control-plane / WireGuard networking recommendations (e.g. §12) are SUPERSEDED for lnrent by the networking addendum (vm-networking-reachability-guidelines.md) and ADR-0008 — reachability is three-plane and Iroh-first; WireGuard is an advanced-optional mode, not the control substrate.

Audience: marketplace developers, node operators, security reviewers, and tenants running services on rented VMs

Goal: define a practical, very safe VM deployment model for an open source marketplace where independent hosts rent VM capacity for sats.

## 1. Core principle

A standard VM is not a cryptographic boundary against the host.

A safe marketplace must never claim that a normal host cannot access tenant data while a VM is running. The safe claim depends on the tier:

```text
Tier 0: basic VPS
  No meaningful privacy guarantee against the host operator.

Tier 1: tenant-encrypted VPS
  Tenant owns the disk key. Host stores ciphertext. Runtime still trusts the host.

Tier 1.5: hardened provider-encrypted VPS
  Host uses strong encryption, isolation, monitoring, and audit logs. Tenant still trusts the host.

Tier 2: attested confidential VM
  Tenant releases secrets only to a measured confidential VM using AMD SEV-SNP, Intel TDX, or equivalent.
```

The marketplace should support all tiers, but every VM listing must advertise its tier honestly.

## 2. Honest security claims

### 2.1 Claims allowed for Tier 1.5

A Tier 1.5 host MAY claim:

```text
VM disks, snapshots, and backups are encrypted at rest with host-managed keys.
The host was installed from a known declarative configuration.
The host uses Secure Boot, TPM-backed unlock, mandatory access control, and remote audit logs.
VM processes are isolated with KVM or Firecracker hardening.
Administrative access is restricted, logged, and designed to be rare.
```

A Tier 1.5 host MUST NOT claim:

```text
The operator cannot access running VM data.
The operator cannot inspect memory.
The operator cannot tamper with the VM boot path.
The platform is trustless.
The platform is equivalent to confidential computing.
```

### 2.2 Claims allowed for Tier 2

A Tier 2 host MAY claim:

```text
The disk key is released only after confidential VM attestation succeeds.
The guest memory is intended to be protected from the host hypervisor.
The tenant can verify the launch measurement and hardware TCB before releasing secrets.
```

Even Tier 2 MUST describe residual trust:

```text
The tenant still trusts the CPU vendor, firmware update process, attestation verifier, guest kernel, and side-channel assumptions.
```

## 3. Threat model

### 3.1 In scope

The design should defend against:

```text
stolen host disks
old drives resold without wiping
copied VM image files
leaked cold snapshots
accidental backup exposure
curious but non-root operators
compromised neighboring guests
basic VM escape attempts
misconfigured consoles
misconfigured SSH
old user accounts left on a machine
unlogged operator actions
untrusted public networks
```

Tier 1 should additionally defend against:

```text
host copying stopped tenant disk images
host reading tenant backups without tenant keys
```

Tier 2 should additionally reduce trust in:

```text
host root
hypervisor introspection
host memory dumps
some boot tampering paths
```

### 3.2 Out of scope for Tier 1.5

Tier 1.5 does not defend cryptographically against:

```text
malicious host root
malicious host kernel
malicious firmware
malicious BMC
physical operator with unlimited access
runtime RAM inspection by the host
QEMU process memory inspection by the host
host-managed storage key extraction
malicious VM boot path changes
side channels
traffic metadata observation
service denial
```

This is acceptable only if the product clearly says the host is trusted.

## 4. Host acceptance policy

A host MUST NOT join the marketplace until it passes onboarding.

### 4.1 Required host capabilities for Tier 1.5

Minimum:

```yaml
cpu_virtualization: required
hardware_iommu: required
tpm2: required
uefi_secure_boot: required
known_clean_install: required
host_full_disk_encryption: required
per_vm_encryption: required
remote_audit_logs: required
password_ssh_login: forbidden
root_ssh_login: forbidden
operator_break_glass_access: restricted_and_logged
memory_snapshots: disabled_by_default
live_migration: disabled_by_default
host_path_passthrough: forbidden_by_default
```

Strongly recommended:

```yaml
ecc_memory: preferred
server_grade_firmware_updates: preferred
dedicated_datacenter_machine: preferred
out_of_band_management_isolated: required_for_servers
secure_boot_key_ownership: marketplace_or_operator_declared
kernel_lockdown: confidentiality_mode_preferred
selinux_or_apparmor: enforcing
qemu_or_firecracker_seccomp: enabled
iommu_dma_protection: enabled
encrypted_swap: required_if_swap_exists
unattended_auto_unlock_without_policy: forbidden
```

### 4.2 Home laptop hosts

Laptop and home nodes can be useful, but they should have a lower default score than datacenter hosts.

A home host should be accepted only when:

```text
the owner accepts that physical access is part of the trust model
Secure Boot is enabled
TPM 2.0 is present
disk encryption is enabled
IOMMU is enabled when available
Thunderbolt and other external DMA paths are disabled or restricted
Wi-Fi management is not used for privileged host access
power loss and reboots are handled safely
```

Home hosts should not receive high-risk workloads by default.

### 4.3 Dedicated server hosts

Dedicated servers need special handling because of BMCs like IPMI, iDRAC, iLO, Redfish, and vendor-specific management systems.

A dedicated server MUST:

```text
transfer provider account ownership or equivalent control
reset all BMC credentials
update BIOS, UEFI, BMC, SSD, NIC, and RAID firmware
isolate BMC on private VPN or management network
remove public BMC exposure
turn off unused BMC services
log BMC access remotely when possible
disable provider rescue access unless explicitly needed
record every provider-side console action
```

BMCs operate outside the host OS and can manage the machine even when it is shut down. Treat BMC access as equivalent to host ownership.

## 5. Onboarding flow

### 5.1 Preferred high-assurance flow

```text
1. Take custody of the machine or provider account.
2. Reset and update firmware and BMC.
3. Configure firmware security settings.
4. Cold boot a known installer image.
5. Wipe all disks.
6. Install the declarative host OS.
7. Enable host disk encryption.
8. Enable Secure Boot and TPM-bound unlock.
9. Install only the node agent and required virtualization stack.
10. Reboot into the final system.
11. Verify boot measurements and host profile.
12. Start receiving marketplace work only after remote policy passes.
```

### 5.2 Medium-assurance flow with nixos-anywhere

`nixos-anywhere` is useful for remote automation. It can connect by SSH, use kexec to boot a NixOS installer, partition and format disks, and install NixOS.

However, kexec starts the new kernel from the already running kernel. It is not the same as a clean firmware boot. Use this only for medium-assurance onboarding unless the previous environment is already trusted.

Acceptable use:

```text
known provider image
low-risk host
fresh server from a reputable provider
no previous tenant data
no suspicion of compromise
later reboot into final Secure Boot state
```

Not acceptable for high-assurance onboarding:

```text
machine previously controlled by an unknown operator
machine suspected of compromise
machine with unknown BMC state
host that will run sensitive workloads immediately
```

### 5.3 Disk wiping policy

On first install:

```text
wipe partition tables
wipe old LUKS headers
wipe old filesystem signatures
wipe old RAID metadata
wipe old LVM metadata
wipe old bootloader entries
create fresh partitions from declaration
create fresh encryption keys
```

If using SSDs, prefer secure erase or crypto erase where reliable. Do not rely on normal file deletion.

## 6. Host operating system baseline

NixOS is a good fit because the host can be mostly declarative and reproducible.

The host OS should be minimal:

```text
no desktop environment
no package manager exposed to tenants
no Docker unless required
no Kubernetes unless required
no shared hosting panel
no password SSH
no long-lived admin shell users except break-glass
no development tools unless needed for operation
```

Required services:

```text
node agent
virtualization runtime
firewall
remote logging agent
health attestation agent
time synchronization
automatic security update mechanism with staged rollout
metrics exporter with no tenant secrets
```

Forbidden by default:

```text
public libvirt socket
public QEMU monitor
public VNC or SPICE
public BMC
unencrypted swap
hibernation
kernel crash dumps with memory
automatic core dumps of QEMU or Firecracker
unrestricted host path passthrough
unrestricted USB passthrough
unrestricted PCI passthrough
```

## 7. Boot integrity

Tier 1.5 should use boot integrity as a gate before releasing host or VM storage keys.

Required:

```text
UEFI boot only
legacy BIOS boot disabled
Secure Boot enabled
custom or controlled Secure Boot keys preferred
kernel and initramfs signed
kernel command line measured or pinned
TPM 2.0 present
boot measurements sent to marketplace verifier
policy rejects unknown measurements
```

Recommended:

```text
kernel lockdown in confidentiality mode
unsigned kernel modules forbidden
kexec disabled after boot
hibernation disabled
/dev/mem, /dev/kmem, and /dev/kcore unavailable
BPF restricted to privileged, audited use
```

Measured boot is not the same as prevention. It allows the verifier to decide whether the machine booted into an expected state.

## 8. Host storage encryption

### 8.1 Storage layers

Use layered encryption:

```text
host root disk encryption
VM storage pool encryption
per-VM disk encryption
per-backup encryption
```

Avoid one global key for all tenants.

### 8.2 Key hierarchy

Recommended model:

```text
host root key
  unlocks only the host OS

storage pool key
  unlocks the pool that stores VM images

VM data encryption key
  unique per VM disk

backup data encryption key
  unique per backup set or tenant backup policy
```

A VM disk key should never be stored plaintext on disk.

### 8.3 Key release

Use a remote KMS-like service controlled by the marketplace or operator policy.

```text
host boots
host agent measures boot state
host agent authenticates to KMS
KMS checks policy
KMS releases storage pool key
node agent starts VM
node agent requests per-VM key
KMS checks VM policy
node agent injects key into QEMU or storage stack as ephemeral secret
node agent drops key from its own memory after handoff
```

For Tier 1.5, this reduces accidental leakage and enables revocation. It does not stop malicious root from stealing keys during runtime.

### 8.4 Libvirt and QEMU encryption

Acceptable implementations:

```text
qcow2 with LUKS encryption
raw image on per-VM LUKS block device
encrypted ZFS dataset with per-VM keys
encrypted LVM volume with per-VM LUKS
```

Preferred:

```text
per-VM LUKS or qcow2 LUKS
unique key per VM
key provided through libvirt secret or equivalent
secret is ephemeral where possible
```

Avoid:

```text
old qcow AES-CBC encryption
one shared host pool key as the only VM protection
plaintext VM images on encrypted host root only
plaintext backups
keys in cloud-init
keys in VM metadata
keys in database rows without envelope encryption
keys in shell scripts
keys in systemd unit files
```

## 9. Tenant-side encryption option

Every serious deployment should offer tenant-side disk encryption as an option.

Tier 1 tenant-managed encryption:

```text
tenant generates disk key locally
tenant builds encrypted image locally
tenant uploads only ciphertext
tenant unlocks VM after boot through initramfs SSH or similar
host never receives the tenant disk key
```

This protects stopped disks and copied snapshots from the host. It does not protect running memory or boot tampering.

Good UX:

```text
provide an official tenant CLI
support remote initramfs unlock
support recovery key rotation
support encrypted backups
warn that reboot requires tenant unlock
```

## 10. VM runtime isolation

### 10.1 Runtime choice

Supported runtimes:

```text
KVM plus QEMU plus libvirt
Firecracker microVMs
Cloud Hypervisor or similar, after security review
```

General-purpose VPS product:

```text
use KVM/QEMU/libvirt first
```

Minimal Linux service product:

```text
consider Firecracker
```

### 10.2 QEMU and libvirt hardening

Required:

```text
QEMU runs unprivileged
unique process identity per VM when feasible
SELinux sVirt or AppArmor sVirt enabled
no host path mounts by default
no arbitrary device passthrough
no public QEMU monitor
no public libvirt management socket
no unaudited console access
cgroups v2 resource limits
seccomp enabled when supported
```

Recommended:

```text
static memory allocation where feasible
memory ballooning disabled for sensitive VMs
vhost features reviewed before enabling
IOThreads limited and monitored
one VM cannot access another VM image
VM image permissions checked continuously
```

### 10.3 Firecracker hardening

If using Firecracker:

```text
use the jailer
use default seccomp filters
never run with no seccomp in production
run each microVM under its own UID and GID
use cgroups
use a chroot or jail directory
expose only required devices
keep the API socket inaccessible to tenants
```

Seccomp reduces kernel attack surface, but it is not a complete sandbox. Combine it with mandatory access control, namespaces, cgroups, and a small runtime.

## 11. Device policy

Default allowed devices:

```text
virtual CPU
virtual memory
virtio block or virtio scsi
virtio net
virtio rng
serial console only when tenant opts in
vsock only when needed
```

Default forbidden devices:

```text
USB passthrough
PCI passthrough
GPU passthrough
host filesystem passthrough
9p filesystem passthrough
virtiofs passthrough
sound devices
camera devices
smartcard devices
arbitrary emulated legacy devices
```

If device passthrough is offered:

```text
require IOMMU
require dedicated host or strict device isolation
warn that passthrough increases VM escape and DMA risk
log every assignment
prevent reassignment without full reset
```

## 12. Network policy

Every VM should receive an isolated network context.

Required:

```text
per-VM tap or equivalent interface
host firewall default deny
anti-spoof rules
no tenant access to host management ports
no tenant access to libvirt, QEMU, KMS, or agent ports
no metadata service by default
rate limits for abusive traffic
DDoS and abuse policy
```

Recommended:

```text
nftables or eBPF policy generated from VM declaration
separate management network
separate tenant data network
WireGuard for host control plane
ingress proxy only when explicitly configured
per-VM traffic accounting
```

Metadata service rule:

```text
Do not provide an EC2-like metadata service unless it is strictly scoped, authenticated, and contains no secrets by default.
```

## 13. Console policy

Consoles are dangerous because they can expose boot prompts, unlock prompts, and application secrets.

Default:

```text
serial console disabled unless requested
VNC disabled
SPICE disabled
screenshot capture disabled
keyboard injection disabled
clipboard sharing disabled
```

When console access is needed:

```text
tenant initiates it
short expiry
strong authentication
action logged remotely
operator cannot silently open it
record whether the session included graphical input
```

## 14. Snapshot and backup policy

### 14.1 Disk snapshots

Cold disk snapshots are safer than live snapshots.

Default:

```text
cold disk snapshots allowed
disk snapshots encrypted with per-snapshot or per-VM keys
snapshot creation logged
snapshot export logged
snapshot restore logged
snapshot deletion logged
```

### 14.2 Memory snapshots

Memory snapshots are high risk.

Default:

```text
live memory snapshots forbidden
suspend-to-disk forbidden
QEMU core dumps forbidden
Firecracker core dumps forbidden
live migration forbidden
```

If enabled for a special product:

```text
tenant must opt in
risk warning required
memory artifact encrypted
artifact has short retention
event logged remotely
operator access audited
```

### 14.3 Backups

Backups must be encrypted before they leave the host or before they are stored in shared storage.

Required:

```text
unique backup key
envelope encryption
retention policy
restore test policy
tenant-visible backup events
cryptographic integrity check
```

Preferred:

```text
tenant-held backup encryption key for sensitive workloads
```

## 15. Operator access model

The safest host is one where operators rarely need shell access.

Default:

```text
no password login
no root SSH login
no long-lived personal SSH keys
short-lived SSH certificates for break-glass
MFA at the access broker
operator sessions recorded where legally acceptable
remote audit log before shell is granted
just-in-time access with reason code
```

The node agent should expose narrow operations:

```text
create VM
start VM
stop VM
reboot VM
attach disk
detach disk
snapshot disk
delete snapshot
rotate key
collect health
```

It should not expose:

```text
arbitrary shell
arbitrary file read
arbitrary command execution
arbitrary libvirt XML injection
arbitrary QEMU args
arbitrary firewall mutation
```

## 16. Node agent design

The node agent is security critical.

Requirements:

```text
small codebase
memory-safe language preferred
minimal dependencies
reproducible builds
signed releases
config generated from marketplace policy
runs as unprivileged as possible
separate helper for privileged actions
no plaintext secrets in logs
all actions authenticated
all actions authorized
all actions audited
```

Key handling:

```text
keys fetched only when needed
keys held for shortest practical time
keys never logged
keys never written to disk
keys never exposed through metrics
process core dumps disabled
swap disabled or encrypted
memory locked where feasible
```

Failure mode:

```text
if policy check fails, do not start VM
if KMS is unavailable, do not invent fallback keys
if host measurement changes, quarantine host
if audit log upload fails, degrade or stop sensitive actions
```

## 17. Remote attestation and health checks

Tier 1.5 should use host attestation and health checks for key release.

Host reports:

```yaml
host_id: stable_public_key
hardware_id: tpm_quote_or_platform_certificate
boot_measurements: pcr_values
secure_boot: true
kernel_lockdown: confidentiality
host_os_generation: nixos_generation_hash
node_agent_version: signed_version
virtualization_runtime: qemu_libvirt_or_firecracker
runtime_versions: pinned_versions
iommu_enabled: true
mac_policy: selinux_or_apparmor_enforcing
firewall_policy_hash: expected_hash
open_ports: expected_set
bmc_status: isolated_or_absent
last_reboot: timestamp
```

Verifier policy:

```text
accept only known host OS generations
accept only known kernel command lines
accept only current node-agent versions
reject unknown Secure Boot state
reject missing TPM quote
reject disabled IOMMU for Tier 1.5
reject disabled MAC policy for Tier 1.5
reject unexpected open management ports
reject stale firmware policy for high-risk hosts
```

Health checks are not enough if the host is malicious. They are still valuable for drift detection and operational safety.

## 18. Confidential VM upgrade path

Design Tier 1.5 so Tier 2 can be added later.

Tier 2 flow:

```text
confidential VM boots
VM creates ephemeral public key
VM requests hardware attestation report
report binds measurement and ephemeral key
tenant verifier checks vendor root, TCB, firmware, and launch measurement
tenant encrypts disk key to ephemeral key
VM receives key and unlocks disk
host never receives plaintext tenant key
```

Tier 2 requirements:

```text
AMD SEV-SNP or Intel TDX capable CPU
firmware configured for confidential computing
current microcode and firmware
QEMU and kernel support
guest image measurement policy
remote attestation verifier
tenant-side key release
TCB freshness policy
security advisory tracking
```

Do not mix Tier 2 language into Tier 1.5 product pages.

## 19. VM image policy

Images can be another source of compromise.

Marketplace-provided images must have:

```text
source repository
reproducible or auditable build process
signed release artifact
SBOM when practical
known default users
no default passwords
no embedded private keys
no embedded marketplace secrets
cloud-init disabled or tightly scoped
security update policy
```

Tenant-provided images:

```text
must be treated as hostile to the host
must not be mounted on host without sandboxing
must not be inspected by privileged parsers unnecessarily
should be converted in a sandbox if conversion is needed
```

Never run tenant-provided image hooks on the host.

## 20. Cloud-init and metadata

Cloud-init is convenient, but it often becomes a secret injection path.

Rules:

```text
SSH public keys are allowed
private keys are forbidden
seed phrases are forbidden
wallet files are forbidden
LUKS keys are forbidden
API tokens are forbidden by default
metadata service must not include secrets by default
```

For tenant secrets, prefer:

```text
tenant logs into VM and provisions secrets
tenant uses their own secret manager
tenant-side encrypted bootstrap payload
Tier 2 attested key release
```

## 21. Logging and transparency

Every sensitive action should create an append-only remote event.

Log events:

```text
host enrolled
host booted
host measurement changed
host entered quarantine
VM created
VM started
VM stopped
VM rebooted
VM deleted
disk attached
disk detached
snapshot created
snapshot exported
snapshot deleted
backup created
backup restored
console opened
console closed
operator shell opened
operator shell closed
KMS key requested
KMS key released
KMS key denied
firmware version changed
node-agent upgraded
firewall policy changed
unexpected process detected
unexpected port detected
```

Tenant-visible transparency:

```text
tenant can see security tier
tenant can see host profile
tenant can see VM lifecycle events
tenant can see snapshot and backup events
tenant can see console events
tenant can see operator access events affecting their VM
```

Logs must not include:

```text
private keys
passphrases
LUKS headers unless intended
memory dumps
full command lines containing secrets
cloud-init user data containing secrets
```

## 22. Quarantine policy

A host enters quarantine when:

```text
boot measurements change unexpectedly
Secure Boot turns off
TPM quote is missing or invalid
node-agent version is unknown
firmware version is blocked
BMC is exposed publicly
unexpected SSH users appear
unexpected SSH keys appear
unexpected management port appears
MAC policy is disabled
IOMMU is disabled
key release policy fails
remote audit logging fails repeatedly
operator opens unapproved shell
VM isolation test fails
```

Quarantine behavior:

```text
stop scheduling new VMs
stop releasing new storage keys for sensitive VMs
notify tenants
preserve evidence
allow tenant evacuation where possible
require human security review
require re-attestation before rejoining
```

## 23. Incident response

Prepare for these incidents before launch:

```text
host stolen
host seized
host operator disappears
host compromised
BMC compromised
VM escape suspected
tenant disk leaked
tenant backup leaked
KMS key leaked
node-agent key leaked
marketplace signing key leaked
firmware vulnerability announced
confidential computing TCB update required
```

Each incident plan should define:

```text
who can quarantine
who can notify tenants
what evidence to preserve
how to rotate keys
how tenants evacuate
how payments are handled
how reputation is updated
when a host can return
```

## 24. Payment does not equal security

Payment escrow, reputation, bonds, and slashing can improve incentives, but they do not replace technical controls.

Use reputation for:

```text
availability history
abuse response
payment behavior
incident history
operator responsiveness
```

Do not use reputation as a substitute for:

```text
encryption
isolation
attestation
logs
key management
firmware hygiene
```

## 25. Security profile schema

Every host should publish a signed profile.

Example:

```yaml
host_id: npub_or_ed25519_key
marketplace_tier: tier_1_5
location_claim: country_or_region_only
hardware:
  cpu_vendor: amd_or_intel
  cpu_model: string
  virtualization: true
  iommu: true
  tpm2: true
  ecc_memory: true
  bmc: present_or_absent
boot:
  uefi: true
  secure_boot: true
  custom_secure_boot_keys: true
  measured_boot: true
  kernel_lockdown: confidentiality
storage:
  host_fde: luks2
  vm_pool_encrypted: true
  per_vm_encryption: true
  backup_encryption: true
runtime:
  engine: kvm_qemu_libvirt
  mac_policy: selinux_svirt_or_apparmor_svirt
  seccomp: true
  memory_snapshots: disabled
  live_migration: disabled
  device_passthrough: disabled_by_default
network:
  management_network: private_vpn
  firewall_default: deny
  metadata_service: disabled
operations:
  password_login: disabled
  root_ssh_login: disabled
  break_glass: short_lived_ssh_certificates
  remote_audit_logs: true
  automatic_quarantine: true
last_verified: iso_timestamp
profile_signature: signature
```

## 26. Tenant-facing product labels

Use simple labels.

```text
Basic VPS
  Cheapest. No special privacy guarantee against the host.

Encrypted VPS
  Tenant disk encryption. Host stores ciphertext when stopped. Manual unlock may be required.

Hardened VPS
  Provider-encrypted, audited, hardened host. Similar trust model to a serious cloud provider, but smaller operator.

Confidential VPS
  Hardware confidential VM with remote attestation. Strongest option when available.
```

For sensitive services like Lightning nodes, Fedimint guardians, wallets, databases, and private relays, recommend:

```text
minimum: tenant-encrypted VPS
better: hardened VPS plus tenant-managed app secrets
best: confidential VPS with attested key release
```

## 27. Pre-launch test plan

Before accepting tenants, run these tests.

### 27.1 Host install tests

```text
verify disks were wiped
verify host root is encrypted
verify VM pool is encrypted
verify TPM is present
verify Secure Boot is enabled
verify kernel lockdown state
verify IOMMU is enabled
verify SSH password login is disabled
verify root SSH login is disabled
verify only expected users exist
verify only expected ports are open
verify BMC is isolated or absent
```

### 27.2 VM isolation tests

```text
compromised guest cannot read host files
compromised guest cannot access another VM disk
compromised guest cannot access libvirt socket
compromised guest cannot access QEMU monitor
compromised guest cannot reach KMS
compromised guest cannot reach host management network
compromised guest cannot spoof another VM IP or MAC
compromised guest cannot escape resource limits
```

### 27.3 Key management tests

```text
VM key is unique
VM key is not stored plaintext on disk
VM key is not logged
VM key is not exposed in metrics
KMS denies key release for wrong host measurement
KMS denies key release for quarantined host
KMS denies key release after host removal
```

### 27.4 Snapshot and backup tests

```text
cold snapshot is encrypted
snapshot export is logged
backup is encrypted
backup restore works
memory snapshot is rejected by default
QEMU core dump is disabled
suspend-to-disk is disabled
```

### 27.5 Operator abuse simulation

```text
try to open console silently
try to create a live memory dump
try to attach host path
try to enable VNC silently
try to add a new SSH key
try to change firewall policy
try to start VM after measurement change
try to request key from wrong host
```

Every test should have a CI or recurring audit equivalent.

## 28. Minimum viable secure launch

For the first production release, aim for this:

```text
NixOS host
cold clean install preferred
nixos-anywhere allowed only for medium-assurance bootstrap
Secure Boot enabled
TPM 2.0 present
host root encrypted
VM storage encrypted
per-VM disk keys
remote KMS-style key release
KVM/QEMU/libvirt
sVirt or AppArmor confinement
no public libvirt socket
no public QEMU monitor
no password SSH
no root SSH
remote audit logs
memory snapshots disabled
live migration disabled
tenant-side LUKS option
host security profile exposed to tenants
quarantine automation
```

That is a credible Tier 1.5.

The next major milestone should be:

```text
confidential VM support
SEV-SNP or TDX attestation verifier
tenant-side key release after attestation
measured guest images
TCB freshness policy
```

## 29. Do not launch checklist

Do not launch a host if any of these are true:

```text
BMC is public on the internet
provider console ownership is unclear
Secure Boot is disabled for Tier 1.5
TPM is missing for Tier 1.5
host disk is unencrypted
VM storage is plaintext
VM keys are shared across tenants
VM keys are stored in config files
SSH password login is enabled
root SSH login is enabled
libvirt socket is reachable by tenants
QEMU monitor is reachable by tenants
memory snapshots are enabled by default
backups are plaintext
remote logs are not working
unexpected users exist
unexpected services listen on the network
firmware is known vulnerable and unpatched
host measurements do not match policy
```

## 30. Recommended repo layout

```text
repo/
  docs/
    security-model.md
    host-onboarding.md
    tenant-security.md
    incident-response.md
    tier-definitions.md
  hosts/
    common/
    tier-1-5/
    tier-2-confidential/
  agent/
    src/
    policy/
    tests/
  verifier/
    src/
    policy/
    test-vectors/
  images/
    guest-initramfs-unlock/
    marketplace-base-image/
  audits/
    host-checks/
    vm-isolation-tests/
    key-release-tests/
```

## 31. Reference sources

These guidelines are based on the public documentation and security models of Linux dm-crypt, QEMU, libvirt, Linux kernel lockdown, systemd-cryptenroll, NixOS tooling, sVirt, Firecracker, BMC hardening guidance, and confidential VM technologies.

Useful references:

- Linux dm-crypt documentation: https://docs.kernel.org/admin-guide/device-mapper/dm-crypt.html
- Linux kernel lockdown manual: https://man7.org/linux/man-pages/man7/kernel_lockdown.7.html
- QEMU qcow2 LUKS format documentation: https://www.qemu.org/docs/master/interop/qcow2.html
- QEMU storage daemon encryption format notes: https://www.qemu.org/docs/master/interop/qemu-storage-daemon-qmp-ref.html
- Libvirt storage encryption XML: https://libvirt.org/formatstorageencryption.html
- Libvirt secret storage and encryption: https://libvirt.org/secretencryption.html
- Libvirt QEMU driver security confinement: https://libvirt.org/drvqemu.html
- Red Hat virtualization security guide for sVirt and SELinux: https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/7/html/virtualization_security_guide/
- Firecracker seccomp documentation: https://github.com/firecracker-microvm/firecracker/blob/main/docs/seccomp.md
- Firecracker production host setup: https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md
- Linux seccomp filter documentation: https://docs.kernel.org/userspace-api/seccomp_filter.html
- nixos-anywhere: https://github.com/nix-community/nixos-anywhere
- Lanzaboote Secure Boot and Measured Boot for NixOS: https://github.com/nix-community/lanzaboote
- systemd-cryptenroll manual: https://man7.org/linux/man-pages/man1/systemd-cryptenroll.1.html
- NSA and CISA BMC hardening guidance: https://media.defense.gov/2023/Jun/14/2003241405/-1/-1/0/CSI_HARDEN_BMCS.PDF
- QEMU AMD SEV documentation: https://www.qemu.org/docs/master/system/i386/amd-memory-encryption.html
- QEMU Intel TDX documentation: https://www.qemu.org/docs/master/system/i386/tdx.html
- AMD SEV-SNP attestation overview: https://www.amd.com/content/dam/amd/en/documents/developer/lss-snp-attestation.pdf
- Intel TDX enabling guide: https://cc-enabling.trustedservices.intel.com/intel-tdx-enabling-guide/02/infrastructure_setup/

## 32. Final rule

When in doubt, make the security claim weaker and the implementation stronger.

The marketplace wins trust by refusing to overclaim.
