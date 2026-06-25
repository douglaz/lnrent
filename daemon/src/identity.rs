//! Operator identity derivation (lnrent-7fp.16; ADR-0004, SPEC.md §4.6).
//!
//! One BIP39 seed is the operator's single backup. From it we derive, deterministically:
//! - the **Nostr identity** — NIP-06 account 0 (`m/44'/1237'/0'/0/0`), the marketplace signer. In
//!   M1a the single key IS the master AND the operational key (`master_pubkey == op_pubkey`,
//!   `box_index = 0`); the master / per-box split is M5 (ADR-0004).
//! - a **PROVISIONAL Fedimint client root secret** (the primary receive backend, ADR-0012) via
//!   HKDF-SHA256 over the SAME seed with info `lnrent:fedimint:v1` — a domain disjoint from the
//!   NIP-06 paths, so the Nostr and Fedimint key spaces can't collide (§4.6). We only DERIVE the
//!   bytes here; the real `fedimint-client` that consumes them is bead .4 (deferred). Bead .4 MUST
//!   confirm this is the exact form `fedimint-client` 0.11.1 ingests for backup/recovery before any
//!   real ecash relies on it; the info string / derived form MAY change when .4 lands. That is fine
//!   for M1a, which runs on MockPayment (no real ecash exists yet) — so this is NOT yet an immutable
//!   on-funds anchor.
//!
//! The seed and derived keys are secret: they live in the data dir with tight perms (config.rs)
//! and are NEVER logged. `OperatorIdentity` deliberately has no `Debug` so key material can't leak
//! through a `{:?}`.
//!
//! Secret-memory hygiene is BEST-EFFORT, not a guarantee. We zeroize the buffers we own — the
//! 64-byte BIP39 seed (via `Zeroizing`, on every return path), the decoded `Mnemonic`'s entropy
//! (bip39's `zeroize` feature), the `Keys` secret (on its own drop), and the Fedimint root secret
//! (on `OperatorIdentity` drop). But `nostr`'s NIP-06 helper (`Keys::from_mnemonic_*`) re-parses the
//! mnemonic and constructs its OWN seed / BIP32 Xpriv intermediates that are not reachable through
//! its public API, so those transient copies are NOT wiped (a known, documented limitation). Do not
//! read these comments as a claim that every copy of the key material is erased.

use hkdf::Hkdf;
use nostr::nips::nip06::FromMnemonic;
use nostr::{Keys, PublicKey, ToBech32};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::ipc::IpcError;

/// The HKDF-SHA256 `info` string that domain-separates the (PROVISIONAL) Fedimint client root
/// secret from the NIP-06 Nostr key paths (ADR-0004/0012, §4.6).
///
/// PROVISIONAL for M1a: bead .4 MUST confirm this derivation matches what `fedimint-client` 0.11.1
/// actually consumes for backup/recovery (e.g. a `DerivableSecret` root input vs a BIP-39 strategy)
/// before any real ecash relies on it. This string — and the derived form — MAY change when .4
/// lands. That is acceptable now because M1a runs on MockPayment: no real ecash exists yet, so this
/// is NOT an immutable on-funds anchor. The `v1` suffix just versions the current provisional scheme.
pub const FEDIMINT_HKDF_INFO: &[u8] = b"lnrent:fedimint:v1";

/// This Box's NIP-06 derivation account. M1a is single-key: account 0 is the master used directly
/// as the operational key (ADR-0004, §4.6); the per-box account (>= 1) split lands in M5.
pub const BOX_INDEX: u32 = 0;

// M1a single-key invariant. Two things assume `BOX_INDEX == 0`: the key is derived at NIP-06
// account `BOX_INDEX` (below), and the persisted row sets `master_pubkey == op_pubkey` (config.rs).
// The M5 split keeps the master at account 0 while the operational key moves to account >= 1, so
// bumping `BOX_INDEX` alone would desync the stored index, the derived key, AND the master/op
// identity. This compile-time gate forces that split to be implemented before `BOX_INDEX` changes,
// rather than leaving a silent runtime mismatch (review P3).
const _: () = assert!(
    BOX_INDEX == 0,
    "M1a derives NIP-06 account 0 and sets master_pubkey == op_pubkey; implement the M5 master/op \
     split before bumping BOX_INDEX"
);

/// The operator's seed-derived identity: the account-0 Nostr signer plus the Fedimint client root
/// secret, both deterministic from one BIP39 seed. Cheap to clone. No `Debug` — it holds secrets.
#[derive(Clone)]
pub struct OperatorIdentity {
    keys: Keys,
    fedimint_root_secret: [u8; 32],
}

impl OperatorIdentity {
    /// Derive the operator identity from a BIP39 `mnemonic` (+ optional passphrase). Deterministic:
    /// the same seed yields the same Nostr key AND the same Fedimint root secret, every time. A
    /// malformed mnemonic returns a structured `identity_invalid` error (never a panic / prompt).
    pub fn from_mnemonic(mnemonic: &str, passphrase: Option<&str>) -> Result<Self, IpcError> {
        let mnemonic = mnemonic.trim();
        // Validate + normalize via the SAME bip39 the nip06 derivation uses (re-exported through
        // the `nip06` feature), so the seed feeding HKDF is byte-identical to the NIP-06 seed.
        // bip39's `zeroize` feature (enabled in Cargo.toml) makes this decoded `Mnemonic` wipe its
        // word indices / entropy on drop, so the intermediate secret copy doesn't linger (§13).
        let parsed = nostr::bip39::Mnemonic::parse_normalized(mnemonic).map_err(|e| IpcError {
            code: "identity_invalid".into(),
            message: format!("invalid BIP39 mnemonic: {e}"),
            retryable: false,
        })?;
        // Wrap OUR 64-byte BIP39 seed in `Zeroizing` so it is wiped on EVERY return path — including
        // the early `?` error from `Keys::from_mnemonic_with_account` below — not just after a
        // successful derivation (review P2: the old explicit `seed.zeroize()` ran only on success).
        // This is honest best-effort: `Keys::from_mnemonic_*` re-parses the mnemonic and builds its
        // OWN seed / BIP32 Xpriv intermediates we cannot reach, so those copies are NOT wiped (see
        // the module doc). We zeroize the buffers we own; we do not claim all copies are erased.
        let seed = Zeroizing::new(parsed.to_seed_normalized(passphrase.unwrap_or_default()));

        // NIP-06 account `BOX_INDEX` (m/44'/1237'/BOX_INDEX'/0/0) — the marketplace signer. M1a is
        // account 0. Deriving with an EXPLICIT `Some(BOX_INDEX)` (rather than the implicit account-0
        // `from_mnemonic`) keeps the stored `box_index` and the derived key bound to the same source,
        // so they can't silently desync (review P3, tied to the `BOX_INDEX == 0` gate above).
        let keys = Keys::from_mnemonic_with_account(mnemonic, passphrase, Some(BOX_INDEX))
            .map_err(|e| IpcError {
                code: "identity_invalid".into(),
                message: format!("deriving NIP-06 account-{BOX_INDEX} key: {e}"),
                retryable: false,
            })?;

        let fedimint_root_secret = derive_fedimint_root_secret(seed.as_slice());
        // `seed` (Zeroizing) wipes itself when it drops at function exit — and on the early-error
        // path above — so the in-memory copy doesn't linger (§13) (review P2).
        Ok(OperatorIdentity {
            keys,
            fedimint_root_secret,
        })
    }

    /// The account-0 Nostr signer the engine (.5) signs listings / DMs with.
    pub fn keys(&self) -> &Keys {
        &self.keys
    }

    /// The account-0 public key (the listing signer + the inbound-DM `#p` recipient address).
    pub fn public_key(&self) -> PublicKey {
        self.keys.public_key()
    }

    /// The account-0 pubkey in hex — in M1a this is both `master_pubkey` and `op_pubkey` (§11).
    pub fn pubkey_hex(&self) -> String {
        self.keys.public_key().to_hex()
    }

    /// The account-0 pubkey as an `npub` (NIP-19 bech32).
    pub fn npub(&self) -> String {
        self.keys
            .public_key()
            .to_bech32()
            .expect("a valid secp256k1 public key always bech32-encodes")
    }

    /// The 32-byte PROVISIONAL Fedimint client root secret (ADR-0012, §4.6). Constructing the real
    /// Fedimint client from it is bead .4 (deferred), which MUST first confirm this is the form
    /// `fedimint-client` 0.11.1 consumes; this exposes only the deterministic secret.
    pub fn fedimint_root_secret(&self) -> &[u8; 32] {
        &self.fedimint_root_secret
    }
}

/// Wipe the Fedimint root secret when the identity drops, so it doesn't linger in freed memory
/// (the `Keys` value zeroizes its own secret key on drop; this covers the bytes we own) (review P3).
impl Drop for OperatorIdentity {
    fn drop(&mut self) {
        self.fedimint_root_secret.zeroize();
    }
}

/// HKDF-SHA256 the 64-byte BIP39 `seed` into the 32-byte PROVISIONAL Fedimint client root secret,
/// domain-separated by [`FEDIMINT_HKDF_INFO`] (§4.6). No salt: the domain separation is carried
/// entirely by the `info`, keeping the derivation a pure function of the seed so it reproduces the
/// same bytes every time (the recoverability property ADR-0012 will rely on once bead .4 confirms
/// this matches `fedimint-client` 0.11.1 — see [`FEDIMINT_HKDF_INFO`] for the provisional caveat).
fn derive_fedimint_root_secret(seed: &[u8]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(None, seed);
    let mut okm = [0u8; 32];
    hk.expand(FEDIMINT_HKDF_INFO, &mut okm)
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    okm
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A FIXED NIP-06 test vector taken from rust-nostr's own nip06 suite: this mnemonic derives a
    /// known account-0 secret key, so the pinned pubkey/npub below are a derivation vector, not a
    /// value we invented.
    const TEST_MNEMONIC: &str =
        "leader monkey parrot ring guide accident before fence cannon height naive bean";
    /// `m/44'/1237'/0'/0/0` pubkey (x-only hex) for `TEST_MNEMONIC` — the public half of the
    /// rust-nostr nip06 vector secret `7f7ff03d…cba9a`.
    const EXPECTED_PUBKEY_HEX: &str =
        "17162c921dc4d2518f9a101db33695df1afb56ab82f5ff3e5da6eec3ca5cd917";
    /// The same key as an `npub` (NIP-19 bech32 of `EXPECTED_PUBKEY_HEX`).
    const EXPECTED_NPUB: &str = "npub1zutzeysacnf9rru6zqwmxd54mud0k44tst6l70ja5mhv8jjumytsd2x7nu";
    /// HKDF-SHA256(seed, info=`lnrent:fedimint:v1`) -> 32 bytes, for the SAME seed (account-0,
    /// no passphrase). Pinned so the PROVISIONAL M1a derivation stays reproducible (a regression is
    /// caught here); bead .4 may revise the scheme once it confirms the real `fedimint-client` form.
    const EXPECTED_FEDIMINT_SECRET_HEX: &str =
        "1da7529d570811568840fec01879cd4a28a3eda2806d04bded10940f16ab9e0d";

    fn hex32(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    // ADR-0004 / §4.6: a FIXED seed derives a DETERMINISTIC account-0 npub/hex that matches the
    // NIP-06 vector — the same seed must always reproduce the same marketplace identity.
    #[test]
    fn account0_identity_is_deterministic_and_matches_nip06_vector() {
        let id = OperatorIdentity::from_mnemonic(TEST_MNEMONIC, None).unwrap();
        assert_eq!(id.pubkey_hex(), EXPECTED_PUBKEY_HEX);
        assert_eq!(id.npub(), EXPECTED_NPUB);

        // Determinism: re-deriving from the same seed yields the identical key, and surrounding
        // whitespace on the mnemonic is normalized away (an agent may pass it with a trailing \n).
        let again = OperatorIdentity::from_mnemonic(&format!("  {TEST_MNEMONIC}\n"), None).unwrap();
        assert_eq!(again.pubkey_hex(), id.pubkey_hex());
    }

    // ADR-0012 / §4.6: the PROVISIONAL Fedimint client root secret HKDF-SHA256(seed,
    // `lnrent:fedimint:v1`) is deterministic and pinned (M1a), in a domain DISJOINT from the Nostr
    // key space. Bead .4 confirms/revises the scheme against the real fedimint-client (deferred).
    #[test]
    fn fedimint_root_secret_is_deterministic_pinned_and_disjoint() {
        let id = OperatorIdentity::from_mnemonic(TEST_MNEMONIC, None).unwrap();
        assert_eq!(
            hex32(id.fedimint_root_secret()),
            EXPECTED_FEDIMINT_SECRET_HEX
        );

        // Re-derivation is identical (recoverable from the seed alone, ADR-0012).
        let again = OperatorIdentity::from_mnemonic(TEST_MNEMONIC, None).unwrap();
        assert_eq!(again.fedimint_root_secret(), id.fedimint_root_secret());

        // Disjoint domains: the Fedimint secret is NOT the Nostr secret key bytes — the HKDF `info`
        // domain separation means one seed safely yields two independent secrets (§4.6).
        assert_ne!(
            id.fedimint_root_secret().as_slice(),
            id.keys().secret_key().to_secret_bytes().as_slice(),
            "Fedimint root secret must not equal the Nostr secret key"
        );
    }

    // A BIP39-invalid mnemonic fails with a structured, non-retryable error — never a panic.
    // (`OperatorIdentity` has no `Debug` so secrets can't leak; match instead of `unwrap_err`.)
    #[test]
    fn invalid_mnemonic_is_a_structured_error() {
        let err = match OperatorIdentity::from_mnemonic("not a real bip39 mnemonic at all", None) {
            Ok(_) => panic!("expected an error for an invalid mnemonic"),
            Err(e) => e,
        };
        assert_eq!(err.code, "identity_invalid");
        assert!(!err.retryable);
    }

    // The acceptance "the derived Keys signs and produces a valid 30402 listing" proves the key is
    // usable by the engine: build_listing + sign_with_keys, then parse_listing round-trips and the
    // coordinate is anchored to the derived pubkey.
    #[test]
    fn derived_keys_sign_a_valid_30402_listing() {
        use lnrent_wire::{build_listing, parse_listing, Listing};

        let id = OperatorIdentity::from_mnemonic(TEST_MNEMONIC, None).unwrap();
        let listing = Listing {
            d: "svc-1".into(),
            operator: id.pubkey_hex(),
            recipe_id: "dummy".into(),
            recipe_version: "1".into(),
            title: "Test service".into(),
            summary: "a derived-key listing".into(),
            amount_sat: 1000,
            period: "month".into(),
            params: vec![],
            operations: vec![],
            tier: None,
            version: lnrent_wire::SCHEMA_VERSION,
        };
        let event = build_listing(&listing)
            .expect("build")
            .sign_with_keys(id.keys())
            .expect("sign with the derived account-0 key");
        let parsed = parse_listing(&event).expect("parse a self-signed listing");
        assert_eq!(
            parsed.listing_id,
            format!("30402:{}:svc-1", id.pubkey_hex()),
            "the coordinate is anchored to the derived account-0 pubkey"
        );
        assert_eq!(parsed.listing.operator, id.pubkey_hex());
    }
}
