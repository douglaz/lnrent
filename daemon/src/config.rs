//! Operator config + headless bootstrap (lnrent-7fp.16; ADR-0004/0012/0014, SPEC.md §4.6/§4.7/§11).
//!
//! Onboarding is HEADLESS (ADR-0014, §4.7): the seed and config arrive via flags / env / a config
//! file / stdin — NEVER a required interactive prompt — so an operator AGENT can bootstrap a Box
//! with no human. A missing/invalid-config bootstrap fails with a DETERMINISTIC structured error
//! ({code, message, retryable}, reusing the §5.1 / [`IpcError`] shape) that maps to a nonzero exit,
//! never a prompt. (The lnrent-onboard skill is a human convenience on top; this library/CLI path
//! is the prompt-free contract.)
//!
//! [`bootstrap`] ties identity + config together: it derives the account-0 identity (identity.rs),
//! writes the single `operator` row (SPEC.md §11) through the sole-writer store actor (ADR-0001),
//! and persists a newly supplied seed to the data dir with tight (0600) perms only after the seed
//! has derived a valid identity. It is IDEMPOTENT: re-running with the same seed yields the same
//! row, never a duplicate or an inconsistent identity. M1a defaults to `payment_backend = mock`
//! (the .4 decision), which needs NO federation config; a Fedimint invite + gateway is required
//! ONLY when `payment_backend = fedimint`.

use std::fmt;
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::json;
use zeroize::Zeroize;

use crate::identity::{OperatorIdentity, BOX_INDEX};
use crate::ipc::IpcError;
use crate::store::Store;

/// A sane default relay set when the operator configures none (§5.2 "default to popular relays").
pub const DEFAULT_RELAYS: &[&str] = &[
    "wss://relay.damus.io",
    "wss://nos.lol",
    "wss://relay.primal.net",
];

/// The default data dir when unset — matches `LNRENT_DATA_DIR`'s default in the daemon/CLI.
const DEFAULT_DATA_DIR: &str = "./data";
/// The default compute backend (host containers; SPEC.md §8.1) when unset.
const DEFAULT_COMPUTE_BACKEND: &str = "host";
/// The data-dir file holding the operator BIP39 seed (0600; §13, never logged).
const SEED_FILE: &str = "operator.seed";
/// The data-dir file holding Fedimint join config (0600; §4.6/§13).
const FEDIMINT_CONFIG_FILE: &str = "fedimint.json";

/// The receive backend the operator runs (SPEC.md §11 `payment_backend`). M1a defaults to `mock`
/// (the .4 MockPayment decision); `fedimint` is the real primary backend (ADR-0012) and REQUIRES a
/// federation invite + gateway. `mock` requires neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentMode {
    Mock,
    Fedimint,
}

impl PaymentMode {
    /// The wire/storage spelling persisted to the `operator.payment_backend` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            PaymentMode::Mock => "mock",
            PaymentMode::Fedimint => "fedimint",
        }
    }

    fn parse(s: &str) -> Result<Self, IpcError> {
        match s {
            "mock" => Ok(PaymentMode::Mock),
            "fedimint" => Ok(PaymentMode::Fedimint),
            other => Err(config_err(format!(
                "unknown payment_backend `{other}` (expected `mock` or `fedimint`)"
            ))),
        }
    }
}

/// Fedimint receive config — REQUIRED only when `payment_backend = fedimint` (ADR-0012, §4.6). The
/// federation invite is also part of the backup: the seed alone can't restore the ecash position
/// (you must know which federation to rejoin), so onboard's backup must include it (§4.6).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FedimintConfig {
    pub invite: String,
    pub gateway: String,
}

impl fmt::Debug for FedimintConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FedimintConfig")
            .field("invite", &"<redacted>")
            .field("gateway", &"<redacted>")
            .finish()
    }
}

/// The loaded operator runtime config — the SPEC.md §11 `operator` row plus the data dir.
#[derive(Clone)]
pub struct OperatorConfig {
    pub data_dir: PathBuf,
    pub relays: Vec<String>,
    pub payment_backend: PaymentMode,
    pub compute_backend: String,
    /// Present iff `payment_backend = fedimint`.
    pub fedimint: Option<FedimintConfig>,
}

impl fmt::Debug for OperatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorConfig")
            .field("data_dir", &self.data_dir)
            .field("relays", &self.relays)
            .field("payment_backend", &self.payment_backend)
            .field("compute_backend", &self.compute_backend)
            .field("fedimint", &self.fedimint)
            .finish()
    }
}

/// The raw, unresolved bootstrap input — exactly what a config file / flags / env / stdin provide
/// (ADR-0014 non-interactive contract). Every field is optional; [`bootstrap`] fills defaults and
/// validates the required-when-fedimint fields, returning a structured error (never a prompt) when
/// a required value is missing. Deserializes from TOML or JSON so an operator agent can hand it in.
#[derive(Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RawConfig {
    pub data_dir: Option<String>,
    pub relays: Option<Vec<String>>,
    pub payment_backend: Option<String>,
    pub compute_backend: Option<String>,
    pub fedimint_invite: Option<String>,
    pub fedimint_gateway: Option<String>,
    /// The BIP39 seed (mnemonic). Optional here because a re-bootstrap reads the persisted seed
    /// from the data dir; a FIRST bootstrap must supply it (else a structured `seed_missing`).
    pub mnemonic: Option<String>,
}

/// A ready operator: the derived signer + secret (identity.rs) and the loaded config. The engine
/// (.5) takes `identity.keys()`; the runtime wiring (.21) holds this whole struct and boots from it.
#[derive(Clone)]
pub struct Operator {
    pub identity: OperatorIdentity,
    pub config: OperatorConfig,
}

/// Headlessly bootstrap the operator (ADR-0014, §4.7): resolve + validate config, derive the
/// account-0 identity from the seed, persist the seed (0600) and the single `operator` row, and
/// return the ready [`Operator`]. Idempotent on the seed; structured errors (never prompts) on bad
/// input — the caller maps `err.code` to a deterministic exit via [`exit_code`].
pub async fn bootstrap(mut raw: RawConfig, store: &Store) -> Result<Operator, IpcError> {
    // The supplied mnemonic is secret (§13). Move it out of `raw` into a guard that zeroizes the
    // plaintext on EVERY return path — including the early `?` errors below (a missing Fedimint
    // config, an invalid mnemonic, an I/O failure), which would otherwise drop the original `String`
    // unwiped because `RawConfig` does not zeroize on drop (review P3/R1). Taking it out also keeps
    // it out of the `&raw` reads the rest of bootstrap makes — only `resolve_seed` needs it.
    let supplied_mnemonic = SuppliedMnemonic(raw.mnemonic.take());

    let mut config = resolve_config(&raw)?;
    // The data dir holds the operator seed (0600) and the state DB; make every directory WE create
    // — plus the leaf data dir even when it pre-existed — owner-only (0700) so a co-tenant local
    // user can't traverse it or read future artifacts in it (defense in depth beyond the per-file
    // perms). Normalize before deciding which components are ours to create so a raw `new/..`
    // segment cannot make us chmod a pre-existing shared parent after creation (review P2/P3).
    config.data_dir = normalize_path_lexically(&config.data_dir);
    create_private_dir_all(&config.data_dir)?;

    // Fedimint needs its join config available BEFORE we touch the store — supplied now OR already
    // durable in the data dir from a prior bootstrap. This fails an explicit `payment_backend=
    // fedimint` FIRST bootstrap (nothing stored yet) with a structured error before persisting any
    // row, while still letting a restored fedimint operator re-bootstrap from the data dir without
    // re-supplying the invite (review P2: don't hard-require the invite when it's already durable).
    if config.payment_backend == PaymentMode::Fedimint {
        validate_fedimint_config_available(&config.data_dir, &raw)?;
    }

    // The seed is authoritative once persisted: a re-bootstrap reads it back (and rejects a
    // conflicting supplied seed), so the derived identity is stable across runs. On first
    // bootstrap, validate/derive before writing the seed file; an invalid mnemonic must not poison
    // the data dir and wedge deterministic retries.
    let seed = resolve_seed(&config.data_dir, supplied_mnemonic.0.as_deref())?;
    let identity = OperatorIdentity::from_mnemonic(&seed.mnemonic, None)?;
    // The identity is derived; the supplied mnemonic is no longer needed — drop the guard now (it
    // zeroizes the plaintext) rather than waiting for function exit, to minimize its lifetime. The
    // working copy now lives in `seed`, zeroized on its own drop. Never logged (§13).
    drop(supplied_mnemonic);

    // If this is a re-bootstrap that omitted `payment_backend`, the store row may inherit a stored
    // `fedimint` backend even though `resolve_config` defaulted the raw value to `mock`. Validate the
    // durable Fedimint config before `persist_operator_row` commits mutable row updates (relays /
    // compute), so a failed config reload can't leave partially-mutated durable state (review P2).
    validate_inherited_fedimint_config_before_store(store, &identity, &config, &raw).await?;

    // Persist the single operator row. On a re-bootstrap this reconciles the mutable config against
    // the stored row — it never silently changes the money-routing backend, and inherits any omitted
    // field instead of resetting it — and returns the authoritative relays/compute so the returned
    // `Operator.config` matches what is durably stored.
    let persisted = persist_operator_row(store, &identity, &config, &raw).await?;
    config.relays = persisted.relays;
    config.compute_backend = persisted.compute_backend;
    config.payment_backend = persisted.payment_backend;
    config.fedimint =
        resolve_durable_fedimint_config(&config.data_dir, config.payment_backend, &raw)?;

    if seed.persist_after_success {
        write_seed(
            &config.data_dir,
            &config.data_dir.join(SEED_FILE),
            &seed.mnemonic,
        )?;
    }
    Ok(Operator { identity, config })
}

/// Resolve [`RawConfig`] into a validated [`OperatorConfig`], applying M1a defaults (mock backend,
/// popular relays, host compute) and enforcing the required-when-fedimint fields.
fn resolve_config(raw: &RawConfig) -> Result<OperatorConfig, IpcError> {
    // A blank value is treated as unset (an agent passing `--payment-backend ""` gets the M1a
    // default, not an "unknown payment_backend ``" error) — consistent with the relay/fedimint
    // handling below.
    let payment_backend = match non_empty(raw.payment_backend.as_deref()) {
        Some(s) => PaymentMode::parse(&s)?,
        None => PaymentMode::Mock, // M1a default (the .4 MockPayment decision)
    };

    let fedimint = match payment_backend {
        // `mock` requires NO federation config; ignore any stray fedimint_* fields rather than
        // failing, so an operator can switch modes without scrubbing the config file.
        PaymentMode::Mock => None,
        // The required-when-fedimint check is DEFERRED to `validate_fedimint_config_available` /
        // `resolve_durable_fedimint_config`, which also accept a config already durable in the data
        // dir — so an explicit `payment_backend=fedimint` re-bootstrap need not re-supply the invite
        // (review P2). Here we only surface a *partial* supplied pair (one of invite/gateway) as an
        // early error, and carry a fully-supplied pair through.
        PaymentMode::Fedimint => supplied_fedimint_config(raw)?,
    };

    let relays = match &raw.relays {
        Some(r) if !r.is_empty() => normalize_relays(r),
        _ => DEFAULT_RELAYS.iter().map(|s| s.to_string()).collect(),
    };

    // Blank `data_dir` / `compute_backend` also fall back to defaults rather than being stored
    // verbatim (a blank data dir would otherwise fail later at create_dir_all) (review P3).
    let data_dir = PathBuf::from(
        non_empty(raw.data_dir.as_deref()).unwrap_or_else(|| DEFAULT_DATA_DIR.to_string()),
    );
    let compute_backend = non_empty(raw.compute_backend.as_deref())
        .unwrap_or_else(|| DEFAULT_COMPUTE_BACKEND.to_string());
    // An EXPLICITLY supplied compute backend is validated against the canonical recipe allowlist
    // (the SAME `host|incus|libvirt|proxmox|cloud-*` set recipe.rs enforces), so a typo like
    // `hsot` fails with a deterministic `config_invalid` instead of being persisted into the
    // operator row — matching the fail-fast `payment_backend` handling and the ADR-0014 "invalid
    // config -> structured error, never stored" contract (review R2 P2). The `host` default is
    // always valid; on a re-bootstrap an omitted backend inherits the (already-validated) stored
    // value in `persist_operator_row`, so this only rejects a bad supplied value.
    if !crate::recipe::is_known_compute_backend(&compute_backend) {
        return Err(config_err(format!(
            "unknown compute_backend `{compute_backend}` (expected `host`, `incus`, `libvirt`, \
             `proxmox`, or `cloud-*`)"
        )));
    }

    Ok(OperatorConfig {
        data_dir,
        relays,
        payment_backend,
        compute_backend,
        fedimint,
    })
}

/// `Some(trimmed)` when `s` is present and non-blank, else `None` — a blank required field is as
/// good as missing (an agent passing `--fedimint-invite ""` gets the same structured error).
fn non_empty(s: Option<&str>) -> Option<String> {
    s.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn normalize_relays(relays: &[String]) -> Vec<String> {
    let filtered: Vec<String> = relays
        .iter()
        .filter_map(|relay| non_empty(Some(relay)))
        .collect();
    if filtered.is_empty() {
        DEFAULT_RELAYS.iter().map(|s| s.to_string()).collect()
    } else {
        filtered
    }
}

struct ResolvedSeed {
    mnemonic: String,
    persist_after_success: bool,
}

/// Wipe the in-memory seed copy when it drops so the mnemonic doesn't linger in freed memory
/// (it is never logged, §13; this closes the in-process residue too) (review P3).
impl Drop for ResolvedSeed {
    fn drop(&mut self) {
        self.mnemonic.zeroize();
    }
}

/// Owns the operator-supplied mnemonic for the lifetime of [`bootstrap`] and zeroizes the plaintext
/// on drop — so it is wiped on EVERY return path, not only after a successful derivation. The
/// supplied `RawConfig` does not zeroize on drop, so an early-error path (missing Fedimint config,
/// invalid mnemonic, I/O failure) would otherwise leave the original `String` lingering in freed
/// memory (§13, never logged) (review P3/R1).
struct SuppliedMnemonic(Option<String>);

impl Drop for SuppliedMnemonic {
    fn drop(&mut self) {
        if let Some(mnemonic) = self.0.as_mut() {
            mnemonic.zeroize();
        }
    }
}

/// Resolve the operator seed (mnemonic). The persisted data-dir seed is authoritative once written:
/// a first bootstrap requires a supplied seed but DOES NOT persist it yet; bootstrap first derives a
/// valid identity and checks the store row, then writes it 0600. A re-bootstrap reads the persisted
/// seed and rejects a DIFFERENT supplied seed (a `seed_conflict` — we never silently overwrite an
/// identity). The seed is secret: it is written/compared, never logged (§13).
fn resolve_seed(data_dir: &Path, supplied: Option<&str>) -> Result<ResolvedSeed, IpcError> {
    let path = data_dir.join(SEED_FILE);
    let supplied = supplied.map(str::trim).filter(|s| !s.is_empty());
    match read_secret_file_to_string(&path, "operator seed")? {
        Some(mut file_buf) => {
            // Move the trimmed mnemonic into the zeroizing `ResolvedSeed` guard IMMEDIATELY, before
            // the conflict check / perms re-tighten below. Both of those are early-return paths, and
            // holding the plaintext in a bare `String` until then would drop it UNwiped on a
            // `seed_conflict` or a `harden_perms` failure (§13) (review P3/R1). With the guard, every
            // exit from this arm zeroizes the persisted-seed copy on drop.
            let resolved = ResolvedSeed {
                mnemonic: file_buf.trim().to_string(),
                persist_after_success: false,
            };
            // Wipe the raw file buffer too; only the guarded copy should remain (§13) (review P3).
            file_buf.zeroize();
            if let Some(sup) = supplied {
                if sup != resolved.mnemonic {
                    return Err(IpcError {
                        code: "seed_conflict".into(),
                        message: "a different operator seed is already persisted in the data dir"
                            .into(),
                        retryable: false,
                    });
                }
            }
            // Re-tighten perms in case the file was created out-of-band with looser perms.
            harden_perms(&path)?;
            Ok(resolved)
        }
        None => {
            let mnemonic = supplied.ok_or_else(|| IpcError {
                code: "seed_missing".into(),
                message:
                    "no operator seed: supply a BIP39 mnemonic (flag/env/config/stdin) on first bootstrap"
                        .into(),
                retryable: false,
            })?;
            Ok(ResolvedSeed {
                mnemonic: mnemonic.to_string(),
                persist_after_success: true,
            })
        }
    }
}

/// Write the seed ATOMICALLY with owner-only (0600) perms; never logged (§13). Staged into an
/// exclusive temp file in the SAME dir (fsync'd), then renamed over the destination: rename(2)
/// within a dir is atomic, so a crash/SIGKILL mid-write can never leave a truncated seed — which
/// would otherwise be re-read as a `seed_conflict` and wedge an otherwise-correct retry, since the
/// operator row is already committed by the time we get here (review P2). A pre-existing seed is
/// never clobbered: identical content is idempotent, different content is a conflict (a rare
/// same-data-dir race; the common sequential conflict is already caught in `resolve_seed`).
fn write_seed(dir: &Path, path: &Path, mnemonic: &str) -> Result<(), IpcError> {
    if let Some(mut existing) = read_secret_file_to_string(path, "operator seed")? {
        let same = existing.trim() == mnemonic;
        existing.zeroize();
        if same {
            harden_perms(path)?;
            return Ok(());
        }
        return Err(IpcError {
            code: "seed_conflict".into(),
            message: "a different operator seed is already persisted in the data dir".into(),
            retryable: false,
        });
    }

    let mut bytes = mnemonic.as_bytes().to_vec();
    bytes.push(b'\n');
    let result = write_secret_file_atomic(dir, path, &bytes, "seed");
    // The staged buffer held the plaintext mnemonic; wipe it now (§13, never logged) (review P3).
    bytes.zeroize();
    result
}

fn resolve_durable_fedimint_config(
    data_dir: &Path,
    payment_backend: PaymentMode,
    raw: &RawConfig,
) -> Result<Option<FedimintConfig>, IpcError> {
    if payment_backend == PaymentMode::Mock {
        return Ok(None);
    }

    if let Some(cfg) = supplied_fedimint_config(raw)? {
        write_fedimint_config(data_dir, &cfg)?;
        return Ok(Some(cfg));
    }

    Ok(Some(read_fedimint_config(data_dir)?))
}

fn supplied_fedimint_config(raw: &RawConfig) -> Result<Option<FedimintConfig>, IpcError> {
    let invite = non_empty(raw.fedimint_invite.as_deref());
    let gateway = non_empty(raw.fedimint_gateway.as_deref());
    match (invite, gateway) {
        (Some(invite), Some(gateway)) => Ok(Some(FedimintConfig { invite, gateway })),
        (None, None) => Ok(None),
        _ => Err(config_err(
            "payment_backend=fedimint requires both `fedimint_invite` and `fedimint_gateway`",
        )),
    }
}

fn read_fedimint_config(data_dir: &Path) -> Result<FedimintConfig, IpcError> {
    let path = data_dir.join(FEDIMINT_CONFIG_FILE);
    let Some(mut raw) = read_secret_file_bytes(&path, "Fedimint config")? else {
        return Err(config_err(
            "payment_backend=fedimint requires `fedimint_invite` and `fedimint_gateway` \
             (no durable fedimint config found in the data dir)",
        ));
    };
    harden_perms(&path)?;
    let parsed = serde_json::from_slice::<FedimintConfig>(&raw).map_err(|e| {
        config_err(format!(
            "invalid durable Fedimint config {}: {e}",
            path.display()
        ))
    });
    raw.zeroize();
    let cfg = parsed?;
    let invite = non_empty(Some(&cfg.invite)).ok_or_else(|| {
        config_err(format!(
            "invalid durable Fedimint config {}: blank invite",
            path.display()
        ))
    })?;
    let gateway = non_empty(Some(&cfg.gateway)).ok_or_else(|| {
        config_err(format!(
            "invalid durable Fedimint config {}: blank gateway",
            path.display()
        ))
    })?;
    Ok(FedimintConfig { invite, gateway })
}

fn write_fedimint_config(data_dir: &Path, cfg: &FedimintConfig) -> Result<(), IpcError> {
    let path = data_dir.join(FEDIMINT_CONFIG_FILE);
    // Reconcile against any stored config. The federation INVITE pins WHERE the ecash lives, so a
    // re-bootstrap that changes it is a `config_conflict` — silently repointing to a new federation
    // could orphan the operator's ecash position (§4.6), so we refuse rather than overwrite. The
    // gateway is fungible (just a routing endpoint) and may be updated freely (review P2).
    if let Some(mut existing_bytes) = read_secret_file_bytes(&path, "Fedimint config")? {
        if let Ok(existing) = serde_json::from_slice::<FedimintConfig>(&existing_bytes) {
            existing_bytes.zeroize();
            if existing.invite.trim() != cfg.invite.trim() {
                return Err(config_conflict_err(
                    "operator already bootstrapped with a different Fedimint federation invite; \
                     refusing to repoint to a new federation on re-bootstrap (this could orphan \
                     the ecash position — clear the data dir to start a fresh federation)",
                ));
            }
            if existing.gateway.trim() == cfg.gateway.trim() {
                // Same federation AND gateway: idempotent.
                harden_perms(&path)?;
                return Ok(());
            }
            // Same federation, updated (fungible) gateway: fall through to rewrite.
        } else {
            existing_bytes.zeroize();
        }
        // An unparseable stored file is replaced by the valid supplied config.
    }

    let mut bytes = serde_json::to_vec(cfg)
        .map_err(|e| internal_err(format!("serializing Fedimint config: {e}")))?;
    bytes.push(b'\n');
    write_secret_file_atomic(data_dir, &path, &bytes, "Fedimint config")
}

/// Fedimint REQUIRES its join config (invite + gateway) before bootstrap touches the store:
/// either supplied in this run OR already durable in the data dir from a prior bootstrap. This is a
/// validation-only check; `resolve_durable_fedimint_config` does the post-commit durable write/load.
fn validate_fedimint_config_available(data_dir: &Path, raw: &RawConfig) -> Result<(), IpcError> {
    if let Some(cfg) = supplied_fedimint_config(raw)? {
        validate_fedimint_config_reconcile(data_dir, &cfg)?;
        return Ok(());
    }
    read_fedimint_config(data_dir).map(|_| ())
}

fn validate_fedimint_config_reconcile(
    data_dir: &Path,
    cfg: &FedimintConfig,
) -> Result<(), IpcError> {
    let path = data_dir.join(FEDIMINT_CONFIG_FILE);
    let Some(mut existing_bytes) = read_secret_file_bytes(&path, "Fedimint config")? else {
        return Ok(());
    };
    let parsed = serde_json::from_slice::<FedimintConfig>(&existing_bytes);
    existing_bytes.zeroize();
    if let Ok(existing) = parsed {
        if existing.invite.trim() != cfg.invite.trim() {
            return Err(config_conflict_err(
                "operator already bootstrapped with a different Fedimint federation invite; \
                 refusing to repoint to a new federation on re-bootstrap (this could orphan \
                 the ecash position — clear the data dir to start a fresh federation)",
            ));
        }
    }
    Ok(())
}

async fn validate_inherited_fedimint_config_before_store(
    store: &Store,
    identity: &OperatorIdentity,
    config: &OperatorConfig,
    raw: &RawConfig,
) -> Result<(), IpcError> {
    if non_empty(raw.payment_backend.as_deref()).is_some() {
        return Ok(());
    }
    if config.payment_backend == PaymentMode::Fedimint {
        return Ok(());
    }
    if stored_payment_backend_for_identity(store, identity)
        .await?
        .as_deref()
        == Some("fedimint")
    {
        validate_fedimint_config_available(&config.data_dir, raw)?;
    }
    Ok(())
}

async fn stored_payment_backend_for_identity(
    store: &Store,
    identity: &OperatorIdentity,
) -> Result<Option<String>, IpcError> {
    let pubkey_hex = identity.pubkey_hex();
    store
        .read(move |c| {
            let row: Option<(String, String)> = c
                .query_row(
                    "SELECT master_pubkey, payment_backend FROM operator LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            Ok(row.and_then(|(master, payment)| (master == pubkey_hex).then_some(payment)))
        })
        .await
        .map_err(|e| internal_err(format!("reading operator row: {e}")))
}

fn read_secret_file_to_string(path: &Path, what: &str) -> Result<Option<String>, IpcError> {
    let Some(mut bytes) = read_secret_file_bytes(path, what)? else {
        return Ok(None);
    };
    let s = std::str::from_utf8(&bytes)
        .map(str::to_string)
        .map_err(|e| config_err(format!("{what} {} is not valid UTF-8: {e}", path.display())));
    bytes.zeroize();
    s.map(Some)
}

fn read_secret_file_bytes(path: &Path, what: &str) -> Result<Option<Vec<u8>>, IpcError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(internal_err(format!("stat {what} {}: {e}", path.display()))),
    };
    if meta.file_type().is_symlink() {
        return Err(config_err(format!(
            "{what} {} must be a regular file in the data dir, not a symlink",
            path.display()
        )));
    }
    if !meta.file_type().is_file() {
        return Err(config_err(format!(
            "{what} {} must be a regular file",
            path.display()
        )));
    }

    let mut file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ELOOP) {
                config_err(format!(
                    "{what} {} must be a regular file in the data dir, not a symlink",
                    path.display()
                ))
            } else if e.kind() == ErrorKind::NotFound {
                internal_err(format!(
                    "{what} {} disappeared while reading it; retry bootstrap",
                    path.display()
                ))
            } else {
                internal_err(format!("reading {what} {}: {e}", path.display()))
            }
        })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| internal_err(format!("reading {what} {}: {e}", path.display())))?;
    Ok(Some(bytes))
}

/// Atomically write a secret/config file with an exclusive 0600 temp file in the same directory.
/// `create_new(true)` refuses pre-existing temp paths, so stale files or symlinks are never reused.
fn write_secret_file_atomic(
    dir: &Path,
    path: &Path,
    bytes: &[u8],
    what: &str,
) -> Result<(), IpcError> {
    let (tmp, mut f) = create_secret_temp_file(dir, path)?;
    // Write + fsync the bytes, then drop the handle; on any error clean up the temp so it can't
    // accumulate or be mistaken for the real seed.
    let staged = f.write_all(bytes).and_then(|_| f.sync_all());
    drop(f);
    if let Err(e) = staged {
        let _ = fs::remove_file(&tmp);
        return Err(internal_err(format!(
            "writing {what} temp {}: {e}",
            tmp.display()
        )));
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(internal_err(format!(
            "installing {what} {}: {e}",
            path.display()
        )));
    }
    harden_perms(path)?;
    // fsync the directory so the rename itself survives a crash (best effort).
    if let Ok(dirf) = File::open(dir) {
        let _ = dirf.sync_all();
    }
    Ok(())
}

fn create_secret_temp_file(dir: &Path, path: &Path) -> Result<(PathBuf, File), IpcError> {
    let prefix = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("secret");
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    for attempt in 0..128 {
        let tmp = dir.join(format!(
            ".{prefix}.tmp.{}.{}.{}",
            std::process::id(),
            stamp,
            attempt
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(f) => return Ok((tmp, f)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(internal_err(format!(
                    "creating temp file {}: {e}",
                    tmp.display()
                )))
            }
        }
    }
    Err(internal_err(format!(
        "creating temp file for {}: too many collisions",
        path.display()
    )))
}

fn harden_perms(path: &Path) -> Result<(), IpcError> {
    let meta = fs::symlink_metadata(path)
        .map_err(|e| internal_err(format!("stat secret file {}: {e}", path.display())))?;
    if meta.file_type().is_symlink() {
        return Err(config_err(format!(
            "secret file {} must be a regular file in the data dir, not a symlink",
            path.display()
        )));
    }
    if !meta.file_type().is_file() {
        return Err(config_err(format!(
            "secret file {} must be a regular file",
            path.display()
        )));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|e| internal_err(format!("perms on {}: {e}", path.display())))
}

fn normalize_path_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(
                    normalized.components().next_back(),
                    Some(Component::Normal(_))
                ) {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

fn create_private_dir_all(dir: &Path) -> Result<(), IpcError> {
    // `exists()` and `create_dir_all` both FOLLOW symlinks, so if the data dir — or any already
    // existing component on the path to it — is a symlink to a directory, the later 0600
    // `operator.seed` / `fedimint.json` writes land in the symlink target, OUTSIDE the private
    // data-dir boundary. `O_NOFOLLOW` only guards the final secret file open, not the directory
    // walk, so a co-tenant who plants such a symlink could redirect the secrets. lstat every
    // existing component and refuse any symlink before we create or write anything (review P2/R1).
    reject_symlinked_components(dir)?;

    let created_dirs: Vec<PathBuf> = dir
        .ancestors()
        .take_while(|p| !p.as_os_str().is_empty() && !p.exists())
        .map(Path::to_path_buf)
        .collect();

    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| internal_err(format!("creating data dir {}: {e}", dir.display())))?;

    // Always tighten the LEAF data dir to 0700, even when it ALREADY existed: it holds our 0600
    // secrets, so it must be owner-only — but `DirBuilder::mode` applies only to components it
    // actually creates, so a leaf an installer or earlier daemon run made under a loose umask
    // (e.g. 0755) would otherwise stay group/world-traversable even though the seed file is 0600
    // (review R1 P2). Pre-existing PARENTS are deliberately left untouched — they may be shared
    // (e.g. a home dir) — so only the leaf plus the components WE created get hardened.
    harden_dir_perms(dir)?;
    for created in created_dirs.iter().rev() {
        if created.as_path() != dir {
            harden_dir_perms(created)?;
        }
    }
    Ok(())
}

/// Refuse to bootstrap through a symlinked directory component. `dir` is already lexically
/// normalized (no `.`/`..` segments), so its ancestors are the literal path components. lstat each
/// EXISTING one (root → leaf, so the error names the shallowest offender) and reject any symlink;
/// components that don't exist yet are ours to create as real 0700 dirs (review P2/R1).
fn reject_symlinked_components(dir: &Path) -> Result<(), IpcError> {
    let mut components: Vec<&Path> = dir
        .ancestors()
        .filter(|p| !p.as_os_str().is_empty())
        .collect();
    components.reverse();
    for component in components {
        match fs::symlink_metadata(component) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(config_err(format!(
                    "data dir component {} is a symlink; refusing to bootstrap through it (it could \
                     redirect the operator seed / config outside the data dir)",
                    component.display()
                )));
            }
            // A real, already-existing component is fine; `create_dir_all` surfaces a clear error
            // later if one is a non-directory file.
            Ok(_) => {}
            // Not created yet — we make it a real dir below; nothing to reject.
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => {
                return Err(internal_err(format!(
                    "stat data dir component {}: {e}",
                    component.display()
                )))
            }
        }
    }
    Ok(())
}

fn harden_dir_perms(dir: &Path) -> Result<(), IpcError> {
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
        .map_err(|e| internal_err(format!("perms on data dir {}: {e}", dir.display())))
}

/// The mutable config that was actually persisted (so the returned `Operator.config` matches the
/// durable row even when a re-bootstrap inherited a stored value).
struct PersistedConfig {
    relays: Vec<String>,
    compute_backend: String,
    payment_backend: PaymentMode,
}

/// The result of reconciling against the (possibly pre-existing) operator row.
enum RowOutcome {
    /// Inserted or refreshed in place; carries the authoritative mutable config.
    Persisted {
        relays: Vec<String>,
        compute: String,
        payment: String,
    },
    /// Same data dir, different master identity — refuse rather than clobber it.
    IdentityConflict,
    /// Same identity, but the re-resolved payment backend differs from the stored one.
    PaymentConflict { stored: String },
}

/// Write the single `operator` row (SPEC.md §11) via the sole-writer store actor (ADR-0001),
/// idempotently. M1a single-key: `master_pubkey == op_pubkey` (account-0 hex), `box_index = 0`.
///
/// Reconciliation on a re-bootstrap (same identity already stored):
/// - `payment_backend` routes real money and is FIXED at bootstrap: an EXPLICITLY supplied
///   different backend is rejected, while an omitted backend inherits the stored one so a restored
///   Fedimint operator can boot from the data dir without silently downgrading to `mock`.
/// - `relays` / `compute_backend` are mutable, but only an EXPLICITLY supplied value updates them;
///   an omitted field inherits the stored value instead of resetting it to a default (review P2).
///
/// A pre-existing row with a DIFFERENT master identity is always a conflict (never overwritten).
async fn persist_operator_row(
    store: &Store,
    identity: &OperatorIdentity,
    config: &OperatorConfig,
    raw: &RawConfig,
) -> Result<PersistedConfig, IpcError> {
    let pubkey_hex = identity.pubkey_hex();
    let payment_backend = config.payment_backend.as_str().to_string();
    let resolved_compute = config.compute_backend.clone();
    let resolved_relays = config.relays.clone();
    // Whether the caller EXPLICITLY supplied each mutable field (vs. it being filled by a default in
    // `resolve_config`) — an omitted field inherits the stored value on a re-bootstrap.
    let relays_supplied = raw.relays.as_ref().is_some_and(|r| !r.is_empty());
    // A blank value counts as unset (consistent with `resolve_config`), so an omitted/blank field
    // inherits the stored row rather than overwriting it with a default.
    let compute_supplied = non_empty(raw.compute_backend.as_deref()).is_some();
    let payment_supplied = non_empty(raw.payment_backend.as_deref()).is_some();

    let outcome = store
        .transaction(move |tx| {
            let existing: Option<(i64, String, String, String, String)> = tx
                .query_row(
                    "SELECT rowid, master_pubkey, payment_backend, compute_backend, relays \
                     FROM operator LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
                )
                .optional()?;
            match existing {
                None => {
                    let relays_json = json!(&resolved_relays).to_string();
                    tx.execute(
                        "INSERT INTO operator \
                         (master_pubkey, box_index, op_pubkey, payment_backend, compute_backend, relays) \
                         VALUES (?, ?, ?, ?, ?, ?)",
                        rusqlite::params![
                            &pubkey_hex,
                            BOX_INDEX,
                            &pubkey_hex,
                            &payment_backend,
                            &resolved_compute,
                            &relays_json
                        ],
                    )?;
                    Ok(RowOutcome::Persisted {
                        relays: resolved_relays,
                        compute: resolved_compute,
                        payment: payment_backend,
                    })
                }
                Some((rowid, master, stored_payment, stored_compute, stored_relays))
                    if master == pubkey_hex =>
                {
                    // Same identity. The money-routing backend is fixed at bootstrap: inherit it
                    // when omitted, reject an explicit change.
                    let payment = if payment_supplied {
                        if stored_payment != payment_backend {
                            return Ok(RowOutcome::PaymentConflict {
                                stored: stored_payment,
                            });
                        }
                        payment_backend
                    } else {
                        stored_payment
                    };
                    if payment != "mock" && payment != "fedimint" {
                        return Ok(RowOutcome::PaymentConflict {
                            stored: payment,
                        });
                    }
                    // Inherit omitted mutable fields from the stored row; a bad stored relays JSON
                    // (shouldn't happen — we wrote it) falls back to the re-resolved value.
                    let relays = if relays_supplied {
                        resolved_relays
                    } else {
                        serde_json::from_str::<Vec<String>>(&stored_relays)
                            .map(|relays| normalize_relays(&relays))
                            .unwrap_or(resolved_relays)
                    };
                    let compute = if compute_supplied {
                        resolved_compute
                    } else {
                        stored_compute
                    };
                    let relays_json = json!(&relays).to_string();
                    tx.execute(
                        "UPDATE operator SET box_index=?, op_pubkey=?, payment_backend=?, \
                         compute_backend=?, relays=? WHERE rowid=?",
                        rusqlite::params![
                            BOX_INDEX,
                            &pubkey_hex,
                            &payment,
                            &compute,
                            &relays_json,
                            rowid
                        ],
                    )?;
                    Ok(RowOutcome::Persisted {
                        relays,
                        compute,
                        payment,
                    })
                }
                // A different identity already owns this data dir.
                Some(_) => Ok(RowOutcome::IdentityConflict),
            }
        })
        .await
        .map_err(|e| internal_err(format!("persisting operator row: {e}")))?;

    match outcome {
        RowOutcome::Persisted {
            relays,
            compute,
            payment,
        } => Ok(PersistedConfig {
            relays,
            compute_backend: compute,
            payment_backend: PaymentMode::parse(&payment)?,
        }),
        RowOutcome::IdentityConflict => Err(IpcError {
            code: "seed_conflict".into(),
            message: "a different operator identity is already bootstrapped in this data dir"
                .into(),
            retryable: false,
        }),
        RowOutcome::PaymentConflict { stored } => Err(config_conflict_err(format!(
            "operator already bootstrapped with payment_backend=`{stored}`; refusing to change it \
             to `{}` on re-bootstrap (the money-routing backend is fixed at bootstrap)",
            config.payment_backend.as_str()
        ))),
    }
}

/// Build a `config_invalid` structured error (non-retryable: re-running with the same bad config
/// fails identically — the operator must fix the config, §4.7).
fn config_err(message: impl Into<String>) -> IpcError {
    IpcError {
        code: "config_invalid".into(),
        message: message.into(),
        retryable: false,
    }
}

/// Build a `config_conflict` structured error (non-retryable: the supplied config contradicts what
/// is already durably bootstrapped — e.g. a re-bootstrap trying to change the fixed payment backend).
fn config_conflict_err(message: impl Into<String>) -> IpcError {
    IpcError {
        code: "config_conflict".into(),
        message: message.into(),
        retryable: false,
    }
}

/// Build an `internal` structured error (retryable: an I/O / store failure may be transient).
fn internal_err(message: impl Into<String>) -> IpcError {
    IpcError {
        code: "internal".into(),
        message: message.into(),
        retryable: true,
    }
}

/// Map a bootstrap-error `code` to a DETERMINISTIC nonzero exit code (ADR-0014 exit taxonomy,
/// mirroring the operator CLI): config/identity/seed problems are operator-fixable input errors
/// (3); an I/O / store failure is internal (5). Always nonzero — success is the caller's `0`.
pub fn exit_code(code: &str) -> u8 {
    match code {
        "config_invalid" | "config_conflict" | "identity_invalid" | "seed_missing"
        | "seed_conflict" => 3,
        "internal" => 5,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The fixed NIP-06 vector seed (see identity.rs): a known account-0 identity.
    const TEST_MNEMONIC: &str =
        "leader monkey parrot ring guide accident before fence cannon height naive bean";
    const EXPECTED_PUBKEY_HEX: &str =
        "17162c921dc4d2518f9a101db33695df1afb56ab82f5ff3e5da6eec3ca5cd917";

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    /// A unique temp data dir per test (all tests share one PID).
    fn temp_data_dir() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("lnrent-cfg-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    // `Operator` has no `Debug` (it holds secrets), so `unwrap_err` won't compile — extract the
    // structured error by hand.
    fn err_of(r: Result<Operator, IpcError>) -> IpcError {
        match r {
            Ok(_) => panic!("expected a structured error, got a ready Operator"),
            Err(e) => e,
        }
    }

    fn raw_mock(dir: &Path) -> RawConfig {
        RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        }
    }

    fn file_mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    // (count, master_pubkey, op_pubkey, box_index, payment_backend, relays)
    async fn read_operator_row(store: &Store) -> (i64, String, String, String, String, String) {
        store
            .read(|c| {
                let count: i64 =
                    c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?;
                let (m, o, b, p, r): (String, String, i64, String, String) = c.query_row(
                    "SELECT master_pubkey, op_pubkey, box_index, payment_backend, relays FROM operator LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )?;
                Ok((count, m, o, b.to_string(), p, r))
            })
            .await
            .unwrap()
    }

    // §4.7 / §11 + M1a mock mode: a fully non-interactive bootstrap (no prompt) from a config value
    // with payment_backend defaulting to mock and NO federation config persists the operator row —
    // master_pubkey == op_pubkey == the account-0 hex, box_index 0, default relays.
    #[tokio::test]
    async fn mock_bootstrap_is_noninteractive_and_persists_operator_row() {
        let dir = temp_data_dir();
        let store = mem_store();
        let op = bootstrap(raw_mock(&dir), &store).await.expect("bootstrap");

        assert_eq!(op.config.payment_backend, PaymentMode::Mock);
        assert!(
            op.config.fedimint.is_none(),
            "mock needs no federation config"
        );
        assert_eq!(
            op.config.relays, DEFAULT_RELAYS,
            "unset relays -> sane default"
        );
        assert_eq!(op.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);

        let (count, master, op_pubkey, box_index, payment, relays) =
            read_operator_row(&store).await;
        assert_eq!(count, 1);
        assert_eq!(master, EXPECTED_PUBKEY_HEX);
        assert_eq!(
            op_pubkey, EXPECTED_PUBKEY_HEX,
            "M1a single key: master == op"
        );
        assert_eq!(box_index, "0");
        assert_eq!(payment, "mock");
        let relays: Vec<String> = serde_json::from_str(&relays).unwrap();
        assert_eq!(relays, DEFAULT_RELAYS);

        // The seed was persisted to the data dir with owner-only (0600) perms and is never logged.
        let seed_path = dir.join(SEED_FILE);
        assert_eq!(file_mode(&seed_path), 0o600, "seed file must be 0600");
        assert!(
            !dir.join(FEDIMINT_CONFIG_FILE).exists(),
            "mock mode must not require or write Fedimint config"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Idempotent: a SECOND bootstrap with the SAME seed yields the same single row (no duplicate /
    // inconsistent state), even though the seed is now read back from the data dir (no re-supply).
    #[tokio::test]
    async fn second_bootstrap_same_seed_is_idempotent() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_mock(&dir), &store).await.expect("first");

        // Re-bootstrap WITHOUT re-supplying the seed: it is read back from the data dir.
        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store).await.expect("second");
        assert_eq!(op2.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);

        let (count, master, ..) = read_operator_row(&store).await;
        assert_eq!(count, 1, "still exactly one operator row");
        assert_eq!(master, EXPECTED_PUBKEY_HEX);
        let _ = fs::remove_dir_all(&dir);
    }

    // §11 fedimint mode: an invite + gateway are loaded and the row records `fedimint`.
    #[tokio::test]
    async fn fedimint_bootstrap_loads_invite_and_gateway() {
        let dir = temp_data_dir();
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            fedimint_invite: Some("fed11invite".into()),
            fedimint_gateway: Some("03gateway".into()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let op = bootstrap(raw, &store).await.expect("fedimint bootstrap");
        assert_eq!(op.config.payment_backend, PaymentMode::Fedimint);
        assert_eq!(
            op.config.fedimint,
            Some(FedimintConfig {
                invite: "fed11invite".into(),
                gateway: "03gateway".into()
            })
        );
        let (_c, _m, _o, _b, payment, _r) = read_operator_row(&store).await;
        assert_eq!(payment, "fedimint");
        let stored = read_fedimint_config(&dir).expect("durable fedimint config");
        assert_eq!(stored, op.config.fedimint.unwrap());
        assert_eq!(
            file_mode(&dir.join(FEDIMINT_CONFIG_FILE)),
            0o600,
            "Fedimint config file must be 0600"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // §4.7 ADR-0014: a missing-required-config run (fedimint without an invite) fails with a
    // structured error + a deterministic NONZERO exit — never an interactive prompt.
    #[tokio::test]
    async fn fedimint_without_invite_fails_with_structured_error_and_nonzero_exit() {
        let dir = temp_data_dir();
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            // no invite/gateway
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0, "config error -> nonzero exit");

        // And it failed BEFORE persisting anything: no operator row, no seed file.
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review regression: an invalid mnemonic on FIRST bootstrap must fail before writing
    // `operator.seed`; otherwise the bad seed wedges every deterministic retry.
    #[tokio::test]
    async fn invalid_mnemonic_on_first_bootstrap_does_not_poison_data_dir() {
        let dir = temp_data_dir();
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some("not a real bip39 mnemonic at all".into()),
            ..Default::default()
        };

        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "identity_invalid");
        assert!(
            !dir.join(SEED_FILE).exists(),
            "invalid first-bootstrap mnemonic must not be persisted"
        );
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0);

        // A corrected retry in the same data dir must now succeed without manual cleanup.
        bootstrap(raw_mock(&dir), &store)
            .await
            .expect("corrected seed can retry cleanly");
        let (count, master, ..) = read_operator_row(&store).await;
        assert_eq!(count, 1);
        assert_eq!(master, EXPECTED_PUBKEY_HEX);
        let _ = fs::remove_dir_all(&dir);
    }

    // A first bootstrap with NO seed supplied (and none persisted yet) is a structured `seed_missing`
    // with a nonzero exit — the headless contract: a structured failure, not a prompt.
    #[tokio::test]
    async fn missing_seed_on_first_bootstrap_is_structured_error() {
        let dir = temp_data_dir();
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "seed_missing");
        assert_ne!(exit_code(&err.code), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // Re-bootstrapping with a DIFFERENT seed in the same data dir is rejected (never silently
    // overwrites the persisted identity).
    #[tokio::test]
    async fn conflicting_seed_is_rejected() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_mock(&dir), &store).await.expect("first");

        let other = "zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo wrong"; // a different (invalid) seed string
        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(other.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw2, &store).await);
        assert_eq!(err.code, "seed_conflict");
        let _ = fs::remove_dir_all(&dir);
    }

    // If the store already belongs to a different identity, bootstrap fails before writing a
    // newly supplied seed into the data dir.
    #[tokio::test]
    async fn store_identity_conflict_does_not_write_new_seed() {
        let dir = temp_data_dir();
        let store = mem_store();
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO operator \
                     (master_pubkey, box_index, op_pubkey, payment_backend, compute_backend, relays) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                    rusqlite::params![
                        "00".repeat(32),
                        BOX_INDEX,
                        "00".repeat(32),
                        "mock",
                        DEFAULT_COMPUTE_BACKEND,
                        "[]"
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let err = err_of(bootstrap(raw_mock(&dir), &store).await);
        assert_eq!(err.code, "seed_conflict");
        assert!(
            !dir.join(SEED_FILE).exists(),
            "store conflict must not leave a stray seed file"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    fn raw_fedimint(dir: &Path) -> RawConfig {
        RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            fedimint_invite: Some("fed11invite".into()),
            fedimint_gateway: Some("03gateway".into()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        }
    }

    // Review P2: a re-bootstrap that OMITS the payment backend must inherit the stored `fedimint`
    // backend and reload the durable Fedimint join config from the data dir — never downgrade to
    // `mock`, and never require flags/env/stdin for values we already durably stored.
    #[tokio::test]
    async fn rebootstrap_omitting_fedimint_inherits_stored_backend_and_config() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store)
            .await
            .expect("first (fedimint) bootstrap");

        // Re-bootstrap with the seed read back from the data dir but NO payment config supplied.
        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store)
            .await
            .expect("fedimint config reload from data dir");
        assert_eq!(op2.config.payment_backend, PaymentMode::Fedimint);
        assert_eq!(
            op2.config.fedimint,
            Some(FedimintConfig {
                invite: "fed11invite".into(),
                gateway: "03gateway".into()
            })
        );

        // The stored backend was NOT downgraded.
        let (count, _m, _o, _b, payment, _r) = read_operator_row(&store).await;
        assert_eq!(count, 1);
        assert_eq!(payment, "fedimint", "stored backend must be untouched");
        let _ = fs::remove_dir_all(&dir);
    }

    // If the operator row says `fedimint` but the durable join config is missing, bootstrap fails
    // with a structured config error instead of returning a half-ready operator.
    #[tokio::test]
    async fn fedimint_row_without_durable_config_is_structured_error() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");
        fs::remove_file(dir.join(FEDIMINT_CONFIG_FILE)).unwrap();

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw2, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2 (R1): if an omitted payment backend inherits stored `fedimint`, the durable
    // Fedimint config must be validated BEFORE relay/compute row updates commit. A missing config
    // returns a structured error and leaves the operator row untouched.
    #[tokio::test]
    async fn inherited_fedimint_missing_config_fails_before_row_mutation() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");
        fs::remove_file(dir.join(FEDIMINT_CONFIG_FILE)).unwrap();

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            relays: Some(vec!["wss://mutated.example".into()]),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw2, &store).await);
        assert_eq!(err.code, "config_invalid");

        let (_c, _m, _o, _b, _p, relays) = read_operator_row(&store).await;
        let relays: Vec<String> = serde_json::from_str(&relays).unwrap();
        assert_eq!(
            relays, DEFAULT_RELAYS,
            "failed Fedimint config validation must not commit relay updates"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2: seed staging must not reuse the old predictable temp path. A planted symlink at
    // that name must not receive seed bytes or become the final `operator.seed`.
    #[tokio::test]
    async fn seed_temp_file_is_exclusive_and_does_not_follow_planted_symlink() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-seed-outside-{}", std::process::id()));
        fs::write(&outside, b"outside\n").unwrap();
        let planted = dir.join(format!("{SEED_FILE}.tmp.{}", std::process::id()));
        std::os::unix::fs::symlink(&outside, &planted).unwrap();

        let store = mem_store();
        bootstrap(raw_mock(&dir), &store).await.expect("bootstrap");
        assert_eq!(
            fs::read_to_string(&outside).unwrap(),
            "outside\n",
            "planted temp symlink must not receive the mnemonic"
        );
        assert!(
            !fs::symlink_metadata(dir.join(SEED_FILE))
                .unwrap()
                .file_type()
                .is_symlink(),
            "final seed file must be a real file in the data dir"
        );
        assert_eq!(file_mode(&dir.join(SEED_FILE)), 0o600);
        let _ = fs::remove_file(&outside);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2 (R1): a persisted seed must be a real data-dir file, not a symlink to another
    // readable file. The target is never read as the operator seed and its perms are not changed.
    #[tokio::test]
    async fn symlinked_persisted_seed_is_rejected() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-symlink-seed-{}", std::process::id()));
        fs::write(&outside, format!("{TEST_MNEMONIC}\n")).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&outside, dir.join(SEED_FILE)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert_eq!(
            file_mode(&outside),
            0o644,
            "bootstrap must not chmod a symlink target outside the data dir"
        );
        assert!(
            fs::symlink_metadata(dir.join(SEED_FILE))
                .unwrap()
                .file_type()
                .is_symlink(),
            "the rejected seed path remains a symlink"
        );
        let _ = fs::remove_file(&outside);
        let _ = fs::remove_dir_all(&dir);
    }

    // The same no-symlink rule applies to durable Fedimint config. If an inherited fedimint row
    // finds `fedimint.json` as a symlink, bootstrap fails before committing mutable row updates.
    #[tokio::test]
    async fn symlinked_fedimint_config_is_rejected_before_row_mutation() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");

        let fedimint_path = dir.join(FEDIMINT_CONFIG_FILE);
        fs::remove_file(&fedimint_path).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-symlink-fedimint-{}", std::process::id()));
        fs::write(
            &outside,
            serde_json::json!({"invite":"fed11invite","gateway":"03gateway"}).to_string(),
        )
        .unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&outside, &fedimint_path).unwrap();

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            relays: Some(vec!["wss://mutated.example".into()]),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw2, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert_eq!(
            file_mode(&outside),
            0o644,
            "bootstrap must not chmod a symlink target outside the data dir"
        );

        let (_c, _m, _o, _b, _p, relays) = read_operator_row(&store).await;
        let relays: Vec<String> = serde_json::from_str(&relays).unwrap();
        assert_eq!(relays, DEFAULT_RELAYS);
        let _ = fs::remove_file(&outside);
        let _ = fs::remove_dir_all(&dir);
    }

    // A re-bootstrap that re-supplies the SAME fedimint config is idempotent (one row, still fedimint).
    #[tokio::test]
    async fn rebootstrap_with_same_fedimint_config_is_idempotent() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");
        let op2 = bootstrap(raw_fedimint(&dir), &store)
            .await
            .expect("second fedimint bootstrap is idempotent");
        assert_eq!(op2.config.payment_backend, PaymentMode::Fedimint);

        let (count, _m, _o, _b, payment, _r) = read_operator_row(&store).await;
        assert_eq!(count, 1, "still exactly one operator row");
        assert_eq!(payment, "fedimint");
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2 (R1): a re-bootstrap that EXPLICITLY restates `payment_backend=fedimint` but does
    // NOT re-supply the invite/gateway must load them from the durable data-dir config — the early
    // requirement check must not reject config we already stored.
    #[tokio::test]
    async fn rebootstrap_explicit_fedimint_without_invite_loads_durable_config() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store)
            .await
            .expect("first (fedimint) bootstrap");

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            // invite/gateway intentionally omitted — they are durable in the data dir.
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store)
            .await
            .expect("explicit fedimint reloads durable invite/gateway");
        assert_eq!(op2.config.payment_backend, PaymentMode::Fedimint);
        assert_eq!(
            op2.config.fedimint,
            Some(FedimintConfig {
                invite: "fed11invite".into(),
                gateway: "03gateway".into()
            })
        );
        let (count, ..) = read_operator_row(&store).await;
        assert_eq!(count, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2 (R2): a re-bootstrap that supplies a DIFFERENT federation invite is a structured
    // `config_conflict` — silently repointing could orphan the ecash position, so the stored invite
    // is left untouched (only an explicit fresh-start may change federations).
    #[tokio::test]
    async fn rebootstrap_with_different_invite_is_config_conflict() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            fedimint_invite: Some("fed11DIFFERENT".into()),
            fedimint_gateway: Some("03gateway".into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw2, &store).await);
        assert_eq!(err.code, "config_conflict");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);

        // The durable invite is left untouched — the conflict never repoints the federation.
        let stored = read_fedimint_config(&dir).expect("durable config still present");
        assert_eq!(stored.invite, "fed11invite", "invite must be unchanged");
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2 (R2): the gateway is a fungible routing endpoint — a re-bootstrap with the SAME
    // invite but a NEW gateway is allowed and updates the durable config.
    #[tokio::test]
    async fn rebootstrap_with_updated_gateway_is_allowed() {
        let dir = temp_data_dir();
        let store = mem_store();
        bootstrap(raw_fedimint(&dir), &store).await.expect("first");

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            payment_backend: Some("fedimint".into()),
            fedimint_invite: Some("fed11invite".into()),
            fedimint_gateway: Some("03NEWGATEWAY".into()),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store)
            .await
            .expect("same federation, updated gateway is allowed");
        assert_eq!(
            op2.config.fedimint,
            Some(FedimintConfig {
                invite: "fed11invite".into(),
                gateway: "03NEWGATEWAY".into()
            })
        );
        let stored = read_fedimint_config(&dir).expect("durable config");
        assert_eq!(stored.invite, "fed11invite");
        assert_eq!(stored.gateway, "03NEWGATEWAY", "fungible gateway updated");
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P3: `create_dir_all` may create intermediate path components; bootstrap hardens EVERY
    // directory it creates to 0700 (not just the leaf), so the path to the 0600 secrets is not
    // group/world-traversable. Pre-existing ancestors are left untouched.
    #[tokio::test]
    async fn created_intermediate_data_dirs_are_hardened_to_0700() {
        let base = temp_data_dir(); // unique path that does not exist yet
        let nested = base.join("inner").join("leaf");
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(nested.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw, &store)
            .await
            .expect("bootstrap a nested data dir");

        assert_eq!(file_mode(&base), 0o700, "created base component hardened");
        assert_eq!(
            file_mode(&base.join("inner")),
            0o700,
            "created intermediate component hardened"
        );
        assert_eq!(file_mode(&nested), 0o700, "leaf data dir hardened");
        // The 0600 seed now sits under a fully owner-only (0700) path.
        assert_eq!(file_mode(&nested.join(SEED_FILE)), 0o600);
        let _ = fs::remove_dir_all(&base);
    }

    // Review P2: raw `..` components must be normalized before we decide which directories this
    // bootstrap owns. Otherwise `base/new/..` can be recorded while missing, then chmod `base`
    // after `create_dir_all` has created `new`.
    #[tokio::test]
    async fn parent_dir_components_do_not_harden_existing_parent() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();

        let raw_data_dir = base.join("new").join("..").join("lnrent");
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(raw_data_dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let op = bootstrap(raw, &store)
            .await
            .expect("bootstrap with parent-dir components");

        let normalized = base.join("lnrent");
        assert_eq!(
            op.config.data_dir, normalized,
            "runtime config uses the normalized data dir path"
        );
        assert_eq!(
            file_mode(&base),
            0o755,
            "pre-existing shared parent must keep its original mode"
        );
        assert!(
            !base.join("new").exists(),
            "normalization must not create a throwaway path component"
        );
        assert_eq!(file_mode(&normalized), 0o700, "created leaf hardened");
        assert_eq!(file_mode(&normalized.join(SEED_FILE)), 0o600);
        let _ = fs::remove_dir_all(&base);
    }

    // Review P2 (R1): when the leaf data dir ALREADY exists with loose perms (an installer or an
    // earlier daemon run under a default umask), bootstrap still tightens it to 0700 — `DirBuilder`
    // only modes components it creates, so the leaf is hardened explicitly. A pre-existing PARENT
    // keeps its mode (it may be shared), so only the secrets dir itself gets locked down.
    #[tokio::test]
    async fn preexisting_leaf_data_dir_is_hardened_to_0700() {
        let base = temp_data_dir();
        let dir = base.join("leaf");
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw, &store)
            .await
            .expect("bootstrap into a pre-existing leaf data dir");

        assert_eq!(
            file_mode(&dir),
            0o700,
            "a pre-existing leaf data dir must be tightened to 0700"
        );
        assert_eq!(
            file_mode(&base),
            0o755,
            "a pre-existing shared parent must keep its original mode"
        );
        assert_eq!(file_mode(&dir.join(SEED_FILE)), 0o600);
        let _ = fs::remove_dir_all(&base);
    }

    // Review P2 (R1): `exists()`/`create_dir_all` FOLLOW symlinks, so a data dir reached through a
    // symlinked component would redirect the 0600 seed / fedimint.json writes outside the private
    // boundary (`O_NOFOLLOW` only guards the leaf file open). lstat-reject the symlinked component
    // before creating or writing anything — nothing lands in the symlink target and no row commits.
    #[tokio::test]
    async fn symlinked_data_dir_component_is_rejected() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-datadir-outside-{}", std::process::id()));
        let _ = fs::remove_dir_all(&outside);
        fs::create_dir_all(&outside).unwrap();
        // A symlinked intermediate component on the path to the data dir.
        let link = base.join("link");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let data_dir = link.join("inner");

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(data_dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);

        // Nothing was created or written through the symlink into the outside target.
        assert!(
            !outside.join("inner").exists(),
            "bootstrap must not create or write through a symlinked data-dir component"
        );
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected before persisting any operator row");
        let _ = fs::remove_dir_all(&outside);
        let _ = fs::remove_dir_all(&base);
    }

    // Review P3 (R2): blank `data_dir` / `compute_backend` / `payment_backend` are treated as unset
    // (fall back to defaults) rather than stored verbatim — a blank data dir must not reach
    // create_dir_all, and a blank payment backend must not become an "unknown payment_backend"
    // error. `resolve_config` is the seam where these normalize.
    #[test]
    fn blank_fields_fall_back_to_defaults() {
        let raw = RawConfig {
            data_dir: Some("  ".into()),
            compute_backend: Some("".into()),
            payment_backend: Some("".into()),
            ..Default::default()
        };
        let cfg = resolve_config(&raw).expect("blank fields resolve to defaults");
        assert_eq!(cfg.data_dir, PathBuf::from(DEFAULT_DATA_DIR));
        assert_eq!(cfg.compute_backend, DEFAULT_COMPUTE_BACKEND);
        assert_eq!(cfg.payment_backend, PaymentMode::Mock);
    }

    // Review P2 (R2): an explicitly supplied compute_backend is validated against the canonical
    // recipe allowlist — a typo fails with a structured `config_invalid` (never stored), while the
    // known fixed backends and any `cloud-*` provider resolve through. Mirrors `payment_backend`.
    #[test]
    fn unknown_compute_backend_is_rejected_and_known_ones_resolve() {
        let raw = RawConfig {
            compute_backend: Some("hsot".into()), // typo for "host"
            ..Default::default()
        };
        let err = match resolve_config(&raw) {
            Ok(_) => panic!("expected an unknown compute_backend to be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);

        for ok in ["host", "incus", "libvirt", "proxmox", "cloud-aws"] {
            let raw = RawConfig {
                compute_backend: Some(ok.into()),
                ..Default::default()
            };
            assert_eq!(
                resolve_config(&raw)
                    .expect("known compute backend resolves")
                    .compute_backend,
                ok
            );
        }
    }

    // Review P3 (R2): relay elements are normalized like the other string fields. Blank entries are
    // dropped, values are trimmed, and a blank-only list falls back to the sane defaults.
    #[test]
    fn relay_entries_are_trimmed_and_blank_entries_are_ignored() {
        let raw = RawConfig {
            relays: Some(vec![
                " wss://a.example ".into(),
                "".into(),
                " \t ".into(),
                "wss://b.example".into(),
            ]),
            ..Default::default()
        };
        let cfg = resolve_config(&raw).unwrap();
        assert_eq!(cfg.relays, vec!["wss://a.example", "wss://b.example"]);

        let blank_only = RawConfig {
            relays: Some(vec!["".into(), " \t ".into()]),
            ..Default::default()
        };
        let cfg = resolve_config(&blank_only).unwrap();
        assert_eq!(cfg.relays, DEFAULT_RELAYS);
    }

    // Review P2: a re-bootstrap that OMITS relays inherits the stored relays rather than silently
    // resetting them to the default set.
    #[tokio::test]
    async fn rebootstrap_omitting_relays_inherits_stored_relays() {
        let dir = temp_data_dir();
        let store = mem_store();
        let custom = vec![
            "wss://custom-a.example".to_string(),
            "wss://custom-b.example".to_string(),
        ];
        let raw1 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            relays: Some(custom.clone()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw1, &store).await.expect("first");

        // Re-bootstrap with the seed read back and NO relays supplied.
        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store).await.expect("second");
        assert_eq!(
            op2.config.relays, custom,
            "omitted relays must inherit the stored set, not reset to default"
        );

        let (_c, _m, _o, _b, _p, relays) = read_operator_row(&store).await;
        let relays: Vec<String> = serde_json::from_str(&relays).unwrap();
        assert_eq!(relays, custom, "stored relays must be untouched");
        let _ = fs::remove_dir_all(&dir);
    }

    // The raw config deserializes from TOML (a config-file source, ADR-0014).
    #[test]
    fn raw_config_parses_from_toml() {
        let toml = r#"
            payment_backend = "fedimint"
            relays = ["wss://a.example", "wss://b.example"]
            fedimint_invite = "fed11x"
            fedimint_gateway = "03y"
        "#;
        let raw: RawConfig = toml::from_str(toml).unwrap();
        let cfg = resolve_config(&raw).unwrap();
        assert_eq!(cfg.payment_backend, PaymentMode::Fedimint);
        assert_eq!(cfg.relays.len(), 2);
        assert_eq!(
            cfg.fedimint,
            Some(FedimintConfig {
                invite: "fed11x".into(),
                gateway: "03y".into()
            })
        );
    }

    // Review P2 (R1): config-file typos must fail deterministically instead of silently applying
    // defaults (e.g. `payment-backend` must not fall back to `payment_backend=mock`).
    #[test]
    fn raw_config_rejects_unknown_fields() {
        let toml = r#"
            payment-backend = "fedimint"
            mnemonic = "not used here"
        "#;
        let err = match toml::from_str::<RawConfig>(toml) {
            Ok(_) => panic!("expected unknown field to be rejected"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("unknown field"),
            "unexpected error: {err}"
        );
    }

    // Review P3 (R2): Debug output must not leak Fedimint invite/gateway values.
    #[test]
    fn config_debug_redacts_fedimint_secrets() {
        let cfg = OperatorConfig {
            data_dir: PathBuf::from("/tmp/lnrent"),
            relays: vec!["wss://relay.example".into()],
            payment_backend: PaymentMode::Fedimint,
            compute_backend: DEFAULT_COMPUTE_BACKEND.into(),
            fedimint: Some(FedimintConfig {
                invite: "fed11SECRET".into(),
                gateway: "03SECRETGATEWAY".into(),
            }),
        };
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("fed11SECRET"));
        assert!(!debug.contains("03SECRETGATEWAY"));
        assert!(debug.contains("<redacted>"));
    }
}
