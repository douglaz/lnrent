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
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::json;
use zeroize::{Zeroize, Zeroizing};

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

impl Drop for FedimintConfig {
    fn drop(&mut self) {
        self.invite.zeroize();
        self.gateway.zeroize();
    }
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

impl Zeroize for RawConfig {
    fn zeroize(&mut self) {
        // The mnemonic is the seed; the Fedimint invite/gateway are also treated as sensitive
        // (§13: redacted in `FedimintConfig`'s `Debug`, persisted 0600). When `RawConfig` is held in
        // a `Zeroizing` guard, wipe those source buffers on every return path. This is best-effort:
        // the ready `OperatorConfig` intentionally retains Fedimint config for runtime use.
        if let Some(mnemonic) = self.mnemonic.as_mut() {
            mnemonic.zeroize();
        }
        if let Some(invite) = self.fedimint_invite.as_mut() {
            invite.zeroize();
        }
        if let Some(gateway) = self.fedimint_gateway.as_mut() {
            gateway.zeroize();
        }
    }
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
pub async fn bootstrap(raw: RawConfig, store: &Store) -> Result<Operator, IpcError> {
    // Keep the whole raw source config under a zeroizing guard for the full bootstrap. The mnemonic
    // is moved into `SuppliedMnemonic` below, but the Fedimint invite/gateway source fields remain in
    // `raw`; the guard wipes those transient source copies on every return path (review R2 P3).
    let mut raw = Zeroizing::new(raw);
    // The supplied mnemonic is secret (§13). Move it out of `raw` into a guard that zeroizes the
    // plaintext on EVERY return path — including the early `?` errors below (a missing Fedimint
    // config, an invalid mnemonic, an I/O failure), which would otherwise drop the original `String`
    // unwiped without a guard (review P3/R1). Taking it out also keeps it out of the `&raw` reads
    // the rest of bootstrap makes — only `resolve_seed` needs it.
    let supplied_mnemonic = SuppliedMnemonic(raw.mnemonic.take().map(Zeroizing::new));

    let mut config = resolve_config(&raw)?;
    // The data dir holds the operator seed (0600) and the state DB; make every directory WE create
    // owner-only (0700) so a co-tenant local user can't traverse it or read future artifacts in it
    // (defense in depth beyond the per-file perms). A PRE-EXISTING directory is never chmod'ed (it
    // may be a shared/system dir like `/tmp` or a home dir, and chmod'ing it when run privileged
    // could damage host perms) — an unsafe pre-existing target is rejected instead (review:
    // dir-perms footgun). Normalize first so a raw `new/..` segment can't make us create or chmod a
    // pre-existing parent.
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
    let seed = resolve_seed(&config.data_dir, supplied_mnemonic.as_deref())?;
    let identity = OperatorIdentity::from_mnemonic(seed.mnemonic.as_str(), None)?;
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
            seed.mnemonic.as_str(),
        )?;
    }
    Ok(Operator { identity, config })
}

// ===== Headless multi-source bootstrap surface (ADR-0014, §4.7) ===================================
//
// `bootstrap` above is the RESOLUTION half (a parsed `RawConfig` -> derive + persist). The functions
// below are the SOURCE half the §4.7 non-interactive contract requires: assemble a `RawConfig` by
// merging the four headless sources — flags, environment, a config file, and stdin — with an
// EXPLICIT precedence, then run a fully non-interactive bootstrap that NEVER prompts. The daemon-
// startup wiring that also calls this is bead .21.

/// The data-dir-relative sqlite state file (matches `lnrentd`'s `{data_dir}/lnrent.sqlite`).
const STATE_DB_FILE: &str = "lnrent.sqlite";

/// The environment variables the `env` source layer reads. `LNRENT_DATA_DIR` matches the
/// daemon/CLI default; `LNRENT_RELAYS` is a comma-separated list. Documented so an operator agent
/// can bootstrap from env alone.
pub const ENV_DATA_DIR: &str = "LNRENT_DATA_DIR";
const ENV_RELAYS: &str = "LNRENT_RELAYS";
const ENV_PAYMENT_BACKEND: &str = "LNRENT_PAYMENT_BACKEND";
const ENV_COMPUTE_BACKEND: &str = "LNRENT_COMPUTE_BACKEND";
const ENV_FEDIMINT_INVITE: &str = "LNRENT_FEDIMINT_INVITE";
const ENV_FEDIMINT_GATEWAY: &str = "LNRENT_FEDIMINT_GATEWAY";
const ENV_MNEMONIC: &str = "LNRENT_MNEMONIC";
/// Optional path to a config file, an alternative to the `--config` flag.
const ENV_CONFIG: &str = "LNRENT_CONFIG";

impl RawConfig {
    /// Merge the four bootstrap sources into one [`RawConfig`] with the documented precedence
    /// **flags > env > file > stdin** (ADR-0014 §4.7 lists flags/env/file/stdin as the headless
    /// inputs; this fixes their order — the most explicit, operator-typed source wins, falling back
    /// to progressively more ambient ones). Each layer is sanitized first (blank strings / empty
    /// relay lists count as UNSET), so a blank value in a higher-precedence source never shadows a
    /// real value in a lower one. Pure and testable.
    pub fn from_sources(
        flags: RawConfig,
        env: RawConfig,
        file: RawConfig,
        stdin: RawConfig,
    ) -> RawConfig {
        flags
            .sanitized()
            .overlay(env.sanitized())
            .overlay(file.sanitized())
            .overlay(stdin.sanitized())
    }

    /// Map blank string fields and empty relay lists to `None` so they don't shadow a
    /// lower-precedence source in [`from_sources`]. The mnemonic is filtered IN PLACE (no trimmed
    /// copy is made — `resolve_seed` trims it later) to avoid spreading the secret across buffers.
    fn sanitized(mut self) -> RawConfig {
        let mnemonic = match self.mnemonic.take() {
            Some(mut mnemonic) if mnemonic.trim().is_empty() => {
                mnemonic.zeroize();
                None
            }
            mnemonic => mnemonic,
        };
        RawConfig {
            data_dir: non_empty(self.data_dir.as_deref()),
            relays: self.relays.and_then(|r| {
                let cleaned: Vec<String> = r.iter().filter_map(|s| non_empty(Some(s))).collect();
                (!cleaned.is_empty()).then_some(cleaned)
            }),
            payment_backend: non_empty(self.payment_backend.as_deref()),
            compute_backend: non_empty(self.compute_backend.as_deref()),
            fedimint_invite: sanitize_secret_string(self.fedimint_invite.take()),
            fedimint_gateway: sanitize_secret_string(self.fedimint_gateway.take()),
            mnemonic,
        }
    }

    /// Fields set in `self` win; `lower` fills only the fields `self` left `None` — so `self` is the
    /// higher-precedence layer. Assumes both layers were already [`sanitized`](Self::sanitized).
    fn overlay(mut self, mut lower: RawConfig) -> RawConfig {
        let mnemonic = overlay_secret_string(self.mnemonic.take(), lower.mnemonic.take());
        let fedimint_invite =
            overlay_secret_string(self.fedimint_invite.take(), lower.fedimint_invite.take());
        let fedimint_gateway =
            overlay_secret_string(self.fedimint_gateway.take(), lower.fedimint_gateway.take());
        RawConfig {
            data_dir: self.data_dir.or(lower.data_dir),
            relays: self.relays.or(lower.relays),
            payment_backend: self.payment_backend.or(lower.payment_backend),
            compute_backend: self.compute_backend.or(lower.compute_backend),
            fedimint_invite,
            fedimint_gateway,
            mnemonic,
        }
    }
}

fn sanitize_secret_string(value: Option<String>) -> Option<String> {
    value.and_then(|mut s| {
        let cleaned = non_empty(Some(&s));
        s.zeroize();
        cleaned
    })
}

fn overlay_secret_string(high: Option<String>, low: Option<String>) -> Option<String> {
    match high {
        Some(high) => {
            if let Some(mut shadowed) = low {
                shadowed.zeroize();
            }
            Some(high)
        }
        None => low,
    }
}

/// Build the `env` source layer from an environment lookup (`get` returns a var's value, or `None`
/// when unset). Pure given `get`, so tests can inject a fake environment. Blank values are
/// sanitized away by [`RawConfig::from_sources`].
fn raw_config_from_env(get: impl Fn(&str) -> Option<String>) -> RawConfig {
    let relays = get(ENV_RELAYS).map(|s| {
        s.split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
    });
    RawConfig {
        data_dir: get(ENV_DATA_DIR),
        relays,
        payment_backend: get(ENV_PAYMENT_BACKEND),
        compute_backend: get(ENV_COMPUTE_BACKEND),
        fedimint_invite: get(ENV_FEDIMINT_INVITE),
        fedimint_gateway: get(ENV_FEDIMINT_GATEWAY),
        mnemonic: get(ENV_MNEMONIC),
    }
}

/// The 1-based line number at `byte_offset` within `text` — for SAFE parse-error locations that
/// report WHERE a document is malformed without ever echoing the (possibly secret-bearing) source
/// content (review R1 P1). Counts on bytes so a non-char-boundary offset can't panic.
fn line_number(text: &str, byte_offset: usize) -> usize {
    let n = byte_offset.min(text.len());
    text.as_bytes()[..n].iter().filter(|&&b| b == b'\n').count() + 1
}

/// Parse a config document (a `file` or `stdin` source) that may be TOML or JSON into a
/// [`RawConfig`]. An empty/whitespace-only document is an empty layer (all `None`).
/// `deny_unknown_fields` means a typo'd key fails deterministically rather than silently defaulting
/// (§4.7). The caller should zeroize the source text afterward — it may carry the mnemonic.
fn parse_raw_config_doc(text: &str, what: &str) -> Result<RawConfig, IpcError> {
    if text.trim().is_empty() {
        return Ok(RawConfig::default());
    }
    // Try TOML (the documented config-file format) first, then JSON. A valid JSON object is not
    // valid TOML and vice versa, so the fallback is unambiguous; only a doc that is neither errors.
    match toml::from_str::<RawConfig>(text) {
        Ok(cfg) => Ok(cfg),
        Err(toml_err) => serde_json::from_str::<RawConfig>(text).map_err(|json_err| {
            // SECURITY (review R1 P1): never embed the raw parser errors in the message. A TOML
            // syntax error on the line carrying `mnemonic = "..."` makes toml's `Display` echo that
            // source line — which would leak the operator seed onto stderr / into logs (the headless
            // entrypoint prints this message to stderr on failure), breaking the §13 "never logged"
            // contract. Report only the non-sensitive LOCATION (line/column) from the parser spans;
            // never the content itself.
            let toml_loc = toml_err
                .span()
                .map(|span| format!("line {}", line_number(text, span.start)))
                .unwrap_or_else(|| "an unknown location".to_string());
            config_err(format!(
                "{what} is not a valid config document: it parses as neither TOML (syntax error near \
                 {toml_loc}, or contains an unknown field) nor JSON (syntax error at line {} column {})",
                json_err.line(),
                json_err.column()
            ))
        }),
    }
}

/// The raw inputs the headless bootstrap entrypoint collects from the process: the parsed `flags`
/// layer, an optional config-file path, and whether to read a config document from stdin.
/// [`load_raw_config`] turns these — plus the process environment — into the merged [`RawConfig`].
pub struct BootstrapInput {
    /// The flag-sourced layer (highest precedence). Built by the CLI from parsed arguments.
    pub flags: RawConfig,
    /// An explicit config-file path (`--config`). Falls back to `LNRENT_CONFIG` when `None`.
    pub config_path: Option<PathBuf>,
    /// Read a TOML/JSON config document from stdin (lowest precedence). The CALLER decides this;
    /// the CLI only sets it for an explicit `--stdin`, so inherited open pipes cannot block.
    pub read_stdin: bool,
}

/// Resolve the config-file path from the explicit `--config` flag (highest) falling back to the
/// `LNRENT_CONFIG` env var. A BLANK value in EITHER source is treated as UNSET, not as a path to an
/// empty filename: templated environments routinely export optional vars as the empty string, and a
/// blank `LNRENT_CONFIG` must not make bootstrap fail trying to read `""` when flags/env already
/// supply a complete config (review R1 P2). Pure given its inputs, so it is unit-testable without
/// touching the process environment.
fn resolve_config_path(flag: Option<PathBuf>, env_config: Option<String>) -> Option<PathBuf> {
    flag.filter(|p| !p.as_os_str().is_empty())
        .or_else(|| non_empty(env_config.as_deref()).map(PathBuf::from))
}

/// Assemble the merged [`RawConfig`] from all four headless sources — flags, environment, config
/// file, stdin — with precedence **flags > env > file > stdin** (ADR-0014 §4.7). Does the file/stdin
/// I/O, then calls the pure [`RawConfig::from_sources`]. Secret-bearing source text (file/stdin,
/// which may carry the mnemonic) is zeroized after parsing, and the merged result is returned in a
/// `Zeroizing` guard so its plaintext mnemonic is wiped on drop even if the caller errors out before
/// handing it to [`bootstrap`] (review R2 P3). Never prompts: stdin is read only when
/// `input.read_stdin` is set by the caller.
pub fn load_raw_config(input: BootstrapInput) -> Result<Zeroizing<RawConfig>, IpcError> {
    let BootstrapInput {
        flags,
        config_path,
        read_stdin,
    } = input;
    let mut flags = Zeroizing::new(flags);
    let mut env = Zeroizing::new(raw_config_from_env(|k| std::env::var(k).ok()));

    let config_path = resolve_config_path(config_path, std::env::var(ENV_CONFIG).ok());
    let mut file = Zeroizing::new(match config_path {
        Some(path) => {
            let mut text = fs::read_to_string(&path)
                .map_err(|e| config_err(format!("reading config file {}: {e}", path.display())))?;
            let parsed = parse_raw_config_doc(&text, &format!("config file {}", path.display()));
            text.zeroize();
            parsed?
        }
        None => RawConfig::default(),
    });

    let mut stdin = Zeroizing::new(if read_stdin {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| internal_err(format!("reading config from stdin: {e}")))?;
        let parsed = parse_raw_config_doc(&text, "config from stdin");
        text.zeroize();
        parsed?
    } else {
        RawConfig::default()
    });

    Ok(Zeroizing::new(RawConfig::from_sources(
        std::mem::take(&mut *flags),
        std::mem::take(&mut *env),
        std::mem::take(&mut *file),
        std::mem::take(&mut *stdin),
    )))
}

/// The resolved, lexically-normalized data dir for a [`RawConfig`] — the SAME path [`bootstrap`]
/// derives (blank/unset -> the `./data` default, then normalized). Used by [`bootstrap_headless`]
/// to open the state DB under the data dir before handing the config to [`bootstrap`].
fn resolve_data_dir(raw: &RawConfig) -> PathBuf {
    let dir = PathBuf::from(
        non_empty(raw.data_dir.as_deref()).unwrap_or_else(|| DEFAULT_DATA_DIR.to_string()),
    );
    normalize_path_lexically(&dir)
}

/// Run a fully non-interactive bootstrap (ADR-0014 §4.7): create the private data dir, open the
/// state DB + sole-writer store actor (ADR-0001) under it, and bootstrap the operator from `raw`.
/// This is the executable entrypoint behind `lnrentd bootstrap`; the daemon-startup path (.21) can
/// reuse it. Structured errors only — never a prompt; the caller maps `err.code` to an exit via
/// [`exit_code`].
pub async fn bootstrap_headless(raw: RawConfig) -> Result<Operator, IpcError> {
    // Hold the merged config in a `Zeroizing` guard so its plaintext mnemonic is wiped on EVERY
    // return path here — including the early `create_private_dir_all` / `Store::open_spawn` errors,
    // which return BEFORE `bootstrap` moves the mnemonic into its own zeroizing guard. `RawConfig`
    // is wrapped in `bootstrap`. `mem::take` hands the real config to `bootstrap`, leaving an empty
    // default to drop harmlessly.
    let mut raw = Zeroizing::new(raw);
    let data_dir = resolve_data_dir(&raw);
    // Create + harden the data dir before opening sqlite under it (sqlite won't create the parent),
    // using the SAME private-dir logic bootstrap applies (rejects symlinked / unsafe targets). The
    // subsequent `bootstrap` call repeats this idempotently on the now-0700 dir.
    create_private_dir_all(&data_dir)?;
    let db_path = data_dir.join(STATE_DB_FILE);
    // SQLite follows symlinks when opening the main DB and WAL sidecars. Vet the paths BEFORE
    // `Store::open_spawn` so a pre-existing symlink cannot create/modify a target outside the data
    // dir and only then get rejected by the post-open hardening pass (review round 9 R1 P2).
    preflight_state_db_paths(&db_path)?;
    let store = Store::open_spawn(&db_path)
        .map_err(|e| internal_err(format!("opening state DB {}: {e}", db_path.display())))?;
    // `open` just created `lnrent.sqlite` (+ its WAL sidecars) at the process umask. A data dir WE
    // create is 0700, but a pre-existing traversable one is deliberately left at its own mode, so the
    // DB's own perms must be tightened to owner-only or its later credential-bearing rows would be
    // group/world-readable — same 0600 contract as the seed/fedimint files (review R1 P2).
    harden_state_db_perms(&db_path)?;
    bootstrap(std::mem::take(&mut *raw), &store).await
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

fn relays_explicitly_supplied(raw: &RawConfig) -> bool {
    raw.relays
        .as_ref()
        .is_some_and(|relays| relays.iter().any(|relay| !relay.trim().is_empty()))
}

struct ResolvedSeed {
    mnemonic: Zeroizing<String>,
    persist_after_success: bool,
}

/// Owns the operator-supplied mnemonic for the lifetime of [`bootstrap`] and zeroizes the plaintext
/// on drop — so it is wiped on EVERY return path, not only after a successful derivation. The
/// supplied `RawConfig` does not zeroize on drop, so an early-error path (missing Fedimint config,
/// invalid mnemonic, I/O failure) would otherwise leave the original `String` lingering in freed
/// memory (§13, never logged) (review P3/R1).
struct SuppliedMnemonic(Option<Zeroizing<String>>);

impl SuppliedMnemonic {
    fn as_deref(&self) -> Option<&str> {
        self.0.as_ref().map(|mnemonic| mnemonic.as_str())
    }
}

/// Canonicalize a BIP39 `mnemonic` to its normalized single-space form, or `None` if it is not a
/// valid BIP39 phrase. Uses the SAME `bip39` (via `nostr`) the derivation uses, so the canonical
/// form a re-bootstrap compares/persists matches what `identity.rs` derives from. The decoded
/// `Mnemonic` zeroizes its entropy on drop (bip39's `zeroize` feature) and the canonical string is
/// wrapped so it is wiped too (§13) (review R1 P2).
fn canonical_mnemonic(mnemonic: &str) -> Option<Zeroizing<String>> {
    nostr::bip39::Mnemonic::parse_normalized(mnemonic.trim())
        .ok()
        .map(|m| Zeroizing::new(m.to_string()))
}

/// True iff `a` and `b` are the SAME operator seed. BIP39 words are whitespace-insensitive, so two
/// equivalent phrases that differ only in spacing MUST compare equal — otherwise an idempotent
/// re-bootstrap of the same seed false-trips `seed_conflict`. When BOTH parse as valid BIP39 we
/// compare canonical forms; otherwise (an invalid stored/supplied string) we fall back to an exact
/// trimmed compare, so a genuinely different seed is still a conflict (review R1 P2).
fn same_mnemonic(a: &str, b: &str) -> bool {
    match (canonical_mnemonic(a), canonical_mnemonic(b)) {
        (Some(ca), Some(cb)) => ca.as_str() == cb.as_str(),
        _ => a.trim() == b.trim(),
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
                mnemonic: Zeroizing::new(file_buf.trim().to_string()),
                persist_after_success: false,
            };
            // Wipe the raw file buffer too; only the guarded copy should remain (§13) (review P3).
            file_buf.zeroize();
            if let Some(sup) = supplied {
                // Compare CANONICAL BIP39 forms so a re-supply that differs only in whitespace from
                // the persisted phrase is recognized as the SAME seed (idempotent), not a false
                // `seed_conflict` (review R1 P2).
                if !same_mnemonic(sup, resolved.mnemonic.as_str()) {
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
            // Persist the CANONICAL single-space BIP39 form when the supplied phrase is valid, so an
            // equivalent re-supply (e.g. different whitespace) re-bootstraps idempotently instead of
            // tripping `seed_conflict` against a raw-spaced stored copy. An invalid phrase is kept
            // verbatim — identity derivation rejects it next, before anything is persisted. The
            // canonical form changes NO derived key: derivation already normalizes whitespace, so the
            // pinned NIP-06 / HKDF vectors are unaffected (review R1 P2).
            Ok(ResolvedSeed {
                mnemonic: canonical_mnemonic(mnemonic)
                    .unwrap_or_else(|| Zeroizing::new(mnemonic.to_string())),
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
        // SECURITY (§13, mirrors `parse_raw_config_doc`'s R1 P1 hardening): never interpolate the
        // raw serde error. Its `Display` echoes the offending scalar — e.g. a top-level type
        // mismatch on a corrupt file reads `invalid type: string "fed11…", expected struct
        // FedimintConfig`, quoting the sensitive invite/gateway — and this message is printed to
        // stderr on bootstrap failure, which would break the "never logged" contract. Report only
        // the non-sensitive parse LOCATION (line/column), never the content.
        config_err(format!(
            "invalid durable Fedimint config {} (parse error at line {} column {})",
            path.display(),
            e.line(),
            e.column()
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
    let result = write_secret_file_atomic(data_dir, &path, &bytes, "Fedimint config");
    // The staged buffer held the plaintext invite + gateway; wipe it before returning, mirroring
    // `write_seed`'s treatment of its serialized secret buffer (§13, best-effort).
    bytes.zeroize();
    result
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

/// Like [`harden_perms`] but a NO-OP when `path` is absent — for sqlite's optional `-wal`/`-shm`
/// WAL sidecars, which may or may not exist depending on journal state.
fn harden_perms_if_present(path: &Path) -> Result<(), IpcError> {
    match fs::symlink_metadata(path) {
        Ok(_) => harden_perms(path),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(internal_err(format!(
            "stat state DB sidecar {}: {e}",
            path.display()
        ))),
    }
}

/// The sqlite sidecar path for `db_path` with `suffix` (`-wal` / `-shm`) appended to the FULL file
/// name (sqlite's own naming), e.g. `…/lnrent.sqlite` -> `…/lnrent.sqlite-wal`.
fn db_sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn preflight_state_db_path(path: &Path, what: &str) -> Result<(), IpcError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
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
    Ok(())
}

/// Refuse pre-existing unsafe sqlite paths BEFORE opening SQLite. SQLite's normal open path follows
/// symlinks; a post-open hardening check is too late because the target may already have been
/// created or modified outside the data dir.
fn preflight_state_db_paths(db_path: &Path) -> Result<(), IpcError> {
    preflight_state_db_path(db_path, "state DB")?;
    preflight_state_db_path(&db_sidecar_path(db_path, "-wal"), "state DB WAL sidecar")?;
    preflight_state_db_path(&db_sidecar_path(db_path, "-shm"), "state DB SHM sidecar")?;
    Ok(())
}

/// Tighten the sqlite state DB (and its `-wal`/`-shm` WAL sidecars) to owner-only `0600`.
///
/// `Store::open` creates `lnrent.sqlite` — and, once WAL mode is enabled, the `-wal`/`-shm`
/// sidecars — with the process UMASK, commonly `0644`: readable by anyone who can traverse the data
/// dir. A data dir WE create is `0700`, so traversal is already blocked there; but a PRE-EXISTING
/// traversable data dir (e.g. a systemd-provisioned `/var/lib/lnrent` at `0750`/`0755`) is
/// deliberately left at its own mode by the dir-perms fix, so without this the DB — which later
/// holds credential-bearing rows like outbox `provision.ready` payloads and native session
/// tickets — would be group/world-readable. Mirror the `0600` seed/`fedimint.json` files and tighten
/// the DB too; the WAL sidecar can hold the same (uncheckpointed) row data, so it gets the same
/// perms (review R1 P2). Idempotent, so a re-bootstrap re-tightens a DB created under a loose umask
/// by an older build.
fn harden_state_db_perms(db_path: &Path) -> Result<(), IpcError> {
    harden_perms(db_path)?;
    harden_perms_if_present(&db_sidecar_path(db_path, "-wal"))?;
    harden_perms_if_present(&db_sidecar_path(db_path, "-shm"))?;
    Ok(())
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

fn absolute_data_dir_for_vetting(dir: &Path) -> Result<PathBuf, IpcError> {
    let cwd = if dir.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().map_err(|e| {
            internal_err(format!(
                "reading current directory for data-dir vetting: {e}"
            ))
        })?
    };
    Ok(absolute_data_dir_for_vetting_from_cwd(dir, &cwd))
}

fn absolute_data_dir_for_vetting_from_cwd(dir: &Path, cwd: &Path) -> PathBuf {
    let full = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        cwd.join(dir)
    };
    normalize_path_lexically(&full)
}

fn vetted_data_dir_components(dir: &Path) -> Result<PathBuf, IpcError> {
    let vet_dir = absolute_data_dir_for_vetting(dir)?;
    vet_data_dir_components_at(&vet_dir)
}

#[cfg(test)]
fn vet_data_dir_components_from_cwd(dir: &Path, cwd: &Path) -> Result<PathBuf, IpcError> {
    let vet_dir = absolute_data_dir_for_vetting_from_cwd(dir, cwd);
    vet_data_dir_components_at(&vet_dir)
}

fn vet_data_dir_components_at(vet_dir: &Path) -> Result<PathBuf, IpcError> {
    // `exists()` and `create_dir_all` both FOLLOW symlinks, so if the data dir, its implicit CWD
    // parent for a relative path, or any already existing component on the path to it is a symlink
    // to a directory, the later 0600 `operator.seed` / `fedimint.json` writes land in the symlink
    // target, OUTSIDE the private data-dir boundary. `O_NOFOLLOW` only guards the final secret file
    // open, not the directory walk, so lstat every existing component and refuse any symlink, unsafe
    // mode, or untrusted owner before creating or writing anything (review P2/R1, R2 P2).
    reject_unsafe_components(vet_dir)?;

    // Never treat the filesystem root `/` as the data dir: it has no parent and holds the entire
    // host, so chmod'ing it would be catastrophic. `vet_dir` is absolute and lexically normalized,
    // so the root is exactly the no-parent case (review: dir-perms footgun on shared/system dirs).
    if vet_dir.parent().is_none() {
        return Err(config_err(
            "refusing to use the filesystem root `/` as the data dir".to_string(),
        ));
    }

    Ok(vet_dir.to_path_buf())
}

fn create_private_dir_all(dir: &Path) -> Result<(), IpcError> {
    let vet_dir = vetted_data_dir_components(dir)?;

    // Whether the LEAF data dir already existed before this bootstrap. A pre-existing dir may be a
    // shared/system dir (`/tmp`, a home dir, a sticky scratch dir); we must NOT chmod it — that
    // could damage host perms under privilege — so we only harden the components WE create, and we
    // reject an unsafe pre-existing leaf rather than trusting or chmod'ing it (review: dir-perms).
    let leaf_preexisted = vet_dir.exists();

    // The path components that DON'T exist yet — the ones this bootstrap is about to create.
    // Computed before the create below, while they're still missing.
    let created_dirs: Vec<PathBuf> = vet_dir
        .ancestors()
        .take_while(|p| !p.as_os_str().is_empty() && !p.exists())
        .map(Path::to_path_buf)
        .collect();

    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&vet_dir)
        .map_err(|e| internal_err(format!("creating data dir {}: {e}", vet_dir.display())))?;

    // Re-scan AFTER creating but BEFORE any chmod. Under a sticky shared ancestor (e.g. `/tmp`)
    // another user can race a not-yet-created component into a symlink between the pre-create scan
    // and `create_dir_all` above; if `create_dir_all` followed it, the component is now a symlink.
    // The chmod loop below must NOT run on a followed symlink — a path-based `set_permissions` would
    // chmod the symlink TARGET (outside the data dir) — so re-lstat every component and reject a
    // symlink FIRST, before touching any perms (review R1 P1). This also rejects it before any 0600
    // secret is written (those writes happen later, in `write_seed` / `write_fedimint_config`).
    reject_unsafe_components(&vet_dir)?;

    // Harden ONLY the directories THIS bootstrap created to owner-only (0700). `DirBuilder::mode`
    // already applied 0700, but a restrictive umask can strip owner bits, so re-tighten explicitly.
    // The chmod goes through a NO-FOLLOW directory handle (`harden_dir_perms`), so a component raced
    // into a symlink AFTER the re-scan above still cannot redirect the chmod onto its target — it is
    // refused instead. We harden shallow→leaf so each created dir is owner-only before we descend. A
    // PRE-EXISTING component (including the leaf) is deliberately never chmod'ed — chmod'ing a dir we
    // did not create is the footgun the dir-perms fix closes (review: dir-perms).
    for created in created_dirs.iter().rev() {
        harden_dir_perms(created)?;
    }

    // If the leaf pre-existed we never chmod it; instead REJECT it when it is unsafe to drop our
    // 0600 secrets into — a group/world-writable or sticky directory we did not create — with a
    // structured error rather than silently trusting or chmod'ing it (review: dir-perms).
    if leaf_preexisted {
        reject_unsafe_preexisting_dir(&vet_dir)?;
    }
    Ok(())
}

/// A PRE-EXISTING leaf data dir is never chmod'ed (it may be shared/system) — but it must still be
/// safe AND usable to drop 0600 secrets into. Reject:
/// - a group/world-writable or sticky-bit directory we did not create (e.g. pointing the data dir
///   straight at `/tmp`, mode 1777): another local user could plant files or race the secret writes
///   there;
/// - a directory we cannot actually WRITE (e.g. a root-owned `0755` `/var/lib/lnrent`, or an
///   operator-owned `0500` dir): the later 0600 seed temp-file / state-DB creation would otherwise
///   fail as a *retryable* `internal` error AFTER the store may have committed the operator row,
///   instead of this DETERMINISTIC `config_invalid` rejection before any state changes (review R1 P2).
///
/// A dir THIS bootstrap created is owner-only 0700 and never reaches this check (review: dir-perms
/// footgun on shared/system dirs).
fn reject_unsafe_preexisting_dir(dir: &Path) -> Result<(), IpcError> {
    let meta = fs::symlink_metadata(dir)
        .map_err(|e| internal_err(format!("stat data dir {}: {e}", dir.display())))?;
    // `reject_unsafe_components` should already have refused a symlinked or non-directory leaf; keep
    // this as a second guard in case the path changed between scans.
    if !meta.file_type().is_dir() {
        return Err(config_err(format!(
            "data dir {} exists but is not a directory",
            dir.display()
        )));
    }
    let mode = meta.permissions().mode();
    // 0o020 = group-writable; 0o002 = others-writable; 0o1000 = sticky bit. Any of these marks a
    // shared dir we must not adopt as a private data dir (directory write permission lets another
    // same-group/local user unlink or replace 0600 files inside it; chmod'ing `/tmp` would also
    // clear its sticky bit).
    if mode & 0o022 != 0 || mode & 0o1000 != 0 {
        return Err(config_err(format!(
            "refusing to use pre-existing group/world-writable or sticky directory {} (mode {:o}) \
             as the data dir; it looks like a shared/system dir — point the data dir at a private \
             location instead",
            dir.display(),
            mode & 0o7777
        )));
    }
    // The dir pre-existed and we will NOT chmod it, so it must already be writable+searchable by US:
    // we drop `operator.seed` (via an exclusive temp file), `fedimint.json`, and the state DB inside
    // it. Pre-flighting this with the EFFECTIVE creds turns an unwritable target into a deterministic
    // `config_invalid` BEFORE the store opens / any row is written, rather than a retryable `internal`
    // failure mid-bootstrap once state may already have changed (review R1 P2).
    if !dir_is_writable_by_us(dir)? {
        return Err(config_err(format!(
            "pre-existing data dir {} is not writable by the daemon (effective uid {}); the operator \
             seed and state DB cannot be created there — fix its ownership/permissions or point the \
             data dir at a writable private location",
            dir.display(),
            current_euid()
        )));
    }
    Ok(())
}

/// Whether the EFFECTIVE user can create and reach entries in `dir` (needs both write and search
/// permission on the directory). Uses `faccessat(AT_EACCESS)` so the check honors the effective
/// uid/gid the daemon actually runs as (a plain `access(2)` would consult the real ids). A clean
/// `EACCES`/`EROFS` means "not writable" (a deterministic config error); anything else is a genuine
/// I/O fault surfaced as retryable `internal`.
fn dir_is_writable_by_us(dir: &Path) -> Result<bool, IpcError> {
    let cpath = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(|_| {
        config_err(format!(
            "data dir path {} contains an interior NUL byte",
            dir.display()
        ))
    })?;
    // SAFETY: `cpath` is a valid NUL-terminated C string that outlives the call; `faccessat` only
    // reads it plus the current process credentials and returns a status code.
    let rc = unsafe {
        libc::faccessat(
            libc::AT_FDCWD,
            cpath.as_ptr(),
            libc::W_OK | libc::X_OK,
            libc::AT_EACCESS,
        )
    };
    if rc == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EACCES) | Some(libc::EROFS) => Ok(false),
        _ => Err(internal_err(format!(
            "checking write access to data dir {}: {err}",
            dir.display()
        ))),
    }
}

/// Refuse to bootstrap through an UNSAFE directory component. `dir` is already absolute and
/// lexically normalized (no `.`/`..` segments), so its ancestors are the literal path components.
/// For relative data dirs this includes the implicit current working directory parent (review R1
/// P2). lstat each EXISTING one (root → leaf, so the error names the shallowest offender) and
/// reject:
/// - any **symlink** — `exists()` / `create_dir_all` follow symlinks, so a symlinked component would
///   redirect the later 0600 seed / fedimint.json writes outside the private boundary (`O_NOFOLLOW`
///   only guards the final file open, not the directory walk) (review P2/R1);
/// - any pre-existing **group/world-writable, non-sticky INTERMEDIATE ancestor** — another user with
///   write access there could plant or swap a component we are about to create beneath it (e.g. race
///   a not-yet-created dir into a symlink between this scan and `create_dir_all`), again redirecting
///   the secrets. A *sticky* writable ancestor (the `/tmp` convention) is allowed only when it is
///   owned by root or the operator; we create our own 0700 dir beneath it. The LEAF data dir is
///   vetted more strictly (sticky included) by [`reject_unsafe_preexisting_dir`], so it is skipped
///   for the mode check here (review R2 P2);
/// - any pre-existing **owner-writable directory owned by neither root nor the operator** — that
///   foreign owner can rename/replace the next component (or secret files if this is the leaf),
///   even when group/world permissions are closed;
/// - any pre-existing **group/world-writable (shared) directory not owned by root or the operator** —
///   the directory's owner can still redirect entries in shared dirs, so a shared parent is trusted
///   only when root or we own it.
///
/// A foreign-owned component with no owner/group/world write bits is allowed: read-only ancestors
/// with foreign owners do not let that uid redirect entries through directory write permission. A
/// component owned by the unmappable **overflow uid** (the root-squash / user-namespace sentinel) is
/// allowed even when owner-writable — no process can act as that uid (see [`overflow_uid`]), so a
/// root-squashed `/` shown as `65534 0755` no longer wrongly blocks every data dir beneath it
/// (review R1 P2). See [`dir_component_hazard`].
///
/// Components that don't exist yet are ours to create as real 0700 dirs; nothing to reject.
fn reject_unsafe_components(dir: &Path) -> Result<(), IpcError> {
    let mut components: Vec<&Path> = dir
        .ancestors()
        .filter(|p| !p.as_os_str().is_empty())
        .collect();
    components.reverse();
    for component in components {
        let meta = match fs::symlink_metadata(component) {
            Ok(meta) => meta,
            // Not created yet — we make it a real dir below; nothing to reject.
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(internal_err(format!(
                    "stat data dir component {}: {e}",
                    component.display()
                )))
            }
        };
        if meta.file_type().is_symlink() {
            return Err(config_err(format!(
                "data dir component {} is a symlink; refusing to bootstrap through it (it could \
                 redirect the operator seed / config outside the data dir)",
                component.display()
            )));
        }
        if !meta.file_type().is_dir() {
            return Err(config_err(format!(
                "data dir component {} exists but is not a directory",
                component.display()
            )));
        }
        let mode = meta.permissions().mode();
        match dir_component_hazard(meta.uid(), mode, current_euid(), component == dir) {
            Some(DirHazard::WritableNonStickyAncestor) => {
                return Err(config_err(format!(
                    "data dir ancestor {} is group/world-writable and not sticky (mode {:o}); refusing \
                     to create the data dir beneath it (another user could redirect the operator seed \
                     / config) — point the data dir under a private location instead",
                    component.display(),
                    mode & 0o7777
                )));
            }
            Some(DirHazard::UntrustedSharedOwner) => {
                return Err(config_err(format!(
                    "data dir component {} is a group/world-writable directory owned by uid {} \
                     instead of the operator uid {} or root; refusing to bootstrap through an \
                     untrusted shared directory (its owner could redirect the operator seed / config)",
                    component.display(),
                    meta.uid(),
                    current_euid()
                )));
            }
            Some(DirHazard::ForeignOwnerWritableComponent) => {
                return Err(config_err(format!(
                    "data dir component {} is owner-writable and owned by uid {} instead of \
                     the operator uid {} or root; refusing to bootstrap through a foreign-owned \
                     writable directory (its owner could redirect the operator seed / config)",
                    component.display(),
                    meta.uid(),
                    current_euid()
                )));
            }
            None => {}
        }
    }
    Ok(())
}

/// A hazard found on a PRE-EXISTING directory component on the path to the data dir.
#[derive(Debug, PartialEq, Eq)]
enum DirHazard {
    /// A group/world-writable, NON-sticky intermediate ancestor: another user can redirect the dirs
    /// we create beneath it.
    WritableNonStickyAncestor,
    /// A group/world-writable (shared) directory owned by neither root nor the operator: its owner
    /// can rename/replace entries even under the sticky bit.
    UntrustedSharedOwner,
    /// A directory owned by neither root nor the operator while its owner has write permission:
    /// that owner can rename/replace child entries even when group/world permissions are closed.
    ForeignOwnerWritableComponent,
}

/// Decide whether a PRE-EXISTING directory component (`owner_uid`, `mode`) is unsafe to bootstrap
/// through, for an operator running as `euid`. `is_leaf` marks the final data-dir component, vetted
/// more strictly by [`reject_unsafe_preexisting_dir`]; here it only suppresses the intermediate
/// writable-ancestor check. Returns the hazard, or `None` when safe.
///
/// A foreign-owned directory with its owner-write bit set is unsafe even when group/world
/// permissions are closed: that owner can rename/replace the next component, or secret files when
/// the component is the leaf. Exceptions, via [`existing_dir_owner_is_safe_for_euid`]: a component
/// owned by root, by the operator, or by the unmappable overflow uid (the root-squash / userns
/// sentinel) is trusted — the last keeps a root-squashed `0755` `/` usable. A foreign-owned component
/// with no write bits is allowed regardless.
fn dir_component_hazard(owner_uid: u32, mode: u32, euid: u32, is_leaf: bool) -> Option<DirHazard> {
    // 0o022 = group/other-writable; 0o1000 = sticky.
    let writable_by_others = mode & 0o022 != 0;
    let owner_writable = mode & 0o200 != 0;
    let sticky = mode & 0o1000 != 0;
    if owner_writable && !existing_dir_owner_is_safe_for_euid(owner_uid, euid) {
        return Some(DirHazard::ForeignOwnerWritableComponent);
    }
    if !writable_by_others {
        return None;
    }
    if !is_leaf && !sticky {
        return Some(DirHazard::WritableNonStickyAncestor);
    }
    // A shared (writable) dir: its owner can rename/replace our entries even under the sticky bit,
    // so trust it only when root or the operator owns it.
    if !existing_dir_owner_is_safe_for_euid(owner_uid, euid) {
        return Some(DirHazard::UntrustedSharedOwner);
    }
    None
}

/// Whether a PRE-EXISTING directory component's `owner_uid` can be trusted not to redirect entries
/// beneath it, for an operator running as `euid`. Trusted: the operator itself (`euid`), root (`0`),
/// and the kernel **overflow uid** — the unmappable root-squash / user-namespace sentinel that NO
/// process can run AS, so it cannot rename/replace anything (see [`overflow_uid`]). A REAL foreign
/// user is never trusted.
fn existing_dir_owner_is_safe_for_euid(owner_uid: u32, euid: u32) -> bool {
    owner_uid == euid || owner_uid == 0 || owner_uid == overflow_uid()
}

/// The kernel overflow / root-squash sentinel uid (`/proc/sys/kernel/overflowuid`, default 65534).
/// Filesystem owners with no valid local mapping surface as this uid — most visibly `/` appearing as
/// `65534` mode `0755` on a root-squashed NFS mount or inside a user-namespace sandbox. NO process
/// can run AS the overflow uid (it is the kernel's "no mapping" placeholder), so a component it owns
/// cannot be used by an attacker to redirect the operator seed / config. We therefore treat it like
/// root/self for owner-trust, which keeps headless bootstrap usable under root-squash WITHOUT
/// trusting any real foreign user (review R1 P2). Read once and cached.
///
/// Residual, documented honestly: on a conventional host this same uid is the runnable `nobody`
/// account, so trusting it also accepts a `nobody`-owned, owner-writable ancestor on the data-dir
/// path. That is a narrow, deliberate trade — a `nobody`-writable directory in your data-dir lineage
/// is already a host misconfiguration, and the owner-INDEPENDENT group/world-writable / non-sticky
/// checks still reject the genuinely dangerous shared-directory cases.
fn overflow_uid() -> u32 {
    static OVERFLOW_UID: OnceLock<u32> = OnceLock::new();
    *OVERFLOW_UID.get_or_init(|| {
        fs::read_to_string("/proc/sys/kernel/overflowuid")
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(65534)
    })
}

fn current_euid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and only reads the current process credentials.
    unsafe { libc::geteuid() }
}

/// Tighten a directory WE created to owner-only (0700) through a NO-FOLLOW handle (`fchmod` on the
/// fd), never the path. A path-based `set_permissions` follows symlinks, so if another local user
/// raced a not-yet-created component into a symlink between the scan and `create_dir_all` (which
/// followed it), a path chmod would land 0700 on the symlink TARGET outside the data dir.
/// `O_NOFOLLOW | O_DIRECTORY` makes the open itself fail (`ELOOP`) on a symlinked final component, so
/// we refuse rather than chmod it; `File::set_permissions` then `fchmod`s the real directory the fd
/// refers to (review R1 P1).
fn harden_dir_perms(dir: &Path) -> Result<(), IpcError> {
    let handle = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(dir)
        .map_err(|e| {
            // `O_NOFOLLOW` on a symlinked final component yields `ELOOP`; with `O_DIRECTORY` also set
            // the kernel may instead report `ENOTDIR` (the un-followed symlink is not a directory).
            // Either way the component is not the real directory we created — refuse rather than
            // chmod it (review R1 P1).
            if matches!(e.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)) {
                config_err(format!(
                    "data dir component {} is a symlink or not a directory; refusing to chmod it \
                     (it could redirect the operator seed / config outside the data dir)",
                    dir.display()
                ))
            } else {
                internal_err(format!(
                    "opening data dir {} to harden perms: {e}",
                    dir.display()
                ))
            }
        })?;
    handle
        .set_permissions(fs::Permissions::from_mode(0o700))
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
    /// More than one `operator` row already exists. The table is meant to be a singleton (SPEC.md
    /// §11, M1a single operator), so a duplicate is corrupt durable state. We refuse rather than
    /// silently reconcile against an arbitrary `LIMIT 1` row — the old code would neither repair nor
    /// reject it (review P3). The same-seed idempotent re-bootstrap (count 0 or 1) is unaffected.
    NotSingleton { count: i64 },
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
    let relays_supplied = relays_explicitly_supplied(raw);
    // A blank value counts as unset (consistent with `resolve_config`), so an omitted/blank field
    // inherits the stored row rather than overwriting it with a default.
    let compute_supplied = non_empty(raw.compute_backend.as_deref()).is_some();
    let payment_supplied = non_empty(raw.payment_backend.as_deref()).is_some();

    let outcome = store
        .transaction(move |tx| {
            // Enforce the §11 operator singleton: a `LIMIT 1` reconcile against a table that somehow
            // holds 2+ rows would pick an arbitrary one and silently leave the duplicate behind. Fail
            // structured instead (review P3). count 0/1 — the normal first/idempotent re-bootstrap —
            // falls straight through.
            //
            // On the concurrent-bootstrap write-skew (review R2 P3): a structural single-row index is
            // deliberately NOT used. A `CREATE UNIQUE INDEX` migration would FAIL to apply to a legacy
            // DB that ALREADY holds duplicates — wedging the store open and denying even this clean
            // structured error — whereas the runtime check degrades gracefully there. And duplicate
            // rows cannot both COMMIT: this whole read-then-insert runs in ONE transaction on the
            // sole-writer store actor (ADR-0001), so two bootstraps in one process serialize; across
            // two processes, SQLite WAL gives each transaction a stable read snapshot, so the second
            // writer's INSERT (after the first commits) fails with a busy/snapshot error — surfaced as
            // a retryable `internal` — and its retry sees the committed row and takes the reconcile
            // path. Either way the table converges to exactly one row.
            let existing_count: i64 =
                tx.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?;
            if existing_count > 1 {
                return Ok(RowOutcome::NotSingleton {
                    count: existing_count,
                });
            }
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
        RowOutcome::NotSingleton { count } => Err(IpcError {
            code: "operator_not_singleton".into(),
            message: format!(
                "operator table holds {count} rows but must hold exactly one (SPEC.md §11); \
                 refusing to bootstrap against corrupt durable state — clear or repair the data dir"
            ),
            // Non-retryable: re-running can't repair a duplicate row; the operator must fix the DB.
            retryable: false,
        }),
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
        "config_invalid"
        | "config_conflict"
        | "identity_invalid"
        | "seed_missing"
        | "seed_conflict"
        | "operator_not_singleton" => 3,
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

    // Review P3: the operator table is a singleton (SPEC.md §11). If corrupt durable state somehow
    // holds two operator rows, bootstrap refuses with a structured `operator_not_singleton` error +
    // nonzero exit rather than reconciling against an arbitrary `LIMIT 1` row — and persists nothing.
    #[tokio::test]
    async fn duplicate_operator_rows_are_rejected_as_not_singleton() {
        let dir = temp_data_dir();
        let store = mem_store();
        // Plant two operator rows directly (corrupt state). The count>1 singleton check fires before
        // any per-identity reconcile, so the specific identities don't matter.
        store
            .transaction(|tx| {
                for k in ["aa", "bb"] {
                    tx.execute(
                        "INSERT INTO operator \
                         (master_pubkey, box_index, op_pubkey, payment_backend, compute_backend, relays) \
                         VALUES (?, ?, ?, ?, ?, ?)",
                        rusqlite::params![
                            k.repeat(32),
                            BOX_INDEX,
                            k.repeat(32),
                            "mock",
                            DEFAULT_COMPUTE_BACKEND,
                            "[]"
                        ],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();

        let err = err_of(bootstrap(raw_mock(&dir), &store).await);
        assert_eq!(err.code, "operator_not_singleton");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);

        // Bootstrap added no row to the already-corrupt table, and wrote no seed.
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(
            count, 2,
            "bootstrap must not add a row to a non-singleton table"
        );
        assert!(
            !dir.join(SEED_FILE).exists(),
            "a not-singleton failure must not persist the seed"
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

    // Review R2 P3 (§13, mirrors `parse_error_does_not_leak_mnemonic`): a CORRUPT durable
    // `fedimint.json` must fail structured WITHOUT echoing its contents. A top-level type mismatch
    // (here a bare JSON string instead of the object) makes serde's `Display` quote the offending
    // scalar — the sensitive invite/gateway — and that message is printed to stderr on bootstrap
    // failure, so the error must report only a non-sensitive parse location, never the content.
    #[test]
    fn corrupt_durable_fedimint_config_error_does_not_leak_contents() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        // Valid JSON, but a bare string is not a `FedimintConfig`: serde reports
        // `invalid type: string "...", expected struct FedimintConfig`, quoting the secret value.
        let secret = "fed11SECRETINVITE0xDEADBEEF";
        fs::write(dir.join(FEDIMINT_CONFIG_FILE), format!("\"{secret}\"")).unwrap();

        let err = match read_fedimint_config(&dir) {
            Ok(_) => panic!("a corrupt durable Fedimint config must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(
            !err.message.contains(secret),
            "fedimint parse error leaked the durable config contents: {}",
            err.message
        );
        assert!(
            !err.message.contains("SECRET"),
            "fedimint parse error leaked config contents: {}",
            err.message
        );
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
        // Pin owner-only perms so the pre-existing leaf is a safe target regardless of test umask
        // (the dir-perms check rejects a world-writable/sticky pre-existing data dir).
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
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
        // Pin owner-only perms so the dir-perms check (which rejects a world-writable/sticky
        // pre-existing data dir) passes and the SEED symlink is the actual rejection under test.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
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

    // Review (dir-perms footgun): a PRE-EXISTING leaf data dir is NEVER chmod'ed — chmod'ing a dir
    // we did not create is the footgun this closes (it could damage a shared/system dir's perms
    // under privilege). A SAFE pre-existing leaf (not world-writable, not sticky) is left at its
    // original mode and bootstrap still succeeds; the 0600 secret files inside still protect the
    // secrets. Components WE create are still hardened to 0700 (covered by the tests above).
    #[tokio::test]
    async fn preexisting_safe_leaf_data_dir_is_left_untouched() {
        let base = temp_data_dir();
        let dir = base.join("leaf");
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        // 0o750: group-traversable but NOT world-writable and NOT sticky — a safe target we leave be.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o750)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw, &store)
            .await
            .expect("bootstrap into a pre-existing safe leaf data dir");

        assert_eq!(
            file_mode(&dir),
            0o750,
            "a pre-existing safe leaf data dir must be left at its original mode (never chmod'ed)"
        );
        assert_eq!(
            file_mode(&base),
            0o755,
            "a pre-existing parent must keep its original mode"
        );
        assert_eq!(file_mode(&dir.join(SEED_FILE)), 0o600);
        let _ = fs::remove_dir_all(&base);
    }

    // Review (dir-perms footgun): pointing the data dir straight at a PRE-EXISTING world-writable or
    // sticky directory we did not create — e.g. `/tmp` (mode 1777) — is rejected with a structured
    // config error rather than chmod'ed (which would clear the sticky bit / damage host perms when
    // run privileged). Nothing is created, no seed written, no operator row committed.
    #[tokio::test]
    async fn preexisting_world_writable_data_dir_is_rejected() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        // 1777: sticky + world-writable — the canonical /tmp-style shared scratch dir.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o1777)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);

        // The shared dir's perms were NOT changed (sticky bit intact), and nothing was written.
        let mode = fs::metadata(&dir).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, 0o1777,
            "bootstrap must not chmod the pre-existing shared dir"
        );
        assert!(
            !dir.join(SEED_FILE).exists(),
            "no seed must be written into a rejected data dir"
        );
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected before persisting any operator row");
        let _ = fs::remove_dir_all(&dir);
    }

    // Review P2: group-writable pre-existing leaves are unsafe too. A same-group user with write
    // permission on the directory can unlink or replace `operator.seed` / `fedimint.json` even when
    // those files are 0600, so bootstrap must fail instead of adopting the directory.
    #[tokio::test]
    async fn preexisting_group_writable_data_dir_is_rejected() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o770)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);

        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o7777,
            0o770,
            "bootstrap must not chmod the pre-existing group-writable dir"
        );
        assert!(
            !dir.join(SEED_FILE).exists(),
            "no seed must be written into a rejected data dir"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R1 P2: a PRE-EXISTING data dir we cannot WRITE (here owner-only `0500`, but equally a
    // root-owned `0755` under an unprivileged daemon) fails with a DETERMINISTIC `config_invalid`
    // BEFORE the store opens or any row is written — not a retryable `internal` fault mid-bootstrap
    // once the seed temp-file / state-DB creation errors (which, with an externally-supplied store,
    // could otherwise land AFTER the operator row already committed). The dir is never chmod'ed.
    #[tokio::test]
    async fn preexisting_unwritable_data_dir_is_rejected_before_state_changes() {
        // Root bypasses directory permission bits, so this access pre-flight is only meaningful for an
        // unprivileged euid; skip under root rather than assert a false negative.
        if current_euid() == 0 {
            return;
        }
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        // 0o500: owner r-x, NO write — we own it but cannot create the seed / DB inside it. Not
        // group/world-writable or sticky, so it passes the shared-dir checks and reaches the
        // writability pre-flight.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o500)).unwrap();

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(
            !err.retryable,
            "an unwritable target is a deterministic config error, not a retryable internal one"
        );
        assert_ne!(exit_code(&err.code), 0);

        assert_eq!(
            fs::metadata(&dir).unwrap().permissions().mode() & 0o7777,
            0o500,
            "bootstrap must not chmod the pre-existing dir"
        );
        assert!(
            !dir.join(SEED_FILE).exists(),
            "no seed must be written into a rejected data dir"
        );
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected before persisting any operator row");

        // Restore owner-write so the temp dir can be cleaned up.
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o700));
        let _ = fs::remove_dir_all(&dir);
    }

    // Review (dir-perms footgun): the filesystem root `/` is never adopted as a data dir — it has no
    // parent and chmod'ing it would be catastrophic, so it fails with a structured config error.
    #[tokio::test]
    async fn filesystem_root_data_dir_is_rejected() {
        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some("/".into()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert_ne!(exit_code(&err.code), 0);
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

    // Review round 7 P2: an existing regular file anywhere on the data-dir path is a deterministic
    // bad config, not a retryable `internal` failure from `create_dir_all`.
    #[tokio::test]
    async fn regular_file_data_dir_component_is_config_invalid() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

        let leaf_file = base.join("leaf-file");
        fs::write(&leaf_file, b"not a directory").unwrap();
        let intermediate_file = base.join("intermediate-file");
        fs::write(&intermediate_file, b"not a directory").unwrap();

        for data_dir in [leaf_file.clone(), intermediate_file.join("leaf")] {
            let store = mem_store();
            let raw = RawConfig {
                data_dir: Some(data_dir.to_string_lossy().into_owned()),
                mnemonic: Some(TEST_MNEMONIC.into()),
                ..Default::default()
            };
            let err = err_of(bootstrap(raw, &store).await);
            assert_eq!(err.code, "config_invalid");
            assert!(!err.retryable);
            assert_ne!(exit_code(&err.code), 0);
            assert!(
                err.message.contains("not a directory"),
                "unexpected error: {}",
                err.message
            );

            let count: i64 = store
                .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
                .await
                .unwrap();
            assert_eq!(count, 0, "rejected before persisting any operator row");
        }

        assert!(
            leaf_file.is_file(),
            "bootstrap must not replace a regular file leaf"
        );
        assert!(
            intermediate_file.is_file(),
            "bootstrap must not replace a regular file intermediate"
        );
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

    // Review P2: a blank relay list passed directly to the public `bootstrap` API is semantically
    // unset. It must inherit stored relays on re-bootstrap instead of being treated as an explicit
    // request to overwrite custom relays with the default relay set.
    #[tokio::test]
    async fn rebootstrap_blank_relays_inherits_stored_relays() {
        let dir = temp_data_dir();
        let store = mem_store();
        let custom = vec!["wss://custom.example".to_string()];
        let raw1 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            relays: Some(custom.clone()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw1, &store).await.expect("first");

        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            relays: Some(vec![" ".into(), "\t".into()]),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store).await.expect("second");
        assert_eq!(op2.config.relays, custom);

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

    // ===== Headless multi-source bootstrap surface (P1, ADR-0014 §4.7) ============================

    // Precedence flags > env > file > stdin: a value set in TWO sources resolves to the
    // higher-precedence one, and each lower source fills only what the higher ones left unset.
    #[test]
    fn from_sources_resolves_to_higher_precedence_value() {
        let flags = RawConfig {
            payment_backend: Some("fedimint".into()),
            ..Default::default()
        };
        let env = RawConfig {
            // also set in flags -> flags wins; and a field only env+file set -> env wins.
            payment_backend: Some("mock".into()),
            compute_backend: Some("incus".into()),
            ..Default::default()
        };
        let file = RawConfig {
            compute_backend: Some("libvirt".into()),
            data_dir: Some("/file/dir".into()),
            ..Default::default()
        };
        let stdin = RawConfig {
            data_dir: Some("/stdin/dir".into()),
            relays: Some(vec!["wss://stdin.example".into()]),
            ..Default::default()
        };

        let merged = RawConfig::from_sources(flags, env, file, stdin);
        assert_eq!(
            merged.payment_backend.as_deref(),
            Some("fedimint"),
            "flags > env"
        );
        assert_eq!(
            merged.compute_backend.as_deref(),
            Some("incus"),
            "env > file"
        );
        assert_eq!(
            merged.data_dir.as_deref(),
            Some("/file/dir"),
            "file > stdin"
        );
        assert_eq!(
            merged.relays,
            Some(vec!["wss://stdin.example".to_string()]),
            "stdin fills what no higher source set"
        );
    }

    // A BLANK value in a higher-precedence source must not shadow a real value in a lower one
    // (a blank flag/env counts as unset, consistent with `resolve_config`).
    #[test]
    fn from_sources_blank_does_not_shadow_lower_source() {
        let flags = RawConfig {
            data_dir: Some("   ".into()),
            ..Default::default()
        };
        let env = RawConfig {
            data_dir: Some("/env/dir".into()),
            ..Default::default()
        };
        let merged =
            RawConfig::from_sources(flags, env, RawConfig::default(), RawConfig::default());
        assert_eq!(merged.data_dir.as_deref(), Some("/env/dir"));
    }

    // The `env` layer reads the documented vars (relays comma-split) via an injected lookup.
    #[test]
    fn raw_config_from_env_reads_documented_vars() {
        let env = raw_config_from_env(|k| match k {
            ENV_DATA_DIR => Some("/env/data".into()),
            ENV_PAYMENT_BACKEND => Some("fedimint".into()),
            ENV_RELAYS => Some("wss://a.example, wss://b.example".into()),
            _ => None,
        });
        assert_eq!(env.data_dir.as_deref(), Some("/env/data"));
        assert_eq!(env.payment_backend.as_deref(), Some("fedimint"));
        assert_eq!(
            env.relays,
            Some(vec![
                "wss://a.example".to_string(),
                "wss://b.example".to_string()
            ])
        );
        assert!(env.compute_backend.is_none());
    }

    // A config document parses as TOML OR JSON; empty is an empty layer; a doc that is neither
    // fails with a structured `config_invalid` (a typo must not silently default, §4.7).
    #[test]
    fn parse_raw_config_doc_accepts_toml_json_and_rejects_garbage() {
        let toml_cfg = parse_raw_config_doc(r#"payment_backend = "fedimint""#, "doc").unwrap();
        assert_eq!(toml_cfg.payment_backend.as_deref(), Some("fedimint"));

        let json_cfg = parse_raw_config_doc(
            r#"{"payment_backend":"mock","relays":["wss://a.example"]}"#,
            "doc",
        )
        .unwrap();
        assert_eq!(json_cfg.payment_backend.as_deref(), Some("mock"));
        assert_eq!(json_cfg.relays, Some(vec!["wss://a.example".to_string()]));

        assert!(parse_raw_config_doc("  \n  ", "doc")
            .unwrap()
            .payment_backend
            .is_none());

        let err = match parse_raw_config_doc("this is neither toml nor json", "doc") {
            Ok(_) => panic!("garbage doc must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
    }

    // A typo'd key is valid TOML syntax but invalid RawConfig because of `deny_unknown_fields`.
    // The document parser falls through to JSON before building its sanitized structured error, so
    // make the message honest without echoing the secret-bearing source text.
    #[test]
    fn parse_raw_config_doc_unknown_field_message_is_not_misleading() {
        let doc = "payment-backend = \"fedimint\"\nmnemonic = \"not logged\"";
        let err = match parse_raw_config_doc(doc, "doc") {
            Ok(_) => panic!("unknown field must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(
            err.message.contains("unknown field"),
            "unexpected error message: {}",
            err.message
        );
        assert!(
            !err.message.contains("not logged"),
            "parse error leaked source content: {}",
            err.message
        );
    }

    // A FULL headless bootstrap from env + file (neither source complete on its own) produces a
    // ready operator and the single persisted row — the §4.7 non-interactive path end to end.
    #[tokio::test]
    async fn headless_bootstrap_from_env_and_file_produces_ready_operator() {
        let dir = temp_data_dir();
        let store = mem_store();

        // env supplies the data dir + the seed; the file supplies the Fedimint config. This secure
        // split is the one the CLI recommends (seed outside argv, durable onboarding config in a
        // file), and it exercises the overlay path where a higher-precedence mnemonic must not wipe
        // lower-precedence Fedimint fields.
        let env = raw_config_from_env(|k| match k {
            ENV_DATA_DIR => Some(dir.to_string_lossy().into_owned()),
            ENV_MNEMONIC => Some(TEST_MNEMONIC.into()),
            _ => None,
        });
        let file = parse_raw_config_doc(
            r#"
                payment_backend = "fedimint"
                relays = ["wss://from-file.example"]
                fedimint_invite = "fed11fromfile"
                fedimint_gateway = "03fromfile"
            "#,
            "test file",
        )
        .unwrap();
        let merged = RawConfig::from_sources(RawConfig::default(), env, file, RawConfig::default());

        let op = bootstrap(merged, &store)
            .await
            .expect("headless bootstrap from env+file");
        assert_eq!(op.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);
        assert_eq!(op.config.payment_backend, PaymentMode::Fedimint);
        assert_eq!(
            op.config.fedimint,
            Some(FedimintConfig {
                invite: "fed11fromfile".into(),
                gateway: "03fromfile".into()
            })
        );
        assert_eq!(
            op.config.relays,
            vec!["wss://from-file.example".to_string()]
        );

        let (count, master, ..) = read_operator_row(&store).await;
        assert_eq!(count, 1);
        assert_eq!(master, EXPECTED_PUBKEY_HEX);
        let _ = fs::remove_dir_all(&dir);
    }

    // A missing REQUIRED value (no seed in any source) fails with the structured `seed_missing`
    // error and a nonzero exit — the headless contract: a structured failure, never a prompt.
    #[tokio::test]
    async fn headless_bootstrap_missing_seed_is_structured_error_with_nonzero_exit() {
        let dir = temp_data_dir();
        let store = mem_store();
        let env = raw_config_from_env(|k| match k {
            ENV_DATA_DIR => Some(dir.to_string_lossy().into_owned()),
            _ => None, // no mnemonic anywhere
        });
        let merged = RawConfig::from_sources(
            RawConfig::default(),
            env,
            RawConfig::default(),
            RawConfig::default(),
        );
        let err = err_of(bootstrap(merged, &store).await);
        assert_eq!(err.code, "seed_missing");
        assert!(!err.retryable);
        assert_ne!(exit_code(&err.code), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    // The executable entrypoint `bootstrap_headless` opens its OWN store under the data dir (the
    // path the daemon uses) and is idempotent across runs — proving the headless path is wired, not
    // just the merge.
    #[tokio::test]
    async fn bootstrap_headless_opens_store_and_persists_operator() {
        let dir = temp_data_dir();
        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let op = bootstrap_headless(raw)
            .await
            .expect("headless bootstrap opens its own store");
        assert_eq!(op.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);

        // It created the 0700 data dir, the state DB, and the 0600 seed under it.
        assert_eq!(file_mode(&dir), 0o700);
        assert!(
            dir.join(STATE_DB_FILE).exists(),
            "state DB created under the data dir"
        );
        assert_eq!(file_mode(&dir.join(SEED_FILE)), 0o600);
        // The state DB is tightened to owner-only too, not left at the process umask (review R1 P2).
        assert_eq!(
            file_mode(&dir.join(STATE_DB_FILE)),
            0o600,
            "state DB must be owner-only, not the umask default"
        );

        // Re-running headlessly (seed read back from the data dir) stays idempotent.
        let op2 = bootstrap_headless(RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .await
        .expect("idempotent re-bootstrap");
        assert_eq!(op2.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R1 P2: `harden_state_db_perms` tightens the sqlite DB and its PRESENT WAL sidecars to
    // owner-only 0600, and TOLERATES an absent sidecar (it need not exist) — never creating it.
    #[test]
    fn harden_state_db_perms_tightens_db_and_present_sidecars() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join(STATE_DB_FILE);
        let wal = db_sidecar_path(&db, "-wal");
        // Main DB + a `-wal` sidecar created world-readable, as sqlite would under a loose umask; the
        // `-shm` sidecar is intentionally left absent.
        fs::write(&db, b"db").unwrap();
        fs::write(&wal, b"wal").unwrap();
        fs::set_permissions(&db, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&wal, fs::Permissions::from_mode(0o644)).unwrap();

        harden_state_db_perms(&db).expect("harden db + present sidecar, tolerate the missing one");
        assert_eq!(file_mode(&db), 0o600, "state DB tightened to owner-only");
        assert_eq!(
            file_mode(&wal),
            0o600,
            "WAL sidecar tightened to owner-only"
        );
        assert!(
            !db_sidecar_path(&db, "-shm").exists(),
            "an absent sidecar must be tolerated, not created"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R1 P2: hardening the DB must NEVER follow a symlink and chmod its target outside the
    // data dir — `harden_perms` refuses a symlinked path, same as the seed/fedimint files.
    #[test]
    fn harden_state_db_perms_refuses_a_symlinked_db() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-db-outside-{}", std::process::id()));
        fs::write(&outside, b"outside").unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&outside, dir.join(STATE_DB_FILE)).unwrap();

        let err = match harden_state_db_perms(&dir.join(STATE_DB_FILE)) {
            Ok(()) => panic!("a symlinked state DB must be refused, not chmod'ed through"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert_eq!(
            file_mode(&outside),
            0o644,
            "must not chmod a symlink target outside the data dir"
        );
        let _ = fs::remove_file(&outside);
        let _ = fs::remove_dir_all(&dir);
    }

    // Review round 9 R1 P2: the state DB path must be rejected BEFORE SQLite opens it. Otherwise
    // `Connection::open` follows a pre-existing symlink and creates/modifies the target outside the
    // data dir before `harden_state_db_perms` gets a chance to refuse it.
    #[tokio::test]
    async fn bootstrap_headless_refuses_symlinked_state_db_before_sqlite_open() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let outside =
            std::env::temp_dir().join(format!("lnrent-db-preopen-outside-{}", std::process::id()));
        let _ = fs::remove_file(&outside);
        std::os::unix::fs::symlink(&outside, dir.join(STATE_DB_FILE)).unwrap();

        let err = err_of(
            bootstrap_headless(RawConfig {
                data_dir: Some(dir.to_string_lossy().into_owned()),
                mnemonic: Some(TEST_MNEMONIC.into()),
                ..Default::default()
            })
            .await,
        );
        assert_eq!(err.code, "config_invalid");
        assert!(
            !outside.exists(),
            "sqlite must not create or modify a symlink target outside the data dir"
        );
        assert!(
            !dir.join(SEED_FILE).exists(),
            "preflight failure must stop before persisting the seed"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn preflight_state_db_paths_refuses_symlinked_sidecar_before_sqlite_open() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join(STATE_DB_FILE);
        let outside =
            std::env::temp_dir().join(format!("lnrent-db-sidecar-outside-{}", std::process::id()));
        let _ = fs::remove_file(&outside);
        std::os::unix::fs::symlink(&outside, db_sidecar_path(&db, "-wal")).unwrap();

        let err = match preflight_state_db_paths(&db) {
            Ok(()) => panic!("a symlinked state DB sidecar must be refused before sqlite opens"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(
            !outside.exists(),
            "sqlite must not create or modify a sidecar symlink target outside the data dir"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R1 P2 (end to end): the reviewer's scenario — bootstrap into a PRE-EXISTING traversable
    // data dir (e.g. a systemd /var/lib/lnrent at 0750, which the dir-perms fix deliberately does NOT
    // chmod) — must still leave the sqlite state DB owner-only 0600, or its later credential-bearing
    // rows would be group/world-readable through the traversable dir.
    #[tokio::test]
    async fn bootstrap_headless_hardens_state_db_in_preexisting_traversable_dir() {
        let dir = temp_data_dir();
        fs::create_dir_all(&dir).unwrap();
        // 0o750: group-traversable but NOT world-writable and NOT sticky — a safe pre-existing leaf
        // we adopt without chmod'ing (covered by `preexisting_safe_leaf_data_dir_is_left_untouched`).
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o750)).unwrap();

        let raw = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap_headless(raw)
            .await
            .expect("headless bootstrap into a pre-existing traversable dir");

        let db = dir.join(STATE_DB_FILE);
        assert_eq!(file_mode(&db), 0o600, "state DB must be owner-only");
        assert_eq!(
            file_mode(&dir),
            0o750,
            "a pre-existing traversable leaf must be left at its own mode (never chmod'ed)"
        );

        // Deterministic proof the hardening runs EVERY bootstrap, independent of the test's ambient
        // umask: loosen the DB out-of-band (as an older build under a loose umask would have left it),
        // then a seed-read-back re-bootstrap must re-tighten it.
        fs::set_permissions(&db, fs::Permissions::from_mode(0o644)).unwrap();
        bootstrap_headless(RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .await
        .expect("idempotent re-bootstrap re-hardens the DB");
        assert_eq!(
            file_mode(&db),
            0o600,
            "re-bootstrap re-tightens a loosened state DB"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R1 P1: a malformed config document that carries the seed must NOT echo its source line
    // in the structured parse error — that message is printed to stderr on failure, and leaking the
    // mnemonic there breaks the §13 "never logged" contract. Only a non-sensitive location is shown.
    #[test]
    fn parse_error_does_not_leak_mnemonic() {
        // A TOML doc carrying the seed with a deliberate syntax error (an unterminated string that
        // runs into the next line). This fails as both TOML and JSON, reaching the error branch.
        let leaky = format!("mnemonic = \"{TEST_MNEMONIC}\nrelays = [\"wss://x\"]");
        let err = match parse_raw_config_doc(&leaky, "config file x") {
            Ok(_) => panic!("a malformed config document must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(
            !err.message.contains(TEST_MNEMONIC),
            "parse error leaked the full mnemonic: {}",
            err.message
        );
        for word in ["leader", "monkey", "parrot", "bean"] {
            assert!(
                !err.message.contains(word),
                "parse error leaked seed word `{word}`: {}",
                err.message
            );
        }
    }

    // Review R1 P2: a blank `LNRENT_CONFIG` (common in templated environments that export optional
    // vars empty) is treated as UNSET, not as a path to an empty filename. The flag wins over env,
    // and a blank flag falls back to env.
    #[test]
    fn config_path_treats_blank_env_as_unset() {
        assert_eq!(resolve_config_path(None, Some(String::new())), None);
        assert_eq!(resolve_config_path(None, Some("   ".into())), None);
        assert_eq!(
            resolve_config_path(None, Some("/etc/lnrent.toml".into())),
            Some(PathBuf::from("/etc/lnrent.toml"))
        );
        assert_eq!(
            resolve_config_path(Some(PathBuf::from("/flag.toml")), Some("/env.toml".into())),
            Some(PathBuf::from("/flag.toml")),
            "explicit flag wins over env"
        );
        assert_eq!(
            resolve_config_path(Some(PathBuf::new()), Some("/env.toml".into())),
            Some(PathBuf::from("/env.toml")),
            "an empty flag falls back to env"
        );
    }

    // Review R1 P2: BIP39 words are whitespace-insensitive, so a re-bootstrap that supplies the SAME
    // seed with different spacing must be idempotent — not a false `seed_conflict`. The persisted
    // seed is the canonical single-space form, and the derived identity is unchanged.
    #[tokio::test]
    async fn rebootstrap_with_whitespace_equivalent_mnemonic_is_idempotent() {
        let dir = temp_data_dir();
        let store = mem_store();

        // First bootstrap with a NON-canonical (double-spaced) but BIP39-equivalent phrase.
        let spaced = TEST_MNEMONIC.replace(' ', "  ");
        let raw1 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(spaced),
            ..Default::default()
        };
        let op1 = bootstrap(raw1, &store)
            .await
            .expect("first bootstrap with a double-spaced mnemonic");
        assert_eq!(op1.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);
        // The seed is persisted in the canonical single-space form.
        let stored = fs::read_to_string(dir.join(SEED_FILE)).unwrap();
        assert_eq!(
            stored.trim(),
            TEST_MNEMONIC,
            "the seed must be persisted in canonical BIP39 form"
        );

        // Re-bootstrap with the standard single-space form: idempotent, NOT a seed_conflict.
        let raw2 = RawConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let op2 = bootstrap(raw2, &store)
            .await
            .expect("an equivalent (re-spaced) mnemonic must not conflict");
        assert_eq!(op2.identity.pubkey_hex(), EXPECTED_PUBKEY_HEX);
        let (count, ..) = read_operator_row(&store).await;
        assert_eq!(count, 1, "still exactly one operator row");
        let _ = fs::remove_dir_all(&dir);
    }

    // Review R2 P2: a pre-existing group/world-writable, NON-sticky INTERMEDIATE ancestor lets
    // another user redirect the dirs we create beneath it, so bootstrap refuses with a structured
    // error — without chmod'ing the ancestor, creating anything, or persisting a row.
    #[tokio::test]
    async fn preexisting_world_writable_intermediate_ancestor_is_rejected() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        // 0o707: world-writable and NOT sticky — an unsafe shared parent we must not create under.
        fs::set_permissions(&base, fs::Permissions::from_mode(0o707)).unwrap();
        let data_dir = base.join("leaf"); // does not exist yet

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(data_dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        let err = err_of(bootstrap(raw, &store).await);
        assert_eq!(err.code, "config_invalid");
        assert!(!err.retryable);

        assert!(
            !data_dir.exists(),
            "nothing must be created beneath an unsafe ancestor"
        );
        assert_eq!(
            fs::metadata(&base).unwrap().permissions().mode() & 0o7777,
            0o707,
            "the unsafe ancestor's perms must not be changed"
        );
        let count: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM operator", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected before persisting any operator row");
        let _ = fs::remove_dir_all(&base);
    }

    // Review R1 P2: for a relative data dir, the implicit current working directory is part of the
    // trusted path. Validate the absolute CWD+dir path before creating anything, so an unsafe CWD
    // cannot redirect the first path component.
    #[test]
    fn relative_data_dir_vets_implicit_current_directory() {
        let cwd = temp_data_dir();
        fs::create_dir_all(&cwd).unwrap();
        fs::set_permissions(&cwd, fs::Permissions::from_mode(0o707)).unwrap();

        let err = match vet_data_dir_components_from_cwd(Path::new("leaf"), &cwd) {
            Ok(_) => panic!("unsafe implicit current directory must be rejected"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert!(
            err.message.contains("group/world-writable"),
            "unexpected error: {}",
            err.message
        );
        assert!(
            !cwd.join("leaf").exists(),
            "validation must not create the relative data dir"
        );
        let _ = fs::remove_dir_all(&cwd);
    }

    // Review R1 P2: a SHARED (group/world-writable) ancestor must be owned by the operator uid, root,
    // or the unmappable overflow uid — a sticky bit does not make it safe when another REAL local
    // user owns it, because that owner can rename/replace entries despite the sticky bit. (This gate
    // is consulted only for writable dirs now; see `dir_component_hazard`.)
    #[test]
    fn existing_component_owner_must_be_operator_or_root() {
        assert!(existing_dir_owner_is_safe_for_euid(1000, 1000));
        assert!(existing_dir_owner_is_safe_for_euid(0, 1000));
        assert!(!existing_dir_owner_is_safe_for_euid(1001, 1000));
        assert!(existing_dir_owner_is_safe_for_euid(0, 0));
        assert!(!existing_dir_owner_is_safe_for_euid(1001, 0));
        // Root-squash / userns sentinel: trusted like root/self because no process can act as it
        // (review R1 P2) — a root-squashed `/` shown as the overflow uid must stay usable.
        assert!(existing_dir_owner_is_safe_for_euid(overflow_uid(), 1000));
    }

    // Review R1 P1/R5 P2 (+ round-6 R1 P2): a foreign-owned component that is not writable by anyone
    // is SAFE on owner grounds, and a REAL-foreign-owned owner-writable component is unsafe because
    // that owner can rename/replace child entries even when group/world bits are closed — EXCEPT the
    // unmappable overflow uid (the root-squash / userns sentinel), which is trusted like root/self
    // because no process can act as it.
    #[test]
    fn foreign_owned_owner_writable_component_is_rejected() {
        let euid = 1000;
        // A genuinely-foreign, MAPPABLE uid (a real other local user) — not euid/root/overflow.
        let foreign = 4242;
        assert_ne!(foreign, overflow_uid());

        // Read-only ancestor owned by a foreign uid: safe regardless of owner.
        assert_eq!(dir_component_hazard(foreign, 0o555, euid, false), None);
        // ...the same as a leaf component.
        assert_eq!(dir_component_hazard(foreign, 0o555, euid, true), None);
        // But a foreign-owned 0755 component is unsafe: its owner can redirect child entries.
        assert_eq!(
            dir_component_hazard(foreign, 0o755, euid, false),
            Some(DirHazard::ForeignOwnerWritableComponent)
        );
        assert_eq!(
            dir_component_hazard(foreign, 0o755, euid, true),
            Some(DirHazard::ForeignOwnerWritableComponent)
        );
        // A group/world-writable, NON-sticky intermediate is unsafe regardless of owner.
        assert_eq!(
            dir_component_hazard(euid, 0o777, euid, false),
            Some(DirHazard::WritableNonStickyAncestor)
        );
        // The /tmp convention: sticky world-writable owned by root or the operator is allowed...
        assert_eq!(dir_component_hazard(0, 0o1777, euid, false), None);
        assert_eq!(dir_component_hazard(euid, 0o1777, euid, false), None);
        // ...but a sticky world-writable dir owned by a real FOREIGN uid is rejected — its owner can
        // bypass the sticky bit and rename/replace our entries.
        assert_eq!(
            dir_component_hazard(foreign, 0o1777, euid, false),
            Some(DirHazard::ForeignOwnerWritableComponent)
        );

        // Root-squash / userns (review R1 P2): the overflow uid is the unmappable sentinel, so a
        // component it owns is trusted even when owner-writable — a `/` shown as `65534 0755` no
        // longer wrongly blocks the data dir beneath it. The owner-INDEPENDENT shared-dir check still
        // rejects a NON-sticky world-writable dir even under the overflow owner.
        let squashed = overflow_uid();
        assert_eq!(dir_component_hazard(squashed, 0o755, euid, false), None);
        assert_eq!(dir_component_hazard(squashed, 0o755, euid, true), None);
        assert_eq!(dir_component_hazard(squashed, 0o1777, euid, false), None);
        assert_eq!(
            dir_component_hazard(squashed, 0o777, euid, false),
            Some(DirHazard::WritableNonStickyAncestor)
        );
    }

    // Review R1 P1: hardening a created data dir must chmod through a NO-FOLLOW handle. If a
    // directory component is (raced into) a symlink, `harden_dir_perms` must REFUSE rather than
    // follow it and chmod the symlink target outside the data dir.
    #[test]
    fn harden_dir_perms_refuses_to_follow_a_symlink() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        let target = base.join("target");
        fs::create_dir_all(&target).unwrap();
        // A mode our 0700 chmod would visibly change if it followed the symlink.
        fs::set_permissions(&target, fs::Permissions::from_mode(0o755)).unwrap();
        let link = base.join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = match harden_dir_perms(&link) {
            Ok(()) => panic!("hardening a symlinked dir component must be refused"),
            Err(e) => e,
        };
        assert_eq!(err.code, "config_invalid");
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o755,
            "the symlink target's perms must be unchanged (no chmod through the symlink)"
        );
        let _ = fs::remove_dir_all(&base);
    }

    // Review R2 P2: a STICKY world-writable ancestor (the `/tmp` convention — sticky bars other users
    // from renaming/removing our entries) is the standard safe shared parent and is ALLOWED; we
    // create our own 0700 dir beneath it. (Rejecting it would break every `/tmp`-rooted data dir.)
    #[tokio::test]
    async fn preexisting_sticky_world_writable_intermediate_is_allowed() {
        let base = temp_data_dir();
        fs::create_dir_all(&base).unwrap();
        // 1777: sticky + world-writable — the canonical /tmp-style shared scratch parent.
        fs::set_permissions(&base, fs::Permissions::from_mode(0o1777)).unwrap();
        let data_dir = base.join("leaf");

        let store = mem_store();
        let raw = RawConfig {
            data_dir: Some(data_dir.to_string_lossy().into_owned()),
            mnemonic: Some(TEST_MNEMONIC.into()),
            ..Default::default()
        };
        bootstrap(raw, &store)
            .await
            .expect("a sticky shared parent is an allowed ancestor");
        assert_eq!(
            file_mode(&data_dir),
            0o700,
            "our created leaf is owner-only"
        );
        assert_eq!(file_mode(&data_dir.join(SEED_FILE)), 0o600);
        let _ = fs::remove_dir_all(&base);
    }
}
