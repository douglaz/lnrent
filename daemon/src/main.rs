//! lnrentd: the lnrent control plane. AI-free runtime path (SPEC.md §4.1).
//! With no subcommand it opens state, spawns the sole-writer store actor (ADR-0001), loads recipes,
//! and serves the operator IPC socket (§4.2). M1 adds the reconcile loop (§6.5), the Nostr engine,
//! and the payment watch alongside.
//!
//! The `bootstrap` subcommand is the headless operator bootstrap (ADR-0014, §4.7): it assembles the
//! config from flags / env / a config file / stdin (precedence flags > env > file > stdin), derives
//! the operator identity, and persists it — fully non-interactive, never a prompt. On failure it
//! prints the structured `{code, message, retryable}` error to stderr and exits nonzero. (The
//! daemon-startup wiring that also bootstraps is bead .21.)

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use lnrentd::backends::{MockPayment, PaymentBackend};
use lnrentd::backup;
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::config::{self, BootstrapInput, PaymentMode, RawConfig};
#[cfg(feature = "fedimint")]
use lnrentd::fedimint_backend::FedimintPayment;
use lnrentd::ipc::IpcError;
use lnrentd::nostr_engine::NostrEngine;
use lnrentd::recipe::Recipe;
use lnrentd::supervisor::{Intervals, Supervisor};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "lnrentd",
    about = "lnrent control plane daemon (and headless operator bootstrap)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Headlessly bootstrap the operator identity + config from flags / env / a config file / stdin
    /// (precedence flags > env > file > stdin), then exit. Non-interactive: a structured error and a
    /// deterministic nonzero exit on failure, never a prompt (ADR-0014, §4.7).
    Bootstrap(BootstrapArgs),
    /// COLD/OFFLINE backup of the STOPPED daemon's durable state (state DB + fedimint dir + config +
    /// seed) into a fresh dir (lnrent-7fp.14). Stop the daemon first: this refuses to run if its IPC
    /// socket is live. Non-interactive; `--json` summary; nonzero exit on error.
    Backup(BackupArgs),
    /// Restore a backup produced by `backup` into a data dir (lnrent-7fp.14). Restores into a
    /// fresh/empty data dir by default; `--force` overwrites a non-empty one. Non-interactive;
    /// `--json` summary; nonzero exit on error.
    Restore(RestoreArgs),
}

/// The flag-sourced bootstrap layer (highest precedence). clap's own `env` support is deliberately
/// NOT used here — env is a separate, lower-precedence layer resolved in `config::load_raw_config`,
/// so the precedence stays explicit rather than clap silently folding env into the flag.
#[derive(Args)]
struct BootstrapArgs {
    /// Daemon data dir (holds the 0600 seed, fedimint config, and state DB). Env: LNRENT_DATA_DIR.
    #[arg(long)]
    data_dir: Option<String>,
    /// Receive backend: `mock` (M1a default) or `fedimint`. Env: LNRENT_PAYMENT_BACKEND.
    #[arg(long)]
    payment_backend: Option<String>,
    /// Compute backend (`host` default, `incus`, `libvirt`, `proxmox`, `cloud-*`). Env: LNRENT_COMPUTE_BACKEND.
    #[arg(long)]
    compute_backend: Option<String>,
    /// A Nostr relay URL; repeat for several. A supplied set overrides lower-precedence relays
    /// wholesale. Env: LNRENT_RELAYS (comma-separated).
    #[arg(long = "relay")]
    relays: Vec<String>,
    /// Fedimint federation invite (required when payment_backend=fedimint and none is durable yet).
    /// Env: LNRENT_FEDIMINT_INVITE.
    #[arg(long)]
    fedimint_invite: Option<String>,
    /// Fedimint gateway. Env: LNRENT_FEDIMINT_GATEWAY.
    #[arg(long)]
    fedimint_gateway: Option<String>,
    /// The operator BIP39 mnemonic (first bootstrap only; read back from the data dir afterward).
    /// Prefer LNRENT_MNEMONIC / a config file / stdin so it doesn't land in the process table.
    #[arg(long)]
    mnemonic: Option<String>,
    /// A TOML or JSON config file to load (lower precedence than flags/env). Env: LNRENT_CONFIG.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Also read a TOML/JSON config document from stdin (lowest precedence). Only read when this
    /// flag is explicit, so inherited open pipes can never block bootstrap.
    #[arg(long)]
    stdin: bool,
    /// Emit the machine-readable JSON result / error instead of human text.
    #[arg(long)]
    json: bool,
}

/// `lnrentd backup` — COLD/OFFLINE backup of the stopped daemon's data dir.
#[derive(Args)]
struct BackupArgs {
    /// The STOPPED daemon's data dir to back up. Env: LNRENT_DATA_DIR; else the `data_dir` from
    /// `--config`/LNRENT_CONFIG; else ./data — resolved exactly as the daemon does.
    #[arg(long)]
    data_dir: Option<String>,
    /// A TOML/JSON config file to resolve `data_dir` from (lower precedence than `--data-dir`/env),
    /// so a `data_dir` set only in the daemon's config file targets the right dir. Env: LNRENT_CONFIG.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Destination directory for the backup set. Must be empty or not-yet-exist, and OUTSIDE the data
    /// dir.
    #[arg(long)]
    dest: PathBuf,
    /// Emit the machine-readable JSON summary / error instead of human text.
    #[arg(long)]
    json: bool,
}

/// `lnrentd restore` — restore a backup set into a data dir.
#[derive(Args)]
struct RestoreArgs {
    /// The data dir to restore INTO (created if absent). Env: LNRENT_DATA_DIR; else the `data_dir`
    /// from `--config`/LNRENT_CONFIG; else ./data — resolved exactly as the daemon does.
    #[arg(long)]
    data_dir: Option<String>,
    /// A TOML/JSON config file to resolve `data_dir` from (lower precedence than `--data-dir`/env),
    /// so a restore targets the SAME dir the daemon would open. Env: LNRENT_CONFIG.
    #[arg(long)]
    config: Option<PathBuf>,
    /// The backup directory produced by `lnrentd backup`.
    #[arg(long = "from")]
    from: PathBuf,
    /// Overwrite a non-empty target data dir (default: refuse, to avoid clobbering live state).
    #[arg(long)]
    force: bool,
    /// Emit the machine-readable JSON summary / error instead of human text.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Command::Bootstrap(args)) => run_bootstrap(args).await,
        Some(Command::Backup(args)) => run_backup(args),
        Some(Command::Restore(args)) => run_restore(args),
        None => match run_daemon().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("lnrentd: {e:#}");
                ExitCode::FAILURE
            }
        },
    }
}

/// The long-running daemon (lnrent-7fp.21): bootstrap the operator identity + config, open state
/// ONCE, connect the Nostr engine, load the operator's recipe, and run the supervised M1a money path
/// (IPC + Nostr inbound + settlement→capture + reconcile + maintenance) until a Ctrl-C / SIGTERM
/// triggers a graceful shutdown.
async fn run_daemon() -> Result<()> {
    tracing_subscriber::fmt::init();

    // With the `fedimint` feature the dependency tree has BOTH rustls providers (aws-lc-rs from
    // fedimint, ring from nostr), so rustls cannot auto-pick one — install aws-lc-rs as the process
    // default BEFORE any TLS (the federation connection + the Nostr wss relays). Idempotent.
    #[cfg(feature = "fedimint")]
    fedimint_core::rustls::install_crypto_provider().await;

    // Bootstrap is idempotent on a re-run (reads back the persisted seed); it opens the state DB
    // ONCE and hands back the shared store handle (no double open).
    let input = BootstrapInput {
        flags: RawConfig::default(),
        config_path: None,
        read_stdin: false,
    };
    let mut raw = config::load_raw_config(input)
        .map_err(|e| anyhow::anyhow!("operator bootstrap failed: {} ({})", e.message, e.code))?;
    // Without the `fedimint` feature, FedimintPayment isn't compiled — reject `fedimint` BEFORE
    // bootstrap persists the operator row/seed (committing a `fedimint` row + `fedimint.json` would
    // brick a later `mock` retry, since the federation invite is never silently repointed). WITH the
    // feature, fedimint is a supported backend (lnrent-o6p) and is allowed to bootstrap.
    #[cfg(not(feature = "fedimint"))]
    if config::resolved_payment_backend(&raw)
        .map_err(|e| anyhow::anyhow!("operator bootstrap failed: {} ({})", e.message, e.code))?
        == PaymentMode::Fedimint
    {
        anyhow::bail!(
            "payment_backend=fedimint requires building lnrentd with --features fedimint; \
             bootstrap with payment_backend=mock"
        );
    }
    let (operator, store) = config::bootstrap_headless_with_store(std::mem::take(&mut *raw))
        .await
        .map_err(|e| anyhow::anyhow!("operator bootstrap failed: {} ({})", e.message, e.code))?;
    tracing::info!(
        operator = %operator.identity.npub(),
        "lnrentd state opened; store actor up (sole writer); operator identity ready"
    );

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    // Select the payment backend. `mock` (the default) keeps an internal clock the supervisor syncs to
    // SystemClock (`set_now` is mock-only, not on the trait) + is seeded NOW so the first invoice (before
    // the first maintenance tick) stamps a live expiry rather than a 1970 one. `fedimint` (lnrent-o6p,
    // behind `--features fedimint`) joins the configured federation; it uses real time -> NO clock-sync.
    let (payment, mock_clock_sync): (Arc<dyn PaymentBackend>, Option<Arc<MockPayment>>) =
        match operator.config.payment_backend {
            PaymentMode::Mock => {
                let mock = Arc::new(MockPayment::new());
                mock.set_now(clock.now());
                (mock.clone(), Some(mock))
            }
            PaymentMode::Fedimint => {
                let backend = build_fedimint_backend(&operator, clock.clone()).await?;
                (backend, None)
            }
        };

    // The operator's recipe (M1a single-recipe): only a recipe that PASSES validation is served.
    let recipes_dir = std::env::var("LNRENT_RECIPES_DIR").unwrap_or_else(|_| "./recipes".into());
    let recipe = load_operator_recipe(&recipes_dir)?;
    tracing::info!(recipe = %recipe.service.id, "operator recipe loaded (validated)");

    // Connect the Nostr engine with the operator account-0 key + configured relays.
    let engine = NostrEngine::connect(
        operator.identity.keys().clone(),
        &operator.config.relays,
        store.clone(),
    )
    .await
    .context("connecting the operator Nostr engine")?;

    let sock = operator.config.data_dir.join("lnrent.sock");
    let mut supervisor = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        recipe,
        sock,
        Intervals::production(),
    )
    .await?;
    // The mock backend's internal clock is kept synced to SystemClock; the real Fedimint backend uses
    // real time and needs no sync.
    if let Some(mock) = mock_clock_sync {
        supervisor = supervisor.with_payment_clock_sync(move |now| mock.set_now(now));
    }
    let running = supervisor.start().await?;

    // Run until a termination signal, then shut down gracefully (drain in-flight + flush outbox).
    wait_for_term_signal().await;
    tracing::info!("lnrentd: termination signal received; shutting down");
    running.shutdown().await
}

/// Construct the real Fedimint payment backend for `payment_backend=fedimint` (lnrent-o6p go-live):
/// join the configured federation with the operator's deterministic root secret and honor the
/// configured gateway. Compiled only with `--features fedimint`; the non-feature build rejects
/// `fedimint` at bootstrap, so its variant just fails clearly if ever reached.
#[cfg(feature = "fedimint")]
async fn build_fedimint_backend(
    operator: &config::Operator,
    clock: Arc<dyn Clock>,
) -> Result<Arc<dyn PaymentBackend>> {
    let fedi = operator
        .config
        .fedimint
        .as_ref()
        .context("payment_backend=fedimint requires a [fedimint] config (invite + gateway)")?;
    let backend = FedimintPayment::join_or_open(
        &fedi.invite,
        &operator.config.data_dir,
        operator.identity.fedimint_root_secret(),
        Some(&fedi.gateway),
        clock,
    )
    .await
    .context("joining the configured Fedimint federation")?;
    tracing::info!("fedimint payment backend joined; real ecash money path active");
    Ok(Arc::new(backend))
}

#[cfg(not(feature = "fedimint"))]
async fn build_fedimint_backend(
    _operator: &config::Operator,
    _clock: Arc<dyn Clock>,
) -> Result<Arc<dyn PaymentBackend>> {
    anyhow::bail!("payment_backend=fedimint requires building lnrentd with --features fedimint")
}

/// Load + validate the operator's recipe(s) and return the single one M1a serves (lowest id wins,
/// for determinism). Fails clearly when no recipe validates.
fn load_operator_recipe(recipes_dir: &str) -> Result<Recipe> {
    let mut recipes: Vec<Recipe> = Recipe::load_all(recipes_dir)
        .with_context(|| format!("loading recipes from {recipes_dir}"))?
        .into_iter()
        .filter(|r| match r.validate() {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(id = %r.service.id, error = %e, "recipe failed validation — DISABLED");
                false
            }
        })
        .collect();
    recipes.sort_by(|a, b| a.service.id.cmp(&b.service.id));
    let mut iter = recipes.into_iter();
    let recipe = iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("no valid recipe found in {recipes_dir}"))?;
    let extra = iter.len();
    if extra > 0 {
        tracing::warn!(chosen = %recipe.service.id, ignored = extra, "M1a serves a single recipe; ignoring the rest");
    }
    Ok(recipe)
}

/// Resolve on Ctrl-C or SIGTERM — the graceful-shutdown trigger (the daemon is Unix-only: it owns a
/// Unix-domain IPC socket).
async fn wait_for_term_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot install SIGTERM handler; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// The headless `lnrentd bootstrap` entrypoint: merge the four sources, run the bootstrap, and emit
/// a structured result/error. Never prompts; always a deterministic exit code (ADR-0014, §4.7).
async fn run_bootstrap(args: BootstrapArgs) -> ExitCode {
    let json = args.json;
    // The seed on the `--mnemonic` flag lands in the process table (`/proc/<pid>/cmdline`, `ps`),
    // readable by other local users. The `--help` text says so, but that is invisible at the moment
    // of misuse — emit a runtime warning to stderr steering the operator to LNRENT_MNEMONIC / a
    // config file / stdin instead. SUPPRESSED under `--json`: a failing `--json` bootstrap writes
    // its structured `{code,message,retryable}` error to STDERR (stdout stays empty on error), and
    // machine callers parse that stderr as a SINGLE JSON document (see
    // tests/bootstrap_cli.rs::json_error_with_mnemonic_flag_is_single_json_document) — a free-text
    // warning line ahead of it would corrupt that parse. `--help` remains the nudge on the `--json`
    // path (review R2 P3: the warning is stderr-bound, but so is the `--json` error channel).
    if args.mnemonic.is_some() && !json {
        eprintln!(
            "lnrentd bootstrap: warning: the seed passed via --mnemonic is visible in the process \
             table to other local users; prefer LNRENT_MNEMONIC, a config file, or --stdin"
        );
    }
    let flags = RawConfig {
        data_dir: args.data_dir,
        relays: if args.relays.is_empty() {
            None
        } else {
            Some(args.relays)
        },
        payment_backend: args.payment_backend,
        compute_backend: args.compute_backend,
        fedimint_invite: args.fedimint_invite,
        fedimint_gateway: args.fedimint_gateway,
        mnemonic: args.mnemonic,
    };
    // Read stdin only when explicitly asked. Auto-reading every non-TTY stdin can block forever
    // when an orchestrator launches us with an inherited open pipe even though flags/env/file
    // already supplied all config.
    let read_stdin = args.stdin;
    let input = BootstrapInput {
        flags,
        config_path: args.config,
        read_stdin,
    };

    let result = match config::load_raw_config(input) {
        // `load_raw_config` returns the merged config in a `Zeroizing` guard (its plaintext mnemonic
        // is wiped on drop); `mem::take` hands the real config to the headless bootstrap, leaving an
        // empty default to drop harmlessly.
        Ok(mut raw) => config::bootstrap_headless(std::mem::take(&mut *raw)).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(op) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "data": { "npub": op.identity.npub(), "pubkey": op.identity.pubkey_hex() }
                    })
                );
            } else {
                println!(
                    "operator bootstrapped: {} ({})",
                    op.identity.npub(),
                    op.identity.pubkey_hex()
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_bootstrap_error(&e, json),
    }
}

/// Resolve the operator data dir for the backup/restore CLIs the SAME way the daemon does — routed
/// through `config::load_raw_config` so the precedence is byte-identical: an explicit `--data-dir`
/// flag (highest), else `LNRENT_DATA_DIR`, else a `data_dir` set in the config file (`--config` flag,
/// else `LNRENT_CONFIG`), else `./data`. Resolving the config-file layer matters on the money path:
/// an operator who sets `data_dir` only in their config file would otherwise get a backup that errors
/// ("no state DB at ./data") and a restore that silently populates the WRONG dir while leaving the
/// real one untouched (review R2 P1). Never reads stdin, so an inherited pipe can't block. A blank
/// value in any source is treated as unset.
fn resolve_data_dir_arg(
    flag: Option<String>,
    config_path: Option<PathBuf>,
) -> Result<PathBuf, IpcError> {
    let input = BootstrapInput {
        flags: RawConfig {
            data_dir: flag,
            ..RawConfig::default()
        },
        config_path,
        read_stdin: false,
    };
    let raw = config::load_raw_config(input)?;
    // Mirror the daemon's default (config::DEFAULT_DATA_DIR == "./data"); the OS resolves `./data`
    // and a normalized `data` to the same directory, so lexical normalization is unnecessary here —
    // what matters is targeting the same dir the daemon would.
    Ok(PathBuf::from(
        raw.data_dir
            .clone()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "./data".to_string()),
    ))
}

/// The headless `lnrentd backup` entrypoint (lnrent-7fp.14): COLD-copy the stopped daemon's durable
/// state into a fresh dir. Refuses if a daemon's IPC socket is live (cold/offline only). Emits a
/// structured `{ok, data|error}` summary; deterministic exit (0 success, nonzero on error).
fn run_backup(args: BackupArgs) -> ExitCode {
    let json = args.json;
    let data_dir = match resolve_data_dir_arg(args.data_dir, args.config) {
        Ok(dir) => dir,
        Err(e) => {
            return emit_op_error(json, "config_error", format!("{} ({})", e.message, e.code))
        }
    };
    // Cold/offline only: a live IPC socket means a daemon is writing this data dir RIGHT NOW, so a
    // copy of the open stores could be torn. Refuse rather than capture an inconsistent backup.
    if backup::daemon_appears_running(&data_dir) {
        return emit_op_error(
            json,
            "daemon_running",
            format!(
                "a daemon appears to be running against {} (its IPC socket {} is live); stop it \
                 first — backup is COLD/OFFLINE only",
                data_dir.display(),
                data_dir.join("lnrent.sock").display()
            ),
        );
    }
    match backup::backup(&data_dir, &args.dest) {
        Ok(m) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "data": {
                            "data_dir": data_dir.display().to_string(),
                            "dest": args.dest.display().to_string(),
                            "state_db": m.state_db,
                            "fedimint_dir": m.fedimint_dir,
                            "fedimint_config": m.fedimint_config,
                            "operator_seed": m.operator_seed,
                            "federations": m.federations,
                        }
                    })
                );
            } else {
                println!(
                    "backup complete: {} -> {}",
                    data_dir.display(),
                    args.dest.display()
                );
                println!("  state db:        yes");
                println!("  fedimint dir:    {}", yes_no(m.fedimint_dir));
                println!("  fedimint config: {}", yes_no(m.fedimint_config));
                println!("  operator seed:   {}", yes_no(m.operator_seed));
                if !m.federations.is_empty() {
                    println!("  federations:     {}", m.federations.join(", "));
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_op_error(json, "backup_failed", format!("{e:#}")),
    }
}

/// The headless `lnrentd restore` entrypoint (lnrent-7fp.14): install a backup set into a data dir.
/// Refuses a non-empty target without `--force`. Emits a structured `{ok, data|error}` summary;
/// deterministic exit (0 success, nonzero on error).
fn run_restore(args: RestoreArgs) -> ExitCode {
    let json = args.json;
    let data_dir = match resolve_data_dir_arg(args.data_dir, args.config) {
        Ok(dir) => dir,
        Err(e) => {
            return emit_op_error(json, "config_error", format!("{} ({})", e.message, e.code))
        }
    };
    // A live daemon on the TARGET would race the restore (and then run on half-overwritten state);
    // refuse the same way backup does.
    if backup::daemon_appears_running(&data_dir) {
        return emit_op_error(
            json,
            "daemon_running",
            format!(
                "a daemon appears to be running against the restore target {} (its IPC socket {} \
                 is live); stop it before restoring",
                data_dir.display(),
                data_dir.join("lnrent.sock").display()
            ),
        );
    }
    match backup::restore(&args.from, &data_dir, args.force) {
        Ok(m) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "data": {
                            "from": args.from.display().to_string(),
                            "data_dir": data_dir.display().to_string(),
                            "state_db": m.state_db,
                            "fedimint_dir": m.fedimint_dir,
                            "fedimint_config": m.fedimint_config,
                            "operator_seed": m.operator_seed,
                            "federations": m.federations,
                        }
                    })
                );
            } else {
                println!(
                    "restore complete: {} -> {}",
                    args.from.display(),
                    data_dir.display()
                );
                println!("  state db:        yes");
                println!("  fedimint dir:    {}", yes_no(m.fedimint_dir));
                println!("  fedimint config: {}", yes_no(m.fedimint_config));
                println!("  operator seed:   {}", yes_no(m.operator_seed));
                if !m.federations.is_empty() {
                    println!("  federations:     {}", m.federations.join(", "));
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => emit_op_error(json, "restore_failed", format!("{e:#}")),
    }
}

fn yes_no(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

/// Print a backup/restore failure as a structured `{ok:false, error:{code, message}}` to STDERR (so
/// `--json` stdout stays clean) and return a nonzero exit. Mirrors the bootstrap error channel.
fn emit_op_error(json: bool, code: &str, message: String) -> ExitCode {
    if json {
        eprintln!(
            "{}",
            serde_json::json!({ "ok": false, "error": { "code": code, "message": message } })
        );
    } else {
        eprintln!("lnrentd: {message} ({code})");
    }
    ExitCode::FAILURE
}

/// Print a bootstrap failure as the structured `{code, message, retryable}` error (ADR-0014 §4.7) to
/// STDERR (so `--json` stdout stays clean) and map its code to a deterministic nonzero exit.
fn emit_bootstrap_error(err: &IpcError, json: bool) -> ExitCode {
    if json {
        eprintln!(
            "{}",
            serde_json::json!({
                "ok": false,
                "error": { "code": err.code, "message": err.message, "retryable": err.retryable }
            })
        );
    } else {
        eprintln!(
            "lnrentd bootstrap: {} ({}{})",
            err.message,
            err.code,
            if err.retryable { ", retryable" } else { "" }
        );
    }
    ExitCode::from(config::exit_code(&err.code))
}
