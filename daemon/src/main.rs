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
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::config::{self, BootstrapInput, PaymentMode, RawConfig};
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

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Command::Bootstrap(args)) => run_bootstrap(args).await,
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

    // Bootstrap is idempotent on a re-run (reads back the persisted seed); it opens the state DB
    // ONCE and hands back the shared store handle (no double open).
    let input = BootstrapInput {
        flags: RawConfig::default(),
        config_path: None,
        read_stdin: false,
    };
    let mut raw = config::load_raw_config(input)
        .map_err(|e| anyhow::anyhow!("operator bootstrap failed: {} ({})", e.message, e.code))?;
    // M1a is MockPayment only (FedimintPayment is a stub, backends.rs). Reject `fedimint` BEFORE
    // bootstrap persists the operator row/seed: committing a `fedimint` row + `fedimint.json` would
    // brick a later retry with `mock` (a `config_conflict`, since the federation invite is never
    // silently repointed). The post-bootstrap check below still guards the inherited case (a
    // re-bootstrap that omits the backend but already stored `fedimint`).
    if config::resolved_payment_backend(&raw)
        .map_err(|e| anyhow::anyhow!("operator bootstrap failed: {} ({})", e.message, e.code))?
        == PaymentMode::Fedimint
    {
        anyhow::bail!(
            "payment_backend=fedimint is not supported in M1a (FedimintPayment is a stub); \
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

    // M1a is MockPayment only — the Fedimint backend is a stub (lnrent-7fp.4, backends.rs). Fail
    // clearly rather than silently mis-route money.
    if operator.config.payment_backend == PaymentMode::Fedimint {
        anyhow::bail!(
            "payment_backend=fedimint is not supported in M1a (FedimintPayment is a stub); \
             bootstrap with payment_backend=mock"
        );
    }
    // Construct the CONCRETE MockPayment so the supervisor can keep its internal clock synced to the
    // SystemClock (`set_now` is mock-only, not on the PaymentBackend trait). Seed it now too, so the
    // very first invoice issued before the first maintenance tick already stamps a live expiry rather
    // than a 1970 one (which reconcile would instantly expire).
    let mock = Arc::new(MockPayment::new());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    mock.set_now(clock.now());
    let payment: Arc<dyn PaymentBackend> = mock.clone();

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
    let supervisor = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        recipe,
        sock,
        Intervals::production(),
    )
    .await?
    .with_payment_clock_sync(move |now| mock.set_now(now));
    let running = supervisor.start().await?;

    // Run until a termination signal, then shut down gracefully (drain in-flight + flush outbox).
    wait_for_term_signal().await;
    tracing::info!("lnrentd: termination signal received; shutting down");
    running.shutdown().await
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
