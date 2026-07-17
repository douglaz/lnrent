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
use lnrentd::alerts::AlertDispatcher;
use lnrentd::backends::{MockPayment, PaymentBackend};
use lnrentd::backup;
use lnrentd::clock::{Clock, SystemClock};
use lnrentd::config::{self, BootstrapInput, PaymentMode, RawConfig};
// lnv1 `fedimint_backend::FedimintPayment` stays in-tree but UNSELECTED (dormant, deleted by
// lnrent-8ym); `payment_backend=fedimint` now builds the lnv2 backend below.
#[cfg(feature = "fedimint")]
use lnrentd::lnv2_backend::Lnv2Payment;
use lnrentd::ipc::IpcError;
use lnrentd::nostr_engine::NostrEngine;
use lnrentd::recipe::Recipe;
use lnrentd::refund_resolver::Resolver;
use lnrentd::supervisor::{Intervals, Supervisor};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use zeroize::Zeroizing;

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
    /// OPTIONAL: encrypt the sensitive set (seed + ecash + state DB) under the passphrase read from
    /// this FILE (a single trailing newline is trimmed), producing `dest/backup.age` beside a
    /// plaintext `MANIFEST.json` instead of the flat plaintext files (lnrent-y4m.6). A path — NOT the
    /// passphrase itself — so it never lands in the process table (`ps`). Absent → today's plaintext
    /// backup.
    #[arg(long)]
    passphrase_file: Option<PathBuf>,
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
    /// The passphrase FILE for an ENCRYPTED backup (`backup --passphrase-file`), read the same way (a
    /// single trailing newline trimmed) — required when the backup's `MANIFEST.json` says `encrypted`
    /// (lnrent-y4m.6). A path — NOT the passphrase itself. Absent on an encrypted backup → a clear
    /// error; supplied for a manifest claiming plaintext → a downgrade-safety error rather than being
    /// silently ignored.
    #[arg(long)]
    passphrase_file: Option<PathBuf>,
    /// Emit the machine-readable JSON summary / error instead of human text.
    #[arg(long)]
    json: bool,
}

/// Synchronous entrypoint (lnrent-y4m.7): each env-consuming path loads its config — which reads
/// and CONSUMES the seed/secret env vars into a zeroizing guard — then scrubs those vars from the
/// daemon's own env while STILL SINGLE-THREADED, and only THEN builds the tokio runtime. This
/// ordering is why `main` is not `#[tokio::main]`: `std::env::remove_var` must not race a
/// worker-thread `getenv`, and `#[tokio::main]` would spawn those workers before any `main`-body
/// code runs. Backup/Restore consume no env secrets and need no runtime.
fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Command::Bootstrap(args)) => run_bootstrap_entry(args),
        Some(Command::Backup(args)) => run_backup(args),
        Some(Command::Restore(args)) => run_restore(args),
        None => run_daemon_entry(),
    }
}

/// The bootstrap SECRET env vars scrubbed after the config load consumes them (lnrent-y4m.7). Mirror
/// of the `ENV_MNEMONIC` / `ENV_FEDIMINT_*` names in `config.rs`.
const SECRET_ENV_VARS: &[&str] = &[
    "LNRENT_MNEMONIC",
    "LNRENT_FEDIMINT_INVITE",
    "LNRENT_FEDIMINT_GATEWAY",
];

/// Remove the bootstrap secrets from the daemon's own process env, AFTER the synchronous config load
/// has consumed them and BEFORE the tokio runtime spawns worker threads (so this `remove_var` cannot
/// race a concurrent `getenv`). Defense-in-depth (lnrent-y4m.7): it does NOT overwrite the
/// kernel-placed initial env block, so `/proc/self/environ` may still show these until the operator
/// launches via systemd-credential/stdin — but the load-bearing guarantee is `run_hook`'s
/// `.env_clear()`, which means no hook ever receives them regardless.
fn scrub_secret_env() {
    for name in SECRET_ENV_VARS {
        std::env::remove_var(name);
    }
}

/// Take an exclusive advisory `flock` on `{data_dir}/lnrentd.lock` (lnrent-urw.9): the single-
/// instance guard. Returns the held file handle — keep it alive for the daemon's lifetime; dropping
/// it (or the process dying) releases the lock. Fails loudly with a "daemon already running" error
/// when another daemon holds it, rather than silently rebinding the IPC socket + double-driving.
fn acquire_data_dir_lock(data_dir: &std::path::Path) -> Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    let path = data_dir.join("lnrentd.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // the lock file's CONTENT is irrelevant; never clobber it
        .open(&path)
        .with_context(|| format!("opening single-instance lock {}", path.display()))?;
    // LOCK_EX | LOCK_NB: fail IMMEDIATELY if another daemon holds it, never block.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(file);
    }
    let err = std::io::Error::last_os_error();
    if matches!(err.raw_os_error(), Some(libc::EWOULDBLOCK)) {
        anyhow::bail!(
            "another lnrentd is already running on data dir {} (holds {}); refusing to start a second instance",
            data_dir.display(),
            path.display()
        );
    }
    Err(anyhow::Error::new(err).context(format!("locking {}", path.display())))
}

/// Build the multi-thread tokio runtime explicitly (see [`main`]); an error is fatal.
fn build_runtime() -> Result<tokio::runtime::Runtime, ExitCode> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            eprintln!("lnrentd: failed to build the async runtime: {e}");
            ExitCode::FAILURE
        })
}

/// Daemon-run path: load config (reads env), scrub secrets single-threaded, build the runtime, run.
fn run_daemon_entry() -> ExitCode {
    let input = BootstrapInput {
        flags: RawConfig::default(),
        config_path: None,
        read_stdin: false,
    };
    let raw = match config::load_raw_config(input) {
        Ok(raw) => raw,
        Err(e) => {
            eprintln!(
                "lnrentd: operator bootstrap failed: {} ({})",
                e.message, e.code
            );
            return ExitCode::FAILURE;
        }
    };
    scrub_secret_env();
    let rt = match build_runtime() {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    match rt.block_on(run_daemon(raw)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("lnrentd: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// Bootstrap path: build the input from flags, load config (reads env), scrub, build runtime, run.
fn run_bootstrap_entry(args: BootstrapArgs) -> ExitCode {
    let json = args.json;
    // The seed on the `--mnemonic` flag lands in the process table (`/proc/<pid>/cmdline`, `ps`),
    // readable by other local users. Emit a runtime warning steering the operator to
    // LNRENT_MNEMONIC / a config file / stdin. SUPPRESSED under `--json` (a `--json` error is a
    // single JSON document on stderr; a free-text line ahead of it would corrupt that parse).
    if args.mnemonic.is_some() && !json {
        eprintln!(
            "lnrentd bootstrap: warning: the seed passed via --mnemonic is visible in the process \
             table to other local users; prefer LNRENT_MNEMONIC, a config file, or --stdin"
        );
    }
    let raw = match config::load_raw_config(bootstrap_input(args)) {
        Ok(raw) => raw,
        Err(e) => return emit_bootstrap_error(&e, json),
    };
    scrub_secret_env();
    let rt = match build_runtime() {
        Ok(rt) => rt,
        Err(code) => return code,
    };
    rt.block_on(run_bootstrap(json, raw))
}

/// The [`BootstrapInput`] for the bootstrap CLI (flags from args + `--config` + `--stdin`).
fn bootstrap_input(args: BootstrapArgs) -> BootstrapInput {
    let flags = RawConfig {
        data_dir: args.data_dir,
        relays: if args.relays.is_empty() {
            None
        } else {
            Some(args.relays)
        },
        payment_backend: args.payment_backend,
        // CUT-3: no CLI knob for the dead `compute_backend`; env (LNRENT_COMPUTE_BACKEND) and file
        // layers still parse for back-compat, but a supplied value is ignored with a warning.
        compute_backend: None,
        fedimint_invite: args.fedimint_invite,
        fedimint_gateway: args.fedimint_gateway,
        // No bootstrap CLI flag for the failover fallback list (lnrent-y4m.8): gateway fallbacks are
        // supplied via the config file / stdin JSON `fedimint_gateway_fallbacks` array (or already
        // durable in fedimint.json). The single-gateway `--fedimint-gateway` flag is unchanged.
        fedimint_gateway_fallbacks: None,
        mnemonic: args.mnemonic,
        // No bootstrap CLI flag for the draining-holdings floor (lnrent-urw.7); it is a runtime
        // warning-condition knob read from env (LNRENT_MIN_HOLDINGS_WARN_MSAT) / the config file.
        min_holdings_warn_msat: None,
    };
    // Read stdin only when explicitly asked. Auto-reading every non-TTY stdin can block forever when
    // an orchestrator launches us with an inherited open pipe even though flags/env/file already
    // supplied all config.
    BootstrapInput {
        flags,
        config_path: args.config,
        read_stdin: args.stdin,
    }
}

/// Log output encoding (lnrent-y4m.11). `Text` is the default plaintext formatter; `Json` is the
/// opt-in structured formatter for downstream log processors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

/// The `LNRENT_LOG_FORMAT` env var this daemon reads to select the log encoding.
const LOG_FORMAT_ENV: &str = "LNRENT_LOG_FORMAT";

/// Map an `LNRENT_LOG_FORMAT` value to a [`LogFormat`]: only a case-insensitive `"json"` selects
/// JSON; unset, `"text"`, or ANY unrecognized value falls back to `Text` (the safe default —
/// a typo must never silently disable logging or change today's behavior). Pure over its input so
/// the mapping is unit-testable without touching the process-global subscriber.
fn parse_log_format(value: Option<&str>) -> LogFormat {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

fn log_format_from_env() -> LogFormat {
    parse_log_format(std::env::var(LOG_FORMAT_ENV).ok().as_deref())
}

/// The long-running daemon (lnrent-7fp.21): bootstrap the operator identity + config, open state
/// ONCE, connect the Nostr engine, load the operator's recipe, and run the supervised M1a money path
/// (IPC + Nostr inbound + settlement→capture + reconcile + maintenance) until a Ctrl-C / SIGTERM
/// triggers a graceful shutdown.
async fn run_daemon(mut raw: Zeroizing<RawConfig>) -> Result<()> {
    // Opt-in structured logging (lnrent-y4m.11): `LNRENT_LOG_FORMAT=json` initializes the JSON
    // formatter so a stranger-operator can ship the money-event structured fields to Loki/ELK for
    // log-based alerting; unset/anything else stays TODAY'S plaintext (the text branch is the
    // literal, byte-for-byte-unchanged `fmt::init()`). Encoding ONLY — same fields, same level
    // filtering, no RUST_LOG change. JSON logs are a pull/scrape path, NOT a substitute for the
    // push alert sink (PR-5).
    match log_format_from_env() {
        LogFormat::Json => tracing_subscriber::fmt().json().init(),
        LogFormat::Text => tracing_subscriber::fmt::init(),
    }

    // With the `fedimint` feature the dependency tree has BOTH rustls providers (aws-lc-rs from
    // fedimint, ring from nostr), so rustls cannot auto-pick one — install aws-lc-rs as the process
    // default BEFORE any TLS (the federation connection + the Nostr wss relays). Idempotent.
    #[cfg(feature = "fedimint")]
    fedimint_core::rustls::install_crypto_provider().await;

    // Config was loaded + the seed/secret env vars scrubbed by the synchronous entrypoint
    // (lnrent-y4m.7) before this runtime existed. Bootstrap is idempotent on a re-run (reads back
    // the persisted seed); it opens the state DB ONCE and hands back the shared store handle.
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
    // Single-instance guard (lnrent-urw.9): resolve + create the data dir and take an exclusive
    // advisory lock on `{data_dir}/lnrentd.lock` BEFORE bootstrap opens sqlite / runs migrations /
    // writes the operator row (codex) — so a systemd restart racing a manual start fails fast
    // WITHOUT mutating any state or rebinding the IPC socket, rather than double-driving
    // subscriptions into duplicate droplets (ADR-0001 sole-writer). Held for the daemon's whole
    // lifetime — the kernel frees the flock on clean exit OR crash, so a later start always succeeds.
    let data_dir = config::prepare_data_dir(&raw)
        .map_err(|e| anyhow::anyhow!("preparing data dir: {} ({})", e.message, e.code))?;
    let _instance_lock = acquire_data_dir_lock(&data_dir)?;

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

    // Connect the Nostr engine with the operator account-0 key + configured relays, then apply the
    // operator-tuned per-pubkey inbound rate-limit knobs (GATE-0 PR-2).
    let engine = NostrEngine::connect(
        operator.identity.keys().clone(),
        &operator.config.relays,
        store.clone(),
    )
    .await
    .context("connecting the operator Nostr engine")?
    .with_inbound_rate_limit(
        lnrentd::config::inbound_rate_capacity(),
        lnrentd::config::inbound_rate_refill_per_min(),
    );

    // GATE-1 alert sink (lnrent-urw.1, PR-5): resolve the recipient — the operator's personal
    // `alert_npub` if set, else a self-DM to the operator key — and honor the enabled default (on
    // for the fedimint money path, off for mock). Built before `store`/`clock` are moved into the
    // supervisor. A malformed `LNRENT_ALERT_NPUB` fails startup loudly rather than silently muting.
    let alerts = if lnrentd::config::alerts_enabled(operator.config.payment_backend) {
        let recipient_hex = match lnrentd::config::alert_npub() {
            Some(npub) => lnrent_wire::PublicKey::parse(&npub)
                .with_context(|| format!("parsing LNRENT_ALERT_NPUB `{npub}`"))?
                .to_hex(),
            None => operator.identity.public_key().to_hex(),
        };
        tracing::info!(recipient = %recipient_hex, "operator alert sink enabled (GATE-1 PR-5)");
        Arc::new(AlertDispatcher::new(
            store.clone(),
            clock.clone(),
            recipient_hex,
        ))
    } else {
        Arc::new(AlertDispatcher::disabled(store.clone(), clock.clone()))
    };

    let sock = operator.config.data_dir.join("lnrent.sock");
    let mut supervisor = Supervisor::build(
        store,
        engine,
        payment,
        clock,
        Arc::new(Resolver::new()),
        alerts,
        recipe,
        sock,
        Intervals::production(),
        lnrentd::config::max_live_holds_per_buyer(),
    )
    .await?
    // GATE-1 draining-holdings warning (lnrent-urw.7): opt in with the operator-configured float
    // floor (`0` = disabled). A builder, mirroring `with_payment_clock_sync`, so `build`'s signature
    // stays fixed for the integration-test callers.
    .with_holdings_floor(operator.config.min_holdings_warn_msat);
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

/// Construct the real Fedimint payment backend for `payment_backend=fedimint` (lnrent-3d5, ADR-0018):
/// the **lnv2** backend ([`Lnv2Payment`]) — the live ecash money path. The lnv1
/// [`lnrentd::fedimint_backend::FedimintPayment`] stays in-tree but UNSELECTED (dormant) until
/// lnrent-8ym deletes it. lnv2 selects gateways natively (by API url), so the configured
/// `[fedimint] gateway` pubkey (an lnv1-era selector) is not consulted here. Compiled only with
/// `--features fedimint`; the non-feature build rejects `fedimint` at
/// bootstrap, so its variant just fails clearly if ever reached.
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
    let backend = Lnv2Payment::join_or_open(
        &fedi.invite,
        &operator.config.data_dir,
        operator.identity.fedimint_root_secret(),
        clock,
    )
    .await
    .context("joining the configured lnv2 Fedimint federation")?;
    tracing::info!("lnv2 fedimint payment backend joined; real ecash money path active");
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

/// The headless `lnrentd bootstrap` entrypoint: run the bootstrap over the pre-loaded config and
/// emit a structured result/error. Never prompts; always a deterministic exit code (ADR-0014, §4.7).
/// Config load + secret-env scrub happened synchronously in [`run_bootstrap_entry`] before the
/// runtime; here `raw` already holds the merged config in a zeroizing guard.
async fn run_bootstrap(json: bool, mut raw: Zeroizing<RawConfig>) -> ExitCode {
    // `mem::take` hands the real config to the headless bootstrap, leaving an empty default to drop
    // harmlessly; the plaintext mnemonic is wiped on drop either way.
    let result = config::bootstrap_headless(std::mem::take(&mut *raw)).await;
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

/// Read the backup passphrase from `path` into a zeroizing buffer, trimming a SINGLE trailing newline
/// (`\n`, or a `\r\n` pair). Passing a FILE (not the passphrase) keeps the secret off argv, where `ps`
/// would expose it; the buffer is a [`Zeroizing<String>`] so it wipes on drop and is never logged. An
/// empty passphrase (empty file, or a file holding only the newline) is refused — age would accept it
/// but it offers ZERO protection for the fund-controlling seed the encrypted mode exists to protect,
/// so it is almost always an operator mistake (lnrent-y4m.6).
fn read_passphrase_file(path: &std::path::Path) -> Result<Zeroizing<String>> {
    let raw = Zeroizing::new(
        std::fs::read_to_string(path)
            .with_context(|| format!("reading passphrase file {}", path.display()))?,
    );
    let trimmed = raw
        .strip_suffix('\n')
        .map(|s| s.strip_suffix('\r').unwrap_or(s))
        .unwrap_or(raw.as_str());
    if trimmed.trim().is_empty() {
        anyhow::bail!(
            "passphrase file {} is empty or whitespace-only; that gives no protection",
            path.display()
        );
    }
    Ok(Zeroizing::new(trimmed.to_owned()))
}

/// The headless `lnrentd backup` entrypoint (lnrent-7fp.14): COLD-copy the stopped daemon's durable
/// state into a fresh dir. Refuses if a daemon's IPC socket is live (cold/offline only). With
/// `--passphrase-file` the sensitive set is age-encrypted into `dest/backup.age` (lnrent-y4m.6). Emits
/// a structured `{ok, data|error}` summary; deterministic exit (0 success, nonzero on error).
fn run_backup(args: BackupArgs) -> ExitCode {
    let json = args.json;
    let data_dir = match resolve_data_dir_arg(args.data_dir, args.config) {
        Ok(dir) => dir,
        Err(e) => {
            return emit_op_error(json, "config_error", format!("{} ({})", e.message, e.code))
        }
    };
    // The OPTIONAL passphrase — read from a file so it never hits argv. Absent -> plaintext mode.
    let passphrase = match args
        .passphrase_file
        .as_deref()
        .map(read_passphrase_file)
        .transpose()
    {
        Ok(pw) => pw,
        Err(e) => return emit_op_error(json, "passphrase_error", format!("{e:#}")),
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
    match backup::backup(&data_dir, &args.dest, passphrase) {
        Ok(m) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "data": {
                            "data_dir": data_dir.display().to_string(),
                            "dest": args.dest.display().to_string(),
                            "encrypted": m.encrypted,
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
                println!("  encrypted:       {}", yes_no(m.encrypted));
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
    // The OPTIONAL passphrase for an ENCRYPTED backup — read from a file (never argv). Absent here and
    // an encrypted manifest -> `restore` bails with a clear "pass --passphrase-file" error.
    let passphrase = match args
        .passphrase_file
        .as_deref()
        .map(read_passphrase_file)
        .transpose()
    {
        Ok(pw) => pw,
        Err(e) => return emit_op_error(json, "passphrase_error", format!("{e:#}")),
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
    match backup::restore(&args.from, &data_dir, args.force, passphrase) {
        Ok(m) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ok": true,
                        "data": {
                            "from": args.from.display().to_string(),
                            "data_dir": data_dir.display().to_string(),
                            "encrypted": m.encrypted,
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
                println!("  encrypted:       {}", yes_no(m.encrypted));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Process-global env mutation must be serialized across tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // lnrent-y4m.11: the log-format selector. Only a case-insensitive "json" opts into JSON; unset,
    // "text", and any typo fall back to Text so a bad value never silently changes logging.
    #[test]
    fn log_format_selector_defaults_to_text_and_only_json_opts_in() {
        assert_eq!(parse_log_format(None), LogFormat::Text);
        assert_eq!(parse_log_format(Some("text")), LogFormat::Text);
        assert_eq!(parse_log_format(Some("json")), LogFormat::Json);
        assert_eq!(parse_log_format(Some("JSON")), LogFormat::Json);
        assert_eq!(parse_log_format(Some("  json  ")), LogFormat::Json);
        assert_eq!(parse_log_format(Some("")), LogFormat::Text);
        assert_eq!(parse_log_format(Some("yaml")), LogFormat::Text, "an unknown value is safe text");
    }

    // lnrent-y4m.7 Part A: the single-threaded scrub removes the seed/secret env vars from the
    // daemon's own in-process environment, so `std::env::var` returns Err afterward. (We do NOT
    // assert on /proc/self/environ — `remove_var` cannot overwrite the kernel-placed initial env
    // block; that caveat is documented, and Part B's `.env_clear()` is the load-bearing guarantee.)
    #[test]
    fn scrub_removes_secret_env_vars() {
        let _g = ENV_LOCK.lock().unwrap();
        let prior: Vec<(&str, Option<String>)> = SECRET_ENV_VARS
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();

        for k in SECRET_ENV_VARS {
            std::env::set_var(k, "sensitive");
        }
        scrub_secret_env();
        let results: Vec<bool> = SECRET_ENV_VARS
            .iter()
            .map(|k| std::env::var(k).is_err())
            .collect();

        // Restore before asserting so a failure can't leak state into other tests.
        for (k, v) in prior {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
        assert!(
            results.iter().all(|&scrubbed| scrubbed),
            "every SECRET_ENV_VARS entry is scrubbed from the daemon env"
        );
    }

    // lnrent-urw.9: the data-dir lock is exclusive — a second acquire while the first is held is
    // refused, and releasing the first frees it for a subsequent start.
    #[test]
    fn data_dir_lock_is_exclusive_and_releases_on_drop() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-lock-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let first = acquire_data_dir_lock(&dir).expect("first daemon takes the lock");
        let second = acquire_data_dir_lock(&dir);
        assert!(second.is_err(), "a second daemon is refused while the first holds the lock");
        assert!(
            second.unwrap_err().to_string().contains("already running"),
            "the refusal is the structured 'daemon already running' error"
        );

        drop(first); // clean exit / crash releases the flock
        let _third = acquire_data_dir_lock(&dir).expect("lock is re-acquirable after release");

        std::fs::remove_dir_all(&dir).ok();
    }
}
