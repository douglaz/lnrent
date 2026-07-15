//! Real Fedimint `PaymentBackend` (lnrent-7fp.4, ADR-0012/0015) — ecash via an EXISTING federation
//! + gateway, replacing `MockPayment`. Feature-gated behind the `fedimint` cargo feature (default
//! OFF) so mock-only builds stay light and CI without a federation keeps passing. Daemon-only:
//! `fedimint-client` 0.11.1 + `fedimint-rocksdb` (a 2nd DB engine, bundled C++) are never compiled
//! into the wasm buyer.
//!
//! Status: `.4b` client construction DONE; `.4c.2` inbound receive (`create_invoice`/`watch`/
//! `lookup`) DONE; `.4c.3` outbound `pay`/`payment_status` DONE — the whole `PaymentBackend` trait
//! is implemented. `.4d` is the live `devimint` regtest integration test; `main.rs` still rejects
//! `payment_backend=fedimint` until that passes + the refund-dest resolver lands (codex review).
//!
//! Design (folding in the codex `.4` review):
//!  - **Gateway required** — `create_bolt11_invoice` with no gateway is internal-only; `.4c` selects
//!    one via `get_gateway(None, false)` (refreshes the cache + picks one, else errors).
//!  - **Idempotency on `external_id`** — an lnrent-owned sqlite index (NOT fedimint's rocksdb) is the
//!    anchor: `create_invoice` returns the stored invoice on a repeat. `extra_meta =
//!    {"lnrent_external_id": …}` is stamped into the fedimint operation so a boot oplog scan
//!    ([`recover_index_from_oplog`]) backfills the index after a crash between minting and
//!    persisting — closing the duplicate-mint window (codex finding #3).
//!  - **Settlement timestamp** — `watch()` streams each open invoice. A LIVE `Claimed` (observed
//!    while watching) pushes a `Settlement{settled_at = now}`. A CACHED terminal `Claimed` (already
//!    settled at subscribe time — e.g. while the daemon was down) only marks the index PAID and does
//!    NOT push: the supervisor settlement catch-up recovers it via `lookup()` with a SAFE capped
//!    timestamp, so a past settlement never over-credits `paid_through` (codex review).
//!  - **Root secret** — lnrent's 32-byte secret (`identity.rs`) is wrapped as a fedimint
//!    `DerivableSecret` (`new_root`) under `StandardDoubleDerive`, so the position is recoverable.

use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;
use tokio::sync::mpsc;

use fedimint_client::{Client, ClientHandleArc, OperationId, RootSecret};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::secp256k1::PublicKey;
use fedimint_core::Amount;
use fedimint_derive_secret::DerivableSecret;
use fedimint_ln_client::{
    InternalPayState, LightningClientInit, LightningClientModule, LightningOperationMeta,
    LightningOperationMetaVariant, LnPayState, LnReceiveState, PayType,
};
use fedimint_ln_common::lightning_invoice::{Bolt11Invoice, Bolt11InvoiceDescription, Description};
use fedimint_ln_common::LightningGateway;
use fedimint_mint_client::MintClientInit;
use fedimint_rocksdb::RocksDb;
use fedimint_wallet_client::WalletClientInit;

use crate::backends::{Invoice, PayStatus, PaymentBackend, PaymentStatus, RefundQuote, Settlement};
use crate::clock::Clock;

/// HKDF salt for wrapping lnrent's (provisional) 32-byte Fedimint root secret (`identity.rs`,
/// already domain-separated by `lnrent:fedimint:v1`) into a fedimint `DerivableSecret`. Fixed +
/// versioned so the derived client secret — and thus the ecash position — is deterministic and
/// recoverable from the operator seed (codex `.4b` note).
const ROOT_SECRET_SALT: &[u8] = b"lnrent:fedimint:client:v1";

/// The lnrent-owned sqlite index, per federation data-dir. The idempotency anchor for
/// `create_invoice`/`pay` (keyed by `external_id` / `idempotency_key`), separate from fedimint's
/// rocksdb (codex finding #3).
const INDEX_DB_FILE: &str = "lnrent_index.db";
const CLIENT_DB_DIR: &str = "client.db";

struct PreparedFedimintPaths {
    client_db: PathBuf,
    index_db: PathBuf,
}

/// Prepare + harden the lnrent-owned Fedimint paths BEFORE any RocksDB/sqlite open (spec F2). The
/// confidentiality boundary is the **0700 directories** (`fedimint/`, `<federation>/`, `client.db/`):
/// once they are owner-only, the note/wallet material inside is unreadable to co-tenant local users
/// regardless of the umask-derived perms RocksDB (SST/LOG) and sqlite (`-wal`/`-shm`) give their own
/// churned files — so the per-file 0600 on `lnrent_index.db`'s main file is belt-and-suspenders, not
/// the load-bearing control. Each path is symlink-refused (lstat + O_NOFOLLOW re-open, `fchmod` on the
/// fd so there is no chmod TOCTOU). NOTE: only each path's *final* component is O_NOFOLLOW-checked; an
/// attacker who could swap an intermediate component (e.g. `fedimint/`) for a symlink already has write
/// access to the operator's 0700 `data_dir` — i.e. is the service user/root — which is outside the
/// co-tenant threat model this closes.
fn prepare_fedimint_paths(data_dir: &Path, federation_id: &str) -> Result<PreparedFedimintPaths> {
    let fedimint_dir = data_dir.join("fedimint");
    prepare_private_dir(&fedimint_dir, "fedimint root dir")?;

    let federation_dir = fedimint_dir.join(federation_id);
    prepare_private_dir(&federation_dir, "fedimint federation dir")?;

    let client_db = federation_dir.join(CLIENT_DB_DIR);
    prepare_private_dir(&client_db, "fedimint client db dir")?;

    let index_db = federation_dir.join(INDEX_DB_FILE);
    prepare_private_file(&index_db, "fedimint lnrent index db")?;

    Ok(PreparedFedimintPaths {
        client_db,
        index_db,
    })
}

fn prepare_private_dir(path: &Path, what: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                anyhow::bail!("{what} {} must not be a symlink", path.display());
            }
            if !meta.file_type().is_dir() {
                anyhow::bail!("{what} {} must be a directory", path.display());
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            match fs::DirBuilder::new().mode(0o700).create(path) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(e).with_context(|| format!("creating {what} {}", path.display()))
                }
            }
        }
        Err(e) => return Err(e).with_context(|| format!("stat {what} {}", path.display())),
    }
    harden_private_dir(path, what)
}

fn harden_private_dir(path: &Path, what: &str) -> Result<()> {
    let handle = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(|e| {
            if matches!(e.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)) {
                anyhow!(
                    "{what} {} must be a real directory, not a symlink",
                    path.display()
                )
            } else {
                anyhow!("opening {what} {} to harden perms: {e}", path.display())
            }
        })?;
    let meta = handle
        .metadata()
        .with_context(|| format!("stat opened {what} {}", path.display()))?;
    if !meta.file_type().is_dir() {
        anyhow::bail!("{what} {} must be a directory", path.display());
    }
    handle
        .set_permissions(fs::Permissions::from_mode(0o700))
        .with_context(|| format!("perms on {what} {}", path.display()))
}

fn prepare_private_file(path: &Path, what: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                anyhow::bail!("{what} {} must not be a symlink", path.display());
            }
            if !meta.file_type().is_file() {
                anyhow::bail!("{what} {} must be a regular file", path.display());
            }
            harden_private_file(path, what)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
            {
                Ok(file) => file
                    .set_permissions(fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("perms on {what} {}", path.display())),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    let meta = fs::symlink_metadata(path)
                        .with_context(|| format!("stat {what} {}", path.display()))?;
                    if meta.file_type().is_symlink() {
                        anyhow::bail!("{what} {} must not be a symlink", path.display());
                    }
                    if !meta.file_type().is_file() {
                        anyhow::bail!("{what} {} must be a regular file", path.display());
                    }
                    harden_private_file(path, what)
                }
                Err(e) => Err(e).with_context(|| format!("creating {what} {}", path.display())),
            }
        }
        Err(e) => Err(e).with_context(|| format!("stat {what} {}", path.display())),
    }
}

fn harden_private_file(path: &Path, what: &str) -> Result<()> {
    let handle = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ELOOP) {
                anyhow!("{what} {} must not be a symlink", path.display())
            } else {
                anyhow!("opening {what} {} to harden perms: {e}", path.display())
            }
        })?;
    let meta = handle
        .metadata()
        .with_context(|| format!("stat opened {what} {}", path.display()))?;
    if !meta.file_type().is_file() {
        anyhow::bail!("{what} {} must be a regular file", path.display());
    }
    handle
        .set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("perms on {what} {}", path.display()))
}

/// Bound on how long `pay()` blocks awaiting a refund to a terminal state before returning and
/// leaving the row PENDING (recoverable). Stops one stuck refund (gateway down, or a payment parked
/// in `WaitingForRefund`) from blocking the serial Refunder / maintenance pass (codex P1).
const PAY_AWAIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Retention floor for reapable CANCELED `fedimint_invoice` rows before the best-effort GC
/// ([`gc_fedimint_invoice_index`], lnrent-y4m.15) may delete them (PAID rows are left — see that fn).
/// Mirrors store.rs's `TERMINAL_ROW_RETENTION_SECS` (30 days).
const FEDIMINT_INDEX_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

/// Retention floor for reapable definitively `FAILED` `fedimint_pay` rows before the best-effort GC
/// ([`gc_fedimint_pay_index`], lnrent-y4m.19) may delete them. Set MUCH longer than the 30-day invoice
/// retention (180 days) and DELIBERATELY not a config knob (single-const discipline, matching
/// store.rs and y4m.15), because a failed pay row participates in the lnrent-kum recovery guard: it
/// identifies the operation whose definitive failure was recorded while startup recovery reconciles
/// any other permanent oplog entries for the same key. Its dead-op marker survives the reap, so a
/// later oplog backfill still records FAILED rather than resurrecting that operation as live.
///
/// `SUCCEEDED` rows are intentionally NOT reapable. [`PaymentBackend::pay`] promises that calling the
/// same key twice never pays twice, [`FedimintPayment::pay_inner`] starts a new operation for an absent
/// key, and the separate store intentionally retains `refund_attempt`/`sweep_attempt` rows. Therefore
/// age alone cannot prove a successful key will never be driven again: deleting it could turn delayed
/// store bookkeeping into a second payment. A permanent tombstone or cross-DB terminal proof would add
/// mechanism for a table bounded by real refund/sweep activity, not a free-flood vector, so the
/// smallest correct y4m.19 scope is long-retention FAILED-row GC while keeping SUCCEEDED idempotency.
const FEDIMINT_PAY_RETENTION_SECS: i64 = 180 * 24 * 60 * 60;

/// Minimum spacing between two flood-path index GC sweeps (lnrent-y4m.15): a burst of
/// `create_invoice`s triggers at most one GC per hour ([`FedimintPayment::gc_index_if_due`]). The
/// throttle is TIME-based via the injected `self.clock`, so tests drive it deterministically with a
/// `TestClock`. A const, not a config knob — the reap is bounded, best-effort maintenance with no
/// operator-tunable policy (matching store.rs's single-const discipline).
const INDEX_GC_INTERVAL_SECS: i64 = 3600;

const INDEX_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS fedimint_invoice (
    external_id   TEXT PRIMARY KEY,
    operation_id  TEXT NOT NULL,
    invoice_id    TEXT NOT NULL,
    bolt11        TEXT NOT NULL,
    payment_hash  TEXT NOT NULL,
    amount_sat    INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'OPEN',
    settled_at    INTEGER
);
CREATE INDEX IF NOT EXISTS fedimint_invoice_by_invoice_id ON fedimint_invoice (invoice_id);
-- Backs the y4m.15 GC predicate (status='CANCELED' AND expires_at < cutoff). Under the
-- distinct-external_id unpaid-order flood this table can hold hundreds of thousands of rows;
-- without this index the hourly reap DELETE would full-scan while holding the sole index mutex
-- that create/lookup/settlement synchronously wait on (review P2). Applies to existing DBs on open.
CREATE INDEX IF NOT EXISTS fedimint_invoice_gc_idx ON fedimint_invoice (status, expires_at);
CREATE TABLE IF NOT EXISTS fedimint_pay (
    idempotency_key  TEXT PRIMARY KEY,
    operation_id     TEXT NOT NULL,
    backend_pay_id   TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    pay_kind         TEXT NOT NULL DEFAULT 'ln',
    -- lnrent-y4m.19: clock time this lifecycle row was (re-)created, refreshed when its current op is
    -- terminally observed, so a FAILED row gets a full retention window. SUCCEEDED rows are never
    -- reaped. NULLABLE: a fresh DB gets it here, a legacy DB gets it via the guarded ALTER in
    -- `ensure_fedimint_pay_gc_schema` (SQLite has no ADD COLUMN IF NOT EXISTS), and any NULL (legacy)
    -- row is NEVER reaped (see gc_fedimint_pay_index). The `fedimint_pay_gc_idx` covering index over
    -- (status, created_at) is created in that same helper, AFTER the ALTER — placing it in this batch
    -- would error on a legacy DB whose column does not yet exist.
    created_at       INTEGER
);
CREATE TABLE IF NOT EXISTS fedimint_pay_dead_op (
    operation_id  TEXT PRIMARY KEY
);
";

/// Real Fedimint backend: the joined fedimint client, the lnrent-owned idempotency index, the
/// registered settlement sender (set by `watch()`), and a clock for observed-settlement timestamps.
pub struct FedimintPayment {
    client: ClientHandleArc,
    index: Arc<Mutex<Connection>>,
    settle_tx: Mutex<Option<mpsc::Sender<Settlement>>>,
    clock: Arc<dyn Clock>,
    /// The operator-configured ORDERED gateway list (`[config.fedimint.gateway] ++
    /// gateway_fallbacks`), HONORED for BOTH invoice creation and refunds rather than a random pick
    /// (codex o6p). [`select_gateway`](Self::select_gateway) tries them IN ORDER and uses the first
    /// one the federation still has REGISTERED, so a gateway leaving the federation no longer stops
    /// receiving + refunds (lnrent-y4m.8 failover). NOTE this failover is registration-based, NOT an
    /// active liveness probe: a primary that crashes but keeps a live registration is still selected
    /// until that registration deregisters / its TTL lapses (a lag), at which point the next gateway
    /// takes over — the fedimint-ln-client API exposes no cheaper per-selection liveness signal. An
    /// EMPTY list selects any available gateway (tests / unset). When no configured gateway is
    /// registered it fails CLOSED, so a total outage errors rather than silently routing money through
    /// an unintended gateway.
    gateways: Vec<PublicKey>,
    /// Serializes `create_invoice`'s check->mint->insert so two concurrent same-`external_id` callers
    /// can't both mint a gateway invoice (the loser would otherwise be stranded — absent from the
    /// index, never watched). Async so it can be held across the mint `.await` (codex P1).
    create_lock: tokio::sync::Mutex<()>,
    /// Serializes outbound pay check->start->index so two concurrent same-key callers cannot both see
    /// an absent pay-index row before either has inserted the PENDING operation.
    pay_start_lock: tokio::sync::Mutex<()>,
    /// Last time the terminal-index GC ran ([`gc_index_if_due`](Self::gc_index_if_due)), unix seconds
    /// via `self.clock` (init 0, so the first production `create_invoice` sweeps). Throttles the
    /// flood-path GC to at most one run per [`INDEX_GC_INTERVAL_SECS`]. The reap uses the distinct
    /// `index` Mutex and the driver runs after `create_lock` is dropped, so the locks are never nested.
    last_index_gc_at: Mutex<i64>,
}

impl FedimintPayment {
    /// Back-compat single-gateway entry point (kept for the `#[ignore]`d live integration tests that
    /// pin ONE gateway via `Option<&str>`): a `None` primary maps to an empty ordered list ("pick any
    /// available gateway"), a `Some` primary to a one-element list. Delegates to
    /// [`join_or_open_with_gateways`](Self::join_or_open_with_gateways), the ordered-failover
    /// constructor `main.rs` uses (lnrent-y4m.8).
    pub async fn join_or_open(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
        gateway: Option<&str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let gateways: Vec<String> = gateway.map(str::to_string).into_iter().collect();
        Self::join_or_open_with_gateways(invite_code, data_dir, root_secret, &gateways, clock).await
    }

    /// Join (first run) or open (subsequent runs) the federation named by `invite_code`, honoring an
    /// ORDERED gateway list with automatic failover (lnrent-y4m.8): invoice-create and refund/sweep-pay
    /// try `gateways` IN ORDER and use the first REACHABLE one. An EMPTY list preserves the "pick any
    /// available gateway" behavior (tests / unset). The fedimint client rocksdb + the lnrent index
    /// sqlite both live under `data_dir/fedimint/<federation_id>/`. `root_secret` is lnrent's
    /// deterministic 32-byte seed (`identity.rs`), wrapped as a fedimint `DerivableSecret` under
    /// `StandardDoubleDerive`. On open it backfills the index from the fedimint oplog (crash-window
    /// recovery).
    pub async fn join_or_open_with_gateways(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
        gateways: &[String],
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let invite: InviteCode = invite_code
            .parse()
            .context("parsing federation invite code")?;
        // Parse each configured gateway pubkey now (fail FAST on a malformed value) — honored for both
        // receive + refund below; an EMPTY list = any available gateway (tests / unset).
        let gateways = gateways
            .iter()
            .map(|g| PublicKey::from_str(g))
            .collect::<Result<Vec<_>, _>>()
            .context(
                "parsing the configured fedimint gateway pubkeys \
                 (config.fedimint.gateway[_fallbacks])",
            )?;
        let federation_id = invite.federation_id().to_string();
        let paths = prepare_fedimint_paths(data_dir, &federation_id)
            .context("preparing fedimint data paths")?;

        let db: Database = RocksDb::build(paths.client_db)
            .open()
            .await
            .context("opening fedimint client rocksdb")?
            .into();

        // mint + ln + wallet only — the fork-stable modules the trait needs. 0.11.1 auto-selects
        // the primary (mint) module by priority, so there is no `with_primary_module_kind` call.
        let mut builder = Client::builder().await.context("fedimint client builder")?;
        builder.with_module(LightningClientInit::default());
        builder.with_module(MintClientInit);
        builder.with_module(WalletClientInit::default());

        let secret = RootSecret::StandardDoubleDerive(DerivableSecret::new_root(
            &root_secret[..],
            ROOT_SECRET_SALT,
        ));

        let endpoints = ConnectorRegistry::build_from_client_defaults()
            .bind()
            .await
            .context("binding fedimint connectors")?;

        let client: ClientHandleArc = if Client::is_initialized(&db).await {
            builder
                .open(endpoints, db, secret)
                .await
                .map(Arc::new)
                .context("opening existing fedimint client")?
        } else {
            builder
                .preview(endpoints, &invite)
                .await
                .context("previewing federation from invite")?
                .join(db, secret)
                .await
                .map(Arc::new)
                .context("joining federation")?
        };

        let conn = Connection::open(paths.index_db).context("opening lnrent index db")?;
        conn.execute_batch(INDEX_SCHEMA)
            .context("initialising lnrent index schema")?;
        // lnrent-y4m.19: dup-tolerantly bring a legacy pay index up to the terminal-`fedimint_pay` GC
        // schema (add `created_at` if missing, then its covering index). Must run AFTER the schema
        // batch, since the covering index references `created_at`.
        ensure_fedimint_pay_gc_schema(&conn)
            .context("applying the fedimint_pay GC schema migration (lnrent-y4m.19)")?;

        let me = Self {
            client,
            index: Arc::new(Mutex::new(conn)),
            settle_tx: Mutex::new(None),
            clock,
            gateways,
            create_lock: tokio::sync::Mutex::new(()),
            pay_start_lock: tokio::sync::Mutex::new(()),
            last_index_gc_at: Mutex::new(0),
        };

        // Backfill any invoice fedimint committed but the daemon never indexed (the crash window
        // between minting and idx_insert). FAIL-CLOSED (codex P1): refusing to start on a recovery
        // error is safer for real money than reopening the duplicate-mint window by continuing.
        let recovered = recover_index_from_oplog(&me.client, &me.index, me.clock.now())
            .await
            .context("fedimint: oplog index recovery failed; refusing to start")?;
        if recovered > 0 {
            tracing::info!(
                backfilled = recovered,
                "fedimint: recovered invoice index rows from oplog"
            );
        }

        // Symmetric backfill for OUTBOUND pays (lnrent-4gt): a crash in pay()'s window (op committed,
        // fedimint_pay row not yet upserted) would otherwise leave a refund key Unknown, so pay(key)
        // would re-parse the maybe-expired bolt11 and fail instead of re-awaiting the op. Also
        // un-hides a retry op left behind a stale FAILED row (lnrent-kum). FAIL-CLOSED.
        let (recovered_pay, unhidden_pay) =
            recover_pay_from_oplog(&me.client, &me.index, me.clock.now())
                .await
                .context("fedimint: oplog pay recovery failed; refusing to start")?;
        if recovered_pay > 0 || unhidden_pay > 0 {
            tracing::info!(
                backfilled = recovered_pay,
                replaced_stale_failed = unhidden_pay,
                "fedimint: recovered pay index rows from oplog"
            );
        }

        me.log_readiness().await;
        Ok(me)
    }

    /// On open, log a one-line Fedimint operability summary: whether the CONFIGURED gateway is
    /// reachable. The ecash balance is deliberately NOT read here (lnrent-urw.10 §F) — a startup
    /// balance query was an implicit automatic wallet read with its own could-not-query failure
    /// branch; the operator gets wallet-vs-books on demand via `lnrent reconcile`. This backend-level
    /// log is a GATEWAY LIVENESS probe only, warning when the gateway is unreachable (that blocks
    /// invoice creation and refunds). Observability ONLY — enables no payment and never fails open.
    async fn log_readiness(&self) {
        let gateway_ok = match self.refund_gateway_ready().await {
            Ok(ok) => ok,
            Err(e) => {
                tracing::info!(error = %e, "fedimint: could not query configured gateway at startup");
                false
            }
        };
        tracing::info!(gateway_ok, "fedimint readiness");
        if fedimint_readiness_warns(gateway_ok) {
            tracing::warn!(
                gateway_ok,
                "fedimint gateway unreachable: cannot create invoices or pay refunds"
            );
        }
    }

    /// Select the outbound lightning gateway for invoice-create and refund/sweep-pay, honoring the
    /// operator's ORDERED gateway list with automatic failover (lnrent-y4m.8). The thin LIVE wrapper
    /// around the pure [`select_first_reachable`] decision helper: it supplies the real
    /// `|pk| ln.get_gateway(Some(pk), false)` probe and is the ONLY part touching the fedimint client.
    /// The configured gateways are tried IN ORDER and the first one the federation still has REGISTERED
    /// is returned; when the list is EMPTY (tests / unset) this falls back to `get_gateway(None, false)`
    /// — the federation's "pick any available gateway" behavior. NOTE `get_gateway(Some(pk), false)`
    /// resolves `pk` against the (refreshed) gateway-registration cache — it is NOT an active liveness
    /// probe, so failover engages when a gateway deregisters / its registration lapses, not the instant
    /// a still-registered gateway's process crashes. Fails CLOSED (Err) when no configured gateway is
    /// registered, so receiving + refunds refuse rather than silently routing money through an
    /// unintended gateway. This is purely a gateway-SELECTION seam: it never mints or pays, so the
    /// caller mints/pays ONCE with the single returned gateway — the create-once idempotency and the
    /// INV-1 caps are untouched.
    async fn select_gateway(&self) -> Result<LightningGateway> {
        // All existing callers (invoice-create, the fee quote, the liquidity check, the plain/sweep
        // pay, readiness) get the unchanged no-preference behavior — the `None` preference reproduces
        // the pre-y4m.18 ordered-failover selection exactly.
        self.select_gateway_preferring(None).await
    }

    /// [`select_gateway`](Self::select_gateway) plus a best-effort `preferred` gateway to try FIRST
    /// (lnrent-y4m.18): the quote-time gateway a refund's [`PaymentBackend::refund_quote`] selected,
    /// threaded to its pay so the quote and the pay bind to ONE decision even if a failover would
    /// otherwise price and pay through different gateways. The pure [`ordered_with_preference`] helper
    /// builds the probe order (preferred first, then the configured order, deduped; a preferred pk NOT
    /// in the configured list is still tried first — it was reachable at quote time). If the hinted
    /// gateway is no longer reachable the ordered probe FALLS BACK to the configured order rather than
    /// failing the pay: the cap preflight against the ACTUAL paying gateway still protects INV-1, so the
    /// residual quote-gateway-vanished window is safe (never an over-cap outlay). `preferred: None`
    /// leaves the pre-y4m.18 behavior byte-for-byte, including the empty-list "any available gateway"
    /// fallback; a preference WITH an empty configured list tries the preferred pk then falls back to
    /// `get_gateway(None, ...)` "any". The [`update_gateway_cache`] fail-closed refresh is unchanged.
    async fn select_gateway_preferring(
        &self,
        preferred: Option<PublicKey>,
    ) -> Result<LightningGateway> {
        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        let ordered = ordered_with_preference(preferred, &self.gateways);
        if ordered.is_empty() {
            // No pin AND no preference (tests / unset): preserve the pre-y4m.8 "any available gateway".
            return ln
                .get_gateway(None, false)
                .await
                .context("selecting an available lightning gateway")?
                .context("no lightning gateway is available");
        }
        // Refresh the federation's gateway-registration cache BEFORE the ordered probe (adversarial
        // codex): `get_gateway(Some(pk), false)` returns a STALE locally-cached hit WITHOUT refreshing
        // (it only refreshes on a MISS), so a primary that DEREGISTERED but still sits in our local
        // cache would be selected forever and failover would never engage. FAIL CLOSED on a refresh
        // error (CodeRabbit): the refresh is a federation round-trip, so its failure means the
        // FEDERATION is unreachable — proceeding on stale data could route money through, or report
        // ready for, a gateway that is actually gone. (A mere gateway DEREGISTRATION — the failover
        // case — does NOT fail this: the federation is up, so the refresh succeeds and the ordered
        // probe below then skips the absent primary to a live fallback.)
        ln.update_gateway_cache()
            .await
            .context("refreshing the fedimint gateway registrations before failover selection")?;
        match select_first_reachable(&ordered, |pk| ln.get_gateway(Some(pk), false)).await {
            Ok(gw) => Ok(gw),
            // Empty configured list + a preference whose pk turned out unreachable: fall back to the
            // federation's "any available gateway" (the empty-list contract), rather than fail-closing
            // on the advisory hint alone. With a NON-empty configured list this arm is unreachable —
            // `ordered` already includes every configured gateway, so a failure there is the genuine
            // total-outage fail-closed and must propagate.
            Err(_) if self.gateways.is_empty() => ln
                .get_gateway(None, false)
                .await
                .context("selecting an available lightning gateway")?
                .context("no lightning gateway is available"),
            Err(e) => Err(e),
        }
    }

    /// Await a refund payment to a terminal state, recording the outcome in the pay index and
    /// returning the backend payment id (the operation-id hex) on success. A DEFINITIVE failure
    /// (funds provably back or never sent) marks the row FAILED and Errs; an AMBIGUOUS terminal state
    /// (cannot prove the recipient was unpaid) writes NO mark — the row stays PENDING — and Errs, so
    /// `payment_status_by_key` reports Pending and the driver re-awaits the SAME operation on the next
    /// drive instead of ever starting a second payment (lnrent-y4m.16). Outbound, so there is no
    /// settled_at/over-credit concern — `into_stream()` (which replays a cached terminal outcome as a
    /// single item) is sufficient, unlike `watch()`.
    async fn await_pay(&self, payment_type: PayType, key: &str) -> Result<String> {
        let op_hex = payment_type.operation_id().fmt_full().to_string();
        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        match payment_type {
            PayType::Lightning(op) => {
                let mut updates = ln
                    .subscribe_ln_pay(op)
                    .await
                    .context("subscribing to refund payment")?
                    .into_stream();
                // fedimint-ln-client emits `AwaitingChange` ONLY after the payment preimage is
                // already in hand (the recipient is provably paid; only the operator's own change
                // output is still settling). Track that transition so a subsequent
                // `UnexpectedError` — which fedimint also emits for a change-output failure AFTER a
                // successful payment — upgrades to SUCCEEDED instead of parking a provably-paid
                // refund as ambiguous (adversarial y4m.16 review; both independent reviewers traced
                // this to the same fedimint 0.11.1 source).
                //
                // This in-memory flag is safe to re-derive on EVERY (re-)await — an interrupted,
                // timed-out (PAY_AWAIT_TIMEOUT), or post-crash re-subscribe loses nothing and the
                // upgrade STILL lands (lnrent-mc8, verified against fedimint v0.11.1 @ 2620789):
                //  - The pay state machine's persisted terminal for this case is `Success(preimage)`
                //    (pay.rs), NOT an error state. `AwaitingChange` and the change-output
                //    `UnexpectedError` are BOTH re-derived LIVE from durable op-meta on each subscribe
                //    (`subscribe_ln_pay` yields `AwaitingChange` then awaits the change output,
                //    lib.rs:1619-1631) — the error is never persisted as the terminal.
                //  - fedimint caches an operation outcome ONLY when a subscriber drains the update
                //    stream to EOF (`caching_operation_update_stream` writes the last update AFTER its
                //    `while let Some` loop, oplog.rs:376). This loop BAILS on the terminal state and
                //    never polls to `None`, so it never arms that cache; nothing else drains this op.
                // So a re-subscribe always replays `Created→Funded→Success` and re-yields
                // `AwaitingChange` (re-setting this flag) BEFORE any re-`UnexpectedError` — mc8's
                // "parks PENDING forever" is unreachable, hence a doc-only close. (One PRE-EXISTING
                // fedimint fragility, NOT this mechanism: the notifier orders replayed states by
                // wall-clock `created_at` with a non-monotonicity FIXME — a severe backward clock step
                // could desync the positional replay, but that hits FIRST-time subscribes equally.)
                let mut preimage_obtained = false;
                while let Some(state) = updates.next().await {
                    if matches!(state, LnPayState::AwaitingChange) {
                        preimage_obtained = true;
                    }
                    // The single money choke point: classify (pure helper) then act on the CLASS, so
                    // the FAILED-vs-leave-PENDING decision is proven variant-by-variant without a
                    // federation (lnrent-y4m.16).
                    match classify_ln_pay_state(&state) {
                        PayStateClass::Success => {
                            pay_idx_mark(&self.index, key, &op_hex, "SUCCEEDED", self.clock.now())?;
                            return Ok(op_hex);
                        }
                        // Created / Funded / AwaitingChange / WaitingForRefund -> keep waiting.
                        PayStateClass::InFlight => {}
                        // Refunded / Canceled -> funds provably back / never sent. The FAILED mark
                        // lets a later drive start a fresh payment WITHOUT a double-pay.
                        //
                        // Documented residual (adversarial y4m.16 review): `Refunded` proves the
                        // OPERATOR's funds are back, not that the Lightning RECIPIENT went unpaid — a
                        // gateway that paid the invoice but lost its contract claim leaves the
                        // recipient paid at the GATEWAY's expense while we refund + retry. The
                        // operator's own outlay stays single (this attempt cost nothing), so INV-1
                        // holds; classifying `Refunded` as ambiguous instead would strand EVERY
                        // routine route-failure refund (it is fedimint's normal failure terminal).
                        PayStateClass::DefinitiveFailure => {
                            // Op-level fact, recorded regardless of the row CAS below: this exact
                            // operation terminally failed with funds provably back, so crash
                            // recovery must never resurrect it (lnrent-kum dead-op ledger).
                            pay_idx_record_dead(&self.index, &op_hex)?;
                            pay_idx_mark(&self.index, key, &op_hex, "FAILED", self.clock.now())?;
                            anyhow::bail!("refund payment failed: {state:?}");
                        }
                        // UnexpectedError -> AMBIGUOUS: NOT proof the recipient was unpaid. Write NO
                        // index mark so the key row STAYS PENDING and bail; `payment_status_by_key`
                        // then reports Pending, so the driver re-awaits THIS SAME operation next drive
                        // (never starts a new pay). A terminally-UnexpectedError op replays the same
                        // state on every re-subscribe, so the row stays PENDING indefinitely — the
                        // INTENDED fail-safe: money ambiguity needs an operator/fedimint resolution,
                        // never an automatic second send. (One known upstream lossy case: the client
                        // stream can collapse a definitive `FundingRejected` into `UnexpectedError`,
                        // which parks a provably-unfunded pay here — indistinguishable at this layer;
                        // the RefundStuck operator alert surfaces it.)
                        PayStateClass::Ambiguous => {
                            if preimage_obtained {
                                // The stream already proved the recipient was paid (AwaitingChange);
                                // this terminal error concerns only the operator's change output.
                                // SUCCEEDED is the money-correct outcome for the refund; the change
                                // loss is operator-internal, so surface it loudly.
                                tracing::warn!(
                                    %key,
                                    op = %op_hex,
                                    ?state,
                                    "refund payment succeeded (preimage obtained) but the change \
                                     output errored; marking SUCCEEDED — the operator's change may \
                                     need fedimint-side recovery"
                                );
                                pay_idx_mark(
                                    &self.index,
                                    key,
                                    &op_hex,
                                    "SUCCEEDED",
                                    self.clock.now(),
                                )?;
                                return Ok(op_hex);
                            }
                            anyhow::bail!(
                                "refund payment reached an ambiguous terminal state: {state:?}; \
                                 leaving PENDING — will re-await this operation, never re-pay"
                            );
                        }
                    }
                }
                anyhow::bail!("refund ln-pay stream ended without a terminal state")
            }
            PayType::Internal(op) => {
                let mut updates = ln
                    .subscribe_internal_pay(op)
                    .await
                    .context("subscribing to internal refund payment")?
                    .into_stream();
                while let Some(state) = updates.next().await {
                    // Same class-based choke point as the lightning branch (lnrent-y4m.16).
                    match classify_internal_pay_state(&state) {
                        PayStateClass::Success => {
                            pay_idx_mark(&self.index, key, &op_hex, "SUCCEEDED", self.clock.now())?;
                            return Ok(op_hex);
                        }
                        // Funding -> keep waiting.
                        PayStateClass::InFlight => {}
                        // FundingFailed / RefundSuccess -> funds never left / provably back. FAILED
                        // is safe: a later drive may start a fresh payment.
                        PayStateClass::DefinitiveFailure => {
                            // Same op-level dead-ledger record as the lightning branch (lnrent-kum).
                            pay_idx_record_dead(&self.index, &op_hex)?;
                            pay_idx_mark(&self.index, key, &op_hex, "FAILED", self.clock.now())?;
                            anyhow::bail!("internal refund payment failed: {state:?}");
                        }
                        // RefundError / UnexpectedError -> AMBIGUOUS (a refund was attempted and
                        // errored, or an unclassified local error): the funds' whereabouts are
                        // unknown. Write NO mark so the row STAYS PENDING and re-awaits this SAME
                        // operation — never a second send. See the lightning branch for the full
                        // rationale.
                        PayStateClass::Ambiguous => {
                            anyhow::bail!(
                                "internal refund payment reached an ambiguous terminal state: \
                                 {state:?}; leaving PENDING — will re-await this operation, never re-pay"
                            );
                        }
                    }
                }
                anyhow::bail!("refund internal-pay stream ended without a terminal state")
            }
        }
    }

    /// `await_pay` under a [`PAY_AWAIT_TIMEOUT`]. On timeout the pay row stays PENDING and an Err is
    /// returned, so `payment_status_by_key` reports Pending (recoverable) and the Refunder re-drives
    /// it on the next pass instead of blocking on a stuck payment (codex P1).
    async fn await_pay_bounded(&self, payment_type: PayType, key: &str) -> Result<String> {
        match tokio::time::timeout(PAY_AWAIT_TIMEOUT, self.await_pay(payment_type, key)).await {
            Ok(r) => r,
            Err(_) => anyhow::bail!(
                "refund payment still pending after {}s; leaving PENDING for the next drive",
                PAY_AWAIT_TIMEOUT.as_secs()
            ),
        }
    }

    /// Park a structurally-invalid refund (bad / zero / mismatched bolt11) as a FAILED key row (no
    /// real operation) so `payment_status_by_key` reports Failed and the Refunder parks it rather
    /// than retrying a bad destination forever (codex P2).
    async fn fail_pay_preflight<T>(&self, key: &str, msg: String) -> Result<T> {
        pay_idx_upsert(
            &self.index,
            key,
            "(preflight-failed)",
            "FAILED",
            "ln",
            self.clock.now(),
        )?;
        anyhow::bail!(msg)
    }

    /// The shared refund/sweep-pay engine behind [`PaymentBackend::pay`],
    /// [`PaymentBackend::pay_refund_capped`], and [`PaymentBackend::pay_capped`]. Idempotent on the key
    /// (SUCCEEDED never re-pays; PENDING re-awaits the SAME operation; FAILED/absent starts fresh).
    /// `cap` sets the pre-send ceiling enforced against the SAME gateway object used to start the
    /// payment, so a NEW outbound op whose payout + advertised fee exceeds the ceiling is refused
    /// before any money moves: [`PayCap::Gross`] is the INV-1 refund cap (payout + fee ≤ received
    /// gross, spec §3.1); [`PayCap::Outlay`] is the operator-sweep quote cap (payout + fee ≤ the quoted
    /// `max_outlay_msat`); [`PayCap::None`] (the plain `pay`) skips the cap.
    async fn pay_inner(
        &self,
        dest: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
        preferred: Option<PublicKey>,
    ) -> Result<String> {
        let payment_type = {
            // Idempotent on the key: serialize check->start->index so concurrent same-key callers cannot
            // both observe "absent" before either inserts the PENDING operation. The guard is released
            // before awaiting terminal settlement so unrelated pays do not wait behind a long HTLC.
            let _pay_guard = self.pay_start_lock.lock().await;
            if let Some((op_hex, status, kind)) = pay_idx_get(&self.index, idempotency_key)? {
                match status.as_str() {
                    "SUCCEEDED" => return Ok(op_hex),
                    "PENDING" => {
                        let op = OperationId::from_str(&op_hex)
                            .map_err(|e| anyhow!("invalid stored pay operation id: {e}"))?;
                        if kind == "internal" {
                            PayType::Internal(op)
                        } else {
                            PayType::Lightning(op)
                        }
                    }
                    _ => {
                        // FAILED -> re-attempt below. Not a double-pay: `await_pay` now writes FAILED
                        // ONLY for DEFINITIVE outcomes (Refunded/Canceled/FundingFailed/RefundSuccess
                        // — funds provably back or never sent) plus structural preflight parks (no
                        // operation was ever started). An AMBIGUOUS outcome (UnexpectedError /
                        // RefundError) instead leaves the row PENDING, so it takes the "PENDING" arm
                        // above and re-awaits the SAME operation — it can never reach this re-pay
                        // (lnrent-y4m.16).
                        self.start_new_pay(dest, amount_sat, idempotency_key, cap, preferred)
                            .await?
                    }
                }
            } else {
                self.start_new_pay(dest, amount_sat, idempotency_key, cap, preferred)
                    .await?
            }
        };

        self.await_pay_bounded(payment_type, idempotency_key).await
    }

    async fn start_new_pay(
        &self,
        dest: &str,
        amount_sat: u64,
        idempotency_key: &str,
        cap: PayCap,
        preferred: Option<PublicKey>,
    ) -> Result<PayType> {
        // Structural preflight failures (bad bolt11, no amount, amount mismatch) happen before any
        // operation exists. Park them as a FAILED key row so payment_status_by_key reports Failed and
        // the Refunder parks the refund for an operator rather than retrying a bad dest forever
        // (codex P2). .4c is bolt11-only; the LNURL/BOLT12 resolver is a separate bead.
        let invoice = match Bolt11Invoice::from_str(dest) {
            Ok(i) => i,
            Err(e) => {
                return self
                    .fail_pay_preflight(idempotency_key, format!("refund bolt11 parse error: {e}"))
                    .await
            }
        };
        let inv_msat = match invoice.amount_milli_satoshis() {
            Some(a) => a,
            None => {
                return self
                    .fail_pay_preflight(idempotency_key, "refund bolt11 has no amount".to_string())
                    .await
            }
        };
        // Checked msat conversion (spec §3.1 overflow discipline): an auto-pay amount whose msat form
        // would overflow u64 is parked for manual handling, never saturated/wrapped.
        let pay_msat = match amount_sat.checked_mul(1000) {
            Some(m) => m,
            None => {
                return self
                    .fail_pay_preflight(
                        idempotency_key,
                        format!("refund amount {amount_sat} sat overflows u64 msats"),
                    )
                    .await
            }
        };
        if inv_msat != pay_msat {
            return self
                .fail_pay_preflight(
                    idempotency_key,
                    format!("refund bolt11 amount {inv_msat} msat != owed {pay_msat} msat"),
                )
                .await;
        }

        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        // Failover selection (lnrent-y4m.8), preferring the quote-time gateway when one was carried
        // (lnrent-y4m.18): the first reachable gateway, hinted first then in the configured order. The
        // SAME chosen gateway object is used for the cap preflight AND pay_bolt11_invoice below, so the
        // fee/route caps are enforced against exactly the gateway the money flows through — the hint
        // only reorders the probe, it can never weaken the cap.
        let gateway = self
            .select_gateway_preferring(preferred)
            .await
            .context("selecting a reachable lightning gateway for the refund")?;
        // Final cap preflight (spec §3.1 / gate1-operator-sweep): quoting and paying are separate
        // awaits, so re-check the cap here against the SAME gateway object we pass into
        // pay_bolt11_invoice — refuse to START a new outbound op whose payout + advertised fee would
        // exceed the ceiling (received gross for INV-1 refunds; the quoted outlay for a sweep). The fee
        // MUST be Fedimint's ACTUAL fee ([`gateway_fee_msat`], mirroring RoutingFees::to_amount), not
        // the naive floor(x*ppm/1e6) — see that helper. All msat arithmetic is widened to u128.
        if let Some(ceiling_msat) = cap.ceiling_msat() {
            let fees = gateway.fees;
            let pay_msat_u128 = u128::from(pay_msat);
            let over_cap = match gateway_fee_msat(
                u64::from(fees.base_msat),
                u64::from(fees.proportional_millionths),
                pay_msat_u128,
            ) {
                Some(fee_msat) => pay_msat_u128 + fee_msat > ceiling_msat,
                None => true, // an unpayable (>100%) schedule: never start an over-cap op
            };
            if over_cap {
                return self
                    .fail_pay_preflight(idempotency_key, cap.over_cap_message(amount_sat))
                    .await;
            }
        }
        let outgoing = ln
            .pay_bolt11_invoice(
                Some(gateway),
                invoice,
                json!({ "lnrent_idempotency_key": idempotency_key }),
            )
            .await
            .context("initiating refund payment")?;

        let kind = if matches!(outgoing.payment_type, PayType::Internal(_)) {
            "internal"
        } else {
            "ln"
        };
        let op_hex = outgoing.payment_type.operation_id().fmt_full().to_string();
        // Crash window: a crash between pay_bolt11_invoice committing and this upsert leaves no key
        // row -> payment_status_by_key=Unknown. recover_pay_from_oplog (on open, symmetric to
        // recover_index_from_oplog) backfills the row from the oplog extra_meta so the next pay(key)
        // re-awaits the OP directly rather than re-parsing the maybe-expired bolt11 (lnrent-4gt).
        pay_idx_upsert(
            &self.index,
            idempotency_key,
            &op_hex,
            "PENDING",
            kind,
            self.clock.now(),
        )?;
        Ok(outgoing.payment_type)
    }

    /// Throttled, best-effort driver for the terminal index GC, called at the END of a successful
    /// `create_invoice` after the mint committed and OUTSIDE `create_lock`. [`index_gc_due_and_stamp`]
    /// gates it to at most one reap per [`INDEX_GC_INTERVAL_SECS`] over `last_index_gc_at`, timed by
    /// the injected `self.clock` (deterministic under a `TestClock`). The SQLite work runs on Tokio's
    /// blocking pool so it does not hold up the successful create response. BEST-EFFORT: a reap error
    /// is logged and swallowed, so GC never changes the result of the already-committed mint.
    ///
    /// A SINGLE throttle covers BOTH indices (lnrent-y4m.19): the same `spawn_blocking` closure reaps
    /// the `fedimint_invoice` flood (y4m.15) AND the definitively failed `fedimint_pay` rows — no second
    /// timer, no second create-path hook. Each non-zero reap is logged independently.
    fn gc_index_if_due(&self) {
        let now = self.clock.now();
        if !index_gc_due_and_stamp(&self.last_index_gc_at, now, INDEX_GC_INTERVAL_SECS) {
            return;
        }
        let index = self.index.clone();
        drop(tokio::task::spawn_blocking(move || {
            match gc_fedimint_invoice_index(&index, now, FEDIMINT_INDEX_RETENTION_SECS) {
                Ok(0) => {}
                Ok(reaped) => tracing::info!(
                    reaped,
                    "fedimint: reaped terminal invoice index rows past retention"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    "fedimint: best-effort invoice index GC failed; ignoring"
                ),
            }
            // lnrent-y4m.19: reap definitively failed fedimint_pay rows under the SAME throttle, still
            // OUTSIDE create_lock, still best-effort (pay-GC can never affect the committed mint).
            match gc_fedimint_pay_index(&index, now, FEDIMINT_PAY_RETENTION_SECS) {
                Ok(0) => {}
                Ok(reaped) => tracing::info!(
                    reaped,
                    "fedimint: reaped definitively failed pay index rows past retention"
                ),
                Err(e) => tracing::warn!(
                    error = %e,
                    "fedimint: best-effort pay index GC failed; ignoring"
                ),
            }
            // The dead-op ledger (fedimint_pay_dead_op) is DELIBERATELY left un-GC'd (lnrent-y4m.19
            // fallback, §5). The bead offered to also reap orphaned markers whose owning fedimint_pay
            // row is gone (`operation_id NOT IN (SELECT operation_id FROM fedimint_pay)`); we DECLINE,
            // because that reap is all cost and no benefit:
            //  1. It is NOT a flood vector. The ledger holds one row per DEFINITIVELY-FAILED pay op, so
            //     it is bounded by real refund/sweep failure volume — not the distinct-external_id
            //     unpaid-order flood y4m.15 exists to bound — so leaving it never defeats this bead's
            //     flood goal.
            //  2. Reaping it would REINTRODUCE a startup brick. Take a key whose refund failed twice:
            //     ops A then A' both reach the dead ledger and the row ends FAILED under A'. Recovery
            //     runs on EVERY startup BEFORE any create-path reap, and today it SKIPs both (A is
            //     dead-under-a-different-op, A' is FAILED-under-the-same-op) — clean forever. If the
            //     180-day reap deleted the row AND both now-orphaned markers, a later restart's
            //     recover_pay_from_oplog would see the two permanent oplog entries under an ABSENT row
            //     with NO dead marker, classify BOTH as live candidates, and FAIL THE PASS on ambiguity
            //     — the daemon would refuse to start. Keeping the markers also makes each normally
            //     reaped FAILED op backfill as FAILED rather than as a live candidate. A trustworthy,
            //     oplog-aware dead-op GC is a narrower follow-up, not bolted on here.
        }));
    }
}

/// The pre-send outlay ceiling [`FedimintPayment::pay_inner`] enforces before starting a NEW
/// outbound op (quoting and paying are separate awaits, so a fee rise between them must refuse, not
/// overspend). All three variants share the SAME `payout_msat + gateway_fee_msat > ceiling` check;
/// only the ceiling and the operator-facing message differ.
#[derive(Clone, Copy)]
enum PayCap {
    /// No cap — the plain `pay` (legacy/internal callers): pay the requested amount.
    None,
    /// INV-1 refund cap (spec §3.1): refuse if payout + fee exceeds the received gross (`gross_sat*1000`).
    Gross(u64),
    /// Operator-sweep outlay cap (gate1-operator-sweep, urw.3): refuse if payout + fee exceeds the
    /// just-quoted `max_outlay_msat`.
    Outlay(u128),
}

impl PayCap {
    /// The ceiling in msats, or `None` for the uncapped plain `pay`.
    fn ceiling_msat(&self) -> Option<u128> {
        match *self {
            PayCap::None => None,
            PayCap::Gross(gross_sat) => Some(u128::from(gross_sat) * 1000),
            PayCap::Outlay(max_outlay_msat) => Some(max_outlay_msat),
        }
    }

    /// The operator-facing preflight-refusal message for an over-cap payout.
    fn over_cap_message(&self, amount_sat: u64) -> String {
        match *self {
            PayCap::None => String::new(),
            PayCap::Gross(gross_sat) => format!(
                "refund payout {amount_sat} sat + gateway fee exceeds the {gross_sat} sat \
                 received (INV-1 cap)"
            ),
            PayCap::Outlay(max_outlay_msat) => format!(
                "sweep payout {amount_sat} sat + gateway fee exceeds the quoted {max_outlay_msat} \
                 msat outlay cap"
            ),
        }
    }
}

#[async_trait]
impl PaymentBackend for FedimintPayment {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        // Serialize check->mint->insert so two concurrent same-external_id callers can't both mint
        // (codex P1): the second waits here, then finds the index populated and returns the winner.
        let create_guard = self.create_lock.lock().await;
        // Idempotent on external_id: a repeat (or crash-retry) returns the stored invoice, never a
        // second gateway invoice.
        if let Some(inv) = idx_get_by_external(&self.index, external_id)? {
            return Ok(inv);
        }

        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        // A gateway is REQUIRED for an externally-payable invoice (codex finding #1). LN-invoice
        // receive INTRINSICALLY needs A gateway — there is no ecash-native receive path — so the
        // ordered failover list (lnrent-y4m.8) REDUCES but cannot eliminate the receive-side
        // single-point-of-failure: if EVERY configured gateway is down, receiving still fails closed.
        // select_gateway tries the configured gateways in order and returns the first REACHABLE one;
        // crucially it does NOT mint, so the create-once mint below still happens EXACTLY ONCE per
        // external_id (select first, then create_bolt11_invoice once — NEVER once per attempted
        // gateway). The idx_get_by_external check above is the create-once anchor, upstream of this.
        let gateway = self
            .select_gateway()
            .await
            .context("selecting a reachable lightning gateway")?;

        let desc = Description::new(memo.to_string())
            .map_err(|e| anyhow!("invalid invoice description: {e}"))?;
        let (op, invoice, _preimage) = ln
            .create_bolt11_invoice(
                Amount::from_sats(amount_sat),
                Bolt11InvoiceDescription::Direct(desc),
                Some(u64::from(expiry_s)),
                json!({ "lnrent_external_id": external_id }),
                Some(gateway),
            )
            .await
            .context("creating gateway bolt11 invoice")?;

        let op_hex = op.fmt_full().to_string();
        let inv = Invoice {
            id: format!("fm-{op_hex}"),
            external_id: external_id.to_string(),
            backend_invoice_id: op_hex.clone(),
            payment_hash: invoice.payment_hash().to_string(),
            bolt11: invoice.to_string(),
            amount_sat,
            // Absolute expiry from our clock at creation (matches the field's contract + MockPayment).
            expires_at: self.clock.now() + i64::from(expiry_s),
        };
        idx_insert(&self.index, &inv, &op_hex)?;

        // If a watcher is already registered, stream this fresh (live) invoice's settlement now;
        // otherwise the next watch() picks it up from the index (status OPEN).
        let tx = self.settle_tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            tokio::spawn(run_receive_task(
                self.client.clone(),
                self.index.clone(),
                tx,
                self.clock.clone(),
                op,
                inv.external_id.clone(),
                inv.id.clone(),
                amount_sat,
                true, // live: a freshly-created invoice pushes Settlement on Claimed
            ));
        }

        // Never schedule index GC while holding create_lock.
        drop(create_guard);
        self.gc_index_if_due();

        Ok(inv)
    }

    async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
        match idx_get_status_by_invoice_id(&self.index, id)? {
            Some((status, expires_at)) => Ok(if status == "PAID" {
                PaymentStatus::Paid
            } else if self.clock.now() >= expires_at {
                PaymentStatus::Expired
            } else {
                PaymentStatus::Open
            }),
            None => Ok(PaymentStatus::Expired), // unknown id -> gone (mirrors MockPayment)
        }
    }

    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        match idx_get_settlement_by_invoice_id(&self.index, id)? {
            // `settled_at` is Some ONLY for a LIVE Claimed (`run_receive_task` live=true); a cached/
            // recovery Claimed left it NULL -> None, so the supervisor catch-up caps conservatively.
            Some((status, expires_at, settled_at)) => Ok(if status == "PAID" {
                (PaymentStatus::Paid, settled_at)
            } else if self.clock.now() >= expires_at {
                (PaymentStatus::Expired, None)
            } else {
                (PaymentStatus::Open, None)
            }),
            None => Ok((PaymentStatus::Expired, None)), // unknown id -> gone (mirrors MockPayment)
        }
    }

    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String> {
        // No gross context here (legacy/internal callers): pay the requested amount with the standard
        // key idempotency and NO cap. The Refunder uses pay_refund_capped and the Sweeper uses
        // pay_capped when they know the ceiling, so the cap is enforced on the money path (spec §3.1).
        self.pay_inner(dest, amount_sat, idempotency_key, PayCap::None, None)
            .await
    }

    /// INV-1 fee-bearing quote (spec §3.1): the largest whole-sat payout for `gross_sat` whose payout
    /// plus the configured gateway's advertised fee fits inside `gross_sat`. Read-only — never mints,
    /// pays, or mutates.
    async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
        // The SAME reachable gateway the refund pay will use (lnrent-y4m.8 failover) — its advertised
        // fee schedule is the operator's exposure. A selection failure (no configured gateway
        // reachable) is a TRANSIENT quote failure (Err), never dust/Ok(0), so the Refunder leaves the
        // row PENDING instead of parking it (spec §3.1).
        let gateway = self
            .select_gateway()
            .await
            .context("selecting a reachable lightning gateway for the refund fee quote")?;
        let fees = gateway.fees;
        Ok(net_payout_sat(
            u64::from(fees.base_msat),
            u64::from(fees.proportional_millionths),
            gross_sat,
        ))
    }

    async fn refund_quote(&self, gross_sat: u64) -> Result<RefundQuote> {
        // ONE gateway decision for the quote (lnrent-y4m.18): select once, derive BOTH the net cap
        // (same `net_payout_sat` math as `refund_net_sat`) and the hint (that gateway's `gateway_id`
        // pubkey hex) from it, so the pay of the SAME attempt can prefer this exact gateway and quote
        // and pay agree on the fee schedule the INV-1 cap is measured against. A selection failure
        // stays a TRANSIENT quote Err (never dust/Ok(0)), exactly as `refund_net_sat`.
        let gateway = self
            .select_gateway()
            .await
            .context("selecting a reachable lightning gateway for the refund fee quote")?;
        let fees = gateway.fees;
        let net_sat = net_payout_sat(
            u64::from(fees.base_msat),
            u64::from(fees.proportional_millionths),
            gross_sat,
        );
        Ok(RefundQuote {
            net_sat,
            // The gateway identity `select_gateway_preferring` matches a preference against (the
            // `LightningGatewayKey` / `get_gateway(Some(pk), _)` key). Advisory + never persisted.
            gateway_hint: Some(gateway.gateway_id.to_string()),
        })
    }

    async fn refund_required_outlay_msat(
        &self,
        gross_sat: u64,
        pay_sat: Option<u64>,
    ) -> Result<u128> {
        // The first reachable gateway in the configured order (lnrent-y4m.8 failover).
        let gateway = self
            .select_gateway()
            .await
            .context("selecting a reachable lightning gateway for the refund liquidity check")?;
        let fees = gateway.fees;
        let pay_sat = pay_sat.unwrap_or_else(|| {
            net_payout_sat(
                u64::from(fees.base_msat),
                u64::from(fees.proportional_millionths),
                gross_sat,
            )
        });
        if pay_sat == 0 {
            return Ok(0);
        }
        let pay_msat = u128::from(pay_sat) * 1000;
        let fee_msat = gateway_fee_msat(
            u64::from(fees.base_msat),
            u64::from(fees.proportional_millionths),
            pay_msat,
        )
        .unwrap_or(u128::MAX);
        Ok(pay_msat.saturating_add(fee_msat))
    }

    async fn pay_refund_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
    ) -> Result<String> {
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Gross(gross_sat),
            None,
        )
        .await
    }

    async fn pay_refund_capped_via(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        idempotency_key: &str,
        gateway_hint: Option<&str>,
    ) -> Result<String> {
        // Parse the advisory quote-time hint (lnrent-y4m.18) into the gateway pubkey the selection seam
        // prefers. An UNPARSEABLE hint is treated as NO hint (fall through to the ordered probe), NOT an
        // error: it is advisory, and the INV-1 cap is enforced against whatever gateway actually pays.
        let preferred = gateway_hint.and_then(|h| PublicKey::from_str(h).ok());
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Gross(gross_sat),
            preferred,
        )
        .await
    }

    async fn pay_capped(
        &self,
        bolt11: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        idempotency_key: &str,
    ) -> Result<String> {
        // The operator sweep passes the just-quoted outlay as the ceiling: a fee that rose since the
        // quote makes payout+fee exceed it, so the preflight refuses rather than overspends (urw.3).
        self.pay_inner(
            bolt11,
            amount_sat,
            idempotency_key,
            PayCap::Outlay(max_outlay_msat),
            None,
        )
        .await
    }

    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus> {
        Ok(map_pay_status(pay_idx_status_by_op(
            &self.index,
            payment_id,
        )?))
    }
    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
        Ok(map_pay_status(pay_idx_status_by_key(
            &self.index,
            idempotency_key,
        )?))
    }

    async fn payment_started_by_key(&self, idempotency_key: &str) -> Result<bool> {
        Ok(pay_idx_status_by_key(&self.index, idempotency_key)?.is_some())
    }

    async fn available_balance_msat(&self) -> Result<Option<u64>> {
        Ok(Some(self.client.get_balance_for_btc().await?.msats))
    }

    async fn refund_gateway_ready(&self) -> Result<bool> {
        // Failover-aware readiness (lnrent-y4m.8): ready iff at least one configured gateway is
        // reachable (an empty list falls back to "any available gateway"). select_gateway fails CLOSED
        // when none is reachable; RefundReadinessProbe + log_readiness treat that Err and an Ok(false)
        // identically as "not ready", and the Err carries the underlying reason for the diagnostic.
        self.select_gateway().await.map(|_| true)
    }

    async fn backend_ready(&self) -> Result<bool> {
        // Federation LIVENESS (lnrent-urw.4): `session_count()` is a cheap authenticated round-trip
        // to the guardians that reaches consensus — NOT a local-DB read like `available_balance_msat`.
        // Any success means the federation is reachable; an error means guardians down / no consensus.
        self.client
            .api()
            .session_count()
            .await
            .map(|_| true)
            .map_err(|e| anyhow::anyhow!("fedimint federation unreachable (session_count): {e}"))
    }

    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        *self.settle_tx.lock().unwrap() = Some(tx.clone());

        // Boot/restart re-subscribe: stream every still-OPEN invoice as a RECOVERY task (live=false)
        // — it marks PAID without pushing; the supervisor catch-up recovers each with a capped
        // timestamp (codex P1). Newly-created invoices get their own live task from create_invoice.
        for row in idx_list_open(&self.index)? {
            let op = match OperationId::from_str(&row.operation_id) {
                Ok(op) => op,
                Err(e) => {
                    tracing::error!(op = %row.operation_id, error = %e, "fedimint: bad stored operation id; skipping");
                    continue;
                }
            };
            tokio::spawn(run_receive_task(
                self.client.clone(),
                self.index.clone(),
                tx.clone(),
                self.clock.clone(),
                op,
                row.external_id,
                row.invoice_id,
                row.amount_sat,
                false, // recovery: mark PAID without pushing; catch-up recovers with a capped ts
            ));
        }

        Ok(rx)
    }
}

/// A still-OPEN index row to (re-)subscribe to on `watch()`.
struct OpenRow {
    external_id: String,
    operation_id: String,
    invoice_id: String,
    amount_sat: u64,
}

/// Stream one invoice operation to settlement. `live` decides provenance: a freshly-created invoice
/// (`live=true`) pushes a `Settlement{settled_at=now}` on `Claimed`; a boot/restart re-subscription
/// (`live=false`) only marks the index PAID (settled_at=NULL) and never pushes, because fedimint
/// replays a settled-while-down op as an ordinary stream indistinguishable from a live transition —
/// the supervisor catch-up then recovers it with a safe capped timestamp, so a late-observed
/// settlement never over-credits `paid_through` (codex P1).
#[allow(clippy::too_many_arguments)]
async fn run_receive_task(
    client: ClientHandleArc,
    index: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<Settlement>,
    clock: Arc<dyn Clock>,
    op: OperationId,
    external_id: String,
    invoice_id: String,
    amount_sat: u64,
    live: bool,
) {
    let op_hex = op.fmt_full().to_string();
    let sub = {
        let ln = match client.get_first_module::<LightningClientModule>() {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "fedimint: no lightning module for receive task");
                return;
            }
        };
        match ln.subscribe_ln_receive(op).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(op = %op_hex, error = %e, "fedimint: subscribe_ln_receive failed");
                return;
            }
        }
    };

    let mut stream = sub.into_stream();
    while let Some(state) = stream.next().await {
        match state {
            LnReceiveState::Claimed => {
                let settled_at = if live { Some(clock.now()) } else { None };
                if let Err(e) = idx_mark_paid(&index, &op_hex, settled_at) {
                    tracing::error!(op = %op_hex, error = %e, "fedimint: index mark-paid failed");
                }
                if let Some(at) = settled_at {
                    let _ = tx
                        .send(Settlement {
                            invoice_id,
                            external_id,
                            amount_sat,
                            settled_at: at,
                        })
                        .await;
                }
                // Poll once past the terminal update so Fedimint's caching wrapper persists the
                // Claimed outcome in its operation log before this task drops the stream.
                let _ = stream.next().await;
                return;
            }
            LnReceiveState::Canceled { reason } => {
                tracing::warn!(op = %op_hex, ?reason, "fedimint: ln receive canceled");
                // Flip the index row out of 'OPEN': rows left OPEN here accumulated forever, and
                // every `watch()` (boot + each settlement-loop restart) re-spawned one receive
                // task per historical unpaid invoice via `idx_list_open`.
                if let Err(e) = idx_mark_canceled(&index, &op_hex) {
                    tracing::warn!(op = %op_hex, error = %e, "fedimint: marking canceled receive in index failed");
                }
                // The operation-log wrapper caches a terminal outcome only when polled through
                // stream completion. Recovery can then distinguish this canceled operation from a
                // paid one after the CANCELED index row is reaped.
                let _ = stream.next().await;
                return;
            }
            _ => {}
        }
    }
}

/// Scan the fedimint operation log for Receive ops stamped with an `lnrent_external_id` that the
/// index is missing, and backfill them — closing the window where fedimint committed an invoice but
/// the daemon crashed before persisting the index row (codex finding #3). A cached Canceled outcome
/// is the only safe skip: invoice age alone cannot distinguish an unpaid expiry from a payment that
/// settled while the daemon was down.
async fn recover_index_from_oplog(
    client: &ClientHandleArc,
    index: &Arc<Mutex<Connection>>,
    now: i64,
) -> Result<usize> {
    let log = client.operation_log();
    let mut backfilled = 0usize;
    let mut last = None;
    loop {
        let page = log.paginate_operations_rev(100, last).await;
        let count = page.len();
        if count == 0 {
            break;
        }
        for (key, entry) in &page {
            if entry.operation_module_kind() != fedimint_ln_common::KIND.as_str() {
                continue;
            }
            let meta: LightningOperationMeta = match entry.try_meta() {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "fedimint: skipping oplog entry with undecodable ln meta");
                    continue;
                }
            };
            let LightningOperationMetaVariant::Receive { invoice, .. } = &meta.variant else {
                continue;
            };
            let outcome = match entry.try_outcome::<LnReceiveState>() {
                Ok(outcome) => outcome,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "fedimint: receive outcome is undecodable; backfilling conservatively"
                    );
                    None
                }
            };
            let expires_at = invoice
                .expires_at()
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            if !receive_backfill_needed(outcome.as_ref(), expires_at, now) {
                continue;
            }
            let Some(ext) = meta
                .extra_meta
                .get("lnrent_external_id")
                .and_then(|v| v.as_str())
            else {
                continue;
            };
            if idx_get_by_external(index, ext)?.is_some() {
                continue;
            }
            let op_hex = key.operation_id.fmt_full().to_string();
            let inv = Invoice {
                id: format!("fm-{op_hex}"),
                external_id: ext.to_string(),
                backend_invoice_id: op_hex.clone(),
                payment_hash: invoice.payment_hash().to_string(),
                bolt11: invoice.to_string(),
                amount_sat: invoice.amount_milli_satoshis().unwrap_or(0) / 1000,
                expires_at,
            };
            idx_insert(index, &inv, &op_hex)?;
            backfilled += 1;
        }
        last = page.last().map(|(k, _)| *k);
        if count < 100 {
            break;
        }
    }
    Ok(backfilled)
}

/// Whether a receive operation found ONLY in the oplog should be re-indexed as a live invoice on
/// recovery. Backfill iff it could still MATTER: it durably SETTLED (`Claimed` — the row is needed
/// for settlement catch-up / refund provenance), OR it is not yet expired and not terminally
/// `Canceled` (still able to receive a payment). A receive that is EXPIRED and not `Claimed` is
/// DEAD and must NEVER be resurrected as OPEN — this is the y4m.15 review P1: a legacy or
/// crash-window CANCELED invoice has an oplog outcome of `None` (the old receive task returned on
/// the terminal item before Fedimint's wrapper cached the outcome — same EOF-drain gap as the pay
/// dead-op ledger), so once the GC reaps its row, an outcome-only rule would re-backfill it as OPEN
/// on the next restart, `watch()` would respawn every historical invoice, and the storage bound the
/// GC exists to provide would be undone. The expiry gate closes that for every `None`/`Canceled`
/// dead row while still recovering a genuinely live (unexpired) or settled (`Claimed`) one.
///
/// `expires_at == 0` (an unparseable/absent bolt11 expiry) is treated as NOT expired, so a live
/// invoice with an odd expiry is never dropped — a missed settlement would be a money bug, whereas
/// the worst case here (re-indexing a rare no-expiry dead row) is a storage nit; fedimint always
/// sets an expiry, so this cannot mask a real dead row in practice.
fn receive_backfill_needed(outcome: Option<&LnReceiveState>, expires_at: i64, now: i64) -> bool {
    if matches!(outcome, Some(LnReceiveState::Claimed)) {
        return true;
    }
    let expired = expires_at != 0 && expires_at < now;
    !expired && !matches!(outcome, Some(LnReceiveState::Canceled { .. }))
}

/// Symmetric to [`recover_index_from_oplog`] but for OUTBOUND pays (lnrent-4gt). A crash in [`pay`]'s
/// window — between `pay_bolt11_invoice` committing the fedimint operation and `pay_idx_upsert`
/// persisting the local row — leaves the idempotency key with NO `fedimint_pay` row, so
/// `payment_status_by_key` reports `Unknown`. The Refunder's next `pay(key)` would then re-parse the
/// original bolt11 (which may have EXPIRED in the meantime) and fail before discovering the in-flight
/// op. Backfilling the row (the op id + ln/internal kind) from the oplog `extra_meta` on open lets
/// `pay(key)` take its early path and re-await the OPERATION directly. Backfilled as `PENDING`; the
/// next `pay(key)` reconstructs the `PayType` from (op, kind) and resolves it to terminal.
///
/// ALSO un-hides an unrecorded LIVE operation behind a stale FAILED row (lnrent-kum): a crash after
/// a same-key retry's `pay_bolt11_invoice` committed but before its upsert replaced the prior
/// attempt's FAILED row would otherwise leave the new op invisible, so a later drive retries AGAIN
/// while it is still in flight — both can settle, an operator double-pay. Per-entry decisions are
/// [`pay_recovery_action`] (pure; see its order-independence + dead-op-ledger arguments —
/// fedimint's oplog is wall-clock-ordered, so the decision must not rely on scan order); PENDING,
/// SUCCEEDED, correctly FAILED, and ledger-dead candidates never mutate anything. Returns
/// `(backfilled, replaced_stale_failed)`. IDEMPOTENT (a re-run changes nothing: replaced rows are
/// PENDING and ledger-dead candidates keep skipping); an undecodable oplog entry is logged +
/// skipped (matching the receive side); `join_or_open` is fail-closed on a pass-level error
/// (refuses to start).
async fn recover_pay_from_oplog(
    client: &ClientHandleArc,
    index: &Arc<Mutex<Connection>>,
    now: i64,
) -> Result<(usize, usize)> {
    let log = client.operation_log();
    let mut backfilled = 0usize;
    let mut replaced_stale_failed = 0usize;
    // key -> the candidates that could take its row (a BACKFILL for an absent row, or a REPLACE
    // for a stale FAILED row — a key's arm cannot change mid-scan because NOTHING is upserted
    // in-scan), collected across the WHOLE scan before ANY row is written. Both arms accumulate
    // for the same reason: two unindexed/unrecorded ops under one key are the SAME
    // cannot-represent-in-one-row ambiguity whichever row state they hide behind (adversarial
    // lnrent-kum review, round 6). Bounded per key (the Vec caps at MAX_LISTED; the count keeps
    // the true total) so one pathological key cannot balloon memory to O(oplog). BTreeMap so the
    // fail-closed diagnostic below is byte-deterministic across restarts.
    const MAX_LISTED: usize = 8;
    #[derive(Default)]
    struct KeyCandidates {
        /// This key's arm: an absent-row backfill (`true`) or a stale-FAILED replace (`false`).
        /// Cannot change mid-scan — nothing is upserted in-scan.
        is_backfill: bool,
        /// Up to [`MAX_LISTED`] possibly-LIVE `(op_hex, kind)` candidates for the PENDING slot.
        listed: Vec<(String, &'static str)>,
        /// TRUE total of possibly-live candidates (may exceed `listed.len()`).
        total_live: usize,
        /// Absent-row arm only: the smallest ledger-DEAD candidate (deterministic pick). Never a
        /// live candidate; used for a truthful FAILED bookkeeping backfill when NO live candidate
        /// exists, and what lets the ambiguity runbook unbrick an absent-row refusal.
        dead_min: Option<(String, &'static str)>,
    }
    let mut pending_writes: std::collections::BTreeMap<String, KeyCandidates> =
        std::collections::BTreeMap::new();
    let mut last = None;
    loop {
        let page = log.paginate_operations_rev(100, last).await;
        let count = page.len();
        if count == 0 {
            break;
        }
        for (key, entry) in &page {
            if entry.operation_module_kind() != fedimint_ln_common::KIND.as_str() {
                continue;
            }
            let meta: LightningOperationMeta = match entry.try_meta() {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(error = %e, "fedimint: skipping oplog entry with undecodable ln meta (pay recovery)");
                    continue;
                }
            };
            let LightningOperationMetaVariant::Pay(pay) = &meta.variant else {
                continue;
            };
            let Some(idk) = meta
                .extra_meta
                .get("lnrent_idempotency_key")
                .and_then(|v| v.as_str())
            else {
                continue;
            };
            let op_hex = key.operation_id.fmt_full().to_string();
            let existing = pay_idx_get(index, idk)?;
            let existing_ref = existing
                .as_ref()
                .map(|(op, status, _)| (op.as_str(), status.as_str()));
            // Dead-candidate test = OUR OWN dead-op ledger (`fedimint_pay_dead_op`), written by
            // `await_pay` the moment it observes a definitive failure. Deliberately NOT any
            // fedimint-side signal — three adversarial review rounds refuted those in turn: the
            // oplog is wall-clock-ordered (not causal), the outcome cache is only written on an
            // EOF-drain `await_pay` never performs, and PROBING via subscribe can itself poison
            // that cache (the wrapper caches the last emitted update on an EOF-without-terminal).
            // The ledger is a local instant lookup with none of those dependencies.
            let candidate_dead = match existing_ref {
                // Both arms that might WRITE consult the ledger: the FAILED arm so a dead
                // candidate never resurrects, and the ABSENT arm so a dead candidate never counts
                // as live toward the ambiguity refusal (codex PR-32 P1: otherwise the operator's
                // runbook insert could never unbrick an absent-row refusal).
                Some((row_op, "FAILED")) if row_op != op_hex => {
                    pay_idx_is_dead(index, &op_hex)?
                }
                None => pay_idx_is_dead(index, &op_hex)?,
                _ => false,
            };
            let kind = if pay.is_internal_payment {
                "internal"
            } else {
                "ln"
            };
            // COLLECT rather than write in-scan — for BOTH arms — so every competing candidate
            // for a key is seen before one is chosen (the row state stays constant during the
            // scan, so later same-key candidates are never masked by an earlier write).
            match pay_recovery_action(existing_ref, &op_hex, candidate_dead) {
                PayRecoveryAction::Skip => {}
                PayRecoveryAction::BackfillDead => {
                    let entry = pending_writes.entry(idk.to_owned()).or_default();
                    entry.is_backfill = true;
                    if entry
                        .dead_min
                        .as_ref()
                        .is_none_or(|(existing_min, _)| op_hex < *existing_min)
                    {
                        entry.dead_min = Some((op_hex.clone(), kind));
                    }
                }
                action @ (PayRecoveryAction::Backfill | PayRecoveryAction::ReplaceStaleFailed) => {
                    let entry = pending_writes.entry(idk.to_owned()).or_default();
                    entry.is_backfill = action == PayRecoveryAction::Backfill;
                    if entry.listed.len() < MAX_LISTED {
                        entry.listed.push((op_hex.clone(), kind));
                    }
                    entry.total_live += 1;
                }
            }
        }
        last = page.last().map(|(k, _)| *k);
        if count < 100 {
            break;
        }
    }
    // FAIL CLOSED on ambiguity BEFORE writing anything (all-or-nothing — NOTHING was upserted
    // in-scan, so a bail here leaves the index byte-identical, matching the bootstrap
    // discipline): in the post-ledger steady state at most ONE unrecorded candidate can exist per
    // key (a second same-key op only ever starts after the first resolved and — for failures —
    // was ledgered), so a competing set means pre-ledger orphans and/or clock-pathology crash
    // artifacts racing a possibly-live op. The single-op row cannot represent that: whichever op
    // it re-awaits, an unchosen candidate could still settle while a later FAILED-driven retry
    // starts a fresh payment — the double-pay this bead exists to close. No automatic choice is
    // money-safe, so REFUSE TO START and hand the operator the exact op ids (adversarial
    // lnrent-kum review, rounds 4-6; full per-key enumeration is lnrent-7so). BTreeMap iteration
    // + sorted op ids keep the refusal text deterministic across restarts.
    let ambiguous: Vec<String> = pending_writes
        .iter()
        .filter(|(_, entry)| entry.total_live > 1)
        .map(|(idk, entry)| {
            let mut ops: Vec<&str> = entry.listed.iter().map(|(op, _)| op.as_str()).collect();
            ops.sort_unstable();
            format!(
                "key {idk}: {} competing unrecorded ops (listing up to {MAX_LISTED}): {ops:?}",
                entry.total_live
            )
        })
        .collect();
    if !ambiguous.is_empty() {
        anyhow::bail!(
            "fedimint pay recovery: MULTIPLE unrecorded operations compete for the same \
             idempotency key (pre-ledger history or a crash under clock pathology) — no automatic \
             choice is money-safe, refusing to start. Verify each op's true state (fedimint oplog \
             / gateway logs), record the genuinely dead ones with: INSERT INTO \
             fedimint_pay_dead_op (operation_id) VALUES ('<op>'); in the pay index DB, then \
             restart. Details: {}",
            ambiguous.join("; ")
        );
    }
    for (idk, entry) in pending_writes {
        if let Some((op_hex, kind)) = entry.listed.first() {
            // Exactly one possibly-live candidate: it takes the PENDING slot (re-awaited by the
            // drivers; the y4m.16 machinery lands its real outcome). Stamped with the recovery `now`
            // (lnrent-y4m.19) so a recovered row is reapable once terminal, never stranded NULL.
            pay_idx_upsert(index, &idk, op_hex, "PENDING", kind, now)?;
        } else if let Some((op_hex, kind)) = &entry.dead_min {
            // Absent row whose every candidate is ledger-dead (e.g. the operator just recorded
            // them per the ambiguity runbook): backfill the TRUTH — FAILED — so the key stops
            // reading Unknown and the drivers may retry normally (funds provably back).
            pay_idx_upsert(index, &idk, op_hex, "FAILED", kind, now)?;
        } else {
            continue;
        }
        if entry.is_backfill {
            backfilled += 1;
        } else {
            replaced_stale_failed += 1;
        }
    }
    Ok((backfilled, replaced_stale_failed))
}

// ---- the lnrent-owned sqlite index (sync; guards never cross an await) ---------------------------

fn idx_get_by_external(index: &Mutex<Connection>, ext: &str) -> Result<Option<Invoice>> {
    let conn = index.lock().unwrap();
    let inv = conn
        .query_row(
            "SELECT external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at
               FROM fedimint_invoice WHERE external_id = ?1",
            params![ext],
            |r| {
                Ok(Invoice {
                    external_id: r.get::<_, String>(0)?,
                    backend_invoice_id: r.get::<_, String>(1)?,
                    id: r.get::<_, String>(2)?,
                    bolt11: r.get::<_, String>(3)?,
                    payment_hash: r.get::<_, String>(4)?,
                    amount_sat: r.get::<_, i64>(5)? as u64,
                    expires_at: r.get::<_, i64>(6)?,
                })
            },
        )
        .optional()?;
    Ok(inv)
}

fn idx_get_status_by_invoice_id(
    index: &Mutex<Connection>,
    id: &str,
) -> Result<Option<(String, i64)>> {
    let conn = index.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT status, expires_at FROM fedimint_invoice WHERE invoice_id = ?1",
            params![id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()?;
    Ok(row)
}

/// Like [`idx_get_status_by_invoice_id`] but ALSO surfaces the stored `settled_at` (NULL for OPEN or
/// for a cached/recovery Claimed; `Some(ts)` only for a LIVE Claimed) — the provenance the supervisor
/// catch-up needs to use a live time exactly vs. cap a recovery (lnrent-zwk).
fn idx_get_settlement_by_invoice_id(
    index: &Mutex<Connection>,
    id: &str,
) -> Result<Option<(String, i64, Option<i64>)>> {
    let conn = index.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT status, expires_at, settled_at FROM fedimint_invoice WHERE invoice_id = ?1",
            params![id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(row)
}

fn idx_list_open(index: &Mutex<Connection>) -> Result<Vec<OpenRow>> {
    let conn = index.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT external_id, operation_id, invoice_id, amount_sat
           FROM fedimint_invoice WHERE status = 'OPEN'",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(OpenRow {
                external_id: r.get::<_, String>(0)?,
                operation_id: r.get::<_, String>(1)?,
                invoice_id: r.get::<_, String>(2)?,
                amount_sat: r.get::<_, i64>(3)? as u64,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn idx_insert(index: &Mutex<Connection>, inv: &Invoice, op_hex: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO fedimint_invoice
            (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at, status)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'OPEN')
         ON CONFLICT(external_id) DO NOTHING",
        params![
            inv.external_id,
            op_hex,
            inv.id,
            inv.bolt11,
            inv.payment_hash,
            inv.amount_sat as i64,
            inv.expires_at,
        ],
    )?;
    Ok(())
}

/// Mark an invoice PAID by its operation id. `settled_at` is `Some` for a LIVE settlement (recorded)
/// and `None` for a cached one (left NULL — the supervisor catch-up supplies a safe timestamp).
fn idx_mark_paid(index: &Mutex<Connection>, op_hex: &str, settled_at: Option<i64>) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE fedimint_invoice SET status = 'PAID', settled_at = COALESCE(?2, settled_at)
           WHERE operation_id = ?1",
        params![op_hex, settled_at],
    )?;
    Ok(())
}

/// Terminal receive cancel (expiry): take the row out of the `idx_list_open` respawn set so
/// `watch()` never re-subscribes a dead invoice again. Guarded on `status='OPEN'` so a late
/// Canceled event can never demote a row a concurrent Claimed already marked PAID. `lookup`
/// still derives Open/Expired from `expires_at` for any non-PAID status, so reads are unchanged.
fn idx_mark_canceled(index: &Mutex<Connection>, op_hex: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE fedimint_invoice SET status = 'CANCELED'
           WHERE operation_id = ?1 AND status = 'OPEN'",
        params![op_hex],
    )?;
    Ok(())
}

/// Dup-tolerant `fedimint_pay` GC schema migration (lnrent-y4m.19): add the `created_at` column if a
/// legacy DB lacks it, then create the covering index that backs the pay reap predicate. `created_at`
/// is in the `fedimint_pay` CREATE TABLE (INDEX_SCHEMA), so a FRESH DB already has it and the ALTER is
/// skipped; a DB created before y4m.19 has the table WITHOUT it, and SQLite has no `ADD COLUMN IF NOT
/// EXISTS` (and `execute_batch` won't add a column to an existing table), so the column is added here,
/// guarded by a PRAGMA-table_info check (mirrors store.rs's `ensure_refund_resolution_columns`). The
/// covering index is created HERE, not in INDEX_SCHEMA, because it references `created_at`: on a legacy
/// DB that column does not exist until this ALTER runs, so a `CREATE INDEX ... (status, created_at)`
/// inside the INDEX_SCHEMA batch would abort before the column is added. Idempotent across re-runs
/// (both the column check and the `IF NOT EXISTS` index guard): a fresh DB, a legacy DB, and a re-run
/// all open cleanly.
fn ensure_fedimint_pay_gc_schema(conn: &Connection) -> Result<()> {
    if !index_has_column(conn, "fedimint_pay", "created_at")? {
        conn.execute_batch("ALTER TABLE fedimint_pay ADD COLUMN created_at INTEGER")?;
    }
    // Covering index for the y4m.19 reap DELETE predicate (status='FAILED' AND created_at < cutoff) so
    // it does not full-scan `fedimint_pay` while holding the sole money-path `index` mutex — the same
    // reasoning as y4m.15's `fedimint_invoice_gc_idx`. Created after the ALTER so `created_at` exists.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS fedimint_pay_gc_idx ON fedimint_pay (status, created_at)",
    )?;
    Ok(())
}

/// Whether `table` has a column named `column` (PRAGMA table_info). A local equivalent of
/// store.rs's non-`pub` `has_column`, backing the dup-tolerant migration above. Returns `false` for a
/// non-existent table (empty PRAGMA result), which is fine: the only caller runs it after the schema
/// batch has created the table.
fn index_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
    for name in rows {
        if name? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Reap past-retention CANCELED `fedimint_invoice` rows, returning the number deleted. A
/// distinct-`external_id` unpaid-order flood inserts one CANCELED row per expired request and
/// previously retained it forever — this is the flood fix.
///
/// The cutoff is clock-capped exactly as store.rs does (`< MIN(unixepoch(), ?now) - ?retention`):
/// `MIN` pins it to whichever of real wall time or injected `now` is earlier, so a clock running ahead
/// cannot reap a fresh row.
///
/// ONLY CANCELED rows are reaped. OPEN rows may still settle and are never touched. PAID rows are
/// deliberately NOT reaped here, for two independent reasons (both raised in round-12 review):
///  - It would be INEFFECTIVE: `recover_index_from_oplog` re-backfills any reaped PAID op from its
///    cached `Claimed` oplog outcome (`receive_backfill_needed(Some(Claimed), ..) == true`) as an
///    OPEN row, which `watch()`→`run_receive_task(live=false)` then re-marks PAID via
///    `idx_mark_paid(None)` — leaving `settled_at` NULL and the row PERMANENTLY un-reapable. The reap
///    yields no durable storage bound across restarts, only churn.
///  - It would be UNSAFE: the PAID row is the record the supervisor settlement catch-up reads via
///    `lookup`/`lookup_settlement`. A live settlement whose credit never reached the store (dropped
///    `Settlement` send, missed capture) leaves the store invoice OPEN while this row is PAID;
///    deleting it on age alone — before capture is durable — could strand a real payment.
///
/// PAID rows are also NOT a free-flood vector (a PAID row means a buyer actually paid, so it is
/// bounded by real economic activity, exactly like `fedimint_pay`). Safe PAID-row tidy-up needs a
/// trustworthy, recovery-stable timestamp and cross-DB proof the store captured the settlement; that
/// design is deferred to the follow-up bead **lnrent-y4m.19** (which already owns the analogous
/// `fedimint_pay` GC), not bolted on here.
fn gc_fedimint_invoice_index(
    index: &Mutex<Connection>,
    now: i64,
    retention_secs: i64,
) -> Result<usize> {
    // FLOOD FIX: past-retention CANCELED rows only (see the fn doc for why PAID rows are left).
    // CHUNKED (adversarial y4m.15 review P2): the first sweep on a flooded DB can face hundreds of
    // thousands of rows, and this DELETE holds the sole `index` mutex that create/lookup/settlement
    // take SYNCHRONOUSLY on async worker threads — one unbounded statement would stall the whole
    // fedimint money path for its duration. Delete in bounded batches by rowid, RELEASING the lock
    // between batches so those callers interleave. `expires_at > 0` excludes a no/unparseable-expiry
    // row (the review P3): its `0` sidesteps the retention floor AND recovery keeps it live
    // (`receive_backfill_needed(None, 0, _) == true`), so reaping it would just churn a
    // reap→rebackfill loop across restarts — GC and recovery must agree to leave it.
    const BATCH: usize = 512;
    let mut total = 0usize;
    loop {
        let deleted = {
            let conn = index.lock().unwrap();
            conn.execute(
                "DELETE FROM fedimint_invoice WHERE rowid IN (
                     SELECT rowid FROM fedimint_invoice
                      WHERE status = 'CANCELED'
                        AND expires_at > 0
                        AND expires_at < MIN(unixepoch(), ?1) - ?2
                      LIMIT ?3)",
                params![now, retention_secs, BATCH],
            )?
        }; // lock released here — other index callers can run before the next batch
        total += deleted;
        if deleted < BATCH {
            break;
        }
    }
    Ok(total)
}

/// Reap definitively `FAILED` `fedimint_pay` rows past a LONG, crash-redrive-safe retention
/// ([`FEDIMINT_PAY_RETENTION_SECS`]), returning the number deleted (lnrent-y4m.19). Mirrors
/// [`gc_fedimint_invoice_index`] EXACTLY in shape: CHUNKED (BATCH=512) with the `index` mutex RELEASED
/// between batches so the money path interleaves, cutoff clock-capped as store.rs does
/// (`< MIN(unixepoch(), ?now) - ?retention`, so a clock running ahead cannot reap a fresh row), backed
/// by the `fedimint_pay_gc_idx` covering index.
///
/// Three invariants keep this from dropping LIVE idempotency state:
///  - PENDING rows are NEVER reaped (`status = 'FAILED'` only): a PENDING row is still
///    in-flight / re-drivable, and deleting it would let a later `pay(key)` start a SECOND op instead
///    of re-awaiting the one already in flight.
///  - SUCCEEDED rows are NEVER reaped. They are the durable implementation of
///    [`PaymentBackend::pay`]'s same-key-never-pays-twice contract, and age cannot prove the separate
///    store has no still-PENDING refund/sweep driver. An absent key makes
///    [`FedimintPayment::pay_inner`] start a new operation; retaining the row is therefore the smallest
///    correct answer to the round-3 P1, not optional hardening.
///  - Rows with a NULL `created_at` are NEVER reaped (`created_at IS NOT NULL`): a legacy pre-y4m.19
///    row (or any row whose age we cannot prove) is treated as "not yet old enough" — the conservative
///    floor, exactly as y4m.15 leaves `expires_at = 0` invoice rows.
///
/// A reaped FAILED operation remains in Fedimint's permanent oplog. Its deliberately retained dead-op
/// marker makes absent-row recovery classify it as `BackfillDead`, so it cannot become a live
/// resurrection candidate; on the next open `recover_pay_from_oplog` re-writes the row as FAILED with a
/// fresh `created_at`. That re-backfill fires on EVERY restart, so this reap is intra-run tidiness, not
/// durable pruning: a real-op FAILED row only reaches reap age after ~180d of UNBROKEN uptime and, once
/// reaped, returns (retention clock reset) the moment the daemon restarts. The lone exception is a
/// `fail_pay_preflight` row (synthetic op `"(preflight-failed)"`, no oplog entry): recovery cannot
/// re-backfill it, so a reaped one stays absent (Unknown) until a driver re-attempts — a deterministic
/// re-fail that pays nothing, since a preflight park means no operation was ever started.
///
/// One accepted, restart-recoverable liveness edge follows, and it is NOT a double-pay — reaping only
/// FAILED (never SUCCEEDED) keeps money-safety. A reaped row reads Unknown, and for a RESOLVED
/// (LN-address/LNURL) refund `refund::plan_payment` treats Unknown as "re-await the persisted invoice,
/// never re-resolve" (its crash-window contract, so an in-flight HTLC is never double-paid). An operator
/// `refund-retry` of a refund that had parked FAILED with an EXPIRED resolved invoice therefore reuses
/// the dead invoice against a reaped row — until the next restart re-backfills FAILED and the retry
/// re-resolves normally. Making that impossible would require coupling the reap to the SEPARATE store's
/// refund state (an explicit NON-GOAL); a definitive FAILED means funds are provably back, so the worst
/// case is this recoverable delay, never a lost or duplicated payment.
///
/// The dead-op ledger (`fedimint_pay_dead_op`) is deliberately LEFT UN-GC'd — see [`gc_index_if_due`]
/// for the reasoned deferral (it is not a flood vector, and reaping it would reintroduce a startup
/// ambiguity brick for a double-failed refund key).
fn gc_fedimint_pay_index(
    index: &Mutex<Connection>,
    now: i64,
    retention_secs: i64,
) -> Result<usize> {
    // CHUNKED for the same reason as the invoice reaper: this DELETE holds the sole `index` mutex that
    // pay/lookup/settlement take SYNCHRONOUSLY on async worker threads, so it must not run unbounded.
    // `fedimint_pay` is bounded by refund/sweep volume (not a free flood), so the backlog is small in
    // practice, but the chunked shape is kept identical for safety and to match y4m.15.
    const BATCH: usize = 512;
    let mut total = 0usize;
    loop {
        let deleted = {
            let conn = index.lock().unwrap();
            conn.execute(
                "DELETE FROM fedimint_pay WHERE rowid IN (
                     SELECT rowid FROM fedimint_pay
                      WHERE status = 'FAILED'
                        AND created_at IS NOT NULL
                        AND created_at < MIN(unixepoch(), ?1) - ?2
                      LIMIT ?3)",
                params![now, retention_secs, BATCH],
            )?
        }; // lock released here — other index callers can run before the next batch
        total += deleted;
        if deleted < BATCH {
            break;
        }
    }
    Ok(total)
}

/// Deterministic throttle gate for the create-path index GC (lnrent-y4m.15): under `last`'s lock,
/// whether a GC is DUE (>= `interval_secs` since the last run) and, when due, STAMP `last = now` so a
/// burst of creates triggers at most ONE reap per interval. Returns whether the caller should run it.
/// Time-based via the injected clock's `now`, so the throttle is unit-tested with a `TestClock` and
/// needs no live federation.
fn index_gc_due_and_stamp(last: &Mutex<i64>, now: i64, interval_secs: i64) -> bool {
    let mut last = last.lock().unwrap();
    // A corrected-backward wall clock must not leave a future stamp suppressing GC indefinitely.
    if now >= *last && now - *last < interval_secs {
        return false;
    }
    *last = now;
    true
}

// ---- the lnrent-owned outbound-pay index (refund idempotency, keyed by idempotency_key) ----------

/// `(operation_id_hex, status, pay_kind)` for a refund key, if any.
fn pay_idx_get(index: &Mutex<Connection>, key: &str) -> Result<Option<(String, String, String)>> {
    let conn = index.lock().unwrap();
    let row = conn
        .query_row(
            "SELECT operation_id, status, pay_kind FROM fedimint_pay WHERE idempotency_key = ?1",
            params![key],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(row)
}

fn pay_idx_status_by_op(index: &Mutex<Connection>, op_hex: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    let s = conn
        .query_row(
            "SELECT status FROM fedimint_pay WHERE operation_id = ?1",
            params![op_hex],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    Ok(s)
}

fn pay_idx_status_by_key(index: &Mutex<Connection>, key: &str) -> Result<Option<String>> {
    let conn = index.lock().unwrap();
    let s = conn
        .query_row(
            "SELECT status FROM fedimint_pay WHERE idempotency_key = ?1",
            params![key],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    Ok(s)
}

/// Insert (or, on a FAILED-then-retry, replace) the pay row for a key as PENDING under a new op.
/// `now` (the caller's `self.clock`/recovery clock) stamps `created_at` so the y4m.19 pay GC can reap
/// this row if it later becomes FAILED and ages past retention; [`pay_idx_mark`] refreshes it again
/// when the op terminalizes so even a long-PENDING attempt gets the full FAILED-row retention. On the
/// ON CONFLICT (retry) branch `created_at` is REFRESHED to `now`: a retry is a fresh lifecycle, so its
/// retention clock must restart — a retried key must never be reaped on the original attempt's age.
fn pay_idx_upsert(
    index: &Mutex<Connection>,
    key: &str,
    op_hex: &str,
    status: &str,
    kind: &str,
    now: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO fedimint_pay
             (idempotency_key, operation_id, backend_pay_id, status, pay_kind, created_at)
         VALUES (?1, ?2, ?2, ?3, ?4, ?5)
         ON CONFLICT(idempotency_key)
           DO UPDATE SET operation_id = ?2, backend_pay_id = ?2, status = ?3, pay_kind = ?4,
                         created_at = ?5",
        params![key, op_hex, status, kind, now],
    )?;
    Ok(())
}

/// Mark a pay row's terminal status, guarded by the OPERATION the waiter actually awaited (a CAS,
/// adversarial y4m.16 review): `pay_start_lock` is released before settlement is awaited, so two
/// callers can await the SAME key concurrently. Without the operation guard a DELAYED waiter that
/// observed operation A's definitive failure could clobber the row AFTER a fresh operation B was
/// started and upserted PENDING under the same key — a later drive would then see FAILED and start a
/// THIRD operation while B is still in flight (a double-pay). Guarding on `operation_id` makes a
/// stale waiter's mark a harmless no-op (0 rows), while the live waiter's op always matches.
///
/// The same guarded update refreshes `created_at` to the terminal-observation `now` for either terminal
/// status (GC consults it only for FAILED). A pay can remain PENDING longer than
/// [`FEDIMINT_PAY_RETENTION_SECS`]; retaining its original start time would make a newly-FAILED row
/// immediately reapable instead of preserving its recovery state for the documented 180-day window.
/// Refreshing inside this operation-guarded CAS starts that full window at terminalization without
/// letting a stale waiter extend or mutate a newer operation's row.
fn pay_idx_mark(
    index: &Mutex<Connection>,
    key: &str,
    op_hex: &str,
    status: &str,
    now: i64,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE fedimint_pay SET status = ?2, created_at = ?4
          WHERE idempotency_key = ?1 AND operation_id = ?3",
        params![key, status, op_hex, now],
    )?;
    Ok(())
}

/// Append an operation to the DEAD-OP LEDGER (`fedimint_pay_dead_op`, lnrent-kum): `await_pay`
/// records every op it observes reaching a DEFINITIVE failure (funds provably back), the moment it
/// observes it and regardless of the row CAS outcome — dead-ness is a property of the OPERATION,
/// not the row. Crash recovery consults this ledger so a dead op is never resurrected behind a
/// stale FAILED row (no burned attempts, no restart oscillation). Append-only, idempotent.
fn pay_idx_record_dead(index: &Mutex<Connection>, op_hex: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO fedimint_pay_dead_op (operation_id) VALUES (?1)",
        params![op_hex],
    )?;
    Ok(())
}

/// Whether an operation is in the dead-op ledger (see [`pay_idx_record_dead`]).
fn pay_idx_is_dead(index: &Mutex<Connection>, op_hex: &str) -> Result<bool> {
    let conn = index.lock().unwrap();
    let dead = conn
        .query_row(
            "SELECT 1 FROM fedimint_pay_dead_op WHERE operation_id = ?1",
            params![op_hex],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(dead)
}

fn map_pay_status(s: Option<String>) -> PayStatus {
    match s.as_deref() {
        Some("SUCCEEDED") => PayStatus::Succeeded,
        Some("FAILED") => PayStatus::Failed,
        Some("PENDING") => PayStatus::Pending,
        _ => PayStatus::Unknown,
    }
}

/// The gateway's advertised fee in MSATS for an outgoing payment of `pay_msat`, computed EXACTLY as
/// Fedimint applies it when funding the outgoing contract — `RoutingFees::to_amount`
/// (fedimint-ln-common `config.rs`), used at `LightningClientModule::pay_bolt11_invoice` as
/// `contract_amount = invoice_amount + to_amount(invoice_amount)` (fedimint-ln-client `lib.rs`). The
/// formula is `base_msat + (ppm>0 ? payment_msat / (1_000_000 / ppm) : 0)`, with INTEGER division on
/// BOTH steps. This MUST be mirrored byte-for-byte rather than the algebraically tempting
/// `payment*ppm/1_000_000`: when `ppm` does not divide `1_000_000` the truncated divisor makes the
/// REAL fee strictly larger, so the naive form would under-quote the cap and let a refund spend more
/// than the gross received — an INV-1 drain (review P1). Returns `None` when the schedule is unpayable:
/// `ppm > 1_000_000` (a >100% proportional fee) divides to a zero `fee_percent`, which Fedimint itself
/// would hit as `payment.msats / 0`; the caller treats it as "no positive payout fits", never zero fee.
fn gateway_fee_msat(base_msat: u64, ppm: u64, pay_msat: u128) -> Option<u128> {
    let base = u128::from(base_msat);
    if ppm == 0 {
        return Some(base); // no proportional component
    }
    let fee_percent = 1_000_000u128 / u128::from(ppm);
    if fee_percent == 0 {
        return None; // ppm > 1_000_000: Fedimint's `payment.msats / 0` — unpayable
    }
    Some(base + pay_msat / fee_percent)
}

/// The largest whole-sat refund payout `n` for a `gross_sat` received amount such that the total
/// outlay — payout plus the gateway's advertised fee on that payout — never exceeds `gross_sat`
/// (INV-1, spec §3.1). The fee is Fedimint's ACTUAL fee ([`gateway_fee_msat`]); by contract `n == 0`
/// is NO payment, hence zero payout AND zero fee, so `valid(0)` is always true and the result is
/// well-defined even when `base_msat > gross_sat*1000` (the empty-set case a naive predicate would
/// produce). Returns `0` only for true dust (no positive whole-sat payout fits), including any
/// unpayable >100% schedule. All msat/fee arithmetic is widened to `u128` so `pay_msat = n*1000`
/// cannot overflow; the result is a binary search of the monotone `valid` predicate — NOT the
/// closed-form msat inverse, which under-floors (e.g. base=0, ppm=1, gross=1 is payable at 1 sat, but
/// the closed form yields 0 sats).
fn net_payout_sat(base_msat: u64, ppm: u64, gross_sat: u64) -> u64 {
    let r_msat = u128::from(gross_sat) * 1000;
    // valid(n): pay_msat(n) + fee_msat(n) <= R_msat, all widened. valid(0) is unconditionally true.
    let valid = |n: u64| -> bool {
        if n == 0 {
            return true;
        }
        let pay_msat = u128::from(n) * 1000;
        match gateway_fee_msat(base_msat, ppm, pay_msat) {
            Some(fee_msat) => pay_msat + fee_msat <= r_msat,
            None => false, // unpayable fee schedule -> no positive payout fits
        }
    };
    // Largest n in [0, gross_sat] satisfying the monotone-decreasing predicate; lo is always valid.
    let mut lo = 0u64;
    let mut hi = gross_sat;
    while lo < hi {
        // Upper mid (lo < mid <= hi): guarantees progress when valid(mid) sets lo=mid, and avoids the
        // `hi - lo + 1` overflow when gross_sat == u64::MAX.
        let mid = hi - (hi - lo) / 2;
        if valid(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// PURE, generic ordered-failover selector (lnrent-y4m.8): try each gateway pubkey IN ORDER via
/// `try_one`, returning the FIRST that resolves to `Ok(Some(gw))` — a REACHABLE gateway. A pubkey
/// that resolves to `Ok(None)` (unregistered / not found) OR `Err(_)` (a transient probe failure) is
/// SKIPPED and selection continues to the next, maximizing failover resilience. Only when EVERY
/// configured gateway is exhausted does it fail CLOSED with a clear error, carrying the last
/// underlying `Err` as context so a diagnosable transient reason is not swallowed. `gateways` should
/// be non-empty (the live wrapper handles the empty "pick any available gateway" case before calling
/// this); an empty slice here simply yields the same fail-closed error without probing. Generic over
/// the gateway type `G` and free of the live client — following the `net_payout_sat` precedent — so
/// the DECISION logic is unit-tested without a federation.
async fn select_first_reachable<G, F, Fut>(gateways: &[PublicKey], try_one: F) -> Result<G>
where
    F: Fn(PublicKey) -> Fut,
    Fut: Future<Output = Result<Option<G>>>,
{
    let mut last_err: Option<anyhow::Error> = None;
    for &pk in gateways {
        match try_one(pk).await {
            Ok(Some(gw)) => return Ok(gw),
            Ok(None) => {}                // registered-but-not-found: try the next in order
            Err(e) => last_err = Some(e), // transient probe failure: remember why, try the next
        }
    }
    match last_err {
        Some(e) => Err(e).context("no configured lightning gateway is reachable"),
        None => anyhow::bail!("no configured lightning gateway is reachable"),
    }
}

/// PURE preference-ordering for the quote/pay gateway binding (lnrent-y4m.18): the probe order the live
/// [`FedimintPayment::select_gateway_preferring`] hands to [`select_first_reachable`]. `preferred` (the
/// gateway a refund's fee QUOTE selected in this attempt) is tried FIRST, then the configured order,
/// with the preferred pk DE-DUPED out of the tail so it is never probed twice. A `preferred` NOT in the
/// configured list is STILL placed first — it was reachable at quote time, so it is the best guess for
/// the pay even if the operator never listed it. `None` yields the configured order unchanged (the
/// pre-y4m.18 selection). Free of the live client — following the `net_payout_sat` /
/// `select_first_reachable` precedent — so the ORDER decision is unit-tested without a federation.
fn ordered_with_preference(preferred: Option<PublicKey>, gateways: &[PublicKey]) -> Vec<PublicKey> {
    let mut ordered = Vec::with_capacity(gateways.len() + 1);
    if let Some(pk) = preferred {
        ordered.push(pk);
    }
    for &pk in gateways {
        if Some(pk) != preferred {
            ordered.push(pk);
        }
    }
    ordered
}

/// PURE money-safety class of a fedimint pay state (lnrent-y4m.16). Splits a terminal pay outcome
/// into "definitively failed" (funds provably returned or never left — safe to mark FAILED and let
/// a later drive start a FRESH payment) versus "ambiguous" (cannot prove the recipient was unpaid —
/// must NEVER be retried, or the daemon could send the money a SECOND time). Free of the live client
/// — following the `net_payout_sat` / `select_first_reachable` / `ordered_with_preference` precedent
/// — so the CLASSIFICATION is unit-tested variant-by-variant without a federation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayStateClass {
    /// Preimage in hand — the recipient is provably paid. `await_pay` marks the row SUCCEEDED.
    Success,
    /// Not terminal yet — keep awaiting the stream. No money decision to make.
    InFlight,
    /// Funds provably returned or never left the wallet, so THIS attempt sent nothing. `await_pay`
    /// marks the row FAILED; a later drive may safely start a fresh payment.
    DefinitiveFailure,
    /// Terminal, but the recipient's paid/unpaid status is UNKNOWN (a post-funding local error, or a
    /// refund that was itself attempted and errored). `await_pay` writes NO mark — the row stays
    /// PENDING — and re-awaits the SAME operation forever. That is the INTENDED fail-safe: a
    /// terminally-ambiguous op replays the same state on every re-subscribe, so it needs an
    /// operator/fedimint resolution, never an automatic second send. Marking FAILED here would let a
    /// later drive start a SECOND payment against a possibly-already-paid invoice (a double-pay).
    Ambiguous,
}

/// Classify an [`LnPayState`] into its [`PayStateClass`] (lnrent-y4m.16). EXHAUSTIVE (no `_` arm) on
/// purpose: a future new fedimint variant is a COMPILE ERROR here, forcing a deliberate money-safety
/// classification rather than silently defaulting to FAILED (and thus a retry).
fn classify_ln_pay_state(state: &LnPayState) -> PayStateClass {
    match state {
        // Preimage in hand — the recipient is provably paid.
        LnPayState::Success { .. } => PayStateClass::Success,
        // The outgoing contract is being created / funded / settled, or a refund is pending — not
        // terminal, keep awaiting (behavior unchanged).
        LnPayState::Created
        | LnPayState::Funded { .. }
        | LnPayState::AwaitingChange
        | LnPayState::WaitingForRefund { .. } => PayStateClass::InFlight,
        // The federation REFUNDED the outgoing contract — the funds are provably back in the wallet,
        // so this attempt sent nothing. DEFINITIVE failure: a fresh pay is safe.
        LnPayState::Refunded { .. } => PayStateClass::DefinitiveFailure,
        // The outgoing contract was CANCELED before it was funded — no money ever left. DEFINITIVE
        // failure: a fresh pay is safe.
        LnPayState::Canceled => PayStateClass::DefinitiveFailure,
        // A local error AFTER the state machine may already have acted on the payment. This is NOT
        // proof the recipient was unpaid — it can be a post-payment bookkeeping failure — so it is
        // AMBIGUOUS: never retry (that risks a double-send); leave the row PENDING.
        LnPayState::UnexpectedError { .. } => PayStateClass::Ambiguous,
    }
}

/// Classify an [`InternalPayState`] (a federation-internal, gateway-less pay) into its
/// [`PayStateClass`] (lnrent-y4m.16). EXHAUSTIVE, same discipline as [`classify_ln_pay_state`].
fn classify_internal_pay_state(state: &InternalPayState) -> PayStateClass {
    match state {
        // Preimage decrypted — the recipient is provably paid.
        InternalPayState::Preimage(_) => PayStateClass::Success,
        // Still funding the incoming contract — not terminal, keep awaiting (behavior unchanged).
        InternalPayState::Funding => PayStateClass::InFlight,
        // The contract funding failed outright — no money ever left. DEFINITIVE failure: safe to
        // start a fresh pay.
        InternalPayState::FundingFailed { .. } => PayStateClass::DefinitiveFailure,
        // The internal payment was refunded successfully — funds are provably back. DEFINITIVE
        // failure: safe to start a fresh pay.
        InternalPayState::RefundSuccess { .. } => PayStateClass::DefinitiveFailure,
        // A refund was ATTEMPTED and itself errored — the funds' whereabouts are UNKNOWN (paid,
        // refunded, or stuck). AMBIGUOUS: never retry; leave the row PENDING.
        InternalPayState::RefundError { .. } => PayStateClass::Ambiguous,
        // An unclassified local error — not proof the recipient was unpaid. AMBIGUOUS: never retry;
        // leave the row PENDING.
        InternalPayState::UnexpectedError(_) => PayStateClass::Ambiguous,
    }
}

/// PURE per-entry decision for [`recover_pay_from_oplog`] (lnrent-kum): what recovery does with an
/// oplog pay operation given the key's existing `fedimint_pay` row. Free of the live client —
/// following the `net_payout_sat` / `select_first_reachable` / `ordered_with_preference` /
/// `classify_ln_pay_state` precedent — so which OPERATION a recovered row points at (the double-pay
/// decision) is unit-tested without a federation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayRecoveryAction {
    /// No row for this key and the op is NOT ledger-dead — the daemon crashed before the FIRST
    /// `pay_idx_upsert` (lnrent-4gt). Upsert the oplog op as PENDING (the original backfill).
    Backfill,
    /// No row for this key but the op IS in the dead-op ledger (it definitively failed under our
    /// eyes; only the row write was lost, or the operator just recorded it per the ambiguity
    /// runbook). It must NOT count as a live candidate — that would keep the ambiguity refusal
    /// firing after the operator resolves it, bricking startup (codex PR-32 P1) — and its
    /// bookkeeping backfill is FAILED, not PENDING (truthful: funds provably back, drivers may
    /// retry normally).
    BackfillDead,
    /// The row says FAILED but points at a DIFFERENT operation than this oplog entry (including the
    /// `"(preflight-failed)"` placeholder), and the entry is not a ledger-dead candidate. Upsert
    /// the oplog op as PENDING so the drivers re-await it instead of retrying behind its back (the
    /// lnrent-kum double-pay window).
    ReplaceStaleFailed,
    /// The row already reflects this key faithfully — leave it untouched.
    Skip,
}

/// Decide the [`PayRecoveryAction`] for one oplog pay entry (lnrent-kum). The crash window this
/// closes: operation A fails definitively (row FAILED), a same-key retry starts operation B, and the
/// daemon crashes after `pay_bolt11_invoice` commits B but before `pay_idx_upsert` replaces A's row
/// — a later drive then sees FAILED and (if the persisted invoice EXPIRED) re-resolves a fresh
/// payment hash and starts operation C while B is still hidden in flight; B and C can BOTH settle
/// (each is INV-1-capped individually, not jointly), so the OPERATOR pays twice.
///
/// `candidate_dead` is the dead-op ledger verdict for THIS oplog entry's operation
/// (`fedimint_pay_dead_op`, written by `await_pay` the moment it observes a definitive failure).
/// The ledger is deliberately the ONLY dead signal — three adversarial review rounds refuted every
/// fedimint-side alternative: scan order (the oplog is wall-clock-ordered, not causal), the outcome
/// cache (only written on an EOF-drain `await_pay` never performs), and subscription probing (the
/// caching wrapper can write a NON-terminal last update as the outcome on an EOF-without-terminal,
/// permanently blinding later re-awaits).
///
/// - `None` row + NOT ledger-dead -> `Backfill`: no row at all (crash before the FIRST upsert;
///   the lnrent-4gt behavior). `None` row + LEDGER-DEAD -> `BackfillDead`: record the truth
///   (FAILED) without ever counting as a live candidate — this is what lets the ambiguity
///   runbook actually unbrick an absent-row refusal (codex PR-32 P1: without it, the operator's
///   `fedimint_pay_dead_op` insert would not reduce the live-candidate count and the daemon
///   would refuse to start forever).
/// - FAILED under a DIFFERENT op than the oplog entry:
///   - `candidate_dead` -> `Skip`: this exact op already definitively failed (funds provably
///     back). Resurrecting it would burn a refund attempt on a replayed failure — and with TWO
///     dead ops under one key, each restart would re-point the row at the OTHER dead op forever,
///     an oscillation that parks the refund at its attempt cap (adversarial lnrent-kum review).
///     This also keeps a NEWER `"(preflight-failed)"` placeholder from being clobbered by a
///     historical dead op.
///   - otherwise -> `ReplaceStaleFailed`: un-hide the possibly-live unrecorded op as PENDING. The
///     drivers re-await it and the y4m.16 machinery lands its REAL outcome (Success -> SUCCEEDED;
///     DefinitiveFailure -> FAILED + a dead-ledger record, so it can never be resurrected again;
///     Ambiguous stays PENDING).
/// - FAILED under the SAME op -> `Skip`: a genuinely failed operation — the FAILED mark is correct
///   and must survive recovery.
/// - PENDING / SUCCEEDED (or any other status) -> `Skip`: NEVER touch a live or completed row.
///
/// Ordering argument (adversarial lnrent-kum review): fedimint's oplog is ordered by WALL-CLOCK
/// time, not causally, so "newest-first" cannot be trusted across a clock rollback. This decision
/// is therefore ORDER-INDEPENDENT: a ledger-dead candidate never replaces anything (no
/// oscillation, at any scan position); a possibly-live candidate replaces a stale FAILED row at
/// ANY scan position; and once replaced the row is PENDING, so every other entry for the key
/// skips.
///
/// Documented residuals (not fixable from a single-op row + this scan):
/// - a SINGLE pre-ledger orphan (an op that failed before the dead-op ledger existed) is not in
///   the ledger -> classified possibly-live -> replaced once -> its re-await lands FAILED and
///   RECORDS it. Converges with at most one burned refund attempt; no restart oscillation;
/// - MULTIPLE competing candidates for one key — whether its row is a stale FAILED or entirely
///   ABSENT — cannot be represented by the single-op row, and NO automatic choice is money-safe:
///   the recovery loop collects BOTH arms without writing in-scan, detects the competition, and
///   FAILS THE PASS (the daemon refuses to start) with every competing op id and a
///   manual-reconciliation runbook. Post-ledger, competition is impossible in normal operation: a
///   second same-key op only ever starts after the first resolved and was ledgered. Full per-key
///   enumeration is lnrent-7so;
/// - an op stamped far enough in the FUTURE of the recovering clock is excluded from
///   `paginate_operations_rev` entirely (upstream starts at now+30s) — it stays hidden until a
///   later restart when the clock catches up; until then the pre-kum window applies to that key.
fn pay_recovery_action(
    existing: Option<(&str, &str)>,
    oplog_op_hex: &str,
    candidate_dead: bool,
) -> PayRecoveryAction {
    match existing {
        None if candidate_dead => PayRecoveryAction::BackfillDead,
        None => PayRecoveryAction::Backfill,
        Some((row_op, "FAILED")) if row_op != oplog_op_hex => {
            if candidate_dead {
                PayRecoveryAction::Skip
            } else {
                PayRecoveryAction::ReplaceStaleFailed
            }
        }
        Some(_) => PayRecoveryAction::Skip,
    }
}

fn fedimint_readiness_warns(gateway_ok: bool) -> bool {
    !gateway_ok
}

#[cfg(test)]
mod index_tests {
    use super::{
        idx_list_open, idx_mark_canceled, idx_mark_paid, pay_idx_get, pay_idx_mark,
        pay_idx_upsert, INDEX_SCHEMA,
    };
    use rusqlite::{params, Connection};
    use std::sync::Mutex;

    fn index_with_row(op_hex: &str, status: &str) -> Mutex<Connection> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO fedimint_invoice
               (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat,
                expires_at, status)
             VALUES (?1, ?2, ?3, 'lnbc1…', 'hash', 100, 4102444800, ?4)",
            params![format!("ext-{op_hex}"), op_hex, format!("inv-{op_hex}"), status],
        )
        .unwrap();
        Mutex::new(conn)
    }

    // A Canceled receive must leave the `watch()` respawn set: rows that stayed 'OPEN' were
    // re-subscribed on every restart, one task per historical unpaid invoice, forever.
    #[test]
    fn canceled_receive_leaves_the_open_respawn_set() {
        let index = index_with_row("op1", "OPEN");
        assert_eq!(idx_list_open(&index).unwrap().len(), 1);
        idx_mark_canceled(&index, "op1").unwrap();
        assert!(idx_list_open(&index).unwrap().is_empty());
    }

    // The OPEN-only guard: a late Canceled event must never demote a row a concurrent Claimed
    // already marked PAID (the settlement provenance would be lost).
    #[test]
    fn canceled_never_demotes_a_paid_row() {
        let index = index_with_row("op2", "OPEN");
        idx_mark_paid(&index, "op2", Some(1000)).unwrap();
        idx_mark_canceled(&index, "op2").unwrap();
        let status: String = index
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM fedimint_invoice WHERE operation_id = 'op2'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "PAID");
    }

    // ---- pay-index terminal-mark CAS (adversarial y4m.16 review) --------------------------------
    // `pay_start_lock` is released before settlement is awaited, so two callers can await the SAME
    // key. A DELAYED waiter that saw operation A's outcome must never clobber the row after a fresh
    // operation B was upserted under the same key — otherwise a later drive sees FAILED and starts a
    // THIRD operation while B is in flight (a double-pay).

    #[test]
    fn stale_waiter_mark_cannot_clobber_a_newer_operation() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        let index = Mutex::new(conn);

        // Operation A runs and definitively fails.
        pay_idx_upsert(&index, "key", "op-A", "PENDING", "ln", 1_000).unwrap();
        pay_idx_mark(&index, "key", "op-A", "FAILED", 2_000).unwrap();
        // The driver retries: operation B is upserted PENDING under the same key.
        pay_idx_upsert(&index, "key", "op-B", "PENDING", "ln", 1_000).unwrap();
        // A delayed second waiter on A replays A's failure — its mark must be a no-op now.
        pay_idx_mark(&index, "key", "op-A", "FAILED", 3_000).unwrap();
        let (op, status, _) = pay_idx_get(&index, "key").unwrap().unwrap();
        assert_eq!((op.as_str(), status.as_str()), ("op-B", "PENDING"),
            "a stale waiter's terminal mark for a superseded operation must not clobber the live row");
        let created_at: i64 = index
            .lock()
            .unwrap()
            .query_row(
                "SELECT created_at FROM fedimint_pay WHERE idempotency_key = 'key'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            created_at, 1_000,
            "the operation guard also prevents a stale waiter from refreshing the newer row's age"
        );
        // The live waiter on B still lands its own terminal mark.
        pay_idx_mark(&index, "key", "op-B", "SUCCEEDED", 4_000).unwrap();
        let (op, status, _) = pay_idx_get(&index, "key").unwrap().unwrap();
        assert_eq!((op.as_str(), status.as_str()), ("op-B", "SUCCEEDED"));
    }
}

#[cfg(test)]
mod index_gc_tests {
    //! Terminal `fedimint_invoice` (lnrent-y4m.15) and `fedimint_pay` (lnrent-y4m.19) index GC unit
    //! tests. These are feature-gated with the backend and exercise standalone seams without a live
    //! federation.
    use super::{
        ensure_fedimint_pay_gc_schema, gc_fedimint_invoice_index, gc_fedimint_pay_index,
        index_gc_due_and_stamp, index_has_column, pay_idx_mark, pay_idx_record_dead,
        pay_idx_upsert, receive_backfill_needed, FEDIMINT_INDEX_RETENTION_SECS,
        FEDIMINT_PAY_RETENTION_SECS, INDEX_GC_INTERVAL_SECS, INDEX_SCHEMA,
    };
    use crate::clock::{Clock, SystemClock, TestClock};
    use fedimint_ln_client::receive::LightningReceiveError;
    use fedimint_ln_client::LnReceiveState;
    use rusqlite::{params, Connection};
    use std::sync::Mutex;

    /// Seed one row with an explicit status / expires_at / settled_at into a fresh in-memory index.
    fn seed(conn: &Connection, ext: &str, status: &str, expires_at: i64, settled_at: Option<i64>) {
        conn.execute(
            "INSERT INTO fedimint_invoice
               (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat,
                expires_at, status, settled_at)
             VALUES (?1, ?2, ?3, 'lnbc1…', 'hash', 100, ?4, ?5, ?6)",
            params![
                ext,
                format!("op-{ext}"),
                format!("inv-{ext}"),
                expires_at,
                status,
                settled_at
            ],
        )
        .unwrap();
    }

    fn count(index: &Mutex<Connection>) -> i64 {
        index
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM fedimint_invoice", [], |r| r.get(0))
            .unwrap()
    }

    fn exists(index: &Mutex<Connection>, ext: &str) -> bool {
        let n: i64 = index
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM fedimint_invoice WHERE external_id = ?1",
                params![ext],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    // The flood reap deletes past-retention CANCELED rows ONLY. OPEN rows may still settle. PAID rows
    // are NEVER reaped here (recovery would resurrect them un-reapably, and they back the settlement
    // catch-up) — regardless of whether settled_at is known or NULL; see gc_fedimint_invoice_index.
    #[test]
    fn reaps_only_canceled_rows_past_retention() {
        // A synthetic clock well below real unixepoch(), so MIN(unixepoch(), now) == now (the store.rs
        // reaper idiom): cutoff = now - RETENTION. Rows strictly before the cutoff are reapable.
        let now = 2 * FEDIMINT_INDEX_RETENTION_SECS;
        let cutoff = FEDIMINT_INDEX_RETENTION_SECS; // now - RETENTION
        let old = cutoff - 1; // strictly past the window -> reapable (if CANCELED)
        let boundary = cutoff; // EXACTLY at the cutoff -> kept (strict `<`)
        let recent = now; // within retention -> kept

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        // REAPED: only the past-retention CANCELED row.
        seed(&conn, "canceled-old", "CANCELED", old, None);
        // SURVIVES: a PAID row is never reaped even with a known, past-retention settled_at.
        seed(&conn, "paid-old", "PAID", 0, Some(old));
        seed(&conn, "canceled-boundary", "CANCELED", boundary, None); // strict `<` keeps the boundary
        seed(&conn, "canceled-recent", "CANCELED", recent, None);
        seed(&conn, "open-ancient", "OPEN", old, None); // OPEN has no terminal -> never reaped
        seed(&conn, "paid-recent", "PAID", 0, Some(recent));
        seed(&conn, "paid-null-settled", "PAID", old, None); // NULL settled_at -> never reaped either

        let index = Mutex::new(conn);

        let reaped = gc_fedimint_invoice_index(&index, now, FEDIMINT_INDEX_RETENTION_SECS).unwrap();
        assert_eq!(reaped, 1, "only the past-retention CANCELED row is reaped");
        assert!(!exists(&index, "canceled-old"));
        assert!(exists(&index, "paid-old"), "PAID rows are never reaped");
        assert!(exists(&index, "canceled-boundary"));
        assert!(exists(&index, "canceled-recent"));
        assert!(exists(&index, "open-ancient"));
        assert!(exists(&index, "paid-recent"));
        assert!(exists(&index, "paid-null-settled"));
        assert_eq!(count(&index), 6);

        // Idempotent: a second sweep at the same clock removes nothing more.
        assert_eq!(
            gc_fedimint_invoice_index(&index, now, FEDIMINT_INDEX_RETENTION_SECS).unwrap(),
            0
        );
    }

    // Adversarial y4m.15 review: the reap is CHUNKED (bounded mutex hold on a flooded DB), so a
    // backlog larger than one batch is fully drained across batches; and a no-expiry (`expires_at
    // == 0`) CANCELED row is NEVER reaped (P3) so GC and recovery agree to leave it (recovery keeps
    // it live), avoiding a reap→rebackfill churn loop.
    #[test]
    fn reap_is_chunked_and_skips_zero_expiry_rows() {
        let now = 2 * FEDIMINT_INDEX_RETENTION_SECS;
        let old = FEDIMINT_INDEX_RETENTION_SECS - 1; // strictly past the window

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        // A backlog well over one 512-row batch — the batch loop must drain all of them.
        let reapable = 512 * 2 + 37;
        for i in 0..reapable {
            seed(&conn, &format!("flood-{i}"), "CANCELED", old, None);
        }
        // A no-expiry CANCELED row: past the window by the `0 < cutoff` arithmetic, but excluded by
        // `expires_at > 0` so it survives (recovery would keep it live).
        seed(&conn, "canceled-zero-expiry", "CANCELED", 0, None);

        let index = Mutex::new(conn);
        let reaped = gc_fedimint_invoice_index(&index, now, FEDIMINT_INDEX_RETENTION_SECS).unwrap();
        assert_eq!(reaped, reapable, "the whole backlog is drained across batches");
        assert!(
            exists(&index, "canceled-zero-expiry"),
            "a zero-expiry CANCELED row is never reaped (GC/recovery agree to keep it)"
        );
        assert_eq!(count(&index), 1);
    }

    // The clock-cap (MIN(unixepoch(), now)): a wall clock that jumped far into the FUTURE cannot reap a
    // fresh row. With `now` set absurdly high, the cutoff pins to the REAL unixepoch(), so a CANCELED
    // row that just became terminal remains within retention and survives.
    #[test]
    fn future_now_cannot_reap_a_fresh_row() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        let real_now = SystemClock.now();
        seed(&conn, "fresh-canceled", "CANCELED", real_now, None);
        let index = Mutex::new(conn);

        let huge_now = real_now + 10 * FEDIMINT_INDEX_RETENTION_SECS; // a badly-fast wall clock
        let reaped =
            gc_fedimint_invoice_index(&index, huge_now, FEDIMINT_INDEX_RETENTION_SECS).unwrap();
        assert_eq!(
            reaped, 0,
            "a future `now` cannot reap a row fresh by the real clock"
        );
        assert!(exists(&index, "fresh-canceled"));
    }

    // Recovery backfills a receive iff it could still matter: settled (Claimed) or live (unexpired
    // and not canceled). It must NOT resurrect a DEAD receive — the y4m.15 review P1: a legacy /
    // crash-window CANCELED invoice has a `None` oplog outcome, so once the GC reaps its row an
    // outcome-only rule would re-backfill it as OPEN on restart and `watch()` would respawn every
    // historical invoice, undoing the storage bound. The expiry gate closes that.
    #[test]
    fn recovery_backfills_live_or_settled_but_never_a_dead_receive() {
        const NOW: i64 = 1_000_000;
        let canceled = LnReceiveState::Canceled {
            reason: LightningReceiveError::Timeout,
        };
        let past = NOW - 1;
        let future = NOW + 1;

        // Settled (Claimed) always recovers — needed for settlement catch-up / refund provenance —
        // even long past expiry.
        assert!(receive_backfill_needed(Some(&LnReceiveState::Claimed), past, NOW));
        // Live: unexpired, not-yet-terminal (None) or explicitly open — recover and watch it.
        assert!(receive_backfill_needed(None, future, NOW));
        // THE P1: expired + None (a legacy/crash-window canceled row) — DEAD, never resurrect.
        assert!(!receive_backfill_needed(None, past, NOW));
        // Expired + decodable Canceled — DEAD.
        assert!(!receive_backfill_needed(Some(&canceled), past, NOW));
        // Unexpired + Canceled — terminally dead even before expiry; skip.
        assert!(!receive_backfill_needed(Some(&canceled), future, NOW));
        // expires_at == 0 (unparseable/absent expiry) is treated as NOT expired, so a live invoice
        // with an odd expiry is never dropped (a missed settlement would be a money bug).
        assert!(receive_backfill_needed(None, 0, NOW));
    }

    // The throttle: two GC-due checks within the interval run the reap AT MOST once; after the interval
    // elapses it runs again. Driven by a TestClock (deterministic, no federation).
    #[test]
    fn throttle_runs_at_most_once_per_interval() {
        let last = Mutex::new(0i64);
        let clock = TestClock::new(1_000_000);

        // First check: due (last==0, now huge) -> runs, and stamps last=now.
        assert!(index_gc_due_and_stamp(
            &last,
            clock.now(),
            INDEX_GC_INTERVAL_SECS
        ));
        // A second check moments later, still within the interval -> NOT due (at most one per burst).
        clock.advance(INDEX_GC_INTERVAL_SECS - 1);
        assert!(!index_gc_due_and_stamp(
            &last,
            clock.now(),
            INDEX_GC_INTERVAL_SECS
        ));
        // One more second reaches the interval boundary (>=) -> due again, stamps again.
        clock.advance(1);
        assert!(index_gc_due_and_stamp(
            &last,
            clock.now(),
            INDEX_GC_INTERVAL_SECS
        ));
        // Immediately after that run -> not due.
        assert!(!index_gc_due_and_stamp(
            &last,
            clock.now(),
            INDEX_GC_INTERVAL_SECS
        ));
    }

    #[test]
    fn throttle_recovers_from_a_backward_clock_correction() {
        let last = Mutex::new(0i64);
        let future = 5_000_000_000i64;
        assert!(index_gc_due_and_stamp(
            &last,
            future,
            INDEX_GC_INTERVAL_SECS
        ));

        let corrected = 1_700_000_000i64;
        assert!(index_gc_due_and_stamp(
            &last,
            corrected,
            INDEX_GC_INTERVAL_SECS
        ));
        assert_eq!(*last.lock().unwrap(), corrected);
        assert!(!index_gc_due_and_stamp(
            &last,
            corrected + INDEX_GC_INTERVAL_SECS - 1,
            INDEX_GC_INTERVAL_SECS
        ));
    }

    // ---- terminal `fedimint_pay` GC (lnrent-y4m.19) ---------------------------------------------

    /// A fresh in-memory pay index with the full schema + y4m.19 migration applied (so `created_at`
    /// and its covering index exist), wrapped for the reaper's `&Mutex<Connection>` API.
    fn open_pay_index() -> Mutex<Connection> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        ensure_fedimint_pay_gc_schema(&conn).unwrap();
        Mutex::new(conn)
    }

    /// Seed one pay row with an explicit status and `created_at` (NULL when `created_at` is `None`).
    fn seed_pay(index: &Mutex<Connection>, key: &str, status: &str, created_at: Option<i64>) {
        index
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO fedimint_pay
                   (idempotency_key, operation_id, backend_pay_id, status, pay_kind, created_at)
                 VALUES (?1, ?2, ?2, ?3, 'ln', ?4)",
                params![key, format!("op-{key}"), status, created_at],
            )
            .unwrap();
    }

    fn pay_exists(index: &Mutex<Connection>, key: &str) -> bool {
        let n: i64 = index
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM fedimint_pay WHERE idempotency_key = ?1",
                params![key],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    fn pay_count(index: &Mutex<Connection>) -> i64 {
        index
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM fedimint_pay", [], |r| r.get(0))
            .unwrap()
    }

    // The pay reap deletes ONLY definitively FAILED rows past retention. SUCCEEDED rows remain the
    // permanent same-key idempotency record; PENDING rows are still in-flight / re-drivable. Neither
    // is ever reaped at any age, and a NULL-created_at (legacy) row is never reaped either — an unknown
    // age is treated as "not yet old enough" (the conservative floor).
    #[test]
    fn reaps_only_failed_pay_rows_past_retention() {
        // Synthetic clock well below real unixepoch(), so MIN(unixepoch(), now) == now: cutoff = now -
        // RETENTION. Rows strictly before the cutoff are reapable (if terminal + non-NULL age).
        let now = 2 * FEDIMINT_PAY_RETENTION_SECS;
        let cutoff = FEDIMINT_PAY_RETENTION_SECS; // now - RETENTION
        let old = cutoff - 1; // strictly past the window -> reapable (if terminal)
        let boundary = cutoff; // EXACTLY at the cutoff -> kept (strict `<`)
        let recent = now; // within retention -> kept

        let index = open_pay_index();
        // REAPED: a past-retention FAILED row.
        seed_pay(&index, "succeeded-old", "SUCCEEDED", Some(old));
        seed_pay(&index, "failed-old", "FAILED", Some(old));
        // SURVIVES: SUCCEEDED is the permanent idempotency record; strict `<` keeps the FAILED
        // boundary; a recent FAILED row is within retention.
        seed_pay(&index, "failed-boundary", "FAILED", Some(boundary));
        seed_pay(&index, "failed-recent", "FAILED", Some(recent));
        // SURVIVES: a PENDING row is in-flight — never reaped, even ancient.
        seed_pay(&index, "pending-ancient", "PENDING", Some(old));
        // SURVIVES: a NULL-age (legacy) terminal row is never reaped.
        seed_pay(&index, "succeeded-null-age", "SUCCEEDED", None);

        let reaped = gc_fedimint_pay_index(&index, now, FEDIMINT_PAY_RETENTION_SECS).unwrap();
        assert_eq!(reaped, 1, "only the past-retention FAILED row is reaped");
        assert!(
            pay_exists(&index, "succeeded-old"),
            "SUCCEEDED remains a permanent same-key idempotency record"
        );
        assert!(!pay_exists(&index, "failed-old"));
        assert!(pay_exists(&index, "failed-boundary"));
        assert!(pay_exists(&index, "failed-recent"));
        assert!(
            pay_exists(&index, "pending-ancient"),
            "PENDING is never reaped"
        );
        assert!(
            pay_exists(&index, "succeeded-null-age"),
            "a NULL-created_at row is never reaped"
        );
        assert_eq!(pay_count(&index), 5);

        // Idempotent: a second sweep at the same clock removes nothing more.
        assert_eq!(
            gc_fedimint_pay_index(&index, now, FEDIMINT_PAY_RETENTION_SECS).unwrap(),
            0
        );
    }

    // A NULL `created_at` (legacy) row is NEVER reaped, no matter how far in the future `now` runs.
    #[test]
    fn pay_reap_never_touches_a_null_created_at_row() {
        let index = open_pay_index();
        seed_pay(&index, "legacy", "FAILED", None);
        // An absurdly large `now` (cutoff way past any real timestamp) still cannot reap a NULL age.
        let huge_now = 100 * FEDIMINT_PAY_RETENTION_SECS;
        assert_eq!(
            gc_fedimint_pay_index(&index, huge_now, FEDIMINT_PAY_RETENTION_SECS).unwrap(),
            0
        );
        assert!(pay_exists(&index, "legacy"));
    }

    // The clock-cap (MIN(unixepoch(), now)): a wall clock jumped far into the FUTURE cannot reap a row
    // whose real `created_at` is recent — the cutoff pins to the REAL unixepoch(). Mirrors the invoice
    // reaper's `future_now_cannot_reap_a_fresh_row`.
    #[test]
    fn future_now_cannot_reap_a_fresh_pay_row() {
        let index = open_pay_index();
        let real_now = SystemClock.now();
        seed_pay(&index, "fresh-failed", "FAILED", Some(real_now));

        let huge_now = real_now + 10 * FEDIMINT_PAY_RETENTION_SECS; // a badly-fast wall clock
        let reaped = gc_fedimint_pay_index(&index, huge_now, FEDIMINT_PAY_RETENTION_SECS).unwrap();
        assert_eq!(
            reaped, 0,
            "a future `now` cannot reap a row fresh by the real clock"
        );
        assert!(pay_exists(&index, "fresh-failed"));
    }

    // The reap is CHUNKED (bounded mutex hold): a backlog larger than one 512-row batch is fully
    // drained across batches. Mirrors the invoice reaper's chunk test.
    #[test]
    fn pay_reap_is_chunked() {
        let now = 2 * FEDIMINT_PAY_RETENTION_SECS;
        let old = FEDIMINT_PAY_RETENTION_SECS - 1; // strictly past the window

        let index = open_pay_index();
        let reapable = 512 * 2 + 37;
        for i in 0..reapable {
            seed_pay(&index, &format!("flood-{i}"), "FAILED", Some(old));
        }
        let reaped = gc_fedimint_pay_index(&index, now, FEDIMINT_PAY_RETENTION_SECS).unwrap();
        assert_eq!(
            reaped, reapable,
            "the whole backlog is drained across batches"
        );
        assert_eq!(pay_count(&index), 0);
    }

    // The dup-tolerant migration: opening a DB whose `fedimint_pay` LACKS `created_at` (a pre-y4m.19
    // legacy DB) adds the column and its covering index without error; re-running is idempotent; and a
    // fresh DB (column already present from the CREATE TABLE) is a no-op ALTER.
    #[test]
    fn pay_gc_schema_migration_is_dup_tolerant() {
        let has_gc_index = |conn: &Connection| -> bool {
            conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                      WHERE type = 'index'
                        AND name = 'fedimint_pay_gc_idx'
                        AND tbl_name = 'fedimint_pay'
                 )",
                [],
                |r| r.get::<_, bool>(0),
            )
            .unwrap()
        };

        // A legacy DB: the `fedimint_pay` table WITHOUT `created_at` (the pre-y4m.19 shape).
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE fedimint_pay (
                 idempotency_key TEXT PRIMARY KEY,
                 operation_id    TEXT NOT NULL,
                 backend_pay_id  TEXT NOT NULL,
                 status          TEXT NOT NULL DEFAULT 'PENDING',
                 pay_kind        TEXT NOT NULL DEFAULT 'ln'
             );",
        )
        .unwrap();
        assert!(!index_has_column(&conn, "fedimint_pay", "created_at").unwrap());
        assert!(!has_gc_index(&conn));
        // The migration adds the column + covering index without error.
        ensure_fedimint_pay_gc_schema(&conn).unwrap();
        assert!(index_has_column(&conn, "fedimint_pay", "created_at").unwrap());
        assert!(has_gc_index(&conn));
        // Re-running is idempotent (no duplicate-column / duplicate-index error).
        ensure_fedimint_pay_gc_schema(&conn).unwrap();
        assert!(index_has_column(&conn, "fedimint_pay", "created_at").unwrap());
        assert!(has_gc_index(&conn));

        // A fresh DB already has the column from INDEX_SCHEMA's CREATE TABLE, so the ALTER is skipped;
        // the helper still creates the index after proving the column exists.
        let fresh = Connection::open_in_memory().unwrap();
        fresh.execute_batch(INDEX_SCHEMA).unwrap();
        assert!(index_has_column(&fresh, "fedimint_pay", "created_at").unwrap());
        assert!(!has_gc_index(&fresh));
        ensure_fedimint_pay_gc_schema(&fresh).unwrap();
        assert!(index_has_column(&fresh, "fedimint_pay", "created_at").unwrap());
        assert!(has_gc_index(&fresh));
    }

    // `pay_idx_upsert` stamps `created_at` on INSERT and REFRESHES it on the ON CONFLICT (retry)
    // branch — a retry is a fresh terminal-lifecycle, so its retention clock restarts (never reaped on
    // the original attempt's age).
    #[test]
    fn pay_upsert_stamps_and_refreshes_created_at() {
        let index = open_pay_index();
        let created_at = |key: &str| -> Option<i64> {
            index
                .lock()
                .unwrap()
                .query_row(
                    "SELECT created_at FROM fedimint_pay WHERE idempotency_key = ?1",
                    params![key],
                    |r| r.get::<_, Option<i64>>(0),
                )
                .unwrap()
        };

        pay_idx_upsert(&index, "key", "op-A", "PENDING", "ln", 1_000).unwrap();
        assert_eq!(created_at("key"), Some(1_000), "INSERT stamps created_at");

        // The retry (ON CONFLICT) branch refreshes created_at to the new `now`.
        pay_idx_upsert(&index, "key", "op-B", "PENDING", "ln", 9_000).unwrap();
        assert_eq!(
            created_at("key"),
            Some(9_000),
            "the retry branch restarts the retention clock"
        );
    }

    // A pay may stay PENDING longer than the 180-day retention before it finally terminalizes. The
    // terminal CAS must restart the retention clock; otherwise the next GC could immediately delete
    // the newly-FAILED recovery guard before a later startup has converged its permanent oplog entry.
    #[test]
    fn terminal_mark_restarts_pay_retention_window() {
        let index = open_pay_index();
        let terminal_at = 2 * FEDIMINT_PAY_RETENTION_SECS;
        pay_idx_upsert(&index, "slow-pay", "op-slow", "PENDING", "ln", 1).unwrap();

        pay_idx_mark(&index, "slow-pay", "op-slow", "FAILED", terminal_at).unwrap();

        let created_at: Option<i64> = index
            .lock()
            .unwrap()
            .query_row(
                "SELECT created_at FROM fedimint_pay WHERE idempotency_key = 'slow-pay'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(created_at, Some(terminal_at));
        assert_eq!(
            gc_fedimint_pay_index(&index, terminal_at, FEDIMINT_PAY_RETENTION_SECS).unwrap(),
            0,
            "a newly-failed row is retained for the full failed-row retention window"
        );
    }

    // Fallback documentation (lnrent-y4m.19 §5): the dead-op ledger (`fedimint_pay_dead_op`) is
    // INTENTIONALLY left un-GC'd — the pay reap deleting a row's op does NOT touch the ledger marker.
    // See gc_index_if_due for the reasoned deferral (not a flood vector; reaping it would reintroduce a
    // startup ambiguity brick for a double-failed refund key).
    #[test]
    fn dead_op_ledger_is_left_ungced_by_the_pay_reap() {
        let now = 2 * FEDIMINT_PAY_RETENTION_SECS;
        let old = FEDIMINT_PAY_RETENTION_SECS - 1;

        let index = open_pay_index();
        // A terminal FAILED row for op-`dead` past retention, plus its dead-op ledger marker.
        seed_pay(&index, "dead", "FAILED", Some(old));
        pay_idx_record_dead(&index, "op-dead").unwrap();

        let reaped = gc_fedimint_pay_index(&index, now, FEDIMINT_PAY_RETENTION_SECS).unwrap();
        assert_eq!(reaped, 1, "the definitively failed pay row is reaped");
        assert!(!pay_exists(&index, "dead"));
        // The dead-op marker SURVIVES — the reap never touches fedimint_pay_dead_op.
        let dead_rows: i64 = index
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM fedimint_pay_dead_op", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            dead_rows, 1,
            "the dead-op ledger is intentionally left un-GC'd"
        );
    }
}

#[cfg(test)]
mod path_prep_tests {
    use super::{prepare_fedimint_paths, CLIENT_DB_DIR, INDEX_DB_FILE};
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_data_dir(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-fedimint-paths-{}-{name}-{n}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn file_mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn prepare_paths_hardens_children_under_preexisting_traversable_data_dir() {
        let dir = temp_data_dir("perms");
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();

        let paths = prepare_fedimint_paths(&dir, "fed").expect("prepare fedimint paths");

        let fedimint_dir = dir.join("fedimint");
        let federation_dir = fedimint_dir.join("fed");
        assert_eq!(file_mode(&dir), 0o755, "parent data dir is not chmod'ed");
        assert_eq!(file_mode(&fedimint_dir), 0o700);
        assert_eq!(file_mode(&federation_dir), 0o700);
        assert_eq!(file_mode(&paths.client_db), 0o700);
        assert_eq!(paths.client_db, federation_dir.join(CLIENT_DB_DIR));
        assert_eq!(paths.index_db, federation_dir.join(INDEX_DB_FILE));
        assert_eq!(file_mode(&paths.index_db), 0o600);
        assert!(
            !fs::symlink_metadata(&paths.index_db)
                .unwrap()
                .file_type()
                .is_symlink(),
            "index main file must not be a symlink"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_paths_refuses_symlinked_fedimint_owned_paths() {
        assert_symlink_refused("fedimint-root", |dir, outside| {
            symlink(outside, dir.join("fedimint")).unwrap();
        });
        assert_symlink_refused("federation-dir", |dir, outside| {
            fs::create_dir_all(dir.join("fedimint")).unwrap();
            symlink(outside, dir.join("fedimint/fed")).unwrap();
        });
        assert_symlink_refused("client-db", |dir, outside| {
            fs::create_dir_all(dir.join("fedimint/fed")).unwrap();
            symlink(outside, dir.join("fedimint/fed").join(CLIENT_DB_DIR)).unwrap();
        });
        assert_symlink_refused("index-db", |dir, outside| {
            fs::create_dir_all(dir.join("fedimint/fed")).unwrap();
            let target = outside.join("target.db");
            fs::write(&target, b"outside").unwrap();
            symlink(target, dir.join("fedimint/fed").join(INDEX_DB_FILE)).unwrap();
        });
    }

    fn assert_symlink_refused(name: &str, setup: impl FnOnce(&Path, &Path)) {
        let dir = temp_data_dir(name);
        let outside = temp_data_dir(&format!("{name}-outside"));
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(&outside).unwrap();
        setup(&dir, &outside);

        assert!(
            prepare_fedimint_paths(&dir, "fed").is_err(),
            "symlinked {name} path must be refused"
        );

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&outside);
    }
}

#[cfg(test)]
mod net_payout_tests {
    //! INV-1 fee-deduction unit tests (spec §5). The whole module is `#[cfg(feature = "fedimint")]`
    //! (see lib.rs), so these run under `cargo test -p lnrentd --features fedimint`. `net_payout_sat`
    //! is pure (no gateway / federation), so it needs no live federation.
    use super::net_payout_sat;

    /// The gateway fee on a payout of `n` whole sats, in msats — an INDEPENDENT restatement of
    /// Fedimint's `RoutingFees::to_amount` (NOT a call into the production helper), so the property
    /// assertions cross-check `net_payout_sat` against the real fee rule: `base + payment/(1e6/ppm)`
    /// with integer division on both steps (`n == 0` is no payment, hence no fee). A `ppm > 1_000_000`
    /// schedule divides to a zero `fee_percent` (Fedimint would divide-by-zero); model it as an
    /// unpayable `u128::MAX` fee so no positive payout is ever valid.
    fn fee_msat(base_msat: u64, ppm: u64, n_sat: u64) -> u128 {
        if n_sat == 0 {
            return 0;
        }
        let pay_msat = u128::from(n_sat) * 1000;
        let base = u128::from(base_msat);
        if ppm == 0 {
            return base;
        }
        let fee_percent = 1_000_000u128 / u128::from(ppm);
        if fee_percent == 0 {
            return u128::MAX; // unpayable >100% schedule
        }
        base + pay_msat / fee_percent
    }

    /// Total outlay (payout + fee) in msats for a payout of `n` whole sats. `saturating_add` so the
    /// `u128::MAX` unpayable-fee sentinel can't overflow the sum (it stays > any real `r_msat`).
    fn outlay_msat(base_msat: u64, ppm: u64, n_sat: u64) -> u128 {
        (u128::from(n_sat) * 1000).saturating_add(fee_msat(base_msat, ppm, n_sat))
    }

    /// The two INV-1 invariants for every `(base, ppm, gross)`: the chosen net fits the gross, and
    /// `net + 1` does NOT — unless `net == gross` already (the no-fee ceiling).
    fn assert_maximal(base_msat: u64, ppm: u64, gross_sat: u64) {
        let net = net_payout_sat(base_msat, ppm, gross_sat);
        let r_msat = u128::from(gross_sat) * 1000;
        assert!(net <= gross_sat, "net {net} exceeds gross {gross_sat}");
        assert!(
            outlay_msat(base_msat, ppm, net) <= r_msat,
            "net {net} outlay exceeds gross {gross_sat} (base={base_msat}, ppm={ppm})"
        );
        if net < gross_sat {
            assert!(
                outlay_msat(base_msat, ppm, net + 1) > r_msat,
                "net+1 ({}) should violate the cap (base={base_msat}, ppm={ppm}, gross={gross_sat})",
                net + 1
            );
        }
    }

    // {base=0, ppm=0}: net == gross for every gross (the MockPayment-equivalent default path).
    #[test]
    fn no_fee_pays_full_gross() {
        for gross in [0u64, 1, 2, 1000, 2_100_000_000_000_000] {
            assert_eq!(net_payout_sat(0, 0, gross), gross);
            assert_maximal(0, 0, gross);
        }
    }

    // {base>0, ppm=0}: a flat base fee reserved off the top; sub-sat remainders are absorbed.
    #[test]
    fn flat_base_fee_only() {
        // gross = 1000 sat = 1_000_000 msat; base = 1500 msat. valid(999)=999000+1500>1e6 (no),
        // valid(998)=998000+1500<=1e6 (yes) -> net 998.
        assert_eq!(net_payout_sat(1500, 0, 1000), 998);
        assert_maximal(1500, 0, 1000);
        // base = exactly 1 sat (1000 msat) -> net = gross - 1.
        assert_eq!(net_payout_sat(1000, 0, 1000), 999);
        assert_maximal(1000, 0, 1000);
    }

    // {base=0, ppm>0}.
    #[test]
    fn proportional_fee_only() {
        assert_maximal(0, 1000, 1_000_000); // 0.1%
        assert_maximal(0, 5000, 50_000);
        assert_maximal(0, 1, 1); // the rounding-regression boundary
    }

    // {base>0, ppm>0}.
    #[test]
    fn base_and_proportional() {
        for &(base, ppm, gross) in &[
            (1000u64, 1000u64, 1_000_000u64),
            (2000, 500, 100_000),
            (1, 1, 1),
            (1, 1, 2),
            (1234, 5678, 9_999_999),
        ] {
            assert_maximal(base, ppm, gross);
        }
    }

    // u32-max base/ppm. A huge flat base dwarfs a small gross (dust). A `ppm > 1_000_000` schedule is
    // a >100% proportional fee: Fedimint's to_amount divides `1_000_000/ppm` to 0, so the fee is
    // effectively unbounded and NO positive payout fits, at ANY gross (not just a small one). A large
    // flat base still leaves most of a large gross payable.
    #[test]
    fn u32_max_base_and_ppm() {
        let max = u64::from(u32::MAX);
        assert_eq!(net_payout_sat(max, 0, 1000), 0, "huge flat base -> dust");
        assert_maximal(max, 0, 1000);
        assert_eq!(net_payout_sat(0, max, 1000), 0, "ppm > 1e6 -> unpayable");
        assert_eq!(
            net_payout_sat(0, max, 2_100_000_000_000_000),
            0,
            "ppm > 1e6 is unpayable even at a large gross (unbounded fee, not just small-gross dust)"
        );
        assert_maximal(0, max, 1000);
        assert_maximal(0, max, 2_100_000_000_000_000);
        // A large flat base still leaves most of a large gross payable.
        assert!(net_payout_sat(max, 0, 2_100_000_000_000_000) > 0);
        assert_maximal(max, 0, 2_100_000_000_000_000);
    }

    // Large `gross_sat` where the widened intermediates (`pay_msat = n*1000` and `r_msat = gross*1000`)
    // overflow a u64 (~1.8e19): u128 widening keeps `valid(n)` exact. ppm <= 1_000_000 so a real
    // nonzero payout is chosen — that is what actually drives the large `pay_msat / fee_percent`
    // division (the corrected to_amount form divides, so the overflow risk moved off the ppm product).
    #[test]
    fn large_gross_overflows_u64_intermediates() {
        assert_maximal(0, 1000, 10_000_000_000_000_000); // 1e16 sat, 0.1%
        assert_maximal(1000, 1000, u64::MAX); // r_msat = u64::MAX*1000 overflows u64
        assert_maximal(u64::from(u32::MAX), 1_000_000, u64::MAX); // 100% proportional + max flat base
    }

    // Exact dust classification by the whole-sat predicate (spec §3.1 dust boundary examples).
    #[test]
    fn dust_boundaries() {
        // base=999 msat, ppm=0, gross=1 sat: 1 sat + 999 msat > 1 sat received -> dust.
        assert_eq!(net_payout_sat(999, 0, 1), 0);
        // base=2000 msat, ppm=0, gross=1 sat: base alone exceeds R_msat -> only n==0 fits -> dust.
        assert_eq!(net_payout_sat(2000, 0, 1), 0);
        // gross=0 is always dust regardless of the schedule.
        assert_eq!(net_payout_sat(0, 0, 0), 0);
        assert_eq!(net_payout_sat(500, 100, 0), 0);
    }

    // Regression: base=0, ppm=1, gross=1 sat returns 1 (fee(1 sat) = floor(1000/1e6) = 0, so 1 sat is
    // payable). The closed-form msat-inverse `floor(max(0,R-base)*1e6/(1e6+ppm)/1000)` floors to 0
    // here — proving the exact whole-sat binary search is used as the final value, not the closed form.
    #[test]
    fn rounding_regression_closed_form_not_used() {
        assert_eq!(net_payout_sat(0, 1, 1), 1);
        assert_maximal(0, 1, 1);
    }

    // P1 regression: a ppm that does NOT divide 1_000_000 charges MORE than the naive
    // floor(x*ppm/1e6). Fedimint's to_amount uses fee_percent = 1_000_000/ppm (integer), so for
    // ppm=600_000 fee_percent=1 and the proportional fee is the WHOLE payment again (100%), not 60%.
    // net must therefore cap at 500 (outlay 2*500 = 1000 sat == the 1000-sat gross), NOT the 625 a
    // naive 60% model would allow — paying 625 would debit 625 + 625 = 1250 sat against a 1000-sat
    // gross, an INV-1 drain. This is the exact defect review P1 found.
    #[test]
    fn nondividing_ppm_uses_actual_fee_not_naive_product() {
        assert_eq!(net_payout_sat(0, 600_000, 1000), 500);
        assert_maximal(0, 600_000, 1000);
        // The naive model's answer (625) over-drains under the ACTUAL fee schedule.
        assert!(outlay_msat(0, 600_000, 625) > u128::from(1000u64) * 1000);
    }

    #[test]
    fn readiness_warns_on_gateway_only() {
        // §F: readiness no longer looks at the balance — only the gateway liveness half remains.
        assert!(!super::fedimint_readiness_warns(true));
        assert!(super::fedimint_readiness_warns(false));
    }
}

#[cfg(test)]
mod select_gateway_tests {
    //! PURE ordered-failover unit tests (lnrent-y4m.8). The whole module is `#[cfg(feature =
    //! "fedimint")]` (see lib.rs), so these run under the default-on `cargo test -p lnrentd`.
    //! `select_first_reachable` is generic and free of the live fedimint client (mirroring the
    //! `net_payout_sat` precedent), so the SELECTION decision is proven with a fake `try_one` over a
    //! stand-in gateway type — no federation. The end-to-end failover proof lives in the `#[ignore]`d
    //! `daemon/tests/fedimint_live.rs`.
    use super::{ordered_with_preference, select_first_reachable};
    use anyhow::{anyhow, Result};
    use fedimint_core::secp256k1::{PublicKey, Secp256k1, SecretKey};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Three DISTINCT valid compressed secp256k1 pubkeys standing in for an ordered gateway list.
    /// Derived from tiny fixed secret keys so they are guaranteed on-curve (no hand-picked-hex
    /// validity risk); the pure selector never touches the curve, it only compares/returns them.
    fn pk(seed: u8) -> PublicKey {
        let secp = Secp256k1::new();
        let sk = SecretKey::from_slice(&[seed; 32]).expect("valid secret key");
        PublicKey::from_secret_key(&secp, &sk)
    }

    // The FIRST reachable gateway in the configured order wins — and selection STOPS there (a healthy
    // primary is never bypassed, and later gateways are not probed).
    #[tokio::test]
    async fn first_reachable_wins_in_order() {
        let gws = [pk(1), pk(2), pk(3)];
        let tried = Mutex::new(Vec::new());
        let got = select_first_reachable(&gws, |p| {
            tried.lock().unwrap().push(p);
            async move { Ok(Some(p)) }
        })
        .await
        .unwrap();
        assert_eq!(got, pk(1), "the first gateway is selected");
        assert_eq!(
            *tried.lock().unwrap(),
            vec![pk(1)],
            "selection stops at the first reachable gateway"
        );
    }

    // Back-compat: a single-gateway list behaves exactly like the pre-failover pin-one config.
    #[tokio::test]
    async fn single_gateway_list_selects_it() {
        let got = select_first_reachable(&[pk(1)], |p| async move { Ok(Some(p)) })
            .await
            .unwrap();
        assert_eq!(got, pk(1));
    }

    // An early Ok(None) (unregistered/not found) AND an early Err (transient) are both SKIPPED, and a
    // later REACHABLE gateway is used — the whole point of failover.
    #[tokio::test]
    async fn early_none_and_err_are_skipped_for_a_later_reachable() {
        let gws = [pk(1), pk(2), pk(3)];
        let tried = Mutex::new(Vec::new());
        let got = select_first_reachable(&gws, |p| {
            tried.lock().unwrap().push(p);
            async move {
                if p == pk(1) {
                    Ok(None) // unregistered / not found -> skip
                } else if p == pk(2) {
                    Err(anyhow!("gateway-2 handshake timeout")) // transient -> skip
                } else {
                    Ok(Some(p)) // pk(3) reachable
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(got, pk(3), "failover reaches the third gateway");
        assert_eq!(
            *tried.lock().unwrap(),
            vec![pk(1), pk(2), pk(3)],
            "gateways are tried strictly in order until one is reachable"
        );
    }

    // ALL gateways unreachable -> a clear fail-closed error that CARRIES the last underlying cause
    // (so a diagnosable transient reason is not swallowed). This is the fail-closed money invariant:
    // receiving + refunds refuse rather than silently routing through an unintended gateway.
    #[tokio::test]
    async fn all_unreachable_is_a_clear_error_carrying_the_last_cause() {
        let gws = [pk(1), pk(2)];
        let got: Result<PublicKey> = select_first_reachable(&gws, |p| async move {
            if p == pk(2) {
                Err(anyhow!("gateway-2 refused connection"))
            } else {
                Ok(None)
            }
        })
        .await;
        let shown = format!("{:#}", got.unwrap_err());
        assert!(
            shown.contains("no configured lightning gateway is reachable"),
            "clear fail-closed message: {shown}"
        );
        assert!(
            shown.contains("gateway-2 refused connection"),
            "the last underlying cause is carried as context: {shown}"
        );
    }

    // An empty list fails closed WITHOUT probing — the live wrapper handles the empty "pick any
    // available gateway" case before ever calling the helper, so the helper's empty branch is a
    // defensive fail-closed, never a silent success.
    #[tokio::test]
    async fn empty_list_is_a_clear_error_without_probing() {
        let calls = AtomicUsize::new(0);
        let got: Result<PublicKey> = select_first_reachable(&[], |p| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(Some(p)) }
        })
        .await;
        assert!(got.is_err(), "an empty list fails closed");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no gateway is probed for an empty list"
        );
    }

    // ---- ordered_with_preference (lnrent-y4m.18): quote/pay gateway binding ------------------------
    // The PURE preference-ordering rule the live pay path feeds `select_first_reachable`, so quote and
    // pay probe the SAME gateway first within one attempt. Proven without a federation.

    // The quote-time gateway is tried FIRST, ahead of the configured order.
    #[test]
    fn preference_is_tried_first() {
        let gws = [pk(1), pk(2), pk(3)];
        assert_eq!(
            ordered_with_preference(Some(pk(2)), &gws),
            vec![pk(2), pk(1), pk(3)],
            "the preferred gateway leads, then the configured order minus it"
        );
    }

    // A preferred gateway ALREADY in the configured list is deduped — probed once (first), never twice.
    #[test]
    fn preference_in_list_is_deduped() {
        let gws = [pk(1), pk(2), pk(3)];
        let ordered = ordered_with_preference(Some(pk(1)), &gws);
        assert_eq!(
            ordered,
            vec![pk(1), pk(2), pk(3)],
            "an already-first preference leaves the order unchanged, no duplicate"
        );
        assert_eq!(
            ordered.iter().filter(|&&p| p == pk(1)).count(),
            1,
            "the preferred pk appears exactly once"
        );
    }

    // No preference == the configured order, byte-for-byte (the pre-y4m.18 selection).
    #[test]
    fn no_preference_is_the_configured_order() {
        let gws = [pk(1), pk(2), pk(3)];
        assert_eq!(
            ordered_with_preference(None, &gws),
            vec![pk(1), pk(2), pk(3)]
        );
    }

    // A preferred gateway NOT in the configured list is STILL tried first — it was reachable at quote
    // time, so it is the best guess for the pay even though the operator never listed it.
    #[test]
    fn preference_not_in_list_is_still_first() {
        let gws = [pk(1), pk(2)];
        assert_eq!(
            ordered_with_preference(Some(pk(9)), &gws),
            vec![pk(9), pk(1), pk(2)],
            "an unlisted preference leads, then the full configured order"
        );
    }

    // Empty configured list + a preference: probe the preferred pk alone (the live wrapper then falls
    // back to "any available gateway" if it is unreachable — see select_gateway_preferring).
    #[test]
    fn empty_list_with_preference_probes_the_preferred() {
        assert_eq!(ordered_with_preference(Some(pk(1)), &[]), vec![pk(1)]);
    }

    // Empty configured list + NO preference: nothing to probe (the empty-list "any gateway" path).
    #[test]
    fn empty_list_without_preference_is_empty() {
        assert!(ordered_with_preference(None, &[]).is_empty());
    }
}

#[cfg(test)]
mod pay_state_class_tests {
    //! PURE terminal-classification unit tests (lnrent-y4m.16). The whole module is `#[cfg(feature =
    //! "fedimint")]` (see lib.rs), so these run under the default-on `cargo test -p lnrentd`. The
    //! classifiers are free of the live client (the `net_payout_sat` / `select_first_reachable`
    //! precedent), so the money-safety class of EVERY pay-state variant is proven variant-by-variant
    //! without a federation. The choke point — `await_pay` marking FAILED only for `DefinitiveFailure`
    //! and leaving `Ambiguous` PENDING — is a single match on these classes; an end-to-end index
    //! proof would need a live client, so it lives in the `#[ignore]`d `daemon/tests/fedimint_live.rs`.
    use super::{
        classify_internal_pay_state, classify_ln_pay_state, InternalPayState, LnPayState,
        PayStateClass,
    };
    use fedimint_ln_client::incoming::IncomingSmError;
    use fedimint_ln_client::pay::GatewayPayError;
    use fedimint_ln_common::contracts::Preimage;

    // A plain constructible `IncomingSmError` for the internal-pay variants that carry one. The class
    // never inspects WHICH error, only the variant, so any concrete error stands in.
    fn sm_err() -> IncomingSmError {
        IncomingSmError::FailedToFundContract {
            error_message: "test".to_string(),
        }
    }

    // ---- LnPayState ----------------------------------------------------------------------------

    // Success carries a preimage -> the recipient is provably paid.
    #[test]
    fn ln_success_is_success() {
        assert_eq!(
            classify_ln_pay_state(&LnPayState::Success {
                preimage: "00".to_string()
            }),
            PayStateClass::Success
        );
    }

    // Every non-terminal state keeps awaiting (unchanged from the pre-y4m.16 behavior).
    #[test]
    fn ln_interim_states_are_in_flight() {
        for s in [
            LnPayState::Created,
            LnPayState::Funded { block_height: 0 },
            LnPayState::AwaitingChange,
            LnPayState::WaitingForRefund {
                error_reason: "pending refund".to_string(),
            },
        ] {
            assert_eq!(
                classify_ln_pay_state(&s),
                PayStateClass::InFlight,
                "{s:?} must keep awaiting, never terminate"
            );
        }
    }

    // MONEY-CRITICAL: the federation refunded the outgoing contract -> funds provably back, so this
    // attempt sent nothing. DEFINITIVE failure -> FAILED -> a fresh pay is safe.
    #[test]
    fn ln_refunded_is_definitive_failure() {
        assert_eq!(
            classify_ln_pay_state(&LnPayState::Refunded {
                gateway_error: GatewayPayError::OutgoingContractError,
            }),
            PayStateClass::DefinitiveFailure
        );
    }

    // MONEY-CRITICAL: canceled before funding -> no money ever left. DEFINITIVE failure -> FAILED.
    #[test]
    fn ln_canceled_is_definitive_failure() {
        assert_eq!(
            classify_ln_pay_state(&LnPayState::Canceled),
            PayStateClass::DefinitiveFailure
        );
    }

    // MONEY-CRITICAL (the whole point of y4m.16): UnexpectedError is NOT proof the recipient was
    // unpaid — it can be a post-payment local failure — so it is AMBIGUOUS and must NEVER be retried
    // (that risks a double-send). `await_pay` leaves such a row PENDING.
    #[test]
    fn ln_unexpected_error_is_ambiguous() {
        assert_eq!(
            classify_ln_pay_state(&LnPayState::UnexpectedError {
                error_message: "post-pay bookkeeping blew up".to_string(),
            }),
            PayStateClass::Ambiguous
        );
    }

    // ---- InternalPayState ----------------------------------------------------------------------

    // A decrypted preimage -> the recipient is provably paid.
    #[test]
    fn internal_preimage_is_success() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::Preimage(Preimage([0u8; 32]))),
            PayStateClass::Success
        );
    }

    // The one non-terminal internal state keeps awaiting.
    #[test]
    fn internal_funding_is_in_flight() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::Funding),
            PayStateClass::InFlight
        );
    }

    // MONEY-CRITICAL: the contract funding failed outright -> no money ever left. DEFINITIVE failure.
    #[test]
    fn internal_funding_failed_is_definitive_failure() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::FundingFailed { error: sm_err() }),
            PayStateClass::DefinitiveFailure
        );
    }

    // MONEY-CRITICAL: the internal payment was refunded successfully -> funds provably back.
    // DEFINITIVE failure -> a fresh pay is safe.
    #[test]
    fn internal_refund_success_is_definitive_failure() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::RefundSuccess {
                out_points: vec![],
                error: sm_err(),
            }),
            PayStateClass::DefinitiveFailure
        );
    }

    // MONEY-CRITICAL: a refund was ATTEMPTED and itself errored -> the funds' whereabouts are UNKNOWN,
    // so AMBIGUOUS; `await_pay` leaves the row PENDING rather than risk a second send.
    #[test]
    fn internal_refund_error_is_ambiguous() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::RefundError {
                error_message: "refund attempt failed".to_string(),
                error: sm_err(),
            }),
            PayStateClass::Ambiguous
        );
    }

    // MONEY-CRITICAL: an unclassified local error -> not proof of non-payment -> AMBIGUOUS.
    #[test]
    fn internal_unexpected_error_is_ambiguous() {
        assert_eq!(
            classify_internal_pay_state(&InternalPayState::UnexpectedError(
                "internal state machine blew up".to_string()
            )),
            PayStateClass::Ambiguous
        );
    }
}

#[cfg(test)]
mod pay_recovery_action_tests {
    //! PURE recovery-decision unit tests (lnrent-kum). The whole module is `#[cfg(feature =
    //! "fedimint")]` (see lib.rs), so these run under the default-on `cargo test -p lnrentd`.
    //! `pay_recovery_action` is free of the live client (the `net_payout_sat` /
    //! `classify_ln_pay_state` precedent), so which OPERATION a recovered row points at — the
    //! double-pay decision — is proven without a federation. The decision is ORDER-INDEPENDENT
    //! (fedimint's oplog is wall-clock-ordered, untrustworthy across a clock rollback) and its
    //! only dead signal is OUR OWN dead-op ledger, so no test here depends on scan position or on
    //! any fedimint-side cache/replay behavior.
    use super::{pay_idx_is_dead, pay_idx_record_dead, pay_recovery_action, PayRecoveryAction};
    use super::INDEX_SCHEMA;
    use rusqlite::Connection;
    use std::sync::Mutex;

    // No row at all + NOT ledger-dead: the daemon crashed before the FIRST upsert. Backfill — the
    // lnrent-4gt behavior.
    #[test]
    fn absent_row_is_backfilled() {
        assert_eq!(
            pay_recovery_action(None, "op-A", false),
            PayRecoveryAction::Backfill
        );
    }

    // RUNBOOK-UNBRICK (codex PR-32 P1): an absent-row candidate that IS ledger-dead must NOT count
    // as a live candidate — otherwise the operator's `fedimint_pay_dead_op` insert (the documented
    // ambiguity runbook) could never reduce the live count for an absent-row refusal and the
    // daemon would refuse to start forever. It backfills as FAILED bookkeeping instead (funds
    // provably back; drivers may retry normally).
    #[test]
    fn absent_row_dead_candidate_is_a_dead_backfill_not_a_live_one() {
        assert_eq!(
            pay_recovery_action(None, "op-A", true),
            PayRecoveryAction::BackfillDead
        );
    }

    // MONEY-CRITICAL (the whole point of lnrent-kum): operation A failed (row FAILED), a same-key
    // retry started operation B, and the daemon crashed before the upsert replaced A's row — B sits
    // in the oplog invisible behind the stale FAILED row, and B is NOT in the dead-op ledger (it
    // never definitively failed under our eyes: it may still be live). A later drive would see
    // FAILED and (if the invoice expired) start operation C while B is still in flight; B and C can
    // BOTH settle (each INV-1-capped individually, not jointly), so the OPERATOR pays twice.
    // Recovery must re-point the row at B as PENDING so the drivers re-await it.
    #[test]
    fn stale_failed_row_is_replaced_by_the_unrecorded_live_op() {
        assert_eq!(
            pay_recovery_action(Some(("op-A", "FAILED")), "op-B", false),
            PayRecoveryAction::ReplaceStaleFailed
        );
    }

    // ADVERSARIAL-REVIEW GUARD (order-independence): a candidate in OUR dead-op ledger already
    // definitively failed — funds provably back. It must never replace a FAILED row: the
    // resurrected op would replay its failure and burn a refund attempt, and with TWO dead ops
    // under one key each restart would re-point the row at the OTHER one forever — an oscillation
    // that parks the refund at its attempt cap. This ledger rule (not scan order, which fedimint's
    // wall-clock oplog cannot guarantee; not the outcome cache, which our await never writes; not
    // probing, which can poison that cache) is what keeps a genuinely-failed history closed.
    #[test]
    fn ledger_dead_candidate_never_resurrects() {
        assert_eq!(
            pay_recovery_action(Some(("op-A", "FAILED")), "op-B", true),
            PayRecoveryAction::Skip
        );
    }

    // The codex PR-review case: a NEWER `"(preflight-failed)"` placeholder (a cap refusal that
    // never started an op) must NOT be clobbered by a historical ledger-dead op — that would waste
    // the refund's attempt budget on a replayed dead failure and could park it at the cap. A
    // placeholder is only replaced by a possibly-LIVE unrecorded op (the true crash window; and a
    // same-key op after a placeholder reuses the SAME invoice/payment-hash, so even a mistaken
    // replace can never double-pay).
    #[test]
    fn placeholder_row_ignores_dead_ops_but_yields_to_live_ones() {
        assert_eq!(
            pay_recovery_action(Some(("(preflight-failed)", "FAILED")), "op-A", true),
            PayRecoveryAction::Skip
        );
        assert_eq!(
            pay_recovery_action(Some(("(preflight-failed)", "FAILED")), "op-B", false),
            PayRecoveryAction::ReplaceStaleFailed
        );
    }

    // A FAILED row pointing at the SAME op the oplog entry holds is a genuinely failed operation —
    // the FAILED mark is correct and must survive recovery (otherwise every restart would reopen
    // every historical definitive failure). Ledger-independent.
    #[test]
    fn genuinely_failed_op_stays_failed() {
        assert_eq!(
            pay_recovery_action(Some(("op-A", "FAILED")), "op-A", false),
            PayRecoveryAction::Skip
        );
        assert_eq!(
            pay_recovery_action(Some(("op-A", "FAILED")), "op-A", true),
            PayRecoveryAction::Skip
        );
    }

    // A PENDING row is never touched: after a replace installs the live unrecorded op as PENDING,
    // every other oplog entry for the key — at ANY scan position, ledger-dead or live — must leave
    // it alone. This is the convergence half of the order-independence argument.
    #[test]
    fn pending_row_is_never_touched() {
        assert_eq!(
            pay_recovery_action(Some(("op-B", "PENDING")), "op-A", false),
            PayRecoveryAction::Skip
        );
    }

    // A completed pay is never reopened, whatever operation an oplog entry names.
    #[test]
    fn succeeded_row_is_never_touched() {
        assert_eq!(
            pay_recovery_action(Some(("op-A", "SUCCEEDED")), "op-B", false),
            PayRecoveryAction::Skip
        );
    }

    // The dead-op LEDGER itself: append-only, idempotent, and an op is dead only after it was
    // recorded. `await_pay` records at its DefinitiveFailure choke points, regardless of the row
    // CAS outcome — dead-ness is a property of the OPERATION, not the row.
    #[test]
    fn dead_op_ledger_roundtrip() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(INDEX_SCHEMA).unwrap();
        let index = Mutex::new(conn);

        assert!(!pay_idx_is_dead(&index, "op-A").unwrap());
        pay_idx_record_dead(&index, "op-A").unwrap();
        assert!(pay_idx_is_dead(&index, "op-A").unwrap());
        // Idempotent: a duplicate record is a no-op, not an error.
        pay_idx_record_dead(&index, "op-A").unwrap();
        assert!(pay_idx_is_dead(&index, "op-A").unwrap());
        assert!(!pay_idx_is_dead(&index, "op-B").unwrap());
    }
}
