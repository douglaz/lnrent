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
use fedimint_mint_client::MintClientInit;
use fedimint_rocksdb::RocksDb;
use fedimint_wallet_client::WalletClientInit;

use crate::backends::{Invoice, PayStatus, PaymentBackend, PaymentStatus, Settlement};
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
CREATE TABLE IF NOT EXISTS fedimint_pay (
    idempotency_key  TEXT PRIMARY KEY,
    operation_id     TEXT NOT NULL,
    backend_pay_id   TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    pay_kind         TEXT NOT NULL DEFAULT 'ln'
);
";

/// Real Fedimint backend: the joined fedimint client, the lnrent-owned idempotency index, the
/// registered settlement sender (set by `watch()`), and a clock for observed-settlement timestamps.
pub struct FedimintPayment {
    client: ClientHandleArc,
    index: Arc<Mutex<Connection>>,
    settle_tx: Mutex<Option<mpsc::Sender<Settlement>>>,
    clock: Arc<dyn Clock>,
    /// The operator-configured gateway pubkey (`config.fedimint.gateway`), HONORED for BOTH invoice
    /// creation and refunds rather than a random pick (codex o6p). `None` selects any available gateway
    /// (tests / unset). `get_gateway(Some(id), false)` fails CLOSED if that gateway is unavailable, so a
    /// misconfigured/offline gateway errors rather than silently routing money through a different one.
    gateway: Option<PublicKey>,
    /// Serializes `create_invoice`'s check->mint->insert so two concurrent same-`external_id` callers
    /// can't both mint a gateway invoice (the loser would otherwise be stranded — absent from the
    /// index, never watched). Async so it can be held across the mint `.await` (codex P1).
    create_lock: tokio::sync::Mutex<()>,
}

impl FedimintPayment {
    /// Join (first run) or open (subsequent runs) the federation named by `invite_code`. The
    /// fedimint client rocksdb + the lnrent index sqlite both live under
    /// `data_dir/fedimint/<federation_id>/`. `root_secret` is lnrent's deterministic 32-byte seed
    /// (`identity.rs`), wrapped as a fedimint `DerivableSecret` under `StandardDoubleDerive`. On
    /// open it backfills the index from the fedimint oplog (crash-window recovery).
    pub async fn join_or_open(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
        gateway: Option<&str>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let invite: InviteCode = invite_code
            .parse()
            .context("parsing federation invite code")?;
        // Parse the configured gateway pubkey now (fail FAST on a malformed value) — honored for both
        // receive + refund below; None = any available gateway (tests / unset).
        let gateway = gateway
            .map(PublicKey::from_str)
            .transpose()
            .context("parsing the configured fedimint gateway pubkey (config.fedimint.gateway)")?;
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

        let me = Self {
            client,
            index: Arc::new(Mutex::new(conn)),
            settle_tx: Mutex::new(None),
            clock,
            gateway,
            create_lock: tokio::sync::Mutex::new(()),
        };

        // Backfill any invoice fedimint committed but the daemon never indexed (the crash window
        // between minting and idx_insert). FAIL-CLOSED (codex P1): refusing to start on a recovery
        // error is safer for real money than reopening the duplicate-mint window by continuing.
        let recovered = recover_index_from_oplog(&me.client, &me.index)
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
        // would re-parse the maybe-expired bolt11 and fail instead of re-awaiting the op. FAIL-CLOSED.
        let recovered_pay = recover_pay_from_oplog(&me.client, &me.index)
            .await
            .context("fedimint: oplog pay recovery failed; refusing to start")?;
        if recovered_pay > 0 {
            tracing::info!(
                backfilled = recovered_pay,
                "fedimint: recovered pay index rows from oplog"
            );
        }

        me.log_readiness().await;
        Ok(me)
    }

    /// On open, log a one-line Fedimint operability summary: the ecash balance and whether the
    /// CONFIGURED gateway is reachable. A zero balance is fine when the daemon has no liabilities; the
    /// supervisor owns the money-readiness warning because it can see the store. This backend-level log
    /// warns only when the gateway is unreachable, since that blocks invoice creation and refunds.
    /// Observability ONLY — enables no payment and never fails open.
    async fn log_readiness(&self) {
        let balance_msat = match self.available_balance_msat().await {
            Ok(msat) => msat,
            Err(e) => {
                tracing::info!(error = %e, "fedimint: could not query ecash balance at startup");
                None
            }
        };
        let gateway_ok = match self.refund_gateway_ready().await {
            Ok(ok) => ok,
            Err(e) => {
                tracing::info!(error = %e, "fedimint: could not query configured gateway at startup");
                false
            }
        };
        tracing::info!(
            balance_msat = ?balance_msat,
            balance_sat = ?balance_msat.map(|msat| msat / 1000),
            gateway_ok,
            "fedimint readiness"
        );
        if fedimint_readiness_warns(balance_msat, gateway_ok) {
            tracing::warn!(
                balance_msat = ?balance_msat,
                balance_sat = ?balance_msat.map(|msat| msat / 1000),
                gateway_ok,
                "fedimint gateway unreachable: cannot create invoices or pay refunds"
            );
        }
    }

    /// Await a refund payment to a terminal state, recording the outcome in the pay index and
    /// returning the backend payment id (the operation-id hex) on success, or an error on a
    /// definitive failure. Outbound, so there is no settled_at/over-credit concern — `into_stream()`
    /// (which replays a cached terminal outcome as a single item) is sufficient, unlike `watch()`.
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
                while let Some(state) = updates.next().await {
                    match state {
                        LnPayState::Success { .. } => {
                            pay_idx_mark(&self.index, key, "SUCCEEDED")?;
                            return Ok(op_hex);
                        }
                        // Created / Funded / AwaitingChange / WaitingForRefund -> keep waiting.
                        LnPayState::Created
                        | LnPayState::Funded { .. }
                        | LnPayState::AwaitingChange
                        | LnPayState::WaitingForRefund { .. } => {}
                        // Refunded / Canceled / UnexpectedError -> definitive failure.
                        other => {
                            pay_idx_mark(&self.index, key, "FAILED")?;
                            anyhow::bail!("refund payment failed: {other:?}");
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
                    match state {
                        InternalPayState::Preimage(_) => {
                            pay_idx_mark(&self.index, key, "SUCCEEDED")?;
                            return Ok(op_hex);
                        }
                        InternalPayState::Funding => {}
                        other => {
                            pay_idx_mark(&self.index, key, "FAILED")?;
                            anyhow::bail!("internal refund payment failed: {other:?}");
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
    async fn fail_pay_preflight(&self, key: &str, msg: String) -> Result<String> {
        pay_idx_upsert(&self.index, key, "(preflight-failed)", "FAILED", "ln")?;
        anyhow::bail!(msg)
    }

    /// The shared refund-pay engine behind [`PaymentBackend::pay`] and
    /// [`PaymentBackend::pay_refund_capped`]. Idempotent on the key (SUCCEEDED never re-pays; PENDING
    /// re-awaits the SAME operation; FAILED/absent starts fresh). When `gross_cap` is `Some(received)`
    /// it enforces the final INV-1 cap against the SAME gateway object used to start the payment, so a
    /// NEW outbound op whose payout + advertised fee exceeds the received gross is refused before any
    /// money moves (spec §3.1). `None` (the plain `pay`) skips the cap.
    async fn pay_inner(
        &self,
        dest: &str,
        amount_sat: u64,
        idempotency_key: &str,
        gross_cap: Option<u64>,
    ) -> Result<String> {
        // Idempotent on the key: a SUCCEEDED key never re-pays; a PENDING key re-awaits the SAME
        // operation (a crash mid-pay resumes — fedimint persists the op); a FAILED key (the prior LN
        // attempt was refunded back to us, so no funds left) or an absent key initiates a fresh
        // attempt. fedimint additionally dedups per invoice payment-hash internally.
        if let Some((op_hex, status, kind)) = pay_idx_get(&self.index, idempotency_key)? {
            match status.as_str() {
                "SUCCEEDED" => return Ok(op_hex),
                "PENDING" => {
                    let op = OperationId::from_str(&op_hex)
                        .map_err(|e| anyhow!("invalid stored pay operation id: {e}"))?;
                    let pt = if kind == "internal" {
                        PayType::Internal(op)
                    } else {
                        PayType::Lightning(op)
                    };
                    return self.await_pay_bounded(pt, idempotency_key).await;
                }
                _ => {} // FAILED -> re-attempt below (the prior payment refunded; not a double-pay)
            }
        }

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
        let gateway = ln
            .get_gateway(self.gateway, false)
            .await
            .context("selecting the configured lightning gateway for the refund")?
            .context("the configured (or any) lightning gateway is unavailable")?;
        // Final INV-1 cap preflight (spec §3.1): quoting and paying are separate awaits, so re-check
        // the cap here against the SAME gateway object we pass into pay_bolt11_invoice — refuse to
        // START a new outbound op whose payout + advertised fee would exceed the received gross. The
        // fee MUST be Fedimint's ACTUAL fee ([`gateway_fee_msat`], mirroring RoutingFees::to_amount),
        // not the naive floor(x*ppm/1e6) — see that helper. All msat arithmetic is widened to u128.
        if let Some(gross_sat) = gross_cap {
            let fees = gateway.fees;
            let pay_msat_u128 = u128::from(pay_msat);
            let over_cap = match gateway_fee_msat(
                u64::from(fees.base_msat),
                u64::from(fees.proportional_millionths),
                pay_msat_u128,
            ) {
                Some(fee_msat) => pay_msat_u128 + fee_msat > u128::from(gross_sat) * 1000,
                None => true, // an unpayable (>100%) schedule: never start an over-gross op
            };
            if over_cap {
                return self
                    .fail_pay_preflight(
                        idempotency_key,
                        format!(
                            "refund payout {amount_sat} sat + gateway fee exceeds the {gross_sat} sat \
                             received (INV-1 cap)"
                        ),
                    )
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
        pay_idx_upsert(&self.index, idempotency_key, &op_hex, "PENDING", kind)?;
        self.await_pay_bounded(outgoing.payment_type, idempotency_key)
            .await
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
        let _create_guard = self.create_lock.lock().await;
        // Idempotent on external_id: a repeat (or crash-retry) returns the stored invoice, never a
        // second gateway invoice.
        if let Some(inv) = idx_get_by_external(&self.index, external_id)? {
            return Ok(inv);
        }

        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        // A gateway is REQUIRED for an externally-payable invoice (codex finding #1). get_gateway
        // refreshes the cache then picks one; Err if the federation has none registered.
        let gateway = ln
            .get_gateway(self.gateway, false)
            .await
            .context("selecting the configured lightning gateway")?
            .context("the configured (or any) lightning gateway is unavailable")?;

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
        // key idempotency and NO INV-1 cap. The Refunder uses pay_refund_capped when it knows the
        // gross, so the cap is enforced on the money path (spec §3.1).
        self.pay_inner(dest, amount_sat, idempotency_key, None)
            .await
    }

    /// INV-1 fee-bearing quote (spec §3.1): the largest whole-sat payout for `gross_sat` whose payout
    /// plus the configured gateway's advertised fee fits inside `gross_sat`. Read-only — never mints,
    /// pays, or mutates.
    async fn refund_net_sat(&self, gross_sat: u64) -> Result<u64> {
        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        // The SAME configured gateway the refund pay will use — its advertised fee schedule is the
        // operator's exposure. A gateway that cannot be read is a TRANSIENT quote failure (Err), never
        // dust/Ok(0), so the Refunder leaves the row PENDING instead of parking it (spec §3.1).
        let gateway = ln
            .get_gateway(self.gateway, false)
            .await
            .context("selecting the configured lightning gateway for the refund fee quote")?
            .context("the configured (or any) lightning gateway is unavailable")?;
        let fees = gateway.fees;
        Ok(net_payout_sat(
            u64::from(fees.base_msat),
            u64::from(fees.proportional_millionths),
            gross_sat,
        ))
    }

    async fn refund_required_outlay_msat(
        &self,
        gross_sat: u64,
        pay_sat: Option<u64>,
    ) -> Result<u128> {
        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        let gateway = ln
            .get_gateway(self.gateway, false)
            .await
            .context("selecting the configured lightning gateway for the refund liquidity check")?
            .context("the configured (or any) lightning gateway is unavailable")?;
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
        self.pay_inner(bolt11, amount_sat, idempotency_key, Some(gross_sat))
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
        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        Ok(ln.get_gateway(self.gateway, false).await?.is_some())
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
                return;
            }
            LnReceiveState::Canceled { reason } => {
                tracing::warn!(op = %op_hex, ?reason, "fedimint: ln receive canceled");
                return;
            }
            _ => {}
        }
    }
}

/// Scan the fedimint operation log for Receive ops stamped with an `lnrent_external_id` that the
/// index is missing, and backfill them — closing the window where fedimint committed an invoice but
/// the daemon crashed before persisting the index row (codex finding #3).
async fn recover_index_from_oplog(
    client: &ClientHandleArc,
    index: &Arc<Mutex<Connection>>,
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
                expires_at: invoice
                    .expires_at()
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
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

/// Symmetric to [`recover_index_from_oplog`] but for OUTBOUND pays (lnrent-4gt). A crash in [`pay`]'s
/// window — between `pay_bolt11_invoice` committing the fedimint operation and `pay_idx_upsert`
/// persisting the local row — leaves the idempotency key with NO `fedimint_pay` row, so
/// `payment_status_by_key` reports `Unknown`. The Refunder's next `pay(key)` would then re-parse the
/// original bolt11 (which may have EXPIRED in the meantime) and fail before discovering the in-flight
/// op. Backfilling the row (the op id + ln/internal kind) from the oplog `extra_meta` on open lets
/// `pay(key)` take its early path and re-await the OPERATION directly. Backfilled as `PENDING`; the
/// next `pay(key)` reconstructs the `PayType` from (op, kind) and resolves it to terminal. IDEMPOTENT
/// (skips keys already indexed); an undecodable oplog entry is logged + skipped (matching the receive
/// side); `join_or_open` is fail-closed on a pass-level error (refuses to start).
async fn recover_pay_from_oplog(
    client: &ClientHandleArc,
    index: &Arc<Mutex<Connection>>,
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
            if pay_idx_status_by_key(index, idk)?.is_some() {
                continue;
            }
            let op_hex = key.operation_id.fmt_full().to_string();
            let kind = if pay.is_internal_payment {
                "internal"
            } else {
                "ln"
            };
            pay_idx_upsert(index, idk, &op_hex, "PENDING", kind)?;
            backfilled += 1;
        }
        last = page.last().map(|(k, _)| *k);
        if count < 100 {
            break;
        }
    }
    Ok(backfilled)
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
fn pay_idx_upsert(
    index: &Mutex<Connection>,
    key: &str,
    op_hex: &str,
    status: &str,
    kind: &str,
) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "INSERT INTO fedimint_pay (idempotency_key, operation_id, backend_pay_id, status, pay_kind)
         VALUES (?1, ?2, ?2, ?3, ?4)
         ON CONFLICT(idempotency_key)
           DO UPDATE SET operation_id = ?2, backend_pay_id = ?2, status = ?3, pay_kind = ?4",
        params![key, op_hex, status, kind],
    )?;
    Ok(())
}

fn pay_idx_mark(index: &Mutex<Connection>, key: &str, status: &str) -> Result<()> {
    let conn = index.lock().unwrap();
    conn.execute(
        "UPDATE fedimint_pay SET status = ?2 WHERE idempotency_key = ?1",
        params![key, status],
    )?;
    Ok(())
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

fn fedimint_readiness_warns(_balance_msat: Option<u64>, gateway_ok: bool) -> bool {
    !gateway_ok
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
    fn readiness_warns_on_gateway_only_not_zero_balance() {
        assert!(!super::fedimint_readiness_warns(Some(0), true));
        assert!(!super::fedimint_readiness_warns(Some(1_500), true));
        assert!(super::fedimint_readiness_warns(Some(0), false));
    }
}
