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

use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;
use tokio::sync::mpsc;

use fedimint_client::{Client, ClientHandleArc, OperationId, RootSecret};
use fedimint_client_module::oplog::UpdateStreamOrOutcome;
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
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
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let invite: InviteCode = invite_code
            .parse()
            .context("parsing federation invite code")?;
        let fed_dir = data_dir
            .join("fedimint")
            .join(invite.federation_id().to_string());
        std::fs::create_dir_all(&fed_dir).context("creating fedimint data dir")?;

        let db: Database = RocksDb::build(fed_dir.join("client.db"))
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

        let conn =
            Connection::open(fed_dir.join(INDEX_DB_FILE)).context("opening lnrent index db")?;
        conn.execute_batch(INDEX_SCHEMA)
            .context("initialising lnrent index schema")?;

        let me = Self {
            client,
            index: Arc::new(Mutex::new(conn)),
            settle_tx: Mutex::new(None),
            clock,
        };

        // Backfill any invoice fedimint committed but the daemon never indexed (crash window). Best
        // effort: on failure, create_invoice still self-heals via the index on the next attempt.
        match recover_index_from_oplog(&me.client, &me.index).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(
                backfilled = n,
                "fedimint: recovered invoice index rows from oplog"
            ),
            Err(e) => {
                tracing::warn!(error = %e, "fedimint: oplog index recovery failed (continuing)")
            }
        }

        Ok(me)
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
            .get_gateway(None, false)
            .await
            .context("selecting a lightning gateway")?
            .context("federation has no available lightning gateway")?;

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

    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String> {
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
                    return self.await_pay(pt, idempotency_key).await;
                }
                _ => {} // FAILED -> re-attempt below (the prior payment refunded; not a double-pay)
            }
        }

        let invoice = Bolt11Invoice::from_str(dest).context("parsing refund bolt11")?;
        let inv_msat = invoice
            .amount_milli_satoshis()
            .context("refund bolt11 has no amount")?;
        anyhow::ensure!(
            inv_msat == amount_sat.saturating_mul(1000),
            "refund bolt11 amount {inv_msat} msat != owed {} msat",
            amount_sat.saturating_mul(1000)
        );

        let ln = self
            .client
            .get_first_module::<LightningClientModule>()
            .context("fedimint: no lightning module")?;
        let gateway = ln
            .get_gateway(None, false)
            .await
            .context("selecting a lightning gateway for the refund")?
            .context("federation has no available lightning gateway")?;
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
        pay_idx_upsert(&self.index, idempotency_key, &op_hex, "PENDING", kind)?;
        self.await_pay(outgoing.payment_type, idempotency_key).await
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

    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        *self.settle_tx.lock().unwrap() = Some(tx.clone());

        // Boot/restart re-subscribe: stream every still-OPEN invoice. A cached (settled-while-down)
        // op is handled inside run_receive_task (mark PAID, no live push; catch-up recovers it).
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

/// Stream one invoice operation to settlement. A LIVE `Claimed` (the op was still pending when we
/// subscribed) marks the index PAID and pushes a `Settlement{settled_at = now}`. A CACHED terminal
/// `Claimed` (already settled at subscribe time) only marks PAID — the supervisor catch-up recovers
/// it with a safe capped timestamp, so a settlement observed late never over-credits.
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

    match sub {
        UpdateStreamOrOutcome::Outcome(state) => {
            // Cached/terminal at subscribe time — settled (or failed) before we watched.
            if matches!(state, LnReceiveState::Claimed) {
                if let Err(e) = idx_mark_paid(&index, &op_hex, None) {
                    tracing::error!(op = %op_hex, error = %e, "fedimint: index mark-paid (cached) failed");
                }
            }
        }
        UpdateStreamOrOutcome::UpdateStream(mut stream) => {
            while let Some(state) = stream.next().await {
                match state {
                    LnReceiveState::Claimed => {
                        let settled_at = clock.now();
                        if let Err(e) = idx_mark_paid(&index, &op_hex, Some(settled_at)) {
                            tracing::error!(op = %op_hex, error = %e, "fedimint: index mark-paid failed");
                        }
                        let _ = tx
                            .send(Settlement {
                                invoice_id,
                                external_id,
                                amount_sat,
                                settled_at,
                            })
                            .await;
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
            let meta: LightningOperationMeta = entry.meta();
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
