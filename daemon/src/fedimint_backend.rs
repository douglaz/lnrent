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
        if inv_msat != amount_sat.saturating_mul(1000) {
            return self
                .fail_pay_preflight(
                    idempotency_key,
                    format!(
                        "refund bolt11 amount {inv_msat} msat != owed {} msat",
                        amount_sat.saturating_mul(1000)
                    ),
                )
                .await;
        }

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
        // Crash window: a crash between pay_bolt11_invoice committing and this upsert leaves no key
        // row -> payment_status_by_key=Unknown. recover_pay_from_oplog (on open, symmetric to
        // recover_index_from_oplog) backfills the row from the oplog extra_meta so the next pay(key)
        // re-awaits the OP directly rather than re-parsing the maybe-expired bolt11 (lnrent-4gt).
        pay_idx_upsert(&self.index, idempotency_key, &op_hex, "PENDING", kind)?;
        self.await_pay_bounded(outgoing.payment_type, idempotency_key)
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
