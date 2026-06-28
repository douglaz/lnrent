//! Real Fedimint `PaymentBackend` (lnrent-7fp.4, ADR-0012/0015) — ecash via an EXISTING federation
//! + gateway, replacing `MockPayment`. Feature-gated behind the `fedimint` cargo feature (default
//! OFF) so mock-only builds stay light and CI without a federation keeps passing. Daemon-only:
//! `fedimint-client` 0.11.1 + `fedimint-rocksdb` (a 2nd DB engine, bundled C++) are never compiled
//! into the wasm buyer.
//!
//! `.4b` (THIS) wires the heavy deps + the client construction (join/open the federation) + the
//! lnrent-owned sqlite idempotency index. The `PaymentBackend` methods are stubbed; the receive/pay
//! state-machine wiring is `.4c`:
//!  - `.4c.2` create_invoice / watch / lookup (gateway bolt11 + `subscribe_ln_receive` fan-in)
//!  - `.4c.3` pay / payment_status (gateway `pay_bolt11_invoice` + `subscribe_ln_pay`)
//!
//! Design (folding in the codex .4 review):
//!  - **Gateway required** — `create_bolt11_invoice` / `pay_bolt11_invoice` with no gateway are
//!    internal-only / fail `NoLnGatewayAvailable`; `.4c` selects one via `get_gateway(None,false)` /
//!    `select_available_gateway` after `update_gateway_cache`.
//!  - **Idempotency on `external_id`** — an lnrent-owned sqlite index (NOT fedimint's rocksdb), plus
//!    `extra_meta = {"lnrent_external_id": …}` stamped into the fedimint operation so a boot oplog
//!    scan can backfill the index after a crash between minting and persisting (codex finding #3).
//!  - **Root secret** — lnrent's 32-byte secret (`identity.rs`) is wrapped as a fedimint
//!    `DerivableSecret` via `new_root`, under `StandardDoubleDerive` (the standard per-federation
//!    derivation), so the client position is deterministically recoverable from the operator seed.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use rusqlite::Connection;

use fedimint_client::{Client, ClientHandleArc, RootSecret};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::db::Database;
use fedimint_core::invite_code::InviteCode;
use fedimint_derive_secret::DerivableSecret;
use fedimint_ln_client::LightningClientInit;
use fedimint_mint_client::MintClientInit;
use fedimint_rocksdb::RocksDb;
use fedimint_wallet_client::WalletClientInit;

use crate::backends::{Invoice, PayStatus, PaymentBackend, PaymentStatus, Settlement};

/// HKDF salt for wrapping lnrent's (provisional) 32-byte Fedimint root secret (`identity.rs`,
/// already domain-separated by `lnrent:fedimint:v1`) into a fedimint `DerivableSecret`. Fixed +
/// versioned so the derived client secret — and thus the ecash position — is deterministic and
/// recoverable from the operator seed (codex `.4b` note).
const ROOT_SECRET_SALT: &[u8] = b"lnrent:fedimint:client:v1";

/// The lnrent-owned sqlite index, per federation data-dir. It is the idempotency anchor for
/// `create_invoice`/`pay` (keyed by `external_id` / `idempotency_key`) and closes the crash window
/// between fedimint committing an operation and the daemon persisting its own row. NOT inside
/// fedimint's rocksdb (codex finding #3).
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
CREATE TABLE IF NOT EXISTS fedimint_pay (
    idempotency_key  TEXT PRIMARY KEY,
    operation_id     TEXT NOT NULL,
    backend_pay_id   TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING'
);
";

/// Real Fedimint backend: holds the joined fedimint client and the lnrent-owned idempotency index.
/// The `watch()` settlement fan-in manager + the receive/pay state-machine wiring land in `.4c`.
pub struct FedimintPayment {
    // `.4c` reads these; `.4b` only constructs them, hence the allow.
    #[allow(dead_code)]
    client: ClientHandleArc,
    #[allow(dead_code)]
    index: Mutex<Connection>,
}

impl FedimintPayment {
    /// Join (first run) or open (subsequent runs) the federation named by `invite_code`. The
    /// fedimint client rocksdb + the lnrent index sqlite both live under
    /// `data_dir/fedimint/<federation_id>/`. `root_secret` is lnrent's deterministic 32-byte seed
    /// (`identity.rs`), wrapped as a fedimint `DerivableSecret` under `StandardDoubleDerive`.
    pub async fn join_or_open(
        invite_code: &str,
        data_dir: &Path,
        root_secret: &[u8; 32],
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

        Ok(Self {
            client,
            index: Mutex::new(conn),
        })
    }
}

#[async_trait]
impl PaymentBackend for FedimintPayment {
    async fn create_invoice(
        &self,
        _amount_sat: u64,
        _memo: &str,
        _expiry_s: u32,
        _external_id: &str,
    ) -> Result<Invoice> {
        todo!(".4c.2: select gateway + create_bolt11_invoice, idempotent on external_id via the index")
    }
    async fn lookup(&self, _id: &str) -> Result<PaymentStatus> {
        todo!(".4c.2: index/operation status -> PaymentStatus")
    }
    async fn pay(&self, _dest: &str, _amount_sat: u64, _idempotency_key: &str) -> Result<String> {
        todo!(".4c.3: pay_bolt11_invoice via a selected gateway, idempotent on the key")
    }
    async fn payment_status(&self, _payment_id: &str) -> Result<PayStatus> {
        todo!(".4c.3: subscribe_ln_pay terminal state -> PayStatus")
    }
    async fn payment_status_by_key(&self, _idempotency_key: &str) -> Result<PayStatus> {
        todo!(".4c.3: pay index / operation-log status by key")
    }
    async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
        todo!(
            ".4c.2: fan-in subscribe_ln_receive across open ops + boot re-subscribe -> Settlement"
        )
    }
}
