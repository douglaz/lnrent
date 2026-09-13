//! ADR-0022 boot-time import of a PRE-ADR-0022 backend side file (`phoenixd_index.db` beside the
//! state DB, `lnv2_index.db` under the federation dir) into `lnrent.sqlite` (lnrent-chgb).
//!
//! Runs ONCE per data dir, after the store is open and the payment backend is constructed (the
//! import needs the backend's live view to break a books-vs-map tie) and before the supervisor starts
//! (the books are quiet). Keyed by the RESOLVED payment mode — `mock` has nothing to import — and
//! recorded durably in the `migration` table (one row per side-file name) INSIDE the import
//! transaction, so a crash between the commit and the file rename is recognised on the next boot
//! (marker present, hash matches) as already imported: rename and continue, never refuse.
//!
//! What it validates before it records completion, and why (ADR-0022 "The import validates coverage
//! before it records completion"): a present, non-empty side file can still be the stale or
//! mismatched one that produced the pre-ADR-0022 "index divergence" incidents, and after this import
//! the missing-row arms in both backends are ASSERTIONS, so this is the last point at which a gap can
//! be reported. Every backend-referencing book row must have a correlation row THAT AGREES:
//!
//! - `invoice` rows carrying the backend's id prefix, against the EFFECTIVE receive-map row for their
//!   `external_id` (phoenixd: the newest `rowid`, since its upsert conflicts on `invoice_id` and a
//!   replacement leaves the old row in place; lnv2: the single row). A book row already eligible under
//!   the store's own retention whose map row is absent or `CANCELED` is pre-reaped instead, so an
//!   `lnv2-*` EXPIRED row whose CANCELED map row the lnv2 reaper legitimately removed does not refuse
//!   boot. A REPLACEMENT (same `external_id`, unsettled OPEN/EXPIRED book row, map row with a DIFFERENT
//!   invoice id that is OPEN / PAID / PAID_UNRECOVERED — never CANCELED) is repaired from the map ONLY
//!   when the backend's current state positively establishes the map row (paid or still payable;
//!   lnv2's `Failure` counts, it is the still-effective PAID_UNRECOVERED receive) AND positively reports
//!   the book row's own invoice terminal-unpaid. Absence proves nothing (phoenixd forgets; a
//!   rolled-back `client.db` can have lost an op), so "A present, B absent" refuses rather than
//!   rewriting B back to A. A PAID or settled book row that disagrees always refuses: money was booked
//!   against data the map no longer describes.
//! - every `SENT` refund/sweep attempt, and every attempt with a `backend_payment_id`, against the pay
//!   map by its DERIVED pay key (`gen_key(external_id, resolution_gen)` for refunds — the stored
//!   `idempotency_key` is the bare gen-0 key; `sweep_attempt.id` for sweeps): the row must exist, its
//!   `bolt11` must equal the attempt's effective bolt11, its payment/operation id must equal
//!   `backend_payment_id` when the attempt has one (recovery commits SENT with NULL there), and for a
//!   SENT attempt it must be `SUCCEEDED` — `pay_other_key_for_hash` skips FAILED rows, so a SENT/FAILED
//!   pair would leave the paid hash unowned. Disagreement refuses; there is no repair for pay rows.
//! - non-terminal and retryable-FAILED attempts WITHOUT a `backend_payment_id`: a PRESENT pay row must
//!   agree (bolt11) or the import refuses; a MISSING one cannot be told apart from "never started",
//!   so the attempt is stamped `migration_unverified_at` and PARKED (the drivers refuse `prepare_pay`
//!   for it) until the backend's own audit adopts a positive match or the operator clears it. That
//!   includes an unresolved-LNURL PENDING row: the books can be a rollback from before the
//!   resolution while the wallet paid after.
//! - every legacy `phoenixd_unbookable_settlement` timer must resolve to an imported receive-map row
//!   (it holds only the phoenixd invoice id; ADR-0023 keys the condition by `external_id`).
//!
//! With the side file ABSENT: correlation-bearing book rows (the id prefix, a SENT attempt, an
//! attempt with a `backend_payment_id`) refuse boot naming the lost file and the runbook — the
//! persisted `payment_backend` selection alone is NOT a reference, a fresh bootstrap writes it before
//! any correlation exists. Otherwise every non-terminal / retryable-FAILED attempt without a
//! `backend_payment_id` is stamped and parked, and a marker is still written (`parked`, or `fresh` when
//! there was nothing to stamp) so the v3 backup writer keys on it from the first migrated boot.
//!
//! The genuinely ambiguous state — side file present, the new tables already non-empty, and no
//! marker (or a marker for a different hash) — refuses boot; so does a populated table with no marker
//! and no file (unknown provenance).

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use crate::store::{Store, TERMINAL_ROW_RETENTION_SECS};

/// Where the operator reads what to do about a refusal below.
pub const RUNBOOK: &str = "docs/go-live.md § \"Upgrading to the self-contained state DB (ADR-0022)\"";

/// The lnv2 side-file name, duplicated here (not read from `lnv2_backend`) so this module — which the
/// backup/restore path also uses — compiles without the `fedimint` feature.
pub const LNV2_LEGACY_INDEX_FILE: &str = "lnv2_index.db";

/// Which backend's side file and tables an import concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Phoenixd,
    Lnv2,
}

impl Backend {
    /// The side-file name, which is also the `migration.name` the marker is keyed by.
    pub fn side_file_name(self) -> &'static str {
        match self {
            Backend::Phoenixd => crate::phoenixd_backend::LEGACY_INDEX_FILE,
            Backend::Lnv2 => LNV2_LEGACY_INDEX_FILE,
        }
    }

    /// The prefix the backend's `create_invoice` stamps on the store's `invoice.id` — the ONLY honest
    /// "this row references that backend" predicate (`backend_invoice_id` holds the bare hash/op).
    pub fn invoice_prefix(self) -> &'static str {
        match self {
            Backend::Phoenixd => "phoenixd-",
            Backend::Lnv2 => "lnv2-",
        }
    }

    fn receive_table(self) -> &'static str {
        match self {
            Backend::Phoenixd => "phoenixd_invoice",
            Backend::Lnv2 => "lnv2_invoice",
        }
    }

    fn pay_table(self) -> &'static str {
        match self {
            Backend::Phoenixd => "phoenixd_pay",
            Backend::Lnv2 => "lnv2_pay",
        }
    }
}

/// The backend's CURRENT view of one receive — the tiebreaker for a books-vs-map disagreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveState {
    /// Paid, or still payable (phoenixd: `isPaid` or not `isExpired`; lnv2: `Claimed`, `Failure` —
    /// the still-effective PAID_UNRECOVERED receive — or a pending/claiming op). Positively the
    /// effective invoice.
    Established,
    /// Positively terminal and unpaid (phoenixd: `isExpired` and not paid; lnv2: `Expired`).
    TerminalUnpaid,
    /// The backend has no record. Proves NOTHING (phoenixd forgets; a rolled-back client db can have
    /// lost an operation), so it never licenses a repair.
    Absent,
}

/// What the import needs from the backend: its live answer for one receive.
#[async_trait]
pub trait LegacyProbe: Send + Sync {
    /// The backend's current state for the receive `invoice_id` (with `payment_hash` and the
    /// correlation `external_id`, whichever the backend keys its history by).
    async fn receive_state(
        &self,
        external_id: &str,
        invoice_id: &str,
        payment_hash: &str,
    ) -> Result<ReceiveState>;
}

/// What one boot's import did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A marker was already present and no side file was found: an ordinary later boot.
    AlreadyMigrated,
    /// The marker matched the file's hash but the file was still there (a crash between the import
    /// commit and the rename): renamed, nothing re-imported.
    RenamedOnly,
    /// The side file was imported and renamed `*.imported`.
    Imported {
        receive_rows: usize,
        pay_rows: usize,
        repaired: usize,
        reaped: usize,
        stamped: usize,
    },
    /// No side file, no correlation-bearing rows, nothing to fence: marker `fresh` written.
    Fresh,
    /// No side file, no correlation-bearing rows, but attempts whose witness would have been in it:
    /// stamped and parked; marker `parked` written.
    Parked { stamped: usize },
}

/// Run the import for `backend` against `side_file`. See the module header for the contract.
pub async fn run(
    store: &Store,
    backend: Backend,
    probe: &dyn LegacyProbe,
    side_file: &Path,
    now: i64,
) -> Result<Outcome> {
    let name = backend.side_file_name().to_string();
    let marker = {
        let name = name.clone();
        store.read(move |c| read_marker(c, &name)).await?
    };
    let present = match std::fs::symlink_metadata(side_file) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => {
            return Err(e).with_context(|| format!("stat legacy index {}", side_file.display()))
        }
    };
    if !present {
        return import_absent(store, backend, marker, &name, now).await;
    }

    // Present. Vet it exactly as the backend used to before opening it (symlink-refused, owner-only).
    crate::fedimint_paths::prepare_private_file(side_file, "legacy backend index")
        .with_context(|| format!("vetting legacy index {}", side_file.display()))?;
    let bytes = std::fs::read(side_file)
        .with_context(|| format!("reading legacy index {}", side_file.display()))?;
    let hash = hex::encode(Sha256::digest(&bytes));
    let tables_non_empty = {
        store
            .read(move |c| backend_tables_non_empty(c, backend))
            .await?
    };
    match marker.as_deref() {
        Some(m) if m == hash => {
            // Crash after the import commit, before the rename: already imported, finish the rename.
            rename_imported(side_file)?;
            return Ok(Outcome::RenamedOnly);
        }
        Some(other) => bail!(
            "refusing to boot: legacy index {} is present but this database already records an \
             import of a DIFFERENT {} (marker hash {other}, file hash {hash}). One of the two was \
             restored from another instant; the operator must decide which is current. {RUNBOOK}",
            side_file.display(),
            name
        ),
        None if tables_non_empty => bail!(
            "refusing to boot: legacy index {} is present, the {} / {} tables in lnrent.sqlite are \
             already populated, and no import marker exists — ambiguous provenance. {RUNBOOK}",
            side_file.display(),
            backend.receive_table(),
            backend.pay_table()
        ),
        None => {}
    }

    let legacy = read_legacy(side_file, backend)
        .with_context(|| format!("reading legacy index {}", side_file.display()))?;
    // Pass 1 (read): what agrees, what is reap-eligible, what needs the backend's tiebreak.
    let plan = {
        let legacy = legacy.clone();
        store
            .read(move |c| analyse(c, backend, &legacy, now))
            .await?
    };
    if let Some(reason) = plan.refusal {
        bail!("refusing to boot: {reason}. {RUNBOOK}");
    }
    // Pass 2 (network): break each books-vs-map tie with the backend's CURRENT view.
    let mut repairs = Vec::new();
    for m in &plan.mismatches {
        let map_state = probe
            .receive_state(&m.map.external_id, &m.map.invoice_id, &m.map.payment_hash)
            .await
            .with_context(|| {
                format!(
                    "asking the backend about map invoice {} (external_id {})",
                    m.map.invoice_id, m.map.external_id
                )
            })?;
        if map_state != ReceiveState::Established {
            bail!(
                "refusing to boot: invoice {} (external_id {}) disagrees with its {} row {} and the \
                 backend does not positively report the map's invoice paid or payable ({map_state:?}) \
                 — the map may be the stale side; no safe repair. {RUNBOOK}",
                m.book.id,
                m.book.external_id,
                backend.receive_table(),
                m.map.invoice_id
            );
        }
        if !m.same_id {
            let book_hash = m
                .book
                .payment_hash
                .clone()
                .or_else(|| {
                    m.book
                        .id
                        .strip_prefix(backend.invoice_prefix())
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let book_state = probe
                .receive_state(&m.book.external_id, &m.book.id, &book_hash)
                .await
                .with_context(|| {
                    format!(
                        "asking the backend about book invoice {} (external_id {})",
                        m.book.id, m.book.external_id
                    )
                })?;
            if book_state != ReceiveState::TerminalUnpaid {
                bail!(
                    "refusing to boot: invoice {} (external_id {}) names a different invoice than \
                     its {} row {}, and the backend does not positively report the book's invoice \
                     terminal-unpaid ({book_state:?}); its absence proves nothing (phoenixd forgets, \
                     a rolled-back client db loses ops), so rewriting the books to the map could \
                     orphan a paid replacement. {RUNBOOK}",
                    m.book.id,
                    m.book.external_id,
                    backend.receive_table(),
                    m.map.invoice_id
                );
            }
        }
        repairs.push(m.map.clone());
    }
    let repaired = repairs.len();
    let reaped = plan.reap.len();
    let stamped = plan.stamp.len();
    let receive_rows = legacy.receive.len();
    let pay_rows = legacy.pay.len();

    // Pass 3 (one transaction): rows in, pre-reap, repairs, RE-VALIDATE from scratch against the
    // repaired books (the marker is written only if nothing is left uncorrelated), stamps, marker.
    let name_w = name.clone();
    let hash_w = hash.clone();
    store
        .transaction(move |tx| {
            insert_legacy(tx, backend, &legacy)?;
            for id in &plan.reap {
                tx.execute("DELETE FROM invoice WHERE id=?1", params![id])?;
            }
            for map in &repairs {
                repair_invoice(tx, map)?;
            }
            let recheck = analyse(tx, backend, &legacy, now)?;
            if let Some(reason) = recheck.refusal {
                bail!("refusing to boot: after repair, {reason}. {RUNBOOK}");
            }
            if !recheck.mismatches.is_empty() || !recheck.reap.is_empty() {
                bail!(
                    "refusing to boot: the import's repair did not converge ({} disagreements, {} \
                     reap candidates remain) — a daemon bug; nothing was committed. {RUNBOOK}",
                    recheck.mismatches.len(),
                    recheck.reap.len()
                );
            }
            for (table, id) in &plan.stamp {
                stamp(tx, table, id, now)?;
            }
            write_marker(tx, &name_w, &hash_w, now)?;
            Ok(())
        })
        .await?;
    rename_imported(side_file)?;
    tracing::info!(
        file = %side_file.display(),
        receive_rows,
        pay_rows,
        repaired,
        reaped,
        stamped,
        "ADR-0022: legacy backend index imported into lnrent.sqlite and renamed *.imported"
    );
    Ok(Outcome::Imported {
        receive_rows,
        pay_rows,
        repaired,
        reaped,
        stamped,
    })
}

/// The side file is ABSENT. Either an ordinary post-migration boot (marker present), a lost file over
/// correlation-bearing books (refuse), or a fresh / parked first boot (marker written).
async fn import_absent(
    store: &Store,
    backend: Backend,
    marker: Option<String>,
    name: &str,
    now: i64,
) -> Result<Outcome> {
    if marker.is_some() {
        return Ok(Outcome::AlreadyMigrated);
    }
    let name = name.to_string();
    store
        .transaction(move |tx| {
            if backend_tables_non_empty(tx, backend)? {
                bail!(
                    "refusing to boot: the {} / {} tables are populated but there is no import \
                     marker and no legacy {} — unknown provenance. {RUNBOOK}",
                    backend.receive_table(),
                    backend.pay_table(),
                    name
                );
            }
            if let Some(row) = first_correlation_bearing_row(tx, backend)? {
                bail!(
                    "refusing to boot: {name} is missing but the books reference this backend ({row}) \
                     and no import marker exists — the correlation file was lost. Restore the data \
                     dir with its {name} (a v2 backup carries it; an lnv2 one lives under \
                     fedimint/), or settle the referenced rows by hand. {RUNBOOK}"
                );
            }
            // Not "fresh" until every legacy attempt whose witness would have been in the file is
            // fenced: its POST, if any, left no id, so it is not correlation-bearing, yet its PREPARED
            // row is gone with the file.
            let to_stamp = unwitnessed_attempts(tx)?;
            for (table, id) in &to_stamp {
                stamp(tx, table, id, now)?;
            }
            let content = if to_stamp.is_empty() { "fresh" } else { "parked" };
            write_marker(tx, &name, content, now)?;
            Ok(if to_stamp.is_empty() {
                Outcome::Fresh
            } else {
                Outcome::Parked {
                    stamped: to_stamp.len(),
                }
            })
        })
        .await
}

// ---------------------------------------------------------------------------------------------------
// The legacy file
// ---------------------------------------------------------------------------------------------------

/// One legacy receive-map row (both backends' columns; the lnv2-only ones default for phoenixd).
#[derive(Debug, Clone)]
struct ReceiveRow {
    external_id: String,
    invoice_id: String,
    bolt11: String,
    payment_hash: String,
    amount_sat: i64,
    expires_at: i64,
    /// lnv2 only.
    operation_id: Option<String>,
    credited_msat: i64,
    /// lnv2: `OPEN | CANCELED | PAID | PAID_UNRECOVERED`. phoenixd has no status; `OPEN` stands in.
    status: String,
    settled_at: Option<i64>,
}

/// One legacy pay-map row. `payment_id` is phoenixd's uuid or lnv2's operation id — whichever the
/// attempt's `backend_payment_id` was recorded as.
#[derive(Debug, Clone)]
struct PayRow {
    key: String,
    bolt11: String,
    payment_hash: Option<String>,
    node_id: Option<String>,
    payment_id: Option<String>,
    status: String,
    terminal_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct TimerRow {
    invoice_id: String,
    first_refusal_at: i64,
}

#[derive(Debug, Clone, Default)]
struct Legacy {
    /// In `rowid` order, so the LAST row per `external_id` is phoenixd's effective one.
    receive: Vec<ReceiveRow>,
    pay: Vec<PayRow>,
    timers: Vec<TimerRow>,
}

impl Legacy {
    /// The EFFECTIVE receive-map row per `external_id`: newest `rowid` for phoenixd (its upsert
    /// conflicts on `invoice_id`, so a replacement leaves the old row in place), the single row for
    /// lnv2.
    fn effective_by_external(&self) -> HashMap<&str, &ReceiveRow> {
        let mut out = HashMap::new();
        for r in &self.receive {
            out.insert(r.external_id.as_str(), r);
        }
        out
    }

    fn pay_by_key(&self) -> HashMap<&str, &PayRow> {
        self.pay.iter().map(|p| (p.key.as_str(), p)).collect()
    }
}

fn has_table(conn: &Connection, table: &str) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
        params![table],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

fn read_legacy(path: &Path, backend: Backend) -> Result<Legacy> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .context("opening the legacy index")?;
    let mut legacy = Legacy::default();
    match backend {
        Backend::Phoenixd => {
            if has_table(&conn, "phoenixd_invoice")? {
                let mut stmt = conn.prepare(
                    "SELECT external_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at
                       FROM phoenixd_invoice ORDER BY rowid",
                )?;
                legacy.receive = stmt
                    .query_map([], |r| {
                        Ok(ReceiveRow {
                            external_id: r.get(0)?,
                            invoice_id: r.get(1)?,
                            bolt11: r.get(2)?,
                            payment_hash: r.get(3)?,
                            amount_sat: r.get(4)?,
                            expires_at: r.get(5)?,
                            operation_id: None,
                            credited_msat: 0,
                            status: "OPEN".to_string(),
                            settled_at: None,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
            }
            if has_table(&conn, "phoenixd_pay")? {
                let mut stmt = conn.prepare(
                    "SELECT idempotency_key, bolt11, payment_hash, node_id, payment_id, status,
                            terminal_at
                       FROM phoenixd_pay ORDER BY rowid",
                )?;
                legacy.pay = stmt
                    .query_map([], |r| {
                        Ok(PayRow {
                            key: r.get(0)?,
                            bolt11: r.get(1)?,
                            payment_hash: r.get(2)?,
                            node_id: r.get(3)?,
                            payment_id: r.get(4)?,
                            status: r.get(5)?,
                            terminal_at: r.get(6)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
            }
            if has_table(&conn, "phoenixd_unbookable_settlement")? {
                let mut stmt = conn.prepare(
                    "SELECT invoice_id, first_refusal_at FROM phoenixd_unbookable_settlement",
                )?;
                legacy.timers = stmt
                    .query_map([], |r| {
                        Ok(TimerRow {
                            invoice_id: r.get(0)?,
                            first_refusal_at: r.get(1)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
            }
        }
        Backend::Lnv2 => {
            if has_table(&conn, "lnv2_invoice")? {
                let mut stmt = conn.prepare(
                    "SELECT external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat,
                            credited_msat, expires_at, status, settled_at
                       FROM lnv2_invoice ORDER BY rowid",
                )?;
                legacy.receive = stmt
                    .query_map([], |r| {
                        Ok(ReceiveRow {
                            external_id: r.get(0)?,
                            operation_id: Some(r.get(1)?),
                            invoice_id: r.get(2)?,
                            bolt11: r.get(3)?,
                            payment_hash: r.get(4)?,
                            amount_sat: r.get(5)?,
                            credited_msat: r.get(6)?,
                            expires_at: r.get(7)?,
                            status: r.get(8)?,
                            settled_at: r.get(9)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
            }
            if has_table(&conn, "lnv2_pay")? {
                let mut stmt = conn.prepare(
                    "SELECT idempotency_key, bolt11, operation_id, status, terminal_at
                       FROM lnv2_pay ORDER BY rowid",
                )?;
                legacy.pay = stmt
                    .query_map([], |r| {
                        Ok(PayRow {
                            key: r.get(0)?,
                            bolt11: r.get(1)?,
                            payment_hash: None,
                            node_id: None,
                            payment_id: Some(r.get(2)?),
                            status: r.get(3)?,
                            terminal_at: r.get(4)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
            }
        }
    }
    Ok(legacy)
}

/// Copy the legacy rows verbatim into the new tables (same shape, same columns).
fn insert_legacy(tx: &Transaction, backend: Backend, legacy: &Legacy) -> Result<()> {
    match backend {
        Backend::Phoenixd => {
            for r in &legacy.receive {
                tx.execute(
                    "INSERT INTO phoenixd_invoice
                        (external_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        r.external_id,
                        r.invoice_id,
                        r.bolt11,
                        r.payment_hash,
                        r.amount_sat,
                        r.expires_at
                    ],
                )?;
            }
            for p in &legacy.pay {
                tx.execute(
                    "INSERT INTO phoenixd_pay
                        (idempotency_key, bolt11, payment_hash, node_id, payment_id, status,
                         terminal_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        p.key,
                        p.bolt11,
                        p.payment_hash.clone().unwrap_or_default(),
                        p.node_id,
                        p.payment_id,
                        p.status,
                        p.terminal_at
                    ],
                )?;
            }
            for t in &legacy.timers {
                tx.execute(
                    "INSERT INTO phoenixd_unbookable_settlement (invoice_id, first_refusal_at)
                     VALUES (?1, ?2)",
                    params![t.invoice_id, t.first_refusal_at],
                )?;
            }
        }
        Backend::Lnv2 => {
            for r in &legacy.receive {
                tx.execute(
                    "INSERT INTO lnv2_invoice
                        (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat,
                         credited_msat, expires_at, status, settled_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        r.external_id,
                        r.operation_id.clone().unwrap_or_default(),
                        r.invoice_id,
                        r.bolt11,
                        r.payment_hash,
                        r.amount_sat,
                        r.credited_msat,
                        r.expires_at,
                        r.status,
                        r.settled_at
                    ],
                )?;
            }
            for p in &legacy.pay {
                tx.execute(
                    "INSERT INTO lnv2_pay (idempotency_key, bolt11, operation_id, status, terminal_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        p.key,
                        p.bolt11,
                        p.payment_id.clone().unwrap_or_default(),
                        p.status,
                        p.terminal_at
                    ],
                )?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------
// The books, and the coverage/agreement analysis (pure over one connection + the legacy rows)
// ---------------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct BookInvoice {
    id: String,
    external_id: String,
    payment_hash: Option<String>,
    bolt11: Option<String>,
    amount_sat: Option<i64>,
    status: Option<String>,
    expires_at: Option<i64>,
    settled_at: Option<i64>,
    issued_at: Option<i64>,
}

impl BookInvoice {
    fn settled(&self) -> bool {
        self.status.as_deref() == Some("PAID") || self.settled_at.is_some()
    }
}

/// One refund or sweep attempt as the pay-map coverage rules see it.
#[derive(Debug, Clone)]
struct BookAttempt {
    table: &'static str,
    id: String,
    /// The DERIVED pay key the backend's map is keyed by.
    key: String,
    status: String,
    backend_payment_id: Option<String>,
    /// The effective destination: `resolved_bolt11`, or a bolt11 `dest` at generation 0; `None` for
    /// an unresolved LNURL/LN-address attempt.
    bolt11: Option<String>,
}

/// A books-vs-map disagreement the backend must arbitrate.
#[derive(Debug, Clone)]
struct Mismatch {
    book: BookInvoice,
    map: ReceiveRow,
    /// Same invoice id, other fields drifted (vs. a replacement under the same `external_id`).
    same_id: bool,
}

#[derive(Debug, Default)]
struct Plan {
    /// Book invoice ids to delete before coverage (retention-eligible, map absent or CANCELED).
    reap: Vec<String>,
    /// Disagreements that need the backend's tiebreak.
    mismatches: Vec<Mismatch>,
    /// `(table, id)` attempts to stamp `migration_unverified_at`.
    stamp: Vec<(&'static str, String)>,
    /// The FIRST hard refusal, if any (reported verbatim).
    refusal: Option<String>,
}

fn load_book_invoices(conn: &Connection, backend: Backend) -> Result<Vec<BookInvoice>> {
    let mut stmt = conn.prepare(
        "SELECT id, external_id, payment_hash, bolt11, amount_sat, status,
                expires_at, settled_at, issued_at
           FROM invoice WHERE id LIKE ?1 ORDER BY rowid",
    )?;
    let rows = stmt
        .query_map(params![format!("{}%", backend.invoice_prefix())], |r| {
            Ok(BookInvoice {
                id: r.get(0)?,
                external_id: r.get(1)?,
                payment_hash: r.get(2)?,
                bolt11: r.get(3)?,
                amount_sat: r.get(4)?,
                status: r.get(5)?,
                expires_at: r.get(6)?,
                settled_at: r.get(7)?,
                issued_at: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_book_attempts(conn: &Connection) -> Result<Vec<BookAttempt>> {
    let mut out = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, idempotency_key, status, backend_payment_id, dest, resolved_bolt11,
                COALESCE(resolution_gen, 0)
           FROM refund_attempt ORDER BY rowid",
    )?;
    let refunds = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, idempotency_key, status, backend_payment_id, dest, resolved_bolt11, gen) in refunds {
        let external_id = crate::refund::external_id_from(&idempotency_key, &id);
        let key = crate::refund::gen_key(&external_id, gen);
        let bolt11 = if gen == 0 {
            dest.filter(|d| lightning_invoice::Bolt11Invoice::from_str(d.trim()).is_ok())
        } else {
            resolved_bolt11
        };
        out.push(BookAttempt {
            table: "refund_attempt",
            id,
            key,
            status,
            backend_payment_id,
            bolt11,
        });
    }
    let mut stmt =
        conn.prepare("SELECT id, bolt11, status, backend_payment_id FROM sweep_attempt ORDER BY rowid")?;
    let sweeps = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, bolt11, status, backend_payment_id) in sweeps {
        out.push(BookAttempt {
            table: "sweep_attempt",
            key: id.clone(),
            id,
            status,
            backend_payment_id,
            bolt11,
        });
    }
    Ok(out)
}

/// Is this book invoice already reapable under the store's own retention rule (`reap_terminal_rows`):
/// EXPIRED, never settled, past the window, and not behind an open refund?
fn reap_eligible(conn: &Connection, b: &BookInvoice, now: i64) -> Result<bool> {
    if b.status.as_deref() != Some("EXPIRED") || b.settled_at.is_some() {
        return Ok(false);
    }
    let anchor = b.expires_at.or(b.issued_at).unwrap_or(i64::MAX);
    if anchor >= now - TERMINAL_ROW_RETENTION_SECS {
        return Ok(false);
    }
    let open_refund: i64 = conn.query_row(
        "SELECT count(*) FROM refund_attempt ra
          WHERE ra.status <> 'SENT'
            AND (CASE WHEN ra.idempotency_key LIKE 'refund:%' THEN substr(ra.idempotency_key, 8)
                      WHEN ra.id LIKE 'ref-%' THEN substr(ra.id, 5)
                      ELSE ra.id END) = ?1",
        params![b.external_id],
        |r| r.get(0),
    )?;
    Ok(open_refund == 0)
}

fn analyse(conn: &Connection, backend: Backend, legacy: &Legacy, now: i64) -> Result<Plan> {
    let mut plan = Plan::default();
    let effective = legacy.effective_by_external();
    let pay = legacy.pay_by_key();

    // Receive coverage.
    for b in load_book_invoices(conn, backend)? {
        let m = effective.get(b.external_id.as_str()).copied();
        // The map is inspected FIRST: a book row already reapable under the store's own retention
        // whose map row is absent or CANCELED is pre-reaped, never refused (the lnv2 reaper deletes
        // CANCELED rows independently of `reap_terminal_rows`). Otherwise an absent row refuses, a
        // CANCELED row for a DIFFERENT invoice refuses (never a replacement), and a CANCELED row for
        // the book's OWN invoice is ordinary coverage (the invoice simply expired at the backend).
        let m = match m {
            None => {
                if reap_eligible(conn, &b, now)? {
                    plan.reap.push(b.id.clone());
                } else {
                    plan.refusal.get_or_insert(format!(
                        "invoice {} (external_id {}) has no {} row in the legacy index",
                        b.id,
                        b.external_id,
                        backend.receive_table()
                    ));
                }
                continue;
            }
            Some(m) if m.status == "CANCELED" => {
                if reap_eligible(conn, &b, now)? {
                    plan.reap.push(b.id.clone());
                    continue;
                }
                if m.invoice_id != b.id {
                    plan.refusal.get_or_insert(format!(
                        "invoice {} (external_id {}) is correlated only to a CANCELED {} row {} \
                         that is not its own invoice — a CANCELED row is never a replacement",
                        b.id,
                        b.external_id,
                        backend.receive_table(),
                        m.invoice_id
                    ));
                    continue;
                }
                m
            }
            Some(m) => m,
        };
        if m.invoice_id == b.id {
            // Same invoice: the row IS the correlation. bolt11 / payment_hash / amount must agree;
            // expires_at may legitimately have moved (the backend reopened a still-payable local
            // window in its own map while the books kept the original), which is repaired for an
            // unsettled row and tolerated for a settled one.
            let disagree = b.bolt11.as_deref().is_some_and(|x| x != m.bolt11)
                || b.payment_hash.as_deref().is_some_and(|x| x != m.payment_hash)
                || b.amount_sat.is_some_and(|x| x != m.amount_sat);
            if disagree {
                if b.settled() {
                    plan.refusal.get_or_insert(format!(
                        "settled invoice {} (external_id {}) disagrees with its {} row on \
                         bolt11/payment_hash/amount_sat — money was booked against data the map no \
                         longer describes",
                        b.id,
                        b.external_id,
                        backend.receive_table()
                    ));
                    continue;
                }
                plan.mismatches.push(Mismatch {
                    book: b,
                    map: m.clone(),
                    same_id: true,
                });
                continue;
            }
            if !b.settled() && b.expires_at != Some(m.expires_at) {
                plan.mismatches.push(Mismatch {
                    book: b,
                    map: m.clone(),
                    same_id: true,
                });
            }
            continue;
        }
        // A DIFFERENT invoice under the same external_id: the replacement shape.
        if b.settled() {
            plan.refusal.get_or_insert(format!(
                "settled invoice {} (external_id {}) is correlated to a different {} invoice {} — \
                 money was booked against an invoice the map no longer describes",
                b.id,
                b.external_id,
                backend.receive_table(),
                m.invoice_id
            ));
            continue;
        }
        if !matches!(m.status.as_str(), "OPEN" | "PAID" | "PAID_UNRECOVERED") {
            plan.refusal.get_or_insert(format!(
                "invoice {} (external_id {}) is correlated to {} row {} in status {} — not a \
                 replacement shape",
                b.id,
                b.external_id,
                backend.receive_table(),
                m.invoice_id,
                m.status
            ));
            continue;
        }
        plan.mismatches.push(Mismatch {
            book: b,
            map: m.clone(),
            same_id: false,
        });
    }

    // Unbookable-settlement timers (phoenixd) must resolve to an imported receive row.
    for t in &legacy.timers {
        if !legacy.receive.iter().any(|r| r.invoice_id == t.invoice_id) {
            plan.refusal.get_or_insert(format!(
                "legacy phoenixd_unbookable_settlement timer for invoice {} has no phoenixd_invoice \
                 row to resolve its subject from",
                t.invoice_id
            ));
        }
    }

    // Pay coverage.
    for a in load_book_attempts(conn)? {
        let m = pay.get(a.key.as_str()).copied();
        let sent = a.status == "SENT";
        let has_id = a.backend_payment_id.is_some();
        match m {
            Some(m) => {
                // Present: it must AGREE. bolt11 always; the id when the attempt carries one; and a
                // SENT attempt's row must be the backend's success state.
                let bolt11_ok = a.bolt11.as_deref() == Some(m.bolt11.as_str());
                let id_ok = match &a.backend_payment_id {
                    Some(id) => m.payment_id.as_deref() == Some(id.as_str()),
                    None => true,
                };
                let status_ok = !sent || m.status == "SUCCEEDED";
                if !(bolt11_ok && id_ok && status_ok) {
                    plan.refusal.get_or_insert(format!(
                        "{} {} (pay key {}, status {}) disagrees with its {} row (bolt11 agrees: \
                         {bolt11_ok}, backend id agrees: {id_ok}, map status {} acceptable: \
                         {status_ok}) — which payment went out is exactly what a stale file cannot \
                         say; no repair for pay rows",
                        a.table,
                        a.id,
                        a.key,
                        a.status,
                        backend.pay_table(),
                        m.status
                    ));
                }
            }
            None if sent || has_id => {
                plan.refusal.get_or_insert(format!(
                    "{} {} (pay key {}, status {}{}) has no {} row in the legacy index — the \
                     historical owner of its payment is missing",
                    a.table,
                    a.id,
                    a.key,
                    a.status,
                    if has_id { ", with a backend payment id" } else { "" },
                    backend.pay_table()
                ));
            }
            None if a.status == "PENDING" || a.status == "FAILED" => {
                // Not validated, on purpose: 'never started' and 'started, witness lost' are
                // indistinguishable. Fence it.
                plan.stamp.push((a.table, a.id.clone()));
            }
            None => {}
        }
    }
    Ok(plan)
}

/// Rewrite the book row for `map.external_id` to the map's invoice (the ADR-0022 replacement rule):
/// id, backend id, hash, bolt11, amount, expiry; `EXPIRED -> OPEN`, never from PAID.
fn repair_invoice(tx: &Transaction, map: &ReceiveRow) -> Result<()> {
    let backend_invoice_id = map
        .operation_id
        .clone()
        .unwrap_or_else(|| map.payment_hash.clone());
    let n = tx.execute(
        "UPDATE invoice
            SET id=?2, backend_invoice_id=?3, payment_hash=?4, bolt11=?5, amount_sat=?6,
                expires_at=?7,
                status = CASE WHEN status='EXPIRED' THEN 'OPEN' ELSE status END
          WHERE external_id=?1 AND status <> 'PAID' AND settled_at IS NULL",
        params![
            map.external_id,
            map.invoice_id,
            backend_invoice_id,
            map.payment_hash,
            map.bolt11,
            map.amount_sat,
            map.expires_at
        ],
    )?;
    if n != 1 {
        bail!(
            "repairing invoice for external_id {} touched {n} rows (expected 1)",
            map.external_id
        );
    }
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at)
         SELECT subscription_id, 'adr0022_import_repair', ?2, strftime('%s','now')
           FROM invoice WHERE external_id=?1",
        params![
            map.external_id,
            serde_json::json!({ "external_id": map.external_id, "invoice_id": map.invoice_id })
                .to_string()
        ],
    )?;
    Ok(())
}

fn stamp(tx: &Transaction, table: &str, id: &str, now: i64) -> Result<()> {
    tx.execute(
        &format!("UPDATE {table} SET migration_unverified_at=?2 WHERE id=?1"),
        params![id, now],
    )?;
    Ok(())
}

/// Every PENDING or FAILED attempt with no `backend_payment_id`, both ledgers.
fn unwitnessed_attempts(conn: &Connection) -> Result<Vec<(&'static str, String)>> {
    let mut out = Vec::new();
    for table in ["refund_attempt", "sweep_attempt"] {
        let mut stmt = conn.prepare(&format!(
            "SELECT id FROM {table}
              WHERE status IN ('PENDING', 'FAILED') AND backend_payment_id IS NULL
              ORDER BY rowid"
        ))?;
        for id in stmt.query_map([], |r| r.get::<_, String>(0))? {
            out.push((table, id?));
        }
    }
    Ok(out)
}

/// The first row that REFERENCES this backend's correlation (ADR-0022's exact predicate): an invoice
/// with the backend's id prefix, a SENT attempt (any key — refund/sweep keys are not
/// backend-specific), or an attempt carrying a `backend_payment_id`. Never the persisted
/// `payment_backend` selection.
fn first_correlation_bearing_row(conn: &Connection, backend: Backend) -> Result<Option<String>> {
    let inv: Option<String> = conn
        .query_row(
            "SELECT id FROM invoice WHERE id LIKE ?1 ORDER BY rowid LIMIT 1",
            params![format!("{}%", backend.invoice_prefix())],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(id) = inv {
        return Ok(Some(format!("invoice {id}")));
    }
    for table in ["refund_attempt", "sweep_attempt"] {
        let row: Option<(String, String)> = conn
            .query_row(
                &format!(
                    "SELECT id, status FROM {table}
                      WHERE status='SENT' OR backend_payment_id IS NOT NULL
                      ORDER BY rowid LIMIT 1"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((id, status)) = row {
            return Ok(Some(format!("{table} {id} ({status})")));
        }
    }
    Ok(None)
}

fn backend_tables_non_empty(conn: &Connection, backend: Backend) -> Result<bool> {
    let n: i64 = conn.query_row(
        &format!(
            "SELECT (SELECT count(*) FROM {}) + (SELECT count(*) FROM {})",
            backend.receive_table(),
            backend.pay_table()
        ),
        [],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

// ---------------------------------------------------------------------------------------------------
// The marker
// ---------------------------------------------------------------------------------------------------

/// The `migration` row for `name`, if any: the imported file's hash, or `fresh` / `parked`.
pub fn read_marker(conn: &Connection, name: &str) -> Result<Option<String>> {
    if !has_table(conn, "migration")? {
        return Ok(None);
    }
    Ok(conn
        .query_row(
            "SELECT content_hash FROM migration WHERE name=?1",
            params![name],
            |r| r.get(0),
        )
        .optional()?)
}

/// Whether ANY import marker exists — "this database is self-contained from here on" — the predicate
/// the v3 backup writer keys on (ADR-0022 Consequences). Tolerates a pre-M12 database (no table).
pub fn any_marker(conn: &Connection) -> Result<bool> {
    if !has_table(conn, "migration")? {
        return Ok(false);
    }
    let n: i64 = conn.query_row("SELECT count(*) FROM migration", [], |r| r.get(0))?;
    Ok(n > 0)
}

fn write_marker(tx: &Transaction, name: &str, content: &str, now: i64) -> Result<()> {
    tx.execute(
        "INSERT INTO migration (name, content_hash, completed_at) VALUES (?1, ?2, ?3)",
        params![name, content, now],
    )?;
    Ok(())
}

fn rename_imported(side_file: &Path) -> Result<()> {
    let mut renamed = side_file.as_os_str().to_owned();
    renamed.push(".imported");
    std::fs::rename(side_file, &renamed).with_context(|| {
        format!(
            "renaming imported legacy index {} -> {}",
            side_file.display(),
            Path::new(&renamed).display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    /// A scripted backend view: `invoice_id -> ReceiveState`, `Absent` for anything unscripted, and a
    /// record of what was asked so a test can prove the tiebreak actually consulted the backend.
    #[derive(Default)]
    struct FakeProbe {
        states: Mutex<HashMap<String, ReceiveState>>,
        asked: Mutex<Vec<String>>,
    }

    impl FakeProbe {
        fn with(states: &[(&str, ReceiveState)]) -> Self {
            let p = Self::default();
            for (id, st) in states {
                p.states.lock().unwrap().insert(id.to_string(), *st);
            }
            p
        }
        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl LegacyProbe for FakeProbe {
        async fn receive_state(
            &self,
            _external_id: &str,
            invoice_id: &str,
            _payment_hash: &str,
        ) -> Result<ReceiveState> {
            self.asked.lock().unwrap().push(invoice_id.to_string());
            Ok(self
                .states
                .lock()
                .unwrap()
                .get(invoice_id)
                .copied()
                .unwrap_or(ReceiveState::Absent))
        }
    }

    fn temp_dir() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-legacy-import-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn mem_store() -> Store {
        Store::spawn(crate::store::open_memory().unwrap())
    }

    /// Write a legacy side file at the pre-ADR-0022 schema (identical DDL to the new tables) and let
    /// the test seed it.
    fn legacy_file(dir: &Path, backend: Backend, seed: impl FnOnce(&Connection)) -> PathBuf {
        let path = dir.join(backend.side_file_name());
        let conn = Connection::open(&path).unwrap();
        match backend {
            Backend::Phoenixd => conn
                .execute_batch(crate::phoenixd_backend::SCHEMA)
                .unwrap(),
            Backend::Lnv2 => conn.execute_batch(LNV2_LEGACY_DDL).unwrap(),
        }
        seed(&conn);
        path
    }

    /// The pre-ADR-0022 lnv2 side-file DDL (literal, so the phoenixd-only build can still exercise
    /// the lnv2 shapes the import must handle).
    const LNV2_LEGACY_DDL: &str = "
CREATE TABLE IF NOT EXISTS lnv2_invoice (
    external_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, invoice_id TEXT NOT NULL,
    bolt11 TEXT NOT NULL, payment_hash TEXT NOT NULL, amount_sat INTEGER NOT NULL,
    credited_msat INTEGER NOT NULL, expires_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN', settled_at INTEGER);
CREATE TABLE IF NOT EXISTS lnv2_pay (
    idempotency_key TEXT PRIMARY KEY, bolt11 TEXT NOT NULL, operation_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PREPARED', terminal_at INTEGER);";

    fn seed_phx_receive(c: &Connection, ext: &str, hash: &str, bolt11: &str, expires_at: i64) {
        c.execute(
            "INSERT INTO phoenixd_invoice (external_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at)
             VALUES (?1, ?2, ?3, ?4, 100, ?5)",
            params![ext, format!("phoenixd-{hash}"), bolt11, hash, expires_at],
        )
        .unwrap();
    }

    fn seed_phx_pay(c: &Connection, key: &str, bolt11: &str, pid: Option<&str>, status: &str) {
        c.execute(
            "INSERT INTO phoenixd_pay (idempotency_key, bolt11, payment_hash, node_id, payment_id, status)
             VALUES (?1, ?2, 'h-pay', 'node', ?3, ?4)",
            params![key, bolt11, pid, status],
        )
        .unwrap();
    }

    #[cfg_attr(not(feature = "fedimint"), allow(dead_code))] // the lnv2 shapes need the lnv2 tables
    fn seed_lnv2_receive(c: &Connection, ext: &str, op: &str, bolt11: &str, expires_at: i64, status: &str) {
        c.execute(
            "INSERT INTO lnv2_invoice (external_id, operation_id, invoice_id, bolt11, payment_hash,
                                       amount_sat, credited_msat, expires_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, 100, 0, ?6, ?7)",
            params![ext, op, format!("lnv2-{op}"), bolt11, format!("h-{op}"), expires_at, status],
        )
        .unwrap();
    }

    /// A book invoice referencing the backend by id prefix. `hash` doubles as the phoenixd id suffix.
    #[allow(clippy::too_many_arguments)]
    async fn seed_book_invoice(
        store: &Store,
        id: &str,
        ext: &str,
        hash: &str,
        bolt11: &str,
        status: &str,
        expires_at: i64,
        settled_at: Option<i64>,
    ) {
        let (id, ext, hash, bolt11, status) = (
            id.to_string(),
            ext.to_string(),
            hash.to_string(),
            bolt11.to_string(),
            status.to_string(),
        );
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO invoice (id, external_id, backend_invoice_id, payment_hash, kind,
                                          bolt11, amount_sat, status, expires_at, issued_at, settled_at)
                     VALUES (?1, ?2, ?3, ?3, 'order', ?4, 100, ?5, ?6, ?6, ?7)",
                    params![id, ext, hash, bolt11, status, expires_at, settled_at],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_refund(
        store: &Store,
        ext: &str,
        status: &str,
        dest: &str,
        resolved_bolt11: Option<&str>,
        gen: i64,
        backend_payment_id: Option<&str>,
    ) {
        let (ext, status, dest) = (ext.to_string(), status.to_string(), dest.to_string());
        let resolved = resolved_bolt11.map(str::to_string);
        let pid = backend_payment_id.map(str::to_string);
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key,
                        backend_payment_id, status, attempts, resolved_bolt11, resolution_gen,
                        created_at, updated_at)
                     VALUES (?1, 's', ?2, 100, ?3, ?4, ?5, 0, ?6, ?7, 0, 0)",
                    params![format!("ref-{ext}"), dest, format!("refund:{ext}"), pid, status, resolved, gen],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn seed_sweep(store: &Store, id: &str, status: &str, bolt11: &str, pid: Option<&str>) {
        let (id, status, bolt11) = (id.to_string(), status.to_string(), bolt11.to_string());
        let pid = pid.map(str::to_string);
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status,
                                                attempts, backend_payment_id, created_at)
                     VALUES (?1, ?2, 100, 100000, ?3, 0, ?4, 0)",
                    params![id, bolt11, status, pid],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    async fn marker(store: &Store, backend: Backend) -> Option<String> {
        let name = backend.side_file_name().to_string();
        store.read(move |c| read_marker(c, &name)).await.unwrap()
    }

    async fn fence(store: &Store, table: &str, id: &str) -> Option<i64> {
        let sql = format!("SELECT migration_unverified_at FROM {table} WHERE id=?1");
        let id = id.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, params![id], |r| r.get(0))?))
            .await
            .unwrap()
    }

    const NOW: i64 = 100 * 24 * 3600;

    // ---------------------------------------------------------------------------------------------
    // The happy path, its crash recovery, and the ambiguous states
    // ---------------------------------------------------------------------------------------------

    /// A populated side file imports exactly once: rows land verbatim, the marker records the file's
    /// hash in the SAME transaction, the file is renamed, and the next boot is an ordinary one.
    #[tokio::test]
    async fn a_populated_side_file_imports_exactly_once_and_is_renamed() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-h1", "e1", "h1", "lnbc1", "OPEN", NOW + 600, None).await;
        // A SENT gen-1 refund (its map row is keyed :g1, NOT the stored bare key) with a NULL
        // backend id — the recovery-committed shape — and a SUCCEEDED map row: covered.
        seed_refund(&store, "e1", "SENT", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "h1", "lnbc1", NOW + 600);
            seed_phx_pay(c, "refund:e1:g1", "lnbc-r1", Some("pid-1"), "SUCCEEDED");
        });
        let bytes = std::fs::read(&file).unwrap();
        let expected_hash = hex::encode(Sha256::digest(&bytes));

        let probe = FakeProbe::default();
        let out = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap();
        assert_eq!(
            out,
            Outcome::Imported { receive_rows: 1, pay_rows: 1, repaired: 0, reaped: 0, stamped: 0 }
        );
        assert!(probe.asked().is_empty(), "nothing disagreed, so the backend was never asked");
        assert_eq!(count(&store, "SELECT count(*) FROM phoenixd_invoice").await, 1);
        assert_eq!(count(&store, "SELECT count(*) FROM phoenixd_pay").await, 1);
        assert_eq!(marker(&store, Backend::Phoenixd).await.as_deref(), Some(expected_hash.as_str()));
        assert!(!file.exists(), "the side file was renamed");
        assert!(dir.join("phoenixd_index.db.imported").exists());

        // The next boot: marker present, no file — nothing to do, nothing re-imported.
        let out = run(&store, Backend::Phoenixd, &probe, &file, NOW + 1).await.unwrap();
        assert_eq!(out, Outcome::AlreadyMigrated);
        assert_eq!(count(&store, "SELECT count(*) FROM phoenixd_invoice").await, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A crash AFTER the import commit and BEFORE the rename is recognised on the next boot (marker
    /// present, hash matches): rename and continue, never refuse, never re-import. RED on "refuse if
    /// both exist" (PR #90 round 3), which wedged the daemon on an ordinary crash in that window.
    #[tokio::test]
    async fn a_crash_between_commit_and_rename_is_recognised_on_the_next_boot() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-h1", "e1", "h1", "lnbc1", "OPEN", NOW + 600, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "h1", "lnbc1", NOW + 600);
        });
        let probe = FakeProbe::default();
        run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap();
        // Undo the rename: the crash happened before it.
        std::fs::rename(dir.join("phoenixd_index.db.imported"), &file).unwrap();

        let out = run(&store, Backend::Phoenixd, &probe, &file, NOW + 1).await.unwrap();
        assert_eq!(out, Outcome::RenamedOnly);
        assert!(!file.exists() && dir.join("phoenixd_index.db.imported").exists());
        assert_eq!(count(&store, "SELECT count(*) FROM phoenixd_invoice").await, 1, "not re-imported");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The genuinely ambiguous state — side file present, new tables populated, no marker — refuses,
    /// and so does a marker recorded for a DIFFERENT file (a restore from another instant).
    #[tokio::test]
    async fn ambiguous_provenance_refuses_to_boot() {
        let dir = temp_dir();
        let store = mem_store();
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "h1", "lnbc1", NOW + 600);
        });
        store
            .transaction(|tx| {
                seed_phx_receive(tx, "e9", "h9", "lnbc9", NOW + 600);
                Ok(())
            })
            .await
            .unwrap();
        let probe = FakeProbe::default();
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("ambiguous provenance"), "{err:#}");
        assert!(file.exists(), "nothing renamed on a refusal");

        // Now a marker for a different hash: an imported file was swapped for another one.
        store
            .transaction(|tx| {
                tx.execute("DELETE FROM phoenixd_invoice", [])?;
                write_marker(tx, "phoenixd_index.db", "0000deadbeef", NOW)
            })
            .await
            .unwrap();
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("DIFFERENT"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A PRESENT file that lacks the correlation of a referenced invoice refuses the whole import:
    /// nothing is imported, no marker is written, the file stays. RED on a file-present-only check,
    /// which would import and mark complete over the gap.
    #[tokio::test]
    async fn a_present_side_file_missing_a_correlation_refuses() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-h1", "e1", "h1", "lnbc1", "OPEN", NOW + 600, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |_c| {});
        let probe = FakeProbe::default();
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("phoenixd-h1") && msg.contains("no phoenixd_invoice row"), "{msg}");
        assert!(msg.contains(RUNBOOK));
        assert_eq!(marker(&store, Backend::Phoenixd).await, None, "no marker on a refusal");
        assert_eq!(count(&store, "SELECT count(*) FROM phoenixd_invoice").await, 0);
        assert!(file.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------------------------------------
    // The absent-file branch
    // ---------------------------------------------------------------------------------------------

    /// Side file ABSENT + correlation-bearing books = a lost file: refuse to boot, per backend and per
    /// predicate (the id prefix; a SENT attempt; an attempt with a backend payment id).
    #[tokio::test]
    async fn an_absent_side_file_over_correlation_bearing_books_refuses() {
        let probe = FakeProbe::default();
        // phoenixd: the invoice id prefix.
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-h1", "e1", "h1", "lnbc1", "OPEN", NOW + 600, None).await;
        let err = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("phoenixd_index.db is missing"), "{err:#}");
        assert!(format!("{err:#}").contains("invoice phoenixd-h1"), "{err:#}");
        assert_eq!(marker(&store, Backend::Phoenixd).await, None);

        // lnv2: the same predicate on its prefix (the tables exist only with the feature).
        if cfg!(feature = "fedimint") {
            let store = mem_store();
            seed_book_invoice(&store, "lnv2-op1", "e1", "op1", "lnbc1", "OPEN", NOW + 600, None).await;
            let err = run(&store, Backend::Lnv2, &probe, &dir.join("lnv2_index.db"), NOW)
                .await
                .unwrap_err();
            assert!(format!("{err:#}").contains("lnv2_index.db is missing"), "{err:#}");
        }

        // A SENT attempt (no invoice at all) is correlation-bearing too — its pay-map row is the
        // historical owner of a payment hash.
        let store = mem_store();
        seed_refund(&store, "e2", "SENT", "buyer@ln.example", Some("lnbc-r2"), 1, None).await;
        let err = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("refund_attempt ref-e2 (SENT)"), "{err:#}");

        // And so is a non-terminal attempt that already carries a backend payment id.
        let store = mem_store();
        seed_sweep(&store, "sweep:h3", "PENDING", "lnbc-s3", Some("pid-3")).await;
        let err = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("sweep_attempt sweep:h3 (PENDING)"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The absent-file branch stamps every legacy non-terminal AND retryable-FAILED attempt without a
    /// backend payment id — INCLUDING an unresolved-LNURL PENDING row (the books can be a rollback
    /// from before the resolution while the wallet paid after) — parks them, and STILL writes a
    /// marker (`parked`) so later backups are stamped v3. RED on a "fresh" predicate that keys on the
    /// persisted selection or that skips the stamps.
    #[tokio::test]
    async fn an_absent_side_file_parks_unwitnessed_attempts_and_writes_a_parked_marker() {
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "PENDING", "buyer@ln.example", None, 0, None).await; // unresolved LNURL
        seed_refund(&store, "e2", "FAILED", "buyer@ln.example", Some("lnbc-r2"), 1, None).await; // retryable
        seed_sweep(&store, "sweep:h3", "PENDING", "lnbc-s3", None).await;
        seed_sweep(&store, "sweep:h4", "SENT", "lnbc-s4", None).await; // wait: SENT is correlation-bearing
        // A SENT sweep would refuse the whole boot (previous test); remove it to isolate the stamps.
        store
            .transaction(|tx| {
                tx.execute("DELETE FROM sweep_attempt WHERE id='sweep:h4'", [])?;
                Ok(())
            })
            .await
            .unwrap();
        let probe = FakeProbe::default();
        let out = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap();
        assert_eq!(out, Outcome::Parked { stamped: 3 });
        assert_eq!(fence(&store, "refund_attempt", "ref-e1").await, Some(NOW));
        assert_eq!(fence(&store, "refund_attempt", "ref-e2").await, Some(NOW));
        assert_eq!(fence(&store, "sweep_attempt", "sweep:h3").await, Some(NOW));
        assert_eq!(marker(&store, Backend::Phoenixd).await.as_deref(), Some("parked"));
        // The next boot is ordinary.
        let out = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW + 1)
            .await
            .unwrap();
        assert_eq!(out, Outcome::AlreadyMigrated);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A FRESH bootstrap (persisted selection, no rows) boots AND writes a `fresh` marker, so its
    /// later backups are stamped v3. The persisted selection alone is never a "lost file".
    #[tokio::test]
    async fn a_fresh_install_writes_a_fresh_marker() {
        let dir = temp_dir();
        let store = mem_store();
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO operator (payment_backend) VALUES ('phoenixd')",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let probe = FakeProbe::default();
        let out = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap();
        assert_eq!(out, Outcome::Fresh);
        assert_eq!(marker(&store, Backend::Phoenixd).await.as_deref(), Some("fresh"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Populated tables with NO marker and NO file are of unknown provenance: refuse.
    #[tokio::test]
    async fn populated_tables_without_a_marker_or_a_file_refuse() {
        let dir = temp_dir();
        let store = mem_store();
        store
            .transaction(|tx| {
                seed_phx_receive(tx, "e1", "h1", "lnbc1", NOW + 600);
                Ok(())
            })
            .await
            .unwrap();
        let probe = FakeProbe::default();
        let err = run(&store, Backend::Phoenixd, &probe, &dir.join("phoenixd_index.db"), NOW)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("unknown provenance"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------------------------------------
    // Receive coverage: pre-reap, replacement repair, refusals
    // ---------------------------------------------------------------------------------------------

    /// An EXPIRED book row already eligible under the store's retention whose map row the backend's
    /// own reaper legitimately removed (or holds CANCELED) is pre-reaped, not refused — the map is
    /// inspected FIRST. A book row the map holds OPEN is never reaped (the replacement rule repairs it).
    #[cfg(feature = "fedimint")]
    #[tokio::test]
    async fn a_retention_eligible_expired_book_row_the_map_lacks_is_pre_reaped() {
        let dir = temp_dir();
        let store = mem_store();
        let old = NOW - TERMINAL_ROW_RETENTION_SECS - 10;
        seed_book_invoice(&store, "lnv2-opGone", "eGone", "h-opGone", "lnbcG", "EXPIRED", old, None).await;
        seed_book_invoice(&store, "lnv2-opCan", "eCan", "h-opCan", "lnbcC", "EXPIRED", old, None).await;
        // A RECENT expired row whose own invoice the map holds CANCELED is ordinary coverage: not
        // reapable yet, not refused.
        seed_book_invoice(&store, "lnv2-opRecent", "eRecent", "h-opRecent", "lnbcR", "EXPIRED", NOW - 10, None).await;
        let file = legacy_file(&dir, Backend::Lnv2, |c| {
            // eGone: no row at all (reaped by gc_lnv2_invoice_index); eCan: still CANCELED.
            seed_lnv2_receive(c, "eCan", "opCan", "lnbcC", old, "CANCELED");
            seed_lnv2_receive(c, "eRecent", "opRecent", "lnbcR", NOW - 10, "CANCELED");
        });
        let probe = FakeProbe::default();
        let out = run(&store, Backend::Lnv2, &probe, &file, NOW).await.unwrap();
        assert_eq!(
            out,
            Outcome::Imported { receive_rows: 2, pay_rows: 0, repaired: 0, reaped: 2, stamped: 0 }
        );
        assert_eq!(count(&store, "SELECT count(*) FROM invoice").await, 1, "the two old rows pre-reaped, the recent one kept");
        assert!(probe.asked().is_empty(), "a same-id CANCELED row needs no tiebreak");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The lnv2 replacement shape (identity + status, never hash history): an unsettled EXPIRED book
    /// row A under an external_id whose map row is a DIFFERENT invoice B that is OPEN — repaired to B
    /// and reopened, ONLY because the backend positively establishes B and reports A terminal-unpaid.
    /// The same with B PAID_UNRECOVERED repairs too (the condition backfill is ADR-0023's).
    #[cfg(feature = "fedimint")]
    #[tokio::test]
    async fn a_live_lnv2_replacement_over_a_stale_book_row_is_repaired_and_reopened() {
        for map_status in ["OPEN", "PAID", "PAID_UNRECOVERED"] {
            let dir = temp_dir();
            let store = mem_store();
            seed_book_invoice(&store, "lnv2-opA", "e1", "opA", "lnbcA", "EXPIRED", NOW - 10, None).await;
            let file = legacy_file(&dir, Backend::Lnv2, |c| {
                seed_lnv2_receive(c, "e1", "opB", "lnbcB", NOW + 600, map_status);
            });
            let probe = FakeProbe::with(&[
                ("lnv2-opB", ReceiveState::Established),
                ("lnv2-opA", ReceiveState::TerminalUnpaid),
            ]);
            let out = run(&store, Backend::Lnv2, &probe, &file, NOW).await.unwrap();
            assert_eq!(
                out,
                Outcome::Imported { receive_rows: 1, pay_rows: 0, repaired: 1, reaped: 0, stamped: 0 },
                "map status {map_status}"
            );
            assert_eq!(probe.asked(), vec!["lnv2-opB".to_string(), "lnv2-opA".to_string()]);
            let (id, bolt11, status): (String, String, String) = store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT id, bolt11, status FROM invoice WHERE external_id='e1'",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )?)
                })
                .await
                .unwrap();
            assert_eq!((id.as_str(), bolt11.as_str(), status.as_str()), ("lnv2-opB", "lnbcB", "OPEN"));
            assert_eq!(count(&store, "SELECT count(*) FROM event_log WHERE kind='adr0022_import_repair'").await, 1);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// The mismatched-restore shape — NEWER books naming replacement B over an OLDER file naming
    /// predecessor A — must NOT rewrite B back to A: the map's row is expired-unpaid (refuse), and
    /// even when A reads established, B's absence from the backend proves nothing (refuse).
    #[tokio::test]
    async fn newer_books_over_an_older_map_are_never_rewritten_back() {
        // 1. The map's A is terminal-unpaid at the backend: the map is the stale side.
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-hB", "e1", "hB", "lnbcB", "OPEN", NOW + 600, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "hA", "lnbcA", NOW - 10);
        });
        let probe = FakeProbe::with(&[("phoenixd-hA", ReceiveState::TerminalUnpaid)]);
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("does not positively report the map's invoice"), "{err:#}");
        assert_eq!(count(&store, "SELECT count(*) FROM invoice WHERE id='phoenixd-hB'").await, 1, "untouched");

        // 2. A established, B ABSENT (phoenixd forgot B): absence proves nothing -> refuse.
        let probe = FakeProbe::with(&[("phoenixd-hA", ReceiveState::Established)]);
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("does not positively report the book's invoice terminal-unpaid"), "{err:#}");
        assert_eq!(count(&store, "SELECT count(*) FROM invoice WHERE id='phoenixd-hB'").await, 1, "untouched");
        assert_eq!(marker(&store, Backend::Phoenixd).await, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// phoenixd's map keeps the OLD row beside a replacement (its upsert conflicts on invoice_id), so
    /// the import compares against the NEWEST rowid per external_id: a book row matching the OLD one
    /// is repaired to the newest, not stamped complete over the buyer's live successor.
    #[tokio::test]
    async fn phoenixd_compares_against_the_newest_map_row_per_external_id() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-hOld", "e1", "hOld", "lnbcOld", "EXPIRED", NOW - 10, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "hOld", "lnbcOld", NOW - 10);
            seed_phx_receive(c, "e1", "hNew", "lnbcNew", NOW + 600);
        });
        let probe = FakeProbe::with(&[
            ("phoenixd-hNew", ReceiveState::Established),
            ("phoenixd-hOld", ReceiveState::TerminalUnpaid),
        ]);
        let out = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap();
        assert!(matches!(out, Outcome::Imported { repaired: 1, receive_rows: 2, .. }), "{out:?}");
        let (id, status): (String, String) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT id, status FROM invoice WHERE external_id='e1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!((id.as_str(), status.as_str()), ("phoenixd-hNew", "OPEN"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A PAID / settled book row that disagrees with the map refuses: money was booked against data
    /// the map no longer describes, and no rule can say which side is right.
    #[tokio::test]
    async fn a_settled_book_row_that_disagrees_refuses() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "phoenixd-hA", "e1", "hA", "lnbcA", "PAID", NOW + 600, Some(NOW)).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_receive(c, "e1", "hB", "lnbcB", NOW + 600);
        });
        let probe = FakeProbe::with(&[
            ("phoenixd-hB", ReceiveState::Established),
            ("phoenixd-hA", ReceiveState::TerminalUnpaid),
        ]);
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("settled invoice phoenixd-hA"), "{err:#}");
        assert!(probe.asked().is_empty(), "a settled disagreement is refused before any probe");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A CANCELED map row is never a replacement.
    #[cfg(feature = "fedimint")]
    #[tokio::test]
    async fn a_canceled_map_row_is_never_a_replacement() {
        let dir = temp_dir();
        let store = mem_store();
        seed_book_invoice(&store, "lnv2-opA", "e1", "opA", "lnbcA", "OPEN", NOW + 600, None).await;
        let file = legacy_file(&dir, Backend::Lnv2, |c| {
            seed_lnv2_receive(c, "e1", "opB", "lnbcB", NOW + 600, "CANCELED");
        });
        let probe = FakeProbe::with(&[("lnv2-opB", ReceiveState::Established)]);
        let err = run(&store, Backend::Lnv2, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("CANCELED"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every legacy `phoenixd_unbookable_settlement` timer must resolve to an imported receive row.
    #[tokio::test]
    async fn a_timer_without_a_receive_row_refuses() {
        let dir = temp_dir();
        let store = mem_store();
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            c.execute(
                "INSERT INTO phoenixd_unbookable_settlement (invoice_id, first_refusal_at)
                 VALUES ('phoenixd-hZ', 5)",
                [],
            )
            .unwrap();
        });
        let probe = FakeProbe::default();
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("timer for invoice phoenixd-hZ"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------------------------------------
    // Pay coverage
    // ---------------------------------------------------------------------------------------------

    /// SENT coverage joins on the DERIVED pay key: a gen-1 LNURL refund must match its `:g1` row
    /// (never the bare gen-0 key), a NULL backend id on the attempt passes against a SUCCEEDED row,
    /// and a SENT attempt over an older FAILED map row REFUSES (the paid hash would be unowned).
    #[tokio::test]
    async fn sent_coverage_uses_the_derived_key_and_requires_a_succeeded_row() {
        // gen-1 SENT, NULL id, :g1 SUCCEEDED row -> covered (the bare key has no row on purpose).
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "SENT", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_pay(c, "refund:e1:g1", "lnbc-r1", Some("pid-1"), "SUCCEEDED");
        });
        let probe = FakeProbe::default();
        assert!(run(&store, Backend::Phoenixd, &probe, &file, NOW).await.is_ok());

        // A row under the BARE key only (gen 0) does not cover a gen-1 attempt.
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "SENT", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_pay(c, "refund:e1", "lnbc-r1", Some("pid-1"), "SUCCEEDED");
        });
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("pay key refund:e1:g1"), "{err:#}");

        // SENT over a FAILED map row for the same key/bolt11 refuses.
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "SENT", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_pay(c, "refund:e1:g1", "lnbc-r1", None, "FAILED");
        });
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("map status FAILED acceptable: false"), "{err:#}");

        // A backend id on the attempt must match the map's payment id.
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "SENT", "buyer@ln.example", Some("lnbc-r1"), 1, Some("pid-other")).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_pay(c, "refund:e1:g1", "lnbc-r1", Some("pid-1"), "SUCCEEDED");
        });
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("backend id agrees: false"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A PENDING attempt without an id but WITH a present pay row must agree with it: a stale PREPARED
    /// row naming a different bolt11 is the shape that lets recovery adopt payment B for liability A.
    #[tokio::test]
    async fn a_present_prepared_row_naming_a_different_bolt11_refuses() {
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "PENDING", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |c| {
            seed_phx_pay(c, "refund:e1:g1", "lnbc-OTHER", None, "PREPARED");
        });
        let probe = FakeProbe::default();
        let err = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap_err();
        assert!(format!("{err:#}").contains("bolt11 agrees: false"), "{err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A present file that omits the map row of a PENDING (or retryable FAILED) attempt without an id
    /// cannot tell 'never started' from 'witness lost': the attempt is stamped and parked, and the
    /// import still completes.
    #[tokio::test]
    async fn a_present_file_omitting_an_unwitnessed_attempt_stamps_it() {
        let dir = temp_dir();
        let store = mem_store();
        seed_refund(&store, "e1", "FAILED", "buyer@ln.example", Some("lnbc-r1"), 1, None).await;
        seed_sweep(&store, "sweep:h2", "PENDING", "lnbc-s2", None).await;
        let file = legacy_file(&dir, Backend::Phoenixd, |_c| {});
        let probe = FakeProbe::default();
        let out = run(&store, Backend::Phoenixd, &probe, &file, NOW).await.unwrap();
        assert_eq!(
            out,
            Outcome::Imported { receive_rows: 0, pay_rows: 0, repaired: 0, reaped: 0, stamped: 2 }
        );
        assert_eq!(fence(&store, "refund_attempt", "ref-e1").await, Some(NOW));
        assert_eq!(fence(&store, "sweep_attempt", "sweep:h2").await, Some(NOW));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
