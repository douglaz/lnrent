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
        let map_missing_or_canceled = match m {
            None => true,
            Some(m) => m.status == "CANCELED" && m.invoice_id != b.id,
        };
        if map_missing_or_canceled {
            if reap_eligible(conn, &b, now)? {
                plan.reap.push(b.id.clone());
                continue;
            }
            let reason = match m {
                None => format!(
                    "invoice {} (external_id {}) has no {} row in the legacy index",
                    b.id,
                    b.external_id,
                    backend.receive_table()
                ),
                Some(m) => format!(
                    "invoice {} (external_id {}) is correlated only to a CANCELED {} row {} that is \
                     not its own invoice — a CANCELED row is never a replacement",
                    b.id,
                    b.external_id,
                    backend.receive_table(),
                    m.invoice_id
                ),
            };
            plan.refusal.get_or_insert(reason);
            continue;
        }
        let m = m.expect("checked above");
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
