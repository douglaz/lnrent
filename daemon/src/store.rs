//! sqlite state + the **sole-writer store actor** (ADR-0001). Schema: SPEC.md §11.
//!
//! One tokio task owns the `Connection` and is the only accessor; every read/write goes
//! through it via a closure, so there are no write races and the sole-writer invariant is
//! structural, not a convention. `transaction()` gives the atomic multi-row transitions the
//! money path needs (e.g. capture: invoice OPEN->PAID + sub PENDING->PROVISIONING in one txn).

use anyhow::{anyhow, Result};
use rusqlite::{Connection, Transaction};
use std::collections::HashSet;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

const SETTLE_REFUND_KINDS_SQL: &str = "'settle_unmatched_refund', 'settle_terminal_refund',
                                         'settle_orphan_refund', 'settle_expired_refund'";

/// How long durable business idempotency rows are retained. Deliberately LONGER than the transport
/// dedupe window (`SEEN_MESSAGE_RETENTION_SECS`, 90d) so the cached response/result OUTLIVES the
/// `seen_message` suppressor: a late duplicate order/op DM redelivered just past the dedupe window
/// still hits the cached response instead of re-executing (a second reservation/invoice or a re-run
/// management hook). The margin is the backstop the two-window design relies on.
pub const IDEMPOTENCY_CACHE_RETENTION_SECS: i64 = 120 * 24 * 60 * 60;

/// Rows removed by one durable idempotency cache sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IdempotencyPruneReport {
    pub op_invocation: usize,
    pub inbound_request: usize,
}

impl IdempotencyPruneReport {
    pub fn total(self) -> usize {
        self.op_invocation + self.inbound_request
    }
}

/// A value-received/not-delivered row for the supervisor's refund-readiness check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefundReadinessLiability {
    pub external_id: String,
    pub gross_sat: u64,
    pub source: RefundReadinessSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefundReadinessSource {
    RefundAttempt(RefundAttemptLiability),
    PaidUndeliveredOrder,
    UnreconciledSettlement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefundAttemptLiability {
    pub status: String,
    pub idempotency_key: String,
    pub dest: Option<String>,
    pub resolved_bolt11: Option<String>,
    pub resolved_expiry: Option<i64>,
    pub resolution_gen: i64,
}

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS operator (
  master_pubkey   TEXT,   -- brand identity (NIP-06 account 0)
  box_index       INTEGER,-- this Box's derivation account
  op_pubkey       TEXT,   -- this Box's operational pubkey
  payment_backend TEXT,
  compute_backend TEXT,
  relays          TEXT
);

CREATE TABLE IF NOT EXISTS recipe (   -- listings are their own table (one recipe -> many)
  id               TEXT PRIMARY KEY,
  version          TEXT,
  manifest_json    TEXT
);

CREATE TABLE IF NOT EXISTS subscription (
  id                   TEXT PRIMARY KEY,
  recipe_id            TEXT,
  listing_id           TEXT,
  instance_id          TEXT,
  buyer_pubkey         TEXT,
  state                TEXT,    -- SubState; SPEC.md §6.3
  params_json          TEXT,
  refund_dest          TEXT,    -- BOLT12 offer or Lightning address
  -- backend handles live on `instance` (instance_id), not duplicated here
  period_s             INTEGER, -- copied from the listing at order time
  renew_lead_s         INTEGER,
  retention_s          INTEGER,
  paid_through         INTEGER, -- hard expiry
  soft_date            INTEGER, -- renewal recommended from here
  next_deadline        INTEGER, -- reconcile-loop cursor
  suspend_not_before   INTEGER, -- suspend FLOOR set ONLY by downtime credit (§6.5)
  created_at           INTEGER,
  updated_at           INTEGER
);

CREATE TABLE IF NOT EXISTS invoice (
  id                 TEXT PRIMARY KEY,
  subscription_id    TEXT,
  external_id        TEXT NOT NULL UNIQUE, -- unique per-invoice token; backend externalId (ADR-0009)
  backend_invoice_id TEXT,        -- the backend's own invoice id
  payment_hash       TEXT,
  kind               TEXT,    -- order | renewal
  bolt11             TEXT,
  amount_sat         INTEGER,
  status             TEXT,    -- OPEN | PAID | EXPIRED
  expires_at         INTEGER, -- bolt11 expiry; order reservation released at this
  applied_at         INTEGER, -- when settlement was captured/applied (durable applied marker)
  issued_at          INTEGER,
  settled_at         INTEGER
);

CREATE TABLE IF NOT EXISTS event_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  subscription_id TEXT,
  kind            TEXT,
  detail_json     TEXT,
  at              INTEGER
);

CREATE TABLE IF NOT EXISTS reservation (   -- capacity held for a PENDING order (§9.3)
  id             TEXT PRIMARY KEY,
  order_id       TEXT NOT NULL UNIQUE,  -- one reservation per order (idempotent re-reserve)
  resources_json TEXT,
  ports_json     TEXT,
  state          TEXT,    -- HELD | CONSUMED | RELEASED  (CONSUMED = an active Instance's hold)
  expires_at     INTEGER,
  created_at     INTEGER
);

CREATE TABLE IF NOT EXISTS daemon_state (  -- single row; heartbeat for downtime credit (§6.5)
  last_heartbeat INTEGER
);

CREATE TABLE IF NOT EXISTS refund_attempt (  -- durable refund ledger (ADR-0009, §6.6)
  id                 TEXT PRIMARY KEY,
  subscription_id    TEXT,
  dest               TEXT,
  amount_sat         INTEGER,
  idempotency_key    TEXT NOT NULL UNIQUE,  -- dedups outbound pay AND the ledger row (refund:<external_id>); INSERT ON CONFLICT DO NOTHING (§6.6)
  backend_payment_id TEXT,           -- from pay(), once known
  status             TEXT NOT NULL,  -- PENDING (durable intent; retry pay(key) safely on restart) | SENT | FAILED
  attempts           INTEGER,
  -- refund-dest RESOLVER (lnrent-ug8): the concrete bolt11 a LN-address/LNURL `dest` resolved to,
  -- cached so a retry re-pays the SAME invoice. `resolution_gen` (0 = bolt11 pass-through, no
  -- resolution; 1+ once resolved) binds each (re-)resolution to its OWN pay key: gen 0 is the BARE
  -- `refund:<external_id>` (the key a pre-ug8 binary paid bolt11 refunds under, so a legacy refund
  -- dedups on the same key — lnrent-4gt), gen>=1 is `refund:<external_id>:g<gen>`. Retries never
  -- double-pay and only a CURRENT-gen Failed+expired invoice is ever re-resolved (the codex P0 fix,
  -- §6.6).
  resolved_bolt11    TEXT,
  resolved_expiry    INTEGER,
  resolution_gen     INTEGER NOT NULL DEFAULT 0,
  created_at         INTEGER,
  updated_at         INTEGER
);

CREATE TABLE IF NOT EXISTS outbox (   -- pending operator->buyer NIP-17 DMs (ADR-0009)
  id              TEXT PRIMARY KEY,
  recipient       TEXT,
  subscription_id TEXT,
  msg_type        TEXT,
  payload_json    TEXT,
  state           TEXT,    -- PENDING | SENT | FAILED (structurally-undeliverable, quarantined)
  attempts        INTEGER,
  created_at      INTEGER,
  sent_at         INTEGER
);

CREATE TABLE IF NOT EXISTS op_invocation (  -- durable buyer management ops (§7.4, ADR-0013)
  sender_pubkey   TEXT NOT NULL,
  request_id      TEXT NOT NULL,    -- the op.request `id`
  subscription_id TEXT,
  op              TEXT,
  state           TEXT NOT NULL CHECK (state IN ('RUNNING','DONE','ERROR')),
  result_json     TEXT,    -- cached op.result data (resent verbatim on a duplicate)
  error_json      TEXT,    -- cached op.result error { code, message, retryable }
  created_at      INTEGER,
  finished_at     INTEGER,
  -- startup recovery: an orphaned RUNNING row (daemon restart mid-op) -> ERROR
  -- {code:"interrupted", retryable:false} without re-running the hook (§5.1, lnrent-7fp.20)
  PRIMARY KEY (sender_pubkey, request_id)  -- idempotency: a dup never re-runs the hook
);

CREATE TABLE IF NOT EXISTS inbound_request (  -- idempotency for order/renew request DMs (§5.1)
  sender_pubkey     TEXT NOT NULL,
  request_id        TEXT NOT NULL,
  kind              TEXT NOT NULL,    -- order | renew
  response_msg_type TEXT,
  response_json     TEXT,             -- cached reply, resent on a duplicate
  created_at        INTEGER,
  PRIMARY KEY (sender_pubkey, request_id)  -- a dup never creates a 2nd reservation/order/invoice
);

CREATE TABLE IF NOT EXISTS box (   -- a hosting box managed by this control node (§4.5, §9.3)
  id             TEXT PRIMARY KEY,
  host_op_pubkey TEXT,             -- the box's operational key (ADR-0004/0010)
  profile_json   TEXT,             -- the signed host security profile (§9.1)
  capacity_json  TEXT,             -- total {cpu, mem_mb, disk_gb, ports}
  state          TEXT,             -- ONLINE | OFFLINE | DRAINING
  last_seen      INTEGER
);

CREATE TABLE IF NOT EXISTS instance (  -- a provisioned unit of work (§4.4)
  id              TEXT PRIMARY KEY,
  subscription_id TEXT,
  box_id          TEXT,
  kind            TEXT,            -- the recipe service id
  handles_json    TEXT,            -- backend handles (container id, peer index, ...)
  state           TEXT,            -- CREATING | RUNNING | STOPPED | DESTROYED
  created_at      INTEGER,
  updated_at      INTEGER
);

CREATE TABLE IF NOT EXISTS listing (  -- one Recipe -> many Listings (CONTEXT glossary)
  id         TEXT PRIMARY KEY,      -- NIP-99 coordinate "30402:<pubkey>:<d>" (§5.4)
  recipe_id  TEXT,
  d_tag        TEXT,                -- the replaceable-event d tag
  event_id     TEXT,                -- latest published event id
  amount_sat   INTEGER,
  period_s     INTEGER,             -- per-Listing timers (§6.3); copied to the subscription at order time
  renew_lead_s INTEGER,
  retention_s  INTEGER,
  state        TEXT,                -- ACTIVE | WITHDRAWN
  updated_at   INTEGER
);

CREATE TABLE IF NOT EXISTS native_connect_session (  -- interactive-op tickets (§7.4/§9.2; M1b+)
  id              TEXT PRIMARY KEY,
  subscription_id TEXT,
  scope           TEXT,            -- which interactive ops the ticket authorizes
  ticket_json     TEXT,            -- the Iroh connection ticket delivered to the buyer
  state           TEXT,            -- ACTIVE | REVOKED  (revoked on suspend/cancel/destroy)
  expires_at      INTEGER,
  created_at      INTEGER
);
"#;

/// Migration 2 (lnrent-7fp.5): the transport-level inbound-DM dedup table. The Nostr engine
/// keys it on the OUTER NIP-17 gift-wrap event id (unique per delivered DM) and writes a row only
/// AFTER a handler durably commits, so a relay redelivery — or a daemon restart — never re-routes a
/// *completed* inbound DM (§5.1). It is best-effort by design: a crash mid-handling leaves no row,
/// so the wrap is reprocessed and the authoritative business idempotency on `(sender, request_id)`
/// in `inbound_request` / `op_invocation` makes that re-run safe. Appended as a new migration —
/// never edit a shipped one.
const M2_SEEN_MESSAGE: &str = r#"
CREATE TABLE IF NOT EXISTS seen_message (  -- transport dedup of inbound gift wraps (§5.1, lnrent-7fp.5)
  event_id  TEXT PRIMARY KEY,  -- the kind-1059 gift-wrap OUTER event id (stable per delivered DM)
  sender    TEXT,              -- decoded sender pubkey (audit)
  msg_type  TEXT,              -- the lnrent DM `type` (audit)
  seen_at   INTEGER NOT NULL   -- when first routed (audit; unix secs)
);
CREATE INDEX IF NOT EXISTS seen_message_seen_at_idx ON seen_message(seen_at);
"#;

/// Migration 3 (lnrent-7fp.22): the downtime-credit suspend FLOOR. A NULLABLE absolute timestamp on
/// `subscription`, set ONLY by the restart downtime-credit path (§6.5, ADR-0005): on restart we
/// credit the operator's outage so a buyer gets their full `renew_lead` window of operator
/// availability before suspension. `paid_through` is NEVER moved (it anchors prepaid money AND the
/// `renew:auto:<sub>:<paid_through>` invoice key); the floor self-expires once a later renewal pushes
/// `paid_through` past it. Existing rows get NULL — no credit, which is correct. Appended as a NEW
/// migration — never edit a shipped migration.
const M3_SUSPEND_NOT_BEFORE: &str =
    "ALTER TABLE subscription ADD COLUMN suspend_not_before INTEGER;";

/// Migration 4 (lnrent-ug8): the refund-dest RESOLVER's generation-bound idempotency columns on
/// `refund_attempt`. `resolved_bolt11`/`resolved_expiry` cache the concrete bolt11 the resolver
/// produced for an LN-address/LNURL `dest` (the buyer is offline at refund time, §6.6), and
/// `resolution_gen` (0 = bolt11 pass-through; 1+ once resolved) binds each (re-)resolution to its OWN
/// backend pay key — gen 0 is the bare `refund:<external_id>` (the key a pre-ug8 binary paid bolt11
/// refunds under, so a legacy refund dedups on it — lnrent-4gt), gen>=1 is `refund:<external_id>:g<gen>`
/// — so a retry never double-pays and only a CURRENT-gen
/// Failed+expired invoice is ever re-resolved. Mirrors the §11 schema (added to the `refund_attempt`
/// CREATE TABLE above), so — like `suspend_not_before` — a fresh DB applies the CREATE first and this
/// ALTER is a tolerated duplicate, while a legacy DB gets the ALTER. Appended — never edit a shipped
/// migration.
const M4_REFUND_RESOLUTION: &str = "
ALTER TABLE refund_attempt ADD COLUMN resolved_bolt11 TEXT;
ALTER TABLE refund_attempt ADD COLUMN resolved_expiry INTEGER;
ALTER TABLE refund_attempt ADD COLUMN resolution_gen INTEGER NOT NULL DEFAULT 0;
";

// lnrent-xjn: back the durable idempotency-cache TTL sweep (reconcile) with indexes so the periodic
// prune does not full-scan these flood-prone tables (mirrors seen_message_seen_at_idx).
const M5_IDEMPOTENCY_CACHE_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS op_invocation_finished_at_idx ON op_invocation(finished_at);
CREATE INDEX IF NOT EXISTS inbound_request_created_at_idx ON inbound_request(created_at);
";

// The maintenance pass scans event_log by kind (the refund-readiness settle/refund journal CTEs in
// load_refund_readiness_liabilities) and by (subscription_id, kind) (the provision-cleanup recovery's
// correlated NOT EXISTS) every few seconds on the sole-writer connection; event_log journals every
// mutation and has no GC yet, so without indexes those are ever-growing full scans serialized ahead
// of money writes.
const M6_EVENT_LOG_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS event_log_kind_idx ON event_log(kind);
CREATE INDEX IF NOT EXISTS event_log_sub_kind_idx ON event_log(subscription_id, kind);
";

/// Ordered migrations (lnrent-7fp.3): index `i` upgrades the DB from schema version `i` to
/// `i+1`. Version 1 is the §11 schema; version 2 adds `seen_message` (lnrent-7fp.5); version 3 adds
/// `subscription.suspend_not_before` (lnrent-7fp.22); version 4 adds the `refund_attempt` resolver
/// columns (lnrent-ug8); version 5 adds the idempotency-cache TTL-sweep indexes (lnrent-xjn);
/// version 6 adds the event_log scan indexes. A future
/// schema change appends a new entry of `ALTER`/`CREATE` statements; **never edit a shipped migration**.
const MIGRATIONS: &[&str] = &[
    SCHEMA,
    M2_SEEN_MESSAGE,
    M3_SUSPEND_NOT_BEFORE,
    M4_REFUND_RESOLUTION,
    M5_IDEMPOTENCY_CACHE_INDEXES,
    M6_EVENT_LOG_INDEXES,
];

/// The target schema version this binary expects (= number of migrations).
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Apply any pending migrations, keyed on `PRAGMA user_version`. Idempotent: opening a
/// current DB is a no-op; opening a v0 DB applies the §11 schema and sets `user_version=1`.
pub fn migrate(conn: &Connection) -> Result<()> {
    let mut current: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    while (current as usize) < MIGRATIONS.len() {
        if let Err(e) = conn.execute_batch(MIGRATIONS[current as usize]) {
            if current == 2
                && is_duplicate_suspend_not_before(&e)
                && has_column(conn, "subscription", "suspend_not_before")?
            {
                // Fresh DBs apply the current CREATE TABLE first; legacy v2 DBs get the ALTER.
            } else if current == 3 && is_duplicate_refund_resolution(&e) {
                // Fresh DBs already have the resolver columns from the §11 schema, so the FIRST ALTER
                // in the M4 batch is a duplicate — and `execute_batch` aborts at that first duplicate.
                // A crash that applied only SOME of the three columns (user_version still 3) would
                // re-enter here with the rest still missing, so add each MISSING column individually:
                // a partially-applied migration self-heals instead of failing startup (review P2).
                ensure_refund_resolution_columns(conn)?;
            } else {
                return Err(e.into());
            }
        }
        // user_version can't be a bound parameter; the value is an internal counter, not input.
        conn.execute_batch(&format!("PRAGMA user_version = {}", current + 1))?;
        current += 1;
    }
    Ok(())
}

fn is_duplicate_suspend_not_before(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(_, Some(msg))
            if msg.contains("duplicate column name: suspend_not_before")
    )
}

fn is_duplicate_refund_resolution(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(_, Some(msg))
            if msg.contains("duplicate column name: resolved_bolt11")
                || msg.contains("duplicate column name: resolved_expiry")
                || msg.contains("duplicate column name: resolution_gen")
    )
}

/// Idempotently add the lnrent-ug8 resolver columns to `refund_attempt`, one at a time, skipping any
/// that already exist. SQLite has no `ADD COLUMN IF NOT EXISTS` and `execute_batch` aborts at the
/// first duplicate, so the M4 batch alone can't complete a PARTIALLY-applied migration (a crash
/// between two of its ALTERs leaves `user_version` at 3 with some columns present and some missing).
/// This per-column add does: re-running M4 after such a crash adds only the still-missing columns,
/// so startup self-heals rather than failing on the leading duplicate (review P2).
fn ensure_refund_resolution_columns(conn: &Connection) -> Result<()> {
    for (col, ddl) in [
        (
            "resolved_bolt11",
            "ALTER TABLE refund_attempt ADD COLUMN resolved_bolt11 TEXT",
        ),
        (
            "resolved_expiry",
            "ALTER TABLE refund_attempt ADD COLUMN resolved_expiry INTEGER",
        ),
        (
            "resolution_gen",
            "ALTER TABLE refund_attempt ADD COLUMN resolution_gen INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        if !has_column(conn, "refund_attempt", col)? {
            conn.execute_batch(ddl)?;
        }
    }
    Ok(())
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
    for name in rows {
        if name? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Open the state database, enable WAL (durability + headroom for a future read-only connection;
/// in this actor all access still runs on one connection thread — §11), and run
/// migrations up to the current `SCHEMA_VERSION`.
pub fn open(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    migrate(&conn)?;
    Ok(conn)
}

/// A unit of work the store actor runs on its `Connection`. Each job does its sqlite work
/// synchronously and sends the typed reply on its own oneshot.
type Job = Box<dyn FnOnce(&mut Connection) + Send>;

/// Cloneable handle to the sole-writer store actor (ADR-0001). All access serializes through
/// the one actor task that owns the `Connection`.
#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Job>,
}

impl Store {
    /// Spawn the actor owning `conn`; returns a cloneable handle. The actor holds the
    /// `Connection` (which is `Send`) only across `recv().await`, never across other awaits.
    pub fn spawn(conn: Connection) -> Store {
        let (tx, mut rx) = mpsc::channel::<Job>(64);
        tokio::spawn(async move {
            let mut conn = conn;
            while let Some(job) = rx.recv().await {
                job(&mut conn);
            }
        });
        Store { tx }
    }

    /// Open the DB (WAL + schema) and spawn the actor in one step.
    pub fn open_spawn(path: impl AsRef<Path>) -> Result<Store> {
        Ok(Store::spawn(open(path)?))
    }

    /// Run `f` inside ONE transaction: **commit** if it returns `Ok`, **roll back** if it
    /// returns `Err`. This is how the handshake gets its atomic multi-row transitions.
    pub async fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Transaction) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.run(move |conn| {
            let txn = conn.transaction()?;
            let out = f(&txn)?;
            txn.commit()?;
            Ok(out)
        })
        .await
    }

    /// Run `f` for **queries only** — `f` gets `&Connection`; all writes go through
    /// `transaction()` (atomic + journaled). The read-only invariant is structural, not just a
    /// doc promise: `f` runs inside a DEFERRED transaction we ALWAYS roll back, so even a stray
    /// mutation cannot persist (codex re-review). DEFERRED takes no write lock for a pure read.
    /// Like every access it runs on the actor thread, so reads serialize with writes (fine for
    /// M1a's low read concurrency; a separate read-only WAL connection is a later optimization,
    /// not the sole-writer guarantee).
    pub async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.run(move |conn| {
            let tx = conn.transaction()?;
            let out = f(&tx)?;
            tx.rollback()?;
            Ok(out)
        })
        .await
    }

    /// Liability set for INV-2 refund-readiness. Buckets are de-duplicated by `external_id` with
    /// precedence: refund_attempt > paid-undelivered order > unreconciled settlement.
    pub async fn refund_readiness_liabilities(&self) -> Result<Vec<RefundReadinessLiability>> {
        self.read(load_refund_readiness_liabilities).await
    }

    /// Prune durable business idempotency caches past the retention window. The cutoff uses SQLite
    /// wall time, capped by the daemon clock that stamps these rows, so synthetic-clock tests and a
    /// lagging daemon clock do not over-prune. RUNNING op invocations are never removed: they are the
    /// in-flight idempotency claim.
    pub async fn prune_idempotency_caches(&self, now: i64) -> Result<IdempotencyPruneReport> {
        self.transaction(move |tx| {
            let op_invocation = tx.execute(
                "DELETE FROM op_invocation
                  WHERE state IN ('DONE', 'ERROR')
                    AND finished_at < MIN(unixepoch(), ?1) - ?2",
                rusqlite::params![now, IDEMPOTENCY_CACHE_RETENTION_SECS],
            )?;
            let inbound_request = tx.execute(
                "DELETE FROM inbound_request
                  WHERE created_at < MIN(unixepoch(), ?1) - ?2",
                rusqlite::params![now, IDEMPOTENCY_CACHE_RETENTION_SECS],
            )?;
            Ok(IdempotencyPruneReport {
                op_invocation,
                inbound_request,
            })
        })
        .await
    }

    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let (rtx, rrx) = oneshot::channel();
        let job: Job = Box::new(move |conn| {
            let _ = rtx.send(f(conn));
        });
        self.tx
            .send(job)
            .await
            .map_err(|_| anyhow!("store actor stopped"))?;
        rrx.await
            .map_err(|_| anyhow!("store actor dropped the reply"))?
    }
}

fn load_refund_readiness_liabilities(conn: &Connection) -> Result<Vec<RefundReadinessLiability>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let refund_external_ids = load_refund_attempt_external_ids(conn)?;

    let refund_sql = format!(
        "WITH journal AS (
             SELECT json_extract(detail_json, '$.external_id') AS external_id,
                    CAST(json_extract(detail_json, '$.amount_sat') AS INTEGER) AS amount_sat
               FROM event_log
              WHERE kind IN ({SETTLE_REFUND_KINDS_SQL})
              GROUP BY external_id
         )
         SELECT r.external_id,
                COALESCE(i.amount_sat, journal.amount_sat) AS received_sat,
                r.status, r.idempotency_key, r.dest, r.resolved_bolt11,
                r.resolved_expiry, r.resolution_gen
           FROM (
                 SELECT *,
                        CASE
                          WHEN idempotency_key LIKE 'refund:%' THEN substr(idempotency_key, 8)
                          WHEN id LIKE 'ref-%' THEN substr(id, 5)
                          ELSE id
                        END AS external_id
                   FROM refund_attempt
                  WHERE status <> 'SENT'
                ) r
           LEFT JOIN invoice i
                  ON i.external_id = r.external_id
                 AND (i.status = 'PAID' OR i.settled_at IS NOT NULL)
           LEFT JOIN journal ON journal.external_id = r.external_id
          ORDER BY r.created_at, r.id"
    );
    let mut stmt = conn.prepare(&refund_sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<i64>>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<i64>>(6)?,
            r.get::<_, Option<i64>>(7)?.unwrap_or(0),
        ))
    })?;
    for row in rows {
        let (
            external_id,
            received_sat,
            status,
            idempotency_key,
            dest,
            resolved_bolt11,
            resolved_expiry,
            resolution_gen,
        ) = row?;
        let Some(gross_sat) = positive_sat(received_sat) else {
            continue;
        };
        if seen.insert(external_id.clone()) {
            out.push(RefundReadinessLiability {
                external_id,
                gross_sat,
                source: RefundReadinessSource::RefundAttempt(RefundAttemptLiability {
                    status,
                    idempotency_key,
                    dest,
                    resolved_bolt11,
                    resolved_expiry,
                    resolution_gen,
                }),
            });
        }
    }

    let mut stmt = conn.prepare(
        "SELECT i.external_id, i.amount_sat
           FROM invoice i
           JOIN subscription s ON s.id = i.subscription_id
          WHERE i.kind = 'order'
            AND (i.status = 'PAID' OR i.settled_at IS NOT NULL)
            AND s.state IN ('PENDING', 'PROVISIONING', 'REFUND_DUE')
          ORDER BY i.issued_at, i.id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, amount_sat) = row?;
        if refund_external_ids.contains(&external_id) {
            continue;
        }
        let Some(gross_sat) = positive_sat(amount_sat) else {
            continue;
        };
        if seen.insert(external_id.clone()) {
            out.push(RefundReadinessLiability {
                external_id,
                gross_sat,
                source: RefundReadinessSource::PaidUndeliveredOrder,
            });
        }
    }

    let mut stmt = conn.prepare(
        "SELECT i.external_id, i.amount_sat
           FROM invoice i
           LEFT JOIN subscription s ON s.id = i.subscription_id
          WHERE i.settled_at IS NOT NULL
            AND i.applied_at IS NULL
            AND COALESCE(i.kind, '') <> 'renewal'
            AND COALESCE(s.state, '') NOT IN ('ACTIVE', 'REFUNDED')
          ORDER BY i.settled_at, i.id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, amount_sat) = row?;
        if refund_external_ids.contains(&external_id) {
            continue;
        }
        let Some(gross_sat) = positive_sat(amount_sat) else {
            continue;
        };
        if seen.insert(external_id.clone()) {
            out.push(RefundReadinessLiability {
                external_id,
                gross_sat,
                source: RefundReadinessSource::UnreconciledSettlement,
            });
        }
    }

    let journal_sql = format!(
        "SELECT json_extract(e.detail_json, '$.external_id') AS external_id,
                CAST(json_extract(e.detail_json, '$.amount_sat') AS INTEGER) AS amount_sat
           FROM event_log e
           LEFT JOIN subscription s ON s.id = e.subscription_id
          WHERE e.kind IN ({SETTLE_REFUND_KINDS_SQL})
            AND COALESCE(s.state, '') NOT IN ('ACTIVE', 'REFUNDED')
          GROUP BY external_id
          ORDER BY e.id"
    );
    let mut stmt = conn.prepare(&journal_sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, amount_sat) = row?;
        if refund_external_ids.contains(&external_id) {
            continue;
        }
        let Some(gross_sat) = positive_sat(amount_sat) else {
            continue;
        };
        if seen.insert(external_id.clone()) {
            out.push(RefundReadinessLiability {
                external_id,
                gross_sat,
                source: RefundReadinessSource::UnreconciledSettlement,
            });
        }
    }

    Ok(out)
}

fn load_refund_attempt_external_ids(conn: &Connection) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    let mut stmt = conn.prepare(
        "SELECT CASE
                  WHEN idempotency_key LIKE 'refund:%' THEN substr(idempotency_key, 8)
                  WHEN id LIKE 'ref-%' THEN substr(id, 5)
                  ELSE id
                END
           FROM refund_attempt",
    )?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    for row in rows {
        ids.insert(row?);
    }
    Ok(ids)
}

fn positive_sat(amount: Option<i64>) -> Option<u64> {
    amount.and_then(|a| (a > 0).then_some(a as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_applies_to_memory_db() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 15);
    }

    #[test]
    fn migrate_fresh_applies_all_migrations() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let v: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let n: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // The §11 schema (15 tables) plus `seen_message` from migration 2 (lnrent-7fp.5).
        assert_eq!(n, 16);
    }

    // M6: the maintenance pass scans event_log every few seconds on the sole-writer connection;
    // these indexes must exist on both fresh and migrated DBs or those scans grow without bound.
    #[test]
    fn event_log_scan_indexes_exist_after_migrate() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        for idx in ["event_log_kind_idx", "event_log_sub_kind_idx"] {
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [idx],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "missing index {idx}");
        }
    }

    #[test]
    fn migrate_is_idempotent_on_current_db() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        conn.execute("INSERT INTO recipe (id, version) VALUES ('r','1')", [])
            .unwrap();
        migrate(&conn).unwrap(); // no-op
        let v: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let n: i64 = conn
            .query_row("SELECT count(*) FROM recipe", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "re-migrating a current db is a no-op (no data loss)");
    }

    // A simulated legacy (v0) DB — schema applied but user_version never set, with data — must
    // migrate to the current version without losing data, and gain the later tables.
    #[test]
    fn simulated_v0_db_migrates_to_current_without_data_loss() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute("INSERT INTO recipe (id, version) VALUES ('legacy','1')", [])
            .unwrap();
        assert_eq!(
            conn.query_row::<i64, _, _>("PRAGMA user_version", [], |r| r.get(0))
                .unwrap(),
            0
        );
        migrate(&conn).unwrap();
        assert_eq!(
            conn.query_row::<i64, _, _>("PRAGMA user_version", [], |r| r.get(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let id: String = conn
            .query_row("SELECT id FROM recipe WHERE id='legacy'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(id, "legacy", "data preserved across migration");
        // Migration 2 (lnrent-7fp.5) reached this legacy DB too — `seen_message` now exists.
        let seen: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='seen_message'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            seen, 1,
            "the later migration created seen_message on the legacy DB"
        );
    }

    // A crash BETWEEN the M4 batch's ALTERs leaves user_version at 3 with some resolver columns
    // present and some missing. Re-running migrate() must add only the still-missing columns and
    // reach the current version — not abort on the leading duplicate (review P2). migrate at v3 runs
    // ONLY M4, which touches just refund_attempt, so the partial table is all we need to set up.
    #[test]
    fn migrate_recovers_partially_applied_refund_resolution() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            // A real v3 DB has all the v1 base tables; include the ones the later migrations index
            // (M5 lnrent-xjn: op_invocation/inbound_request; M6: event_log) so the partial-M4
            // recovery still applies the full migration chain to current.
            "CREATE TABLE refund_attempt (id TEXT PRIMARY KEY, status TEXT, resolved_bolt11 TEXT);
             CREATE TABLE op_invocation (sender_pubkey TEXT, request_id TEXT, state TEXT, finished_at INTEGER);
             CREATE TABLE inbound_request (sender_pubkey TEXT, request_id TEXT, created_at INTEGER);
             CREATE TABLE event_log (id INTEGER PRIMARY KEY AUTOINCREMENT, subscription_id TEXT,
                                     kind TEXT, detail_json TEXT, at INTEGER);
             PRAGMA user_version = 3;",
        )
        .unwrap();
        assert!(has_column(&conn, "refund_attempt", "resolved_bolt11").unwrap());
        assert!(!has_column(&conn, "refund_attempt", "resolved_expiry").unwrap());
        assert!(!has_column(&conn, "refund_attempt", "resolution_gen").unwrap());

        migrate(&conn).unwrap();

        assert_eq!(
            conn.query_row::<i64, _, _>("PRAGMA user_version", [], |r| r.get(0))
                .unwrap(),
            SCHEMA_VERSION,
            "migrate reaches the current version after recovering the partial M4"
        );
        for col in ["resolved_bolt11", "resolved_expiry", "resolution_gen"] {
            assert!(
                has_column(&conn, "refund_attempt", col).unwrap(),
                "{col} present after recovery"
            );
        }
    }

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    async fn count(store: &Store, sql: &str) -> i64 {
        let sql = sql.to_string();
        store
            .read(move |c| Ok(c.query_row(&sql, [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    // A transaction commits on Ok and ROLLS BACK on Err (the atomicity the money path needs).
    #[tokio::test]
    async fn transaction_commits_and_rolls_back() {
        let s = mem_store();

        // Commit path: insert a recipe row.
        s.transaction(|tx| {
            tx.execute("INSERT INTO recipe (id, version) VALUES ('r1', '1')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let n: i64 = s
            .read(|c| Ok(c.query_row("SELECT count(*) FROM recipe", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(n, 1);

        // Rollback path: insert then return Err -> the insert must not persist.
        let res: Result<()> = s
            .transaction(|tx| {
                tx.execute("INSERT INTO recipe (id, version) VALUES ('r2', '1')", [])?;
                Err(anyhow!("boom"))
            })
            .await;
        assert!(res.is_err());
        let n: i64 = s
            .read(|c| Ok(c.query_row("SELECT count(*) FROM recipe", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(n, 1, "the rolled-back insert must not persist");
    }

    // The capture transition (invoice OPEN->PAID + sub PENDING->PROVISIONING) is all-or-nothing.
    #[tokio::test]
    async fn atomic_capture_rolls_back_on_error() {
        let s = mem_store();
        s.transaction(|tx| {
            tx.execute("INSERT INTO subscription (id, state) VALUES ('s1', 'PENDING')", [])?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, status) VALUES ('i1','s1','x1','OPEN')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        // A capture that fails partway must leave BOTH rows unchanged.
        let res: Result<()> = s
            .transaction(|tx| {
                tx.execute(
                    "UPDATE invoice SET status='PAID' WHERE id='i1' AND status='OPEN'",
                    [],
                )?;
                tx.execute(
                    "UPDATE subscription SET state='PROVISIONING' WHERE id='s1'",
                    [],
                )?;
                Err(anyhow!("crash mid-capture"))
            })
            .await;
        assert!(res.is_err());
        let (inv, sub): (String, String) = s
            .read(|c| {
                let inv =
                    c.query_row("SELECT status FROM invoice WHERE id='i1'", [], |r| r.get(0))?;
                let sub = c.query_row("SELECT state FROM subscription WHERE id='s1'", [], |r| {
                    r.get(0)
                })?;
                Ok((inv, sub))
            })
            .await
            .unwrap();
        assert_eq!(
            (inv.as_str(), sub.as_str()),
            ("OPEN", "PENDING"),
            "capture rolled back atomically"
        );

        // The successful capture commits both.
        s.transaction(|tx| {
            tx.execute(
                "UPDATE invoice SET status='PAID' WHERE id='i1' AND status='OPEN'",
                [],
            )?;
            tx.execute(
                "UPDATE subscription SET state='PROVISIONING' WHERE id='s1'",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let (inv, sub): (String, String) = s
            .read(|c| {
                let inv =
                    c.query_row("SELECT status FROM invoice WHERE id='i1'", [], |r| r.get(0))?;
                let sub = c.query_row("SELECT state FROM subscription WHERE id='s1'", [], |r| {
                    r.get(0)
                })?;
                Ok((inv, sub))
            })
            .await
            .unwrap();
        assert_eq!((inv.as_str(), sub.as_str()), ("PAID", "PROVISIONING"));
    }

    // Concurrent writers serialize through the actor with no lost update.
    #[tokio::test]
    async fn commands_serialize_no_lost_update() {
        let s = mem_store();
        s.transaction(|tx| {
            tx.execute("INSERT INTO daemon_state (last_heartbeat) VALUES (0)", [])?;
            Ok(())
        })
        .await
        .unwrap();

        const N: i64 = 100;
        let mut handles = Vec::new();
        for _ in 0..N {
            let s = s.clone();
            handles.push(tokio::spawn(async move {
                s.transaction(|tx| {
                    tx.execute(
                        "UPDATE daemon_state SET last_heartbeat = last_heartbeat + 1",
                        [],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let total: i64 = s
            .read(|c| Ok(c.query_row("SELECT last_heartbeat FROM daemon_state", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(
            total, N,
            "every increment landed (serialized, no lost update)"
        );
    }

    // read() is structurally read-only: a stray write inside it is visible within the closure
    // but rolled back, so it never persists (codex re-review — not just a doc promise).
    #[tokio::test]
    async fn read_rolls_back_stray_mutations() {
        let s = mem_store();
        s.transaction(|tx| {
            tx.execute("INSERT INTO daemon_state (last_heartbeat) VALUES (7)", [])?;
            Ok(())
        })
        .await
        .unwrap();
        // A read whose closure mistakenly writes: the write is seen WITHIN the read txn...
        let seen: i64 = s
            .read(|c| {
                c.execute("UPDATE daemon_state SET last_heartbeat = 999", [])?;
                Ok(c.query_row("SELECT last_heartbeat FROM daemon_state", [], |r| r.get(0))?)
            })
            .await
            .unwrap();
        assert_eq!(
            seen, 999,
            "the stray write is visible inside the read's own txn"
        );
        // ...but it was rolled back, so the durable value is untouched.
        let after: i64 = s
            .read(|c| Ok(c.query_row("SELECT last_heartbeat FROM daemon_state", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(
            after, 7,
            "the stray write did NOT persist — read() is structurally read-only"
        );
    }

    // The two idempotency keys (op_invocation, inbound_request) + refund_attempt UNIQUE:
    // a duplicate insert is a no-op, never a second row (§5.1, §6.6).
    #[tokio::test]
    async fn idempotency_keys_no_op_on_duplicate() {
        let s = mem_store();
        let dupes = [
            ("op_invocation",
             "INSERT OR IGNORE INTO op_invocation (sender_pubkey, request_id, state) VALUES ('p','r1','DONE')"),
            ("inbound_request",
             "INSERT OR IGNORE INTO inbound_request (sender_pubkey, request_id, kind) VALUES ('p','r1','order')"),
            ("refund_attempt",
             "INSERT OR IGNORE INTO refund_attempt (id, idempotency_key, status) VALUES (hex(randomblob(8)),'refund:x1','PENDING')"),
        ];
        for (table, sql) in dupes {
            for _ in 0..2 {
                let sql = sql.to_string();
                s.transaction(move |tx| {
                    tx.execute(&sql, [])?;
                    Ok(())
                })
                .await
                .unwrap();
            }
            let q = format!("SELECT count(*) FROM {table}");
            let n: i64 = s
                .read(move |c| Ok(c.query_row(&q, [], |r| r.get(0))?))
                .await
                .unwrap();
            assert_eq!(
                n, 1,
                "{table}: duplicate insert must not create a second row"
            );
        }
    }

    #[tokio::test]
    async fn prune_idempotency_caches_removes_only_old_terminal_rows() {
        let s = mem_store();
        let now = 2 * IDEMPOTENCY_CACHE_RETENTION_SECS;
        s.transaction(move |tx| {
            tx.execute(
                "INSERT INTO op_invocation
                    (sender_pubkey, request_id, subscription_id, op, state, result_json,
                     error_json, created_at, finished_at)
                 VALUES
                    ('buyer', 'old-done', 's1', 'restart', 'DONE', '{}', NULL,
                     ?1 - ?2 - 1, ?1 - ?2 - 1),
                    ('buyer', 'old-error', 's1', 'restart', 'ERROR', NULL, '{}',
                     ?1 - ?2 - 1, ?1 - ?2 - 1),
                    ('buyer', 'old-running', 's1', 'restart', 'RUNNING', NULL, NULL,
                     ?1 - ?2 - 1, ?1 - ?2 - 1),
                    ('buyer', 'recent-done', 's1', 'restart', 'DONE', '{}', NULL,
                     ?1, ?1 - ?2 + 1)",
                rusqlite::params![now, IDEMPOTENCY_CACHE_RETENTION_SECS],
            )?;
            tx.execute(
                "INSERT INTO inbound_request
                    (sender_pubkey, request_id, kind, response_msg_type, response_json, created_at)
                 VALUES
                    ('buyer', 'old-order', 'order', 'order.invoice', '{}',
                     ?1 - ?2 - 1),
                    ('buyer', 'recent-order', 'order', 'order.invoice', '{}',
                     ?1 - ?2 + 1)",
                rusqlite::params![now, IDEMPOTENCY_CACHE_RETENTION_SECS],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let pruned = s.prune_idempotency_caches(now).await.unwrap();
        assert_eq!(
            pruned,
            IdempotencyPruneReport {
                op_invocation: 2,
                inbound_request: 1,
            }
        );
        assert_eq!(pruned.total(), 3);

        assert_eq!(
            count(&s, "SELECT count(*) FROM op_invocation").await,
            2,
            "only old terminal op_invocation rows are pruned"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM op_invocation WHERE request_id='old-running' AND state='RUNNING'",
            )
            .await,
            1,
            "RUNNING rows keep their in-flight idempotency claim"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM op_invocation WHERE request_id='recent-done'"
            )
            .await,
            1,
            "recent terminal op_invocation rows stay within retention"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM inbound_request WHERE request_id='recent-order'"
            )
            .await,
            1,
            "recent inbound_request rows stay within retention"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM inbound_request WHERE request_id='old-order'"
            )
            .await,
            0,
            "old inbound_request rows are pruned"
        );

        assert_eq!(
            s.prune_idempotency_caches(now).await.unwrap(),
            IdempotencyPruneReport::default(),
            "re-running the sweep is idempotent"
        );
    }

    // WAL is enabled on a real file DB (durability + a future RO read path; §11).
    #[tokio::test]
    async fn wal_is_enabled() {
        let path = std::env::temp_dir().join(format!("lnrent-store-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let conn = open(&path).unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode.to_lowercase(), "wal");
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
