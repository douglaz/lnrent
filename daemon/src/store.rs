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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

// `pub(crate)` so the ledger `expected_msat` helper (lnrent-urw.10) reuses the SAME settle-refund
// journal-kind list this module's readiness liability scan uses — one definition of the INV-3
// Class-B receipt provenance.
pub(crate) const SETTLE_REFUND_KINDS_SQL: &str = "'settle_unmatched_refund', 'settle_terminal_refund',
                                         'settle_orphan_refund', 'settle_expired_refund'";

/// How long durable business idempotency rows are retained. Deliberately LONGER than the transport
/// dedupe window (`SEEN_MESSAGE_RETENTION_SECS`, 90d) so the cached response/result OUTLIVES the
/// `seen_message` suppressor: a late duplicate order/op DM redelivered just past the dedupe window
/// still hits the cached response instead of re-executing (a second reservation/invoice or a re-run
/// management hook). The margin is the backstop the two-window design relies on.
pub const IDEMPOTENCY_CACHE_RETENTION_SECS: i64 = 120 * 24 * 60 * 60;

/// How long terminal, unreferenced business rows are retained before the terminal-row reaper
/// (lnrent-y4m.2) deletes them. A generous 30-day floor for rows the predicates below can prove are
/// no longer live; money/correlation rows fail closed and are kept rather than trusting the window.
/// This is the single operator-tunable knob: one named const, no per-table/config policy engine
/// (an explicit non-goal — "operator-tunable" is a one-edit const, not plumbing).
pub const TERMINAL_ROW_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

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

/// Rows removed by one terminal-row reap, per table (logging + tests, lnrent-y4m.2).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapCounts {
    pub reservation: usize,
    pub invoice: usize,
    pub event_log: usize,
    pub instance: usize,
    pub subscription: usize,
}

impl ReapCounts {
    pub fn total(self) -> usize {
        self.reservation + self.invoice + self.event_log + self.instance + self.subscription
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
  state                TEXT,    -- lifecycle state string; SPEC.md §6.3
  params_json          TEXT,
  refund_dest          TEXT,    -- LN address or HTTPS LNURL (§6.4 F3/F6; BOLT12/raw bolt11 rejected at intake)
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
CREATE TABLE IF NOT EXISTS teardown_failure (  -- orphaned-instance dead-letter (lnrent-urw.2, GATE-1 PR-6)
  id              TEXT PRIMARY KEY,  -- stable `td:<subscription_id>:<hook>` so a repeat failure upserts
  subscription_id TEXT NOT NULL,
  hook            TEXT NOT NULL,     -- the lifecycle hook that failed (M1a: `destroy`)
  handles_json    TEXT,              -- the instance handles the retry re-runs the hook with
  attempts        INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,              -- capped like MAX_TEARDOWN_ERROR_CHARS
  first_failed_at INTEGER NOT NULL,
  last_attempt_at INTEGER NOT NULL,
  resolved_at     INTEGER            -- NULL = still open (provider-side cleanup owed)
);
CREATE INDEX IF NOT EXISTS teardown_failure_open_idx ON teardown_failure(resolved_at);

CREATE TABLE IF NOT EXISTS sweep_attempt (  -- durable operator-sweep ledger (gate1-operator-sweep, urw.3)
  id                 TEXT PRIMARY KEY,   -- sweep:<payment_hash> (== the outbound pay key; unique per invoice)
  bolt11             TEXT,               -- the operator's own invoice being paid
  amount_sat         INTEGER,            -- the invoice's own whole-sat amount
  max_outlay_msat    INTEGER NOT NULL,   -- the QUOTED outlay cap; subtracted from ledger surplus AND expected_msat while PENDING/SENT
  status             TEXT NOT NULL,      -- PENDING (durable intent; re-drive pay(key) safely on restart) | SENT | FAILED
  attempts           INTEGER NOT NULL DEFAULT 0,
  backend_payment_id TEXT,               -- from pay_capped(), once known
  last_error         TEXT,
  created_at         INTEGER,
  sent_at            INTEGER
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

// lnrent-urw.2 (GATE-1 PR-6): the orphaned-instance dead-letter table. A NEW table, so
// `CREATE TABLE IF NOT EXISTS` is inherently idempotent — a fresh DB creates it from the §11 SCHEMA
// above and this migration is a no-op; a legacy DB gets the table here. No duplicate-column dance.
// Appended — never edit a shipped migration.
const M7_TEARDOWN_FAILURE: &str = "
CREATE TABLE IF NOT EXISTS teardown_failure (
  id              TEXT PRIMARY KEY,
  subscription_id TEXT NOT NULL,
  hook            TEXT NOT NULL,
  handles_json    TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  first_failed_at INTEGER NOT NULL,
  last_attempt_at INTEGER NOT NULL,
  resolved_at     INTEGER
);
CREATE INDEX IF NOT EXISTS teardown_failure_open_idx ON teardown_failure(resolved_at);
";

// gate1-operator-sweep (urw.3): the operator-sweep ledger table. A NEW table, so
// `CREATE TABLE IF NOT EXISTS` is inherently idempotent — a fresh DB creates it from the §11 SCHEMA
// above and this migration is a no-op; a legacy DB gets the table here (mirrors M7_TEARDOWN_FAILURE).
// No duplicate-column dance. Appended — never edit a shipped migration.
const M8_SWEEP_ATTEMPT: &str = "
CREATE TABLE IF NOT EXISTS sweep_attempt (
  id                 TEXT PRIMARY KEY,
  bolt11             TEXT,
  amount_sat         INTEGER,
  max_outlay_msat    INTEGER NOT NULL,
  status             TEXT NOT NULL,
  attempts           INTEGER NOT NULL DEFAULT 0,
  backend_payment_id TEXT,
  last_error         TEXT,
  created_at         INTEGER,
  sent_at            INTEGER
);
";

// lnrent-y4m.2: back the per-tick terminal-row reaper with simple predicate indexes. These are not a
// lifecycle policy engine; they keep the fixed DELETE set from full-scanning flood-prone tables on
// the sole-writer store actor.
const M9_TERMINAL_ROW_REAPER_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS reservation_state_created_at_idx ON reservation(state, created_at);
CREATE INDEX IF NOT EXISTS invoice_reap_predicate_idx ON invoice(status, settled_at, expires_at, issued_at);
CREATE INDEX IF NOT EXISTS invoice_subscription_id_idx ON invoice(subscription_id);
CREATE INDEX IF NOT EXISTS event_log_at_kind_idx ON event_log(at, kind);
CREATE INDEX IF NOT EXISTS instance_updated_at_idx ON instance(updated_at);
CREATE INDEX IF NOT EXISTS instance_subscription_id_idx ON instance(subscription_id);
CREATE INDEX IF NOT EXISTS subscription_state_updated_at_idx ON subscription(state, updated_at);
-- The sub reap correlates on each open-obligation table (codex P2); back those guards too so a large
-- flood-grown outbox (SENT rows are not pruned) or dead-letter set can't force a per-candidate scan.
CREATE INDEX IF NOT EXISTS outbox_subscription_state_idx ON outbox(subscription_id, state);
CREATE INDEX IF NOT EXISTS teardown_failure_subscription_idx ON teardown_failure(subscription_id, resolved_at);
CREATE INDEX IF NOT EXISTS native_connect_session_subscription_idx ON native_connect_session(subscription_id, state);
";

/// Ordered migrations (lnrent-7fp.3): index `i` upgrades the DB from schema version `i` to
/// `i+1`. Version 1 is the §11 schema; version 2 adds `seen_message` (lnrent-7fp.5); version 3 adds
/// `subscription.suspend_not_before` (lnrent-7fp.22); version 4 adds the `refund_attempt` resolver
/// columns (lnrent-ug8); version 5 adds the idempotency-cache TTL-sweep indexes (lnrent-xjn);
/// version 6 adds the event_log scan indexes; version 7 adds teardown failures; version 8 adds
/// operator sweeps; version 9 adds terminal-row reaper indexes. A future
/// schema change appends a new entry of `ALTER`/`CREATE` statements; **never edit a shipped migration**.
const MIGRATIONS: &[&str] = &[
    SCHEMA,
    M2_SEEN_MESSAGE,
    M3_SUSPEND_NOT_BEFORE,
    M4_REFUND_RESOLUTION,
    M5_IDEMPOTENCY_CACHE_INDEXES,
    M6_EVENT_LOG_INDEXES,
    M7_TEARDOWN_FAILURE,
    M8_SWEEP_ATTEMPT,
    M9_TERMINAL_ROW_REAPER_INDEXES,
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
    let path = path.as_ref();
    if let Ok(meta) = path.metadata() {
        if meta.is_file() && meta.len() == 0 {
            return Err(anyhow!(
                "state DB file exists but is empty for {}; refusing to initialize a new database \
                 over possible truncation/data loss (remove it only for a deliberate first run, or \
                 restore from backup)",
                path.display()
            ));
        }
    }
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
    // Integrity gate (lnrent-y4m.3): a corrupt/truncated state DB must fail startup LOUDLY here
    // rather than surface as a late opaque error on the money path. `quick_check` is the cheap
    // structural scan (it skips `integrity_check`'s expensive index-vs-table cross-check); it yields
    // exactly one row `"ok"` on a healthy DB, or one-or-more error-description rows otherwise, so the
    // first row alone is the verdict.
    let verdict: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
    if verdict != "ok" {
        return Err(anyhow!(
            "state DB integrity check failed for {}: {verdict}",
            path.display()
        ));
    }
    migrate(&conn)?;
    Ok(conn)
}

/// Classify a `rusqlite::Error` as the fatal "can no longer durably write" family — disk-full,
/// corruption, IO, or a read-only filesystem — the errors that mean the daemon must stop attempting
/// money writes (lnrent-y4m.3). EVERYTHING else returns false: a `ConstraintViolation`, a
/// `QueryReturnedNoRows`, a CAS miss, `SQLITE_BUSY`/`SQLITE_LOCKED` (transient contention), or any
/// non-`SqliteFailure` error is an ordinary business outcome and must NOT latch the store degraded
/// (that would take a healthy daemon offline).
///
/// NOTE: a fatal error raised INSIDE a `transaction()` closure is only classifiable if it reaches
/// here as a real `rusqlite::Error` — money-write closures MUST propagate store errors with `?`
/// (which preserves the concrete error via `anyhow`'s `From`), NOT stringify them with
/// `.map_err(|e| anyhow!("…: {e}"))`, or the `downcast_ref` in `trip_if_fatal_anyhow` cannot see it.
/// All current money closures use `?`; the commit itself would also fault on the same bad disk.
///
/// Variant names verified against libsqlite3-sys 0.28 (rusqlite 0.31): `SQLITE_FULL`→`DiskFull`,
/// `SQLITE_CORRUPT`→`DatabaseCorrupt`, `SQLITE_NOTADB`→`NotADatabase`, `SQLITE_IOERR`→
/// `SystemIoFailure` (the whole IOERR family maps to this one primary code), `SQLITE_READONLY`→
/// `ReadOnly` (a disk remounted read-only after IO errors — a real "cannot write" degradation).
fn is_fatal_db_error(e: &rusqlite::Error) -> bool {
    match e {
        rusqlite::Error::SqliteFailure(ffi_err, _) => matches!(
            ffi_err.code,
            rusqlite::ErrorCode::DiskFull
                | rusqlite::ErrorCode::DatabaseCorrupt
                | rusqlite::ErrorCode::NotADatabase
                | rusqlite::ErrorCode::SystemIoFailure
                | rusqlite::ErrorCode::ReadOnly
        ),
        _ => false,
    }
}

/// Trip the latching degraded flag and log the transition prominently. The `tracing::error!` IS the
/// operator's out-of-band signal: a store-backed alert cannot deliver once writes are refused (its
/// own outbox enqueue is a `transaction()`), so the log line is the honest channel (lnrent-y4m.3).
fn latch_degraded(degraded: &AtomicBool, err: impl std::fmt::Display) {
    degraded.store(true, Ordering::Release);
    tracing::error!(
        db_error = %err,
        "FATAL DB error on a money write: store LATCHED to degraded read-only mode. Money writes \
         are now refused (reads/status still served). Restore the DB from backup and restart to clear."
    );
}

/// Trip degraded if a `rusqlite::Error` surfaced by `conn.transaction()` / `txn.commit()` is fatal,
/// then convert it to `anyhow` (as before). Non-fatal errors pass through untouched.
fn trip_if_fatal_sqlite(degraded: &AtomicBool, e: rusqlite::Error) -> anyhow::Error {
    if is_fatal_db_error(&e) {
        latch_degraded(degraded, &e);
    }
    anyhow::Error::new(e)
}

/// Trip degraded if a fatal `rusqlite::Error` is hiding inside the `anyhow::Error` a write raised
/// INSIDE the closure (`f(&txn)` yields `anyhow::Result`, so the concrete sqlite error arrives
/// downcast-able). The original `anyhow` error is returned unchanged so caller context is preserved.
fn trip_if_fatal_anyhow(degraded: &AtomicBool, e: anyhow::Error) -> anyhow::Error {
    if let Some(sqlite_err) = e.downcast_ref::<rusqlite::Error>() {
        if is_fatal_db_error(sqlite_err) {
            latch_degraded(degraded, &e);
        }
    }
    e
}

/// A unit of work the store actor runs on its `Connection`. Each job does its sqlite work
/// synchronously and sends the typed reply on its own oneshot.
type Job = Box<dyn FnOnce(&mut Connection) + Send>;

/// Cloneable handle to the sole-writer store actor (ADR-0001). All access serializes through
/// the one actor task that owns the `Connection`.
#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Job>,
    /// Latching read-only guard (lnrent-y4m.3). Tripped once a fatal DB error (disk-full /
    /// corruption / IO) is seen on a money commit; from then on `transaction()` refuses writes for
    /// the process lifetime while `read()`/status keep serving. `#[derive(Clone)]` shares this `Arc`
    /// across every handle, and the sole-writer actor closure is the only setter. Recovery is the
    /// operator's restore-then-restart (a restart re-runs `open()`'s `quick_check`) — no auto-clear,
    /// no background retry, no clear-degraded command, by design.
    degraded: Arc<AtomicBool>,
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
        Store {
            tx,
            degraded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Open the DB (WAL + schema) and spawn the actor in one step.
    pub fn open_spawn(path: impl AsRef<Path>) -> Result<Store> {
        Ok(Store::spawn(open(path)?))
    }

    /// Whether the store has latched into degraded read-only mode after a fatal DB error
    /// (disk-full / corruption / IO): money writes are being refused while reads/status still serve
    /// (lnrent-y4m.3). Surfaced in the operator `Request::Money` readiness report so a status poll —
    /// not just the daemon log — reveals it (an agent-native operator polls status, not journald).
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Run `f` inside ONE transaction: **commit** if it returns `Ok`, **roll back** if it
    /// returns `Err`. This is how the handshake gets its atomic multi-row transitions.
    pub async fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Transaction) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        // Latching read-only guard (lnrent-y4m.3): the store write choke point used by the money
        // core (and maintenance writes). This closure runs serially on the sole-writer actor — the
        // authoritative serialization point — so the load/store on `degraded` need no extra lock,
        // and `read()` uses `run()` directly and stays un-gated so reads/status keep serving
        // degraded.
        let degraded = self.degraded.clone();
        self.run(move |conn| {
            // Refuse BEFORE opening a txn so a refused write cannot partially apply.
            if degraded.load(Ordering::Acquire) {
                return Err(anyhow!(
                    "store is in degraded read-only mode after a fatal DB error; money writes are \
                     refused — restore from backup and restart"
                ));
            }
            // A fatal disk-full/corruption/IO error from ANY of the three sqlite steps trips the
            // latch so no further money write is attempted against a DB we cannot durably write; a
            // business `Err` (CAS miss, constraint, no-rows) propagates WITHOUT tripping. On the
            // fatal path the txn is never committed (aborted here or dropped), so atomicity holds.
            let txn = conn
                .transaction()
                .map_err(|e| trip_if_fatal_sqlite(&degraded, e))?;
            let out = f(&txn).map_err(|e| trip_if_fatal_anyhow(&degraded, e))?;
            txn.commit().map_err(|e| trip_if_fatal_sqlite(&degraded, e))?;
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

    /// Reap terminal, unreferenced business rows past [`TERMINAL_ROW_RETENTION_SECS`] (lnrent-y4m.2,
    /// GATE-1 PR-10). Companion to [`Store::prune_idempotency_caches`]: that trims the idempotency
    /// caches; this trims the reaper-safe business rows (`reservation`/`invoice`/`event_log`/`instance`/
    /// `subscription`) that only flip to terminal state and then persist FOREVER. Money rows are never
    /// touched — see MONEY-SAFETY below for exactly which invoices/journals/subscriptions are kept.
    ///
    /// ONE transaction, deleting children before the parent (the schema declares no
    /// `ON DELETE CASCADE`). The `subscription` is removed LAST and only once it has NO surviving money
    /// child (`invoice`/`event_log`/`instance`/`reservation`), NO open refund, and NO open operational
    /// obligation (unresolved `teardown_failure`, PENDING `outbox`, ACTIVE `native_connect_session`), so
    /// a reap never strands one. (Closed operational back-rows may dangle harmlessly — see the
    /// `subscription_sql` note.) Every
    /// predicate
    /// gates on the clock-capped cutoff `< MIN(unixepoch(), ?1) - ?2`, so a synthetic-clock test or a
    /// daemon clock running AHEAD cannot over-prune (same idiom as `prune_idempotency_caches`).
    ///
    /// MONEY-SAFETY (INV-2/INV-3, verified against `capture.rs` + `ledger.rs`): a row the money core
    /// counts, or may still need to route a late settlement, is NEVER reaped:
    ///   * A *settled* invoice (`status='PAID' OR settled_at IS NOT NULL`) is a Class-A receipt and a
    ///     `settle_*_refund` `event_log` row is a Class-B receipt (`ledger::sum_receipts_msat`); the
    ///     `refund_attempt`/`sweep_attempt` rows that offset them are never reaped.
    ///   * A never-settled `EXPIRED` invoice IS reaped past the window, regardless of its owning sub's
    ///     state (an unpaid `request_id`/`renew.request` flood mints one invoice per order OR renewal,
    ///     and expired renewals accrue on long-lived ACTIVE subs — so gating on sub-liveness would leak
    ///     the renewal flood). The retention window is itself the "settlement can no longer arrive"
    ///     proof: a lightning/fedimint invoice cannot settle after its own expiry; the only "late"
    ///     delivery is oplog-replay of a *pre-expiry* settlement (a multi-week outage), and even then
    ///     `capture_txn`'s unmatched branch records a single refund intent rather than swallow money
    ///     (`capture.rs`) — conserved as a parked+alerted refund, not lost. Fail closed: a settled
    ///     invoice (Class-A receipt) and an invoice behind an open refund both stay.
    ///   * A `subscription` still referenced by a non-terminal (`status <> 'SENT'`) `refund_attempt` is
    ///     money owed and is kept. Fail closed: unsure ⇒ keep.
    ///
    /// The rows this method deletes are the safe subset that cannot route or prove money: released
    /// holds, destroyed/orphaned instances, non-money+resolved journals, and the invoice+subscription
    /// pair of a fully-lapsed unpaid order past the retention window.
    pub async fn reap_terminal_rows(&self, now: i64) -> Result<ReapCounts> {
        // The external_id a still-open (non-SENT) refund_attempt is keyed to — the SAME derivation
        // `load_refund_attempt_external_ids` uses. An invoice referenced by such a row is money owed
        // and is kept even past the window. The set is filtered `ext IS NOT NULL` so the outer `NOT IN`
        // is exact: a TEXT PRIMARY KEY is NOT implicitly NOT-NULL in SQLite (unlike INTEGER/WITHOUT
        // ROWID/STRICT — CodeRabbit), so the `ELSE ra.id` branch could in principle be NULL, and a
        // single NULL in a `NOT IN` set makes the predicate UNKNOWN for every row — suppressing all
        // deletes (fail-closed, but it would stall the GC). Filtering keeps the efficient set-based form
        // (built once, vs a per-candidate correlated scan).
        let open_refund_ext = "SELECT ext FROM (
                    SELECT CASE
                      WHEN ra.idempotency_key LIKE 'refund:%' THEN substr(ra.idempotency_key, 8)
                      WHEN ra.id LIKE 'ref-%' THEN substr(ra.id, 5)
                      ELSE ra.id
                    END AS ext
                    FROM refund_attempt ra
                    WHERE ra.status <> 'SENT'
                  ) WHERE ext IS NOT NULL";
        // A never-settled EXPIRED invoice past the window is reaped REGARDLESS of its owning sub's
        // state. The retention window itself is the "settlement can no longer arrive" proof — a
        // lightning/fedimint invoice cannot settle after its own expiry, and 30d dwarfs any oplog-replay
        // horizon — so no sub-liveness gate is needed. Gating on one would in fact leak the RENEWAL
        // flood: an ACTIVE/SUSPENDED sub accrues one `kind='renewal'` invoice per `renew.request`
        // (order_intake.rs), and an expired unpaid renewal on a long-lived sub would then never reap,
        // growing `invoice` without bound (codex P2). KEPT: a settled invoice (Class-A receipt, via
        // `settled_at IS NULL`) and an invoice behind an open refund (money owed, via the open-refund
        // guard). If the (impossible) late settlement of a reaped invoice still lands, capture's
        // unmatched branch conserves it as a parked refund — money is never swallowed.
        let invoice_sql = format!(
            "DELETE FROM invoice
              WHERE status = 'EXPIRED' AND settled_at IS NULL
                AND COALESCE(expires_at, issued_at) < MIN(unixepoch(), ?1) - ?2
                AND external_id NOT IN ({open_refund_ext})"
        );
        // Every settle-refund journal row is a Class-B ledger receipt (and backs refund readiness), so
        // it is kept. Non-money journals are reaped once past the window except durable recovery
        // markers that are still unresolved: provision cleanup retry intents and RESUMING renewal
        // baselines. Those rows are active state until their matching done/resolution journal exists.
        let event_log_sql = format!(
            r#"DELETE FROM event_log
              WHERE at < MIN(unixepoch(), ?1) - ?2
                AND kind NOT IN ({SETTLE_REFUND_KINDS_SQL})
                AND NOT (
                      kind = 'provision_cleanup_pending'
                  AND EXISTS (
                        SELECT 1 FROM subscription s
                         WHERE s.id = event_log.subscription_id
                      )
                  AND NOT EXISTS (
                        SELECT 1 FROM event_log d
                         WHERE d.subscription_id = event_log.subscription_id
                           AND d.kind = 'provision_cleanup_done'
                           AND d.detail_json = ('{{"pending_event_id":' || event_log.id || '}}')
                      )
                )
                AND NOT (
                      kind = 'renew_resume'
                  AND EXISTS (
                        SELECT 1 FROM subscription s
                         WHERE s.id = event_log.subscription_id
                           AND s.state = 'RESUMING'
                      )
                  AND NOT EXISTS (
                        SELECT 1 FROM event_log r
                         WHERE r.subscription_id = event_log.subscription_id
                           AND r.kind IN ('resume_active', 'resume_failed')
                           AND r.id > event_log.id
                      )
                )"#
        );
        // A reservation has no terminal timestamp (a paid hold stays CONSUMED for the sub's whole life
        // and only flips to RELEASED at terminate), so `created_at` alone would delete a hold in the
        // same tick it is freed. Measure its retention against its owning subscription's terminal
        // window instead (`order_id == subscription.id`): reap a RELEASED hold only once its sub is
        // itself reapable (terminal AND past the window) — or has no sub row at all (a failed order's
        // abandoned hold, reaped on its own `created_at`). RELEASED is the only terminal reservation
        // state (the schema writes HELD | CONSUMED | RELEASED).
        let reservation_sql = "DELETE FROM reservation
                  WHERE state = 'RELEASED'
                    AND created_at < MIN(unixepoch(), ?1) - ?2
                    AND NOT EXISTS (
                         SELECT 1 FROM subscription s
                          WHERE s.id = reservation.order_id
                            AND ( s.state NOT IN ('EXPIRED', 'TERMINATED', 'REFUNDED')
                               OR s.updated_at >= MIN(unixepoch(), ?1) - ?2 )
                        )";
        // An `instance` (the durable record of a provisioned box) has no terminal timestamp of its own
        // either: `fire_destroy` terminates the sub and releases the hold but never rewrites the
        // instance row, which stays 'RUNNING' for the sub's whole life (reconcile.rs). Gating on an
        // instance state the terminate path never sets ('DESTROYED') would reap NOTHING and leave the
        // `NOT EXISTS instance` guard below permanently blocking a provisioned-then-terminated sub. So
        // measure retention against the OWNING subscription's terminal window, exactly like a
        // reservation: reap an instance only once its sub is itself reapable (terminal AND past the
        // window), or has no sub row at all (an orphan, reaped on its own `updated_at`). A live sub's
        // instance is always kept.
        let instance_sql = "DELETE FROM instance
                  WHERE updated_at < MIN(unixepoch(), ?1) - ?2
                    AND NOT EXISTS (
                         SELECT 1 FROM subscription s
                          WHERE s.id = instance.subscription_id
                            AND ( s.state NOT IN ('EXPIRED', 'TERMINATED', 'REFUNDED')
                               OR s.updated_at >= MIN(unixepoch(), ?1) - ?2 )
                        )";
        // Parent last: a terminal sub is reaped only once it has NO surviving MONEY row, RECOVERY
        // journal, or OPEN OBLIGATION. A kept receipt-invoice or settle-refund journal blocks the
        // delete, so the sub's money-safety falls out of the child guards; the explicit refund_attempt
        // guard is belt-and-suspenders. Beyond money we also refuse to reap while an operational
        // obligation is still open — an unresolved `teardown_failure` (provider cleanup owed), a PENDING
        // `outbox` DM (undelivered buyer message), or an ACTIVE `native_connect_session` (live ticket) —
        // so a reap never strands one. (A CLOSED operational back-ref — a SENT refund/outbox row, a
        // resolved dead-letter, a REVOKED session — may still dangle after the sub is gone; the schema
        // declares no FKs and every such row is read directly by its own id/state, never via a join to
        // `subscription`, so a dangling `subscription_id` is harmless. Reaping those closed rows is a
        // separate GC and out of this bead's flood-bounding scope.)
        let subscription_sql = "DELETE FROM subscription
              WHERE state IN ('EXPIRED', 'TERMINATED', 'REFUNDED')
                AND updated_at < MIN(unixepoch(), ?1) - ?2
                AND NOT EXISTS (SELECT 1 FROM invoice     i WHERE i.subscription_id = subscription.id)
                AND NOT EXISTS (SELECT 1 FROM event_log   e WHERE e.subscription_id = subscription.id)
                AND NOT EXISTS (SELECT 1 FROM instance    n WHERE n.subscription_id = subscription.id)
                AND NOT EXISTS (SELECT 1 FROM reservation r WHERE r.order_id = subscription.id)
                AND NOT EXISTS (SELECT 1 FROM refund_attempt ra
                                 WHERE ra.subscription_id = subscription.id
                                   AND ra.status <> 'SENT')
                AND NOT EXISTS (SELECT 1 FROM teardown_failure t
                                 WHERE t.subscription_id = subscription.id
                                   AND t.resolved_at IS NULL)
                AND NOT EXISTS (SELECT 1 FROM outbox o
                                 WHERE o.subscription_id = subscription.id
                                   AND o.state = 'PENDING')
                AND NOT EXISTS (SELECT 1 FROM native_connect_session ncs
                                 WHERE ncs.subscription_id = subscription.id
                                   AND ncs.state = 'ACTIVE')";

        self.transaction(move |tx| {
            // Children first (FK-safe; the schema has no ON DELETE CASCADE).
            let instance =
                tx.execute(instance_sql, rusqlite::params![now, TERMINAL_ROW_RETENTION_SECS])?;
            let reservation =
                tx.execute(reservation_sql, rusqlite::params![now, TERMINAL_ROW_RETENTION_SECS])?;
            let invoice =
                tx.execute(&invoice_sql, rusqlite::params![now, TERMINAL_ROW_RETENTION_SECS])?;
            let event_log =
                tx.execute(&event_log_sql, rusqlite::params![now, TERMINAL_ROW_RETENTION_SECS])?;
            let subscription = tx.execute(
                subscription_sql,
                rusqlite::params![now, TERMINAL_ROW_RETENTION_SECS],
            )?;
            Ok(ReapCounts {
                reservation,
                invoice,
                event_log,
                instance,
                subscription,
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
                    -- MAX (not a bare grouped column): a REDELIVERED settlement re-journals the same
                    -- external_id, so aggregate deterministically — a NULL/malformed duplicate must
                    -- not zero out the real gross and understate `required_msat` (false coverage).
                    MAX(CAST(json_extract(detail_json, '$.amount_sat') AS INTEGER)) AS amount_sat
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
                -- MAX for deterministic dedup across a redelivered settlement (see the refund CTE
                -- above): a NULL/malformed duplicate must not understate this unreconciled liability.
                MAX(CAST(json_extract(e.detail_json, '$.amount_sat') AS INTEGER)) AS amount_sat
           FROM event_log e
           LEFT JOIN subscription s ON s.id = e.subscription_id
          WHERE e.kind IN ({SETTLE_REFUND_KINDS_SQL})
            AND COALESCE(s.state, '') NOT IN ('ACTIVE', 'REFUNDED')
          GROUP BY external_id
          ORDER BY external_id"
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
        // 15 §11 tables + `teardown_failure` (lnrent-urw.2) + `sweep_attempt` (gate1-operator-sweep,
        // urw.3). `seen_message` is migration-only (M2).
        assert_eq!(n, 17);
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
        // The §11 schema (15 tables) plus `seen_message` (migration 2, lnrent-7fp.5) plus
        // `teardown_failure` (SCHEMA + migration 7, lnrent-urw.2) plus `sweep_attempt` (SCHEMA +
        // migration 8, gate1-operator-sweep, urw.3).
        assert_eq!(n, 18);
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

    // lnrent-y4m.2: the terminal-row reaper runs every reconcile tick on tables that can be
    // attacker-grown by distinct request IDs, so its age/status predicates need indexes just like the
    // idempotency-cache sweep does.
    #[test]
    fn terminal_row_reaper_indexes_exist_after_migrate() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        for idx in [
            "reservation_state_created_at_idx",
            "invoice_reap_predicate_idx",
            "invoice_subscription_id_idx",
            "event_log_at_kind_idx",
            "instance_updated_at_idx",
            "instance_subscription_id_idx",
            "subscription_state_updated_at_idx",
            "outbox_subscription_state_idx",
            "teardown_failure_subscription_idx",
            "native_connect_session_subscription_idx",
        ] {
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
            // A real v3 DB has all the v1 base tables; include the ones later migrations index so
            // the partial-M4 recovery still applies the full migration chain to current.
            "CREATE TABLE refund_attempt (id TEXT PRIMARY KEY, status TEXT, resolved_bolt11 TEXT);
             CREATE TABLE op_invocation (sender_pubkey TEXT, request_id TEXT, state TEXT, finished_at INTEGER);
             CREATE TABLE inbound_request (sender_pubkey TEXT, request_id TEXT, created_at INTEGER);
             CREATE TABLE event_log (id INTEGER PRIMARY KEY AUTOINCREMENT, subscription_id TEXT,
                                     kind TEXT, detail_json TEXT, at INTEGER);
             CREATE TABLE reservation (id TEXT PRIMARY KEY, order_id TEXT, state TEXT, created_at INTEGER);
             CREATE TABLE invoice (id TEXT PRIMARY KEY, subscription_id TEXT, external_id TEXT,
                                   status TEXT, settled_at INTEGER, expires_at INTEGER, issued_at INTEGER);
             CREATE TABLE instance (id TEXT PRIMARY KEY, subscription_id TEXT, state TEXT, updated_at INTEGER);
             CREATE TABLE subscription (id TEXT PRIMARY KEY, state TEXT, updated_at INTEGER);
             CREATE TABLE outbox (id TEXT PRIMARY KEY, subscription_id TEXT, state TEXT);
             CREATE TABLE native_connect_session (id TEXT PRIMARY KEY, subscription_id TEXT, state TEXT);
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

    // lnrent-y4m.2: the terminal-row reaper deletes only terminal, past-window rows; live/non-terminal
    // rows and the row sitting EXACTLY at the cutoff (predicate is strict `<`) survive. Re-running is a
    // no-op (idempotent).
    #[tokio::test]
    async fn reap_deletes_only_terminal_rows_past_window() {
        let s = mem_store();
        // A synthetic clock well below real unixepoch(), so MIN(unixepoch(), now) == now (same idiom
        // as the prune test): cutoff = now - RETENTION = TERMINAL_ROW_RETENTION_SECS.
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let cutoff = TERMINAL_ROW_RETENTION_SECS;
        let old = cutoff - 1; // strictly past the window -> reaped
        let boundary = cutoff; // EXACTLY at the cutoff -> kept (strict `<`)
        let recent = now; // within retention -> kept

        s.transaction(move |tx| {
            // reservation: old-released (reaped), boundary-released (kept, strict `<`),
            // old-held (kept, not terminal), recent-released (kept, within window).
            tx.execute(
                "INSERT INTO reservation (id, order_id, state, created_at) VALUES
                   ('r-old',      'o-r-old',      'RELEASED', ?1),
                   ('r-boundary', 'o-r-boundary', 'RELEASED', ?2),
                   ('r-held',     'o-r-held',     'HELD',     ?1),
                   ('r-recent',   'o-r-recent',   'RELEASED', ?3)",
                rusqlite::params![old, boundary, recent],
            )?;
            // invoice: an old never-settled EXPIRED invoice with no live owning sub is a fully-lapsed
            // unpaid order -> reaped. A recent EXPIRED invoice is within the window -> kept. A settled
            // invoice (settled_at set) is a Class-A ledger receipt -> kept even past the window. (The
            // owning-subscription window + open-refund guards are exercised in dedicated tests below.)
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at, settled_at) VALUES
                   ('i-old-exp',     NULL, 'x-old-exp',     'order', 'EXPIRED', ?1, ?1, NULL),
                   ('i-recent-exp',  NULL, 'x-recent-exp',  'order', 'EXPIRED', ?2, ?2, NULL),
                   ('i-old-settled', NULL, 'x-old-settled', 'order', 'EXPIRED', ?1, ?1, ?1)",
                rusqlite::params![old, recent],
            )?;
            // event_log: old audit row (reaped), recent audit row (kept), old settle-refund journal
            // (kept — a Class-B ledger receipt / refund-readiness source).
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES
                   (NULL, 'order_placed',            '{}', ?1),
                   (NULL, 'order_placed',            '{}', ?2),
                   ('s',  'settle_unmatched_refund', '{\"external_id\":\"x-refund\",\"amount_sat\":1}', ?1)",
                rusqlite::params![old, recent],
            )?;
            // instance: an instance has no terminal timestamp of its own, so an orphan (no owning sub)
            // is reaped on its own `updated_at` — old (reaped), boundary (kept, strict `<`), recent
            // (kept). The owning-subscription window is exercised in a dedicated test below.
            tx.execute(
                "INSERT INTO instance (id, subscription_id, state, updated_at) VALUES
                   ('n-old',      NULL, 'RUNNING', ?1),
                   ('n-boundary', NULL, 'RUNNING', ?2),
                   ('n-recent',   NULL, 'RUNNING', ?3)",
                rusqlite::params![old, boundary, recent],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped,
            ReapCounts {
                reservation: 1,
                invoice: 1,
                event_log: 1,
                instance: 1,
                subscription: 0,
            }
        );
        assert_eq!(reaped.total(), 4);

        // Boundary: the row stamped EXACTLY at the cutoff survives (strict `<`).
        assert_eq!(count(&s, "SELECT count(*) FROM reservation WHERE id='r-boundary'").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM reservation WHERE id='r-old'").await, 0);
        assert_eq!(
            count(&s, "SELECT count(*) FROM reservation WHERE id='r-held'").await,
            1,
            "non-terminal (HELD) reservation kept"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM reservation WHERE id='r-recent'").await, 1);

        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-old-exp'").await,
            0,
            "a fully-lapsed unpaid EXPIRED invoice (no live sub) is reaped past the window"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-recent-exp'").await,
            1,
            "an EXPIRED invoice within the retention window is kept"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-old-settled'").await,
            1,
            "a settled invoice is a ledger receipt — kept"
        );

        assert_eq!(
            count(&s, &format!("SELECT count(*) FROM event_log WHERE kind='order_placed' AND at={old}")).await,
            0,
            "old audit journal row reaped"
        );
        assert_eq!(
            count(&s, &format!("SELECT count(*) FROM event_log WHERE kind='order_placed' AND at={recent}")).await,
            1
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM event_log WHERE kind='settle_unmatched_refund'").await,
            1,
            "settle-refund journal row kept even past the window"
        );

        assert_eq!(count(&s, "SELECT count(*) FROM instance WHERE id='n-old'").await, 0);
        assert_eq!(
            count(&s, "SELECT count(*) FROM instance WHERE id='n-boundary'").await,
            1,
            "instance stamped EXACTLY at the cutoff survives (strict `<`)"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM instance WHERE id='n-recent'").await, 1);

        assert_eq!(
            s.reap_terminal_rows(now).await.unwrap(),
            ReapCounts::default(),
            "re-running the reap is idempotent"
        );
    }

    // lnrent-y4m.2: reaping a `subscription` is child-safe (never orphans an invoice/event_log/
    // instance) AND money-safe (a settled-invoice receipt or an open refund keeps the whole sub). A
    // fully-lapsed unpaid order is reaped whole — its released hold, non-money journals, EXPIRED
    // invoice, and the childless terminal sub all go in one FK-safe transaction (invoice before sub).
    #[tokio::test]
    async fn reap_of_subscription_is_child_and_money_safe() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;
        let recent = now;

        s.transaction(move |tx| {
            // sub-flood: a fully-lapsed unpaid order (EXPIRED sub past window). Its released reservation,
            // non-money journals, EXPIRED invoice, and the childless sub are ALL reaped in one txn.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-flood', 'EXPIRED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at)
                 VALUES ('i-flood', 'sub-flood', 'x-flood', 'order', 'EXPIRED', ?1, ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO reservation (id, order_id, state, created_at)
                 VALUES ('res-flood', 'sub-flood', 'RELEASED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES
                   ('sub-flood', 'order_placed', '{}', ?1),
                   ('sub-flood', 'reconcile_order_expired', '{}', ?1)",
                rusqlite::params![old],
            )?;
            // sub-paid: TERMINATED but with a PAID (settled) invoice receipt -> invoice kept -> the sub
            // is kept too (reaping it would orphan the receipt).
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-paid', 'TERMINATED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, issued_at, settled_at)
                 VALUES ('i-paid', 'sub-paid', 'x-paid', 'order', 'PAID', ?1, ?1)",
                rusqlite::params![old],
            )?;
            // sub-owed: REFUNDED but a non-SENT refund_attempt still references it (money owed) -> kept.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-owed', 'REFUNDED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO refund_attempt (id, subscription_id, idempotency_key, status, created_at, updated_at)
                 VALUES ('ref-owed', 'sub-owed', 'refund:x-owed', 'PENDING', ?1, ?1)",
                rusqlite::params![old],
            )?;
            // sub-recent: terminal but within retention -> kept. sub-active: not terminal -> kept.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES
                   ('sub-recent', 'EXPIRED', ?1),
                   ('sub-active', 'ACTIVE',  ?2)",
                rusqlite::params![recent, old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped,
            ReapCounts {
                reservation: 1,
                invoice: 1,
                event_log: 2,
                instance: 0,
                subscription: 1,
            },
            "the fully-lapsed flood order is reaped whole (hold + journals + invoice + sub)"
        );

        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-flood'").await,
            0,
            "the childless terminal flood sub is reaped (invoice deleted first, same txn)"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE subscription_id='sub-flood'").await,
            0,
            "the fully-lapsed order's EXPIRED invoice is reaped"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM reservation WHERE order_id='sub-flood'").await,
            0,
            "released reservation reaped"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM event_log WHERE subscription_id='sub-flood'").await,
            0,
            "old non-money journal rows reaped"
        );

        // Money-safe / orphan-safe subs kept.
        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-paid'").await,
            1,
            "a receipt invoice blocks the sub reap (no orphan)"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM invoice WHERE id='i-paid'").await, 1);
        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-owed'").await,
            1,
            "an open (non-SENT) refund => money owed => sub kept"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-recent'").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-active'").await, 1);

        // FK/orphan integrity: no surviving invoice/event_log/instance points to a missing sub.
        let orphans = count(
            &s,
            "SELECT
               (SELECT count(*) FROM invoice   i WHERE i.subscription_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM subscription s WHERE s.id=i.subscription_id))
             + (SELECT count(*) FROM event_log e WHERE e.subscription_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM subscription s WHERE s.id=e.subscription_id))
             + (SELECT count(*) FROM instance  n WHERE n.subscription_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM subscription s WHERE s.id=n.subscription_id))",
        )
        .await;
        assert_eq!(orphans, 0, "no orphaned child rows after reap");
    }

    // lnrent-y4m.2: a reservation has no terminal timestamp of its own, so it is reaped against its
    // OWNING subscription's terminal window — a hold freed at a recent terminate is NOT deleted in the
    // same tick, and a still-CONSUMED hold keeps its (otherwise reapable) sub from being reaped.
    #[tokio::test]
    async fn reap_reservation_follows_owning_subscription_window() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;
        let recent = now;

        s.transaction(move |tx| {
            // sub-fresh-term: TERMINATED only RECENTLY, but its hold was created long ago and just
            // RELEASED — the hold must survive (retention runs from the sub's terminal window).
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-fresh-term', 'TERMINATED', ?1)",
                rusqlite::params![recent],
            )?;
            tx.execute(
                "INSERT INTO reservation (id, order_id, state, created_at)
                 VALUES ('res-fresh', 'sub-fresh-term', 'RELEASED', ?1)",
                rusqlite::params![old],
            )?;
            // sub-consumed: reapable (EXPIRED + past window) but its hold is still CONSUMED — reaping the
            // sub would orphan it, so BOTH are kept.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-consumed', 'EXPIRED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO reservation (id, order_id, state, created_at)
                 VALUES ('res-consumed', 'sub-consumed', 'CONSUMED', ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped,
            ReapCounts::default(),
            "nothing reaped: one sub too recent, one hold still consumed"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM reservation WHERE id='res-fresh'").await,
            1,
            "a hold freed at a recent terminate survives its retention window"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-consumed'").await,
            1,
            "a still-CONSUMED hold keeps its sub (no orphan)"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM reservation WHERE id='res-consumed'").await, 1);
    }

    // lnrent-y4m.2: an instance has no terminal timestamp of its own — `fire_destroy` terminates the
    // sub but leaves the instance 'RUNNING' — so it is reaped against its OWNING subscription's
    // terminal window, exactly like a reservation. A live sub's instance is kept; a recently-terminated
    // sub's instance is kept; and a past-window terminated sub's instance is reaped, which then lets the
    // now-childless sub be reaped in the same tick (the provisioned-then-terminated lifecycle the reaper
    // must finally bound — gating on a never-set 'DESTROYED' state reaped none of them).
    #[tokio::test]
    async fn reap_instance_follows_owning_subscription_window() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;
        let recent = now;

        s.transaction(move |tx| {
            // sub-live: ACTIVE — its RUNNING instance is a live box and must never be reaped.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-live', 'ACTIVE', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO instance (id, subscription_id, state, created_at, updated_at)
                 VALUES ('n-live', 'sub-live', 'RUNNING', ?1, ?1)",
                rusqlite::params![old],
            )?;
            // sub-fresh-term: TERMINATED only RECENTLY; its instance (created long ago, still 'RUNNING'
            // — the terminate path never rewrites it) must survive the sub's retention window.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-fresh-term', 'TERMINATED', ?1)",
                rusqlite::params![recent],
            )?;
            tx.execute(
                "INSERT INTO instance (id, subscription_id, state, created_at, updated_at)
                 VALUES ('n-fresh', 'sub-fresh-term', 'RUNNING', ?1, ?1)",
                rusqlite::params![old],
            )?;
            // sub-gone: TERMINATED and past the window, with only a still-'RUNNING' instance child. The
            // instance is reaped first, then the now-childless terminal sub.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-gone', 'TERMINATED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO instance (id, subscription_id, state, created_at, updated_at)
                 VALUES ('n-gone', 'sub-gone', 'RUNNING', ?1, ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped,
            ReapCounts {
                instance: 1,
                subscription: 1,
                ..ReapCounts::default()
            },
            "only the past-window terminated sub's instance — then the now-childless sub — is reaped"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM instance WHERE id='n-live'").await,
            1,
            "a live sub's RUNNING instance is never reaped"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM instance WHERE id='n-fresh'").await,
            1,
            "an instance of a recently-terminated sub survives the retention window"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM instance WHERE id='n-gone'").await, 0);
        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-gone'").await,
            0,
            "reaping the last child instance lets the childless terminal sub be reaped too"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-live'").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-fresh-term'").await, 1);

        // FK/orphan integrity: no surviving instance points to a missing sub.
        let orphans = count(
            &s,
            "SELECT count(*) FROM instance n WHERE n.subscription_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM subscription s WHERE s.id=n.subscription_id)",
        )
        .await;
        assert_eq!(orphans, 0, "no orphaned instance after reap");
    }

    // lnrent-y4m.2 review hardening: `event_log` also contains durable recovery journals, not only
    // audit rows. The reaper must keep unresolved cleanup/resume markers past the age window, then
    // may reap them once their matching resolution marker exists.
    #[tokio::test]
    async fn reap_preserves_unresolved_recovery_journals() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;

        s.transaction(move |tx| {
            tx.execute(
                "INSERT INTO subscription (id, recipe_id, state, updated_at) VALUES
                   ('sub-cleanup', 'recipe-a', 'REFUND_DUE', ?1),
                   ('sub-resume',  'recipe-a', 'RESUMING',   ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES
                   ('sub-cleanup', 'provision_cleanup_pending', '{\"handles\":\"lost\"}', ?1),
                   ('sub-resume',  'renew_resume',              '{}',                     ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped.event_log, 0,
            "unresolved recovery journals are still live retry state"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM event_log
                  WHERE kind IN ('provision_cleanup_pending', 'renew_resume')",
            )
            .await,
            2
        );

        let cleanup_id = count(
            &s,
            "SELECT id FROM event_log WHERE kind='provision_cleanup_pending'",
        )
        .await;
        let done_detail = format!("{{\"pending_event_id\":{cleanup_id}}}");
        s.transaction(move |tx| {
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES
                   ('sub-cleanup', 'provision_cleanup_done', ?1,   ?2),
                   ('sub-resume',  'resume_active',          '{}', ?2)",
                rusqlite::params![done_detail, old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped.event_log, 4,
            "resolved recovery journals are ordinary old non-money journals"
        );
        assert_eq!(
            count(
                &s,
                "SELECT count(*) FROM event_log
                  WHERE kind IN ('provision_cleanup_pending', 'provision_cleanup_done',
                                 'renew_resume', 'resume_active')",
            )
            .await,
            0
        );
    }

    // lnrent-y4m.2 MONEY-SAFETY: every settle-refund journal row is a ledger receipt / refund-
    // readiness source and is NEVER reaped. An EXPIRED-unsettled invoice behind a still-OPEN (non-SENT)
    // refund is kept (money owed); once that refund reaches SENT and no live sub remains, the now-fully-
    // resolved invoice is reapable — but the settle-refund receipt journals stay regardless.
    #[tokio::test]
    async fn reap_keeps_settle_refund_journal_and_invoices_behind_open_refunds() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;

        s.transaction(move |tx| {
            // Two settle-refund journal rows well past the window: one behind an OPEN (PENDING) refund,
            // one behind a SENT refund. Both are Class-B ledger receipts, so BOTH are kept.
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES
                   ('s-open', 'settle_terminal_refund', '{\"external_id\":\"ext-open\",\"amount_sat\":5}', ?1),
                   ('s-sent', 'settle_terminal_refund', '{\"external_id\":\"ext-sent\",\"amount_sat\":5}', ?1)",
                rusqlite::params![old],
            )?;
            // An EXPIRED-unsettled invoice referenced by an OPEN refund (money owed) must be kept.
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at)
                 VALUES ('i-open', 's-open', 'ext-open', 'order', 'EXPIRED', ?1, ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO refund_attempt (id, subscription_id, idempotency_key, status, created_at, updated_at) VALUES
                   ('ref-open', 's-open', 'refund:ext-open', 'PENDING', ?1, ?1),
                   ('ref-sent', 's-sent', 'refund:ext-sent', 'SENT',    ?1, ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped, ReapCounts::default(), "no money row is reaped while a liability is open");
        assert_eq!(
            count(&s, "SELECT count(*) FROM event_log WHERE kind LIKE 'settle\\_%' ESCAPE '\\'").await,
            2,
            "every settle-refund journal row (Class-B receipt) kept, open OR sent"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-open'").await,
            1,
            "invoice behind an OPEN refund kept (money owed)"
        );

        // Once the refund is SENT (no longer owed) and no live sub references it, the fully-resolved
        // EXPIRED-unsettled invoice is reapable — but the settle-refund journal receipts still stay.
        s.transaction(|tx| {
            tx.execute("UPDATE refund_attempt SET status='SENT' WHERE id='ref-open'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let reaped2 = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped2.invoice, 1, "the EXPIRED invoice is reaped once its refund is SENT and no live sub remains");
        assert_eq!(count(&s, "SELECT count(*) FROM invoice WHERE id='i-open'").await, 0);
        assert_eq!(
            count(&s, "SELECT count(*) FROM event_log WHERE kind LIKE 'settle\\_%' ESCAPE '\\'").await,
            2,
            "settle-refund journal rows (ledger receipts) kept even with all refunds SENT"
        );
    }

    // lnrent-y4m.2 (CodeRabbit): a NULL-derived open-refund key must NOT poison the `NOT IN` set and
    // stall the whole invoice GC. A TEXT PRIMARY KEY is not implicitly NOT-NULL in SQLite, so
    // refund_attempt.id CAN be NULL; the `ELSE ra.id` derivation would then be NULL. The set is filtered
    // IS NOT NULL, so an unrelated fully-lapsed invoice still reaps.
    #[tokio::test]
    async fn reap_invoice_gc_survives_a_null_derived_open_refund_key() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;

        s.transaction(move |tx| {
            // A non-SENT refund_attempt whose derived ext is NULL: NULL id + a non-'refund:' key, so the
            // CASE falls to `ELSE ra.id` = NULL. Without the IS NOT NULL filter this single NULL would
            // make `external_id NOT IN (…NULL…)` UNKNOWN for every invoice, suppressing all deletes.
            tx.execute(
                "INSERT INTO refund_attempt (id, idempotency_key, status, created_at, updated_at)
                 VALUES (NULL, 'legacy-non-refund-key', 'PENDING', ?1, ?1)",
                rusqlite::params![old],
            )?;
            // An unrelated fully-lapsed EXPIRED invoice that must still reap.
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at)
                 VALUES ('i-unrelated', NULL, 'x-unrelated', 'order', 'EXPIRED', ?1, ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped.invoice, 1, "a NULL-derived open-refund key does not stall the invoice GC");
        assert_eq!(count(&s, "SELECT count(*) FROM invoice WHERE id='i-unrelated'").await, 0);
    }

    // lnrent-y4m.2 (codex P2): an EXPIRED-unsettled invoice past the window is reaped REGARDLESS of its
    // owning sub's state — the window itself is the settlement-can't-arrive proof. A live ACTIVE sub's
    // expired RENEWAL invoices are reaped (else a renew.request flood grows `invoice` without bound)
    // while the live sub itself stays; a settled invoice and a within-window invoice are kept; a
    // recently-EXPIRED invoice is kept until it too is past the window.
    #[tokio::test]
    async fn reap_expired_invoices_regardless_of_sub_liveness() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;
        let recent = now;

        s.transaction(move |tx| {
            // sub-live: an ACTIVE (paid, running) sub that accrued unpaid renewal invoices — the renewal
            // flood. Its old EXPIRED renewal is reaped; a settled renewal (receipt) and a recently-
            // expired one are kept; the live sub itself is never reaped.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-live', 'ACTIVE', ?1)",
                rusqlite::params![old],
            )?;
            // sub-dead: terminal AND past window — its order invoice, then the childless sub, are reaped.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-dead', 'EXPIRED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at, settled_at) VALUES
                   ('i-renew-old',     'sub-live', 'x-renew-old',     'renewal', 'EXPIRED', ?1, ?1, NULL),
                   ('i-renew-recent',  'sub-live', 'x-renew-recent',  'renewal', 'EXPIRED', ?2, ?2, NULL),
                   ('i-renew-settled', 'sub-live', 'x-renew-settled', 'renewal', 'PAID',    ?1, ?1, ?1),
                   ('i-dead',          'sub-dead', 'x-dead',          'order',   'EXPIRED', ?1, ?1, NULL)",
                rusqlite::params![old, recent],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped.invoice, 2, "both fully-expired-past-window invoices reap (live-sub renewal + dead-sub order)");
        assert_eq!(reaped.subscription, 1, "only the terminal childless sub is reaped");
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-renew-old'").await,
            0,
            "an expired renewal invoice on a LIVE sub is reaped (closes the renewal flood)"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-renew-recent'").await,
            1,
            "a within-window renewal invoice is kept"
        );
        assert_eq!(
            count(&s, "SELECT count(*) FROM invoice WHERE id='i-renew-settled'").await,
            1,
            "a settled renewal invoice (Class-A receipt) is kept"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM invoice WHERE id='i-dead'").await, 0);
        assert_eq!(
            count(&s, "SELECT count(*) FROM subscription WHERE id='sub-live'").await,
            1,
            "the live ACTIVE sub is never reaped, even with all its old invoices gone"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-dead'").await, 0);
    }

    // lnrent-y4m.2: a terminal, past-window sub is NOT reaped while an OPEN operational obligation
    // still references it — an unresolved teardown_failure (provider cleanup owed), a PENDING outbox DM,
    // or an ACTIVE connect session — so a reap never strands one. Once the obligation closes, the sub is
    // reaped; CLOSED back-rows (resolved dead-letter, SENT outbox, REVOKED session) do NOT block it.
    #[tokio::test]
    async fn reap_of_subscription_waits_for_open_operational_obligations() {
        let s = mem_store();
        let now = 2 * TERMINAL_ROW_RETENTION_SECS;
        let old = TERMINAL_ROW_RETENTION_SECS - 1;

        s.transaction(move |tx| {
            // Three terminal, past-window subs each blocked by ONE open obligation.
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES
                   ('sub-td',   'TERMINATED', ?1),
                   ('sub-ob',   'EXPIRED',    ?1),
                   ('sub-ncs',  'TERMINATED', ?1),
                   ('sub-done', 'TERMINATED', ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO teardown_failure (id, subscription_id, hook, attempts, first_failed_at, last_attempt_at, resolved_at) VALUES
                   ('td:sub-td:destroy',   'sub-td',   'destroy', 1, ?1, ?1, NULL),
                   ('td:sub-done:destroy', 'sub-done', 'destroy', 1, ?1, ?1, ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO outbox (id, subscription_id, msg_type, state, attempts, created_at) VALUES
                   ('ob:sub-ob:1',   'sub-ob',   'billing.receipt', 'PENDING', 1, ?1),
                   ('ob:sub-done:1', 'sub-done', 'billing.receipt', 'SENT',    1, ?1)",
                rusqlite::params![old],
            )?;
            tx.execute(
                "INSERT INTO native_connect_session (id, subscription_id, scope, state, created_at) VALUES
                   ('ncs:sub-ncs:1',  'sub-ncs',  'op', 'ACTIVE',  ?1),
                   ('ncs:sub-done:1', 'sub-done', 'op', 'REVOKED', ?1)",
                rusqlite::params![old],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        // sub-done has only CLOSED back-rows -> reaped now; the other three are blocked by open obligations.
        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped.subscription, 1, "only the sub with all obligations closed is reaped");
        assert_eq!(count(&s, "SELECT count(*) FROM subscription WHERE id='sub-done'").await, 0);
        for (id, why) in [
            ("sub-td", "unresolved teardown_failure blocks the reap"),
            ("sub-ob", "PENDING outbox DM blocks the reap"),
            ("sub-ncs", "ACTIVE connect session blocks the reap"),
        ] {
            assert_eq!(
                count(&s, &format!("SELECT count(*) FROM subscription WHERE id='{id}'")).await,
                1,
                "{why}"
            );
        }

        // Close each open obligation; now all three reap.
        s.transaction(move |tx| {
            tx.execute("UPDATE teardown_failure SET resolved_at=?1 WHERE id='td:sub-td:destroy'", rusqlite::params![old])?;
            tx.execute("UPDATE outbox SET state='SENT' WHERE id='ob:sub-ob:1'", [])?;
            tx.execute("UPDATE native_connect_session SET state='REVOKED' WHERE id='ncs:sub-ncs:1'", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let reaped2 = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(reaped2.subscription, 3, "all three reap once their obligations close");
        assert_eq!(count(&s, "SELECT count(*) FROM subscription").await, 0);
    }

    // lnrent-y4m.2: the clock-capped cutoff `< MIN(unixepoch(), now) - RETENTION` — a daemon clock
    // running far AHEAD of real wall time cannot over-prune future-stamped rows (mirrors the prune
    // sweep's cap).
    #[tokio::test]
    async fn reap_clock_cap_prevents_over_pruning_with_a_fast_clock() {
        let s = mem_store();
        let future = 4_000_000_000_i64; // ~year 2096, safely above real unixepoch()
        let now = future + 2 * TERMINAL_ROW_RETENTION_SECS;
        // Rows stamped in the future relative to real wall time. Under the UNCAPPED cutoff
        // (now - RETENTION = future + RETENTION) they would delete; the MIN(unixepoch(), now) cap
        // (unixepoch() - RETENTION, far below `future`) saves them.
        s.transaction(move |tx| {
            tx.execute(
                "INSERT INTO reservation (id, order_id, state, created_at) VALUES ('r-future', 'o-future', 'RELEASED', ?1)",
                rusqlite::params![future],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status, expires_at, issued_at)
                 VALUES ('i-future', NULL, 'x-future', 'order', 'EXPIRED', ?1, ?1)",
                rusqlite::params![future],
            )?;
            tx.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (NULL, 'order_placed', '{}', ?1)",
                rusqlite::params![future],
            )?;
            tx.execute(
                "INSERT INTO subscription (id, state, updated_at) VALUES ('sub-future', 'EXPIRED', ?1)",
                rusqlite::params![future],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let reaped = s.reap_terminal_rows(now).await.unwrap();
        assert_eq!(
            reaped,
            ReapCounts::default(),
            "clock-capped cutoff prevents over-pruning future-stamped rows"
        );
        assert_eq!(count(&s, "SELECT count(*) FROM reservation").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM invoice").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM event_log").await, 1);
        assert_eq!(count(&s, "SELECT count(*) FROM subscription").await, 1);
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

    // ---- lnrent-y4m.3: disk-full / corruption degraded-mode guard ----

    /// Build a synthetic `SqliteFailure` for the given primary result code — no live DB needed, so
    /// the classifier boundary can be tested against exact `ErrorCode`s (`ffi::Error::new` masks the
    /// code down to its primary, so extended IOERR codes land on `SystemIoFailure` too).
    fn sqlite_failure(code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), Some("synthetic".into()))
    }

    // The classifier fires ONLY for the disk-full / corruption / IO family — the errors that mean the
    // DB can no longer be durably written. Business outcomes must never latch the store degraded.
    #[test]
    fn is_fatal_db_error_only_for_disk_corruption_io_family() {
        use rusqlite::ffi;
        // Fatal family: SQLITE_FULL / SQLITE_CORRUPT / SQLITE_NOTADB / SQLITE_IOERR, plus an extended
        // IOERR code that masks down to the SQLITE_IOERR primary (-> SystemIoFailure).
        for code in [
            ffi::SQLITE_FULL,
            ffi::SQLITE_CORRUPT,
            ffi::SQLITE_NOTADB,
            ffi::SQLITE_IOERR,
            ffi::SQLITE_IOERR_WRITE,
            ffi::SQLITE_READONLY,
        ] {
            assert!(
                is_fatal_db_error(&sqlite_failure(code)),
                "expected fatal for sqlite code {code}"
            );
        }
        // NOT fatal: a constraint violation (UNIQUE/CAS miss) is a business error.
        assert!(!is_fatal_db_error(&sqlite_failure(ffi::SQLITE_CONSTRAINT)));
        // NOT fatal: transient contention (BUSY/LOCKED) must not latch a healthy daemon offline.
        assert!(!is_fatal_db_error(&sqlite_failure(ffi::SQLITE_BUSY)));
        assert!(!is_fatal_db_error(&sqlite_failure(ffi::SQLITE_LOCKED)));
        // NOT fatal: non-`SqliteFailure` variants (a no-rows / empty-result outcome).
        assert!(!is_fatal_db_error(&rusqlite::Error::QueryReturnedNoRows));
        assert!(!is_fatal_db_error(&rusqlite::Error::ExecuteReturnedResults));
    }

    // A fatal DB error surfaced from INSIDE the closure trips the latch; the NEXT write is then
    // refused with the distinctive degraded message WITHOUT running its closure, while reads still
    // serve and the refused write leaves no partial state.
    #[tokio::test]
    async fn fatal_error_trips_degraded_and_refuses_subsequent_writes() {
        let s = mem_store();
        assert!(!s.is_degraded(), "healthy store is not degraded");

        // Seed a row so we can prove reads still work while degraded.
        s.transaction(|tx| {
            tx.execute("INSERT INTO recipe (id, version) VALUES ('r1', '1')", [])?;
            Ok(())
        })
        .await
        .unwrap();

        // A write whose closure raises a fatal SqliteFailure as an anyhow error — the shape a real
        // in-closure disk-full write produces (`f(&txn)` yields `anyhow::Result`).
        let err = s
            .transaction(|_tx| -> Result<()> {
                Err(anyhow::Error::new(sqlite_failure(rusqlite::ffi::SQLITE_FULL)))
            })
            .await
            .unwrap_err();
        assert!(
            err.downcast_ref::<rusqlite::Error>().is_some(),
            "original concrete error preserved through the classifier"
        );
        assert!(
            s.degraded.load(Ordering::Acquire),
            "latch tripped by the fatal error"
        );
        assert!(s.is_degraded(), "public is_degraded() reflects the tripped latch");

        // A subsequent write is refused with the distinctive message WITHOUT running its closure.
        let ran = Arc::new(AtomicBool::new(false));
        let ran_probe = ran.clone();
        let refused = s
            .transaction(move |tx| {
                ran_probe.store(true, Ordering::SeqCst);
                tx.execute("INSERT INTO recipe (id, version) VALUES ('r2', '1')", [])?;
                Ok(())
            })
            .await
            .unwrap_err();
        assert!(
            refused.to_string().contains("degraded read-only mode"),
            "distinctive degraded message, got: {refused}"
        );
        assert!(
            !ran.load(Ordering::SeqCst),
            "a refused write must not run its closure"
        );

        // Reads/status still serve while degraded, and the refused write applied nothing.
        assert_eq!(count(&s, "SELECT count(*) FROM recipe").await, 1);
    }

    // A plain business `Err` and a constraint violation both leave the latch clear, so the store
    // stays writable. Tripping degraded on these would take a healthy daemon offline.
    #[tokio::test]
    async fn business_error_does_not_trip_degraded() {
        let s = mem_store();

        // A closure returning a plain business error rolls back and does NOT trip.
        let res: Result<()> = s
            .transaction(|tx| {
                tx.execute("INSERT INTO recipe (id, version) VALUES ('r1', '1')", [])?;
                Err(anyhow!("business rule violated"))
            })
            .await;
        assert!(res.is_err());
        assert!(
            !s.degraded.load(Ordering::Acquire),
            "a business error must not latch degraded"
        );

        // A PRIMARY-KEY constraint violation is likewise a business error, not fatal.
        s.transaction(|tx| {
            tx.execute("INSERT INTO recipe (id, version) VALUES ('dup', '1')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        let dup: Result<()> = s
            .transaction(|tx| {
                tx.execute("INSERT INTO recipe (id, version) VALUES ('dup', '2')", [])?;
                Ok(())
            })
            .await;
        assert!(dup.is_err(), "duplicate PK must error");
        assert!(
            !s.degraded.load(Ordering::Acquire),
            "a constraint violation must not latch degraded"
        );

        // The store is still writable: a fresh write commits ('dup' + 'r2' == 2 rows).
        s.transaction(|tx| {
            tx.execute("INSERT INTO recipe (id, version) VALUES ('r2', '1')", [])?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(count(&s, "SELECT count(*) FROM recipe").await, 2);
    }

    // `open()`'s startup gate: a healthy freshly-created file DB passes; an existing zero-byte DB
    // and a file with a corrupt interior data page both fail startup LOUDLY instead of silently
    // becoming empty state or surfacing as a late opaque runtime failure.
    #[tokio::test]
    async fn open_quick_check_gate() {
        let dir = std::env::temp_dir();
        let uniq = std::process::id();
        let rm_all = |p: &std::path::Path| {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(p.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(p.with_extension("sqlite-shm"));
        };

        // Healthy path: a fresh file DB opens and its quick_check verdict is exactly "ok".
        let healthy = dir.join(format!("lnrent-qc-ok-{uniq}.sqlite"));
        rm_all(&healthy);
        let conn = open(&healthy).unwrap();
        let verdict: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(verdict, "ok");
        drop(conn);
        // Reopening the (now populated) healthy WAL DB still passes the gate.
        assert!(open(&healthy).is_ok());
        rm_all(&healthy);

        // Existing zero-byte DB path: SQLite would initialize this as a brand-new empty database
        // before quick_check, so refuse it before `Connection::open` while still allowing the
        // missing-file first-run path above.
        let empty = dir.join(format!("lnrent-qc-empty-{uniq}.sqlite"));
        rm_all(&empty);
        std::fs::File::create(&empty).unwrap();
        let err = open(&empty).unwrap_err();
        assert!(
            err.to_string().contains("exists but is empty"),
            "expected empty-file startup refusal, got: {err}"
        );
        rm_all(&empty);

        // Corrupt path: build a valid DB with enough rows to span several pages, checkpoint so all
        // data lives in the main file, then overwrite an interior data page with garbage.
        let corrupt = dir.join(format!("lnrent-qc-bad-{uniq}.sqlite"));
        rm_all(&corrupt);
        let conn = open(&corrupt).unwrap();
        for i in 0..64 {
            conn.execute(
                "INSERT INTO recipe (id, version, manifest_json) VALUES (?1, '1', ?2)",
                rusqlite::params![format!("id-{i}"), "x".repeat(256)],
            )
            .unwrap();
        }
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);
        // Drop the WAL/shm sidecars so the reopen reads the (corrupt) main file directly.
        let _ = std::fs::remove_file(corrupt.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(corrupt.with_extension("sqlite-shm"));

        // Overwrite page 2 (the first data page; page 1 holds header + schema) so the header still
        // identifies a database (the journal_mode pragma works) but a b-tree page is malformed —
        // quick_check catches it.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&corrupt)
                .unwrap();
            f.seek(SeekFrom::Start(page_size as u64)).unwrap();
            f.write_all(&vec![0xFF; page_size as usize]).unwrap();
            f.flush().unwrap();
        }

        let err = open(&corrupt).unwrap_err();
        assert!(
            err.to_string().contains("integrity check failed"),
            "expected structured integrity error, got: {err}"
        );
        rm_all(&corrupt);
    }
}
