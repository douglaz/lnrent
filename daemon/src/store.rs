//! sqlite state + the **sole-writer store actor** (ADR-0001). Schema: SPEC.md §11.
//!
//! One tokio task owns the `Connection` and is the only accessor; every read/write goes
//! through it via a closure, so there are no write races and the sole-writer invariant is
//! structural, not a convention. `transaction()` gives the atomic multi-row transitions the
//! money path needs (e.g. capture: invoice OPEN->PAID + sub PENDING->PROVISIONING in one txn).

use anyhow::{anyhow, Result};
use rusqlite::{Connection, Transaction};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

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
  created_at         INTEGER,
  updated_at         INTEGER
);

CREATE TABLE IF NOT EXISTS outbox (   -- pending operator->buyer NIP-17 DMs (ADR-0009)
  id              TEXT PRIMARY KEY,
  recipient       TEXT,
  subscription_id TEXT,
  msg_type        TEXT,
  payload_json    TEXT,
  state           TEXT,    -- PENDING | SENT
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

/// Ordered migrations (lnrent-7fp.3): index `i` upgrades the DB from schema version `i` to
/// `i+1`. Version 1 is the §11 schema; version 2 adds `seen_message` (lnrent-7fp.5). A future
/// schema change appends a new entry of `ALTER`/`CREATE` statements; **never edit a shipped
/// migration**.
const MIGRATIONS: &[&str] = &[SCHEMA, M2_SEEN_MESSAGE];

/// The target schema version this binary expects (= number of migrations).
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

/// Apply any pending migrations, keyed on `PRAGMA user_version`. Idempotent: opening a
/// current DB is a no-op; opening a v0 DB applies the §11 schema and sets `user_version=1`.
pub fn migrate(conn: &Connection) -> Result<()> {
    let mut current: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    while (current as usize) < MIGRATIONS.len() {
        conn.execute_batch(MIGRATIONS[current as usize])?;
        // user_version can't be a bound parameter; the value is an internal counter, not input.
        conn.execute_batch(&format!("PRAGMA user_version = {}", current + 1))?;
        current += 1;
    }
    Ok(())
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

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
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
