//! sqlite state. The daemon is the sole writer (ADR-0001). Schema: SPEC.md §11.

use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

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
  order_id       TEXT,
  resources_json TEXT,
  ports_json     TEXT,
  state          TEXT,    -- HELD | CONSUMED | RELEASED
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

/// Open the state database and ensure the schema exists.
pub fn open(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
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
}
