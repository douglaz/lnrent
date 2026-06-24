//! Order-time capacity reservation + order-param validation (lnrent-7fp.7, SPEC.md §9.3,
//! §6.4). To kill the concurrent-order race for the last slot, capacity is reserved **at
//! order time** (before the invoice), atomically with the availability check via the store
//! actor (ADR-0001) — so there is no TOCTOU. A reservation is `HELD` from order through
//! provisioning, `CONSUMED` once the Instance is `ACTIVE`, and `RELEASED` on expiry / refund
//! / terminate. `available = host budget - live usage`, where live usage is every `CONSUMED`
//! reservation plus every `HELD` reservation whose TTL has not passed.

use crate::recipe::{Recipe, Resources};
use crate::store::Store;
use anyhow::{bail, Result};
use serde_json::{Map, Value};

/// The operator-configured rentable budget for a host (set at onboard, §9.3).
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub cpu: u32,
    pub mem_mb: u32,
    pub disk_gb: u32,
    pub ports: u32,
}

/// What an order needs reserved.
#[derive(Debug, Clone, Default)]
pub struct Request {
    pub resources: Resources,
    pub ports: u32,
}

/// Outcome of an atomic reserve attempt. `CapacityFull` is a normal business result (it maps
/// to `order.error{code:"capacity_full"}`), not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reserve {
    Reserved,
    CapacityFull,
}

/// Validate an order's params against the recipe's `[[params]]` (§7.1): every `required`
/// param must be present, and a `number`/`bool` param must have the right JSON type. Run at
/// pre-flight, before any money moves.
pub fn validate_params(recipe: &Recipe, params: &Map<String, Value>) -> Result<()> {
    for p in &recipe.params {
        match params.get(&p.key) {
            None => {
                if p.required {
                    bail!("missing required param `{}`", p.key);
                }
            }
            Some(v) => {
                let ok = match p.ty.as_str() {
                    "string" => v.is_string(),
                    "number" | "int" | "integer" => v.is_number(),
                    "bool" | "boolean" => v.is_boolean(),
                    _ => true, // unknown declared type: accept (recipe's own concern)
                };
                if !ok {
                    bail!("param `{}` has the wrong type (expected {})", p.key, p.ty);
                }
            }
        }
    }
    Ok(())
}

/// Atomically check availability and, if there is room, create a `HELD` reservation for
/// `order_id` with `reservation_id`, TTL `expires_at`. Returns `Reserved` or `CapacityFull`.
/// The whole check-and-insert runs in ONE store transaction, and the store actor serializes
/// transactions, so two concurrent orders for the last slot can never both succeed.
pub async fn reserve(
    store: &Store,
    reservation_id: &str,
    order_id: &str,
    req: Request,
    budget: Budget,
    expires_at: i64,
    now: i64,
) -> Result<Reserve> {
    let (rid, oid) = (reservation_id.to_string(), order_id.to_string());
    let resources_json = serde_json::to_string(&req.resources)?;
    let ports_json = format!("{{\"count\":{}}}", req.ports);

    store
        .transaction(move |tx| {
            let (uc, um, ud, up) = live_usage(tx, now)?;
            if uc + req.resources.cpu > budget.cpu
                || um + req.resources.mem_mb > budget.mem_mb
                || ud + req.resources.disk_gb > budget.disk_gb
                || up + req.ports > budget.ports
            {
                return Ok(Reserve::CapacityFull);
            }
            // Idempotent per order: a retry (same order_id, e.g. after a pre-commit crash)
            // refreshes the existing reservation rather than creating a duplicate hold.
            tx.execute(
                "INSERT INTO reservation (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'HELD', ?5, ?6)
                 ON CONFLICT(order_id) DO UPDATE SET
                   resources_json=excluded.resources_json, ports_json=excluded.ports_json,
                   state='HELD', expires_at=excluded.expires_at",
                rusqlite::params![rid, oid, resources_json, ports_json, expires_at, now],
            )?;
            journal(tx, &oid, "reserve", &resources_json, now)?;
            Ok(Reserve::Reserved)
        })
        .await
}

/// HELD -> CONSUMED, when the Instance reaches `ACTIVE` (§9.3). A CONSUMED reservation IS the
/// active Instance's capacity hold. Requires exactly one matching HELD reservation — a wrong
/// `order_id` or a non-HELD state is an error, not a silent no-op (codex #7).
pub async fn consume(store: &Store, order_id: &str, now: i64) -> Result<()> {
    let oid = order_id.to_string();
    store
        .transaction(move |tx| {
            let n = tx.execute(
                "UPDATE reservation SET state='CONSUMED' WHERE order_id=?1 AND state='HELD'",
                rusqlite::params![oid],
            )?;
            if n != 1 {
                bail!("consume: no HELD reservation for order `{oid}` (affected {n})");
            }
            journal(tx, &oid, "reserve_consume", "{}", now)?;
            Ok(())
        })
        .await
}

/// -> RELEASED, on invoice expiry / refund / terminate (§9.3). Idempotent: returns whether a
/// HELD/CONSUMED reservation was actually released (false = nothing live to release).
pub async fn release(store: &Store, order_id: &str, now: i64) -> Result<bool> {
    let oid = order_id.to_string();
    store
        .transaction(move |tx| {
            let n = tx.execute(
                "UPDATE reservation SET state='RELEASED' WHERE order_id=?1 AND state IN ('HELD','CONSUMED')",
                rusqlite::params![oid],
            )?;
            if n > 0 {
                journal(tx, &oid, "reserve_release", "{}", now)?;
            }
            Ok(n > 0)
        })
        .await
}

/// Journal a capacity mutation to `event_log` in the same txn (every mutation is journaled,
/// ADR-0001/§6.5). `subscription_id` carries the order id.
fn journal(tx: &rusqlite::Transaction, order_id: &str, kind: &str, detail_json: &str, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![order_id, kind, detail_json, now],
    )?;
    Ok(())
}

/// Sum of live usage = every CONSUMED reservation (each is an active Instance's hold) + every
/// HELD reservation still within its TTL (an expired-but-not-yet-released HELD must not block
/// new orders). With one reservation per order (`UNIQUE(order_id)`) and checked `consume`,
/// CONSUMED reservations ARE the active Instances, so this equals the spec's
/// `budget - (active Instances + live reservations)` (§9.3). Uses sqlite's JSON1.
fn live_usage(tx: &rusqlite::Transaction, now: i64) -> rusqlite::Result<(u32, u32, u32, u32)> {
    tx.query_row(
        "SELECT
            COALESCE(SUM(json_extract(resources_json,'$.cpu')),0),
            COALESCE(SUM(json_extract(resources_json,'$.mem_mb')),0),
            COALESCE(SUM(json_extract(resources_json,'$.disk_gb')),0),
            COALESCE(SUM(json_extract(ports_json,'$.count')),0)
         FROM reservation
         WHERE state='CONSUMED' OR (state='HELD' AND expires_at > ?1)",
        rusqlite::params![now],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;
    use serde_json::json;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    fn budget_one() -> Budget {
        Budget { cpu: 1, mem_mb: 1024, disk_gb: 20, ports: 1 }
    }
    fn req_one() -> Request {
        Request { resources: Resources { cpu: 1, mem_mb: 1024, disk_gb: 20 }, ports: 1 }
    }

    // Two concurrent orders for the last slot: exactly one HELD, the other CapacityFull.
    #[tokio::test]
    async fn concurrent_orders_race_one_slot_no_toctou() {
        let s = mem_store();
        let (a, b) = (s.clone(), s.clone());
        let ta = tokio::spawn(async move {
            reserve(&a, "res-a", "order-a", req_one(), budget_one(), 1_000, 0).await.unwrap()
        });
        let tb = tokio::spawn(async move {
            reserve(&b, "res-b", "order-b", req_one(), budget_one(), 1_000, 0).await.unwrap()
        });
        let (ra, rb) = (ta.await.unwrap(), tb.await.unwrap());
        // exactly one Reserved, one CapacityFull
        assert_ne!(ra, rb, "the two orders must get different outcomes");
        let held: i64 = s
            .read(|c| Ok(c.query_row("SELECT count(*) FROM reservation WHERE state='HELD'", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(held, 1, "exactly one reservation is HELD");
    }

    #[tokio::test]
    async fn reserve_consume_release_lifecycle() {
        let s = mem_store();
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0).await.unwrap(),
            Reserve::Reserved
        );
        let state = |s: &Store| {
            let s = s.clone();
            async move {
                s.read(|c| Ok(c.query_row("SELECT state FROM reservation WHERE id='r1'", [], |r| r.get::<_, String>(0))?))
                    .await
                    .unwrap()
            }
        };
        assert_eq!(state(&s).await, "HELD");
        consume(&s, "o1", 1).await.unwrap();
        assert_eq!(state(&s).await, "CONSUMED");
        assert!(release(&s, "o1", 2).await.unwrap(), "released a live reservation");
        assert_eq!(state(&s).await, "RELEASED");
        // releasing again is a no-op that returns false (idempotent)
        assert!(!release(&s, "o1", 3).await.unwrap());
    }

    // consume on a wrong/absent order_id is an error, not a silent no-op (codex #7).
    #[tokio::test]
    async fn consume_requires_a_held_reservation() {
        let s = mem_store();
        reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0).await.unwrap();
        assert!(consume(&s, "nope", 1).await.is_err(), "no HELD reservation for a wrong order");
        consume(&s, "o1", 1).await.unwrap();
        assert!(consume(&s, "o1", 2).await.is_err(), "already CONSUMED -> error, not silent");
    }

    // An expired HELD reservation must not block a new order (it's no longer live).
    #[tokio::test]
    async fn expired_held_reservation_frees_the_slot() {
        let s = mem_store();
        // r1 HELD with TTL=100.
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 100, 0).await.unwrap(),
            Reserve::Reserved
        );
        // At now=50 the slot is still taken.
        assert_eq!(
            reserve(&s, "r2", "o2", req_one(), budget_one(), 200, 50).await.unwrap(),
            Reserve::CapacityFull
        );
        // At now=150 (past r1's TTL) the slot is free again.
        assert_eq!(
            reserve(&s, "r3", "o3", req_one(), budget_one(), 300, 150).await.unwrap(),
            Reserve::Reserved
        );
    }

    #[tokio::test]
    async fn params_validated_against_manifest() {
        let dir = format!("{}/../recipes/wireguard", env!("CARGO_MANIFEST_DIR"));
        let r = crate::recipe::Recipe::load(&dir).unwrap();
        // wireguard requires a `pubkey` string param.
        let ok = json!({"pubkey": "abc"}).as_object().unwrap().clone();
        validate_params(&r, &ok).expect("valid params accepted");

        let missing = json!({}).as_object().unwrap().clone();
        assert!(validate_params(&r, &missing).is_err(), "missing required param rejected");

        let wrong_type = json!({"pubkey": 123}).as_object().unwrap().clone();
        assert!(validate_params(&r, &wrong_type).is_err(), "wrong-type param rejected");
    }
}
