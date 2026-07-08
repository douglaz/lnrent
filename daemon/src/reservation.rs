//! Order-time capacity reservation + order-param validation (lnrent-7fp.7, SPEC.md §9.3,
//! §6.4). To kill the concurrent-order race for the last slot, capacity is reserved **at
//! order time** (before the invoice), atomically with the availability check via the store
//! actor (ADR-0001) — so there is no TOCTOU. A reservation is `HELD` from order through
//! provisioning, `CONSUMED` once the Instance is `ACTIVE`, and `RELEASED` on expiry / refund
//! / terminate. `available = host budget - live usage`, where live usage is every `CONSUMED`
//! reservation plus every `HELD` unpaid reservation whose TTL has not passed, plus every `HELD`
//! paid/provisioning order even past its initial invoice TTL.

use crate::recipe::{Recipe, Resources};
use crate::store::Store;
use anyhow::{bail, Result};
use rusqlite::{OptionalExtension, Transaction};
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
#[allow(clippy::too_many_arguments)] // cohesive reserve inputs; a params struct would not clarify
pub async fn reserve(
    store: &Store,
    reservation_id: &str,
    order_id: &str,
    req: Request,
    budget: Budget,
    expires_at: i64,
    now: i64,
    max_live_holds_per_buyer: u32,
) -> Result<Reserve> {
    let (rid, oid) = (reservation_id.to_string(), order_id.to_string());
    let resources_json = serde_json::to_string(&req.resources)?;
    let ports_json = format!("{{\"count\":{}}}", req.ports);

    store
        .transaction(move |tx| {
            // reserve() is idempotent per order_id, but a re-reserve must never resurrect a
            // reservation that has already moved past HELD (codex re-review).
            let existing: Option<String> = tx
                .query_row(
                    "SELECT state FROM reservation WHERE order_id=?1",
                    rusqlite::params![oid],
                    |r| r.get(0),
                )
                .optional()?;
            match existing.as_deref() {
                // The capacity is already held by an ACTIVE Instance — idempotent success, and
                // crucially we do NOT downgrade CONSUMED back to HELD.
                Some("CONSUMED") => return Ok(Reserve::Reserved),
                // A terminal reservation is never re-held: renewals get a fresh order_id, so a
                // re-reserve of a RELEASED order is a caller bug, surfaced rather than silently
                // re-holding capacity.
                Some("RELEASED") => bail!("reserve: order `{oid}` reservation is already RELEASED"),
                _ => {} // None (new) or HELD (legit pre-commit retry) fall through.
            }
            // Per-pubkey anti-griefing cap (PR-1, GATE-0). The `#p` recipient tag is public and any
            // free keypair can reach `order.request`, so an unbounded stranger could cycle unpaid
            // holds to strand a small host at zero cost. Count THIS sender's live HELD holds (same
            // live predicate `live_usage` uses — a paid/PROVISIONING hold still counts) EXCLUDING
            // this order's own hold, and refuse above the cap with the ordinary `CapacityFull`
            // business result (leaks nothing about the cap). Self-exclusion keeps an idempotent
            // re-reserve of an already-held order from counting against itself. `max=0` refuses all
            // orders (0 >= 0) by construction — not special-cased.
            if let Some(like) = sender_like_pattern(&oid) {
                let held = live_hold_count(tx, now, &like, &oid)?;
                if held >= max_live_holds_per_buyer {
                    return Ok(Reserve::CapacityFull);
                }
            }
            // Capacity check EXCLUDES this order's own live hold, so a HELD retry on a full host
            // can't reject itself as CapacityFull (it already accounts for that capacity).
            let (uc, um, ud, up) = live_usage(tx, now, &oid)?;
            if uc + req.resources.cpu > budget.cpu
                || um + req.resources.mem_mb > budget.mem_mb
                || ud + req.resources.disk_gb > budget.disk_gb
                || up + req.ports > budget.ports
            {
                return Ok(Reserve::CapacityFull);
            }
            // Insert a new HELD hold, or refresh the existing HELD retry's TTL/resources. The
            // CONSUMED/RELEASED cases were handled above, so the only conflict reaching here is a
            // HELD row.
            tx.execute(
                "INSERT INTO reservation (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'HELD', ?5, ?6)
                 ON CONFLICT(order_id) DO UPDATE SET
                   resources_json=excluded.resources_json, ports_json=excluded.ports_json,
                   expires_at=excluded.expires_at",
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
        .transaction(move |tx| consume_txn(tx, &oid, now))
        .await
}

/// No live `HELD` reservation to consume for an order — the sole *business* failure of
/// [`consume_txn`]: a concurrent refund/terminate already RELEASED the hold, or it was already
/// CONSUMED. This is distinct from a transient rusqlite/journal error, so the provision step can
/// turn THIS into REFUND_DUE while letting a real DB error propagate and abort the drive rather
/// than refund a successfully-provisioned buyer (Reviewer 2).
#[derive(Debug)]
pub struct NoHeldReservation {
    pub order_id: String,
    pub affected: usize,
}

impl std::fmt::Display for NoHeldReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consume: no HELD reservation for order `{}` (affected {})",
            self.order_id, self.affected
        )
    }
}

impl std::error::Error for NoHeldReservation {}

/// HELD -> CONSUMED inside an EXISTING transaction (the txn-bound variant of [`consume`]). The
/// provision step (lnrent-7fp.10) calls this so the reservation is consumed in the SAME txn that
/// moves the subscription to ACTIVE — an ACTIVE sub then atomically holds live capacity, with no
/// window where it is live without a CONSUMED hold.
///
/// Consume does NOT gate on `expires_at`: it only ever runs once the order is PAID and settled
/// (the sub is in PROVISIONING), so the hold is committed capacity that must be honored through
/// provisioning. The TTL is the pre-payment GC for *unpaid* holds ([`live_usage`]); refunding a
/// paid buyer just because a slow hook or a restart pushed activation past the TTL would be wrong
/// (codex P2). The only *business* failure is a missing/non-HELD hold (e.g. a concurrent refund
/// RELEASED it), surfaced as a typed [`NoHeldReservation`] the caller turns into REFUND_DUE; a
/// transient DB error propagates as itself.
pub fn consume_txn(tx: &Transaction, order_id: &str, now: i64) -> Result<()> {
    let n = tx.execute(
        "UPDATE reservation SET state='CONSUMED' WHERE order_id=?1 AND state='HELD'",
        rusqlite::params![order_id],
    )?;
    if n != 1 {
        return Err(NoHeldReservation {
            order_id: order_id.to_string(),
            affected: n,
        }
        .into());
    }
    journal(tx, order_id, "reserve_consume", "{}", now)?;
    Ok(())
}

/// -> RELEASED, on invoice expiry / refund / terminate (§9.3). Idempotent: returns whether a
/// HELD/CONSUMED reservation was actually released (false = nothing live to release).
pub async fn release(store: &Store, order_id: &str, now: i64) -> Result<bool> {
    let oid = order_id.to_string();
    store
        .transaction(move |tx| release_txn(tx, &oid, now))
        .await
}

/// -> RELEASED inside an EXISTING transaction (the txn-bound variant of [`release`]). §9.3 has
/// expiry / `destroy` / refund / terminate release the reservation, so a refunded or cancelled
/// order leaves nothing reserved. The provision FAILURE path does NOT release here — it leaves the
/// paid hold HELD for the refund executor (lnrent-7fp.11), mirroring capture's refund paths.
/// Idempotent: returns whether a live reservation was released.
pub fn release_txn(tx: &Transaction, order_id: &str, now: i64) -> Result<bool> {
    let n = tx.execute(
        "UPDATE reservation SET state='RELEASED' WHERE order_id=?1 AND state IN ('HELD','CONSUMED')",
        rusqlite::params![order_id],
    )?;
    if n > 0 {
        journal(tx, order_id, "reserve_release", "{}", now)?;
    }
    Ok(n > 0)
}

/// Journal a capacity mutation to `event_log` in the same txn (every mutation is journaled,
/// ADR-0001/§6.5). `subscription_id` carries the order id.
fn journal(
    tx: &rusqlite::Transaction,
    order_id: &str,
    kind: &str,
    detail_json: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![order_id, kind, detail_json, now],
    )?;
    Ok(())
}

/// Sum of live usage = every CONSUMED reservation (each is an active Instance's hold) + every
/// HELD reservation still within its TTL + every HELD paid/provisioning order even past TTL,
/// EXCLUDING `exclude_order_id` (so a re-reserve doesn't count its own hold and reject itself).
/// Expired unpaid HELD rows do not block new orders, but a paid order that capture has moved into
/// PROVISIONING keeps its capacity while the idempotent provision hook/restart path catches up.
/// With one reservation per order (`UNIQUE(order_id)`) and checked `consume`, CONSUMED reservations
/// ARE the active Instances, so this equals the spec's
/// `budget - (active Instances + live reservations)` (§9.3). Uses sqlite's JSON1.
fn live_usage(
    tx: &rusqlite::Transaction,
    now: i64,
    exclude_order_id: &str,
) -> rusqlite::Result<(u32, u32, u32, u32)> {
    tx.query_row(
        "SELECT
            COALESCE(SUM(json_extract(resources_json,'$.cpu')),0),
            COALESCE(SUM(json_extract(resources_json,'$.mem_mb')),0),
            COALESCE(SUM(json_extract(resources_json,'$.disk_gb')),0),
            COALESCE(SUM(json_extract(ports_json,'$.count')),0)
         FROM reservation r
         WHERE (
             r.state='CONSUMED'
             OR (
               r.state='HELD'
               AND (
                 r.expires_at > ?1
                 OR EXISTS (
                   SELECT 1 FROM invoice i
                   WHERE i.subscription_id = r.order_id
                     AND i.kind='order'
                     AND i.status='PAID'
                 )
                 OR EXISTS (
                   SELECT 1 FROM subscription s
                   WHERE s.id = r.order_id
                     AND s.state='PROVISIONING'
                 )
               )
             )
           )
           AND r.order_id <> ?2",
        rusqlite::params![now, exclude_order_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
}

/// The `LIKE` pattern matching every order id from the same sender: `ord:<sender_hex>:%`, i.e.
/// everything through the second colon plus `%`. The order id is `ord:<sender_hex>:<tail>`
/// (order_intake.rs), and `sender_hex` is lowercase pubkey hex — it contains no `LIKE` metacharacters
/// (`%`/`_`), so the pattern needs no escaping. Returns `None` for an id that is not in the two-colon
/// order form (then the per-pubkey cap is skipped for that reserve — fail-open is safe: the budget
/// check still bounds capacity, and every real `order.request` id is in this form).
fn sender_like_pattern(order_id: &str) -> Option<String> {
    let mut colons = order_id.match_indices(':');
    let _first = colons.next()?;
    let (second, _) = colons.next()?;
    Some(format!("{}%", &order_id[..=second]))
}

/// Count this sender's LIVE HELD reservations (the per-pubkey anti-griefing cap denominator, PR-1).
/// Uses the SAME live-HELD predicate as [`live_usage`] — a HELD row counts while unexpired, or once
/// its order invoice is PAID, or while its subscription is PROVISIONING — so a paid in-flight order
/// still consumes the cap and a stale expired-never-paid row does not. CONSUMED rows (active
/// Instances) are NOT counted: the cap bounds outstanding holds, not completed rentals. Excludes
/// `exclude_order_id` so an idempotent re-reserve of an already-held order is not counted against
/// itself.
fn live_hold_count(
    tx: &rusqlite::Transaction,
    now: i64,
    sender_like: &str,
    exclude_order_id: &str,
) -> rusqlite::Result<u32> {
    tx.query_row(
        "SELECT COUNT(*)
         FROM reservation r
         WHERE r.state='HELD'
           AND (
             r.expires_at > ?1
             OR EXISTS (
               SELECT 1 FROM invoice i
               WHERE i.subscription_id = r.order_id AND i.kind='order' AND i.status='PAID'
             )
             OR EXISTS (
               SELECT 1 FROM subscription s
               WHERE s.id = r.order_id AND s.state='PROVISIONING'
             )
           )
           AND r.order_id LIKE ?2
           AND r.order_id <> ?3",
        rusqlite::params![now, sender_like, exclude_order_id],
        |r| r.get(0),
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
        Budget {
            cpu: 1,
            mem_mb: 1024,
            disk_gb: 20,
            ports: 1,
        }
    }
    fn req_one() -> Request {
        Request {
            resources: Resources {
                cpu: 1,
                mem_mb: 1024,
                disk_gb: 20,
            },
            ports: 1,
        }
    }

    // Two concurrent orders for the last slot: exactly one HELD, the other CapacityFull.
    #[tokio::test]
    async fn concurrent_orders_race_one_slot_no_toctou() {
        let s = mem_store();
        let (a, b) = (s.clone(), s.clone());
        let ta = tokio::spawn(async move {
            reserve(
                &a,
                "res-a",
                "order-a",
                req_one(),
                budget_one(),
                1_000,
                0,
                u32::MAX,
            )
            .await
            .unwrap()
        });
        let tb = tokio::spawn(async move {
            reserve(
                &b,
                "res-b",
                "order-b",
                req_one(),
                budget_one(),
                1_000,
                0,
                u32::MAX,
            )
            .await
            .unwrap()
        });
        let (ra, rb) = (ta.await.unwrap(), tb.await.unwrap());
        // exactly one Reserved, one CapacityFull
        assert_ne!(ra, rb, "the two orders must get different outcomes");
        let held: i64 = s
            .read(|c| {
                Ok(c.query_row(
                    "SELECT count(*) FROM reservation WHERE state='HELD'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(held, 1, "exactly one reservation is HELD");
    }

    #[tokio::test]
    async fn reserve_consume_release_lifecycle() {
        let s = mem_store();
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        let state = |s: &Store| {
            let s = s.clone();
            async move {
                s.read(|c| {
                    Ok(
                        c.query_row("SELECT state FROM reservation WHERE id='r1'", [], |r| {
                            r.get::<_, String>(0)
                        })?,
                    )
                })
                .await
                .unwrap()
            }
        };
        assert_eq!(state(&s).await, "HELD");
        consume(&s, "o1", 1).await.unwrap();
        assert_eq!(state(&s).await, "CONSUMED");
        assert!(
            release(&s, "o1", 2).await.unwrap(),
            "released a live reservation"
        );
        assert_eq!(state(&s).await, "RELEASED");
        // releasing again is a no-op that returns false (idempotent)
        assert!(!release(&s, "o1", 3).await.unwrap());
    }

    // consume on a wrong/absent order_id is an error, not a silent no-op (codex #7).
    #[tokio::test]
    async fn consume_requires_a_held_reservation() {
        let s = mem_store();
        reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0, u32::MAX)
            .await
            .unwrap();
        assert!(
            consume(&s, "nope", 1).await.is_err(),
            "no HELD reservation for a wrong order"
        );
        consume(&s, "o1", 1).await.unwrap();
        assert!(
            consume(&s, "o1", 2).await.is_err(),
            "already CONSUMED -> error, not silent"
        );
    }

    // A paid order's hold is consumed through provisioning even if its TTL has passed: consume only
    // runs post-payment, so a slow hook / restart must not turn the hold into a spurious refund
    // (codex P2). The TTL only GCs *unpaid* holds (see `expired_held_reservation_frees_the_slot`).
    #[tokio::test]
    async fn consume_honors_a_paid_hold_past_its_ttl() {
        let s = mem_store();
        reserve(&s, "r1", "o1", req_one(), budget_one(), 100, 0, u32::MAX)
            .await
            .unwrap();
        // now=101 is past the TTL of 100, yet the (paid) hold is still consumed.
        consume(&s, "o1", 101).await.unwrap();
        let state: String = s
            .read(|c| {
                Ok(c.query_row(
                    "SELECT state FROM reservation WHERE order_id='o1'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(state, "CONSUMED");
    }

    // A re-reserve of the SAME order on a host the order already fills must stay Reserved — it
    // must not count its own hold and reject itself as CapacityFull (codex re-review).
    #[tokio::test]
    async fn reserve_is_idempotent_when_order_already_fills_the_host() {
        let s = mem_store();
        // o1 takes the whole single-slot budget.
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        // Re-reserving o1 (crash-retry) on the now-full host is still Reserved, not CapacityFull.
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 2_000, 10, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        // Still exactly one HELD row (refreshed, not duplicated); TTL bumped to 2000.
        let (held, ttl): (i64, i64) = s
            .read(|c| {
                Ok(c.query_row(
                "SELECT count(*), COALESCE(MAX(expires_at),0) FROM reservation WHERE state='HELD'",
                [], |r| Ok((r.get(0)?, r.get(1)?)))?)
            })
            .await
            .unwrap();
        assert_eq!(held, 1);
        assert_eq!(ttl, 2_000, "the HELD retry refreshed the TTL");
    }

    // A re-reserve must never resurrect a CONSUMED (active) or RELEASED (terminal) reservation.
    #[tokio::test]
    async fn reserve_never_resurrects_consumed_or_released() {
        let s = mem_store();
        let state = |s: &Store| {
            let s = s.clone();
            async move {
                s.read(|c| {
                    Ok(c.query_row(
                        "SELECT state FROM reservation WHERE order_id='o1'",
                        [],
                        |r| r.get::<_, String>(0),
                    )?)
                })
                .await
                .unwrap()
            }
        };
        // CONSUMED: a re-reserve is idempotent success but stays CONSUMED (not downgraded).
        reserve(&s, "r1", "o1", req_one(), budget_one(), 1_000, 0, u32::MAX)
            .await
            .unwrap();
        consume(&s, "o1", 1).await.unwrap();
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 2_000, 2, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        assert_eq!(
            state(&s).await,
            "CONSUMED",
            "re-reserve must not downgrade CONSUMED to HELD"
        );
        // RELEASED: a re-reserve is refused outright (renewals get a fresh order_id).
        release(&s, "o1", 3).await.unwrap();
        assert!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 4_000, 4, u32::MAX)
                .await
                .is_err(),
            "re-reserving a RELEASED order is a caller bug, surfaced not resurrected"
        );
        assert_eq!(
            state(&s).await,
            "RELEASED",
            "the terminal reservation is untouched"
        );
    }

    // An expired HELD reservation must not block a new order (it's no longer live).
    #[tokio::test]
    async fn expired_held_reservation_frees_the_slot() {
        let s = mem_store();
        // r1 HELD with TTL=100.
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 100, 0, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        // At now=50 the slot is still taken.
        assert_eq!(
            reserve(&s, "r2", "o2", req_one(), budget_one(), 200, 50, u32::MAX)
                .await
                .unwrap(),
            Reserve::CapacityFull
        );
        // At now=150 (past r1's TTL) the slot is free again.
        assert_eq!(
            reserve(&s, "r3", "o3", req_one(), budget_one(), 300, 150, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
    }

    #[tokio::test]
    async fn paid_held_reservation_counts_even_after_ttl() {
        let s = mem_store();
        assert_eq!(
            reserve(&s, "r1", "o1", req_one(), budget_one(), 100, 0, u32::MAX)
                .await
                .unwrap(),
            Reserve::Reserved
        );
        s.transaction(|tx| {
            tx.execute(
                "INSERT INTO subscription (id, state) VALUES ('o1', 'PROVISIONING')",
                [],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status)
                 VALUES ('i1', 'o1', 'order:o1', 'order', 'PAID')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(
            reserve(&s, "r2", "o2", req_one(), budget_one(), 300, 150, u32::MAX)
                .await
                .unwrap(),
            Reserve::CapacityFull,
            "a paid order keeps its HELD capacity while provisioning catches up"
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
        assert!(
            validate_params(&r, &missing).is_err(),
            "missing required param rejected"
        );

        let wrong_type = json!({"pubkey": 123}).as_object().unwrap().clone();
        assert!(
            validate_params(&r, &wrong_type).is_err(),
            "wrong-type param rejected"
        );
    }

    // ---- PR-1: per-pubkey live-HELD anti-griefing cap (GATE-0) -----------------------------------

    /// A budget large enough that the per-pubkey cap, not the host budget, is the only limiter here.
    fn budget_big() -> Budget {
        Budget {
            cpu: 1000,
            mem_mb: 1_000_000,
            disk_gb: 1_000_000,
            ports: 1000,
        }
    }

    /// Reserve using the daemon's sender-embedding order id form `ord:<sender>:<tail>`.
    async fn reserve_order(
        s: &Store,
        sender: &str,
        tail: &str,
        expires_at: i64,
        now: i64,
        cap: u32,
    ) -> Reserve {
        reserve(
            s,
            &format!("res:{sender}:{tail}"),
            &format!("ord:{sender}:{tail}"),
            req_one(),
            budget_big(),
            expires_at,
            now,
            cap,
        )
        .await
        .unwrap()
    }

    // The Nth live hold from one buyer key is refused; a different key is unaffected (per-pubkey).
    #[tokio::test]
    async fn per_buyer_cap_blocks_nth_hold_only() {
        let s = mem_store();
        let cap = 2;
        assert_eq!(
            reserve_order(&s, "aaa", "1", 10_000, 0, cap).await,
            Reserve::Reserved
        );
        assert_eq!(
            reserve_order(&s, "aaa", "2", 10_000, 0, cap).await,
            Reserve::Reserved
        );
        // aaa is at the cap of 2 live holds -> a third DISTINCT order is refused.
        assert_eq!(
            reserve_order(&s, "aaa", "3", 10_000, 0, cap).await,
            Reserve::CapacityFull
        );
        // A different buyer key reserves freely at the same time.
        assert_eq!(
            reserve_order(&s, "bbb", "1", 10_000, 0, cap).await,
            Reserve::Reserved
        );
        assert_eq!(
            reserve_order(&s, "bbb", "2", 10_000, 0, cap).await,
            Reserve::Reserved
        );
    }

    // max=0 refuses every order (0 >= 0 by construction), not special-cased.
    #[tokio::test]
    async fn per_buyer_cap_zero_refuses_all_orders() {
        let s = mem_store();
        assert_eq!(
            reserve_order(&s, "aaa", "1", 10_000, 0, 0).await,
            Reserve::CapacityFull
        );
    }

    // An idempotent re-reserve of the SAME order id is NOT counted against itself (self-exclusion):
    // a buyer at the cap can still complete/retry an order they already hold.
    #[tokio::test]
    async fn per_buyer_cap_excludes_self_on_re_reserve() {
        let s = mem_store();
        let cap = 1;
        assert_eq!(
            reserve_order(&s, "aaa", "1", 10_000, 0, cap).await,
            Reserve::Reserved
        );
        // aaa is at the cap of 1, but re-reserving the SAME order continues it.
        assert_eq!(
            reserve_order(&s, "aaa", "1", 10_000, 0, cap).await,
            Reserve::Reserved
        );
        // a DISTINCT new order from aaa is refused.
        assert_eq!(
            reserve_order(&s, "aaa", "2", 10_000, 0, cap).await,
            Reserve::CapacityFull
        );
    }

    // An expired-and-never-paid hold no longer counts toward the cap.
    #[tokio::test]
    async fn per_buyer_cap_expired_unpaid_hold_frees_slot() {
        let s = mem_store();
        let cap = 1;
        assert_eq!(
            reserve_order(&s, "aaa", "1", 100, 0, cap).await,
            Reserve::Reserved
        );
        // at t=200 the first hold is expired-unpaid -> frees the cap slot -> a new order is allowed.
        assert_eq!(
            reserve_order(&s, "aaa", "2", 10_000, 200, cap).await,
            Reserve::Reserved
        );
    }

    // Counting-rule regression: a paid/PROVISIONING hold still counts toward the cap even PAST its
    // reservation TTL — the cap must use the same live predicate, not `expires_at` alone.
    #[tokio::test]
    async fn per_buyer_cap_paid_or_provisioning_hold_still_counts() {
        let s = mem_store();
        let cap = 1;
        assert_eq!(
            reserve_order(&s, "aaa", "1", 100, 0, cap).await,
            Reserve::Reserved
        );
        // mark ord:aaa:1 PAID + PROVISIONING (mirrors capture -> provisioning).
        s.transaction(|tx| {
            tx.execute(
                "INSERT INTO subscription (id, state) VALUES ('ord:aaa:1', 'PROVISIONING')",
                [],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, status)
                 VALUES ('i1', 'ord:aaa:1', 'order:ord:aaa:1', 'order', 'PAID')",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        // at t=200 the first hold is PAST its TTL but PAID/PROVISIONING -> still counts -> aaa at cap.
        assert_eq!(
            reserve_order(&s, "aaa", "2", 10_000, 200, cap).await,
            Reserve::CapacityFull
        );
    }

    // Preserved invariant (oversell guard): the hold TTL is not shortened — the persisted reservation
    // `expires_at` equals the order-invoice `expires_at` passed in (see reserve()'s invariant note).
    #[tokio::test]
    async fn hold_ttl_equals_order_invoice_expiry() {
        let s = mem_store();
        let expires_at = 987_654;
        assert_eq!(
            reserve_order(&s, "aaa", "1", expires_at, 0, 2).await,
            Reserve::Reserved
        );
        let got: i64 = s
            .read(|c| {
                Ok(c.query_row(
                    "SELECT expires_at FROM reservation WHERE order_id='ord:aaa:1'",
                    [],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(
            got, expires_at,
            "hold TTL must equal the order-invoice expiry (no shortening -> no oversell)"
        );
    }
}
