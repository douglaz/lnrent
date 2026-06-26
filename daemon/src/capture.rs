//! Idempotent, **invoice-status-first** settlement capture (lnrent-7fp.8, SPEC §6.2/§6.3/§6.6).
//!
//! A payment backend can REDELIVER a settlement (a Fedimint/phoenixd reconnect), so capture must
//! be idempotent. The idempotency guard is the INVOICE STATUS (an `OPEN -> PAID` compare-and-swap),
//! and the OUTCOME is chosen by the SETTLED INVOICE's class — not by the subscription state alone,
//! so a renewal settlement that lands while ACTIVE is never mis-routed to a refund. All of it runs
//! in ONE store transaction (the sole-writer actor serializes them), so a replay touches 0 rows
//! and `paid_through` can never double-extend.
//!
//! Capture only DETECTS refunds (writes the durable `PENDING` `refund_attempt` row); it never runs
//! provision (lnrent-7fp.10), executes a refund (lnrent-7fp.11), or fires deadlines (reconcile,
//! lnrent-7fp.9).

use crate::backends::Settlement;
use crate::store::Store;
use anyhow::Result;
use rusqlite::{params, OptionalExtension, Transaction};

/// What a settlement did. Every settlement maps to exactly one of these (the money path is total);
/// all are normal results, not errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capture {
    /// Replay / already-applied invoice — nothing changed.
    NoOp,
    /// OPEN order invoice on a PENDING sub -> PROVISIONING (the first capture).
    Captured,
    /// OPEN renewal invoice on an ACTIVE sub -> `paid_through` extended.
    Renewed,
    /// OPEN renewal invoice on a SUSPENDED sub -> resumed (ACTIVE) + extended.
    Resumed,
    /// Settled-but-terminal / expired / unmatched -> exactly one `refund_attempt` (PENDING).
    RefundDue,
}

/// Apply `settlement` to the durable state. Idempotent: a redelivered settlement is a no-op (or,
/// for a terminal/expired invoice, contributes no second refund row — the refund key is UNIQUE).
pub async fn capture(store: &Store, settlement: Settlement, now: i64) -> Result<Capture> {
    store
        .transaction(move |tx| capture_txn(tx, &settlement, now))
        .await
}

fn capture_txn(tx: &Transaction, s: &Settlement, now: i64) -> Result<Capture> {
    // Look the invoice up by its correlation token (external_id UNIQUE, NOT NULL — ADR-0009).
    // expires_at is the bolt11/reservation expiry; it gates the order-capture path in apply_paid.
    #[allow(clippy::type_complexity)]
    let inv: Option<(String, String, Option<String>, Option<String>, Option<i64>)> = tx
        .query_row(
            "SELECT id, status, kind, subscription_id, expires_at FROM invoice WHERE external_id = ?1",
            params![s.external_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?;
    let (inv_id, status, kind, sub_id, expires_at) = match inv {
        // Unmatched settlement: the backend should pre-filter these, but if one slips through we
        // record a single refund intent (sub/dest unknown) rather than swallow money.
        None => {
            // Journal only when the refund intent is actually NEW, so a redelivered unmatched
            // settlement is a true no-op (no audit-row spam) — the UNIQUE key already dedups it.
            if refund_intent(tx, None, None, s, now)? {
                journal(tx, None, "settle_unmatched_refund", s, now)?;
            }
            return Ok(Capture::RefundDue);
        }
        Some(v) => v,
    };

    match status.as_str() {
        // Already applied — a redelivery/replay. The whole point of the status guard.
        "PAID" => Ok(Capture::NoOp),
        "OPEN" => {
            // OPEN -> PAID compare-and-swap (the durable applied marker). On the sole-writer actor
            // this always affects 1 row; the guard makes a duplicate apply a no-op anyway.
            let n = tx.execute(
                "UPDATE invoice SET status='PAID', settled_at=?2, applied_at=?2 WHERE id=?1 AND status='OPEN'",
                params![inv_id, s.settled_at],
            )?;
            if n == 0 {
                return Ok(Capture::NoOp);
            }
            apply_paid(tx, kind.as_deref(), sub_id.as_deref(), expires_at, s, now)
        }
        // EXPIRED or any other terminal invoice status: funds arrived too late -> refund. Stamp
        // settled_at for audit, keep the terminal status, write exactly one refund intent. Journal
        // only when something actually changed so a redelivery doesn't append duplicate audit rows.
        _ => {
            let stamped = tx.execute(
                "UPDATE invoice SET settled_at=?2 WHERE id=?1 AND settled_at IS NULL",
                params![inv_id, s.settled_at],
            )?;
            let dest = sub_refund_dest(tx, sub_id.as_deref())?;
            let refunded = refund_intent(tx, sub_id.as_deref(), dest.as_deref(), s, now)?;
            if stamped > 0 || refunded {
                journal(tx, sub_id.as_deref(), "settle_terminal_refund", s, now)?;
            }
            Ok(Capture::RefundDue)
        }
    }
}

/// The subscription fields capture needs to route a paid invoice (`period_s`/`renew_lead_s`/
/// `retention_s` defaulted to 0 when NULL).
struct SubRow {
    state: String,
    period_s: i64,
    renew_lead_s: i64,
    retention_s: i64,
    paid_through: Option<i64>,
    refund_dest: Option<String>,
}

/// The invoice just flipped OPEN -> PAID. Route the subscription move by the invoice CLASS and the
/// current sub state.
fn apply_paid(
    tx: &Transaction,
    kind: Option<&str>,
    sub_id: Option<&str>,
    order_expires_at: Option<i64>,
    s: &Settlement,
    now: i64,
) -> Result<Capture> {
    let sub_id = match sub_id {
        Some(id) => id,
        // PAID invoice with no subscription (shouldn't happen) -> refund rather than strand funds.
        None => {
            refund_intent(tx, None, None, s, now)?;
            journal(tx, None, "settle_orphan_refund", s, now)?;
            return Ok(Capture::RefundDue);
        }
    };
    let sub: Option<SubRow> = tx
        .query_row(
            "SELECT state, period_s, renew_lead_s, retention_s, paid_through, refund_dest
             FROM subscription WHERE id = ?1",
            params![sub_id],
            |r| {
                Ok(SubRow {
                    state: r.get(0)?,
                    period_s: r.get::<_, Option<i64>>(1)?.unwrap_or(0),
                    renew_lead_s: r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                    retention_s: r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                    paid_through: r.get(4)?,
                    refund_dest: r.get(5)?,
                })
            },
        )
        .optional()?;
    let SubRow {
        state,
        period_s,
        renew_lead_s,
        retention_s,
        paid_through,
        refund_dest,
    } = match sub {
        Some(v) => v,
        None => {
            refund_intent(tx, Some(sub_id), None, s, now)?;
            journal(tx, Some(sub_id), "settle_orphan_refund", s, now)?;
            return Ok(Capture::RefundDue);
        }
    };

    // Route by the invoice CLASS (Some("order")/Some("renewal")) — a missing/unknown kind is NOT
    // assumed to be an order; it falls through to refund rather than silently capturing.
    match (kind, state.as_str()) {
        // First capture: PENDING -> PROVISIONING. paid_through is set later, at ACTIVE (lnrent-7fp.10).
        (Some("order"), "PENDING") => {
            // Settlement-expiry gate: funds arrived (the OPEN -> PAID CAS already ran), but if the
            // settlement lands at or after the order invoice's expiry the reservation was released
            // by reconcile, or is due to be; route to refund instead of provisioning. INCLUSIVE
            // boundary (`>=`), symmetric to the renewal retention gate below; a NULL expires_at
            // means no expiry, so provision as before. Like the retention/terminal arms this only
            // DETECTS the refund — it leaves the sub state untouched (capture never moves it here).
            if let Some(exp) = order_expires_at {
                if s.settled_at >= exp {
                    refund_intent(tx, Some(sub_id), refund_dest.as_deref(), s, now)?;
                    journal(tx, Some(sub_id), "settle_expired_refund", s, now)?;
                    return Ok(Capture::RefundDue);
                }
            }
            tx.execute(
                "UPDATE subscription SET state='PROVISIONING', updated_at=?2 WHERE id=?1",
                params![sub_id, now],
            )?;
            journal(tx, Some(sub_id), "capture_order", s, now)?;
            Ok(Capture::Captured)
        }
        // Renewal on a live (or resumable) sub: extend, and resume if it had lapsed to SUSPENDED.
        // paid_through = max(paid_through, settled_at) + period — early renewals stack, a late one
        // never lands in the past (§6.3).
        (Some("renewal"), st @ ("ACTIVE" | "SUSPENDED")) => {
            // Reconcile is periodic, so a sub can still read ACTIVE/SUSPENDED past its retention
            // end (paid_through + retention_s) before the TERMINATED flip lands. A settlement that
            // arrives at or after retention end is too late: the Instance is destroyed (or due to
            // be — reconcile fires `destroy` at `now >= retention end`, §6.5), so refund rather
            // than resurrect service. The boundary is INCLUSIVE-terminal to match that `>=`; the
            // retention window `[paid_through, paid_through + retention_s)` IS the late-renewal grace.
            if let Some(pt) = paid_through {
                if s.settled_at >= pt + retention_s {
                    refund_intent(tx, Some(sub_id), refund_dest.as_deref(), s, now)?;
                    journal(tx, Some(sub_id), "settle_terminal_refund", s, now)?;
                    return Ok(Capture::RefundDue);
                }
            }
            let new_paid_through =
                paid_through.unwrap_or(s.settled_at).max(s.settled_at) + period_s;
            let soft = new_paid_through - renew_lead_s;
            tx.execute(
                "UPDATE subscription
                   SET state='ACTIVE', paid_through=?2, soft_date=?3, next_deadline=?3, updated_at=?4
                 WHERE id=?1",
                params![sub_id, new_paid_through, soft, now],
            )?;
            let resumed = st == "SUSPENDED";
            journal(
                tx,
                Some(sub_id),
                if resumed {
                    "renew_resume"
                } else {
                    "renew_extend"
                },
                s,
                now,
            )?;
            Ok(if resumed {
                Capture::Resumed
            } else {
                Capture::Renewed
            })
        }
        // Settled-but-terminal: an order invoice whose sub already moved on, or a renewal on a
        // terminal sub. Funds arrived (invoice is PAID) but there's no service to grant -> refund,
        // and DO NOT resurrect the order.
        _ => {
            refund_intent(tx, Some(sub_id), refund_dest.as_deref(), s, now)?;
            journal(tx, Some(sub_id), "settle_terminal_refund", s, now)?;
            Ok(Capture::RefundDue)
        }
    }
}

/// Read a subscription's refund destination (BOLT12 offer / LN address), if the sub exists.
fn sub_refund_dest(tx: &Transaction, sub_id: Option<&str>) -> Result<Option<String>> {
    let Some(sub_id) = sub_id else {
        return Ok(None);
    };
    Ok(tx
        .query_row(
            "SELECT refund_dest FROM subscription WHERE id = ?1",
            params![sub_id],
            |r| r.get(0),
        )
        .optional()?
        .flatten())
}

/// Write the durable refund INTENT as a single `PENDING` row keyed by `refund:<external_id>`
/// (UNIQUE). `ON CONFLICT DO NOTHING` => a redelivered terminal settlement contributes exactly one
/// refund row (§6.6). Returns `true` iff a NEW row was inserted (so callers can skip a redundant
/// journal entry on replay). Execution is lnrent-7fp.11's job.
fn refund_intent(
    tx: &Transaction,
    sub_id: Option<&str>,
    dest: Option<&str>,
    s: &Settlement,
    now: i64,
) -> Result<bool> {
    let n = tx.execute(
        "INSERT INTO refund_attempt
            (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'PENDING', 0, ?6, ?6)
         ON CONFLICT(idempotency_key) DO NOTHING",
        params![
            format!("ref-{}", s.external_id),
            sub_id,
            dest,
            s.amount_sat as i64,
            format!("refund:{}", s.external_id),
            now
        ],
    )?;
    Ok(n > 0)
}

/// Journal a settlement event to `event_log` in the same txn (every mutation is journaled,
/// ADR-0001/§6.5).
fn journal(
    tx: &Transaction,
    sub_id: Option<&str>,
    kind: &str,
    s: &Settlement,
    now: i64,
) -> Result<()> {
    let detail = serde_json::json!({
        "external_id": s.external_id,
        "amount_sat": s.amount_sat,
        "settled_at": s.settled_at,
    })
    .to_string();
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?1, ?2, ?3, ?4)",
        params![sub_id, kind, detail, now],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::Settlement;
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    /// Seed a subscription + an invoice; returns the invoice's external_id.
    #[allow(clippy::too_many_arguments)]
    async fn seed(
        s: &Store,
        sub_id: &str,
        sub_state: &str,
        paid_through: Option<i64>,
        period_s: i64,
        renew_lead_s: i64,
        retention_s: i64,
        refund_dest: Option<&str>,
        inv_kind: &str,
        inv_status: &str,
        external_id: &str,
    ) {
        let (sub_id, sub_state, refund_dest, inv_kind, inv_status, ext) = (
            sub_id.to_string(),
            sub_state.to_string(),
            refund_dest.map(|s| s.to_string()),
            inv_kind.to_string(),
            inv_status.to_string(),
            external_id.to_string(),
        );
        s.transaction(move |tx| {
            tx.execute(
                "INSERT INTO subscription (id, state, period_s, renew_lead_s, retention_s, paid_through, refund_dest, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0)",
                params![sub_id, sub_state, period_s, renew_lead_s, retention_s, paid_through, refund_dest],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                 VALUES (?1, ?2, ?3, ?4, 1000, ?5, 0)",
                params![format!("inv-{ext}"), sub_id, ext, inv_kind, inv_status],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    }

    fn settlement(external_id: &str, settled_at: i64) -> Settlement {
        Settlement {
            invoice_id: format!("inv-{external_id}"),
            external_id: external_id.to_string(),
            amount_sat: 1000,
            settled_at,
        }
    }

    async fn sub_state(s: &Store, id: &str) -> String {
        let id = id.to_string();
        s.read(move |c| {
            Ok(c.query_row(
                "SELECT state FROM subscription WHERE id=?1",
                params![id],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap()
    }
    async fn inv_status(s: &Store, ext: &str) -> (String, Option<i64>) {
        let ext = ext.to_string();
        s.read(move |c| {
            Ok(c.query_row(
                "SELECT status, applied_at FROM invoice WHERE external_id=?1",
                params![ext],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap()
    }
    async fn refund_count(s: &Store) -> i64 {
        s.read(|c| Ok(c.query_row("SELECT count(*) FROM refund_attempt", [], |r| r.get(0))?))
            .await
            .unwrap()
    }
    /// Stamp an order invoice's bolt11 expiry (`seed` leaves `expires_at` NULL).
    async fn set_expiry(s: &Store, ext: &str, exp: i64) {
        let ext = ext.to_string();
        s.transaction(move |tx| {
            tx.execute(
                "UPDATE invoice SET expires_at=?2 WHERE external_id=?1",
                params![ext, exp],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn order_capture_moves_pending_to_provisioning() {
        let s = mem_store();
        seed(
            &s, "o1", "PENDING", None, 100, 10, 0, None, "order", "OPEN", "ext1",
        )
        .await;
        assert_eq!(
            capture(&s, settlement("ext1", 500), 1).await.unwrap(),
            Capture::Captured
        );
        assert_eq!(sub_state(&s, "o1").await, "PROVISIONING");
        let (st, applied) = inv_status(&s, "ext1").await;
        assert_eq!(st, "PAID");
        assert_eq!(applied, Some(500), "applied_at stamped from the settlement");
        assert_eq!(refund_count(&s).await, 0);
    }

    // Settlement-expiry gate: an order settlement landing AT or AFTER the invoice's expiry is too
    // late (the reservation was, or is due to be, released by reconcile) -> refund, not provision.
    // Boundary is INCLUSIVE (`>=`), symmetric to the retention gate; like that gate it only DETECTS
    // the refund, so the sub state is left untouched (stays PENDING, NOT PROVISIONING).
    #[tokio::test]
    async fn order_settlement_at_or_after_expiry_refunds_not_provisions() {
        // expiry E = 1000: exercise the inclusive boundary (settled_at == E) and strictly after.
        for settled_at in [1000, 1500] {
            let s = mem_store();
            seed(
                &s,
                "o1",
                "PENDING",
                None,
                100,
                10,
                0,
                Some("lnaddr@x"),
                "order",
                "OPEN",
                "ext1",
            )
            .await;
            set_expiry(&s, "ext1", 1000).await;
            assert_eq!(
                capture(&s, settlement("ext1", settled_at), 1)
                    .await
                    .unwrap(),
                Capture::RefundDue,
                "settled_at={settled_at} >= expiry -> refund"
            );
            assert_eq!(
                sub_state(&s, "o1").await,
                "PENDING",
                "settled_at={settled_at}: an expired order does NOT provision"
            );
            // Funds arrived, so the OPEN->PAID CAS still flipped the invoice; one refund recorded
            // (with the sub's dest, like the other refund paths).
            assert_eq!(inv_status(&s, "ext1").await.0, "PAID");
            assert_eq!(
                refund_count(&s).await,
                1,
                "settled_at={settled_at}: exactly one refund for the expired order"
            );
        }
    }

    // One second before expiry is in time -> provision as today (unchanged from the no-expiry path).
    #[tokio::test]
    async fn order_settlement_before_expiry_provisions() {
        let s = mem_store();
        seed(
            &s,
            "o1",
            "PENDING",
            None,
            100,
            10,
            0,
            Some("lnaddr@x"),
            "order",
            "OPEN",
            "ext1",
        )
        .await;
        set_expiry(&s, "ext1", 1000).await;
        assert_eq!(
            capture(&s, settlement("ext1", 999), 1).await.unwrap(),
            Capture::Captured,
            "one second before expiry still provisions"
        );
        assert_eq!(sub_state(&s, "o1").await, "PROVISIONING");
        assert_eq!(inv_status(&s, "ext1").await.0, "PAID");
        assert_eq!(refund_count(&s).await, 0);
    }

    // A NULL expires_at means NO expiry is enforced -> provision even far in the future.
    #[tokio::test]
    async fn order_settlement_with_null_expiry_provisions() {
        let s = mem_store();
        seed(
            &s, "o1", "PENDING", None, 100, 10, 0, None, "order", "OPEN", "ext1",
        )
        .await; // seed leaves expires_at NULL
        assert_eq!(
            capture(&s, settlement("ext1", 1_000_000), 1).await.unwrap(),
            Capture::Captured,
            "a NULL expires_at never expires -> provision"
        );
        assert_eq!(sub_state(&s, "o1").await, "PROVISIONING");
        assert_eq!(refund_count(&s).await, 0);
    }

    #[tokio::test]
    async fn replayed_settlement_is_a_noop() {
        let s = mem_store();
        seed(
            &s, "o1", "PENDING", None, 100, 10, 0, None, "order", "OPEN", "ext1",
        )
        .await;
        capture(&s, settlement("ext1", 500), 1).await.unwrap();
        // A redelivery of the same settlement changes nothing.
        assert_eq!(
            capture(&s, settlement("ext1", 999), 2).await.unwrap(),
            Capture::NoOp
        );
        assert_eq!(sub_state(&s, "o1").await, "PROVISIONING");
        let (_, applied) = inv_status(&s, "ext1").await;
        assert_eq!(
            applied,
            Some(500),
            "applied_at is NOT overwritten by the replay"
        );
    }

    #[tokio::test]
    async fn renewal_extends_paid_through_with_max_formula() {
        let s = mem_store();
        // ACTIVE, paid_through=1000, period=100. An EARLY renewal (settled_at < paid_through).
        seed(
            &s,
            "o1",
            "ACTIVE",
            Some(1000),
            100,
            10,
            1000,
            None,
            "renewal",
            "OPEN",
            "rext",
        )
        .await;
        assert_eq!(
            capture(&s, settlement("rext", 500), 1).await.unwrap(),
            Capture::Renewed
        );
        let pt: i64 = {
            let id = "o1".to_string();
            s.read(move |c| {
                Ok(c.query_row(
                    "SELECT paid_through FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
        };
        assert_eq!(pt, 1100, "max(1000,500)+100");
        assert_eq!(sub_state(&s, "o1").await, "ACTIVE");
    }

    #[tokio::test]
    async fn late_renewal_never_lands_in_the_past() {
        let s = mem_store();
        // paid_through already lapsed (100) but the sub is still ACTIVE; settled_at=1000 is later.
        seed(
            &s,
            "o1",
            "ACTIVE",
            Some(100),
            100,
            10,
            2000,
            None,
            "renewal",
            "OPEN",
            "rext",
        )
        .await;
        capture(&s, settlement("rext", 1000), 1).await.unwrap();
        let pt: i64 = {
            let id = "o1".to_string();
            s.read(move |c| {
                Ok(c.query_row(
                    "SELECT paid_through FROM subscription WHERE id=?1",
                    params![id],
                    |r| r.get(0),
                )?)
            })
            .await
            .unwrap()
        };
        assert_eq!(
            pt, 1100,
            "max(100,1000)+100 — settled_at, not the stale paid_through"
        );
    }

    #[tokio::test]
    async fn renewal_resumes_a_suspended_sub() {
        let s = mem_store();
        seed(
            &s,
            "o1",
            "SUSPENDED",
            Some(100),
            100,
            10,
            2000,
            None,
            "renewal",
            "OPEN",
            "rext",
        )
        .await;
        assert_eq!(
            capture(&s, settlement("rext", 1000), 1).await.unwrap(),
            Capture::Resumed
        );
        assert_eq!(
            sub_state(&s, "o1").await,
            "ACTIVE",
            "a paid renewal resumes a suspended sub"
        );
    }

    // A renewal that lands AFTER retention (paid_through + retention_s) must refund, not resurrect
    // — even while the row still reads ACTIVE/SUSPENDED because reconcile hasn't flipped it to
    // TERMINATED yet (codex re-review: the after-retention renewal gate).
    #[tokio::test]
    async fn renewal_past_retention_refunds_not_resurrects() {
        for state in ["ACTIVE", "SUSPENDED"] {
            let s = mem_store();
            // paid_through=100, retention=50 -> retention ends at 150; settlement at 200 is too late.
            seed(
                &s,
                "o1",
                state,
                Some(100),
                100,
                10,
                50,
                Some("lnaddr@x"),
                "renewal",
                "OPEN",
                "rext",
            )
            .await;
            assert_eq!(
                capture(&s, settlement("rext", 200), 1).await.unwrap(),
                Capture::RefundDue,
                "{state}: a past-retention renewal refunds"
            );
            assert_eq!(
                sub_state(&s, "o1").await,
                state,
                "{state}: the sub is NOT resurrected to ACTIVE"
            );
            // Invoice still flipped PAID (funds arrived) and exactly one refund recorded.
            assert_eq!(inv_status(&s, "rext").await.0, "PAID");
            assert_eq!(
                refund_count(&s).await,
                1,
                "{state}: one refund for the late renewal"
            );
        }
    }

    // The retention boundary is inclusive-terminal: a renewal at exactly paid_through+retention_s
    // refunds (reconcile fires `destroy` at `now >= retention end`), one second earlier renews.
    #[tokio::test]
    async fn retention_boundary_is_inclusive_terminal() {
        // paid_through=100, retention=50 -> retention end at 150.
        let at_boundary = mem_store();
        seed(
            &at_boundary,
            "o1",
            "ACTIVE",
            Some(100),
            100,
            10,
            50,
            Some("d"),
            "renewal",
            "OPEN",
            "rext",
        )
        .await;
        assert_eq!(
            capture(&at_boundary, settlement("rext", 150), 1)
                .await
                .unwrap(),
            Capture::RefundDue,
            "settling exactly at retention end is too late -> refund"
        );

        let just_inside = mem_store();
        seed(
            &just_inside,
            "o2",
            "ACTIVE",
            Some(100),
            100,
            10,
            50,
            Some("d"),
            "renewal",
            "OPEN",
            "rin",
        )
        .await;
        assert_eq!(
            capture(&just_inside, settlement("rin", 149), 1)
                .await
                .unwrap(),
            Capture::Renewed,
            "one second inside the window still renews"
        );
    }

    // A settlement on an invoice with NULL/unknown kind is NOT assumed to be an order: it must not
    // capture/provision — it refunds (codex re-review: route by explicit invoice class).
    #[tokio::test]
    async fn null_kind_invoice_does_not_capture() {
        let s = mem_store();
        s.transaction(|tx| {
            tx.execute(
                "INSERT INTO subscription (id, state, period_s, renew_lead_s, retention_s, created_at, updated_at)
                 VALUES ('o1', 'PENDING', 100, 10, 0, 0, 0)",
                [],
            )?;
            tx.execute(
                "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                 VALUES ('inv-ext1', 'o1', 'ext1', NULL, 1000, 'OPEN', 0)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(
            capture(&s, settlement("ext1", 500), 1).await.unwrap(),
            Capture::RefundDue
        );
        assert_eq!(
            sub_state(&s, "o1").await,
            "PENDING",
            "a null-kind invoice never captures the order"
        );
        assert_eq!(refund_count(&s).await, 1);
    }

    #[tokio::test]
    async fn settlement_on_an_expired_invoice_refunds_once() {
        let s = mem_store();
        seed(
            &s,
            "o1",
            "PENDING",
            None,
            100,
            10,
            0,
            Some("lnaddr@x"),
            "order",
            "EXPIRED",
            "ext1",
        )
        .await;
        assert_eq!(
            capture(&s, settlement("ext1", 500), 1).await.unwrap(),
            Capture::RefundDue
        );
        // Invoice stays EXPIRED (not flipped to PAID) but is stamped for audit.
        let (st, _) = inv_status(&s, "ext1").await;
        assert_eq!(st, "EXPIRED");
        assert_eq!(refund_count(&s).await, 1);
        // A redelivery does NOT create a second refund (UNIQUE refund key).
        assert_eq!(
            capture(&s, settlement("ext1", 600), 2).await.unwrap(),
            Capture::RefundDue
        );
        assert_eq!(
            refund_count(&s).await,
            1,
            "exactly one refund row per terminal settlement"
        );
        // The refund row carries the sub's dest + the deterministic key.
        let (dest, key): (Option<String>, String) = {
            s.read(|c| {
                Ok(c.query_row(
                    "SELECT dest, idempotency_key FROM refund_attempt WHERE subscription_id='o1'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap()
        };
        assert_eq!(dest.as_deref(), Some("lnaddr@x"));
        assert_eq!(key, "refund:ext1");
    }

    #[tokio::test]
    async fn settlement_on_a_terminal_sub_pays_invoice_then_refunds() {
        let s = mem_store();
        // An OPEN order invoice whose sub already TERMINATED.
        seed(
            &s,
            "o1",
            "TERMINATED",
            None,
            100,
            10,
            0,
            Some("lnaddr@x"),
            "order",
            "OPEN",
            "ext1",
        )
        .await;
        assert_eq!(
            capture(&s, settlement("ext1", 500), 1).await.unwrap(),
            Capture::RefundDue
        );
        let (st, _) = inv_status(&s, "ext1").await;
        assert_eq!(st, "PAID", "funds arrived, so the invoice is PAID...");
        assert_eq!(
            sub_state(&s, "o1").await,
            "TERMINATED",
            "...but the order is not resurrected"
        );
        assert_eq!(refund_count(&s).await, 1);
    }

    #[tokio::test]
    async fn unmatched_settlement_records_a_refund_intent() {
        let s = mem_store();
        // No invoice for this external_id at all.
        assert_eq!(
            capture(&s, settlement("ghost", 500), 1).await.unwrap(),
            Capture::RefundDue
        );
        assert_eq!(refund_count(&s).await, 1);
        let (sub, key): (Option<String>, String) = {
            s.read(|c| {
                Ok(c.query_row(
                    "SELECT subscription_id, idempotency_key FROM refund_attempt",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap()
        };
        assert_eq!(sub, None, "no sub known for an unmatched settlement");
        assert_eq!(key, "refund:ghost");
    }
}
