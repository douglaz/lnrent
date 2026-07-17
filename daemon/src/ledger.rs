//! Ledger-authoritative money core (lnrent-urw.10, spec §D): `expected_msat`, the LOCAL sqlite
//! lower bound on what the ecash wallet SHOULD hold. Pure local reads — NO federation balance
//! call — so it is the operand every AUTOMATIC money path uses (refund readiness, §E) and the
//! books figure the operator `reconcile` command (§F) compares the real wallet against. The single
//! sanctioned live-balance read is the reconcile handler in `ipc.rs`; this module never reads it.
//!
//! `expected_msat = Σ wallet-credited receipts − Σ committed refund caps − Σ sweep caps`, a
//! CONSERVATIVE LOWER bound (≤ the real spendable wallet), `u128` with saturating subtraction
//! (never underflows). Receipts use exact `received_msat`: gross for lnv1/mock, invoice minus the
//! gateway receive fee for lnv2. A committed refund subtracts its whole-sat REFUNDABLE WALLET-CREDIT
//! cap (`refund_attempt.amount_sat`; `received_msat / 1000` for lnv2, legacy gross otherwise), while a
//! sweep subtracts its MAX-outlay cap. INV-1 makes each cap ≥ the REAL outlay: `pay_refund_capped`
//! refuses a debit above the refund cap, and capped sweep does the same for `max_outlay_msat`. Any
//! sub-sat receive-credit remainder was never authorized for refund and stays on the receipt side.
//! Subtracting the cap is therefore conservative — `expected_msat` never sits ABOVE the real wallet.
//! The wallet legitimately runs ABOVE this floor (fee savings run it up), which is exactly why
//! `reconcile` reads wallet ≥ expected as OK and only wallet < expected as DRIFT (a genuine loss /
//! accounting gap for a human).
//! Reading the balance in automatic paths creates reconciliation races and an automatic
//! balance-query failure class; the ledger is the same history the balance aggregates, on a clock
//! we control (ADR-0016 / §E rationale).

use crate::backends::{PayStatus, PaymentBackend};
use crate::refund::{external_id_from, gen_key};
use crate::store::{Store, SETTLE_REFUND_KINDS_SQL};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;

/// A refund_attempt row that MIGHT lock funds out of the spendable wallet. Read locally, then
/// classified async: a `SENT` row, or one whose CURRENT-generation pay has durable started evidence
/// in the local pay index, has committed its funds and is subtracted.
struct RefundCommitment {
    external_id: String,
    resolution_gen: i64,
    amount_msat: u128,
    status: String,
}

/// The three §D terms read in ONE store pass. Receipts + sweep caps are final; the refund rows
/// still need the async started-evidence probe before they can be subtracted.
struct LedgerReads {
    receipts_msat: u128,
    refunds: Vec<RefundCommitment>,
    sweep_caps_msat: u128,
}

/// The ledger's conservative lower bound on spendable wallet holdings, in msats (spec §D).
///
/// Pure LOCAL: the sqlite ledger + the local pay index (`payment_started_by_key`). Makes NO
/// federation balance call. `u128` throughout with saturating subtraction, so the result is ≥ 0.
pub async fn expected_msat(store: &Store, payment: &Arc<dyn PaymentBackend>) -> Result<u128> {
    let reads = store.read(read_ledger_terms).await?;

    // Subtract the refunds whose funds are already committed. A started-but-PENDING refund MUST be
    // subtracted: the outgoing contract has locked those funds even before the row flips to SENT.
    // Terminal Failed means funds returned and a retry still needs liquidity, so it is not
    // subtracted even though a historical pay-index row exists.
    let mut committed_msat: u128 = 0;
    for r in &reads.refunds {
        let committed = if r.status == "SENT" {
            true
        } else {
            // The SAME started-evidence disambiguator INV-2/recovery use: the evidence lives under
            // the CURRENT-generation pay key the BACKEND saw (`refund:<ext>` for gen 0,
            // `refund:<ext>:g<n>` for gen>=1), NOT the stable `idempotency_key` ledger anchor.
            let key = gen_key(&r.external_id, r.resolution_gen);
            match payment.payment_status_by_key(&key).await? {
                PayStatus::Succeeded | PayStatus::Pending => true,
                PayStatus::Failed => false,
                PayStatus::Unknown => payment.payment_started_by_key(&key).await?,
            }
        };
        if committed {
            committed_msat = committed_msat.saturating_add(r.amount_msat);
        }
    }

    Ok(reads
        .receipts_msat
        .saturating_sub(committed_msat)
        .saturating_sub(reads.sweep_caps_msat))
}

fn read_ledger_terms(conn: &Connection) -> Result<LedgerReads> {
    Ok(LedgerReads {
        receipts_msat: sum_receipts_msat(conn)?,
        refunds: load_refund_commitments(conn)?,
        sweep_caps_msat: sum_sweep_caps_msat(conn)?,
    })
}

/// Σ actual wallet credit of every captured receipt, de-duped by external payment id across BOTH
/// INV-3 provenance classes and counted ONCE each. Legacy rows fall back to `gross_sat * 1000`.
/// `pub(crate)`: the operator sweep (gate1-operator-sweep, urw.3) reuses the IDENTICAL receipt base
/// for its surplus `earned` term, so the sweep can never authorize against different provenance.
pub(crate) fn sum_receipts_msat(conn: &Connection) -> Result<u128> {
    // Class A — settled invoice rows. `invoice.external_id` is UNIQUE, so no intra-class dup.
    let mut received_by_ext: HashMap<String, u128> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT external_id,
                COALESCE(received_msat, CASE WHEN amount_sat > 0 THEN amount_sat * 1000 END)
           FROM invoice
          WHERE status = 'PAID' OR settled_at IS NOT NULL",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, received_msat) = row?;
        if let Some(msat) = positive_msat(received_msat) {
            received_by_ext.insert(external_id, msat);
        }
    }

    // Class B — settle-refund journal entries. A receipt already counted in Class A keeps its invoice
    // credit (precedence via `or_insert`), so one present in BOTH classes counts once. MAX makes a
    // redelivered settlement deterministic and ignores a malformed duplicate.
    let class_b_sql = format!(
        "SELECT json_extract(detail_json, '$.external_id') AS external_id,
                MAX(COALESCE(
                    CAST(json_extract(detail_json, '$.received_msat') AS INTEGER),
                    CAST(json_extract(detail_json, '$.amount_sat') AS INTEGER) * 1000
                )) AS received_msat
           FROM event_log
          WHERE kind IN ({SETTLE_REFUND_KINDS_SQL})
          GROUP BY external_id"
    );
    let mut stmt = conn.prepare(&class_b_sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, Option<String>>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, received_msat) = row?;
        let (Some(external_id), Some(msat)) = (external_id, positive_msat(received_msat)) else {
            continue;
        };
        received_by_ext.entry(external_id).or_insert(msat);
    }

    Ok(received_by_ext.values().copied().sum())
}

/// Every refund_attempt row that MIGHT have committed funds. FAILED rows are included and filtered
/// out by the started-evidence probe in [`expected_msat`]; a row with no whole-sat amount subtracts
/// nothing, so it is skipped here.
fn load_refund_commitments(conn: &Connection) -> Result<Vec<RefundCommitment>> {
    let mut stmt = conn.prepare(
        "SELECT id, idempotency_key, amount_sat, status, resolution_gen
           FROM refund_attempt",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<i64>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<i64>>(4)?.unwrap_or(0),
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, idempotency_key, amount_sat, status, resolution_gen) = row?;
        let Some(sat) = positive_sat(amount_sat) else {
            continue;
        };
        out.push(RefundCommitment {
            external_id: external_id_from(&idempotency_key, &id),
            resolution_gen,
            amount_msat: u128::from(sat) * 1000,
            status,
        });
    }
    Ok(out)
}

/// Σ `max_outlay_msat` of in-flight (SENT/PENDING) sweep rows — operator payouts that have locked
/// funds out of the wallet. The `sweep_attempt` table is owned by urw.3 and does NOT exist yet, so
/// probe `sqlite_master` FIRST: absent ⇒ this term is 0 (querying a missing table would panic).
/// Keeps the helper forward-complete for when urw.3 lands.
fn sum_sweep_caps_msat(conn: &Connection) -> Result<u128> {
    let sweep_table_present: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sweep_attempt'",
        [],
        |r| r.get(0),
    )?;
    if sweep_table_present == 0 {
        return Ok(0);
    }
    let sum_msat: i64 = conn.query_row(
        "SELECT COALESCE(SUM(max_outlay_msat), 0)
           FROM sweep_attempt
          WHERE status IN ('SENT', 'PENDING')",
        [],
        |r| r.get(0),
    )?;
    Ok(u128::try_from(sum_msat).unwrap_or(0))
}

/// A whole-sat amount only if strictly positive — a NULL or non-positive receipt/refund contributes
/// nothing, and skipping it only lowers the bound (conservative). `pub(crate)`: the sweep surplus
/// (urw.3) filters its reserved/paid-out amounts with the SAME positivity rule.
pub(crate) fn positive_sat(amount: Option<i64>) -> Option<u64> {
    match amount {
        Some(a) if a > 0 => Some(a as u64),
        _ => None,
    }
}

fn positive_msat(amount: Option<i64>) -> Option<u128> {
    positive_sat(amount).map(u128::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{Invoice, PayStatus, PaymentStatus, Settlement};
    use crate::store::{Store, SCHEMA};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    /// A payment double whose `payment_started_by_key` is steerable and whose
    /// `available_balance_msat` PANICS — `expected_msat` must never read the federation balance.
    #[derive(Default)]
    struct StartedPayment {
        started: StdMutex<HashSet<String>>,
        statuses: StdMutex<HashMap<String, PayStatus>>,
    }

    impl StartedPayment {
        fn set_started(&self, key: &str) {
            self.started.lock().unwrap().insert(key.to_string());
        }
        fn set_status(&self, key: &str, status: PayStatus) {
            self.statuses
                .lock()
                .unwrap()
                .insert(key.to_string(), status);
        }
    }

    #[async_trait]
    impl PaymentBackend for StartedPayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("ledger tests do not create invoices")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!("ledger tests do not look up invoices")
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            unimplemented!("ledger tests do not look up settlements")
        }
        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            unimplemented!("ledger tests do not pay")
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!("ledger tests check by key")
        }
        async fn payment_status_by_key(&self, key: &str) -> Result<PayStatus> {
            Ok(*self
                .statuses
                .lock()
                .unwrap()
                .get(key)
                .unwrap_or(&PayStatus::Unknown))
        }
        async fn payment_started_by_key(&self, key: &str) -> Result<bool> {
            Ok(self.started.lock().unwrap().contains(key))
        }
        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            panic!("expected_msat must never read the federation balance")
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("ledger tests do not watch settlements")
        }
    }

    fn store_with(setup: impl FnOnce(&Connection)) -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        setup(&conn);
        Store::spawn(conn)
    }

    fn no_start_payment() -> Arc<dyn PaymentBackend> {
        Arc::new(StartedPayment::default())
    }

    #[tokio::test]
    async fn expected_sums_both_provenance_classes_and_dedups_shared_receipt() {
        let store = store_with(|c| {
            // Class A only: a settled invoice (5 sat).
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i-a', 'extA', 'order', 5, 'PAID')",
                [],
            )
            .unwrap();
            // Class B only: a settle-refund journal entry (7 sat).
            c.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                 VALUES ('s', 'settle_unmatched_refund', '{\"external_id\":\"extB\",\"amount_sat\":7}', 0)",
                [],
            )
            .unwrap();
            // Present in BOTH classes (extC, 3 sat): must be counted ONCE.
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i-c', 'extC', 'order', 3, 'PAID')",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                 VALUES ('s', 'settle_terminal_refund', '{\"external_id\":\"extC\",\"amount_sat\":3}', 0)",
                [],
            )
            .unwrap();
        });

        // 5 + 7 + 3 = 15 sat gross (extC counted once), no refunds/sweeps.
        assert_eq!(
            expected_msat(&store, &no_start_payment()).await.unwrap(),
            15_000
        );
    }

    // A settlement REDELIVERED (fedimint reconnect) re-journals the same external_id. The receipt
    // must count ONCE with the real amount, deterministically — even if a duplicate row carries a
    // NULL/malformed amount, MAX picks the real positive value (codex-adversarial robustness).
    #[tokio::test]
    async fn redelivered_class_b_receipt_counts_once_deterministically() {
        let store = store_with(|c| {
            for detail in [
                "{\"external_id\":\"extR\",\"amount_sat\":9}",
                "{\"external_id\":\"extR\",\"amount_sat\":9}", // exact redelivery
                "{\"external_id\":\"extR\"}", // malformed dup: amount absent -> NULL
            ] {
                c.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at)
                     VALUES ('s', 'settle_orphan_refund', ?1, 0)",
                    [detail],
                )
                .unwrap();
            }
        });
        // Counted once at 9 sat (the NULL dup does not zero it out).
        assert_eq!(
            expected_msat(&store, &no_start_payment()).await.unwrap(),
            9_000
        );
    }

    #[tokio::test]
    async fn sent_and_started_refunds_subtract_but_failed_terminal_and_unstarted_do_not() {
        let store = store_with(|c| {
            for ext in ["sent", "started", "failed", "terminal_failed"] {
                c.execute(
                    &format!(
                        "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                         VALUES ('i-{ext}', '{ext}', 'order', 10, 'PAID')"
                    ),
                    [],
                )
                .unwrap();
            }
            // SENT (subtract), PENDING-but-started (subtract), FAILED-not-started (do NOT subtract).
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r-sent', 'd', 10, 'refund:sent', 'SENT', 1, 0)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r-started', 'd', 10, 'refund:started', 'PENDING', 1, 0)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r-failed', 'd', 10, 'refund:failed', 'FAILED', 3, 0)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r-terminal-failed', 'd', 10, 'refund:terminal_failed', 'PENDING', 1, 0)",
                [],
            )
            .unwrap();
        });
        let dbl = Arc::new(StartedPayment::default());
        dbl.set_started("refund:started"); // gen 0 → the pay key equals the idempotency anchor
        dbl.set_started("refund:terminal_failed");
        dbl.set_status("refund:terminal_failed", PayStatus::Failed);
        let payment: Arc<dyn PaymentBackend> = dbl;

        // 40 gross - 10 (SENT) - 10 (started Unknown) = 20 sat; failed/unstarted and terminal
        // Failed pay-index rows are NOT subtracted because their funds are available for retry.
        assert_eq!(expected_msat(&store, &payment).await.unwrap(), 20_000);
    }

    #[tokio::test]
    async fn sweep_caps_subtract_only_in_flight_rows_when_table_present() {
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i1', 'ext', 'order', 100, 'PAID')",
                [],
            )
            .unwrap();
            // urw.3 now owns this table in the base SCHEMA (bolt11/amount_sat nullable); IF NOT EXISTS
            // keeps this forward-compat setup a harmless no-op against the real table.
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS sweep_attempt (
                   id TEXT PRIMARY KEY, status TEXT NOT NULL, max_outlay_msat INTEGER NOT NULL
                 );",
            )
            .unwrap();
            c.execute(
                "INSERT INTO sweep_attempt (id, status, max_outlay_msat) VALUES ('sw-sent', 'SENT', 20000)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO sweep_attempt (id, status, max_outlay_msat) VALUES ('sw-pending', 'PENDING', 5000)",
                [],
            )
            .unwrap();
            // A terminal sweep does NOT lock funds — it must be ignored.
            c.execute(
                "INSERT INTO sweep_attempt (id, status, max_outlay_msat) VALUES ('sw-failed', 'FAILED', 999999)",
                [],
            )
            .unwrap();
        });

        // 100_000 msat − (20000 + 5000) = 75_000 msat; the FAILED sweep is not subtracted.
        assert_eq!(
            expected_msat(&store, &no_start_payment()).await.unwrap(),
            75_000
        );
    }

    #[tokio::test]
    async fn expected_does_not_panic_when_sweep_table_absent() {
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i1', 'ext', 'order', 2, 'PAID')",
                [],
            )
            .unwrap();
        });

        // No `sweep_attempt` table (the default schema) → the sweep term is 0, no panic.
        assert_eq!(
            expected_msat(&store, &no_start_payment()).await.unwrap(),
            2_000
        );
    }

    #[tokio::test]
    async fn expected_saturates_at_zero_when_outflows_exceed_receipts() {
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i1', 'ext', 'order', 1, 'PAID')",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r', 'd', 5, 'refund:ext', 'SENT', 1, 0)",
                [],
            )
            .unwrap();
        });

        // 1_000 receipts − 5_000 SENT refund → saturates at 0 (never underflows below zero).
        assert_eq!(expected_msat(&store, &no_start_payment()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn expected_uses_actual_receive_credit_not_gross_invoice_amount() {
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice
                    (id, external_id, kind, amount_sat, received_msat, status)
                 VALUES ('i-fee', 'ext-fee', 'order', 1000, 995500, 'PAID')",
                [],
            )
            .unwrap();
        });

        assert_eq!(
            expected_msat(&store, &no_start_payment()).await.unwrap(),
            995_500,
            "the gateway's inbound fee is not counted as spendable wallet holdings"
        );
    }
}
