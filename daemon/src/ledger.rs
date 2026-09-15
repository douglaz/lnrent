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

/// **Committed** (CONTEXT.md § Billing), defined ONCE and re-derived per read, never stored: the
/// books must assume this Attempt's money has left the wallet or is locked out of it — it was sent,
/// it is Fenced (ADR-0022: the witness that would let the backend answer may be lost, and the legacy
/// payment may have landed), or the backend holds a pay witness that is pending or succeeded. A
/// conservative exclusion, not proof the funds moved. A terminal `Failed` returns the funds, so a
/// started-then-failed Attempt is NOT committed and a retry still needs liquidity.
///
/// There is no "started evidence" distinct from the status read: both shipped backends answer
/// `payment_status_by_key` from the same pay row they would have answered "started" from, and each
/// backend's `map_pay_status` covers every status it writes, so `Unknown` IS "no row"
/// (lnrent-2v2v).
pub(crate) fn committed(status: &str, fenced: bool, pay: PayStatus) -> bool {
    status == "SENT" || fenced || matches!(pay, PayStatus::Pending | PayStatus::Succeeded)
}

/// The ONE production read of a backend's pay witness for an Attempt's pay key (a refund's
/// `gen_key`, a sweep's id). Drivers call this for their transition decisions — they need the
/// four-way [`PayStatus`], never a money predicate — and every money reader goes through
/// [`observe`]. `clippy.toml` refuses the bare trait method everywhere else.
#[allow(clippy::disallowed_methods)] // the sanctioned delegate
pub(crate) async fn attempt_pay_status(payment: &dyn PaymentBackend, key: &str) -> Result<PayStatus> {
    payment.payment_status_by_key(key).await
}

/// One refund Attempt's state as the books see it: lifecycle status, fence, and the backend's pay
/// witness under its CURRENT-generation pay key (`refund:<ext>` for gen 0, `refund:<ext>:g<n>` for
/// gen>=1 — the key the BACKEND saw, not the stable `idempotency_key` ledger anchor). The amount is
/// the row's refundable wallet-credit cap; it is 0 for a row with no whole-sat amount, which then
/// subtracts nothing but still carries its witness for readiness.
pub(crate) struct RefundCommitment {
    pub(crate) idempotency_key: String,
    pay_key: String,
    amount_msat: u128,
    status: String,
    fenced: bool,
    /// Filled by [`observe`]; `Unknown` until then.
    pub(crate) pay: PayStatus,
}

impl RefundCommitment {
    pub(crate) fn committed(&self) -> bool {
        committed(&self.status, self.fenced, self.pay)
    }
}

/// The §D terms read in ONE store pass. Receipts + sweep caps are final; the refund rows still need
/// the backend witness ([`observe`]) before they can be classified.
pub(crate) struct LedgerReads {
    receipts_msat: u128,
    refunds: Vec<RefundCommitment>,
    sweep_caps_msat: u128,
}

/// [`LedgerReads`] after ONE observation of every refund row's pay witness. A report that needs
/// both expected holdings and per-refund classification (readiness) builds this once and reads
/// both from it, so a witness that flips between two probes can never land on both sides of
/// `expected < required` (lnrent-4br3).
pub(crate) struct LedgerSnapshot {
    reads: LedgerReads,
}

impl LedgerSnapshot {
    /// `Σ receipts − Σ committed refund caps − Σ sweep caps`, saturating at 0 (spec §D).
    pub(crate) fn expected_msat(&self) -> u128 {
        let committed_msat = self
            .reads
            .refunds
            .iter()
            .filter(|r| r.committed())
            .fold(0u128, |acc, r| acc.saturating_add(r.amount_msat));
        self.reads
            .receipts_msat
            .saturating_sub(committed_msat)
            .saturating_sub(self.reads.sweep_caps_msat)
    }

    /// The refund row with this ledger anchor (`refund_attempt.idempotency_key`, UNIQUE), if it was
    /// present when the snapshot was read.
    // ponytail: linear scan per lookup; index by key if refund rows ever number in the thousands.
    pub(crate) fn refund(&self, idempotency_key: &str) -> Option<&RefundCommitment> {
        self.reads
            .refunds
            .iter()
            .find(|r| r.idempotency_key == idempotency_key)
    }
}

/// The ledger's conservative lower bound on spendable wallet holdings, in msats (spec §D).
///
/// Pure LOCAL: the sqlite ledger + the backend's local pay index. Makes NO federation balance call.
/// `u128` throughout with saturating subtraction, so the result is ≥ 0.
pub async fn expected_msat(store: &Store, payment: &Arc<dyn PaymentBackend>) -> Result<u128> {
    let reads = store.read(read_ledger_terms).await?;
    Ok(observe(reads, payment.as_ref()).await?.expected_msat())
}

/// Probe each refund row's pay witness exactly ONCE. A row already Committed by its status or its
/// fence is not probed: the witness could not change the answer, and a lookup error on it must not
/// fail a report that never needed it.
pub(crate) async fn observe(
    mut reads: LedgerReads,
    payment: &dyn PaymentBackend,
) -> Result<LedgerSnapshot> {
    for r in &mut reads.refunds {
        if !r.committed() {
            r.pay = attempt_pay_status(payment, &r.pay_key).await?;
        }
    }
    Ok(LedgerSnapshot { reads })
}

pub(crate) fn read_ledger_terms(conn: &Connection) -> Result<LedgerReads> {
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

/// Every refund_attempt row. FAILED rows are included and excluded by [`committed`]; a row with no
/// whole-sat amount carries `amount_msat: 0` (subtracts nothing) so readiness can still find it.
fn load_refund_commitments(conn: &Connection) -> Result<Vec<RefundCommitment>> {
    let mut stmt = conn.prepare(
        "SELECT id, idempotency_key, amount_sat, status, resolution_gen,
                migration_unverified_at IS NOT NULL
           FROM refund_attempt",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<i64>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<i64>>(4)?.unwrap_or(0),
            r.get::<_, bool>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, idempotency_key, amount_sat, status, resolution_gen, fenced) = row?;
        out.push(RefundCommitment {
            pay_key: gen_key(&external_id_from(&idempotency_key, &id), resolution_gen),
            idempotency_key,
            amount_msat: positive_sat(amount_sat).map_or(0, |sat| u128::from(sat) * 1000),
            status,
            fenced,
            pay: PayStatus::Unknown,
        });
    }
    Ok(out)
}

/// Σ `max_outlay_msat` of in-flight (SENT/PENDING) sweep rows — operator payouts that have locked
/// funds out of the wallet — plus every ADR-0022-fenced row whatever its status (its legacy payment
/// may have landed; the sweep surplus counts it the same way). The `sweep_attempt` table is owned by urw.3 and does NOT exist yet, so
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
          WHERE status IN ('SENT', 'PENDING') OR migration_unverified_at IS NOT NULL",
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
    use crate::store::Store;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    /// A payment double whose per-key `payment_status_by_key` is steerable and whose
    /// `available_balance_msat` PANICS — `expected_msat` must never read the federation balance.
    #[derive(Default)]
    struct StartedPayment {
        statuses: StdMutex<HashMap<String, PayStatus>>,
    }

    impl StartedPayment {
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
        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            panic!("expected_msat must never read the federation balance")
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("ledger tests do not watch settlements")
        }
    }

    fn store_with(setup: impl FnOnce(&Connection)) -> Store {
        // The full runtime schema (migrations included): the ledger reads ADR-0022's
        // `migration_unverified_at`, which the bare §11 SCHEMA does not carry.
        let conn = crate::store::open_memory().unwrap();
        setup(&conn);
        Store::spawn(conn)
    }

    fn no_start_payment() -> Arc<dyn PaymentBackend> {
        Arc::new(StartedPayment::default())
    }

    /// CONTEXT.md § Committed, as a table: sent or fenced commit whatever the witness says; an
    /// unfenced live row commits only on a Pending/Succeeded witness, and a terminal Failed (funds
    /// returned) or no witness at all (Unknown) does not.
    #[test]
    fn committed_is_sent_or_fenced_or_a_live_witness() {
        use PayStatus::*;
        for (status, fenced, pay, want) in [
            ("SENT", false, Unknown, true),
            ("SENT", false, Failed, true),
            ("PENDING", true, Failed, true),
            ("FAILED", true, Unknown, true),
            ("PENDING", false, Pending, true),
            ("PENDING", false, Succeeded, true),
            ("PENDING", false, Failed, false),
            ("PENDING", false, Unknown, false),
            ("FAILED", false, Unknown, false),
            ("FAILED", false, Failed, false),
        ] {
            assert_eq!(
                committed(status, fenced, pay),
                want,
                "committed({status:?}, fenced={fenced}, {pay:?})"
            );
        }
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
        dbl.set_status("refund:started", PayStatus::Pending); // gen 0 → the pay key equals the anchor
        dbl.set_status("refund:terminal_failed", PayStatus::Failed);
        let payment: Arc<dyn PaymentBackend> = dbl;

        // 40 gross - 10 (SENT) - 10 (started, Pending witness) = 20 sat; failed/unstarted and terminal
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

    /// ADR-0022 (codex #91 P2): a fenced attempt is committed whatever its status and whatever the
    /// backend answers — a fenced FAILED refund the backend calls Failed, and a fenced FAILED sweep,
    /// both subtract. RED on a ledger that keys only on status / started evidence.
    #[tokio::test]
    async fn fenced_refunds_and_sweeps_are_committed_whatever_their_status() {
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i1', 'ext', 'order', 100, 'PAID')",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts,
                    resolution_gen, migration_unverified_at)
                 VALUES ('r-fenced', 'd', 10, 'refund:fenced', 'FAILED', 3, 0, 42)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO sweep_attempt (id, status, max_outlay_msat, migration_unverified_at)
                 VALUES ('sw-fenced', 'FAILED', 5000, 42)",
                [],
            )
            .unwrap();
        });
        let dbl = Arc::new(StartedPayment::default());
        dbl.set_status("refund:fenced", PayStatus::Failed);
        let payment: Arc<dyn PaymentBackend> = dbl;
        // 100_000 − 10_000 (fenced refund, despite Failed) − 5_000 (fenced FAILED sweep) = 85_000.
        assert_eq!(expected_msat(&store, &payment).await.unwrap(), 85_000);
    }

    /// A row Committed by its status or fence needs no witness, so a backend that cannot answer for
    /// it must not fail the report (`observe` skips the probe). RED on an observe that probes every row.
    #[tokio::test]
    async fn observe_does_not_probe_rows_committed_by_status_or_fence() {
        struct UnqueryablePayment;
        #[async_trait]
        impl PaymentBackend for UnqueryablePayment {
            async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
                unimplemented!()
            }
            async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
                unimplemented!()
            }
            async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
                unimplemented!()
            }
            async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
                unimplemented!()
            }
            async fn payment_status(&self, _: &str) -> Result<PayStatus> {
                unimplemented!()
            }
            async fn payment_status_by_key(&self, key: &str) -> Result<PayStatus> {
                anyhow::bail!("backend index unreadable for {key}")
            }
            async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
                unimplemented!()
            }
        }
        let store = store_with(|c| {
            c.execute(
                "INSERT INTO invoice (id, external_id, kind, amount_sat, status)
                 VALUES ('i1', 'ext', 'order', 100, 'PAID')",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen)
                 VALUES ('r-sent', 'd', 10, 'refund:sent', 'SENT', 1, 0)",
                [],
            )
            .unwrap();
            c.execute(
                "INSERT INTO refund_attempt (id, dest, amount_sat, idempotency_key, status, attempts,
                    resolution_gen, migration_unverified_at)
                 VALUES ('r-fenced', 'd', 5, 'refund:fenced', 'FAILED', 3, 0, 42)",
                [],
            )
            .unwrap();
        });
        let payment: Arc<dyn PaymentBackend> = Arc::new(UnqueryablePayment);
        // 100_000 − 10_000 (SENT) − 5_000 (fenced) = 85_000, with no backend read at all.
        assert_eq!(expected_msat(&store, &payment).await.unwrap(), 85_000);
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
