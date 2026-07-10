//! Operator sweep (gate1-operator-sweep, urw.3, ADR-0016): a daemon-safe payout that pays the
//! OPERATOR's own bolt11 from LEDGER SURPLUS, capped so it can never overspend. Authorization is
//! computed from the sqlite ledger ONLY — this module NEVER reads a federation balance (the pay is
//! the fail-safe; drift is the `reconcile` command's job). Mirrors `refund.rs`'s `Refunder`: the
//! surplus gate + a durable PENDING intent + a capped pay, with [`Sweeper::drive`] as the crash-
//! recovery path the supervisor runs at boot and on the maintenance interval.
//!
//! Surplus (spec §surplus), `u128` saturating throughout, ALL three terms drawn from the SAME
//! provenance set the `expected_msat` receipt base uses ([`crate::ledger::sum_receipts_msat`]):
//!
//! ```text
//! surplus_msat = receipts_msat − reserved_msat − paid_out_msat
//! ALLOW iff surplus_msat >= outlay_msat(sweep)
//! ```
//!
//! - `receipts_msat`  = Σ gross of ALL captured receipts (settled invoice rows + settle-refund
//!   journal entries), de-duped by external id — reused verbatim from the ledger, never re-summed.
//! - `reserved_msat`  = Σ gross ONCE per external id of (a) captured receipts still AT RISK (their
//!   sub is PENDING/PROVISIONING/RESUMING/REFUND_DUE — a state-machine refund path still exists) and
//!   (b) every NON-terminal `refund_attempt` (status != 'SENT', incl. unpriceable). Fail closed:
//!   when a receipt's finality is unprovable it is RESERVED, never sweepable.
//! - `paid_out_msat`  = Σ gross of SENT `refund_attempt` rows + Σ `max_outlay_msat` of in-flight
//!   (SENT/PENDING) `sweep_attempt` rows.
//! - `outlay_msat`    = a gateway QUOTE for the pay about to be made
//!   ([`crate::backends::PaymentBackend::refund_required_outlay_msat`]) — pricing, NOT a balance read.
//!
//! The receipts/reserved/paid-out reads run inside ONE `store.read`/`store.transaction` so the gate
//! cannot interleave with a capture/refund commit (single-writer, ADR-0001).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use lightning_invoice::Bolt11Invoice;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::{json, Value};

use crate::alerts::{AlertDispatcher, AlertKind, AlertRow};
use crate::backends::{PayStatus, PaymentBackend};
use crate::clock::Clock;
use crate::ledger::{positive_sat, sum_receipts_msat};
use crate::refund::parse_whole_sat;
use crate::store::{Store, SETTLE_REFUND_KINDS_SQL};

/// The three §surplus terms, in msats, from ONE consistent ledger snapshot. `u128` saturating so the
/// derived surplus is never negative and never over-authorizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Surplus {
    pub earned_msat: u128,
    pub reserved_msat: u128,
    pub paid_out_msat: u128,
}

impl Surplus {
    /// `receipts − reserved − paid_out`, saturating at 0.
    pub(crate) fn surplus_msat(&self) -> u128 {
        self.earned_msat
            .saturating_sub(self.reserved_msat)
            .saturating_sub(self.paid_out_msat)
    }
}

/// Compute the ledger surplus in ONE connection pass. `exclude_sweep_id` drops that row's own cap
/// from `paid_out_msat` — used ONLY by crash recovery re-gating a PENDING sweep against itself (its
/// cap is already subtracted the moment the row exists; counting it again would demand the funds
/// twice and falsely supersede a sweep that fit exactly, spec). Pure LOCAL reads — NO balance call.
pub(crate) fn read_surplus(conn: &Connection, exclude_sweep_id: Option<&str>) -> Result<Surplus> {
    // `earned` — the IDENTICAL receipt base `expected_msat` uses (never re-summed here).
    let earned_msat = sum_receipts_msat(conn)?;

    // `reserved`: external_id -> gross_sat, counted ONCE per external_id (a receipt with BOTH an
    // at-risk sub and a refund row keys one map entry, so it is reserved once).
    let mut reserved: HashMap<String, u64> = HashMap::new();
    // `paid_out`: SENT refunds (same COALESCE provenance as reserved) + in-flight sweep caps.
    let mut paid_out_msat: u128 = 0;

    // (b) every `refund_attempt`, priced at the received gross the receipt base uses
    //     (COALESCE(invoice.amount_sat, settle-refund journal amount)). Non-terminal rows RESERVE;
    //     SENT rows PAY OUT. Mirrors load_refund_readiness_liabilities' refund CTE (store.rs).
    let refund_sql = format!(
        "WITH journal AS (
             SELECT json_extract(detail_json, '$.external_id') AS external_id,
                    MAX(CAST(json_extract(detail_json, '$.amount_sat') AS INTEGER)) AS amount_sat
               FROM event_log
              WHERE kind IN ({SETTLE_REFUND_KINDS_SQL})
              GROUP BY external_id
         )
         SELECT r.external_id,
                -- Price from the receipt base (invoice / settle-refund journal) to match `earned`'s
                -- provenance exactly. FAIL CLOSED (spec): if a non-terminal refund somehow has NO
                -- receipt-base provenance (a malformed/partial row that atomic capture should make
                -- impossible), fall back to the refund row's OWN recorded gross rather than skip it —
                -- reserving it (over-refusing) is the safe direction; never leave a live liability
                -- unreserved (codex-adversarial: NULL provenance must not silently drop the reserve).
                COALESCE(i.amount_sat, journal.amount_sat, r.own_amount_sat) AS received_sat,
                r.status
           FROM (
                 SELECT status,
                        amount_sat AS own_amount_sat,
                        CASE
                          WHEN idempotency_key LIKE 'refund:%' THEN substr(idempotency_key, 8)
                          WHEN id LIKE 'ref-%' THEN substr(id, 5)
                          ELSE id
                        END AS external_id
                   FROM refund_attempt
                ) r
           LEFT JOIN invoice i
                  ON i.external_id = r.external_id
                 AND (i.status = 'PAID' OR i.settled_at IS NOT NULL)
           LEFT JOIN journal ON journal.external_id = r.external_id"
    );
    let mut stmt = conn.prepare(&refund_sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<i64>>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (external_id, received_sat, status) = row?;
        let Some(gross_sat) = positive_sat(received_sat) else {
            continue;
        };
        if status == "SENT" {
            paid_out_msat = paid_out_msat.saturating_add(u128::from(gross_sat) * 1000);
        } else {
            reserved.insert(external_id, gross_sat);
        }
    }

    // (a) captured receipts still AT RISK: a settled invoice (order OR renewal) whose sub is in a
    //     state from which a refund path still exists. INCLUDES 'RESUMING' — a suspended-renewal
    //     receipt is final only once the resume lands ACTIVE; the readiness scan omits it, the sweep
    //     must not. Keyed by external_id, so it de-dups with (b) at gross.
    let mut stmt = conn.prepare(
        "SELECT i.external_id, i.amount_sat
           FROM invoice i
           JOIN subscription s ON s.id = i.subscription_id
          WHERE (i.status = 'PAID' OR i.settled_at IS NOT NULL)
            AND s.state IN ('PENDING', 'PROVISIONING', 'RESUMING', 'REFUND_DUE')",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    })?;
    for row in rows {
        let (external_id, amount_sat) = row?;
        let Some(gross_sat) = positive_sat(amount_sat) else {
            continue;
        };
        reserved.insert(external_id, gross_sat);
    }

    let reserved_msat: u128 = reserved.values().map(|s| u128::from(*s) * 1000).sum();

    // In-flight sweep caps (SENT/PENDING) already committed to a payout — minus the row being
    // crash-recovered (see `exclude_sweep_id`).
    let mut stmt =
        conn.prepare("SELECT id, max_outlay_msat FROM sweep_attempt WHERE status IN ('SENT', 'PENDING')")?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (id, cap_msat) = row?;
        if Some(id.as_str()) == exclude_sweep_id {
            continue;
        }
        paid_out_msat = paid_out_msat.saturating_add(u128::try_from(cap_msat).unwrap_or(0));
    }

    Ok(Surplus {
        earned_msat,
        reserved_msat,
        paid_out_msat,
    })
}

/// A structured refusal, mapped 1:1 to an IPC error code the operator agent branches on. Money never
/// moved for any of these (the pay is the last step; a `FeeRose`/in-flight parks the durable row, not
/// the ledger). `Internal` carries an unexpected store error.
#[derive(Debug)]
pub enum SweepError {
    /// Zero-amount or unparseable bolt11 (`sweep_invalid`).
    Invalid(String),
    /// The gateway could not price the outlay (`sweep_unpriceable`).
    Unpriceable(String),
    /// Another sweep is already in flight — one at a time (`sweep_busy`).
    Busy(String),
    /// Ledger surplus is below the required outlay (`sweep_insufficient`).
    Insufficient(String),
    /// The capped pay refused (the gateway fee rose past the quote) — durable row parked FAILED
    /// (`sweep_fee_rose`).
    FeeRose(String),
    /// The pay is in-flight but unconfirmed; the durable row stays PENDING for recovery
    /// (`sweep_in_flight`).
    InFlight(String),
    /// An unexpected store/internal error (`internal`).
    Internal(anyhow::Error),
}

impl SweepError {
    pub fn code(&self) -> &'static str {
        match self {
            SweepError::Invalid(_) => "sweep_invalid",
            SweepError::Unpriceable(_) => "sweep_unpriceable",
            SweepError::Busy(_) => "sweep_busy",
            SweepError::Insufficient(_) => "sweep_insufficient",
            SweepError::FeeRose(_) => "sweep_fee_rose",
            SweepError::InFlight(_) => "sweep_in_flight",
            SweepError::Internal(_) => "internal",
        }
    }

    pub fn message(&self) -> String {
        match self {
            SweepError::Invalid(m)
            | SweepError::Unpriceable(m)
            | SweepError::Busy(m)
            | SweepError::Insufficient(m)
            | SweepError::FeeRose(m)
            | SweepError::InFlight(m) => m.clone(),
            SweepError::Internal(e) => format!("{e:#}"),
        }
    }
}

/// A dry-run quote (no writes): the surplus breakdown + the outlay + the ALLOW/REFUSE verdict.
#[derive(Debug, Clone)]
pub struct QuoteView {
    pub amount_sat: u64,
    pub outlay_msat: u128,
    pub earned_msat: u128,
    pub reserved_msat: u128,
    pub paid_out_msat: u128,
    pub surplus_msat: u128,
    pub allow: bool,
}

/// The result of a successful execute: the durable row reached SENT (or was already SENT on a
/// re-submit of the same invoice).
#[derive(Debug, Clone)]
pub struct ExecOutcome {
    pub id: String,
    pub amount_sat: u64,
    pub max_outlay_msat: u128,
    pub status: String,
    pub backend_payment_id: Option<String>,
    /// True when a re-submit of the same bolt11 returned the cached SENT row (no second pay).
    pub cached: bool,
}

/// What one [`Sweeper::drive`] did — every count is a normal result, not an error.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    pub sent: usize,
    pub failed: usize,
    /// Left PENDING this drive (ambiguous pay / no started evidence resolved yet) — retried next.
    pub pending: usize,
}

/// The per-pay outcome, shared by execute and [`Sweeper::drive`].
enum PayOutcome {
    Sent(Option<String>),
    Failed(String),
    Pending,
}

/// The gate decision from the ONE serialized gate+write transaction.
enum GateDecision {
    AlreadySent(ExecOutcome),
    Busy,
    Insufficient(Surplus),
    Proceed,
}

/// Pays the operator's own bolt11 from ledger surplus. Holds the SAME store + payment the rest of the
/// money path uses, plus a clock and an optional alert sink (the supervisor injects the real one).
pub struct Sweeper {
    store: Store,
    payment: Arc<dyn PaymentBackend>,
    clock: Arc<dyn Clock>,
    /// Optional GATE-1 alert sink: surfaces a parked FAILED sweep as a durable operator DM. `None`
    /// for the IPC execute path (a WARN log suffices — the operator gets the structured reply live)
    /// and focused tests; the supervisor injects the real one via [`Sweeper::with_alerts`].
    alerts: Option<Arc<AlertDispatcher>>,
}

impl Sweeper {
    pub fn new(store: Store, payment: Arc<dyn PaymentBackend>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            payment,
            clock,
            alerts: None,
        }
    }

    /// Inject the GATE-1 alert sink (the supervisor wires it) so a parked FAILED sweep additionally
    /// enqueues a durable `SweepFailed` operator DM inside the FAILED transaction.
    pub fn with_alerts(mut self, alerts: Arc<AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Dry-run quote: parse the invoice, price the outlay, read the surplus, and report the verdict.
    /// No writes. `sweep_invalid` on a zero-amount/unparseable bolt11; `sweep_unpriceable` if the
    /// gateway cannot price the outlay.
    pub async fn quote(&self, bolt11: &str) -> Result<QuoteView, SweepError> {
        let (_hash, amount_sat) = parse_sweep_invoice(bolt11, self.clock.now())?;
        let outlay_msat = self.quote_outlay(amount_sat).await?;
        let surplus = self
            .store
            .read(move |c| read_surplus(c, None))
            .await
            .map_err(SweepError::Internal)?;
        Ok(QuoteView {
            amount_sat,
            outlay_msat,
            earned_msat: surplus.earned_msat,
            reserved_msat: surplus.reserved_msat,
            paid_out_msat: surplus.paid_out_msat,
            surplus_msat: surplus.surplus_msat(),
            allow: surplus.surplus_msat() >= outlay_msat,
        })
    }

    /// Execute the sweep: quote the outlay, then — in ONE serialized transaction — return the cached
    /// SENT row on a re-submit, refuse a concurrent in-flight sweep (`sweep_busy`) or an insufficient
    /// surplus (`sweep_insufficient`), else write the durable PENDING intent (+ journal). Only then
    /// pay, capped at the quoted outlay; success → SENT, a cap refusal (fee rose) → FAILED.
    pub async fn execute(&self, bolt11: &str) -> Result<ExecOutcome, SweepError> {
        let (payment_hash, amount_sat) = parse_sweep_invoice(bolt11, self.clock.now())?;
        let id = format!("sweep:{payment_hash}");
        let outlay_msat = self.quote_outlay(amount_sat).await?;
        let now = self.clock.now();

        // Refuse a value that cannot round-trip through the i64 ledger columns BEFORE writing intent
        // (coderabbit): `amount_sat`/`max_outlay_msat` persist as i64, so a clamp (`unwrap_or(MAX)`)
        // or truncating cast would under-record the committed cap and let later reads subtract too
        // little. Unreachable for any real invoice (> total BTC supply), but a money value must refuse
        // an out-of-range conversion, not silently narrow it.
        if i64::try_from(amount_sat).is_err() || i64::try_from(outlay_msat).is_err() {
            return Err(SweepError::Invalid(
                "invoice amount or outlay exceeds the ledger's representable range".to_string(),
            ));
        }

        match self
            .gate_and_write(&id, bolt11, &payment_hash, amount_sat, outlay_msat, now)
            .await
            .map_err(SweepError::Internal)?
        {
            GateDecision::AlreadySent(outcome) => return Ok(outcome),
            GateDecision::Busy => {
                return Err(SweepError::Busy(
                    "another sweep is already in flight; only one at a time".to_string(),
                ))
            }
            GateDecision::Insufficient(surplus) => {
                return Err(SweepError::Insufficient(format!(
                    "ledger surplus {} msat is below the required {} msat outlay",
                    surplus.surplus_msat(),
                    outlay_msat
                )))
            }
            GateDecision::Proceed => {}
        }

        // PENDING is durable — its cap is now subtracted from surplus/expected. Pay, capped.
        match self
            .capped_pay(&id, bolt11, &payment_hash, amount_sat, outlay_msat, now)
            .await
            .map_err(SweepError::Internal)?
        {
            PayOutcome::Sent(pid) => Ok(ExecOutcome {
                id,
                amount_sat,
                max_outlay_msat: outlay_msat,
                status: "SENT".to_string(),
                backend_payment_id: pid,
                cached: false,
            }),
            PayOutcome::Failed(reason) => Err(SweepError::FeeRose(reason)),
            PayOutcome::Pending => Err(SweepError::InFlight(
                "sweep pay is in-flight but unconfirmed; it will be reconciled on the next \
                 maintenance pass"
                    .to_string(),
            )),
        }
    }

    /// Crash-recovery drive (boot + maintenance): finish every PENDING `sweep_attempt`. A terminal
    /// key status wins first (`Succeeded` => SENT, `Failed` => FAILED); an in-flight `Pending` key
    /// re-awaits by key. Only an `Unknown` key falls back to started evidence or, if not started,
    /// re-runs the surplus gate against the CURRENT ledger EXCLUDING this row's own cap; still fits
    /// ⇒ capped-pay, else park FAILED `superseded_by_liability`. Idempotent and safe to call
    /// repeatedly.
    pub async fn drive(&self) -> Result<SweepReport> {
        let mut report = SweepReport::default();
        for row in self.pending_sweeps().await? {
            let Some(bolt11) = row.bolt11.clone() else {
                tracing::warn!(sweep = %row.id, "PENDING sweep has no bolt11; leaving for manual handling");
                report.pending += 1;
                continue;
            };
            let payment_hash = payment_hash_of(&row.id);
            let outcome = match self.recovery_status(&row.id).await {
                PayStatus::Succeeded => {
                    self.commit_sent(&row.id, &payment_hash, None, self.clock.now())
                        .await?;
                    PayOutcome::Sent(None)
                }
                PayStatus::Pending => {
                    self.capped_pay(
                        &row.id,
                        &bolt11,
                        &payment_hash,
                        row.amount_sat,
                        row.max_outlay_msat,
                        self.clock.now(),
                    )
                    .await?
                }
                PayStatus::Failed => {
                    let reason = "backend payment key is FAILED before the sweep ledger terminalized"
                        .to_string();
                    self.commit_failed(&row.id, &payment_hash, self.clock.now(), &reason)
                        .await?;
                    PayOutcome::Failed(reason)
                }
                PayStatus::Unknown if self.payment.payment_started_by_key(&row.id).await? => {
                    // Durable evidence of an op in the fedimint crash window, but no key row yet:
                    // re-await by key/payment hash. A definite FAILED key never reaches this branch.
                    self.capped_pay(
                        &row.id,
                        &bolt11,
                        &payment_hash,
                        row.amount_sat,
                        row.max_outlay_msat,
                        self.clock.now(),
                    )
                    .await?
                }
                PayStatus::Unknown => {
                    let surplus = {
                        let id = row.id.clone();
                        self.store
                            .read(move |c| read_surplus(c, Some(id.as_str())))
                            .await?
                    };
                    if surplus.surplus_msat() >= row.max_outlay_msat {
                        self.capped_pay(
                            &row.id,
                            &bolt11,
                            &payment_hash,
                            row.amount_sat,
                            row.max_outlay_msat,
                            self.clock.now(),
                        )
                        .await?
                    } else {
                        let reason = format!(
                            "superseded_by_liability: surplus {} msat (excl. this sweep) fell below the \
                             committed {} msat cap",
                            surplus.surplus_msat(),
                            row.max_outlay_msat
                        );
                        self.commit_failed(&row.id, &payment_hash, self.clock.now(), &reason)
                            .await?;
                        PayOutcome::Failed(reason)
                    }
                }
            };
            match outcome {
                PayOutcome::Sent(_) => report.sent += 1,
                PayOutcome::Failed(_) => report.failed += 1,
                PayOutcome::Pending => report.pending += 1,
            }
        }
        Ok(report)
    }

    /// Quote the exact outlay msats for a NEW sweep pay (pricing, NOT a balance read). `Err` ⇒ refuse
    /// `sweep_unpriceable`.
    async fn quote_outlay(&self, amount_sat: u64) -> Result<u128, SweepError> {
        self.payment
            .refund_required_outlay_msat(amount_sat, Some(amount_sat))
            .await
            .map_err(|e| SweepError::Unpriceable(format!("gateway could not price the sweep: {e:#}")))
    }

    /// The ONE serialized gate + durable-intent transaction (single-writer, ADR-0001): re-submit
    /// short-circuit, busy refusal, surplus gate, then the PENDING write + `kind='sweep'` journal.
    async fn gate_and_write(
        &self,
        id: &str,
        bolt11: &str,
        payment_hash: &str,
        amount_sat: u64,
        outlay_msat: u128,
        now: i64,
    ) -> Result<GateDecision> {
        // `execute` already refused an out-of-range value; convert (never clamp) so a future caller
        // cannot silently under-record the cap.
        let outlay_i64 = i64::try_from(outlay_msat)
            .map_err(|_| anyhow::anyhow!("outlay {outlay_msat} msat exceeds the i64 ledger range"))?;
        let amount_i64 = i64::try_from(amount_sat)
            .map_err(|_| anyhow::anyhow!("amount {amount_sat} sat exceeds the i64 ledger range"))?;
        let (id_s, bolt11_s, hash_s) = (id.to_string(), bolt11.to_string(), payment_hash.to_string());
        self.store
            .transaction(move |tx| {
                // Re-submit of the SAME invoice: a completed sweep returns its cached SENT row; a
                // still-PENDING one is busy (the drive will finish it).
                let existing: Option<(String, Option<i64>, i64, Option<String>)> = tx
                    .query_row(
                        "SELECT status, amount_sat, max_outlay_msat, backend_payment_id
                           FROM sweep_attempt WHERE id=?1",
                        params![id_s],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )
                    .optional()?;
                if let Some((status, amt, cap, pid)) = existing {
                    if status == "SENT" {
                        return Ok(GateDecision::AlreadySent(ExecOutcome {
                            id: id_s.clone(),
                            amount_sat: amt.unwrap_or(0).max(0) as u64,
                            max_outlay_msat: u128::try_from(cap).unwrap_or(0),
                            status: "SENT".to_string(),
                            backend_payment_id: pid,
                            cached: true,
                        }));
                    }
                    if status == "PENDING" {
                        return Ok(GateDecision::Busy);
                    }
                    // FAILED -> re-attempt (the upsert resets it to PENDING below).
                }
                // One in-flight sweep at a time: any PENDING row (necessarily a DIFFERENT invoice —
                // this id's PENDING case returned above) blocks a new one.
                let pending: i64 = tx.query_row(
                    "SELECT count(*) FROM sweep_attempt WHERE status='PENDING'",
                    [],
                    |r| r.get(0),
                )?;
                if pending > 0 {
                    return Ok(GateDecision::Busy);
                }
                // Surplus gate: this row is not yet PENDING/SENT (a prior FAILED is excluded from
                // paid_out), so exclude nothing.
                let surplus = read_surplus(tx, None)?;
                if surplus.surplus_msat() < outlay_msat {
                    return Ok(GateDecision::Insufficient(surplus));
                }
                // Durable intent BEFORE pay: PENDING row (upsert over a prior FAILED) + journal. Its
                // cap is now subtracted from surplus/expected the moment this commits.
                tx.execute(
                    "INSERT INTO sweep_attempt
                        (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
                     VALUES (?1, ?2, ?3, ?4, 'PENDING', 0, ?5)
                     ON CONFLICT(id) DO UPDATE SET
                        bolt11=excluded.bolt11, amount_sat=excluded.amount_sat,
                        max_outlay_msat=excluded.max_outlay_msat, status='PENDING', attempts=0,
                        backend_payment_id=NULL, last_error=NULL, created_at=excluded.created_at,
                        sent_at=NULL",
                    params![id_s, bolt11_s, amount_i64, outlay_i64, now],
                )?;
                journal_sweep(
                    tx,
                    &json!({
                        "payment_hash": hash_s,
                        "phase": "intent",
                        "amount_sat": amount_sat,
                        "max_outlay_msat": outlay_i64,
                    }),
                    now,
                )?;
                Ok(GateDecision::Proceed)
            })
            .await
    }

    /// Pay the operator invoice capped at the quoted outlay, then record the outcome. A pay `Err` is
    /// re-checked by key (mirroring the refunder): `Succeeded` records SENT (the pay actually
    /// landed), a definite `Failed` parks FAILED, and an ambiguous `Pending`/`Unknown`/lookup-error
    /// leaves the row PENDING (its cap stays subtracted) for the next drive — never releasing a cap
    /// while the payment might still settle.
    async fn capped_pay(
        &self,
        id: &str,
        bolt11: &str,
        payment_hash: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        now: i64,
    ) -> Result<PayOutcome> {
        match self
            .payment
            .pay_capped(bolt11, amount_sat, max_outlay_msat, id)
            .await
        {
            Ok(pid) => {
                self.commit_sent(id, payment_hash, Some(pid.clone()), now).await?;
                Ok(PayOutcome::Sent(Some(pid)))
            }
            Err(e) => match self.status_after_error(id).await {
                PayStatus::Succeeded => {
                    self.commit_sent(id, payment_hash, None, now).await?;
                    Ok(PayOutcome::Sent(None))
                }
                PayStatus::Failed => {
                    let reason = format!("capped sweep pay refused: {e:#}");
                    tracing::warn!(sweep = %id, error = %format!("{e:#}"), "sweep pay refused; parking FAILED");
                    self.commit_failed(id, payment_hash, now, &reason).await?;
                    Ok(PayOutcome::Failed(reason))
                }
                status @ (PayStatus::Pending | PayStatus::Unknown) => {
                    tracing::warn!(
                        sweep = %id,
                        error = %format!("{e:#}"),
                        ?status,
                        "sweep pay ambiguous; row stays PENDING for recovery"
                    );
                    Ok(PayOutcome::Pending)
                }
            },
        }
    }

    /// Re-check the key after a pay error; a lookup error is treated as `Unknown` (terminalizing while
    /// the backend cannot answer is unsafe — the payment may still settle).
    async fn status_after_error(&self, key: &str) -> PayStatus {
        match self.payment.payment_status_by_key(key).await {
            Ok(status) => status,
            Err(e) => {
                tracing::warn!(sweep = %key, error = %format!("{e:#}"), "sweep status lookup failed after pay error");
                PayStatus::Unknown
            }
        }
    }

    /// Recovery status lookup. A lookup failure is non-terminal: the payment may still settle, so treat
    /// it as `Unknown` and let the started-evidence branch decide whether to re-await or re-gate.
    async fn recovery_status(&self, key: &str) -> PayStatus {
        match self.payment.payment_status_by_key(key).await {
            Ok(status) => status,
            Err(e) => {
                tracing::warn!(sweep = %key, error = %format!("{e:#}"), "sweep recovery status lookup failed");
                PayStatus::Unknown
            }
        }
    }

    /// CAS the row to SENT (guarded on `status='PENDING'`) + journal. Returns whether it committed.
    async fn commit_sent(
        &self,
        id: &str,
        payment_hash: &str,
        backend_payment_id: Option<String>,
        now: i64,
    ) -> Result<bool> {
        let (id_s, hash_s) = (id.to_string(), payment_hash.to_string());
        self.store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE sweep_attempt
                        SET status='SENT', backend_payment_id=COALESCE(?2, backend_payment_id),
                            attempts=COALESCE(attempts, 0)+1, sent_at=?3
                      WHERE id=?1 AND status='PENDING'",
                    params![id_s, backend_payment_id, now],
                )?;
                if updated == 0 {
                    return Ok(false);
                }
                journal_sweep(tx, &json!({ "payment_hash": hash_s, "phase": "sent" }), now)?;
                Ok(true)
            })
            .await
    }

    /// CAS the row to FAILED (guarded on `status='PENDING'`) + journal, and — if an alert sink is
    /// wired — enqueue a terminal `SweepFailed` operator DM INSIDE the same transaction so a crash
    /// can never drop the alert for a park the row will never re-enter. Otherwise a WARN log only.
    async fn commit_failed(&self, id: &str, payment_hash: &str, now: i64, reason: &str) -> Result<()> {
        let alert_row = self.alerts.as_ref().and_then(|a| {
            a.terminal_alert_row(
                AlertKind::SweepFailed,
                id,
                &format!("operator sweep {id} parked FAILED: {reason}"),
            )
        });
        let (id_s, hash_s, reason_s) = (id.to_string(), payment_hash.to_string(), reason.to_string());
        let parked = self
            .store
            .transaction(move |tx| {
                let updated = tx.execute(
                    "UPDATE sweep_attempt
                        SET status='FAILED', attempts=COALESCE(attempts, 0)+1, last_error=?2
                      WHERE id=?1 AND status='PENDING'",
                    params![id_s, reason_s],
                )?;
                if updated == 0 {
                    return Ok(false);
                }
                journal_sweep(
                    tx,
                    &json!({ "payment_hash": hash_s, "phase": "failed", "reason": reason_s }),
                    now,
                )?;
                if let Some(alert) = &alert_row {
                    enqueue_alert_row(tx, alert, now)?;
                }
                Ok(true)
            })
            .await?;
        if parked {
            tracing::warn!(sweep = %id, reason, "operator sweep parked FAILED");
        }
        Ok(())
    }

    /// Every PENDING `sweep_attempt` row the drive must finish.
    async fn pending_sweeps(&self) -> Result<Vec<PendingSweep>> {
        self.store
            .read(|c| {
                let mut stmt = c.prepare(
                    "SELECT id, bolt11, COALESCE(amount_sat, 0), max_outlay_msat
                       FROM sweep_attempt
                      WHERE status='PENDING'
                      ORDER BY created_at, id",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok(PendingSweep {
                            id: r.get(0)?,
                            bolt11: r.get(1)?,
                            amount_sat: r.get::<_, i64>(2)?.max(0) as u64,
                            max_outlay_msat: u128::try_from(r.get::<_, i64>(3)?).unwrap_or(0),
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
    }
}

/// A PENDING `sweep_attempt` row for the crash-recovery drive.
struct PendingSweep {
    id: String,
    bolt11: Option<String>,
    amount_sat: u64,
    max_outlay_msat: u128,
}

/// Parse the operator's sweep invoice: `(payment_hash, amount_sat)`. A parse failure, an amountless
/// invoice, a sub-sat amount, or an explicit zero amount is `sweep_invalid` — none is payable.
fn parse_sweep_invoice(bolt11: &str, now: i64) -> Result<(String, u64), SweepError> {
    let inv = Bolt11Invoice::from_str(bolt11)
        .map_err(|e| SweepError::Invalid(format!("bolt11 parse error: {e}")))?;
    let payment_hash = inv.payment_hash().to_string();
    let amount_sat = parse_whole_sat(bolt11).map_err(SweepError::Invalid)?;
    if amount_sat == 0 {
        return Err(SweepError::Invalid(
            "zero-amount invoice; a sweep needs an amount".to_string(),
        ));
    }
    // Reject an already-expired invoice UP FRONT (against the daemon clock, not wall time) so it
    // never passes the surplus gate and writes a doomed PENDING intent — whose cap would block the
    // one-in-flight slot until it terminalizes, and which a no-validation backend could even record
    // SENT (codex). Checked here at the single parse point for both quote and execute.
    if inv.would_expire(std::time::Duration::from_secs(u64::try_from(now).unwrap_or(0))) {
        return Err(SweepError::Invalid(
            "invoice has expired; re-issue a fresh one".to_string(),
        ));
    }
    Ok((payment_hash, amount_sat))
}

/// The payment hash for a `sweep:<payment_hash>` row id (the id IS the pay key).
fn payment_hash_of(id: &str) -> String {
    id.strip_prefix("sweep:").unwrap_or(id).to_string()
}

/// Journal a `kind='sweep'` money transition (sub_id NULL; the detail carries the payment_hash +
/// phase), like other money transitions in `event_log`.
fn journal_sweep(tx: &Transaction, detail: &Value, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (NULL, 'sweep', ?1, ?2)",
        params![detail.to_string(), now],
    )?;
    Ok(())
}

/// Insert a prepared terminal `operator.alert` outbox row in the caller's txn (atomic with the FAILED
/// transition). `ON CONFLICT DO NOTHING` on the stable id makes a re-drive idempotent.
fn enqueue_alert_row(tx: &Transaction, row: &AlertRow, now: i64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox
            (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
         VALUES (?1, ?2, NULL, 'operator.alert', ?3, 'PENDING', 0, ?4)
         ON CONFLICT(id) DO NOTHING",
        params![row.id, row.recipient, row.payload, now],
    )?;
    Ok(())
}

/// The `lnrent money` sweep view (gate1-operator-sweep): the surplus breakdown + the most recent
/// sweep. Pure LOCAL ledger reads — no network, no balance call.
pub(crate) async fn money_sweep_view(store: &Store) -> Result<Value> {
    store
        .read(|c| {
            let surplus = read_surplus(c, None)?;
            let last_sweep: Option<Value> = c
                .query_row(
                    "SELECT id, status, amount_sat, max_outlay_msat, created_at, sent_at
                       FROM sweep_attempt
                      ORDER BY COALESCE(sent_at, created_at) DESC, id DESC
                      LIMIT 1",
                    [],
                    |r| {
                        Ok(json!({
                            "id": r.get::<_, String>(0)?,
                            "status": r.get::<_, String>(1)?,
                            "amount_sat": r.get::<_, Option<i64>>(2)?,
                            "max_outlay_msat": r.get::<_, i64>(3)?,
                            "created_at": r.get::<_, Option<i64>>(4)?,
                            "sent_at": r.get::<_, Option<i64>>(5)?,
                        }))
                    },
                )
                .optional()?;
            Ok(json!({
                "earned_msat": surplus.earned_msat,
                "reserved_msat": surplus.reserved_msat,
                "paid_out_msat": surplus.paid_out_msat,
                "surplus_msat": surplus.surplus_msat(),
                "last_sweep": last_sweep,
            }))
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{Invoice, MockPayment, PaymentStatus, Settlement};
    use crate::clock::TestClock;
    use crate::refund_resolver::mint_bolt11;
    use crate::store::{Store, SCHEMA};
    use async_trait::async_trait;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    const META: &str = r#"[["text/plain","lnrent sweep"]]"#;

    fn mem_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn
    }

    fn mem_store() -> Store {
        Store::spawn(mem_conn())
    }

    // -- direct-connection surplus seeding (pure-ledger, NO backend) ----------

    fn seed_sub(c: &Connection, id: &str, state: &str) {
        c.execute(
            "INSERT INTO subscription (id, state, created_at, updated_at) VALUES (?1, ?2, 0, 0)",
            params![id, state],
        )
        .unwrap();
    }

    fn seed_receipt(c: &Connection, external_id: &str, sub_id: &str, kind: &str, amount_sat: i64) {
        c.execute(
            "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'PAID', 0)",
            params![format!("inv-{external_id}"), sub_id, external_id, kind, amount_sat],
        )
        .unwrap();
    }

    fn seed_refund(c: &Connection, external_id: &str, sub_id: &str, amount_sat: i64, status: &str) {
        c.execute(
            "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
             VALUES (?1, ?2, 'lnaddr@buyer', ?3, ?4, ?5, 0, 0, 0)",
            params![
                format!("ref-{external_id}"),
                sub_id,
                amount_sat,
                format!("refund:{external_id}"),
                status
            ],
        )
        .unwrap();
    }

    fn seed_sweep_row(c: &Connection, id: &str, status: &str, max_outlay_msat: i64) {
        c.execute(
            "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
             VALUES (?1, 'lnbc1', 1, ?2, ?3, 0, 0)",
            params![id, max_outlay_msat, status],
        )
        .unwrap();
    }

    fn surplus(c: &Connection) -> u128 {
        read_surplus(c, None).unwrap().surplus_msat()
    }

    #[test]
    fn active_receipt_sweepable_provisioning_reserved() {
        // ONE ACTIVE 100k + ONE PROVISIONING 100k must leave EXACTLY 100k sweepable, not 0.
        let c = mem_conn();
        seed_sub(&c, "A", "ACTIVE");
        seed_sub(&c, "B", "PROVISIONING");
        seed_receipt(&c, "order:A", "A", "order", 100_000);
        seed_receipt(&c, "order:B", "B", "order", 100_000);
        assert_eq!(surplus(&c), 100_000_000);
    }

    #[test]
    fn at_risk_states_reserve_at_gross_active_nets_final() {
        // Each at-risk state reserves its receipt at gross (nets to 0); ACTIVE is final (sweepable).
        for state in ["PENDING", "PROVISIONING", "RESUMING", "REFUND_DUE"] {
            let c = mem_conn();
            seed_sub(&c, "s", state);
            seed_receipt(&c, "order:s", "s", "order", 50_000);
            assert_eq!(surplus(&c), 0, "{state} receipt must be fully reserved");
        }
        let c = mem_conn();
        seed_sub(&c, "s", "ACTIVE");
        seed_receipt(&c, "order:s", "s", "order", 50_000);
        assert_eq!(surplus(&c), 50_000_000, "an ACTIVE receipt is final and sweepable");
    }

    #[test]
    fn suspended_renewal_receipt_sweepable_only_after_resume_lands_active() {
        // A renewal receipt on a RESUMING sub is reserved; once the resume lands ACTIVE it is final.
        let c = mem_conn();
        seed_sub(&c, "s", "RESUMING");
        seed_receipt(&c, "renew:s:1", "s", "renewal", 30_000);
        assert_eq!(surplus(&c), 0, "a RESUMING renewal receipt is reserved");

        let c = mem_conn();
        seed_sub(&c, "s", "ACTIVE");
        seed_receipt(&c, "renew:s:1", "s", "renewal", 30_000);
        assert_eq!(surplus(&c), 30_000_000, "an applied (ACTIVE) renewal receipt is sweepable");
    }

    #[test]
    fn nonterminal_refunds_reserve_at_gross_with_dedup_sent_subtracts() {
        // A receipt with a NON-terminal refund AND an at-risk sub counts its gross ONCE (dedup).
        let c = mem_conn();
        seed_sub(&c, "s", "REFUND_DUE");
        seed_receipt(&c, "order:s", "s", "order", 80_000);
        seed_refund(&c, "order:s", "s", 80_000, "PENDING");
        assert_eq!(surplus(&c), 0, "reserved once, not twice");

        // An unpriceable (still PENDING) refund also reserves at gross.
        let c = mem_conn();
        seed_sub(&c, "s", "ACTIVE"); // sub already final, but the PENDING refund still reserves
        seed_receipt(&c, "order:s", "s", "order", 80_000);
        seed_refund(&c, "order:s", "s", 80_000, "PENDING");
        assert_eq!(surplus(&c), 0, "a pending refund reserves even on an ACTIVE sub");

        // A SENT refund subtracts via paid_out and nets the refunded receipt to 0.
        let c = mem_conn();
        seed_sub(&c, "s", "REFUNDED");
        seed_receipt(&c, "order:s", "s", "order", 80_000);
        seed_refund(&c, "order:s", "s", 80_000, "SENT");
        assert_eq!(surplus(&c), 0, "a SENT refund nets its receipt to 0");
    }

    #[test]
    fn nonterminal_refund_without_receipt_provenance_reserves_at_its_own_gross_fail_closed() {
        // FAIL CLOSED (codex-adversarial): a non-terminal refund whose external_id has NO
        // receipt-base provenance (no settled invoice, no settle-refund journal — a malformed row
        // atomic capture should make impossible) must STILL reserve, at its OWN recorded gross,
        // never silently drop to 0. A separate FINAL receipt is sweepable; the orphan refund
        // over-refuses against it (the safe direction).
        let c = mem_conn();
        seed_sub(&c, "a", "ACTIVE");
        seed_receipt(&c, "order:a", "a", "order", 100_000); // final -> sweepable
        seed_refund(&c, "orphan-x", "b", 30_000, "PENDING"); // no receipt for 'orphan-x'
        assert_eq!(
            surplus(&c),
            70_000_000,
            "the provenance-less refund reserves at its own 30k gross (fail-closed), not 0"
        );
    }

    #[test]
    fn in_flight_sweeps_subtract_but_failed_ones_do_not() {
        let c = mem_conn();
        seed_sub(&c, "A", "ACTIVE");
        seed_receipt(&c, "order:A", "A", "order", 100_000); // earned 100_000_000
        seed_sweep_row(&c, "sweep:sent", "SENT", 20_000_000);
        seed_sweep_row(&c, "sweep:pending", "PENDING", 5_000_000);
        seed_sweep_row(&c, "sweep:failed", "FAILED", 999_000_000); // terminal — not subtracted
        assert_eq!(surplus(&c), 75_000_000);
    }

    #[test]
    fn uncaptured_settlement_is_inert_then_sweepable_only_at_active() {
        // Uncaptured: the invoice is still OPEN in our ledger (capture stamps PAID/settled_at). It is
        // not a receipt, so surplus is unchanged and there is nothing to sweep.
        let c = mem_conn();
        seed_sub(&c, "s", "PENDING");
        c.execute(
            "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
             VALUES ('inv-o', 's', 'order:s', 'order', 100000, 'OPEN', 0)",
            [],
        )
        .unwrap();
        assert_eq!(surplus(&c), 0, "an uncaptured settlement contributes nothing");

        // After capture it is at-risk (PROVISIONING) — counted AND reserved, nets 0.
        c.execute("UPDATE invoice SET status='PAID' WHERE id='inv-o'", []).unwrap();
        c.execute("UPDATE subscription SET state='PROVISIONING' WHERE id='s'", []).unwrap();
        assert_eq!(surplus(&c), 0, "a captured-but-at-risk receipt is reserved");

        // Only at ACTIVE does it become sweepable.
        c.execute("UPDATE subscription SET state='ACTIVE' WHERE id='s'", []).unwrap();
        assert_eq!(surplus(&c), 100_000_000);
    }

    // -- backend doubles ------------------------------------------------------

    /// A payment double for the execute/drive tests: outlay quote at `quote_fee_msat`, capped pay at
    /// `pay_fee_msat` (so the two differ to simulate a fee rise between quote and send), idempotent on
    /// the key. `available_balance_msat` PANICS — the sweep path must never read the wallet balance.
    #[derive(Default)]
    struct SweepPayment {
        inner: Mutex<SweepPayState>,
    }

    #[derive(Default)]
    struct SweepPayState {
        paid: HashSet<String>,     // keys with a recorded send
        started: HashSet<String>,  // keys payment_started_by_key reports as an in-flight op
        failed: HashSet<String>,   // keys whose pay refused (status_by_key -> Failed)
        sends: usize,              // NEW sends (not idempotent re-awaits)
        quote_fee_msat: u128,
        pay_fee_msat: u128,
    }

    impl SweepPayment {
        fn new() -> Self {
            Self::default()
        }
        fn set_fees(&self, quote_fee_msat: u128, pay_fee_msat: u128) {
            let mut st = self.inner.lock().unwrap();
            st.quote_fee_msat = quote_fee_msat;
            st.pay_fee_msat = pay_fee_msat;
        }
        /// Simulate execute having paid the key then crashed before SENT: the key is recorded paid AND
        /// reports started evidence, so the drive re-awaits by key.
        fn seed_started_paid(&self, key: &str) {
            let mut st = self.inner.lock().unwrap();
            st.paid.insert(key.to_string());
            st.started.insert(key.to_string());
        }
        /// Simulate a backend preflight/cap failure that wrote a FAILED key row, then crashed before the
        /// sweep ledger row could be parked FAILED.
        fn seed_started_failed(&self, key: &str) {
            let mut st = self.inner.lock().unwrap();
            st.failed.insert(key.to_string());
            st.started.insert(key.to_string());
        }
        fn sends(&self) -> usize {
            self.inner.lock().unwrap().sends
        }
    }

    #[async_trait]
    impl PaymentBackend for SweepPayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            unimplemented!("sweep never receives")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            unimplemented!("sweep never looks up invoices")
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            unimplemented!("sweep never looks up settlements")
        }
        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            unimplemented!("sweep uses pay_capped")
        }
        async fn refund_required_outlay_msat(
            &self,
            gross_sat: u64,
            pay_sat: Option<u64>,
        ) -> Result<u128> {
            let st = self.inner.lock().unwrap();
            let pay = pay_sat.unwrap_or(gross_sat);
            Ok(u128::from(pay) * 1000 + st.quote_fee_msat)
        }
        async fn pay_capped(
            &self,
            _bolt11: &str,
            amount_sat: u64,
            max_outlay_msat: u128,
            key: &str,
        ) -> Result<String> {
            let mut st = self.inner.lock().unwrap();
            if st.paid.contains(key) {
                return Ok(format!("sweep-pay-{key}")); // idempotent re-await, no new send
            }
            let outlay = u128::from(amount_sat) * 1000 + st.pay_fee_msat;
            if outlay > max_outlay_msat {
                // A gateway-fee rise refuses a NEW op; record a FAILED key (fedimint parity) so a
                // status re-check reads Failed.
                st.failed.insert(key.to_string());
                st.started.insert(key.to_string());
                anyhow::bail!("sweep cap refused: outlay {outlay} msat > cap {max_outlay_msat} msat");
            }
            st.sends += 1;
            st.paid.insert(key.to_string());
            st.started.insert(key.to_string());
            Ok(format!("sweep-pay-{key}"))
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            unimplemented!("sweep checks by key")
        }
        async fn payment_status_by_key(&self, key: &str) -> Result<PayStatus> {
            let st = self.inner.lock().unwrap();
            Ok(if st.paid.contains(key) {
                PayStatus::Succeeded
            } else if st.failed.contains(key) {
                PayStatus::Failed
            } else {
                PayStatus::Unknown
            })
        }
        async fn payment_started_by_key(&self, key: &str) -> Result<bool> {
            Ok(self.inner.lock().unwrap().started.contains(key))
        }
        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            panic!("the sweep path must never read the federation balance")
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            unimplemented!("sweep never watches")
        }
    }

    async fn seed_final_receipt(store: &Store, external_id: &str, sub_id: &str, amount_sat: i64) {
        let (external_id, sub_id) = (external_id.to_string(), sub_id.to_string());
        store
            .transaction(move |tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, created_at, updated_at) VALUES (?1, 'ACTIVE', 0, 0)",
                    params![sub_id],
                )?;
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES (?1, ?2, ?3, 'order', ?4, 'PAID', 0)",
                    params![format!("inv-{external_id}"), sub_id, external_id, amount_sat],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn sweep_row(store: &Store, id: &str) -> Option<(String, Option<i64>, i64)> {
        let id = id.to_string();
        store
            .read(move |c| {
                Ok(c.query_row(
                    "SELECT status, amount_sat, max_outlay_msat FROM sweep_attempt WHERE id=?1",
                    params![id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?)
            })
            .await
            .unwrap()
    }

    async fn event_kinds(store: &Store) -> Vec<String> {
        store
            .read(|c| {
                let mut stmt = c.prepare("SELECT kind FROM event_log ORDER BY id")?;
                let rows = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .unwrap()
    }

    async fn refund_attempt_count(store: &Store) -> i64 {
        store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM refund_attempt", [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    /// The status of the single sweep_attempt row (tests seed at most one per store).
    async fn single_sweep_status(store: &Store) -> String {
        store
            .read(|c| Ok(c.query_row("SELECT status FROM sweep_attempt", [], |r| r.get(0))?))
            .await
            .unwrap()
    }

    fn sweeper(store: &Store, payment: Arc<dyn PaymentBackend>) -> Sweeper {
        Sweeper::new(store.clone(), payment, Arc::new(TestClock::new(1_000)))
    }

    #[tokio::test]
    async fn quote_reports_breakdown_and_verdict() {
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let s = sweeper(&store, payment);

        let bolt11 = mint_bolt11(50_000 * 1000, META, 1_000, 3_600);
        let q = s.quote(&bolt11).await.unwrap();
        assert_eq!(q.amount_sat, 50_000);
        assert_eq!(q.outlay_msat, 50_000_000);
        assert_eq!(q.earned_msat, 100_000_000);
        assert_eq!(q.surplus_msat, 100_000_000);
        assert!(q.allow, "surplus 100_000_000 covers outlay 50_000_000");

        // A larger invoice than the surplus is REFUSEd (no writes either way).
        let big = mint_bolt11(200_000 * 1000, META, 1_000, 3_600);
        let q2 = s.quote(&big).await.unwrap();
        assert!(!q2.allow, "outlay 200_000_000 exceeds surplus 100_000_000");
    }

    #[tokio::test]
    async fn execute_happy_path_sends_journals_and_drops_expected() {
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let s = sweeper(&store, payment.clone());

        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);
        let out = s.execute(&bolt11).await.unwrap();
        assert_eq!(out.status, "SENT");
        assert!(!out.cached);
        assert_eq!(out.max_outlay_msat, 40_000_000);

        let (status, amt, cap) = sweep_row(&store, &out.id).await.unwrap();
        assert_eq!((status.as_str(), amt, cap), ("SENT", Some(40_000), 40_000_000));

        // A 'sweep' intent AND a 'sweep' sent journal row exist (kind='sweep').
        let kinds = event_kinds(&store).await;
        assert_eq!(kinds.iter().filter(|k| *k == "sweep").count(), 2);

        // The SENT sweep cap drops ledger expected holdings: 100_000_000 − 40_000_000.
        let expected = crate::ledger::expected_msat(&store, &payment).await.unwrap();
        assert_eq!(expected, 60_000_000);

        // Nothing was written to refund_attempt (a sweep never enters refund liability).
        assert_eq!(refund_attempt_count(&store).await, 0);
    }

    #[tokio::test]
    async fn resubmit_same_invoice_returns_cached_success() {
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        let payment = Arc::new(SweepPayment::new());
        let s = sweeper(&store, payment.clone());

        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);
        let first = s.execute(&bolt11).await.unwrap();
        assert!(!first.cached);
        assert_eq!(payment.sends(), 1);

        let second = s.execute(&bolt11).await.unwrap();
        assert!(second.cached, "re-submit returns the cached SENT row");
        assert_eq!(second.status, "SENT");
        assert_eq!(payment.sends(), 1, "no second pay");
    }

    #[tokio::test]
    async fn zero_amount_is_sweep_invalid_and_writes_nothing() {
        let store = mem_store();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let s = sweeper(&store, payment);

        let zero = mint_bolt11(0, META, 1_000, 3_600); // amountless bolt11
        let err = s.execute(&zero).await.unwrap_err();
        assert_eq!(err.code(), "sweep_invalid");
        assert_eq!(refund_attempt_count(&store).await, 0);
        let n: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM sweep_attempt", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(n, 0, "a refused sweep writes no sweep_attempt row");
    }

    #[tokio::test]
    async fn expired_invoice_is_rejected_up_front_before_intent() {
        let store = mem_store();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let s = sweeper(&store, payment); // clock now = 1_000

        // Minted at ts=100, expiry 10s -> expired at 110, well before the daemon clock's now=1_000.
        let expired = mint_bolt11(50_000 * 1000, META, 100, 10);
        assert_eq!(
            s.quote(&expired).await.unwrap_err().code(),
            "sweep_invalid",
            "quote refuses an expired invoice"
        );
        let err = s.execute(&expired).await.unwrap_err();
        assert_eq!(err.code(), "sweep_invalid", "execute refuses before writing intent");
        let n: i64 = store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM sweep_attempt", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(n, 0, "no doomed PENDING row is written for an expired invoice");
    }

    #[tokio::test]
    async fn unpriceable_quote_refuses() {
        struct Unpriceable;
        #[async_trait]
        impl PaymentBackend for Unpriceable {
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
            async fn refund_required_outlay_msat(&self, _: u64, _: Option<u64>) -> Result<u128> {
                anyhow::bail!("gateway down")
            }
            async fn payment_status(&self, _: &str) -> Result<PayStatus> {
                unimplemented!()
            }
            async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
                unimplemented!()
            }
            async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
                unimplemented!()
            }
        }
        let store = mem_store();
        let payment: Arc<dyn PaymentBackend> = Arc::new(Unpriceable);
        let s = sweeper(&store, payment);
        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);
        assert_eq!(s.quote(&bolt11).await.unwrap_err().code(), "sweep_unpriceable");
        assert_eq!(s.execute(&bolt11).await.unwrap_err().code(), "sweep_unpriceable");
    }

    #[tokio::test]
    async fn second_concurrent_sweep_is_busy() {
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        // A DIFFERENT invoice's sweep is already PENDING.
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
                     VALUES ('sweep:other', 'lnbc1', 10, 10000000, 'PENDING', 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let s = sweeper(&store, payment);
        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);
        assert_eq!(s.execute(&bolt11).await.unwrap_err().code(), "sweep_busy");
    }

    #[tokio::test]
    async fn fee_rise_between_quote_and_send_refuses_and_parks_failed() {
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        // Quote sees fee 0 (outlay = amount*1000); the send sees a risen fee of 100 msat.
        let payment = Arc::new(SweepPayment::new());
        payment.set_fees(0, 100);
        let s = sweeper(&store, payment.clone());

        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);
        let err = s.execute(&bolt11).await.unwrap_err();
        assert_eq!(err.code(), "sweep_fee_rose");
        assert_eq!(payment.sends(), 0, "nothing paid");
        assert_eq!(single_sweep_status(&store).await, "FAILED", "the durable row is parked FAILED");
    }

    #[tokio::test]
    async fn drive_reawaits_started_key_paying_exactly_once() {
        // execute paid the key then crashed before SENT: a PENDING row + a started+paid key.
        let store = mem_store();
        let id = "sweep:deadbeef";
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
                     VALUES ('sweep:deadbeef', 'lnbc1', 40000, 40000000, 'PENDING', 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment = Arc::new(SweepPayment::new());
        payment.seed_started_paid(id);
        let s = sweeper(&store, payment.clone());

        let report = s.drive().await.unwrap();
        assert_eq!(report.sent, 1);
        assert_eq!(payment.sends(), 0, "re-await never re-sends (idempotent on key)");
        assert_eq!(sweep_row(&store, id).await.unwrap().0, "SENT");
    }

    #[tokio::test]
    async fn drive_parks_failed_key_without_bypassing_current_gate() {
        // A backend cap/preflight failure can leave a FAILED key row while the sweep ledger row is still
        // PENDING if the daemon crashes before commit_failed. Recovery must not treat the loose
        // started-evidence bit as committed funds and send after a new liability consumed the surplus.
        let store = mem_store();
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, created_at, updated_at) VALUES ('B', 'PROVISIONING', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES ('inv-B', 'B', 'order:B', 'order', 100000, 'PAID', 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
                     VALUES ('sweep:failed-key', 'lnbc1', 100000, 100000000, 'PENDING', 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment = Arc::new(SweepPayment::new());
        payment.seed_started_failed("sweep:failed-key");
        let s = sweeper(&store, payment.clone());

        let report = s.drive().await.unwrap();
        assert_eq!(report.failed, 1);
        assert_eq!(payment.sends(), 0, "a FAILED key is parked, not re-paid");
        let (status, _, _) = sweep_row(&store, "sweep:failed-key").await.unwrap();
        assert_eq!(status, "FAILED");
    }

    #[tokio::test]
    async fn drive_not_started_refuses_when_superseded_by_liability() {
        // A PENDING sweep (cap 100_000_000) whose funds a NEW at-risk liability has since consumed.
        let store = mem_store();
        // earned 100_000_000, but the whole 100k receipt is now at-risk (PROVISIONING) -> reserved.
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, created_at, updated_at) VALUES ('B', 'PROVISIONING', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES ('inv-B', 'B', 'order:B', 'order', 100000, 'PAID', 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at)
                     VALUES ('sweep:x', 'lnbc1', 100000, 100000000, 'PENDING', 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment = Arc::new(SweepPayment::new()); // not-started (no seeded key)
        let s = sweeper(&store, payment.clone());

        let report = s.drive().await.unwrap();
        assert_eq!(report.failed, 1);
        assert_eq!(payment.sends(), 0, "nothing paid");
        let (status, _, _) = sweep_row(&store, "sweep:x").await.unwrap();
        assert_eq!(status, "FAILED");
        let last_error: Option<String> = store
            .read(|c| Ok(c.query_row("SELECT last_error FROM sweep_attempt WHERE id='sweep:x'", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert!(last_error.unwrap().contains("superseded_by_liability"));
    }

    #[tokio::test]
    async fn no_balance_read_across_quote_execute_drive() {
        // The SweepPayment double PANICS on available_balance_msat; the whole path must never call it.
        let store = mem_store();
        seed_final_receipt(&store, "order:A", "A", 100_000).await;
        let payment = Arc::new(SweepPayment::new());
        let s = sweeper(&store, payment.clone());
        let bolt11 = mint_bolt11(40_000 * 1000, META, 1_000, 3_600);

        s.quote(&bolt11).await.unwrap();
        s.execute(&bolt11).await.unwrap();
        s.drive().await.unwrap(); // no PENDING rows left, but the read path still must not panic
        // Reaching here means available_balance_msat was never called.
    }
}
