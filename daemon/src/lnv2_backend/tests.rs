//! Unit matrix for the lnv2 backend money logic (lnrent-3d5). Every flow test drives a REAL
//! `Lnv2Payment` over an in-memory sqlite index + a scripted `FakeLnv2Ops`, so the mandated behaviors
//! run under `cargo test --workspace` without a federation, and each asserts the intended arm actually
//! FIRED ([9A] non-vacuity). The pure fee/selection helpers are tested directly where appropriate.

#![allow(clippy::disallowed_methods)] // tests exercise the bare seams; clippy.toml guards production
use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusqlite::params;
use serde_json::{json, Value};

use super::*;
use crate::backends::{Invoice, Lnv2Probe, PayStatus, PaymentBackend, PaymentStatus};
use crate::clock::{Clock, TestClock};
use crate::recipe::Recipe;
use crate::reconcile::Reconciler;
use crate::store::Store;

// --------------------------------------------------------------------------------------------------
// Scripted fake fedimint seam
// --------------------------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum SendFinalScript {
    Terminal(SendFinal),
    Ambiguous,
}

struct FakeState {
    next_op: u64,
    next_inv: u64,
    send_calls: Vec<(String, Option<String>)>, // (bolt11, gateway)
    forced_send: HashMap<String, SendAttempt>, // per-bolt11 forced response (consumed once)
    op_keys: HashMap<String, String>,          // op -> embedded lnrent_key
    // Per-bolt11 encoded msat amount. Unregistered => None (the amount preflight is skipped, so existing
    // tests using amountless fake bolt11s pass unchanged); a registered value drives the mismatch guard.
    invoice_amounts: HashMap<String, u64>,
    op_final: HashMap<String, SendFinalScript>,
    /// Ops whose `await_send_final` never returns (a federation that never settles the terminal).
    send_hangs: HashSet<String>,
    receive_final: HashMap<String, ReceiveFinal>,
    /// Ops `receive_state` reports MISSING from the operation log (a rolled-back client db).
    receive_missing: HashSet<String>,
    receive_subscriptions: HashMap<String, usize>,
    // Per-op count of transient errors `await_receive_final` returns before the scripted terminal
    // (models a federation stream blip); each call consumes one.
    receive_errors: HashMap<String, usize>,
    claimed_credit_msat: Option<u64>,
    claimed_credit_error: Option<String>,
    consensus_fee_msat: u64,
    outlay_overrides: HashMap<u128, u128>,
    outlay_calls: Vec<u128>,
    gateways: Vec<String>,
    gateway_fees: HashMap<String, Option<GatewaySendFee>>,
    gateway_errors: HashMap<String, String>,
    gateway_hangs: HashSet<String>,
    guardians_ok: bool,
    module_present: bool,
    balance: u64,
}

struct FakeLnv2Ops {
    st: Mutex<FakeState>,
}

fn zero_fee() -> GatewaySendFee {
    GatewaySendFee {
        default_base_msat: 0,
        default_ppm: 0,
        minimum_base_msat: 0,
        minimum_ppm: 0,
        default_expiration_delta: 0,
        minimum_expiration_delta: 0,
    }
}

fn flat_fee(base_msat: u64, ppm: u64) -> GatewaySendFee {
    GatewaySendFee {
        default_base_msat: base_msat,
        default_ppm: ppm,
        minimum_base_msat: base_msat,
        minimum_ppm: ppm,
        default_expiration_delta: 0,
        minimum_expiration_delta: 0,
    }
}

impl FakeLnv2Ops {
    fn new() -> Arc<Self> {
        let mut gateway_fees = HashMap::new();
        gateway_fees.insert("gw://a".to_string(), Some(zero_fee()));
        Arc::new(Self {
            st: Mutex::new(FakeState {
                next_op: 0,
                next_inv: 0,
                send_calls: Vec::new(),
                forced_send: HashMap::new(),
                op_keys: HashMap::new(),
                invoice_amounts: HashMap::new(),
                op_final: HashMap::new(),
                send_hangs: HashSet::new(),
                receive_final: HashMap::new(),
                receive_missing: HashSet::new(),
                receive_subscriptions: HashMap::new(),
                receive_errors: HashMap::new(),
                claimed_credit_msat: None,
                claimed_credit_error: None,
                consensus_fee_msat: 0,
                outlay_overrides: HashMap::new(),
                outlay_calls: Vec::new(),
                gateways: vec!["gw://a".to_string()],
                gateway_fees,
                gateway_errors: HashMap::new(),
                gateway_hangs: HashSet::new(),
                guardians_ok: true,
                module_present: true,
                balance: 1_000_000,
            }),
        })
    }

    fn force_send(&self, bolt11: &str, a: SendAttempt) {
        self.st
            .lock()
            .unwrap()
            .forced_send
            .insert(bolt11.to_string(), a);
    }
    fn set_op_key(&self, op: &str, key: &str) {
        self.st
            .lock()
            .unwrap()
            .op_keys
            .insert(op.to_string(), key.to_string());
    }
    /// Register the msat amount `bolt11` encodes, so the pay-path amount preflight can be exercised.
    fn set_invoice_amount_msat(&self, bolt11: &str, msat: u64) {
        self.st
            .lock()
            .unwrap()
            .invoice_amounts
            .insert(bolt11.to_string(), msat);
    }
    fn set_send_final(&self, op: &str, s: SendFinalScript) {
        self.st.lock().unwrap().op_final.insert(op.to_string(), s);
    }
    /// Make `await_send_final(op)` hang forever (the terminal never arrives).
    fn set_send_hangs(&self, op: &str) {
        self.st.lock().unwrap().send_hangs.insert(op.to_string());
    }
    /// Script `receive_state(op)` to answer MISSING (the op is gone from the client's log).
    #[allow(dead_code)]
    fn set_receive_missing(&self, op: &str) {
        self.st
            .lock()
            .unwrap()
            .receive_missing
            .insert(op.to_string());
    }

    fn set_receive_final(&self, op: &str, r: ReceiveFinal) {
        self.st
            .lock()
            .unwrap()
            .receive_final
            .insert(op.to_string(), r);
    }
    fn receive_subscribe_count(&self, op: &str) -> usize {
        self.st
            .lock()
            .unwrap()
            .receive_subscriptions
            .get(op)
            .copied()
            .unwrap_or(0)
    }
    /// Script `n` transient subscription errors for `op` before its terminal is observed.
    fn set_receive_errors(&self, op: &str, n: usize) {
        self.st
            .lock()
            .unwrap()
            .receive_errors
            .insert(op.to_string(), n);
    }
    /// Errors still pending for `op` — 0 proves the retry loop consumed them ([9A] non-vacuity).
    fn remaining_receive_errors(&self, op: &str) -> usize {
        self.st
            .lock()
            .unwrap()
            .receive_errors
            .get(op)
            .copied()
            .unwrap_or(0)
    }
    fn set_receive_credit_msat(&self, amount_msat: u64) {
        self.st.lock().unwrap().claimed_credit_msat = Some(amount_msat);
    }
    fn set_receive_credit_error(&self, error: &str) {
        self.st.lock().unwrap().claimed_credit_error = Some(error.to_string());
    }
    fn set_consensus_fee_msat(&self, amount_msat: u64) {
        self.st.lock().unwrap().consensus_fee_msat = amount_msat;
    }
    fn set_outlay(&self, contract_msat: u128, outlay_msat: u128) {
        self.st
            .lock()
            .unwrap()
            .outlay_overrides
            .insert(contract_msat, outlay_msat);
    }
    fn outlay_calls(&self) -> Vec<u128> {
        self.st.lock().unwrap().outlay_calls.clone()
    }
    fn set_gateways(&self, gws: Vec<String>) {
        self.st.lock().unwrap().gateways = gws;
    }
    fn set_gateway_fee(&self, gw: &str, fee: Option<GatewaySendFee>) {
        self.st
            .lock()
            .unwrap()
            .gateway_fees
            .insert(gw.to_string(), fee);
    }
    fn set_gateway_error(&self, gw: &str, error: &str) {
        self.st
            .lock()
            .unwrap()
            .gateway_errors
            .insert(gw.to_string(), error.to_string());
    }
    fn set_gateway_hang(&self, gw: &str) {
        self.st.lock().unwrap().gateway_hangs.insert(gw.to_string());
    }
    fn set_guardians(&self, ok: bool) {
        self.st.lock().unwrap().guardians_ok = ok;
    }
    fn set_module_present(&self, present: bool) {
        self.st.lock().unwrap().module_present = present;
    }
    fn send_count(&self) -> usize {
        self.st.lock().unwrap().send_calls.len()
    }
    /// The (bolt11, gateway) of the most recent `send()` — proves WHICH gateway actually funded ([9A]).
    fn last_send_call(&self) -> Option<(String, Option<String>)> {
        self.st.lock().unwrap().send_calls.last().cloned()
    }
}

#[async_trait]
impl Lnv2Ops for FakeLnv2Ops {
    async fn receive(
        &self,
        _amount_msat: u64,
        _expiry_s: u32,
        _memo: &str,
        _custom_meta: Value,
    ) -> Result<Lnv2NewInvoice> {
        let mut st = self.st.lock().unwrap();
        st.next_inv += 1;
        let n = st.next_inv;
        Ok(Lnv2NewInvoice {
            bolt11: format!("lnbcfake{n}"),
            payment_hash: format!("{n:064x}"),
            op: format!("rop{n}"),
        })
    }

    async fn await_receive_final(&self, op: &str) -> Result<ReceiveFinal> {
        *self
            .st
            .lock()
            .unwrap()
            .receive_subscriptions
            .entry(op.to_string())
            .or_default() += 1;
        // A scripted transient error (federation stream blip) is consumed first, so the backend's
        // re-subscribe loop must call again to make progress.
        {
            let mut st = self.st.lock().unwrap();
            if let Some(n) = st.receive_errors.get_mut(op) {
                if *n > 0 {
                    *n -= 1;
                    return Err(anyhow!("receive subscription stream error"));
                }
            }
        }
        // Poll until the test scripts a terminal for this op, capped so a stray task can't spin.
        for _ in 0..1000 {
            if let Some(r) = self.st.lock().unwrap().receive_final.get(op).copied() {
                return Ok(r);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(ReceiveFinal::Expired)
    }

    async fn receive_state(&self, op: &str) -> Result<ReceiveProbe> {
        let st = self.st.lock().unwrap();
        if st.receive_missing.contains(op) {
            return Ok(ReceiveProbe::Missing);
        }
        Ok(match st.receive_final.get(op).copied() {
            Some(ReceiveFinal::Claimed) => ReceiveProbe::Claimed,
            Some(ReceiveFinal::Expired) => ReceiveProbe::Expired,
            Some(ReceiveFinal::Failure) => ReceiveProbe::Failure,
            None => ReceiveProbe::Pending,
        })
    }

    async fn claimed_credit_msat(&self, _op: &str) -> Result<u64> {
        let st = self.st.lock().unwrap();
        if let Some(error) = &st.claimed_credit_error {
            return Err(anyhow!(error.clone()));
        }
        Ok(st.claimed_credit_msat.unwrap_or(1_000_000))
    }

    async fn send(&self, bolt11: &str, gateway: Option<&str>, custom_meta: Value) -> SendAttempt {
        let mut st = self.st.lock().unwrap();
        st.send_calls
            .push((bolt11.to_string(), gateway.map(str::to_string)));
        let key = extract_lnrent_key(&custom_meta).unwrap_or_default();
        if let Some(forced) = st.forced_send.remove(bolt11) {
            if let SendAttempt::Started(op)
            | SendAttempt::InProgress(op)
            | SendAttempt::AlreadyPaid(op) = &forced
            {
                st.op_keys.entry(op.clone()).or_insert_with(|| key.clone());
            }
            return forced;
        }
        st.next_op += 1;
        let op = format!("op{}", st.next_op);
        st.op_keys.insert(op.clone(), key);
        SendAttempt::Started(op)
    }

    fn send_operation_id(&self, bolt11: &str) -> Result<String> {
        Ok(format!("prepared:{bolt11}"))
    }

    fn invoice_amount_msat(&self, bolt11: &str) -> Result<Option<u64>> {
        Ok(self.st.lock().unwrap().invoice_amounts.get(bolt11).copied())
    }

    async fn await_send_final(&self, op: &str) -> Result<SendFinal> {
        if self.st.lock().unwrap().send_hangs.contains(op) {
            pending::<()>().await;
            unreachable!("pending future never completes");
        }
        match self.st.lock().unwrap().op_final.get(op).copied() {
            Some(SendFinalScript::Terminal(f)) => Ok(f),
            Some(SendFinalScript::Ambiguous) => Err(anyhow!("ambiguous / timed out")),
            None => Ok(SendFinal::Success), // default: the send lands
        }
    }

    async fn send_op_lnrent_key(&self, op: &str) -> Result<SendOpLookup> {
        Ok(match self.st.lock().unwrap().op_keys.get(op).cloned() {
            Some(key) => SendOpLookup::Present((!key.is_empty()).then_some(key)),
            None => SendOpLookup::Missing,
        })
    }

    async fn list_gateways(&self) -> Result<Vec<String>> {
        Ok(self.st.lock().unwrap().gateways.clone())
    }

    async fn gateway_send_fee(&self, gateway: &str) -> Result<Option<GatewaySendFee>> {
        let (hangs, error, fee) = {
            let st = self.st.lock().unwrap();
            (
                st.gateway_hangs.contains(gateway),
                st.gateway_errors.get(gateway).cloned(),
                st.gateway_fees.get(gateway).cloned().unwrap_or(None),
            )
        };
        if hangs {
            pending::<()>().await;
            unreachable!("pending future never completes");
        }
        if let Some(error) = error {
            return Err(anyhow!(error.clone()));
        }
        Ok(fee)
    }

    async fn outlay_for_contract_msat(&self, contract_msat: u128) -> Result<u128> {
        if contract_msat == 0 {
            return Ok(0);
        }
        let mut st = self.st.lock().unwrap();
        st.outlay_calls.push(contract_msat);
        Ok(st
            .outlay_overrides
            .get(&contract_msat)
            .copied()
            .unwrap_or_else(|| contract_msat.saturating_add(u128::from(st.consensus_fee_msat))))
    }

    async fn balance_msat(&self) -> Result<u64> {
        Ok(self.st.lock().unwrap().balance)
    }

    async fn guardians_reachable(&self) -> Result<()> {
        if self.st.lock().unwrap().guardians_ok {
            Ok(())
        } else {
            Err(anyhow!("guardians down"))
        }
    }

    async fn lnv2_module_present(&self) -> bool {
        self.st.lock().unwrap().module_present
    }
}

/// An in-memory state DB at the full runtime schema (ADR-0022: the backend's correlation tables live
/// in `lnrent.sqlite`, so a backend test needs the store, not a throwaway file).
fn test_store() -> Store {
    Store::spawn(crate::store::open_memory().expect("in-memory store"))
}

fn backend_with(fake: Arc<FakeLnv2Ops>, clock: Arc<dyn Clock>) -> Lnv2Payment {
    Lnv2Payment::with_ops(fake, test_store(), clock)
}

/// The CALLER's half of the ADR-0022 contract, as the daemon's issuance and send drivers perform it:
/// `issue_invoice` + commit of its `persist` under `Store::transaction_then` (lease + hook), and
/// `prepare_pay` + commit of its PREPARED row before `pay`. Every test that used to call the backend's
/// row-creating entry points directly goes through these, so the flows under test are the production
/// ones.
#[async_trait]
trait TestBackend {
    async fn create_invoice_t(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice>;
    async fn prepare_t(&self, key: &str, bolt11: &str) -> Result<()>;
    async fn pay_refund_capped_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        key: &str,
    ) -> Result<String>;
    async fn pay_refund_capped_via_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        key: &str,
        gateway_hint: Option<&str>,
    ) -> Result<String>;
    async fn pay_capped_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        key: &str,
    ) -> Result<String>;
}

#[async_trait]
impl TestBackend for Lnv2Payment {
    async fn create_invoice_t(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        let issued = self
            .issue_invoice(amount_sat, memo, expiry_s, external_id)
            .await?;
        let persist = issued.persist;
        self.store
            .transaction_then(move |tx| persist(tx), issued.lease, issued.after_commit)
            .await?;
        Ok(issued.invoice)
    }

    async fn prepare_t(&self, key: &str, bolt11: &str) -> Result<()> {
        let prepared = self.prepare_pay(key, bolt11).await?;
        let persist = prepared.persist;
        self.store
            .transaction_then(move |tx| persist(tx), prepared.lease, None)
            .await
    }

    async fn pay_refund_capped_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        key: &str,
    ) -> Result<String> {
        self.prepare_t(key, bolt11).await?;
        self.pay_refund_capped(bolt11, amount_sat, gross_sat, key)
            .await
    }

    async fn pay_refund_capped_via_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        gross_sat: u64,
        key: &str,
        gateway_hint: Option<&str>,
    ) -> Result<String> {
        self.prepare_t(key, bolt11).await?;
        self.pay_refund_capped_via(bolt11, amount_sat, gross_sat, key, gateway_hint)
            .await
    }

    async fn pay_capped_t(
        &self,
        bolt11: &str,
        amount_sat: u64,
        max_outlay_msat: u128,
        key: &str,
    ) -> Result<String> {
        self.prepare_t(key, bolt11).await?;
        self.pay_capped(bolt11, amount_sat, max_outlay_msat, key)
            .await
    }
}

fn clock(now: i64) -> Arc<dyn Clock> {
    Arc::new(TestClock::new(now))
}

// --------------------------------------------------------------------------------------------------
// Pure helpers
// --------------------------------------------------------------------------------------------------

#[test]
fn fee_matches_payment_fee_absolute_fee() {
    // pay_msat*ppm/1e6 + base, integer floor — mirrors PaymentFee::absolute_fee byte-for-byte.
    assert_eq!(lnv2_fee_msat(0, 0, 1_000_000), 0);
    assert_eq!(lnv2_fee_msat(100_000, 0, 1_000_000), 100_000); // flat base
    assert_eq!(lnv2_fee_msat(0, 10_000, 1_000_000), 10_000); // 1% of 1_000_000 msat
    assert_eq!(lnv2_fee_msat(2_000, 3_000, 1_000_000), 2_000 + 3_000); // TRANSACTION_FEE_DEFAULT shape
                                                                       // floor on the proportional step
    assert_eq!(lnv2_fee_msat(0, 1, 999), 0);
}

#[test]
fn lnv2_send_usable_enforces_limits_componentwise_stricter_than_send() {
    // lnrent's selection guard enforces the client's hard limits (lib.rs:576-582): 100 sat + 1.5% fee
    // AND expiration_delta <= EXPIRATION_DELTA_LIMIT [1440], for BOTH the default and minimum schedules.
    // The guard is COMPONENTWISE (base AND ppm each within limit) and thus deliberately STRICTER than
    // send()'s LEXICOGRAPHIC send_fee.le(&SEND_FEE_LIMIT) gate (lnrent-z2v).
    let usable = |d_base, d_ppm, m_base, m_ppm, d_exp, m_exp| {
        lnv2_send_usable(&GatewaySendFee {
            default_base_msat: d_base,
            default_ppm: d_ppm,
            minimum_base_msat: m_base,
            minimum_ppm: m_ppm,
            default_expiration_delta: d_exp,
            minimum_expiration_delta: m_exp,
        })
    };
    // Exactly at the fee + expiration limits: usable.
    assert!(usable(100_000, 15_000, 100_000, 15_000, 1440, 1440));
    // Base one msat over the limit: refused.
    assert!(!usable(100_001, 0, 0, 0, 0, 0));
    // Base at the limit, ppm one over: refused.
    assert!(!usable(100_000, 15_001, 0, 0, 0, 0));
    // z2v regression anchor: low base (just under the 100_000 limit) + a ppm FAR over the 15_000 limit.
    // The OLD lexicographic compare ACCEPTED this (base 99_999 < 100_000 sorts the tuple below the limit
    // regardless of ppm); the componentwise guard REJECTS it so its proportional fee can never blow the
    // ~1.5%+100sat ceiling / INV-1 cap.
    assert!(!usable(99_999, 1_000_000, 99_999, 1_000_000, 1440, 1440));
    // A cheap DEFAULT but an over-limit MINIMUM schedule is still refused (both must pass).
    assert!(!usable(0, 0, 100_001, 0, 0, 0));
    // Expiration one block over the limit on either schedule: refused.
    assert!(!usable(0, 0, 0, 0, 1441, 0));
    assert!(!usable(0, 0, 0, 0, 0, 1441));
    // Well inside every limit: usable (a genuinely cheap gateway is NOT falsely rejected).
    assert!(usable(0, 10_000, 0, 10_000, 100, 100));
}

#[tokio::test]
async fn net_payout_respects_inv1_and_is_exact_at_the_boundary() {
    // Zero fee: the whole gross is payable.
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1));
    assert_eq!(backend.refund_quote(1000).await.unwrap().net_sat, 1000);
    // 1% proportional: payout p (msat) + p/100 <= gross*1000. gross=1010 sat => 1000 sat fits
    // (1_000_000 + 10_000 = 1_010_000), 1001 does not (1_001_000 + 10_010 > 1_010_000).
    let fee = flat_fee(0, 10_000);
    fake.set_gateway_fee("gw://a", Some(fee));
    assert_eq!(backend.refund_quote(1010).await.unwrap().net_sat, 1000);
    // 100-sat flat base on a 100-sat gross: exactly dust (100 payout + 100 fee = 200 > 100).
    fake.set_gateway_fee("gw://a", Some(flat_fee(100_000, 0)));
    assert_eq!(backend.refund_quote(100).await.unwrap().net_sat, 0);
    // 100-sat flat base on 150 gross: at most 50 payout (50 + 100 = 150).
    assert_eq!(backend.refund_quote(150).await.unwrap().net_sat, 50);
}

#[tokio::test]
async fn net_payout_uses_the_worse_of_default_and_minimum() {
    // default cheaper, minimum dearer: the cap must reserve the dearer (minimum) fee.
    let fee = GatewaySendFee {
        default_base_msat: 0,
        default_ppm: 0,
        minimum_base_msat: 100_000, // 100 sat
        minimum_ppm: 0,
        default_expiration_delta: 0,
        minimum_expiration_delta: 0,
    };
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_fee("gw://a", Some(fee));
    let backend = backend_with(fake, clock(1));
    assert_eq!(backend.refund_quote(150).await.unwrap().net_sat, 50);
}

#[tokio::test]
async fn caps_dry_run_both_gateway_schedules_when_mint_funding_is_non_monotone() {
    // Upstream `send_parameters` uses the 50-sat MINIMUM schedule for a direct swap and the 100-sat
    // DEFAULT otherwise. The smaller 1_050_000-msat contract can nevertheless select a dearer set of
    // mint notes than the 1_100_000-msat contract, so gateway fee alone does not identify worst outlay.
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_fee(
        "gw://a",
        Some(GatewaySendFee {
            default_base_msat: 100_000,
            default_ppm: 0,
            minimum_base_msat: 50_000,
            minimum_ppm: 0,
            default_expiration_delta: 0,
            minimum_expiration_delta: 0,
        }),
    );
    fake.set_outlay(1_100_000, 1_100_000);
    fake.set_outlay(1_050_000, 1_200_000);
    let backend = backend_with(fake.clone(), clock(1));

    let refund_err = backend
        .pay_refund_capped_t("lnbcNonMonotoneRefund", 1000, 1150, "keyNonMonotoneRefund")
        .await
        .expect_err("the direct-swap funding outlay exceeds the refund cap");
    assert!(format!("{refund_err:#}").contains("INV-1 cap"));

    let sweep_err = backend
        .pay_capped_t(
            "lnbcNonMonotoneSweep",
            1000,
            1_150_000,
            "keyNonMonotoneSweep",
        )
        .await
        .expect_err("the direct-swap funding outlay exceeds the sweep cap");
    assert!(format!("{sweep_err:#}").contains("INV-1 cap"));
    assert_eq!(
        fake.send_count(),
        0,
        "both cap-refusal arms fired before send ([9A])"
    );
    let calls = fake.outlay_calls();
    assert_eq!(
        calls.iter().filter(|&&amount| amount == 1_100_000).count(),
        2,
        "each cap preflight dry-ran the default-schedule contract"
    );
    assert_eq!(
        calls.iter().filter(|&&amount| amount == 1_050_000).count(),
        2,
        "each cap preflight dry-ran the minimum-schedule contract"
    );
}

#[test]
fn ordered_with_preference_puts_the_hint_first_and_dedups() {
    let gws = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    assert_eq!(
        lnv2_ordered_with_preference(Some("b"), &gws),
        vec!["b", "a", "c"]
    );
    // an unknown hint (a gateway that left the federation) is dropped, not prepended.
    assert_eq!(
        lnv2_ordered_with_preference(Some("z"), &gws),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        lnv2_ordered_with_preference(None, &gws),
        vec!["a", "b", "c"]
    );
}

#[test]
fn extract_key_reads_lnrent_key() {
    assert_eq!(
        extract_lnrent_key(&json!({ "lnrent_key": "refund:x:g1" })),
        Some("refund:x:g1".to_string())
    );
    assert_eq!(extract_lnrent_key(&json!({ "other": 1 })), None);
}

// --------------------------------------------------------------------------------------------------
// Pay idempotency + crash windows
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_key_same_invoice_dedups_to_one_send() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    let a = backend
        .pay_refund_capped_t("lnbcD", 500, 500, "keyD")
        .await
        .expect("first pay succeeds");
    let b = backend
        .pay_refund_capped_t("lnbcD", 500, 500, "keyD")
        .await
        .expect("second pay is idempotent");
    assert_eq!(a, b, "same key -> same op id");
    assert_eq!(fake.send_count(), 1, "the second pay must NOT send again");
    assert_eq!(
        backend.payment_status_by_key("keyD").await.unwrap(),
        PayStatus::Succeeded
    );
}

#[tokio::test]
async fn different_key_same_invoice_fails_closed_8a() {
    let fake = FakeLnv2Ops::new();
    // The invoice was already paid by a FOREIGN order (key1); its op carries key1.
    fake.set_op_key("opForeign", "key1");
    fake.force_send("lnbcX", SendAttempt::AlreadyPaid("opForeign".to_string()));
    let backend = backend_with(fake.clone(), clock(1000));

    let err = backend
        .pay_refund_capped_t("lnbcX", 500, 500, "key2")
        .await
        .expect_err("[8A] must fail closed on a cross-order same-invoice collision");
    assert!(
        format!("{err:#}").contains("[8A]"),
        "the failure names the [8A] guard: {err:#}"
    );
    // We must NOT have adopted the foreign op as key2's success — and OUR key is parked FAILED (never
    // left Unknown), so the Refunder re-resolves a fresh invoice instead of re-awaiting this dead bolt11
    // forever (the collision-liveness fix).
    assert_eq!(
        backend.payment_status_by_key("key2").await.unwrap(),
        PayStatus::Failed,
        "the collision parks OUR key FAILED, never crediting the foreign payment"
    );
    // payment_status_by_op for the foreign op must not be bound to key2 either: our FAILED row carries a
    // sentinel op, so the foreign op is absent from our pay index.
    assert_eq!(
        backend.payment_status("opForeign").await.unwrap(),
        PayStatus::Unknown,
        "the foreign op is not recorded in our pay index"
    );
}

#[tokio::test]
async fn different_key_same_invoice_after_failure_fails_before_attempt_advance() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    let op0 = fake.send_operation_id("lnbcFailedElsewhere").unwrap();
    // Upstream lnv2 advances a terminally-failed invoice from attempt 0 to attempt 1 instead of
    // returning a dedup error. The operation-log precheck must therefore catch the foreign attempt 0
    // BEFORE send(), or key2 could fund a second contract for the same bolt11 without [8A] firing.
    fake.set_op_key(&op0, "key1");
    fake.set_send_final(&op0, SendFinalScript::Terminal(SendFinal::Failure));

    let err = backend
        .pay_refund_capped_t("lnbcFailedElsewhere", 500, 500, "key2")
        .await
        .expect_err("[8A] must reject an existing foreign attempt before lnv2 advances it");
    assert!(format!("{err:#}").contains("[8A]"), "{err:#}");
    assert_eq!(
        fake.send_count(),
        0,
        "the foreign attempt-0 arm fired before any attempt-1 send ([9A])"
    );
    // The foreign operation is never persisted as key2's op, but key2 IS parked FAILED so the Refunder
    // re-resolves a fresh invoice instead of re-awaiting this permanently-unusable bolt11.
    assert_eq!(
        backend.payment_status_by_key("key2").await.unwrap(),
        PayStatus::Failed,
        "the collision parks OUR key FAILED to unlock a fresh-invoice re-resolution"
    );
    // The FAILED row must NOT carry the foreign op — pay_status_by_op(foreign) stays Unknown.
    assert_eq!(
        backend.payment_status(&op0).await.unwrap(),
        PayStatus::Unknown,
        "our FAILED row uses a sentinel op, never the foreign operation"
    );
}

#[tokio::test]
async fn collision_failed_key_never_resends_and_stays_failed() {
    // Liveness after an [8A] collision (reviewer round 4): once OUR key is parked FAILED, a re-drive
    // (the generation gate re-planning) must return the terminal WITHOUT a second send — the FAILED row
    // is what unlocks a fresh-invoice re-resolution, and re-sending the dead bolt11 would loop forever.
    let fake = FakeLnv2Ops::new();
    fake.set_op_key("opForeign", "key1");
    fake.force_send(
        "lnbcShared",
        SendAttempt::AlreadyPaid("opForeign".to_string()),
    );
    let backend = backend_with(fake.clone(), clock(1000));

    backend
        .pay_refund_capped_t("lnbcShared", 500, 500, "key2")
        .await
        .expect_err("[8A] collision fails closed");
    assert_eq!(
        backend.payment_status_by_key("key2").await.unwrap(),
        PayStatus::Failed
    );
    let sends_after_collision = fake.send_count();

    // Re-drive the SAME key: the FAILED row short-circuits before any send (NO-RETRY on the same bolt11).
    backend
        .pay_refund_capped_t("lnbcShared", 500, 500, "key2")
        .await
        .expect_err("a FAILED collision key never re-pays the same invoice");
    assert_eq!(
        fake.send_count(),
        sends_after_collision,
        "the FAILED collision key must NOT send again ([9A] NO-RETRY)"
    );
    assert_eq!(
        backend.payment_status_by_key("key2").await.unwrap(),
        PayStatus::Failed,
        "still FAILED so the Refunder re-resolves a fresh invoice at the next generation"
    );
}

#[tokio::test]
async fn invoice_amount_larger_than_owed_is_refused_before_send() {
    // Reviewer round 4: lnv2 send() funds the invoice's ENCODED amount, so an invoice for MORE than we
    // owe (a buggy/hostile resolver) must be refused BEFORE the cap/send — otherwise the cap, computed on
    // the small declared amount_sat, passes while send() overspends past the INV-1 cap. Parity with the
    // lnv1 `inv_msat != pay_msat` guard.
    let fake = FakeLnv2Ops::new();
    // We owe 500 sat, but the resolved invoice encodes 900 sat (900_000 msat).
    fake.set_invoice_amount_msat("lnbcBig", 900_000);
    let backend = backend_with(fake.clone(), clock(1000));

    let err = backend
        .pay_refund_capped_t("lnbcBig", 500, 500, "keyBig")
        .await
        .expect_err("an over-amount invoice must be refused before funding");
    assert!(
        format!("{err:#}").contains("invoice amount"),
        "the failure names the amount mismatch: {err:#}"
    );
    assert_eq!(
        fake.send_count(),
        0,
        "the amount preflight fired before any send ([9A])"
    );
    // FAILED (not Unknown) so the Refunder re-resolves a fresh invoice, exactly like the over-cap arm.
    assert_eq!(
        backend.payment_status_by_key("keyBig").await.unwrap(),
        PayStatus::Failed,
        "an amount mismatch parks FAILED to unlock fresh-invoice re-resolution"
    );
}

#[tokio::test]
async fn invoice_amount_equal_to_owed_pays_normally() {
    // Non-vacuity companion: with the encoded amount registered and EQUAL to what we owe, the preflight
    // is a no-op and the pay lands — proving the guard rejects on mismatch, not merely on registration.
    let fake = FakeLnv2Ops::new();
    fake.set_invoice_amount_msat("lnbcExact", 500_000);
    let backend = backend_with(fake.clone(), clock(1000));

    backend
        .pay_refund_capped_t("lnbcExact", 500, 500, "keyExact")
        .await
        .expect("a matching-amount invoice pays through the preflight");
    assert_eq!(fake.send_count(), 1, "the send actually fired ([9A])");
    assert_eq!(
        backend.payment_status_by_key("keyExact").await.unwrap(),
        PayStatus::Succeeded
    );
}

#[tokio::test]
async fn crash_before_send_commits_recovers_by_resending() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    // Simulate the process dying after the PREPARED intent commit but before `send()` committed an op.
    let op0 = fake.send_operation_id("lnbcC").unwrap();
    let op_w = op0.clone();
    backend
        .store
        .transaction(move |tx| pay_insert_prepared(tx, "keyC", "lnbcC", &op_w))
        .await
        .unwrap();

    let op = backend
        .pay_refund_capped_t("lnbcC", 500, 500, "keyC")
        .await
        .expect("PREPARED+missing recovery rechecks the cap and sends");
    assert_eq!(op, "op1");
    assert_eq!(
        fake.send_count(),
        1,
        "the crash-before-send recovery arm fired exactly one send ([9A])"
    );
    assert_eq!(
        backend.payment_status_by_key("keyC").await.unwrap(),
        PayStatus::Succeeded
    );
    assert_eq!(
        backend.payment_status(&op).await.unwrap(),
        PayStatus::Succeeded
    );
}

#[tokio::test]
async fn prepared_mapping_for_a_different_invoice_fails_closed() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    let op_a = fake.send_operation_id("lnbcA").unwrap();
    let op_w = op_a.clone();
    backend
        .store
        .transaction(move |tx| pay_insert_prepared(tx, "keyMismatch", "lnbcA", &op_w))
        .await
        .unwrap();

    let err = backend
        .pay_refund_capped_t("lnbcB", 500, 500, "keyMismatch")
        .await
        .expect_err("a durable key cannot be rebound to another invoice");
    assert!(
        format!("{err:#}").contains("prepared operation mismatch"),
        "the persisted key/invoice guard fired: {err:#}"
    );
    assert_eq!(
        fake.send_count(),
        0,
        "mapping mismatch fails before any backend send ([9A])"
    );
    let row = backend
        .store
        .read(|c| pay_get(c, "keyMismatch"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.operation_id, op_a, "the original mapping is unchanged");
    assert_eq!(row.status, "PREPARED");
}

#[test]
fn failed_to_fund_is_a_definite_retryable_no_operation_error() {
    let classified =
        real::classify_send_error(fedimint_lnv2_client::SendPaymentError::FailedToFundPayment(
            "insufficient ecash".to_string(),
        ));
    assert!(
        matches!(classified, SendAttempt::Retryable(ref e) if e == "insufficient ecash"),
        "the real FailedToFundPayment arm must clear PREPARED, not expose a nonexistent Pending op"
    );
}

#[tokio::test]
async fn definitive_prefunding_send_error_is_terminal() {
    let fake = FakeLnv2Ops::new();
    fake.force_send(
        "lnbcExpired",
        SendAttempt::Rejected("invoice expired".to_string()),
    );
    let backend = backend_with(fake.clone(), clock(1000));

    let err = backend
        .pay_refund_capped_t("lnbcExpired", 500, 500, "keyExpired")
        .await
        .expect_err("a definitive invoice rejection cannot succeed on a retry");
    assert!(format!("{err:#}").contains("invoice expired"));
    assert_eq!(
        backend.payment_status_by_key("keyExpired").await.unwrap(),
        PayStatus::Failed,
        "a fresh, definitely pre-funding rejection must unlock fresh-invoice generation"
    );
    assert_eq!(
        fake.send_count(),
        1,
        "the rejection arm actually fired ([9A])"
    );
}

#[tokio::test]
async fn crash_after_send_commits_adopts_the_inflight_op() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    // Simulate the crash window: the intent row is persisted but the op was never recorded.
    let op0 = fake.send_operation_id("lnbcA").unwrap();
    let op_w = op0.clone();
    backend
        .store
        .transaction(move |tx| pay_insert_prepared(tx, "keyA", "lnbcA", &op_w))
        .await
        .unwrap();
    // The federation already funded op0 under OUR key; recovery finds and awaits it directly.
    fake.set_op_key(&op0, "keyA");

    let op = backend
        .pay_refund_capped_t("lnbcA", 500, 500, "keyA")
        .await
        .expect("adopts the in-flight op and awaits it to Success");
    assert_eq!(
        op, op0,
        "adopted the already-funded operation, not a fresh one"
    );
    assert_eq!(
        fake.send_count(),
        0,
        "crash recovery never submits a second send ([9A])"
    );
    assert_eq!(
        backend.payment_status_by_key("keyA").await.unwrap(),
        PayStatus::Succeeded
    );
}

// --------------------------------------------------------------------------------------------------
// Terminal mapping + NO-RETRY
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn definitive_failure_parks_failed_and_never_resends_same_bolt11() {
    let fake = FakeLnv2Ops::new();
    fake.set_op_key("opF", "keyF");
    fake.set_send_final("opF", SendFinalScript::Terminal(SendFinal::Refunded));
    fake.force_send("lnbcF", SendAttempt::Started("opF".to_string()));
    let backend = backend_with(fake.clone(), clock(1000));

    let err = backend
        .pay_refund_capped_t("lnbcF", 500, 500, "keyF")
        .await
        .expect_err("a Refunded send is a definitive failure");
    assert!(format!("{err:#}").contains("definitive failure"));
    assert_eq!(
        backend.payment_status_by_key("keyF").await.unwrap(),
        PayStatus::Failed,
        "Refunded/Failure -> Failed (the Refunder re-resolves a fresh generation)"
    );

    // NO-RETRY: re-driving the SAME key must NOT send the same bolt11 again.
    let err2 = backend
        .pay_refund_capped_t("lnbcF", 500, 500, "keyF")
        .await
        .expect_err("a FAILED key stays terminal");
    assert!(format!("{err2:#}").contains("previously failed"));
    assert_eq!(
        fake.send_count(),
        1,
        "the failed key never re-sends the same bolt11"
    );
}

#[tokio::test]
async fn ambiguous_await_stays_pending_and_reawaits_same_op() {
    let fake = FakeLnv2Ops::new();
    fake.set_op_key("opAmb", "keyAmb");
    fake.set_send_final("opAmb", SendFinalScript::Ambiguous);
    fake.force_send("lnbcAmb", SendAttempt::Started("opAmb".to_string()));
    let backend = backend_with(fake.clone(), clock(1000));

    let err = backend
        .pay_refund_capped_t("lnbcAmb", 500, 500, "keyAmb")
        .await
        .expect_err("an ambiguous await surfaces as a transient error");
    assert!(format!("{err:#}").contains("still pending"));
    assert_eq!(
        backend.payment_status_by_key("keyAmb").await.unwrap(),
        PayStatus::Pending,
        "ambiguous leaves the row PENDING with its op (never FAILED)"
    );

    // The op later settles; re-driving the key re-awaits the SAME op — no second send.
    fake.set_send_final("opAmb", SendFinalScript::Terminal(SendFinal::Success));
    let op = backend
        .pay_refund_capped_t("lnbcAmb", 500, 500, "keyAmb")
        .await
        .expect("re-await lands");
    assert_eq!(op, "opAmb");
    assert_eq!(fake.send_count(), 1, "re-await must NOT send again");
    assert_eq!(
        backend.payment_status_by_key("keyAmb").await.unwrap(),
        PayStatus::Succeeded
    );
}

// --------------------------------------------------------------------------------------------------
// INV-1 cap + fee-rise refusal + failover
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn over_cap_records_definitive_no_send_failure_7a() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_fee("gw://a", Some(flat_fee(100_000, 0))); // 100-sat flat fee
    let backend = backend_with(fake.clone(), clock(1000));

    // payout 500 + fee 100 = 600 > gross 500 -> refuse.
    let err = backend
        .pay_refund_capped_t("lnbcCap", 500, 500, "keyCap")
        .await
        .expect_err("over-cap must refuse");
    assert!(format!("{err:#}").contains("INV-1 cap"));
    assert_eq!(fake.send_count(), 0, "no send is attempted over cap");
    // [7A]: the op-log absence check ran before the cap, so this Failed is a definitive NO-SEND
    // outcome. The Refunder can safely advance and re-quote under a fresh invoice generation.
    assert_eq!(
        backend.payment_status_by_key("keyCap").await.unwrap(),
        PayStatus::Failed
    );
}

#[tokio::test]
async fn fee_rise_between_quote_and_pay_refuses() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));

    // Quote at zero fee: full 1000 net, hint = the priced gateway.
    let quote = backend.refund_quote(1000).await.expect("quote");
    assert_eq!(quote.net_sat, 1000);
    assert_eq!(quote.gateway_hint.as_deref(), Some("gw://a"));

    // The gateway raises its fee before the pay. 50-sat is well within lnv2's send limit (so the gateway
    // stays send-usable and is not skipped), but payout 1000 + fee 50 = 1050 sat busts the 1000-sat gross
    // INV-1 cap — this exercises the CAP refusal, not the send-policy skip.
    fake.set_gateway_fee("gw://a", Some(flat_fee(50_000, 0))); // 50-sat flat
    let err = backend
        .pay_refund_capped_via_t("lnbcFR", 1000, 1000, "keyFR", quote.gateway_hint.as_deref())
        .await
        .expect_err("a fee that rose past the cap must refuse");
    assert!(format!("{err:#}").contains("INV-1 cap"));
    assert_eq!(fake.send_count(), 0);
    assert_eq!(
        backend.payment_status_by_key("keyFR").await.unwrap(),
        PayStatus::Failed,
        "the fee-rise refusal must unlock a fresh refund generation"
    );
}

#[tokio::test]
async fn consensus_fees_reduce_quote_and_are_enforced_at_pay() {
    let fake = FakeLnv2Ops::new();
    fake.set_consensus_fee_msat(1_500);
    let backend = backend_with(fake.clone(), clock(1000));

    let quote = backend.refund_quote(1000).await.expect("quote");
    assert_eq!(
        quote.net_sat, 998,
        "the quote reserves the lnv2 output plus mint input/output consensus fees"
    );

    let err = backend
        .pay_refund_capped_t("lnbcConsensusFee", 999, 1000, "keyConsensusFee")
        .await
        .expect_err("999 sat + 1.5 sat consensus fees exceeds the 1000-sat cap");
    assert!(format!("{err:#}").contains("INV-1 cap"), "{err:#}");
    assert_eq!(
        fake.send_count(),
        0,
        "the consensus-fee cap arm fired before send ([9A])"
    );
}

#[tokio::test]
async fn retry_after_prefunding_error_rechecks_the_cap() {
    let fake = FakeLnv2Ops::new();
    fake.force_send(
        "lnbcRetryCap",
        SendAttempt::Retryable("gateway down".to_string()),
    );
    let backend = backend_with(fake.clone(), clock(1000));

    backend
        .pay_refund_capped_t("lnbcRetryCap", 500, 500, "keyRetryCap")
        .await
        .expect_err("the first gateway preflight fails before funding");
    assert_eq!(
        fake.send_count(),
        1,
        "the pre-funding error arm actually fired ([9A])"
    );

    fake.set_gateway_fee("gw://a", Some(flat_fee(100_000, 0))); // 500 + 100 > 500 gross
    let err = backend
        .pay_refund_capped_t("lnbcRetryCap", 500, 500, "keyRetryCap")
        .await
        .expect_err("the retry must not bypass INV-1 after the fee rises");
    assert!(format!("{err:#}").contains("INV-1 cap"));
    assert_eq!(
        fake.send_count(),
        1,
        "the over-cap retry is refused before a second backend send ([9A])"
    );
    assert_eq!(
        backend.payment_status_by_key("keyRetryCap").await.unwrap(),
        PayStatus::Failed,
        "after the retryable send was removed, the separately-proven no-send cap refusal is terminal"
    );
}

#[tokio::test]
async fn quote_fails_over_past_an_unreachable_gateway() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec!["gw://down".to_string(), "gw://up".to_string()]);
    fake.set_gateway_fee("gw://down", None); // unreachable
    fake.set_gateway_fee("gw://up", Some(zero_fee()));
    let backend = backend_with(fake.clone(), clock(1000));

    let quote = backend
        .refund_quote(1000)
        .await
        .expect("quote fails over to gw://up");
    assert_eq!(quote.net_sat, 1000);
    assert_eq!(
        quote.gateway_hint.as_deref(),
        Some("gw://up"),
        "the unreachable primary is skipped; the reachable fallback is chosen"
    );
}

#[tokio::test]
async fn quote_times_out_a_hung_gateway_and_reaches_the_next() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec!["gw://hung".to_string(), "gw://up".to_string()]);
    fake.set_gateway_hang("gw://hung");
    fake.set_gateway_fee("gw://up", Some(zero_fee()));
    let backend = backend_with(fake, clock(1000));

    let quote = backend
        .refund_quote(1000)
        .await
        .expect("a black-holed gateway is bounded and skipped");
    assert_eq!(quote.net_sat, 1000);
    assert_eq!(
        quote.gateway_hint.as_deref(),
        Some("gw://up"),
        "the timeout arm fired and ordered failover reached the healthy gateway ([9A])"
    );
}

#[tokio::test]
async fn no_reachable_gateway_is_a_transient_err_not_dust() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_fee("gw://a", None); // the only gateway is unreachable
    let backend = backend_with(fake.clone(), clock(1000));
    assert!(
        backend.refund_net_sat(1000).await.is_err(),
        "a quote with no reachable gateway is Err (transient), never Ok(0) dust"
    );
    assert!(backend.refund_quote(1000).await.is_err());
}

#[tokio::test]
async fn pay_fails_over_past_a_send_incompatible_gateway() {
    // A responsive gateway whose send fee exceeds lnv2's SEND_FEE_LIMIT must be SKIPPED: lnv2 send()
    // would refuse it before funding, and the deterministic-order selection would re-pick it every drive
    // and strand the refund PENDING. Selection continues to a send-usable gateway instead.
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec!["gw://greedy".to_string(), "gw://ok".to_string()]);
    fake.set_gateway_fee("gw://greedy", Some(flat_fee(200_000, 0))); // 200 sat > 100 sat send limit
    fake.set_gateway_fee("gw://ok", Some(zero_fee()));
    let backend = backend_with(fake.clone(), clock(1000));

    // The quote skips the greedy gateway and prices/pins the compliant one.
    let quote = backend.refund_quote(1000).await.expect("quote fails over");
    assert_eq!(quote.net_sat, 1000);
    assert_eq!(
        quote.gateway_hint.as_deref(),
        Some("gw://ok"),
        "[9A]: the send-incompatible gateway is skipped; the compliant fallback is chosen"
    );

    // A capped pay routes via the compliant gateway and lands.
    backend
        .pay_refund_capped_via_t("lnbcFO", 1000, 1000, "keyFO", quote.gateway_hint.as_deref())
        .await
        .expect("the pay routes via the send-usable gateway");
    assert_eq!(
        backend.payment_status_by_key("keyFO").await.unwrap(),
        PayStatus::Succeeded
    );
    // [9A]: the funding send went to the compliant gateway, NEVER the refused greedy one.
    let (bolt11, gw) = fake.last_send_call().expect("a send was attempted");
    assert_eq!(bolt11, "lnbcFO");
    assert_eq!(
        gw.as_deref(),
        Some("gw://ok"),
        "funding never pins the refused gateway"
    );
}

#[tokio::test]
async fn quote_errs_when_only_gateway_is_send_incompatible() {
    // The sole gateway is reachable but send-incompatible: selection returns a TRANSIENT Err (not dust,
    // not a pinned-but-refused endpoint), so the refund stays recoverable rather than pinning a gateway
    // send() rejects on every drive. The diagnostic names the send-policy skip.
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_fee("gw://a", Some(flat_fee(200_000, 0))); // 200-sat base > lnv2 send limit
    let backend = backend_with(fake.clone(), clock(1000));
    let err = backend
        .refund_quote(1000)
        .await
        .expect_err("no send-usable gateway is a transient Err");
    assert!(
        format!("{err:#}").contains("above the lnv2 client"),
        "the diagnostic names the send-policy skip: {err:#}"
    );
    assert!(
        backend.refund_gateway_ready().await.is_err(),
        "not ready when no gateway is send-usable"
    );
}

// --------------------------------------------------------------------------------------------------
// Receive: idempotency + live-vs-recovery settlement timestamps
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn create_invoice_is_idempotent_on_external_id() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    let a = backend
        .create_invoice_t(1000, "m", 3600, "extI")
        .await
        .unwrap();
    let b = backend
        .create_invoice_t(9999, "other", 60, "extI")
        .await
        .unwrap();
    assert_eq!(a.id, b.id, "same external_id -> same invoice");
    assert_eq!(
        a.bolt11, b.bolt11,
        "an OPEN row is reused verbatim, never re-minted (create-once)"
    );
    assert_eq!(
        b.amount_sat, 1000,
        "the original amount is returned unchanged"
    );
    assert_eq!(a.expires_at, 1000 + 3600);
}

#[tokio::test]
async fn canceled_invoice_is_replaced_by_a_fresh_payable_one() {
    let ext = "renew:auto:sub1:42";
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1_000));
    let dead = backend.create_invoice_t(1000, "m", 3600, ext).await.unwrap();
    backend
        .store
        .transaction(move |tx| {
            tx.execute(
                "UPDATE lnv2_invoice SET status = 'CANCELED' WHERE external_id = ?1",
                params![ext],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let fresh = backend.create_invoice_t(1000, "m", 3600, ext).await.unwrap();
    assert_ne!(
        fresh.bolt11, dead.bolt11,
        "the expired bolt11 must never be handed back as payable"
    );
    assert_ne!(
        fresh.backend_invoice_id, dead.backend_invoice_id,
        "a fresh receive operation backs the replacement"
    );
    let (stored, status) = backend
        .store
        .read(move |c| idx_get_by_external(c, ext))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, "OPEN");
    assert_eq!(
        stored.backend_invoice_id, fresh.backend_invoice_id,
        "the canceled row is atomically replaced by the fresh operation"
    );
    assert_eq!(
        backend.lookup_settlement(&fresh.id).await.unwrap(),
        (PaymentStatus::Open, None),
        "the replacement is returned as payable"
    );
    // The daemon store can still hold the retired id; it must never look payable again.
    assert_eq!(
        backend.lookup_settlement(&dead.id).await.unwrap(),
        (PaymentStatus::Expired, None),
        "the replaced-away invoice id never reports Open/Paid"
    );
}

/// lnrent-l07s: a LOST lnv2 index row over a PAID invoice must FAIL CLOSED.
///
/// This is the bead's break-test INVERTED. Before the fix the second lookup answered
/// `Ok((Expired, None))` — and reconcile treats `Expired` as licence to expire the local invoice,
/// so a buyer who actually paid was expired with no capture, no refund and no operator DM. phoenixd
/// already fails closed on the identical shape (`phoenixd_backend/tests.rs`,
/// `lookup_settlement("phoenixd-orphan")` is `Err`).
#[tokio::test]
async fn a_lost_index_row_over_a_paid_invoice_fails_closed() {
    let fake = FakeLnv2Ops::new();
    fake.set_receive_credit_msat(995_500);
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extLOST")
        .await
        .unwrap();
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);
    let _ = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("a settlement arrives")
        .expect("channel open");

    assert_eq!(
        backend
            .lookup_settlement_by_ref(&inv.id, &inv.external_id)
            .await
            .unwrap(),
        (PaymentStatus::Paid, Some(5_000)),
        "precondition: with the index intact, this invoice is PAID"
    );

    // The incident: the lnv2 index row is gone. The MAIN store still holds the invoice and still
    // asks about it by the same id AND external_id.
    {
        let n = backend
            .store
            .transaction(|tx| Ok(tx.execute("DELETE FROM lnv2_invoice", [])?))
            .await
            .unwrap();
        assert_eq!(n, 1, "exactly the one row under test was removed");
    }

    let err = backend
        .lookup_settlement_by_ref(&inv.id, &inv.external_id)
        .await
        .expect_err("a paid invoice whose index row is gone must never report an unpaid expiry");
    let err = format!("{err:#}");
    assert!(
        err.contains("row missing"),
        "the diagnostic names the condition an operator greps for: {err}"
    );
    assert!(
        err.contains(&inv.id) && err.contains(&inv.external_id),
        "the diagnostic names the invoice id and the external_id: {err}"
    );
}

/// The retired-id contract (lnrent-9yz) survives the fail-closed fix: `idx_insert` rewrote
/// `invoice_id` in place when it replaced the CANCELED row for this `external_id`, so a row for the
/// external_id EXISTS carrying a different id. That is a RETIRED id, not a lost index — `Expired` is
/// honest, and the daemon store may still hold it. Deliberately a separate test:
/// `canceled_invoice_is_replaced_by_a_fresh_payable_one` stays green UNCHANGED.
#[tokio::test]
async fn a_retired_invoice_id_still_reports_expired_through_the_ref_lookup() {
    let ext = "renew:auto:sub1:42";
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1_000));
    let dead = backend.create_invoice_t(1000, "m", 3600, ext).await.unwrap();
    backend
        .store
        .transaction(move |tx| {
            tx.execute(
                "UPDATE lnv2_invoice SET status = 'CANCELED' WHERE external_id = ?1",
                params![ext],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let fresh = backend.create_invoice_t(1000, "m", 3600, ext).await.unwrap();
    assert_ne!(
        fresh.id, dead.id,
        "[9A]: the replacement retired the old id"
    );

    assert_eq!(
        backend
            .lookup_settlement_by_ref(&dead.id, ext)
            .await
            .unwrap(),
        (PaymentStatus::Expired, None),
        "a row for the external_id carrying a DIFFERENT id is a retired id, not a lost index"
    );
    assert_eq!(
        backend
            .lookup_settlement_by_ref(&fresh.id, ext)
            .await
            .unwrap(),
        (PaymentStatus::Open, None),
        "the replacement is still payable"
    );
}

/// A FOREIGN backend's invoice id (an operator switched backends) reports `Expired`, never `Err`.
/// `order_invoice_may_expire` returns `Ok(false)` on `Err` (reconcile.rs), so an id that errors
/// forever would hold the ORDER open and its capacity reservation HELD forever. lnv2 can only fail
/// closed for ids it could have minted itself.
#[tokio::test]
async fn a_foreign_backend_invoice_id_expires_order_and_releases_reservation() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake, clock(5_000));
    assert_eq!(
        backend
            .lookup_settlement_by_ref("phoenixd-deadbeef", "order:o1")
            .await
            .unwrap(),
        (PaymentStatus::Expired, None),
        "a foreign-prefix id must never stall reconcile behind a permanent Err"
    );

    let store = test_store();
    store
        .transaction(|tx| {
            tx.execute_batch(
                "INSERT INTO subscription
                    (id, recipe_id, state, buyer_pubkey, period_s, renew_lead_s, retention_s,
                     next_deadline, created_at, updated_at)
                 VALUES ('o1', 'dummy', 'PENDING', 'buyer', 100, 10, 0, 5, 0, 0);
                 INSERT INTO invoice
                    (id, subscription_id, external_id, kind, amount_sat, status, expires_at,
                     issued_at)
                 VALUES ('phoenixd-deadbeef', 'o1', 'order:o1', 'order', 100, 'OPEN', 5, 0);
                 INSERT INTO reservation
                    (id, order_id, resources_json, ports_json, state, expires_at, created_at)
                 VALUES ('res-o1', 'o1', '{\"cpu\":1}', '{\"count\":0}', 'HELD', 5, 0);",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let recipe = Recipe::load(format!(
        "{}/../recipes/dummy",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    let reconciler = Reconciler::new(store.clone(), Arc::new(backend), recipe);

    let report = reconciler.reconcile_tick(5_000).await.unwrap();
    assert_eq!(report.expired, 1, "the foreign-id order expiry fired");
    let states: (String, String, String) = store
        .read(|c| {
            Ok(c.query_row(
                "SELECT s.state, i.status, r.state
                   FROM subscription s
                   JOIN invoice i ON i.subscription_id=s.id
                   JOIN reservation r ON r.order_id=s.id
                  WHERE s.id='o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        states,
        (
            "EXPIRED".to_string(),
            "EXPIRED".to_string(),
            "RELEASED".to_string()
        ),
        "foreign ids expire the order and invoice and release the capacity reservation"
    );
}

#[tokio::test]
async fn paid_and_unrecovered_rows_are_still_reused_by_create_invoice() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake, clock(2_000));

    for status in ["PAID", "PAID_UNRECOVERED"] {
        let ext = format!("ext{status}");
        let original = backend
            .create_invoice_t(1000, "m", 3600, &ext)
            .await
            .unwrap();
        let ext_w = ext.clone();
        backend
            .store
            .transaction(move |tx| {
                tx.execute(
                    "UPDATE lnv2_invoice SET status = ?1 WHERE external_id = ?2",
                    params![status, ext_w],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let repeated = backend
            .create_invoice_t(9999, "other", 60, &ext)
            .await
            .unwrap();
        assert_eq!(
            (repeated.id, repeated.bolt11),
            (original.id, original.bolt11),
            "{status} settlement provenance must be reused, not replaced"
        );
        let ext_r = ext.clone();
        assert_eq!(
            backend
                .store
                .read(move |c| idx_get_by_external(c, &ext_r))
                .await
                .unwrap()
                .unwrap()
                .1,
            status
        );
    }
}

#[tokio::test]
async fn live_settlement_pushes_true_timestamp() {
    let fake = FakeLnv2Ops::new();
    fake.set_receive_credit_msat(995_500);
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extL")
        .await
        .unwrap();
    // The customer pays: the LIVE receive task observes Claimed.
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);

    let s = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("a settlement arrives")
        .expect("channel open");
    assert_eq!(s.external_id, "extL");
    assert_eq!(s.amount_sat, 1000);
    assert_eq!(
        s.received_msat, 995_500,
        "settlement carries the post-claim credit after consensus fees"
    );
    assert_eq!(
        s.settled_at, 5_000,
        "the LIVE settled_at is the observation time"
    );
    assert_eq!(
        backend.lookup_settlement(&inv.id).await.unwrap(),
        (PaymentStatus::Paid, Some(5_000)),
        "lnrent-zwk: a live Claimed reports Some(ts)"
    );
    assert_eq!(
        backend.received_amount_msat(&inv.id).await.unwrap(),
        Some(995_500),
        "recovery can read the same fee-adjusted credit from the durable index"
    );
}

#[tokio::test]
async fn recovery_settlement_marks_paid_without_a_timestamp_or_push() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(7_000));
    // Create the invoice with NO watcher yet (so it is OPEN), then script it already-Claimed.
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extR")
        .await
        .unwrap();
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);

    // watch() re-subscribes the OPEN invoice as a RECOVERY task (live=false).
    let mut rx = backend.watch().await.unwrap();

    // Poll until the recovery task has marked it PAID.
    let mut settled = None;
    for _ in 0..500 {
        let ls = backend.lookup_settlement(&inv.id).await.unwrap();
        if ls.0 == PaymentStatus::Paid {
            settled = Some(ls);
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        settled,
        Some((PaymentStatus::Paid, None)),
        "lnrent-zwk: a recovery Claimed reports None (unknown true time)"
    );
    // A recovery settlement does NOT push on watch().
    assert!(
        rx.try_recv().is_err(),
        "recovery must not push a Settlement (the catch-up recovers it via lookup)"
    );
}

#[tokio::test]
async fn pending_at_restart_then_claimed_is_recovery_not_reconnect_time() {
    // A boot re-subscribe (watch) of an invoice still Pending at restart. Even though the daemon
    // observes the Claimed live-in-wall-clock AFTER restart, this is RECOVERY provenance: fedimint's
    // `ModuleNotifier::subscribe` replays the op's states from the earliest `Pending` on every subscribe
    // (fedimint-client-module notifier.rs:54-138), so the backend cannot distinguish "watched from
    // before payment" from "replayed after it settled while down". Stamping reconnect-time would let a
    // payment made before expiry be observed after expiry and wrongly refunded; instead settled_at is
    // NULL and nothing is pushed — settlement catch-up supplies a conservative in-window ts (lnrent-zwk).
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(7_500));
    // The invoice predates watch/restart (created with no watcher) but has NOT been paid yet.
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extRestartPending")
        .await
        .unwrap();
    let mut rx = backend.watch().await.unwrap();

    for _ in 0..500 {
        if fake.receive_subscribe_count(&inv.backend_invoice_id) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        fake.receive_subscribe_count(&inv.backend_invoice_id),
        1,
        "[9A]: watch actually re-subscribed the operation while still Pending, before payment"
    );
    // The customer now pays; the boot-subscribed (recovery) task observes Claimed.
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);

    // Poll until the recovery task has marked it PAID.
    let mut settled = None;
    for _ in 0..500 {
        let ls = backend.lookup_settlement(&inv.id).await.unwrap();
        if ls.0 == PaymentStatus::Paid {
            settled = Some(ls);
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        settled,
        Some((PaymentStatus::Paid, None)),
        "a boot re-subscribe stamps NULL: reconnect-time is NOT a trustworthy settled_at"
    );
    assert!(
        rx.try_recv().is_err(),
        "recovery provenance must NOT push a live Settlement (catch-up recovers it via lookup)"
    );
}

#[tokio::test]
async fn receive_subscription_error_resubscribes_and_still_settles() {
    // reviewer P2: a transient receive-subscription error must NOT strand the invoice. The task
    // re-subscribes to the same op; once the customer pays, the resubscribed task still observes
    // Claimed. The blind interval means its timestamp provenance is recovery, so it marks PAID without
    // pushing and settlement catch-up supplies the conservative timestamp.
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    // Script the blip BEFORE issuing: the live watcher now starts from the store actor's post-commit
    // hook (ADR-0022), so it can be polled before this test resumes, and an error scripted after the
    // fact could land in a task that has already subscribed cleanly. The fake mints deterministic op
    // ids (`rop<n>`), so the first receive is `rop1`.
    fake.set_receive_errors("rop1", 2);
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extErr")
        .await
        .unwrap();
    assert_eq!(inv.backend_invoice_id, "rop1", "the scripted op is the one just minted");
    // First two subscription attempts error (a federation blip), then the terminal is observable.
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);

    let mut settled = None;
    for _ in 0..500 {
        let status = backend.lookup_settlement(&inv.id).await.unwrap();
        if status.0 == PaymentStatus::Paid {
            settled = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    assert_eq!(
        settled,
        Some((PaymentStatus::Paid, None)),
        "a post-error terminal has recovery provenance, never reconnect-time provenance"
    );
    assert_eq!(
        fake.remaining_receive_errors(&inv.backend_invoice_id),
        0,
        "[9A]: the retry loop actually consumed both scripted errors"
    );
    assert!(
        rx.try_recv().is_err(),
        "a post-error recovery settlement is caught up from the index, not pushed with a false time"
    );
}

#[tokio::test]
async fn receive_mint_failure_is_persisted_as_paid_unrecovered() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extMintFailure")
        .await
        .unwrap();

    // In upstream lnv2 this terminal is reachable only after Lightning payment confirmation entered
    // Claiming and mint-output issuance failed.
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Failure);

    let mut surfaced = None;
    for _ in 0..500 {
        match backend.lookup_settlement(&inv.id).await {
            Err(e) => {
                surfaced = Some(format!("{e:#}"));
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(3)).await,
        }
    }
    assert!(
        surfaced
            .as_deref()
            .is_some_and(|e| e.contains("Lightning payment") && e.contains("manual")),
        "[9A]: the paid-but-unrecovered arm must surface a durable operator-facing error"
    );
    let inv_id = inv.id.clone();
    let status: String = backend
        .store
        .read(move |c| {
            Ok(c.query_row(
                "SELECT status FROM lnv2_invoice WHERE invoice_id=?1",
                params![inv_id],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        status, "PAID_UNRECOVERED",
        "never misclassified as CANCELED"
    );
    assert!(
        rx.try_recv().is_err(),
        "unminted ecash cannot be booked as a spendable Settlement"
    );
}

#[tokio::test]
async fn claimed_credit_decode_failure_is_paid_unrecovered_not_open() {
    let fake = FakeLnv2Ops::new();
    fake.set_receive_credit_error("claim transaction cannot be decoded");
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extCreditDecodeFailure")
        .await
        .unwrap();

    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);

    let mut surfaced = None;
    for _ in 0..500 {
        match backend.lookup_settlement(&inv.id).await {
            Err(e) => {
                surfaced = Some(format!("{e:#}"));
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(3)).await,
        }
    }
    assert!(
        surfaced
            .as_deref()
            .is_some_and(|e| e.contains("Lightning payment") && e.contains("manual")),
        "[9A]: a Claimed operation with unknown exact credit becomes a durable liability"
    );
    assert!(
        rx.try_recv().is_err(),
        "unknown credit must never be exposed as a spendable Settlement"
    );
}

#[tokio::test]
async fn claimed_terminal_retries_sqlite_persistence_before_push() {
    let fake = FakeLnv2Ops::new();
    fake.set_receive_credit_msat(995_500);
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();
    let inv = backend
        .create_invoice_t(1000, "m", 3600, "extPersistRetry")
        .await
        .unwrap();
    backend
        .store
        .transaction(|tx| {
            tx.execute_batch(
                "CREATE TRIGGER fail_lnv2_terminal
                 BEFORE UPDATE OF status ON lnv2_invoice
                 BEGIN SELECT RAISE(ABORT, 'forced terminal write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();

    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        backend.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Open,
        "the injected sqlite failure kept the terminal transition from committing"
    );
    assert!(
        rx.try_recv().is_err(),
        "Settlement is not pushed before PAID is durable"
    );
    backend
        .store
        .transaction(|tx| {
            tx.execute_batch("DROP TRIGGER fail_lnv2_terminal;")?;
            Ok(())
        })
        .await
        .unwrap();

    let settlement = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("terminal persistence retry completes")
        .expect("channel open");
    assert_eq!(settlement.received_msat, 995_500);
    assert_eq!(backend.lookup(&inv.id).await.unwrap(), PaymentStatus::Paid);
    assert_eq!(
        fake.receive_subscribe_count(&inv.backend_invoice_id),
        1,
        "[9A]: sqlite retry retained the observed terminal instead of re-subscribing"
    );
}

// --------------------------------------------------------------------------------------------------
// Doctor negative matrix (the probe classification; rendering + exit codes live in preflight tests)
// --------------------------------------------------------------------------------------------------

async fn probe(fake: &Arc<FakeLnv2Ops>) -> Lnv2Probe {
    let backend = backend_with(fake.clone(), clock(1000));
    backend
        .lnv2_functional_probe()
        .await
        .expect("probe never errs")
}

#[tokio::test]
async fn doctor_probe_healthy() {
    let fake = FakeLnv2Ops::new();
    assert_eq!(probe(&fake).await, Lnv2Probe::Healthy);
}

#[tokio::test]
async fn doctor_probe_guardians_unreachable() {
    let fake = FakeLnv2Ops::new();
    fake.set_guardians(false);
    assert!(matches!(
        probe(&fake).await,
        Lnv2Probe::GuardiansUnreachable(_)
    ));
}

#[tokio::test]
async fn doctor_probe_module_absent() {
    let fake = FakeLnv2Ops::new();
    fake.set_module_present(false);
    assert_eq!(probe(&fake).await, Lnv2Probe::ModuleAbsent);
}

#[tokio::test]
async fn doctor_probe_gateway_absent() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec![]);
    assert_eq!(probe(&fake).await, Lnv2Probe::GatewayAbsent);
}

#[tokio::test]
async fn doctor_probe_gateway_unreachable() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec!["gw://x".to_string()]);
    fake.set_gateway_fee("gw://x", None);
    assert!(matches!(
        probe(&fake).await,
        Lnv2Probe::GatewayUnreachable(_)
    ));
}

#[tokio::test]
async fn doctor_probe_gateway_send_incompatible_is_not_healthy() {
    // A gateway is advertised and responsive but its send fee exceeds lnv2's SEND_FEE_LIMIT, so the pay
    // path cannot actually use it. The probe MUST NOT report Healthy (else the doctor lies while refunds
    // stick PENDING); it surfaces the send-policy reason so the operator fixes/removes the gateway.
    let fake = FakeLnv2Ops::new();
    fake.set_gateways(vec!["gw://greedy".to_string()]);
    fake.set_gateway_fee("gw://greedy", Some(flat_fee(200_000, 0)));
    let result = probe(&fake).await;
    assert!(
        matches!(&result, Lnv2Probe::GatewayUnreachable(e) if e.contains("above the lnv2 client")),
        "a send-incompatible-only gateway must not be Healthy: {result:?}"
    );
}

#[tokio::test]
async fn doctor_probe_preserves_concrete_gateway_error() {
    let fake = FakeLnv2Ops::new();
    fake.set_gateway_error("gw://a", "connection refused");
    let result = probe(&fake).await;
    assert!(
        matches!(&result, Lnv2Probe::GatewayUnreachable(e) if e.contains("connection refused")),
        "gateway outage diagnostics must preserve the concrete routing-info error: {result:?}"
    );
}

// --------------------------------------------------------------------------------------------------
// Terminal index retention
// --------------------------------------------------------------------------------------------------

#[test]
fn terminal_index_gc_reaps_only_safe_old_rows() {
    let now = 2 * PAY_INDEX_RETENTION_SECS;
    let old_invoice = now - INVOICE_INDEX_RETENTION_SECS - 1;
    let old_pay = now - PAY_INDEX_RETENTION_SECS - 1;
    let conn = crate::store::open_memory().unwrap();

    for (ext, status, expires_at) in [
        ("canceled-old", "CANCELED", old_invoice),
        ("canceled-recent", "CANCELED", now),
        ("open-old", "OPEN", old_invoice),
        ("paid-old", "PAID", old_invoice),
        ("unrecovered-old", "PAID_UNRECOVERED", old_invoice),
    ] {
        conn.execute(
            "INSERT INTO lnv2_invoice
               (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat,
                credited_msat, expires_at, status)
             VALUES (?1, ?2, ?3, 'lnbc', 'hash', 1, 1000, ?4, ?5)",
            params![
                ext,
                format!("op-{ext}"),
                format!("inv-{ext}"),
                expires_at,
                status
            ],
        )
        .unwrap();
    }
    for (key, status, terminal_at) in [
        ("failed-old", "FAILED", Some(old_pay)),
        ("failed-recent", "FAILED", Some(now)),
        ("failed-undated", "FAILED", None),
        ("pending-old", "PENDING", Some(old_pay)),
        ("succeeded-old", "SUCCEEDED", Some(old_pay)),
    ] {
        conn.execute(
            "INSERT INTO lnv2_pay (idempotency_key, bolt11, operation_id, status, terminal_at)
             VALUES (?1, 'lnbc', ?2, ?3, ?4)",
            params![key, format!("op-{key}"), status, terminal_at],
        )
        .unwrap();
    }
    assert_eq!(
        gc_lnv2_invoice_index(&conn, now, INVOICE_INDEX_RETENTION_SECS).unwrap(),
        1,
        "only old canceled traffic is disposable"
    );
    assert_eq!(
        gc_lnv2_pay_index(&conn, now, PAY_INDEX_RETENTION_SECS).unwrap(),
        1,
        "only old, dated definitive failures are disposable"
    );

    let invoice_statuses: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT status FROM lnv2_invoice ORDER BY external_id")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert_eq!(
        invoice_statuses,
        vec!["CANCELED", "OPEN", "PAID", "PAID_UNRECOVERED"]
    );
    let pay_statuses: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT status FROM lnv2_pay ORDER BY idempotency_key")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert_eq!(
        pay_statuses,
        vec!["FAILED", "FAILED", "PENDING", "SUCCEEDED"]
    );
}

// --------------------------------------------------------------------------------------------------
// Balance + readiness pass-through
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn balance_and_readiness_pass_through() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1000));
    assert_eq!(
        backend.available_balance_msat().await.unwrap(),
        Some(1_000_000)
    );
    assert!(backend.backend_ready().await.unwrap());
    assert!(backend.refund_gateway_ready().await.unwrap());

    fake.set_guardians(false);
    assert!(
        backend.backend_ready().await.is_err(),
        "guardians down -> not ready"
    );
    fake.set_gateway_fee("gw://a", None);
    assert!(
        backend.refund_gateway_ready().await.is_err(),
        "no reachable gateway -> not ready"
    );
}

// --------------------------------------------------------------------------------------------------
// lnrent-7wbo: the outbound evidence probe, answered from the FEDERATION's oplog
// --------------------------------------------------------------------------------------------------

/// The whole point of the seam: no `lnv2_pay` row exists (the index was lost, or the pay never
/// started), and lnv2 still answers from the deterministic attempt-0 operation. `Missing` from the
/// oplog is PROOF no send was ever made for this invoice, so the sweep may terminalize instead of
/// parking its cap forever — the case the codex bot raised on PR #85.
#[tokio::test]
async fn outbound_status_by_ref_proves_absence_when_attempt_zero_never_existed() {
    let fake = FakeLnv2Ops::new();
    let be = backend_with(fake.clone(), clock(1_000));

    // Nothing seeded: `send_operation_id` derives `prepared:<bolt11>`, which the oplog does not hold.
    assert_eq!(
        be.outbound_status_by_ref("hash-irrelevant", "lnbc-never-sent")
            .await
            .unwrap(),
        Some(PayStatus::Failed),
        "attempt-0 absent from the oplog proves no send was ever started"
    );
}

/// An attempt-0 operation that DOES exist is not resolved here: `await_send_final` blocks, and a
/// foreign `lnrent_key` on the same invoice is the [8A] cross-order collision. Both are "cannot
/// answer", which parks the sweep rather than terminalizing it (lnrent-8l8c owns teaching this arm
/// to report Succeeded/Pending).
#[tokio::test]
async fn outbound_status_by_ref_cannot_answer_once_an_attempt_exists() {
    let fake = FakeLnv2Ops::new();
    let be = backend_with(fake.clone(), clock(1_000));
    let bolt11 = "lnbc-already-attempted";

    fake.st
        .lock()
        .unwrap()
        .op_keys
        .insert(format!("prepared:{bolt11}"), "sweep:deadbeef".to_string());

    assert_eq!(
        be.outbound_status_by_ref("hash-irrelevant", bolt11).await.unwrap(),
        None,
        "an existing attempt is not evidence of failure, so the caller must not terminalize"
    );
}

/// Defensive: with no invoice there is nothing to derive an operation from, so the honest answer is
/// "cannot answer" — never the absence proof above.
#[tokio::test]
async fn outbound_status_by_ref_cannot_answer_without_an_invoice() {
    let fake = FakeLnv2Ops::new();
    let be = backend_with(fake, clock(1_000));

    assert_eq!(
        be.outbound_status_by_ref("hash-irrelevant", "").await.unwrap(),
        None
    );
}

// --------------------------------------------------------------------------------------------------
// ADR-0022 (lnrent-chgb): the leased issuance, the actor-run watcher hook, prepare_pay, and the
// two-phase pay lock.
// --------------------------------------------------------------------------------------------------

/// Two concurrent same-`external_id` issuances mint EXACTLY one lnv2 invoice. The create guard rides
/// `Issued::lease` through the caller's commit, so the second caller's check runs against the
/// committed row. RED on a guard released at `issue_invoice` return (or captured by `persist`, which
/// returns before the commit): both callers would pass the check and mint two tweaks, stranding one.
#[tokio::test]
async fn two_concurrent_same_external_id_issuances_mint_exactly_once() {
    let fake = FakeLnv2Ops::new();
    let backend = Arc::new(backend_with(fake.clone(), clock(1_000)));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let b = backend.clone();
        tasks.spawn(async move {
            let issued = b.issue_invoice(1000, "m", 3600, "extRace").await.unwrap();
            // A deliberate yield between issuing and committing widens the window a released guard
            // would open.
            tokio::task::yield_now().await;
            let persist = issued.persist;
            b.store
                .transaction_then(move |tx| persist(tx), issued.lease, issued.after_commit)
                .await
                .unwrap();
            issued.invoice
        });
    }
    let mut invoices = Vec::new();
    while let Some(inv) = tasks.join_next().await {
        invoices.push(inv.unwrap());
    }
    assert_eq!(
        fake.st.lock().unwrap().next_inv,
        1,
        "exactly one receive was minted at the federation"
    );
    assert_eq!(invoices[0].bolt11, invoices[1].bolt11, "both callers hold the SAME invoice");
    let rows: i64 = backend
        .store
        .read(|c| Ok(c.query_row("SELECT count(*) FROM lnv2_invoice", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

/// While the caller holds an `Issued`, the create guard is still taken; the store actor releases it
/// only after the commit. A second issuer for the same key blocks until then and finds the row.
#[tokio::test]
async fn the_create_lease_is_held_until_the_callers_commit() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1_000));
    let issued = backend.issue_invoice(1000, "m", 3600, "extLease").await.unwrap();
    assert!(
        backend.create_lock.try_lock().is_err(),
        "the guard is alive inside Issued::lease, not released at return"
    );
    let persist = issued.persist;
    backend
        .store
        .transaction_then(move |tx| persist(tx), issued.lease, issued.after_commit)
        .await
        .unwrap();
    assert!(backend.create_lock.try_lock().is_ok(), "released by the actor after the commit");
}

/// The live receive watcher starts from the store actor's post-commit hook, and ONLY if `persist`
/// ran: a transaction that commits a refusal without the invoice (the listing-withdrawn order write,
/// the lost soft-reminder CAS) must not start a watcher against an absent row. RED with the hook
/// unconditional: the first branch would subscribe to an op whose row was never written.
#[tokio::test]
async fn the_live_watcher_starts_only_after_commit_and_only_if_persist_ran() {
    let fake = FakeLnv2Ops::new();
    fake.set_receive_credit_msat(995_500);
    let backend = backend_with(fake.clone(), clock(5_000));
    let mut rx = backend.watch().await.unwrap();

    // 1. A committed NO-OP (the caller's closure never calls persist): no watcher.
    let issued = backend.issue_invoice(1000, "m", 3600, "extNoop").await.unwrap();
    let op_noop = issued.invoice.backend_invoice_id.clone();
    let _dropped = issued.persist;
    backend
        .store
        .transaction_then(|_tx| Ok(()), issued.lease, issued.after_commit)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        fake.receive_subscribe_count(&op_noop),
        0,
        "a committed refusal starts no watcher"
    );

    // 2. persist ran: the watcher subscribes after the commit, and its Claimed is LIVE.
    let issued = backend.issue_invoice(1000, "m", 3600, "extLive").await.unwrap();
    let inv = issued.invoice.clone();
    assert_eq!(
        fake.receive_subscribe_count(&inv.backend_invoice_id),
        0,
        "nothing subscribes BEFORE the commit (a Claimed against an uncommitted row would be lost)"
    );
    let persist = issued.persist;
    backend
        .store
        .transaction_then(move |tx| persist(tx), issued.lease, issued.after_commit)
        .await
        .unwrap();
    fake.set_receive_final(&inv.backend_invoice_id, ReceiveFinal::Claimed);
    let settlement = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("the post-commit watcher observes the payment")
        .expect("channel open");
    assert_eq!(settlement.external_id, "extLive");
    assert_eq!(settlement.settled_at, 5_000, "live provenance: the true clock");
}

/// `pay(key)` READS the PREPARED row the driver committed and never creates one: a key nobody
/// prepared is refused before any federation call.
#[tokio::test]
async fn pay_without_a_prepared_row_is_refused_before_any_send() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1_000));
    let err = backend
        .pay_refund_capped("lnbcUnprepared", 500, 500, "keyUnprepared")
        .await
        .expect_err("no PREPARED row -> refuse");
    assert!(
        format!("{err:#}").contains("no PREPARED row"),
        "names the missing prepare: {err:#}"
    );
    assert_eq!(fake.send_count(), 0, "nothing reached the federation");
    assert_eq!(
        backend.payment_status_by_key("keyUnprepared").await.unwrap(),
        PayStatus::Unknown
    );
}

/// The pay lock is held through the durable PENDING write and RELEASED before `await_send_final`
/// (ADR-0022 "The lease covers the check-and-create only"): a pay stuck awaiting finality must not
/// block an unrelated sweep's pay. RED with the lock held across the await: the second pay would
/// never start.
#[tokio::test]
async fn an_lnv2_pay_awaiting_finality_does_not_block_an_unrelated_pay() {
    let fake = FakeLnv2Ops::new();
    let backend = Arc::new(backend_with(fake.clone(), clock(1_000)));
    // The first send's op never reaches a terminal.
    fake.set_send_hangs("op1");
    let b = backend.clone();
    let stuck = tokio::spawn(async move { b.pay_refund_capped_t("lnbcStuck", 500, 500, "keyStuck").await });
    // Give it time to fund and enter the unlocked await.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fake.send_count(), 1, "the first pay funded and is awaiting");
    let other = tokio::time::timeout(
        Duration::from_secs(2),
        backend.pay_capped_t("lnbcOther", 100, 100_000, "sweep:other"),
    )
    .await
    .expect("an unrelated pay completes while the first awaits finality")
    .unwrap();
    assert_eq!(other, "op2");
    stuck.abort();
}

/// Two keys targeting ONE bolt11 (hence one deterministic attempt-0 op) yield exactly one PREPARED
/// row: the second key's `prepare_pay` runs its local [8A] check under the lease, after the first
/// row is visible, and records its own terminal refusal instead. Its `pay` then never sends.
#[tokio::test]
async fn two_keys_targeting_one_bolt11_prepare_exactly_one_prepared_row() {
    let fake = FakeLnv2Ops::new();
    let backend = Arc::new(backend_with(fake.clone(), clock(1_000)));
    let first = backend.prepare_pay("keyOne", "lnbcShared2").await.unwrap();
    // The second prepare blocks on the lease until the first commits.
    let b = backend.clone();
    let second = tokio::spawn(async move { b.prepare_t("keyTwo", "lnbcShared2").await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!second.is_finished(), "the second prepare waits for the first's commit");
    let persist = first.persist;
    backend
        .store
        .transaction_then(move |tx| persist(tx), first.lease, None)
        .await
        .unwrap();
    second.await.unwrap().unwrap();
    let (prepared, failed): (i64, i64) = backend
        .store
        .read(|c| {
            Ok(c.query_row(
                "SELECT sum(status='PREPARED'), sum(status='FAILED') FROM lnv2_pay
                  WHERE bolt11='lnbcShared2'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!((prepared, failed), (1, 1), "one PREPARED owner, one recorded refusal");
    let err = backend
        .pay_refund_capped("lnbcShared2", 500, 500, "keyTwo")
        .await
        .expect_err("the refused key never sends");
    assert!(format!("{err:#}").contains("previously failed definitively"), "{err:#}");
    assert_eq!(fake.send_count(), 0);
}

/// The issuance crash story (SPEC §6.6, lnv2 half): an `issue_invoice` whose commit never happened
/// leaves NO row, and the retry mints a FRESH invoice — lnv2 draws a new tweak per `receive()` and
/// has no committed map row to reuse; the orphan is harmless because nobody holds its bolt11.
#[tokio::test]
async fn an_uncommitted_issue_leaves_no_row_and_the_retry_mints_fresh() {
    let fake = FakeLnv2Ops::new();
    let backend = backend_with(fake.clone(), clock(1_000));
    let orphan = backend.issue_invoice(1000, "m", 3600, "extCrash").await.unwrap();
    let first = orphan.invoice.clone();
    drop(orphan); // the crash: persist never ran
    let rows: i64 = backend
        .store
        .read(|c| Ok(c.query_row("SELECT count(*) FROM lnv2_invoice", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(rows, 0, "no half-written correlation");
    let again = backend.create_invoice_t(1000, "m", 3600, "extCrash").await.unwrap();
    assert_ne!(again.bolt11, first.bolt11, "a fresh mint, never the orphan's bolt11");
    assert_eq!(fake.st.lock().unwrap().next_inv, 2, "two receives at the federation, one orphan");
}

