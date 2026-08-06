//! Unit matrix for the phoenixd backend money logic (lnrent-xk3). Every flow test drives a REAL
//! `PhoenixdPayment` over an in-memory sqlite index + a scripted `FakePhoenixdOps` whose responses are
//! the EXACT JSON shapes measured against the live phoenixd 0.9.0 node (see the module header), so
//! the mandated behaviors run under `cargo test --workspace` with no phoenixd. Each asserts the
//! intended arm actually FIRED ([9A] non-vacuity) — a refusal test also asserts that NO payment was
//! POSTed, not merely that an error came back.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusqlite::Connection;

use super::*;
use crate::backends::{PayStatus, PaymentBackend, PaymentStatus};
use crate::clock::{Clock, TestClock};

// --------------------------------------------------------------------------------------------------
// Scripted fake phoenixd
// --------------------------------------------------------------------------------------------------

#[derive(Default)]
struct FakeState {
    /// `externalId` -> the ARRAY `GET /payments/incoming?externalId=` returns.
    incoming: HashMap<String, Vec<PhoenixdIncoming>>,
    /// payment hash -> the `GET /payments/outgoingbyhash/{hash}` record (absent = phoenixd's 404).
    outgoing: HashMap<String, PhoenixdOutgoing>,
    /// Scripted `POST /payinvoice` answers, consumed in order; empty = the default node behavior
    /// (dedup a hash already paid, otherwise pay).
    pay_script: VecDeque<std::result::Result<PayAttempt, String>>,
    /// `(amountSat, description, externalId, expirySeconds)` per `POST /createinvoice`.
    create_calls: Vec<(u64, String, String, u32)>,
    pay_calls: Vec<String>,
    incoming_calls: Vec<String>,
    balance: (u64, u64),
    node_ok: bool,
    node_version: String,
    /// `getinfo.nodeId` — the WALLET identity a `PREPARED` attempt is pinned to.
    node_id: String,
    /// Swap `node_id` to this the moment `pay_invoice` is called, modelling a wallet that changes
    /// UNDER an in-flight POST (proxy failover, a restore onto another node). The receipt-less arm
    /// re-reads `getinfo` after the POST precisely so that swap is caught.
    node_id_after_pay: Option<String>,
    /// Same, for the RELEASE: phoenixd restarted onto a new build between the POST and the
    /// `outgoingbyhash` re-read. The marker's meaning is only measured for the verified release, so
    /// a record read after that swap must not resolve anything.
    node_version_after_pay: Option<String>,
    /// Publish this outgoing record only WHEN `pay_invoice` is called — i.e. the POST is what
    /// created it. That is the real sequence, and it matters: lnrent probes `outgoingbyhash` BEFORE
    /// the POST and permanently declines to attribute a record when one already existed, so a test
    /// that seeds the record up front is exercising the pre-existing-history refusal, not whatever
    /// it meant to test.
    outgoing_after_pay: Option<(String, PhoenixdOutgoing)>,
    /// Scripted non-2xx statuses for `GET /getinfo` / `GET /getbalance`, delivered as the TYPED error
    /// the real HTTP layer returns — so the doctor probe's status classification is exercised with no
    /// socket (`node_ok = false` remains the transport-failure shape).
    node_info_status: Option<u16>,
    balance_status: Option<u16>,
    next_id: u64,
    create_amount_override: Option<u64>,
    /// Force createinvoice to return a bolt11 that DISAGREES with its own paymentHash side-field.
    create_bolt11_hash_mismatch: bool,
}

struct FakePhoenixdOps {
    st: Mutex<FakeState>,
}

impl FakePhoenixdOps {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            st: Mutex::new(FakeState {
                balance: (0, 0),
                node_ok: true,
                node_version: FeeSchedule::default().verified_version.clone(),
                node_id: "027e48node".to_string(),
                ..FakeState::default()
            }),
        })
    }

    fn set_incoming(&self, external_id: &str, records: Vec<PhoenixdIncoming>) {
        self.st
            .lock()
            .unwrap()
            .incoming
            .insert(external_id.to_string(), records);
    }

    fn set_outgoing(&self, record: PhoenixdOutgoing) {
        self.st
            .lock()
            .unwrap()
            .outgoing
            .insert(record.payment_hash.clone(), record);
    }

    fn set_outgoing_for(&self, requested_hash: &str, record: PhoenixdOutgoing) {
        self.st
            .lock()
            .unwrap()
            .outgoing
            .insert(requested_hash.to_string(), record);
    }

    fn script_pay(&self, answer: std::result::Result<PayAttempt, String>) {
        self.st.lock().unwrap().pay_script.push_back(answer);
    }

    fn set_balance(&self, balance_sat: u64, fee_credit_sat: u64) {
        self.st.lock().unwrap().balance = (balance_sat, fee_credit_sat);
    }

    fn pay_calls(&self) -> Vec<String> {
        self.st.lock().unwrap().pay_calls.clone()
    }

    fn create_calls(&self) -> Vec<(u64, String, String, u32)> {
        self.st.lock().unwrap().create_calls.clone()
    }

    /// One entry per `GET /payments/incoming?externalId=` round-trip — what the settlement poll's
    /// cost is measured in.
    fn incoming_calls(&self) -> Vec<String> {
        self.st.lock().unwrap().incoming_calls.clone()
    }

    fn set_create_amount_override(&self, amount_sat: u64) {
        self.st.lock().unwrap().create_amount_override = Some(amount_sat);
    }

    fn set_create_bolt11_hash_mismatch(&self) {
        self.st.lock().unwrap().create_bolt11_hash_mismatch = true;
    }

    fn set_node_version(&self, version: &str) {
        self.st.lock().unwrap().node_version = version.to_string();
    }

    fn fail_node_info_with_status(&self, status: u16) {
        self.st.lock().unwrap().node_info_status = Some(status);
    }

    fn fail_balance_with_status(&self, status: u16) {
        self.st.lock().unwrap().balance_status = Some(status);
    }

    /// Model the operator repointing `phoenixd_url` at a DIFFERENT phoenixd wallet (or restoring
    /// phoenixd onto a fresh payments DB): same API, different `getinfo.nodeId`, no history.
    fn set_node_id(&self, node_id: &str) {
        self.st.lock().unwrap().node_id = node_id.to_string();
    }

    fn set_node_id_after_pay(&self, node_id: &str) {
        self.st.lock().unwrap().node_id_after_pay = Some(node_id.to_string());
    }

    fn set_node_version_after_pay(&self, version: &str) {
        self.st.lock().unwrap().node_version_after_pay = Some(version.to_string());
    }

    fn set_outgoing_after_pay(&self, hash: &str, record: PhoenixdOutgoing) {
        self.st.lock().unwrap().outgoing_after_pay = Some((hash.to_string(), record));
    }
}

#[async_trait]
impl PhoenixdOps for FakePhoenixdOps {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        description: &str,
        external_id: &str,
        expiry_s: u32,
    ) -> Result<PhoenixdNewInvoice> {
        let mut st = self.st.lock().unwrap();
        st.create_calls.push((
            amount_sat,
            description.to_string(),
            external_id.to_string(),
            expiry_s,
        ));
        let n = st.next_id;
        st.next_id += 1;
        let response_amount_sat = st.create_amount_override.unwrap_or(amount_sat);
        // A REAL signed bolt11 whose encoded hash+amount agree with the JSON side-fields, because
        // `create_invoice` now refuses a response where they disagree (a proxy/phoenixd returning a
        // bolt11 for a different hash would make the settlement permanently unobservable). Tests
        // that WANT the mismatch drive it through `create_amount_override`.
        let bolt11 = mint_bolt11(response_amount_sat.saturating_mul(1000), n as u8);
        let payment_hash = if st.create_bolt11_hash_mismatch {
            // A bolt11 for one hash advertised under another — the proxy/compromised-node shape.
            hash_of(&mint_bolt11(response_amount_sat.saturating_mul(1000), n.wrapping_add(200) as u8))
        } else {
            hash_of(&bolt11)
        };
        let record = PhoenixdIncoming {
            payment_hash: payment_hash.clone(),
            bolt11,
            is_paid: false,
            is_expired: false,
            requested_sat: response_amount_sat,
            received_sat: 0,
            completed_at_ms: None,
            expires_at_ms: None,
        };
        st.incoming
            .entry(external_id.to_string())
            .or_default()
            .push(record.clone());
        Ok(PhoenixdNewInvoice {
            amount_sat: response_amount_sat,
            payment_hash,
            bolt11: record.bolt11,
        })
    }

    async fn incoming_by_external_id(&self, external_id: &str) -> Result<Vec<PhoenixdIncoming>> {
        let mut st = self.st.lock().unwrap();
        st.incoming_calls.push(external_id.to_string());
        Ok(st.incoming.get(external_id).cloned().unwrap_or_default())
    }

    async fn pay_invoice(&self, bolt11: &str) -> Result<PayAttempt> {
        let mut st = self.st.lock().unwrap();
        st.pay_calls.push(bolt11.to_string());
        if let Some(swapped) = st.node_id_after_pay.take() {
            st.node_id = swapped;
        }
        if let Some(swapped) = st.node_version_after_pay.take() {
            st.node_version = swapped;
        }
        if let Some((hash, record)) = st.outgoing_after_pay.take() {
            st.outgoing.insert(hash, record);
        }
        if let Some(scripted) = st.pay_script.pop_front() {
            return scripted.map_err(|e| anyhow!(e));
        }
        // Default: model the live node. A bolt11 whose hash phoenixd ALREADY paid comes back as the
        // measured DUPLICATE shape (200, `reason`, no receipt); anything else pays.
        let payment_hash = super::parse_dest(bolt11)
            .expect("the fake only receives parseable bolt11s")
            .payment_hash;
        if st.outgoing.get(&payment_hash).is_some_and(|r| r.is_paid) {
            return Ok(PayAttempt::NoReceipt {
                reason: Some("this invoice has already been paid".to_string()),
            });
        }
        let n = st.next_id;
        st.next_id += 1;
        let payment_id = format!("pay-{n}");
        let amount_msat = super::parse_dest(bolt11)
            .expect("parseable")
            .amount_msat
            .unwrap_or(0);
        st.outgoing.insert(
            payment_hash.clone(),
            PhoenixdOutgoing {
                payment_id: payment_id.clone(),
                payment_hash: payment_hash.clone(),
                is_paid: true,
                // `fees` is MSAT on the wire; the fake charges exactly the measured schedule.
                fees_msat: FeeSchedule::default().fee_msat(u128::from(amount_msat)) as u64,
                // A settled payment carries the terminal marker (MEASURED: `isPaid` true +
                // `completedAt` SET is the SUCCEEDED leg).
                completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
            },
        );
        Ok(PayAttempt::Paid {
            payment_id,
            payment_hash,
        })
    }

    async fn outgoing_by_hash(&self, payment_hash: &str) -> Result<Option<PhoenixdOutgoing>> {
        Ok(self.st.lock().unwrap().outgoing.get(payment_hash).cloned())
    }

    async fn balance(&self) -> Result<PhoenixdBalance> {
        let st = self.st.lock().unwrap();
        if let Some(status) = st.balance_status {
            return Err(scripted_http_error("getbalance", status));
        }
        let (balance_sat, fee_credit_sat) = st.balance;
        Ok(PhoenixdBalance {
            balance_sat,
            fee_credit_sat,
        })
    }

    async fn node_info(&self) -> Result<PhoenixdNodeInfo> {
        let st = self.st.lock().unwrap();
        if let Some(status) = st.node_info_status {
            return Err(scripted_http_error("getinfo", status));
        }
        if st.node_ok {
            Ok(PhoenixdNodeInfo {
                node_id: st.node_id.clone(),
                version: st.node_version.clone(),
            })
        } else {
            Err(anyhow!("phoenixd unreachable"))
        }
    }
}

/// The terminal marker as the live node actually reported it (epoch MILLIS): the `completedAt` on
/// the terminated read of the 2026-07-26 in-flight/terminated comparison
/// (`outgoing_is_terminally_failed`). Its VALUE is never load-bearing — presence is the signal — but
/// using the measured one keeps the fixtures traceable to the drill.
const MEASURED_COMPLETED_AT_MS: i64 = 1_785_101_384_178;

/// The EXACT error the real HTTP layer returns for a non-2xx answer (same type, same rendered text),
/// so a fake-driven test proves the same classification a live node's response would.
fn scripted_http_error(what: &str, status: u16) -> anyhow::Error {
    let status_text = reqwest::StatusCode::from_u16(status)
        .expect("a valid HTTP status")
        .to_string();
    PhoenixdHttpError::new(what, status, status_text).into()
}

// --------------------------------------------------------------------------------------------------
// Fixtures
// --------------------------------------------------------------------------------------------------

fn backend(ops: Arc<FakePhoenixdOps>, clock: TestClock) -> PhoenixdPayment {
    let index = Connection::open_in_memory().expect("in-memory index");
    index.execute_batch(INDEX_SCHEMA).expect("index schema");
    PhoenixdPayment::with_ops(ops, index, Arc::new(clock), FeeSchedule::default())
}

/// Mint a valid SIGNED bolt11 with a payment hash derived from `seed`, so the pay tests can use
/// several DISTINCT destination invoices (the payment hash is what the whole idempotency/recovery
/// story keys on).
fn mint_bolt11(amount_msat: u64, seed: u8) -> String {
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
    use std::time::{Duration, SystemTime};

    let sk = SecretKey::from_slice(&[0x11u8; 32]).unwrap();
    let payment_hash = sha256::Hash::hash(&[seed; 32]);
    InvoiceBuilder::new(Currency::Regtest)
        .amount_milli_satoshis(amount_msat)
        .description("lnrent phoenixd test".to_string())
        .payment_hash(payment_hash)
        .payment_secret(PaymentSecret([42u8; 32]))
        .timestamp(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000))
        .min_final_cltv_expiry_delta(144)
        .expiry_time(Duration::from_secs(3600))
        .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &sk))
        .expect("test bolt11 builds")
        .to_string()
}

fn hash_of(bolt11: &str) -> String {
    super::parse_dest(bolt11).expect("parseable").payment_hash
}

/// The live-measured receive record: a 25_000-sat invoice that credited only 2_723 sat because ACINQ
/// opened the first channel (`fees` 22_277_000 MSAT).
fn live_measured_receive(payment_hash: &str) -> PhoenixdIncoming {
    PhoenixdIncoming {
        payment_hash: payment_hash.to_string(),
        bolt11: "lnbc25u1measured".to_string(),
        is_paid: true,
        is_expired: false,
        requested_sat: 25_000,
        received_sat: 2_723,
        completed_at_ms: Some(1_753_400_000_000),
        expires_at_ms: None,
    }
}

// --------------------------------------------------------------------------------------------------
// Pure money helpers (units + INV-1 arithmetic)
// --------------------------------------------------------------------------------------------------

// The live anchor: a 120-sat payout was charged `fees` = 4480 MSAT (4 sat + 0.4%, i.e. 4.48 sat). If
// this were ever computed in sat the figure would be 4 (routingFeeSat, rounded DOWN) or 4_480_000.
#[test]
fn trampoline_reserve_matches_the_live_measured_fee_in_msat() {
    assert_eq!(
        FeeSchedule::default().fee_msat(sat_to_msat(120)),
        4_480,
        "120 sat payout reserves 4480 MSAT (the measured phoenixd 0.9.0 fee), not 4 sat"
    );
    assert_eq!(
        FeeSchedule::default().fee_msat(0),
        4_000,
        "the base applies at any payout"
    );
}

#[test]
fn trampoline_reserve_rounds_the_proportional_part_up() {
    // 1 sat payout: 1000 * 4000 / 1e6 = 4.0 exactly -> 4004 msat.
    assert_eq!(FeeSchedule::default().fee_msat(sat_to_msat(1)), 4_004);
    // A payout whose proportional fee is fractional must round UP (reserving more is INV-1-safe).
    assert_eq!(
        FeeSchedule::default().fee_msat(1),
        4_001,
        "1 msat -> ceil(0.004) = 1 msat"
    );
}

#[test]
fn net_payout_leaves_room_for_the_reserved_fee() {
    // 130 sat gross: 125 sat payout costs 125_000 + 4_000 + 500 = 129_500 msat (fits); 126 would be
    // 130_504 msat (does not).
    assert_eq!(FeeSchedule::default().net_payout_sat(130), 125);
    let payout_msat = sat_to_msat(125);
    assert!(payout_msat + FeeSchedule::default().fee_msat(payout_msat) <= sat_to_msat(130));
    let over_msat = sat_to_msat(126);
    assert!(over_msat + FeeSchedule::default().fee_msat(over_msat) > sat_to_msat(130));
}

#[test]
fn net_payout_is_zero_for_true_dust() {
    // Below the 4-sat base fee nothing positive can be sent.
    assert_eq!(FeeSchedule::default().net_payout_sat(4), 0);
    assert_eq!(FeeSchedule::default().net_payout_sat(0), 0);
}

// UNITS: `fees` is MSAT. A 120-sat payout with a 4480-MSAT fee fits a 124_480-msat ceiling exactly;
// misreading those 4480 as SAT would compute a 4_480_000-msat outlay and flag a bogus overrun.
#[test]
fn inv1_overrun_reads_fees_as_msat() {
    assert_eq!(
        inv1_overrun_msat(sat_to_msat(120), 4_480, Some(124_480)),
        None,
        "120 sat + 4480 MSAT fits a 124_480 msat ceiling exactly"
    );
    assert_eq!(
        inv1_overrun_msat(sat_to_msat(120), 4_481, Some(124_480)),
        Some(1),
        "one msat over the ceiling is reported as an overrun of one msat"
    );
    assert_eq!(
        inv1_overrun_msat(sat_to_msat(120), 9_999, None),
        None,
        "an uncapped pay has no ceiling to breach"
    );
}

#[test]
fn completed_at_converts_millis_to_seconds() {
    assert_eq!(
        epoch_secs_from_ms(Some(1_753_400_000_000)),
        Some(1_753_400_000)
    );
    assert_eq!(epoch_secs_from_ms(None), None);
    assert_eq!(epoch_secs_from_ms(Some(0)), None, "no timestamp, not 1970");
}

#[test]
fn reusable_incoming_prefers_a_paid_record_then_an_unexpired_one() {
    let paid = PhoenixdIncoming {
        is_paid: true,
        is_expired: true, // a paid-but-lapsed record still wins: the money arrived
        ..live_measured_receive("aa")
    };
    let expired = PhoenixdIncoming {
        payment_hash: "bb".into(),
        is_paid: false,
        is_expired: true,
        ..live_measured_receive("bb")
    };
    let open = PhoenixdIncoming {
        payment_hash: "cc".into(),
        is_paid: false,
        is_expired: false,
        ..live_measured_receive("cc")
    };
    assert_eq!(
        select_reusable_incoming(&[expired.clone(), paid.clone()]).map(|r| r.payment_hash.as_str()),
        Some("aa")
    );
    assert_eq!(
        select_reusable_incoming(&[expired.clone(), open.clone()]).map(|r| r.payment_hash.as_str()),
        Some("cc")
    );
    assert!(
        select_reusable_incoming(&[expired]).is_none(),
        "an expired unpaid record is re-invoiced, not reused"
    );
}

// --------------------------------------------------------------------------------------------------
// Receive
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn create_invoice_is_idempotent_on_external_id() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let a = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    let b = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    assert_eq!(a.id, b.id, "same external_id -> the same invoice");
    assert_eq!(a.bolt11, b.bolt11);
    assert_eq!(
        a.expires_at, 1_600,
        "lnrent's OWN expiry window: now + expiry_s"
    );
    assert_eq!(
        ops.create_calls().len(),
        1,
        "the second call must not create a second phoenixd invoice"
    );
}

#[tokio::test]
async fn create_invoice_replaces_a_cached_invoice_phoenixd_reports_expired() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let first = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming(
        "ext:1",
        vec![PhoenixdIncoming {
            payment_hash: first.payment_hash.clone(),
            bolt11: first.bolt11.clone(),
            is_paid: false,
            is_expired: true,
            requested_sat: first.amount_sat,
            received_sat: 0,
            completed_at_ms: None,
            expires_at_ms: None,
        }],
    );

    let replacement = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .expect("an actually-expired unpaid mapping must be replaceable");

    assert_ne!(replacement.payment_hash, first.payment_hash);
    assert_eq!(
        ops.create_calls().len(),
        2,
        "the cached expired invoice must not wedge this external id forever"
    );
    clock.set(first.expires_at);
    assert_eq!(
        be.lookup(&first.id).await.unwrap(),
        PaymentStatus::Expired,
        "creating a successor must retain the old invoice-id correlation for reconcile"
    );
}

#[tokio::test]
async fn create_invoice_rejects_a_mismatched_createinvoice_amount() {
    let ops = FakePhoenixdOps::new();
    ops.set_create_amount_override(24_999);
    let be = backend(ops.clone(), TestClock::new(1_000));

    let err = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .expect_err("phoenixd must not be allowed to change the billed amount");

    assert!(
        format!("{err:#}").contains("returned amountSat 24999"),
        "unexpected error: {err:#}"
    );
}

// The crash window phoenixd's `externalId` index closes: an invoice created by a previous attempt
// that died before the local row was written is REUSED, never duplicated.
#[tokio::test]
async fn create_invoice_reuses_an_invoice_phoenixd_already_holds() {
    let ops = FakePhoenixdOps::new();
    ops.set_incoming(
        "ext:1",
        vec![PhoenixdIncoming {
            payment_hash: "abcd".into(),
            bolt11: "lnbcorphan".into(),
            is_paid: false,
            is_expired: false,
            requested_sat: 25_000,
            received_sat: 0,
            completed_at_ms: None,
            expires_at_ms: None,
        }],
    );
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    assert_eq!(inv.bolt11, "lnbcorphan", "the orphaned invoice is adopted");
    assert_eq!(inv.payment_hash, "abcd");
    assert!(
        ops.create_calls().is_empty(),
        "no second phoenixd invoice may be created for one external id"
    );
    // And it is now the durable local mapping.
    assert_eq!(be.lookup(&inv.id).await.unwrap(), PaymentStatus::Open);
}

// Same crash window, but the orphaned invoice was PAID meanwhile. Its window is HISTORY — capture
// judges the settlement timely or late against it — so it is recovered from phoenixd's own
// `expiresAt` rather than reopened as a fresh one, matching the cached arm which preserves the
// original for exactly that reason.
#[tokio::test]
async fn a_reused_paid_invoice_keeps_its_original_window_instead_of_a_fresh_one() {
    let ops = FakePhoenixdOps::new();
    ops.set_incoming(
        "ext:1",
        vec![PhoenixdIncoming {
            expires_at_ms: Some(1_200_000),
            completed_at_ms: Some(1_150_000),
            ..live_measured_receive("abcd")
        }],
    );
    let be = backend(ops.clone(), TestClock::new(2_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    assert_eq!(
        inv.expires_at, 1_200,
        "the paid invoice's own window, not now + expiry_s (2_600)"
    );
    assert!(ops.create_calls().is_empty());

    // A node that reports no `expiresAt` still recovers the payment rather than stranding it.
    let ops = FakePhoenixdOps::new();
    ops.set_incoming("ext:2", vec![live_measured_receive("abcd")]);
    let be = backend(ops.clone(), TestClock::new(2_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:2")
        .await
        .unwrap();
    assert_eq!(inv.expires_at, 2_600);
    assert_eq!(be.lookup(&inv.id).await.unwrap(), PaymentStatus::Paid);
}

#[tokio::test]
async fn create_invoice_refuses_a_stored_invoice_for_a_different_amount() {
    let ops = FakePhoenixdOps::new();
    ops.set_incoming(
        "ext:1",
        vec![PhoenixdIncoming {
            requested_sat: 9_000,
            is_paid: false,
            ..live_measured_receive("abcd")
        }],
    );
    let be = backend(ops.clone(), TestClock::new(1_000));
    let err = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .expect_err("a mismatched stored amount must fail closed");
    assert!(
        format!("{err:#}").contains("refusing to reuse or duplicate"),
        "unexpected error: {err:#}"
    );
    assert!(ops.create_calls().is_empty(), "and nothing is minted");
}

// THE headline regression anchor (fact 1): the wallet credit is `receivedSat`, NOT the invoice face
// value. Booking the gross here would make lnrent believe it holds ~9x what actually arrived.
#[tokio::test]
async fn received_credit_is_net_of_the_phoenixd_receive_fee() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);

    assert_eq!(
        be.received_amount_msat(&inv.id).await.unwrap(),
        Some(2_723_000),
        "the credit is receivedSat*1000"
    );
    assert_ne!(
        be.received_amount_msat(&inv.id).await.unwrap(),
        Some(25_000_000),
        "never the requestedSat face value"
    );
}

#[tokio::test]
async fn lookup_settlement_reports_paid_with_the_true_completed_at() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    assert_eq!(
        be.lookup_settlement(&inv.id).await.unwrap(),
        (PaymentStatus::Open, None)
    );

    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    let (status, settled_at) = be.lookup_settlement(&inv.id).await.unwrap();
    assert_eq!(status, PaymentStatus::Paid);
    assert_eq!(
        settled_at,
        Some(1_753_400_000),
        "phoenixd reports the TRUE instant, so this is a LIVE settled_at (epoch ms -> secs)"
    );
}

#[tokio::test]
async fn lookup_expires_on_lnrents_own_window_but_a_paid_invoice_stays_paid() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    clock.set(1_600);
    assert_eq!(
        be.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Expired,
        "past lnrent's expires_at (createinvoice carries no expiry parameter — module header)"
    );

    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    assert_eq!(
        be.lookup(&inv.id).await.unwrap(),
        PaymentStatus::Paid,
        "a LATE payment is still Paid; capture's expiry gate decides what to do with it"
    );
}

#[tokio::test]
async fn settlement_poll_delivers_a_payment_after_lnrents_local_expiry() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let inv = be
        .create_invoice(25_000, "memo", 10, "ext:1")
        .await
        .unwrap();
    clock.set(1_020);
    ops.set_incoming(
        "ext:1",
        vec![PhoenixdIncoming {
            payment_hash: inv.payment_hash.clone(),
            bolt11: inv.bolt11.clone(),
            is_paid: true,
            is_expired: false,
            requested_sat: 25_000,
            received_sat: 2_723,
            completed_at_ms: Some(1_015_000),
            expires_at_ms: None,
        }],
    );
    let mut rx = be.watch().await.expect("watch starts its polling task");
    let settlement = rx.recv().await.expect("late payment must be pushed");
    assert_eq!(settlement.invoice_id, inv.id);
    assert_eq!(settlement.external_id, "ext:1");
    assert_eq!(settlement.received_msat, 2_723_000);
    assert_eq!(settlement.settled_at, 1_015);
}

// The poll must not re-deliver a row forever. `phoenixd_invoice` is never GC'd, so re-emitting every
// paid row every cycle costs one HTTP round-trip AND one sole-writer store transaction per invoice
// EVER created, per cycle — unbounded load on the money path, whose idempotent captures change
// nothing. A delivered settlement retires the row; the settlement itself is already in the channel.
#[tokio::test]
async fn a_delivered_settlement_is_not_polled_or_delivered_again() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let clock_dyn: Arc<dyn Clock> = Arc::new(clock);
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:paid")
        .await
        .unwrap();
    ops.set_incoming("ext:paid", vec![live_measured_receive(&inv.payment_hash)]);

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let mut retired: HashSet<String> = HashSet::new();

    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    let settlement = rx.recv().await.expect("the first poll delivers the payment");
    assert_eq!(settlement.invoice_id, inv.id);
    assert_eq!(settlement.received_msat, 2_723_000);
    let polls_after_delivery = ops.incoming_calls().len();

    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert_eq!(
        ops.incoming_calls().len(),
        polls_after_delivery,
        "a delivered row must cost no further phoenixd round-trip"
    );
    assert!(
        rx.try_recv().is_err(),
        "a delivered settlement must not be re-emitted every cycle"
    );
}

// The other retirement, on phoenixd's OWN evidence: an invoice it reports expired can no longer move
// money (the same fact `create_invoice` uses to mint a successor). A still-live unpaid invoice must
// keep being polled — lnrent's shorter local expiry is NOT grounds to stop watching.
#[tokio::test]
async fn an_expired_unpaid_invoice_retires_but_a_live_one_keeps_being_polled() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let lapsed = be
        .create_invoice(25_000, "memo", 10, "ext:lapsed")
        .await
        .unwrap();
    let live = be
        .create_invoice(25_000, "memo", 10, "ext:live")
        .await
        .unwrap();
    ops.set_incoming(
        "ext:lapsed",
        vec![PhoenixdIncoming {
            payment_hash: lapsed.payment_hash.clone(),
            bolt11: lapsed.bolt11.clone(),
            is_paid: false,
            is_expired: true,
            requested_sat: 25_000,
            received_sat: 0,
            completed_at_ms: None,
            expires_at_ms: None,
        }],
    );
    // Unpaid, and phoenixd still considers it payable — even though lnrent's own window has lapsed.
    clock.set(1_020);

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = Arc::new(clock.clone());
    let mut retired: HashSet<String> = HashSet::new();

    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    let polls: Vec<String> = ops.incoming_calls();
    assert_eq!(
        polls.iter().filter(|e| *e == "ext:lapsed").count(),
        2,
        "an invoice phoenixd reports expired and unpaid can never settle: 1 create-time check + the \
         single poll that observed the expiry, and never again"
    );
    assert_eq!(
        polls.iter().filter(|e| *e == "ext:live").count(),
        3,
        "a live invoice is polled every cycle (1 create-time check + 2 polls), local expiry or not"
    );

    // A late payment on the still-live invoice is still observed.
    ops.set_incoming(
        "ext:live",
        vec![PhoenixdIncoming {
            payment_hash: live.payment_hash.clone(),
            bolt11: live.bolt11.clone(),
            is_paid: true,
            is_expired: false,
            requested_sat: 25_000,
            received_sat: 2_723,
            completed_at_ms: Some(1_019_000),
            expires_at_ms: None,
        }],
    );
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    let settlement = rx.recv().await.expect("the late payment still lands");
    assert_eq!(settlement.invoice_id, live.id);
    assert_eq!(settlement.settled_at, 1_019);
}

// An id lnrent's state DB holds but the index does not is DIVERGENCE (nothing deletes an index
// row), so the payment state is unknown — never "expired". Answering Expired would let reconcile
// expire an invoice phoenixd may have been paid; Err makes every caller defer and retry instead.
#[tokio::test]
async fn an_unknown_invoice_id_fails_closed_instead_of_reporting_expired() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let err = be
        .lookup("phoenixd-nosuch")
        .await
        .expect_err("an id with no index row has an UNKNOWN state, not a lapsed one");
    assert!(
        format!("{err:#}").contains("must not be treated as expired"),
        "unexpected error: {err:#}"
    );
    assert!(be.lookup_settlement("phoenixd-nosuch").await.is_err());
}

#[tokio::test]
async fn a_missing_live_incoming_record_fails_closed_instead_of_reporting_expired() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", Vec::new());
    clock.set(inv.expires_at);

    let err = be
        .lookup(&inv.id)
        .await
        .expect_err("absence from phoenixd is UNKNOWN, not proof of non-payment");
    assert!(
        format!("{err:#}").contains("must not be treated as expired"),
        "unexpected error: {err:#}"
    );
}

// `received_amount_msat` must NEVER answer Ok(None) for phoenixd: the caller reads None as "credit
// the gross", which on a channel-opening receive over-credits by ~9x.
#[tokio::test]
async fn received_amount_fails_closed_instead_of_reporting_none() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();

    let err = be
        .received_amount_msat(&inv.id)
        .await
        .expect_err("an unpaid invoice has no observed credit");
    assert!(format!("{err:#}").contains("refusing to book a credit"));
    let err = be
        .received_amount_msat("phoenixd-nosuch")
        .await
        .expect_err("an unknown invoice has no observed credit");
    assert!(format!("{err:#}").contains("refusing to guess"));
}

// ADR-0019's phoenixd caveat: a receive small enough to land entirely in `Part.FeeCredit` is counted
// by `receivedSat` but NOT by the spendable `balanceSat`, so booking it as refundable liability would
// let a forced refund draw down the operator's own balance. phoenixd 0.9.0 exposes no per-payment
// attribution, so the wallet-level `getbalance` is the only signal: a receipt the fee credit could
// account for in FULL *and* the spendable balance cannot cover at all is refused rather than booked.
#[tokio::test]
async fn a_receipt_no_spendable_balance_could_back_is_refused_not_booked() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    // The whole 2_723-sat credit could be sitting in the 3_000-sat fee credit, and the wallet holds
    // less than the receipt in spendable funds: booking it would promise a refund it cannot pay.
    ops.set_balance(2_722, 3_000);

    let err = be
        .received_amount_msat(&inv.id)
        .await
        .expect_err("an unbacked receipt must not be booked as liability");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("fee credit") && rendered.contains("2723"),
        "unexpected error: {rendered}"
    );

    // The watch poll enforces the identical rule: nothing is emitted, and the row is NOT retired, so
    // the next cycle re-checks it.
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));
    let mut retired: HashSet<String> = HashSet::new();
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert!(
        rx.try_recv().is_err(),
        "a receipt with no provable spendable credit must not reach capture"
    );
    assert!(retired.is_empty(), "the row must stay watched for a re-check");

    // Funding the wallet clears it: the refusal is retried, not durable.
    ops.set_balance(2_723, 3_000);
    assert_eq!(
        be.received_amount_msat(&inv.id).await.unwrap(),
        Some(2_723_000)
    );
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert_eq!(
        rx.recv().await.expect("now deliverable").received_msat,
        2_723_000
    );
}

// The refusal above has NO escape downstream — the settlement poll holds the receipt back,
// `settlement_catch_up` leaves the invoice OPEN, and reconcile will not expire an invoice the backend
// reports `Paid` — so it must fire only on the evidence it actually has. A standing wallet-level fee
// credit says NOTHING about whether a given receipt reached the channel, so on its own it would wedge
// every paid order smaller than that credit: no capture, no provisioning, no refund, forever. A
// wallet that can pay a refund of the receipt books it (with the warning) instead.
#[tokio::test]
async fn a_fee_credit_larger_than_the_receipt_does_not_wedge_a_funded_wallet() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    // A standing fee credit well above this 2_723-sat receipt — but 50_000 sat of SPENDABLE funds.
    ops.set_balance(50_000, 30_000);

    assert_eq!(
        be.received_amount_msat(&inv.id).await.unwrap(),
        Some(2_723_000),
        "a paid order must not be stranded over an unrelated wallet-level fee credit"
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));
    let mut retired: HashSet<String> = HashSet::new();
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert_eq!(
        rx.recv().await.expect("the settlement reaches capture").received_msat,
        2_723_000
    );
}

#[test]
fn fee_credit_backing_is_classified_on_the_wallet_level_signal() {
    assert_eq!(credit_backing(2_723, 0, 0), CreditBacking::FullyBacked);
    assert_eq!(
        credit_backing(2_723, 10, 0),
        CreditBacking::UnattributedButPayable { fee_credit_sat: 10 },
        "a fee credit smaller than the receipt leaves the difference provably in the channel"
    );
    assert_eq!(
        credit_backing(2_723, 2_723, 2_723),
        CreditBacking::UnattributedButPayable {
            fee_credit_sat: 2_723
        },
        "the fee credit could account for it, but the wallet can pay a refund of it"
    );
    assert_eq!(
        credit_backing(2_723, 2_723, 2_722),
        CreditBacking::UnbackedAndUnpayable {
            fee_credit_sat: 2_723,
            balance_sat: 2_722
        },
        "could be fee credit in full AND no spendable funds to refund it"
    );
    // The 2026-07-26 LIVE measurement: a 1_000-sat receive on a channel-less wallet landed entirely
    // in fee credit (`{balanceSat: 0, feeCreditSat: 1000}`). Distinct from the row above, which is a
    // constructed near-miss: here the spendable balance is EXACTLY zero — the real-world boundary,
    // not one sat below the receipt. It is NOT a distinct CODE boundary: `credit_backing` tests
    // `balance_sat < received_sat` and treats zero like any other shortfall. What this row pins is
    // provenance — the triple a real node actually produced lands on the refusing arm.
    assert_eq!(
        credit_backing(1_000, 1_000, 0),
        CreditBacking::UnbackedAndUnpayable {
            fee_credit_sat: 1_000,
            balance_sat: 0
        },
        "the measured fee-credit-only receive: receivedSat counts it, balanceSat cannot spend it"
    );
}

// The same measurement driven END TO END over the record phoenixd actually returned. On 2026-07-26 a
// 1_000-sat invoice was paid from a real Lightning node to a mainnet phoenixd 0.9.0 with ZERO
// channels: the wallet went `{balanceSat: 0, feeCreditSat: 0}` -> `{balanceSat: 0, feeCreditSat:
// 1000}` with NO channel opened, and the incoming record reported `receivedSat: 1000` with `fees: 0`
// — INDISTINGUISHABLE from a genuinely spendable receive, and carrying no per-receipt attribution
// field to tell them apart. Confirmed in the same session: an outbound `payinvoice` off that wallet
// FAILED ("payment could not be sent through existing channels, check individual failures"), so fee
// credit provably cannot fund a refund. The wallet-level `getbalance` is therefore the ONLY signal,
// and it must refuse here.
#[tokio::test]
async fn the_measured_fee_credit_only_receive_is_refused_end_to_end() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(1_000, "lnrent-itw fee-credit measurement", 86_400, "ext:itw")
        .await
        .unwrap();
    // The measured record, in every field `PhoenixdIncoming` carries — this drives the RULE, over
    // the `PhoenixdOps` seam; the verbatim JSON's trip through the wire decoders is pinned
    // separately in `real::decode_tests` (those types are private to that module). Its `fees: 0` has
    // no counterpart here because `receivedSat` is ALREADY net of the incoming `fees` (module header:
    // `receivedSat=2723` on a 25_000-sat request at `fees=22_277_000` msat), so there is nothing
    // left to subtract. What the record has NO field for is fee-CREDIT attribution — nothing says
    // which part of `receivedSat` the wallet cannot spend.
    ops.set_incoming(
        "ext:itw",
        vec![PhoenixdIncoming {
            payment_hash: inv.payment_hash.clone(),
            bolt11: inv.bolt11.clone(),
            is_paid: true,
            is_expired: false,
            requested_sat: 1_000,
            received_sat: 1_000,
            completed_at_ms: Some(1_785_097_938_613),
            expires_at_ms: Some(1_785_184_321_565),
        }],
    );
    ops.set_balance(0, 1_000);

    let err = be
        .received_amount_msat(&inv.id)
        .await
        .expect_err("a receipt that landed entirely in fee credit must not become liability");
    let rendered = format!("{err:#}");
    // [9A]: pin the `UnbackedAndUnpayable` arm by all three measured figures, not merely "an error"
    // — the unpaid/unknown-invoice refusals of this same call carry different text.
    assert!(
        rendered.contains("received 1000 sat")
            && rendered.contains("1000 sat of non-spendable fee credit")
            && rendered.contains("only 0 sat spendable"),
        "unexpected error: {rendered}"
    );

    // And the refusal holds on the path that would otherwise book it: nothing reaches capture, and
    // the row stays watched so funding the wallet retries it.
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = Arc::new(TestClock::new(1_000));
    let mut retired: HashSet<String> = HashSet::new();
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert!(
        rx.try_recv().is_err(),
        "a receipt the wallet demonstrably cannot refund must not reach capture"
    );
    assert!(retired.is_empty(), "the row must stay watched for a re-check");

    // [9A] again, for the negative assertions above: they cannot on their own tell "the fee-credit
    // rule refused it" from "the poll never reached this row at all". Fund the wallet to exactly the
    // receipt and the SAME poll delivers it — proving the row was pollable and the refusal was the
    // only thing holding it. This leg also pins the residual ADR-0019 now permanently accepts, on
    // the measured figures: once `balanceSat` covers the receipt, a receive that may have been fee
    // credit in full IS booked (`UnattributedButPayable`), over-booking by at most that receipt.
    ops.set_balance(1_000, 1_000);
    assert_eq!(
        be.received_amount_msat(&inv.id).await.unwrap(),
        Some(1_000_000)
    );
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    let delivered = rx.recv().await.expect("now deliverable");
    assert_eq!(delivered.received_msat, 1_000_000);
    // Dated by the record's OWN `completedAt`, not the daemon clock (which this test holds at
    // 1_000): capture sets `paid_through = settled_at + period`, so a settlement dated by the
    // observing clock would bill from the wrong instant. The measured millisecond figure in secs.
    assert_eq!(delivered.settled_at, 1_785_097_938);
}

#[tokio::test]
async fn balance_excludes_the_non_spendable_fee_credit() {
    let ops = FakePhoenixdOps::new();
    ops.set_balance(2_598, 5_000);
    let be = backend(ops.clone(), TestClock::new(1_000));
    assert_eq!(
        be.available_balance_msat().await.unwrap(),
        Some(2_598_000),
        "balanceSat*1000 only — feeCreditSat is an LSP credit, not spendable funds"
    );
}

// --------------------------------------------------------------------------------------------------
// Pay: idempotency, phoenixd's own dedup, recovery
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn pay_records_the_key_and_never_pays_twice() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 1);

    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:1:g1")
        .await
        .unwrap();
    assert_eq!(ops.pay_calls().len(), 1);
    assert_eq!(
        be.payment_status_by_key("refund:order:1:g1").await.unwrap(),
        PayStatus::Succeeded
    );
    assert_eq!(be.payment_status(&id).await.unwrap(), PayStatus::Succeeded);
    assert!(be
        .payment_started_by_key("refund:order:1:g1")
        .await
        .unwrap());

    let again = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:1:g1")
        .await
        .unwrap();
    assert_eq!(
        again, id,
        "the same key returns the same phoenixd payment id"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "and NO second payinvoice is ever issued for it"
    );
}

// Fact 2: phoenixd answers a repeated same-bolt11 payinvoice with HTTP 200 + `reason` and NO
// paymentId/preimage. For a key we already own that is SUCCESS-equivalent — decided by re-reading
// `outgoingbyhash`, never by the English string.
#[tokio::test]
async fn a_receipt_less_duplicate_response_is_success_equivalent() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 2);
    let hash = hash_of(&bolt11);

    // phoenixd already paid this hash (e.g. our own POST landed but the response was lost).
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-live".into(),
        payment_hash: hash.clone(),
        is_paid: true,
        fees_msat: 4_480,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("this invoice has already been paid".into()),
    }));

    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:2:g1")
        .await
        .unwrap();
    assert_eq!(id, "pay-live", "the real payment is adopted as this key's");
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "the duplicate answer must not trigger another pay attempt"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:2:g1").await.unwrap(),
        PayStatus::Succeeded
    );
}

// A receipt-less answer with NO paid record proves nothing moved but also gives no proof it never
// will: the key must stay PENDING with its witness intact, never be marked paid off the string.
#[tokio::test]
async fn a_receipt_less_response_without_a_record_stays_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 3);
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("this invoice has already been paid".into()),
    }));

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:3:g1")
        .await
        .expect_err("no paid record => not a success");
    assert!(format!("{err:#}").contains("leaving the attempt in flight"));
    assert_eq!(
        be.payment_status_by_key("refund:order:3:g1").await.unwrap(),
        PayStatus::Pending,
        "the witness stays PREPARED so recovery can resolve it"
    );
}

// The same terminal marker on the OTHER arm: `payinvoice` answered 200 with no receipt (which it
// also does on FAILURE — `reason`, no preimage), and the record it points at is terminal and unpaid.
// The receipt-less arm resolves that to FAILED exactly as recovery does, so a refund that fails
// synchronously does not have to wait for a later drive to be told so.
#[tokio::test]
async fn a_receipt_less_response_over_a_terminally_failed_record_resolves_failed() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 31);
    let hash = hash_of(&bolt11);

    // phoenixd's record for this hash appears BECAUSE of the POST: it landed, the route failed, the
    // payment terminated. Seeding it up front instead would mean phoenixd already had history for
    // the hash before this attempt, which the pre-POST probe declines to attribute — a different
    // behaviour from the one under test.
    ops.set_outgoing_after_pay(&hash, PhoenixdOutgoing {
        payment_id: "pay-route-failed".into(),
        payment_hash: hash.clone(),
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("payment could not be sent through existing channels".into()),
    }));

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:31:g1")
        .await
        .expect_err("a terminal failure is not a success");
    assert_eq!(
        be.payment_status_by_key("refund:order:31:g1").await.unwrap(),
        PayStatus::Failed,
        "the receipt-less arm resolves a terminal record instead of parking it Pending"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "resolving the terminal never re-POSTs inside the same drive"
    );
    assert!(
        format!("{err:#}").contains("FAILED at phoenixd"),
        "unexpected error: {err:#}"
    );
}

// The receipt-less mirror's WALLET bind. phoenixd answers the POST with no receipt, and the wallet
// answering changes before the post-POST `getinfo` re-read (proxy failover, a restore onto another
// node). That other wallet's terminal record is evidence about ITS payment history, not about the
// attempt we made against the prepared one, which may still be live — resolving on it would unlock a
// retry that pays twice. The bind only works because this arm re-reads `getinfo` at decision time;
// reusing the pre-POST answer (which `require_supported_version` had already validated) could never
// catch a swap that happens after it.
// The receipt-less mirror's VERSION bind, and the reason it must re-read `getinfo` instead of
// reusing the pre-POST answer. `require_supported_version` validated the release BEFORE the POST, so
// a bind against that value is deterministically true and proves nothing. Here phoenixd restarts
// onto an unverified release inside the window between the POST and the `outgoingbyhash` re-read:
// `completedAt` means whatever the NEW build says it means, so the record must not resolve anything.
#[tokio::test]
async fn a_receipt_less_terminal_record_on_a_release_swapped_under_the_post_stays_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 92);
    let hash = hash_of(&bolt11);
    let key = "refund:order:92:g1";

    ops.set_node_version_after_pay("0.10.0-unverified");
    ops.set_outgoing_after_pay(
        &hash,
        PhoenixdOutgoing {
            payment_id: "pay-release-swapped".into(),
            payment_hash: hash.clone(),
            is_paid: false,
            fees_msat: 0,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("payment failed".into()),
    }));

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("a marker read on an unverified release cannot resolve the attempt");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending,
        "a release swap under the POST must leave the attempt Pending"
    );
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    assert!(
        format!("{err:#}").contains("release that marker was measured against"),
        "unexpected error: {err:#}"
    );
}

// ISOLATES THE PRE-POST PROBE, and covers the restore-from-backup double pay. lnrent's ledger
// witnesses only its OWN POSTs, and `backup.rs` deliberately keeps the phoenixd WALLET out of the
// backup — so restoring an older snapshot gives lnrent a CLEAN ledger over a hash phoenixd already
// has history for. The ledger gate cannot see that; only asking phoenixd before POSTing can. Here
// the index is empty (as after a restore) while phoenixd already holds a terminal record: the new
// attempt must never adopt it, or the retry it unlocks pays over a possibly-live payment.
#[tokio::test]
async fn a_terminal_record_that_predates_our_post_is_never_adopted_after_a_restore() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 93);
    let hash = hash_of(&bolt11);
    let key = "refund:order:93:g1";

    // phoenixd's history from BEFORE the restored snapshot. Nothing in lnrent's index mentions it.
    ops.set_outgoing_for(
        &hash,
        PhoenixdOutgoing {
            payment_id: "pay-from-before-the-snapshot".into(),
            payment_hash: hash.clone(),
            is_paid: false,
            fees_msat: 0,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );
    ops.script_pay(Err("timeout".into()));

    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("the POST is ambiguous");
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("a record that predates our POST proves nothing about our attempt");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending,
        "a pre-existing record must leave the attempt Pending even though the ledger is clean"
    );
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    assert!(
        format!("{err:#}").contains("already held a record for that"),
        "the refusal must come from the PRE-POST probe, not the ledger: {err:#}"
    );
}

#[tokio::test]
async fn a_receipt_less_terminal_record_from_another_wallet_stays_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 91);
    let hash = hash_of(&bolt11);
    let key = "refund:order:91:g1";

    ops.set_node_id("03preparedwallet");
    ops.set_node_id_after_pay("03otherwallet");
    ops.set_outgoing_after_pay(
        &hash,
        PhoenixdOutgoing {
            payment_id: "pay-other-wallet-receiptless".into(),
            payment_hash: hash.clone(),
            is_paid: false,
            fees_msat: 0,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("payment failed".into()),
    }));

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("another wallet's terminal record cannot resolve this attempt");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending,
        "a wallet swap under the POST must leave the attempt Pending"
    );
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("03otherwallet") && rendered.contains("03preparedwallet"),
        "the error must name both wallets: {rendered}"
    );
}

#[tokio::test]
async fn a_receipt_less_terminal_record_for_another_hash_stays_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 32);
    let requested_hash = hash_of(&bolt11);
    let wrong_hash = "fe".repeat(32);

    ops.set_outgoing_for(
        &requested_hash,
        PhoenixdOutgoing {
            payment_id: "pay-wrong-hash".into(),
            payment_hash: wrong_hash.clone(),
            is_paid: false,
            fees_msat: 0,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );
    ops.script_pay(Ok(PayAttempt::NoReceipt {
        reason: Some("payment failed".into()),
    }));

    let key = "refund:order:32:g1";
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("terminal evidence for another hash cannot resolve this attempt");
    assert_eq!(be.payment_status_by_key(key).await.unwrap(), PayStatus::Pending);
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains(&wrong_hash) && rendered.contains(&requested_hash),
        "unexpected error: {rendered}"
    );
}

// Fact 3: an `outgoingbyhash` 404 PROVES the pay never landed, so the retry is a genuinely new
// attempt (and re-runs the INV-1 preflight).
#[tokio::test]
async fn recovery_treats_a_404_as_never_paid_and_retries() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 4);

    // A first attempt dies with an ambiguous transport error, leaving the PREPARED witness.
    ops.script_pay(Err("connection reset".into()));
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:4:g1")
        .await
        .expect_err("a transport error is ambiguous");
    assert!(format!("{err:#}").contains("stays pending"));
    assert_eq!(
        be.payment_status_by_key("refund:order:4:g1").await.unwrap(),
        PayStatus::Pending,
        "an ambiguous error must NEVER delete the recovery witness"
    );

    // phoenixd 404s the hash -> the money never moved -> the retry pays for real.
    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:4:g1")
        .await
        .unwrap();
    assert_eq!(
        ops.pay_calls().len(),
        2,
        "the proven-unpaid attempt is retried"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:4:g1").await.unwrap(),
        PayStatus::Succeeded
    );
    assert_eq!(be.payment_status(&id).await.unwrap(), PayStatus::Succeeded);
}

// …but that 404 is only proof about the wallet the attempt was PREPARED against. Repoint
// `phoenixd_url` at another phoenixd (or restore one onto a fresh payments DB) and every recorded
// hash 404s there while the original node may have paid it — re-POSTing would be a second payment of
// a live refund destination. The witness records `getinfo.nodeId`, so the retry is refused instead.
#[tokio::test]
async fn recovery_never_retries_a_404_from_a_different_phoenixd_wallet() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 41);
    let key = "refund:order:41:g1";

    ops.script_pay(Err("connection reset".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("a transport error is ambiguous");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending
    );

    // The operator repoints lnrent at a different wallet, which has never seen this hash.
    ops.set_node_id("03deadbeefnode");
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("another node's 404 is no evidence about our attempt");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("03deadbeefnode") && rendered.contains("refusing to pay again"),
        "unexpected error: {rendered}"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "the second payinvoice POST must never happen"
    );
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending,
        "the witness stays PREPARED (Pending): the Refunder re-awaits, RefundStuck surfaces it"
    );

    // Point it back at the wallet that owns the attempt and the proven-unpaid retry proceeds.
    ops.set_node_id("027e48node");
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect("the original wallet's 404 still proves the attempt never landed");
    assert_eq!(ops.pay_calls().len(), 2);
}

#[tokio::test]
async fn recovery_refuses_an_invoice_that_expired_after_an_ambiguous_attempt() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000_000);
    let be = backend(ops.clone(), clock.clone());
    let bolt11 = mint_bolt11(120_000, 40);
    let key = "refund:order:40:g1";

    // The first POST becomes ambiguous while the invoice is live, leaving a PREPARED witness.
    ops.script_pay(Err("connection reset".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("a transport error is ambiguous");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending
    );
    assert_eq!(ops.pay_calls().len(), 1);

    // The recorded hash is still a clean 404, but its bolt11 has expired during the outage. Recovery
    // records a definite no-start refusal directly, without a second payinvoice POST.
    clock.advance(3_600);
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("an expired invoice cannot be reposted after clean-404 recovery");
    assert!(format!("{err:#}").contains("destination invoice expired"));
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Failed,
        "clean 404 plus an expired bolt11 is a definite no-start terminal"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "expiry is rejected before a second payinvoice POST"
    );
}

#[tokio::test]
async fn recovery_adopts_a_paid_record_without_paying_again() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 5);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout waiting for payinvoice".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:5:g1")
        .await
        .expect_err("ambiguous");

    // The POST had in fact landed: phoenixd now has the paid record.
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-landed".into(),
        payment_hash: hash,
        is_paid: true,
        fees_msat: 4_480,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:5:g1")
        .await
        .unwrap();
    assert_eq!(id, "pay-landed");
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "a landed payment is adopted, never re-sent"
    );
}

#[tokio::test]
async fn recovery_rejects_a_paid_record_for_a_different_hash() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 51);
    let prepared_hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout waiting for payinvoice".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:51:g1")
        .await
        .expect_err("ambiguous");

    // Model a corrupt/unexpected response body: the requested URL names the PREPARED hash, but the
    // record body names another hash. The success CAS must reject it and preserve the witness.
    ops.set_outgoing_for(
        &prepared_hash,
        PhoenixdOutgoing {
            payment_id: "pay-wrong-hash".into(),
            payment_hash: "ff".repeat(32),
            is_paid: true,
            fees_msat: 4_480,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:51:g1")
        .await
        .expect_err("a mismatched recovery record cannot be adopted");
    assert!(format!("{err:#}").contains("persisted recovery witness was not changed"));
    assert_eq!(
        be.payment_status_by_key("refund:order:51:g1")
            .await
            .unwrap(),
        PayStatus::Pending,
        "the PREPARED witness remains pending after the CAS miss"
    );
    assert_eq!(
        be.payment_status("pay-wrong-hash").await.unwrap(),
        PayStatus::Unknown,
        "the mismatched payment id is never reported as this key's success"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "recovery never issues a second payinvoice POST"
    );
}

// The measured truth table as a table: `completedAt` is the terminal marker, `isPaid` splits the two
// terminals. Only the middle row is a definite failure; everything else must resolve toward Pending,
// because the cost of a wrong `true` here is a DOUBLE PAY.
#[test]
fn the_measured_terminal_marker_classifies_all_three_legs() {
    let record = |is_paid: bool, completed_at_ms: Option<i64>| PhoenixdOutgoing {
        payment_id: "3d9d75f9-6940-45f6-b611-575a21a91ef9".into(),
        payment_hash: "00".repeat(32),
        is_paid,
        fees_msat: 0,
        completed_at_ms,
    };
    assert!(
        !outgoing_is_terminally_failed(&record(true, Some(MEASURED_COMPLETED_AT_MS))),
        "isPaid + completedAt is the SUCCEEDED leg, never a failure"
    );
    assert!(
        outgoing_is_terminally_failed(&record(false, Some(MEASURED_COMPLETED_AT_MS))),
        "unpaid + completedAt is the FAILED leg"
    );
    assert!(
        !outgoing_is_terminally_failed(&record(false, None)),
        "unpaid + ABSENT completedAt is the PENDING leg — resolving it would re-pay a live payment"
    );
    // Not a shape any measured record produced. It resolves toward Pending on purpose: a marker
    // lnrent does not recognise must never be read as terminal.
    assert!(!outgoing_is_terminally_failed(&record(false, Some(0))));
    assert!(!outgoing_is_terminally_failed(&record(false, Some(-1))));
}

// The MEASURED in-flight leg: unpaid AND no `completedAt` key at all. That is the one shape lnrent
// still cannot resolve, so it stays Pending — for as many drives as it takes — and is never retried
// blindly (fact 3) and never marked terminal. Getting this backwards re-POSTs a live payment, so the
// assertion that matters is the ABSENCE of a second `payinvoice`.
#[tokio::test]
async fn recovery_never_retries_an_in_flight_outgoing_record() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 6);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:6:g1")
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-inflight".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        // The whole point: phoenixd sent no `completedAt` field, so the payment is still in flight.
        completed_at_ms: None,
    });

    // Drive it repeatedly: "Pending forever" is the contract, not "Pending once". The two MONEY
    // assertions are checked before the message one, so this test cannot be satisfied by wording.
    for drive in 1..=3 {
        let err = be
            .pay_refund_capped(&bolt11, 120, 130, "refund:order:6:g1")
            .await
            .expect_err("an in-flight record is ambiguous, not a green light");
        assert_eq!(
            be.payment_status_by_key("refund:order:6:g1").await.unwrap(),
            PayStatus::Pending,
            "drive {drive}: an in-flight payment never becomes terminal"
        );
        assert_eq!(
            ops.pay_calls().len(),
            1,
            "drive {drive}: NO second payinvoice may ever be issued for an in-flight payment"
        );
        assert!(
            format!("{err:#}").contains("carries no terminal completedAt"),
            "drive {drive}: unexpected error: {err:#}"
        );
    }
}

// The MEASURED failure leg (2026-07-26, phoenixd 0.9.0-b072567): unpaid AND `completedAt` SET is a
// route/liquidity failure that terminated. Nothing is outstanding, so the key resolves FAILED and the
// Refunder can retry it — which is what ends the every-failed-refund-needs-an-operator-DM regime.
#[tokio::test]
async fn recovery_resolves_a_terminally_failed_outgoing_record_and_lets_the_retry_run() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 61);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:61:g1")
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-failed".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:61:g1")
        .await
        .expect_err("a terminal failure is still not a payment");
    assert!(
        format!("{err:#}").contains("FAILED at phoenixd"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "resolving the terminal does NOT itself re-POST"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:61:g1").await.unwrap(),
        PayStatus::Failed,
        "unpaid + completedAt is a definite failure, not an ambiguity"
    );

    // The payoff: the Refunder's next drive of the SAME key re-runs the preflight and pays, with no
    // operator in the loop (`failed_refund_can_reuse_invoice() == true`).
    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:61:g1")
        .await
        .expect("a FAILED key re-attempts the same bolt11");
    assert_eq!(ops.pay_calls().len(), 2, "the retry actually POSTed");
    assert_eq!(
        be.payment_status_by_key("refund:order:61:g1").await.unwrap(),
        PayStatus::Succeeded
    );
    assert_eq!(be.payment_status(&id).await.unwrap(), PayStatus::Succeeded);
}

#[tokio::test]
async fn recovery_keeps_a_terminal_record_for_another_hash_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 67);
    let requested_hash = hash_of(&bolt11);
    let wrong_hash = "fd".repeat(32);
    let key = "refund:order:67:g1";

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("ambiguous");
    ops.set_outgoing_for(
        &requested_hash,
        PhoenixdOutgoing {
            payment_id: "pay-wrong-hash".into(),
            payment_hash: wrong_hash.clone(),
            is_paid: false,
            fees_msat: 0,
            completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
        },
    );

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("terminal evidence for another hash cannot resolve this attempt");
    assert_eq!(be.payment_status_by_key(key).await.unwrap(), PayStatus::Pending);
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains(&wrong_hash) && rendered.contains(&requested_hash),
        "unexpected error: {rendered}"
    );
}

#[tokio::test]
async fn recovery_keeps_terminal_evidence_from_another_wallet_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 68);
    let hash = hash_of(&bolt11);
    let key = "refund:order:68:g1";

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-other-wallet".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    ops.set_node_id("03differentwallet");

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("another wallet's record says nothing about the prepared attempt");
    assert_eq!(be.payment_status_by_key(key).await.unwrap(), PayStatus::Pending);
    assert_eq!(ops.pay_calls().len(), 1, "no retry is unlocked");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("03differentwallet") && rendered.contains("payment history"),
        "unexpected error: {rendered}"
    );
}

// Cross-key double-pay regression: key A terminally failed after POSTing P1. `FAILED` no longer
// blocks key B's legitimate POST of the same invoice, but it DOES prove that `outgoingbyhash` has
// multiple possible answers. If B times out while P2 is live and phoenixd returns P1, B must stay
// Pending; resolving it would let B's next drive POST P3 over live P2.
#[tokio::test]
async fn another_keys_buried_attempt_never_resolves_the_current_attempt() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 64);
    let hash = hash_of(&bolt11);
    let key_a = "refund:order:64-a:g1";
    let key_b = "refund:order:64-b:g1";

    // Key A POSTs P1 and then safely resolves its first failure.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key_a)
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "p1".into(),
        payment_hash: hash.clone(),
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    be.pay_refund_capped(&bolt11, 120, 130, key_a)
        .await
        .expect_err("a terminal failure is not a payment");
    assert_eq!(
        be.payment_status_by_key(key_a).await.unwrap(),
        PayStatus::Failed,
        "the first POST under a fresh hash keeps the operational win"
    );

    // A FAILED foreign key is allowed by the cross-key start guard, so B POSTs P2 and times out. The
    // fake deliberately keeps returning the FIRST buried record, P1.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key_b)
        .await
        .expect_err("ambiguous");
    assert_eq!(ops.pay_calls().len(), 2, "A's P1 and B's possibly-live P2 POSTed");

    // P1 says nothing about B's P2. Repeated recovery must neither resolve B nor POST P3.
    for drive in 1..=3 {
        let err = be
            .pay_refund_capped(&bolt11, 120, 130, key_b)
            .await
            .expect_err("another key's buried record resolves nothing");
        assert_eq!(
            be.payment_status_by_key(key_b).await.unwrap(),
            PayStatus::Pending,
            "drive {drive}: B's possibly-live P2 must remain Pending"
        );
        assert_eq!(
            ops.pay_calls().len(),
            2,
            "drive {drive}: B must never issue a second payinvoice over its possibly-live P2"
        );
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("earlier or foreign POST may exist")
                || rendered.contains("already held a record for that"),
            "drive {drive}: the refusal must be an ATTRIBUTION one — either guard may fire first, \
             but nothing else may let this resolve: {rendered}"
        );
    }
}

// Same-key three-attempt regression. This seeds the exact durable state an older unsafe build can
// leave: P1 and P2 are buried, but the one `payment_id` slot remembers only P2. Attempt 3 is live;
// phoenixd returns the FIRST record P1. Comparing record ids (`P1 != P2`) would wrongly resolve
// attempt 3 and unlock P4, so attribution must instead reject every hash with any prior POST.
#[tokio::test]
async fn the_first_of_two_buried_attempts_never_resolves_a_third_attempt() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 66);
    let hash = hash_of(&bolt11);
    let key = "refund:order:66:g1";

    // Attempt 1 POSTs P1 and resolves normally: this is the feature's supported first-failure case.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("attempt 1 is ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "p1".into(),
        payment_hash: hash.clone(),
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("attempt 1 terminally failed");

    // Attempt 2 POSTs and times out. Model the persisted P2 burial an older unsafe build could then
    // write; this intentionally bypasses today's guard so recovery compatibility is exercised.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("attempt 2 is ambiguous");
    pay_upsert_failed(
        &be.index,
        key,
        &bolt11,
        &hash,
        1_001,
        Some("p2"),
    )
    .unwrap();

    // Attempt 3 POSTs and may be live. The fake still returns the first buried record P1.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("attempt 3 is ambiguous");
    assert_eq!(ops.pay_calls().len(), 3);

    for drive in 1..=3 {
        let err = be
            .pay_refund_capped(&bolt11, 120, 130, key)
            .await
            .expect_err("P1 cannot resolve possibly-live P3");
        assert_eq!(
            be.payment_status_by_key(key).await.unwrap(),
            PayStatus::Pending,
            "drive {drive}: P3 must remain Pending even though P1 differs from remembered P2"
        );
        assert_eq!(
            ops.pay_calls().len(),
            3,
            "drive {drive}: no P4 may be POSTed over possibly-live P3"
        );
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("earlier or foreign POST may exist")
                || rendered.contains("already held a record for that"),
            "drive {drive}: the refusal must be an ATTRIBUTION one — either guard may fire first, \
             but nothing else may let this resolve: {rendered}"
        );
    }
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending
    );
}

// A preflight refusal between two attempts must not ERASE which attempt was already buried. The
// refusal itself never POSTs, so it un-buries nothing — but if it cleared the memory, the next POST's
// ambiguity would once again be resolvable by the stale record, reopening the double pay above.
#[tokio::test]
async fn a_preflight_refusal_does_not_forget_the_attempt_already_buried() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 65);
    let hash = hash_of(&bolt11);
    let key = "refund:order:65:g1";

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-attempt-1".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("terminal failure");

    // The retry is refused by the INV-1 preflight instead of POSTing (a cap that no longer covers the
    // payout plus the reserve). Nothing was sent, so nothing about attempt 1 changed.
    let err = be
        .pay_refund_capped(&bolt11, 120, 120, key)
        .await
        .expect_err("the cap cannot cover the outlay");
    assert!(
        format!("{err:#}").contains("exceeding the INV-1 cap"),
        "unexpected error: {err:#}"
    );
    assert_eq!(ops.pay_calls().len(), 1, "a refusal never POSTs");

    // A later retry does POST, and goes ambiguous.
    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("ambiguous");
    assert_eq!(ops.pay_calls().len(), 2);

    // The buried attempt is still the only record phoenixd offers, and it must still be recognised as
    // buried across the refusal that happened in between.
    be.pay_refund_capped(&bolt11, 120, 130, key)
        .await
        .expect_err("a buried attempt's record resolves nothing");
    assert_eq!(
        be.payment_status_by_key(key).await.unwrap(),
        PayStatus::Pending,
        "the refusal must not have erased which attempt was already buried"
    );
    assert_eq!(
        ops.pay_calls().len(),
        2,
        "NO third payinvoice over a possibly-live attempt"
    );
}

// The VERSION BIND. The truth table above is observed behaviour of ONE release, so on any other one
// the terminal marker means nothing to lnrent and the key must fall back to exactly the stay-Pending
// behaviour that predates the measurement — a `RefundStuck` DM, never a payment decided on an
// assumption about someone else's build.
#[tokio::test]
async fn a_terminal_marker_on_an_unverified_release_stays_pending() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 62);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:62:g1")
        .await
        .expect_err("ambiguous");
    // Byte-for-byte the record the arm above resolves to FAILED...
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-failed".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    // ...but the operator upgraded phoenixd under it.
    ops.set_node_version("0.10.0-bdeadbee");

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:62:g1")
        .await
        .expect_err("an unverified release cannot resolve the marker");
    // The STATE is the claim; the message is checked after it, so removing the bind cannot be
    // papered over by rewording.
    assert_eq!(
        be.payment_status_by_key("refund:order:62:g1").await.unwrap(),
        PayStatus::Pending,
        "the version mismatch falls back to stay-Pending even though completedAt is SET"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "and the fallback never re-POSTs either"
    );
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("not the release that marker was measured against")
            && !rendered.contains("version is unreadable"),
        "unexpected version-mismatch error: {rendered}"
    );

    // Downgrade the claim, not the evidence: put the verified release back and the SAME record now
    // resolves — proving the mismatch, not some unrelated refusal, is what held it Pending.
    ops.set_node_version(&FeeSchedule::default().verified_version);
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:62:g1")
        .await
        .expect_err("still not a payment");
    assert_eq!(
        be.payment_status_by_key("refund:order:62:g1").await.unwrap(),
        PayStatus::Failed
    );
}

// A node lnrent cannot even ask for its version is the same fallback: no marker resolution, stay
// Pending. (`getinfo` answering 503 is the shape a proxy in front of a restarting node gives.)
#[tokio::test]
async fn a_terminal_marker_stays_pending_when_the_version_cannot_be_read() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 63);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:63:g1")
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-failed".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(MEASURED_COMPLETED_AT_MS),
    });
    ops.fail_node_info_with_status(503);

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:63:g1")
        .await
        .expect_err("an unreadable version cannot resolve the marker");
    assert_eq!(
        be.payment_status_by_key("refund:order:63:g1").await.unwrap(),
        PayStatus::Pending
    );
    assert_eq!(ops.pay_calls().len(), 1);
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("version is unreadable")
            && !rendered.contains("not the release that marker was measured against"),
        "unexpected unreadable-version error: {rendered}"
    );
}

// --------------------------------------------------------------------------------------------------
// Pay: INV-1 and the structural preflights
// --------------------------------------------------------------------------------------------------

#[tokio::test]
async fn inv1_refuses_a_payout_whose_reserve_exceeds_the_receipt() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 7);

    // 120 sat payout costs 124_480 msat with the reserve; a 124-sat receipt cannot cover it.
    let err = be
        .pay_refund_capped(&bolt11, 120, 124, "refund:order:7:g1")
        .await
        .expect_err("payout + reserve exceeds the net credit");
    assert!(
        format!("{err:#}").contains("exceeding the INV-1 cap"),
        "unexpected error: {err:#}"
    );
    assert!(
        ops.pay_calls().is_empty(),
        "the refusal must happen BEFORE any money moves"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:7:g1").await.unwrap(),
        PayStatus::Failed,
        "a proven no-payment refusal is FAILED, so the Refunder can re-quote at a fresh generation"
    );

    // One sat more of receipt and the same payout fits exactly.
    be.pay_refund_capped(&bolt11, 120, 125, "refund:order:7:g1")
        .await
        .expect("124_480 msat fits a 125_000 msat cap");
    assert_eq!(ops.pay_calls().len(), 1);
}

#[tokio::test]
async fn pay_capped_refuses_when_the_outlay_ceiling_is_too_low() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 8);

    let err = be
        .pay_capped(&bolt11, 120, 124_479, "sweep:1")
        .await
        .expect_err("one msat short of the true outlay");
    assert!(format!("{err:#}").contains("exceeding the INV-1 cap"));
    assert!(ops.pay_calls().is_empty());

    be.pay_capped(&bolt11, 120, 124_480, "sweep:1")
        .await
        .expect("the exact outlay is allowed");
    assert_eq!(ops.pay_calls().len(), 1);
}

#[tokio::test]
async fn refund_quote_and_outlay_use_the_trampoline_reserve() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    assert_eq!(be.refund_net_sat(130).await.unwrap(), 125);
    let quote = be.refund_quote(130).await.unwrap();
    assert_eq!(quote.net_sat, 125);
    assert_eq!(quote.gateway_hint, None, "phoenixd has no gateway concept");
    assert_eq!(
        be.refund_required_outlay_msat(130, Some(120))
            .await
            .unwrap(),
        124_480
    );
    assert_eq!(
        be.refund_required_outlay_msat(130, None).await.unwrap(),
        129_500,
        "no explicit payout -> price the net cap (125 sat) plus its reserve"
    );
    assert_eq!(
        be.refund_required_outlay_msat(4, None).await.unwrap(),
        0,
        "dust needs no outlay"
    );
}

// `payinvoice` sends the invoice's ENCODED amount, so a destination for a different (larger) amount
// would slip past a cap computed on `amount_sat`.
#[tokio::test]
async fn a_destination_for_a_different_amount_is_refused_before_paying() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(500_000, 9); // 500 sat invoice, but we owe 120

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:9:g1")
        .await
        .expect_err("invoice amount != owed");
    assert!(format!("{err:#}").contains("!= owed 120 sat"));
    assert!(ops.pay_calls().is_empty());
    assert_eq!(
        be.payment_status_by_key("refund:order:9:g1").await.unwrap(),
        PayStatus::Failed
    );
}

#[tokio::test]
async fn an_amountless_destination_is_refused_before_paying() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = {
        use bitcoin::hashes::{sha256, Hash};
        use bitcoin::secp256k1::{Secp256k1, SecretKey};
        use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
        use std::time::{Duration, SystemTime};
        let sk = SecretKey::from_slice(&[0x11u8; 32]).unwrap();
        InvoiceBuilder::new(Currency::Regtest)
            .description("amountless".to_string())
            .payment_hash(sha256::Hash::hash(&[0x5au8; 32]))
            .payment_secret(PaymentSecret([42u8; 32]))
            .timestamp(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000))
            .min_final_cltv_expiry_delta(144)
            .expiry_time(Duration::from_secs(3600))
            .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &sk))
            .expect("amountless bolt11 builds")
            .to_string()
    };

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:10:g1")
        .await
        .expect_err("an amountless invoice cannot be bounded");
    assert!(format!("{err:#}").contains("encodes no amount"));
    assert!(ops.pay_calls().is_empty());
}

// [8A]: phoenixd dedups by payment hash node-wide, so adopting a bolt11 another key already owns
// would credit ONE real payment to TWO lnrent refunds.
#[tokio::test]
async fn a_destination_owned_by_another_key_is_refused_without_paying() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 11);

    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:11:g1")
        .await
        .expect("the first key pays it");
    assert_eq!(ops.pay_calls().len(), 1);

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:OTHER:g1")
        .await
        .expect_err("a second key must not adopt the first key's payment");
    assert!(
        format!("{err:#}").contains("already owned by idempotency key refund:order:11:g1"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "and the collision is caught BEFORE any second payinvoice"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:OTHER:g1")
            .await
            .unwrap(),
        PayStatus::Failed
    );

    // ORDERING: the refused key must be recorded FAILED DIRECTLY, never through PREPARED — a
    // PREPARED row naming a hash another key already paid is exactly what recovery would adopt. So a
    // re-drive re-refuses instead of resolving that hash into this key's success.
    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:OTHER:g1")
        .await
        .expect_err("a re-drive must re-refuse, never adopt the other key's payment");
    assert!(format!("{err:#}").contains("already owned by idempotency key"));
    assert_eq!(
        be.payment_status_by_key("refund:order:OTHER:g1")
            .await
            .unwrap(),
        PayStatus::Failed,
        "the colliding key never becomes SUCCEEDED"
    );
    assert_eq!(ops.pay_calls().len(), 1);
}

// `failed_refund_can_reuse_invoice() == true` is only sound if a FAILED key actually re-attempts:
// a FAILED row means nothing is outstanding under the key, so a retry re-runs the preflight and can
// now succeed (e.g. after the receipt grew enough to cover the reserve).
#[tokio::test]
async fn a_failed_key_re_runs_the_preflight_on_the_same_invoice() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    assert!(be.failed_refund_can_reuse_invoice());
    let bolt11 = mint_bolt11(120_000, 12);

    be.pay_refund_capped(&bolt11, 120, 124, "refund:order:12:g1")
        .await
        .expect_err("over cap");
    assert_eq!(
        be.payment_status_by_key("refund:order:12:g1")
            .await
            .unwrap(),
        PayStatus::Failed
    );

    let id = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:12:g1")
        .await
        .expect("a refused key retries the same bolt11 once it fits");
    assert_eq!(ops.pay_calls().len(), 1);
    assert_eq!(be.payment_status(&id).await.unwrap(), PayStatus::Succeeded);
}

#[tokio::test]
async fn a_succeeded_key_can_never_be_walked_back_to_prepared() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 13);
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:13:g1")
        .await
        .unwrap();

    let index = be.index.clone();
    let err = pay_upsert_prepared(
        &index,
        "refund:order:13:g1",
        &bolt11,
        &hash_of(&bolt11),
        "027e48node",
        false,
    )
    .expect_err("a completed payment must never return to in-flight");
    assert!(format!("{err:#}").contains("already SUCCEEDED"));
}

#[tokio::test]
async fn readiness_requires_a_reachable_fee_compatible_node() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    assert!(be.backend_ready().await.unwrap());
    assert!(be.refund_gateway_ready().await.unwrap());
    ops.set_node_version("0.9.1");
    assert!(
        be.backend_ready().await.is_err(),
        "a version whose fee schedule was not verified fails readiness"
    );
    assert!(be.refund_gateway_ready().await.is_err());
    ops.st.lock().unwrap().node_ok = false;
    assert!(
        be.backend_ready().await.is_err(),
        "an unreachable node fails CLOSED, never a silent Ok(false)"
    );
    assert!(be.refund_gateway_ready().await.is_err());
}

#[tokio::test]
async fn incompatible_version_refuses_a_new_pay_before_prepared_or_post() {
    let ops = FakePhoenixdOps::new();
    ops.set_node_version("0.10.0");
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 52);

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:52:g1")
        .await
        .expect_err("an unverified fee schedule must fail closed");
    let rendered = format!("{err:#}");
    assert!(rendered.contains("0.10.0"));
    assert!(rendered.contains(&FeeSchedule::default().verified_version));
    assert!(
        ops.pay_calls().is_empty(),
        "the version preflight runs before payinvoice"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:52:g1")
            .await
            .unwrap(),
        PayStatus::Unknown,
        "the mismatch is detected before a PREPARED witness is recorded"
    );
}

// --------------------------------------------------------------------------------------------------
// Operator doctor probe (lnrent-5mi)
// --------------------------------------------------------------------------------------------------

// The probe answers the two conditions the money path fails CLOSED on, as SEPARATE operator states
// instead of one Err string: reachable+authenticated, and running the verified release. Healthy node
// -> Ready, carrying the version the doctor prints and phoenixd's `feeCreditSat`.
#[tokio::test]
async fn the_probe_reports_a_healthy_node_ready_with_its_version_and_fee_credit() {
    let ops = FakePhoenixdOps::new();
    ops.set_balance(50_000, 2_723);
    let be = backend(ops.clone(), TestClock::new(1_000));

    let probe = be.phoenixd_probe().await.unwrap();
    assert_eq!(
        probe,
        PhoenixdProbe::Ready {
            version: FeeSchedule::default().verified_version.clone(),
            verified_version: FeeSchedule::default().verified_version.clone(),
            balance_sat: 50_000,
            fee_credit_sat: 2_723,
        }
    );
}

// MEASURED 2026-07-25: a live node reports `getinfo.version` WITH a build suffix
// ("0.9.0-b072567") while the schedule pins the RELEASE ("0.9.0"). The suffix is the git hash
// directly, so its leading `b` is not a marker and another build can start with any hex digit. The
// probe must reuse the money path's predicate, not an exact-equality compare — which would fail the
// doctor on the very node the schedule was verified against (and disagree with a money path that
// pays happily) — and must preserve either hash shape for the operator report.
#[tokio::test]
async fn the_probe_accepts_the_build_suffixed_version_the_money_path_accepts() {
    for running_version in ["0.9.0-b072567", "0.9.0-a1b2c3d"] {
        let ops = FakePhoenixdOps::new();
        ops.set_node_version(running_version);
        let be = backend(ops.clone(), TestClock::new(1_000));

        assert!(
            matches!(
                be.phoenixd_probe().await.unwrap(),
                PhoenixdProbe::Ready { ref version, .. } if version == running_version
            ),
            "a build-suffixed 0.9.0 node is the verified release and its git hash stays visible"
        );
        assert!(
            be.backend_ready().await.unwrap(),
            "…and the money path agrees, which is the point of sharing the predicate"
        );
    }
}

// An unverified release: the doctor reports the MISMATCH (both versions) rather than an unreachable
// node, and the money path refuses the same node — one predicate, two surfaces.
#[tokio::test]
async fn the_probe_reports_an_unverified_release_as_a_version_mismatch() {
    let ops = FakePhoenixdOps::new();
    ops.set_node_version("0.10.0-bdeadbee");
    let be = backend(ops.clone(), TestClock::new(1_000));

    assert_eq!(
        be.phoenixd_probe().await.unwrap(),
        PhoenixdProbe::VersionMismatch {
            running: "0.10.0-bdeadbee".to_string(),
            verified: FeeSchedule::default().verified_version.clone(),
        }
    );
    assert!(be.backend_ready().await.is_err());
}

// A node that never answers is UNREACHABLE, carrying the diagnostic — distinct from a node that
// answered and rejected the credentials.
#[tokio::test]
async fn the_probe_reports_a_dead_node_as_unreachable() {
    let ops = FakePhoenixdOps::new();
    ops.st.lock().unwrap().node_ok = false;
    let be = backend(ops.clone(), TestClock::new(1_000));

    match be.phoenixd_probe().await.unwrap() {
        PhoenixdProbe::Unreachable(e) => assert!(e.contains("phoenixd unreachable"), "{e}"),
        other => panic!("expected Unreachable, got {other:?}"),
    }
}

// A REJECTED api password is its own operator remedy, so the probe reads the STATUS CODE (401 from
// phoenixd itself, 403 from a fronting proxy on the https deployment shape) rather than guessing from
// the message. Any other non-2xx stays unreachable-shaped: it is not an authentication answer.
#[tokio::test]
async fn the_probe_distinguishes_a_rejected_password_from_an_unreachable_node() {
    for (status, expected) in [
        (401u16, Some(401u16)),
        (403, Some(403)),
        (500, None),
        (404, None),
    ] {
        let ops = FakePhoenixdOps::new();
        ops.fail_node_info_with_status(status);
        let be = backend(ops.clone(), TestClock::new(1_000));
        match (be.phoenixd_probe().await.unwrap(), expected) {
            (PhoenixdProbe::AuthRejected { status: got }, Some(want)) => assert_eq!(got, want),
            (PhoenixdProbe::Unreachable(e), None) => {
                assert!(e.contains(&status.to_string()), "carries the status: {e}")
            }
            (other, _) => panic!("HTTP {status} classified as {other:?}"),
        }
    }
}

// `getbalance` runs only AFTER `getinfo` already authenticated with the same credential, so NO
// status it returns — 401/403 included — may be rendered as a rejected api password: that would send
// the operator to fix a password the previous call demonstrably accepted. Every failure here is
// endpoint-accurate instead, naming getbalance and carrying the status (a path-restricting proxy in
// front of the node is the realistic cause of a 401 on one endpoint but not the other).
#[tokio::test]
async fn a_failing_getbalance_never_blames_the_password_getinfo_just_accepted() {
    for status in [401u16, 403, 502] {
        let ops = FakePhoenixdOps::new();
        ops.fail_balance_with_status(status);
        let be = backend(ops.clone(), TestClock::new(1_000));
        match be.phoenixd_probe().await.unwrap() {
            PhoenixdProbe::BalanceUnavailable(e) => {
                assert!(e.contains("getbalance"), "names the failing endpoint: {e}");
                assert!(e.contains(&status.to_string()), "carries the status: {e}");
            }
            other => panic!("HTTP {status} on getbalance classified as {other:?}"),
        }
    }
}

// --------------------------------------------------------------------------------------------------
// Secret hygiene: the api password must never reach a log line, a Debug render, or an error string.
// --------------------------------------------------------------------------------------------------

// Deliberately shaped like a valid phoenixd build id: generic syntax validation would accept
// `0.9.0-{TEST_PASSWORD}`, so the real ops layer must use the actual credential to redact it.
const TEST_PASSWORD: &str = "b9f3a77";

#[test]
fn real_ops_debug_redacts_the_api_password() {
    let ops = super::real::RealPhoenixdOps::new("http://127.0.0.1:9740/", TEST_PASSWORD)
        .expect("loopback ops build");
    let rendered = format!("{ops:?}");
    assert!(
        !rendered.contains(TEST_PASSWORD),
        "Debug leaked the api password: {rendered}"
    );
    assert!(rendered.contains("<redacted>"));
}

// Every error string this backend can surface is printed to operator logs, so no failure path may
// interpolate the credential. Port 1 on loopback refuses instantly, exercising the real error paths.
#[tokio::test]
async fn real_ops_errors_never_contain_the_api_password() {
    let ops = super::real::RealPhoenixdOps::new("http://127.0.0.1:1/", TEST_PASSWORD)
        .expect("loopback ops build");
    let mut errors = Vec::new();
    errors.push(format!("{:#}", ops.node_info().await.unwrap_err()));
    errors.push(format!("{:#}", ops.balance().await.unwrap_err()));
    errors.push(format!(
        "{:#}",
        ops.create_invoice(1000, "memo", "ext:1", 600)
            .await
            .unwrap_err()
    ));
    errors.push(format!(
        "{:#}",
        ops.incoming_by_external_id("ext:1").await.unwrap_err()
    ));
    errors.push(format!("{:#}", ops.pay_invoice("lnbc1").await.unwrap_err()));
    errors.push(format!(
        "{:#}",
        ops.outgoing_by_hash("00ff").await.unwrap_err()
    ));
    for e in &errors {
        assert!(
            !e.contains(TEST_PASSWORD),
            "an error string leaked the api password: {e}"
        );
    }
}

// Readiness reporting consumes the typed error from `require_supported_version`, not the full doctor
// probe. Prove the typed path applies the real ops layer's credential-aware version projection before
// the error can reach a log.
#[tokio::test]
async fn readiness_version_error_never_contains_the_api_password() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = sock.read(&mut buf).await.unwrap();
        let body = format!(
            r#"{{"nodeId":"027e48node","version":"9.9.9-{TEST_PASSWORD}"}}"#
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(response.as_bytes()).await.unwrap();
    });

    let ops = super::real::RealPhoenixdOps::new(&format!("http://{addr}/"), TEST_PASSWORD)
        .expect("loopback ops build");
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let be = PhoenixdPayment::with_ops(
        Arc::new(ops),
        index,
        Arc::new(TestClock::new(1_000)),
        FeeSchedule::default(),
    );

    let error = be
        .backend_ready()
        .await
        .expect_err("the unverified release must fail readiness");
    let failure = error
        .downcast_ref::<PhoenixdReadinessError>()
        .expect("readiness failure keeps its typed phoenixd classification");
    let detail = crate::preflight::phoenixd_check_from_probe(failure.probe().clone())
        .expect("the failure renders a preflight check")
        .detail;

    for rendered in [format!("{error:#}"), detail] {
        assert!(
            !rendered.contains(TEST_PASSWORD),
            "readiness output leaked the api password: {rendered}"
        );
        assert!(
            rendered.contains(REDACTED_PHOENIXD_VERSION),
            "unsafe remote version was not visibly redacted: {rendered}"
        );
    }
}

// A preflight report is OPERATOR-FACING output that gets pasted into issues, so the api password must
// not survive into any check the doctor prints. This drives the REAL HTTP layer (the only component
// that ever holds the credential) through the REAL rendering: a node that refuses to connect, one
// that answers 401, and one whose getinfo succeeds before getbalance fails. Error bodies echo the
// password to prove none of these operator-facing states carry them.
#[tokio::test]
async fn the_doctor_probe_never_prints_the_api_password() {
    let mut rendered = Vec::new();

    // Unreachable: port 1 on loopback refuses instantly, so the detail carries a REAL transport error.
    let dead = super::real::RealPhoenixdOps::new("http://127.0.0.1:1/", TEST_PASSWORD)
        .expect("loopback ops build");
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let be = PhoenixdPayment::with_ops(
        Arc::new(dead),
        index,
        Arc::new(TestClock::new(1_000)),
        FeeSchedule::default(),
    );
    let probe = be.phoenixd_probe().await.unwrap();
    assert!(
        matches!(probe, PhoenixdProbe::Unreachable(_)),
        "expected Unreachable, got {probe:?}"
    );
    rendered.push(format!("{probe:?}"));
    rendered.push(
        crate::preflight::phoenixd_check_from_probe(probe)
            .expect("an applicable probe renders a check")
            .detail,
    );

    // Auth rejected by a server whose error body echoes the credential back at us.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        while let Ok((mut sock, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            let body = format!("rejected password {TEST_PASSWORD}");
            let resp = format!(
                "HTTP/1.1 401 Unauthorized\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        }
    });
    let rejecting = super::real::RealPhoenixdOps::new(&format!("http://{addr}/"), TEST_PASSWORD)
        .expect("loopback ops build");
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let be = PhoenixdPayment::with_ops(
        Arc::new(rejecting),
        index,
        Arc::new(TestClock::new(1_000)),
        FeeSchedule::default(),
    );
    let probe = be.phoenixd_probe().await.unwrap();
    assert_eq!(
        probe,
        PhoenixdProbe::AuthRejected { status: 401 },
        "a live 401 is classified from the STATUS, not the body"
    );
    rendered.push(format!("{probe:?}"));
    rendered.push(
        crate::preflight::phoenixd_check_from_probe(probe)
            .expect("an applicable probe renders a check")
            .detail,
    );

    // getinfo succeeds, then getbalance returns a non-auth failure whose body echoes the password.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for _ in 0..2 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = if request.starts_with("GET /getinfo ") {
                (
                    "200 OK",
                    r#"{"nodeId":"027e48node","version":"0.9.0-b072567"}"#.to_string(),
                )
            } else {
                (
                    "502 Bad Gateway",
                    format!("upstream echoed password {TEST_PASSWORD}"),
                )
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }
    });
    let balance_failing =
        super::real::RealPhoenixdOps::new(&format!("http://{addr}/"), TEST_PASSWORD)
            .expect("loopback ops build");
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let be = PhoenixdPayment::with_ops(
        Arc::new(balance_failing),
        index,
        Arc::new(TestClock::new(1_000)),
        FeeSchedule::default(),
    );
    let probe = be.phoenixd_probe().await.unwrap();
    assert!(
        matches!(probe, PhoenixdProbe::BalanceUnavailable(_)),
        "expected BalanceUnavailable, got {probe:?}"
    );
    rendered.push(format!("{probe:?}"));
    rendered.push(
        crate::preflight::phoenixd_check_from_probe(probe)
            .expect("an applicable probe renders a check")
            .detail,
    );

    // A successful response is remote-controlled too. A malicious endpoint/proxy receives the Basic
    // header and can echo the password through `version`; release-prefix matching accepts this shape,
    // so the preflight-specific projection must redact it without changing the money-path predicate.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for _ in 0..2 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);
            let body = if request.starts_with("GET /getinfo ") {
                format!(
                    r#"{{"nodeId":"027e48node","version":"0.9.0-{TEST_PASSWORD}"}}"#
                )
            } else {
                r#"{"balanceSat":50000,"feeCreditSat":0}"#.to_string()
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        }
    });
    let echoing = super::real::RealPhoenixdOps::new(&format!("http://{addr}/"), TEST_PASSWORD)
        .expect("loopback ops build");
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let be = PhoenixdPayment::with_ops(
        Arc::new(echoing),
        index,
        Arc::new(TestClock::new(1_000)),
        FeeSchedule::default(),
    );
    let probe = be.phoenixd_probe().await.unwrap();
    assert!(
        matches!(probe, PhoenixdProbe::Ready { .. }),
        "the compatible release remains ready, got {probe:?}"
    );
    rendered.push(format!("{probe:?}"));
    rendered.push(
        crate::preflight::phoenixd_check_from_probe(probe)
            .expect("an applicable probe renders a check")
            .detail,
    );

    for s in &rendered {
        assert!(
            !s.contains(TEST_PASSWORD),
            "the doctor leaked the api password: {s}"
        );
    }
}

// A phoenixd error body is written by whatever answered the call — phoenixd itself, or the operator's
// TLS terminator — so a body that echoes the request's `Authorization` header would put the api
// password in the operator's logs. §13 says that never happens, and it must not depend on the
// upstream's manners.
#[test]
fn a_logged_error_body_can_never_carry_the_api_password() {
    // The plaintext credential is replaced wherever it appears.
    let scrubbed = super::real::loggable_error_body(
        &format!("upstream rejected token {TEST_PASSWORD} for /payinvoice"),
        TEST_PASSWORD,
    )
    .expect("an ordinary body is still logged");
    assert!(!scrubbed.contains(TEST_PASSWORD), "leaked: {scrubbed}");
    assert!(scrubbed.contains("<redacted>"));

    // The ENCODED form no plaintext match could catch: drop the body whole rather than truncate it,
    // because a truncated credential is still a credential.
    for echo in [
        "400 Bad Request\nAuthorization: Basic OnNlY3JldA==",
        "unexpected header basic OnNlY3JldA==",
    ] {
        assert_eq!(
            super::real::loggable_error_body(echo, TEST_PASSWORD),
            None,
            "a body that looks like an auth-header echo must not be logged at all"
        );
    }

    // The operator's actual diagnostic still survives — this log line is their only signal for WHY a
    // refund will not go out.
    assert_eq!(
        super::real::loggable_error_body("insufficient balance", TEST_PASSWORD).as_deref(),
        Some("insufficient balance")
    );
    assert_eq!(super::real::loggable_error_body("   ", TEST_PASSWORD), None);
}

// MEASURED 2026-07-25 on the live node: `GET /payments/incoming?externalId=…` WITHOUT `all=true`
// returns only RECEIVED payments — an unpaid invoice comes back as `[]`, and so does an expired one.
// Every OPEN-invoice answer in this backend is read from that array, so losing the flag would make
// `lookup` report UNKNOWN for the entire life of an unpaid invoice. The fake seam cannot catch that
// (it is the HTTP layer), so the query itself is asserted here.
#[test]
fn the_incoming_query_asks_for_unpaid_records_too() {
    let base = url::Url::parse("https://phoenixd.example/sub/path/payments/incoming").unwrap();
    let url = super::real::incoming_by_external_id_url(&base, "lnrent:order:7:g1");
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(
        pairs.contains(&("all".to_string(), "true".to_string())),
        "unpaid/expired records are invisible without all=true: {url}"
    );
    assert!(pairs.contains(&(
        "externalId".to_string(),
        "lnrent:order:7:g1".to_string()
    )));
    assert!(
        url.as_str().contains("lnrent%3Aorder%3A7%3Ag1"),
        "the external id is percent-encoded: {url}"
    );
    assert!(url.as_str().starts_with("https://phoenixd.example/sub/path/"));
}

// lnrent's own window is pushed down to the bolt11 (measured honoured), so the invoice the buyer
// holds dies with the reservation instead of staying payable for phoenixd's 24 h default.
#[tokio::test]
async fn create_invoice_pushes_lnrents_expiry_down_to_phoenixd() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let inv = be
        .create_invoice(25_000, "memo", 900, "ext:1")
        .await
        .unwrap();
    assert_eq!(inv.expires_at, 1_900, "lnrent still keeps its own window");
    assert_eq!(
        ops.create_calls(),
        vec![(25_000, "memo".to_string(), "ext:1".to_string(), 900)],
        "expirySeconds carries lnrent's window to phoenixd"
    );
}

// A row drops out on lnrent's clock alone once the bolt11 it names cannot be paid any more, and it
// does so IN THE QUERY: `phoenixd_invoice` is never GC'd, so a poll that read every historical row
// every 5 s would cost O(all invoices ever) in reads, allocation and index-lock time forever, and a
// row phoenixd stops returning would also be re-fetched every cycle — one SEQUENTIAL round-trip per
// live row, `HTTP_TIMEOUT` apiece against an unreachable node.
#[tokio::test]
async fn the_poll_retires_a_row_the_clock_says_can_no_longer_be_paid() {
    let ops = FakePhoenixdOps::new();
    let clock = TestClock::new(1_000);
    let be = backend(ops.clone(), clock.clone());
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    // phoenixd stops returning the record entirely (never `isExpired`, so no evidence-based exit).
    ops.set_incoming("ext:1", vec![]);
    let calls_after_create = ops.incoming_calls().len();

    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = Arc::new(clock.clone());
    let mut retired: HashSet<String> = HashSet::new();

    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert_eq!(
        ops.incoming_calls().len(),
        calls_after_create + 1,
        "inside the window the row is still polled"
    );
    assert!(retired.is_empty());
    assert_eq!(
        idx_pollable_invoices(&be.index, 1_000 - SETTLEMENT_POLL_GRACE_SECS)
            .unwrap()
            .len(),
        1,
        "inside the window the row is still read"
    );

    clock.set(inv.expires_at + SETTLEMENT_POLL_GRACE_SECS + 1);
    assert!(poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &tx, &mut retired).await);
    assert_eq!(
        ops.incoming_calls().len(),
        calls_after_create + 1,
        "past the grace the row costs no round-trip"
    );
    assert!(
        idx_pollable_invoices(&be.index, clock.now() - SETTLEMENT_POLL_GRACE_SECS)
            .unwrap()
            .is_empty(),
        "past the grace the row is not even read: the poll's cost tracks LIVE invoices, not the \
         never-GC'd table"
    );
}

// ADR-0019: the trampoline schedule is operator config with a version-verified default, and the
// money path fails closed until the operator configures one they verified for their own release.
#[tokio::test]
async fn an_operator_verified_schedule_replaces_the_default_reserve_and_version() {
    let schedule = FeeSchedule {
        verified_version: "0.9.1".to_string(),
        base_msat: 5_000,
        ppm: 6_000,
    };
    let index = Connection::open_in_memory().unwrap();
    index.execute_batch(INDEX_SCHEMA).unwrap();
    let ops = FakePhoenixdOps::new();
    ops.set_node_version("0.9.1");
    let be = PhoenixdPayment::with_ops(
        ops.clone(),
        index,
        Arc::new(TestClock::new(1_000)),
        schedule.clone(),
    );

    assert!(
        be.backend_ready().await.unwrap(),
        "the release the operator verified is accepted"
    );
    // 120 sat payout: 5_000 base + ceil(120_000 * 6_000 / 1e6) = 5_720 msat.
    assert_eq!(
        be.refund_required_outlay_msat(130, Some(120)).await.unwrap(),
        125_720
    );
    assert_eq!(schedule.fee_msat(sat_to_msat(120)), 5_720);

    // …and the default schedule still refuses that node, so an upgrade cannot silently keep
    // reserving the old (too small) fee.
    let default_be = backend(ops.clone(), TestClock::new(1_000));
    assert!(default_be.backend_ready().await.is_err());
}

// MEASURED: a real node reports `getinfo.version` WITH a build suffix ("0.9.0-b072567"), so an exact
// string pin would reject the very release the schedule was verified on.
#[test]
fn a_build_suffixed_version_still_matches_the_verified_release() {
    let schedule = FeeSchedule::default();
    assert!(schedule.matches_running_version("0.9.0-b072567"));
    assert!(schedule.matches_running_version("0.9.0"));
    assert!(!schedule.matches_running_version("0.9.1"));
    assert!(!schedule.matches_running_version("0.10.0-b072567"));
    let pinned_build = FeeSchedule {
        verified_version: "0.9.0-b072567".to_string(),
        ..FeeSchedule::default()
    };
    assert!(
        pinned_build.matches_running_version("0.9.0-b072567"),
        "an operator may pin one exact build"
    );
    assert!(!pinned_build.matches_running_version("0.9.0-other"));
}

// codex PR-61 P2 regression: the JSON side-fields are not authoritative — the buyer pays the
// `serialized` bolt11. A phoenixd (or a terminating TLS proxy, which the remote deployment shape
// allows) that returns a bolt11 encoding a DIFFERENT payment hash than the advertised `paymentHash`
// must be REFUSED, not persisted: lnrent would index a hash nobody can pay, so the buyer's
// settlement would be permanently unobservable — paid, no service, no refund.
#[tokio::test]
async fn create_invoice_refuses_a_bolt11_whose_hash_disagrees_with_the_response() {
    let ops = FakePhoenixdOps::new();
    ops.set_create_bolt11_hash_mismatch();
    let be = backend(ops.clone(), TestClock::new(1_000));

    let err = be
        .create_invoice(25_000, "memo", 600, "ext:hash-mismatch")
        .await
        .expect_err("a bolt11 whose encoded hash disagrees with paymentHash must be refused");

    let msg = format!("{err:#}");
    assert!(
        msg.contains("bolt11 encoding"),
        "the error must name the hash disagreement, got: {msg}"
    );
}
