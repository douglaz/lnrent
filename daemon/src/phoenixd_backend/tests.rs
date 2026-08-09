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

/// phoenixd's `completedAt` from the 2026-07-26 live measurement, epoch MILLIS. Used wherever a test
/// builds a record the measured truth table says must carry one: `isPaid=true` implies the marker is
/// SET, so a paid record with `None` is a shape the live node never emits and would test green
/// against an impossible input.
const MEASURED_COMPLETED_AT_MS: i64 = 1_785_101_384_178;

/// The operator warning on the book-and-warn arm must name the half of the refusal that ACTUALLY
/// failed. Refusing needs `fee_credit >= received` AND `balance < received`; the arm fires when at
/// least one fails, so "both did not hold" is false whenever only one did. Three successive attempts
/// at this sentence were wrong in one direction or the other, which is why the branch is a function
/// with this test under it.
#[test]
fn the_credit_booking_reason_names_the_half_that_actually_failed() {
    // The case the surrounding docs are written on: credit (999) < receipt (1000), balance zero.
    // The BALANCE half of the refusal holds here, so a message blaming the balance would be false.
    let reason = super::credit_booking_reason(1_000, 999);
    assert!(
        reason.contains("fee credit is smaller"),
        "credit<receipt must blame the credit: {reason}"
    );
    assert!(
        reason.contains("may still be zero"),
        "and must NOT imply the balance covers a refund: {reason}"
    );

    // The other disjunct: the credit could cover the whole receipt, so the arm can only have been
    // reached because the balance covers it. Blaming the credit here would be the inverse error.
    let reason = super::credit_booking_reason(1_000, 5_000);
    assert!(
        reason.contains("spendable balance covers"),
        "credit>=receipt must blame the balance: {reason}"
    );
    assert!(
        !reason.contains("fee credit is smaller"),
        "and must not also claim the credit is smaller: {reason}"
    );

    // Boundary: exactly equal is NOT "smaller", so it takes the balance-covers branch.
    assert!(super::credit_booking_reason(1_000, 1_000).contains("spendable balance covers"));

    // And the classifier really does route both of those inputs to this arm, or the reasons above
    // would be describing a branch that never fires.
    assert!(matches!(
        super::credit_backing(1_000, 999, 0),
        super::CreditBacking::UnattributedButPayable { .. }
    ));
    assert!(matches!(
        super::credit_backing(1_000, 5_000, 2_000),
        super::CreditBacking::UnattributedButPayable { .. }
    ));
}
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

    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
    let settlement = rx.recv().await.expect("the first poll delivers the payment");
    assert_eq!(settlement.invoice_id, inv.id);
    assert_eq!(settlement.received_msat, 2_723_000);
    let polls_after_delivery = ops.incoming_calls().len();

    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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

    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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

// An outgoing record that exists but is not paid is an UNVERIFIED phoenixd state: never retried
// blindly (fact 3), never marked terminal.
// The other half of the measured truth table. An unpaid record that DOES carry `completedAt` means
// the payment failed on the release this was measured on — but lnrent still refuses to act on it,
// because `outgoingbyhash` returns one record per hash and it cannot be proven to be THIS attempt.
// Same outcome as the in-flight shape, different operator message: conflating the two hides whether
// a refund is stuck mid-flight or stuck on an unattributable failure.
// The receipt-less path needs the same split as recovery: it is the FIRST error an operator sees
// after a POST that came back without a receipt, and calling a record that already terminated "in
// flight" is simply false. Outcome is identical for both shapes (Pending, no second payinvoice);
// only the text differs.
#[tokio::test]
async fn a_receipt_less_post_reports_terminal_and_in_flight_records_differently() {
    for (label, completed_at_ms, expect, reject) in [
        ("in flight", None, "still in flight", "DOES carry a completion time"),
        (
            "terminal",
            Some(MEASURED_COMPLETED_AT_MS),
            "DOES carry a completion time",
            "still in flight",
        ),
    ] {
        let ops = FakePhoenixdOps::new();
        let be = backend(ops.clone(), TestClock::new(1_000));
        let bolt11 = mint_bolt11(120_000, 41);
        let hash = hash_of(&bolt11);
        let key = "refund:order:41:g1";

        ops.set_outgoing_for(
            &hash,
            PhoenixdOutgoing {
                payment_id: "pay-receiptless".into(),
                payment_hash: hash.clone(),
                is_paid: false,
                fees_msat: 0,
                completed_at_ms,
            },
        );
        ops.script_pay(Ok(PayAttempt::NoReceipt {
            reason: Some("payment failed".into()),
        }));

        let err = be
            .pay_refund_capped(&bolt11, 120, 130, key)
            .await
            .expect_err("a receipt-less POST is never a success");
        assert_eq!(
            be.payment_status_by_key(key).await.unwrap(),
            PayStatus::Pending,
            "{label}: both shapes stay Pending"
        );
        assert_eq!(ops.pay_calls().len(), 1, "{label}: no second payinvoice");
        let rendered = format!("{err:#}");
        assert!(rendered.contains(expect), "{label}: missing its own wording: {rendered}");
        assert!(
            !rendered.contains(reject),
            "{label}: must not borrow the other shape's wording: {rendered}"
        );
    }
}

#[tokio::test]
async fn a_terminal_outgoing_record_stays_pending_but_reads_differently() {
    let ops = FakePhoenixdOps::new();
    let be = backend(ops.clone(), TestClock::new(1_000));
    let bolt11 = mint_bolt11(120_000, 7);
    let hash = hash_of(&bolt11);

    ops.script_pay(Err("timeout".into()));
    be.pay_refund_capped(&bolt11, 120, 130, "refund:order:7:g1")
        .await
        .expect_err("ambiguous");
    ops.set_outgoing(PhoenixdOutgoing {
        payment_id: "pay-terminally-failed".into(),
        payment_hash: hash,
        is_paid: false,
        fees_msat: 0,
        completed_at_ms: Some(1_785_101_384_178),
    });

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:7:g1")
        .await
        .expect_err("an unattributable terminal record is not a green light either");
    assert_eq!(
        be.payment_status_by_key("refund:order:7:g1").await.unwrap(),
        PayStatus::Pending,
        "a terminal marker must NOT resolve the key while it cannot be attributed"
    );
    assert_eq!(ops.pay_calls().len(), 1, "and it must never unlock a second payinvoice");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("DOES carry a completion time"),
        "the terminal shape must be reported as such: {rendered}"
    );
    assert!(
        !rendered.contains("still IN FLIGHT"),
        "and must not be reported as in flight: {rendered}"
    );
}

#[tokio::test]
async fn recovery_never_retries_an_unpaid_outgoing_record() {
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
        completed_at_ms: None,
    });

    let err = be
        .pay_refund_capped(&bolt11, 120, 130, "refund:order:6:g1")
        .await
        .expect_err("an unpaid record is ambiguous, not a green light");
    // The in-flight shape must SAY in flight: this is the message an operator reads off a stuck
    // refund, and calling it a failed-attribution would state something false about the record.
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("still IN FLIGHT"),
        "the absent-completedAt shape must be reported as in flight: {rendered}"
    );
    assert!(
        !rendered.contains("DOES carry a completion time"),
        "it must not borrow the terminal shape's wording: {rendered}"
    );
    assert_eq!(
        ops.pay_calls().len(),
        1,
        "and NO second payinvoice is issued"
    );
    assert_eq!(
        be.payment_status_by_key("refund:order:6:g1").await.unwrap(),
        PayStatus::Pending
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
// our FAILED rows provably never POSTed, so a retry re-runs the preflight and can now succeed (e.g.
// after the receipt grew enough to cover the reserve).
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

    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
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

fn backend_with_alerts(
    ops: Arc<FakePhoenixdOps>,
    clock: Arc<TestClock>,
) -> (PhoenixdPayment, crate::store::Store) {
    let state = Connection::open_in_memory().expect("in-memory state db");
    state.execute_batch(crate::store::SCHEMA).expect("state schema");
    let store = crate::store::Store::spawn(state);
    let clock: Arc<dyn Clock> = clock;
    let alerts = Arc::new(crate::alerts::AlertDispatcher::new(
        store.clone(),
        clock.clone(),
        "op-npub-hex".into(),
    ));
    let index = Connection::open_in_memory().expect("in-memory index");
    index.execute_batch(INDEX_SCHEMA).expect("index schema");
    let be = PhoenixdPayment::with_ops(ops, index, clock, FeeSchedule::default())
        .with_alerts(alerts);
    (be, store)
}

/// Same wiring as [`backend_with_alerts`], except the timing table is DROPPED after the schema
/// runs — so `idx_record_fee_credit_refusal` fails for real (no such table) instead of through a
/// seam that could drift from the production error path. Models a read-only or corrupt index DB
/// whose outbox is still writable.
fn backend_with_alerts_and_no_timing_table(
    ops: Arc<FakePhoenixdOps>,
    clock: Arc<TestClock>,
) -> (PhoenixdPayment, crate::store::Store) {
    let state = Connection::open_in_memory().expect("in-memory state db");
    state.execute_batch(crate::store::SCHEMA).expect("state schema");
    let store = crate::store::Store::spawn(state);
    let clock: Arc<dyn Clock> = clock;
    let alerts = Arc::new(crate::alerts::AlertDispatcher::new(
        store.clone(),
        clock.clone(),
        "op-npub-hex".into(),
    ));
    let index = Connection::open_in_memory().expect("in-memory index");
    index.execute_batch(INDEX_SCHEMA).expect("index schema");
    index
        .execute_batch("DROP TABLE phoenixd_unbookable_settlement;")
        .expect("drop the timing table");
    let be =
        PhoenixdPayment::with_ops(ops, index, clock, FeeSchedule::default()).with_alerts(alerts);
    (be, store)
}

async fn operator_alerts(store: &crate::store::Store) -> Vec<lnrent_wire::OperatorAlert> {
    let payloads: Vec<String> = store
        .read(|c| {
            let mut stmt = c.prepare(
                "SELECT payload_json FROM outbox WHERE msg_type='operator.alert' ORDER BY id",
            )?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .expect("read the alert rows");
    payloads
        .into_iter()
        .map(|p| {
            match serde_json::from_str::<lnrent_wire::Msg>(&p).expect("a serialized Msg") {
                lnrent_wire::Msg::OperatorAlert(a) => a,
                other => panic!("expected an operator alert, got {other:?}"),
            }
        })
        .collect()
}

/// The settle instant of the live-measured receive, in seconds. It is only a stable test-clock
/// baseline now: the alert age deliberately starts at lnrent's first local refusal instead.
fn measured_receive_settled_at() -> i64 {
    epoch_secs_from_ms(live_measured_receive("h").completed_at_ms)
        .expect("the measured record carries a completedAt")
}

/// Two ages that pin [`UNBOOKABLE_SETTLEMENT_ALERT_S`] from BOTH sides, and are deliberately NOT
/// derived from it. A test clock offset by the constant itself is self-cancelling: it moves with the
/// constant, so the boundary pair below stays green whether the threshold is 0 or 30 days — which is
/// exactly how the round-2 version passed, re-proved by mutating the constant to `0`.
///
/// - [`STILL_THE_RETRY_S`]: the first refusal was a minute ago and lnrent's own settlement poll
///   (`SETTLEMENT_POLL_INTERVAL`) has re-checked it a dozen times. Funding the wallet books it with
///   no human involved, so this is not yet the operator's problem — no DM.
/// - [`THE_OPERATORS_PROBLEM_S`]: it has stood for an hour. Automatic recovery has demonstrably not
///   happened, the buyer's money is sitting in an order that is not progressing, and the whole point
///   of this bead is that the operator's phone does not stay silent through that — DM.
const STILL_THE_RETRY_S: i64 = 12 * SETTLEMENT_POLL_INTERVAL.as_secs() as i64;
const THE_OPERATORS_PROBLEM_S: i64 = 60 * 60;

/// Arrange the live-measured fee-credit refusal — 2_723 sat received into a wallet holding 3_000 sat
/// of fee credit and 2_722 spendable — and return the invoice.
async fn arrange_fee_credit_refusal(be: &PhoenixdPayment, ops: &Arc<FakePhoenixdOps>) -> Invoice {
    let inv = be
        .create_invoice(25_000, "memo", 600, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    ops.set_balance(2_722, 3_000);
    inv
}

#[tokio::test]
async fn a_fee_credit_refusal_alerts_the_operator_with_its_reason_and_remedy() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(measured_receive_settled_at()));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;

    be.received_amount_msat(&inv.id)
        .await
        .expect_err("the first local refusal starts the threshold");
    assert!(operator_alerts(&store).await.is_empty());
    // A refusal that has stood an HOUR. Fixed rather than derived from the threshold, so this
    // forbids the threshold from growing past an hour; the exact edge is pinned below.
    clock.advance(THE_OPERATORS_PROBLEM_S);
    be.received_amount_msat(&inv.id)
        .await
        .expect_err("the refusal itself is unchanged: an unbacked receipt is still not booked");

    let alerts = operator_alerts(&store).await;
    assert_eq!(alerts.len(), 1, "one durable operator DM: {alerts:?}");
    assert_eq!(alerts[0].kind, "settlement_unbookable");
    assert_eq!(alerts[0].subject, "fee_credit");
    let detail = &alerts[0].detail;
    assert!(
        detail.contains("FEE-CREDIT REFUSAL")
            && detail.contains("2723 sat received")
            && detail.contains(&inv.id)
            && detail.contains("REMEDY")
            && detail.contains("SPENDABLE balance")
            && !detail.contains("INDEX DIVERGENCE"),
        "fee-credit reason and remedy must be self-contained: {detail}"
    );
    // The REMEDY figure is the SHORTFALL, not the receipt. The refusal is
    // `credit >= received && balance < received`, so it lifts the moment spendable reaches the
    // receipt: here the wallet already holds 2722 against a 2723-sat receipt, so ONE sat clears it.
    // Printing the receipt instead would tell a stranger operator to send 2723 — overfunding by
    // nearly the whole amount on the very case this alert exists for (an unfunded first sale).
    assert!(
        detail.contains("1 sat more clears THIS one"),
        "the remedy must name the shortfall (2723 received - 2722 spendable = 1): {detail}"
    );
    assert!(
        !detail.contains("2723 sat more"),
        "and must NOT ask for the whole receipt when most of it is already held: {detail}"
    );
}

// A `getbalance` outage is EXCLUDED from this alert on purpose: the remedy for a node that will not
// answer is not "fund the wallet", and the exclusion is by typed downcast so the decision can never
// drift with an error string. Untested, it is a comment: rewriting the arm to alert on any
// `spendable_credit_msat` error keeps the suite green. The cost is not just a wrong DM — the subject
// is the shared `fee_credit` constant, so one mislabeled outage alert takes that subject's cooldown
// and SUPPRESSES the next genuine refusal, turning a reporting bug into the silence this bead exists
// to end.
#[tokio::test]
async fn a_getbalance_outage_does_not_masquerade_as_a_fee_credit_refusal() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(measured_receive_settled_at()));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;
    // Same settlement, same instant — the ONLY change is that the balance read now fails.
    ops.fail_balance_with_status(503);

    be.received_amount_msat(&inv.id)
        .await
        .expect_err("an unanswered getbalance still fails closed");

    assert!(
        operator_alerts(&store).await.is_empty(),
        "a node outage must not DM the fee-credit remedy, nor burn that subject's cooldown"
    );
}

// The threshold is measured from a row in the index. If that row cannot be WRITTEN, the age is
// unmeasurable — and an unmeasurable age must not be read as "too fresh to mention". This is the
// bead's own failure mode wearing a different hat: a read-only or corrupt index DB would otherwise
// suppress every DM forever while the outbox stayed perfectly writable, which is exactly the silence
// gc7 exists to end. Contrast `a_fresh_fee_credit_refusal_does_not_alert_before_the_threshold`: same
// instant, same refusal, no clock advance — the ONLY difference is whether the timing row can be
// persisted, so what this pins is provably the fallback and nothing else.
#[tokio::test]
async fn a_settlement_alert_still_lands_when_its_timing_row_cannot_be_persisted() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(measured_receive_settled_at()));
    let (be, store) = backend_with_alerts_and_no_timing_table(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;

    be.received_amount_msat(&inv.id)
        .await
        .expect_err("the refusal itself is unchanged: an unbacked receipt is still not booked");

    // No clock advance, deliberately. With a healthy index this instant is silent.
    let alerts = operator_alerts(&store).await;
    assert_eq!(
        alerts.len(),
        1,
        "an unpersistable threshold must speak immediately, not fall silent: {alerts:?}"
    );
    assert_eq!(alerts[0].kind, "settlement_unbookable");
    assert_eq!(alerts[0].subject, "fee_credit");
    assert!(
        alerts[0].detail.contains("REMEDY"),
        "and it must still carry the remedy an operator acts on: {}",
        alerts[0].detail
    );
}

// The other half of the age gate. Without this the threshold is decorative: every refusal would DM
// the operator the instant the payment lands, including the ordinary one lnrent books itself on the
// next retry seconds later. Four instants, one refusal, nothing changing between them but the
// clock — so what moves the alert is provably the AGE and nothing else.
// The threshold must not outlive the LAST observer. A late payment — landed after the local invoice
// expired — is watched only by the settlement poll, which retires the row past
// `expires_at + SETTLEMENT_POLL_GRACE_SECS` (catch-up scans `status='OPEN'` only). First observed
// inside the last threshold of that window, a plain age gate would defer, the row would retire, and
// the operator would never be told: the exact permanent silence this bead exists to end.
//
// Two clocks either side of that boundary, one refusal, nothing else different. Deleting the
// last-look arm fails the second half; hard-coding it true fails the first.
#[tokio::test]
async fn a_refusal_the_poll_is_about_to_retire_alerts_without_waiting_out_the_threshold() {
    for past_the_boundary in [false, true] {
        let ops = FakePhoenixdOps::new();
        let clock = Arc::new(TestClock::new(measured_receive_settled_at() - 600));
        let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
        let inv = arrange_fee_credit_refusal(&be, &ops).await;

        // The instant the poll retires this row...
        let retires_at = inv.expires_at + SETTLEMENT_POLL_GRACE_SECS;
        // ...and the last instant at which a whole threshold still fits before it.
        clock.set(retires_at - UNBOOKABLE_SETTLEMENT_ALERT_S + i64::from(past_the_boundary));

        // FIRST local sighting in both halves: age 0, so the gate alone decides.
        be.received_amount_msat(&inv.id)
            .await
            .expect_err("the refusal itself is unchanged either side of the boundary");

        let alerts = operator_alerts(&store).await;
        if past_the_boundary {
            assert_eq!(
                alerts.len(),
                1,
                "past the boundary the threshold would outlive the poll, so it must speak NOW"
            );
            assert!(
                alerts[0].detail.contains("FEE-CREDIT REFUSAL"),
                "and it is the fee-credit reason: {:?}",
                alerts[0]
            );
            // The remedy is caller-independent: it must NOT claim re-checking has stopped, which is
            // false whenever catch-up is still watching.
            assert!(
                !alerts[0].detail.contains("stops re-checking"),
                "the trigger must not resurrect the refuted 'lnrent stopped looking' claim: {:?}",
                alerts[0]
            );
        } else {
            assert!(
                alerts.is_empty(),
                "a whole threshold still fits before {retires_at}, so the retry gets it first: \
                 {alerts:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_fresh_fee_credit_refusal_does_not_alert_before_the_threshold() {
    let ops = FakePhoenixdOps::new();
    let first_refusal_at = measured_receive_settled_at();
    let clock = Arc::new(TestClock::new(first_refusal_at));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;

    // 1. First local observation: the durable threshold begins here.
    be.received_amount_msat(&inv.id)
        .await
        .expect_err("the first local refusal starts the threshold");
    assert!(operator_alerts(&store).await.is_empty());

    // 2. A minute old. Fixed, NOT derived from the threshold — this is what forbids the threshold
    //    from shrinking to zero and DMing the operator about every ordinary fee-credit receipt.
    clock.set(first_refusal_at + STILL_THE_RETRY_S);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert!(
        operator_alerts(&store).await.is_empty(),
        "a refusal {STILL_THE_RETRY_S}s old is still the automatic retry's, not the operator's"
    );

    // 3. One second short of the threshold: the edge itself, still silent...
    clock.set(first_refusal_at + UNBOOKABLE_SETTLEMENT_ALERT_S - 1);
    be.received_amount_msat(&inv.id)
        .await
        .expect_err("the refusal is the same one second early");
    assert!(
        operator_alerts(&store).await.is_empty(),
        "a receipt that has not yet stood {UNBOOKABLE_SETTLEMENT_ALERT_S}s must not DM the operator"
    );

    // 4. ...and one second later — the SAME refusal — it does.
    clock.set(first_refusal_at + UNBOOKABLE_SETTLEMENT_ALERT_S);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert_eq!(
        operator_alerts(&store).await.len(),
        1,
        "the threshold, not the refusal, is what held the alert back"
    );
}

#[tokio::test]
async fn a_remote_phoenixd_clock_ahead_cannot_silence_the_fee_credit_alert() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(
        measured_receive_settled_at() - 24 * 60 * 60,
    ));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;

    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert!(
        operator_alerts(&store).await.is_empty(),
        "the first local refusal starts the threshold"
    );
    clock.advance(UNBOOKABLE_SETTLEMENT_ALERT_S);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert_eq!(
        operator_alerts(&store).await.len(),
        1,
        "remote completedAt ahead of lnrent cannot suppress a locally old refusal"
    );
}

#[tokio::test]
async fn a_remote_phoenixd_clock_behind_cannot_make_a_fresh_fee_credit_refusal_alert() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(
        measured_receive_settled_at() + 24 * 60 * 60,
    ));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    let inv = arrange_fee_credit_refusal(&be, &ops).await;

    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert!(
        operator_alerts(&store).await.is_empty(),
        "a remote completedAt behind lnrent cannot bypass the local threshold"
    );
    clock.advance(UNBOOKABLE_SETTLEMENT_ALERT_S);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    assert_eq!(operator_alerts(&store).await.len(), 1, "the local threshold fires");
}

// The catch-up seam above is not the only observer. Once the local invoice EXPIRES, settlement
// catch-up drops it (`status='OPEN'` only) and this poll — whose grace window exists to catch a LATE
// payment — is all that is left. Before lnrent-gc7 this path only logged, so a late payment refused
// by fee credit was silent forever.
#[tokio::test]
async fn the_settlement_poll_alerts_a_fee_credit_refusal_it_alone_observes() {
    let ops = FakePhoenixdOps::new();
    let settled_at = measured_receive_settled_at();
    let clock = Arc::new(TestClock::new(settled_at - 600));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());
    // A 10s window, paid late: lnrent's own expires_at is long past by the time the poll runs.
    let inv = be
        .create_invoice(25_000, "memo", 10, "ext:1")
        .await
        .unwrap();
    ops.set_incoming("ext:1", vec![live_measured_receive(&inv.payment_hash)]);
    ops.set_balance(2_722, 3_000);
    clock.set(settled_at + 600);
    assert!(
        clock.now() > inv.expires_at,
        "the scenario requires lnrent's local window to have lapsed"
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let ops_dyn: Arc<dyn PhoenixdOps> = ops.clone();
    let clock_dyn: Arc<dyn Clock> = clock.clone();
    let mut retired: HashSet<String> = HashSet::new();
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );
    assert!(operator_alerts(&store).await.is_empty(), "first refusal starts the threshold");
    clock.advance(UNBOOKABLE_SETTLEMENT_ALERT_S);
    assert!(
        poll_settlements_once(&ops_dyn, &be.index, &clock_dyn, &be.alerts, &tx, &mut retired).await
    );

    assert!(
        rx.try_recv().is_err(),
        "the refusal is unchanged: the poll still emits no settlement"
    );
    assert!(
        !retired.contains(&inv.id),
        "and still re-checks the row, so funding the wallet books it"
    );
    let alerts = operator_alerts(&store).await;
    assert_eq!(alerts.len(), 1, "the poll must report it too: {alerts:?}");
    assert_eq!(alerts[0].subject, "fee_credit");
    assert!(
        alerts[0].detail.contains("FEE-CREDIT REFUSAL")
            && alerts[0].detail.contains("REMEDY")
            && alerts[0].detail.contains(&inv.id),
        "same reason and remedy from either observer: {}",
        alerts[0].detail
    );
}

#[tokio::test]
async fn two_receipts_one_unfunded_wallet_is_one_alert_not_one_per_receipt() {
    let ops = FakePhoenixdOps::new();
    let settled_at = measured_receive_settled_at();
    let clock = Arc::new(TestClock::new(settled_at + THE_OPERATORS_PROBLEM_S));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());

    let first = arrange_fee_credit_refusal(&be, &ops).await;
    let second = be
        .create_invoice(25_000, "memo", 600, "ext:2")
        .await
        .unwrap();
    ops.set_incoming("ext:2", vec![live_measured_receive(&second.payment_hash)]);
    assert_ne!(first.id, second.id, "two distinct held-back receipts");

    be.received_amount_msat(&first.id).await.unwrap_err();
    be.received_amount_msat(&second.id).await.unwrap_err();
    assert!(operator_alerts(&store).await.is_empty());
    clock.advance(THE_OPERATORS_PROBLEM_S);
    be.received_amount_msat(&first.id).await.unwrap_err();
    be.received_amount_msat(&second.id).await.unwrap_err();

    let alerts = operator_alerts(&store).await;
    assert_eq!(
        alerts.len(),
        1,
        "one wallet-level condition is one DM, however many receipts it holds back: {alerts:?}"
    );
    assert_eq!(alerts[0].subject, "fee_credit");
    assert!(
        alerts[0].detail.contains("WALLET-level")
            && alerts[0].detail.contains("EVERY receipt held back"),
        "and it must SAY it covers the others, or the operator fixes one receipt: {}",
        alerts[0].detail
    );
}

// Both details are written right up against `MAX_ALERT_DETAIL_CHARS` (1024), and the cap truncates
// from the TAIL — which is where each remedy's final instruction lives ("do not recreate or expire
// the invoice"; "the orders do not progress until you do"). A real invoice id is 73 chars
// (`phoenixd-` + a 64-hex payment hash), not the short one the other tests use, so measure with one.
#[tokio::test]
async fn a_full_length_unbookable_detail_is_not_truncated() {
    let ops = FakePhoenixdOps::new();
    let settled_at = measured_receive_settled_at();
    let clock = Arc::new(TestClock::new(settled_at + THE_OPERATORS_PROBLEM_S));
    let (be, store) = backend_with_alerts(ops.clone(), clock.clone());

    let inv = arrange_fee_credit_refusal(&be, &ops).await;
    assert_eq!(inv.id.chars().count(), 73, "a real invoice id: {}", inv.id);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    clock.advance(THE_OPERATORS_PROBLEM_S);
    be.received_amount_msat(&inv.id).await.unwrap_err();
    // A same-shape id the index has never seen — the divergence arm, with a full-length id too.
    be.lookup_settlement(&format!("phoenixd-{}", "f".repeat(64)))
        .await
        .unwrap_err();

    let alerts = operator_alerts(&store).await;
    assert_eq!(alerts.len(), 2, "one of each reason: {alerts:?}");
    for a in &alerts {
        assert!(
            !a.detail.ends_with('…'),
            "{} was truncated at {} chars; shorten it or the remedy loses its tail: {}",
            a.subject,
            a.detail.chars().count(),
            a.detail
        );
    }
}

#[tokio::test]
async fn an_index_divergence_alerts_with_a_distinct_reason_and_remedy() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(1_000));
    let (be, store) = backend_with_alerts(ops, clock);

    be.lookup_settlement("phoenixd-orphan").await.unwrap_err();

    let alerts = operator_alerts(&store).await;
    assert_eq!(alerts.len(), 1, "one durable operator DM: {alerts:?}");
    assert_eq!(alerts[0].kind, "settlement_unbookable");
    assert_eq!(alerts[0].subject, "index_diverged");
    let detail = &alerts[0].detail;
    assert!(
        detail.contains("INDEX DIVERGENCE")
            && detail.contains("phoenixd-orphan")
            && detail.contains("UNKNOWN")
            // This DM must NOT carry a restore command, and must not imply a safe restore
            // EXISTS. Deciding a backup is safe needs to know which refunds already paid, and
            // lnrent's only record of that is `phoenixd_pay` in the index whose loss IS the
            // incident. Three schemes for proving it were refuted on lnrent-ole, every one a
            // double pay. So the ABSENCE of a command is asserted, not merely the pointer.
            && !detail.contains("lnrentd restore")
            && detail.contains("docs/go-live.md")
            && detail.contains("pay a refund a SECOND")
            && detail.contains("original wallet")
            && !detail.contains("FEE-CREDIT REFUSAL"),
        "index reason and shipped recovery remedy must be self-contained: {detail}"
    );
    // "Just restore from last night" is the reflex this text exists to stop, and it must be
    // stopped OUTRIGHT rather than deferred to a checklist: the runbook no longer has one. A DM
    // that merely said "don't restore from this message alone" would still read as "there is a
    // procedure, go find it".
    assert!(
        detail.contains("do NOT \
             restore from a backup"),
        "the alert must forbid restoring outright, not defer it to a checklist: {detail}"
    );
    // The no-safe-backup branch must give an instruction the operator can actually CARRY OUT.
    // "reconcile by hand" was not one: `lnrent reconcile` is report-only, nothing reconstructs the
    // missing rows, and writing the DB by hand is forbidden (sole sqlite writer, ADR-0001). Naming a
    // procedure that does not exist is worse than naming none — it reads as a supported path.
    // ORDER, not just presence. Both reviewers found the same defect independently: the DM used to
    // say "stop the daemon, then ... stop new orders (`lnrent listing withdraw`)", and withdraw
    // talks to the daemon over its socket — so followed literally, the one step that halts new
    // orders could not run, and every order taken meanwhile is another buyer to settle by hand.
    // Asserting only that the command APPEARS would have passed on the broken version.
    {
        let withdraw = detail.find("listing withdraw").expect("names the withdraw verb");
        let stop_daemon = detail.find("stop the daemon").expect("names stopping the daemon");
        assert!(
            withdraw < stop_daemon,
            "withdraw must come BEFORE stopping the daemon — it needs the socket: {detail}"
        );
        assert!(
            detail.contains("IN THIS ORDER"),
            "and the remedy must flag that its order is load-bearing: {detail}"
        );
    }
    // Saying "no repair" is only half an instruction. The operator is mid-incident with a buyer's
    // money unbooked, so the DM must also name what they CAN do — and every verb here is real:
    // `lnrent listing withdraw` exists, and phoenixd's own records are what settlement reads.
    assert!(
        detail.contains("NO safe repair and NO repair command")
            && detail.contains("lnrent \
             listing withdraw")
            && detail.contains("settle the affected buyers out of band"),
        "and must give an ACTIONABLE next step, not just a refusal: {detail}"
    );
    // The subject is GLOBAL and the cooldown dedups, so this alert names ONE affected invoice
    // however many there are. An operator who verifies a candidate backup against the named id
    // alone can pick one that omits the others, and the whole-dir restore then drops those orders.
    // The text must say so and give the rule for finding the rest.
    assert!(
        detail.contains("ONE EXAMPLE, not the set")
            && detail.contains("every OPEN invoice your state DB has"),
        "the alert must say the named invoice is one example and how to enumerate the rest: {detail}"
    );
    // WHY a restore is unsafe, not just that it is. Without the mechanism an operator reads the
    // refusal as excessive caution and restores anyway: the rollback drops lnrent's record of which
    // refunds already paid while phoenixd keeps that history, so the second pay is not a risk but
    // the expected outcome of re-driving a restored PENDING refund.
    assert!(
        detail.contains("rolls back lnrent's only record of which refunds already \
             paid")
            && detail.contains("while phoenixd keeps that history"),
        "and must give the MECHANISM, so the refusal does not read as mere caution: {detail}"
    );
}

// One lost `phoenixd_index.db` diverges EVERY open invoice, and catch-up re-observes each one every
// tick. Keying the cooldown per invoice would put N copies of one identical remedy into the outbox
// that also carries provision.ready/billing.* — every window, forever, since outbox rows are never
// reaped. The condition is global, so the subject is.
#[tokio::test]
async fn a_whole_diverged_index_is_one_alert_not_one_per_invoice() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(1_000));
    let (be, store) = backend_with_alerts(ops, clock);

    for id in ["phoenixd-orphan-a", "phoenixd-orphan-b", "phoenixd-orphan-c"] {
        be.lookup_settlement(id).await.unwrap_err();
        be.received_amount_msat(id).await.unwrap_err();
    }

    let alerts = operator_alerts(&store).await;
    assert_eq!(
        alerts.len(),
        1,
        "six sightings of ONE divergence are one DM: {alerts:?}"
    );
    assert_eq!(alerts[0].subject, "index_diverged");
}

#[tokio::test]
async fn the_same_unbookable_settlement_alerts_once_per_cooldown_window() {
    let ops = FakePhoenixdOps::new();
    let clock = Arc::new(TestClock::new(1_000));
    let (be, store) = backend_with_alerts(ops, clock.clone());

    be.lookup_settlement("phoenixd-orphan").await.unwrap_err();
    clock.set(1_000 + crate::alerts::ALERT_COOLDOWN_S - 1);
    be.lookup_settlement("phoenixd-orphan").await.unwrap_err();
    assert_eq!(operator_alerts(&store).await.len(), 1, "inside cooldown");

    clock.set(1_000 + crate::alerts::ALERT_COOLDOWN_S);
    be.lookup_settlement("phoenixd-orphan").await.unwrap_err();
    assert_eq!(operator_alerts(&store).await.len(), 2, "after cooldown");
}
