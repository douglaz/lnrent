//! Subsystem backends. SPEC.md §8. v1 implements Compute (host) + Network
//! (WireGuard) and Payment (Fedimint, ADR-0012); these are M0 stubs that compile and fail
//! loudly until M1 fills them in.

use anyhow::{bail, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tokio::sync::mpsc;

use async_trait::async_trait;

/// Where a workload runs. SPEC.md §8.1 (was `ProvisionBackend` in early drafts).
pub trait ComputeBackend: Send + Sync {
    /// Create a container/VM; returns the Instance handle to record.
    fn create(&self, spec: &Value) -> Result<Value>;
    fn stop(&self, handle: &Value) -> Result<()>;
    fn start(&self, handle: &Value) -> Result<()>;
    fn destroy(&self, handle: &Value) -> Result<()>;
    fn exec(&self, handle: &Value, cmd: &[String]) -> Result<String>;
}

/// Network management. SPEC.md §8.2.
pub trait NetworkBackend: Send + Sync {
    fn add_wireguard_peer(&self, spec: &Value) -> Result<Value>;
    fn remove_wireguard_peer(&self, peer: &str) -> Result<()>;
    fn open_port(&self, spec: &Value) -> Result<Value>;
    fn close_port(&self, handle: &Value) -> Result<()>;
}

/// Receiving and refunding Lightning. SPEC.md §6.1. No hold invoices on the v1
/// backends, so `pay` exists for capture-then-refund (ADR-0003).
///
/// Async because the real backend (Fedimint, lnrent-7fp.4) is async to the core
/// (`fedimint-client`); the in-memory `MockPayment` satisfies it trivially. Every call site is
/// already inside an `async fn`, so it just `.await`s here (no runtime-nesting bridge).
#[async_trait]
pub trait PaymentBackend: Send + Sync {
    /// Create (or return the existing) invoice. **Idempotent on `external_id`**: a repeated
    /// call with the same `external_id` MUST return the same invoice, not a duplicate — so a
    /// retry after a crash regenerates the same `external_id` and reuses the invoice (§6.6).
    async fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str, // binds settlement -> order (ADR-0009); deterministic per invoice class (§6.6)
    ) -> Result<Invoice>;
    async fn lookup(&self, id: &str) -> Result<PaymentStatus>;
    /// Invoice status PLUS the backend's observed-LIVE settled_at. `Some(ts)` is returned ONLY for a
    /// settlement the backend observed live (its true time is known); `None` for a not-paid invoice
    /// OR a RECOVERY settlement (settled while the daemon was down, so the true time is unknown). The
    /// supervisor's settlement catch-up uses `Some(ts)` EXACTLY and caps only on `None` — so a late
    /// LIVE payment refunds (capture's g5p gate) instead of being stamped just-in-window and wrongly
    /// provisioned (lnrent-zwk). `lookup()` stays the status-only seam reconcile uses (unchanged).
    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)>;
    /// Outbound payment, used for refunds. **Idempotent on `idempotency_key`**: calling twice
    /// with the same key never pays twice (ADR-0009, SPEC §6.6). Returns a backend payment id.
    async fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String>;
    /// Status of an outbound payment by its backend id (ADR-0009 refund ledger).
    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus>;
    /// Check an in-flight refund by its idempotency key after a crash (SPEC §6.6). An
    /// optimization only — retrying `pay(key)` is always safe (the key dedups).
    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus>;
    /// Stream of settled payments (push). `Settlement.external_id` carries the order id
    /// (SPEC §6.1). M1a wires this to the Fedimint client settlement stream.
    async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>>;
}

#[derive(Debug, Clone)]
pub struct Invoice {
    pub id: String,                 // our local invoice id (§11 invoice.id)
    pub external_id: String,        // unique per-invoice token binding settlement->order (ADR-0009)
    pub backend_invoice_id: String, // the backend's own invoice id (§11 invoice.backend_invoice_id)
    pub payment_hash: String,       // bolt11 payment hash (§11 invoice.payment_hash)
    pub bolt11: String,
    pub amount_sat: u64,
    pub expires_at: i64, // bolt11 expiry (unix secs); the order reservation is released at this (§9.3)
}

/// Status of one of OUR inbound invoices (receiving). SPEC.md §6.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentStatus {
    Open,
    Paid,
    Expired,
}

/// Status of an OUTBOUND payment (a refund), distinct from invoice status (§6.6). `Unknown`
/// is the honest answer when an in-flight refund can be neither confirmed nor refuted;
/// recovery retries `pay(key)` regardless (the key dedups), so `Unknown` never strands a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayStatus {
    Unknown,
    Pending,
    Succeeded,
    Failed,
}

/// A settled incoming payment for one of OUR invoices. The backend filters out any
/// non-lnrent payments, so `external_id` is always the invoice's correlation token
/// (ADR-0009); capture binds it to the order.
#[derive(Debug, Clone)]
pub struct Settlement {
    pub invoice_id: String,
    pub external_id: String,
    pub amount_sat: u64,
    pub settled_at: i64, // when the backend observed settlement (unix secs); capture sets
                         // paid_through = settled_at + period (§6.3), so it must come from here
}

/// `host` compute: runs directly on the Box, no isolation. SPEC.md §8.1.
pub struct HostCompute;

impl ComputeBackend for HostCompute {
    fn create(&self, _spec: &Value) -> Result<Value> {
        bail!("host.create not implemented (M0 stub)")
    }
    fn stop(&self, _handle: &Value) -> Result<()> {
        bail!("host.stop not implemented (M0 stub)")
    }
    fn start(&self, _handle: &Value) -> Result<()> {
        bail!("host.start not implemented (M0 stub)")
    }
    fn destroy(&self, _handle: &Value) -> Result<()> {
        bail!("host.destroy not implemented (M0 stub)")
    }
    fn exec(&self, _handle: &Value, _cmd: &[String]) -> Result<String> {
        bail!("host.exec not implemented (M0 stub)")
    }
}

/// WireGuard network backend. SPEC.md §8.2.
pub struct WireguardNetwork;

impl NetworkBackend for WireguardNetwork {
    fn add_wireguard_peer(&self, _spec: &Value) -> Result<Value> {
        bail!("wireguard.add_peer not implemented (M0 stub)")
    }
    fn remove_wireguard_peer(&self, _peer: &str) -> Result<()> {
        bail!("wireguard.remove_peer not implemented (M0 stub)")
    }
    fn open_port(&self, _spec: &Value) -> Result<Value> {
        bail!("network.open_port not implemented (M0 stub)")
    }
    fn close_port(&self, _handle: &Value) -> Result<()> {
        bail!("network.close_port not implemented (M0 stub)")
    }
}

/// Fedimint payment backend (PRIMARY, ADR-0012): ecash via an existing federation +
/// gateway. Cannot hold invoices (ADR-0003). phoenixd is a secondary backend (M3).
pub struct FedimintPayment;

#[async_trait]
impl PaymentBackend for FedimintPayment {
    async fn create_invoice(
        &self,
        _amount_sat: u64,
        _memo: &str,
        _expiry_s: u32,
        _external_id: &str,
    ) -> Result<Invoice> {
        bail!("fedimint.create_invoice not implemented (M0 stub)")
    }
    async fn lookup(&self, _id: &str) -> Result<PaymentStatus> {
        bail!("fedimint.lookup not implemented (M0 stub)")
    }
    async fn lookup_settlement(&self, _id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        bail!("fedimint.lookup_settlement not implemented (M0 stub)")
    }
    async fn pay(&self, _dest: &str, _amount_sat: u64, _idempotency_key: &str) -> Result<String> {
        bail!("fedimint.pay not implemented (M0 stub)")
    }
    async fn payment_status(&self, _payment_id: &str) -> Result<PayStatus> {
        bail!("fedimint.payment_status not implemented (M0 stub)")
    }
    async fn payment_status_by_key(&self, _idempotency_key: &str) -> Result<PayStatus> {
        bail!("fedimint.payment_status_by_key not implemented (M0 stub)")
    }
    async fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
        bail!("fedimint.watch not implemented (M0 stub)")
    }
}

/// Deterministic in-memory PaymentBackend — the M1a money-path FIXTURE (the operator chose a
/// mock over a live federation for now). It issues fake bolt11s (idempotent on `external_id`),
/// treats every refund `pay` as an immediate success (idempotent on the key), and lets a
/// driver/test push settlements via `settle()` (also delivered on the `watch()` stream). This is
/// NOT the real Fedimint backend (lnrent-7fp.4) — that stays a deferred follow-up; this lets the
/// capture/refund/reconcile money path be built and proven without a federation.
#[derive(Default)]
pub struct MockPayment {
    state: Mutex<MockState>,
}

#[derive(Default)]
struct MockState {
    invoices: HashMap<String, Invoice>, // external_id -> Invoice (idempotency anchor)
    paid: HashSet<String>,              // external_ids observed settled
    settled_at: HashMap<String, i64>,   // external_id -> LIVE settled ts (None on recovery)
    payments: HashMap<String, String>,  // refund idempotency_key -> backend payment id
    seq: u64,
    now: i64, // mock wall clock; create_invoice stamps absolute expiry, lookup honors it
    settle_tx: Option<mpsc::Sender<Settlement>>,
}

impl MockPayment {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the mock's clock (unix secs). `create_invoice` stamps `expires_at = now + expiry`,
    /// and `lookup` returns `Expired` once `now` passes it — so expiry/reconcile tests exercise a
    /// realistic backend instead of an invoice that never expires.
    pub fn set_now(&self, now: i64) {
        self.state.lock().unwrap().now = now;
    }

    /// Drive a LIVE settlement for a previously-created invoice (by `external_id`): mark it paid,
    /// record the real `settled_at` (so `lookup_settlement` can surface it), and, if a `watch()`
    /// stream is open, push the `Settlement`. Returns the Settlement so a caller can also hand it
    /// straight to capture. Errors if no such invoice was created.
    pub fn settle(&self, external_id: &str, settled_at: i64) -> Result<Settlement> {
        let mut st = self.state.lock().unwrap();
        let inv = st
            .invoices
            .get(external_id)
            .ok_or_else(|| anyhow::anyhow!("mock: no invoice for external_id {external_id}"))?
            .clone();
        st.paid.insert(external_id.to_string());
        st.settled_at.insert(external_id.to_string(), settled_at);
        let s = Settlement {
            invoice_id: inv.id,
            external_id: external_id.to_string(),
            amount_sat: inv.amount_sat,
            settled_at,
        };
        if let Some(tx) = &st.settle_tx {
            let _ = tx.try_send(s.clone());
        }
        Ok(s)
    }

    /// Mark a previously-created invoice paid as a RECOVERY settlement: it was settled while the
    /// daemon was DOWN, so the backend has NO live timestamp (`lookup_settlement` reports `None` and
    /// the supervisor catch-up must cap conservatively). Unlike [`settle`](Self::settle) it records
    /// no live ts and does NOT push on the `watch()` stream (there was no watcher). Errors if no such
    /// invoice was created.
    pub fn settle_recovered(&self, external_id: &str) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.invoices.contains_key(external_id) {
            bail!("mock: no invoice for external_id {external_id}");
        }
        st.paid.insert(external_id.to_string());
        Ok(())
    }
}

#[async_trait]
impl PaymentBackend for MockPayment {
    async fn create_invoice(
        &self,
        amount_sat: u64,
        _memo: &str,
        expiry_s: u32,
        external_id: &str,
    ) -> Result<Invoice> {
        let mut st = self.state.lock().unwrap();
        if let Some(inv) = st.invoices.get(external_id) {
            return Ok(inv.clone()); // idempotent on external_id (a crash-retry reuses the invoice)
        }
        let n = st.seq;
        st.seq += 1;
        let inv = Invoice {
            id: format!("mock-inv-{n}"),
            external_id: external_id.to_string(),
            backend_invoice_id: format!("mock-bk-{n}"),
            payment_hash: format!("{n:064x}"),
            bolt11: format!("lnbcmock1{external_id}"),
            amount_sat,
            expires_at: st.now + i64::from(expiry_s), // absolute unix secs (matches the field's doc)
        };
        st.invoices.insert(external_id.to_string(), inv.clone());
        Ok(inv)
    }
    async fn lookup(&self, id: &str) -> Result<PaymentStatus> {
        let st = self.state.lock().unwrap();
        match st.invoices.values().find(|inv| inv.id == id) {
            Some(inv) if st.paid.contains(&inv.external_id) => Ok(PaymentStatus::Paid),
            Some(inv) if st.now >= inv.expires_at => Ok(PaymentStatus::Expired), // past its expiry
            Some(_) => Ok(PaymentStatus::Open),
            None => Ok(PaymentStatus::Expired), // unknown id -> gone
        }
    }
    async fn lookup_settlement(&self, id: &str) -> Result<(PaymentStatus, Option<i64>)> {
        let st = self.state.lock().unwrap();
        match st.invoices.values().find(|inv| inv.id == id) {
            // Paid: surface the LIVE ts (`settle`) — `None` for a recovery settle (`settle_recovered`).
            Some(inv) if st.paid.contains(&inv.external_id) => Ok((
                PaymentStatus::Paid,
                st.settled_at.get(&inv.external_id).copied(),
            )),
            Some(inv) if st.now >= inv.expires_at => Ok((PaymentStatus::Expired, None)),
            Some(_) => Ok((PaymentStatus::Open, None)),
            None => Ok((PaymentStatus::Expired, None)),
        }
    }
    async fn pay(&self, _dest: &str, _amount_sat: u64, idempotency_key: &str) -> Result<String> {
        let mut st = self.state.lock().unwrap();
        if let Some(pid) = st.payments.get(idempotency_key) {
            return Ok(pid.clone()); // idempotent on key -> never pays twice
        }
        let n = st.seq;
        st.seq += 1;
        let pid = format!("mock-pay-{n}");
        st.payments.insert(idempotency_key.to_string(), pid.clone());
        Ok(pid)
    }
    async fn payment_status(&self, payment_id: &str) -> Result<PayStatus> {
        let st = self.state.lock().unwrap();
        Ok(if st.payments.values().any(|p| p == payment_id) {
            PayStatus::Succeeded
        } else {
            PayStatus::Unknown
        })
    }
    async fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus> {
        let st = self.state.lock().unwrap();
        Ok(if st.payments.contains_key(idempotency_key) {
            PayStatus::Succeeded
        } else {
            PayStatus::Unknown
        })
    }
    async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
        let (tx, rx) = mpsc::channel(64);
        self.state.lock().unwrap().settle_tx = Some(tx);
        Ok(rx)
    }
}

/// Storage subsystem. SPEC.md §8.3 (phased, M7) — trait stub.
pub trait StorageBackend: Send + Sync {
    fn create_volume(&self, spec: &Value) -> Result<Value>;
    fn snapshot(&self, handle: &Value) -> Result<Value>;
    fn destroy_volume(&self, handle: &Value) -> Result<()>;
}

/// Observability subsystem, read-only. SPEC.md §8.4 (phased, M7) — trait stub.
pub trait Observability: Send + Sync {
    fn status(&self, instance: &Value) -> Result<Value>;
    fn logs(&self, instance: &Value, lines: u32) -> Result<String>;
}

#[cfg(test)]
mod mock_payment_tests {
    use super::*;

    #[tokio::test]
    async fn create_invoice_is_idempotent_on_external_id() {
        let m = MockPayment::new();
        let a = m.create_invoice(1000, "memo", 3600, "ext1").await.unwrap();
        let b = m.create_invoice(9999, "other", 60, "ext1").await.unwrap();
        assert_eq!(
            a.id, b.id,
            "same external_id -> same invoice, never a duplicate"
        );
        assert_eq!(
            b.amount_sat, 1000,
            "the original invoice is returned unchanged"
        );
    }

    #[tokio::test]
    async fn lookup_honors_absolute_expiry() {
        let m = MockPayment::new();
        m.set_now(1_000);
        let inv = m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        assert_eq!(inv.expires_at, 1_060, "absolute expiry = now + expiry_s");
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Open);
        m.set_now(1_100); // past expiry
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Expired);
    }

    #[tokio::test]
    async fn settle_flips_lookup_to_paid_even_past_expiry() {
        let m = MockPayment::new();
        let inv = m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        m.settle("ext1", 30).unwrap();
        m.set_now(10_000); // a paid invoice stays Paid regardless of the clock
        assert_eq!(m.lookup(&inv.id).await.unwrap(), PaymentStatus::Paid);
    }

    #[tokio::test]
    async fn pay_is_idempotent_on_key() {
        let m = MockPayment::new();
        let p1 = m.pay("dest", 500, "refund:x").await.unwrap();
        let p2 = m.pay("dest", 500, "refund:x").await.unwrap();
        assert_eq!(p1, p2, "same key -> same payment id, never pays twice");
        assert_eq!(
            m.payment_status_by_key("refund:x").await.unwrap(),
            PayStatus::Succeeded
        );
        assert_eq!(
            m.payment_status_by_key("refund:never").await.unwrap(),
            PayStatus::Unknown
        );
    }

    #[tokio::test]
    async fn settle_pushes_on_the_watch_stream() {
        let m = MockPayment::new();
        m.create_invoice(1000, "memo", 60, "ext1").await.unwrap();
        let mut rx = m.watch().await.unwrap();
        let pushed = m.settle("ext1", 42).unwrap();
        let got = rx.recv().await.expect("a settlement arrives on watch()");
        assert_eq!(got.external_id, "ext1");
        assert_eq!(got.amount_sat, 1000);
        assert_eq!(got.settled_at, 42);
        assert_eq!(pushed.external_id, got.external_id);
    }
}
