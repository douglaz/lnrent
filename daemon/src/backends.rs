//! Subsystem backends. SPEC.md §8. v1 implements Compute (host) + Network
//! (WireGuard) and Payment (Fedimint, ADR-0012); these are M0 stubs that compile and fail
//! loudly until M1 fills them in.

use anyhow::{bail, Result};
use serde_json::Value;

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
pub trait PaymentBackend: Send + Sync {
    /// Create (or return the existing) invoice. **Idempotent on `external_id`**: a repeated
    /// call with the same `external_id` MUST return the same invoice, not a duplicate — so a
    /// retry after a crash regenerates the same `external_id` and reuses the invoice (§6.6).
    fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str, // binds settlement -> order (ADR-0009); deterministic per invoice class (§6.6)
    ) -> Result<Invoice>;
    fn lookup(&self, id: &str) -> Result<PaymentStatus>;
    /// Outbound payment, used for refunds. **Idempotent on `idempotency_key`**: calling twice
    /// with the same key never pays twice (ADR-0009, SPEC §6.6). Returns a backend payment id.
    fn pay(&self, dest: &str, amount_sat: u64, idempotency_key: &str) -> Result<String>;
    /// Status of an outbound payment by its backend id (ADR-0009 refund ledger).
    fn payment_status(&self, payment_id: &str) -> Result<PayStatus>;
    /// Check an in-flight refund by its idempotency key after a crash (SPEC §6.6). An
    /// optimization only — retrying `pay(key)` is always safe (the key dedups).
    fn payment_status_by_key(&self, idempotency_key: &str) -> Result<PayStatus>;
    /// Stream of settled payments (push). `Settlement.external_id` carries the order id
    /// (SPEC §6.1). M1a wires this to the Fedimint client settlement stream.
    fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>>;
}

#[derive(Debug, Clone)]
pub struct Invoice {
    pub id: String,
    pub external_id: String, // unique per-invoice token binding settlement->order (ADR-0009)
    pub bolt11: String,
    pub amount_sat: u64,
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

impl PaymentBackend for FedimintPayment {
    fn create_invoice(
        &self,
        _amount_sat: u64,
        _memo: &str,
        _expiry_s: u32,
        _external_id: &str,
    ) -> Result<Invoice> {
        bail!("fedimint.create_invoice not implemented (M0 stub)")
    }
    fn lookup(&self, _id: &str) -> Result<PaymentStatus> {
        bail!("fedimint.lookup not implemented (M0 stub)")
    }
    fn pay(&self, _dest: &str, _amount_sat: u64, _idempotency_key: &str) -> Result<String> {
        bail!("fedimint.pay not implemented (M0 stub)")
    }
    fn payment_status(&self, _payment_id: &str) -> Result<PayStatus> {
        bail!("fedimint.payment_status not implemented (M0 stub)")
    }
    fn payment_status_by_key(&self, _idempotency_key: &str) -> Result<PayStatus> {
        bail!("fedimint.payment_status_by_key not implemented (M0 stub)")
    }
    fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
        bail!("fedimint.watch not implemented (M0 stub)")
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
