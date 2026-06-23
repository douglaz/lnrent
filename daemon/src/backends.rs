//! Subsystem backends. SPEC.md §8. v1 implements Compute (host) + Network
//! (WireGuard) and Payment (phoenixd); these are M0 stubs that compile and fail
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
    fn create_invoice(
        &self,
        amount_sat: u64,
        memo: &str,
        expiry_s: u32,
        external_id: &str, // binds settlement -> order (ADR-0009)
    ) -> Result<Invoice>;
    fn lookup(&self, id: &str) -> Result<PaymentStatus>;
    /// Outbound payment, used for refunds. Returns a backend payment id for status/dedup.
    fn pay(&self, dest: &str, amount_sat: u64) -> Result<String>;
    /// Stream of settled payments (push). `Settlement.external_id` carries the order id
    /// (SPEC §6.1). M1a wires this to the phoenixd websocket.
    fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>>;
}

#[derive(Debug, Clone)]
pub struct Invoice {
    pub id: String,
    pub external_id: String, // = order/subscription id (ADR-0009)
    pub bolt11: String,
    pub amount_sat: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentStatus {
    Open,
    Paid,
    Expired,
}

/// A settled incoming payment; `external_id` correlates to the order (SPEC §6.1).
#[derive(Debug, Clone)]
pub struct Settlement {
    pub invoice_id: String,
    pub external_id: Option<String>,
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

/// phoenixd payment backend. SPEC.md §6.1. Cannot hold invoices (ADR-0003).
pub struct PhoenixdPayment;

impl PaymentBackend for PhoenixdPayment {
    fn create_invoice(
        &self,
        _amount_sat: u64,
        _memo: &str,
        _expiry_s: u32,
        _external_id: &str,
    ) -> Result<Invoice> {
        bail!("phoenixd.create_invoice not implemented (M0 stub)")
    }
    fn lookup(&self, _id: &str) -> Result<PaymentStatus> {
        bail!("phoenixd.lookup not implemented (M0 stub)")
    }
    fn pay(&self, _dest: &str, _amount_sat: u64) -> Result<String> {
        bail!("phoenixd.pay not implemented (M0 stub)")
    }
    fn watch(&self) -> Result<tokio::sync::mpsc::Receiver<Settlement>> {
        bail!("phoenixd.watch not implemented (M0 stub)")
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
