//! Core domain types. See SPEC.md §4.4, §6.3 and CONTEXT.md.

use serde::{Deserialize, Serialize};

pub type Id = String;

/// Subscription lifecycle state. SPEC.md §6.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SubState {
    /// Pre-flight passed, first invoice issued, awaiting payment.
    Pending,
    /// First payment captured; running `provision`.
    Provisioning,
    /// Provisioned and paid through `paid_through`.
    Active,
    /// Past `paid_through` unpaid; service stopped, data kept.
    Suspended,
    /// Retention elapsed; Instance destroyed.
    Terminated,
    /// First invoice expired unpaid; order dead.
    Expired,
    /// Buyer cancelled.
    Cancelled,
    /// Provision failed after capture; refund owed.
    #[serde(rename = "REFUND_DUE")]
    RefundDue,
    /// Refund paid back to the buyer.
    Refunded,
}

/// Who owns an Instance: the operator (self-use) or a subscription (rented). SPEC.md §4.4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Owner {
    Operator,
    Subscription { id: Id },
}

/// A managed, lifecycle-bearing resource: VM, container, WireGuard peer, volume,
/// guardian. SPEC.md §4.4.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: Id,
    pub recipe_id: String,
    pub owner: Owner,
    pub box_id: String,
    /// Backend handles needed to manage the Instance later.
    pub handle: serde_json::Value,
}

/// A durable paid relationship between a Buyer and a Listing. SPEC.md §6.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: Id,
    pub recipe_id: String,
    pub buyer_pubkey: String,
    pub state: SubState,
    pub params: serde_json::Value,
    /// BOLT12 offer or Lightning address for refunds (ADR-0003).
    pub refund_dest: Option<String>,
    pub period_s: i64,
    pub renew_lead_s: i64,
    pub retention_s: i64,
    /// Hard expiry; service interrupted after this. SPEC.md §6.2.
    pub paid_through: Option<i64>,
    /// `paid_through - renew_lead_s`; renewal recommended from here.
    pub soft_date: Option<i64>,
    /// Reconcile-loop cursor. SPEC.md §6.5.
    pub next_deadline: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// State strings must match the spec's sqlite values exactly (SPEC.md §6.3/§11).
    /// Guards the REFUND_DUE rename: `rename_all = "UPPERCASE"` alone yields REFUNDDUE.
    #[test]
    fn substate_serializes_to_spec_values() {
        let cases = [
            (SubState::Pending, "\"PENDING\""),
            (SubState::Provisioning, "\"PROVISIONING\""),
            (SubState::RefundDue, "\"REFUND_DUE\""),
            (SubState::Refunded, "\"REFUNDED\""),
        ];
        for (state, want) in cases {
            assert_eq!(serde_json::to_string(&state).unwrap(), want);
        }
    }
}
