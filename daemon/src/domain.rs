//! Core domain types. See SPEC.md §4.4, §6.3 and CONTEXT.md.

use serde::{Deserialize, Serialize};

pub type Id = String;

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
