//! The buyer-core error taxonomy and the `--json` error envelope (SPEC.md §4.7, §5.1, ADR-0014).
//!
//! Every buyer flow returns [`BuyerError`]. The CLI renders [`BuyerError::envelope`] as the
//! `{ code, message, retryable }` failure body and exits with [`BuyerError::exit_code`]. The two
//! are the agent-facing contract, so they live in core (shared by the CLI and the web client) and
//! map deterministically onto the exit-code taxonomy of the bead.

use lnrent_wire::WireError;
use serde::Serialize;

/// A buyer-flow failure, tagged by category so the CLI can map it onto a deterministic exit code
/// and a structured error body. `Remote` carries the operator's own nested error verbatim (so an
/// `order.error` / `op.result` error reaches the agent unchanged); every other variant is a
/// local/transport/protocol failure detected on the buyer side.
#[derive(Debug, thiserror::Error)]
pub enum BuyerError {
    /// A listing / subscription the buyer asked for does not exist. (exit 2)
    #[error("not found: {0}")]
    NotFound(String),
    /// Malformed input from the caller (bad flag, unparseable JSON params, missing key). (exit 3)
    #[error("bad request: {0}")]
    BadRequest(String),
    /// A locally-detectable invalid state (e.g. a key file that already exists). (exit 3)
    #[error("invalid state: {0}")]
    InvalidState(String),
    /// An `interactive`-kind op/listing — out of scope for M1a (Iroh sessions, §9.2). (exit 3)
    #[error("unsupported interactive operation: {0}")]
    UnsupportedInteractive(String),
    /// No correlated reply arrived before the deadline (the operator may be slow/offline). (exit 4)
    #[error("relay timeout: {0}")]
    Timeout(String),
    /// A transport-level failure talking to the relay (retryable). (exit 4)
    #[error("transport error: {0}")]
    Transport(String),
    /// An unexpected internal failure (signing, serialization). (exit 5)
    #[error("internal error: {0}")]
    Internal(String),
    /// The operator answered with a structured error (`order.error` / `op.result` error) — its
    /// `{ code, message, retryable }` is surfaced to the agent unchanged. (exit 6)
    #[error("remote error ({}): {}", .0.code, .0.message)]
    Remote(WireError),
    /// A provenance / protocol failure: a reply from the wrong sender, a `request_id` that does not
    /// correlate, or an unparseable / unsigned listing. (exit 7)
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// The `{ code, message, retryable }` error body rendered under `{"ok":false,"error":...}` on
/// stderr (SPEC.md §4.7, §5.1). Stable field names so an agent can branch on them.
#[derive(Debug, Clone, Serialize)]
pub struct ErrEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

impl BuyerError {
    /// The deterministic process exit code (the bead taxonomy): 0 ok; 2 not_found; 3 bad_request /
    /// invalid_state / unsupported_interactive; 4 relay_timeout / transport; 5 internal; 6
    /// remote_error; 7 provenance/protocol.
    pub fn exit_code(&self) -> u8 {
        match self {
            BuyerError::NotFound(_) => 2,
            BuyerError::BadRequest(_)
            | BuyerError::InvalidState(_)
            | BuyerError::UnsupportedInteractive(_) => 3,
            BuyerError::Timeout(_) | BuyerError::Transport(_) => 4,
            BuyerError::Internal(_) => 5,
            BuyerError::Remote(_) => 6,
            BuyerError::Protocol(_) => 7,
        }
    }

    /// The structured error body for the `--json` envelope. For a `Remote` error the code /
    /// message / retryable come from the operator's nested error; for every local error they come
    /// from the variant.
    pub fn envelope(&self) -> ErrEnvelope {
        let (code, message, retryable) = match self {
            BuyerError::NotFound(m) => ("not_found", m.clone(), false),
            BuyerError::BadRequest(m) => ("bad_request", m.clone(), false),
            BuyerError::InvalidState(m) => ("invalid_state", m.clone(), false),
            BuyerError::UnsupportedInteractive(m) => ("unsupported_interactive", m.clone(), false),
            BuyerError::Timeout(m) => ("relay_timeout", m.clone(), true),
            BuyerError::Transport(m) => ("transport", m.clone(), true),
            BuyerError::Internal(m) => ("internal", m.clone(), false),
            // The operator's own code is preserved (e.g. `unauthorized`, `capacity_full`).
            BuyerError::Remote(e) => {
                return ErrEnvelope {
                    code: e.code.clone(),
                    message: e.message.clone(),
                    retryable: e.retryable,
                }
            }
            BuyerError::Protocol(m) => ("protocol", m.clone(), false),
        };
        ErrEnvelope {
            code: code.to_string(),
            message,
            retryable,
        }
    }
}
