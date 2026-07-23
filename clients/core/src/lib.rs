//! `lnrent-buyer-core` (lnrent-7fp.13): the target-agnostic buyer half of the lnrent protocol
//! (SPEC.md §4.7, §5, ADR-0014). It owns the buyer flows — discover listings, place an order,
//! surface the invoice (NEVER pay), await `provision.ready`, run management ops, renew, resend
//! delivery, cancel — over NIP-17 gift-wrapped DMs + NIP-99 listings, built on the shared
//! [`lnrent_wire`] codec.
//!
//! Pure protocol: all I/O is injected as traits the host implements ([`Relay`], [`Clock`], and a
//! `NostrSigner` from the wire crate), so the SAME flows run on the native CLI and, by construction,
//! a wasm32 web client — no native-only dependency leaks in here.

pub mod client;
pub mod error;
pub mod relay;

pub use client::{BuyerClient, RenewReply};
pub use error::{BuyerError, ErrEnvelope};
pub use relay::{Clock, GiftWrapStream, Relay, RelayError};

// Re-export the wire codec so a host depends on one crate for the shared types it renders/builds.
pub use lnrent_wire;
