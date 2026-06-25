//! NIP-17 gift-wrap transport for [`Msg`]: a NIP-59 seal + wrap over NIP-44 v2 (SPEC.md §5.1).
//!
//! Pure functions with a signer as an explicit seam — no relay or payment I/O. The caller owns
//! the signer and the relay; this module only turns a [`Msg`] into a sealed kind-1059 event and
//! back. The async signatures come from rust-nostr's signer abstraction (a `Keys` signs
//! synchronously, while NIP-07 / remote signers are async).

use nostr::nips::nip59;
use nostr::{Event, EventBuilder, Kind, NostrSigner, PublicKey};

use crate::dm::Msg;
use crate::error::Error;

/// Gift-wrap `msg` from `sender` to `recipient` (SPEC.md §5.1). Returns the signed
/// kind-1059 event ready to publish to a relay. The lnrent JSON rides as the content of a
/// NIP-17 private DM rumor (kind 14).
pub async fn gift_wrap<S>(sender: &S, recipient: &PublicKey, msg: &Msg) -> Result<Event, Error>
where
    S: NostrSigner,
{
    let content = serde_json::to_string(msg)?;
    let sender_pubkey = sender
        .get_public_key()
        .await
        .map_err(|e| Error::GiftWrap(e.to_string()))?;
    let rumor = EventBuilder::private_msg_rumor(*recipient, content).build(sender_pubkey);
    EventBuilder::gift_wrap(sender, recipient, rumor, [])
        .await
        .map_err(|e| Error::GiftWrap(e.to_string()))
}

/// A decoded gift wrap: the `sender` pubkey (verified from the seal) and the lnrent `msg`.
/// The operator authorizes a request by matching `sender` to the subscription's
/// `buyer_pubkey` (SPEC.md §5.1, §7.4) — so the sender is returned alongside the message.
#[derive(Debug, Clone)]
pub struct Unwrapped {
    pub sender: PublicKey,
    pub msg: Msg,
}

/// Decode a gift wrap addressed to `recipient_keys` back into its lnrent message (SPEC.md §5.1).
/// Verifies the NIP-59 seal (so `sender` is authentic) and parses the rumor content as a [`Msg`].
pub async fn gift_unwrap<S>(recipient: &S, wrap: &Event) -> Result<Unwrapped, Error>
where
    S: NostrSigner,
{
    // Verify the OUTER envelope before decrypting: it must be a kind-1059 gift wrap whose id and
    // signature check out. The inner seal still authenticates the sender, but verifying the outer
    // event stops a tampered/forged envelope from being trusted as a unit — important because a
    // consumer may use the outer `Event.id` as a relay-duplicate key (codex review).
    if wrap.kind != Kind::GiftWrap {
        return Err(Error::NotGiftWrap);
    }
    wrap.verify()
        .map_err(|e| Error::InvalidEvent(e.to_string()))?;
    let gift = nip59::extract_rumor(recipient, wrap)
        .await
        .map_err(|e| match e {
            nip59::Error::NotGiftWrap => Error::NotGiftWrap,
            other => Error::GiftWrap(other.to_string()),
        })?;
    // NIP-59 can wrap a rumor of any kind, but lnrent messages ride only in a NIP-17 private DM
    // (kind 14). Reject anything else before trusting its content, so a non-DM rumor that merely
    // happens to hold valid lnrent JSON can't be processed as a protocol message.
    if gift.rumor.kind != Kind::PrivateDirectMessage {
        return Err(Error::NotPrivateDm);
    }
    let msg: Msg = serde_json::from_str(&gift.rumor.content)?;
    Ok(Unwrapped {
        sender: gift.sender,
        msg,
    })
}
