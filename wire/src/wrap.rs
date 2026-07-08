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

/// Upper bound on a gift-wrapped rumor's content before it is JSON-decoded (gdu.4). Enforced in
/// [`gift_unwrap`] — the earliest point where the decrypted bytes exist — so the JSON decode work
/// per wrap is bounded at the lnrent layer. 64 KiB matches the refund resolver's response-body
/// cap and is generous for every legitimate lnrent message type. Compliant NIP-59 encryption
/// cannot produce over-cap content today (NIP-44's 65,535-byte plaintext ceiling applies to the
/// seal's base64-expanded ciphertext, capping rumor content just under 40 KiB) — this guard pins
/// the bound HERE so a future transport/library change cannot silently lift it.
pub const MAX_INBOUND_CONTENT_BYTES: usize = 64 * 1024;

/// The [`MAX_INBOUND_CONTENT_BYTES`] guard, split out so the exact boundary is unit-testable:
/// the transport's own ceiling sits below the cap, so an at-cap rumor can't be produced through
/// [`gift_wrap`] to exercise this through the full round trip.
fn ensure_content_within_bound(content: &str) -> Result<(), Error> {
    if content.len() > MAX_INBOUND_CONTENT_BYTES {
        return Err(Error::ContentTooLarge {
            len: content.len(),
            max: MAX_INBOUND_CONTENT_BYTES,
        });
    }
    Ok(())
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
    // Size-bound the content BEFORE the JSON decode (gdu.4). An over-cap wrap takes the same
    // `Err` path as any undecodable wrap, so the engine disposes of it identically (bounded
    // negative cache, no `seen_message` row).
    ensure_content_within_bound(&gift.rumor.content)?;
    let msg: Msg = serde_json::from_str(&gift.rumor.content)?;
    Ok(Unwrapped {
        sender: gift.sender,
        msg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // gdu.4 boundary, on the extracted guard: content of exactly MAX_INBOUND_CONTENT_BYTES
    // passes; one byte over is ContentTooLarge. Tested here because NIP-44 v2's 65,535-byte
    // plaintext ceiling means gift_wrap cannot produce an at-cap rumor for a round-trip test.
    #[test]
    fn content_bound_is_boundary_exact() {
        let at_cap = "x".repeat(MAX_INBOUND_CONTENT_BYTES);
        ensure_content_within_bound(&at_cap).expect("content exactly at the cap passes");

        let over = "x".repeat(MAX_INBOUND_CONTENT_BYTES + 1);
        match ensure_content_within_bound(&over) {
            Err(Error::ContentTooLarge { len, max }) => {
                assert_eq!(len, MAX_INBOUND_CONTENT_BYTES + 1);
                assert_eq!(max, MAX_INBOUND_CONTENT_BYTES);
            }
            other => panic!("expected ContentTooLarge, got {other:?}"),
        }
    }
}
