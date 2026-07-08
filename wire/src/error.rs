//! The single error type for the wire codec. Kept small: a consumer either decodes a
//! message/listing or it doesn't.

use core::fmt;

/// Anything that can go wrong encoding or decoding an lnrent wire message or listing.
#[derive(Debug)]
pub enum Error {
    /// A DM message or listing content failed JSON ser/de.
    Json(serde_json::Error),
    /// A NIP-44/NIP-59 gift-wrap step failed (encrypt, seal, or unwrap). Carries the
    /// underlying rust-nostr message; the variants are too many to mirror exactly.
    GiftWrap(String),
    /// The event handed to `gift_unwrap` was not a NIP-59 gift wrap (kind 1059).
    NotGiftWrap,
    /// The gift wrap's inner rumor was not a NIP-17 private DM (kind 14), so its content is not
    /// an lnrent message.
    NotPrivateDm,
    /// The gift wrap's rumor content exceeds [`crate::wrap::MAX_INBOUND_CONTENT_BYTES`], so it is
    /// rejected before the JSON decode (gdu.4: bounds the decode work an unauthenticated sender
    /// can force per wrap).
    ContentTooLarge { len: usize, max: usize },
    /// The event handed to `parse_listing` was not a NIP-99 classified listing (kind 30402).
    NotListing,
    /// A Nostr event failed id/signature verification before being trusted.
    InvalidEvent(String),
    /// A required listing tag or field was absent.
    Missing(&'static str),
    /// A listing being built exceeds the parser's bounded array limits.
    TooMany { field: &'static str, max: usize },
    /// The listing content declares a schema version this codec does not understand.
    UnsupportedSchemaVersion { found: u32, supported: u32 },
    /// The listing's NIP-99 price amount was not an unsigned integer.
    InvalidPriceAmount { found: String },
    /// The listing's NIP-99 price currency was not `SAT` (SPEC.md §5.4 fixes this unit).
    InvalidPriceCurrency { found: String },
    /// The listing's `operator` tag was not a valid Nostr public key (SPEC.md §5.4 / §4.6 —
    /// the operator is a master pubkey).
    InvalidOperator { found: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Json(e) => write!(f, "json: {e}"),
            Error::GiftWrap(e) => write!(f, "gift wrap: {e}"),
            Error::NotGiftWrap => f.write_str("event is not a NIP-59 gift wrap (kind 1059)"),
            Error::NotPrivateDm => {
                f.write_str("gift wrap rumor is not a NIP-17 private DM (kind 14)")
            }
            Error::ContentTooLarge { len, max } => {
                write!(f, "rumor content too large ({len} bytes, max {max})")
            }
            Error::NotListing => f.write_str("event is not a NIP-99 listing (kind 30402)"),
            Error::InvalidEvent(e) => write!(f, "nostr event verification failed: {e}"),
            Error::Missing(field) => write!(f, "listing is missing `{field}`"),
            Error::TooMany { field, max } => {
                write!(f, "listing has too many `{field}` entries (max {max})")
            }
            Error::UnsupportedSchemaVersion { found, supported } => {
                write!(
                    f,
                    "unsupported listing schema version {found} (supported {supported})"
                )
            }
            Error::InvalidPriceAmount { found } => {
                write!(
                    f,
                    "listing price amount is not an unsigned integer: {found}"
                )
            }
            Error::InvalidPriceCurrency { found } => {
                write!(f, "listing price currency must be SAT, got {found}")
            }
            Error::InvalidOperator { found } => {
                write!(f, "listing operator is not a valid public key: {found}")
            }
        }
    }
}

impl std::error::Error for Error {
    /// Preserve the underlying cause for the variants that wrap a typed error, so consumers
    /// using `anyhow`/`{:#}` keep the full chain (e.g. the serde_json detail behind `Json`).
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    // `Error::Json` preserves the underlying serde_json cause in the error chain so consumers
    // using `anyhow`/`{:#}` don't lose the JSON detail; the string-carrying variants have none.
    #[test]
    fn json_variant_exposes_its_source() {
        let json_err: Error = serde_json::from_str::<u32>("not json").unwrap_err().into();
        assert!(matches!(json_err, Error::Json(_)));
        assert!(json_err.source().is_some());

        assert!(Error::NotGiftWrap.source().is_none());
        assert!(Error::GiftWrap("boom".into()).source().is_none());
    }
}
