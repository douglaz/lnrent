//! NIP-99 30402 classified-listing build/parse (SPEC.md §5.4). A listing pins concrete
//! pricing for one recipe and publishes the buyer-facing surface: the order `params` schema
//! and the recipe's management-operation declarations (the operator-internal `hook` is NEVER
//! published). The standard NIP-99 fields ride as event tags; the lnrent metadata rides in the
//! event `content` under an `lnrent` object carrying a schema `version`.
//!
//! Building is a pure seam: [`build_listing`] returns an unsigned [`EventBuilder`] the caller
//! signs with its operational key (no signer I/O here). [`parse_listing`] reads a signed event.

use core::fmt;
use core::marker::PhantomData;

use nostr::{Event, EventBuilder, Kind, PublicKey, Tag, TagKind};
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// The NIP-99 classified-listing kind lnrent listings use (SPEC.md §5.4).
pub const LISTING_KIND: u16 = 30402;

/// The lnrent listing schema version embedded in the event content (SPEC.md §5.4). Bump when
/// the `content` layout changes; parsers tolerate unknown fields within a version.
pub const SCHEMA_VERSION: u32 = 1;

/// Upper bound on the operation declarations a parser accepts from one listing — a
/// forward-compat DoS guard (SPEC.md §5.4 "bound the array size"). Enforced *during*
/// deserialization (see [`deserialize_bounded`]) so a hostile array can never be fully
/// materialized before the bound is checked.
pub const MAX_OPERATIONS: usize = 64;

/// Upper bound on the parameter declarations a parser accepts from a single array — the order
/// `params` schema and each operation's `params` (SPEC.md §5.4). Same forward-compat DoS guard
/// as [`MAX_OPERATIONS`], bounding allocation as the array is read.
pub const MAX_PARAMS: usize = 64;

/// Deserialize a JSON array into a `Vec<T>`, erroring as soon as it exceeds `max` elements
/// instead of after a hostile array is fully materialized — the SPEC.md §5.4 "bound the array
/// size" guard, enforced at allocation time. `what` names the element for the error message.
fn deserialize_bounded<'de, D, T>(
    deserializer: D,
    max: usize,
    what: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVisitor<T> {
        max: usize,
        what: &'static str,
        _marker: PhantomData<T>,
    }
    impl<'de, T: Deserialize<'de>> Visitor<'de> for BoundedVisitor<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "an array of at most {} {}", self.max, self.what)
        }
        fn visit_seq<A>(self, mut seq: A) -> Result<Vec<T>, A::Error>
        where
            A: SeqAccess<'de>,
        {
            // Never trust the size hint for preallocation — it can be attacker-controlled.
            let mut out: Vec<T> = Vec::new();
            while let Some(item) = seq.next_element::<T>()? {
                if out.len() >= self.max {
                    return Err(de::Error::custom(format!(
                        "too many {} (max {})",
                        self.what, self.max
                    )));
                }
                out.push(item);
            }
            Ok(out)
        }
    }
    deserializer.deserialize_seq(BoundedVisitor::<T> {
        max,
        what,
        _marker: PhantomData,
    })
}

/// `deserialize_with` shim: a `Vec<OperationDecl>` bounded by [`MAX_OPERATIONS`].
fn de_operations<'de, D>(deserializer: D) -> Result<Vec<OperationDecl>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded(deserializer, MAX_OPERATIONS, "operations")
}

/// `deserialize_with` shim: a `Vec<ParamDecl>` bounded by [`MAX_PARAMS`].
fn de_params<'de, D>(deserializer: D) -> Result<Vec<ParamDecl>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded(deserializer, MAX_PARAMS, "params")
}

/// A buyer-supplied parameter (mirrors recipe `[[params]]`, SPEC.md §7.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamDecl {
    pub key: String,
    pub label: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub required: bool,
}

/// A recipe-declared management operation as PUBLISHED in a listing (SPEC.md §5.4, §7.4).
/// Mirrors recipe `[[operation]]` minus the operator-internal `hook`, which is never
/// serialized into a listing — the buyer renders the `ops` interface from this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationDecl {
    pub name: String,
    pub label: String,
    /// `request` | `interactive` (SPEC.md §7.4); a string so future kinds still parse.
    pub kind: String,
    #[serde(default, deserialize_with = "de_params")]
    pub params: Vec<ParamDecl>,
}

/// The lnrent view of a NIP-99 30402 listing (SPEC.md §5.4): the standard classified fields
/// plus the lnrent metadata a buyer needs to order and to discover the management surface.
///
/// Not directly JSON-serialized: its wire form is a kind-30402 event (tags + `content`), via
/// [`build_listing`] / [`parse_listing`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// NIP-99 `d` identifier; the listing's addressable coordinate is `30402:<pubkey>:<d>`.
    pub d: String,
    /// The operator (master) pubkey in hex — the `operator` tag (SPEC.md §5.3).
    pub operator: String,
    pub recipe_id: String,
    pub recipe_version: String,
    pub title: String,
    pub summary: String,
    pub amount_sat: u64,
    pub period: String,
    /// The order `params` schema the buyer fills in `order.request` (SPEC.md §7.1).
    pub params: Vec<ParamDecl>,
    /// The published management-operation declarations (SPEC.md §7.4); `hook` is never included.
    pub operations: Vec<OperationDecl>,
    /// The honest security tier the listing advertises (`recipe.provisioning.tier`: `0` | `1` |
    /// `1.5` | `2`, SPEC.md §9.1, ADR-0007). VM listings MUST carry it so buyer-core/web can make
    /// the tier decision from structured data instead of parsing prose; `None` when the service
    /// declares no tier. An open string so a future tier still parses.
    pub tier: Option<String>,
    /// The lnrent listing schema version (SPEC.md §5.4).
    pub version: u32,
}

/// A parsed listing plus its addressable coordinate (SPEC.md §5.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedListing {
    pub listing: Listing,
    /// `30402:<pubkey>:<d>` — exactly what `order.request.listing_id` references.
    pub listing_id: String,
}

/// The lnrent metadata block carried in the event `content` (SPEC.md §5.4).
#[derive(Debug, Serialize, Deserialize)]
struct Content {
    lnrent: LnrentMeta,
}

#[derive(Debug, Serialize, Deserialize)]
struct LnrentMeta {
    version: u32,
    recipe: RecipeRef,
    /// The honest security tier (§9.1); omitted when the listing declares none, and tolerated
    /// as absent on parse so a non-VM listing need not carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tier: Option<String>,
    #[serde(default, deserialize_with = "de_params")]
    params: Vec<ParamDecl>,
    #[serde(default, deserialize_with = "de_operations")]
    operations: Vec<OperationDecl>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RecipeRef {
    id: String,
    version: String,
}

/// The addressable coordinate of a listing: `30402:<pubkey_hex>:<d>` (SPEC.md §5.4). Stable
/// across price edits (republishing the same `(kind, pubkey, d)`).
pub fn listing_coordinate(pubkey_hex: &str, d: &str) -> String {
    format!("{LISTING_KIND}:{pubkey_hex}:{d}")
}

/// Build an unsigned NIP-99 30402 event for `listing` (SPEC.md §5.4). The caller signs it with
/// its operational key; `listing_id` is then `30402:<signer-pubkey>:<d>`. Pure — no signer I/O.
pub fn build_listing(listing: &Listing) -> Result<EventBuilder, Error> {
    validate_listing(listing)?;

    let content = Content {
        lnrent: LnrentMeta {
            version: listing.version,
            recipe: RecipeRef {
                id: listing.recipe_id.clone(),
                version: listing.recipe_version.clone(),
            },
            tier: listing.tier.clone(),
            params: listing.params.clone(),
            operations: listing.operations.clone(),
        },
    };
    let content = serde_json::to_string(&content)?;
    let tags = vec![
        Tag::identifier(listing.d.clone()),
        Tag::custom(TagKind::custom("title"), [listing.title.clone()]),
        Tag::custom(TagKind::custom("summary"), [listing.summary.clone()]),
        // NIP-99 price tag: ["price", <amount>, <currency>, <frequency>].
        Tag::custom(
            TagKind::custom("price"),
            [
                listing.amount_sat.to_string(),
                "SAT".to_string(),
                listing.period.clone(),
            ],
        ),
        // lnrent operator tag: the master pubkey the listing's brand belongs to (§5.3).
        Tag::custom(TagKind::custom("operator"), [listing.operator.clone()]),
    ];
    Ok(EventBuilder::new(Kind::Custom(LISTING_KIND), content).tags(tags))
}

fn validate_listing(listing: &Listing) -> Result<(), Error> {
    if listing.version != SCHEMA_VERSION {
        return Err(Error::UnsupportedSchemaVersion {
            found: listing.version,
            supported: SCHEMA_VERSION,
        });
    }
    // The `d` tag is half the addressable coordinate `30402:<pubkey>:<d>` — an empty `d` yields a
    // malformed coordinate, so reject it (codex review).
    if listing.d.is_empty() {
        return Err(Error::Missing("d"));
    }
    // `operator` is a master pubkey (§5.4); reject anything that isn't a valid public key so a
    // consumer never trusts a bogus operator field.
    if PublicKey::from_hex(&listing.operator).is_err() {
        return Err(Error::InvalidOperator {
            found: listing.operator.clone(),
        });
    }
    if listing.params.len() > MAX_PARAMS {
        return Err(Error::TooMany {
            field: "params",
            max: MAX_PARAMS,
        });
    }
    if listing.operations.len() > MAX_OPERATIONS {
        return Err(Error::TooMany {
            field: "operations",
            max: MAX_OPERATIONS,
        });
    }
    for op in &listing.operations {
        if op.params.len() > MAX_PARAMS {
            return Err(Error::TooMany {
                field: "operation.params",
                max: MAX_PARAMS,
            });
        }
    }
    Ok(())
}

/// Parse a signed NIP-99 30402 event back into a [`Listing`] and its coordinate (SPEC.md §5.4).
/// Tolerates unknown tags and unknown `content` fields (forward-compat) and bounds the
/// operation and params arrays ([`MAX_OPERATIONS`] / [`MAX_PARAMS`]) as they deserialize.
pub fn parse_listing(event: &Event) -> Result<ParsedListing, Error> {
    if event.kind != Kind::Custom(LISTING_KIND) {
        return Err(Error::NotListing);
    }
    event
        .verify()
        .map_err(|e| Error::InvalidEvent(e.to_string()))?;

    let mut d = None;
    let mut title = None;
    let mut summary = None;
    let mut operator = None;
    let mut amount_sat = None;
    let mut period = None;
    for tag in event.tags.iter() {
        let s = tag.as_slice();
        match s.first().map(String::as_str) {
            Some("d") if d.is_none() => d = s.get(1).cloned(),
            Some("title") if title.is_none() => title = s.get(1).cloned(),
            Some("summary") if summary.is_none() => summary = s.get(1).cloned(),
            Some("operator") if operator.is_none() => operator = s.get(1).cloned(),
            Some("price") if amount_sat.is_none() => {
                let amount = s.get(1).ok_or(Error::Missing("price.amount"))?;
                let currency = s.get(2).ok_or(Error::Missing("price.currency"))?;
                let frequency = s.get(3).ok_or(Error::Missing("price.frequency"))?;
                if currency != "SAT" {
                    return Err(Error::InvalidPriceCurrency {
                        found: currency.clone(),
                    });
                }
                amount_sat =
                    Some(
                        amount
                            .parse::<u64>()
                            .map_err(|_| Error::InvalidPriceAmount {
                                found: amount.clone(),
                            })?,
                    );
                period = Some(frequency.clone());
            }
            // Unknown tags and later duplicates of single-valued tags are tolerated
            // (forward-compat, §5.4). NIP-01 addressability keys parameterized events by the
            // first `d` tag, so parse the first occurrence for all single-valued fields.
            _ => {}
        }
    }

    let d = d.ok_or(Error::Missing("d"))?;
    // A present-but-empty `d` makes a malformed coordinate `30402:<pubkey>:` — reject it like a
    // missing one (codex review).
    if d.is_empty() {
        return Err(Error::Missing("d"));
    }
    // The operation/params arrays are bounded as they deserialize (`de_operations` /
    // `de_params`), so an oversized listing surfaces here as `Error::Json` (SPEC.md §5.4).
    let content: Content = serde_json::from_str(&event.content)?;
    let meta = content.lnrent;
    if meta.version != SCHEMA_VERSION {
        return Err(Error::UnsupportedSchemaVersion {
            found: meta.version,
            supported: SCHEMA_VERSION,
        });
    }

    let operator = operator.ok_or(Error::Missing("operator"))?;
    // The operator tag is a master pubkey (§5.4); reject a non-pubkey before any consumer trusts
    // it (codex review). The listing_id is still derived from event.pubkey, not this field.
    if PublicKey::from_hex(&operator).is_err() {
        return Err(Error::InvalidOperator { found: operator });
    }
    let listing = Listing {
        operator,
        recipe_id: meta.recipe.id,
        recipe_version: meta.recipe.version,
        title: title.ok_or(Error::Missing("title"))?,
        summary: summary.ok_or(Error::Missing("summary"))?,
        amount_sat: amount_sat.ok_or(Error::Missing("price.amount"))?,
        period: period.ok_or(Error::Missing("price.frequency"))?,
        params: meta.params,
        operations: meta.operations,
        tier: meta.tier,
        version: meta.version,
        d: d.clone(),
    };
    let listing_id = listing_coordinate(&event.pubkey.to_hex(), &d);
    Ok(ParsedListing {
        listing,
        listing_id,
    })
}
