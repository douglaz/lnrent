//! `lnrent-buyer`: the BUYER's agent surface (ADR-0014, SPEC.md §4.7, lnrent-7fp.13). A complete,
//! AI-free, non-interactive `--json` CLI over NIP-17 gift-wrapped DMs + NIP-99 listings — NOT MCP.
//! It discovers listings, places orders, surfaces the invoice (NEVER pays), awaits provisioning,
//! checks status, and runs management ops, all over `lnrent-buyer-core`. Mirrors the operator CLI's
//! envelope (daemon/src/bin/lnrent.rs): success → stdout `{"ok":true,"data":…}`, failure → stderr
//! `{"ok":false,"error":{code,message,retryable}}`, with a deterministic exit code.

mod relay;

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use lnrent_buyer_core::{BuyerClient, BuyerError};
use lnrent_wire::{Keys, ParsedListing, PublicKey};
use nostr_sdk::ToBech32;
use serde_json::{json, Value};

use crate::relay::{NostrRelay, SysClock};

#[derive(Parser)]
#[command(
    name = "lnrent-buyer",
    about = "lnrent buyer CLI (agent-grade; talks to operators over Nostr; never pays)"
)]
struct Cli {
    /// Emit machine-readable JSON (stable fields) instead of human text.
    #[arg(long, global = true)]
    json: bool,
    /// Relay websocket URL (e.g. wss://relay.example).
    #[arg(long, global = true, env = "LNRENT_BUYER_RELAY")]
    relay: Option<String>,
    /// Operator pubkey (hex or npub) — the DM peer and the listing author.
    #[arg(long, global = true, env = "LNRENT_BUYER_OPERATOR")]
    operator: Option<String>,
    /// Buyer key file (nsec/hex). Falls back to LNRENT_BUYER_KEY_FILE, then LNRENT_BUYER_NSEC.
    #[arg(long, global = true, env = "LNRENT_BUYER_KEY_FILE")]
    key_file: Option<String>,
    /// Per-exchange deadline in seconds (relay round-trips).
    #[arg(long, global = true, default_value_t = 30)]
    timeout: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Manage the local buyer identity (key file).
    Identity {
        #[command(subcommand)]
        cmd: IdentityCmd,
    },
    /// Discover the operator's listings (or `listings get <id>`).
    Listings {
        #[command(subcommand)]
        cmd: Option<ListingsCmd>,
    },
    /// Place / await an order.
    Order {
        #[command(subcommand)]
        cmd: OrderCmd,
    },
    /// Subscription status. NOTE: this triggers an operator-side re-delivery of the latest
    /// provision.ready (a side-effecting round-trip, not a pure read) — M1a has no read-only status.
    Subs {
        #[command(subcommand)]
        cmd: SubsCmd,
    },
    /// Request a renewal invoice on demand.
    Renew {
        sub_id: String,
        /// Idempotency key for safe retries (see `order create --request-id`).
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Delivery operations (resend the latest credentials).
    Delivery {
        #[command(subcommand)]
        cmd: DeliveryCmd,
    },
    /// Send sub.cancel. Fire-and-forget; confirmation arrives later as billing.notice.
    Cancel { sub_id: String },
    /// Management operations: `ops <sub> list`, or `ops <sub> <op> [--params-json …]`.
    Ops {
        sub_id: String,
        /// The operation name, or the literal `list` to enumerate the published ops.
        op: String,
        #[arg(long)]
        params_json: Option<String>,
        /// Idempotency key for safe retries (see `order create --request-id`).
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum IdentityCmd {
    /// Generate a new buyer key, writing it 0600 to the key file. Prints the npub, never the secret.
    New,
    /// Print the buyer npub for the configured key.
    Show,
}

#[derive(Subcommand)]
enum ListingsCmd {
    /// Fetch one listing by coordinate `30402:<pubkey>:<d>`.
    Get { id: String },
}

#[derive(Subcommand)]
enum OrderCmd {
    /// Place an order; returns the invoice (bolt11 + amount + expiry). The buyer settles it itself.
    Create {
        listing_id: String,
        #[arg(long)]
        params_json: Option<String>,
        #[arg(long)]
        refund_dest: Option<String>,
        /// Idempotency key: reuse the SAME value to retry after a relay_timeout/transport error
        /// without creating a duplicate order — the operator dedupes on (sender, request_id)
        /// (SPEC §5.1). Omit for a fresh random id (NOT safe to blindly retry).
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Await provision.ready for an order (the order id IS the subscription id).
    Wait { order_id: String },
}

#[derive(Subcommand)]
enum SubsCmd {
    /// Current delivery state — re-sends the latest provision.ready (side-effecting; see `subs`).
    Status { sub_id: String },
}

#[derive(Subcommand)]
enum DeliveryCmd {
    /// Ask the operator to re-send the latest credentials.
    Resend { sub_id: String },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => return handle_parse_error(e, std::env::args().skip(1)),
    };
    let json = cli.json;
    match run(cli).await {
        Ok(data) => {
            render_ok(data, json);
            ExitCode::SUCCESS
        }
        Err(err) => render_err(&err, json),
    }
}

/// Whether the raw argv requested `--json`. The clap parse FAILED, so we can't read `cli.json` —
/// scan the tokens directly (a bare bool flag; `--` before it makes `--json` positional, which a
/// parse-error argv would not sanely contain, so a literal token match is the pragmatic contract).
fn argv_has_json(args: impl Iterator<Item = String>) -> bool {
    args.take_while(|a| a != "--").any(|a| a == "--json")
}

/// Keep the `--json` machine contract on a clap ARGV PARSE FAILURE (lnrent-y4m.14): the buyer CLI
/// is agent-driven, so a bad-flags failure must not escape as clap's plaintext exit **2** — which
/// both breaks the `{ok:false,error:{…}}` envelope AND collides with the taxonomy's exit 2 =
/// `not_found`. When `--json` is present, emit a `bad_request` envelope (matching
/// `BuyerError::BadRequest`'s exit **3**) to stderr; otherwise fall through to clap's human
/// usage/exit. Only an EXPLICIT `--help`/`--version` bypasses JSON — those are not errors, so
/// render clap's normal output (exit 0) even under `--json`, since an agent asking for help wants
/// the help text. Notably NOT `DisplayHelpOnMissingArgumentOrSubcommand` (codex PR-41 review): a
/// MISSING required subcommand (e.g. `lnrent-buyer --json order`) is a genuine parse failure an
/// agent must receive as the `bad_request` envelope, not clap's help/plaintext — so it falls
/// through to the `--json` check below.
fn handle_parse_error(e: clap::Error, args: impl Iterator<Item = String>) -> ExitCode {
    use clap::error::ErrorKind;
    if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
        e.exit(); // prints help/version to stdout, exits 0 — diverges
    }
    if argv_has_json(args) {
        // The specific failure is clap's first rendered line ("error: unexpected argument …"); the
        // trailing usage block is dropped so the machine message stays a single actionable line.
        let rendered = e.to_string();
        let message = rendered
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .unwrap_or("argv parse error")
            .trim_start_matches("error:")
            .trim();
        eprintln!(
            "{}",
            json!({ "ok": false, "error": { "code": "bad_request", "message": message, "retryable": false } })
        );
        return ExitCode::from(3);
    }
    e.exit(); // clap's human usage to stderr, its default exit 2 — diverges
}

async fn run(cli: Cli) -> Result<Value, BuyerError> {
    // Identity verbs are local — no relay/operator needed.
    if let Cmd::Identity { cmd } = &cli.cmd {
        return match cmd {
            IdentityCmd::New => identity_new(&cli),
            IdentityCmd::Show => identity_show(&cli),
        };
    }

    // Everything else needs a key, a relay, and an operator.
    let keys = load_keys(&cli)?;
    let operator = resolve_operator(&cli)?;
    let timeout = Duration::from_secs(cli.timeout);
    let relay = build_relay(&cli, keys.clone(), timeout).await?;
    // The active verb's `--request-id` (if any) is threaded through the Clock seam: buyer-core mints
    // each request id via `Clock::new_request_id`, so a fixed id makes a retried order/renew/op reuse
    // the operator's `(sender, request_id)` dedup key (SPEC §5.1) instead of duplicating.
    let clock = SysClock::with_request_id(request_id_for(&cli.cmd));
    let buyer = BuyerClient::new(&relay, &keys, &clock, operator, timeout);

    match cli.cmd {
        Cmd::Identity { .. } => unreachable!("handled above"),
        Cmd::Listings { cmd: None } => {
            let listings = buyer.discover_listings().await?;
            Ok(json!({ "listings": listings.iter().map(listing_json).collect::<Vec<_>>() }))
        }
        Cmd::Listings {
            cmd: Some(ListingsCmd::Get { id }),
        } => {
            let listing = buyer.get_listing(&id).await?;
            Ok(listing_json(&listing))
        }
        Cmd::Order {
            cmd:
                OrderCmd::Create {
                    listing_id,
                    params_json,
                    refund_dest,
                    // request_id was consumed into the Clock above.
                    ..
                },
        } => {
            let params = parse_params(params_json.as_deref())?;
            let inv = buyer.create_order(&listing_id, params, refund_dest).await?;
            to_data(&inv)
        }
        Cmd::Order {
            cmd: OrderCmd::Wait { order_id },
        } => {
            let ready = buyer.wait_provision(&order_id).await?;
            to_data(&ready)
        }
        Cmd::Subs {
            cmd: SubsCmd::Status { sub_id },
        } => {
            // No subscription-status message exists in M1a; the re-delivered provision.ready is the
            // current delivered state. Flag the source so an agent isn't misled.
            let ready = buyer.resend_delivery(&sub_id).await?;
            let mut data = to_data(&ready)?;
            if let Value::Object(map) = &mut data {
                map.insert("source".into(), json!("delivery.resend"));
            }
            Ok(data)
        }
        Cmd::Renew { sub_id, .. } => {
            let inv = buyer.renew(&sub_id).await?;
            to_data(&inv)
        }
        Cmd::Delivery {
            cmd: DeliveryCmd::Resend { sub_id },
        } => {
            let ready = buyer.resend_delivery(&sub_id).await?;
            let mut data = to_data(&ready)?;
            if let Value::Object(map) = &mut data {
                map.insert("resent".into(), json!(true));
            }
            Ok(data)
        }
        Cmd::Cancel { sub_id } => {
            buyer.cancel(&sub_id).await?;
            Ok(json!({
                "subscription_id": sub_id,
                "sent": true,
                "note": "sub.cancel sent; confirmation arrives later as billing.notice",
            }))
        }
        Cmd::Ops {
            sub_id,
            op,
            params_json,
            // request_id was consumed into the Clock above.
            ..
        } => {
            let ops = buyer.list_ops().await?;
            if op == "list" {
                return Ok(json!({
                    "subscription_id": sub_id,
                    "operations": ops.iter().map(|o| json!({
                        "name": o.name, "label": o.label, "kind": o.kind, "params": o.params,
                    })).collect::<Vec<_>>(),
                }));
            }
            // Resolve the published kind so an interactive op is refused before any send (advisory:
            // an op absent from the listing is left for the operator to authorize/reject).
            let op_kind = ops.iter().find(|o| o.name == op).map(|o| o.kind.as_str());
            let params = parse_params(params_json.as_deref())?;
            let result = buyer.invoke_op(&sub_id, &op, op_kind, params).await?;
            to_data(&result)
        }
    }
}

// -- identity ----------------------------------------------------------------------------------

fn identity_new(cli: &Cli) -> Result<Value, BuyerError> {
    let path = cli.key_file.clone().ok_or_else(|| {
        BuyerError::BadRequest(
            "identity new needs a path: pass --key-file or set LNRENT_BUYER_KEY_FILE".into(),
        )
    })?;
    let keys = Keys::generate();
    let nsec = keys
        .secret_key()
        .to_bech32()
        .map_err(|e| BuyerError::Internal(format!("encoding nsec: {e}")))?;
    write_secret_0600(&path, &nsec)?;
    Ok(json!({
        "npub": npub(&keys)?,
        "pubkey": keys.public_key().to_hex(),
        "key_file": path,
    }))
}

fn identity_show(cli: &Cli) -> Result<Value, BuyerError> {
    let keys = load_keys(cli)?;
    Ok(json!({ "npub": npub(&keys)?, "pubkey": keys.public_key().to_hex() }))
}

fn npub(keys: &Keys) -> Result<String, BuyerError> {
    keys.public_key()
        .to_bech32()
        .map_err(|e| BuyerError::Internal(format!("encoding npub: {e}")))
}

/// Write a secret to `path` with mode 0600, refusing to clobber an existing file (atomic via
/// O_CREAT|O_EXCL). The secret is NEVER printed.
fn write_secret_0600(path: &str, contents: &str) -> Result<(), BuyerError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if Path::new(path).exists() {
        return Err(BuyerError::InvalidState(format!(
            "key file {path} already exists; refusing to overwrite"
        )));
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| BuyerError::Internal(format!("creating key file {path}: {e}")))?;
    writeln!(f, "{contents}")
        .map_err(|e| BuyerError::Internal(format!("writing key file: {e}")))?;
    Ok(())
}

// -- resolution helpers ------------------------------------------------------------------------

fn load_keys(cli: &Cli) -> Result<Keys, BuyerError> {
    if let Some(path) = &cli.key_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| BuyerError::BadRequest(format!("reading key file {path}: {e}")))?;
        return Keys::parse(raw.trim())
            .map_err(|e| BuyerError::BadRequest(format!("parsing key file {path}: {e}")));
    }
    if let Ok(nsec) = std::env::var("LNRENT_BUYER_NSEC") {
        return Keys::parse(nsec.trim())
            .map_err(|e| BuyerError::BadRequest(format!("parsing LNRENT_BUYER_NSEC: {e}")));
    }
    Err(BuyerError::BadRequest(
        "no buyer key: pass --key-file, or set LNRENT_BUYER_KEY_FILE / LNRENT_BUYER_NSEC".into(),
    ))
}

/// The idempotency key (`--request-id`) the active verb carries, if any. Only the request-minting
/// verbs (`order create`, `renew`, `ops`) expose it; everything else is naturally re-runnable, so it
/// is `None`. Threaded into [`SysClock::with_request_id`] so a retried request reuses the operator's
/// `(sender, request_id)` dedup key (SPEC §5.1) instead of duplicating the order/reservation/op.
fn request_id_for(cmd: &Cmd) -> Option<String> {
    match cmd {
        Cmd::Order {
            cmd: OrderCmd::Create { request_id, .. },
        } => request_id.clone(),
        Cmd::Renew { request_id, .. } => request_id.clone(),
        Cmd::Ops { request_id, .. } => request_id.clone(),
        _ => None,
    }
}

fn resolve_operator(cli: &Cli) -> Result<PublicKey, BuyerError> {
    let raw = cli.operator.as_deref().ok_or_else(|| {
        BuyerError::BadRequest("no operator: pass --operator or set LNRENT_BUYER_OPERATOR".into())
    })?;
    PublicKey::parse(raw)
        .map_err(|e| BuyerError::BadRequest(format!("invalid operator pubkey `{raw}`: {e}")))
}

async fn build_relay(cli: &Cli, keys: Keys, timeout: Duration) -> Result<NostrRelay, BuyerError> {
    let url = cli.relay.as_deref().ok_or_else(|| {
        BuyerError::BadRequest("no relay: pass --relay or set LNRENT_BUYER_RELAY".into())
    })?;
    NostrRelay::connect(keys, url, timeout)
        .await
        .map_err(|e| BuyerError::Transport(format!("connecting to relay {url}: {e}")))
}

fn parse_params(raw: Option<&str>) -> Result<Value, BuyerError> {
    match raw {
        None => Ok(json!({})),
        Some(s) => {
            let v: Value = serde_json::from_str(s).map_err(|e| {
                BuyerError::BadRequest(format!("--params-json is not valid JSON: {e}"))
            })?;
            if v.is_object() {
                Ok(v)
            } else {
                Err(BuyerError::BadRequest(
                    "--params-json must be a JSON object".into(),
                ))
            }
        }
    }
}

// -- rendering ---------------------------------------------------------------------------------

/// Serialize a wire reply (OrderInvoice / ProvisionReady / …) into the `data` payload.
fn to_data<T: serde::Serialize>(value: &T) -> Result<Value, BuyerError> {
    serde_json::to_value(value).map_err(|e| BuyerError::Internal(format!("rendering reply: {e}")))
}

/// A listing rendered for the `--json` surface. `Listing` is not directly `Serialize` (its wire form
/// is a 30402 event), so build the stable buyer-facing shape by hand; `ParamDecl`/`OperationDecl`
/// serialize directly.
fn listing_json(parsed: &ParsedListing) -> Value {
    let l = &parsed.listing;
    json!({
        "listing_id": parsed.listing_id,
        "d": l.d,
        "operator": l.operator,
        "recipe_id": l.recipe_id,
        "recipe_version": l.recipe_version,
        "title": l.title,
        "summary": l.summary,
        "amount_sat": l.amount_sat,
        "period": l.period,
        "tier": l.tier,
        "params": l.params,
        "operations": l.operations,
        "version": l.version,
    })
}

fn render_ok(data: Value, as_json: bool) {
    if as_json {
        // stdout stays clean for `| jq`.
        println!("{}", json!({ "ok": true, "data": data }));
    } else {
        match &data {
            Value::Null => println!("ok"),
            v => println!(
                "{}",
                serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
            ),
        }
    }
}

fn render_err(err: &BuyerError, as_json: bool) -> ExitCode {
    let env = err.envelope();
    if as_json {
        // Errors go to stderr so a piped `--json` stdout stays clean.
        eprintln!(
            "{}",
            json!({ "ok": false, "error": { "code": env.code, "message": env.message, "retryable": env.retryable } })
        );
    } else {
        eprintln!("lnrent-buyer: {} ({})", env.message, env.code);
    }
    ExitCode::from(err.exit_code())
}

#[cfg(test)]
mod tests {
    use super::argv_has_json;

    #[test]
    fn argv_has_json_detects_the_flag_before_a_terminator() {
        let has = |v: &[&str]| argv_has_json(v.iter().map(|s| s.to_string()));
        assert!(has(&["listings", "--json"]));
        assert!(has(&["--json", "listings"]));
        assert!(!has(&["listings"]));
        // `--` ends option parsing, so a `--json` AFTER it is positional, not the flag.
        assert!(!has(&["listings", "--", "--json"]));
        assert!(has(&["--json", "--", "positional"]));
    }
}
