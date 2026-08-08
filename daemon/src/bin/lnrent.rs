//! `lnrent`: the operator CLI — the OPERATOR's agent surface (ADR-0014). It talks to lnrentd
//! over a Unix-domain socket (the daemon is the sole writer, ADR-0001); it never touches
//! sqlite directly. Every command supports `--json`, runs non-interactively, and returns a
//! deterministic exit code so an operator agent can drive it. No MCP/HTTP server.

use clap::{Parser, Subcommand};
use lnrentd::ipc::{self, Reply, Request};
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "lnrent",
    version,
    about = "lnrent operator CLI (agent-grade; talks to lnrentd over a unix socket)"
)]
struct Cli {
    /// Emit machine-readable JSON (stable fields) instead of human text.
    #[arg(long, global = true)]
    json: bool,
    /// Daemon data dir (the socket is <data-dir>/lnrent.sock).
    #[arg(long, global = true, env = "LNRENT_DATA_DIR", default_value = "./data")]
    data_dir: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Daemon status (recipe + subscription counts).
    Status,
    /// List loaded recipes.
    Recipes,
    /// Show the daemon's ecash position: ledger-expected holdings, gateway, federation, and
    /// refund-liability coverage. Network-free apart from the gateway/federation liveness probes.
    Money,
    /// Reconcile the live federation wallet against the ledger books (the ONLY command that reads the
    /// wallet balance): reports wallet vs expected holdings + an OK/DRIFT/UNKNOWN verdict (UNKNOWN =
    /// a backend with no observable balance, e.g. the mock). Report-only.
    Reconcile,
    /// Preflight the three EXTERNAL go-live dependencies — refund gateway, federation guardians,
    /// provider API token (DO_TOKEN) — with per-check pass/fail + diagnostics. Exits nonzero when
    /// any check fails, so an operator agent can gate subsequent launch promotion on it. Read-only:
    /// `listing publish` runs the same checks and is what actually gates publication. Alias: `doctor`.
    #[command(alias = "doctor")]
    Preflight,
    /// Publish or withdraw your public 30402 listing. A fresh daemon starts QUIET — until you
    /// `listing publish`, no buyer can order. Publishing persists: every later restart republishes
    /// on its own.
    Listing {
        #[command(subcommand)]
        cmd: ListingCmd,
    },
    /// List subscriptions.
    Subs,
    /// Inspect one subscription.
    Sub { id: String },
    /// List OPEN teardown dead-letters: failed `destroy` hooks + stuck provision cleanups. A
    /// provider resource (e.g. a droplet) may still be billing until these resolve.
    Teardowns,
    /// Show per-relay connectivity (the out-of-band read for a relay blackout: the alert cannot be
    /// delivered while the pool is down).
    Relays,
    /// List non-terminal + parked refunds (the per-item view behind `money`'s parked_count).
    Refunds,
    /// Re-drive one parked (FAILED) refund: reset it to PENDING so the refunder retries the real
    /// resolver + capped-pay path. The only refund actuator — there is no cancel/abandon.
    #[command(name = "refund-retry")]
    RefundRetry { id: String },
    /// Sweep operator profit to your own bolt11 from ledger SURPLUS (sales − reserves − payouts),
    /// capped so it can never overspend. Quotes by default (prints the surplus breakdown + verdict);
    /// pays only with --yes. Authorized from the ledger only — the federation balance is never read.
    Sweep {
        /// The operator's own bolt11 invoice to pay (must carry an amount).
        bolt11: String,
        /// Execute the sweep (default is a dry-run quote only).
        #[arg(long)]
        yes: bool,
    },
    /// Admin: force-suspend a subscription.
    Suspend { id: String },
    /// Admin: force-resume a suspended subscription.
    Resume { id: String },
    /// Dev-only commands. Require LNRENT_DEV=1 and mock payment backend support.
    Dev {
        #[command(subcommand)]
        cmd: DevCmd,
    },
}

#[derive(Subcommand)]
enum DevCmd {
    /// Settle the open MockPayment invoice for a subscription.
    Settle { subscription_id: String },
}

#[derive(Subcommand)]
enum ListingCmd {
    /// GO LIVE: run preflight, then publish the 30402 so buyers can discover + order. REFUSED on a
    /// structural failure (your own misconfiguration — no override) and on an unverified dependency
    /// unless you pass --accept-unverified. Persists across restarts.
    Publish {
        /// Publish even though a dependency (guardians, a gateway, the provider API, phoenixd)
        /// could not be reached. Use when a third party's outage is the only thing in the way.
        /// If it is still down when a buyer orders, the cost differs by dependency: a
        /// payment-backend outage refuses the order, but a provider outage lets the buyer pay and
        /// be refunded net of fees.
        #[arg(long)]
        accept_unverified: bool,
    },
    /// STOP TAKING ORDERS: mark the listing withdrawn (order intake refuses immediately) and ask the
    /// relays to drop the 30402, best-effort. Persists across restarts — nothing republishes it
    /// until you `listing publish` again.
    Withdraw,
}

/// Exit-code taxonomy (agent-grade, ADR-0014): 0 ok; 1 preflight check(s) failed (lnrent-y4m.9) or
/// an unrecognized daemon error code; 2 not_found; 3 bad_request/invalid_state;
/// 4 ipc/connection failure OR a graceful-shutdown restart race (lnrent-j3c); 5 internal.
fn exit_for(err_code: &str) -> ExitCode {
    ExitCode::from(exit_code_for(err_code))
}

/// The raw error-code → exit-number mapping, split out from [`exit_for`] so it is unit-testable
/// (`std::process::ExitCode` is neither `PartialEq` nor introspectable).
fn exit_code_for(err_code: &str) -> u8 {
    match err_code {
        "not_found" => 2,
        // Request-level refusals the operator can act on, incl. the structured sweep refusals
        // (gate1-operator-sweep, urw.3): a bad/zero invoice, an unpriceable quote, another sweep in
        // flight, an insufficient surplus, a fee rise past the quote, or an in-flight-unconfirmed pay.
        // ... and the publication-gate refusals (lnrent-i23): `listing_blocked` is the operator's own
        // misconfiguration (fix it — there is no override), `listing_unverified` is a dependency that
        // could not be reached (retry, or re-run with --accept-unverified). Both are decisions for
        // the operator, not daemon failures, so both land on 3 with the rest of them.
        "bad_request" | "invalid_state" | "dev_disabled" | "unsupported" | "sweep_invalid"
        | "sweep_unpriceable" | "sweep_busy" | "sweep_insufficient" | "sweep_fee_rose"
        | "sweep_in_flight" | "listing_blocked" | "listing_unverified" => 3,
        // A read-only request cancelled because the daemon is gracefully shutting down (lnrent-j3c):
        // a TRANSIENT restart race (the reply carries retryable:true), not a hard failure. Map it to
        // the same transient IPC/connection exit as an unreachable daemon (`ipc_unreachable`) so shell
        // automation retries against the replacement daemon instead of classifying a restart as a
        // failed gate (which the default exit 1 would).
        "shutting_down" => 4,
        "internal" => 5,
        _ => 1,
    }
}

#[derive(Clone, Copy)]
enum HumanRender {
    Generic,
    Money,
    Reconcile,
    Preflight,
    Teardowns,
    Refunds,
    Relays,
    Sweep,
    Listing,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let sock = format!("{}/lnrent.sock", cli.data_dir);
    // `sweep` is quote-then-confirm: it makes TWO IPC calls (a dry-run quote, then execute on --yes),
    // so it doesn't fit the single request->reply flow below (gate1-operator-sweep, urw.3).
    if let Cmd::Sweep { bolt11, yes } = &cli.cmd {
        return run_sweep(&sock, bolt11.clone(), *yes, cli.json).await;
    }
    // `preflight` is one request->reply, but its exit code is gated on the AGGREGATE verdict
    // INSIDE the data (lnrent-y4m.9): a healthy IPC round-trip carrying a failed check must still
    // exit nonzero so an agent can gate subsequent launch promotion on it.
    if let Cmd::Preflight = &cli.cmd {
        return run_preflight(&sock, cli.json).await;
    }
    let (req, human_render) = match cli.cmd {
        Cmd::Status => (Request::Status, HumanRender::Generic),
        Cmd::Recipes => (Request::Recipes, HumanRender::Generic),
        Cmd::Money => (Request::Money, HumanRender::Money),
        Cmd::Reconcile => (Request::Reconcile, HumanRender::Reconcile),
        // Handled by the aggregate-gated early return above.
        Cmd::Preflight => unreachable!("preflight is dispatched before this match"),
        Cmd::Subs => (Request::Subs, HumanRender::Generic),
        Cmd::Sub { id } => (Request::Sub { id }, HumanRender::Generic),
        Cmd::Teardowns => (Request::Teardowns, HumanRender::Teardowns),
        Cmd::Relays => (Request::Relays, HumanRender::Relays),
        Cmd::Refunds => (Request::Refunds, HumanRender::Refunds),
        Cmd::RefundRetry { id } => (Request::RefundRetry { id }, HumanRender::Generic),
        Cmd::Listing {
            cmd: ListingCmd::Publish { accept_unverified },
        } => (
            Request::ListingPublish { accept_unverified },
            HumanRender::Listing,
        ),
        Cmd::Listing {
            cmd: ListingCmd::Withdraw,
        } => (Request::ListingWithdraw, HumanRender::Listing),
        Cmd::Suspend { id } => (Request::AdminSuspend { id }, HumanRender::Generic),
        Cmd::Resume { id } => (Request::AdminResume { id }, HumanRender::Generic),
        Cmd::Dev {
            cmd: DevCmd::Settle { subscription_id },
        } => (Request::DevSettle { subscription_id }, HumanRender::Generic),
        // Handled by the quote-then-confirm early return above.
        Cmd::Sweep { .. } => unreachable!("sweep is dispatched before this match"),
    };

    match ipc::call(&sock, req).await {
        Ok(reply) => render(reply, cli.json, human_render),
        Err(e) => {
            // The daemon isn't reachable — a structured, deterministic failure (retryable:
            // the daemon may come up). Errors go to stderr so `--json` stdout stays clean.
            if cli.json {
                eprintln!(
                    "{}",
                    serde_json::json!({"ok": false, "error": {"code": "ipc", "message": e.to_string(), "retryable": true}})
                );
            } else {
                eprintln!("lnrent: cannot reach lnrentd at {sock}: {e}");
            }
            ExitCode::from(4)
        }
    }
}

fn render(reply: Reply, as_json: bool, human_render: HumanRender) -> ExitCode {
    if as_json {
        // Stable shape on success AND failure; errors go to stderr so piped `--json` stdout
        // stays clean for `| jq` (§4.7).
        let s = serde_json::to_string(&reply).unwrap();
        if reply.ok {
            println!("{s}");
        } else {
            eprintln!("{s}");
        }
    } else if reply.ok {
        match reply.data {
            Some(serde_json::Value::Null) | None => println!("ok"),
            Some(v) => match human_render {
                HumanRender::Generic => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
                HumanRender::Money => render_money_human(&v),
                HumanRender::Reconcile => render_reconcile_human(&v),
                HumanRender::Preflight => render_preflight_human(&v),
                HumanRender::Teardowns => render_teardowns_human(&v),
                HumanRender::Refunds => render_refunds_human(&v),
                HumanRender::Relays => render_relays_human(&v),
                HumanRender::Sweep => render_sweep_human(&v),
                HumanRender::Listing => render_listing_human(&v),
            },
        }
    } else if let Some(err) = &reply.error {
        // A refusal may carry the per-check diagnostics behind it (the lnrent-i23 publication gate
        // does). Print them: the REMEDY an operator needs lives in each check's `detail`, and the
        // one-line message only names which checks refused.
        if let Some(failed) = reply
            .data
            .as_ref()
            .and_then(|d| d.get("failed"))
            .and_then(serde_json::Value::as_array)
        {
            for c in failed {
                let s = |k: &str| c.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
                eprintln!("  \u{00d7} {} \u{b7} {}", s("name"), s("detail"));
            }
        }
        eprintln!("lnrent: {} ({})", err.message, err.code);
    }
    match &reply.error {
        Some(err) => exit_for(&err.code),
        None => ExitCode::SUCCESS,
    }
}

fn render_money_human(v: &serde_json::Value) {
    println!("{}", money_human_text(v));
}

fn money_human_text(v: &serde_json::Value) -> String {
    // §E: the balance operand is the LEDGER lower bound (`expected_msat`), not a live wallet read.
    // Wallet-vs-books drift is the `reconcile` command's job.
    let expected = v
        .get("expected_msat")
        .and_then(serde_json::Value::as_u64)
        .map(|n| format!("{n} msat"))
        .unwrap_or_else(|| "unknown".to_string());
    let ok_str = |k: &str| {
        if v.get(k).and_then(serde_json::Value::as_bool).unwrap_or(false) {
            "ok"
        } else {
            "not ok"
        }
    };
    let gateway = ok_str("gateway_ok");
    let federation = ok_str("federation_ok");
    let gross = v
        .get("gross_liability_sat")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let required = v
        .get("required_msat")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let parked = v
        .get("parked_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let ready = v
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let degraded = v
        .get("degraded_read_only")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let warning = v.get("warning").and_then(serde_json::Value::as_str);
    // lnrent-p2e: these optional fields exist only when the already-run readiness seams carried a
    // typed phoenixd failure. Stable machine keys retain their historical names; the human view must
    // not send a phoenixd operator looking for a federation or gateway their deployment does not
    // have. `readiness_failure_detail` is preflight's sanitized diagnostic and remedy verbatim.
    let phoenixd_detail = match (
        v.get("readiness_failure_backend")
            .and_then(serde_json::Value::as_str),
        v.get("readiness_failure_detail")
            .and_then(serde_json::Value::as_str),
    ) {
        (Some("phoenixd"), Some(detail)) => Some(detail),
        _ => None,
    };

    // Label on the CONFIGURED backend, which the daemon reports in every readiness state — NOT on
    // the presence of a failure detail, which exists only when something failed (codex on PR #66).
    let is_phoenixd = v
        .get("readiness_backend")
        .and_then(serde_json::Value::as_str)
        == Some("phoenixd")
        || phoenixd_detail.is_some();

    let mut lines = vec![format!("Expected holdings (ledger): {expected}")];
    if is_phoenixd {
        lines.push(format!("Phoenixd node: {federation}"));
        lines.push(format!("Refund pay: {gateway}"));
    } else {
        lines.push(format!("Federation: {federation}"));
        lines.push(format!("Gateway: {gateway}"));
    }
    lines.push(format!(
        "Outstanding liabilities: {gross} sat gross, {required} msat required"
    ));
    lines.push(format!("Parked count: {parked}"));
    let unbookable = v
        .get("recent_unbookable_settlement_alerts")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let unbookable_unknown = v
        .get("recent_unbookable_settlement_alerts_unknown")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let unbookable_disabled = v
        .get("recent_unbookable_settlement_alerts_disabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if unbookable_disabled {
        // A DIFFERENT remedy from the storage-failure case below, so it gets its own line: nothing
        // is broken, the operator switched the recording off, and this view is derived from it.
        lines.push(
            "\x1b[1;31mUnbookable settlement alert history: UNAVAILABLE\x1b[0m — DM alerts are \
             disabled (`LNRENT_ALERTS_ENABLED`), so nothing is recorded and this view cannot tell \
             you whether a paid settlement is sitting unbooked. This is NOT a report of zero. \
             Re-enable alerts, or check phoenixd's own records directly."
                .to_string(),
        );
    } else if unbookable_unknown {
        lines.push(
            "\x1b[1;31mUnbookable settlement alert history: UNKNOWN\x1b[0m — the daemon could not \
             read its durable alert history, so it cannot say whether settlements are being held \
             back — this is NOT a report of zero. Check the daemon's storage error, then re-run. \
             (`lnrent status` carries the same fields as raw JSON, without this framing.)"
                .to_string(),
        );
    } else if unbookable > 0 {
        // CONDITIONS, not receipts. The daemon dedupes this history per alert subject
        // (`alerts.rs`), and both unbookable-settlement subjects are deliberately global — one for
        // the whole wallet's fee credit, one for the whole diverged index (`phoenixd_backend.rs`) —
        // because neither judgement is per-receipt. So an unfunded wallet holding back fifty
        // receipts is ONE incident here, and a bare number would read as one stuck order.
        let noun = if unbookable == 1 {
            "condition"
        } else {
            "conditions"
        };
        lines.push(format!(
            // Deliberately NOT "the detail says which": neither detail enumerates the receipts it
            // covers — both say outright that they cover more than the one invoice they name — so
            // an operator sent looking for a list would find one example and fund for it alone.
            "\x1b[1;31mUnbookable settlements: {unbookable} {noun} alerting\x1b[0m \
             (alert history; may already be resolved. Each covers every receipt it holds back — \
             the detail says what to do.)"
        ));
        let alerts = v
            .get("recent_unbookable_settlement_alert_details")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for a in alerts {
            let s = |k: &str| a.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
            let at = a.get("at").and_then(serde_json::Value::as_i64).unwrap_or(0);
            lines.push(format!(
                "  \u{b7} alerted_at={at} \u{b7} {} \u{b7} {}",
                s("subject"),
                s("detail")
            ));
        }
    }
    // The degraded/read-only latch (lnrent-y4m.3) takes precedence over reserve readiness: the daemon
    // is refusing money writes after a fatal DB error, so a human operator must see it here — not only
    // in the daemon log — regardless of whether reserves are sufficient.
    if degraded {
        lines.push(
            "Status: \x1b[1;31mDEGRADED (read-only)\x1b[0m — money writes refused after a fatal DB \
             error; restore the state DB from backup and restart"
                .to_string(),
        );
    } else if ready {
        // `ready` is REFUND-LIABILITY readiness: reserves cover the open refund liability. It says
        // nothing about receipts lnrent could not BOOK, and the fee-credit case is exactly one where
        // both hold at once — an unfunded wallet's first small sale leaves a paid receipt unbooked
        // while the (tiny) refund liability is still covered. A bare READY printed under the red
        // block above reads as an all-clear over a buyer's money sitting unbooked, so qualify the
        // human line. The machine field is untouched.
        if unbookable > 0 || unbookable_unknown || unbookable_disabled {
            lines.push(
                "Status: \x1b[1mREADY (refund liability only)\x1b[0m — but see the unbookable \
                 settlements above; this line does not cover them. That block is alert HISTORY, so \
                 a listed incident may already be resolved (or, if the history could not be read, \
                 unknown) — check before acting on it"
                    .to_string(),
            );
        } else {
            lines.push("Status: \x1b[1mREADY\x1b[0m".to_string());
        }
    } else if let Some(detail) = phoenixd_detail {
        lines.push(format!(
            "Status: \x1b[1mNOT READY (phoenixd)\x1b[0m — {detail}"
        ));
    } else {
        lines.push(format!(
            "Status: \x1b[1mNOT READY ({})\x1b[0m",
            warning.unwrap_or("unknown")
        ));
    }
    lines.join("\n")
}

/// Human render for `lnrent reconcile` (lnrent-urw.10 §F): the live wallet vs the ledger books and
/// the OK/DRIFT verdict. Report-only — a DRIFT verdict is a signal for a human to investigate.
fn render_reconcile_human(v: &serde_json::Value) {
    let msat = |k: &str| {
        v.get(k)
            .and_then(serde_json::Value::as_u64)
            .map(|n| format!("{n} msat"))
            .unwrap_or_else(|| "unknown".to_string())
    };
    let verdict = v.get("verdict").and_then(serde_json::Value::as_str).unwrap_or("?");

    println!("Wallet (federation): {}", msat("wallet_msat"));
    println!("Expected (ledger books): {}", msat("expected_msat"));
    match verdict {
        "OK" => println!("Verdict: \x1b[1mOK\x1b[0m (wallet covers the books)"),
        "DRIFT" => println!(
            "Verdict: \x1b[1mDRIFT\x1b[0m (wallet holds less than the books — investigate)"
        ),
        other => println!("Verdict: \x1b[1m{other}\x1b[0m (no observable wallet balance for this backend)"),
    }
}

/// `lnrent preflight` (alias `doctor`, lnrent-y4m.9): one IPC round-trip, then the exit code comes
/// from the AGGREGATE check verdict in the data — not just the IPC envelope — so `preflight` in a
/// go-live script fails the pipeline when any external dependency is broken.
async fn run_preflight(sock: &str, as_json: bool) -> ExitCode {
    match ipc::call(sock, Request::Preflight).await {
        Ok(reply) => {
            let failed = preflight_checks_failed(&reply);
            let code = render(reply, as_json, HumanRender::Preflight);
            if failed {
                ExitCode::from(1)
            } else {
                code
            }
        }
        Err(e) => ipc_unreachable(sock, e, as_json),
    }
}

/// The check names a healthy daemon's preflight report MUST contain (adversarial y4m.9 review):
/// exit-0 is an AGENT GATE, so the CLI validates the report STRUCTURALLY instead of trusting the
/// aggregate bit — a version-skewed or buggy daemon replying `ok:true` with missing or
/// contradictory checks must exit 1, never silently pass. Future daemon-side checks are accepted
/// (forward-compatible) but must each pass. `recipe_preflight` is required (lnrent-1sr): a current
/// daemon always emits it once a recipe is loaded (SKIP when the recipe declares no preflight hook),
/// so an older daemon that omits it — silently dropping the provisioning-param guard — must fail here.
///
/// `phoenixd` is deliberately NOT listed (lnrent-5mi): that check is emitted only for a phoenixd
/// backend, so globally requiring it would make every matching mock/Fedimint daemon fail. When it is
/// present, `every_check_passes` below still gates it; version-skew cannot be detected from a report
/// that intentionally carries no backend discriminator.
const PREFLIGHT_REQUIRED_CHECKS: [&str; 5] = [
    "gateway",
    "federation",
    "lnv2",
    "provider_token",
    "recipe_preflight",
];

/// PURE aggregate→exit mapping for `preflight`: exit 1 (distinct from the taxonomy codes 2..5)
/// unless the reply is a WELL-FORMED passing report — aggregate `ok: true`, a checks array in
/// which EVERY check has `ok: true` (a contradiction with the aggregate fails closed), and every
/// [`PREFLIGHT_REQUIRED_CHECKS`] name present. A malformed/absent/incomplete report counts as
/// failed — never silently a pass. An IPC-LEVEL error is not this path's job: `render`'s taxonomy
/// exit already covers it (hence `false` here).
fn preflight_checks_failed(reply: &Reply) -> bool {
    if !reply.ok {
        // A WELL-FORMED error reply keeps `render`'s taxonomy exit (2..5). But a deserializable
        // yet inconsistent envelope — ok:false with NO error object — would fall through render
        // at exit 0 (adversarial y4m.9 review): fail it here; exit 0 stays reserved for a
        // structurally passing report.
        return reply.error.is_none();
    }
    let Some(data) = reply.data.as_ref() else {
        return true;
    };
    if data.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return true;
    }
    let Some(checks) = data.get("checks").and_then(serde_json::Value::as_array) else {
        return true;
    };
    let every_check_passes = checks.iter().all(|c| {
        c.get("ok").and_then(serde_json::Value::as_bool) == Some(true)
            && c.get("name").and_then(serde_json::Value::as_str).is_some()
    });
    let all_required_present = PREFLIGHT_REQUIRED_CHECKS.iter().all(|required| {
        checks
            .iter()
            .any(|c| c.get("name").and_then(serde_json::Value::as_str) == Some(required))
    });
    !(every_check_passes && all_required_present)
}

/// Human render for `lnrent preflight` (lnrent-y4m.9): the per-check verdicts + the aggregate.
fn render_preflight_human(v: &serde_json::Value) {
    let checks = v
        .get("checks")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for c in &checks {
        let s = |k: &str| c.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
        let ok = c.get("ok").and_then(serde_json::Value::as_bool) == Some(true);
        let mark = if ok { "\u{2022}" } else { "\u{00d7}" };
        println!("  {} {} \u{b7} {}", mark, s("name"), s("detail"));
        // Preflight is the read-only REHEARSAL for `listing publish` (go-live.md §4 before §5), so a
        // failure has to say which side of that gate it lands on — otherwise the operator cannot tell
        // a hard block from something `--accept-unverified` would let them launch past.
        if !ok {
            println!("      {}", publish_gate_note(c));
        }
    }
    if v.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        println!("Preflight: \x1b[1mPASS\x1b[0m");
    } else {
        println!(
            "Preflight: \x1b[1mFAIL\x1b[0m \u{2014} fix the failing check(s) before promoting"
        );
    }
}

/// What a failing preflight check means for `lnrent listing publish` (lnrent-i23), from the `class`
/// the daemon minted with the verdict. An unknown/absent class is described neutrally rather than
/// guessed at — the daemon is the only classifier.
fn publish_gate_note(check: &serde_json::Value) -> &'static str {
    match check.get("class").and_then(serde_json::Value::as_str) {
        Some("structural") => {
            "\u{21b3} BLOCKS `listing publish` — your own configuration; there is no override"
        }
        // Deliberately NOT "the money path still fails closed" — that is only true of the payment
        // backend. `order_intake` never consults the compute provider, so overriding a provider
        // reachability failure lets a Buyer pay into an outage and be refunded net of fees.
        Some("reachability") => {
            "\u{21b3} blocks `listing publish` unless you pass --accept-unverified (a third party \
             is down). If it is STILL down when a Buyer orders: a payment-backend outage refuses \
             the order, but a provider outage lets them pay and be refunded net of fees"
        }
        _ => "\u{21b3} fix this before `listing publish`",
    }
}

/// Human render for `lnrent listing publish|withdraw` (lnrent-i23). Says LIVE or NOT LIVE first —
/// that is the one thing an operator must not misread — then the coordinate they share, then any
/// caveat (an overridden check, a relay that did not take the event / the retraction).
fn render_listing_human(v: &serde_json::Value) {
    println!("{}", listing_human_text(v));
}

fn listing_human_text(v: &serde_json::Value) -> String {
    let s = |k: &str| v.get(k).and_then(serde_json::Value::as_str);
    let published = v
        .get("published")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    // The headline splits `published` (the DURABLE answer: order intake gates on it) from
    // DISCOVERABILITY, because a reply can carry both `published: true` and a `relay_error` — and
    // that error means ZERO relays took the event (`nostr_engine::require_relay_acceptance` fails
    // only when none did). Promising "buyers can discover it" there would be exactly the false
    // belief this gate exists to remove, so that half is claimed only when a relay accepted one.
    // The ⚠ line below names the error and the automatic remedy.
    let headline = match (published, s("relay_error").is_some()) {
        (true, false) => "Listing: \x1b[1mLIVE\x1b[0m — buyers can discover and order it",
        (true, true) => {
            "Listing: \x1b[1mLIVE\x1b[0m — order intake accepts orders, but NO relay took this \
             30402, so buyers may not be able to find it"
        }
        (false, _) => "Listing: \x1b[1mNOT LIVE\x1b[0m — order intake refuses every order",
    };
    let mut lines = vec![headline.to_string()];
    if let Some(id) = s("listing_id") {
        lines.push(format!("  coordinate: {id}"));
    }
    if let Some(note) = s("note") {
        lines.push(format!("  note: {note}"));
    }
    for w in v
        .get("warnings")
        .and_then(serde_json::Value::as_array)
        .unwrap_or(&Vec::new())
    {
        let f = |k: &str| w.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
        lines.push(format!(
            "  \u{26a0} published UNVERIFIED: {} \u{b7} {}",
            f("name"),
            f("detail")
        ));
    }
    // The two relay caveats. Neither changes the durable answer above — the row is what order
    // intake reads — but an operator who is told "LIVE" deserves to know no relay took the event.
    if let Some(e) = s("relay_error") {
        lines.push(format!(
            "  \u{26a0} the 30402 did not reach a relay ({e}); the next restart republishes it"
        ));
    }
    if let Some(e) = s("retract_error") {
        lines.push(format!(
            "  \u{26a0} the relays were not told to drop it ({e}); they may keep serving the stale \
             listing, but every order is refused"
        ));
    }
    // NOT a relay problem — a relay took the event. The local write failed, and the usual cause
    // makes the "LIVE" above misleading in the other direction: a full disk or IO error latches the
    // store degraded, which refuses the reservation writes order intake needs, so the listing is
    // discoverable while the daemon can accept nothing.
    if let Some(e) = s("persist_error") {
        lines.push(format!(
            "  \u{26a0} a relay TOOK the listing but its event id could not be recorded ({e}) — \
             check `lnrent money`/`lnrent status`: if the store is degraded, orders are being \
             refused even though buyers can discover you"
        ));
    }
    lines.join("\n")
}

/// Human render for `lnrent teardowns` (lnrent-urw.2): the owed provider teardowns, or a clean line.
fn render_teardowns_human(v: &serde_json::Value) {
    let get_i64 = |k: &str| v.get(k).and_then(serde_json::Value::as_i64).unwrap_or(0);
    let failures = v
        .get("teardown_failures")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let cleanups = get_i64("provision_cleanups_open");
    let total = get_i64("open_total");
    if total == 0 {
        println!("No owed teardowns — every destroy/cleanup has completed.");
        return;
    }
    println!("Owed teardowns: \x1b[1m{total}\x1b[0m (a provider resource may still be billing)");
    for f in &failures {
        let s = |k: &str| f.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
        let n = |k: &str| f.get(k).and_then(serde_json::Value::as_i64).unwrap_or(0);
        println!(
            "  \u{2022} sub {} \u{b7} {} \u{b7} {} attempt(s) \u{b7} owed {}s \u{b7} next retry in {}s",
            s("subscription_id"),
            s("hook"),
            n("attempts"),
            n("owed_for_s"),
            n("next_retry_in_s"),
        );
        if let Some(err) = f.get("last_error").and_then(serde_json::Value::as_str) {
            println!("      last error: {err}");
        }
    }
    if cleanups > 0 {
        println!("  + {cleanups} provision-failure cleanup(s) owed (auto-retried every reconcile tick)");
    }
}

/// Human render for `lnrent refunds` (lnrent-urw.5): the non-terminal + parked refunds, or a clean
/// line. `refund-retry <id>` re-drives a parked (FAILED) one.
fn render_refunds_human(v: &serde_json::Value) {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        println!("No pending or parked refunds.");
        return;
    }
    let parked = rows
        .iter()
        .filter(|r| r.get("status").and_then(serde_json::Value::as_str) == Some("FAILED"))
        .count();
    println!(
        "Refunds: \x1b[1m{}\x1b[0m ({parked} parked FAILED — retry with `lnrent refund-retry <id>`)",
        rows.len()
    );
    for r in &rows {
        let s = |k: &str| r.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
        let n = |k: &str| r.get(k).and_then(serde_json::Value::as_i64);
        println!(
            "  \u{2022} {} \u{b7} {} \u{b7} {} sat \u{b7} {} \u{b7} {} attempt(s) \u{b7} age {}s",
            s("id"),
            s("dest_form"),
            n("amount_sat").map(|a| a.to_string()).unwrap_or_else(|| "?".into()),
            s("status"),
            n("attempts").unwrap_or(0),
            n("age_s").unwrap_or(0),
        );
    }
}

/// The `lnrent sweep <bolt11> [--yes]` quote-then-confirm flow (gate1-operator-sweep, urw.3): render
/// the dry-run quote first, then execute ONLY on `--yes`. Money never moves without `--yes`. In JSON
/// EXECUTION (`--json --yes`) the OK quote is SUPPRESSED so stdout carries EXACTLY ONE authoritative
/// envelope — the execute result — never a stale `ok:true` quote ahead of a failed execute.
async fn run_sweep(sock: &str, bolt11: String, yes: bool, as_json: bool) -> ExitCode {
    // 1. Dry-run quote (surplus breakdown + verdict).
    let quote = match ipc::call(sock, Request::SweepQuote { bolt11: bolt11.clone() }).await {
        Ok(r) => r,
        Err(e) => return ipc_unreachable(sock, e, as_json),
    };
    let quote_ok = quote.ok;
    // A machine caller of `--json --yes` parses stdout as THE command result, so it must see one
    // envelope. Rendering the OK quote too would leave a stale `ok:true` on stdout even when the
    // execute below then fails (surplus changed, another sweep raced in, the capped pay refused) — the
    // caller would read a failed sweep as success. Suppress the advisory quote there; the execute arm
    // re-validates, re-prices, and re-gates, so its single reply is authoritative. A quote that ITSELF
    // failed (invalid/unpriceable) IS the one envelope — surface it and stop before executing.
    let suppress_quote = as_json && yes && quote_ok;
    if !suppress_quote {
        let quote_code = render(quote, as_json, HumanRender::Sweep);
        if !quote_ok {
            return quote_code;
        }
        if !yes {
            if !as_json {
                println!("\nDry run only — re-run with --yes to execute the sweep.");
            }
            return ExitCode::SUCCESS;
        }
    }

    // 2. Execute (only on --yes) — the single authoritative envelope in JSON mode.
    match ipc::call(sock, Request::Sweep { bolt11 }).await {
        Ok(reply) => render(reply, as_json, HumanRender::Sweep),
        Err(e) => ipc_unreachable(sock, e, as_json),
    }
}

/// The daemon-unreachable failure (exit 4): a structured, deterministic error to stderr so `--json`
/// stdout stays clean. Shared by the sweep flow and mirrors `main`'s inline handling.
fn ipc_unreachable(sock: &str, e: impl std::fmt::Display, as_json: bool) -> ExitCode {
    if as_json {
        eprintln!(
            "{}",
            serde_json::json!({"ok": false, "error": {"code": "ipc", "message": e.to_string(), "retryable": true}})
        );
    } else {
        eprintln!("lnrent: cannot reach lnrentd at {sock}: {e}");
    }
    ExitCode::from(4)
}

/// Human render for `lnrent sweep` (gate1-operator-sweep): the dry-run quote (surplus breakdown +
/// ALLOW/REFUSE verdict) OR the execute result (SENT / cached), detected by the reply's fields.
fn render_sweep_human(v: &serde_json::Value) {
    let msat = |k: &str| v.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
    let amount = v.get("amount_sat").and_then(serde_json::Value::as_u64).unwrap_or(0);
    if let Some(verdict) = v.get("verdict").and_then(serde_json::Value::as_str) {
        // Quote breakdown.
        println!("Sweep quote: {amount} sat (outlay {} msat)", msat("outlay_msat"));
        println!("  Earned:   {} msat", msat("earned_msat"));
        println!("  Reserved: {} msat", msat("reserved_msat"));
        println!("  Paid out: {} msat", msat("paid_out_msat"));
        println!("  Surplus:  {} msat", msat("surplus_msat"));
        match verdict {
            "ALLOW" => println!("Verdict: \x1b[1mALLOW\x1b[0m (surplus covers the sweep)"),
            other => println!("Verdict: \x1b[1m{other}\x1b[0m (surplus does not cover the sweep)"),
        }
    } else {
        // Execute result.
        let status = v.get("status").and_then(serde_json::Value::as_str).unwrap_or("?");
        let cached = v.get("cached").and_then(serde_json::Value::as_bool).unwrap_or(false);
        if cached {
            println!("Sweep already completed: {amount} sat (\x1b[1m{status}\x1b[0m).");
        } else {
            println!("Sweep \x1b[1m{status}\x1b[0m: {amount} sat.");
        }
    }
}

/// Human render for `lnrent relays` (lnrent-urw.6): per-relay connectivity, or a blackout warning
/// when every relay is down. An empty list means the maintenance loop hasn't refreshed yet.
fn render_relays_human(v: &serde_json::Value) {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        println!("No relay status yet (daemon starting, or no relays configured).");
        return;
    }
    let connected = rows
        .iter()
        .filter(|r| r.get("connected").and_then(serde_json::Value::as_bool) == Some(true))
        .count();
    if connected == 0 {
        println!(
            "Relays: \x1b[1m{}/{} connected — BLACKOUT\x1b[0m (no inbound orders or outbound DMs are flowing)",
            connected,
            rows.len()
        );
    } else {
        println!("Relays: \x1b[1m{}/{} connected\x1b[0m", connected, rows.len());
    }
    for r in &rows {
        let s = |k: &str| r.get(k).and_then(serde_json::Value::as_str).unwrap_or("?");
        let mark = if r.get("connected").and_then(serde_json::Value::as_bool) == Some(true) {
            "\u{2022}"
        } else {
            "\u{00d7}"
        };
        let last = r
            .get("last_connected_at")
            .and_then(serde_json::Value::as_i64)
            .map(|t| format!("last connected @{t}"))
            .unwrap_or_else(|| "never connected".to_string());
        println!("  {} {} \u{b7} {} \u{b7} {}", mark, s("url"), s("status"), last);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // codex on PR #66: the backend-correct labels must apply in EVERY readiness state, not only
    // when something failed. A HEALTHY phoenixd carries no failure detail, so labelling off that
    // detail fell back to "Federation:"/"Gateway:" — i.e. the operator with no guardians was told
    // about guardians precisely when everything was working, which is the common case.
    #[test]
    fn money_human_uses_phoenixd_terms_when_the_node_is_healthy() {
        let rendered = money_human_text(&json!({
            "expected_msat": 0,
            "gateway_ok": true,
            "federation_ok": true,
            "gross_liability_sat": 0,
            "required_msat": 0,
            "parked_count": 0,
            "ready": true,
            "warning": null,
            "degraded_read_only": false,
            // Identity is present in every state; no failure detail exists because nothing failed.
            "readiness_backend": "phoenixd",
        }));

        assert!(rendered.contains("Phoenixd node: ok"), "{rendered}");
        assert!(rendered.contains("Refund pay: ok"), "{rendered}");
        for wrong_subsystem in ["federation", "guardian", "gateway"] {
            assert!(
                !rendered.to_lowercase().contains(wrong_subsystem),
                "healthy phoenixd money output named {wrong_subsystem}: {rendered}"
            );
        }
    }

    #[test]
    fn money_human_uses_phoenixd_failure_terms_and_preflight_remedy() {
        let detail = "phoenixd 0.9.1 is not the 0.9.0 release its trampoline fee schedule was \
                      verified against — verify this release's schedule and configure it ([phoenixd] \
                      fee_schedule_version/fee_base_msat/fee_ppm)";
        let rendered = money_human_text(&json!({
            "expected_msat": 0,
            "gateway_ok": false,
            "federation_ok": false,
            "gross_liability_sat": 0,
            "required_msat": 0,
            "parked_count": 0,
            "ready": false,
            "warning": "FederationDown",
            "degraded_read_only": false,
            "readiness_failure_backend": "phoenixd",
            "readiness_failure_detail": detail,
            "recent_unbookable_settlement_alerts": 1,
            "recent_unbookable_settlement_alert_details": [{
                "subject": "fee_credit",
                "detail": "fee credit; REMEDY: fund SPENDABLE balance",
                "at": 900
            }],
        }));

        assert!(rendered.contains("Phoenixd node: not ok"), "{rendered}");
        assert!(rendered.contains("Refund pay: not ok"), "{rendered}");
        assert!(rendered.contains(detail), "{rendered}");
        assert!(rendered.contains("Unbookable settlements: 1 condition alerting"));
        assert!(rendered.contains("may already be resolved"));
        assert!(rendered.contains("alerted_at=900"));
        assert!(rendered.contains("fee_credit") && rendered.contains("REMEDY"));
        assert!(
            rendered.contains("[phoenixd] fee_schedule_version/fee_base_msat/fee_ppm"),
            "{rendered}"
        );
        for wrong_subsystem in ["federation", "guardian", "gateway"] {
            assert!(
                !rendered.to_lowercase().contains(wrong_subsystem),
                "phoenixd money output named {wrong_subsystem}: {rendered}"
            );
        }
    }

    // The number counts CONDITIONS, and both of them can be alerting at once. Each carries a
    // different reason and a different remedy, so rendering one and summarizing the rest — or
    // labelling the count as receipts/orders — would send the operator to fix the wrong thing.
    #[test]
    fn money_human_lists_every_unbookable_condition_with_its_own_remedy() {
        let rendered = money_human_text(&json!({
            "expected_msat": 0,
            "gross_liability_sat": 0,
            "required_msat": 0,
            "parked_count": 0,
            "ready": true,
            "degraded_read_only": false,
            "recent_unbookable_settlement_alerts": 2,
            "recent_unbookable_settlement_alert_details": [
                {
                    "subject": "fee_credit",
                    "detail": "FEE-CREDIT REFUSAL; REMEDY: fund SPENDABLE balance",
                    "at": 900
                },
                {
                    "subject": "index_diverged",
                    "detail": "INDEX DIVERGENCE; REMEDY: restore from your NEWEST backup",
                    "at": 800
                },
            ],
        }));

        assert!(
            rendered.contains("Unbookable settlements: 2 conditions alerting"),
            "the count is of conditions, and reads as plural: {rendered}"
        );
        assert!(
            rendered.contains("alerted_at=900 \u{b7} fee_credit \u{b7} FEE-CREDIT REFUSAL; REMEDY: \
                               fund SPENDABLE balance"),
            "{rendered}"
        );
        assert!(
            rendered.contains("alerted_at=800 \u{b7} index_diverged \u{b7} INDEX DIVERGENCE; \
                               REMEDY: restore from your NEWEST backup"),
            "the second condition's OWN remedy, not a summary of the first: {rendered}"
        );
    }

    #[test]
    fn money_human_reports_unreadable_unbookable_history_as_unknown() {
        let rendered = money_human_text(&json!({
            "expected_msat": 0,
            "gross_liability_sat": 0,
            "required_msat": 0,
            "parked_count": 0,
            "ready": true,
            "degraded_read_only": false,
            "recent_unbookable_settlement_alerts_unknown": true,
        }));

        assert!(
            rendered.contains("Unbookable settlement alert history: UNKNOWN"),
            "{rendered}"
        );
        assert!(
            rendered.contains("cannot say whether settlements are being held back"),
            "{rendered}"
        );
        assert!(!rendered.contains("Unbookable settlements: 0"), "{rendered}");
    }

    // `ready` means only that reserves cover the open REFUND liability, so it can be true while a
    // paid receipt sits unbooked — the unfunded-wallet first sale is exactly that case. A bare
    // READY printed under the unbookable block reads as an all-clear over a buyer's money, so the
    // human line is qualified. Nothing else in the suite asserts the Status line here: the two
    // tests above render with `ready: true` and alerts present but check only the alert rows, so
    // reverting the qualification to a bare "READY" left everything green.
    #[test]
    fn money_human_does_not_print_a_bare_ready_over_an_unbookable_settlement() {
        let base = |extra: serde_json::Value| {
            let mut v = json!({
                "expected_msat": 0,
                "gross_liability_sat": 0,
                "required_msat": 0,
                "parked_count": 0,
                "ready": true,
                "degraded_read_only": false,
            });
            let (obj, extra) = (v.as_object_mut().unwrap(), extra);
            for (k, val) in extra.as_object().unwrap() {
                obj.insert(k.clone(), val.clone());
            }
            v
        };

        // Both paths into the block: a counted condition, and an unreadable history.
        for (label, payload) in [
            ("counted", base(json!({ "recent_unbookable_settlement_alerts": 1 }))),
            (
                "unknown",
                base(json!({ "recent_unbookable_settlement_alerts_unknown": true })),
            ),
        ] {
            let rendered = money_human_text(&payload);
            assert!(
                rendered.contains("READY (refund liability only)"),
                "{label}: READY must be qualified when a settlement may be unbooked: {rendered}"
            );
            assert!(
                rendered.contains("this line does not cover them"),
                "{label}: and must say what it does not cover: {rendered}"
            );
        }

        // The control: with nothing unbookable, the plain READY is still what prints.
        let clean = money_human_text(&base(json!({ "recent_unbookable_settlement_alerts": 0 })));
        assert!(
            clean.contains("Status: \u{1b}[1mREADY\u{1b}[0m"),
            "an all-clear stays an all-clear when nothing is held back: {clean}"
        );
    }

    // With the DM sink off there are no outbox rows, so the view cannot see the condition — and
    // the CLI is then the operator's ONLY surface. Reporting 0 there would be a bare all-clear over
    // a buyer's unbooked receipt, which is the failure this whole bead exists to prevent. It gets
    // its own line rather than reusing the storage-failure UNKNOWN because the remedy differs:
    // re-enable alerts, not fix the database.
    #[test]
    fn money_human_reports_a_disabled_alert_sink_as_unavailable_not_zero() {
        let rendered = money_human_text(&json!({
            "expected_msat": 0,
            "gross_liability_sat": 0,
            "required_msat": 0,
            "parked_count": 0,
            "ready": true,
            "degraded_read_only": false,
            "recent_unbookable_settlement_alerts_disabled": true,
        }));

        assert!(
            rendered.contains("Unbookable settlement alert history: UNAVAILABLE"),
            "{rendered}"
        );
        assert!(
            rendered.contains("NOT a report of zero"),
            "it must refuse to be read as an all-clear: {rendered}"
        );
        assert!(
            !rendered.contains("Unbookable settlements: 0"),
            "and must never print a count it cannot know: {rendered}"
        );
        // The remedy is re-enabling alerts, NOT chasing a storage error.
        assert!(
            !rendered.contains("could not read its durable alert history"),
            "the storage-failure wording would send the operator to the wrong fix: {rendered}"
        );
        assert!(
            rendered.contains("READY (refund liability only)"),
            "and READY must be qualified when the view cannot see the condition: {rendered}"
        );
    }

    #[test]
    fn money_human_fedimint_wording_is_unchanged() {
        let rendered = money_human_text(&json!({
            "expected_msat": 1000,
            "gateway_ok": true,
            "federation_ok": false,
            "gross_liability_sat": 2,
            "required_msat": 2000,
            "parked_count": 0,
            "ready": false,
            "warning": "FederationDown",
            "degraded_read_only": false,
        }));

        assert_eq!(
            rendered,
            "Expected holdings (ledger): 1000 msat\n\
             Federation: not ok\n\
             Gateway: ok\n\
             Outstanding liabilities: 2 sat gross, 2000 msat required\n\
             Parked count: 0\n\
             Status: \u{1b}[1mNOT READY (FederationDown)\u{1b}[0m"
        );
    }

    // lnrent-y4m.9: the aggregate→exit mapping is pure and STRUCTURAL (adversarial review) —
    // exit 0 only for a well-formed passing report with every required check present and passing;
    // malformed, incomplete, or self-contradictory reports exit nonzero; an IPC-level error is
    // left to `render`'s taxonomy exit.
    // The error-code → exit taxonomy (ADR-0014). lnrent-j3c: `shutting_down` (a read-only request
    // cancelled by a graceful shutdown) must map to the TRANSIENT ipc/connection exit 4 — the same
    // bucket as an unreachable daemon — so an agent retries a restart race instead of reading the
    // default exit 1 as a hard failure.
    #[test]
    fn exit_code_for_maps_the_error_taxonomy() {
        assert_eq!(exit_code_for("not_found"), 2);
        assert_eq!(exit_code_for("bad_request"), 3);
        assert_eq!(exit_code_for("sweep_in_flight"), 3);
        // lnrent-j3c: the graceful-shutdown restart race is transient/retryable, NOT exit 1.
        assert_eq!(exit_code_for("shutting_down"), 4);
        assert_eq!(exit_code_for("internal"), 5);
        // An unrecognized daemon error code falls through to the generic failure exit.
        assert_eq!(exit_code_for("some_unknown_code"), 1);
    }

    #[test]
    fn preflight_checks_failed_maps_the_aggregate() {
        let full_pass = |names: &[&str]| {
            Reply::ok(json!({
                "ok": true,
                "checks": names
                    .iter()
                    .map(|n| json!({"name": n, "ok": true, "detail": "ok"}))
                    .collect::<Vec<_>>(),
            }))
        };
        assert!(!preflight_checks_failed(&full_pass(&[
            "gateway",
            "federation",
            "lnv2",
            "provider_token",
            "recipe_preflight",
        ])));
        // A report MISSING a required check (here: lnv2) fails closed, even all-passing.
        assert!(preflight_checks_failed(&full_pass(&[
            "gateway",
            "federation",
            "provider_token",
            "recipe_preflight",
        ])));
        // lnrent-1sr: a report missing recipe_preflight (a version-skewed pre-1sr daemon) fails
        // closed — the provisioning-param guard must not silently vanish.
        assert!(preflight_checks_failed(&full_pass(&[
            "gateway",
            "federation",
            "lnv2",
            "provider_token",
        ])));
        // lnrent-5mi: `phoenixd` is backend-conditional, so a mock/Fedimint report that OMITS it is
        // structurally valid — that is the first assertion above, and it is why the name cannot join
        // PREFLIGHT_REQUIRED_CHECKS. A phoenixd operator's report carries it as an extra passing check.
        assert!(!preflight_checks_failed(&full_pass(&[
            "gateway",
            "federation",
            "lnv2",
            "provider_token",
            "recipe_preflight",
            "phoenixd",
        ])));
        // …and when it is PRESENT and failing, the every-check gate must trip even though the name is
        // not required — including against a daemon that contradicts itself with `ok: true`, since
        // that is the whole reason the CLI re-derives the verdict from the checks.
        assert!(preflight_checks_failed(&Reply::ok(json!({
            "ok": true,
            "checks": [
                {"name": "gateway", "ok": true, "detail": "skipped (phoenixd backend)"},
                {"name": "federation", "ok": true, "detail": "skipped (phoenixd backend)"},
                {"name": "lnv2", "ok": true, "detail": "skipped (phoenixd backend)"},
                {"name": "phoenixd", "ok": false, "detail": "REJECTED the api password"},
                {"name": "provider_token", "ok": true, "detail": "ok"},
                {"name": "recipe_preflight", "ok": true, "detail": "ok"},
            ],
        }))));
        // Forward-compatible: an EXTRA (unknown) passing check is accepted.
        assert!(!preflight_checks_failed(&full_pass(&[
            "gateway",
            "federation",
            "lnv2",
            "provider_token",
            "recipe_preflight",
            "future_check",
        ])));

        let fail = Reply::ok(json!({
            "ok": false,
            "checks": [{"name": "gateway", "ok": false, "detail": "down"}],
        }));
        assert!(preflight_checks_failed(&fail));

        // STRUCTURAL fail-closed (adversarial review): a daemon replying ok:true with an EMPTY or
        // INCOMPLETE checks array, a contradictory per-check verdict, a non-bool aggregate, or no
        // data at all must exit nonzero — the exit code is an agent gate.
        assert!(preflight_checks_failed(&Reply::ok(json!({"ok": true, "checks": []}))));
        assert!(preflight_checks_failed(&Reply::ok(
            json!({"ok": true, "checks": [{"name": "gateway", "ok": true, "detail": "ok"}]})
        )));
        assert!(preflight_checks_failed(&Reply::ok(json!({
            "ok": true,
            "checks": [
                {"name": "gateway", "ok": true, "detail": "ok"},
                {"name": "federation", "ok": false, "detail": "down"},
                {"name": "provider_token", "ok": true, "detail": "ok"},
            ],
        }))));
        assert!(preflight_checks_failed(&Reply::ok(json!({"checks": []}))));
        assert!(preflight_checks_failed(&Reply::ok(json!({"ok": "yes"}))));

        // An IPC-level error keeps render's taxonomy exit; this mapping stays out of it.
        assert!(!preflight_checks_failed(&Reply::err("internal", "boom")));

        // But an INCONSISTENT envelope — ok:false with NO error object — is malformed, not a
        // taxonomy error: render would exit 0 on it, so this gate must fail it (adversarial
        // y4m.9 review).
        let ok_false_no_error: Reply =
            serde_json::from_value(json!({"ok": false})).expect("deserializable envelope");
        assert!(preflight_checks_failed(&ok_false_no_error));
    }

    // lnrent-i23: both publication refusals are OPERATOR decisions (fix the config / decide whether
    // to launch anyway), not daemon failures — so they land on the request-refusal exit 3 and never
    // on the generic 1 that would make them indistinguishable from an unknown error code.
    #[test]
    fn publication_refusals_map_to_the_request_refusal_exit() {
        assert_eq!(exit_code_for("listing_blocked"), 3);
        assert_eq!(exit_code_for("listing_unverified"), 3);
    }

    // `lnrent preflight` is sold as the read-only rehearsal for `listing publish` (go-live.md §4
    // before §5), so a failing check has to say which side of that gate it lands on — otherwise the
    // operator cannot tell a hard block from something `--accept-unverified` would let them past.
    #[test]
    fn a_failing_preflight_check_says_what_it_does_to_publication() {
        let note = |class| publish_gate_note(&json!({"name": "x", "ok": false, "class": class}));
        assert!(
            note("structural").contains("no override"),
            "{}",
            note("structural")
        );
        assert!(
            note("reachability").contains("--accept-unverified"),
            "{}",
            note("reachability")
        );
        // An unclassified failure is described neutrally rather than guessed at — the daemon is the
        // only classifier, and an older daemon's report has no `class` at all.
        let unknown = publish_gate_note(&json!({"name": "x", "ok": false}));
        assert!(!unknown.contains("--accept-unverified"), "{unknown}");
        assert!(unknown.contains("listing publish"), "{unknown}");
    }

    // The one thing an operator must not misread, in both directions.
    #[test]
    fn listing_human_states_live_or_not_live() {
        let live = listing_human_text(&json!({
            "published": true,
            "state": "ACTIVE",
            "listing_id": "30402:abc:dummy",
            "event_id": "ev1",
            "warnings": [],
        }));
        assert!(live.contains("LIVE"), "{live}");
        assert!(live.contains("30402:abc:dummy"), "{live}");

        let down = listing_human_text(&json!({
            "published": false,
            "state": "WITHDRAWN",
            "listing_id": "30402:abc:dummy",
            "retracted_from_relays": true,
        }));
        assert!(down.contains("NOT LIVE"), "{down}");
    }

    // An override is only honest if it SAYS what was overridden, and a "LIVE" that no relay took is
    // only honest if it says that too.
    #[test]
    fn listing_human_surfaces_the_override_and_relay_caveats() {
        let rendered = listing_human_text(&json!({
            "published": true,
            "state": "ACTIVE",
            "listing_id": "30402:abc:dummy",
            "relay_error": "no relays accepted publishing 30402 listing",
            "warnings": [
                {"name": "federation", "ok": false, "detail": "guardians unreachable",
                 "class": "reachability"}
            ],
        }));
        assert!(rendered.contains("UNVERIFIED"), "{rendered}");
        assert!(rendered.contains("federation"), "{rendered}");
        assert!(rendered.contains("guardians unreachable"), "{rendered}");
        assert!(rendered.contains("did not reach a relay"), "{rendered}");
        // ...and the HEADLINE itself must not promise discovery no relay can serve: `relay_error`
        // means zero relays accepted the event, so the operator is live (orders are accepted) and
        // undiscoverable at the same time, and both halves have to be said.
        assert!(rendered.contains("LIVE"), "{rendered}");
        assert!(
            !rendered.contains("can discover"),
            "a publication no relay took must not claim buyers can discover it: {rendered}"
        );
    }

    #[test]
    fn listing_human_reports_a_failed_retraction_without_claiming_it_is_still_live() {
        let rendered = listing_human_text(&json!({
            "published": false,
            "state": "WITHDRAWN",
            "listing_id": "30402:abc:dummy",
            "retracted_from_relays": false,
            "retract_error": "no relays accepted retracting 30402 listing",
        }));
        assert!(rendered.contains("NOT LIVE"), "{rendered}");
        assert!(rendered.contains("were not told to drop it"), "{rendered}");
        assert!(rendered.contains("every order is refused"), "{rendered}");
    }
}
