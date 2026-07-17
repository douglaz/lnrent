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
    /// any check fails, so an operator agent can gate subsequent launch promotion on it. The daemon
    /// publishes before IPC starts, so this is not a publication interlock. Alias: `doctor`.
    #[command(alias = "doctor")]
    Preflight,
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
        "bad_request" | "invalid_state" | "dev_disabled" | "unsupported" | "sweep_invalid"
        | "sweep_unpriceable" | "sweep_busy" | "sweep_insufficient" | "sweep_fee_rose"
        | "sweep_in_flight" => 3,
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
            },
        }
    } else if let Some(err) = &reply.error {
        eprintln!("lnrent: {} ({})", err.message, err.code);
    }
    match &reply.error {
        Some(err) => exit_for(&err.code),
        None => ExitCode::SUCCESS,
    }
}

fn render_money_human(v: &serde_json::Value) {
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

    println!("Expected holdings (ledger): {expected}");
    println!("Federation: {federation}");
    println!("Gateway: {gateway}");
    println!("Outstanding liabilities: {gross} sat gross, {required} msat required");
    println!("Parked count: {parked}");
    // The degraded/read-only latch (lnrent-y4m.3) takes precedence over reserve readiness: the daemon
    // is refusing money writes after a fatal DB error, so a human operator must see it here — not only
    // in the daemon log — regardless of whether reserves are sufficient.
    if degraded {
        println!(
            "Status: \x1b[1;31mDEGRADED (read-only)\x1b[0m — money writes refused after a fatal DB \
             error; restore the state DB from backup and restart"
        );
    } else if ready {
        println!("Status: \x1b[1mREADY\x1b[0m");
    } else {
        println!(
            "Status: \x1b[1mNOT READY ({})\x1b[0m",
            warning.unwrap_or("unknown")
        );
    }
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
/// (forward-compatible) but must each pass.
const PREFLIGHT_REQUIRED_CHECKS: [&str; 4] = ["gateway", "federation", "lnv2", "provider_token"];

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
        let mark = if c.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
            "\u{2022}"
        } else {
            "\u{00d7}"
        };
        println!("  {} {} \u{b7} {}", mark, s("name"), s("detail"));
    }
    if v.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        println!("Preflight: \x1b[1mPASS\x1b[0m");
    } else {
        println!(
            "Preflight: \x1b[1mFAIL\x1b[0m \u{2014} fix the failing check(s) before promoting"
        );
    }
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
            "gateway", "federation", "lnv2", "provider_token",
        ])));
        // A report MISSING a required check (here: lnv2) fails closed, even all-passing.
        assert!(preflight_checks_failed(&full_pass(&[
            "gateway", "federation", "provider_token",
        ])));
        // Forward-compatible: an EXTRA (unknown) passing check is accepted.
        assert!(!preflight_checks_failed(&full_pass(&[
            "gateway", "federation", "lnv2", "provider_token", "future_check",
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
}
