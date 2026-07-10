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
    /// wallet balance): reports wallet vs expected holdings + an OK/DRIFT verdict. Report-only.
    Reconcile,
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

/// Exit-code taxonomy (agent-grade, ADR-0014): 0 ok; 2 not_found; 3 bad_request/invalid_state;
/// 4 ipc/connection failure; 5 internal.
fn exit_for(err_code: &str) -> ExitCode {
    match err_code {
        "not_found" => ExitCode::from(2),
        "bad_request" | "invalid_state" | "dev_disabled" | "unsupported" => ExitCode::from(3),
        "internal" => ExitCode::from(5),
        _ => ExitCode::from(1),
    }
}

#[derive(Clone, Copy)]
enum HumanRender {
    Generic,
    Money,
    Reconcile,
    Teardowns,
    Refunds,
    Relays,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let (req, human_render) = match cli.cmd {
        Cmd::Status => (Request::Status, HumanRender::Generic),
        Cmd::Recipes => (Request::Recipes, HumanRender::Generic),
        Cmd::Money => (Request::Money, HumanRender::Money),
        Cmd::Reconcile => (Request::Reconcile, HumanRender::Reconcile),
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
    };
    let sock = format!("{}/lnrent.sock", cli.data_dir);

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
                HumanRender::Teardowns => render_teardowns_human(&v),
                HumanRender::Refunds => render_refunds_human(&v),
                HumanRender::Relays => render_relays_human(&v),
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
    let warning = v.get("warning").and_then(serde_json::Value::as_str);

    println!("Expected holdings (ledger): {expected}");
    println!("Federation: {federation}");
    println!("Gateway: {gateway}");
    println!("Outstanding liabilities: {gross} sat gross, {required} msat required");
    println!("Parked count: {parked}");
    if ready {
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
