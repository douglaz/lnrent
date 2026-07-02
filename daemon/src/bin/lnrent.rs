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
    /// Show the daemon's ecash position: balance, gateway, and refund-liability coverage.
    Money,
    /// List subscriptions.
    Subs,
    /// Inspect one subscription.
    Sub { id: String },
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
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let (req, human_render) = match cli.cmd {
        Cmd::Status => (Request::Status, HumanRender::Generic),
        Cmd::Recipes => (Request::Recipes, HumanRender::Generic),
        Cmd::Money => (Request::Money, HumanRender::Money),
        Cmd::Subs => (Request::Subs, HumanRender::Generic),
        Cmd::Sub { id } => (Request::Sub { id }, HumanRender::Generic),
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
    let balance = v
        .get("balance_msat")
        .and_then(serde_json::Value::as_u64)
        .map(|n| format!("{n} msat"))
        .unwrap_or_else(|| "unknown".to_string());
    let gateway = if v
        .get("gateway_ok")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        "ok"
    } else {
        "not ok"
    };
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

    println!("Balance: {balance}");
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
