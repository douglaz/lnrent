//! `lnrent`: the operator CLI — the OPERATOR's agent surface (ADR-0014). It talks to lnrentd
//! over a Unix-domain socket (the daemon is the sole writer, ADR-0001); it never touches
//! sqlite directly. Every command supports `--json`, runs non-interactively, and returns a
//! deterministic exit code so an operator agent can drive it. No MCP/HTTP server.

use clap::{Parser, Subcommand};
use lnrentd::ipc::{self, Reply, Request};
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "lnrent", about = "lnrent operator CLI (agent-grade; talks to lnrentd over a unix socket)")]
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
    /// List subscriptions.
    Subs,
    /// Inspect one subscription.
    Sub { id: String },
    /// Admin: force-suspend a subscription.
    Suspend { id: String },
    /// Admin: force-resume a suspended subscription.
    Resume { id: String },
}

/// Exit-code taxonomy (agent-grade, ADR-0014): 0 ok; 2 not_found; 3 bad_request/invalid_state;
/// 4 ipc/connection failure; 5 internal.
fn exit_for(err_code: &str) -> ExitCode {
    match err_code {
        "not_found" => ExitCode::from(2),
        "bad_request" | "invalid_state" => ExitCode::from(3),
        "internal" => ExitCode::from(5),
        _ => ExitCode::from(1),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let req = match cli.cmd {
        Cmd::Status => Request::Status,
        Cmd::Recipes => Request::Recipes,
        Cmd::Subs => Request::Subs,
        Cmd::Sub { id } => Request::Sub { id },
        Cmd::Suspend { id } => Request::AdminSuspend { id },
        Cmd::Resume { id } => Request::AdminResume { id },
    };
    let sock = format!("{}/lnrent.sock", cli.data_dir);

    match ipc::call(&sock, req).await {
        Ok(reply) => render(reply, cli.json),
        Err(e) => {
            // The daemon isn't reachable — a structured, deterministic failure (retryable:
            // the daemon may come up). Errors go to stderr so `--json` stdout stays clean.
            if cli.json {
                eprintln!("{}", serde_json::json!({"ok": false, "error": {"code": "ipc", "message": e.to_string(), "retryable": true}}));
            } else {
                eprintln!("lnrent: cannot reach lnrentd at {sock}: {e}");
            }
            ExitCode::from(4)
        }
    }
}

fn render(reply: Reply, as_json: bool) -> ExitCode {
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
            Some(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
        }
    } else if let Some(err) = &reply.error {
        eprintln!("lnrent: {} ({})", err.message, err.code);
    }
    match &reply.error {
        Some(err) => exit_for(&err.code),
        None => ExitCode::SUCCESS,
    }
}
