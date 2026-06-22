//! lnrent: the operator CLI. It talks to lnrentd; the daemon is the sole writer
//! of state (ADR-0001). M0 is a command skeleton.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "lnrent", about = "lnrent operator CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Show daemon status (M1).
    Status,
    /// List loaded recipes (M1).
    Recipes,
    /// Inspect subscriptions (M1).
    Subs,
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Status => println!("lnrent: status not implemented (M0 stub)"),
        Cmd::Recipes => println!("lnrent: recipes listing not implemented (M0 stub)"),
        Cmd::Subs => println!("lnrent: subs not implemented (M0 stub)"),
    }
}
