//! lnrent-y4m.14: the buyer CLI's `--json` contract must hold even on a clap ARGV PARSE FAILURE.
//! An agent branches on the exit code and parses the envelope, so a bad-flags failure must NOT
//! escape as clap's plaintext exit 2 (which collides with the taxonomy's exit 2 = not_found) when
//! `--json` was requested. Runs the real binary via `CARGO_BIN_EXE_lnrent-buyer`.

use std::process::Command;

fn run(args: &[&str]) -> (i32, String, String) {
    let bin = env!("CARGO_BIN_EXE_lnrent-buyer");
    let out = Command::new(bin).args(args).output().expect("spawn buyer binary");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// A bad flag WITH --json: exit 3 (bad_request), and a well-formed JSON error envelope on stderr —
// never clap's plaintext exit 2 (which an agent can't tell from not_found).
#[test]
fn bad_argv_with_json_emits_bad_request_envelope_exit_3() {
    let (code, _stdout, stderr) = run(&["--json", "listings", "--no-such-flag"]);
    assert_eq!(code, 3, "argv parse failure under --json must exit 3; stderr:\n{stderr}");
    let v: serde_json::Value =
        serde_json::from_str(stderr.trim()).unwrap_or_else(|e| panic!("stderr not JSON ({e}):\n{stderr}"));
    assert_eq!(v["ok"], serde_json::json!(false));
    assert_eq!(v["error"]["code"], serde_json::json!("bad_request"));
    assert_eq!(v["error"]["retryable"], serde_json::json!(false));
    assert!(
        v["error"]["message"].as_str().map(|m| !m.is_empty()).unwrap_or(false),
        "message must be a non-empty actionable line"
    );
}

// The SAME bad flag WITHOUT --json: clap's normal human behavior — exit 2, plaintext (not JSON).
#[test]
fn bad_argv_without_json_keeps_clap_default_exit_2() {
    let (code, _stdout, stderr) = run(&["listings", "--no-such-flag"]);
    assert_eq!(code, 2, "without --json, clap's default exit 2 is preserved");
    assert!(
        serde_json::from_str::<serde_json::Value>(stderr.trim()).is_err(),
        "human usage is plaintext, not a JSON envelope"
    );
}

// --help is NOT an error even under --json: clap renders help and exits 0.
#[test]
fn help_is_not_an_error_under_json() {
    let (code, stdout, _stderr) = run(&["--json", "--help"]);
    assert_eq!(code, 0, "--help exits 0");
    assert!(stdout.contains("lnrent buyer CLI") || stdout.contains("Usage"), "help text is rendered");
}
