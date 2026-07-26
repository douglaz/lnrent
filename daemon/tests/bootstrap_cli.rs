use lnrentd::config;
use rusqlite::Connection;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TEST_MNEMONIC: &str =
    "leader monkey parrot ring guide accident before fence cannon height naive bean";

const LNRENT_ENV: &[&str] = &[
    "LNRENT_DATA_DIR",
    "LNRENT_RELAYS",
    "LNRENT_PAYMENT_BACKEND",
    "LNRENT_COMPUTE_BACKEND",
    "LNRENT_FEDIMINT_INVITE",
    // The phoenixd vars are scrubbed too (lnrent-9gi): an ambient
    // LNRENT_PHOENIXD_ACCEPT_UNSUPPORTED in a developer's shell would otherwise silently satisfy the
    // unsupported-backend opt-in the refusal test asserts is MISSING.
    "LNRENT_PHOENIXD_URL",
    "LNRENT_PHOENIXD_API_PASSWORD",
    "LNRENT_PHOENIXD_ACCEPT_UNSUPPORTED",
    "LNRENT_MNEMONIC",
    "LNRENT_CONFIG",
];

fn temp_base(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lnrent-bootstrap-cli-{name}-{}-{nanos}",
        std::process::id()
    ))
}

fn lnrentd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lnrentd"));
    for key in LNRENT_ENV {
        cmd.env_remove(key);
    }
    cmd
}

fn relays_from_db(data_dir: &Path) -> Vec<String> {
    let conn = Connection::open(data_dir.join("lnrent.sqlite")).unwrap();
    let relays: String = conn
        .query_row("SELECT relays FROM operator LIMIT 1", [], |row| row.get(0))
        .unwrap();
    serde_json::from_str(&relays).unwrap()
}

#[test]
fn missing_seed_emits_structured_error_and_nonzero_exit() {
    let base = temp_base("missing-seed");
    let data_dir = base.join("data");
    fs::create_dir_all(&base).unwrap();
    // Pin owner-only perms so the data-dir ancestor check is umask-independent (a 0777 base under a
    // permissive test umask would otherwise be rejected as a world-writable, non-sticky ancestor).
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

    let output = lnrentd()
        .args([
            "bootstrap",
            "--json",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(3));
    assert!(
        output.stdout.is_empty(),
        "json errors must go to stderr, not stdout"
    );
    let stderr: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(stderr["ok"], false);
    assert_eq!(stderr["error"]["code"], "seed_missing");
    assert_eq!(stderr["error"]["retryable"], false);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn json_error_with_mnemonic_flag_is_single_json_document() {
    let base = temp_base("json-mnemonic-error");
    let data_dir = base.join("data");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

    let output = lnrentd()
        .args([
            "bootstrap",
            "--json",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--mnemonic",
            TEST_MNEMONIC,
            "--payment-backend",
            "fedimint",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(stderr["ok"], false);
    assert_eq!(stderr["error"]["code"], "config_invalid");

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn piped_stdin_is_ignored_unless_stdin_flag_is_set() {
    let base = temp_base("stdin-ignored");
    let data_dir = base.join("data");
    let config_path = base.join("bootstrap.toml");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&config_path, r#"relays = ["wss://from-file.example"]"#).unwrap();

    let mut child = lnrentd()
        .args([
            "bootstrap",
            "--json",
            "--config",
            config_path.to_str().unwrap(),
        ])
        .env("LNRENT_DATA_DIR", &data_dir)
        .env("LNRENT_MNEMONIC", TEST_MNEMONIC)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stdin = child.stdin.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            drop(stdin);
            let output = child.wait_with_output().unwrap();
            panic!(
                "bootstrap blocked on inherited piped stdin; status={:?} stderr={}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        relays_from_db(&data_dir),
        vec!["wss://from-file.example".to_string()]
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn stdin_source_is_used_when_stdin_flag_is_set() {
    let base = temp_base("stdin-explicit");
    let data_dir = base.join("data");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

    let mut child = lnrentd()
        .args([
            "bootstrap",
            "--json",
            "--stdin",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(
        stdin,
        "mnemonic = \"{TEST_MNEMONIC}\"\nrelays = [\"wss://from-stdin.example\"]"
    )
    .unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        relays_from_db(&data_dir),
        vec!["wss://from-stdin.example".to_string()]
    );

    let _ = fs::remove_dir_all(&base);
}

// Review R1 P2: a blank `LNRENT_CONFIG` (templated environments routinely export optional vars as
// the empty string) must be treated as UNSET — bootstrap must not fail trying to read `""` as a
// config file when flags/env already supply a complete config.
#[test]
fn blank_config_env_is_ignored_not_read_as_empty_path() {
    let base = temp_base("blank-config-env");
    let data_dir = base.join("data");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

    let output = lnrentd()
        .args(["bootstrap", "--json"])
        .env("LNRENT_DATA_DIR", &data_dir)
        .env("LNRENT_MNEMONIC", TEST_MNEMONIC)
        .env("LNRENT_CONFIG", "") // blank -> must be treated as unset, not read as ""
        .stdin(Stdio::null())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "a blank LNRENT_CONFIG must not fail bootstrap; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        relays_from_db(&data_dir),
        default_relays(),
        "no config supplied -> default relays"
    );

    let _ = fs::remove_dir_all(&base);
}

// lnrent-9gi: phoenixd ships in EVERY build, so the OPERATOR-FACING refusal is what keeps real money
// off an un-launch-gated backend. Assert it end to end through the real binary: a phoenixd bootstrap
// without the explicit opt-in exits nonzero with a structured error that names the open gates and
// leaks no api password, persists NO operator row — and the same invocation WITH the opt-in succeeds,
// so lnrent-tof's staging acceptance can still run. Removed with the guard by lnrent-ehu.
#[test]
fn phoenixd_needs_the_unsupported_opt_in_to_bootstrap() {
    let base = temp_base("phoenixd-opt-in");
    let data_dir = base.join("data");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();

    let phoenixd_bootstrap = |accept: Option<&str>| {
        let mut cmd = lnrentd();
        cmd.args([
            "bootstrap",
            "--json",
            "--payment-backend",
            "phoenixd",
            "--phoenixd-url",
            "http://127.0.0.1:9740",
        ])
        .env("LNRENT_DATA_DIR", &data_dir)
        .env("LNRENT_MNEMONIC", TEST_MNEMONIC)
        .env("LNRENT_PHOENIXD_API_PASSWORD", "hunter2");
        if let Some(accept) = accept {
            cmd.env("LNRENT_PHOENIXD_ACCEPT_UNSUPPORTED", accept);
        }
        cmd.stdin(Stdio::null()).output().unwrap()
    };

    let refused = phoenixd_bootstrap(None);
    assert!(!refused.status.success(), "phoenixd must fail closed");
    assert_eq!(refused.status.code(), Some(3), "config_invalid exits 3");
    let stderr: serde_json::Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert_eq!(stderr["error"]["code"], "config_invalid");
    let message = stderr["error"]["message"].as_str().unwrap();
    for gate in ["lnrent-kr1", "lnrent-itw", "docs/go-live.md"] {
        assert!(
            message.contains(gate),
            "refusal must name {gate}: {message}"
        );
    }
    assert!(
        !String::from_utf8_lossy(&refused.stderr).contains("hunter2"),
        "the refusal must not echo the phoenixd api password"
    );
    // Nothing durable: the operator row is never committed, so falling back to another backend on
    // this data dir is not bricked by a pinned `payment_backend='phoenixd'`.
    let db = data_dir.join("lnrent.sqlite");
    if db.exists() {
        let conn = Connection::open(&db).unwrap();
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM operator", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a refused phoenixd bootstrap persists no operator");
    }

    let accepted = phoenixd_bootstrap(Some("true"));
    assert!(
        accepted.status.success(),
        "an explicit opt-in must start (lnrent-tof runs on it); stderr={}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let conn = Connection::open(data_dir.join("lnrent.sqlite")).unwrap();
    let backend: String = conn
        .query_row("SELECT payment_backend FROM operator LIMIT 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(backend, "phoenixd");

    let _ = fs::remove_dir_all(&base);
}

fn default_relays() -> Vec<String> {
    config::DEFAULT_RELAYS
        .iter()
        .map(|relay| relay.to_string())
        .collect()
}
