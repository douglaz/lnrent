//! Recipe hook runner. SPEC.md §7.2, lnrent-7fp.6. Executes a recipe hook (a lifecycle hook
//! or a management-op hook) as a child process OFF the reconcile loop, with: a timeout
//! (kill on expiry), bounded stdout/stderr capture (a runaway hook can't exhaust memory),
//! a JSON document on stdin, and JSON on stdout. Secrets ride stdin JSON, not argv/env (§13).
//!
//! A non-zero exit, a timeout, or non-JSON stdout is a failure — the daemon does not advance
//! state on a failed hook (§7.2).

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// Default per-hook wall-clock budget; a hook exceeding it is killed and treated as failure.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
/// Cap on captured stdout/stderr (each), so a runaway hook cannot exhaust memory.
pub const OUTPUT_CAP: usize = 1 << 20; // 1 MiB

/// A successful hook run: the parsed stdout JSON (the delivery payload / op result data).
#[derive(Debug, Clone)]
pub struct HookOutput {
    pub stdout_json: Value,
}

/// Run `hook` (an absolute path) with `input` on stdin, bounded by `timeout` and `OUTPUT_CAP`.
pub async fn run_hook(hook: &Path, input: &Value, timeout: Duration) -> Result<HookOutput> {
    match tokio::time::timeout(timeout, run_inner(hook, input)).await {
        Ok(res) => res,
        // The future is dropped on timeout; `kill_on_drop` reaps the child.
        Err(_) => bail!("hook {} timed out after {timeout:?}", hook.display()),
    }
}

async fn run_inner(hook: &Path, input: &Value) -> Result<HookOutput> {
    let mut child = Command::new(hook)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawning hook {}", hook.display()))?;

    // Feed the JSON document on stdin, then close it so the hook sees EOF.
    if let Some(mut si) = child.stdin.take() {
        let bytes = serde_json::to_vec(input)?;
        si.write_all(&bytes).await.ok();
        si.shutdown().await.ok();
    }

    // Read stdout + stderr CONCURRENTLY (so a full pipe can't deadlock the child), each
    // capped at OUTPUT_CAP bytes — anything beyond the cap is truncated, not buffered.
    let out = child.stdout.take().context("hook stdout missing")?;
    let err = child.stderr.take().context("hook stderr missing")?;
    // Drain BOTH pipes fully (so the child never blocks on a full pipe -> no deadlock) while
    // RETAINING only the first OUTPUT_CAP bytes of each (so memory stays bounded).
    let ((out_buf, out_trunc), (err_buf, _err_trunc)) =
        tokio::try_join!(read_capped(out, OUTPUT_CAP), read_capped(err, OUTPUT_CAP))
            .context("reading hook output")?;

    if out_trunc {
        let _ = child.start_kill();
        bail!("hook {} stdout exceeded the {OUTPUT_CAP}-byte cap", hook.display());
    }

    let status = child.wait().await.context("waiting on hook")?;
    if !status.success() {
        bail!(
            "hook {} failed (exit {:?}): {}",
            hook.display(),
            status.code(),
            String::from_utf8_lossy(&err_buf)
        );
    }

    let stdout_json: Value = serde_json::from_slice(&out_buf)
        .map_err(|e| anyhow!("hook {} stdout is not JSON: {e}", hook.display()))?;
    Ok(HookOutput { stdout_json })
}

/// Read `r` to EOF (draining it so the writer never blocks on a full pipe), retaining only
/// the first `cap` bytes. Returns the (bounded) bytes and whether anything was truncated.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    mut r: R,
    cap: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut truncated = false;
    loop {
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        if buf.len() < cap {
            let take = (cap - buf.len()).min(n);
            buf.extend_from_slice(&chunk[..take]);
            truncated |= take < n;
        } else {
            truncated = true; // keep draining, just stop retaining
        }
    }
    Ok((buf, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    // Write an executable script into a unique temp dir and return its path.
    fn write_hook(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lnrent-runner-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    #[tokio::test]
    async fn hook_returns_stdout_json_and_sees_stdin() {
        // Echo back a field read from the stdin JSON, proving the I/O contract.
        let hook = write_hook(
            "echo",
            "#!/usr/bin/env bash\nread -r line\necho '{\"ok\":true}'\n",
        );
        let out = run_hook(&hook, &json!({"x": 1}), DEFAULT_TIMEOUT).await.unwrap();
        assert_eq!(out.stdout_json, json!({"ok": true}));
    }

    #[tokio::test]
    async fn nonzero_exit_is_failure() {
        let hook = write_hook("fail", "#!/usr/bin/env bash\necho '{}' ; exit 1\n");
        let err = run_hook(&hook, &json!({}), DEFAULT_TIMEOUT).await.unwrap_err();
        assert!(err.to_string().contains("failed (exit"));
    }

    #[tokio::test]
    async fn timeout_kills_and_fails() {
        let hook = write_hook("slow", "#!/usr/bin/env bash\nsleep 5\necho '{}'\n");
        let err = run_hook(&hook, &json!({}), Duration::from_millis(200)).await.unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    // §15/§7.2: the trivial M1a recipe validates, provisions returning a delivery payload, and
    // its minimal request-kind op runs and returns JSON — the end-to-end recipe+runner contract.
    #[tokio::test]
    async fn trivial_dummy_recipe_provisions_and_runs_op() {
        use crate::recipe::Recipe;
        let dir = format!("{}/../recipes/dummy", env!("CARGO_MANIFEST_DIR"));
        let r = Recipe::load(&dir).expect("load dummy recipe");
        r.validate().expect("dummy recipe validates");

        let prov = run_hook(&r.hook("provision"), &json!({"subscription": {"id": "s1"}}), DEFAULT_TIMEOUT)
            .await
            .expect("provision runs");
        assert!(prov.stdout_json.get("payload").is_some(), "provision returns a delivery payload");

        let op = r.operation("status").expect("status op declared");
        let res = run_hook(&r.op_hook(op), &json!({}), DEFAULT_TIMEOUT).await.expect("op runs");
        assert_eq!(res.stdout_json["state"], json!("running"));
    }

    #[tokio::test]
    async fn non_json_stdout_is_failure() {
        let hook = write_hook("garbage", "#!/usr/bin/env bash\necho not-json\n");
        let err = run_hook(&hook, &json!({}), DEFAULT_TIMEOUT).await.unwrap_err();
        assert!(err.to_string().contains("not JSON"));
    }

    #[tokio::test]
    async fn oversized_stdout_is_truncated_not_unbounded() {
        // Emit far more than the cap; the read must stop at OUTPUT_CAP, not buffer it all.
        let hook = write_hook(
            "flood",
            "#!/usr/bin/env bash\nhead -c 5000000 /dev/zero | tr '\\0' 'a'\n",
        );
        // Must fail FAST on the cap (not buffer it all, not wait out the timeout).
        let err = run_hook(&hook, &json!({}), Duration::from_secs(10)).await.unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }
}
