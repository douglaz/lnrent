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
/// A timeout, a cap breach on EITHER pipe, a non-zero exit, or non-JSON stdout is a failure —
/// and the child is explicitly killed + reaped (not left to best-effort drop cleanup).
pub async fn run_hook(hook: &Path, input: &Value, timeout: Duration) -> Result<HookOutput> {
    let mut child = spawn_hook(hook).await?;

    let si = child.stdin.take();
    let input_bytes = serde_json::to_vec(input)?;
    let out = child.stdout.take().context("hook stdout missing")?;
    let err = child.stderr.take().context("hook stderr missing")?;

    // Feed stdin on a DETACHED task so it never gates the result: a hook that never drains stdin
    // + a large payload would block `write_all` on a full pipe buffer, so it must not run inline
    // before the timeout (unbounded hang) NOR be join!ed with the reads (a cap breach would still
    // wait on the stuck feed) — codex re-review. The reads alone decide success/cap/timeout; on
    // any exit path `reap()` kills the child, which closes stdin and lets this task finish
    // (BrokenPipe). A hook that closes stdin early yields BrokenPipe too — best-effort feed.
    let feed = tokio::spawn(async move {
        if let Some(mut si) = si {
            let _ = si.write_all(&input_bytes).await;
            let _ = si.shutdown().await;
        }
    });
    // Read both pipes concurrently, each bounded by OUTPUT_CAP — a cap breach on EITHER pipe
    // returns Err *immediately* (no draining, no hang), independent of the stdin feed.
    let read = async { tokio::try_join!(read_capped(out, OUTPUT_CAP), read_capped(err, OUTPUT_CAP)) };
    let (out_buf, err_buf) = match tokio::time::timeout(timeout, read).await {
        Err(_) => {
            reap(&mut child).await;
            bail!("hook {} timed out after {timeout:?}", hook.display());
        }
        Ok(Err(e)) => {
            // cap breach or read error -> kill the (possibly still-writing) child and fail
            reap(&mut child).await;
            return Err(anyhow!("hook {}: {e}", hook.display()));
        }
        Ok(Ok(bufs)) => bufs,
    };
    // Reads are done; the stdin feed (normally already finished) is no longer needed.
    feed.abort();

    // Both pipes hit EOF -> the child has closed its outputs; wait for exit (bounded).
    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(s) => s.context("waiting on hook")?,
        Err(_) => {
            reap(&mut child).await;
            bail!("hook {} did not exit after closing its output", hook.display());
        }
    };
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

/// Spawn the hook, retrying briefly on TRANSIENT spawn failures: ETXTBSY (a just-written hook can
/// still be momentarily open-for-write — and a parallel fork/exec can race the same way) and
/// EAGAIN (fork hitting a transient resource limit under load). A real ENOENT/EACCES/etc. is NOT
/// retried — a missing or non-executable hook fails fast. Worst-case added delay ~150ms.
async fn spawn_hook(hook: &Path) -> Result<tokio::process::Child> {
    const ETXTBSY: i32 = 26; // "Text file busy"
    const EAGAIN: i32 = 11; // fork: resource temporarily unavailable
    let mut attempt = 0u32;
    loop {
        match Command::new(hook)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true) // backstop; we also reap explicitly
            .spawn()
        {
            Ok(child) => return Ok(child),
            Err(e) if attempt < 5 && matches!(e.raw_os_error(), Some(ETXTBSY) | Some(EAGAIN)) => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10 * attempt as u64)).await;
            }
            Err(e) => return Err(anyhow::Error::new(e).context(format!("spawning hook {}", hook.display()))),
        }
    }
}

/// Explicitly kill the child and reap it (bounded), so a timed-out/over-producing hook leaves
/// no zombie — `kill_on_drop` is only a backstop.
async fn reap(child: &mut tokio::process::Child) {
    let _ = child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
}

/// Read `r` to EOF, retaining the bytes, but return an error the moment the total would exceed
/// `cap` — so an over-producing hook fails FAST (memory- and time-bounded), not after draining.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(mut r: R, cap: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        if buf.len() + n > cap {
            return Err(std::io::Error::other(format!(
                "output exceeded the {cap}-byte cap"
            )));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    // Write an executable script into a unique temp dir and return its path.
    fn write_hook(name: &str, body: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("lnrent-runner-{}-{seq}-{name}", std::process::id()));
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
        assert!(err.to_string().contains("failed (exit"), "got: {err}");
    }

    #[tokio::test]
    async fn timeout_kills_and_fails() {
        let hook = write_hook("slow", "#!/usr/bin/env bash\nsleep 5\necho '{}'\n");
        let err = run_hook(&hook, &json!({}), Duration::from_millis(200)).await.unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    // A hook that never drains stdin + a payload larger than the pipe buffer must still hit the
    // timeout — the stdin write must not block BEFORE the timeout applies (codex re-review).
    // Under the pre-fix code this test would hang forever instead of failing.
    #[tokio::test]
    async fn large_stdin_to_a_nonreading_hook_times_out() {
        let hook = write_hook("ignore-stdin", "#!/usr/bin/env bash\nsleep 5\n");
        let big = json!({ "blob": "x".repeat(256 * 1024) }); // >> the ~64 KiB pipe buffer
        let err = run_hook(&hook, &big, Duration::from_millis(300)).await.unwrap_err();
        assert!(err.to_string().contains("timed out"), "got: {err}");
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
    async fn oversized_stdout_fails_fast_on_cap() {
        // Emit far more than the cap; must fail FAST on the cap (not buffer it, not wait out
        // the timeout) — note the SHORT timeout: a draining impl would hang to it.
        let hook = write_hook(
            "flood-out",
            "#!/usr/bin/env bash\nhead -c 5000000 /dev/zero | tr '\\0' 'a'\n",
        );
        let err = run_hook(&hook, &json!({}), Duration::from_millis(800)).await.unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }

    // A hook that BOTH ignores a large stdin AND overproduces stdout must still fail FAST on the
    // cap — the stuck stdin feed must not gate the cap result (codex re-review). The SHORT timeout
    // is far longer than a fast-cap fail but far shorter than draining 5 MB at pipe speed, so a
    // "wait on the stuck feed" impl would surface as a timeout, not a cap error.
    #[tokio::test]
    async fn oversized_stdout_with_undrained_stdin_still_fails_fast_on_cap() {
        let hook = write_hook(
            "flood-out-ignore-stdin",
            "#!/usr/bin/env bash\nhead -c 5000000 /dev/zero | tr '\\0' 'a'\nsleep 5\n",
        );
        let big = json!({ "blob": "x".repeat(256 * 1024) }); // >> the pipe buffer; never drained
        let err = run_hook(&hook, &big, Duration::from_millis(800)).await.unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }

    #[tokio::test]
    async fn oversized_stderr_fails_fast_on_cap() {
        // A cap breach on STDERR (not stdout) must also fail fast (codex #2).
        let hook = write_hook(
            "flood-err",
            "#!/usr/bin/env bash\nhead -c 5000000 /dev/zero | tr '\\0' 'a' >&2\necho '{}'\n",
        );
        let err = run_hook(&hook, &json!({}), Duration::from_millis(800)).await.unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }
}
