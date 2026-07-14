//! Recipe hook runner. SPEC.md §7.2, lnrent-7fp.6. Executes a recipe hook (a lifecycle hook
//! or a management-op hook) as a child process OFF the reconcile loop, with: a timeout
//! (kill on expiry), bounded stdout/stderr capture (a runaway hook can't exhaust memory),
//! a JSON document on stdin, and JSON on stdout. Secrets ride stdin JSON, not argv/env (§13).
//!
//! A non-zero exit, a timeout, or non-JSON stdout is a failure — the daemon does not advance
//! state on a failed hook (§7.2).
//!
//! **Hook env hygiene (lnrent-y4m.7, §13):** the child starts from a CLEARED environment. Only a
//! fixed base allowlist ([`BASE_HOOK_ENV`], the vars a shell script needs to run tools) plus the
//! recipe's own declared `provisioning.env` passthrough list reach the hook — each forwarded from
//! the daemon env only if present. The daemon's `LNRENT*` namespace (which may hold the BIP39 seed)
//! therefore NEVER reaches a hook by construction; secrets ride the stdin JSON, as the contract
//! always intended.

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

/// The fixed base env every hook receives (lnrent-y4m.7): the minimum for a shell script to find
/// tools and behave sanely. Nothing else passes by default — in particular no `LNRENT*` var. Each
/// is forwarded from the daemon env only when it is set there.
const BASE_HOOK_ENV: &[&str] = &["PATH", "HOME", "LANG", "LC_ALL", "TZ", "TMPDIR"];

/// Build the child env for a hook: the base allowlist plus the recipe's declared `env` passthrough,
/// each taken from the daemon env only when present. `.env_clear()` + exactly these — so the seed
/// and every other daemon var are excluded by construction. Recipe names are already
/// `[A-Z0-9_]`-shaped and `LNRENT*`-free ([`crate::recipe::Recipe::validate`]).
fn hook_env(env_passthrough: &[String]) -> Vec<(String, String)> {
    hook_env_from(|k| std::env::var(k).ok(), env_passthrough)
}

/// The pure allowlist logic behind [`hook_env`], with the env lookup injected so it is testable
/// without mutating the process environment: for each name in the base allowlist then the recipe
/// passthrough, forward `(name, value)` iff `get` returns a value. Nothing outside those two lists
/// is ever forwarded.
fn hook_env_from(
    get: impl Fn(&str) -> Option<String>,
    env_passthrough: &[String],
) -> Vec<(String, String)> {
    BASE_HOOK_ENV
        .iter()
        .map(|s| s.to_string())
        .chain(env_passthrough.iter().cloned())
        .filter_map(|name| get(&name).map(|v| (name, v)))
        .collect()
}

/// A successful hook run: the parsed stdout JSON (the delivery payload / op result data).
#[derive(Debug, Clone)]
pub struct HookOutput {
    pub stdout_json: Value,
}

/// Failure/cancellation backstop for a hook process group. Tokio's `kill_on_drop` kills only the
/// immediate child; this guard group-kills every descendant too whenever `run_hook` leaves without
/// having explicitly reaped the group — a DROPPED future (a caller cancelled on shutdown, leader
/// still alive) or a clean-exit-but-FAILURE bail (non-zero exit / non-JSON stdout, where a hook
/// that backgrounded a detached child would otherwise orphan it). It is disarmed only where the
/// group is already handled: after `reap` (which group-kills BEFORE it waits) on the timeout/cap/
/// no-exit paths, and on the SUCCESS path. See the `child.wait()` site for the one documented
/// residual (a reaped-leader `killpg` relies on a surviving descendant keeping the pgid reserved).
struct HookProcessGroup {
    pgid: Option<i32>,
}

impl HookProcessGroup {
    fn new(child: &tokio::process::Child) -> Self {
        Self {
            pgid: child.id().map(|pid| pid as i32),
        }
    }

    fn disarm(&mut self) {
        self.pgid = None;
    }
}

impl Drop for HookProcessGroup {
    fn drop(&mut self) {
        if let Some(pgid) = self.pgid {
            kill_hook_process_group(pgid);
        }
    }
}

/// Run `hook` (an absolute path) with `input` on stdin, bounded by `timeout` and `OUTPUT_CAP`.
/// A timeout, a cap breach on EITHER pipe, a non-zero exit, or non-JSON stdout is a failure —
/// failure cleanup group-kills any still-running hook processes and makes a bounded reap attempt
/// for the immediate child (not left only to Tokio's child-drop cleanup).
///
/// `env_passthrough` is the recipe's `provisioning.env` allowlist; the hook receives ONLY the base
/// env + those vars (lnrent-y4m.7). Callers pass `&recipe.provisioning.env`.
pub async fn run_hook(
    hook: &Path,
    input: &Value,
    timeout: Duration,
    env_passthrough: &[String],
) -> Result<HookOutput> {
    let mut child = spawn_hook(hook, env_passthrough).await?;
    let mut process_group = HookProcessGroup::new(&child);

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
    let read =
        async { tokio::try_join!(read_capped(out, OUTPUT_CAP), read_capped(err, OUTPUT_CAP)) };
    let (out_buf, err_buf) = match tokio::time::timeout(timeout, read).await {
        Err(_) => {
            reap(&mut child).await;
            process_group.disarm();
            bail!("hook {} timed out after {timeout:?}", hook.display());
        }
        Ok(Err(e)) => {
            // cap breach or read error -> kill the (possibly still-writing) child and fail
            reap(&mut child).await;
            process_group.disarm();
            return Err(anyhow!("hook {}: {e}", hook.display()));
        }
        Ok(Ok(bufs)) => bufs,
    };
    // Reads are done; the stdin feed (normally already finished) is no longer needed.
    feed.abort();

    // Both pipes hit EOF -> the child has closed its outputs; wait for exit (bounded). NOTE the
    // guard stays ARMED through the status/JSON checks below ON PURPOSE: a hook that backgrounds a
    // detached child (one that redirected the inherited stdout/stderr, so the pipes could reach
    // EOF while it runs) and then exits non-zero or emits non-JSON would otherwise orphan that
    // child — the same billed-resource leak this bead closes for the timeout path. On those bail
    // paths the guard's `Drop` group-kills it. A `wait()` ERROR likewise keeps the guard armed
    // (the `?` returns before any disarm) — correct, because the leader may still be alive.
    //
    // The one documented residual (adversarial y4m.12 review P3): on a clean-exit bail the leader
    // has just been reaped, so the guard's `Drop` `killpg(pgid)` relies on a SURVIVING descendant
    // to keep the pgid reserved (the exact orphan case it targets). If instead the last descendant
    // also exited in the intervening window, the `killpg` gets a harmless `ESRCH` — unless the
    // pgid were recycled onto an unrelated group in that same window. That window is a few
    // straight-line instructions with NO `.await` (so no scheduler yield) and pid recycling is not
    // immediate on Linux, making it effectively unreachable; it is inherent to reaping-the-leader-
    // then-signaling and not worth trading for the real clean-exit orphan gap above.
    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(s) => s.context("waiting on hook")?,
        Err(_) => {
            reap(&mut child).await;
            process_group.disarm();
            bail!(
                "hook {} did not exit after closing its output",
                hook.display()
            );
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
    process_group.disarm();
    Ok(HookOutput { stdout_json })
}

/// Spawn the hook, retrying briefly on TRANSIENT spawn failures: ETXTBSY (a just-written hook can
/// still be momentarily open-for-write — and a parallel fork/exec can race the same way) and
/// EAGAIN (fork hitting a transient resource limit under load). A real ENOENT/EACCES/etc. is NOT
/// retried — a missing or non-executable hook fails fast. Worst-case added delay ~150ms.
async fn spawn_hook(hook: &Path, env_passthrough: &[String]) -> Result<tokio::process::Child> {
    const ETXTBSY: i32 = 26; // "Text file busy"
    const EAGAIN: i32 = 11; // fork: resource temporarily unavailable
    let env = hook_env(env_passthrough);
    let mut attempt = 0u32;
    loop {
        match Command::new(hook)
            // Hook env hygiene (lnrent-y4m.7): start from EMPTY, add only the base allowlist + the
            // recipe's declared passthrough. The daemon's env (incl. any LNRENT_MNEMONIC seed)
            // never reaches the hook.
            .env_clear()
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            // Put the hook in its OWN process group (pgid == the child's pid — a NEW group led by
            // the child, so the DAEMON is never a member). On a timeout `reap` group-kills that
            // pgid, reaping not just the hook shell but every descendant it forked — e.g. an
            // in-flight `curl` mid droplet-create that would otherwise survive the shell's SIGKILL,
            // finish provisioning AFTER the daemon declared failure + refunded, and leave an
            // orphaned, still-billing droplet no teardown ever recorded (lnrent-y4m.12).
            .process_group(0)
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
            Err(e) => {
                return Err(
                    anyhow::Error::new(e).context(format!("spawning hook {}", hook.display()))
                )
            }
        }
    }
}

/// Explicitly kill the timed-out/over-producing hook — the WHOLE process group, not just the
/// immediate child — and reap it (bounded), so no zombie AND no orphaned descendant survives.
/// `spawn_hook` made the hook its own group leader (pgid == pid), so a group-kill reaps any
/// grandchild it forked (e.g. an in-flight `curl` still creating a billed droplet) that would
/// outlive a SIGKILL of just the shell (lnrent-y4m.12). `kill_on_drop` is only a backstop.
async fn reap(child: &mut tokio::process::Child) {
    // Capture the pid FIRST: once `child.wait()` reaps the process, `id()` returns None, so we must
    // read it before any wait. `None` here means it was already reaped — nothing to signal.
    if let Some(pid) = child.id() {
        kill_hook_process_group(pid as i32);
    }
    // Preserve the bounded 5s reap-wait; the group-kill above covers the child, so no separate
    // `start_kill` is needed. `kill_on_drop` remains the last-resort backstop.
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
}

fn kill_hook_process_group(pgid: i32) {
    // SAFETY: `pgid` comes only from our own just-spawned child, which `process_group(0)` made the
    // leader of a new group. It therefore names exactly that hook's group, never the daemon's.
    let rc = unsafe { libc::killpg(pgid, libc::SIGKILL) };
    if rc == -1 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::ESRCH) {
            tracing::debug!(pgid, error = %err, "killpg on hook group failed (best-effort)");
        }
    }
}

/// Read `r` to EOF, retaining the bytes, but return an error the moment the total would exceed
/// `cap` — so an over-producing hook fails FAST (memory- and time-bounded), not after draining.
async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    mut r: R,
    cap: usize,
) -> std::io::Result<Vec<u8>> {
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

    // Write an executable script into a fresh, unique temp dir and return its path. The dir name
    // mixes the pid, a high-resolution timestamp, a process-local monotonic seq, and the hook name
    // so two runs can never collide on the same path — even if the OS reuses this pid across
    // separate test-process runs (PID namespaces recycle small pids). Freshness matters beyond
    // avoiding a `create_dir` `AlreadyExists` panic: the group-kill tests read a `gc.pid` file from
    // this dir, and a stale pid left by an earlier run would let those tests pass VACUOUSLY.
    // `create_dir` (not `_all`) then asserts the dir really is brand-new.
    fn write_hook(name: &str, body: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "lnrent-runner-{}-{nanos}-{seq}-{name}",
            std::process::id()
        ));
        std::fs::create_dir(&dir).unwrap();
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
        let out = run_hook(&hook, &json!({"x": 1}), DEFAULT_TIMEOUT, &[])
            .await
            .unwrap();
        assert_eq!(out.stdout_json, json!({"ok": true}));
    }

    #[tokio::test]
    async fn failed_clean_exit_group_kills_forked_descendant() {
        for (name, exit, expected) in [
            ("nonzero", "echo '{}'; exit 1", "failed (exit"),
            ("non-json", "echo nope", "stdout is not JSON"),
        ] {
            // Redirect the background process's stdio so the runner sees EOF and reaps the shell
            // while the descendant is still alive. Both failure decisions happen after that wait.
            let hook = write_hook(
                name,
                &format!(
                    "#!/usr/bin/env bash\n\
                     gcdir=\"$(dirname \"$0\")\"\n\
                     sleep 30 >/dev/null 2>&1 &\n\
                     echo \"$!\" > \"$gcdir/gc.pid\"\n\
                     {exit}\n"
                ),
            );
            let pidfile = hook.parent().unwrap().join("gc.pid");

            let err = run_hook(&hook, &json!({}), DEFAULT_TIMEOUT, &[])
                .await
                .unwrap_err();
            assert!(err.to_string().contains(expected), "got: {err}");

            let gc_pid = std::fs::read_to_string(pidfile)
                .unwrap()
                .trim()
                .parse::<i32>()
                .unwrap();
            let mut gone = false;
            for _ in 0..50 {
                let rc = unsafe { libc::kill(gc_pid, 0) };
                if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                    gone = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(
                gone,
                "descendant pid {gc_pid} survived the {name} hook failure"
            );
        }
    }

    #[tokio::test]
    async fn timeout_kills_and_fails() {
        let hook = write_hook("slow", "#!/usr/bin/env bash\nsleep 5\necho '{}'\n");
        let err = run_hook(&hook, &json!({}), Duration::from_millis(200), &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    // lnrent-y4m.12: a timed-out hook that forked a LONG-LIVED grandchild must leave NO surviving
    // descendant after `reap` — the group-kill takes the whole process group, not just the immediate
    // shell. A SIGKILL of only the child would orphan that grandchild: the invisible-billed-droplet
    // failure (an in-flight `curl` completing the provision AFTER the daemon declared failure and
    // refunded) that this change exists to prevent. We prove the grandchild is GONE by a BOUNDED
    // `kill(pid, 0)` -> ESRCH poll (signal 0 delivers nothing — it only probes existence), NOT by
    // letting it self-exit; a regression fails the assertion instead of hanging the test.
    #[tokio::test]
    async fn timeout_group_kills_forked_descendant() {
        // A subshell backgrounds `sleep`, so the sleep is this hook's GRANDCHILD (mirroring a curl
        // spawned by a provisioning subshell). It stays in the hook's process group — no setsid/new
        // group, which would escape the group-kill. Record its pid next to the script (via $0's
        // dir, no env mutation), then hang so run_hook times out and `reap` group-kills.
        let hook = write_hook(
            "fork-grandchild",
            "#!/usr/bin/env bash\n\
             gcdir=\"$(dirname \"$0\")\"\n\
             ( sleep 30 & echo \"$!\" > \"$gcdir/gc.pid\" )\n\
             sleep 30\n",
        );
        let pidfile = hook.parent().unwrap().join("gc.pid");

        let err = run_hook(&hook, &json!({}), Duration::from_millis(300), &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"), "got: {err}");

        // The grandchild pid was written at hook start; by the time run_hook returned (timeout +
        // reap) the file exists. Poll briefly to be robust against the write race, then parse it.
        let mut gc_pid = None;
        for _ in 0..50 {
            if let Ok(s) = std::fs::read_to_string(&pidfile) {
                if let Ok(p) = s.trim().parse::<i32>() {
                    gc_pid = Some(p);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let gc_pid = gc_pid.expect("hook recorded the grandchild pid");

        // Assert the grandchild is GONE. `reap` group-killed before returning, so it is already
        // dead or being reaped by init; the bounded poll only absorbs that reaping latency.
        // `kill(pid, 0)` sends NO signal (existence probe only) — safe, and we only ever probe a
        // pid our own hook spawned.
        let mut gone = false;
        for _ in 0..50 {
            let rc = unsafe { libc::kill(gc_pid, 0) };
            if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            gone,
            "grandchild pid {gc_pid} survived reap — the hook's process group was not killed"
        );
    }

    // Dropping an in-flight run_hook future (as supervisor shutdown does after its bounded drain)
    // must kill the hook's process group too. `kill_on_drop` covers only the immediate shell, so
    // without the process-group drop guard this leaves the long-lived grandchild running.
    #[tokio::test]
    async fn cancelled_hook_group_kills_forked_descendant() {
        let hook = write_hook(
            "cancel-fork-grandchild",
            "#!/usr/bin/env bash\n\
             gcdir=\"$(dirname \"$0\")\"\n\
             ( sleep 30 & echo \"$!\" > \"$gcdir/gc.pid\" )\n\
             sleep 30\n",
        );
        let pidfile = hook.parent().unwrap().join("gc.pid");
        let task =
            tokio::spawn(async move { run_hook(&hook, &json!({}), DEFAULT_TIMEOUT, &[]).await });

        let mut gc_pid = None;
        for _ in 0..50 {
            if let Ok(s) = std::fs::read_to_string(&pidfile) {
                if let Ok(p) = s.trim().parse::<i32>() {
                    gc_pid = Some(p);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let gc_pid = match gc_pid {
            Some(pid) => pid,
            None => {
                task.abort();
                let _ = task.await;
                panic!("hook did not record the grandchild pid before cancellation");
            }
        };

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let mut gone = false;
        for _ in 0..50 {
            let rc = unsafe { libc::kill(gc_pid, 0) };
            if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if !gone {
            // Regression cleanup: signal only the pid this test hook recorded, so a failing test
            // does not itself leave the long-lived descendant behind.
            unsafe {
                libc::kill(gc_pid, libc::SIGKILL);
            }
        }
        assert!(
            gone,
            "grandchild pid {gc_pid} survived cancellation of run_hook"
        );
    }

    // A hook that never drains stdin + a payload larger than the pipe buffer must still hit the
    // timeout — the stdin write must not block BEFORE the timeout applies (codex re-review).
    // Under the pre-fix code this test would hang forever instead of failing.
    #[tokio::test]
    async fn large_stdin_to_a_nonreading_hook_times_out() {
        let hook = write_hook("ignore-stdin", "#!/usr/bin/env bash\nsleep 5\n");
        let big = json!({ "blob": "x".repeat(256 * 1024) }); // >> the ~64 KiB pipe buffer
        let err = run_hook(&hook, &big, Duration::from_millis(300), &[])
            .await
            .unwrap_err();
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

        let prov = run_hook(
            &r.hook("provision"),
            &json!({"subscription": {"id": "s1"}}),
            DEFAULT_TIMEOUT,
            &r.provisioning.env,
        )
        .await
        .expect("provision runs");
        assert!(
            prov.stdout_json.get("payload").is_some(),
            "provision returns a delivery payload"
        );

        let op = r.operation("status").expect("status op declared");
        let res = run_hook(&r.op_hook(op), &json!({}), DEFAULT_TIMEOUT, &[])
            .await
            .expect("op runs");
        assert_eq!(res.stdout_json["state"], json!("running"));
    }

    #[tokio::test]
    async fn non_json_stdout_is_failure() {
        let hook = write_hook("garbage", "#!/usr/bin/env bash\necho not-json\n");
        let err = run_hook(&hook, &json!({}), DEFAULT_TIMEOUT, &[])
            .await
            .unwrap_err();
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
        let err = run_hook(&hook, &json!({}), Duration::from_millis(800), &[])
            .await
            .unwrap_err();
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
        let err = run_hook(&hook, &big, Duration::from_millis(800), &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }

    #[tokio::test]
    async fn oversized_stderr_fails_fast_on_cap() {
        // A cap breach on STDERR (not stdout) must also fail fast (codex #2).
        let hook = write_hook(
            "flood-err",
            "#!/usr/bin/env bash\nhead -c 5000000 /dev/zero | tr '\\0' 'a' >&2\necho '{}'\n",
        );
        let err = run_hook(&hook, &json!({}), Duration::from_millis(800), &[])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeded the"), "got: {err}");
    }

    // lnrent-y4m.7: the pure allowlist logic. Forward ONLY the base env + the recipe passthrough,
    // each only when the daemon env has it — never a seed/undeclared var, even if present in env.
    #[test]
    fn hook_env_forwards_only_base_and_declared_vars() {
        let daemon_env = |k: &str| -> Option<String> {
            match k {
                "PATH" => Some("/usr/bin".into()),
                "HOME" => Some("/root".into()),
                "LNRENT_MNEMONIC" => Some("twelve seed words".into()),
                "DO_TOKEN" => Some("dop_v1_secret".into()),
                "UNDECLARED_X" => Some("nope".into()),
                _ => None, // LANG/LC_ALL/TZ/TMPDIR unset here
            }
        };
        let env = hook_env_from(daemon_env, &["DO_TOKEN".to_string()]);
        let has = |k: &str| env.iter().any(|(name, _)| name == k);

        assert!(
            has("PATH") && has("HOME"),
            "base allowlist forwarded when present"
        );
        assert!(has("DO_TOKEN"), "a recipe-declared var passes through");
        assert!(!has("LNRENT_MNEMONIC"), "the seed is NEVER forwarded");
        assert!(
            !has("UNDECLARED_X"),
            "an undeclared daemon var is not forwarded"
        );
        // Unset base vars are simply skipped (not forwarded empty).
        assert!(!has("LANG"), "an unset base var is skipped");
    }

    // lnrent-y4m.7: prove the REAL `.env_clear()` at the process level — a hook spawned while the
    // daemon (this test process) has LNRENT_MNEMONIC + an undeclared var set does NOT see them, but
    // DOES see a recipe-declared var + the base PATH. Serialized (process-global env mutation) and
    // restores prior values.
    #[tokio::test]
    async fn spawned_hook_never_sees_the_seed_or_undeclared_env() {
        // A tokio mutex (not std) so the guard can be safely held across the `run_hook` await while
        // the process-global env is mutated — no other env-mutating test runs concurrently.
        static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _guard = ENV_LOCK.lock().await;

        let prior_seed = std::env::var("LNRENT_MNEMONIC").ok();
        let prior_tok = std::env::var("Y4M7_TEST_TOKEN").ok();
        let prior_undecl = std::env::var("Y4M7_TEST_UNDECLARED").ok();
        std::env::set_var("LNRENT_MNEMONIC", "twelve seed words");
        std::env::set_var("Y4M7_TEST_TOKEN", "declared-value");
        std::env::set_var("Y4M7_TEST_UNDECLARED", "leak-me");

        // A hook that reports which vars it can see, as JSON booleans (declare -p succeeds iff set).
        let hook = write_hook(
            "dump-env",
            "#!/usr/bin/env bash\nread -r _ 2>/dev/null || true\n\
             chk() { if declare -p \"$1\" >/dev/null 2>&1; then echo true; else echo false; fi; }\n\
             printf '{\"path\":%s,\"seed\":%s,\"token\":%s,\"undeclared\":%s}\\n' \
             \"$(chk PATH)\" \"$(chk LNRENT_MNEMONIC)\" \"$(chk Y4M7_TEST_TOKEN)\" \"$(chk Y4M7_TEST_UNDECLARED)\"\n",
        );
        let out = run_hook(
            &hook,
            &json!({}),
            DEFAULT_TIMEOUT,
            &["Y4M7_TEST_TOKEN".to_string()],
        )
        .await
        .unwrap();
        let j = out.stdout_json;

        // Restore before asserting so a failure can't leak env into other tests.
        match prior_seed {
            Some(v) => std::env::set_var("LNRENT_MNEMONIC", v),
            None => std::env::remove_var("LNRENT_MNEMONIC"),
        }
        match prior_tok {
            Some(v) => std::env::set_var("Y4M7_TEST_TOKEN", v),
            None => std::env::remove_var("Y4M7_TEST_TOKEN"),
        }
        match prior_undecl {
            Some(v) => std::env::set_var("Y4M7_TEST_UNDECLARED", v),
            None => std::env::remove_var("Y4M7_TEST_UNDECLARED"),
        }

        assert_eq!(
            j["seed"],
            json!(false),
            "the LNRENT_MNEMONIC seed NEVER reaches a hook"
        );
        assert_eq!(
            j["undeclared"],
            json!(false),
            "an undeclared daemon var does not reach a hook"
        );
        assert_eq!(
            j["token"],
            json!(true),
            "a recipe-declared var passes through"
        );
        assert_eq!(j["path"], json!(true), "the base PATH is present");
    }
}
