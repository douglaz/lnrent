//! Local CLI <-> daemon IPC over a Unix-domain socket (lnrent-7fp.12; ADR-0001, ADR-0014;
//! SPEC §4.2/§4.7/§10). The daemon owns the socket; the `lnrent` CLI and Claude skills act
//! ONLY through it — they never touch sqlite directly, so the daemon stays the sole writer.
//! This is the OPERATOR's agent surface: every reply is structured JSON (so an operator agent
//! drives it), and it is never network-reachable (a UDS with owner-only perms + a peer-uid
//! gate on accept, no HTTP/MCP).

use crate::backends::{PaymentBackend, DEV_SETTLE_UNSUPPORTED};
use crate::clock::Clock;
use crate::provision;
use crate::recipe::Recipe;
use crate::refund_resolver::{detect_form, DestForm};
use crate::relay_status::RelayStatusCell;
use crate::store::Store;
use crate::supervisor::{refund_readiness_report_with_probe, RefundReadinessProbe};
use crate::teardown;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

/// A request from the CLI to the daemon. One JSON object per line.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Status,
    Recipes,
    Money,
    /// Wallet-vs-books reconciliation (lnrent-urw.10 §F): the SOLE place the live federation balance
    /// is read. Report-only — compares `available_balance_msat()` against the ledger `expected_msat`
    /// and returns `{wallet_msat, expected_msat, verdict}`; never mutates state or gates a payment.
    Reconcile,
    /// Go-live preflight (lnrent-y4m.9): probe the three EXTERNAL dependencies — refund gateway,
    /// federation guardians, provider API token — and report per-check `{name, ok, detail}` plus an
    /// aggregate `ok`. Read-only; the only network I/O is the three probes themselves.
    Preflight,
    Subs,
    Sub { id: String },
    /// Open teardown dead-letters (lnrent-urw.2): failed retention `destroy` hooks + the
    /// provision-failure cleanup backlog — provider resources that may still be billing.
    Teardowns,
    /// Per-relay liveness (lnrent-urw.6): `{url, connected, status, last_connected_at}` from the
    /// nostr-sdk pool. The out-of-band read for a relay blackout (the alert can't be delivered
    /// during one).
    Relays,
    /// Non-terminal + parked-FAILED refunds (lnrent-urw.5): the per-item view behind `lnrent money`'s
    /// `parked_count`.
    Refunds,
    /// Re-drive one parked-FAILED refund (lnrent-urw.5): reset it to PENDING so the refunder re-runs
    /// the real resolver + capped-pay path. The only refund actuator — there is no cancel/abandon.
    RefundRetry { id: String },
    /// DRY-RUN operator sweep quote (gate1-operator-sweep, urw.3): price the outlay + read the ledger
    /// surplus for `bolt11` and report the ALLOW/REFUSE verdict. No writes, no balance read.
    SweepQuote { bolt11: String },
    /// EXECUTE an operator sweep (gate1-operator-sweep, urw.3): gate on ledger surplus, write the
    /// durable PENDING intent, and pay the operator's own `bolt11` capped at the quote. Ledger-only
    /// authorization — the federation balance is never read.
    Sweep { bolt11: String },
    AdminSuspend { id: String },
    AdminResume { id: String },
    DevSettle { subscription_id: String },
}

impl Request {
    /// Whether this request writes durable state and/or gates a payment. The graceful shutdown
    /// drain (`serve_with_shutdown`) branches on this to decide who it MUST await: a MUTATING op in
    /// flight is run to completion so its committed txn + reply is never lost; a READ-ONLY op is
    /// drain-EXEMPT — it commits nothing, so a graceful shutdown may cancel it mid-flight and reply
    /// "shutting down" promptly instead of pinning the drain.
    ///
    /// WHY the split (lnrent-j3c): read-only requests include the slow `Preflight` (network probes
    /// bounded at 10s/15s in `preflight.rs`, well above the 3s `SHUTDOWN_DRAIN`) and the
    /// network-touching `Reconcile`/`Money`. One of those in flight at shutdown would pin the drain
    /// past the supervisor's grace, forcing the abort (supervisor.rs `timeout(SHUTDOWN_DRAIN, ..)`
    /// → `AbortOnDrop`) that kills a CONCURRENT committed-but-unreplied handler — the exact window
    /// y4m.13 closed for idle peers, otherwise reopened by a slow dispatch. Cancelling a read-only
    /// op loses nothing durable (a dropped store read / network probe commits nothing). MUTATING
    /// ops (`RefundRetry`/`Sweep`/`AdminSuspend`/`AdminResume`/`DevSettle`) write a durable txn
    /// and/or gate a payment, so the drain MUST let them finish — never cancelled. Most are a fast
    /// local sqlite txn + reply that comfortably fits `SHUTDOWN_DRAIN` (`RefundRetry` only CASes a
    /// row to PENDING; the admin transitions and `DevSettle` are single txns). The ONE exception is
    /// `Request::Sweep`, whose dispatch awaits a real Lightning/fedimint pay bounded at
    /// `lnv2_backend::PAY_AWAIT_TIMEOUT` (120s) — far above `SHUTDOWN_DRAIN`. We STILL do not
    /// cancel it: dropping an in-flight capped pay mid-settle would risk paying out without recording
    /// SENT, so money safety wins over drain promptness. A slow `Sweep` in flight at shutdown can
    /// therefore still overrun the drain and force the supervisor abort — but that is a PRE-EXISTING
    /// hazard j3c neither introduces nor widens (all ops were awaited before), and the sweep's
    /// durable PENDING intent makes an aborted pay idempotently recoverable on restart. j3c closes
    /// only the READ-ONLY slow-dispatch reopening of the window, not the sweep case. `SweepQuote` is
    /// a DRY-RUN (no writes) → read-only.
    ///
    /// EXHAUSTIVE by design (no `_` wildcard): a future variant must fail to compile here until it
    /// is classified, so nothing silently defaults into the drain-exempt (cancel-me) bucket.
    fn is_mutating(&self) -> bool {
        match self {
            Request::RefundRetry { .. }
            | Request::Sweep { .. }
            | Request::AdminSuspend { .. }
            | Request::AdminResume { .. }
            | Request::DevSettle { .. } => true,
            Request::Status
            | Request::Recipes
            | Request::Money
            | Request::Reconcile
            | Request::Preflight
            | Request::Subs
            | Request::Sub { .. }
            | Request::Teardowns
            | Request::Relays
            | Request::Refunds
            | Request::SweepQuote { .. } => false,
        }
    }
}

/// A structured error a caller (human or agent) can branch on (mirrors §5.1 error shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: String,
    pub message: String,
    pub retryable: bool, // §5.1/§4.7 structured-error taxonomy
}

/// The daemon's reply. One JSON object per line: `{ok, data?}` or `{ok:false, error}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<IpcError>,
}

impl Reply {
    pub fn ok(data: Value) -> Reply {
        Reply {
            ok: true,
            data: Some(data),
            error: None,
        }
    }
    pub fn err(code: &str, message: impl Into<String>) -> Reply {
        Reply {
            ok: false,
            data: None,
            error: Some(IpcError {
                code: code.into(),
                message: message.into(),
                retryable: false,
            }),
        }
    }
}

/// Max bytes for one request frame; an over-long line is rejected (a same-user process must
/// not be able to memory-DoS the daemon, codex #9). Requests are tiny JSON.
const MAX_REQUEST_BYTES: u64 = 1 << 18; // 256 KiB

/// Deadline for reading the ONE request frame off an accepted connection (lnrent-y4m.13). A
/// legitimate client writes its single JSON line immediately after connecting, so seconds is
/// generous. The bound exists for the graceful drain: `serve_with_shutdown` AWAITS in-flight
/// handlers at shutdown (so an admin txn + reply isn't lost), so without it one idle peer
/// (connect, send nothing) pins the drain until the supervisor's SHUTDOWN_DRAIN (3s) abort
/// kills the whole task set — which can kill a CONCURRENT handler after its txn committed but
/// before its reply was written. The idle-path budget — this deadline plus
/// [`TIMED_OUT_REPLY_WRITE_BOUND`] — stays UNDER that 3s window, so even an idle connection
/// accepted the instant before shutdown self-completes inside the graceful drain.
/// (A `cfg!` expression, not attribute-split consts: this file's convention — which the
/// one-call-site invariant test relies on — is that the first cfg-test ATTRIBUTE in the text
/// marks the tests module. Test-tuned to milliseconds: the tests hold real idle sockets
/// against this deadline.)
const REQUEST_READ_DEADLINE: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(500)
} else {
    REQUEST_READ_DEADLINE_PROD
};

/// The PRODUCTION read deadline, named so the compile-time budget guard below checks the real
/// value even in test builds (where [`REQUEST_READ_DEADLINE`] is test-tuned to milliseconds).
const REQUEST_READ_DEADLINE_PROD: std::time::Duration = std::time::Duration::from_secs(2);

// Compile-time budget guard (adversarial y4m.13 review): the idle-connection path — the
// PRODUCTION read deadline plus the bounded timeout reply — must complete inside the
// supervisor's SHUTDOWN_DRAIN window, or one idle peer accepted just before shutdown would again
// force the abort that can kill a concurrent committed-but-unreplied handler. A future bump of
// either constant that silently reintroduces that window becomes a compile error here.
const _: () = assert!(
    REQUEST_READ_DEADLINE_PROD.as_millis() + TIMED_OUT_REPLY_WRITE_BOUND.as_millis()
        < crate::supervisor::SHUTDOWN_DRAIN.as_millis(),
    "idle IPC connection budget must stay under the supervisor drain window"
);

/// Bound on the best-effort `bad_request` reply after a read timeout: the tiny reply fits the
/// connection's empty send buffer (so this never blocks in practice), but a peer that somehow
/// wedges the write must not wedge the handler — the prompt close, not the reply, is the point.
const TIMED_OUT_REPLY_WRITE_BOUND: std::time::Duration = std::time::Duration::from_millis(500);

/// Peer-cred authorization for one accepted IPC connection (lnrent-y4m.10, defense-in-depth
/// behind the 0600 socket perms): only the daemon's own uid — the operator account — or root
/// (who could bypass any uid check anyway) may issue operator commands. Pure, so the decision
/// is unit-testable without a foreign-uid peer (which would need root to construct).
fn peer_allowed(peer_uid: u32, daemon_uid: u32) -> bool {
    peer_uid == daemon_uid || peer_uid == 0
}

/// Atomically create a fresh private staging directory whose socket path suffix
/// (`/.iXXXXXX/s`) is shorter than `/lnrent.sock`, preserving every final path that fits in
/// `sockaddr_un.sun_path`. Existing names are skipped rather than removed: another daemon may
/// still be using one, and crash leftovers are harmless unreachable directories.
fn create_staging_dir(parent: &Path) -> Result<PathBuf> {
    const NAME_SPACE: u64 = 1 << 24;
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    for _ in 0..NAME_SPACE {
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % NAME_SPACE;
        let staging = parent.join(format!(".i{n:06x}"));
        match std::fs::DirBuilder::new().mode(0o700).create(&staging) {
            Ok(()) => return Ok(staging),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("creating ipc staging dir {}", staging.display()))
            }
        }
    }
    anyhow::bail!(
        "all private ipc staging names are occupied in {}",
        parent.display()
    )
}

/// Bind the IPC listener with NO perms window and NO missing-path window (lnrent-y4m.10).
/// The socket inode is created inside a fresh 0700 staging directory next to the final path —
/// unreachable by other users regardless of the process umask — chmod'd to 0600 while still
/// unreachable, then atomically `rename(2)`d onto `path`. The rename also REPLACES a stale
/// socket in the same syscall, closing the old remove-then-bind gap where a client could
/// observe the path missing. The listener serves the inode regardless of the path move.
/// A staged bind (not `umask(2)`) because umask is PROCESS-GLOBAL: flipping it here would race
/// concurrently-spawned hook processes' file creation.
/// Returns the listener plus the socket file's owner uid — the daemon's effective uid, captured
/// while the inode was still private, for the per-connection peer-cred gate (no libc call).
fn bind_owner_only(path: &Path) -> Result<(UnixListener, u32)> {
    // Validate the FINAL path against the sockaddr_un limit up front (codex PR-34): the staged
    // path (`/.iXXXXXX/s`) is one byte SHORTER than `/lnrent.sock`, so for a data dir exactly one
    // byte over the limit the staged bind would succeed and the rename would publish a live
    // socket at a pathname clients cannot even encode — the daemon would run while every operator
    // IPC call fails. Fail startup loudly instead, exactly as the old direct bind did.
    if path.as_os_str().len() > MAX_SUN_PATH_BYTES {
        anyhow::bail!(
            "ipc socket path {} is {} bytes — longer than the {MAX_SUN_PATH_BYTES}-byte unix \
             socket limit; use a shorter data dir",
            path.display(),
            path.as_os_str().len()
        );
    }
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let staging = create_staging_dir(parent)?;
    let bound = bind_staged(&staging, path);
    // Success or error, the staging dir must not linger next to the socket.
    let _ = std::fs::remove_dir_all(&staging);
    bound
}

/// Max usable `sun_path` bytes on Linux (108 including the terminating NUL).
const MAX_SUN_PATH_BYTES: usize = 107;

/// The inside of [`bind_owner_only`], split out so the caller removes the staging dir on both
/// the success and every error path.
fn bind_staged(staging: &Path, path: &Path) -> Result<(UnixListener, u32)> {
    // mkdir's mode is still masked by the umask — it can only STRIP bits from 0700, never widen,
    // so the dir is private regardless — but an aggressive umask could strip owner bits and break
    // the bind below. Pin 0700 explicitly.
    std::fs::set_permissions(staging, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("perms on {}", staging.display()))?;
    // One-char temp name (the rename target defines the final name): keeps the staged path from
    // exceeding sun_path where the final path itself still fits.
    let tmp = staging.join("s");
    let listener =
        UnixListener::bind(&tmp).with_context(|| format!("binding {}", tmp.display()))?;
    // Belt-and-suspenders: owner-only on the socket itself while it is still unreachable.
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("perms on {}", tmp.display()))?;
    // The just-created socket's owner IS the daemon's effective uid.
    let daemon_uid = std::fs::metadata(&tmp)
        .with_context(|| format!("stat {}", tmp.display()))?
        .uid();
    evict_unsafe_final_path(path, daemon_uid)?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("publishing {} -> {}", tmp.display(), path.display()))?;
    Ok((listener, daemon_uid))
}

/// Remove an UNSAFE pre-existing entry at the final socket path just before publishing
/// (adversarial y4m.10 review): until the rename lands, whatever sits at `path` keeps serving
/// startup-racing clients — a loose or foreign-owned socket, a plain file, or a symlink (which
/// clients would follow) could impersonate the daemon for that window. Planting one requires
/// write access to the data dir (with which an attacker could as well swap the socket at ANY
/// later moment), so this is defense-in-depth, not a trust boundary. ONLY an owner-only (0600,
/// our-euid) real socket — the legit remnant of a previous run — is left in place, preserving the
/// no-missing-path-window atomic replacement for the normal restart; anything else is unlinked (a
/// brief missing-path window beats serving an impostor endpoint). `symlink_metadata` so a symlink
/// is judged (and removed) as itself, never followed.
fn evict_unsafe_final_path(path: &Path, daemon_uid: u32) -> Result<()> {
    use std::os::unix::fs::FileTypeExt;
    let meta = match std::fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("inspecting stale {}", path.display())),
    };
    let safe_stale_socket = meta.file_type().is_socket()
        && meta.mode() & 0o777 == 0o600
        && meta.uid() == daemon_uid;
    if !safe_stale_socket {
        tracing::warn!(
            path = %path.display(),
            "removing an unsafe pre-existing entry at the ipc socket path (not an owner-only \
             socket) before publishing the real one"
        );
        std::fs::remove_file(path)
            .with_context(|| format!("removing unsafe stale {}", path.display()))?;
    }
    Ok(())
}

/// Serve IPC on `path` until the listener errors. Each connection is one request -> one reply.
/// The socket is published owner-only via an atomic staged bind ([`bind_owner_only`] — never
/// observable looser than 0600, and a stale socket is replaced with no missing-path window), and
/// every accepted peer is uid-gated ([`peer_allowed`]) before any request byte is read. This is
/// the never-shutdown form; the daemon supervisor (lnrent-7fp.21) uses [`serve_with_shutdown`].
pub async fn serve(
    store: Store,
    recipes: Arc<Vec<Recipe>>,
    clock: Arc<dyn Clock>,
    payment: Arc<dyn PaymentBackend>,
    relays: RelayStatusCell,
    path: impl AsRef<Path>,
) -> Result<()> {
    // A signal that never fires: the loop only ends on a listener error.
    let (_never, rx) = tokio::sync::watch::channel(false);
    serve_with_shutdown(store, recipes, clock, payment, relays, path, rx).await
}

/// Like [`serve`] but stops accepting new connections once `shutdown` flips to `true`, returning
/// `Ok(())` for a graceful stop. In-flight connections each commit on their own spawned task; the
/// store actor (sole writer, ADR-0001) serializes their writes regardless of this accept loop. The
/// wire protocol is unchanged — this only adds a cancellation arm to the accept loop.
pub async fn serve_with_shutdown(
    store: Store,
    recipes: Arc<Vec<Recipe>>,
    clock: Arc<dyn Clock>,
    payment: Arc<dyn PaymentBackend>,
    relays: RelayStatusCell,
    path: impl AsRef<Path>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let path = path.as_ref();
    if *shutdown.borrow() {
        return Ok(()); // already shutting down — never bind
    }
    let (listener, daemon_uid) = bind_owner_only(path)?;
    tracing::info!(socket = %path.display(), "ipc serving (staged 0600 bind, atomic publish)");
    // Track the spawned per-connection handlers so a graceful shutdown can AWAIT the ones still
    // in flight — committing an admin txn and writing its reply — instead of dropping them when the
    // accept loop stops (the handlers were previously detached, so a shutdown could lose an in-flight
    // admin txn+reply, violating the graceful-shutdown AC).
    let mut conns = tokio::task::JoinSet::new();
    // Cooperative shutdown signal for READ-ONLY handlers (lnrent-j3c). The drain below AWAITS
    // in-flight handlers so a MUTATING op lands its commit + reply — but a read-only op commits
    // nothing, and the slow ones (`Preflight`'s 10s/15s probes, `Reconcile`/`Money`'s network
    // touches) would pin that await past `SHUTDOWN_DRAIN` and force the supervisor abort that can
    // kill a concurrent committed-but-unreplied MUTATING handler. So on shutdown we cancel this
    // token; a read-only handler observing it drops its in-flight dispatch (safe — nothing durable)
    // and returns a prompt "shutting_down" reply. MUTATING handlers ignore the token and always run
    // to completion. In the never-shutdown [`serve`] wrapper this token is simply never cancelled.
    let shutdown_token = CancellationToken::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _addr) = accepted?;
                // Peer-cred gate (lnrent-y4m.10): a foreign-uid peer is dropped BEFORE any
                // request byte is read — no reply, no protocol surface. Unreadable creds on a
                // platform that has SO_PEERCRED are a refusal, never a pass.
                match stream.peer_cred() {
                    Ok(cred) if peer_allowed(cred.uid(), daemon_uid) => {}
                    Ok(cred) => {
                        tracing::warn!(peer_uid = cred.uid(), "ipc: refused connection from foreign uid");
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "ipc: refused connection (peer credentials unreadable)");
                        continue;
                    }
                }
                let (store, recipes, clock, payment, relays) = (
                    store.clone(),
                    recipes.clone(),
                    clock.clone(),
                    payment.clone(),
                    relays.clone(),
                );
                let token = shutdown_token.clone();
                conns.spawn(async move {
                    if let Err(e) =
                        handle_conn(stream, store, recipes, clock, payment, relays, token).await
                    {
                        tracing::warn!(error = %e, "ipc connection error");
                    }
                });
                // Reap completed handlers so the set doesn't grow unbounded under steady load.
                while conns.try_join_next().is_some() {}
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(socket = %path.display(), "ipc serve: shutdown signaled; draining in-flight handlers");
                    let _ = std::fs::remove_file(path);
                    // Wake read-only handlers FIRST (lnrent-j3c): a slow read-only op (e.g.
                    // `Preflight`'s 10s/15s probes) observes this cancel and returns its prompt
                    // "shutting_down" reply instead of pinning the await-drain below past
                    // SHUTDOWN_DRAIN. Must fire BEFORE the drain loop so those handlers are already
                    // winding down as it awaits them.
                    shutdown_token.cancel();
                    // Let in-flight handlers finish: MUTATING handlers ignore the token and run
                    // their txn + reply to completion; read-only handlers self-complete promptly on
                    // the cancel above. Most mutating ops are a fast local txn, so with read-only
                    // ops now exempt a slow READ-ONLY dispatch no longer forces the supervisor abort.
                    // Still ultimately bounded by the supervisor's shutdown grace, which aborts this
                    // whole task if the drain overruns — the one residual overrun is an in-flight
                    // `Sweep`, whose fedimint pay (`PAY_AWAIT_TIMEOUT`, 120s) we deliberately never
                    // cancel for money safety; that pre-existing gap is unchanged by j3c and its
                    // durable PENDING intent makes an aborted pay recoverable on restart.
                    while conns.join_next().await.is_some() {}
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_conn(
    stream: UnixStream,
    store: Store,
    recipes: Arc<Vec<Recipe>>,
    clock: Arc<dyn Clock>,
    payment: Arc<dyn PaymentBackend>,
    relays: RelayStatusCell,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let (rd, mut wr) = stream.into_split();
    // Bounded read: cap the request frame so an over-long line can't exhaust memory, and put a
    // deadline on it (lnrent-y4m.13) so an idle peer can't hold this handler open — the graceful
    // shutdown drain awaits it. A read within the deadline behaves byte-for-byte as before.
    let mut rd = BufReader::new(rd.take(MAX_REQUEST_BYTES));
    let mut line = String::new();
    match tokio::time::timeout(REQUEST_READ_DEADLINE, rd.read_line(&mut line)).await {
        Err(_elapsed) => {
            // Read deadline hit: reply best-effort — mirroring the over-long/unterminated-frame
            // path — and close promptly. The write is bounded and its result ignored: an
            // unresponsive peer must not wedge the close either (one that stopped reading just
            // sees the close). Logged so a wedged/crashed same-uid client is diagnosable — the
            // foreign-uid refusal path warns, this should too (review P3).
            tracing::warn!(
                deadline = ?REQUEST_READ_DEADLINE,
                "ipc connection sent no request within the read deadline; closing"
            );
            let mut out =
                serde_json::to_vec(&Reply::err("bad_request", "request read timed out"))?;
            out.push(b'\n');
            let _ = tokio::time::timeout(TIMED_OUT_REPLY_WRITE_BOUND, async {
                let _ = wr.write_all(&out).await;
                let _ = wr.flush().await;
            })
            .await;
            return Ok(());
        }
        Ok(read) => read?,
    };
    let reply = if !line.ends_with('\n') {
        // hit the byte cap without a line terminator -> over-long / malformed frame
        Reply::err("bad_request", "request too large or unterminated")
    } else {
        match serde_json::from_str::<Request>(line.trim()) {
            Ok(req) if req.is_mutating() => {
                // A MUTATING op must land its durable commit + reply within the drain and is NEVER
                // cancelled (lnrent-j3c) — dropping it mid-flight could lose a committed txn or a
                // paid-but-unrecorded settle. Most are a fast local sqlite txn + reply the drain
                // easily fits; the exception is `Sweep`, whose fedimint pay can run to
                // `PAY_AWAIT_TIMEOUT` (120s) and which we still don't cancel for money safety (see
                // `Request::is_mutating` for the residual-gap reasoning).
                dispatch(req, &store, &recipes, &clock, &payment, &relays).await
            }
            Ok(req) => {
                // A READ-ONLY op is drain-EXEMPT (lnrent-j3c): if a graceful shutdown fires while it
                // runs, cancel it and reply promptly rather than pin the drain (the slow `Preflight`
                // probes run 10s/15s, past SHUTDOWN_DRAIN). On cancel the `dispatch` future is
                // DROPPED — safe for a read-only op, which commits nothing durable (a dropped store
                // read / network probe loses nothing). The tiny error reply then goes out the
                // unchanged write path below, exactly like any other `Reply`.
                tokio::select! {
                    r = dispatch(req, &store, &recipes, &clock, &payment, &relays) => r,
                    _ = shutdown_token.cancelled() => Reply {
                        ok: false,
                        data: None,
                        error: Some(IpcError {
                            code: "shutting_down".into(),
                            message: "daemon is shutting down; read-only request aborted, retry after restart".into(),
                            // A restart race is transient; machine callers may safely retry the
                            // read-only request against the replacement daemon.
                            retryable: true,
                        }),
                    },
                }
            }
            Err(e) => Reply::err("bad_request", format!("invalid request: {e}")),
        }
    };
    let mut out = serde_json::to_vec(&reply)?;
    out.push(b'\n');
    wr.write_all(&out).await?;
    wr.flush().await?;
    Ok(())
}

/// Route a request to the store actor (reads) / a journaled transaction (admin mutations).
pub async fn dispatch(
    req: Request,
    store: &Store,
    recipes: &Arc<Vec<Recipe>>,
    clock: &Arc<dyn Clock>,
    payment: &Arc<dyn PaymentBackend>,
    relays: &RelayStatusCell,
) -> Reply {
    match req {
        Request::Status => {
            let subs = store
                .read(|c| {
                    Ok(c.query_row("SELECT count(*) FROM subscription", [], |r| {
                        r.get::<_, i64>(0)
                    })?)
                })
                .await;
            let recipe_id = recipes.first().map(|r| r.service.id.clone()).unwrap_or_default();
            // `open_teardowns` folds BOTH owed-cleanup sources (lnrent-urw.2): the reconcile
            // `teardown_failure` rows and the provision-failure cleanup backlog.
            let open_teardowns = async {
                let failures = teardown::open_count(store).await?;
                let (cleanups, _) = provision::open_cleanups_summary_for(store, &recipe_id).await?;
                anyhow::Ok(failures + cleanups)
            }
            .await;
            // A one-line relay-liveness summary (lnrent-urw.6): total vs currently-connected, from
            // the maintenance loop's shared snapshot. `relays_connected < relays_total` (or 0/0
            // before the first refresh) is the at-a-glance blackout signal; `Request::Relays` has
            // the per-relay detail.
            let relay_rows = relays.get();
            let relays_connected = relay_rows.iter().filter(|r| r.connected).count();
            match (subs, open_teardowns) {
                (Ok(n), Ok(t)) => Reply::ok(json!({
                    "daemon": "ok",
                    "recipes": recipes.len(),
                    "subscriptions": n,
                    "open_teardowns": t,
                    "relays_total": relay_rows.len(),
                    "relays_connected": relays_connected,
                })),
                (Err(e), _) | (_, Err(e)) => Reply::err("internal", e.to_string()),
            }
        }

        Request::Relays => Reply::ok(json!(relays.get())),

        Request::Recipes => {
            let list: Vec<Value> = recipes
                .iter()
                .map(|r| json!({"id": r.service.id, "name": r.service.name, "version": r.service.version, "summary": r.service.summary}))
                .collect();
            Reply::ok(json!(list))
        }

        Request::Money => {
            // §E: network-free apart from the gateway + federation LIVENESS probes. The balance
            // operand is the ledger `expected_msat` (carried in the report), NOT a wallet read — a
            // plain `lnrent money` makes NO `available_balance_msat` call.
            let probe = RefundReadinessProbe::query(payment).await;
            match refund_readiness_report_with_probe(store, payment, &probe).await {
                Ok(report) => {
                    let mut money =
                        report.to_money_value(probe.gateway_ok(), probe.federation_ok());
                    // Fold in the operator-sweep view (gate1-operator-sweep, urw.3): the surplus
                    // breakdown + the last sweep — all pure LOCAL ledger reads, no network.
                    match crate::sweep::money_sweep_view(store).await {
                        Ok(sweep_view) => {
                            if let Some(obj) = money.as_object_mut() {
                                if let Some(extra) = sweep_view.as_object() {
                                    for (k, v) in extra {
                                        obj.insert(k.clone(), v.clone());
                                    }
                                }
                                // Surface the degraded/read-only latch (lnrent-y4m.3): a status poll
                                // must reveal that money writes are being refused after a fatal DB
                                // error — otherwise the report looks healthy while every write fails.
                                obj.insert(
                                    "degraded_read_only".to_string(),
                                    serde_json::json!(store.is_degraded()),
                                );
                            }
                            Reply::ok(money)
                        }
                        Err(e) => Reply::err("internal", e.to_string()),
                    }
                }
                Err(e) => Reply::err("internal", e.to_string()),
            }
        }

        Request::Reconcile => {
            // §F: the SINGLE sanctioned `available_balance_msat` call site. Report-only — read the
            // live wallet ONCE, compute the ledger books (`expected_msat`), and report drift for a
            // HUMAN. It MUST NOT mutate state, gate a payment, or auto-refuse anything.
            match crate::ledger::expected_msat(store, payment).await {
                Err(e) => Reply::err("internal", e.to_string()),
                Ok(expected_msat) => match payment.available_balance_msat().await {
                    // wallet >= books ⇒ OK (fee savings run the wallet above the lower bound); below
                    // ⇒ DRIFT (a fedimint-level loss, a missed sweep/refund accounting, or a ledger
                    // bug — for a human to investigate).
                    Ok(Some(wallet_msat)) => {
                        let verdict = if u128::from(wallet_msat) >= expected_msat {
                            "OK"
                        } else {
                            "DRIFT"
                        };
                        Reply::ok(json!({
                            "wallet_msat": wallet_msat,
                            "expected_msat": expected_msat,
                            "verdict": verdict,
                        }))
                    }
                    // A backend with no observable balance (e.g. MockPayment): report it, never panic.
                    Ok(None) => Reply::ok(json!({
                        "wallet_msat": Value::Null,
                        "expected_msat": expected_msat,
                        "verdict": "UNKNOWN",
                    })),
                    // A failed wallet query here just errors the command — the operator retries; there
                    // is no alert class and no daemon state to change.
                    Err(e) => Reply::err("internal", format!("wallet balance query failed: {e:#}")),
                },
            }
        }

        Request::Preflight => {
            // The provider token resolves from the daemon env exactly as `runner::hook_env`
            // forwards it to a declaring recipe's hooks; the probe is the real DigitalOcean
            // client. Tests inject stubs at the `preflight` module seam instead — no test hits
            // the real API.
            let token = crate::preflight::read_token_env();
            let probe = crate::preflight::DoTokenProbe::new();
            Reply::ok(crate::preflight::preflight_report(payment, recipes, token, &probe).await)
        }

        Request::Subs => match store.read(query_subs).await {
            Ok(list) => Reply::ok(json!(list)),
            Err(e) => Reply::err("internal", e.to_string()),
        },

        Request::Sub { id } => {
            let id2 = id.clone();
            match store.read(move |c| query_sub(c, &id2)).await {
                Ok(Some(v)) => Reply::ok(v),
                Ok(None) => Reply::err("not_found", format!("no subscription `{id}`")),
                Err(e) => Reply::err("internal", e.to_string()),
            }
        }

        Request::Teardowns => {
            let recipe_id = recipes.first().map(|r| r.service.id.clone()).unwrap_or_default();
            let now = clock.now();
            let result = async {
                let failures = teardown::open_rows(store).await?;
                let (cleanups_open, cleanups_oldest) =
                    provision::open_cleanups_summary_for(store, &recipe_id).await?;
                anyhow::Ok((failures, cleanups_open, cleanups_oldest))
            }
            .await;
            match result {
                Ok((failures, cleanups_open, cleanups_oldest)) => {
                    let rows: Vec<Value> = failures.iter().map(|r| r.to_value(now)).collect();
                    Reply::ok(json!({
                        "teardown_failures": rows,
                        "provision_cleanups_open": cleanups_open,
                        "provision_cleanups_oldest_at": cleanups_oldest,
                        "open_total": rows.len() as i64 + cleanups_open,
                    }))
                }
                Err(e) => Reply::err("internal", e.to_string()),
            }
        }

        Request::Refunds => {
            let now = clock.now();
            match store.read(move |c| query_refunds(c, now)).await {
                Ok(list) => Reply::ok(json!(list)),
                Err(e) => Reply::err("internal", e.to_string()),
            }
        }

        Request::RefundRetry { id } => refund_retry(store, &id, clock.now()).await,

        Request::SweepQuote { bolt11 } => {
            // §surplus: a DRY-RUN over the ledger — price the outlay + read surplus, no writes and no
            // balance read. Verdict ALLOW iff surplus covers the outlay.
            let sweeper =
                crate::sweep::Sweeper::new(store.clone(), payment.clone(), clock.clone());
            match sweeper.quote(&bolt11).await {
                Ok(q) => Reply::ok(json!({
                    "amount_sat": q.amount_sat,
                    "outlay_msat": q.outlay_msat,
                    "earned_msat": q.earned_msat,
                    "reserved_msat": q.reserved_msat,
                    "paid_out_msat": q.paid_out_msat,
                    "surplus_msat": q.surplus_msat,
                    "verdict": if q.allow { "ALLOW" } else { "REFUSE" },
                })),
                Err(e) => Reply::err(e.code(), e.message()),
            }
        }

        Request::Sweep { bolt11 } => {
            // Execute: gate + durable PENDING intent + capped pay. Ledger-only authorization; the
            // structured refusals (`sweep_invalid`/`sweep_unpriceable`/`sweep_busy`/
            // `sweep_insufficient`/`sweep_fee_rose`) never move money. No alert sink is wired here
            // (the operator gets this structured reply live); the supervisor's drive carries alerts.
            let sweeper =
                crate::sweep::Sweeper::new(store.clone(), payment.clone(), clock.clone());
            match sweeper.execute(&bolt11).await {
                Ok(o) => Reply::ok(json!({
                    "id": o.id,
                    "amount_sat": o.amount_sat,
                    "max_outlay_msat": o.max_outlay_msat,
                    "status": o.status,
                    "backend_payment_id": o.backend_payment_id,
                    "swept": true,
                    "cached": o.cached,
                })),
                Err(e) => Reply::err(e.code(), e.message()),
            }
        }

        Request::AdminSuspend { id } => {
            admin_transition(
                store,
                &id,
                &["ACTIVE"],
                "SUSPENDED",
                "admin_suspend",
                clock.now(),
            )
            .await
        }
        Request::AdminResume { id } => {
            admin_transition(
                store,
                &id,
                &["SUSPENDED"],
                "ACTIVE",
                "admin_resume",
                clock.now(),
            )
            .await
        }
        Request::DevSettle { subscription_id } => {
            dev_settle_subscription(store, payment, clock, subscription_id).await
        }
    }
}

async fn dev_settle_subscription(
    store: &Store,
    payment: &Arc<dyn PaymentBackend>,
    clock: &Arc<dyn Clock>,
    subscription_id: String,
) -> Reply {
    if std::env::var("LNRENT_DEV").ok().as_deref() != Some("1") {
        return Reply::err(
            "dev_disabled",
            "dev settle requires LNRENT_DEV=1 and is only for the mock payment backend",
        );
    }

    let id = subscription_id.clone();
    let external_id = match store
        .read(move |c| {
            match c.query_row(
                "SELECT external_id
                   FROM invoice
                  WHERE subscription_id=?1 AND status='OPEN'
                  ORDER BY issued_at DESC, id DESC
                  LIMIT 1",
                rusqlite::params![id],
                |r| r.get::<_, String>(0),
            ) {
                Ok(external_id) => Ok(Some(external_id)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
    {
        Ok(Some(external_id)) => external_id,
        Ok(None) => {
            return Reply::err(
                "not_found",
                format!("no OPEN invoice for subscription `{subscription_id}`"),
            )
        }
        Err(e) => return Reply::err("internal", e.to_string()),
    };

    let settled_at = clock.now();
    match payment.dev_settle(&external_id, settled_at).await {
        Ok(()) => Reply::ok(json!({
            "subscription_id": subscription_id,
            "external_id": external_id,
            "settled_at": settled_at,
        })),
        Err(e) => {
            let message = e.to_string();
            if message == DEV_SETTLE_UNSUPPORTED {
                Reply::err("unsupported", message)
            } else if message.contains("no OPEN invoice") {
                Reply::err("not_found", message)
            } else {
                Reply::err("internal", message)
            }
        }
    }
}

/// An admin force-transition: CAS the subscription state from one of `from` to `to`, journaled
/// to `event_log`, all in one store transaction (sole writer, ADR-0001). The reconcile/provision
/// integration runs the actual lifecycle hooks; this is the operator override of the state.
async fn admin_transition(
    store: &Store,
    id: &str,
    from: &[&str],
    to: &str,
    kind: &str,
    now: i64,
) -> Reply {
    let id = id.to_string();
    let to = to.to_string();
    let from: Vec<String> = from.iter().map(|s| s.to_string()).collect();
    let res: Result<bool> = store
        .transaction({
            let (id, to, kind, from) = (id.clone(), to.clone(), kind.to_string(), from.clone());
            move |tx| {
                let placeholders = from.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let sql = format!(
                    "UPDATE subscription SET state=?, updated_at=? WHERE id=? AND state IN ({placeholders})"
                );
                let mut params: Vec<&dyn rusqlite::ToSql> = vec![&to, &now, &id];
                for f in &from {
                    params.push(f);
                }
                let n = tx.execute(&sql, params.as_slice())?;
                if n == 0 {
                    return Ok(false);
                }
                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?, ?, ?, ?)",
                    rusqlite::params![id, kind, json!({"to": to}).to_string(), now],
                )?;
                Ok(true)
            }
        })
        .await;
    match res {
        Ok(true) => Reply::ok(json!({"id": id, "state": to})),
        Ok(false) => Reply::err(
            "invalid_state",
            format!("subscription `{id}` not in {from:?}"),
        ),
        Err(e) => Reply::err("internal", e.to_string()),
    }
}

/// The short `dest_form` label for the `lnrent refunds` view: how the buyer's refund destination
/// resolves (lnrent-urw.5). `none` = no dest recorded (a legacy/manual liability); `unknown` = a
/// dest that no longer parses (surfaced, not hidden).
fn dest_form_label(dest: Option<&str>) -> &'static str {
    match dest.map(str::trim).filter(|d| !d.is_empty()) {
        None => "none",
        Some(d) => match detect_form(d) {
            Ok(DestForm::LnAddress { .. }) => "ln_address",
            Ok(DestForm::Lnurl(_)) => "lnurl",
            Ok(DestForm::Bolt11) => "bolt11",
            Err(_) => "unknown",
        },
    }
}

/// Non-terminal (`PENDING`) + parked (`FAILED`) refund rows — the per-item view behind
/// `lnrent money`'s `parked_count` (lnrent-urw.5). Projects only PERSISTED fields (no fabricated
/// error class) plus the derived `dest_form` and ages.
fn query_refunds(c: &rusqlite::Connection, now: i64) -> Result<Vec<Value>> {
    let mut stmt = c.prepare(
        "SELECT id, subscription_id, dest, amount_sat, status, COALESCE(attempts, 0),
                created_at, updated_at
           FROM refund_attempt
          WHERE status IN ('PENDING','FAILED')
          ORDER BY updated_at DESC, id",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let dest: Option<String> = r.get(2)?;
            let created_at: Option<i64> = r.get(6)?;
            Ok(json!({
                "id": r.get::<_, String>(0)?,
                "subscription_id": r.get::<_, Option<String>>(1)?,
                "dest_form": dest_form_label(dest.as_deref()),
                "amount_sat": r.get::<_, Option<i64>>(3)?,
                "status": r.get::<_, String>(4)?,
                "attempts": r.get::<_, i64>(5)?,
                "created_at": created_at,
                "updated_at": r.get::<_, Option<i64>>(7)?,
                "age_s": created_at.map(|c| now - c),
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Re-drive one parked-FAILED refund (lnrent-urw.5): CAS it back to `PENDING` with `attempts=0` so
/// the refunder re-runs the REAL resolver + INV-1-capped pay path on its next drive. Guarded on
/// `status='FAILED'` — a retry of a non-parked id mutates nothing and returns a structured error.
/// Journaled. This is the ONLY refund actuator; there is deliberately no cancel/abandon.
async fn refund_retry(store: &Store, id: &str, now: i64) -> Reply {
    let id = id.to_string();
    let res: Result<bool> = store
        .transaction({
            let id = id.clone();
            move |tx| {
                // Read the row's identity BEFORE mutating, to (a) gate on FAILED and (b) compute the
                // stable billing.refund outbox id for the supersede below.
                let row: Option<(String, Option<String>, String)> =
                    rusqlite::OptionalExtension::optional(tx.query_row(
                        "SELECT status, subscription_id, idempotency_key FROM refund_attempt WHERE id=?1",
                        rusqlite::params![id],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    ))?;
                let Some((status, sub, idempotency_key)) = row else {
                    return Ok(false);
                };
                if status != "FAILED" {
                    return Ok(false);
                }

                tx.execute(
                    "UPDATE refund_attempt SET status='PENDING', attempts=0, updated_at=?2
                     WHERE id=?1 AND status='FAILED'",
                    rusqlite::params![id, now],
                )?;
                // SUPERSEDE the stale parked-FAILED billing.refund DM (codex): its stable id would
                // otherwise ON CONFLICT-block the success DM commit_sent enqueues when the retry pays,
                // so the buyer could be told "failed" for a refund that actually settled. Deleting it
                // lets the next terminalization (SENT or a fresh FAILED) enqueue the CURRENT outcome.
                let outbox_id = crate::refund::refund_outbox_id(&idempotency_key, &id);
                tx.execute("DELETE FROM outbox WHERE id=?1", rusqlite::params![outbox_id])?;

                tx.execute(
                    "INSERT INTO event_log (subscription_id, kind, detail_json, at) VALUES (?, ?, ?, ?)",
                    rusqlite::params![sub, "refund_retry_requested", json!({"refund": id}).to_string(), now],
                )?;
                Ok(true)
            }
        })
        .await;
    match res {
        Ok(true) => Reply::ok(json!({"id": id, "status": "PENDING", "requeued": true})),
        Ok(false) => Reply::err(
            "invalid_state",
            format!("refund `{id}` is not a parked (FAILED) refund — nothing to retry"),
        ),
        Err(e) => Reply::err("internal", e.to_string()),
    }
}

fn query_subs(c: &rusqlite::Connection) -> Result<Vec<Value>> {
    let mut stmt = c.prepare(
        "SELECT id, recipe_id, state, paid_through, soft_date FROM subscription ORDER BY created_at",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(json!({
                "id": r.get::<_, String>(0)?,
                "recipe_id": r.get::<_, Option<String>>(1)?,
                "state": r.get::<_, Option<String>>(2)?,
                "paid_through": r.get::<_, Option<i64>>(3)?,
                "soft_date": r.get::<_, Option<i64>>(4)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn query_sub(c: &rusqlite::Connection, id: &str) -> Result<Option<Value>> {
    let v = c
        .query_row(
            "SELECT id, recipe_id, listing_id, state, paid_through, soft_date FROM subscription WHERE id=?",
            rusqlite::params![id],
            |r| {
                Ok(json!({
                    "id": r.get::<_, String>(0)?,
                    "recipe_id": r.get::<_, Option<String>>(1)?,
                    "listing_id": r.get::<_, Option<String>>(2)?,
                    "state": r.get::<_, Option<String>>(3)?,
                    "paid_through": r.get::<_, Option<i64>>(4)?,
                    "soft_date": r.get::<_, Option<i64>>(5)?,
                }))
            },
        );
    // Only "no such row" is None; a real error (corruption, type mismatch) must propagate,
    // not masquerade as not_found (codex #10).
    match v {
        Ok(row) => Ok(Some(row)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// CLIENT: connect to the daemon socket, send `req`, return its `Reply`.
pub async fn call(path: impl AsRef<Path>, req: Request) -> Result<Reply> {
    let mut stream = UnixStream::connect(path.as_ref())
        .await
        .with_context(|| format!("connecting to lnrentd at {}", path.as_ref().display()))?;
    let mut buf = serde_json::to_vec(&req)?;
    buf.push(b'\n');
    stream.write_all(&buf).await?;
    stream.flush().await?;
    let (rd, _wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut line = String::new();
    rd.read_line(&mut line).await?;
    serde_json::from_str(line.trim()).context("parsing daemon reply")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::{
        Invoice, MockPayment, PayStatus, PaymentBackend, PaymentStatus, Settlement,
    };
    use crate::store::{Store, SCHEMA};
    use async_trait::async_trait;
    use rusqlite::Connection;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    #[derive(Default)]
    struct RecordingPayment {
        balance_msat: StdMutex<Option<u64>>,
        // §E/§F: `available_balance_msat` PANICS unless explicitly armed — so every `money` test
        // proves the plain view makes NO wallet read, and only `reconcile` (which arms it) sees it.
        allow_balance_read: StdMutex<bool>,
        gateway_ok: StdMutex<bool>,
        gateway_sequence: StdMutex<VecDeque<bool>>,
        statuses: StdMutex<HashMap<String, PayStatus>>,
        started: StdMutex<HashSet<String>>,
        calls: StdMutex<Vec<&'static str>>,
    }

    impl RecordingPayment {
        fn new(balance_msat: Option<u64>, gateway_ok: bool) -> Self {
            Self {
                balance_msat: StdMutex::new(balance_msat),
                allow_balance_read: StdMutex::new(false),
                gateway_ok: StdMutex::new(gateway_ok),
                gateway_sequence: StdMutex::new(VecDeque::new()),
                statuses: StdMutex::new(HashMap::new()),
                started: StdMutex::new(HashSet::new()),
                calls: StdMutex::new(Vec::new()),
            }
        }

        fn with_gateway_sequence(balance_msat: Option<u64>, gateway_ok: Vec<bool>) -> Self {
            Self {
                balance_msat: StdMutex::new(balance_msat),
                allow_balance_read: StdMutex::new(false),
                gateway_ok: StdMutex::new(false),
                gateway_sequence: StdMutex::new(VecDeque::from(gateway_ok)),
                statuses: StdMutex::new(HashMap::new()),
                started: StdMutex::new(HashSet::new()),
                calls: StdMutex::new(Vec::new()),
            }
        }

        /// Arm the wallet read for the `reconcile` test — the ONE sanctioned balance call site.
        fn allow_balance_read(&self) {
            *self.allow_balance_read.lock().unwrap() = true;
        }

        fn record(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl PaymentBackend for RecordingPayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            self.record("create_invoice");
            anyhow::bail!("money must not create invoices")
        }

        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            self.record("lookup");
            anyhow::bail!("money must not look up inbound invoices")
        }

        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            self.record("lookup_settlement");
            anyhow::bail!("money must not look up settlements")
        }

        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            self.record("pay");
            anyhow::bail!("money must not pay")
        }

        async fn refund_required_outlay_msat(
            &self,
            gross_sat: u64,
            pay_sat: Option<u64>,
        ) -> Result<u128> {
            self.record("refund_required_outlay_msat");
            Ok(u128::from(pay_sat.unwrap_or(gross_sat)) * 1000)
        }

        async fn pay_refund_capped(&self, _: &str, _: u64, _: u64, _: &str) -> Result<String> {
            self.record("pay_refund_capped");
            anyhow::bail!("money must not pay capped refunds")
        }

        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            self.record("payment_status");
            anyhow::bail!("money must not check by backend payment id")
        }

        async fn payment_status_by_key(&self, key: &str) -> Result<PayStatus> {
            self.record("payment_status_by_key");
            Ok(*self
                .statuses
                .lock()
                .unwrap()
                .get(key)
                .unwrap_or(&PayStatus::Unknown))
        }

        async fn payment_started_by_key(&self, key: &str) -> Result<bool> {
            self.record("payment_started_by_key");
            Ok(self.started.lock().unwrap().contains(key))
        }

        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            assert!(
                *self.allow_balance_read.lock().unwrap(),
                "only reconcile may read the wallet balance (§F); this call site must not"
            );
            self.record("available_balance_msat");
            Ok(*self.balance_msat.lock().unwrap())
        }

        async fn refund_gateway_ready(&self) -> Result<bool> {
            self.record("refund_gateway_ready");
            if let Some(ok) = self.gateway_sequence.lock().unwrap().pop_front() {
                return Ok(ok);
            }
            Ok(*self.gateway_ok.lock().unwrap())
        }

        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            self.record("watch");
            anyhow::bail!("money must not watch settlements")
        }
    }

    /// A test-only backend whose `available_balance_msat` SIGNALS that it was entered and then
    /// BLOCKS for `delay` (lnrent-j3c) — used to hold a read-only `Reconcile` handler in flight
    /// across a shutdown. Every other method bails: the reconcile path over an EMPTY store touches
    /// only the balance read (`ledger::expected_msat` makes no payment call with no refunds).
    struct SlowBalancePayment {
        entered: mpsc::UnboundedSender<()>,
        delay: std::time::Duration,
    }

    #[async_trait]
    impl PaymentBackend for SlowBalancePayment {
        async fn create_invoice(&self, _: u64, _: &str, _: u32, _: &str) -> Result<Invoice> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn lookup(&self, _: &str) -> Result<PaymentStatus> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn lookup_settlement(&self, _: &str) -> Result<(PaymentStatus, Option<i64>)> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn pay(&self, _: &str, _: u64, _: &str) -> Result<String> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn payment_status(&self, _: &str) -> Result<PayStatus> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn payment_status_by_key(&self, _: &str) -> Result<PayStatus> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
        async fn available_balance_msat(&self) -> Result<Option<u64>> {
            // Announce we're now inside the slow read-only call, then block. A cooperative shutdown
            // must DROP this future (via the read-only select!) rather than await the full `delay`.
            let _ = self.entered.send(());
            tokio::time::sleep(self.delay).await;
            Ok(Some(1))
        }
        async fn watch(&self) -> Result<mpsc::Receiver<Settlement>> {
            anyhow::bail!("SlowBalancePayment: only available_balance_msat is exercised")
        }
    }

    fn mem_store() -> Store {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Store::spawn(conn)
    }

    async fn money_data(store: &Store, payment: &Arc<dyn PaymentBackend>) -> Value {
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let reply =
            dispatch(Request::Money, store, &recipes, &clock, payment, &RelayStatusCell::new())
                .await;
        assert!(reply.ok, "money reply should be ok: {:?}", reply.error);
        reply.data.expect("money returns data")
    }

    async fn reconcile_data(store: &Store, payment: &Arc<dyn PaymentBackend>) -> Value {
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let reply = dispatch(
            Request::Reconcile,
            store,
            &recipes,
            &clock,
            payment,
            &RelayStatusCell::new(),
        )
        .await;
        assert!(reply.ok, "reconcile reply should be ok: {:?}", reply.error);
        reply.data.expect("reconcile returns data")
    }

    async fn seed_pending_refund_liability(store: &Store) {
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, buyer_pubkey, created_at, updated_at)
                     VALUES ('sub-1', 'REFUND_DUE', 'buyer', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice
                        (id, subscription_id, external_id, kind, amount_sat, status,
                         settled_at, applied_at, issued_at)
                     VALUES ('inv-1', 'sub-1', 'order:sub-1', 'order', 2, 'PAID', 10, 10, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO refund_attempt
                        (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts,
                         resolved_bolt11, resolved_expiry, resolution_gen, created_at, updated_at)
                     VALUES
                        ('ref-order:sub-1', 'sub-1', 'lnaddr@buyer', 2, 'refund:order:sub-1',
                         'PENDING', 0, 'persisted-bolt11', 100, 1, 0, 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn store_money_snapshot(store: &Store) -> Value {
        store
            .read(|c| {
                let subscriptions = c
                    .prepare("SELECT id, state, updated_at FROM subscription ORDER BY id")?
                    .query_map([], |r| {
                        Ok(json!({
                            "id": r.get::<_, String>(0)?,
                            "state": r.get::<_, Option<String>>(1)?,
                            "updated_at": r.get::<_, Option<i64>>(2)?,
                        }))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let invoices = c
                    .prepare(
                        "SELECT external_id, status, applied_at, settled_at FROM invoice ORDER BY id",
                    )?
                    .query_map([], |r| {
                        Ok(json!({
                            "external_id": r.get::<_, String>(0)?,
                            "status": r.get::<_, Option<String>>(1)?,
                            "applied_at": r.get::<_, Option<i64>>(2)?,
                            "settled_at": r.get::<_, Option<i64>>(3)?,
                        }))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let refunds = c
                    .prepare(
                        "SELECT id, status, attempts, backend_payment_id, resolved_bolt11,
                                resolution_gen
                           FROM refund_attempt
                          ORDER BY id",
                    )?
                    .query_map([], |r| {
                        Ok(json!({
                            "id": r.get::<_, String>(0)?,
                            "status": r.get::<_, String>(1)?,
                            "attempts": r.get::<_, Option<i64>>(2)?,
                            "backend_payment_id": r.get::<_, Option<String>>(3)?,
                            "resolved_bolt11": r.get::<_, Option<String>>(4)?,
                            "resolution_gen": r.get::<_, i64>(5)?,
                        }))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let event_count: i64 =
                    c.query_row("SELECT count(*) FROM event_log", [], |r| r.get(0))?;
                let outbox_count: i64 =
                    c.query_row("SELECT count(*) FROM outbox", [], |r| r.get(0))?;
                Ok(json!({
                    "subscriptions": subscriptions,
                    "invoices": invoices,
                    "refunds": refunds,
                    "event_count": event_count,
                    "outbox_count": outbox_count,
                }))
            })
            .await
            .unwrap()
    }

    async fn serve_temp() -> (Store, std::path::PathBuf) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        // seed one subscription
        conn.execute(
            "INSERT INTO subscription (id, recipe_id, state, created_at) VALUES ('s1','dummy','ACTIVE',1)",
            [],
        )
        .unwrap();
        let store = Store::spawn(conn);
        let dir = format!("{}/../recipes", env!("CARGO_MANIFEST_DIR"));
        let recipes = Arc::new(Recipe::load_all(&dir).unwrap());
        let sock = temp_sock("serve");
        let (s2, sock2) = (store.clone(), sock.clone());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        tokio::spawn(async move {
            let _ = serve(s2, recipes, clock, payment, RelayStatusCell::new(), &sock2).await;
        });
        // wait for bind
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        (store, sock)
    }

    /// A unique temp socket path (all tests share one PID, so add a per-binary sequence).
    fn temp_sock(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("lnrent-ipc-{tag}-{}-{n}.sock", std::process::id()))
    }

    // y4m.10: the pure peer-cred decision — the daemon's own uid and root are the ONLY
    // authorized peers. A disallowed-uid CONNECTION isn't constructible without root, so the
    // decision is tested pure and wired at the accept site.
    #[test]
    fn peer_allowed_only_daemon_uid_and_root() {
        assert!(peer_allowed(1000, 1000), "same uid commands its own daemon");
        assert!(peer_allowed(0, 1000), "root is always allowed");
        assert!(!peer_allowed(1001, 1000), "foreign uid refused");
        assert!(
            !peer_allowed(1000, 0),
            "non-root peer refused by a root daemon"
        );
        assert!(peer_allowed(0, 0));
    }

    // y4m.10 review: staging must not make a valid final `lnrent.sock` path fail merely because
    // the detour is longer. Linux accepts 107 pathname bytes in sockaddr_un (108 including NUL).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn staged_bind_preserves_the_final_socket_path_limit() {
        use std::os::unix::ffi::OsStrExt;
        use std::sync::atomic::{AtomicU64, Ordering};

        const MAX_PATH_BYTES: usize = 107;
        const SOCKET_NAME: &str = "lnrent.sock";
        static SEQ: AtomicU64 = AtomicU64::new(0);

        let base = std::env::temp_dir();
        let parent_len = MAX_PATH_BYTES - 1 - SOCKET_NAME.len();
        let prefix = format!(
            "lnrent-ipc-path-limit-{}-{}-",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        );
        let component_len = parent_len - base.as_os_str().as_bytes().len() - 1;
        assert!(
            prefix.len() <= component_len,
            "temporary directory path is too long"
        );
        let parent = base.join(format!(
            "{prefix}{}",
            "x".repeat(component_len - prefix.len())
        ));
        std::fs::create_dir(&parent).unwrap();
        let sock = parent.join(SOCKET_NAME);
        assert_eq!(sock.as_os_str().as_bytes().len(), MAX_PATH_BYTES);

        let (listener, _) = bind_owner_only(&sock).unwrap();
        drop(listener);
        std::fs::remove_file(&sock).unwrap();
        std::fs::remove_dir(&parent).unwrap();
    }

    // codex PR-34: a final path ONE byte over the sun_path limit must FAIL STARTUP with a clear
    // diagnostic — the staged path is one byte shorter, so without the up-front guard the bind
    // would succeed and the rename would publish a live socket at a pathname clients cannot even
    // encode (the daemon runs, every operator IPC call fails).
    #[tokio::test]
    async fn over_limit_final_path_fails_loudly_instead_of_publishing_dead() {
        use std::os::unix::ffi::OsStrExt;

        const SOCKET_NAME: &str = "lnrent.sock";
        let base = std::env::temp_dir();
        // Build a parent dir so <parent>/lnrent.sock is EXACTLY one byte over the limit.
        let target_parent_len = MAX_SUN_PATH_BYTES + 1 - 1 - SOCKET_NAME.len();
        let seed = format!(
            "{}/lnrent-ipc-overlimit-{}-",
            base.as_os_str().to_str().unwrap(),
            std::process::id()
        );
        let pad = target_parent_len
            .checked_sub(seed.len())
            .expect("temp dir short enough for this test");
        let parent = std::path::PathBuf::from(format!("{seed}{}", "x".repeat(pad)));
        std::fs::create_dir_all(&parent).unwrap();
        let sock = parent.join(SOCKET_NAME);
        assert_eq!(sock.as_os_str().as_bytes().len(), MAX_SUN_PATH_BYTES + 1);

        let err = bind_owner_only(&sock).expect_err("must refuse an over-limit final path");
        assert!(
            err.to_string().contains("unix socket limit"),
            "clear diagnostic: {err:#}"
        );
        assert!(
            std::fs::symlink_metadata(&sock).is_err(),
            "nothing was published at the dead path"
        );
        let _ = std::fs::remove_dir_all(&parent);
    }

    const LOOSE_UMASK_HELPER_ENV: &str = "LNRENT_TEST_LOOSE_UMASK_BIND";

    // y4m.10: the socket is NEVER observable looser than 0600 even under a deliberately loose
    // umask — the inode is chmod'd inside the 0700 staging dir BEFORE the atomic publish. Run the
    // mutation in a subprocess because umask(2) is process-global and Rust tests run in parallel.
    #[test]
    fn bind_is_owner_only_even_under_loose_umask() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ipc::tests::loose_umask_bind_helper",
                "--nocapture",
            ])
            .env(LOOSE_UMASK_HELPER_ENV, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("running 1 test"),
            "loose-umask helper failed or did not run\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn loose_umask_bind_helper() {
        if std::env::var_os(LOOSE_UMASK_HELPER_ENV).is_none() {
            return;
        }
        use std::os::unix::fs::MetadataExt;
        let sock = temp_sock("umask");
        let old = unsafe { libc::umask(0) };
        let bound = bind_owner_only(&sock);
        unsafe { libc::umask(old) };
        let (_listener, daemon_uid) = bound.unwrap();
        let meta = std::fs::metadata(&sock).unwrap();
        assert_eq!(
            meta.permissions().mode() & 0o7777,
            0o600,
            "no umask leak onto the published socket"
        );
        assert_eq!(
            daemon_uid,
            meta.uid(),
            "captured uid is the socket owner (daemon euid)"
        );
        let _ = std::fs::remove_file(&sock);
    }

    // y4m.10: binding over a PRE-EXISTING stale socket is an ATOMIC replace. A poller hammers
    // the path across the whole swap window: the path must NEVER be missing (the old
    // remove-then-bind gap), any NEW inode must already be 0600, and a connect must never see
    // NotFound or PermissionDenied — only the stale socket's refusal or a live accept. The
    // round trip at the end proves the listener serves the RENAMED inode.
    #[tokio::test]
    async fn rebind_over_stale_socket_is_atomic_and_owner_only() {
        use std::os::unix::fs::MetadataExt;

        let sock = temp_sock("swap");
        // What a crashed daemon leaves behind: a bound-then-dropped (dead) socket file, at the
        // 0600 owner-only mode every real bind publishes — the one stale shape
        // `evict_unsafe_final_path` deliberately KEEPS so the replace stays atomic (an unsafe
        // stale is evicted instead; see evicts_unsafe_preexisting_entries_before_publish).
        drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
        std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
        let stale_ino = std::fs::metadata(&sock).unwrap().ino();

        let poller = {
            let sock = sock.clone();
            std::thread::spawn(move || {
                let (mut missing, mut loose, mut denied) = (0u32, 0u32, 0u32);
                // Bounded (~10s) so a broken bind fails the test instead of hanging it.
                for _ in 0..20_000 {
                    match std::fs::metadata(&sock) {
                        Err(_) => missing += 1,
                        Ok(m) if m.ino() != stale_ino => {
                            if m.permissions().mode() & 0o7777 != 0o600 {
                                loose += 1;
                            }
                        }
                        Ok(_) => {} // still the stale inode
                    }
                    match std::os::unix::net::UnixStream::connect(&sock) {
                        Ok(_) => return (missing, loose, denied, true),
                        Err(e) => match e.kind() {
                            std::io::ErrorKind::NotFound => missing += 1,
                            std::io::ErrorKind::PermissionDenied => denied += 1,
                            _ => {} // the stale socket: connection refused
                        },
                    }
                    std::thread::sleep(std::time::Duration::from_micros(500));
                }
                (missing, loose, denied, false)
            })
        };

        // Start the daemon side while the poller is watching the path.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let store = Store::spawn(conn);
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let sock2 = sock.clone();
        tokio::spawn(async move {
            let _ = serve(
                store,
                recipes,
                clock,
                payment,
                RelayStatusCell::new(),
                &sock2,
            )
            .await;
        });

        let (missing, loose, denied, connected) =
            tokio::task::spawn_blocking(move || poller.join().unwrap())
                .await
                .unwrap();
        assert!(connected, "a client eventually reaches the NEW socket");
        assert_eq!(missing, 0, "the path is NEVER missing across the swap");
        assert_eq!(
            loose, 0,
            "the new socket is 0600 from its first observable instant"
        );
        assert_eq!(denied, 0, "no filesystem-permissions error around the swap");

        // The listener serves the inode bound at the staging path, now at the final path.
        let st = call(&sock, Request::Status).await.unwrap();
        assert!(st.ok, "round trip over the renamed inode: {:?}", st.error);
        let _ = std::fs::remove_file(&sock);
    }

    // y4m.10 (adversarial review): an UNSAFE pre-existing entry at the final path — a
    // loose-perms socket, a plain file, or a symlink — must be EVICTED before publish rather
    // than keep serving startup-racing clients until the rename lands. Only the owner-only 0600
    // socket a real previous run leaves behind is kept (the atomic-replace case above). The
    // symlink is judged as itself (never followed): its TARGET must survive.
    #[tokio::test]
    async fn evicts_unsafe_preexisting_entries_before_publish() {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        // A loose-perms stale socket (what a pre-y4m.10 daemon could leave under a loose umask).
        let sock = temp_sock("evict-loose");
        drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
        std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o666)).unwrap();
        let (_l, uid) = bind_owner_only(&sock).unwrap();
        let meta = std::fs::metadata(&sock).unwrap();
        assert_eq!(meta.permissions().mode() & 0o7777, 0o600);
        assert_eq!(meta.uid(), uid);
        let _ = std::fs::remove_file(&sock);

        // A plain file squatting on the path.
        let sock = temp_sock("evict-file");
        std::fs::write(&sock, b"impostor").unwrap();
        let (_l, _) = bind_owner_only(&sock).unwrap();
        let meta = std::fs::metadata(&sock).unwrap();
        assert!(
            meta.file_type().is_socket(),
            "the squatting file is replaced by the real socket"
        );
        let _ = std::fs::remove_file(&sock);

        // A symlink at the path: removed AS a symlink; its target is untouched.
        let sock = temp_sock("evict-link");
        let target = temp_sock("evict-link-target");
        std::fs::write(&target, b"decoy target").unwrap();
        std::os::unix::fs::symlink(&target, &sock).unwrap();
        let (_l, _) = bind_owner_only(&sock).unwrap();
        let meta = std::fs::symlink_metadata(&sock).unwrap();
        assert!(
            meta.file_type().is_socket() && !meta.file_type().is_symlink(),
            "the symlink itself is evicted and replaced by the real socket"
        );
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"decoy target",
            "the symlink TARGET is never followed or touched"
        );
        let _ = std::fs::remove_file(&sock);
        let _ = std::fs::remove_file(&target);
    }

    // y4m.13: an idle client (connect, send nothing) is cut at the read deadline with the
    // structured `bad_request` reply — its handler self-completes instead of pinning forever.
    #[tokio::test]
    async fn idle_client_is_timed_out_with_a_bad_request_reply() {
        let (_store, sock) = serve_temp().await;
        let connected_at = std::time::Instant::now();
        let stream = UnixStream::connect(&sock).await.unwrap();
        // Keep the write half ALIVE (named binding): dropping it would half-close and hand the
        // server an EOF — the unterminated-frame path, not the timeout under test.
        let (rd, _wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut line = String::new();
        tokio::time::timeout(REQUEST_READ_DEADLINE * 4, rd.read_line(&mut line))
            .await
            .expect("the timed-out reply arrives ~at the deadline, the handler never hangs")
            .unwrap();
        let elapsed = connected_at.elapsed();
        let reply: Reply = serde_json::from_str(line.trim()).unwrap();
        assert!(!reply.ok);
        let err = reply.error.unwrap();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("timed out"), "message: {}", err.message);
        assert!(
            elapsed < REQUEST_READ_DEADLINE * 3,
            "cut at ~the deadline, not some later fallback: {elapsed:?}"
        );
    }

    // y4m.13: a slow-but-legitimate client that sends its line just UNDER the deadline still
    // gets a normal reply — the deadline only cuts peers that never complete a frame.
    #[tokio::test]
    async fn slow_client_under_the_deadline_gets_a_normal_reply() {
        let (_store, sock) = serve_temp().await;
        let mut stream = UnixStream::connect(&sock).await.unwrap();
        // A QUARTER of the deadline (not half — review P3): the server's clock starts at accept,
        // so under a parallel suite half the deadline left too little slack against scheduling
        // jitter and the test could flake.
        tokio::time::sleep(REQUEST_READ_DEADLINE / 4).await;
        let mut buf = serde_json::to_vec(&Request::Status).unwrap();
        buf.push(b'\n');
        stream.write_all(&buf).await.unwrap();
        stream.flush().await.unwrap();
        let (rd, _wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        let mut line = String::new();
        rd.read_line(&mut line).await.unwrap();
        let reply: Reply = serde_json::from_str(line.trim()).unwrap();
        assert!(reply.ok, "slow-but-legit request succeeds: {:?}", reply.error);
        assert_eq!(reply.data.unwrap()["subscriptions"], json!(1));
    }

    // y4m.13 — THE bead scenario: at shutdown `serve_with_shutdown` AWAITS in-flight handlers.
    // Before the read deadline, one idle client pinned that drain until the supervisor's abort
    // fallback killed the whole task set — able to kill a concurrent handler between its txn
    // commit and its reply (a committed-but-unacknowledged admin action). Now the idle handler
    // self-completes at the deadline, the drain returns promptly (never reaching an abort), and
    // a well-behaved connection accepted BEFORE shutdown still commits its admin txn AND gets
    // its reply.
    #[tokio::test]
    async fn shutdown_drain_is_not_wedged_by_an_idle_client() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO subscription (id, recipe_id, state, created_at) VALUES ('s1','dummy','ACTIVE',1)",
            [],
        )
        .unwrap();
        let store = Store::spawn(conn);
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());
        let sock = temp_sock("drain");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_with_shutdown(
            store.clone(),
            recipes,
            clock,
            payment,
            RelayStatusCell::new(),
            sock.clone(),
            shutdown_rx,
        ));
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // The idle client: accepted, then never sends a byte (write half kept alive).
        let idle = UnixStream::connect(&sock).await.unwrap();
        let (idle_rd, _idle_wr) = idle.into_split();
        // The well-behaved client: its connection also arrives before shutdown.
        let mut legit = UnixStream::connect(&sock).await.unwrap();
        // A probe round-trip AFTER both connects: the accept loop dequeues in order, so its
        // reply proves both handlers above are already spawned (in flight at shutdown time),
        // not parked in the listen backlog where dropping the listener would just kill them.
        let probe = call(&sock, Request::Status).await.unwrap();
        assert!(probe.ok, "probe: {:?}", probe.error);

        shutdown_tx.send(true).unwrap();
        let shutdown_at = std::time::Instant::now();

        // The in-flight well-behaved handler still commits its admin txn and replies.
        let mut buf = serde_json::to_vec(&Request::AdminSuspend { id: "s1".into() }).unwrap();
        buf.push(b'\n');
        legit.write_all(&buf).await.unwrap();
        legit.flush().await.unwrap();
        let (legit_rd, _legit_wr) = legit.into_split();
        let mut legit_rd = BufReader::new(legit_rd);
        let mut line = String::new();
        legit_rd.read_line(&mut line).await.unwrap();
        let reply: Reply = serde_json::from_str(line.trim()).unwrap();
        assert!(
            reply.ok,
            "the in-flight admin action gets its reply through the drain: {:?}",
            reply.error
        );
        assert_eq!(reply.data.unwrap()["state"], "SUSPENDED");

        // The idle client (still reading) gets the structured timed-out reply.
        let mut idle_rd = BufReader::new(idle_rd);
        let mut idle_line = String::new();
        tokio::time::timeout(REQUEST_READ_DEADLINE * 4, idle_rd.read_line(&mut idle_line))
            .await
            .expect("the idle handler self-completes at the deadline")
            .unwrap();
        let idle_reply: Reply = serde_json::from_str(idle_line.trim()).unwrap();
        assert_eq!(idle_reply.error.unwrap().code, "bad_request");

        // THE assertion: the graceful drain completes promptly — bounded by the read deadline,
        // never hanging toward an abort fallback that could kill committed-but-unreplied work.
        let drained = tokio::time::timeout(REQUEST_READ_DEADLINE * 4, server)
            .await
            .expect("graceful drain must not hang on the idle handler")
            .unwrap();
        assert!(drained.is_ok(), "serve_with_shutdown returns Ok: {drained:?}");
        assert!(
            shutdown_at.elapsed() < REQUEST_READ_DEADLINE * 3,
            "drain bounded by the read deadline, not an abort window: {:?}",
            shutdown_at.elapsed()
        );

        // Committed AND acknowledged: the admin txn survived the shutdown.
        let state: String = store
            .read(|c| {
                Ok(c.query_row("SELECT state FROM subscription WHERE id='s1'", [], |r| {
                    r.get(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(state, "SUSPENDED");
    }

    // lnrent-j3c: `is_mutating` is the compile-time-forced (exhaustive-match, no `_`) classifier the
    // shutdown drain branches on. This pins the RUNTIME truth table: MUTATING ⇒ drain awaits it;
    // read-only ⇒ drain-exempt (cancellable). A new variant makes `is_mutating` fail to compile, and
    // if it is misclassified this test is where the wrong bucket shows up.
    #[test]
    fn is_mutating_classifies_every_request_variant() {
        // MUTATING: writes a durable txn and/or gates a payment → the drain MUST let it finish.
        assert!(Request::RefundRetry { id: "x".into() }.is_mutating());
        assert!(Request::Sweep { bolt11: "x".into() }.is_mutating());
        assert!(Request::AdminSuspend { id: "x".into() }.is_mutating());
        assert!(Request::AdminResume { id: "x".into() }.is_mutating());
        assert!(Request::DevSettle {
            subscription_id: "x".into()
        }
        .is_mutating());
        // READ-ONLY: commits nothing → drain-exempt (cancelled promptly at shutdown). Includes the
        // slow `Preflight` and the network-touching `Reconcile`/`Money`, plus the `SweepQuote`
        // dry-run.
        assert!(!Request::Status.is_mutating());
        assert!(!Request::Recipes.is_mutating());
        assert!(!Request::Money.is_mutating());
        assert!(!Request::Reconcile.is_mutating());
        assert!(!Request::Preflight.is_mutating());
        assert!(!Request::Subs.is_mutating());
        assert!(!Request::Sub { id: "x".into() }.is_mutating());
        assert!(!Request::Teardowns.is_mutating());
        assert!(!Request::Relays.is_mutating());
        assert!(!Request::Refunds.is_mutating());
        assert!(!Request::SweepQuote { bolt11: "x".into() }.is_mutating());
    }

    // lnrent-j3c — THE bead scenario: a SLOW read-only op in flight must NOT pin the graceful drain.
    // A `Reconcile` handler is parked inside a ~10s balance read; when shutdown flips, the read-only
    // exemption cancels it, so `serve_with_shutdown` returns WELL under that 10s sleep (proving the
    // handler was cancelled, not awaited) and the client sees the structured `shutting_down` reply.
    // Before the fix the drain would await the full sleep and force the supervisor's overrun-abort —
    // the committed-but-unreplied kill window y4m.13 closed for idle peers, reopened by slow dispatch.
    #[tokio::test]
    async fn slow_read_only_op_does_not_pin_the_shutdown_drain() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let store = Store::spawn(conn);
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let payment: Arc<dyn PaymentBackend> = Arc::new(SlowBalancePayment {
            entered: entered_tx,
            delay: std::time::Duration::from_secs(10),
        });
        let sock = temp_sock("slow-readonly");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_with_shutdown(
            store,
            recipes,
            clock,
            payment,
            RelayStatusCell::new(),
            sock.clone(),
            shutdown_rx,
        ));
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // A read-only Reconcile that will block ~10s inside `available_balance_msat`.
        let mut client = UnixStream::connect(&sock).await.unwrap();
        let mut buf = serde_json::to_vec(&Request::Reconcile).unwrap();
        buf.push(b'\n');
        client.write_all(&buf).await.unwrap();
        client.flush().await.unwrap();

        // Confirm the handler is genuinely IN FLIGHT in the slow read-only call before shutting down
        // (not still parked in the read or the listen backlog).
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("the reconcile handler reaches the slow balance read")
            .expect("entered signal");

        // Flip shutdown and time the drain: it must cancel the read-only op, NOT await the 10s sleep.
        let shutdown_at = std::time::Instant::now();
        shutdown_tx.send(true).unwrap();

        let drained = tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .expect("graceful drain must not await the 10s read-only sleep")
            .unwrap();
        assert!(
            drained.is_ok(),
            "serve_with_shutdown returns Ok: {drained:?}"
        );
        assert!(
            shutdown_at.elapsed() < std::time::Duration::from_secs(2),
            "drain cancelled the read-only op instead of awaiting its 10s sleep: {:?}",
            shutdown_at.elapsed()
        );

        // The client gets the structured, retryable `shutting_down` reply: this is a transient
        // restart race, not a permanent request failure.
        let (rd, _wr) = client.into_split();
        let mut rd = BufReader::new(rd);
        let mut line = String::new();
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), rd.read_line(&mut line))
            .await
            .expect("a prompt reply, never the 10s sleep")
            .unwrap();
        assert!(n > 0, "shutdown cancellation returns a structured reply");
        let reply: Reply = serde_json::from_str(line.trim()).unwrap();
        assert!(!reply.ok, "read-only op aborted at shutdown: {reply:?}");
        let error = reply.error.unwrap();
        assert_eq!(error.code, "shutting_down");
        assert!(error.retryable, "a restart race is transient");
        let _ = std::fs::remove_file(&sock);
    }

    // lnrent-j3c: a concurrent MUTATING op still lands its reply through the SAME drain that
    // cancels a slow read-only op. The `AdminSuspend` handler commits its txn and replies (the token
    // must NOT abort it), while the parked ~10s `Reconcile` is cancelled — so the drain both honors
    // the mutating commit AND completes promptly.
    #[tokio::test]
    async fn mutating_op_completes_through_drain_despite_a_slow_readonly_op() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO subscription (id, recipe_id, state, created_at) VALUES ('s1','dummy','ACTIVE',1)",
            [],
        )
        .unwrap();
        let store = Store::spawn(conn);
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let payment: Arc<dyn PaymentBackend> = Arc::new(SlowBalancePayment {
            entered: entered_tx,
            delay: std::time::Duration::from_secs(10),
        });
        let sock = temp_sock("drain-mut");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_with_shutdown(
            store.clone(),
            recipes,
            clock,
            payment,
            RelayStatusCell::new(),
            sock.clone(),
            shutdown_rx,
        ));
        for _ in 0..50 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // The slow read-only op: send Reconcile, wait until it's inside the 10s balance read.
        let mut slow = UnixStream::connect(&sock).await.unwrap();
        let mut buf = serde_json::to_vec(&Request::Reconcile).unwrap();
        buf.push(b'\n');
        slow.write_all(&buf).await.unwrap();
        slow.flush().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("the reconcile handler reaches the slow balance read")
            .expect("entered signal");

        // The well-behaved MUTATING client connects and sends its AdminSuspend BEFORE shutdown, so its
        // handler reads the request the moment it is spawned — the request bytes never race the
        // (test-tuned, 500ms) read deadline that starts at accept (adversarial j3c review P3). A probe
        // round-trip AFTER both clients connect proves both handlers are already spawned (in flight at
        // shutdown), not parked in the listen backlog. The admin txn is mutating, so the shutdown token
        // never cancels it; whether it commits just before or during the drain, its reply must land and
        // the drain must still finish promptly.
        let mut legit = UnixStream::connect(&sock).await.unwrap();
        let mut buf = serde_json::to_vec(&Request::AdminSuspend { id: "s1".into() }).unwrap();
        buf.push(b'\n');
        legit.write_all(&buf).await.unwrap();
        legit.flush().await.unwrap();
        let probe = call(&sock, Request::Status).await.unwrap();
        assert!(probe.ok, "probe: {:?}", probe.error);

        shutdown_tx.send(true).unwrap();
        let shutdown_at = std::time::Instant::now();

        // The in-flight mutating handler still commits its admin txn and replies — never aborted by
        // the token (its request was sent above, before shutdown).
        let (legit_rd, _legit_wr) = legit.into_split();
        let mut legit_rd = BufReader::new(legit_rd);
        let mut line = String::new();
        legit_rd.read_line(&mut line).await.unwrap();
        let reply: Reply = serde_json::from_str(line.trim()).unwrap();
        assert!(
            reply.ok,
            "the in-flight admin action gets its reply through the drain: {:?}",
            reply.error
        );
        assert_eq!(reply.data.unwrap()["state"], "SUSPENDED");

        // The drain still completes promptly — the slow read-only op was cancelled, not awaited.
        let drained = tokio::time::timeout(std::time::Duration::from_secs(3), server)
            .await
            .expect("graceful drain must not await the 10s read-only sleep")
            .unwrap();
        assert!(
            drained.is_ok(),
            "serve_with_shutdown returns Ok: {drained:?}"
        );
        assert!(
            shutdown_at.elapsed() < std::time::Duration::from_secs(2),
            "drain bounded by cancellation of the read-only op, not the 10s sleep: {:?}",
            shutdown_at.elapsed()
        );

        // Committed AND acknowledged: the admin txn survived the shutdown.
        let state: String = store
            .read(|c| {
                Ok(
                    c.query_row("SELECT state FROM subscription WHERE id='s1'", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(state, "SUSPENDED");
    }

    #[tokio::test]
    async fn money_covered_zero_liabilities_with_mock_payment() {
        let store = mem_store();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());

        let data = money_data(&store, &payment).await;

        assert_eq!(data["ready"], json!(true));
        assert_eq!(data["warning"], Value::Null);
        assert_eq!(data["liability_count"], json!(0));
        assert_eq!(data["expected_msat"], json!(0));
        assert_eq!(data["gateway_ok"], json!(true));
    }

    #[tokio::test]
    async fn money_reports_ledger_expected_and_probe_gateway_at_zero_liabilities() {
        // Proves money reports the LEDGER `expected_msat` (a settled receipt on the books) and the
        // probe gateway UNCONDITIONALLY — even with nothing owed — and makes NO wallet read (the
        // double PANICS on `available_balance_msat`).
        let store = mem_store();
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status)
                     VALUES ('inv-1', 'sub-1', 'order:sub-1', 'order', 12, 'PAID')",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment = Arc::new(RecordingPayment::new(None, false));
        let payment_dyn: Arc<dyn PaymentBackend> = payment.clone();

        let data = money_data(&store, &payment_dyn).await;

        assert_eq!(data["expected_msat"], json!(12_000));
        assert_eq!(data["gateway_ok"], json!(false));
        assert_eq!(data["ready"], json!(true)); // nothing owed → covered
        assert_eq!(data["warning"], Value::Null);
        assert!(
            !payment.calls().contains(&"available_balance_msat"),
            "plain money must not read the wallet balance"
        );
    }

    #[tokio::test]
    async fn money_uses_one_backend_probe_snapshot_for_display_and_readiness() {
        let store = mem_store();
        seed_pending_refund_liability(&store).await;
        let payment = Arc::new(RecordingPayment::with_gateway_sequence(
            Some(10_000),
            vec![true, false],
        ));
        let payment_dyn: Arc<dyn PaymentBackend> = payment.clone();

        let data = money_data(&store, &payment_dyn).await;

        assert_eq!(data["gateway_ok"], json!(true));
        assert_eq!(data["ready"], json!(true));
        assert_eq!(data["warning"], Value::Null);
        assert_eq!(
            payment
                .calls()
                .iter()
                .filter(|call| **call == "refund_gateway_ready")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn money_reports_uncovered_refund_liability() {
        let store = mem_store();
        seed_pending_refund_liability(&store).await;
        // A SENT sweep locked 1_000 msat of the 2_000-msat receipt out of the books, so the ledger's
        // expected holdings (1_000) fall below the 2_000-msat required refund outlay.
        store
            .transaction(|tx| {
                tx.execute_batch(
                    "CREATE TABLE IF NOT EXISTS sweep_attempt (
                       id TEXT PRIMARY KEY, status TEXT NOT NULL, max_outlay_msat INTEGER NOT NULL
                     );",
                )?;
                tx.execute(
                    "INSERT INTO sweep_attempt (id, status, max_outlay_msat) VALUES ('sw-1', 'SENT', 1000)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment = Arc::new(RecordingPayment::new(None, true));
        let payment_dyn: Arc<dyn PaymentBackend> = payment;

        let data = money_data(&store, &payment_dyn).await;

        assert_eq!(data["ready"], json!(false));
        assert_eq!(data["warning"], json!("InsufficientBalance"));
        assert_eq!(data["liability_count"], json!(1));
        assert_eq!(data["gross_liability_sat"], json!(2));
        assert_eq!(data["required_msat"], json!(2_000));
        assert_eq!(data["expected_msat"], json!(1_000));
    }

    #[tokio::test]
    async fn money_is_readonly_for_store_and_payment_backend() {
        let store = mem_store();
        seed_pending_refund_liability(&store).await;
        let before = store_money_snapshot(&store).await;
        let payment = Arc::new(RecordingPayment::new(Some(10_000), true));
        let payment_dyn: Arc<dyn PaymentBackend> = payment.clone();

        let data = money_data(&store, &payment_dyn).await;

        assert_eq!(data["ready"], json!(true));
        assert_eq!(store_money_snapshot(&store).await, before);
        let calls = payment.calls();
        // §E: money reads only the two liveness probes + the readiness liability-pricing probes —
        // NEVER `available_balance_msat` (the double panics if it is, so its absence is enforced).
        for required in [
            "refund_gateway_ready",
            "refund_required_outlay_msat",
            "payment_status_by_key",
            "payment_started_by_key",
        ] {
            assert!(
                calls.contains(&required),
                "missing read-only call {required}: {calls:?}"
            );
        }
        for call in &calls {
            assert!(
                matches!(
                    *call,
                    "refund_gateway_ready"
                        | "refund_required_outlay_msat"
                        | "payment_status_by_key"
                        | "payment_started_by_key"
                ),
                "money made a non-read-only or balance-reading payment call: {calls:?}"
            );
        }
    }

    #[tokio::test]
    async fn reconcile_reports_ok_when_wallet_covers_books_reads_once_and_mutates_nothing() {
        let store = mem_store();
        seed_pending_refund_liability(&store).await; // a 2-sat receipt on the books → expected 2_000
        let before = store_money_snapshot(&store).await;
        let payment = Arc::new(RecordingPayment::new(Some(9_000), true));
        payment.allow_balance_read(); // reconcile is the ONE sanctioned wallet read
        let payment_dyn: Arc<dyn PaymentBackend> = payment.clone();

        let data = reconcile_data(&store, &payment_dyn).await;

        assert_eq!(data["wallet_msat"], json!(9_000));
        assert_eq!(data["expected_msat"], json!(2_000));
        assert_eq!(data["verdict"], json!("OK")); // wallet 9_000 ≥ books 2_000
        assert_eq!(
            payment
                .calls()
                .iter()
                .filter(|c| **c == "available_balance_msat")
                .count(),
            1,
            "reconcile reads the wallet EXACTLY once"
        );
        assert_eq!(
            store_money_snapshot(&store).await,
            before,
            "reconcile is report-only — it mutates nothing"
        );
    }

    #[tokio::test]
    async fn reconcile_reports_drift_when_wallet_below_books() {
        let store = mem_store();
        seed_pending_refund_liability(&store).await; // expected 2_000
        let payment = Arc::new(RecordingPayment::new(Some(1_500), true));
        payment.allow_balance_read();
        let payment_dyn: Arc<dyn PaymentBackend> = payment.clone();

        let data = reconcile_data(&store, &payment_dyn).await;

        assert_eq!(data["wallet_msat"], json!(1_500));
        assert_eq!(data["expected_msat"], json!(2_000));
        assert_eq!(data["verdict"], json!("DRIFT")); // wallet 1_500 < books 2_000
        assert_eq!(
            payment
                .calls()
                .iter()
                .filter(|c| **c == "available_balance_msat")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn reconcile_reports_unknown_when_backend_has_no_balance() {
        // A backend with no observable balance (MockPayment) → wallet null, verdict UNKNOWN, no panic.
        let store = mem_store();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());

        let data = reconcile_data(&store, &payment).await;

        assert_eq!(data["wallet_msat"], Value::Null);
        assert_eq!(data["expected_msat"], json!(0));
        assert_eq!(data["verdict"], json!("UNKNOWN"));
    }

    #[test]
    fn available_balance_msat_has_exactly_one_non_test_call_site() {
        // §F invariant: after urw.10 the live wallet balance is read at EXACTLY one place — the
        // reconcile handler. Scan the daemon source, exclude test code (everything from the first
        // `#[cfg(test)]` onward, per this crate's file convention) and the trait def/impl (a `.`
        // method call, so `fn available_balance_msat` never matches), and assert exactly one
        // `.available_balance_msat(` call remains.
        fn collect_rs(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    collect_rs(&path, out);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    out.push(path);
                }
            }
        }
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs(&src, &mut files);

        let mut calls = 0usize;
        for f in &files {
            // A SEPARATE-FILE test module (`foo/tests.rs`, gated by `#[cfg(test)] mod tests;` in the
            // parent) is entirely test code, but its `#[cfg(test)]` gate lives in the parent file — so
            // the split heuristic below cannot see it. Skip `tests.rs` files wholesale: they are
            // test-only by this repo's convention, and counting them would flag legitimate test calls.
            if f.file_name().and_then(|n| n.to_str()) == Some("tests.rs") {
                continue;
            }
            let text = std::fs::read_to_string(f).unwrap();
            // Non-test code precedes the first `#[cfg(test)]` in every other file (Rust convention here).
            let non_test = text.split("#[cfg(test)]").next().unwrap_or("");
            calls += non_test.matches(".available_balance_msat(").count();
        }
        assert_eq!(
            calls, 1,
            "available_balance_msat must have exactly one non-test call site (the reconcile handler)"
        );
    }

    #[tokio::test]
    async fn status_recipes_subs_round_trip() {
        let (_store, sock) = serve_temp().await;

        let st = call(&sock, Request::Status).await.unwrap();
        assert!(st.ok);
        assert_eq!(st.data.unwrap()["subscriptions"], json!(1));

        let rs = call(&sock, Request::Recipes).await.unwrap();
        assert!(
            rs.ok
                && rs
                    .data
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r["id"] == "dummy")
        );

        let subs = call(&sock, Request::Subs).await.unwrap();
        let arr = subs.data.unwrap();
        assert_eq!(arr[0]["id"], "s1");
        assert_eq!(arr[0]["state"], "ACTIVE");
    }

    // urw.5: `refunds` lists non-terminal + parked rows with their persisted fields, and
    // `refund-retry` resets a parked (FAILED) row to PENDING while rejecting a non-parked id.
    #[tokio::test]
    async fn refunds_lists_and_retry_requeues_only_parked() {
        let store = mem_store();
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(crate::backends::MockPayment::new());

        store
            .transaction(|tx| {
                // A parked (FAILED) refund with an LN-address dest, and a PENDING one.
                tx.execute(
                    "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                     VALUES ('r-failed', 's1', 'a@b.com', 500, 'refund:r-failed', 'FAILED', 5, 100, 200)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                     VALUES ('r-pending', 's2', 'lnbc1...', 700, 'refund:r-pending', 'PENDING', 1, 100, 150)",
                    [],
                )?;
                // A SENT row must NOT appear in the list.
                tx.execute(
                    "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                     VALUES ('r-sent', 's3', 'x@y.com', 300, 'refund:r-sent', 'SENT', 1, 100, 160)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        // List: the two non-terminal rows, with dest_form + fields; the SENT row excluded.
        let list = dispatch(Request::Refunds, &store, &recipes, &clock, &payment, &RelayStatusCell::new()).await;
        let arr = list.data.unwrap();
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "PENDING + FAILED listed, SENT excluded");
        let failed = arr.iter().find(|r| r["id"] == "r-failed").unwrap();
        assert_eq!(failed["dest_form"], "ln_address");
        assert_eq!(failed["amount_sat"], json!(500));
        assert_eq!(failed["status"], "FAILED");
        assert_eq!(failed["attempts"], json!(5));

        // Retry the parked row → PENDING/attempts=0.
        let retry = dispatch(
            Request::RefundRetry { id: "r-failed".into() },
            &store,
            &recipes,
            &clock,
            &payment,
            &RelayStatusCell::new(),
        )
        .await;
        assert!(retry.ok, "retry of a parked refund succeeds: {:?}", retry.error);
        let (status, attempts): (String, i64) = store
            .read(|c| {
                Ok(c.query_row(
                    "SELECT status, attempts FROM refund_attempt WHERE id='r-failed'",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!((status.as_str(), attempts), ("PENDING", 0));

        // Retry a non-parked id (the SENT row) → invalid_state, mutates nothing.
        let bad = dispatch(
            Request::RefundRetry { id: "r-sent".into() },
            &store,
            &recipes,
            &clock,
            &payment,
            &RelayStatusCell::new(),
        )
        .await;
        assert!(!bad.ok);
        assert_eq!(bad.error.unwrap().code, "invalid_state");
        let sent_status: String = store
            .read(|c| Ok(c.query_row("SELECT status FROM refund_attempt WHERE id='r-sent'", [], |r| r.get(0))?))
            .await
            .unwrap();
        assert_eq!(sent_status, "SENT", "a non-parked refund is untouched");
    }

    // urw.5 (codex P2): retry must SUPERSEDE the stale parked-FAILED billing.refund DM at the
    // deterministic outbox id, so the later success DM isn't ON CONFLICT-blocked (buyer told
    // "failed" for a refund that actually paid).
    #[tokio::test]
    async fn retry_supersedes_the_stale_failed_refund_dm() {
        let store = mem_store();
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(crate::backends::MockPayment::new());

        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, created_at, updated_at)
                     VALUES ('ref-order:s1', 's1', 'a@b.com', 500, 'refund:order:s1', 'FAILED', 5, 10, 20)",
                    [],
                )?;
                // The stale parked-FAILED billing.refund DM at the deterministic outbox id.
                tx.execute(
                    "INSERT INTO outbox (id, recipient, subscription_id, msg_type, payload_json, state, attempts, created_at)
                     VALUES ('outbox:refund:order:s1', 'buyerhex', 's1', 'billing.refund',
                             '{\"type\":\"billing.refund\",\"subscription_id\":\"s1\",\"amount_sat\":500,\"status\":\"failed\"}',
                             'PENDING', 0, 20)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let outbox_count = |store: Store| async move {
            store
                .read(|c| {
                    Ok(c.query_row(
                        "SELECT count(*) FROM outbox WHERE id='outbox:refund:order:s1'",
                        [],
                        |r| r.get::<_, i64>(0),
                    )?)
                })
                .await
                .unwrap()
        };
        assert_eq!(outbox_count(store.clone()).await, 1, "stale failed DM present");

        let retry = dispatch(
            Request::RefundRetry { id: "ref-order:s1".into() },
            &store,
            &recipes,
            &clock,
            &payment,
            &RelayStatusCell::new(),
        )
        .await;
        assert!(retry.ok, "retry: {:?}", retry.error);
        assert_eq!(
            outbox_count(store).await,
            0,
            "the stale failed DM is superseded so a fresh success DM can enqueue"
        );
    }

    // urw.2: `teardowns` lists open dead-letters and `status` folds `open_teardowns`.
    #[tokio::test]
    async fn teardowns_and_status_surface_open_dead_letters() {
        let store = mem_store();
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(crate::backends::MockPayment::new());

        // Empty to start.
        let td = dispatch(Request::Teardowns, &store, &recipes, &clock, &payment, &RelayStatusCell::new()).await;
        assert_eq!(td.data.unwrap()["open_total"], json!(0));
        let st = dispatch(Request::Status, &store, &recipes, &clock, &payment, &RelayStatusCell::new()).await;
        assert_eq!(st.data.unwrap()["open_teardowns"], json!(0));

        // Record a failed teardown → it surfaces in both.
        crate::teardown::record_failure(&store, "s1", "destroy", None, "boom", 100)
            .await
            .unwrap();

        let td = dispatch(Request::Teardowns, &store, &recipes, &clock, &payment, &RelayStatusCell::new()).await;
        let d = td.data.unwrap();
        assert_eq!(d["open_total"], json!(1));
        assert_eq!(d["teardown_failures"][0]["subscription_id"], "s1");
        assert_eq!(d["teardown_failures"][0]["hook"], "destroy");

        let st = dispatch(Request::Status, &store, &recipes, &clock, &payment, &RelayStatusCell::new()).await;
        assert_eq!(st.data.unwrap()["open_teardowns"], json!(1));
    }

    // lnrent-urw.6: `Request::Relays` returns the shared snapshot verbatim, and `Status` folds a
    // connected/total summary from the same cell.
    #[tokio::test]
    async fn relays_and_status_surface_pool_liveness() {
        use crate::relay_status::RelayStatusRow;
        let store = mem_store();
        let recipes = Arc::new(Vec::<Recipe>::new());
        let clock: Arc<dyn Clock> = Arc::new(crate::clock::TestClock::new(1_000));
        let payment: Arc<dyn PaymentBackend> = Arc::new(crate::backends::MockPayment::new());
        let cell = RelayStatusCell::new();
        cell.set(vec![
            RelayStatusRow {
                url: "wss://a".into(),
                connected: true,
                status: "Connected".into(),
                last_connected_at: Some(900),
            },
            RelayStatusRow {
                url: "wss://b".into(),
                connected: false,
                status: "Disconnected".into(),
                last_connected_at: None,
            },
        ]);

        let relays = dispatch(Request::Relays, &store, &recipes, &clock, &payment, &cell).await;
        let arr = relays.data.unwrap();
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["url"], "wss://a");
        assert_eq!(arr[0]["connected"], json!(true));
        assert_eq!(arr[0]["last_connected_at"], json!(900));
        assert_eq!(arr[1]["connected"], json!(false));
        assert_eq!(arr[1]["last_connected_at"], Value::Null);

        let st = dispatch(Request::Status, &store, &recipes, &clock, &payment, &cell).await;
        let d = st.data.unwrap();
        assert_eq!(d["relays_total"], json!(2));
        assert_eq!(d["relays_connected"], json!(1));

        // An empty cell (pre-refresh / no relays) surfaces 0/0, never an error.
        let st0 = dispatch(
            Request::Status,
            &store,
            &recipes,
            &clock,
            &payment,
            &RelayStatusCell::new(),
        )
        .await;
        let d0 = st0.data.unwrap();
        assert_eq!(d0["relays_total"], json!(0));
        assert_eq!(d0["relays_connected"], json!(0));
    }

    #[tokio::test]
    async fn admin_suspend_routes_through_store_and_journals() {
        let (store, sock) = serve_temp().await;

        let r = call(&sock, Request::AdminSuspend { id: "s1".into() })
            .await
            .unwrap();
        assert!(r.ok);
        assert_eq!(r.data.unwrap()["state"], "SUSPENDED");

        // state changed AND an event_log row was written (journaled).
        let (state, events): (String, i64) = store
            .read(|c| {
                let s = c.query_row("SELECT state FROM subscription WHERE id='s1'", [], |r| r.get(0))?;
                let e = c.query_row("SELECT count(*) FROM event_log WHERE subscription_id='s1' AND kind='admin_suspend'", [], |r| r.get(0))?;
                Ok((s, e))
            })
            .await
            .unwrap();
        assert_eq!(state, "SUSPENDED");
        assert_eq!(events, 1, "the admin action was journaled to event_log");

        // resume back
        let r = call(&sock, Request::AdminResume { id: "s1".into() })
            .await
            .unwrap();
        assert_eq!(r.data.unwrap()["state"], "ACTIVE");
    }

    #[tokio::test]
    async fn structured_errors_for_missing_and_bad_state() {
        let (_store, sock) = serve_temp().await;

        let nf = call(&sock, Request::Sub { id: "nope".into() })
            .await
            .unwrap();
        assert!(!nf.ok);
        assert_eq!(nf.error.unwrap().code, "not_found");

        // s1 is ACTIVE, so resume (SUSPENDED->ACTIVE) is an invalid transition.
        let bad = call(&sock, Request::AdminResume { id: "s1".into() })
            .await
            .unwrap();
        assert!(!bad.ok);
        assert_eq!(bad.error.unwrap().code, "invalid_state");
    }

    // gate1-operator-sweep (urw.3): SweepQuote -> Sweep end-to-end over the IPC socket on
    // MockPayment. Asserts the ALLOW verdict, the SENT reply, and the PERSISTED sweep_attempt +
    // event_log rows (NOT relay delivery).
    #[tokio::test]
    async fn sweep_quote_then_execute_persists_rows_over_ipc() {
        let (store, sock) = serve_temp().await;
        // s1 is ACTIVE (a final receipt): give it a PAID order invoice so the ledger has surplus.
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES ('inv-s1', 's1', 'order:s1', 'order', 100000, 'PAID', 0)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let bolt11 = crate::refund_resolver::mint_bolt11(40_000 * 1000, "meta", 1_000, 3_600);

        let q = call(&sock, Request::SweepQuote { bolt11: bolt11.clone() })
            .await
            .unwrap();
        assert!(q.ok, "quote: {:?}", q.error);
        let qd = q.data.unwrap();
        assert_eq!(qd["verdict"], "ALLOW");
        assert_eq!(qd["amount_sat"], json!(40_000));
        assert_eq!(qd["surplus_msat"], json!(100_000_000));

        let s = call(&sock, Request::Sweep { bolt11 }).await.unwrap();
        assert!(s.ok, "sweep: {:?}", s.error);
        let sd = s.data.unwrap();
        assert_eq!(sd["status"], "SENT");
        assert_eq!(sd["swept"], json!(true));

        let (sent, events): (i64, i64) = store
            .read(|c| {
                let sent = c.query_row(
                    "SELECT count(*) FROM sweep_attempt WHERE status='SENT'",
                    [],
                    |r| r.get(0),
                )?;
                let events =
                    c.query_row("SELECT count(*) FROM event_log WHERE kind='sweep'", [], |r| r.get(0))?;
                Ok((sent, events))
            })
            .await
            .unwrap();
        assert_eq!(sent, 1, "one SENT sweep_attempt row persisted");
        assert_eq!(events, 2, "intent + sent 'sweep' journal rows persisted");
    }

    // gate1-operator-sweep: `lnrent money` folds the surplus breakdown + last_sweep (pure ledger).
    #[tokio::test]
    async fn money_includes_surplus_breakdown_and_last_sweep() {
        let store = mem_store();
        // A final (ACTIVE) receipt of 7 sat plus a SENT sweep that paid out 3_000 msat of it.
        store
            .transaction(|tx| {
                tx.execute(
                    "INSERT INTO subscription (id, state, created_at, updated_at) VALUES ('s', 'ACTIVE', 0, 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO invoice (id, subscription_id, external_id, kind, amount_sat, status, issued_at)
                     VALUES ('inv', 's', 'order:s', 'order', 7, 'PAID', 0)",
                    [],
                )?;
                tx.execute(
                    "INSERT INTO sweep_attempt (id, bolt11, amount_sat, max_outlay_msat, status, attempts, created_at, sent_at)
                     VALUES ('sweep:h', 'lnbc1', 3, 3000, 'SENT', 1, 10, 20)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let payment: Arc<dyn PaymentBackend> = Arc::new(MockPayment::new());

        let data = money_data(&store, &payment).await;
        assert_eq!(data["earned_msat"], json!(7_000));
        assert_eq!(data["paid_out_msat"], json!(3_000));
        assert_eq!(data["surplus_msat"], json!(4_000)); // 7_000 earned − 3_000 swept
        assert_eq!(data["last_sweep"]["id"], "sweep:h");
        assert_eq!(data["last_sweep"]["status"], "SENT");
    }
}
