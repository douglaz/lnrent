//! Local CLI <-> daemon IPC over a Unix-domain socket (lnrent-7fp.12; ADR-0001, ADR-0014;
//! SPEC §4.2/§4.7/§10). The daemon owns the socket; the `lnrent` CLI and Claude skills act
//! ONLY through it — they never touch sqlite directly, so the daemon stays the sole writer.
//! This is the OPERATOR's agent surface: every reply is structured JSON (so an operator agent
//! drives it), and it is never network-reachable (a UDS with owner-only perms, no HTTP/MCP).

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
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

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
    AdminSuspend { id: String },
    AdminResume { id: String },
    DevSettle { subscription_id: String },
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

/// Serve IPC on `path` until the listener errors. Each connection is one request -> one reply.
/// The socket is created owner-only and is removed-then-rebound to clear a stale socket. This is
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
    let _ = std::fs::remove_file(path);
    let listener =
        UnixListener::bind(path).with_context(|| format!("binding {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("perms on {}", path.display()))?;
    tracing::info!(socket = %path.display(), "ipc serving");
    // Track the spawned per-connection handlers so a graceful shutdown can AWAIT the ones still
    // in flight — committing an admin txn and writing its reply — instead of dropping them when the
    // accept loop stops (the handlers were previously detached, so a shutdown could lose an in-flight
    // admin txn+reply, violating the graceful-shutdown AC).
    let mut conns = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _addr) = accepted?;
                let (store, recipes, clock, payment, relays) = (
                    store.clone(),
                    recipes.clone(),
                    clock.clone(),
                    payment.clone(),
                    relays.clone(),
                );
                conns.spawn(async move {
                    if let Err(e) = handle_conn(stream, store, recipes, clock, payment, relays).await
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
                    // Let in-flight handlers finish their txn + reply. Bounded by the supervisor's
                    // shutdown grace, which aborts this whole task if the drain overruns.
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
) -> Result<()> {
    let (rd, mut wr) = stream.into_split();
    // Bounded read: cap the request frame so an over-long line can't exhaust memory.
    let mut rd = BufReader::new(rd.take(MAX_REQUEST_BYTES));
    let mut line = String::new();
    rd.read_line(&mut line).await?;
    let reply = if !line.ends_with('\n') {
        // hit the byte cap without a line terminator -> over-long / malformed frame
        Reply::err("bad_request", "request too large or unterminated")
    } else {
        match serde_json::from_str::<Request>(line.trim()) {
            Ok(req) => dispatch(req, &store, &recipes, &clock, &payment, &relays).await,
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
                    Reply::ok(report.to_money_value(probe.gateway_ok(), probe.federation_ok()))
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
        // Unique per test (all tests share one PID), so concurrent tests don't clobber the socket.
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let sock = std::env::temp_dir().join(format!("lnrent-ipc-{}-{n}.sock", std::process::id()));
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
            let text = std::fs::read_to_string(f).unwrap();
            // Non-test code precedes the first `#[cfg(test)]` in every file (Rust convention here).
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
}
