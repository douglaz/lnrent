//! Local CLI <-> daemon IPC over a Unix-domain socket (lnrent-7fp.12; ADR-0001, ADR-0014;
//! SPEC §4.2/§4.7/§10). The daemon owns the socket; the `lnrent` CLI and Claude skills act
//! ONLY through it — they never touch sqlite directly, so the daemon stays the sole writer.
//! This is the OPERATOR's agent surface: every reply is structured JSON (so an operator agent
//! drives it), and it is never network-reachable (a UDS with owner-only perms, no HTTP/MCP).

use crate::clock::{Clock, SystemClock};
use crate::recipe::Recipe;
use crate::store::Store;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

/// A request from the CLI to the daemon. One JSON object per line.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Status,
    Recipes,
    Subs,
    Sub { id: String },
    AdminSuspend { id: String },
    AdminResume { id: String },
}

/// A structured error a caller (human or agent) can branch on (mirrors §5.1 error shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcError {
    pub code: String,
    pub message: String,
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
        Reply { ok: true, data: Some(data), error: None }
    }
    pub fn err(code: &str, message: impl Into<String>) -> Reply {
        Reply { ok: false, data: None, error: Some(IpcError { code: code.into(), message: message.into() }) }
    }
}

/// Serve IPC on `path` until the listener errors. Each connection is one request -> one reply.
/// The socket is created owner-only and is removed-then-rebound to clear a stale socket.
pub async fn serve(store: Store, recipes: Arc<Vec<Recipe>>, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).with_context(|| format!("binding {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("perms on {}", path.display()))?;
    tracing::info!(socket = %path.display(), "ipc serving");
    loop {
        let (stream, _addr) = listener.accept().await?;
        let (store, recipes) = (store.clone(), recipes.clone());
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, store, recipes).await {
                tracing::warn!(error = %e, "ipc connection error");
            }
        });
    }
}

async fn handle_conn(stream: UnixStream, store: Store, recipes: Arc<Vec<Recipe>>) -> Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let mut line = String::new();
    rd.read_line(&mut line).await?;
    let reply = match serde_json::from_str::<Request>(line.trim()) {
        Ok(req) => dispatch(req, &store, &recipes).await,
        Err(e) => Reply::err("bad_request", format!("invalid request: {e}")),
    };
    let mut out = serde_json::to_vec(&reply)?;
    out.push(b'\n');
    wr.write_all(&out).await?;
    wr.flush().await?;
    Ok(())
}

/// Route a request to the store actor (reads) / a journaled transaction (admin mutations).
pub async fn dispatch(req: Request, store: &Store, recipes: &Arc<Vec<Recipe>>) -> Reply {
    match req {
        Request::Status => match store
            .read(|c| Ok(c.query_row("SELECT count(*) FROM subscription", [], |r| r.get::<_, i64>(0))?))
            .await
        {
            Ok(n) => Reply::ok(json!({"daemon": "ok", "recipes": recipes.len(), "subscriptions": n})),
            Err(e) => Reply::err("internal", e.to_string()),
        },

        Request::Recipes => {
            let list: Vec<Value> = recipes
                .iter()
                .map(|r| json!({"id": r.service.id, "name": r.service.name, "version": r.service.version, "summary": r.service.summary}))
                .collect();
            Reply::ok(json!(list))
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

        Request::AdminSuspend { id } => admin_transition(store, &id, &["ACTIVE"], "SUSPENDED", "admin_suspend").await,
        Request::AdminResume { id } => admin_transition(store, &id, &["SUSPENDED"], "ACTIVE", "admin_resume").await,
    }
}

/// An admin force-transition: CAS the subscription state from one of `from` to `to`, journaled
/// to `event_log`, all in one store transaction (sole writer, ADR-0001). The reconcile/provision
/// integration runs the actual lifecycle hooks; this is the operator override of the state.
async fn admin_transition(store: &Store, id: &str, from: &[&str], to: &str, kind: &str) -> Reply {
    let id = id.to_string();
    let to = to.to_string();
    let from: Vec<String> = from.iter().map(|s| s.to_string()).collect();
    let now = SystemClock.now();
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
        Ok(false) => Reply::err("invalid_state", format!("subscription `{id}` not in {from:?}")),
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
        )
        .ok();
    Ok(v)
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
    use crate::store::{Store, SCHEMA};
    use rusqlite::Connection;

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
        tokio::spawn(async move {
            let _ = serve(s2, recipes, &sock2).await;
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
    async fn status_recipes_subs_round_trip() {
        let (_store, sock) = serve_temp().await;

        let st = call(&sock, Request::Status).await.unwrap();
        assert!(st.ok);
        assert_eq!(st.data.unwrap()["subscriptions"], json!(1));

        let rs = call(&sock, Request::Recipes).await.unwrap();
        assert!(rs.ok && rs.data.unwrap().as_array().unwrap().iter().any(|r| r["id"] == "dummy"));

        let subs = call(&sock, Request::Subs).await.unwrap();
        let arr = subs.data.unwrap();
        assert_eq!(arr[0]["id"], "s1");
        assert_eq!(arr[0]["state"], "ACTIVE");
    }

    #[tokio::test]
    async fn admin_suspend_routes_through_store_and_journals() {
        let (store, sock) = serve_temp().await;

        let r = call(&sock, Request::AdminSuspend { id: "s1".into() }).await.unwrap();
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
        let r = call(&sock, Request::AdminResume { id: "s1".into() }).await.unwrap();
        assert_eq!(r.data.unwrap()["state"], "ACTIVE");
    }

    #[tokio::test]
    async fn structured_errors_for_missing_and_bad_state() {
        let (_store, sock) = serve_temp().await;

        let nf = call(&sock, Request::Sub { id: "nope".into() }).await.unwrap();
        assert!(!nf.ok);
        assert_eq!(nf.error.unwrap().code, "not_found");

        // s1 is ACTIVE, so resume (SUSPENDED->ACTIVE) is an invalid transition.
        let bad = call(&sock, Request::AdminResume { id: "s1".into() }).await.unwrap();
        assert!(!bad.ok);
        assert_eq!(bad.error.unwrap().code, "invalid_state");
    }
}
