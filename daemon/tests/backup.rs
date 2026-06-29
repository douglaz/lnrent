//! OFFLINE integration test for the cold backup + restore (lnrent-7fp.14 PART A). Builds a populated
//! data dir (state DB rows + a fake fedimint subtree + config + seed), runs `backup()` to a temp
//! dest, `restore()`s into a FRESH data dir, and asserts the full commitment set reproduces EXACTLY.
//! No live federation / no `fedimint` feature: the fedimint subtree is exercised as opaque bytes.

use lnrentd::backup::{backup, restore};
use lnrentd::store;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const FED_ID: &str = "fed11deadbeefcafe";

/// The lnrent-owned fedimint idempotency index schema (mirrors `fedimint_backend.rs`'s
/// `INDEX_SCHEMA`; inlined here because that module is `fedimint`-feature-gated). The test only
/// needs a populated `lnrent_index.db` to copy + reread, so this fixture stands in for it.
const INDEX_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS fedimint_invoice (
    external_id   TEXT PRIMARY KEY,
    operation_id  TEXT NOT NULL,
    invoice_id    TEXT NOT NULL,
    bolt11        TEXT NOT NULL,
    payment_hash  TEXT NOT NULL,
    amount_sat    INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'OPEN',
    settled_at    INTEGER
);
CREATE TABLE IF NOT EXISTS fedimint_pay (
    idempotency_key  TEXT PRIMARY KEY,
    operation_id     TEXT NOT NULL,
    backend_pay_id   TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    pay_kind         TEXT NOT NULL DEFAULT 'ln'
);
";

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lnrent-backup-test-{tag}-{}-{nanos}",
        std::process::id()
    ))
}

/// The permission bits (low 9) of `path`.
fn mode(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// chmod `path` to `bits` (for loosening a source file so a test can prove backup TIGHTENS it).
fn set_mode(path: &std::path::Path, bits: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(bits)).unwrap();
}

/// Populate the lnrent state DB at `data_dir/lnrent.sqlite` with one row in each of the
/// commitment-bearing tables the backup must reproduce.
fn populate_state_db(data_dir: &std::path::Path) {
    let conn = store::open(data_dir.join("lnrent.sqlite")).unwrap();
    conn.execute(
        "INSERT INTO subscription (id, recipe_id, buyer_pubkey, state, paid_through, soft_date, period_s, created_at, updated_at)
         VALUES (?,?,?,?,?,?,?,?,?)",
        params!["sub1", "recipe-a", "buyerpk", "ACTIVE", 1_900_000_000i64, 1_800_000_000i64, 2_592_000i64, 1_000i64, 1_000i64],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO invoice (id, subscription_id, external_id, kind, bolt11, amount_sat, status, expires_at, issued_at)
         VALUES (?,?,?,?,?,?,?,?,?)",
        params!["inv1", "sub1", "ext-1", "order", "lnbc1example", 1234i64, "PAID", 1_900_000_000i64, 1_000i64],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO refund_attempt (id, subscription_id, dest, amount_sat, idempotency_key, status, attempts, resolution_gen, created_at, updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?)",
        params!["ref1", "sub1", "buyer@ln.example", 1234i64, "refund:ext-1", "PENDING", 1i64, 0i64, 1_000i64, 1_000i64],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO reservation (id, order_id, resources_json, ports_json, state, expires_at, created_at)
         VALUES (?,?,?,?,?,?,?)",
        params!["res1", "order-1", "{\"cpu\":2}", "[8080]", "HELD", 1_900_000_000i64, 1_000i64],
    )
    .unwrap();
    // Connection dropped here -> closed (the cold-backup precondition).
}

/// Build a fake `fedimint/<FED_ID>/` subtree: a `client.db/` rocksdb dir with a sentinel file, and a
/// populated `lnrent_index.db`.
fn populate_fedimint_dir(data_dir: &std::path::Path) {
    let fed_dir = data_dir.join("fedimint").join(FED_ID);
    let client_db = fed_dir.join("client.db");
    fs::create_dir_all(&client_db).unwrap();
    fs::write(client_db.join("CURRENT"), b"rocksdb-sentinel\n").unwrap();

    let idx = Connection::open(fed_dir.join("lnrent_index.db")).unwrap();
    idx.execute_batch(INDEX_SCHEMA).unwrap();
    idx.execute(
        "INSERT INTO fedimint_invoice (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at, status)
         VALUES (?,?,?,?,?,?,?,?)",
        params!["ext-1", "op-1", "invid-1", "lnbc1example", "ph-1", 1234i64, 1_900_000_000i64, "PAID"],
    )
    .unwrap();
    idx.execute(
        "INSERT INTO fedimint_pay (idempotency_key, operation_id, backend_pay_id, status, pay_kind)
         VALUES (?,?,?,?,?)",
        params!["refund:ext-1", "op-2", "bpay-1", "SENT", "ln"],
    )
    .unwrap();
}

#[test]
fn backup_restore_round_trip_reproduces_state_and_fedimint() {
    let base = temp_dir("roundtrip");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();

    populate_state_db(&data_dir);
    populate_fedimint_dir(&data_dir);
    fs::write(
        data_dir.join("fedimint.json"),
        r#"{"invite":"fed11invite","gateway":"03gateway"}"#,
    )
    .unwrap();
    fs::write(
        data_dir.join("operator.seed"),
        "leader monkey parrot ring guide accident before fence cannon height naive bean\n",
    )
    .unwrap();

    // --- backup ---
    let dest = base.join("backup");
    let manifest = backup(&data_dir, &dest).unwrap();
    assert!(manifest.state_db);
    assert!(manifest.fedimint_dir);
    assert!(manifest.fedimint_config);
    assert!(manifest.operator_seed);
    assert_eq!(manifest.federations, vec![FED_ID.to_string()]);
    assert!(dest.join("lnrent.sqlite").is_file());
    assert!(
        !dest.join("lnrent.sqlite-wal").exists() && !dest.join("lnrent.sqlite-shm").exists(),
        "VACUUM INTO must produce a coherent single file, not -wal/-shm sidecars"
    );
    assert!(dest.join("MANIFEST.json").is_file());

    // --- restore into a FRESH data dir ---
    let restored = base.join("restored");
    restore(&dest, &restored, false).unwrap();

    // --- the lnrent state rows reproduce EXACTLY (reopened via the store) ---
    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let (state, paid_through, soft_date, period_s): (String, i64, i64, i64) = conn
        .query_row(
            "SELECT state, paid_through, soft_date, period_s FROM subscription WHERE id='sub1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), paid_through, soft_date, period_s),
        ("ACTIVE", 1_900_000_000, 1_800_000_000, 2_592_000)
    );

    let (ext, status, amount): (String, String, i64) = conn
        .query_row(
            "SELECT external_id, status, amount_sat FROM invoice WHERE id='inv1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (ext.as_str(), status.as_str(), amount),
        ("ext-1", "PAID", 1234)
    );

    let (key, rstatus, res_gen): (String, String, i64) = conn
        .query_row(
            "SELECT idempotency_key, status, resolution_gen FROM refund_attempt WHERE id='ref1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (key.as_str(), rstatus.as_str(), res_gen),
        ("refund:ext-1", "PENDING", 0)
    );

    let (order, rsstate): (String, String) = conn
        .query_row(
            "SELECT order_id, state FROM reservation WHERE id='res1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((order.as_str(), rsstate.as_str()), ("order-1", "HELD"));
    drop(conn);

    // --- the fedimint subtree reproduces: rocksdb sentinel + the lnrent index rows ---
    let rfed = restored.join("fedimint").join(FED_ID);
    let sentinel = rfed.join("client.db").join("CURRENT");
    assert!(sentinel.is_file(), "restored rocksdb sentinel file");
    assert_eq!(fs::read_to_string(&sentinel).unwrap(), "rocksdb-sentinel\n");

    let idx = Connection::open(rfed.join("lnrent_index.db")).unwrap();
    let (inv_status, inv_amount): (String, i64) = idx
        .query_row(
            "SELECT status, amount_sat FROM fedimint_invoice WHERE external_id='ext-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((inv_status.as_str(), inv_amount), ("PAID", 1234));
    let (pay_status, pay_kind): (String, String) = idx
        .query_row(
            "SELECT status, pay_kind FROM fedimint_pay WHERE idempotency_key='refund:ext-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((pay_status.as_str(), pay_kind.as_str()), ("SENT", "ln"));
    drop(idx);

    // --- config + seed reproduce ---
    assert_eq!(
        fs::read_to_string(restored.join("fedimint.json")).unwrap(),
        r#"{"invite":"fed11invite","gateway":"03gateway"}"#
    );
    assert!(restored.join("operator.seed").is_file());

    // --- restore REFUSES a non-empty data dir without --force ---
    let err = restore(&dest, &restored, false).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not empty") || msg.contains("--force"),
        "expected a non-empty-target refusal, got: {err}"
    );
    // ...and SUCCEEDS with force.
    restore(&dest, &restored, true).unwrap();

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_without_fedimint_or_seed_is_not_an_error() {
    let base = temp_dir("nofed");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir); // state DB only — no fedimint dir / config / seed

    let dest = base.join("backup");
    let manifest = backup(&data_dir, &dest).unwrap();
    assert!(manifest.state_db);
    assert!(
        !manifest.fedimint_dir,
        "no fedimint dir -> absent, not an error"
    );
    assert!(!manifest.fedimint_config);
    assert!(!manifest.operator_seed);
    assert!(manifest.federations.is_empty());
    assert!(!dest.join("fedimint.json").exists());
    assert!(!dest.join("operator.seed").exists());

    let restored = base.join("restored");
    let m = restore(&dest, &restored, false).unwrap();
    assert!(m.state_db);
    assert!(!restored.join("fedimint").exists());
    // The state DB still restored and reads back.
    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM subscription", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn restore_force_replaces_target_wholesale_no_stale_artifacts() {
    // A forced restore must make the target EQUAL the backup, not union the two: a `fedimint/` dir,
    // `operator.seed`, or `fedimint.json` the target had but the backup does NOT must be dropped. A
    // surviving stale seed / fedimint dir alongside a restored state DB is a money-path hazard (it
    // could mis-derive refund keys or strand ecash).
    let base = temp_dir("subsetforce");

    // The backup captures a state DB ONLY (no fedimint, no seed, no config).
    let src_data = base.join("src");
    fs::create_dir_all(&src_data).unwrap();
    populate_state_db(&src_data);
    let dest = base.join("backup");
    let m = backup(&src_data, &dest).unwrap();
    assert!(!m.fedimint_dir && !m.operator_seed && !m.fedimint_config);

    // The target already holds a fuller, DIFFERENT set: a fedimint subtree + a stale seed + a stale
    // config + a (stale) state DB with its WAL sidecars.
    let target = base.join("target");
    fs::create_dir_all(&target).unwrap();
    populate_state_db(&target);
    populate_fedimint_dir(&target);
    fs::write(target.join("operator.seed"), "STALE-SEED\n").unwrap();
    fs::write(target.join("fedimint.json"), r#"{"invite":"STALE"}"#).unwrap();

    // Force-restore the subset backup over it.
    restore(&dest, &target, true).unwrap();

    // The stale artifacts the backup did not carry are GONE — the target equals the backup.
    assert!(
        !target.join("fedimint").exists(),
        "stale fedimint dir must be cleared on a force restore"
    );
    assert!(
        !target.join("operator.seed").exists(),
        "stale operator.seed must be cleared on a force restore"
    );
    assert!(
        !target.join("fedimint.json").exists(),
        "stale fedimint.json must be cleared on a force restore"
    );
    // The state DB is the restored one, with no stale WAL/SHM sidecars alongside it.
    assert!(target.join("lnrent.sqlite").is_file());
    assert!(
        !target.join("lnrent.sqlite-wal").exists() && !target.join("lnrent.sqlite-shm").exists(),
        "a wholesale swap must not leave stale -wal/-shm sidecars"
    );
    // ...and it reads back through the store.
    let conn = store::open(target.join("lnrent.sqlite")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM subscription", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);

    // No staging/old scratch dirs leak in the parent after a successful restore.
    let leaked: Vec<_> = fs::read_dir(&base)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".lnrent-restore-"))
        .collect();
    assert!(leaked.is_empty(), "restore scratch dirs leaked: {leaked:?}");

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_refuses_a_data_dir_with_no_state_db() {
    let base = temp_dir("nodb");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    // No lnrent.sqlite -> wrong/empty data dir; backup must error rather than write a useless set.
    let err = backup(&data_dir, &base.join("backup")).unwrap_err();
    assert!(
        err.to_string().contains("no state DB"),
        "expected a missing-state-DB error, got: {err}"
    );
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn restore_refuses_a_corrupt_backup_set() {
    let base = temp_dir("corrupt");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    fs::write(data_dir.join("operator.seed"), "seed words\n").unwrap();

    let dest = base.join("backup");
    backup(&data_dir, &dest).unwrap();
    // Corrupt the set: delete a file the manifest says is present. Restore must NOT silently drop it.
    fs::remove_file(dest.join("operator.seed")).unwrap();

    let err = restore(&dest, &base.join("restored"), false).unwrap_err();
    assert!(
        err.to_string().contains("incomplete/corrupt"),
        "expected an incomplete-backup error, got: {err}"
    );
    // The target was never created (validation happens before any write).
    assert!(!base.join("restored").join("lnrent.sqlite").exists());

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_and_restore_harden_fedimint_files_to_0600() {
    // The fedimint subtree carries ecash-bearing material (client.db, lnrent_index.db); both backup
    // and restore must tighten it to owner-only rather than carry the source's umask mode (R2 P2).
    let base = temp_dir("perms");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    populate_fedimint_dir(&data_dir);
    // Loosen the SOURCE files to world-readable, so passing perms prove an active tighten (not an
    // inherited 0600 from the test's umask).
    let src_fed = data_dir.join("fedimint").join(FED_ID);
    set_mode(&src_fed.join("client.db").join("CURRENT"), 0o644);
    set_mode(&src_fed.join("lnrent_index.db"), 0o644);

    let dest = base.join("backup");
    backup(&data_dir, &dest).unwrap();
    let bk_fed = dest.join("fedimint").join(FED_ID);
    assert_eq!(
        mode(&bk_fed.join("client.db").join("CURRENT")),
        0o600,
        "backed-up rocksdb file must be owner-only"
    );
    assert_eq!(
        mode(&bk_fed.join("lnrent_index.db")),
        0o600,
        "backed-up lnrent index must be owner-only"
    );

    let restored = base.join("restored");
    restore(&dest, &restored, false).unwrap();
    let r_fed = restored.join("fedimint").join(FED_ID);
    assert_eq!(
        mode(&r_fed.join("client.db").join("CURRENT")),
        0o600,
        "restored rocksdb file must be owner-only"
    );
    assert_eq!(
        mode(&r_fed.join("lnrent_index.db")),
        0o600,
        "restored lnrent index must be owner-only"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_refuses_a_dest_inside_the_data_dir() {
    // A dest UNDER the data dir would make the recursive fedimint copy descend into its own output
    // forever (disk fill); a dest EQUAL to the data dir is the same self-overlap (review R1 P2).
    let base = temp_dir("destinside");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    populate_fedimint_dir(&data_dir);

    let inside = data_dir.join("fedimint").join("backup-here");
    let err = backup(&data_dir, &inside).unwrap_err();
    assert!(
        err.to_string().contains("overlap"),
        "expected an overlap refusal for a nested dest, got: {err}"
    );
    // The data dir was not clobbered (its real fedimint subtree survives).
    assert!(data_dir.join("fedimint").join(FED_ID).is_dir());

    let err2 = backup(&data_dir, &data_dir).unwrap_err();
    assert!(
        err2.to_string().contains("overlap"),
        "expected an overlap refusal for dest == data_dir, got: {err2}"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn restore_refuses_a_target_inside_the_backup() {
    // Restoring INTO a subdir of the backup source is the symmetric self-overlap.
    let base = temp_dir("restoreoverlap");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    let dest = base.join("backup");
    backup(&data_dir, &dest).unwrap();

    let err = restore(&dest, &dest.join("restored-here"), false).unwrap_err();
    assert!(
        err.to_string().contains("overlap"),
        "expected an overlap refusal, got: {err}"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn daemon_appears_running_is_false_for_an_unbindable_overlong_socket_path() {
    // A socket path longer than sun_path (~108 bytes) can never have been bound by a daemon, so
    // liveness must read NOT running rather than fail-closed on the ambiguous error (review R2 P3).
    let long = std::env::temp_dir().join("z".repeat(200));
    assert!(
        long.join("lnrent.sock").as_os_str().len() > 108,
        "the test path must exceed sun_path to exercise the InvalidInput branch"
    );
    assert!(
        !lnrentd::backup::daemon_appears_running(&long),
        "an unbindable (too-long) socket path -> not running"
    );
}

#[test]
fn daemon_appears_running_detects_a_live_socket() {
    let base = temp_dir("livesock");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    assert!(
        !lnrentd::backup::daemon_appears_running(&data_dir),
        "no socket -> not running"
    );

    let sock = data_dir.join("lnrent.sock");
    let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
    assert!(
        lnrentd::backup::daemon_appears_running(&data_dir),
        "a live IPC socket -> running"
    );
    drop(listener);
    // The socket FILE lingers after the listener is gone; connecting to it is refused -> not running.
    assert!(
        !lnrentd::backup::daemon_appears_running(&data_dir),
        "a stale socket file (no listener) -> not running"
    );

    let _ = fs::remove_dir_all(&base);
}

// --- CLI: the `backup` / `restore` subcommands are non-interactive, --json, exit 0/nonzero. -------

const LNRENT_ENV: &[&str] = &[
    "LNRENT_DATA_DIR",
    "LNRENT_RELAYS",
    "LNRENT_PAYMENT_BACKEND",
    "LNRENT_COMPUTE_BACKEND",
    "LNRENT_FEDIMINT_INVITE",
    "LNRENT_FEDIMINT_GATEWAY",
    "LNRENT_MNEMONIC",
    "LNRENT_CONFIG",
];

fn lnrentd() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lnrentd"));
    for key in LNRENT_ENV {
        cmd.env_remove(key);
    }
    cmd
}

#[test]
fn cli_backup_then_restore_round_trip_json() {
    let base = temp_dir("cli");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    {
        let conn = store::open(data_dir.join("lnrent.sqlite")).unwrap();
        conn.execute(
            "INSERT INTO subscription (id, state, paid_through) VALUES ('cli-sub','ACTIVE',42)",
            [],
        )
        .unwrap();
    }

    let dest = base.join("bk");
    let out = lnrentd()
        .args([
            "backup",
            "--json",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--dest",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "backup exit; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["state_db"], true);

    let restored = base.join("restored");
    let out = lnrentd()
        .args([
            "restore",
            "--json",
            "--data-dir",
            restored.to_str().unwrap(),
            "--from",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "restore exit; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], true);

    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let pt: i64 = conn
        .query_row(
            "SELECT paid_through FROM subscription WHERE id='cli-sub'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pt, 42);
    drop(conn);

    // A second restore over the now-populated target WITHOUT --force: nonzero exit + structured err.
    let out = lnrentd()
        .args([
            "restore",
            "--json",
            "--data-dir",
            restored.to_str().unwrap(),
            "--from",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "restore without --force must fail");
    assert!(
        out.stdout.is_empty(),
        "json errors go to stderr, not stdout"
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "restore_failed");

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn cli_backup_resolves_data_dir_from_config_file() {
    // An operator who sets `data_dir` ONLY in the daemon's config file must get a backup of THAT dir,
    // not `./data`. The CLI resolves the data dir through the same merge the daemon uses, so both the
    // `--config` flag and `LNRENT_CONFIG` env are honored (review R2 P1).
    let base = temp_dir("cfgdir");
    let data_dir = base.join("operator-state"); // a distinctive, NON-default location
    fs::create_dir_all(&data_dir).unwrap();
    {
        let conn = store::open(data_dir.join("lnrent.sqlite")).unwrap();
        conn.execute(
            "INSERT INTO subscription (id, state, paid_through) VALUES ('cfg-sub','ACTIVE',77)",
            [],
        )
        .unwrap();
    }
    // A TOML config file whose only setting is data_dir -> the distinctive location.
    let cfg = base.join("lnrent.toml");
    fs::write(
        &cfg,
        format!("data_dir = {:?}\n", data_dir.to_str().unwrap()),
    )
    .unwrap();

    // --- via the --config flag, NO --data-dir ---
    let dest = base.join("bk-flag");
    let out = lnrentd()
        .args([
            "backup",
            "--json",
            "--config",
            cfg.to_str().unwrap(),
            "--dest",
            dest.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "backup via --config must target the config's data_dir; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Proof it captured the config's data_dir (not ./data): the distinctive row is in the artifact.
    let conn = store::open(dest.join("lnrent.sqlite")).unwrap();
    let pt: i64 = conn
        .query_row(
            "SELECT paid_through FROM subscription WHERE id='cfg-sub'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pt, 77);
    drop(conn);

    // --- via LNRENT_CONFIG env (the daemon's actual mechanism), NO --data-dir/--config ---
    let dest_env = base.join("bk-env");
    let out = lnrentd()
        .env("LNRENT_CONFIG", cfg.to_str().unwrap())
        .args(["backup", "--json", "--dest", dest_env.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "backup via LNRENT_CONFIG must target the config's data_dir; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let conn = store::open(dest_env.join("lnrent.sqlite")).unwrap();
    let pt: i64 = conn
        .query_row(
            "SELECT paid_through FROM subscription WHERE id='cfg-sub'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pt, 77);
    drop(conn);

    let _ = fs::remove_dir_all(&base);
}
