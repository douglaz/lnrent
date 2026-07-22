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
use zeroize::Zeroizing;

/// Wrap a test passphrase as the `Some(Zeroizing<String>)` the encrypted backup/restore path takes.
fn pass(s: &str) -> Option<Zeroizing<String>> {
    Some(Zeroizing::new(s.to_string()))
}

const FED_ID: &str = "fed11deadbeefcafe";

/// A stand-in for the lnrent-owned lnv2 idempotency index db (lnv2_index.db). Backup treats the whole
/// `fedimint/` subtree (including any `*_index.db`) as opaque bytes, so this test only needs SOME
/// populated sqlite db under the subtree to copy + reread — the exact table shape is immaterial.
const INDEX_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS lnv2_invoice (
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
CREATE TABLE IF NOT EXISTS lnv2_pay (
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
/// populated `lnv2_index.db`.
fn populate_fedimint_dir(data_dir: &std::path::Path) {
    let fed_dir = data_dir.join("fedimint").join(FED_ID);
    let client_db = fed_dir.join("client.db");
    fs::create_dir_all(&client_db).unwrap();
    fs::write(client_db.join("CURRENT"), b"rocksdb-sentinel\n").unwrap();

    let idx = Connection::open(fed_dir.join("lnv2_index.db")).unwrap();
    idx.execute_batch(INDEX_SCHEMA).unwrap();
    idx.execute(
        "INSERT INTO lnv2_invoice (external_id, operation_id, invoice_id, bolt11, payment_hash, amount_sat, expires_at, status)
         VALUES (?,?,?,?,?,?,?,?)",
        params!["ext-1", "op-1", "invid-1", "lnbc1example", "ph-1", 1234i64, 1_900_000_000i64, "PAID"],
    )
    .unwrap();
    idx.execute(
        "INSERT INTO lnv2_pay (idempotency_key, operation_id, backend_pay_id, status, pay_kind)
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
    let manifest = backup(&data_dir, &dest, None).unwrap();
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
    restore(&dest, &restored, false, None).unwrap();

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

    let idx = Connection::open(rfed.join("lnv2_index.db")).unwrap();
    let (inv_status, inv_amount): (String, i64) = idx
        .query_row(
            "SELECT status, amount_sat FROM lnv2_invoice WHERE external_id='ext-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((inv_status.as_str(), inv_amount), ("PAID", 1234));
    let (pay_status, pay_kind): (String, String) = idx
        .query_row(
            "SELECT status, pay_kind FROM lnv2_pay WHERE idempotency_key='refund:ext-1'",
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
    let err = restore(&dest, &restored, false, None).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not empty") || msg.contains("--force"),
        "expected a non-empty-target refusal, got: {err}"
    );
    // ...and SUCCEEDS with force.
    restore(&dest, &restored, true, None).unwrap();

    let _ = fs::remove_dir_all(&base);
}

// --- ENCRYPTED mode (lnrent-y4m.6): optional passphrase-encrypted backup ------------------------

const SEED_WORDS: &str =
    "leader monkey parrot ring guide accident before fence cannon height naive bean\n";

#[test]
fn encrypted_backup_restore_round_trip_reproduces_state_and_fedimint() {
    // Mirror of the plaintext round-trip, but WITH a passphrase: the sensitive set must live ONLY
    // inside `backup.age` (no plaintext seed/sqlite on disk in dest), yet decrypt+restore to a
    // byte/row-identical data dir.
    let base = temp_dir("enc-roundtrip");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();

    populate_state_db(&data_dir);
    populate_fedimint_dir(&data_dir);
    fs::write(
        data_dir.join("fedimint.json"),
        r#"{"invite":"fed11invite","gateway":"03gateway"}"#,
    )
    .unwrap();
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    // --- encrypted backup ---
    let dest = base.join("backup");
    let manifest = backup(&data_dir, &dest, pass("correct horse battery staple")).unwrap();
    assert!(manifest.encrypted, "manifest must record encrypted:true");
    assert!(manifest.state_db);
    assert!(manifest.fedimint_dir);
    assert!(manifest.fedimint_config);
    assert!(manifest.operator_seed);
    assert_eq!(manifest.federations, vec![FED_ID.to_string()]);

    // The dest holds ONLY the plaintext manifest + the single age artifact — NO plaintext secrets.
    assert!(dest.join("MANIFEST.json").is_file());
    assert!(dest.join("backup.age").is_file(), "the age artifact exists");
    assert_eq!(
        mode(&dest.join("backup.age")),
        0o600,
        "the age artifact must be owner-only"
    );
    assert!(
        !dest.join("lnrent.sqlite").exists(),
        "no plaintext state DB may sit in an encrypted backup dir"
    );
    assert!(
        !dest.join("operator.seed").exists(),
        "no plaintext seed may sit in an encrypted backup dir"
    );
    assert!(
        !dest.join("fedimint.json").exists() && !dest.join("fedimint").exists(),
        "no plaintext fedimint config/dir may sit in an encrypted backup dir"
    );
    // No leftover VACUUM scratch file leaked into dest.
    let scratch: Vec<_> = fs::read_dir(&dest)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".lnrent-backup-vacuum"))
        .collect();
    assert!(scratch.is_empty(), "vacuum scratch leaked: {scratch:?}");
    let source_scratch: Vec<_> = fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".lnrent-backup-vacuum"))
        .collect();
    assert!(
        source_scratch.is_empty(),
        "vacuum scratch was not cleaned from the trusted source: {source_scratch:?}"
    );

    // The plaintext MANIFEST carries only metadata flags — NO secret bytes (seed words / DB magic).
    let manifest_bytes = fs::read(dest.join("MANIFEST.json")).unwrap();
    let manifest_str = String::from_utf8(manifest_bytes).unwrap();
    assert!(
        !manifest_str.contains("leader monkey parrot"),
        "the plaintext manifest must not contain seed words"
    );
    assert!(
        !manifest_str.contains("SQLite format"),
        "the plaintext manifest must not contain DB bytes"
    );
    assert!(manifest_str.contains("\"encrypted\": true"));

    // --- restore WITH the same passphrase into a FRESH data dir ---
    let restored = base.join("restored");
    let rm = restore(
        &dest,
        &restored,
        false,
        pass("correct horse battery staple"),
    )
    .unwrap();
    assert!(rm.encrypted);

    // --- the lnrent state rows reproduce EXACTLY ---
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
    // Verify the SAME full state-DB fidelity as the plaintext round-trip (CodeRabbit): the crypto path
    // must round-trip every table, not a subset.
    let (inv_ext, inv_status2, inv_amt): (String, String, i64) = conn
        .query_row(
            "SELECT external_id, status, amount_sat FROM invoice WHERE id='inv1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((inv_ext.as_str(), inv_status2.as_str(), inv_amt), ("ext-1", "PAID", 1234));
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
    assert_eq!(
        mode(&sentinel),
        0o600,
        "restored ecash file must be owner-only"
    );
    let idx = Connection::open(rfed.join("lnv2_index.db")).unwrap();
    let (inv_status, inv_amount): (String, i64) = idx
        .query_row(
            "SELECT status, amount_sat FROM lnv2_invoice WHERE external_id='ext-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((inv_status.as_str(), inv_amount), ("PAID", 1234));
    let (pay_status, pay_kind): (String, String) = idx
        .query_row(
            "SELECT status, pay_kind FROM lnv2_pay WHERE idempotency_key='refund:ext-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((pay_status.as_str(), pay_kind.as_str()), ("SENT", "ln"));
    drop(idx);

    // --- config + seed reproduce byte-for-byte ---
    assert_eq!(
        fs::read_to_string(restored.join("fedimint.json")).unwrap(),
        r#"{"invite":"fed11invite","gateway":"03gateway"}"#
    );
    assert_eq!(fs::read_to_string(restored.join("operator.seed")).unwrap(), SEED_WORDS);
    assert_eq!(mode(&restored.join("operator.seed")), 0o600);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn encrypted_restore_with_wrong_passphrase_leaves_target_untouched() {
    let base = temp_dir("enc-wrongpass");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    let dest = base.join("backup");
    backup(&data_dir, &dest, pass("the-real-passphrase")).unwrap();

    // (a) A wrong passphrase into a FRESH (absent) target: clean Err, and the target is never created.
    let fresh = base.join("fresh-target");
    let err = restore(&dest, &fresh, false, pass("wrong-passphrase")).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("decrypt"),
        "expected a decrypt failure, got: {err}"
    );
    assert!(
        !fresh.exists(),
        "a failed decrypt must not create the restore target"
    );
    // No staging scratch leaked in the parent.
    let leaked: Vec<_> = fs::read_dir(&base)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".lnrent-restore-"))
        .collect();
    assert!(leaked.is_empty(), "restore scratch leaked: {leaked:?}");

    // (b) A wrong passphrase with --force over a POPULATED target: the target must be byte-UNCHANGED
    //     (the swap only runs on full decrypt success, so a bad passphrase never touches live state).
    let populated = base.join("populated");
    fs::create_dir_all(&populated).unwrap();
    fs::write(populated.join("sentinel.txt"), b"ORIGINAL-LIVE-STATE\n").unwrap();
    let err = restore(&dest, &populated, true, pass("still-wrong")).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("decrypt"),
        "expected a decrypt failure, got: {err}"
    );
    assert_eq!(
        fs::read_to_string(populated.join("sentinel.txt")).unwrap(),
        "ORIGINAL-LIVE-STATE\n",
        "a wrong-passphrase --force restore must leave the live target byte-unchanged"
    );
    assert!(
        !populated.join("lnrent.sqlite").exists(),
        "no restored file may leak into the target on a failed decrypt"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn encrypted_restore_rejects_a_truncated_ciphertext() {
    // A truncated `backup.age` (tail removed) must be REJECTED by age's AEAD integrity, never silently
    // accepted as a complete restore — even with the CORRECT passphrase. `tar::unpack` stops at the
    // tar terminator, so restore drains the age reader to EOF to authenticate the final chunk
    // (adversarial codex). The failure happens inside staging, so the target is left untouched.
    let base = temp_dir("enc-truncated");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    let dest = base.join("backup");
    backup(&data_dir, &dest, pass("the-real-passphrase")).unwrap();

    // Lop the last 16 bytes off the encrypted artifact so its final age STREAM chunk is incomplete.
    let age_path = dest.join("backup.age");
    let len = fs::metadata(&age_path).unwrap().len();
    assert!(len > 16, "artifact should be larger than the truncation");
    let f = fs::OpenOptions::new().write(true).open(&age_path).unwrap();
    f.set_len(len - 16).unwrap();
    drop(f);

    // Correct passphrase, but the ciphertext no longer authenticates -> restore fails, target untouched.
    let fresh = base.join("fresh-target");
    let err = restore(&dest, &fresh, false, pass("the-real-passphrase")).unwrap_err();
    assert!(
        !fresh.join("lnrent.sqlite").exists() && !fresh.join("operator.seed").exists(),
        "a truncated ciphertext must not produce a partial restore; err was: {err}"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn encrypted_restore_without_passphrase_is_a_clear_error() {
    let base = temp_dir("enc-nopass");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);

    let dest = base.join("backup");
    backup(&data_dir, &dest, pass("some-passphrase")).unwrap();

    // Restore an ENCRYPTED backup with NO passphrase -> a clear error that says it's encrypted.
    let restored = base.join("restored");
    let err = restore(&dest, &restored, false, None).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("encrypted") && msg.contains("passphrase"),
        "expected an 'encrypted; pass --passphrase-file' error, got: {err}"
    );
    assert!(
        !restored.join("lnrent.sqlite").exists(),
        "the target must be untouched when the passphrase is missing"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn encrypted_restore_refuses_plaintext_manifest_downgrade() {
    let base = temp_dir("enc-downgrade");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    let dest = base.join("backup");
    backup(&data_dir, &dest, pass("the-real-passphrase")).unwrap();

    // Simulate tamperable media: flip only the unauthenticated routing bit and add a plausible
    // plaintext set. Restore must not silently ignore the operator's passphrase and install it.
    let manifest_path = dest.join("MANIFEST.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["encrypted"] = serde_json::Value::Bool(false);
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    fs::copy(data_dir.join("lnrent.sqlite"), dest.join("lnrent.sqlite")).unwrap();
    fs::copy(data_dir.join("operator.seed"), dest.join("operator.seed")).unwrap();

    let with_passphrase = base.join("with-passphrase");
    let err = restore(
        &dest,
        &with_passphrase,
        false,
        pass("the-real-passphrase"),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("refusing a possible encrypted-backup downgrade"),
        "expected downgrade refusal, got: {err}"
    );
    assert!(!with_passphrase.exists(), "downgrade must not touch target");

    // Even without a supplied passphrase, the remaining age artifact contradicts the plaintext bit
    // and must prevent plaintext routing.
    let without_passphrase = base.join("without-passphrase");
    let err = restore(&dest, &without_passphrase, false, None).unwrap_err();
    assert!(
        err.to_string().contains("backup.age is present"),
        "expected age-artifact downgrade refusal, got: {err}"
    );
    assert!(!without_passphrase.exists(), "downgrade must not touch target");

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn encrypted_backup_refuses_a_symlinked_fedimint_root() {
    // `Path::is_dir()` FOLLOWS a symlink, so a symlinked `fedimint` root would make the encrypted tar
    // walk escape the data dir and archive bytes from OUTSIDE the captured set. The root must be
    // refused just like a symlinked seed/config already is (review R1 P2).
    let base = temp_dir("enc-fedsymlink");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    // A real fedimint subtree living OUTSIDE the data dir, reachable only through a symlink at the root.
    let outside = base.join("outside-fedimint");
    fs::create_dir_all(outside.join(FED_ID)).unwrap();
    fs::write(outside.join(FED_ID).join("SECRET"), b"external-ecash\n").unwrap();
    std::os::unix::fs::symlink(&outside, data_dir.join("fedimint")).unwrap();

    let dest = base.join("backup");
    let err = backup(&data_dir, &dest, pass("a-real-passphrase")).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("symlink"),
        "expected a symlinked-fedimint-root refusal, got: {err}"
    );
    // Nothing was archived — no age artifact leaked into dest.
    assert!(
        !dest.join("backup.age").exists(),
        "a refused symlink root must not produce an age artifact"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_with_empty_passphrase_is_refused() {
    // The public `backup`/`restore` APIs are the fund-safety boundary: an empty OR whitespace-only
    // passphrase must be refused (review R1 P2 + adversarial codex) rather than producing an artifact
    // stamped `encrypted:true` yet protected by ~nothing. The backup guard fires before dest is created.
    let base = temp_dir("empty-pass");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);

    let dest = base.join("backup");
    for pw in ["", "   ", "\t "] {
        let err = backup(&data_dir, &dest, pass(pw)).unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("passphrase"),
            "expected a passphrase refusal for {pw:?}, got: {err}"
        );
        assert!(
            !dest.exists(),
            "a refused-passphrase backup must not create the dest dir ({pw:?})"
        );
    }

    // The restore API rejects a whitespace-only passphrase at ITS boundary too.
    let real = base.join("real-backup");
    backup(&data_dir, &real, pass("a-real-passphrase")).unwrap();
    let err = restore(&real, &base.join("t"), false, pass("   ")).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("passphrase"),
        "restore must refuse a whitespace-only passphrase, got: {err}"
    );

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn plaintext_backup_layout_is_unchanged_by_the_encrypted_feature() {
    // The DEFAULT (no-passphrase) path must still produce today's flat-file layout: the secret files
    // land as plaintext in dest, there is NO backup.age, and the manifest records encrypted:false.
    let base = temp_dir("plain-unchanged");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);
    populate_fedimint_dir(&data_dir);
    fs::write(data_dir.join("operator.seed"), SEED_WORDS).unwrap();

    let dest = base.join("backup");
    let manifest = backup(&data_dir, &dest, None).unwrap();
    assert!(!manifest.encrypted, "plaintext backup -> encrypted:false");
    assert!(
        dest.join("lnrent.sqlite").is_file(),
        "plaintext layout keeps the flat state DB"
    );
    assert!(dest.join("operator.seed").is_file());
    assert!(dest.join("fedimint").join(FED_ID).is_dir());
    assert!(
        !dest.join("backup.age").exists(),
        "the plaintext path must NOT emit an age artifact"
    );
    // The plaintext manifest OMITS the `encrypted` field entirely (skip_serializing_if), so it is
    // byte-for-byte identical to a pre-y4m.6 manifest — the encrypted feature leaves the plaintext
    // path unchanged (adversarial codex #8). Only an encrypted backup writes `"encrypted": true`.
    let manifest_str = fs::read_to_string(dest.join("MANIFEST.json")).unwrap();
    assert!(
        !manifest_str.contains("encrypted"),
        "plaintext manifest must omit the encrypted field (byte-identical to pre-y4m.6), got: {manifest_str}"
    );

    // ...and it restores identically through the plaintext path.
    let restored = base.join("restored");
    restore(&dest, &restored, false, None).unwrap();
    assert_eq!(fs::read_to_string(restored.join("operator.seed")).unwrap(), SEED_WORDS);
    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM subscription", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn old_v1_manifest_without_encrypted_field_still_restores() {
    // A backup written by a pre-y4m.6 build has a v1 manifest with NO `encrypted` field. The
    // `#[serde(default)]` must let it deserialize (as plaintext) so those backups stay restorable —
    // otherwise the field would silently strand every existing operator backup (a data-loss bug).
    let base = temp_dir("old-manifest");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir);

    let dest = base.join("backup");
    backup(&data_dir, &dest, None).unwrap();

    // Rewrite the manifest to drop the `encrypted` key, simulating an old v1 plaintext manifest.
    let manifest_path = dest.join("MANIFEST.json");
    let mut v: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
    v.as_object_mut().unwrap().remove("encrypted");
    assert!(
        v.get("encrypted").is_none(),
        "the test fixture must have no encrypted field"
    );
    fs::write(&manifest_path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    // Restore still works via the plaintext path (encrypted defaults to false).
    let restored = base.join("restored");
    let m = restore(&dest, &restored, false, None).unwrap();
    assert!(!m.encrypted, "a field-less manifest defaults to plaintext");
    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM subscription", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    drop(conn);

    let _ = fs::remove_dir_all(&base);
}

#[test]
fn backup_without_fedimint_or_seed_is_not_an_error() {
    let base = temp_dir("nofed");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    populate_state_db(&data_dir); // state DB only — no fedimint dir / config / seed

    let dest = base.join("backup");
    let manifest = backup(&data_dir, &dest, None).unwrap();
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
    let m = restore(&dest, &restored, false, None).unwrap();
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
    let m = backup(&src_data, &dest, None).unwrap();
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
    restore(&dest, &target, true, None).unwrap();

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
    let err = backup(&data_dir, &base.join("backup"), None).unwrap_err();
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
    backup(&data_dir, &dest, None).unwrap();
    // Corrupt the set: delete a file the manifest says is present. Restore must NOT silently drop it.
    fs::remove_file(dest.join("operator.seed")).unwrap();

    let err = restore(&dest, &base.join("restored"), false, None).unwrap_err();
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
    // The fedimint subtree carries ecash-bearing material (client.db, lnv2_index.db); both backup
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
    set_mode(&src_fed.join("lnv2_index.db"), 0o644);

    let dest = base.join("backup");
    backup(&data_dir, &dest, None).unwrap();
    let bk_fed = dest.join("fedimint").join(FED_ID);
    assert_eq!(
        mode(&bk_fed.join("client.db").join("CURRENT")),
        0o600,
        "backed-up rocksdb file must be owner-only"
    );
    assert_eq!(
        mode(&bk_fed.join("lnv2_index.db")),
        0o600,
        "backed-up lnrent index must be owner-only"
    );

    let restored = base.join("restored");
    restore(&dest, &restored, false, None).unwrap();
    let r_fed = restored.join("fedimint").join(FED_ID);
    assert_eq!(
        mode(&r_fed.join("client.db").join("CURRENT")),
        0o600,
        "restored rocksdb file must be owner-only"
    );
    assert_eq!(
        mode(&r_fed.join("lnv2_index.db")),
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
    let err = backup(&data_dir, &inside, None).unwrap_err();
    assert!(
        err.to_string().contains("overlap"),
        "expected an overlap refusal for a nested dest, got: {err}"
    );
    // The data dir was not clobbered (its real fedimint subtree survives).
    assert!(data_dir.join("fedimint").join(FED_ID).is_dir());

    let err2 = backup(&data_dir, &data_dir, None).unwrap_err();
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
    backup(&data_dir, &dest, None).unwrap();

    let err = restore(&dest, &dest.join("restored-here"), false, None).unwrap_err();
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
fn cli_encrypted_backup_then_restore_round_trip_json() {
    // The `--passphrase-file` mode over the CLI: encrypted backup -> `encrypted:true` in JSON, a
    // single `backup.age` and NO plaintext sqlite in dest; restore with the same file round-trips;
    // a WRONG passphrase file fails with the `restore_failed` envelope.
    let base = temp_dir("cli-enc");
    let data_dir = base.join("data");
    fs::create_dir_all(&data_dir).unwrap();
    {
        let conn = store::open(data_dir.join("lnrent.sqlite")).unwrap();
        conn.execute(
            "INSERT INTO subscription (id, state, paid_through) VALUES ('cli-enc','ACTIVE',99)",
            [],
        )
        .unwrap();
    }
    // The passphrase lives in a FILE (never argv). A trailing newline is trimmed by the daemon.
    let pass_file = base.join("pass.txt");
    fs::write(&pass_file, "operator-cli-secret\n").unwrap();

    // --- encrypted backup ---
    let dest = base.join("bk");
    let out = lnrentd()
        .args([
            "backup",
            "--json",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--dest",
            dest.to_str().unwrap(),
            "--passphrase-file",
            pass_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "encrypted backup exit; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["encrypted"], true);
    assert!(dest.join("backup.age").is_file());
    assert!(
        !dest.join("lnrent.sqlite").exists(),
        "no plaintext state DB in an encrypted backup dir"
    );

    // --- restore with the SAME passphrase file ---
    let restored = base.join("restored");
    let out = lnrentd()
        .args([
            "restore",
            "--json",
            "--data-dir",
            restored.to_str().unwrap(),
            "--from",
            dest.to_str().unwrap(),
            "--passphrase-file",
            pass_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "encrypted restore exit; stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["data"]["encrypted"], true);
    let conn = store::open(restored.join("lnrent.sqlite")).unwrap();
    let pt: i64 = conn
        .query_row(
            "SELECT paid_through FROM subscription WHERE id='cli-enc'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pt, 99);
    drop(conn);

    // --- a WRONG passphrase file: nonzero exit + structured restore_failed, target untouched ---
    let wrong_file = base.join("wrong.txt");
    fs::write(&wrong_file, "not-the-passphrase\n").unwrap();
    let fresh = base.join("fresh");
    let out = lnrentd()
        .args([
            "restore",
            "--json",
            "--data-dir",
            fresh.to_str().unwrap(),
            "--from",
            dest.to_str().unwrap(),
            "--passphrase-file",
            wrong_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "a wrong passphrase file must fail restore"
    );
    assert!(out.stdout.is_empty(), "json errors go to stderr, not stdout");
    let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "restore_failed");
    assert!(
        !fresh.join("lnrent.sqlite").exists(),
        "a failed decrypt must not write the target"
    );

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
