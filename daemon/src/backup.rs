//! COLD / OFFLINE operator backup + restore of the daemon's durable state (lnrent-7fp.14 PART A;
//! SPEC.md §4.6/§11/§12, ADR-0004/0012/0015).
//!
//! This is the **cold** path: it runs while the DAEMON IS STOPPED (a standalone CLI), so there are
//! no racing writers and a plain directory copy of the closed stores is consistent. It deliberately
//! does NOT do online/litestream-style continuous replication, rocksdb checkpoint APIs, or open the
//! fedimint client — the live "ecash-spendable-after-restore" proof is a separate follow-up (PART B).
//!
//! The data-dir layout it captures (see `config.rs` for the data dir + sqlite path, and
//! `fedimint_paths.rs` for the fedimint paths):
//!
//! - `<data_dir>/lnrent.sqlite`             — the lnrent state DB (WAL mode): subscriptions /
//!   invoices / paid_through / reservations / the refund ledger / outbox / op_invocation.
//! - `<data_dir>/fedimint.json`             — the federation invite/config (iff fedimint configured).
//! - `<data_dir>/operator.seed`             — the BIP39 seed (iff present).
//! - `<data_dir>/fedimint/<federation_id>/` — the fedimint client RocksDB (`client.db/`) AND the
//!   lnrent-owned `lnv2_index.db` (the `lnv2_invoice` / `lnv2_pay` idempotency index).
//!
//! The sqlite is captured with `VACUUM INTO` — a single coherent artifact that folds in the WAL, so
//! the `-wal`/`-shm` sidecars are never raw-copied. The fedimint directory + the config + the seed
//! are copied as opaque bytes (cold copy is safe since the daemon is stopped). A `MANIFEST.json`
//! records exactly which files were present, so restore can sanity-check the set and surface a clear
//! error if it is incomplete/corrupt rather than silently dropping a commitment-bearing file.
//!
//! ## Optional passphrase-encrypted mode (lnrent-y4m.6)
//!
//! The default path above writes the FUND-CONTROLLING secrets (the BIP39 `operator.seed`, the ecash-
//! bearing `fedimint/` dir + `fedimint.json`, and the state DB) in PLAINTEXT — fine for a cold,
//! operator-controlled data dir, but a backup exists to be MOVED (USB stick, cloud bucket), and at
//! rest those bytes are a sweepable seed to anyone who gets the media. So `backup`/`restore` take an
//! OPTIONAL passphrase: when present, the sensitive set is bundled into ONE `tar` stream that is fed
//! through the audited `age` crate (scrypt passphrase KDF + ChaCha20-Poly1305 AEAD; we NEVER hand-roll
//! crypto) and written as a single `dest/backup.age`. The `MANIFEST.json` stays PLAINTEXT (metadata
//! only — no secret bytes) so restore reads it first and routes on `Manifest.encrypted`. Because that
//! routing flag is not authenticated, restore also refuses the plaintext path when a passphrase or
//! `backup.age` is present: tampering cannot silently downgrade an encrypted restore into an
//! unauthenticated plaintext restore. (That guard covers SAME-media tampering — a flipped
//! `encrypted` bit or a lingering `backup.age`. A WHOLLY substituted plaintext backup — attacker-
//! swapped media carrying its own `encrypted: false` manifest and NO `backup.age`, restored WITHOUT a
//! passphrase — is inherent to unauthenticated plaintext backups and out of scope; the operator rule
//! is therefore to ALWAYS restore an encrypted backup WITH its passphrase, never let a runbook fall
//! back to the plaintext path.) A wrong passphrase or any ciphertext tamper fails cleanly on
//! the AEAD tag; because restore decrypts+untars INSIDE the transactional staging dir, the atomic
//! swap runs only on FULL success, so a bad passphrase leaves the target data dir UNTOUCHED. The
//! default (no-passphrase) backup path is byte-for-byte unchanged.

use std::fs;
use std::io::BufReader;
use std::iter;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

/// The data-dir state DB (matches `config.rs`'s `STATE_DB_FILE`).
const STATE_DB_FILE: &str = "lnrent.sqlite";
/// The BIP39 seed file (matches `config.rs`'s `SEED_FILE`).
const SEED_FILE: &str = "operator.seed";
/// The Fedimint join config (matches `config.rs`'s `FEDIMINT_CONFIG_FILE`).
const FEDIMINT_CONFIG_FILE: &str = "fedimint.json";
/// The per-federation fedimint subtree (matches `fedimint_paths.rs`'s `data_dir/fedimint/<id>/`).
const FEDIMINT_DIR: &str = "fedimint";
/// The daemon's IPC socket (matches `main.rs`'s `data_dir.join("lnrent.sock")`); a *live* socket is
/// the cheap "is a daemon running against this data dir?" tell.
const IPC_SOCK_FILE: &str = "lnrent.sock";
/// The backup self-description, written/read by [`backup`]/[`restore`].
const MANIFEST_FILE: &str = "MANIFEST.json";
/// The single age-encrypted `tar` artifact in an ENCRYPTED backup dir (lnrent-y4m.6): it holds the
/// sqlite snapshot + `operator.seed` + `fedimint.json` + the `fedimint/` subtree. In encrypted mode
/// `dest/` contains ONLY this file plus the plaintext [`MANIFEST_FILE`]; the secret files never touch
/// disk in `dest`.
const BACKUP_AGE_FILE: &str = "backup.age";
/// The manifest `schema` stamp — restore refuses anything that is not an lnrent backup.
const BACKUP_SCHEMA: &str = "lnrent-backup";
/// The backup-format version this build writes and understands. Bump on a breaking layout change.
const BACKUP_FORMAT_VERSION: u32 = 1;

/// The backup self-description (`MANIFEST.json`). Restore reads this FIRST and uses it to verify the
/// set is complete — every artifact recorded `true` here MUST be present in the backup dir, else the
/// backup is incomplete/corrupt and restore refuses (a money/durability path must never silently
/// drop a commitment the operator made).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Always [`BACKUP_SCHEMA`]; restore rejects a foreign/corrupt manifest.
    pub schema: String,
    /// The backup-format version ([`BACKUP_FORMAT_VERSION`]); restore rejects an unknown version.
    pub version: u32,
    /// Wall-clock seconds since the unix epoch when the backup was taken (audit only).
    pub created_unix: u64,
    /// The lnrent state DB — always backed up (its absence is a hard error in [`backup`]).
    pub state_db: bool,
    /// `fedimint.json` was present and captured.
    pub fedimint_config: bool,
    /// `operator.seed` was present and captured.
    pub operator_seed: bool,
    /// The `fedimint/` subtree was present and captured.
    pub fedimint_dir: bool,
    /// The federation ids (the `fedimint/<id>/` subdir names) captured, sorted for determinism.
    pub federations: Vec<String>,
    /// `true` iff the sensitive set was written to [`BACKUP_AGE_FILE`] under a passphrase instead of
    /// as plaintext files (lnrent-y4m.6). `#[serde(default)]` keeps OLD v1 plaintext manifests (which
    /// lack the field) deserializing as `false`, so the format version stays 1 — the manifest itself
    /// carries NO secret bytes either way, only this descriptive flag. `skip_serializing_if` OMITS the
    /// field when `false`, so a PLAINTEXT manifest is byte-for-byte identical to a pre-y4m.6 one
    /// (adversarial codex — the plaintext path must be unchanged); only an encrypted backup writes it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub encrypted: bool,
}

/// `skip_serializing_if` predicate for the `encrypted` flag (omit it when `false`).
fn is_false(b: &bool) -> bool {
    !*b
}

/// Cheap, best-effort liveness check: a daemon is *running against* `data_dir` iff its IPC socket
/// accepts a connection. Used by the CLI to refuse an online backup. The result is interpreted
/// fail-SAFE — only these outcomes are read as "not running":
/// - `NotFound` (no socket file at all),
/// - `ConnectionRefused` (a stale socket FILE left by a crashed daemon, with no listener), and
/// - `InvalidInput` (the path cannot form a valid socket address at all — e.g. it exceeds
///   `sun_path`'s ~108-byte limit; a daemon could never have *bound* such a path either, so none can
///   be live there — review R2 P3).
///
/// `InvalidInput` (an overlong / malformed socket path) also reads as "not running": the daemon binds
/// the SAME `<data_dir>/lnrent.sock` this probes (`main.rs` builds the socket from the resolved
/// `data_dir` with no canonicalization, and `backup`/`restore` resolve the identical `data_dir`), so a
/// path the probe cannot form is one the daemon could not have bound either — no daemon can be live on
/// it. There is no daemon-on-a-normalized-different-path mismatch to fail open on (review R3 P2). Any
/// *other* connect error (notably `PermissionDenied` — a live daemon whose socket the caller cannot
/// reach) IS ambiguous, so we report "running" and let the CLI refuse rather than risk copying open
/// stores. TOCTOU between this check and the copy is inherent; it is a guard, not a lock — the
/// operator is still responsible for stopping the daemon.
pub fn daemon_appears_running(data_dir: &Path) -> bool {
    use std::io::ErrorKind;
    match std::os::unix::net::UnixStream::connect(data_dir.join(IPC_SOCK_FILE)) {
        Ok(_) => true, // a listener accepted the connection -> a daemon is live
        Err(e) => !matches!(
            e.kind(),
            ErrorKind::NotFound | ErrorKind::ConnectionRefused | ErrorKind::InvalidInput
        ),
    }
}

/// COLD backup of the stopped daemon's `data_dir` into a fresh `dest` directory. Returns the written
/// [`Manifest`]. The caller (CLI) is responsible for first verifying no daemon is running
/// ([`daemon_appears_running`]); this function assumes the stores are closed.
///
/// `dest` must be empty or not-yet-exist — a backup is a clean, self-contained set, so we never mix
/// it with stale artifacts (and `VACUUM INTO` itself refuses to overwrite an existing file).
///
/// `passphrase` selects the mode (lnrent-y4m.6):
/// - `None` → today's PLAINTEXT layout (flat secret files + `MANIFEST.json`), byte-for-byte unchanged.
/// - `Some(pw)` → the sensitive set is tar'd + age-encrypted into `dest/backup.age`; the secret files
///   never touch disk in `dest`, and the plaintext `MANIFEST.json` records `encrypted: true`. An
///   EMPTY `pw` is REFUSED here — age would encrypt under it and stamp `encrypted: true`, yet the
///   fund-controlling seed would be protected by a trivially guessable secret. The CLI already
///   rejects an empty passphrase FILE, but this public API is the real fund-safety boundary (a non-CLI
///   caller could hand us `Some("")`), so the invariant is enforced here too (review R1 P2).
pub fn backup(
    data_dir: &Path,
    dest: &Path,
    passphrase: Option<Zeroizing<String>>,
) -> Result<Manifest> {
    let src_db = data_dir.join(STATE_DB_FILE);
    if !is_regular_file(&src_db) {
        bail!(
            "no state DB at {} — is the data dir correct? (cold backup must run against a real, \
             stopped daemon data dir)",
            src_db.display()
        );
    }

    // Refuse an empty OR whitespace-only passphrase BEFORE creating anything: it selects encrypted
    // mode but gives ~zero protection to the fund-controlling seed the mode exists to protect (review
    // R1 P2 + adversarial codex — `"   "` must be rejected too, at this fund-safety boundary).
    if passphrase.as_ref().is_some_and(|pw| pw.trim().is_empty()) {
        bail!(
            "refusing to create an encrypted backup with an empty/whitespace-only passphrase — it \
             offers no protection for the seed + ecash; supply a real passphrase"
        );
    }

    // A `dest` INSIDE the data dir would make the fedimint copy below recurse into its own freshly
    // written output (`<dest>/fedimint/<dest-rel>/fedimint/...`) until the disk fills, and a
    // `data_dir` inside `dest` is an equally nonsensical self-overlap. Reject either BEFORE creating
    // anything (review R1 P2). This guards BOTH the plaintext copy and the encrypted tar walk.
    assert_disjoint(data_dir, dest, "data dir", "backup dest")?;

    prepare_empty_dest(dest)?;
    // A newly-created `dest` directory entry is only crash-durable after fsyncing its PARENT (review
    // R3 P2); without this a crash after "backup complete" could lose the whole backup dir despite the
    // per-file + manifest-last fsync ordering. Harmless when `dest` already existed.
    fsync_dir(&parent_dir(dest))?;

    match passphrase {
        None => backup_plaintext(data_dir, dest, &src_db),
        Some(pw) => backup_encrypted(data_dir, dest, &src_db, &pw),
    }
}

/// The default PLAINTEXT backup body (unchanged from lnrent-7fp.14): the secret files land flat in
/// `dest`, each hardened + fsynced, with the manifest written LAST.
fn backup_plaintext(data_dir: &Path, dest: &Path, src_db: &Path) -> Result<Manifest> {
    // 1. The state DB: VACUUM INTO folds the WAL into ONE coherent file and never emits -wal/-shm
    //    sidecars, so the artifact is internally consistent without raw-copying journal state.
    let dest_db = dest.join(STATE_DB_FILE);
    vacuum_into(src_db, &dest_db)?;
    // The state DB can carry credential-bearing rows (outbox payloads, native session tickets);
    // VACUUM INTO writes it at the process umask, so tighten it to owner-only like the seed file.
    harden_file_0600(&dest_db)?;
    fsync_file(&dest_db)?;

    // 2. The fedimint subtree (client.db rocksdb + lnv2_index.db), copied as opaque bytes.
    let src_fed = data_dir.join(FEDIMINT_DIR);
    let fedimint_dir = src_fed.is_dir();
    let mut federations = Vec::new();
    if fedimint_dir {
        let dest_fed = dest.join(FEDIMINT_DIR);
        copy_dir_recursive(&src_fed, &dest_fed)?;
        federations = list_subdirs(&dest_fed)?;
    }

    // 3. The federation config + the seed — absent (not an error) when fedimint isn't configured.
    let fedimint_config = copy_if_present(
        &data_dir.join(FEDIMINT_CONFIG_FILE),
        &dest.join(FEDIMINT_CONFIG_FILE),
    )?;
    let operator_seed = copy_if_present(&data_dir.join(SEED_FILE), &dest.join(SEED_FILE))?;

    // 4. Make every DATA file's directory entry durable BEFORE the manifest. With this fsync ordering
    //    the manifest is the last entry to hit disk, so its presence on recovery truly implies a
    //    complete, durable set (rather than a torn one a crash could leave behind).
    fsync_dir(dest)?;

    // 5. The manifest — written + fsynced LAST, so its presence also marks the backup as complete.
    let manifest = Manifest {
        schema: BACKUP_SCHEMA.to_string(),
        version: BACKUP_FORMAT_VERSION,
        created_unix: now_unix(),
        state_db: true,
        fedimint_config,
        operator_seed,
        fedimint_dir,
        federations,
        encrypted: false,
    };
    write_manifest(&dest.join(MANIFEST_FILE), &manifest)?;
    // Re-fsync the dest dir so the manifest's own directory entry is durable strictly after the data.
    fsync_dir(dest)?;
    Ok(manifest)
}

/// The OPTIONAL passphrase-encrypted backup body (lnrent-y4m.6): VACUUM the state DB into a private
/// temp, tar {snapshot, seed, config, fedimint subtree} through the `age` passphrase writer into ONE
/// `dest/backup.age`, then write the plaintext manifest LAST. The secret files never land in `dest`;
/// the only bytes at rest there are the AEAD ciphertext + the metadata-only manifest.
fn backup_encrypted(
    data_dir: &Path,
    dest: &Path,
    src_db: &Path,
    passphrase: &Zeroizing<String>,
) -> Result<Manifest> {
    // Determine the present set WITHOUT copying (the tar walk below reads the sources directly). Same
    // symlink-refusal on the config/seed as the plaintext `copy_if_present`.
    let src_fed = data_dir.join(FEDIMINT_DIR);
    // `Path::is_dir()` FOLLOWS a symlink, so a symlinked `fedimint` ROOT would let the tar walk escape
    // the data dir and pull bytes from OUTSIDE the captured set — the entry-level guard in
    // `tar_append_dir_recursive` only refuses symlinks INSIDE the tree. Apply the module's
    // symlink-refusal invariant to the root too, before walking it (review R1 P2).
    let fedimint_dir = fedimint_root_is_real_dir(&src_fed)?;
    let federations = if fedimint_dir {
        list_subdirs(&src_fed)?
    } else {
        Vec::new()
    };
    let fedimint_config = present_regular_or_reject(&data_dir.join(FEDIMINT_CONFIG_FILE))?;
    let operator_seed = present_regular_or_reject(&data_dir.join(SEED_FILE))?;

    // VACUUM the state DB into a PRIVATE scratch file on the already-trusted SOURCE filesystem,
    // tar+encrypt it with the rest, then delete it — win OR lose. It must never touch the portable
    // destination: unlinking a plaintext file there would still leave recoverable blocks and gives
    // sync tooling/crashes a window to observe it (review round 1 P1).
    let vacuum_tmp = data_dir.join(format!(
        ".lnrent-backup-vacuum-{}-{}.tmp",
        std::process::id(),
        now_nanos()
    ));
    let age_path = dest.join(BACKUP_AGE_FILE);
    let bundled = (|| -> Result<()> {
        vacuum_into(src_db, &vacuum_tmp)?;
        harden_file_0600(&vacuum_tmp)?;
        write_encrypted_bundle(
            &age_path,
            &vacuum_tmp,
            data_dir,
            fedimint_dir,
            fedimint_config,
            operator_seed,
            passphrase,
        )
    })();
    // Always remove the plaintext snapshot, whether the bundle succeeded or failed, and make the
    // unlink durable. A cleanup failure is a backup failure: reporting success while a fund-bearing
    // plaintext scratch file remains would violate encrypted-at-rest mode.
    remove_file_if_exists(&vacuum_tmp).context("removing plaintext encrypted-backup scratch file")?;
    fsync_dir(data_dir).context("making encrypted-backup scratch cleanup durable")?;
    bundled?;

    // The AEAD artifact is at least as sensitive as the seed — tighten it to owner-only and fsync.
    harden_file_0600(&age_path)?;
    fsync_file(&age_path)?;
    // Make the artifact's directory entry durable BEFORE the manifest (manifest-last ordering).
    fsync_dir(dest)?;

    let manifest = Manifest {
        schema: BACKUP_SCHEMA.to_string(),
        version: BACKUP_FORMAT_VERSION,
        created_unix: now_unix(),
        state_db: true,
        fedimint_config,
        operator_seed,
        fedimint_dir,
        federations,
        encrypted: true,
    };
    write_manifest(&dest.join(MANIFEST_FILE), &manifest)?;
    fsync_dir(dest)?;
    Ok(manifest)
}

/// Restore a backup produced by [`backup`] from `src` INTO `data_dir`, returning the [`Manifest`]
/// describing what was restored.
///
/// Refuses to clobber a non-empty `data_dir` unless `force` is set (the CLI maps `--force` here); the
/// default is a fresh/empty target. The whole backup set is validated against the manifest BEFORE any
/// file is written, so an incomplete/corrupt backup fails before touching the target rather than
/// half-restoring the operator's commitments.
///
/// The install is staged-then-swapped: the full set is assembled (and fsynced) in a sibling staging
/// directory, then installed. This gives two guarantees the money path needs:
/// - `--force` makes the target EQUAL the backup — a stale `fedimint/` dir, `operator.seed`, or
///   `fedimint.json` the old target had but the backup does NOT is dropped, never unioned in (a
///   state-DB ↔ seed ↔ fedimint mismatch could mis-derive refund keys or strand ecash).
/// - the swap is ATOMIC (a single `renameat2(RENAME_EXCHANGE)` on Linux, with a guarded
///   move-aside fallback on filesystems that lack it), so a crash mid-restore leaves either the prior
///   data dir or the complete new one — never a half-written merge, and never a momentarily-ABSENT
///   data dir that a restarting daemon could mistake for a first boot and overwrite with a fresh
///   identity (review R1 P1).
///
/// `passphrase` (lnrent-y4m.6): required when `manifest.encrypted`; an encrypted backup with no
/// passphrase is rejected up front with a clear message. Conversely, supplying one for a manifest
/// that claims plaintext is rejected rather than silently ignoring the operator's authentication
/// intent. A wrong passphrase fails on the AEAD tag INSIDE staging, so the swap never runs and the
/// target data dir is left UNTOUCHED.
pub fn restore(
    src: &Path,
    data_dir: &Path,
    force: bool,
    passphrase: Option<Zeroizing<String>>,
) -> Result<Manifest> {
    // 0. The backup source and the restore target must be DISJOINT: restoring INTO a subdir of the
    //    backup (or backing the target up into the source) would let the staged swap move part of the
    //    source out from under the copy. Reject the overlap up front (mirror of the backup-side
    //    guard, review R1 P2).
    assert_disjoint(src, data_dir, "restore source", "data dir")?;

    // Reject a whitespace-only passphrase at THIS API boundary too (adversarial codex): it is not a
    // real passphrase, and letting it through would only surface later as a confusing wrong-passphrase
    // decrypt error.
    if passphrase.as_ref().is_some_and(|pw| pw.trim().is_empty()) {
        bail!("refusing to restore with an empty/whitespace-only passphrase — supply the real passphrase");
    }

    // 1. Read + validate the manifest (PLAINTEXT in both modes; restore routes on `encrypted`).
    let manifest = read_manifest(&src.join(MANIFEST_FILE))?;
    if manifest.schema != BACKUP_SCHEMA {
        bail!(
            "{} is not an lnrent backup (manifest schema = {:?})",
            src.display(),
            manifest.schema
        );
    }
    if manifest.version != BACKUP_FORMAT_VERSION {
        bail!(
            "unsupported backup format version {} (this build understands version {})",
            manifest.version,
            BACKUP_FORMAT_VERSION
        );
    }

    // 2. Verify the backup set is COMPLETE before mutating the target — never silently drop a file
    //    the manifest says was captured. In encrypted mode the sensitive files live inside
    //    `backup.age`, so here we only confirm that artifact + a passphrase are present; the inner set
    //    is re-checked AFTER decrypt, still inside staging (so the target is never touched on failure).
    if manifest.encrypted {
        if !is_regular_file(&src.join(BACKUP_AGE_FILE)) {
            bail!("backup is incomplete/corrupt: manifest records an encrypted backup but {BACKUP_AGE_FILE} is missing");
        }
        if passphrase.is_none() {
            bail!(
                "this backup is encrypted; pass --passphrase-file to restore it (the {BACKUP_AGE_FILE} \
                 artifact holds the seed + ecash under a passphrase)"
            );
        }
    } else {
        // `MANIFEST.json` is intentionally plaintext metadata, so its `encrypted` bit cannot by
        // itself authenticate the restore mode. Never silently ignore an operator-supplied
        // passphrase, and never route around a present age artifact: either condition means treating
        // this as plaintext could install attacker-supplied fund state after a manifest downgrade.
        if passphrase.is_some() {
            bail!(
                "backup manifest says plaintext but a passphrase was supplied; refusing a possible encrypted-backup downgrade"
            );
        }
        let age_path = src.join(BACKUP_AGE_FILE);
        match fs::symlink_metadata(&age_path) {
            Ok(_) => bail!(
                "backup manifest says plaintext but {BACKUP_AGE_FILE} is present; refusing a possible encrypted-backup downgrade"
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(anyhow!("stat {}: {e}", age_path.display())),
        }
        let src_db = src.join(STATE_DB_FILE);
        if !manifest.state_db || !is_regular_file(&src_db) {
            bail!(
                "backup is incomplete/corrupt: missing state DB {} (a restore must reproduce the \
                 operator's commitments exactly)",
                src_db.display()
            );
        }
        if manifest.fedimint_config && !is_regular_file(&src.join(FEDIMINT_CONFIG_FILE)) {
            bail!(
                "backup is incomplete/corrupt: manifest records {FEDIMINT_CONFIG_FILE} but it is missing"
            );
        }
        if manifest.operator_seed && !is_regular_file(&src.join(SEED_FILE)) {
            bail!("backup is incomplete/corrupt: manifest records {SEED_FILE} but it is missing");
        }
        if manifest.fedimint_dir {
            let src_fed = src.join(FEDIMINT_DIR);
            if !src_fed.is_dir() {
                bail!("backup is incomplete/corrupt: manifest records the {FEDIMINT_DIR}/ subtree but it is missing");
            }
            for fed in &manifest.federations {
                if !src_fed.join(fed).is_dir() {
                    bail!(
                        "backup is incomplete/corrupt: federation dir {FEDIMINT_DIR}/{fed} is missing"
                    );
                }
            }
        }
    }

    // 3. Refuse to clobber a non-empty target unless forced.
    if data_dir.exists() && !data_dir.is_dir() {
        bail!(
            "restore target {} exists and is not a directory",
            data_dir.display()
        );
    }
    if dir_non_empty(data_dir)? && !force {
        bail!(
            "restore target {} is not empty; restore into a fresh data dir, or pass --force to \
             overwrite",
            data_dir.display()
        );
    }

    // 4. Assemble the full set in a sibling staging dir, fsync it, then atomically swap it in. The
    //    staging dir holds EXACTLY the backup's artifacts, so the swap replaces the target wholesale
    //    — no stale `fedimint/` / seed / config from a forced non-empty target survives, and no stale
    //    `-wal`/`-shm` sidecars can linger (staging never had any). The parent must exist so staging
    //    lands on the same filesystem (rename is only atomic within one filesystem).
    let parent = parent_dir(data_dir);
    fs::create_dir_all(&parent)
        .with_context(|| format!("creating restore parent dir {}", parent.display()))?;

    let staging = with_staging(&parent, |stage| {
        if manifest.encrypted {
            // Decrypt + untar the sensitive set INTO staging. A wrong passphrase / truncated / tampered
            // artifact fails here (AEAD tag) — before the swap — so the target stays untouched. The
            // passphrase presence was checked above, so `expect` cannot fire.
            let pw = passphrase
                .as_ref()
                .expect("encrypted restore requires a passphrase (checked above)");
            decrypt_and_unpack(&src.join(BACKUP_AGE_FILE), stage, pw)?;
            // The decrypted set must match what the manifest promised, and every extracted file/dir is
            // re-hardened (0600/0700) + fsynced — the untar wrote at the tar entries' modes.
            finalize_decrypted_staging(stage, &manifest)?;
        } else {
            let src_db = src.join(STATE_DB_FILE);
            let dst_db = stage.join(STATE_DB_FILE);
            fs::copy(&src_db, &dst_db).with_context(|| {
                format!("restoring {} -> {}", src_db.display(), dst_db.display())
            })?;
            harden_file_0600(&dst_db)?;
            fsync_file(&dst_db)?;

            if manifest.fedimint_dir {
                copy_dir_recursive(&src.join(FEDIMINT_DIR), &stage.join(FEDIMINT_DIR))?;
            }
            if manifest.fedimint_config {
                restore_secret_file(
                    &src.join(FEDIMINT_CONFIG_FILE),
                    &stage.join(FEDIMINT_CONFIG_FILE),
                )?;
            }
            if manifest.operator_seed {
                restore_secret_file(&src.join(SEED_FILE), &stage.join(SEED_FILE))?;
            }
        }
        // Make the staged set's own directory entries durable before the swap.
        fsync_dir(stage)
    })?;

    swap_into_place(&staging, data_dir, &parent)?;
    Ok(manifest)
}

// ===== helpers ===================================================================================

/// `VACUUM INTO` the source state DB to `dest_db`. Opened read-WRITE (the daemon is stopped, so there
/// is no racing writer) so SQLite recovers an uncheckpointed WAL and the produced file reflects the
/// latest committed state EXACTLY — critical for the money path. The target must not pre-exist
/// (guaranteed by [`prepare_empty_dest`]); VACUUM INTO refuses to overwrite.
fn vacuum_into(src_db: &Path, dest_db: &Path) -> Result<()> {
    let dest_str = dest_db
        .to_str()
        .ok_or_else(|| anyhow!("backup dest path is not valid UTF-8: {}", dest_db.display()))?;
    let conn = Connection::open(src_db)
        .with_context(|| format!("opening state DB {}", src_db.display()))?;
    conn.execute("VACUUM INTO ?1", [dest_str])
        .with_context(|| format!("VACUUM INTO {}", dest_db.display()))?;
    Ok(())
}

/// Copy a secret/credential file into the restored data dir and tighten it to owner-only (0600),
/// matching the `config.rs` perms contract for the seed / fedimint config.
fn restore_secret_file(src: &Path, dst: &Path) -> Result<()> {
    remove_file_if_exists(dst)?;
    fs::copy(src, dst)
        .with_context(|| format!("restoring {} -> {}", src.display(), dst.display()))?;
    harden_file_0600(dst)?;
    fsync_file(dst)
}

/// Recursively copy `src` -> `dst` (files + subdirs). Regular files are copied byte-for-byte via
/// `fs::copy`, then tightened to owner-only (0600) and fsynced; created directories are tightened to
/// owner-only (0700) and fsynced, consistent with the rest of the module's hardening. The fedimint
/// subtree carries ecash-bearing material (the `client.db` RocksDB and the `lnv2_index.db`) that is
/// at least as sensitive as the seed, so it gets the SAME 0600 hardening rather than inheriting the
/// source's umask mode bits (review R2 P2). A symlink in the tree is REFUSED rather than silently
/// dereferenced — the fedimint subtree is plain files/dirs, and following a link could pull bytes
/// from outside the captured set. Used for the opaque fedimint subtree.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).with_context(|| format!("creating {}", dst.display()))?;
    fs::set_permissions(dst, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("setting owner-only perms on {}", dst.display()))?;
    for entry in fs::read_dir(src).with_context(|| format!("listing {}", src.display()))? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            bail!(
                "refusing to copy symlink {} in the fedimint subtree (expected plain files/dirs only)",
                from.display()
            );
        } else if file_type.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            fs::copy(&from, &to)
                .with_context(|| format!("copying {} -> {}", from.display(), to.display()))?;
            // Ecash material — harden to owner-only like the seed/state DB (review R2 P2).
            harden_file_0600(&to)?;
            fsync_file(&to)?;
        }
    }
    fsync_dir(dst)?;
    Ok(())
}

/// Copy `src` -> `dst` iff `src` is a present regular file; returns whether it was copied. The copy
/// is tightened to owner-only (0600) and fsynced (these are the seed / federation config — secrets).
/// A symlink or non-regular file is rejected (we never back up a secret via a symlink); absence is
/// `Ok(false)`.
fn copy_if_present(src: &Path, dst: &Path) -> Result<bool> {
    match fs::symlink_metadata(src) {
        Ok(meta) if meta.file_type().is_file() => {
            fs::copy(src, dst)
                .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
            harden_file_0600(dst)?;
            fsync_file(dst)?;
            Ok(true)
        }
        Ok(meta) if meta.file_type().is_symlink() => bail!(
            "{} is a symlink; refusing to back up a secret/config via a symlink",
            src.display()
        ),
        Ok(_) => bail!("{} is not a regular file", src.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(anyhow!("stat {}: {e}", src.display())),
    }
}

// ===== encrypted-mode helpers (lnrent-y4m.6) =====================================================

/// Report whether the fedimint subtree ROOT at `src` is a present REAL directory to archive, refusing
/// a symlink. `Path::is_dir()` follows a symlink; the encrypted tar would then walk the link target
/// and pull in bytes from OUTSIDE the data dir, breaking the captured-set boundary the symlink-refusal
/// contract exists to hold (review R1 P2). A symlink → error; a real dir → `Ok(true)`; a missing root
/// → `Ok(false)` (fedimint simply isn't configured); any other non-dir → `Ok(false)`, matching the
/// plaintext path's `is_dir()` skip for a stray non-directory at that path.
fn fedimint_root_is_real_dir(src: &Path) -> Result<bool> {
    match fs::symlink_metadata(src) {
        Ok(meta) if meta.file_type().is_symlink() => bail!(
            "{} is a symlink; refusing to back up the fedimint subtree via a symlink (expected a real \
             directory)",
            src.display()
        ),
        Ok(meta) => Ok(meta.file_type().is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(anyhow!("stat {}: {e}", src.display())),
    }
}

/// Report whether `src` is a present regular file WITHOUT copying it — the encrypted path's analogue
/// of [`copy_if_present`] (the tar walk reads the source directly). Applies the SAME symlink-refusal:
/// we never bundle a secret reached via a symlink. Absence is `Ok(false)`.
fn present_regular_or_reject(src: &Path) -> Result<bool> {
    match fs::symlink_metadata(src) {
        Ok(meta) if meta.file_type().is_file() => Ok(true),
        Ok(meta) if meta.file_type().is_symlink() => bail!(
            "{} is a symlink; refusing to back up a secret/config via a symlink",
            src.display()
        ),
        Ok(_) => bail!("{} is not a regular file", src.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(anyhow!("stat {}: {e}", src.display())),
    }
}

/// Wrap the passphrase bytes in age's zeroizing `SecretString` at the crypto boundary. The transient
/// `String` copy becomes owned by the `SecretString` (wiped on drop); the caller's [`Zeroizing`]
/// buffer is wiped independently. The passphrase is NEVER logged or placed in an error message.
fn passphrase_secret(passphrase: &Zeroizing<String>) -> age::secrecy::SecretString {
    age::secrecy::SecretString::from(passphrase.as_str().to_owned())
}

/// Stream {`sqlite_snapshot` as `lnrent.sqlite`, `operator.seed`, `fedimint.json`, the `fedimint/`
/// subtree} through a `tar` builder wrapped in the `age` passphrase writer, producing `age_path`. The
/// output file is tightened to 0600 the instant it exists — BEFORE any encrypted payload is written —
/// then the sensitive bytes are STREAMED (the ecash `client.db` is never buffered whole in memory).
/// age's default scrypt work factor + ChaCha20-Poly1305 AEAD are used as-is (no hand-tuning). The
/// `finish()` calls are load-bearing: skipping the age `finish` truncates the file and makes it
/// undecryptable.
fn write_encrypted_bundle(
    age_path: &Path,
    sqlite_snapshot: &Path,
    data_dir: &Path,
    fedimint_dir: bool,
    fedimint_config: bool,
    operator_seed: bool,
    passphrase: &Zeroizing<String>,
) -> Result<()> {
    let file =
        fs::File::create(age_path).with_context(|| format!("creating {}", age_path.display()))?;
    // Tighten BEFORE writing the encrypted payload, so the AEAD bytes are never world-readable.
    harden_file_0600(age_path)?;

    let encryptor = age::Encryptor::with_user_passphrase(passphrase_secret(passphrase));
    let mut age_writer = encryptor
        .wrap_output(file)
        .context("initializing the age passphrase writer")?;
    {
        let mut tar = tar::Builder::new(&mut age_writer);
        tar_append_file(&mut tar, sqlite_snapshot, Path::new(STATE_DB_FILE))?;
        if operator_seed {
            tar_append_file(&mut tar, &data_dir.join(SEED_FILE), Path::new(SEED_FILE))?;
        }
        if fedimint_config {
            tar_append_file(
                &mut tar,
                &data_dir.join(FEDIMINT_CONFIG_FILE),
                Path::new(FEDIMINT_CONFIG_FILE),
            )?;
        }
        if fedimint_dir {
            tar_append_dir_recursive(
                &mut tar,
                &data_dir.join(FEDIMINT_DIR),
                Path::new(FEDIMINT_DIR),
            )?;
        }
        tar.finish().context("finalizing the backup tar stream")?;
    }
    // Write age's final AEAD chunk; the caller fsyncs the completed artifact.
    age_writer
        .finish()
        .context("finalizing the age-encrypted backup")?;
    Ok(())
}

/// Append the regular file `path` to the tar builder under the archive path `arch`. Reads via a file
/// handle so the entry's size/mode come from the actual file; long paths are handled by tar's GNU
/// long-name extension, so deep rocksdb/federation paths do not overflow the ustar header.
fn tar_append_file<W: std::io::Write>(
    builder: &mut tar::Builder<W>,
    path: &Path,
    arch: &Path,
) -> Result<()> {
    let mut f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    builder
        .append_file(arch, &mut f)
        .with_context(|| format!("adding {} to the backup tar", path.display()))?;
    Ok(())
}

/// Recursively append `src` into the tar builder under the archive path `arch` (files + subdirs). A
/// symlink is REFUSED (never dereferenced), mirroring [`copy_dir_recursive`]'s guard — the fedimint
/// subtree is plain files/dirs, and following a link could pull bytes from outside the captured set.
/// Stored modes are not load-bearing: RESTORE re-hardens every extracted entry to 0600/0700.
fn tar_append_dir_recursive<W: std::io::Write>(
    builder: &mut tar::Builder<W>,
    src: &Path,
    arch: &Path,
) -> Result<()> {
    builder
        .append_dir(arch, src)
        .with_context(|| format!("adding dir {} to the backup tar", src.display()))?;
    for entry in fs::read_dir(src).with_context(|| format!("listing {}", src.display()))? {
        let entry = entry?;
        let from = entry.path();
        let child_arch = arch.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            bail!(
                "refusing to back up symlink {} in the fedimint subtree (expected plain files/dirs only)",
                from.display()
            );
        } else if file_type.is_dir() {
            tar_append_dir_recursive(builder, &from, &child_arch)?;
        } else {
            tar_append_file(builder, &from, &child_arch)?;
        }
    }
    Ok(())
}

/// Decrypt `age_path` with `passphrase` and untar the sensitive set INTO `stage`. Called only from
/// inside the restore staging closure, so a wrong passphrase / truncated / tampered artifact fails
/// here (age's AEAD tag) BEFORE the atomic swap — the live target is never touched. The passphrase is
/// never included in the surfaced error.
fn decrypt_and_unpack(age_path: &Path, stage: &Path, passphrase: &Zeroizing<String>) -> Result<()> {
    let file =
        fs::File::open(age_path).with_context(|| format!("opening {}", age_path.display()))?;
    let decryptor = age::Decryptor::new_buffered(BufReader::new(file))
        .context("reading the encrypted backup header")?;
    let identity = age::scrypt::Identity::new(passphrase_secret(passphrase));
    let reader = decryptor
        .decrypt(iter::once(&identity as &dyn age::Identity))
        .context("decrypting backup (wrong passphrase or corrupt/tampered artifact)")?;
    // Extract at the tar entries' modes into the 0700 staging dir (not world-reachable); every entry
    // is re-hardened afterward by `finalize_decrypted_staging`. tar's default unpack sanitizes paths.
    let mut archive = tar::Archive::new(reader);
    archive.set_preserve_permissions(false);
    archive
        .unpack(stage)
        .context("extracting the decrypted backup")?;
    // `tar::unpack` STOPS at the tar terminator, so it does NOT read the age stream to EOF — which is
    // where age authenticates the FINAL STREAM chunk. Drain the reader so a truncated / appended /
    // tampered ciphertext is REJECTED by age's AEAD tag HERE (inside staging, BEFORE any swap), not
    // silently accepted as a complete restore (adversarial codex).
    let mut reader = archive.into_inner();
    std::io::copy(&mut reader, &mut std::io::sink())
        .context("authenticating the full backup ciphertext (age integrity check to EOF)")?;
    Ok(())
}

/// After a decrypt+untar into `stage`, verify the extracted set matches what the plaintext `manifest`
/// promised (never silently drop a commitment), then re-harden every entry to owner-only (files 0600,
/// dirs 0700) and fsync it — mirroring the plaintext restore's hardening.
fn finalize_decrypted_staging(stage: &Path, manifest: &Manifest) -> Result<()> {
    let db = stage.join(STATE_DB_FILE);
    if !is_regular_file(&db) {
        bail!(
            "decrypted backup is incomplete/corrupt: missing state DB {} (a restore must reproduce \
             the operator's commitments exactly)",
            db.display()
        );
    }
    if manifest.operator_seed && !is_regular_file(&stage.join(SEED_FILE)) {
        bail!(
            "decrypted backup is incomplete/corrupt: manifest records {SEED_FILE} but it is missing"
        );
    }
    if manifest.fedimint_config && !is_regular_file(&stage.join(FEDIMINT_CONFIG_FILE)) {
        bail!("decrypted backup is incomplete/corrupt: manifest records {FEDIMINT_CONFIG_FILE} but it is missing");
    }
    if manifest.fedimint_dir {
        let fed = stage.join(FEDIMINT_DIR);
        if !fed.is_dir() {
            bail!("decrypted backup is incomplete/corrupt: manifest records the {FEDIMINT_DIR}/ subtree but it is missing");
        }
        for f in &manifest.federations {
            if !fed.join(f).is_dir() {
                bail!("decrypted backup is incomplete/corrupt: federation dir {FEDIMINT_DIR}/{f} is missing");
            }
        }
    }
    harden_and_fsync_tree(stage)
}

/// Recursively tighten every entry under `root` to owner-only (files 0600, dirs 0700) and fsync it.
/// A symlink is REFUSED — our own artifacts never contain one (backup refuses them, and the AEAD
/// authenticates the payload), so a symlink here means a corrupt set, not something to dereference.
fn harden_and_fsync_tree(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("listing {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            bail!(
                "refusing symlink {} in the decrypted backup (expected plain files/dirs only)",
                path.display()
            );
        } else if file_type.is_dir() {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("setting owner-only perms on {}", path.display()))?;
            harden_and_fsync_tree(&path)?;
            fsync_dir(&path)?;
        } else {
            harden_file_0600(&path)?;
            fsync_file(&path)?;
        }
    }
    Ok(())
}

/// The immediate subdirectory names of `dir`, sorted (the captured federation ids).
fn list_subdirs(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

/// Ensure `dest` is a fresh, empty directory (creating it 0700 if absent). A pre-existing non-empty
/// dest — or a non-directory at that path — is refused.
fn prepare_empty_dest(dest: &Path) -> Result<()> {
    match fs::symlink_metadata(dest) {
        Ok(meta) => {
            if !meta.file_type().is_dir() {
                bail!(
                    "backup dest {} exists and is not a directory",
                    dest.display()
                );
            }
            if dir_non_empty(dest)? {
                bail!(
                    "backup dest {} is not empty; choose a fresh/empty destination (a backup must be \
                     a clean, self-contained set)",
                    dest.display()
                );
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(anyhow!("stat backup dest {}: {e}", dest.display())),
    }
    create_private_dir(dest)
}

/// `create_dir_all` + tighten to owner-only (0700). The dir holds secrets (seed, state DB), so
/// owner-only traversal matches the `config.rs` data-dir hardening.
fn create_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("setting owner-only perms on {}", path.display()))
}

fn harden_file_0600(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting owner-only perms on {}", path.display()))
}

fn write_manifest(path: &Path, manifest: &Manifest) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(manifest).context("serializing backup manifest")?;
    bytes.push(b'\n');
    fs::write(path, &bytes).with_context(|| format!("writing {}", path.display()))?;
    fsync_file(path)
}

/// Build the restore set in a fresh, hardened (0700) sibling staging dir under `parent` and return
/// its path. `build` populates it; if it fails the half-built staging dir is removed and the error
/// propagates, so the real target is never touched on a partial restore. The staging name is hidden
/// and pid/time-tagged so it never collides with the target or a concurrent restore.
fn with_staging<F>(parent: &Path, build: F) -> Result<PathBuf>
where
    F: FnOnce(&Path) -> Result<()>,
{
    let staging = parent.join(format!(
        ".lnrent-restore-staging-{}-{}",
        std::process::id(),
        now_nanos()
    ));
    remove_dir_if_exists(&staging)?;
    create_private_dir(&staging)?;
    match build(&staging) {
        Ok(()) => Ok(staging),
        Err(e) => {
            // Surface a cleanup failure: a partially-decrypted seed/ecash must NOT be silently left in
            // the staging dir (adversarial codex). NotFound is fine — nothing to remove.
            match fs::remove_dir_all(&staging) {
                Ok(()) => Err(e),
                Err(ce) if ce.kind() == std::io::ErrorKind::NotFound => Err(e),
                Err(ce) => Err(e.context(format!(
                    "restore failed AND could not remove staging dir {} (possible plaintext secret \
                     residue — remove it manually): {ce}",
                    staging.display()
                ))),
            }
        }
    }
}

/// Atomically install the fully-staged `staging` dir as `data_dir`.
///
/// When the target does not yet exist this is a single `rename(2)`. When it DOES exist (a `--force`
/// restore over live state) the naive "move the old dir aside, then move the new one in" has a crash
/// window where `data_dir` is momentarily ABSENT — and a daemon restarted in that window would treat
/// the missing dir as a first boot and bootstrap a FRESH empty identity over the operator's
/// commitments. To close that window we swap the two directory entries with a single atomic
/// `renameat2(RENAME_EXCHANGE)` (Linux): at every instant `data_dir` resolves to a COMPLETE directory
/// — the old set or the new one, never nothing. After the swap the OLD set lives at the `staging`
/// path and is dropped. On a filesystem/kernel without `RENAME_EXCHANGE` we fall back to
/// move-aside-then-in (the small window, with rollback on a failed install — no worse than a plain
/// `mv`). Finally the parent dir is fsynced so the rename is durable. (review R1 P1)
fn swap_into_place(staging: &Path, data_dir: &Path, parent: &Path) -> Result<()> {
    if data_dir.exists() {
        match rename_exchange(staging, data_dir) {
            Ok(()) => {
                // `data_dir` now holds the NEW set; the staging path now holds the OLD one — drop it.
                let _ = fs::remove_dir_all(staging);
            }
            Err(e) if exchange_unsupported(&e) => {
                swap_via_move_aside(staging, data_dir, parent)?;
            }
            Err(e) => {
                let _ = fs::remove_dir_all(staging);
                return Err(anyhow!(
                    "atomically installing restored data dir {}: {e}",
                    data_dir.display()
                ));
            }
        }
    } else {
        fs::rename(staging, data_dir)
            .with_context(|| format!("installing restored data dir {}", data_dir.display()))?;
    }
    fsync_dir(parent)
}

/// Fallback swap for filesystems/kernels that lack `RENAME_EXCHANGE`: move the existing target aside,
/// move the staged set in, and roll the original back if the install fails. This re-opens a brief
/// window where `data_dir` is absent (between the two renames) — used ONLY when the atomic exchange
/// above is unavailable; it is no worse than a plain `mv` over the directory.
fn swap_via_move_aside(staging: &Path, data_dir: &Path, parent: &Path) -> Result<()> {
    let old = parent.join(format!(
        ".lnrent-restore-old-{}-{}",
        std::process::id(),
        now_nanos()
    ));
    remove_dir_if_exists(&old)?;
    fs::rename(data_dir, &old)
        .with_context(|| format!("moving existing {} aside", data_dir.display()))?;
    if let Err(e) = fs::rename(staging, data_dir) {
        // Best-effort rollback: restore the original so we never leave the target missing.
        let _ = fs::rename(&old, data_dir);
        let _ = fs::remove_dir_all(staging);
        return Err(anyhow!(
            "installing restored data dir {}: {e}",
            data_dir.display()
        ));
    }
    let _ = fs::remove_dir_all(&old);
    Ok(())
}

/// Atomically exchange two EXISTING directory entries `a` <-> `b` via `renameat2(RENAME_EXCHANGE)`.
/// Both paths must exist. Linux-only; on other targets this is a stub that reports `ENOSYS` so the
/// caller falls back to the non-atomic swap.
#[cfg(target_os = "linux")]
fn rename_exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::io::{Error, ErrorKind};
    use std::os::unix::ffi::OsStrExt;
    let ca = CString::new(a.as_os_str().as_bytes())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains an interior NUL"))?;
    let cb = CString::new(b.as_os_str().as_bytes())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains an interior NUL"))?;
    // SAFETY: `ca`/`cb` are valid NUL-terminated C strings borrowed for the duration of the call;
    // `AT_FDCWD` resolves both relative to the current directory, and `RENAME_EXCHANGE` atomically
    // swaps the two existing directory entries (or fails, leaving both untouched).
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            ca.as_ptr(),
            libc::AT_FDCWD,
            cb.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn rename_exchange(_a: &Path, _b: &Path) -> std::io::Result<()> {
    Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
}

/// True iff `e` means "this kernel/filesystem cannot do `RENAME_EXCHANGE`", so the caller should fall
/// back to a non-atomic swap: `ENOSYS` (no `renameat2` / non-Linux build), `EINVAL` (flag rejected),
/// or `ENOTSUP` (filesystem refuses the flag).
fn exchange_unsupported(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::ENOTSUP)
    )
}

/// The directory that will hold a freshly-created `path`, for placing a same-filesystem staging dir.
/// A bare relative name (no parent component) lands in the current directory.
fn parent_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Reject when `a` and `b` OVERLAP — either is equal to, or nested inside, the other. Backup uses
/// this to stop a `dest` from sitting inside the data dir (the recursive fedimint copy would otherwise
/// descend into its own output and fill the disk); restore uses it to stop the source and target from
/// nesting. Compares absolute, lexically-normalized paths COMPONENT-wise (so `/a/data` and `/a/data2`
/// do not count as nested). It is a foot-gun guard, not a security boundary, so it does not resolve
/// symlinks.
fn assert_disjoint(a: &Path, b: &Path, a_label: &str, b_label: &str) -> Result<()> {
    let na = absolute_lexical(a);
    let nb = absolute_lexical(b);
    if na.starts_with(&nb) || nb.starts_with(&na) {
        bail!(
            "{a_label} {} and {b_label} {} overlap — one is inside the other; choose paths that do \
             not nest",
            a.display(),
            b.display()
        );
    }
    Ok(())
}

/// Make `p` absolute (joining the current dir when relative) and resolve `.`/`..` LEXICALLY, without
/// touching the filesystem — so it also works for a `dest` that does not exist yet. `..` never climbs
/// above the root.
fn absolute_lexical(p: &Path) -> PathBuf {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(p)
    };
    let mut out = PathBuf::new();
    for comp in abs.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// fsync a regular file's contents + metadata (durability for the money path).
fn fsync_file(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("fsync {}", path.display()))
}

/// fsync a directory so its entry list (created/renamed names) is durable. On Linux an fsync of a
/// directory fd flushes its dirents.
fn fsync_dir(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("fsync dir {}", path.display()))
}

fn read_manifest(path: &Path) -> Result<Manifest> {
    let bytes = fs::read(path).with_context(|| {
        format!(
            "reading backup manifest {} (is this an lnrent backup directory?)",
            path.display()
        )
    })?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing backup manifest {}", path.display()))
}

/// True iff `path` is a present REGULAR file (not a symlink, not a dir).
fn is_regular_file(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| m.file_type().is_file())
        .unwrap_or(false)
}

/// True iff `path` is a directory with at least one entry. A missing path is empty (`false`).
fn dir_non_empty(path: &Path) -> Result<bool> {
    match fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().is_some()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(anyhow!("reading dir {}: {e}", path.display())),
    }
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow!("removing {}: {e}", path.display())),
    }
}

fn remove_dir_if_exists(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(anyhow!("removing dir {}: {e}", path.display())),
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Monotonic-enough nanosecond tag for unique, collision-free staging dir names.
fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
