//! Shared, hardened data-dir path preparation for the fedimint backend. The live `lnv2_backend` lays
//! its fedimint rocksdb under `data_dir/fedimint/<federation_id>/`; this module owns the
//! create-and-harden of that tree. (The retired lnv1 backend, which shared this module, was deleted by
//! lnrent-8ym.) It is NOT feature-gated — pure std/libc, no fedimint dependency.
//!
//! Before ADR-0022 this module also laid down the backends' private sqlite index files
//! (`lnv2_index.db` under the federation dir, `phoenixd_index.db` beside the state DB). Those
//! correlation maps now live in `lnrent.sqlite`; the only remaining files here are the fedimint client's
//! own. [`prepare_private_file`] stays for the legacy import, which needs to vet a pre-existing side
//! file (symlink-refused, owner-only) before reading it.
//!
//! The confidentiality boundary is the **0700 directories** (`fedimint/`, `<federation>/`, the client
//! db dir): once owner-only, the note/wallet material inside is unreadable to co-tenant local users
//! regardless of the umask-derived perms rocksdb gives its churned files. Each path's FINAL component
//! is symlink-refused (lstat + `O_NOFOLLOW` re-open, perms set on the fd so there is no chmod TOCTOU).
//! Swapping an INTERMEDIATE component for a symlink already requires write access to the operator's
//! 0700 `data_dir` — i.e. being the service user/root — which is outside the co-tenant threat model
//! this closes.

use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

/// The hardened fedimint paths for one federation data-dir.
pub struct FedimintPaths {
    pub client_db: PathBuf,
    /// `data_dir/fedimint/<federation_id>/` — where the pre-ADR-0022 `lnv2_index.db` lived; the
    /// boot-time legacy import looks there.
    pub federation_dir: PathBuf,
}

/// Create + harden `data_dir/fedimint/<federation_id>/<client_db_dir>` before the rocksdb open.
/// Returns the absolute paths. Deliberately creates NO sqlite side file any more (ADR-0022): a fresh
/// empty `lnv2_index.db` would read as a legacy index to import, and the import would then have to
/// tell "empty because new" from "empty because lost".
pub fn prepare_fedimint_paths(
    data_dir: &Path,
    federation_id: &str,
    client_db_dir: &str,
) -> Result<FedimintPaths> {
    let fedimint_dir = data_dir.join("fedimint");
    prepare_private_dir(&fedimint_dir, "fedimint root dir")?;

    let federation_dir = fedimint_dir.join(federation_id);
    prepare_private_dir(&federation_dir, "fedimint federation dir")?;

    let client_db = federation_dir.join(client_db_dir);
    prepare_private_dir(&client_db, "fedimint client db dir")?;

    Ok(FedimintPaths {
        client_db,
        federation_dir,
    })
}

fn prepare_private_dir(path: &Path, what: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                anyhow::bail!("{what} {} must not be a symlink", path.display());
            }
            if !meta.file_type().is_dir() {
                anyhow::bail!("{what} {} must be a directory", path.display());
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            match fs::DirBuilder::new().mode(0o700).create(path) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => {
                    return Err(e).with_context(|| format!("creating {what} {}", path.display()))
                }
            }
        }
        Err(e) => return Err(e).with_context(|| format!("stat {what} {}", path.display())),
    }
    harden_private_dir(path, what)
}

fn harden_private_dir(path: &Path, what: &str) -> Result<()> {
    let handle = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(|e| {
            if matches!(e.raw_os_error(), Some(libc::ELOOP) | Some(libc::ENOTDIR)) {
                anyhow!("{what} {} must be a real directory, not a symlink", path.display())
            } else {
                anyhow!("opening {what} {} to harden perms: {e}", path.display())
            }
        })?;
    let meta = handle
        .metadata()
        .with_context(|| format!("stat opened {what} {}", path.display()))?;
    if !meta.file_type().is_dir() {
        anyhow::bail!("{what} {} must be a directory", path.display());
    }
    handle
        .set_permissions(fs::Permissions::from_mode(0o700))
        .with_context(|| format!("perms on {what} {}", path.display()))
}

/// Create (0600, `O_NOFOLLOW`) or vet-and-harden ONE regular file inside an already-private data dir,
/// refusing a symlinked or non-regular target. Used by the legacy side-file import (ADR-0022) to vet
/// a pre-existing `phoenixd_index.db` / `lnv2_index.db` before reading it — hence `pub`.
pub fn prepare_private_file(path: &Path, what: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                anyhow::bail!("{what} {} must not be a symlink", path.display());
            }
            if !meta.file_type().is_file() {
                anyhow::bail!("{what} {} must be a regular file", path.display());
            }
            harden_private_file(path, what)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
            {
                Ok(file) => file
                    .set_permissions(fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("perms on {what} {}", path.display())),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    let meta = fs::symlink_metadata(path)
                        .with_context(|| format!("stat {what} {}", path.display()))?;
                    if meta.file_type().is_symlink() {
                        anyhow::bail!("{what} {} must not be a symlink", path.display());
                    }
                    if !meta.file_type().is_file() {
                        anyhow::bail!("{what} {} must be a regular file", path.display());
                    }
                    harden_private_file(path, what)
                }
                Err(e) => Err(e).with_context(|| format!("creating {what} {}", path.display())),
            }
        }
        Err(e) => Err(e).with_context(|| format!("stat {what} {}", path.display())),
    }
}

fn harden_private_file(path: &Path, what: &str) -> Result<()> {
    let handle = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ELOOP) {
                anyhow!("{what} {} must not be a symlink", path.display())
            } else {
                anyhow!("opening {what} {} to harden perms: {e}", path.display())
            }
        })?;
    let meta = handle
        .metadata()
        .with_context(|| format!("stat opened {what} {}", path.display()))?;
    if !meta.file_type().is_file() {
        anyhow::bail!("{what} {} must be a regular file", path.display());
    }
    handle
        .set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("perms on {what} {}", path.display()))
}
