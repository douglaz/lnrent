//! Recipe manifest and loader. SPEC.md §7. Recipes are trusted code (ADR-0002).

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Lifecycle hooks every recipe must ship (SPEC.md §7, §7.2).
const LIFECYCLE_HOOKS: [&str; 5] = ["provision", "suspend", "resume", "destroy", "healthcheck"];

/// True if `backend` is a known compute backend selector (SPEC.md §8.1): one of the fixed
/// `host | incus | libvirt | proxmox`, or a `cloud-*` provider. This is the canonical allowlist,
/// shared by recipe validation here and operator-config validation (config.rs) so the two can't
/// drift apart.
pub(crate) fn is_known_compute_backend(backend: &str) -> bool {
    matches!(backend, "host" | "incus" | "libvirt" | "proxmox") || backend.starts_with("cloud-")
}

/// True if `path` exists and is an executable regular file (any exec bit set).
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

/// A parsed `recipe.toml` plus the directory it came from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recipe {
    pub service: Service,
    pub pricing: Pricing,
    pub provisioning: Provisioning,
    pub os: Os,
    #[serde(default)]
    pub params: Vec<Param>,
    /// Buyer-facing management operations (SPEC.md §7.4, ADR-0013). Empty = none declared.
    #[serde(default, rename = "operation")]
    pub operations: Vec<Operation>,
    #[serde(skip)]
    pub dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    pub name: String,
    pub summary: String,
    pub version: String,
    #[serde(default)]
    pub category: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pricing {
    pub amount_sat: u64,
    pub period: String,
    pub renew_lead: String,
    pub retention: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provisioning {
    /// Compute backend selector: host | incus | libvirt | proxmox | cloud-* (SPEC.md §8.1).
    pub backend: String,
    pub isolation: String,
    /// Honest security tier the Listing advertises: "0" | "1" | "1.5" | "2" (ADR-0007, §9.1).
    #[serde(default = "default_tier")]
    pub tier: String,
    #[serde(default)]
    pub resources: Resources,
}

fn default_tier() -> String {
    "0".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    #[serde(default)]
    pub cpu: u32,
    #[serde(default)]
    pub mem_mb: u32,
    #[serde(default)]
    pub disk_gb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Os {
    pub supports: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Param {
    pub key: String,
    pub label: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub required: bool,
}

/// A buyer-facing management operation declared by a recipe (SPEC.md §7.4, ADR-0013).
/// `kind` selects the transport: `request` rides the NIP-17 `op.request`/`op.result` DM
/// pair; `interactive` rides the Iroh Native-connect session (§9.2). `hook` is a **bare
/// filename** (no path separators, no `..`) resolved as `<recipe-dir>/ops/<hook>`; the
/// recipe runner (lnrent-7fp.6) rejects any non-bare hook to prevent path traversal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub name: String,
    pub label: String,
    /// "request" | "interactive" (validated by the recipe runner, lnrent-7fp.6).
    pub kind: String,
    pub hook: String,
    #[serde(default)]
    pub params: Vec<Param>,
}

impl Operation {
    /// True if `hook` is a safe bare filename (no path separators, no `..`, non-empty).
    /// `Recipe::validate()` (lnrent-7fp.6) enforces this before dispatch (§7.4).
    pub fn hook_is_safe(&self) -> bool {
        !self.hook.is_empty()
            && !self.hook.contains('/')
            && !self.hook.contains('\\')
            && self.hook != ".."
            && self.hook != "."
    }
}

impl Recipe {
    /// Load a recipe from a directory containing `recipe.toml`.
    pub fn load(dir: impl AsRef<Path>) -> Result<Recipe> {
        let dir = dir.as_ref().to_path_buf();
        let manifest = dir.join("recipe.toml");
        let text = std::fs::read_to_string(&manifest)
            .with_context(|| format!("reading {}", manifest.display()))?;
        let mut recipe: Recipe =
            toml::from_str(&text).with_context(|| format!("parsing {}", manifest.display()))?;
        recipe.dir = dir;
        Ok(recipe)
    }

    /// Load every recipe directory under `root` (one level deep). A recipe that fails to PARSE
    /// is skipped (logged), not fatal — one bad manifest must not blank the whole catalog
    /// (codex #5). Callers should still `validate()` each before using it.
    pub fn load_all(root: impl AsRef<Path>) -> Result<Vec<Recipe>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() && path.join("recipe.toml").exists() {
                match Recipe::load(&path) {
                    Ok(r) => out.push(r),
                    Err(e) => {
                        tracing::warn!(dir = %path.display(), error = %e, "skipping unparseable recipe")
                    }
                }
            }
        }
        Ok(out)
    }

    /// Absolute path to a lifecycle hook executable in this recipe.
    pub fn hook(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Look up a declared management operation by name (SPEC.md §7.4).
    pub fn operation(&self, name: &str) -> Option<&Operation> {
        self.operations.iter().find(|op| op.name == name)
    }

    /// Absolute path to a management operation's hook executable, resolved as
    /// `<recipe-dir>/ops/<hook>`. `hook` is a bare filename (validated by
    /// `Operation::hook_is_safe` / the recipe runner) so this cannot escape `ops/`.
    pub fn op_hook(&self, op: &Operation) -> PathBuf {
        self.dir.join("ops").join(&op.hook)
    }

    /// Validate a recipe before it is used (lnrent-7fp.6, SPEC.md §7.2/§7.4/§9.1):
    /// the lifecycle hooks exist and are executable, the backend/isolation/tier/OS are known,
    /// and every declared management operation is well-formed (valid kind, safe bare hook,
    /// request-kind hook present + executable + contained in `ops/`, unique names).
    pub fn validate(&self) -> Result<()> {
        // Lifecycle hooks present + executable.
        for name in LIFECYCLE_HOOKS {
            let h = self.hook(name);
            if !is_executable(&h) {
                bail!(
                    "recipe `{}`: lifecycle hook `{name}` missing or not executable ({})",
                    self.service.id,
                    h.display()
                );
            }
        }

        // Backend / isolation / tier / OS (§8.1, §9.1, ADR-0007).
        let backend = &self.provisioning.backend;
        if !is_known_compute_backend(backend) {
            bail!(
                "recipe `{}`: unknown compute backend `{backend}`",
                self.service.id
            );
        }
        if !matches!(
            self.provisioning.isolation.as_str(),
            "none" | "container" | "vm"
        ) {
            bail!(
                "recipe `{}`: unknown isolation `{}`",
                self.service.id,
                self.provisioning.isolation
            );
        }
        if !matches!(self.provisioning.tier.as_str(), "0" | "1" | "1.5" | "2") {
            bail!(
                "recipe `{}`: invalid security tier `{}` (must be 0|1|1.5|2)",
                self.service.id,
                self.provisioning.tier
            );
        }
        if self.os.supports.is_empty() {
            bail!("recipe `{}`: os.supports is empty", self.service.id);
        }
        for os in &self.os.supports {
            if !matches!(os.as_str(), "nixos" | "debian") {
                bail!(
                    "recipe `{}`: unsupported OS `{os}` (nixos|debian)",
                    self.service.id
                );
            }
        }

        // Management operations (§7.4, ADR-0013).
        let mut seen = HashSet::new();
        for op in &self.operations {
            if !seen.insert(op.name.as_str()) {
                bail!(
                    "recipe `{}`: duplicate operation name `{}`",
                    self.service.id,
                    op.name
                );
            }
            if !matches!(op.kind.as_str(), "request" | "interactive") {
                bail!(
                    "recipe `{}`: operation `{}` has unknown kind `{}` (request|interactive)",
                    self.service.id,
                    op.name,
                    op.kind
                );
            }
            if !op.hook_is_safe() {
                bail!(
                    "recipe `{}`: operation `{}` has unsafe hook `{}` (bare filename only)",
                    self.service.id,
                    op.name,
                    op.hook
                );
            }
            // request-kind ops run on dispatch, so their hook must exist + be executable now;
            // interactive ops bind a session target (M1b) and are not executed here.
            if op.kind == "request" {
                let h = self.op_hook(op);
                if !is_executable(&h) {
                    bail!(
                        "recipe `{}`: operation `{}` hook missing or not executable ({})",
                        self.service.id,
                        op.name,
                        h.display()
                    );
                }
                // Defense-in-depth: the canonicalized hook must stay inside `ops/` (no symlink escape).
                let ops_dir = self.dir.join("ops");
                let (canon_ops, canon_hook) = (ops_dir.canonicalize(), h.canonicalize());
                if let (Ok(co), Ok(ch)) = (canon_ops, canon_hook) {
                    if !ch.starts_with(&co) {
                        bail!(
                            "recipe `{}`: operation `{}` hook escapes ops/ ({})",
                            self.service.id,
                            op.name,
                            ch.display()
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // SPEC.md §7.4 / ADR-0013: `[[operation]]` blocks parse into `operations`, and the
    // `ops/<hook>` path resolves under the recipe dir.
    #[test]
    fn manifest_operations_parse() {
        let toml = r#"
[service]
id = "wireguard"
name = "WireGuard VPN"
summary = "x"
version = "0.1.0"

[pricing]
amount_sat = 5000
period = "30d"
renew_lead = "7d"
retention = "7d"

[provisioning]
backend = "host"
isolation = "none"

[os]
supports = ["nixos"]

[[operation]]
name = "get-config"
label = "Download WireGuard config"
kind = "request"
hook = "get-config"
"#;
        let mut recipe: Recipe = toml::from_str(toml).expect("parse");
        recipe.dir = PathBuf::from("/recipes/wireguard");
        assert_eq!(recipe.operations.len(), 1);
        let op = recipe.operation("get-config").expect("operation present");
        assert_eq!(op.kind, "request");
        // `hook` is a bare name resolved under ops/ (no traversal).
        assert!(op.hook_is_safe());
        assert_eq!(
            recipe.op_hook(op),
            PathBuf::from("/recipes/wireguard/ops/get-config")
        );
        // A recipe with no [[operation]] blocks defaults to an empty op set.
        assert!(recipe.operation("nope").is_none());
    }

    // §7.4: a hook with a path separator or `..` must be rejected (no traversal out of ops/).
    #[test]
    fn unsafe_op_hooks_are_rejected() {
        let bad = ["../escape", "ops/nested", "a/b", "..", ""];
        for h in bad {
            let op = Operation {
                name: "x".into(),
                label: "x".into(),
                kind: "request".into(),
                hook: h.to_string(),
                params: vec![],
            };
            assert!(!op.hook_is_safe(), "expected {h:?} to be rejected");
        }
        let ok = Operation {
            name: "status".into(),
            label: "Status".into(),
            kind: "request".into(),
            hook: "status".into(),
            params: vec![],
        };
        assert!(ok.hook_is_safe());
    }

    fn wireguard() -> Recipe {
        let dir = format!("{}/../recipes/wireguard", env!("CARGO_MANIFEST_DIR"));
        Recipe::load(&dir).expect("load wireguard recipe")
    }

    // §7.2/§7.4/§9.1: the shipped wireguard recipe passes validation (lifecycle hooks +
    // request-op hooks exist and are executable, backend/tier/os/ops are well-formed).
    #[test]
    fn wireguard_recipe_validates() {
        wireguard()
            .validate()
            .expect("wireguard recipe should validate");
    }

    #[test]
    fn validate_rejects_unknown_backend_isolation_tier_os() {
        for mutate in [
            (|r: &mut Recipe| r.provisioning.backend = "bogus".into()) as fn(&mut Recipe),
            |r: &mut Recipe| r.provisioning.isolation = "weird".into(),
            |r: &mut Recipe| r.provisioning.tier = "9".into(),
            |r: &mut Recipe| r.os.supports = vec!["windows".into()],
            |r: &mut Recipe| r.os.supports.clear(),
        ] {
            let mut r = wireguard();
            mutate(&mut r);
            assert!(r.validate().is_err(), "expected validation failure");
        }
    }

    #[test]
    fn validate_rejects_missing_lifecycle_hook() {
        let mut r = wireguard();
        r.dir = PathBuf::from("/nonexistent-recipe-dir");
        assert!(r.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_operation() {
        // unknown kind
        let mut r = wireguard();
        r.operations.push(Operation {
            name: "weird".into(),
            label: "w".into(),
            kind: "telepathy".into(),
            hook: "weird".into(),
            params: vec![],
        });
        assert!(r.validate().is_err());

        // duplicate op name
        let mut r = wireguard();
        let dup = r.operations[0].clone();
        r.operations.push(dup);
        assert!(r.validate().is_err());

        // request-kind hook that doesn't exist under ops/
        let mut r = wireguard();
        r.operations.push(Operation {
            name: "ghost".into(),
            label: "g".into(),
            kind: "request".into(),
            hook: "ghost".into(),
            params: vec![],
        });
        assert!(r.validate().is_err());
    }
}
