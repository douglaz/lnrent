//! Recipe manifest and loader. SPEC.md §7. Recipes are trusted code (ADR-0002).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

    /// Load every recipe directory under `root` (one level deep).
    pub fn load_all(root: impl AsRef<Path>) -> Result<Vec<Recipe>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.path().join("recipe.toml").exists() {
                out.push(Recipe::load(entry.path())?);
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
}
