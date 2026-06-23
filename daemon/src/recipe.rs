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
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub resources: Resources,
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
}
