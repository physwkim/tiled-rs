//! Client-side profile loader.
//!
//! Mirrors `tiled/profiles.py` (`paths`, `load_profiles`, `list_profiles`,
//! `create_profile`, `delete_profile`, `get_default_profile_name`,
//! `set_default_profile_name`). Profiles live in a hierarchy of YAML files;
//! later directories in `paths()` override earlier ones.
//!
//! Compared to the Python implementation we skip jsonschema validation —
//! callers get the parsed content as `serde_yaml::Value` and can validate
//! themselves if they care.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{ClientError, Result};

/// Search path. Listed lowest-precedence to highest. The user-config dir is
/// last so it overrides everything else.
pub fn paths() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();

    if let Ok(custom) = std::env::var("TILED_SITE_PROFILES") {
        out.push(PathBuf::from(custom));
    } else {
        out.push(PathBuf::from("/etc/tiled/profiles"));
        if let Some(p) = dirs::config_dir() {
            out.push(p.join("tiled").join("profiles"));
        }
    }

    if let Some(prefix) = env_prefix() {
        out.push(prefix.join("etc").join("tiled").join("profiles"));
    }

    if let Ok(custom) = std::env::var("TILED_PROFILES") {
        out.push(PathBuf::from(custom));
    } else if let Some(home) = dirs::home_dir() {
        out.push(home.join(".config").join("tiled").join("profiles"));
    }

    if let Some(p) = dirs::config_dir() {
        let user = p.join("tiled").join("profiles");
        if !out.contains(&user) {
            out.push(user);
        }
    }

    // Dedup while preserving order.
    let mut seen = std::collections::HashSet::new();
    out.retain(|p| seen.insert(p.clone()));
    out
}

fn env_prefix() -> Option<PathBuf> {
    // Roughly equivalent to Python's `sys.prefix` — a Python env's root.
    // We approximate by checking VIRTUAL_ENV and CONDA_PREFIX, then fall back
    // to the binary's directory.
    if let Ok(p) = std::env::var("VIRTUAL_ENV") {
        return Some(PathBuf::from(p));
    }
    if let Ok(p) = std::env::var("CONDA_PREFIX") {
        return Some(PathBuf::from(p));
    }
    None
}

/// Result of loading every profile from disk.
#[derive(Debug, Clone, Default)]
pub struct ProfileSet {
    /// Profile name → (source filepath, parsed content).
    pub profiles: HashMap<String, (PathBuf, serde_yaml::Value)>,
}

impl ProfileSet {
    pub fn get(&self, name: &str) -> Option<&(PathBuf, serde_yaml::Value)> {
        self.profiles.get(name)
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.profiles.keys().cloned().collect();
        names.sort();
        names
    }
}

/// Load every YAML profile in the search path. Later directories override
/// earlier ones; same-directory collisions are dropped with a warning.
pub fn load_profiles() -> Result<ProfileSet> {
    let levels = gather_profiles(&paths(), false)?;
    Ok(resolve_precedence(&levels))
}

/// `name → source_filepath` summary, like Python `list_profiles()`.
pub fn list_profiles() -> Result<HashMap<String, PathBuf>> {
    Ok(load_profiles()?
        .profiles
        .into_iter()
        .map(|(k, (p, _))| (k, p))
        .collect())
}

fn gather_profiles(
    paths: &[PathBuf],
    strict: bool,
) -> Result<Vec<HashMap<PathBuf, serde_yaml::Value>>> {
    let mut levels = Vec::with_capacity(paths.len());
    for path in paths {
        let mut level: HashMap<PathBuf, serde_yaml::Value> = HashMap::new();
        if !path.is_dir() {
            levels.push(level);
            continue;
        }
        let entries = match std::fs::read_dir(path) {
            Ok(it) => it,
            Err(_) => {
                levels.push(level);
                continue;
            }
        };
        for entry in entries.flatten() {
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let ext = p.extension().and_then(|e| e.to_str());
            if !matches!(ext, Some("yml") | Some("yaml")) {
                continue;
            }
            let body = match std::fs::read_to_string(&p) {
                Ok(s) => s,
                Err(e) => {
                    if strict {
                        return Err(ClientError::Invalid(format!(
                            "read profile {}: {e}",
                            p.display()
                        )));
                    }
                    tracing::warn!(target: "tiled.profiles", "skipping {}: {e}", p.display());
                    continue;
                }
            };
            let value: serde_yaml::Value = match serde_yaml::from_str(&body) {
                Ok(v) => v,
                Err(e) => {
                    if strict {
                        return Err(ClientError::Invalid(format!(
                            "parse profile {}: {e}",
                            p.display()
                        )));
                    }
                    tracing::warn!(target: "tiled.profiles", "skipping {}: {e}", p.display());
                    continue;
                }
            };
            if !value.is_mapping() {
                if strict {
                    return Err(ClientError::Invalid(format!(
                        "profile file {} has no top-level mapping",
                        p.display()
                    )));
                }
                continue;
            }
            level.insert(p, value);
        }
        levels.push(level);
    }
    Ok(levels)
}

fn resolve_precedence(levels: &[HashMap<PathBuf, serde_yaml::Value>]) -> ProfileSet {
    let mut combined: HashMap<String, (PathBuf, serde_yaml::Value)> = HashMap::new();
    let mut collisions: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for level in levels {
        // Map profile_name → list of files in this level that define it.
        let mut name_to_files: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for (filepath, content) in level {
            if let serde_yaml::Value::Mapping(m) = content {
                for (k, _v) in m {
                    if let Some(name) = k.as_str() {
                        name_to_files
                            .entry(name.to_string())
                            .or_default()
                            .push(filepath.clone());
                    }
                }
            }
        }
        for (name, files) in name_to_files {
            collisions.remove(&name);
            if files.len() > 1 {
                collisions.insert(name.clone(), files);
                combined.remove(&name);
                continue;
            }
            let Some(filepath) = files.into_iter().next() else {
                continue;
            };
            let content = level
                .get(&filepath)
                .and_then(|m| m.as_mapping())
                .and_then(|m| m.get(serde_yaml::Value::String(name.clone())))
                .cloned();
            if let Some(content) = content {
                combined.insert(name, (filepath, content));
            }
        }
    }
    for (name, files) in collisions {
        tracing::warn!(
            target: "tiled.profiles",
            "profile name '{}' defined in multiple files in same directory: {:?}",
            name,
            files
        );
    }
    ProfileSet { profiles: combined }
}

/// Compose a YAML document with one profile.
fn compose_profile(name: &str, uri: &str, verify: bool) -> Result<String> {
    let content = serde_yaml::Mapping::from_iter([(
        serde_yaml::Value::String(name.into()),
        serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([
            (
                serde_yaml::Value::String("uri".into()),
                serde_yaml::Value::String(uri.into()),
            ),
            (
                serde_yaml::Value::String("verify".into()),
                serde_yaml::Value::Bool(verify),
            ),
        ])),
    )]);
    let value = serde_yaml::Value::Mapping(content);
    serde_yaml::to_string(&value).map_err(|e| ClientError::Invalid(format!("yaml dump: {e}")))
}

/// Create a new profile in the user-config directory (highest precedence).
pub fn create_profile(uri: &str, name: &str, verify: bool, overwrite: bool) -> Result<PathBuf> {
    let dir = paths()
        .into_iter()
        .next_back()
        .ok_or_else(|| ClientError::Invalid("no user-config dir available".into()))?;
    std::fs::create_dir_all(&dir)
        .map_err(|e| ClientError::Invalid(format!("mkdir {}: {e}", dir.display())))?;
    let filepath = dir.join(format!("{name}.yml"));
    if filepath.exists() && !overwrite {
        return Err(ClientError::Invalid(format!(
            "profile '{name}' already exists at {} (overwrite=true to replace)",
            filepath.display()
        )));
    }
    let body = compose_profile(name, uri, verify)?;
    std::fs::write(&filepath, body)
        .map_err(|e| ClientError::Invalid(format!("write {}: {e}", filepath.display())))?;
    Ok(filepath)
}

/// Delete the first matching profile (highest precedence wins).
pub fn delete_profile(name: &str) -> Result<Option<PathBuf>> {
    for path in paths().into_iter().rev() {
        for ext in &["yml", "yaml"] {
            let filepath = path.join(format!("{name}.{ext}"));
            if filepath.exists() {
                std::fs::remove_file(&filepath)
                    .map_err(|e| ClientError::Invalid(format!("rm {}: {e}", filepath.display())))?;
                return Ok(Some(filepath));
            }
        }
    }
    Ok(None)
}

fn default_profile_marker_path() -> Option<PathBuf> {
    let last = paths().into_iter().next_back()?;
    Some(
        last.parent()
            .map(|p| p.to_path_buf())
            .unwrap_or(last)
            .join("default_profile"),
    )
}

/// Read the saved default profile name.
pub fn get_default_profile_name() -> Option<String> {
    let p = default_profile_marker_path()?;
    std::fs::read_to_string(&p)
        .ok()
        .map(|s| s.trim().to_string())
}

/// Persist `name` as the default profile (or clear when `None`).
pub fn set_default_profile_name(name: Option<&str>) -> Result<()> {
    let p = default_profile_marker_path()
        .ok_or_else(|| ClientError::Invalid("no user-config dir available".into()))?;
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ClientError::Invalid(format!("mkdir {}: {e}", parent.display())))?;
    }
    match name {
        None => {
            if p.exists() {
                std::fs::remove_file(&p)
                    .map_err(|e| ClientError::Invalid(format!("rm {}: {e}", p.display())))?;
            }
        }
        Some(n) => {
            // Verify the profile exists.
            let profiles = load_profiles()?;
            if !profiles.profiles.contains_key(n) {
                return Err(ClientError::Invalid(format!("profile '{n}' not found")));
            }
            std::fs::write(&p, n)
                .map_err(|e| ClientError::Invalid(format!("write {}: {e}", p.display())))?;
        }
    }
    Ok(())
}

/// One profile parsed into a struct.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Profile {
    pub uri: Option<String>,
    pub api_key: Option<String>,
    pub structure_clients: Option<serde_yaml::Value>,
    pub headers: Option<HashMap<String, String>>,
    pub timeout: Option<f64>,
    pub verify: Option<bool>,
    /// Inline server config (used by Python's `direct:` profiles).
    pub direct: Option<serde_yaml::Value>,
    /// Cache config sub-doc.
    pub cache: Option<serde_yaml::Value>,
    /// Anything we don't model is preserved here.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

impl Profile {
    /// Look up a profile by name and parse it as a [`Profile`].
    pub fn lookup(name: &str) -> Result<(PathBuf, Self)> {
        let set = load_profiles()?;
        let (path, value) = set
            .profiles
            .get(name)
            .cloned()
            .ok_or_else(|| ClientError::Invalid(format!("profile '{name}' not found")))?;
        let profile: Profile = serde_yaml::from_value(value)
            .map_err(|e| ClientError::Invalid(format!("profile parse: {e}")))?;
        Ok((path, profile))
    }
}

/// Build a `Context` from a profile name. Mirrors `from_profile()` in Python
/// (just the `from_uri` arm — `direct:` profiles aren't supported because
/// they require an in-process server).
pub async fn from_profile(name: &str) -> Result<crate::any_client::AnyClient> {
    let (_path, profile) = Profile::lookup(name)?;
    let uri = profile
        .uri
        .ok_or_else(|| ClientError::Invalid(format!("profile '{name}' has no 'uri' field")))?;
    let mut opts = crate::context::ContextOptions::default();
    if let Some(k) = profile.api_key {
        opts = opts.api_key(k);
    }
    crate::constructors::from_uri_with_options(&uri, opts, false).await
}

// Silence unused-import in lib if no caller uses Path directly.
#[allow(dead_code)]
fn _path_token(_p: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_includes_user_config() {
        let ps = paths();
        assert!(!ps.is_empty(), "expected at least one search path");
    }

    #[test]
    fn compose_profile_yaml_round_trip() {
        let yaml = compose_profile("test", "http://localhost:8000", true).unwrap();
        let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let m = value.as_mapping().unwrap();
        let inner = m
            .get(serde_yaml::Value::String("test".into()))
            .unwrap()
            .as_mapping()
            .unwrap();
        assert_eq!(
            inner.get(serde_yaml::Value::String("uri".into())).unwrap(),
            &serde_yaml::Value::String("http://localhost:8000".into())
        );
    }

    #[test]
    fn load_profiles_handles_temp_dir() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("foo.yml");
        std::fs::write(&f, "demo:\n  uri: http://example.com\n  verify: true\n").unwrap();
        let levels = gather_profiles(&[dir.path().to_path_buf()], true).unwrap();
        let resolved = resolve_precedence(&levels);
        assert_eq!(resolved.profiles.len(), 1);
        assert!(resolved.profiles.contains_key("demo"));
    }

    #[test]
    fn collision_in_same_dir_drops_profile() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.yml"), "shared:\n  uri: http://a\n").unwrap();
        std::fs::write(dir.path().join("b.yml"), "shared:\n  uri: http://b\n").unwrap();
        let levels = gather_profiles(&[dir.path().to_path_buf()], false).unwrap();
        let resolved = resolve_precedence(&levels);
        // Both files at same level define 'shared' → omitted.
        assert!(!resolved.profiles.contains_key("shared"));
    }

    #[test]
    fn later_level_overrides_earlier() {
        let lo = tempfile::tempdir().unwrap();
        let hi = tempfile::tempdir().unwrap();
        std::fs::write(lo.path().join("p.yml"), "x:\n  uri: http://low\n").unwrap();
        std::fs::write(hi.path().join("p.yml"), "x:\n  uri: http://high\n").unwrap();
        let levels =
            gather_profiles(&[lo.path().to_path_buf(), hi.path().to_path_buf()], true).unwrap();
        let resolved = resolve_precedence(&levels);
        let (_, content) = resolved.profiles.get("x").unwrap();
        let uri = content
            .as_mapping()
            .and_then(|m| m.get(serde_yaml::Value::String("uri".into())))
            .and_then(|v| v.as_str())
            .unwrap();
        assert_eq!(uri, "http://high");
    }
}
