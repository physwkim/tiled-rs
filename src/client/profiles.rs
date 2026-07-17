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
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::client::context::{ClientTimeout, ContextOptions};
use crate::client::error::{ClientError, Result};

/// Search path. Listed lowest-precedence to highest. The user-config dir is
/// last so it overrides everything else.
pub fn paths() -> Vec<PathBuf> {
    build_paths(
        std::env::var("TILED_SITE_PROFILES").ok(),
        std::env::var("TILED_PROFILES").ok(),
        env_prefix(),
        dirs::home_dir(),
        dirs::config_dir(),
    )
}

/// Pure precedence builder behind [`paths`]. Parameterized on the
/// environment/platform inputs so the ordering can be tested deterministically
/// across platforms (e.g. macOS `config_dir` = `~/Library/Application Support`
/// vs Linux `~/.config`).
///
/// Mirrors `tiled/profiles.py::_all_paths` (lowest → highest precedence):
/// system → env-prefix → user. The OS user-config dir is appended **last** so
/// it has the highest precedence, matching Python's `platformdirs.user_config_dir`
/// entry. It is intentionally NOT pushed in the system branch — doing so pinned
/// it at system precedence and inverted the order on macOS (F4).
fn build_paths(
    site_profiles_env: Option<String>,
    profiles_env: Option<String>,
    env_prefix: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    config_dir: Option<PathBuf>,
) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();

    if let Some(custom) = site_profiles_env {
        out.push(PathBuf::from(custom));
    } else {
        out.push(PathBuf::from("/etc/tiled/profiles"));
    }

    if let Some(prefix) = env_prefix {
        out.push(prefix.join("etc").join("tiled").join("profiles"));
    }

    if let Some(custom) = profiles_env {
        out.push(PathBuf::from(custom));
    } else if let Some(home) = home_dir {
        out.push(home.join(".config").join("tiled").join("profiles"));
    }

    if let Some(p) = config_dir {
        let user = p.join("tiled").join("profiles");
        // Guard against the Linux case where `config_dir` == `~/.config`, so the
        // hard-coded user path above already covers it; otherwise append it as
        // the highest-precedence entry.
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
            let mut value: serde_yaml::Value = match serde_yaml::from_str(&body) {
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
            // Expand $VAR / ${VAR} so a `${SECRET_KEY}` in a profile resolves
            // before use — upstream `parse()` (utils.py) runs
            // `expand_environment_variables` on every loaded profile file.
            crate::env_expand::expand_env_vars(&mut value);
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

/// HTTP client timeouts, in seconds. Mirrors the `timeout` object in Python
/// tiled's client-profiles schema (`tiled/config_schemas/client_profiles.yml`),
/// which types `timeout` strictly as an object with `connection`, `read`,
/// `write`, and `pool` number fields. The upstream schema declares `type:
/// object`, so a bare scalar is rejected there and is not accepted here either.
/// Every field is optional; upstream fills the gaps from `DEFAULT_TIMEOUT_PARAMS`
/// (`tiled/client/constructors.py:241`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub struct ProfileTimeout {
    pub connection: Option<f64>,
    pub read: Option<f64>,
    pub write: Option<f64>,
    pub pool: Option<f64>,
}

/// One profile parsed into a struct.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Profile {
    pub uri: Option<String>,
    pub api_key: Option<String>,
    pub structure_clients: Option<serde_yaml::Value>,
    pub headers: Option<HashMap<String, String>>,
    pub timeout: Option<ProfileTimeout>,
    pub verify: Option<bool>,
    /// Hard ceiling on concurrent connections. Mirrors the schema's
    /// `max_connections`; feeds `ContextOptions::max_connections`.
    pub max_connections: Option<usize>,
    /// Filepath to the token cache directory. Modeled for schema fidelity but
    /// not wired: the Rust client derives the per-server token directory from
    /// the API URI and cannot override it per construction.
    pub token_cache: Option<String>,
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

/// Convert a seconds value into a [`Duration`], rejecting the inputs
/// [`Duration::from_secs_f64`] would panic on (negative, non-finite, overflow)
/// so a malformed profile surfaces an error instead of aborting construction.
fn timeout_secs_to_duration(field: &str, secs: f64) -> Result<Duration> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(ClientError::Invalid(format!(
            "profile timeout.{field} must be a non-negative number of seconds, got {secs}"
        )));
    }
    Ok(Duration::from_secs_f64(secs))
}

/// Map a parsed [`Profile`] onto the resolved URI plus the transport
/// [`ContextOptions`] the Rust client can express. Split out from
/// [`from_profile`] so the field mapping is unit-testable without a live
/// server.
///
/// Mirrors the merge in `tiled/client/constructors.py::from_profile`, honouring
/// every profile field `ContextOptions` can carry: `uri`, `api_key`, `headers`,
/// `verify`, `timeout` (`connection`→connect, `read`→read), `max_connections`.
///
/// Fields the Rust client cannot express are intentionally dropped:
/// - `timeout.write` / `timeout.pool`: reqwest exposes no per-request write or
///   pool-acquire timeout ([`ClientTimeout`] carries neither).
/// - `cache`: `ContextOptions::cache` is an `HttpCache` that cannot express the
///   schema's `readonly` / `max_item_size`, so honouring the config partially
///   would silently ignore a safety setting.
/// - `structure_clients`: the Rust client has no numpy/dask or import-path
///   client dispatch to bind these to.
/// - `token_cache`: the token-cache directory is derived from the API URI and
///   is not overridable per construction.
/// - `direct`: requires an in-process ASGI server the Rust client has no
///   analogue for.
/// - `username` / `auth_provider`: deprecated and ignored upstream too.
fn profile_to_options(name: &str, profile: Profile) -> Result<(String, ContextOptions)> {
    let uri = profile
        .uri
        .ok_or_else(|| ClientError::Invalid(format!("profile '{name}' has no 'uri' field")))?;
    let mut opts = ContextOptions::default();
    if let Some(k) = profile.api_key {
        opts = opts.api_key(k);
    }
    if let Some(headers) = profile.headers {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (k, v) in headers {
            let header_name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| ClientError::Invalid(format!("invalid header name '{k}': {e}")))?;
            let value = HeaderValue::from_str(&v).map_err(|e| {
                ClientError::Invalid(format!("invalid value for header '{k}': {e}"))
            })?;
            map.insert(header_name, value);
        }
        opts = opts.headers(map);
    }
    if let Some(verify) = profile.verify {
        opts = opts.verify(verify);
    }
    if let Some(t) = profile.timeout {
        // connection→connect, read→read; write/pool have no reqwest analogue.
        opts = opts.timeout(ClientTimeout {
            connect: t
                .connection
                .map(|s| timeout_secs_to_duration("connection", s))
                .transpose()?,
            read: t
                .read
                .map(|s| timeout_secs_to_duration("read", s))
                .transpose()?,
        });
    }
    if let Some(mc) = profile.max_connections {
        opts = opts.max_connections(mc);
    }
    Ok((uri, opts))
}

/// Build a `Context` from a profile name. Mirrors `from_profile()` in Python
/// (just the `from_uri` arm — `direct:` profiles aren't supported because
/// they require an in-process server). Every profile field the Rust client can
/// express is wired through [`profile_to_options`].
pub async fn from_profile(name: &str) -> Result<crate::client::any_client::AnyClient> {
    let (_path, profile) = Profile::lookup(name)?;
    let (uri, opts) = profile_to_options(name, profile)?;
    crate::client::constructors::from_uri_with_options(&uri, opts, false).await
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

    // w28-F3: a loaded profile has $VAR / ${VAR} expanded (upstream
    // utils.expand_environment_variables, run from `parse()` on every profile
    // file). Uses the already-set PATH var so nothing is mutated — mutating
    // process env would race the parallel test binary.
    #[test]
    fn gather_profiles_expands_set_env_var() {
        let path_val = std::env::var("PATH").expect("PATH is set in the test env");
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("p.yml"),
            "my_profile:\n  uri: \"${PATH}\"\n  api_key: \"$PATH\"\n",
        )
        .unwrap();

        let levels = gather_profiles(&[dir.path().to_path_buf()], true).unwrap();
        let value = levels
            .iter()
            .flat_map(|m| m.values())
            .next()
            .expect("one profile file parsed");
        // Both the braced and bare forms in the profile expand.
        assert_eq!(value["my_profile"]["uri"].as_str().unwrap(), path_val);
        assert_eq!(value["my_profile"]["api_key"].as_str().unwrap(), path_val);

        // And the expansion survives typed deserialization into a Profile.
        let profile: Profile =
            serde_yaml::from_value(value["my_profile"].clone()).expect("profile parse");
        assert_eq!(profile.uri.as_deref(), Some(path_val.as_str()));
        assert_eq!(profile.api_key.as_deref(), Some(path_val.as_str()));
    }

    // FINDING 1: an upstream-shaped profile must carry every expressible field
    // (uri, api_key, headers, verify, timeout, max_connections) into the
    // ContextOptions the client is built from — not just uri + api_key.
    #[test]
    fn profile_to_options_maps_all_expressible_fields() {
        let yaml = r#"
uri: https://tiled.example.com/api/v1
api_key: secret-key
verify: false
max_connections: 4
headers:
  X-Custom: hello
  X-Extra: world
timeout:
  connection: 5.0
  read: 30.0
  write: 30.0
  pool: 5.0
"#;
        let profile: Profile = serde_yaml::from_str(yaml).expect("profile parses");
        let (uri, opts) = profile_to_options("demo", profile).expect("maps");
        assert_eq!(uri, "https://tiled.example.com/api/v1");
        assert_eq!(opts.api_key.as_deref(), Some("secret-key"));
        assert_eq!(opts.verify, Some(false));
        assert_eq!(opts.max_connections, Some(4));
        let headers = opts.headers.expect("headers mapped");
        assert_eq!(headers.get("X-Custom").unwrap().to_str().unwrap(), "hello");
        assert_eq!(headers.get("X-Extra").unwrap().to_str().unwrap(), "world");
        let t = opts.timeout.expect("timeout mapped");
        // connection→connect, read→read; write/pool are dropped (no field).
        assert_eq!(t.connect, Some(Duration::from_secs(5)));
        assert_eq!(t.read, Some(Duration::from_secs(30)));
    }

    // Boundary: a partial timeout leaves the omitted knob None so the client
    // builder falls back to its module default (parity with upstream's
    // merge-over-DEFAULT_TIMEOUT_PARAMS).
    #[test]
    fn profile_to_options_partial_timeout_leaves_connect_none() {
        let yaml = "uri: http://localhost:8000\ntimeout:\n  read: 60.0\n";
        let profile: Profile = serde_yaml::from_str(yaml).unwrap();
        let (_uri, opts) = profile_to_options("p", profile).unwrap();
        let t = opts.timeout.expect("timeout set");
        assert_eq!(t.read, Some(Duration::from_secs(60)));
        assert_eq!(t.connect, None);
    }

    // Boundary: a uri-only profile sets no transport overrides — the resulting
    // client is byte-for-byte the bare-client default.
    #[test]
    fn profile_to_options_uri_only_has_no_overrides() {
        let profile: Profile = serde_yaml::from_str("uri: http://localhost:8000\n").unwrap();
        let (uri, opts) = profile_to_options("p", profile).unwrap();
        assert_eq!(uri, "http://localhost:8000");
        assert!(opts.api_key.is_none());
        assert!(opts.headers.is_none());
        assert!(opts.verify.is_none());
        assert!(opts.timeout.is_none());
        assert!(opts.max_connections.is_none());
    }

    // Boundary: a profile with no uri is rejected with a clear message.
    #[test]
    fn profile_to_options_missing_uri_errors() {
        let profile: Profile = serde_yaml::from_str("verify: true\n").unwrap();
        let err = profile_to_options("noname", profile).unwrap_err();
        assert!(format!("{err}").contains("no 'uri' field"), "{err}");
    }

    // Boundary: a negative timeout would panic Duration::from_secs_f64; it must
    // surface as an error instead of aborting construction.
    #[test]
    fn profile_to_options_rejects_negative_timeout() {
        let yaml = "uri: http://x\ntimeout:\n  read: -1.0\n";
        let profile: Profile = serde_yaml::from_str(yaml).unwrap();
        let err = profile_to_options("p", profile).unwrap_err();
        assert!(format!("{err}").contains("non-negative"), "{err}");
    }

    // Boundary: a header value carrying a control character is not a valid
    // HeaderValue and must be reported, not silently dropped.
    #[test]
    fn profile_to_options_invalid_header_value_errors() {
        let yaml = "uri: http://x\nheaders:\n  X-Bad: \"a\\nb\"\n";
        let profile: Profile = serde_yaml::from_str(yaml).unwrap();
        let err = profile_to_options("p", profile).unwrap_err();
        assert!(
            format!("{err}").contains("invalid value for header"),
            "{err}"
        );
    }

    // FINDING 2: an upstream-shaped profile types `timeout` as an object
    // {connection, read, write, pool}. The prior `Option<f64>` field made such
    // a profile fail to deserialize entirely; it must now parse.
    #[test]
    fn profile_timeout_upstream_object_deserializes() {
        let yaml = r#"
uri: http://localhost:8000
timeout:
  connection: 5.0
  read: 30.0
  write: 30.0
  pool: 5.0
"#;
        let profile: Profile = serde_yaml::from_str(yaml).expect("upstream profile must parse");
        let t = profile.timeout.expect("timeout object must be present");
        assert_eq!(t.connection, Some(5.0));
        assert_eq!(t.read, Some(30.0));
        assert_eq!(t.write, Some(30.0));
        assert_eq!(t.pool, Some(5.0));
    }

    // Boundary: every timeout field is optional. A partial object leaves the
    // omitted knobs as None (upstream fills them from DEFAULT_TIMEOUT_PARAMS).
    #[test]
    fn profile_timeout_partial_object_leaves_missing_none() {
        let yaml = "timeout:\n  read: 60.0\n";
        let profile: Profile = serde_yaml::from_str(yaml).expect("partial timeout must parse");
        let t = profile.timeout.expect("timeout object must be present");
        assert_eq!(t.read, Some(60.0));
        assert_eq!(t.connection, None);
        assert_eq!(t.write, None);
        assert_eq!(t.pool, None);
    }

    // Boundary: a profile with no `timeout` key parses with `timeout == None`.
    #[test]
    fn profile_without_timeout_is_none() {
        let profile: Profile =
            serde_yaml::from_str("uri: http://localhost:8000\n").expect("must parse");
        assert!(profile.timeout.is_none());
    }

    // F4: the OS user-config dir must be the highest-precedence (last) entry,
    // not demoted to system precedence. Exercised via the pure builder so the
    // macOS layout (config_dir != ~/.config) is deterministic on any platform.
    #[test]
    fn user_config_dir_is_highest_precedence_macos_layout() {
        let home = PathBuf::from("/Users/me");
        let config_dir = PathBuf::from("/Users/me/Library/Application Support");
        let ps = build_paths(
            None,
            None,
            None,
            Some(home.clone()),
            Some(config_dir.clone()),
        );

        let user = config_dir.join("tiled").join("profiles");
        assert_eq!(
            ps.last(),
            Some(&user),
            "user_config_dir/tiled/profiles must be last (highest precedence): {ps:?}"
        );
        // It must NOT also appear at a lower (system) position.
        let first = ps.iter().position(|p| p == &user).unwrap();
        assert_eq!(first, ps.len() - 1, "user dir must not be demoted: {ps:?}");
        // The hard-coded ~/.config user path stays strictly below it.
        let xdg = home.join(".config").join("tiled").join("profiles");
        let xdg_idx = ps.iter().position(|p| p == &xdg).unwrap();
        assert!(
            xdg_idx < first,
            "~/.config must rank below user_config_dir: {ps:?}"
        );
        // The system path is below the user dirs.
        assert_eq!(ps.first(), Some(&PathBuf::from("/etc/tiled/profiles")));
    }

    #[test]
    fn user_config_dir_collapses_on_linux_layout() {
        let home = PathBuf::from("/home/me");
        // Linux: dirs::config_dir() == ~/.config, so it coincides with the
        // hard-coded user path and must appear exactly once, last.
        let config_dir = PathBuf::from("/home/me/.config");
        let ps = build_paths(
            None,
            None,
            None,
            Some(home.clone()),
            Some(config_dir.clone()),
        );

        let user = config_dir.join("tiled").join("profiles");
        assert_eq!(ps.last(), Some(&user));
        assert_eq!(
            ps.iter().filter(|p| **p == user).count(),
            1,
            "no duplicate user dir on Linux: {ps:?}"
        );
    }

    #[test]
    fn site_env_overrides_system_entry_user_dir_still_last() {
        let home = PathBuf::from("/Users/me");
        let config_dir = PathBuf::from("/Users/me/Library/Application Support");
        let ps = build_paths(
            Some("/custom/site/profiles".into()),
            None,
            None,
            Some(home),
            Some(config_dir.clone()),
        );
        // TILED_SITE_PROFILES replaces the hard-coded /etc entry.
        assert_eq!(ps.first(), Some(&PathBuf::from("/custom/site/profiles")));
        assert!(!ps.contains(&PathBuf::from("/etc/tiled/profiles")));
        // And the user config dir is still last.
        assert_eq!(ps.last(), Some(&config_dir.join("tiled").join("profiles")));
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
