//! Configuration file parsing for tiled-rs.
//!
//! Supports YAML config files compatible with the databroker/Tiled config format.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Context;
use serde::Deserialize;

use tiled_access::{AccessPolicy, PassthroughPolicy, TagBasedPolicy};
use tiled_auth::ScopeSet;

/// Top-level configuration.
#[derive(Debug, Deserialize)]
pub struct TiledConfig {
    #[serde(default)]
    pub trees: Vec<TreeConfig>,
    /// Optional `catalog:` block — Python tiled's *recommended* single-catalog
    /// config form (`config.py:271`). Mutually exclusive with `trees:`
    /// (Python `reconcile_catalog_and_trees`, `config.py:331`): a config may
    /// specify the recommended single `catalog:` or the advanced multi-tree
    /// `trees:`, never both. See [`CatalogConfig`].
    #[serde(default)]
    pub catalog: Option<CatalogConfig>,
    #[serde(default)]
    pub authentication: Option<AuthConfig>,
    /// Optional `web:` section. Right now only `spec_views` is read —
    /// mirrors upstream tiled PR #1349.
    #[serde(default)]
    pub web: Option<WebConfig>,
    /// Optional `access_control:` section selecting and configuring the
    /// per-node access policy. Mirrors Python tiled's `access_control:`
    /// config block (an `access_policy` selector plus `args`). Absent →
    /// the server keeps its default behaviour (PassthroughPolicy when an
    /// auth DB is configured, no policy otherwise).
    #[serde(default)]
    pub access_control: Option<AccessControlConfig>,
    /// Maximum decoded size (bytes) of a single array/table data response.
    /// Mirrors Python tiled's top-level `response_bytesize_limit:` setting
    /// (config.py:279, default 300_000_000). Read handlers return 400 when
    /// the decoded payload would exceed this.
    #[serde(default = "default_response_bytesize_limit")]
    pub response_bytesize_limit: usize,
    /// Per-request timeout in seconds (L5). Default: 30.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Whether the raw-asset download endpoints (`/api/v1/asset/bytes` and
    /// `/api/v1/asset/manifest`) may serve backing files from disk. Mirrors
    /// Python `expose_raw_assets`, which defaults to `True` (settings.py:57).
    #[serde(default = "default_expose_raw_assets")]
    pub expose_raw_assets: bool,
    /// CORS allowed origins. Mirrors Python tiled's top-level
    /// `allow_origins:` (`config.py:281`). A single `"*"` entry means
    /// permissive (any origin). Empty → only same-origin requests pass.
    /// The `--allow-origin` CLI flag, when given, takes precedence over
    /// this list.
    #[serde(default)]
    pub allow_origins: Vec<String>,
    /// The largest number of matching nodes for which the search/list endpoint
    /// returns an exact `COUNT(*)` total. When the true count exceeds this
    /// value the reported `meta.count` is capped at this limit (the lower
    /// bound). Mirrors Python `Settings.exact_count_limit` (`settings.py`,
    /// default 100).
    #[serde(default = "default_exact_count_limit")]
    pub exact_count_limit: u64,
    /// Catch-all for config keys the Rust port does not yet model.
    /// Captured keys are warn-logged once at startup (see
    /// [`TiledConfig::warn_unknown_keys`]) so operators know their
    /// setting had no effect, instead of being silently dropped.
    #[serde(flatten)]
    pub unknown: BTreeMap<String, serde_yaml::Value>,
}

/// Default for [`TiledConfig::response_bytesize_limit`] — 300 MB, matching
/// Python tiled (`settings.py:40`).
pub fn default_response_bytesize_limit() -> usize {
    300_000_000
}

/// Default for [`TiledConfig::request_timeout_secs`] — 30 s.
pub fn default_request_timeout_secs() -> u64 {
    30
}

/// Default for [`TiledConfig::expose_raw_assets`] — `true`, matching Python
/// tiled (`settings.py:57`).
pub fn default_expose_raw_assets() -> bool {
    true
}

/// Default for [`TiledConfig::exact_count_limit`] — 100, matching Python
/// tiled (`Settings.exact_count_limit`, `settings.py`).
pub fn default_exact_count_limit() -> u64 {
    100
}

// `Default` is hand-written (not derived) so `response_bytesize_limit` agrees
// with its serde default (300 MB) instead of `usize::default()` (0), which
// would otherwise make every response "exceed" the limit.
impl Default for TiledConfig {
    fn default() -> Self {
        Self {
            trees: Vec::new(),
            catalog: None,
            authentication: None,
            web: None,
            access_control: None,
            response_bytesize_limit: default_response_bytesize_limit(),
            request_timeout_secs: default_request_timeout_secs(),
            expose_raw_assets: default_expose_raw_assets(),
            allow_origins: Vec::new(),
            exact_count_limit: default_exact_count_limit(),
            unknown: BTreeMap::new(),
        }
    }
}

/// `access_control:` block.
///
/// Mirrors the shape of Python tiled's `access_control:` config: an
/// `access_policy` selector plus a policy-specific `args` map. Python uses
/// an import path (`"tiled...:TagBasedAccessPolicy"`); Rust cannot import
/// arbitrary code at runtime, so `access_policy` is a short built-in
/// selector instead.
///
/// ```yaml
/// access_control:
///   access_policy: tag_based       # passthrough | none | tag_based
///   args:
///     default_scopes: [read:metadata, read:data]   # optional; default read-only
///     grants:                                       # required for tag_based
///       "11111111-1111-1111-1111-111111111111": [team-a]
///       "22222222-2222-2222-2222-222222222222": [team-b, team-c]
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct AccessControlConfig {
    /// Built-in policy selector: `passthrough`/`none` (the default
    /// behaviour — narrows nothing) or `tag_based`.
    pub access_policy: String,
    /// Policy-specific arguments. Ignored for `passthrough`/`none`.
    #[serde(default)]
    pub args: AccessControlArgs,
}

/// Arguments for the selected access policy. Only the fields the chosen
/// policy consumes are read; the rest stay at their defaults.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AccessControlArgs {
    /// Effective scopes a principal receives on a node whose tags they are
    /// granted (`tag_based`). Parsed as a list of scope strings, e.g.
    /// `[read:metadata, read:data]`. Defaults to read-only.
    #[serde(default)]
    pub default_scopes: Option<ScopeSet>,
    /// Map of principal UUID → granted tags (`tag_based`). A node is visible
    /// to a principal iff it is untagged (public) or carries a tag the
    /// principal was granted here. Required for `tag_based`.
    #[serde(default)]
    pub grants: Option<HashMap<String, Vec<String>>>,
}

impl AccessControlConfig {
    /// Construct the configured policy. Returns a clear error for an unknown
    /// policy name or for `tag_based` missing its required `args`.
    pub fn build(&self) -> anyhow::Result<Arc<dyn AccessPolicy>> {
        match self.access_policy.as_str() {
            "passthrough" | "none" => Ok(Arc::new(PassthroughPolicy)),
            "tag_based" => {
                let grants = self.args.grants.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "access_control: policy 'tag_based' requires 'args.grants' \
                         (a map of principal UUID -> [tags])"
                    )
                })?;
                let default_scopes = self
                    .args
                    .default_scopes
                    .clone()
                    .unwrap_or_else(ScopeSet::read_only);
                let mut policy = TagBasedPolicy::new(default_scopes);
                for (uuid, tags) in grants {
                    for tag in tags {
                        policy.grant(uuid, tag);
                    }
                }
                Ok(Arc::new(policy))
            }
            other => Err(anyhow::anyhow!(
                "access_control: unknown access_policy '{other}' \
                 (expected 'passthrough', 'none', or 'tag_based')"
            )),
        }
    }
}

/// `web:` block. Currently only spec-view registrations live here;
/// other web-related options (theming, auth-button labels, etc.) can
/// land here later without breaking compatibility.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct WebConfig {
    /// External viewer registrations exposed via `GET /settings.json`.
    #[serde(default)]
    pub spec_views: Vec<SpecViewConfig>,
}

/// One `spec_views[*]` entry. Wire-compatible with `tiled_web::SpecViewEntry`
/// — we keep a separate type so the CLI doesn't depend on tiled-web when
/// the `web` feature is off.
#[derive(Debug, Clone, Deserialize)]
pub struct SpecViewConfig {
    pub spec: String,
    pub url: String,
    #[serde(default)]
    pub label: Option<String>,
}

/// A single tree (data source) definition.
#[derive(Debug, Deserialize)]
pub struct TreeConfig {
    /// URL path where this tree is mounted (e.g. "/raw").
    /// Stored for forward compatibility with Python tiled's
    /// multi-tree configs; the Rust server currently mounts a single
    /// tree at the API root.
    #[allow(dead_code)]
    #[serde(default = "default_path")]
    pub path: String,
    /// Adapter type (e.g. "mongo_normalized").
    #[serde(alias = "tree")]
    pub adapter: String,
    /// Arguments passed to the adapter.
    #[serde(default)]
    pub args: TreeArgs,
}

/// Arguments for a tree adapter.
#[derive(Debug, Deserialize, Default)]
pub struct TreeArgs {
    /// MongoDB URI.
    pub uri: Option<String>,
    /// Handler registry — kept for Python tiled config compatibility.
    /// The Rust server uses the built-in handler registry; user-supplied
    /// handler factories aren't wired up yet.
    #[allow(dead_code)]
    #[serde(default)]
    pub handler_registry: std::collections::HashMap<String, String>,
}

/// One entry in `authentication.tiled_admins`. Mirrors Python's `TiledAdmin`
/// model (`config.py`): the `(provider, id)` pair identifies the external
/// identity that should be bootstrapped to the `"admin"` role at startup.
#[derive(Debug, Clone, Deserialize)]
pub struct TiledAdmin {
    /// The authentication provider name (e.g. `"ldap"`, `"oidc"`,
    /// `"password"`). Matches the `provider` column of the `identities` table.
    pub provider: String,
    /// The subject identifier within that provider (the `sub` / external `id`
    /// value stored in the `identities` table). Mirrors Python's `id` field on
    /// `TiledAdmin`.
    pub id: String,
}

/// Authentication configuration.
#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    /// Accepted from Python tiled configs but not yet wired into the
    /// Rust server's auth middleware (single-user API key is the only
    /// auth path today).
    #[allow(dead_code)]
    #[serde(default)]
    pub allow_anonymous_access: bool,
    pub single_user_api_key: Option<String>,
    /// HMAC-SHA256 signing secrets for JWT tokens, supporting key rotation.
    /// Mirrors Python `Authentication.secret_keys` (`config.py:147`).
    /// The **first** secret signs new tokens; all are tried in order when
    /// verifying (rotation: prepend the new secret, keep the old one until
    /// its tokens expire). Env `TILED_SECRET_KEYS` (JSON array) takes
    /// precedence over this field; `--jwt-secret` is the fallback.
    #[serde(default)]
    pub secret_keys: Option<Vec<String>>,
    /// Access-token lifetime in seconds. Mirrors Python
    /// `Authentication.access_token_max_age` (`config.py:150`, default 900 s).
    /// Only takes effect in multi-user mode (`--auth-db-uri`). When absent
    /// the compiled-in default (15 min) is used.
    #[serde(default)]
    pub access_token_max_age: Option<f64>,
    /// Refresh-token lifetime in seconds. Mirrors Python
    /// `Authentication.refresh_token_max_age` (`config.py:151`, default 7 d).
    /// Only takes effect in multi-user mode (`--auth-db-uri`). When absent
    /// the compiled-in default (7 days) is used.
    #[serde(default)]
    pub refresh_token_max_age: Option<f64>,
    /// Principals that must have the `"admin"` role at server startup.
    /// Mirrors Python `Authentication.tiled_admins` (`config.py`). The server
    /// bootstraps each `(provider, id)` pair idempotently: the identity is
    /// created if absent, then the principal's role is set to `"admin"`.
    /// Only takes effect in multi-user mode (`--auth-db-uri`).
    #[serde(default)]
    pub tiled_admins: Vec<TiledAdmin>,
    /// Catch-all for authentication config keys the Rust port does not yet
    /// model (e.g. `providers`, `session_max_age`).
    /// Warn-logged at startup.
    #[serde(flatten)]
    pub unknown: BTreeMap<String, serde_yaml::Value>,
}

/// `catalog:` block — Python tiled's recommended config form
/// (`config.py:60`, `CatalogConfig`). Specifies a single persistent catalog
/// (SQLite/Postgres) by URI.
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogConfig {
    /// SQLite or Postgres URI for the persistent catalog
    /// (e.g. `sqlite:///path/to/catalog.db`).
    pub uri: String,
    /// Directories the server may create internally-managed storage in.
    /// Mirrors Python `CatalogConfig.writable_storage` (`config.py:64`).
    /// CLI `--writable-storage` takes the union; these config-file paths
    /// are also folded into the read allow-list (writable ⊆ readable).
    #[serde(default)]
    pub writable_storage: Vec<String>,
    /// Directories the file-backed read resolver is allowed to serve files
    /// from. Mirrors Python `CatalogConfig.readable_storage` (`config.py:65`).
    /// CLI `--allowed-data-dir` takes the union.
    #[serde(default)]
    pub readable_storage: Vec<String>,
    /// Whether to create the catalog DB when it does not exist.
    /// Mirrors Python `CatalogConfig.init_if_not_exists` (`config.py:66`).
    /// The Rust server always runs `migrate()` which creates the schema if
    /// absent (equivalent to `true`); this field is parsed so a standard
    /// Python config with `init_if_not_exists: false` does not trigger the
    /// unknown-key warning, but the value has no effect.
    #[serde(default)]
    pub init_if_not_exists: bool,
    /// Maximum number of connections in the sqlx pool for the catalog DB.
    /// Mirrors Python `CatalogConfig.catalog_pool_size` (`config.py`, default
    /// 5). Absent → the pool's compiled-in default (8 for SQLite, 16 for
    /// Postgres). Passed to `PoolOptions::max_connections` at pool creation.
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,
    /// Maximum connections for the storage adapter pool. Mirrors Python
    /// `CatalogConfig.storage_pool_size` (`config.py`, default 5). Parsed to
    /// remove the unknown-key warning; the Rust server has no separate storage
    /// SQL pool (file adapters open/close per request), so this value is
    /// accepted but has no effect.
    #[serde(default)]
    pub storage_pool_size: Option<u32>,
    /// SQLAlchemy-style overflow allowance for the catalog pool. Mirrors Python
    /// `CatalogConfig.catalog_max_overflow` (`config.py`, default 10). Parsed
    /// to remove the unknown-key warning; sqlx pools have no overflow concept
    /// (`max_connections` is the hard total), so this value is accepted but has
    /// no effect.
    #[serde(default)]
    pub catalog_max_overflow: Option<u32>,
    /// SQLAlchemy-style overflow for the storage pool. Mirrors Python
    /// `CatalogConfig.storage_max_overflow` (`config.py`, default 10). Parsed
    /// to remove the unknown-key warning; no effect in Rust.
    #[serde(default)]
    pub storage_max_overflow: Option<u32>,
    /// Catch-all for catalog config keys the Rust port does not yet model
    /// (e.g. `metadata`, `specs`, `adapters_by_mimetype`, `mount_node`).
    /// Warn-logged at startup.
    #[serde(flatten)]
    pub unknown: BTreeMap<String, serde_yaml::Value>,
}

fn default_path() -> String {
    "/".to_string()
}

impl TiledConfig {
    /// Load configuration from a YAML file *or* a directory of YAML files.
    ///
    /// When `path` is a directory, every `*.yml`/`*.yaml` file in it is merged
    /// (cli-M6, Python `parse_configs`, `config.py:472`): `trees:` lists
    /// concatenate across files; any other top-level key appearing in more
    /// than one file is a conflict. `TILED_*` env vars then override the
    /// merged file values, matching Python's env-over-file precedence
    /// (`settings_customise_sources`, `config.py:48`).
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let p = std::path::Path::new(path);
        let mut config: Self = if p.is_dir() {
            Self::from_dir(p)?
        } else {
            // Name the path in both failure modes: a bare io::Error
            // ("No such file or directory") or serde_yaml parse error gives the
            // user no way to tell which config file was at fault.
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("reading config file {path}"))?;
            serde_yaml::from_str(&content).with_context(|| format!("parsing config file {path}"))?
        };
        config.apply_env_overrides()?;
        config
            .reconcile_catalog_and_trees()
            .with_context(|| format!("in config {path}"))?;
        config.warn_unknown_keys();
        Ok(config)
    }

    /// Merge every `*.yml`/`*.yaml` file in a config directory (cli-M6).
    ///
    /// Files are processed in sorted filename order so numeric `config.d`
    /// prefixes (`00-base.yml`, `10-trees.yml`) layer predictably — Python
    /// iterates in arbitrary filesystem order (`config.py:476`), which makes
    /// the merge result order-dependent; sorting removes that nondeterminism.
    fn from_dir(dir: &std::path::Path) -> anyhow::Result<Self> {
        use serde_yaml::Value;

        // Top-level keys whose list values concatenate across files — Python's
        // `mergeable_lists` (`config.py:484`) is {allow_origins, specs, trees};
        // `specs` is not yet a field of this Rust config.
        const MERGEABLE: &[&str] = &["allow_origins", "trees"];

        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .with_context(|| format!("reading config directory {}", dir.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.is_file()
                    && matches!(
                        p.extension().and_then(|e| e.to_str()),
                        Some("yml") | Some("yaml")
                    )
            })
            .collect();
        files.sort();

        let mut merged = serde_yaml::Mapping::new();
        for f in &files {
            let content = std::fs::read_to_string(f)
                .with_context(|| format!("reading config file {}", f.display()))?;
            let value: Value = serde_yaml::from_str(&content)
                .with_context(|| format!("parsing config file {}", f.display()))?;
            let map = match value {
                Value::Mapping(map) => map,
                // An empty file parses to Null — skip it.
                Value::Null => continue,
                _ => anyhow::bail!("config file {} is not a YAML mapping", f.display()),
            };
            for (k, v) in map {
                let key_str = k.as_str().unwrap_or_default().to_string();
                if merged.contains_key(&k) {
                    if MERGEABLE.contains(&key_str.as_str()) {
                        match (merged.get_mut(&k).expect("key present"), v) {
                            (Value::Sequence(existing), Value::Sequence(new)) => {
                                existing.extend(new)
                            }
                            _ => anyhow::bail!(
                                "config key '{key_str}' must be a list to merge \
                                 across files (in {})",
                                f.display()
                            ),
                        }
                    } else {
                        anyhow::bail!("duplicate configuration for '{key_str}' in {}", f.display());
                    }
                } else {
                    merged.insert(k, v);
                }
            }
        }

        serde_yaml::from_value(Value::Mapping(merged))
            .with_context(|| format!("merging config directory {}", dir.display()))
    }

    /// Overlay `TILED_*` environment variables on the file/dir config
    /// (cli-M6). Python gives env vars priority over the config file for all
    /// `TILED_*`-prefixed settings (`settings_customise_sources`). Rust applies
    /// this for the config-only scalar fields the server consumes; the
    /// single-user API key already takes env priority via clap
    /// (`--api-key`'s `env`) and [`Self::api_key`]. Nested structures
    /// (`trees`, `authentication`, …) are not env-mapped — pydantic's
    /// `__`-delimited nested env is rarely used and out of scope here.
    fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        self.overlay_response_bytesize_limit(
            std::env::var("TILED_RESPONSE_BYTESIZE_LIMIT")
                .ok()
                .as_deref(),
        )
    }

    /// Pure core of the `TILED_RESPONSE_BYTESIZE_LIMIT` overlay (cli-M6),
    /// separated from the env read so the parse is unit-testable. `None`
    /// (unset) leaves the current value; an invalid value is a hard error
    /// (fail fast, matching pydantic's `ValidationError` on a bad env value).
    fn overlay_response_bytesize_limit(&mut self, raw: Option<&str>) -> anyhow::Result<()> {
        if let Some(raw) = raw {
            self.response_bytesize_limit = raw.trim().parse::<usize>().with_context(|| {
                format!("TILED_RESPONSE_BYTESIZE_LIMIT={raw:?} is not a valid byte count")
            })?;
        }
        Ok(())
    }

    /// Enforce Python's `catalog`/`trees` mutual exclusion
    /// (`config.py:331`, `reconcile_catalog_and_trees`): the recommended
    /// single `catalog:` block and the advanced multi-tree `trees:` list may
    /// not both be specified. Validation-only — unlike Python, the Rust
    /// server keeps the two representations separate and bridges `catalog:`
    /// into the catalog via [`Self::catalog_uri`].
    fn reconcile_catalog_and_trees(&self) -> anyhow::Result<()> {
        if self.catalog.is_some() && !self.trees.is_empty() {
            anyhow::bail!(
                "The configuration 'catalog' specifies a single catalog, whereas \
                 'trees' can specify multiple catalogs. It is not allowed to use \
                 both 'catalog' and 'trees'."
            );
        }
        Ok(())
    }

    /// Emit a `tracing::warn!` for every config key at each level that the
    /// Rust port does not model. Called once at startup after a successful
    /// parse so operators learn which keys had no effect rather than having
    /// them silently vanish.
    pub fn warn_unknown_keys(&self) {
        for key in self.unknown.keys() {
            tracing::warn!("config key '{key}' is not modelled in tiled-rs and has no effect");
        }
        if let Some(auth) = &self.authentication {
            for key in auth.unknown.keys() {
                tracing::warn!(
                    "config key 'authentication.{key}' is not modelled in tiled-rs \
                     and has no effect"
                );
            }
        }
        if let Some(catalog) = &self.catalog {
            for key in catalog.unknown.keys() {
                tracing::warn!(
                    "config key 'catalog.{key}' is not modelled in tiled-rs and has no effect"
                );
            }
        }
    }

    /// Extract the MongoDB URI from the first tree that looks like a mongo adapter.
    pub fn mongo_uri(&self) -> Option<&str> {
        self.trees.iter().find_map(|t| {
            if t.adapter.contains("mongo") || t.adapter.contains("Mongo") {
                t.args.uri.as_deref()
            } else {
                None
            }
        })
    }

    /// Extract the persistent-catalog URI: from the recommended `catalog:`
    /// block, or from a `trees:` entry whose adapter selects the catalog
    /// (Python `TREE_ALIASES = {"catalog": "tiled.catalog:from_uri"}`,
    /// `config.py:39`). Symmetric with [`Self::mongo_uri`].
    pub fn catalog_uri(&self) -> Option<&str> {
        if let Some(catalog) = &self.catalog {
            return Some(catalog.uri.as_str());
        }
        self.trees.iter().find_map(|t| {
            if t.adapter.contains("catalog") || t.adapter.contains("Catalog") {
                t.args.uri.as_deref()
            } else {
                None
            }
        })
    }

    /// Extract the API key from authentication config or env var.
    pub fn api_key(&self) -> Option<String> {
        self.authentication
            .as_ref()
            .and_then(|a| a.single_user_api_key.clone())
            .or_else(|| std::env::var("TILED_SINGLE_USER_API_KEY").ok())
    }

    /// CORS allowed origins declared in the config file (`allow_origins:`).
    pub fn allow_origins(&self) -> &[String] {
        &self.allow_origins
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_bytesize_limit_defaults_and_overrides() {
        // Absent in YAML → 300 MB (serde default).
        let cfg: TiledConfig = serde_yaml::from_str("trees: []").unwrap();
        assert_eq!(cfg.response_bytesize_limit, 300_000_000);
        // `Default` agrees with the serde default (not usize::default() == 0).
        assert_eq!(TiledConfig::default().response_bytesize_limit, 300_000_000);
        // Explicit value is honored.
        let cfg: TiledConfig = serde_yaml::from_str("response_bytesize_limit: 42").unwrap();
        assert_eq!(cfg.response_bytesize_limit, 42);
    }

    // cli-L4: a failed read must name the offending config path.
    #[test]
    fn from_file_read_error_names_the_path() {
        let missing = "/nonexistent/dir/tiled-cli-test-config.yml";
        let err = TiledConfig::from_file(missing).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(missing),
            "read error must name the config path; got: {rendered}"
        );
    }

    #[test]
    fn from_file_parse_error_names_the_path() {
        // Write invalid YAML to a temp file so the read succeeds but the parse
        // fails — the parse error must also name the path.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yml");
        std::fs::write(&path, "trees: [unterminated").unwrap();
        let path_str = path.to_str().unwrap();
        let err = TiledConfig::from_file(path_str).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(path_str),
            "parse error must name the config path; got: {rendered}"
        );
    }

    // cli-M4: the recommended `catalog: {uri: ...}` block must resolve to the
    // catalog URI the server opens — without it a valid Python config never
    // starts a server.
    #[test]
    fn catalog_block_resolves_catalog_uri() {
        let cfg: TiledConfig =
            serde_yaml::from_str("catalog:\n  uri: sqlite:///data/catalog.db\n").unwrap();
        assert_eq!(cfg.catalog_uri(), Some("sqlite:///data/catalog.db"));
        // A catalog block is not a mongo source.
        assert_eq!(cfg.mongo_uri(), None);
    }

    // cli-M4: a `trees:` entry whose adapter selects the catalog
    // (Python TREE_ALIASES "catalog") also resolves the catalog URI.
    #[test]
    fn catalog_adapter_tree_resolves_catalog_uri() {
        let cfg: TiledConfig =
            serde_yaml::from_str("trees:\n  - tree: catalog\n    args: {uri: sqlite:///t.db}\n")
                .unwrap();
        assert_eq!(cfg.catalog_uri(), Some("sqlite:///t.db"));
    }

    // cli-M4: `catalog:` and `trees:` are mutually exclusive (Python
    // reconcile_catalog_and_trees).
    #[test]
    fn catalog_and_trees_are_mutually_exclusive() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "catalog:\n  uri: sqlite:///c.db\n\
             trees:\n  - tree: mongo_normalized\n    args: {uri: mongodb://h/db}\n",
        )
        .unwrap();
        let err = cfg.reconcile_catalog_and_trees().unwrap_err();
        assert!(
            format!("{err}").contains("not allowed to use both"),
            "mutual-exclusion error expected; got: {err}"
        );
    }

    // cli-M4: a `catalog:`-only config (no `trees:`) passes reconciliation.
    #[test]
    fn catalog_only_config_reconciles() {
        let cfg: TiledConfig = serde_yaml::from_str("catalog:\n  uri: sqlite:///c.db\n").unwrap();
        assert!(cfg.reconcile_catalog_and_trees().is_ok());
    }

    // cli-M6: a directory of config files merges — `trees:` lists concatenate
    // across files (config.d layering), other keys come from their one file.
    #[test]
    fn config_directory_merges_trees_and_scalar_keys() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("10-trees.yml"),
            "trees:\n  - tree: mongo_normalized\n    args: {uri: mongodb://a/db}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("20-more.yml"),
            "trees:\n  - tree: mongo_normalized\n    args: {uri: mongodb://b/db}\n\
             response_bytesize_limit: 99\n",
        )
        .unwrap();
        let cfg = TiledConfig::from_file(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(cfg.trees.len(), 2, "trees from both files must concatenate");
        // Sorted filename order: 10-trees before 20-more.
        assert_eq!(cfg.trees[0].args.uri.as_deref(), Some("mongodb://a/db"));
        assert_eq!(cfg.trees[1].args.uri.as_deref(), Some("mongodb://b/db"));
        assert_eq!(cfg.response_bytesize_limit, 99);
    }

    #[test]
    fn allow_origins_parses_from_yaml() {
        // Absent → empty (no CORS allow-list from config).
        let cfg: TiledConfig = serde_yaml::from_str("trees: []").unwrap();
        assert!(cfg.allow_origins().is_empty());
        // Present → parsed into the field/accessor.
        let cfg: TiledConfig =
            serde_yaml::from_str("allow_origins:\n  - https://a.example\n  - https://b.example\n")
                .unwrap();
        assert_eq!(
            cfg.allow_origins(),
            [
                "https://a.example".to_string(),
                "https://b.example".to_string()
            ]
        );
    }

    #[test]
    fn allow_origins_concatenates_across_config_directory() {
        // allow_origins is a mergeable list (Python config.py:484), so a
        // config.d split across files concatenates instead of conflicting.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("10-a.yml"),
            "allow_origins:\n  - https://a.example\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("20-b.yml"),
            "allow_origins:\n  - https://b.example\n",
        )
        .unwrap();
        let cfg = TiledConfig::from_file(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(
            cfg.allow_origins(),
            [
                "https://a.example".to_string(),
                "https://b.example".to_string()
            ]
        );
    }

    // cli-M6: a non-mergeable key duplicated across files is a conflict.
    #[test]
    fn config_directory_rejects_duplicate_nonmergeable_key() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.yml"), "response_bytesize_limit: 1\n").unwrap();
        std::fs::write(dir.path().join("b.yml"), "response_bytesize_limit: 2\n").unwrap();
        let err = TiledConfig::from_file(dir.path().to_str().unwrap()).unwrap_err();
        assert!(
            format!("{err:#}").contains("duplicate configuration for 'response_bytesize_limit'"),
            "expected duplicate-key conflict; got: {err:#}"
        );
    }

    // catalog.writable_storage and catalog.readable_storage parse into the
    // CatalogConfig fields (config.py:64-65) and do NOT appear in the
    // unknown catch-all — they are modelled and wired.
    #[test]
    fn catalog_storage_fields_parse_and_are_not_unknown() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "catalog:\n  uri: sqlite:///c.db\n  writable_storage:\n    - /data/write\n  readable_storage:\n    - /data/read\n  init_if_not_exists: true\n",
        )
        .unwrap();
        let catalog = cfg.catalog.as_ref().expect("catalog block present");
        assert_eq!(catalog.writable_storage, ["/data/write"]);
        assert_eq!(catalog.readable_storage, ["/data/read"]);
        assert!(catalog.init_if_not_exists);
        // These modelled keys must NOT appear in the unknown catch-all.
        assert!(
            !catalog.unknown.contains_key("writable_storage"),
            "writable_storage is modelled — must not appear in catch-all"
        );
        assert!(
            !catalog.unknown.contains_key("readable_storage"),
            "readable_storage is modelled — must not appear in catch-all"
        );
        assert!(
            !catalog.unknown.contains_key("init_if_not_exists"),
            "init_if_not_exists is modelled — must not appear in catch-all"
        );
    }

    // authentication.access_token_max_age and refresh_token_max_age parse into
    // AuthConfig and are NOT in the unknown catch-all (they are modelled and
    // wired to Issuer::with_ttls in lib.rs).
    #[test]
    fn auth_token_ages_parse_and_are_not_unknown() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "trees: []\nauthentication:\n  access_token_max_age: 900\n  refresh_token_max_age: 604800\n",
        )
        .unwrap();
        let auth = cfg
            .authentication
            .as_ref()
            .expect("authentication block present");
        assert_eq!(auth.access_token_max_age, Some(900.0));
        assert_eq!(auth.refresh_token_max_age, Some(604_800.0));
        assert!(
            !auth.unknown.contains_key("access_token_max_age"),
            "access_token_max_age is modelled — must not appear in catch-all"
        );
        assert!(
            !auth.unknown.contains_key("refresh_token_max_age"),
            "refresh_token_max_age is modelled — must not appear in catch-all"
        );
    }

    // authentication.secret_keys parses into AuthConfig.secret_keys and does
    // NOT appear in the unknown catch-all (it is modelled and wired to Issuer).
    #[test]
    fn auth_secret_keys_parse_and_are_not_unknown() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "trees: []\nauthentication:\n  secret_keys:\n    - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n    - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n",
        )
        .unwrap();
        let auth = cfg
            .authentication
            .as_ref()
            .expect("authentication block present");
        let keys = auth.secret_keys.as_ref().expect("secret_keys present");
        assert_eq!(keys.len(), 2);
        assert_eq!(
            keys[0],
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        // Must not land in the catch-all.
        assert!(
            !auth.unknown.contains_key("secret_keys"),
            "secret_keys is modelled — must not appear in catch-all"
        );
    }

    // Structural: unknown top-level config keys must land in the catch-all
    // `unknown` map instead of being silently dropped (config-parity guard).
    #[test]
    fn unknown_top_level_key_is_captured_not_dropped() {
        let cfg: TiledConfig = serde_yaml::from_str("some_future_key: 42\ntrees: []").unwrap();
        assert!(
            cfg.unknown.contains_key("some_future_key"),
            "unknown top-level key 'some_future_key' must land in the catch-all map; \
             got unknown={:?}",
            cfg.unknown
        );
        assert_eq!(
            cfg.unknown["some_future_key"],
            serde_yaml::Value::Number(42.into()),
            "captured value must match the YAML value"
        );
        // Known fields must NOT appear in unknown.
        assert!(
            !cfg.unknown.contains_key("trees"),
            "known key 'trees' must not appear in the catch-all"
        );
    }

    // exact_count_limit is a modelled field — parses to u64 and must NOT appear
    // in the unknown catch-all. Mirrors Python Settings.exact_count_limit
    // (settings.py, default 100).
    #[test]
    fn exact_count_limit_parses_and_is_not_unknown() {
        // Default: absent in YAML → 100.
        let cfg: TiledConfig = serde_yaml::from_str("trees: []").unwrap();
        assert_eq!(cfg.exact_count_limit, 100);
        // Default::default() agrees.
        assert_eq!(TiledConfig::default().exact_count_limit, 100);
        // Explicit value is honoured.
        let cfg: TiledConfig = serde_yaml::from_str("exact_count_limit: 500\ntrees: []").unwrap();
        assert_eq!(cfg.exact_count_limit, 500);
        // Modelled key must NOT appear in the catch-all.
        assert!(
            !cfg.unknown.contains_key("exact_count_limit"),
            "exact_count_limit is modelled — must not appear in catch-all; \
             got unknown={:?}",
            cfg.unknown
        );
    }

    // Unknown authentication sub-keys must land in AuthConfig.unknown.
    #[test]
    fn unknown_auth_key_is_captured_not_dropped() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "trees: []\nauthentication:\n  session_max_age: 3600\n  providers: []\n",
        )
        .unwrap();
        let auth = cfg
            .authentication
            .as_ref()
            .expect("authentication block present");
        assert!(
            auth.unknown.contains_key("session_max_age"),
            "unknown auth key 'session_max_age' must be captured; got {:?}",
            auth.unknown
        );
        assert!(
            auth.unknown.contains_key("providers"),
            "unknown auth key 'providers' must be captured; got {:?}",
            auth.unknown
        );
        // Known auth fields must NOT appear in unknown.
        assert!(
            !auth.unknown.contains_key("single_user_api_key"),
            "known key 'single_user_api_key' must not appear in auth catch-all"
        );
    }

    // authentication.tiled_admins parses into a typed Vec<TiledAdmin> and
    // must NOT appear in the unknown catch-all (it is modelled and wired to
    // the startup admin-bootstrap).
    #[test]
    fn auth_tiled_admins_parse_and_are_not_unknown() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "trees: []\n\
             authentication:\n\
             \x20 tiled_admins:\n\
             \x20   - provider: ldap\n\
             \x20     id: alice\n\
             \x20   - provider: oidc\n\
             \x20     id: bob@example.com\n",
        )
        .unwrap();
        let auth = cfg
            .authentication
            .as_ref()
            .expect("authentication block present");
        assert_eq!(
            auth.tiled_admins.len(),
            2,
            "two tiled_admins entries must parse"
        );
        assert_eq!(auth.tiled_admins[0].provider, "ldap");
        assert_eq!(auth.tiled_admins[0].id, "alice");
        assert_eq!(auth.tiled_admins[1].provider, "oidc");
        assert_eq!(auth.tiled_admins[1].id, "bob@example.com");
        // tiled_admins is modelled — must NOT appear in the catch-all.
        assert!(
            !auth.unknown.contains_key("tiled_admins"),
            "tiled_admins is modelled — must not appear in auth catch-all; \
             got unknown={:?}",
            auth.unknown
        );
    }

    // Unknown catalog sub-keys must land in CatalogConfig.unknown.
    #[test]
    fn unknown_catalog_key_is_captured_not_dropped() {
        let cfg: TiledConfig = serde_yaml::from_str(
            "catalog:\n  uri: sqlite:///c.db\n  metadata: {foo: bar}\n  mount_node: /\n",
        )
        .unwrap();
        let catalog = cfg.catalog.as_ref().expect("catalog block present");
        assert!(
            catalog.unknown.contains_key("metadata"),
            "unknown catalog key 'metadata' must be captured; got {:?}",
            catalog.unknown
        );
        assert!(
            catalog.unknown.contains_key("mount_node"),
            "unknown catalog key 'mount_node' must be captured; got {:?}",
            catalog.unknown
        );
        // Known catalog fields must NOT appear in unknown.
        assert!(
            !catalog.unknown.contains_key("uri"),
            "known key 'uri' must not appear in catalog catch-all"
        );
    }

    // catalog.catalog_pool_size parses and does not appear in catalog.unknown.
    #[test]
    fn catalog_pool_size_parses_and_is_not_unknown() {
        // Absent → None (caller falls back to built-in defaults).
        let cfg: TiledConfig = serde_yaml::from_str("catalog:\n  uri: \"sqlite:\"").unwrap();
        let cat = cfg.catalog.as_ref().unwrap();
        assert_eq!(cat.catalog_pool_size, None);
        assert_eq!(cat.storage_pool_size, None);
        assert_eq!(cat.catalog_max_overflow, None);
        assert_eq!(cat.storage_max_overflow, None);

        // Explicit values are honoured.
        let cfg: TiledConfig = serde_yaml::from_str(
            "catalog:\n  uri: \"sqlite:\"\n  catalog_pool_size: 3\n  storage_pool_size: 4\n  catalog_max_overflow: 8\n  storage_max_overflow: 9\n",
        )
        .unwrap();
        let cat = cfg.catalog.as_ref().unwrap();
        assert_eq!(cat.catalog_pool_size, Some(3));
        assert_eq!(cat.storage_pool_size, Some(4));
        assert_eq!(cat.catalog_max_overflow, Some(8));
        assert_eq!(cat.storage_max_overflow, Some(9));

        // All four are modelled — must NOT appear in the catch-all.
        for key in [
            "catalog_pool_size",
            "storage_pool_size",
            "catalog_max_overflow",
            "storage_max_overflow",
        ] {
            assert!(
                !cat.unknown.contains_key(key),
                "'{key}' is modelled — must not appear in catalog catch-all; got {:?}",
                cat.unknown
            );
        }
    }

    // cli-M6: TILED_RESPONSE_BYTESIZE_LIMIT overlays the file value
    // (env-over-file precedence); an invalid value is a hard error.
    #[test]
    fn response_bytesize_env_overlay() {
        let mut cfg = TiledConfig::default();
        // Unset → unchanged.
        cfg.overlay_response_bytesize_limit(None).unwrap();
        assert_eq!(cfg.response_bytesize_limit, 300_000_000);
        // Set → override (env wins over file).
        cfg.overlay_response_bytesize_limit(Some(" 4096 ")).unwrap();
        assert_eq!(cfg.response_bytesize_limit, 4096);
        // Invalid → hard error.
        assert!(
            cfg.overlay_response_bytesize_limit(Some("not-a-number"))
                .is_err()
        );
    }
}
