//! Configuration file parsing for tiled-rs.
//!
//! Supports YAML config files compatible with the databroker/Tiled config format.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use tiled_access::{AccessPolicy, PassthroughPolicy, TagBasedPolicy};
use tiled_auth::ScopeSet;

/// Top-level configuration.
#[derive(Debug, Deserialize)]
pub struct TiledConfig {
    #[serde(default)]
    pub trees: Vec<TreeConfig>,
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
}

/// Default for [`TiledConfig::response_bytesize_limit`] — 300 MB, matching
/// Python tiled (`settings.py:40`).
pub fn default_response_bytesize_limit() -> usize {
    300_000_000
}

// `Default` is hand-written (not derived) so `response_bytesize_limit` agrees
// with its serde default (300 MB) instead of `usize::default()` (0), which
// would otherwise make every response "exceed" the limit.
impl Default for TiledConfig {
    fn default() -> Self {
        Self {
            trees: Vec::new(),
            authentication: None,
            web: None,
            access_control: None,
            response_bytesize_limit: default_response_bytesize_limit(),
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
}

fn default_path() -> String {
    "/".to_string()
}

impl TiledConfig {
    /// Load configuration from a YAML file.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
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

    /// Extract the API key from authentication config or env var.
    pub fn api_key(&self) -> Option<String> {
        self.authentication
            .as_ref()
            .and_then(|a| a.single_user_api_key.clone())
            .or_else(|| std::env::var("TILED_SINGLE_USER_API_KEY").ok())
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
}
