//! Configuration file parsing for tiled-rs.
//!
//! Supports YAML config files compatible with the databroker/Tiled config format.

use serde::Deserialize;

/// Top-level configuration.
#[derive(Debug, Deserialize, Default)]
pub struct TiledConfig {
    #[serde(default)]
    pub trees: Vec<TreeConfig>,
    #[serde(default)]
    pub authentication: Option<AuthConfig>,
    /// Optional `web:` section. Right now only `spec_views` is read —
    /// mirrors upstream tiled PR #1349.
    #[serde(default)]
    pub web: Option<WebConfig>,
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
