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
}

/// A single tree (data source) definition.
#[derive(Debug, Deserialize)]
pub struct TreeConfig {
    /// URL path where this tree is mounted (e.g. "/raw").
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
    /// Handler registry (not used in Rust, kept for compatibility).
    #[serde(default)]
    pub handler_registry: std::collections::HashMap<String, String>,
}

/// Authentication configuration.
#[derive(Debug, Deserialize)]
pub struct AuthConfig {
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
