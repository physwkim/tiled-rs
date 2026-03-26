//! Application state shared across all request handlers.

use std::sync::Arc;

use tiled_core::adapters::ContainerAdapter;
use tiled_serialization::SerializationRegistry;

/// CORS origin policy.
#[derive(Clone, Debug)]
pub enum CorsOriginPolicy {
    /// Allow all origins (development / single-user mode).
    Permissive,
    /// Restrict to these specific origins.
    AllowList(Vec<String>),
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub root_tree: Arc<dyn ContainerAdapter>,
    pub serialization_registry: Arc<SerializationRegistry>,
    pub query_names: Vec<String>,
    /// Static base URL for links, if set. When `None`, derive from request Host header.
    pub base_url: Option<String>,
    pub cors_policy: CorsOriginPolicy,
}

impl AppState {
    /// Resolve the base URL for link generation.
    /// Prefers the static `base_url` if set, otherwise derives from request headers.
    pub fn resolve_base_url(&self, headers: &axum::http::HeaderMap) -> String {
        if let Some(ref url) = self.base_url {
            return url.clone();
        }

        // Try X-Forwarded-Host first (reverse proxy), then Host header.
        let host = headers
            .get("x-forwarded-host")
            .or_else(|| headers.get("host"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("localhost");

        let scheme = headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("http");

        format!("{scheme}://{host}")
    }
}
