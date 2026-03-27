//! Application state shared across all request handlers.

use std::sync::Arc;

use tiled_core::adapters::ContainerAdapter;
use tiled_serialization::SerializationRegistry;

/// CORS origin policy.
#[derive(Clone, Debug)]
pub enum CorsOriginPolicy {
    Permissive,
    AllowList(Vec<String>),
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub root_tree: Arc<dyn ContainerAdapter>,
    pub serialization_registry: Arc<SerializationRegistry>,
    pub query_names: Vec<String>,
    pub base_url: Option<String>,
    pub cors_policy: CorsOriginPolicy,
    pub trust_forwarded_headers: bool,
    /// Single-user API key. `None` = anonymous access allowed.
    pub api_key: Option<String>,
}

impl AppState {
    pub fn resolve_base_url(&self, headers: &axum::http::HeaderMap) -> String {
        if let Some(ref url) = self.base_url {
            return url.clone();
        }

        let (host, scheme) = if self.trust_forwarded_headers {
            let h = headers
                .get("x-forwarded-host")
                .or_else(|| headers.get("host"))
                .and_then(|v| v.to_str().ok())
                .unwrap_or("localhost");
            let s = headers
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("http");
            (h, s)
        } else {
            let h = headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("localhost");
            (h, "http")
        };

        format!("{scheme}://{host}")
    }
}
