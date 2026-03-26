//! Application state shared across all request handlers.

use std::sync::Arc;

use tiled_core::adapters::ContainerAdapter;
use tiled_serialization::SerializationRegistry;

/// CORS origin policy.
#[derive(Clone, Debug)]
pub enum CorsOriginPolicy {
    /// Allow all origins (must be explicitly opted in with `--allow-origin '*'`).
    Permissive,
    /// Restrict to these specific origins. Empty list = same-origin only.
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
    /// Whether to trust X-Forwarded-Host / X-Forwarded-Proto headers.
    /// Only enable when running behind a trusted reverse proxy.
    pub trust_forwarded_headers: bool,
}

impl AppState {
    /// Resolve the base URL for link generation.
    ///
    /// Priority:
    /// 1. Static `base_url` (from `--public-url`).
    /// 2. If `trust_forwarded_headers`, use `X-Forwarded-Host` / `X-Forwarded-Proto`.
    /// 3. Fall back to the request `Host` header.
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
