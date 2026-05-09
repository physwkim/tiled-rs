//! Application state shared across all request handlers.

use std::sync::Arc;

use tiled_auth::{AuthDb, Authenticator, Issuer};
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
    /// Optional persistent catalog. When present, write endpoints
    /// (`POST /register`, `PATCH /metadata`, `PUT /data_source`,
    /// `DELETE /metadata`) operate against it; without it those
    /// endpoints return 501 Not Implemented.
    pub catalog: Option<tiled_catalog::Catalog>,

    /// Optional auth backend. When present, the server runs in multi-user
    /// mode: API keys are looked up in this DB, JWTs are issued/verified
    /// by `issuer`, and authenticators handle login. When `None`, the
    /// server falls back to single-user `api_key` (or anonymous).
    pub auth_db: Option<AuthDb>,
    /// JWT issuer paired with `auth_db`. Always `Some` when `auth_db` is.
    pub issuer: Option<Issuer>,
    /// Username/password authenticators keyed by their public name (which
    /// becomes the `/auth/{name}/login` mount point).
    pub authenticators: Vec<Arc<dyn Authenticator>>,
    /// Optional proxy-header authenticator. Honoured only when
    /// `trust_forwarded_headers` is also true.
    pub proxied_header_auth: Option<Arc<tiled_auth::ProxiedHeaderAuthenticator>>,
    /// Optional external OIDC validator. Bearer tokens that aren't
    /// signed by the local Issuer fall through to this validator
    /// (tiled#1364, #1343). When validation succeeds, the matching
    /// principal is upserted into `auth_db` automatically.
    pub external_oidc: Option<Arc<tiled_auth::ExternalOidcValidator>>,
    /// CIDR/IP list permitted to set X-Forwarded-* headers when
    /// `trust_forwarded_headers` is on. `None` = trust the headers from
    /// any peer (only safe when the listener is bound to a private
    /// network); empty Vec = trust nobody (effectively disables proxy
    /// header parsing); non-empty Vec = match the connecting peer's IP
    /// against the list. Mirrors uvicorn's `forwarded_allow_ips` and the
    /// fix in bluesky/tiled#148.
    pub forwarded_allow_ips: Option<Vec<std::net::IpAddr>>,
    /// Per-endpoint request body size cap (bytes). Pre-multipart payloads
    /// (POST register, PATCH metadata, PUT data_source) larger than this
    /// are rejected with 413. Default 10 MiB matches the metadata size
    /// limit in tiled-catalog so a too-large catalog payload fails fast.
    pub max_request_body_bytes: usize,

    /// In-process pub/sub bus for WebSocket subscribers. Write handlers
    /// publish to it after a successful catalog write; subscribers
    /// connected to /api/v1/{family}/subscribe/{*path} receive matching
    /// updates.
    pub streaming_bus: crate::streaming::StreamingBus,
    /// Optional AccessPolicy. When set, search/read handlers consult it
    /// for per-node scope decisions (tiled#287). `None` keeps the
    /// scope check entirely on the auth middleware (current default —
    /// session JWT scopes apply uniformly across the tree).
    pub access_policy: Option<Arc<dyn tiled_access::AccessPolicy>>,
    /// Scope set issued to a session created via `/auth/{provider}/login`
    /// or device-code grant. Defaults to the full set (matches single-
    /// user behaviour); operators can narrow it to the safe defaults
    /// (read:metadata + read:data + create + register) per Python tiled's
    /// SimpleAccessPolicy. Per-key fine-grained scopes still flow
    /// through `POST /auth/apikeys`.
    pub default_login_scopes: tiled_auth::ScopeSet,
}

impl AppState {
    pub fn resolve_base_url(&self, headers: &axum::http::HeaderMap) -> String {
        self.resolve_base_url_with_peer(headers, None)
    }

    /// Like [`resolve_base_url`] but lets the caller pass the connecting
    /// peer's IP — used so X-Forwarded-Host is honoured only when the
    /// peer is listed in `forwarded_allow_ips`.
    pub fn resolve_base_url_with_peer(
        &self,
        headers: &axum::http::HeaderMap,
        peer_ip: Option<std::net::IpAddr>,
    ) -> String {
        if let Some(ref url) = self.base_url {
            return url.clone();
        }

        let trust = self.trust_forwarded_headers
            && self.peer_is_trusted(peer_ip);
        let (host, scheme) = if trust {
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

    pub fn peer_is_trusted(&self, peer_ip: Option<std::net::IpAddr>) -> bool {
        match (&self.forwarded_allow_ips, peer_ip) {
            // None = "trust any peer" (legacy default).
            (None, _) => true,
            // Empty list = "trust nobody".
            (Some(list), _) if list.is_empty() => false,
            (Some(list), Some(ip)) => list.iter().any(|allowed| *allowed == ip),
            // Allow-list configured but we don't know the peer → don't trust.
            (Some(_), None) => false,
        }
    }
}
