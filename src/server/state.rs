//! Application state shared across all request handlers.

use std::sync::Arc;

use crate::auth::{AuthDb, Authenticator, Issuer};
use crate::core::adapters::ContainerAdapter;
use crate::serialization::SerializationRegistry;

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
    pub catalog: Option<crate::catalog::Catalog>,

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
    pub proxied_header_auth: Option<Arc<crate::auth::ProxiedHeaderAuthenticator>>,
    /// Optional external OIDC validator. Bearer tokens that aren't
    /// signed by the local Issuer fall through to this validator
    /// (tiled#1364, #1343). When validation succeeds, the matching
    /// principal is upserted into `auth_db` automatically.
    pub external_oidc: Option<Arc<crate::auth::ExternalOidcValidator>>,
    /// SAML 2.0 SP-initiated providers. Each entry exposes
    /// `GET /api/v1/auth/saml/{name}/login` (redirect to IdP) and
    /// `POST /api/v1/auth/saml/{name}/acs` (validate Response + mint session).
    /// Requires the `saml` feature.
    #[cfg(feature = "saml")]
    pub saml_providers: Vec<Arc<crate::auth::SamlProvider>>,
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
    /// Maximum decoded size (bytes) of a single array/table data response.
    /// Read handlers measure the decoded payload BEFORE serialization and
    /// return 400 when it exceeds this. Default 300_000_000 matches Python
    /// tiled's `response_bytesize_limit` (config.py / settings.py).
    pub response_bytesize_limit: usize,

    /// In-process pub/sub bus for WebSocket subscribers. Write handlers
    /// publish to it after a successful catalog write; subscribers
    /// connected to /api/v1/stream/single/{*path} receive matching
    /// updates.
    pub streaming_bus: crate::server::streaming::StreamingBus,
    /// Optional AccessPolicy. When set, search/read handlers consult it
    /// for per-node scope decisions (tiled#287). `None` keeps the
    /// scope check entirely on the auth middleware (current default —
    /// session JWT scopes apply uniformly across the tree).
    pub access_policy: Option<Arc<dyn crate::access::AccessPolicy>>,
    /// Scope set issued to a session created via `/auth/{provider}/login`
    /// or device-code grant. Defaults to the full set (matches single-
    /// user behaviour); operators can narrow it to the safe defaults
    /// (read:metadata + read:data + create + register) per Python tiled's
    /// SimpleAccessPolicy. Per-key fine-grained scopes still flow
    /// through `POST /auth/apikeys`.
    pub default_login_scopes: crate::auth::ScopeSet,
    /// Skip the bundled WebUI shell mount when false. Compile-time the
    /// `web` feature still has to be on; this is a runtime toggle for
    /// `--no-web` deployments.
    pub enable_web: bool,
    /// Optional directory the SPA assets are served from. `None` uses
    /// the compiled-in placeholder bundle. Honoured only when the
    /// `web` feature is compiled in.
    pub web_assets_dir: Option<std::path::PathBuf>,
    /// Operator-configured spec → external-viewer URL mappings, returned
    /// to the SPA via `GET /settings.json` (mirrors upstream tiled
    /// PR #1349's `spec_views` settings entry). Empty by default —
    /// configure via the YAML `web.spec_views` section. Defined here
    /// (not in tiled-web) so the field is always present on AppState
    /// regardless of the `web` feature, which keeps construction sites
    /// uniform across crates.
    pub spec_views: Vec<SpecViewEntry>,
    /// Configuration for the webhook subsystem (upstream tiled #1353).
    /// `None` disables webhooks entirely (no router, no dispatcher).
    /// `Some(_)` enables them with the per-field overrides in
    /// `WebhookConfig`.
    pub webhook_config: Option<crate::server::webhook_dispatch::WebhookConfig>,
    /// Per-request timeout in seconds.  Requests that do not complete within
    /// this window receive HTTP 408.  Default: 30.  Python tiled delegates
    /// timeouts to the ASGI server (uvicorn) and has no in-app default;
    /// 30 s matches the common uvicorn keepalive default.
    pub request_timeout_secs: u64,
    /// Whether the raw-asset download endpoints (`GET /api/v1/asset/bytes` and
    /// `/api/v1/asset/manifest`) are allowed to serve backing files from disk.
    /// Mirrors Python `Settings.expose_raw_assets`, which defaults to `True`
    /// (settings.py:57); when `false` both endpoints return 403 even for
    /// otherwise-valid requests.
    pub expose_raw_assets: bool,
    /// Cap on the exact `COUNT(*)` total reported in `meta.count` for search /
    /// list responses. When the true count exceeds this value, `meta.count` is
    /// set to this limit (a lower-bound estimate). Mirrors Python
    /// `Settings.exact_count_limit` (`settings.py`, default 100).
    pub exact_count_limit: u64,
    /// Single owner for process-lifetime background tasks (upstream tiled
    /// #1018 — "background-task lifecycle"). Long-lived spawns (e.g. the
    /// webhook dispatcher) must register here via [`BackgroundTasks::spawn`]
    /// instead of calling `tokio::spawn` directly, so the CLI's shutdown
    /// path can signal and await them exactly once. See
    /// [`BackgroundTasks::shutdown`].
    pub background_tasks: BackgroundTasks,
}

/// Owner for background tasks meant to live for the process's lifetime,
/// not a single request (upstream tiled #1018).
///
/// A bare `tokio::spawn` detaches immediately: the returned `JoinHandle` is
/// the only handle on the task, and dropping it (or never storing it)
/// leaves the task running with nothing to cancel or await it.
/// `axum::serve(..).with_graceful_shutdown(..)` only waits for in-flight
/// HTTP connections to finish — it has no idea a detached task exists, so
/// the process can exit (or be reaped by a supervisor) while the task is
/// mid-work.
///
/// `BackgroundTasks` closes that gap: every long-lived task is registered
/// via [`spawn`](Self::spawn) instead, and [`shutdown`](Self::shutdown) —
/// called exactly once, after the HTTP listener stops accepting new
/// connections — signals every registered task via [`cancellation`
/// ](Self::cancellation) and then awaits all of them before returning.
#[derive(Clone)]
pub struct BackgroundTasks {
    joins: Arc<std::sync::Mutex<tokio::task::JoinSet<()>>>,
    cancel: Arc<tokio::sync::watch::Sender<bool>>,
}

impl Default for BackgroundTasks {
    fn default() -> Self {
        Self::new()
    }
}

impl BackgroundTasks {
    pub fn new() -> Self {
        let (cancel, _rx) = tokio::sync::watch::channel(false);
        Self {
            joins: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            cancel: Arc::new(cancel),
        }
    }

    /// A receiver a registered task's loop selects on alongside its normal
    /// work. `shutdown()` flips the watched value to `true`; the task
    /// should treat that as "stop, cooperatively, now."
    pub fn cancellation(&self) -> tokio::sync::watch::Receiver<bool> {
        self.cancel.subscribe()
    }

    /// Register a process-lifetime task. This is the only sanctioned path
    /// onto the runtime for such tasks — call this instead of
    /// `tokio::spawn` so `shutdown()` can find and await it. The task
    /// itself is responsible for exiting once `cancellation()` reports
    /// `true` (typically via `tokio::select!`); `shutdown()` does not abort
    /// tasks, it only signals and waits.
    pub fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.joins
            .lock()
            .expect("BackgroundTasks registry mutex poisoned")
            .spawn(fut);
    }

    /// Signal every registered task to stop, then await all of them.
    ///
    /// MUST be called exactly once, and only after the HTTP listener has
    /// stopped accepting new connections (i.e. after
    /// `axum::serve(..).with_graceful_shutdown(..)` resolves) — that
    /// ordering ensures no new work arrives for a task that is already
    /// being told to stop.
    pub async fn shutdown(&self) {
        let _ = self.cancel.send(true);
        let mut joins = std::mem::replace(
            &mut *self
                .joins
                .lock()
                .expect("BackgroundTasks registry mutex poisoned"),
            tokio::task::JoinSet::new(),
        );
        while let Some(res) = joins.join_next().await {
            if let Err(e) = res {
                tracing::warn!(target: "tiled.lifecycle", "background task panicked: {e}");
            }
        }
    }
}

/// One `spec_views` entry. Wire-compatible with tiled-web's
/// `SpecViewEntry`; lives here so `AppState` is feature-flag-free.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SpecViewEntry {
    pub spec: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl AppState {
    /// No authentication backend is configured at all — neither a single-user
    /// `api_key` nor a multi-user `auth_db`. This is the single source of the
    /// "no auth configured" concept that drives both the loud startup warning
    /// ([`crate::server::build_app`]) and the anonymous full-scope request fallback
    /// (the auth middleware). Python always has a single-user key, so this
    /// fully-open mode has no upstream parity — it is a deliberate demo escape
    /// hatch the operator is warned about at startup.
    pub fn no_auth_configured(&self) -> bool {
        self.api_key.is_none() && self.auth_db.is_none()
    }

    /// Enforce single-user / multi-user **mode exclusivity**. A multi-user
    /// auth database and a single-user API key are mutually exclusive auth
    /// backends. Upstream consults the single-user key *only* when no
    /// authenticators are configured (`if not authenticated`,
    /// `tiled/server/authentication.py:350`); once an auth DB is present the
    /// single-user key is never compared. This drops the single-user key when
    /// an auth DB is configured, so `auth_db.is_some() ⟹ api_key.is_none()`
    /// holds for the running app and the auth-middleware single-user
    /// fall-through cannot grant scopes against a multi-user server. Returns
    /// `true` iff a key was dropped, so the caller can warn. Called once, from
    /// [`crate::server::build_app`] — the single funnel every server (CLI and
    /// tests) passes through — so the illegal both-set state is unrepresentable
    /// in any built app rather than guarded by a per-request runtime branch.
    pub(crate) fn enforce_auth_mode_exclusivity(&mut self) -> bool {
        if self.auth_db.is_some() && self.api_key.is_some() {
            self.api_key = None;
            true
        } else {
            false
        }
    }

    /// The default value for [`Self::default_login_scopes`] when the operator
    /// does not narrow it. The **full** scope set, so a login/API key carries
    /// its principal's full role scopes: `role ∩ default == role` (identity).
    /// Upstream imposes no global login-scope cap below role scopes — it mints
    /// the principal's role scopes straight into the session token
    /// (`"scp": list(role scopes)`, `tiled/server/authentication.py:856`). A
    /// narrower default (e.g. `read_only()`) silently strips every write/create
    /// scope even from an admin, contradicting the field's documented intent;
    /// this is the single source of truth for that default so the CLI wiring
    /// and the doc cannot drift.
    pub fn default_login_scopes() -> crate::auth::ScopeSet {
        crate::auth::ScopeSet::full()
    }

    pub fn resolve_base_url(&self, headers: &axum::http::HeaderMap) -> String {
        self.resolve_base_url_with_peer(headers, None)
    }

    /// Like [`Self::resolve_base_url`] but lets the caller pass the connecting
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

        let trust = self.trust_forwarded_headers && self.peer_is_trusted(peer_ip);
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
            (Some(list), Some(ip)) => list.contains(&ip),
            // Allow-list configured but we don't know the peer → don't trust.
            (Some(_), None) => false,
        }
    }
}
