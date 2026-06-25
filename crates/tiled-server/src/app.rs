//! Application builder — constructs the Axum Router with all routes.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post, put};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use tiled_auth::ScopeSet;

use crate::auth_context::{AuthContext, AuthKind};
use crate::auth_router;
use crate::router;
use crate::state::{AppState, CorsOriginPolicy};

/// Build the Axum application with all routes attached.
pub fn build_app(state: AppState) -> Router {
    // server-C1: warn loudly, once at startup, when the server is fully open.
    // With neither a single-user api_key nor an auth_db, the auth middleware
    // grants anonymous callers FULL scope (read AND write). Python always mints
    // a single-user key, so this mode has no upstream parity; the CLI now
    // auto-generates a key, so reaching here means a library embedder wired
    // AppState without auth. Make that impossible to miss.
    if state.no_auth_configured() {
        tracing::warn!(
            "No authentication configured (no api_key, no auth_db): the server \
             grants ANONYMOUS FULL ACCESS — any client can read and write. This \
             is a demo/dev mode only. Set api_key or auth_db before exposing it."
        );
    }

    // Spawn the webhook dispatcher (upstream tiled #1353) when enabled
    // by config + a catalog DB is present. The dispatcher subscribes to
    // the streaming bus's root channel so it sees every event without
    // touching request paths.
    if let (Some(cfg), Some(catalog)) = (state.webhook_config.as_ref(), state.catalog.as_ref()) {
        // `spawn` already drives the dispatcher on the runtime via `tokio::spawn`
        // and hands back the `JoinHandle`. We intentionally detach it: the
        // dispatcher lives for the lifetime of the process and is torn down when
        // the runtime stops. Drop the handle explicitly (not `let _`) so the
        // detach is deliberate rather than an accidentally-dropped future.
        drop(crate::webhook_dispatch::spawn(
            catalog.clone(),
            state.streaming_bus.clone(),
            cfg.clone(),
        ));
    }

    let cors = match &state.cors_policy {
        CorsOriginPolicy::Permissive => CorsLayer::permissive(),
        CorsOriginPolicy::AllowList(origins) => {
            let parsed: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();
            CorsLayer::new()
                .allow_origin(parsed)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::PATCH,
                    Method::DELETE,
                ])
                .allow_headers(tower_http::cors::Any)
        }
    };

    let mut app = Router::new()
        // Operational endpoints (never require auth)
        .route("/health", get(router::health))
        .route("/ready", get(router::ready))
        // Discovery endpoint — clients (incl. the SPA) need to fetch
        // this BEFORE they can authenticate, since it advertises which
        // providers are available. Mirrors upstream tiled, which also
        // exposes `/api/v1/` anonymously.
        .route("/api/v1/", get(router::about));

    // Public auth endpoints — login/refresh/device/initiate must work
    // without prior auth (otherwise login is unreachable). Each handler
    // does its own credential check where required.
    //
    // `device/approve` is also public: it accepts either an existing tiled
    // session bearer OR an external OIDC token in the request body (tiled
    // #1377). The handler resolves the credential itself via
    // `resolve_header_auth` / `external_oidc`, so it does not need the
    // auth middleware to gate it.
    let public_auth = Router::new()
        .route("/api/v1/auth/{provider}/login", post(auth_router::login))
        .route("/api/v1/auth/refresh", post(auth_router::refresh))
        .route(
            "/api/v1/auth/device/initiate",
            post(auth_router::device_initiate),
        )
        .route("/api/v1/auth/device/token", post(auth_router::device_token))
        .route(
            "/api/v1/auth/device/approve",
            post(auth_router::device_approve),
        );

    // Authenticated auth endpoints — must run inside the auth middleware
    // so AuthContext is populated.
    let private_auth = Router::new()
        .route("/api/v1/auth/logout", post(auth_router::logout))
        .route("/api/v1/auth/whoami", get(auth_router::whoami))
        .route(
            "/api/v1/auth/apikeys",
            get(auth_router::api_key_list).post(auth_router::api_key_create),
        )
        .route(
            "/api/v1/auth/apikeys/{first_eight}",
            delete(auth_router::api_key_revoke),
        )
        .route(
            "/api/v1/auth/principal",
            post(auth_router::create_service_principal),
        )
        .route(
            "/api/v1/auth/principal/{uuid}",
            get(auth_router::get_principal),
        );

    // WebSocket subscribe route is intentionally OUTSIDE the auth
    // middleware — browsers can't set the `Authorization` header on
    // WebSocket connections, so we accept the upgrade unauthenticated
    // and run a first-message handshake inside the handler (tiled#1351).
    // Server-side auth still happens before any data flows; if the
    // handshake fails the socket is closed.
    //
    // Path mirrors upstream `tiled` exactly: a single family-agnostic
    // `/api/v1/stream/single/{path}` (router.py:750). The node's
    // structure family is derived from the catalog/tree lookup inside
    // the handler, not from the URL — so one route serves every family.
    let ws = Router::new().route(
        "/api/v1/stream/single/{*path}",
        get(crate::streaming::ws_subscribe),
    );

    // Data API endpoints — auth middleware always runs and either
    // populates AuthContext or returns 401.
    let api = Router::new()
        .route("/api/v1/metadata/", get(router::metadata_root))
        .route("/api/v1/metadata/{*path}", get(router::metadata))
        .route("/api/v1/search/", get(router::search_root))
        .route("/api/v1/search/{*path}", get(router::search))
        .route("/api/v1/distinct/", get(router::distinct_root))
        .route("/api/v1/distinct/{*path}", get(router::distinct))
        .route(
            "/api/v1/array/block/{*path}",
            get(router::array_block).put(router::array_block_put),
        )
        .route(
            "/api/v1/array/full/{*path}",
            get(router::array_full)
                .patch(router::array_append)
                .put(router::array_full_put),
        )
        .route("/api/v1/container/full/", get(router::container_full_root))
        .route(
            "/api/v1/container/full/{*path}",
            get(router::container_full),
        )
        .route("/api/v1/array/full", post(router::array_full_post))
        .route("/api/v1/container/full", post(router::container_full_post))
        .route(
            "/api/v1/table/partition/{*path}",
            get(router::table_partition).post(router::post_table_partition),
        )
        .route(
            "/api/v1/table/full/{*path}",
            get(router::table_full).put(router::table_full_put),
        )
        .route("/api/v1/table/full", post(router::table_full_post))
        // Ragged read path (Python router.py:838 `get_ragged_full`). GET-only:
        // Python serves no GET `/ragged/block` (the advertised `block` link
        // targets the PUT-only write route).
        .route("/api/v1/ragged/full/{*path}", get(router::ragged_full))
        // Awkward array read+write paths (Python router.py:1704/2272).
        // GET/PUT /awkward/full — full array read or write.
        // GET+POST /awkward/buffers — filtered buffer fetch (GET uses ?form_key=
        // query params; POST uses a JSON-array body for large key sets).
        .route(
            "/api/v1/awkward/full/{*path}",
            get(router::awkward_full).put(router::put_awkward_full),
        )
        .route(
            "/api/v1/awkward/buffers/{*path}",
            get(router::awkward_buffers).post(router::post_awkward_buffers),
        )
        .route("/api/v1/register/", post(router::register_root))
        .route("/api/v1/register/{*path}", post(router::register))
        .route("/api/v1/metadata/{*path}", patch(router::patch_metadata))
        // PUT /metadata wholesale-replaces metadata/specs/access_blob, distinct
        // from PATCH's partial json-patch/merge-patch (Python router.py:2420).
        .route("/api/v1/metadata/{*path}", put(router::put_metadata))
        .route("/api/v1/metadata/{*path}", delete(router::delete_metadata))
        // POST /metadata is the client's common (asset-free) write path; it
        // shares the create core with /register but rejects externally-managed
        // assets (Python parity: router.py:1769).
        .route("/api/v1/metadata/", post(router::post_metadata_root))
        .route("/api/v1/metadata/{*path}", post(router::post_metadata))
        // Revision history (Python router.py:2496-2535). GET lists, DELETE
        // (with ?number=N) drops one. Catalog-only → 405 without a catalog.
        .route("/api/v1/revisions/{*path}", get(router::get_revisions))
        .route("/api/v1/revisions/{*path}", delete(router::delete_revision))
        // Raw-asset download (Python router.py:2570-2723). Gated by
        // `expose_raw_assets`; catalog-only → 405 without a catalog.
        .route("/api/v1/asset/bytes/{*path}", get(router::get_asset_bytes))
        .route(
            "/api/v1/asset/manifest/{*path}",
            get(router::get_asset_manifest),
        )
        .route("/api/v1/data_source/{*path}", put(router::put_data_source))
        .route("/documents/{*path}", get(router::get_documents))
        // Webhooks (upstream tiled #1353).
        .route(
            "/api/v1/webhooks/target/{*path}",
            post(crate::webhook_router::register).get(crate::webhook_router::list),
        )
        .route(
            "/api/v1/webhooks/{id}",
            delete(crate::webhook_router::delete),
        )
        .route(
            "/api/v1/webhooks/history/{id}",
            get(crate::webhook_router::history),
        );

    let guarded = api
        .merge(private_auth)
        // ETag/If-None-Match for JSON responses. Layered INSIDE auth (auth is
        // applied after, so it wraps this): the ETag layer only ever sees
        // responses from authorized handlers, never the 401 auth short-circuits.
        .layer(axum::middleware::from_fn(crate::etag::etag_json_responses))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    app = app.merge(public_auth).merge(ws).merge(guarded);

    let body_limit = state.max_request_body_bytes;
    let api_app = app
        .layer(axum::extract::DefaultBodyLimit::max(body_limit))
        .layer(axum::middleware::from_fn(correlation_id_middleware))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            timeout_middleware,
        ))
        .layer(CompressionLayer::new())
        // L2: the default request span records the full URI, including the
        // query string — so a credential passed as `?api_key=...` (a supported
        // auth form, see resolve_auth) would land in the trace/logs. Record
        // only the path; the query never enters the span. (Header creds like
        // `Authorization: Apikey/Bearer` are not recorded by the span at all.)
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::extract::Request| {
                tracing::info_span!(
                    "request",
                    method = %request.method(),
                    path = %request.uri().path(),
                    version = ?request.version(),
                )
            }),
        )
        .layer(cors)
        .with_state(state.clone());

    // Browser surface (SPA shell + admin pages). Mounted alongside the
    // API after with_state — tiled-web's router carries its own
    // WebState internally, so it merges cleanly into a stateless
    // Router<()>. Behind the `web` feature so headless deployments can
    // strip the static bundle from the binary.
    #[cfg(feature = "web")]
    {
        if state.enable_web {
            let bus = state.streaming_bus.clone();
            let web_state = tiled_web::WebState {
                auth_db: state.auth_db.clone(),
                issuer: state.issuer.clone(),
                default_login_scopes: state.default_login_scopes.clone(),
                login_provider: "dummy".into(),
                channel_count_fn: std::sync::Arc::new(move || bus.channel_count()),
                // Honor X-Forwarded-Proto for the cookie Secure flag only when
                // we trust a fronting proxy's forwarded headers. peer_ip is not
                // plumbed here (no ConnectInfo), so peer_is_trusted(None) folds
                // to "no allow-list configured", consistent with how
                // resolve_base_url treats forwarded headers.
                trust_forwarded_proto: state.trust_forwarded_headers && state.peer_is_trusted(None),
                assets_dir: state.web_assets_dir.clone(),
                spec_views: state
                    .spec_views
                    .iter()
                    .map(|s| tiled_web::SpecViewEntry {
                        spec: s.spec.clone(),
                        url: s.url.clone(),
                        label: s.label.clone(),
                    })
                    .collect(),
                authenticator: state
                    .authenticators
                    .iter()
                    .find(|a| a.name() == "dummy")
                    .cloned(),
            };
            return api_app.merge(tiled_web::build_router(web_state));
        }
    }
    api_app
}

/// Generates a per-request `x-tiled-request-id` if the client didn't set
/// one and emits it on the response. Mirrors tiled#673 so logs and client
/// errors share a correlation key.
async fn correlation_id_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::HeaderValue;

    let header_name = "x-tiled-request-id";
    let request_id = match request
        .headers()
        .get(header_name)
        .and_then(|v| v.to_str().ok())
    {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => short_request_id(),
    };

    let mut request = request;
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    let mut response = next.run(request).await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(header_name, value);
    }
    response
}

#[derive(Debug, Clone)]
pub struct RequestId(pub String);

fn short_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{counter:x}")
}

/// Universal auth middleware.
///
/// Resolves the request's credentials in order of precedence:
/// 1. `Authorization: Bearer <jwt>` — multi-user session token via the
///    auth DB.
/// 2. `Authorization: Apikey <key>` (or `?api_key=...`) — multi-user API
///    key via the auth DB; falls back to the single-user CLI flag.
/// 3. Trusted proxy header (`X-Forwarded-User`) — only when
///    `trust_forwarded_headers` is on AND a proxied authenticator is
///    registered.
/// 4. Anonymous — when no auth backend is configured at all, traffic
///    passes through with full scopes (existing behaviour for demo /
///    Mongo deployments).
///
/// The resolved `AuthContext` is inserted into the request extensions so
/// downstream handlers can use the `AuthContext` extractor.
async fn auth_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Pre-extract everything we need from the request so resolve_auth
    // doesn't borrow from `request` itself; that simplifies the borrow
    // graph for the future and lets axum's trait inference see a Send
    // future.
    let headers = request.headers().clone();
    let query = request.uri().query().unwrap_or("").to_string();
    let ctx = match resolve_auth_owned(&state, &headers, &query).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let mut request = request;
    request.extensions_mut().insert(ctx);
    next.run(request).await
}

async fn resolve_auth_owned(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    query: &str,
) -> Result<AuthContext, axum::response::Response> {
    resolve_auth_inner(state, headers, query).await
}

/// Header-based auth resolution shared with the WebSocket handler so it
/// can honour an `Authorization: Bearer ...` or `?api_key=` upgrade
/// without going through the HTTP middleware (the WS routes are
/// mounted outside the middleware to support tiled#1351 first-message
/// auth). Returns `None` when no header credential was supplied or the
/// presented one was rejected; the caller falls back to the in-band
/// handshake.
pub async fn resolve_header_auth(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> Option<AuthContext> {
    resolve_auth_inner(state, headers, "").await.ok()
}

/// Validate a Bearer JWT outside the request middleware (used by the WS
/// handshake). Tries the local Issuer first; falls through to the
/// configured external OIDC validator (tiled#1364, #1343).
pub async fn validate_bearer(state: &AppState, token: &str) -> Result<AuthContext, String> {
    let db = state
        .auth_db
        .as_ref()
        .ok_or_else(|| "server has no auth_db; bearer not supported".to_string())?;
    if let Some(issuer) = state.issuer.as_ref()
        && let Ok(claims) = issuer.verify_access(token)
    {
        let session = db
            .lookup_session(&claims.sid)
            .await
            .map_err(|_| "session not found".to_string())?;
        if session.revoked {
            return Err("session revoked".into());
        }
        if session.expiration_time <= chrono::Utc::now() {
            return Err("session expired".into());
        }
        db.touch_session(&claims.sid).await.ok();
        let principal = db
            .get_principal(session.principal_id)
            .await
            .map_err(|_| "principal lookup failed".to_string())?
            .ok_or_else(|| "principal not found".to_string())?;
        return Ok(AuthContext {
            principal: Some(Arc::new(principal)),
            scopes: claims.scopes.intersect(&session.scopes),
            kind: AuthKind::Session,
        });
    }
    if let Some(validator) = state.external_oidc.as_ref() {
        let validated = validator
            .validate(token)
            .await
            .map_err(|e| format!("external oidc: {e}"))?;
        let (principal, identity) = db
            .ensure_principal(&validated.provider, &validated.sub)
            .await
            .map_err(|e| format!("ensure principal: {e}"))?;
        db.touch_identity_login(identity.id).await.ok();
        // Role scopes always apply. For an Entra-style provider (one with a
        // `scopes_map`), the token's `scp` claim is translated to tiled scopes
        // and unioned on top — Python `get_current_scopes` returns
        // `token_scopes | role_scopes` (authentication.py:434). A plain OIDC
        // provider yields `validated.scopes == None`, so role scopes stand alone.
        let role_scopes = mint_session_scopes(&principal, state);
        let scopes = match &validated.scopes {
            Some(token_scopes) => token_scopes.union(&role_scopes),
            None => role_scopes,
        };
        return Ok(AuthContext {
            principal: Some(Arc::new(principal.clone())),
            scopes,
            kind: AuthKind::Session,
        });
    }
    Err("no JWT issuer or external OIDC configured".into())
}

/// Validate an Apikey outside the request middleware. Multi-user DB
/// first, single-user CLI flag fallback. Same constant-time compare
/// the middleware uses (R3 timing-attack fix).
pub async fn validate_apikey(state: &AppState, key: &str) -> Result<AuthContext, String> {
    if let Some(db) = state.auth_db.as_ref()
        && let Ok(record) = db.verify_api_key(key).await
    {
        let principal = db
            .get_principal(record.principal_id)
            .await
            .map_err(|_| "principal lookup failed".to_string())?
            .ok_or_else(|| "principal vanished".to_string())?;
        let scopes = resolve_api_key_scopes(&record.scopes, &principal, state);
        return Ok(AuthContext {
            principal: Some(Arc::new(principal)),
            scopes,
            kind: AuthKind::ApiKey,
        });
    }
    if let Some(expected) = state.api_key.as_ref() {
        if expected.is_empty() {
            return Err("server misconfigured: empty api_key".into());
        }
        use subtle::ConstantTimeEq;
        if key.as_bytes().ct_eq(expected.as_bytes()).into() {
            return Ok(AuthContext {
                principal: None,
                scopes: ScopeSet::full(),
                kind: AuthKind::SingleUserKey,
            });
        }
    }
    Err("invalid api key".into())
}

async fn resolve_auth_inner(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    query: &str,
) -> Result<AuthContext, axum::response::Response> {
    // ---- 1. Bearer JWT ----
    // Local Issuer first; falls through to ExternalOidcValidator
    // (tiled#1364, #1343) for tokens issued by upstream IdPs (Entra,
    // Auth0, Keycloak, …) when one is configured.
    if state.auth_db.is_some()
        && let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok())
        && let Some(token) = auth.strip_prefix("Bearer ")
    {
        return validate_bearer(state, token)
            .await
            .map_err(|e| unauthorized(&e));
    }

    // ---- 2. Apikey (multi-user → single-user fallback) ----
    let api_key_value = extract_api_key(headers, query);
    if let Some(key) = api_key_value {
        if let Some(db) = state.auth_db.as_ref()
            && let Ok(record) = db.verify_api_key(&key).await
        {
            let principal = match db.get_principal(record.principal_id).await {
                Ok(Some(p)) => Arc::new(p),
                _ => return Err(unauthorized("principal vanished")),
            };
            let scopes = resolve_api_key_scopes(&record.scopes, &principal, state);
            return Ok(AuthContext {
                principal: Some(principal),
                scopes,
                kind: AuthKind::ApiKey,
            });
        }
        if let Some(expected) = state.api_key.as_ref() {
            if expected.is_empty() {
                return Err(unauthorized("server misconfigured: empty api_key"));
            }
            use subtle::ConstantTimeEq;
            if key.as_bytes().ct_eq(expected.as_bytes()).into() {
                return Ok(AuthContext {
                    principal: None,
                    scopes: ScopeSet::full(),
                    kind: AuthKind::SingleUserKey,
                });
            }
        }
        return Err(unauthorized("invalid api key"));
    }

    // ---- 3. Proxied header ----
    if state.trust_forwarded_headers
        && let (Some(prox), Some(db)) = (state.proxied_header_auth.as_ref(), state.auth_db.as_ref())
        && let Some(subject) = prox.extract(headers)
    {
        let (principal, identity) = match db.ensure_principal(&subject.provider, &subject.sub).await
        {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(target: "tiled.auth", "proxied principal: {e}");
                return Err(unauthorized("proxied principal lookup failed"));
            }
        };
        db.touch_identity_login(identity.id).await.ok();
        return Ok(AuthContext {
            principal: Some(Arc::new(principal.clone())),
            scopes: mint_session_scopes(&principal, state),
            kind: AuthKind::Proxied,
        });
    }

    // ---- 4. Anonymous fallback ----
    // No auth backend configured at all: behaviour matches pre-multi-user
    // tiled-rs — full access. Operators that want to lock the server down
    // configure single-user `api_key` or wire the auth DB. `build_app` logs a
    // loud startup warning for this mode (server-C1); the predicate lives on
    // AppState so the warning and this grant can never diverge.
    if state.no_auth_configured() {
        return Ok(AuthContext {
            principal: None,
            scopes: ScopeSet::full(),
            kind: AuthKind::Anonymous,
        });
    }
    Err(unauthorized("authentication required"))
}

/// Derive session scopes for a principal using the same formula as
/// `auth_router::login` and `device_token`: role-based scope cap
/// intersected with the operator's `default_login_scopes`.
pub(crate) fn mint_session_scopes(principal: &tiled_auth::Principal, state: &AppState) -> ScopeSet {
    tiled_auth::ScopeSet::for_role(&principal.role).intersect(&state.default_login_scopes)
}

/// Resolve the effective scopes for an authenticated API key. The `inherit`
/// metascope is expanded to the principal's *current* role scopes (Python
/// dynamic inheritance, `authentication.py:372-381`), then the result is
/// capped by the principal's role and the operator's `default_login_scopes`,
/// so a downgraded principal's keys lose elevated scopes immediately. Single
/// owner for both the middleware (`resolve_auth_inner`) and the WS handshake
/// (`validate_apikey`) api-key paths.
fn resolve_api_key_scopes(
    key_scopes: &ScopeSet,
    principal: &tiled_auth::Principal,
    state: &AppState,
) -> ScopeSet {
    let role_scopes = tiled_auth::ScopeSet::for_role(&principal.role);
    key_scopes
        .expand_inherit(&role_scopes)
        .intersect(&mint_session_scopes(principal, state))
}

fn extract_api_key(headers: &axum::http::HeaderMap, query: &str) -> Option<String> {
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok())
        && let Some(key) = auth.strip_prefix("Apikey ")
    {
        return Some(key.to_string());
    }
    for pair in query.split('&') {
        if let Some(v) = pair.strip_prefix("api_key=") {
            return Some(v.to_string());
        }
    }
    None
}

fn unauthorized(msg: &str) -> axum::response::Response {
    (StatusCode::UNAUTHORIZED, msg.to_string()).into_response()
}

/// Request timeout middleware.
async fn timeout_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let limit = Duration::from_secs(state.request_timeout_secs);
    match tokio::time::timeout(limit, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (StatusCode::REQUEST_TIMEOUT, "Request timed out").into_response(),
    }
}
