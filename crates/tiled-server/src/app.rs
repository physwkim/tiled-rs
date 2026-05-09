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
    let cors = match &state.cors_policy {
        CorsOriginPolicy::Permissive => CorsLayer::permissive(),
        CorsOriginPolicy::AllowList(origins) => {
            let parsed: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();
            CorsLayer::new()
                .allow_origin(parsed)
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers(tower_http::cors::Any)
        }
    };

    let mut app = Router::new()
        // Operational endpoints (never require auth)
        .route("/health", get(router::health))
        .route("/ready", get(router::ready));

    // Public auth endpoints — login/refresh/device/initiate must work
    // without prior auth (otherwise login is unreachable). Each handler
    // does its own credential check where required.
    let public_auth = Router::new()
        .route("/api/v1/auth/{provider}/login", post(auth_router::login))
        .route("/api/v1/auth/refresh", post(auth_router::refresh))
        .route(
            "/api/v1/auth/device/initiate",
            post(auth_router::device_initiate),
        )
        .route(
            "/api/v1/auth/device/token",
            post(auth_router::device_token),
        );

    // Authenticated auth endpoints — must run inside the auth middleware
    // so AuthContext is populated.
    let private_auth = Router::new()
        .route("/api/v1/auth/logout", post(auth_router::logout))
        .route("/api/v1/auth/whoami", get(auth_router::whoami))
        .route(
            "/api/v1/auth/device/approve",
            post(auth_router::device_approve),
        )
        .route(
            "/api/v1/auth/apikeys",
            get(auth_router::api_key_list).post(auth_router::api_key_create),
        )
        .route(
            "/api/v1/auth/apikeys/{first_eight}",
            delete(auth_router::api_key_revoke),
        );

    // WebSocket subscribe routes are intentionally OUTSIDE the auth
    // middleware — browsers can't set the `Authorization` header on
    // WebSocket connections, so we accept the upgrade unauthenticated
    // and run a first-message handshake inside the handler (tiled#1351).
    // Server-side auth still happens before any data flows; if the
    // handshake fails the socket is closed.
    let ws = Router::new()
        .route(
            "/api/v1/array/subscribe/{*path}",
            get(crate::streaming::ws_subscribe),
        )
        .route(
            "/api/v1/container/subscribe/{*path}",
            get(crate::streaming::ws_subscribe),
        )
        .route(
            "/api/v1/table/subscribe/{*path}",
            get(crate::streaming::ws_subscribe),
        );

    // Data API endpoints — auth middleware always runs and either
    // populates AuthContext or returns 401.
    let api = Router::new()
        .route("/api/v1/", get(router::about))
        .route("/api/v1/metadata/", get(router::metadata_root))
        .route("/api/v1/metadata/{*path}", get(router::metadata))
        .route("/api/v1/search/", get(router::search_root))
        .route("/api/v1/search/{*path}", get(router::search))
        .route("/api/v1/array/block/{*path}", get(router::array_block))
        .route(
            "/api/v1/table/partition/{*path}",
            get(router::table_partition),
        )
        .route("/api/v1/register/", post(router::register_root))
        .route("/api/v1/register/{*path}", post(router::register))
        .route("/api/v1/metadata/{*path}", patch(router::patch_metadata))
        .route("/api/v1/metadata/{*path}", delete(router::delete_metadata))
        .route("/api/v1/data_source/{*path}", put(router::put_data_source))
        .route("/documents/{*path}", get(router::get_documents));

    let guarded = api
        .merge(private_auth)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    app = app.merge(public_auth).merge(ws).merge(guarded);

    let body_limit = state.max_request_body_bytes;
    app.layer(axum::extract::DefaultBodyLimit::max(body_limit))
        .layer(axum::middleware::from_fn(correlation_id_middleware))
        .layer(axum::middleware::from_fn(timeout_middleware))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
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
    request.extensions_mut().insert(RequestId(request_id.clone()));
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
    if let Some(issuer) = state.issuer.as_ref() {
        if let Ok(claims) = issuer.verify_access(token) {
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
        return Ok(AuthContext {
            principal: Some(Arc::new(principal)),
            scopes: state.default_login_scopes.clone(),
            kind: AuthKind::Session,
        });
    }
    Err("no JWT issuer or external OIDC configured".into())
}

/// Validate an Apikey outside the request middleware. Multi-user DB
/// first, single-user CLI flag fallback. Same constant-time compare
/// the middleware uses (R3 timing-attack fix).
pub async fn validate_apikey(state: &AppState, key: &str) -> Result<AuthContext, String> {
    if let Some(db) = state.auth_db.as_ref() {
        if let Ok(record) = db.verify_api_key(key).await {
            let principal = db
                .get_principal(record.principal_id)
                .await
                .map_err(|_| "principal lookup failed".to_string())?
                .ok_or_else(|| "principal vanished".to_string())?;
            return Ok(AuthContext {
                principal: Some(Arc::new(principal)),
                scopes: record.scopes,
                kind: AuthKind::ApiKey,
            });
        }
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
        if let Some(db) = state.auth_db.as_ref() {
            if let Ok(record) = db.verify_api_key(&key).await {
                let principal = match db.get_principal(record.principal_id).await {
                    Ok(Some(p)) => Arc::new(p),
                    _ => return Err(unauthorized("principal vanished")),
                };
                return Ok(AuthContext {
                    principal: Some(principal),
                    scopes: record.scopes,
                    kind: AuthKind::ApiKey,
                });
            }
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
    if state.trust_forwarded_headers {
        if let (Some(prox), Some(db)) = (
            state.proxied_header_auth.as_ref(),
            state.auth_db.as_ref(),
        ) {
            if let Some(subject) = prox.extract(headers) {
                let (principal, identity) = match db
                    .ensure_principal(&subject.provider, &subject.sub)
                    .await
                {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(target: "tiled.auth", "proxied principal: {e}");
                        return Err(unauthorized("proxied principal lookup failed"));
                    }
                };
                db.touch_identity_login(identity.id).await.ok();
                return Ok(AuthContext {
                    principal: Some(Arc::new(principal)),
                    scopes: ScopeSet::full(),
                    kind: AuthKind::Proxied,
                });
            }
        }
    }

    // ---- 4. Anonymous fallback ----
    // No auth backend configured at all: behaviour matches pre-multi-user
    // tiled-rs — full access. Operators that want to lock the server down
    // configure single-user `api_key` or wire the auth DB.
    let no_auth_configured =
        state.api_key.is_none() && state.auth_db.is_none();
    if no_auth_configured {
        return Ok(AuthContext {
            principal: None,
            scopes: ScopeSet::full(),
            kind: AuthKind::Anonymous,
        });
    }
    Err(unauthorized("authentication required"))
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
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match tokio::time::timeout(Duration::from_secs(30), next.run(request)).await {
        Ok(response) => response,
        Err(_) => (StatusCode::REQUEST_TIMEOUT, "Request timed out").into_response(),
    }
}
