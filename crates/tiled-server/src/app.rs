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

    // Auth endpoints — exempt from the auth middleware (you can't login
    // through a wall that demands a login token).
    let auth_routes = Router::new()
        .route("/api/v1/auth/{provider}/login", post(auth_router::login))
        .route("/api/v1/auth/refresh", post(auth_router::refresh))
        .route("/api/v1/auth/logout", post(auth_router::logout))
        .route("/api/v1/auth/whoami", get(auth_router::whoami))
        .route(
            "/api/v1/auth/device/initiate",
            post(auth_router::device_initiate),
        )
        .route(
            "/api/v1/auth/device/token",
            post(auth_router::device_token),
        )
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

    let api = api
        .merge(auth_routes)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    app = app.merge(api);

    app.layer(axum::middleware::from_fn(timeout_middleware))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
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

async fn resolve_auth_inner(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    query: &str,
) -> Result<AuthContext, axum::response::Response> {
    // ---- 1. Bearer JWT ----
    if let (Some(db), Some(issuer)) = (state.auth_db.as_ref(), state.issuer.as_ref()) {
        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok())
            && let Some(token) = auth.strip_prefix("Bearer ")
        {
            return match issuer.verify_access(token) {
                Ok(claims) => {
                    // Honour session revocation in real time.
                    let session = match db.lookup_session(&claims.sid).await {
                        Ok(s) => s,
                        Err(_) => return Err(unauthorized("session not found")),
                    };
                    if session.revoked {
                        return Err(unauthorized("session revoked"));
                    }
                    if session.expiration_time <= chrono::Utc::now() {
                        return Err(unauthorized("session expired"));
                    }
                    db.touch_session(&claims.sid).await.ok();
                    let principal = match db
                        .get_principal(session.principal_id)
                        .await
                    {
                        Ok(Some(p)) => Arc::new(p),
                        _ => return Err(unauthorized("principal not found")),
                    };
                    Ok(AuthContext {
                        principal: Some(principal),
                        scopes: claims.scopes.intersect(&session.scopes),
                        kind: AuthKind::Session,
                    })
                }
                Err(_) => Err(unauthorized("invalid bearer token")),
            };
        }
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
