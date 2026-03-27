//! Application builder — constructs the Axum Router with all routes.

use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::router;
use crate::state::{AppState, CorsOriginPolicy};

/// Build the Axum application with all routes attached.
pub fn build_app(state: AppState) -> Router {
    let cors = match &state.cors_policy {
        CorsOriginPolicy::Permissive => CorsLayer::permissive(),
        CorsOriginPolicy::AllowList(origins) => {
            let parsed: Vec<HeaderValue> = origins
                .iter()
                .filter_map(|o| o.parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(parsed)
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
                .allow_headers(tower_http::cors::Any)
        }
    };

    let needs_auth = state.api_key.is_some();

    let mut app = Router::new()
        // Operational endpoints (never require auth)
        .route("/health", get(router::health))
        .route("/ready", get(router::ready));

    // API endpoints
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
        // Bluesky document streaming (databroker compat)
        .route("/documents/{*path}", get(router::get_documents));

    // Apply auth middleware only to API routes when api_key is set
    app = if needs_auth {
        app.merge(api.layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api_key_middleware,
        )))
    } else {
        app.merge(api)
    };

    app.layer(axum::middleware::from_fn(timeout_middleware))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

/// API key authentication middleware.
///
/// Checks `?api_key=` query param or `Authorization: Apikey <key>` header.
async fn api_key_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let expected = match &state.api_key {
        Some(key) => key,
        None => return next.run(request).await,
    };

    // Check query parameter: ?api_key=<key>
    if let Some(query) = request.uri().query() {
        for pair in query.split('&') {
            if let Some(value) = pair.strip_prefix("api_key=")
                && value == expected
            {
                return next.run(request).await;
            }
        }
    }

    // Check Authorization header: "Apikey <key>"
    if let Some(auth) = request.headers().get("authorization")
        && let Ok(auth_str) = auth.to_str()
        && let Some(key) = auth_str.strip_prefix("Apikey ")
        && key == expected
    {
        return next.run(request).await;
    }

    (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response()
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
