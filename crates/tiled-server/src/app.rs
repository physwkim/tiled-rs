//! Application builder — constructs the Axum Router with all routes.

use std::time::Duration;

use axum::http::{HeaderValue, Method, StatusCode};
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

    Router::new()
        // Operational endpoints
        .route("/health", get(router::health))
        .route("/ready", get(router::ready))
        // API endpoints
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
        // Middleware stack (outermost → innermost)
        .layer(axum::middleware::from_fn(timeout_middleware))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

/// Request timeout middleware that returns 408 on timeout.
async fn timeout_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    match tokio::time::timeout(Duration::from_secs(30), next.run(request)).await {
        Ok(response) => response,
        Err(_elapsed) => (
            StatusCode::REQUEST_TIMEOUT,
            "Request timed out",
        )
            .into_response(),
    }
}

use axum::response::IntoResponse;
