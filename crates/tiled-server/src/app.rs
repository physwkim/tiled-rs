//! Application builder — constructs the Axum Router with all routes.
//!
//! Corresponds to `tiled/server/app.py`.

use axum::http::{HeaderValue, Method};
use axum::routing::get;
use axum::Router;
use tower_http::cors::CorsLayer;

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
        .layer(cors)
        .with_state(state)
}
