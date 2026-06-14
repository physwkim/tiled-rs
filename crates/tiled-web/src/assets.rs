//! Embedded SPA bundle.
//!
//! Compile-time embed of `assets/spa/`. The directory is populated by
//! `build.rs`: `trunk build` output if trunk is on PATH, otherwise the
//! committed `assets/spa-placeholder/` is copied in. Routes:
//!   - `GET /` → `index.html`
//!   - `GET /static/<file>` → matching asset
//!   - `GET /<spa-route>` → `index.html` (SPA fallback)
//!
//! Operators can override at runtime with `--web-assets-dir <path>` to
//! load from disk instead of the embedded bundle.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "assets/spa/"]
struct SpaAssets;

#[derive(Clone)]
struct AssetsState {
    dir: Option<PathBuf>,
}

pub fn spa_router_with(dir: Option<PathBuf>) -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/static/{file}", get(serve_static))
        .with_state(Arc::new(AssetsState { dir }))
}

async fn serve_index(State(state): State<Arc<AssetsState>>) -> Response {
    serve_path(&state, "index.html").await
}

async fn serve_static(State(state): State<Arc<AssetsState>>, Path(file): Path<String>) -> Response {
    serve_path(&state, &file).await
}

async fn serve_path(state: &AssetsState, path: &str) -> Response {
    // Prefer disk first when an assets_dir is configured — operators
    // typically swap in the real bluesky/tiled WebUI bundle this way.
    if let Some(dir) = &state.dir {
        let candidate = dir.join(path);
        if let Ok(bytes) = tokio::fs::read(&candidate).await {
            return ok_response(path, bytes);
        }
        // index.html fallback inside disk dir for SPA routes.
        let fallback = dir.join("index.html");
        if path != "index.html"
            && let Ok(bytes) = tokio::fs::read(&fallback).await
        {
            return ok_response("index.html", bytes);
        }
    }
    match SpaAssets::get(path) {
        Some(asset) => ok_response(path, asset.data.to_vec()),
        None => (StatusCode::NOT_FOUND, "asset not found").into_response(),
    }
}

fn ok_response(path: &str, bytes: Vec<u8>) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime.as_ref())
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    // Cache static assets for a year — they're immutable per build.
    // index.html intentionally NOT cached so a reload picks up new
    // bundle hashes.
    if path != "index.html" {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
    }
    (StatusCode::OK, headers, bytes).into_response()
}
