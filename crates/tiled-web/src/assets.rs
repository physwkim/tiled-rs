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
        // --- path-traversal jail ---
        // Preliminary: reject any component that is not a plain filename —
        // '..' (ParentDir), '/' prefix (RootDir), Windows drive prefix, or
        // a bare '.' (CurDir). Only Component::Normal passes.
        use std::path::Component;
        let safe = std::path::Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
        if !safe {
            return (StatusCode::NOT_FOUND, "asset not found").into_response();
        }

        let candidate = dir.join(path);

        // Canonicalize-based jail check: resolves symlinks and confirms the
        // resolved path is still inside the assets directory.
        // canonicalize() returns Err when the file does not exist; we treat
        // that the same as before — fall through to the index.html fallback
        // and embedded assets.
        if let Ok(canon_candidate) = tokio::fs::canonicalize(&candidate).await {
            let canon_dir = match tokio::fs::canonicalize(dir).await {
                Ok(d) => d,
                Err(_) => {
                    return (StatusCode::NOT_FOUND, "asset not found").into_response();
                }
            };
            if !canon_candidate.starts_with(&canon_dir) {
                return (StatusCode::NOT_FOUND, "asset not found").into_response();
            }
            // File exists and is within the jail — safe to serve.
            if let Ok(bytes) = tokio::fs::read(&candidate).await {
                return ok_response(path, bytes);
            }
        }

        // index.html fallback inside disk dir for SPA routes (hardcoded path,
        // not subject to user input).
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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;

    async fn make_state(dir: &std::path::Path) -> AssetsState {
        AssetsState {
            dir: Some(dir.to_path_buf()),
        }
    }

    #[tokio::test]
    async fn normal_asset_is_served() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("app.js"), b"console.log('hi');").unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "app.js").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dotdot_component_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "..").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dotdot_slash_path_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "../etc/passwd").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn absolute_path_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "/etc/passwd").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
