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
    // Bare routes (the SPA shell) may fall back to index.html.
    serve_path(&state, "index.html", true).await
}

async fn serve_static(State(state): State<Arc<AssetsState>>, Path(file): Path<String>) -> Response {
    // A missing /static/<file> is a hard 404 — never the index.html shell.
    // Falling back would return HTML+200 for a missing asset, breaking
    // cache-busting and hiding genuine 404s.
    serve_path(&state, &file, false).await
}

async fn serve_path(state: &AssetsState, path: &str, allow_index_fallback: bool) -> Response {
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

        // index.html fallback inside disk dir for SPA bare routes only
        // (hardcoded path, not subject to user input). Skipped for /static
        // requests so a missing asset 404s instead of returning the shell.
        let fallback = dir.join("index.html");
        if allow_index_fallback
            && path != "index.html"
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
    // Stop browsers from MIME-sniffing a served asset into a different,
    // possibly executable, content type.
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
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
        let resp = serve_path(&state, "app.js", false).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dotdot_component_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "..", false).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn dotdot_slash_path_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "../etc/passwd", false).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn absolute_path_returns_404() {
        let tmp = tempfile::tempdir().unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "/etc/passwd", false).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn served_asset_sets_nosniff() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("app.js"), b"console.log('hi');").unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "app.js", false).await;
        assert_eq!(
            resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
    }

    #[tokio::test]
    async fn missing_static_asset_returns_404_not_index() {
        // A configured assets_dir with an index.html present, but the
        // requested /static asset missing. Must 404 (allow_index_fallback
        // = false), not return the index.html shell with 200.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("index.html"), b"<html>shell</html>").unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "missing.js", false).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "missing /static asset must 404, not fall back to index.html"
        );
    }

    #[tokio::test]
    async fn bare_route_falls_back_to_index() {
        // A bare SPA route (allow_index_fallback = true) with the asset
        // missing still serves the index.html shell — the SPA-routing case.
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("index.html"), b"<html>shell</html>").unwrap();
        let state = make_state(tmp.path()).await;
        let resp = serve_path(&state, "some-spa-route", true).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
