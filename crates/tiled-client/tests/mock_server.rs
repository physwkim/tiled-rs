//! Inline-mock-server tests for write paths, OIDC refresh-on-401, cache hit,
//! Vary handling, and CSRF rotation.
//!
//! These tests don't go through tiled-server (which has no write endpoints)
//! — they spin up an axum app per test that mimics the relevant endpoint
//! shapes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::extract::{Path, State};
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, SET_COOKIE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use tiled_client::{Context, ContextOptions, HttpCache};

async fn spawn(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    format!("http://{addr}")
}

fn about_payload() -> serde_json::Value {
    serde_json::json!({
        "api_version": 0,
        "library_version": "test",
        "formats": {},
        "aliases": {},
        "queries": [],
        "authentication": {
            "required": false,
            "providers": [],
            "links": {
                "refresh_session": "/auth/session/refresh",
                "whoami": "/auth/whoami",
                "logout": null,
            }
        },
        "links": {},
        "meta": {},
    })
}

// ---------------------------------------------------------------------------
// (1) POST register endpoint round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_json_round_trip() {
    #[derive(Default)]
    struct ServerState {
        last_body: Mutex<Option<serde_json::Value>>,
    }
    let state: Arc<ServerState> = Arc::new(ServerState::default());

    async fn handle_register(
        State(state): State<Arc<ServerState>>,
        Path(_path): Path<String>,
        Json(body): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        *state.last_body.lock().await = Some(body.clone());
        Json(serde_json::json!({"id": "ok"}))
    }
    async fn handle_about() -> impl IntoResponse {
        Json(about_payload())
    }

    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .route("/api/v1/register/{*path}", post(handle_register))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    let url =
        url::Url::parse(&format!("{base}/api/v1/register/bluesky")).unwrap();
    let body = serde_json::json!({
        "structure_family": "container",
        "metadata": {"start": {"uid": "abc"}},
        "specs": [{"name": "BlueskyRun"}],
        "key": "abc",
    });
    let resp = ctx.post_json(&url, &body).await.unwrap();
    assert_eq!(resp.status(), 200);
    let recorded = state.last_body.lock().await.clone().unwrap();
    assert_eq!(recorded, body);
}

// ---------------------------------------------------------------------------
// (2) Cache hit avoids second HTTP call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cache_serves_second_get_without_calling_server() {
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

    async fn handle_about(State(c): State<Arc<AtomicU32>>) -> impl IntoResponse {
        c.fetch_add(1, Ordering::SeqCst);
        let body = about_payload();
        (
            [(CACHE_CONTROL, "public, max-age=3600")],
            Json(body),
        )
    }
    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .with_state(counter.clone());
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().cache(cache),
    )
    .unwrap();
    let _ = ctx.server_info().await.unwrap();
    // Second call should be served from cache.
    let url = url::Url::parse(&format!("{base}/api/v1/")).unwrap();
    let _ = ctx.get(&url).await.unwrap();
    let _ = ctx.get(&url).await.unwrap();
    let calls = counter.load(Ordering::SeqCst);
    // First call from server_info (warm), follow-ups served from cache.
    assert_eq!(calls, 1, "expected one origin hit, got {calls}");
}

// ---------------------------------------------------------------------------
// (3) Cache respects Vary by serving different Accept variants separately
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cache_keys_by_accept() {
    async fn handle(headers: HeaderMap) -> impl IntoResponse {
        let accept = headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        (
            [
                (CACHE_CONTROL, "public, max-age=3600".to_string()),
                ("vary".parse().unwrap(), "Accept".into()),
            ],
            accept.into_bytes(),
        )
    }
    let app = Router::new().route("/api/v1/data", get(handle));
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().cache(cache),
    )
    .unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/data")).unwrap();
    let b1 = ctx.get_bytes(&url, "application/json").await.unwrap();
    let b2 = ctx.get_bytes(&url, "application/x-msgpack").await.unwrap();
    assert_eq!(&b1[..], b"application/json");
    assert_eq!(&b2[..], b"application/x-msgpack");
    // Re-fetch — should be served from cache.
    let b1b = ctx.get_bytes(&url, "application/json").await.unwrap();
    let b2b = ctx.get_bytes(&url, "application/x-msgpack").await.unwrap();
    assert_eq!(b1, b1b);
    assert_eq!(b2, b2b);
}

// ---------------------------------------------------------------------------
// (4) OIDC refresh-on-401: replace Authorization header instead of duplicating
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auth_refresh_on_401_replaces_authorization() {
    #[derive(Default)]
    struct State401 {
        seen_old: AtomicU32,
        seen_new: AtomicU32,
        refresh_count: AtomicU32,
    }
    let state: Arc<State401> = Arc::new(State401::default());

    async fn handle_about() -> impl IntoResponse {
        let mut about = about_payload();
        about["authentication"]["links"]["refresh_session"] =
            "auth/session/refresh".into();
        ([(SET_COOKIE, "tiled_csrf=csrf-token; Path=/")], Json(about))
    }

    async fn handle_metadata(
        State(state): State<Arc<State401>>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        // count Authorization header values
        let all: Vec<&[u8]> = headers
            .get_all(AUTHORIZATION)
            .iter()
            .map(|v| v.as_bytes())
            .collect();
        if all.len() > 1 {
            return (StatusCode::BAD_REQUEST, "duplicate Authorization header").into_response();
        }
        let val = all.first().map(|b| std::str::from_utf8(b).unwrap_or(""))
            .unwrap_or("");
        if val == "Bearer old-token" {
            state.seen_old.fetch_add(1, Ordering::SeqCst);
            return (StatusCode::UNAUTHORIZED, "expired").into_response();
        }
        if val == "Bearer new-token" {
            state.seen_new.fetch_add(1, Ordering::SeqCst);
            return Json(serde_json::json!({
                "data": {
                    "id": "",
                    "attributes": {"ancestors": [], "structure_family": "container"},
                    "links": {}
                }
            }))
            .into_response();
        }
        (StatusCode::UNAUTHORIZED, "unknown token").into_response()
    }

    async fn handle_refresh(
        State(state): State<Arc<State401>>,
        Json(_body): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        state.refresh_count.fetch_add(1, Ordering::SeqCst);
        Json(serde_json::json!({
            "access_token": "new-token",
            "refresh_token": "new-refresh",
        }))
    }

    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .route("/api/v1/metadata/", get(handle_metadata))
        .route("/api/v1/auth/session/refresh", post(handle_refresh))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    // Pre-load tokens via configure_auth (no interactive prompt).
    let _ = ctx.server_info().await.unwrap();
    ctx.configure_auth(
        tiled_client::Tokens {
            access_token: "old-token".into(),
            refresh_token: "old-refresh".into(),
            id_token: None,
        },
        false,
    )
    .await
    .unwrap();

    let url = url::Url::parse(&format!("{base}/api/v1/metadata/")).unwrap();
    let resp = ctx.get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(state.seen_old.load(Ordering::SeqCst), 1);
    assert_eq!(state.seen_new.load(Ordering::SeqCst), 1);
    assert_eq!(state.refresh_count.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// (5) CSRF rotation: cookie value changes, x-csrf header follows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn csrf_token_rotates_on_subsequent_responses() {
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

    async fn handle_about(State(c): State<Arc<AtomicU32>>) -> impl IntoResponse {
        let n = c.fetch_add(1, Ordering::SeqCst);
        let cookie = format!("tiled_csrf=token-{n}; Path=/");
        ([(SET_COOKIE, cookie)], Json(about_payload()))
    }
    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .with_state(counter.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/")).unwrap();
    let _ = ctx.get(&url).await.unwrap();
    assert_eq!(ctx.csrf_token().await.as_deref(), Some("token-0"));
    let _ = ctx.get(&url).await.unwrap();
    assert_eq!(ctx.csrf_token().await.as_deref(), Some("token-1"));
}

// ---------------------------------------------------------------------------
// (6) ClientResolver dispatch wraps a node with a custom client type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_resolver_emits_custom_variant() {
    use std::any::Any;
    use tiled_client::any_client::{AnyClient, ClientResolver};
    use tiled_client::base::Item;

    #[derive(Debug)]
    struct DatasetFlavored(String);

    #[derive(Debug)]
    struct Resolver;
    impl ClientResolver for Resolver {
        fn resolve(
            &self,
            _ctx: &Context,
            item: &Item,
            _include_data_sources: bool,
        ) -> Option<tiled_client::Result<std::sync::Arc<dyn Any + Send + Sync>>> {
            if item.attributes.specs.as_deref().unwrap_or(&[]).iter().any(|s| s.name == "xarray_dataset") {
                Some(Ok(std::sync::Arc::new(DatasetFlavored(item.id.clone()))))
            } else {
                None
            }
        }
    }

    async fn handle_about() -> impl IntoResponse {
        Json(about_payload())
    }
    async fn handle_metadata() -> impl IntoResponse {
        Json(serde_json::json!({
            "data": {
                "id": "fancy",
                "attributes": {
                    "ancestors": [],
                    "structure_family": "container",
                    "specs": [{"name": "xarray_dataset"}],
                    "metadata": {},
                },
                "links": {}
            }
        }))
    }
    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .route("/api/v1/metadata/", get(handle_metadata));
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().resolver(Arc::new(Resolver)),
    )
    .unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/metadata/")).unwrap();
    let resp = ctx.get(&url).await.unwrap();
    let envelope: serde_json::Value =
        tiled_client::utils::decode_response(resp).await.unwrap();
    let item: Item = serde_json::from_value(envelope["data"].clone()).unwrap();
    let any = AnyClient::from_item(ctx, item, false).unwrap();
    let custom: &DatasetFlavored = any.as_custom().expect("custom variant");
    assert_eq!(custom.0, "fancy");
}

// ---------------------------------------------------------------------------
// (7) 304 Not Modified roundtrip — server says "still fresh", client serves
//     from cache and updates the entry's freshness from the 304 response.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn revalidation_with_304_serves_cached_body() {
    use std::sync::atomic::AtomicU32;

    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

    async fn handle(
        State(c): State<Arc<AtomicU32>>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let n = c.fetch_add(1, Ordering::SeqCst);
        // First request: full 200 with ETag + max-age=0 (forces revalidate next time).
        if n == 0 {
            return (
                [
                    (CACHE_CONTROL, "public, max-age=0".to_string()),
                    ("etag".parse().unwrap(), "\"v1\"".into()),
                ],
                axum::body::Body::from("hello-body"),
            )
                .into_response();
        }
        // Subsequent requests: must include If-None-Match: "v1" → 304.
        let inm = headers
            .get("if-none-match")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if inm == "\"v1\"" {
            return (
                StatusCode::NOT_MODIFIED,
                [
                    (CACHE_CONTROL, "public, max-age=60".to_string()),
                    ("etag".parse().unwrap(), "\"v1\"".to_string()),
                ],
            )
                .into_response();
        }
        (StatusCode::INTERNAL_SERVER_ERROR, "no validators sent").into_response()
    }
    let app = Router::new()
        .route("/api/v1/data", get(handle))
        .with_state(counter.clone());
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().cache(cache),
    )
    .unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/data")).unwrap();
    // First fetch: 200 from origin.
    let b1 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b1[..], b"hello-body");
    // Second fetch: cache stale (max-age=0) → If-None-Match → 304 → cached body.
    let b2 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b2[..], b"hello-body");
    // Both went to origin, but body content remained the cached one.
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    // Third fetch: now the 304 response had max-age=60, so we serve from cache
    // without hitting origin.
    let b3 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b3[..], b"hello-body");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// (8) POST invalidates cache for the same URL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_invalidates_cached_get_for_same_url() {
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

    async fn handle_get(
        State(c): State<Arc<AtomicU32>>,
    ) -> impl IntoResponse {
        let n = c.fetch_add(1, Ordering::SeqCst);
        let body = format!("v{n}");
        (
            [(CACHE_CONTROL, "public, max-age=3600")],
            axum::body::Body::from(body),
        )
    }
    async fn handle_post() -> impl IntoResponse {
        Json(serde_json::json!({"ok": true}))
    }

    let app = Router::new()
        .route(
            "/api/v1/thing",
            get(handle_get).post(handle_post),
        )
        .with_state(counter.clone());
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().cache(cache),
    )
    .unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/thing")).unwrap();
    let b1 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b1[..], b"v0");
    // Second GET: served from cache.
    let b2 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b2[..], b"v0");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    // POST invalidates the cached GET.
    ctx.post_json(&url, &serde_json::json!({"x": 1})).await.unwrap();
    // Next GET goes back to origin.
    let b3 = ctx.get_bytes(&url, "application/octet-stream").await.unwrap();
    assert_eq!(&b3[..], b"v1");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// (9) AnyClient::Clone with Custom variant doesn't panic.
// ---------------------------------------------------------------------------

#[test]
fn any_client_custom_clone_is_safe() {
    use std::sync::Arc as StdArc;
    use tiled_client::any_client::AnyClient;

    #[derive(Debug)]
    struct Marker(u32);

    let custom: AnyClient = AnyClient::Custom(StdArc::new(Marker(42)));
    let cloned = custom.clone();
    let m: &Marker = cloned.as_custom().expect("downcast");
    assert_eq!(m.0, 42);
}

#[allow(dead_code)]
const _: fn() = || {
    let _ = HashMap::<String, String>::new();
};
