//! Inline-mock-server tests for write paths, OIDC refresh-on-401, cache hit,
//! Vary handling, and CSRF rotation.
//!
//! These tests don't go through tiled-server (which has no write endpoints)
//! — they spin up an axum app per test that mimics the relevant endpoint
//! shapes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, SET_COOKIE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::{Barrier, Mutex};

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
    let url = url::Url::parse(&format!("{base}/api/v1/register/bluesky")).unwrap();
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
        ([(CACHE_CONTROL, "public, max-age=3600")], Json(body))
    }
    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .with_state(counter.clone());
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().cache(cache)).unwrap();
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
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().cache(cache)).unwrap();
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
        about["authentication"]["links"]["refresh_session"] = "auth/session/refresh".into();
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
        let val = all
            .first()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
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
// (5b) client-M1: token refresh must echo the LIVE (rotated) csrf as x-csrf,
// not a construction-time snapshot, and must capture a csrf the refresh
// response itself rotates. Native (non-OIDC) mode uses the x-csrf header.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn refresh_uses_live_csrf_and_captures_rotation() {
    #[derive(Default)]
    struct StateM1 {
        seen_refresh_csrf: Mutex<Option<String>>,
    }
    let state: Arc<StateM1> = Arc::new(StateM1::default());

    async fn handle_about() -> impl IntoResponse {
        let mut about = about_payload();
        about["authentication"]["links"]["refresh_session"] = "auth/session/refresh".into();
        // Initial csrf snapshot at login time.
        (
            [(SET_COOKIE, "tiled_csrf=csrf-initial; Path=/")],
            Json(about),
        )
    }

    // Rotates the csrf cookie on a normal authenticated GET (goes through
    // send_with_auth → maybe_capture_csrf), simulating server-side rotation
    // after login but before the refresh.
    async fn handle_rotate() -> impl IntoResponse {
        (
            [(SET_COOKIE, "tiled_csrf=csrf-rotated; Path=/")],
            Json(serde_json::json!({
                "data": {"id": "", "attributes": {"ancestors": [], "structure_family": "container"}, "links": {}}
            })),
        )
    }

    async fn handle_metadata(headers: HeaderMap) -> impl IntoResponse {
        let val = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if val == "Bearer new-token" {
            return Json(serde_json::json!({
                "data": {"id": "", "attributes": {"ancestors": [], "structure_family": "container"}, "links": {}}
            }))
            .into_response();
        }
        (StatusCode::UNAUTHORIZED, "expired").into_response()
    }

    async fn handle_refresh(
        State(state): State<Arc<StateM1>>,
        headers: HeaderMap,
        Json(_body): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        // Record the x-csrf the client actually sent.
        let csrf = headers
            .get("x-csrf")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        *state.seen_refresh_csrf.lock().await = Some(csrf);
        // The refresh response itself rotates the csrf cookie again.
        (
            [(SET_COOKIE, "tiled_csrf=csrf-after-refresh; Path=/")],
            Json(serde_json::json!({
                "access_token": "new-token",
                "refresh_token": "new-refresh",
            })),
        )
    }

    let app = Router::new()
        .route("/api/v1/", get(handle_about))
        .route("/api/v1/metadata/", get(handle_metadata))
        .route("/api/v1/rotate/", get(handle_rotate))
        .route("/api/v1/auth/session/refresh", post(handle_refresh))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
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

    // Server rotates csrf AFTER configure_auth froze the original snapshot.
    let rotate_url = url::Url::parse(&format!("{base}/api/v1/rotate/")).unwrap();
    let _ = ctx.get(&rotate_url).await.unwrap();
    assert_eq!(ctx.csrf_token().await.as_deref(), Some("csrf-rotated"));

    // A request with the expired token triggers a refresh; the refresh must
    // carry the rotated csrf, not the frozen "csrf-initial".
    let url = url::Url::parse(&format!("{base}/api/v1/metadata/")).unwrap();
    let resp = ctx.get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        state.seen_refresh_csrf.lock().await.as_deref(),
        Some("csrf-rotated"),
        "refresh must echo the LIVE csrf, not the construction-time snapshot"
    );
    assert_eq!(
        ctx.csrf_token().await.as_deref(),
        Some("csrf-after-refresh"),
        "the csrf rotated by the refresh response must be captured into the shared store"
    );
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
            if item
                .attributes
                .specs
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(|s| s.name == "xarray_dataset")
            {
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
    let envelope: serde_json::Value = tiled_client::utils::decode_response(resp).await.unwrap();
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

    async fn handle(State(c): State<Arc<AtomicU32>>, headers: HeaderMap) -> impl IntoResponse {
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
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().cache(cache)).unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/data")).unwrap();
    // First fetch: 200 from origin.
    let b1 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(&b1[..], b"hello-body");
    // Second fetch: cache stale (max-age=0) → If-None-Match → 304 → cached body.
    let b2 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(&b2[..], b"hello-body");
    // Both went to origin, but body content remained the cached one.
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    // Third fetch: now the 304 response had max-age=60, so we serve from cache
    // without hitting origin.
    let b3 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(&b3[..], b"hello-body");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// (8) POST invalidates cache for the same URL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_invalidates_cached_get_for_same_url() {
    let counter: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

    async fn handle_get(State(c): State<Arc<AtomicU32>>) -> impl IntoResponse {
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
        .route("/api/v1/thing", get(handle_get).post(handle_post))
        .with_state(counter.clone());
    let base = spawn(app).await;

    let cache = HttpCache::in_memory(1024 * 1024);
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().cache(cache)).unwrap();
    let url = url::Url::parse(&format!("{base}/api/v1/thing")).unwrap();
    let b1 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(&b1[..], b"v0");
    // Second GET: served from cache.
    let b2 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(&b2[..], b"v0");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    // POST invalidates the cached GET.
    ctx.post_json(&url, &serde_json::json!({"x": 1}))
        .await
        .unwrap();
    // Next GET goes back to origin.
    let b3 = ctx
        .get_bytes(&url, "application/octet-stream")
        .await
        .unwrap();
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

// ---------------------------------------------------------------------------
// (10) Concurrent 401s: single-flight refresh — only ONE network refresh call
//      even when N tasks all get 401 at the same time with the same stale token.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_401s_single_refresh() {
    const N: usize = 5;

    #[derive(Default)]
    struct ConcState {
        refresh_count: AtomicU32,
    }

    let state: Arc<ConcState> = Arc::new(ConcState::default());

    async fn handle_about_conc() -> impl IntoResponse {
        let mut about = about_payload();
        about["authentication"]["links"]["refresh_session"] = "auth/session/refresh".into();
        ([(SET_COOKIE, "tiled_csrf=csrf-token; Path=/")], Json(about))
    }

    async fn handle_metadata_conc(headers: HeaderMap) -> impl IntoResponse {
        let val = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if val == "Bearer old-token" {
            return (StatusCode::UNAUTHORIZED, "expired").into_response();
        }
        if val == "Bearer new-token" {
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

    async fn handle_refresh_conc(
        State(s): State<Arc<ConcState>>,
        Json(_body): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        let n = s.refresh_count.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // First call: succeed and hand out a fresh token pair.
            Json(serde_json::json!({
                "access_token": "new-token",
                "refresh_token": "new-refresh",
            }))
            .into_response()
        } else {
            // Subsequent calls: simulate a single-use (or already-rotated)
            // refresh token by returning 401.  The buggy code (no lock) would
            // clear the freshly-saved refresh token here, destroying the session.
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"detail": "refresh_token already used"})),
            )
                .into_response()
        }
    }

    let app = Router::new()
        .route("/api/v1/", get(handle_about_conc))
        .route("/api/v1/metadata/", get(handle_metadata_conc))
        .route("/api/v1/auth/session/refresh", post(handle_refresh_conc))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    ctx.server_info().await.unwrap();
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

    // A barrier ensures all N tasks call ctx.get() at the same instant so
    // they all send "Bearer old-token" before any refresh can happen.
    let barrier = Arc::new(Barrier::new(N));
    let url = url::Url::parse(&format!("{base}/api/v1/metadata/")).unwrap();

    let mut handles = Vec::new();
    for _ in 0..N {
        let ctx = ctx.clone();
        let url = url.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            ctx.get(&url).await
        }));
    }

    for h in handles {
        h.await
            .expect("task did not panic")
            .expect("all concurrent requests must succeed");
    }

    // The single-flight lock must have serialised the refresh: exactly one
    // network call to the token endpoint regardless of how many tasks raced.
    assert_eq!(
        state.refresh_count.load(Ordering::SeqCst),
        1,
        "expected exactly one refresh, got more — single-flight lock broken"
    );
}

// ---------------------------------------------------------------------------
// (11) ContainerClient.get within an active search routes through KeyLookup
//      (client M2). `node.search(q).get(key)` must look up `key` *within* the
//      filtered results — sending a KeyLookup filter plus the active queries to
//      the `search` link — and raise KeyNotFound when `key` is filtered out,
//      NOT fetch /metadata/.../key unconditionally. Python container.py:280-310.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_within_active_search_routes_through_keylookup() {
    use tiled_client::queries::Key;
    use tiled_client::{AnyClient, ContainerClient, Context, error::ClientError};

    // Fixed two-child "DB" the search endpoint filters over:
    //   alpha -> color=red ; beta -> color=blue
    fn child_container(name: &str) -> serde_json::Value {
        serde_json::json!({
            "id": name,
            "attributes": {
                "ancestors": [],
                "structure_family": "container",
                "metadata": {},
            },
            "links": {
                "self": format!("http://placeholder/api/v1/metadata/{name}"),
                "search": format!("http://placeholder/api/v1/search/{name}"),
                "full": format!("http://placeholder/api/v1/container/full/{name}"),
            }
        })
    }

    // Honor a `lookup` key filter together with an `eq(color, ...)` metadata
    // filter, mirroring the server's combined narrowing.
    async fn handle_search(
        axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
    ) -> impl IntoResponse {
        let lookup = |k: &str| {
            params
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        };
        let lookup_key = lookup("filter[lookup][condition][key]");
        let eq_key = lookup("filter[eq][condition][key]");
        let eq_val = lookup("filter[eq][condition][value]");
        let db = [("alpha", "red"), ("beta", "blue")];
        let mut data = Vec::new();
        for (name, color) in db {
            let key_ok = lookup_key.as_deref().map(|lk| lk == name).unwrap_or(true);
            let eq_ok = match (&eq_key, &eq_val) {
                // eq value is JSON-encoded by the client (e.g. "red" -> "\"red\"").
                (Some(k), Some(v)) if k == "color" => v == &format!("\"{color}\""),
                (Some(_), _) => false,
                _ => true,
            };
            if key_ok && eq_ok {
                data.push(child_container(name));
            }
        }
        let count = data.len();
        // Omit `links` entirely so it defaults to None — an empty object would
        // fail to deserialize into PaginationLinks (which requires `self`).
        Json(serde_json::json!({"data": data, "meta": {"count": count}}))
    }

    let app = Router::new()
        .route("/api/v1/", get(|| async { Json(about_payload()) }))
        .route("/api/v1/search/", get(handle_search));
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    let root_item = serde_json::from_value(serde_json::json!({
        "id": "",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": {
            "self": format!("{base}/api/v1/metadata/"),
            "search": format!("{base}/api/v1/search/"),
            "full": format!("{base}/api/v1/container/full/"),
        }
    }))
    .unwrap();
    let root = ContainerClient::from_item(ctx, root_item, false).unwrap();

    // Within a color=red filter, alpha is present and beta is filtered out.
    let filtered = root.search(Key::new("color").eq("red"));

    let alpha = filtered
        .get("alpha")
        .await
        .expect("alpha is in the filtered results");
    assert!(
        matches!(alpha, AnyClient::Container(_)),
        "get within search must return the matched child"
    );

    let err = filtered.get("beta").await.unwrap_err();
    assert!(
        matches!(err, ClientError::KeyNotFound(_)),
        "a key filtered out of the active search must be KeyNotFound, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// (12) ContainerClient.len() sends the `fields=count` projection hint (client
//      L1). Python container.py:206 sends `fields=count` so the server returns
//      only the count without materializing the item page (core.py:264 →
//      `items = []`). The Rust server ignores the hint but still returns
//      `meta.count`, so this is correct against both servers.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn len_sends_fields_count_projection() {
    use tiled_client::{ContainerClient, Context};

    #[derive(Default)]
    struct ServerState {
        last_params: Mutex<Vec<(String, String)>>,
    }
    let state: Arc<ServerState> = Arc::new(ServerState::default());

    async fn handle_search(
        State(state): State<Arc<ServerState>>,
        axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
    ) -> impl IntoResponse {
        *state.last_params.lock().await = params;
        // Mirror Python's count-only response: empty `data`, count in `meta`.
        Json(serde_json::json!({"data": [], "meta": {"count": 42}}))
    }

    let app = Router::new()
        .route("/api/v1/", get(|| async { Json(about_payload()) }))
        .route("/api/v1/search/", get(handle_search))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    // No inline `structure.count` → len() falls through to the search endpoint.
    let root_item = serde_json::from_value(serde_json::json!({
        "id": "",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": {
            "self": format!("{base}/api/v1/metadata/"),
            "search": format!("{base}/api/v1/search/"),
        }
    }))
    .unwrap();
    let root = ContainerClient::from_item(ctx, root_item, false).unwrap();

    let n = root.len().await.unwrap();
    assert_eq!(
        n, 42,
        "len() must read meta.count from the count-only response"
    );

    let params = state.last_params.lock().await.clone();
    assert!(
        params.iter().any(|(k, v)| k == "fields" && v == "count"),
        "len() must send the fields=count projection hint, got: {params:?}"
    );
}

// ---------------------------------------------------------------------------
// (13) ContainerClient.keys() sends the empty `fields=` projection hint and
//      parses id-only entries (client L2). Python container.py:243 sends
//      `fields=""` so the server returns only ids (core.py:248,476,611-620 →
//      attributes={"ancestors": ...}, self-link only). The Rust client must
//      send the hint AND deserialize the id-only resource shape.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn keys_sends_empty_fields_projection_and_parses_id_only_entries() {
    use tiled_client::{ContainerClient, Context};

    #[derive(Default)]
    struct ServerState {
        last_params: Mutex<Vec<(String, String)>>,
    }
    let state: Arc<ServerState> = Arc::new(ServerState::default());

    // Python's `fields=""` resource: id + attributes{ancestors} + self-link
    // only. No structure_family/structure/metadata.
    fn id_only(name: &str) -> serde_json::Value {
        serde_json::json!({
            "id": name,
            "attributes": { "ancestors": [] },
            "links": { "self": format!("http://placeholder/api/v1/metadata/{name}") }
        })
    }

    async fn handle_search(
        State(state): State<Arc<ServerState>>,
        axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
    ) -> impl IntoResponse {
        *state.last_params.lock().await = params;
        // Honor the projection by emitting id-only rows (no `next` link → one page).
        let data = vec![id_only("alpha"), id_only("beta")];
        Json(serde_json::json!({"data": data, "meta": {"count": 2}}))
    }

    let app = Router::new()
        .route("/api/v1/", get(|| async { Json(about_payload()) }))
        .route("/api/v1/search/", get(handle_search))
        .with_state(state.clone());
    let base = spawn(app).await;

    let (ctx, _) = Context::from_uri(&base).unwrap();
    let root_item = serde_json::from_value(serde_json::json!({
        "id": "",
        "attributes": { "ancestors": [], "structure_family": "container", "metadata": {} },
        "links": {
            "self": format!("{base}/api/v1/metadata/"),
            "search": format!("{base}/api/v1/search/"),
        }
    }))
    .unwrap();
    let root = ContainerClient::from_item(ctx, root_item, false).unwrap();

    let keys = root.keys().await.unwrap();
    assert_eq!(
        keys,
        vec!["alpha".to_string(), "beta".to_string()],
        "keys() must parse id-only entries into names"
    );

    let params = state.last_params.lock().await.clone();
    assert!(
        params.iter().any(|(k, v)| k == "fields" && v.is_empty()),
        "keys() must send the empty fields= projection hint, got: {params:?}"
    );
}

#[allow(dead_code)]
const _: fn() = || {
    let _ = HashMap::<String, String>::new();
};
