//! Per-principal API-key / session caps (Python parity: `API_KEY_LIMIT = 100`,
//! `SESSION_LIMIT = 200`, `tiled/server/authentication.py:84-85`).
//!
//! Upstream hard-rejects at the limit with `HTTPException(400)` *before* the
//! insert (`authentication.py:817-823` sessions, `:1215-1221` keys). tiled-rs
//! enforces the same cap inside the sole INSERT-owner (`create_session` /
//! `create_api_key`), so every caller path is bounded by construction and the
//! HTTP routes surface 400.
//!
//! Counting matches upstream: ALL rows for the principal count, with no
//! expiration/revoked exclusion. These tests seed rows up to `limit - 1`
//! directly through the real DB API, then cross the boundary over HTTP so the
//! `200` (allowed) → `400` (rejected) transition is observed at the route.
//!
//! The seed counts (`99`, `199`) are `limit - 1`; the limits mirror the private
//! `API_KEY_LIMIT` / `SESSION_LIMIT` constants in `src/auth/{api_key,session}.rs`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{ApiKeyCreate, AuthDb, DummyAuthenticator, Issuer, ScopeSet};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::AppState;

/// Build a multi-user app and hand back a clone of its `AuthDb` so a test can
/// seed rows directly, plus the `TempDir` guarding the on-disk sqlite files.
async fn make_app() -> (axum::Router, AuthDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
        auth_db: Some(auth_db.clone()),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes: AppState::default_login_scopes(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (tiled_rs::server::build_app(state), auth_db, dir)
}

fn login_request() -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "alice", "password": "wonderland"})).unwrap(),
        ))
        .unwrap()
}

/// Log in and return the raw response (status + body) for inspection.
async fn login(app: &axum::Router) -> axum::response::Response {
    app.clone().oneshot(login_request()).await.unwrap()
}

/// Log in and extract the access token (asserts the login itself was 200).
async fn login_access_token(app: &axum::Router) -> String {
    let resp = login(app).await;
    assert_eq!(resp.status(), StatusCode::OK, "login should succeed");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

fn create_apikey_request(access: &str) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/apikeys")
        .header("authorization", format!("Bearer {access}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&json!({})).unwrap()))
        .unwrap()
}

/// The 100th API key for a principal is allowed (200); the 101st is rejected
/// with HTTP 400 — matching upstream's `HTTPException(400)` at `API_KEY_LIMIT`.
#[tokio::test]
async fn api_key_creation_capped_at_limit() {
    let (app, db, _dir) = make_app().await;

    // Materialise alice's principal, then seed it up to `API_KEY_LIMIT - 1`
    // (= 99) keys directly. The boundary-crossing 100th/101st go over HTTP.
    let (principal, _identity) = db.ensure_principal("dummy", "alice").await.unwrap();
    for _ in 0..99 {
        db.create_api_key(ApiKeyCreate {
            principal_id: principal.id,
            note: None,
            scopes: ScopeSet::read_only(),
            expiration_time: None,
        })
        .await
        .expect("seeding below the limit must succeed");
    }

    let access = login_access_token(&app).await;

    // 100th key: at the limit boundary but still allowed.
    let hundredth = app
        .clone()
        .oneshot(create_apikey_request(&access))
        .await
        .unwrap();
    assert_eq!(
        hundredth.status(),
        StatusCode::OK,
        "the 100th key (== API_KEY_LIMIT) must still be created"
    );

    // 101st key: over the limit → 400 (pre-fix this returns 200).
    let over = app
        .clone()
        .oneshot(create_apikey_request(&access))
        .await
        .unwrap();
    assert_eq!(
        over.status(),
        StatusCode::BAD_REQUEST,
        "the 101st key (> API_KEY_LIMIT) must be rejected with HTTP 400"
    );
}

/// The 200th session for a principal is allowed (200); the 201st login is
/// rejected with HTTP 400 — matching upstream's `HTTPException(400)` at
/// `SESSION_LIMIT`.
#[tokio::test]
async fn session_creation_capped_at_limit() {
    let (app, db, _dir) = make_app().await;

    // Seed alice up to `SESSION_LIMIT - 1` (= 199) sessions directly (plain
    // INSERTs, no password hashing). The 200th/201st are minted via HTTP login.
    let (principal, _identity) = db.ensure_principal("dummy", "alice").await.unwrap();
    let expires = chrono::Utc::now() + chrono::Duration::days(1);
    for _ in 0..199 {
        db.create_session(principal.id, ScopeSet::read_only(), expires, json!({}))
            .await
            .expect("seeding below the limit must succeed");
    }

    // 200th session: at the limit boundary but still allowed.
    assert_eq!(
        login(&app).await.status(),
        StatusCode::OK,
        "the 200th session (== SESSION_LIMIT) must still be created"
    );

    // 201st session: over the limit → 400 (pre-fix this returns 200).
    assert_eq!(
        login(&app).await.status(),
        StatusCode::BAD_REQUEST,
        "the 201st session (> SESSION_LIMIT) must be rejected with HTTP 400"
    );
}
