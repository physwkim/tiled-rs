//! Session-lifetime parity: a login session's absolute cap is `session_max_age`
//! (default 365 d), NOT the 7-day refresh-token TTL.
//!
//! Upstream keeps two distinct knobs: `create_session` sets
//! `expiration_time = utcnow() + session_max_age` (365 d, `authentication.py:826`,
//! `settings.py:36`), while the refresh *token* lives `refresh_token_max_age`
//! (7 d, `settings.py:35`). `slide_session` rotates the refresh token on each
//! refresh but never extends `expiration_time` (`authentication.py:1489/1540`).
//! tiled-rs previously collapsed the absolute cap onto the 7-day refresh TTL, so
//! sessions hard-expired ~52x sooner. These tests pin the restored split.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use chrono::Utc;
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer, ScopeSet};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::AppState;

/// Build a multi-user app and hand back clones of the `Issuer` and `AuthDb` so
/// the test can decode the refresh token's `sid` and inspect the created
/// session row directly.
async fn make_app() -> (axum::Router, Issuer, AuthDb, tempfile::TempDir) {
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

    // Clones for post-login inspection; the originals move into AppState.
    let issuer_probe = issuer.clone();
    let db_probe = auth_db.clone();

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
        auth_db: Some(auth_db),
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
        default_login_scopes: ScopeSet::full(),
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
    (
        tiled_rs::server::build_app(state),
        issuer_probe,
        db_probe,
        dir,
    )
}

/// POST /auth/dummy/login for alice; returns `(access_token, refresh_token)`.
async fn login(app: &axum::Router) -> (String, String) {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "alice", "password": "wonderland"})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "login should succeed");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (
        body["access_token"].as_str().unwrap().to_string(),
        body["refresh_token"].as_str().unwrap().to_string(),
    )
}

/// A freshly created login session must be capped at `session_max_age`
/// (default 365 d), not the 7-day refresh-token TTL. Pre-fix this session
/// expired in ~7 days.
#[tokio::test]
async fn login_session_expires_at_session_max_age_not_refresh_ttl() {
    let (app, issuer, db, _dir) = make_app().await;
    let (_access, refresh) = login(&app).await;

    let sid = issuer.verify_refresh(&refresh).unwrap().sid;
    let session = db.lookup_session(&sid).await.unwrap();

    let ttl_days = (session.expiration_time - Utc::now()).num_days();
    // Default session_max_age is 365 d. Allow slack for clock/rounding but keep
    // the assertion far above the 7-day refresh TTL so the regression is pinned.
    assert!(
        ttl_days >= 300,
        "session must be capped at session_max_age (~365 d), got {ttl_days} d \
         (collapsed onto the 7-day refresh TTL?)"
    );
}

/// Refreshing a session rotates the refresh token but must NOT extend the
/// session's `expiration_time` — upstream `slide_session` never slides the
/// absolute cap. This pins that tiled-rs does not silently introduce sliding.
#[tokio::test]
async fn refresh_does_not_extend_session_expiration() {
    let (app, issuer, db, _dir) = make_app().await;
    let (_access, refresh) = login(&app).await;

    let sid = issuer.verify_refresh(&refresh).unwrap().sid;
    let before = db.lookup_session(&sid).await.unwrap().expiration_time;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/refresh")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({ "refresh_token": refresh })).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "refresh should succeed");

    let after = db.lookup_session(&sid).await.unwrap().expiration_time;
    assert_eq!(
        before, after,
        "refresh must not extend the session's absolute expiration_time"
    );
}
