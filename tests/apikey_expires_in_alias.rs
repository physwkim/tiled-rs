//! ITEM 6 (Wave-22 backlog): the JSON API-key create routes must honor the
//! canonical (python-tiled) client's `expires_in` field, not silently drop it.
//!
//! Upstream client posts `{"expires_in": <seconds>}`
//! (`tiled/client/context.py:840,883`; admin variant `:1243,1274`) against a
//! server schema whose field is `expires_in`
//! (`tiled/server/schemas.py:449` `APIKeyRequestParams.expires_in`). tiled-rs
//! canonicalized the field to `expires_in_seconds`
//! (`src/server/auth_router.rs` `ApiKeyCreateRequest`) — an intentional,
//! documented divergence in the Rust client (`src/client/context.rs:951`,
//! `src/client/admin.rs:105`). But with no serde alias and no
//! `deny_unknown_fields`, a python-client body deserializes with
//! `expires_in_seconds = None` → the key is minted with NO expiry and 200 is
//! returned: a silent permanent credential the holder believes will expire.
//!
//! `#[serde(alias = "expires_in")]` makes the server accept both spellings.
//! The user route (`POST /api/v1/auth/apikeys`) and the admin route
//! (`POST /api/v1/auth/principal/{uuid}/apikey`) share `ApiKeyCreateRequest`,
//! so one alias closes both — both are exercised below.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer, ScopeSet};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::AppState;

/// Multi-user app: `alice` keeps role `user` (has `create:apikeys`); `root` is
/// promoted to `admin` (has `admin:apikeys`). Returns the app and alice's uuid
/// (the admin route's target principal). Pool size 1 mirrors the cold-start
/// race note in `put_data_source_scope_integration.rs`.
async fn build_app() -> (axum::Router, String) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();
    dummy.add_user("root", "toor").unwrap();

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
        catalog: Some(catalog),
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
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (tiled_rs::server::build_app(state), alice.uuid)
}

async fn login(app: &axum::Router, username: &str, password: &str) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": username, "password": password})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "login should succeed");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    format!("Bearer {}", body["access_token"].as_str().unwrap())
}

/// POST an api-key-create body and return `(status, parsed response JSON)`.
async fn post_create(
    app: &axum::Router,
    uri: &str,
    bearer: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("authorization", bearer)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Assert the create response carries a concrete expiry ~`want_secs` from now.
/// Pre-fix, `expiration_time` is `null` (the `expires_in` field was dropped and
/// the key never expires), so `as_str()` is `None` and this fails.
fn assert_expires_in(resp: &serde_json::Value, want_secs: i64) {
    let raw = resp["expiration_time"].as_str().unwrap_or_else(|| {
        panic!(
            "expiration_time must be a concrete timestamp (python-client `expires_in` honored), \
             got {} — the key was minted with no expiry",
            resp["expiration_time"]
        )
    });
    let exp = chrono::DateTime::parse_from_rfc3339(raw)
        .unwrap()
        .with_timezone(&chrono::Utc);
    let delta = (exp - chrono::Utc::now()).num_seconds();
    assert!(
        (want_secs - 100..=want_secs).contains(&delta),
        "expiry must be ~{want_secs}s out, got {delta}s (raw {raw})"
    );
}

/// User route: alice posts a python-client-shaped body `{"expires_in": 3600}`
/// to `POST /api/v1/auth/apikeys`. The key must expire in ~3600s, not never.
#[tokio::test]
async fn user_route_honors_python_client_expires_in() {
    let (app, _alice) = build_app().await;
    let bearer = login(&app, "alice", "wonderland").await;
    let (status, body) = post_create(
        &app,
        "/api/v1/auth/apikeys",
        &bearer,
        json!({"note": "c2", "expires_in": 3600}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create should succeed: {body}");
    assert_expires_in(&body, 3600);
}

/// Admin route: root (admin) posts `{"expires_in": 3600}` to
/// `POST /api/v1/auth/principal/{alice}/apikey`. Same shared `ApiKeyCreateRequest`,
/// so the same alias must apply.
#[tokio::test]
async fn admin_route_honors_python_client_expires_in() {
    let (app, alice_uuid) = build_app().await;
    let bearer = login(&app, "root", "toor").await;
    let (status, body) = post_create(
        &app,
        &format!("/api/v1/auth/principal/{alice_uuid}/apikey"),
        &bearer,
        json!({"expires_in": 3600}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "admin create should succeed: {body}"
    );
    assert_expires_in(&body, 3600);
}
