//! `default_login_scopes` parity: the shipped default must not cap a
//! credential below its principal's role scopes.
//!
//! Upstream mints the principal's full role scopes into the session token at
//! login with no global cap (`"scp": role scopes`,
//! `tiled/server/authentication.py:856`). tiled-rs intersects role scopes with
//! `default_login_scopes` (auth_router::login), so the shipped default must be
//! the full set for `role ∩ default == role` (identity). A `read_only()`
//! default silently strips every write/create scope even from an admin — the
//! pre-fix production bug this pins.

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

/// Build a multi-user app whose `default_login_scopes` is the given set.
async fn make_app(default_login_scopes: ScopeSet) -> (axum::Router, tempfile::TempDir) {
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
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes,
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
    (tiled_rs::server::build_app(state), dir)
}

async fn status_of(app: &axum::Router, req: Request<Body>) -> StatusCode {
    app.clone().oneshot(req).await.unwrap().status()
}

/// Log in alice (a `user`-role principal — `for_role("user")` includes
/// `create:apikeys`) and try to create an API key. Returns the create status.
/// The only variable is `default_login_scopes`, so a 403 here means the
/// default capped `create:apikeys` out of the session.
async fn login_then_create_apikey_status(default_login_scopes: ScopeSet) -> StatusCode {
    let (app, _dir) = make_app(default_login_scopes).await;

    let login = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "alice", "password": "wonderland"})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(login).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "login should succeed");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let access = body["access_token"].as_str().unwrap().to_string();

    let create = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/apikeys")
        .header("authorization", format!("Bearer {access}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"note": "c2"})).unwrap(),
        ))
        .unwrap();
    status_of(&app, create).await
}

/// The pre-fix production default (`read_only()`) caps a `user` below
/// `create:apikeys`, and the shipped default (`AppState::default_login_scopes`)
/// does not — role scopes pass through, matching upstream.
#[tokio::test]
async fn shipped_default_passes_role_scopes_through_readonly_would_cap() {
    // read_only() (the pre-fix production value) strips create:apikeys → 403.
    let capped = login_then_create_apikey_status(ScopeSet::read_only()).await;
    assert_eq!(
        capped,
        StatusCode::FORBIDDEN,
        "read_only() default caps a user's role scopes below create:apikeys"
    );

    // The shipped default imposes no cap → create:apikeys passes through → 200.
    let shipped = login_then_create_apikey_status(AppState::default_login_scopes()).await;
    assert_eq!(
        shipped,
        StatusCode::OK,
        "shipped default must pass role scopes through (upstream authentication.py:856)"
    );
}
