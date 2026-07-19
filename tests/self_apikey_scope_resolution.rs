//! F4 regression: the self-service `POST /api/v1/auth/apikeys` route must
//! resolve scopes exactly like upstream `generate_apikey`
//! (`authentication.py:1174`), which `new_apikey` (self) and
//! `apikey_for_principal` (admin) both call:
//!
//!  * omitted `scopes` default to `["inherit"]` — the live metascope that
//!    expands to the principal's role scopes at use time — NOT a frozen
//!    snapshot of the caller's current session scopes; and
//!  * each requested scope is capped by the principal's ROLE ceiling
//!    (`principal_scopes | {"inherit"}`), NOT the caller's (possibly narrower)
//!    session scopes.
//!
//! The port's self route previously defaulted to `auth.scopes.clone()` (a
//! session snapshot) and capped by `auth.scopes` (session), so an omitted-scope
//! key froze a concrete scope list and a reduced-scope session could not mint a
//! key up to its role ceiling. Both routes now share `resolve_apikey_scopes`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer, Scope, ScopeSet};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build an app with a dummy authenticator and one user `alice` (role `user`),
/// server-wide login scopes capped to `default_login_scopes`.
async fn build(default_login_scopes: ScopeSet) -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1: warm the single connection so no request cold-starts a new
    // SQLite connection under a saturated runner (cold-start CANTOPEN flake).
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();
    auth_db.ensure_principal("dummy", "alice").await.unwrap();

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

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        root_path: String::new(),
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
        allow_anonymous_access: false,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
    };
    tiled_rs::server::build_app(state)
}

async fn login(app: &axum::Router) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/dummy/login")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"username": "alice", "password": "wonderland"})).unwrap(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    format!("Bearer {}", body["access_token"].as_str().unwrap())
}

async fn post_apikeys(
    app: &axum::Router,
    bearer: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/apikeys")
        .header("content-type", "application/json")
        .header("authorization", bearer)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// Omitted `scopes` must default to `["inherit"]`, not a frozen snapshot of the
/// caller's concrete session scopes — visible both in the create response and
/// in the subsequent key listing. `default_login_scopes = full()` makes the
/// session scopes a concrete multi-scope set (`for_role("user")`), so a
/// snapshot default would show that list instead of `["inherit"]`.
#[tokio::test]
async fn self_apikey_omitted_scopes_default_to_inherit() {
    let app = build(ScopeSet::full()).await;
    let bearer = login(&app).await;

    let (status, body) = post_apikeys(&app, &bearer, json!({})).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    assert_eq!(
        body["scopes"],
        json!(["inherit"]),
        "omitted scopes must default to the inherit metascope, not a session snapshot: {body}"
    );

    // The listing must report the same stored scopes.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/apikeys")
        .header("authorization", &bearer)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        list[0]["scopes"],
        json!(["inherit"]),
        "listed key must also carry the inherit metascope: {list}"
    );
}

/// A requested scope must be capped by the principal's ROLE ceiling, not the
/// caller's session scopes. Here the session is narrowed (via
/// `default_login_scopes`) to only `read:metadata` + `create:apikeys`, but the
/// `user` role also grants `write:metadata`. Requesting `write:metadata` must
/// succeed (it is within the role ceiling) even though it is outside the
/// current session — previously the session cap rejected it with 403.
#[tokio::test]
async fn self_apikey_scopes_capped_by_role_not_session() {
    let narrow = ScopeSet::from_iter([Scope::ReadMetadata, Scope::CreateApiKeys]);
    let app = build(narrow).await;
    let bearer = login(&app).await;

    // Sanity: write:metadata is in the user role but NOT in the narrowed session.
    assert!(ScopeSet::for_role("user").contains(Scope::WriteMetadata));

    let (status, body) = post_apikeys(&app, &bearer, json!({"scopes": ["write:metadata"]})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a role-permitted scope must be grantable even when outside the current session: {body}"
    );
    assert_eq!(
        body["scopes"],
        json!(["write:metadata"]),
        "the granted key must carry exactly the requested role-permitted scope: {body}"
    );
}
