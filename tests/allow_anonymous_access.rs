//! `allow_anonymous_access` matrix (Wave-25 P7, AUTH-POSTURE).
//!
//! Pins the operator opt-in that admits an unauthenticated request as the
//! **public** principal with only the public read scopes, split cleanly from
//! the `no_auth_configured()` dev escape hatch (full scope). Mirrors upstream
//! `get_current_scopes`: `PUBLIC_SCOPES if allow_anonymous_access else
//! NO_SCOPES` (`tiled/server/authentication.py:437`) and
//! `authentication.required = not allow_anonymous_access` (`router.py:205`).
//!
//! Boundaries pinned here:
//! * flag OFF + no creds  → 401 (unchanged, default posture)
//! * flag ON  + no creds  → 200 on a public read route, with public scopes only
//! * flag ON  + no creds  → 403 on a write route AND on apikey-create
//! * flag ON  + valid creds → authenticated principal wins over anon
//! * no_auth_configured    → full anonymous scope, independent of the flag

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build a **multi-user** app (auth DB + dummy authenticator + catalog) with
/// `allow_anonymous_access` set to `flag`. Auth is genuinely configured, so
/// `no_auth_configured()` is false and only the flag governs anonymous
/// admission.
async fn build_multi_user_app(flag: bool) -> (axum::Router, tempfile::TempDir) {
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

    let state = tiled_rs::server::AppState {
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
        allow_anonymous_access: flag,
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
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
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

/// Build a **no-auth-configured** app (no api_key, no auth_db) with the flag
/// off — the dev/demo escape hatch. `no_auth_configured()` is true, so
/// anonymous callers get the FULL scope set regardless of the flag.
async fn build_no_auth_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = tiled_rs::server::AppState {
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
        // Flag deliberately OFF: the dev escape hatch must grant full anonymous
        // access on its own, independent of allow_anonymous_access.
        allow_anonymous_access: false,
        catalog: Some(catalog.clone()),
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_cache: tiled_rs::server::streaming_cache::disabled(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
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

async fn json_request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let body_bytes = match body {
        Some(v) => serde_json::to_vec(&v).unwrap(),
        None => Vec::new(),
    };
    let req = req
        .header("content-type", "application/json")
        .body(Body::from(body_bytes))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

fn container_body() -> serde_json::Value {
    json!({
        "key": "x",
        "structure_family": "container",
        "metadata": {},
        "specs": [],
        "data_sources": [],
    })
}

/// Default posture: flag off, no credentials → 401 on a protected read route.
/// This is the regression pin for "the flag defaults OFF".
#[tokio::test]
async fn flag_off_anonymous_read_is_unauthorized() {
    let (app, _dir) = build_multi_user_app(false).await;
    let (status, _) = json_request(&app, Method::GET, "/api/v1/metadata/", &[], None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Flag off, no creds → apikey-create also 401 (rejected at the middleware).
#[tokio::test]
async fn flag_off_anonymous_apikey_create_is_unauthorized() {
    let (app, _dir) = build_multi_user_app(false).await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[],
        Some(json!({"note": "x", "scopes": ["read:metadata"]})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Flag on, no creds → 200 on a public read route, and `whoami` reports the
/// anonymous principal with EXACTLY the public read scopes — no write, create,
/// or credential scope. This pins both admission and the exact scope set.
#[tokio::test]
async fn flag_on_anonymous_read_gets_public_scopes_only() {
    let (app, _dir) = build_multi_user_app(true).await;

    let (status, _) = json_request(&app, Method::GET, "/api/v1/metadata/", &[], None).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = json_request(&app, Method::GET, "/api/v1/auth/whoami", &[], None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "Anonymous");
    assert_eq!(body["principal"], "anonymous");
    // BTreeSet<Scope> iterates in enum-declaration order: ReadMetadata before
    // ReadData. Exactly these two scopes — no write/create/credential scope.
    assert_eq!(body["scopes"], json!(["read:metadata", "read:data"]));
}

/// Flag on, no creds → a write route is 403 (admitted, but public scopes lack
/// the write/create/register scopes). NOT 401: the request is admitted, then
/// scope-checked.
#[tokio::test]
async fn flag_on_anonymous_write_is_forbidden() {
    let (app, _dir) = build_multi_user_app(true).await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        &[],
        Some(container_body()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Flag on, no creds → apikey-create is 403 (admitted, but public scopes lack
/// `create:apikeys`). An anonymous caller must never mint credentials.
#[tokio::test]
async fn flag_on_anonymous_apikey_create_is_forbidden() {
    let (app, _dir) = build_multi_user_app(true).await;
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[],
        Some(json!({"note": "x", "scopes": ["read:metadata"]})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Flag on, but valid credentials present → the authenticated principal wins
/// over the anonymous fallback: `whoami` reports a Session (not Anonymous),
/// and the principal keeps its full scopes (can create an API key).
#[tokio::test]
async fn flag_on_valid_credentials_win_over_anonymous() {
    let (app, _dir) = build_multi_user_app(true).await;

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let access = body["access_token"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {access}");

    // whoami with the token → Session principal, not the anonymous fallback.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/whoami",
        &[("authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "Session");
    assert_ne!(body["principal"], "anonymous");

    // The authenticated principal keeps full scopes: apikey-create succeeds,
    // proving anon admission did not cap a real credential.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[("authorization", &bearer)],
        Some(json!({"note": "k", "scopes": ["read:metadata", "read:data"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// `no_auth_configured()` grants FULL anonymous scope regardless of the flag
/// (here off): the dev/demo escape hatch is unchanged and is a DISTINCT path
/// from `allow_anonymous_access`. Anonymous read succeeds and `whoami` shows
/// write scopes the public grant would never include.
#[tokio::test]
async fn no_auth_configured_grants_full_anonymous_scope() {
    let (app, _dir) = build_no_auth_app().await;

    let (status, _) = json_request(&app, Method::GET, "/api/v1/metadata/", &[], None).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = json_request(&app, Method::GET, "/api/v1/auth/whoami", &[], None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "Anonymous");
    let scopes = body["scopes"].as_array().unwrap();
    let scope_strs: Vec<&str> = scopes.iter().map(|s| s.as_str().unwrap()).collect();
    assert!(
        scope_strs.contains(&"write:metadata"),
        "no_auth_configured must grant full (write) scope, got {scope_strs:?}"
    );
}
