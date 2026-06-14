//! End-to-end auth flow: dummy login → access read endpoint with JWT →
//! create api key → use api key → revoke api key.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_catalog::Catalog;
use tiled_core::adapters::ContainerAdapter;
use tiled_core::queries::Query;

async fn build_test_app() -> (axum::Router, tempfile::TempDir, Catalog, AuthDb) {
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

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
        auth_db: Some(auth_db.clone()),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
    };
    (tiled_server::build_app(state), dir, catalog, auth_db)
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

#[tokio::test]
async fn login_yields_jwt_then_jwt_authorizes_metadata_read() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    // Anonymous read is rejected.
    let (status, _) = json_request(&app, Method::GET, "/api/v1/metadata/", &[], None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Login with the dummy authenticator.
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
    let refresh = body["refresh_token"].as_str().unwrap().to_string();

    // Read with Bearer JWT — passes.
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &format!("Bearer {access}"))],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // whoami reflects the principal.
    let (_, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/whoami",
        &[("authorization", &format!("Bearer {access}"))],
        None,
    )
    .await;
    assert_eq!(body["kind"], "Session");

    // Refresh issues a new access token.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert!(body["access_token"].is_string());

    // Logout revokes the session — subsequent read is 401 even if the
    // JWT itself hasn't expired.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/logout",
        &[("authorization", &format!("Bearer {access}"))],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &format!("Bearer {access}"))],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn api_key_via_db_grants_scope_subset() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    // Login → access token with full scopes.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let access = body["access_token"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {access}");

    // Create an api key with read-only scopes.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[("authorization", &bearer)],
        Some(json!({
            "note": "read-only",
            "scopes": ["read:metadata", "read:data"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let secret = body["secret"].as_str().unwrap().to_string();
    let first_eight = body["first_eight"].as_str().unwrap().to_string();

    // GET metadata using the api key — should succeed.
    let apikey_header = format!("Apikey {secret}");
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &apikey_header)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // POST register using the api key — should be 403 (read-only scope).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        &[("authorization", &apikey_header)],
        Some(json!({
            "key": "x",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Revoke the api key — must use the original bearer (which has
    // ApiKeyRevoke scope via full scopes).
    let (status, _) = json_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/apikeys/{first_eight}"),
        &[("authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Revoked key → 401.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &apikey_header)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn write_endpoint_demands_write_scope() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // First login creates alice as a 'user' role principal. We need 'register'
    // scope for POST /register/, which is only in the admin role (Python parity:
    // user role does not include 'register'). Upgrade alice to admin so the
    // second login issues a token with full scopes including 'register'.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let alice_sub = body["identity"]["id"].as_str().unwrap().to_string();
    let (alice, _) = auth_db.ensure_principal("dummy", &alice_sub).await.unwrap();
    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();

    // Log in again so the new session reflects admin scopes (includes 'register').
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let access = body["access_token"].as_str().unwrap().to_string();

    // POST register with admin scopes — passes.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        &[("authorization", &format!("Bearer {access}"))],
        Some(json!({
            "key": "node1",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}
