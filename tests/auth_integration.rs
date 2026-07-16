//! End-to-end auth flow: dummy login → access read endpoint with JWT →
//! create api key → use api key → revoke api key.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer, Scope};
use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

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
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (tiled_rs::server::build_app(state), dir, catalog, auth_db)
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

/// Finding 1: a key granted the `inherit` metascope must dynamically inherit
/// the principal's *current* role scopes at access time (Python parity), not
/// be a dead, permission-less credential. A role downgrade must take effect
/// on the next request without re-issuing the key.
#[tokio::test]
async fn inherit_api_key_dynamically_inherits_current_role() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // Bootstrap alice, then promote to admin (only a holder of `inherit` —
    // i.e. an admin whose role set includes it — may grant it on a key).
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

    // Re-login so the session reflects admin scopes, then mint an `inherit` key.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[("authorization", &bearer)],
        Some(json!({ "note": "inherit", "scopes": ["inherit"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let apikey = format!("Apikey {}", body["secret"].as_str().unwrap());

    // While alice is admin, the inherit key carries admin scopes: it can read
    // AND register. (On the unfixed code the key resolves to nothing → 403.)
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &apikey)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "inherit key must inherit read scope"
    );

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        &[("authorization", &apikey)],
        Some(json!({
            "key": "inh_admin",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "inherit key must inherit admin's register scope"
    );

    // Downgrade alice to 'user'. The SAME key must now resolve to the narrower
    // role: read still works, but register (admin-only) is forbidden.
    auth_db
        .update_principal_role(alice.id, "user")
        .await
        .unwrap();

    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &apikey)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "user role still has read scope");

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/register/",
        &[("authorization", &apikey)],
        Some(json!({
            "key": "inh_user",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "role downgrade must revoke register on the next request"
    );
}

#[tokio::test]
async fn refresh_rotates_token_and_returns_full_response() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let original_refresh = body["refresh_token"].as_str().unwrap().to_string();

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": original_refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["access_token"].is_string(), "missing access_token");
    assert!(body["refresh_token"].is_string(), "missing refresh_token");
    assert!(
        body["refresh_token_expires_in"].is_number(),
        "missing refresh_token_expires_in"
    );
    assert!(body["expires_in"].is_number(), "missing expires_in");
    let new_refresh = body["refresh_token"].as_str().unwrap().to_string();

    // The new refresh token must itself be a valid, usable refresh token
    // (functional rotation: the returned token works for the next cycle).
    let (status, body2) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": new_refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "second refresh failed: {body2}");
}

/// Finding 2: refresh must re-derive access scopes from the principal's
/// current role (Python slide_session parity), so a role downgrade takes
/// effect on the next refresh rather than surviving until hard expiry.
#[tokio::test]
async fn refresh_rederives_scopes_from_current_role() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();

    // alice logs in, is promoted to admin, then re-logs in for an admin session.
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

    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let refresh = body["refresh_token"].as_str().unwrap().to_string();
    // Sanity: the admin access token carries the admin-only `register` scope.
    let admin_claims = issuer
        .verify_access(body["access_token"].as_str().unwrap())
        .unwrap();
    assert!(admin_claims.scopes.contains(Scope::Register));

    // Downgrade to 'user', then refresh.
    auth_db
        .update_principal_role(alice.id, "user")
        .await
        .unwrap();
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The refreshed access token reflects the *current* (user) role: read
    // survives, admin-only `register` is gone. On the unfixed code the frozen
    // admin session scopes would persist and `register` would still be set.
    let claims = issuer
        .verify_access(body["access_token"].as_str().unwrap())
        .unwrap();
    assert!(
        claims.scopes.contains(Scope::ReadData),
        "user role retains read"
    );
    assert!(
        !claims.scopes.contains(Scope::Register),
        "role downgrade must drop admin-only scopes on refresh"
    );
}

/// Finding 2: refresh re-derives from role but must NEVER widen a
/// deliberately-narrowed session beyond its stored scopes.
#[tokio::test]
async fn refresh_does_not_widen_a_narrowed_session() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();

    // An admin principal, but a session deliberately narrowed to read-only.
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();
    let narrow = tiled_rs::auth::ScopeSet::read_only();
    let session = auth_db
        .create_session(
            alice.id,
            narrow,
            chrono::Utc::now() + chrono::Duration::hours(1),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let refresh = issuer.issue_refresh(&alice.uuid, &session.uuid).unwrap();

    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Even though alice is admin, the narrowed session is not re-widened.
    let claims = issuer
        .verify_access(body["access_token"].as_str().unwrap())
        .unwrap();
    assert!(
        claims.scopes.contains(Scope::ReadData),
        "narrow read survives"
    );
    assert!(
        !claims.scopes.contains(Scope::Register),
        "a narrowed session must not regain admin scopes on refresh"
    );
    assert!(
        !claims.scopes.contains(Scope::WriteData),
        "a narrowed session must not regain write scopes on refresh"
    );
}

#[tokio::test]
async fn revoked_and_missing_session_refresh_return_identical_opaque_401() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // Login to get a valid refresh token.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let access = body["access_token"].as_str().unwrap().to_string();
    let refresh = body["refresh_token"].as_str().unwrap().to_string();

    // Revoke the session via logout.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/logout",
        &[("authorization", &format!("Bearer {access}"))],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Refresh on a revoked session → opaque 401.
    let (status, revoked_body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{revoked_body}");

    // Forge a valid-looking refresh JWT for a session UUID that never existed.
    let issuer = tiled_rs::auth::Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let _ = auth_db; // keep alive
    let bogus_refresh = issuer
        .issue_refresh(
            "00000000-0000-0000-0000-000000000000",
            "deadbeef-dead-dead-dead-deaddeadbeef",
        )
        .unwrap();

    let (status, missing_body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": bogus_refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{missing_body}");

    // Both must return the same error message (opaque — does not reveal revoked vs missing).
    let revoked_msg = revoked_body["error"]["message"].as_str().unwrap_or("");
    let missing_msg = missing_body["error"]["message"].as_str().unwrap_or("");
    assert_eq!(
        revoked_msg, missing_msg,
        "error messages differ: revoked={revoked_msg:?} missing={missing_msg:?}"
    );
    assert_eq!(revoked_msg, "Session has expired. Please re-authenticate.");
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

/// Security gate: POST /api/v1/auth/principal requires `write:principals`
/// scope. A regular user (role="user") lacks that scope and must be rejected
/// with 403, not 401, because they ARE authenticated — they just don't hold
/// the required scope.
#[tokio::test]
async fn create_service_principal_non_admin_rejected() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    // Login as alice (role="user" by default, no write:principals scope).
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

    // POST /auth/principal — must be 403 (authenticated but missing scope).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/principal?role=user",
        &[("authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "non-admin must be rejected with 403"
    );
}

/// Admin path: POST /api/v1/auth/principal succeeds for a principal holding
/// `write:principals` scope and the response contains type="service" and the
/// requested role.
#[tokio::test]
async fn create_service_principal_admin_succeeds() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // Login as alice and promote to admin.
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

    // Re-login so the session carries admin scopes.
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

    // POST /auth/principal?role=user — must succeed.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/principal?role=user",
        &[("authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "admin must be able to create a service principal: {body}"
    );
    assert_eq!(body["type"], "service", "response must carry type=service");
    assert_eq!(
        body["role"], "user",
        "response must carry the requested role"
    );
    assert!(
        body["uuid"].is_string() && !body["uuid"].as_str().unwrap().is_empty(),
        "response must carry a uuid"
    );
}

// ---------------------------------------------------------------------------
// Device-code OIDC approval tests (tiled#1377)
// ---------------------------------------------------------------------------

/// Build an AppState + App with an ExternalOidcValidator wired in, using an
/// HS256 key pre-seeded into the cache so no JWKS network call is needed.
/// Returns (app, auth_db, validator) so tests can mint matching tokens.
async fn build_oidc_app() -> (
    axum::Router,
    AuthDb,
    Arc<tiled_rs::auth::ExternalOidcValidator>,
    tempfile::TempDir,
) {
    let (state, auth_db, validator, dir) = build_oidc_state(std::collections::HashMap::new()).await;
    (tiled_rs::server::build_app(state), auth_db, validator, dir)
}

/// Same as [`build_oidc_app`] but returns the `AppState` (so a test can call
/// `validate_bearer` directly) and lets the caller configure the provider's
/// `scopes_map` for the Entra scope-translation path (#1360).
///
/// Returns the backing [`tempfile::TempDir`] so the caller can keep it alive:
/// the catalog/auth SQLite pools open in WAL mode and grow connections lazily,
/// so if the temp directory is dropped (deleted) while the app is live, the
/// next pooled connection open fails with SQLITE_CANTOPEN (a load-dependent
/// 500). The caller must bind it (e.g. `_dir`) for the test's duration.
async fn build_oidc_state(
    scopes_map: std::collections::HashMap<String, Vec<tiled_rs::auth::Scope>>,
) -> (
    tiled_rs::server::AppState,
    AuthDb,
    Arc<tiled_rs::auth::ExternalOidcValidator>,
    tempfile::TempDir,
) {
    use jsonwebtoken::Algorithm;
    use tiled_rs::auth::{ExternalOidcValidator, OidcProvider};

    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();

    let validator = Arc::new(
        ExternalOidcValidator::new(vec![OidcProvider {
            name: "test-idp".into(),
            jwks_url: "https://example.test/jwks".into(), // never fetched
            issuer: "https://issuer.test/".into(),
            audiences: vec!["tiled-test".into()],
            subject_claim: "sub".into(),
            identity_mapping: tiled_rs::auth::IdentityMapping::Standard,
            algorithms: vec![Algorithm::HS256],
            scopes_map,
            client_id: None,
            client_secret: None,
            authorization_endpoint: None,
            token_endpoint: None,
            extra_scopes: Vec::new(),
            end_session_endpoint: None,
            redirect_on_success: None,
            redirect_on_failure: None,
        }])
        .unwrap(),
    );
    // Pre-seed the cache so validate() skips the JWKS HTTP call.
    validator
        .inject_key_for_test(
            "test-idp",
            "test-kid-1",
            jsonwebtoken::DecodingKey::from_secret(b"oidc-test-secret"),
            Algorithm::HS256,
        )
        .await;

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
        catalog: Some(catalog),
        auth_db: Some(auth_db.clone()),
        issuer: Some(issuer),
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: Some(validator.clone()),
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: vec![],
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (state, auth_db, validator, dir)
}

/// Mint an HS256 OIDC token for the given subject using the test key.
/// Token is signed with secret `b"oidc-test-secret"`, kid `"test-kid-1"`,
/// iss `"https://issuer.test/"`, aud `"tiled-test"`.
fn mint_test_oidc_token(sub: &str) -> String {
    use chrono::Utc;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    let now = Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": "https://issuer.test/",
        "aud": "tiled-test",
        "sub": sub,
        "exp": now + 3600,
        "nbf": now - 60,
        "iat": now - 60,
    });
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test-kid-1".into());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_secret(b"oidc-test-secret"),
    )
    .unwrap()
}

/// Mint an HS256 OIDC token carrying a space-separated `scp` claim, using
/// the same test key as [`mint_test_oidc_token`].
fn mint_oidc_token_with_scp(sub: &str, scp: &str) -> String {
    use chrono::Utc;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    let now = Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": "https://issuer.test/",
        "aud": "tiled-test",
        "sub": sub,
        "exp": now + 3600,
        "nbf": now - 60,
        "iat": now - 60,
        "scp": scp,
    });
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test-kid-1".into());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_secret(b"oidc-test-secret"),
    )
    .unwrap()
}

/// #1360 end-to-end: an Entra-style provider (one with a `scopes_map`)
/// translates the bearer token's `scp` claim to tiled scopes and unions them
/// onto the principal's role scopes (Python `get_current_scopes`:
/// `token_scopes | role_scopes`, `authentication.py:434`). A new OIDC
/// principal gets the `"user"` role, which never grants `read:principals`;
/// the token's `scp` maps to exactly that scope, so its presence in the
/// resolved session proves the translation reached the auth context.
#[tokio::test]
async fn oidc_entra_scp_unions_into_session_scopes() {
    let mut scopes_map = std::collections::HashMap::new();
    scopes_map.insert(
        "api://tiled/admin.read".to_string(),
        vec![Scope::ReadPrincipals],
    );
    let (state, _auth_db, _validator, _dir) = build_oidc_state(scopes_map).await;

    let token = mint_oidc_token_with_scp("entra-sub-1", "api://tiled/admin.read");
    let ctx = tiled_rs::server::app::validate_bearer(&state, &token)
        .await
        .expect("Entra OIDC token must validate");

    assert!(
        ctx.scopes.contains(Scope::ReadPrincipals),
        "scp-mapped scope must be unioned into the session (the user role never grants it)"
    );
    assert!(
        ctx.scopes.contains(Scope::ReadData),
        "role scopes must still apply alongside the translated token scopes"
    );
}

/// A plain OIDC provider (no `scopes_map`) ignores `scp`: the session's
/// scopes come from the principal's role alone, so an `scp` that would map to
/// `read:principals` under Entra confers nothing here.
#[tokio::test]
async fn oidc_plain_provider_ignores_scp_claim() {
    let (state, _auth_db, _validator, _dir) =
        build_oidc_state(std::collections::HashMap::new()).await;
    let token = mint_oidc_token_with_scp("plain-sub-1", "api://tiled/admin.read");
    let ctx = tiled_rs::server::app::validate_bearer(&state, &token)
        .await
        .expect("plain OIDC token must validate");

    assert!(
        !ctx.scopes.contains(Scope::ReadPrincipals),
        "a provider without a scopes_map must not derive scopes from scp"
    );
    assert!(
        ctx.scopes.contains(Scope::ReadData),
        "role scopes still apply"
    );
}

/// Invariant: a valid OIDC token in the approval body MUST create (or
/// upsert) the principal, approve the device code, and allow the CLI
/// client to poll a Granted status with access tokens.
#[tokio::test]
async fn device_approve_oidc_valid_creates_principal_and_approves() {
    let (app, auth_db, _validator, _dir) = build_oidc_app().await;

    // Initiate a device code.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/initiate",
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initiate: {body}");
    let user_code = body["user_code"].as_str().unwrap().to_string();
    let device_code = body["device_code"].as_str().unwrap().to_string();

    // Approve using an OIDC token — no tiled session required.
    let oidc_token = mint_test_oidc_token("alice-oidc-sub");
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[],
        Some(json!({"user_code": user_code, "oidc_token": oidc_token})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "approve: {body}");

    // Poll: must return Granted with tokens.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/token",
        &[],
        Some(json!({"device_code": device_code})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "token: {body}");
    assert!(body["access_token"].is_string(), "missing access_token");
    assert!(body["refresh_token"].is_string(), "missing refresh_token");

    // Principal was upserted: ensure_principal on the same sub returns the same id.
    let (p1, _) = auth_db
        .ensure_principal("test-idp", "alice-oidc-sub")
        .await
        .unwrap();
    let (p2, _) = auth_db
        .ensure_principal("test-idp", "alice-oidc-sub")
        .await
        .unwrap();
    assert_eq!(p1.id, p2.id, "ensure_principal must be idempotent");
}

/// Invariant: an invalid (forged/malformed) OIDC token MUST NOT create
/// a principal or approve the device code. The code must remain Pending.
#[tokio::test]
async fn device_approve_oidc_invalid_does_not_approve() {
    let (app, auth_db, _validator, _dir) = build_oidc_app().await;

    // Initiate.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/initiate",
        &[],
        None,
    )
    .await;
    let user_code = body["user_code"].as_str().unwrap().to_string();
    let device_code = body["device_code"].as_str().unwrap().to_string();

    // Record the principal count before.
    let before_count = auth_db.list_api_keys(None).await.unwrap(); // just a sanity DB call

    // Submit a forged (garbage) token.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[],
        Some(json!({
            "user_code": user_code,
            "oidc_token": "not.a.real.jwt"
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "forged token must be rejected"
    );

    // Token signed with the WRONG key — same structure but wrong signature.
    let bad_token = {
        use jsonwebtoken::{Algorithm, EncodingKey, Header};
        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "iss": "https://issuer.test/",
            "aud": "tiled-test",
            "sub": "attacker",
            "exp": now + 3600,
            "nbf": now - 60,
        });
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("test-kid-1".into());
        jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(b"wrong-secret")).unwrap()
    };
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[],
        Some(json!({
            "user_code": user_code,
            "oidc_token": bad_token
        })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "wrong-key token must be rejected"
    );

    // Device code must still be pending — not approved.
    // An expired token (we can check by polling — should still say pending).
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/token",
        &[],
        Some(json!({"device_code": device_code})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "device code must still be pending after rejected approval: {body}"
    );
    let detail = body["error"]["message"].as_str().unwrap_or("");
    assert_eq!(detail, "authorization_pending");

    // No principal was created for the attacker subject.
    let attacker_identity = auth_db.find_identity("test-idp", "attacker").await.unwrap();
    assert!(
        attacker_identity.is_none(),
        "failed validation must not create a principal"
    );
    drop(before_count);
}

/// Invariant: submitting an oidc_token when no external_oidc validator is
/// configured must be rejected (validation error) before any DB access.
#[tokio::test]
async fn device_approve_oidc_no_validator_configured_is_rejected() {
    // Use the standard (non-OIDC) test app — external_oidc is None.
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/initiate",
        &[],
        None,
    )
    .await;
    let user_code = body["user_code"].as_str().unwrap().to_string();

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[],
        Some(json!({"user_code": user_code, "oidc_token": "any.token.here"})),
    )
    .await;
    // 422 (Validation) because external_oidc is not configured.
    assert!(
        status == StatusCode::UNPROCESSABLE_ENTITY || status == StatusCode::BAD_REQUEST,
        "expected 400/422 when no validator configured, got {status}"
    );
}

/// Invariant: with no oidc_token and no session, approval must be rejected.
#[tokio::test]
async fn device_approve_no_credentials_rejected() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/initiate",
        &[],
        None,
    )
    .await;
    let user_code = body["user_code"].as_str().unwrap().to_string();
    let device_code = body["device_code"].as_str().unwrap().to_string();

    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[],
        Some(json!({"user_code": user_code})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "unauthenticated approval must be rejected"
    );

    // Code still pending.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/token",
        &[],
        Some(json!({"device_code": device_code})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "code must remain pending: {body}"
    );
}

/// Invariant: the session-based approval path (Bearer token, no oidc_token)
/// must still work after device_approve was moved from private_auth to
/// public_auth. A logged-in user should be able to approve via their session.
#[tokio::test]
async fn device_approve_session_bearer_still_works() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    // Login to get a session bearer token.
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

    // Initiate a device code.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/initiate",
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initiate: {body}");
    let user_code = body["user_code"].as_str().unwrap().to_string();
    let device_code = body["device_code"].as_str().unwrap().to_string();

    // Approve using the session bearer (no oidc_token).
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/approve",
        &[("authorization", &bearer)],
        Some(json!({"user_code": user_code})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "approve: {body}");

    // Poll: must return Granted.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/device/token",
        &[],
        Some(json!({"device_code": device_code})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "token: {body}");
    assert!(body["access_token"].is_string(), "missing access_token");
}

// ---------------------------------------------------------------------------
// New endpoint tests: session revoke, apikey info, list principals,
// admin per-principal apikey management
// ---------------------------------------------------------------------------

/// `POST /api/v1/auth/session/revoke` with a valid refresh token revokes the
/// session so subsequent refreshes fail (Python authentication.py:1437 parity).
/// The refresh token itself IS the ownership proof — no Bearer header required.
#[tokio::test]
async fn session_revoke_by_token_revokes_session() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let refresh = body["refresh_token"].as_str().unwrap().to_string();

    // Revoke via POST (no auth header needed).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/session/revoke",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "revoke must return 204");

    // Refreshing a revoked session → 401.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "refresh on revoked session must be 401"
    );

    // Second revoke call must also return an error (already revoked).
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/session/revoke",
        &[],
        Some(json!({"refresh_token": refresh})),
    )
    .await;
    assert_ne!(
        status,
        StatusCode::NO_CONTENT,
        "second revoke on already-revoked session must not succeed silently"
    );
}

/// `DELETE /api/v1/auth/session/revoke/{session_id}` allows an authenticated
/// principal to revoke their own session by UUID (Python authentication.py:1432).
/// Attempting to revoke another principal's session returns 404 (not 403 —
/// opaque to avoid leaking existence of the session).
#[tokio::test]
async fn session_revoke_by_id_own_session_and_ownership_check() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // alice logs in.
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let alice_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());
    let alice_refresh = body["refresh_token"].as_str().unwrap().to_string();
    let alice_sub = body["identity"]["id"].as_str().unwrap().to_string();
    // ensure_principal so `_alice` keeps the record (ensure is idempotent).
    let (_alice, _) = auth_db.ensure_principal("dummy", &alice_sub).await.unwrap();

    // Decode the refresh token to extract the session UUID (sid field).
    let issuer = tiled_rs::auth::Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let claims = issuer.verify_refresh(&alice_refresh).unwrap();
    let session_uuid = claims.sid.clone();

    // bob gets a principal directly (no login flow needed — just need a session).
    let (bob_p, _) = auth_db.ensure_principal("dummy", "bob").await.unwrap();
    let bob_session = auth_db
        .create_session(
            bob_p.id,
            tiled_rs::auth::ScopeSet::full(),
            chrono::Utc::now() + chrono::Duration::hours(1),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let issuer = tiled_rs::auth::Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let bob_access = issuer
        .issue_access(
            &bob_p.uuid,
            &bob_session.uuid,
            tiled_rs::auth::ScopeSet::full(),
            serde_json::json!({}),
        )
        .unwrap();
    let bob_bearer = format!("Bearer {bob_access}");

    // bob tries to revoke alice's session → 404 (opaque).
    let (status, _) = json_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/session/revoke/{session_uuid}"),
        &[("authorization", &bob_bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "cross-principal session revoke must be 404 (opaque)"
    );

    // alice revokes her own session.
    let (status, _) = json_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/session/revoke/{session_uuid}"),
        &[("authorization", &alice_bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "own-session revoke must be 204"
    );

    // Refresh with alice's token on the now-revoked session → 401.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/refresh",
        &[],
        Some(json!({"refresh_token": alice_refresh})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "refreshing revoked session must 401"
    );
}

/// `GET /api/v1/auth/apikey` returns info about the API key used in the
/// current request (Python current_apikey_info, authentication.py:1584).
/// Using a Bearer token (non-API-key auth) must return 401.
#[tokio::test]
async fn current_apikey_info_returns_key_metadata() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    // Create a read-only API key.
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/apikeys",
        &[("authorization", &bearer)],
        Some(json!({"note": "info-test", "scopes": ["read:metadata"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let secret = body["secret"].as_str().unwrap().to_string();
    let first_eight = body["first_eight"].as_str().unwrap().to_string();
    let apikey_header = format!("Apikey {secret}");

    // GET /auth/apikey with the API key — must return key metadata.
    let (status, info) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/apikey",
        &[("authorization", &apikey_header)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(info["first_eight"], first_eight);
    assert_eq!(info["note"], "info-test");
    let scopes = info["scopes"].as_array().unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0], "read:metadata");

    // GET /auth/apikey with a Bearer token (not an API key) → 401.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/apikey",
        &[("authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "non-apikey auth must be rejected at GET /auth/apikey"
    );
}

/// `GET /api/v1/auth/principal` (list) requires `read:principals` scope (admin
/// role only). A user-role caller → 403. An admin caller receives a paginated
/// list of all principals (Python authentication.py:1247-1286 parity).
#[tokio::test]
async fn list_principals_admin_only_and_paginated() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // alice logs in as user (no read:principals).
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let alice_sub = body["identity"]["id"].as_str().unwrap().to_string();
    let user_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());
    let (alice, _) = auth_db.ensure_principal("dummy", &alice_sub).await.unwrap();

    // User role → 403.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/principal",
        &[("authorization", &user_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "user must be 403 on list");

    // Promote alice to admin, re-login.
    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let admin_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    // Admin list → 200, at least one principal (alice herself).
    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/principal",
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let principals = body.as_array().expect("list response must be an array");
    assert!(
        !principals.is_empty(),
        "list must contain at least alice's principal"
    );
    let found_alice = principals.iter().any(|p| p["uuid"] == alice.uuid);
    assert!(found_alice, "alice's principal must appear in the list");

    // Each entry must NOT leak internal fields.
    for p in principals {
        assert!(
            p.get("id").is_none(),
            "internal id must not appear in list response"
        );
    }

    // Pagination: offset=1 should return one fewer entry.
    let (status, page2) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/principal?page%5Boffset%5D=1",
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page2}");
    let page2_arr = page2.as_array().unwrap();
    assert_eq!(
        page2_arr.len() + 1,
        principals.len(),
        "offset=1 must return one fewer entry than the full list"
    );
}

/// Admin can create an API key for another principal
/// (`POST /api/v1/auth/principal/{uuid}/apikey`) and then revoke it
/// (`DELETE /api/v1/auth/principal/{uuid}/apikey?first_eight=...`).
/// Attempting the same operations as a non-admin → 403.
/// Attempting to revoke a key that doesn't belong to the target principal → 404.
#[tokio::test]
async fn admin_create_and_revoke_principal_apikey() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // alice promotes herself to admin.
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
    let user_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    // Non-admin creates a service principal — must fail.
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/principal?role=user",
        &[("authorization", &user_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let admin_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    // Create a service principal (bot).
    let (status, bot) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/principal?role=user",
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bot}");
    let bot_uuid = bot["uuid"].as_str().unwrap().to_string();

    // Non-admin cannot create a key for bot.
    let (status, _) = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/auth/principal/{bot_uuid}/apikey"),
        &[("authorization", &user_bearer)],
        Some(json!({"note": "bot-key", "scopes": ["read:metadata"]})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "non-admin must not create keys for other principals"
    );

    // Admin creates a key for bot.
    let (status, key) = json_request(
        &app,
        Method::POST,
        &format!("/api/v1/auth/principal/{bot_uuid}/apikey"),
        &[("authorization", &admin_bearer)],
        Some(json!({"note": "bot-key", "scopes": ["read:metadata"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{key}");
    let bot_secret = key["secret"].as_str().unwrap().to_string();
    let bot_first_eight = key["first_eight"].as_str().unwrap().to_string();

    // The key actually works.
    let bot_apikey = format!("Apikey {bot_secret}");
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &bot_apikey)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bot key must grant access");

    // Admin revokes alice's OWN key from bot's endpoint → 404 (key doesn't
    // belong to bot).
    let alice_key = {
        let (_, k) = json_request(
            &app,
            Method::POST,
            "/api/v1/auth/apikeys",
            &[("authorization", &admin_bearer)],
            Some(json!({"note": "alice-key"})),
        )
        .await;
        k["first_eight"].as_str().unwrap().to_string()
    };
    let (status, _) = json_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/principal/{bot_uuid}/apikey?first_eight={alice_key}"),
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "revoking alice's key on bot's endpoint must 404"
    );

    // Admin revokes bot's key correctly.
    let (status, _) = json_request(
        &app,
        Method::DELETE,
        &format!("/api/v1/auth/principal/{bot_uuid}/apikey?first_eight={bot_first_eight}"),
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "admin revoke must succeed");

    // The key is now dead.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/",
        &[("authorization", &bot_apikey)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "revoked key must be 401");
}

/// auth H2: `GET /api/v1/auth/principal/{uuid}` returns the principal with its
/// linked identities (Python `schemas.Principal.identities` via
/// `selectinload`, authentication.py:1325-1361). The identity `id` is the
/// upstream subject and the internal row id / `principal_id` never leak. The
/// endpoint is `read:principals`-gated, so a `user`-role caller is forbidden.
#[tokio::test]
async fn get_principal_returns_identities_admin_only() {
    let (app, _dir, _cat, auth_db) = build_test_app().await;

    // alice logs in (this creates her principal + a "dummy"/alice identity).
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let alice_sub = body["identity"]["id"].as_str().unwrap().to_string();
    let user_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());
    let (alice, _) = auth_db.ensure_principal("dummy", &alice_sub).await.unwrap();

    // A user-role caller lacks read:principals → 403.
    let (status, _) = json_request(
        &app,
        Method::GET,
        &format!("/api/v1/auth/principal/{}", alice.uuid),
        &[("authorization", &user_bearer)],
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "read:principals is required to read a principal"
    );

    // Promote alice to admin and re-login for an admin session.
    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();
    let (_, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/auth/dummy/login",
        &[],
        Some(json!({"username": "alice", "password": "wonderland"})),
    )
    .await;
    let admin_bearer = format!("Bearer {}", body["access_token"].as_str().unwrap());

    let (status, body) = json_request(
        &app,
        Method::GET,
        &format!("/api/v1/auth/principal/{}", alice.uuid),
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["uuid"], alice.uuid);
    let identities = body["identities"].as_array().expect("identities array");
    assert_eq!(identities.len(), 1, "alice has one linked identity");
    assert_eq!(identities[0]["id"], alice_sub, "identity id is the subject");
    assert_eq!(identities[0]["provider"], "dummy");
    assert!(
        identities[0].get("principal_id").is_none(),
        "internal principal_id FK must not leak"
    );

    // Unknown uuid → 404.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/auth/principal/00000000-0000-0000-0000-000000000000",
        &[("authorization", &admin_bearer)],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// OIDC authorization-code flow tests (#1178)
// ---------------------------------------------------------------------------

/// Build an AppState and Router wired with a code-flow-capable OIDC provider.
/// The provider uses HS256 with a pre-injected test key so no JWKS fetch is
/// needed.
async fn build_code_flow_app(
    token_endpoint: &str,
) -> (
    axum::Router,
    Arc<tiled_rs::auth::ExternalOidcValidator>,
    tiled_rs::auth::AuthDb,
    tempfile::TempDir,
) {
    build_code_flow_app_with_mapping(token_endpoint, tiled_rs::auth::IdentityMapping::Standard)
        .await
}

/// Like [`build_code_flow_app`] but with a configurable identity mapping, so a
/// test can exercise the Entra OBO path (G3). Returns the [`AuthDb`] handle so
/// a test can seed an OIDC flow state directly (the PKCE store is DB-backed —
/// G6).
async fn build_code_flow_app_with_mapping(
    token_endpoint: &str,
    identity_mapping: tiled_rs::auth::IdentityMapping,
) -> (
    axum::Router,
    Arc<tiled_rs::auth::ExternalOidcValidator>,
    tiled_rs::auth::AuthDb,
    tempfile::TempDir,
) {
    use jsonwebtoken::Algorithm;
    use tiled_rs::auth::{AuthDb, ExternalOidcValidator, Issuer, OidcProvider};

    let dir = tempfile::tempdir().unwrap();
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let issuer = Issuer::new(b"code-flow-test-secret-32bytes!!!").unwrap();

    let validator = Arc::new(
        ExternalOidcValidator::new(vec![OidcProvider {
            name: "mock-idp".into(),
            jwks_url: "https://mock-idp.test/jwks".into(), // pre-seeded; never fetched
            issuer: "https://mock-idp.test/".into(),
            audiences: vec!["tiled-code-client".into()],
            subject_claim: "sub".into(),
            identity_mapping,
            algorithms: vec![Algorithm::HS256],
            scopes_map: std::collections::HashMap::new(),
            client_id: Some("tiled-code-client".into()),
            client_secret: None,
            authorization_endpoint: Some("https://mock-idp.test/authorize".into()),
            token_endpoint: Some(token_endpoint.to_string()),
            extra_scopes: Vec::new(),
            end_session_endpoint: Some("https://mock-idp.test/logout".into()),
            redirect_on_success: None,
            redirect_on_failure: None,
        }])
        .unwrap(),
    );
    // Pre-seed the JWKS cache so id_token validation skips the HTTP fetch.
    validator
        .inject_key_for_test(
            "mock-idp",
            "test-kid",
            jsonwebtoken::DecodingKey::from_secret(b"mock-idp-secret!!"),
            Algorithm::HS256,
        )
        .await;

    let cat_uri = format!("sqlite://{}", dir.path().join("cat.db").display());
    let catalog = tiled_rs::catalog::Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn tiled_rs::core::adapters::ContainerAdapter> = Arc::new(
        tiled_rs::catalog::CatalogAdapter::root(catalog.clone(), resolver),
    );
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
        auth_db: Some(auth_db.clone()),
        issuer: Some(issuer),
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: Some(validator.clone()),
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: vec![],
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    let app = tiled_rs::server::build_app(state);
    (app, validator, auth_db, dir)
}

/// Mint an HS256 id_token for code-flow tests with the mock-idp key.
fn mint_code_flow_id_token(sub: &str, nonce: &str) -> String {
    use chrono::Utc;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = Utc::now().timestamp();
    let claims = serde_json::json!({
        "iss": "https://mock-idp.test/",
        "aud": "tiled-code-client",
        "sub": sub,
        "exp": now + 3600,
        "nbf": now - 60,
        "iat": now - 60,
        "nonce": nonce,
    });
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test-kid".into());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_secret(b"mock-idp-secret!!"),
    )
    .unwrap()
}

/// GET /api/v1/auth/provider/mock-idp/authorize → 302 to IdP with
/// code_challenge + state in the Location header.
#[tokio::test]
async fn oidc_authorize_302_with_pkce_params() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/provider/mock-idp/authorize")
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "must redirect");

    let location = resp
        .headers()
        .get("location")
        .expect("Location header must be set")
        .to_str()
        .unwrap()
        .to_string();

    let url = reqwest::Url::parse(&location).expect("Location must be a valid URL");
    assert_eq!(url.host_str(), Some("mock-idp.test"));
    assert_eq!(url.path(), "/authorize");

    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    assert_eq!(
        params.get("response_type").map(|s| s.as_ref()),
        Some("code"),
        "response_type=code required"
    );
    assert_eq!(
        params.get("client_id").map(|s| s.as_ref()),
        Some("tiled-code-client")
    );
    assert!(
        params.contains_key("code_challenge"),
        "PKCE code_challenge must be present"
    );
    assert_eq!(
        params.get("code_challenge_method").map(|s| s.as_ref()),
        Some("S256"),
        "S256 method required"
    );
    assert!(
        params.contains_key("state"),
        "state parameter must be present"
    );
    assert!(
        params.contains_key("nonce"),
        "nonce parameter must be present"
    );
}

/// G5: the About endpoint advertises the IdP's `end_session_endpoint` as
/// `authentication.links.logout` (OIDC RP-Initiated Logout 1.0), and exposes
/// the rest of the links block the client depends on (whoami / refresh /
/// revoke). `refresh_session` points at tiled's own refresh route, not the IdP.
#[tokio::test]
async fn about_advertises_oidc_end_session_endpoint_as_logout() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/",
        &[("host", "localhost:8000")],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let links = &body["authentication"]["links"];
    assert_eq!(
        links["logout"].as_str(),
        Some("https://mock-idp.test/logout"),
        "logout must be the IdP's end_session_endpoint"
    );
    assert_eq!(
        links["refresh_session"].as_str(),
        Some("http://localhost:8000/api/v1/auth/refresh"),
        "refresh_session must be tiled's own refresh route, not the IdP token endpoint"
    );
    assert_eq!(
        links["whoami"].as_str(),
        Some("http://localhost:8000/api/v1/auth/whoami")
    );
    assert_eq!(
        links["apikey"].as_str(),
        Some("http://localhost:8000/api/v1/auth/apikey")
    );
    assert_eq!(
        links["revoke_session"].as_str(),
        Some("http://localhost:8000/api/v1/auth/session/revoke/{session_id}")
    );
}

/// G5: without an external OIDC provider, `authentication.links.logout` falls
/// back to tiled's own logout route (the links block is still built because an
/// internal authenticator is configured).
#[tokio::test]
async fn about_logout_falls_back_to_local_route_without_oidc() {
    let (app, _dir, _cat, _auth_db) = build_test_app().await;

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/",
        &[("host", "localhost:8000")],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let links = &body["authentication"]["links"];
    assert_eq!(
        links["logout"].as_str(),
        Some("http://localhost:8000/api/v1/auth/logout"),
        "logout must fall back to tiled's own route when no OIDC provider is configured"
    );
}

/// A login-capable external OIDC provider (has `client_id` +
/// `authorization_endpoint`) is advertised in `authentication.providers` as a
/// `mode=external` entry whose `auth_endpoint` is tiled's own `/authorize`
/// route. Deliberate divergence from Python's IdP-direct
/// `ProxiedOIDCAuthenticator`: tiled-rs brokers the device flow itself, so the
/// entry surfaces NEITHER `client_id` NOR `token_endpoint` (either would
/// mis-drive the tiled-client refresh / device grant).
#[tokio::test]
async fn about_advertises_login_capable_oidc_provider() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/",
        &[("host", "localhost:8000")],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let providers = body["authentication"]["providers"].as_array().unwrap();
    assert_eq!(
        providers.len(),
        1,
        "exactly the one login-capable OIDC provider must be advertised: {providers:?}"
    );
    let p = &providers[0];
    assert_eq!(p["provider"].as_str(), Some("mock-idp"));
    assert_eq!(p["mode"].as_str(), Some("external"));
    assert_eq!(
        p["links"]["auth_endpoint"].as_str(),
        Some("http://localhost:8000/api/v1/auth/provider/mock-idp/authorize"),
        "auth_endpoint must be tiled's own brokered /authorize route"
    );
    assert!(
        p["links"].get("client_id").is_none(),
        "client_id must NOT be advertised (would switch client refresh to form-encoded OAuth)"
    );
    assert!(
        p["links"].get("token_endpoint").is_none(),
        "token_endpoint must NOT be advertised (would flip the device grant to IdP-direct OAuth2)"
    );
}

/// A bearer-only external OIDC validator (no `client_id` /
/// `authorization_endpoint` — it only accepts tokens minted elsewhere) cannot
/// drive a login, so it is NOT advertised in `authentication.providers`. The
/// links block is still present (an external OIDC validator is configured).
#[tokio::test]
async fn about_omits_bearer_only_oidc_provider() {
    let (app, _auth_db, _validator, _dir) = build_oidc_app().await;

    let (status, body) = json_request(
        &app,
        Method::GET,
        "/api/v1/",
        &[("host", "localhost:8000")],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(
        body["authentication"]["providers"]
            .as_array()
            .unwrap()
            .len(),
        0,
        "a bearer-only validator must not be advertised as a login provider"
    );
    // The links block is still built (external OIDC validator is configured).
    assert!(
        body["authentication"]["links"]["whoami"].is_string(),
        "links block must still be present for a bearer-validator deployment"
    );
}

/// GET /authorize for an unknown provider → Validation error (not 302).
#[tokio::test]
async fn oidc_authorize_unknown_provider_returns_error() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/provider/no-such-provider/authorize")
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::SEE_OTHER,
        "unknown provider must not redirect"
    );
}

/// GET /callback with an unknown state → 401.
#[tokio::test]
async fn oidc_callback_unknown_state_rejected() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/provider/mock-idp/callback?code=bogus-code&state=no-such-state")
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "unknown state must be rejected with 401"
    );
}

/// GET /callback with expired state → 401.
#[tokio::test]
async fn oidc_callback_expired_state_rejected() {
    use chrono::Duration;
    let (app, _validator, db, _dir) = build_code_flow_app("https://mock-idp.test/token").await;

    // Seed an already-expired flow state (negative TTL → expiration in the past).
    db.create_oidc_flow_state(
        "expired-state",
        "mock-idp",
        "some-verifier",
        "some-nonce",
        Duration::seconds(-1),
    )
    .await
    .unwrap();

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/provider/mock-idp/callback?code=any-code&state=expired-state")
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "expired state must be rejected with 401"
    );
}

/// Full /authorize → /callback flow with a mock token endpoint.
///
/// The mock serves a signed id_token with the correct nonce. The
/// callback handler exchanges the code, validates the id_token, and
/// returns tiled access + refresh tokens.
#[tokio::test]
async fn oidc_callback_with_mock_idp_mints_session() {
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::post;
    use chrono::Duration;
    use std::collections::HashMap;

    // Spin up a local mock token endpoint.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/token");

    let known_nonce = "test-nonce-value-abc";
    let known_state = "test-state-value-xyz";

    // The mock returns a signed id_token embedding the known nonce.
    let id_token = mint_code_flow_id_token("bob", known_nonce);
    let id_token_clone = id_token.clone();
    let mock_app = Router::new().route(
        "/token",
        post(move |body: Bytes| {
            let token = id_token_clone.clone();
            async move {
                // Verify that PKCE verifier and grant_type are present.
                let form: HashMap<String, String> =
                    form_urlencoded::parse(body.as_ref()).into_owned().collect();
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("authorization_code")
                );
                assert!(
                    form.contains_key("code_verifier"),
                    "mock IdP: code_verifier must be sent"
                );
                // G3: the token POST must request offline_access so the IdP
                // returns a refresh_token for the downstream OBO refresh. The
                // scope is the sorted set (Python `" ".join(sorted(...))`); with
                // no extra_scopes that is "offline_access openid".
                assert_eq!(
                    form.get("scope").map(String::as_str),
                    Some("offline_access openid"),
                    "mock IdP: token POST must request 'offline_access openid'"
                );
                axum::Json(serde_json::json!({
                    "id_token": token,
                    "access_token": "mock-access-token",
                    "token_type": "Bearer",
                }))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let (app, _validator, db, _dir) = build_code_flow_app(&token_endpoint).await;

    // Pre-seed the known flow state (DB-backed) with our known state + nonce.
    db.create_oidc_flow_state(
        known_state,
        "mock-idp",
        "known-verifier",
        known_nonce,
        Duration::minutes(10),
    )
    .await
    .unwrap();

    let callback_uri =
        format!("/api/v1/auth/provider/mock-idp/callback?code=mock-code&state={known_state}");
    let req = Request::builder()
        .method(Method::GET)
        .uri(&callback_uri)
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "callback must return 200 with tokens"
    );
    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(
        body.get("access_token").is_some(),
        "response must contain access_token"
    );
    assert!(
        body.get("refresh_token").is_some(),
        "response must contain refresh_token"
    );
    assert_eq!(
        body.get("token_type").and_then(|v| v.as_str()),
        Some("Bearer")
    );
    assert_eq!(
        body.pointer("/identity/provider").and_then(|v| v.as_str()),
        Some("mock-idp")
    );
    assert_eq!(
        body.pointer("/identity/id").and_then(|v| v.as_str()),
        Some("bob"),
        "identity.id must be the sub from the id_token"
    );
}

/// G3: a provider configured with `extra_scopes` (the Entra resource-scope
/// mechanism) appends them — sorted into the `openid offline_access` baseline —
/// to the token POST. Exercises the full chain provider.extra_scopes →
/// exchange_code_flow → post_token_request → wire, calling the validator
/// directly (no HTTP app needed).
#[tokio::test]
async fn exchange_code_flow_appends_provider_extra_scopes() {
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::post;
    use jsonwebtoken::Algorithm;
    use std::collections::HashMap;
    use tiled_rs::auth::{ExternalOidcValidator, OidcFlowState, OidcProvider};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/token");

    let known_nonce = "extra-scope-nonce";
    let id_token = mint_code_flow_id_token("carol", known_nonce);
    let id_token_clone = id_token.clone();
    let mock_app = Router::new().route(
        "/token",
        post(move |body: Bytes| {
            let token = id_token_clone.clone();
            async move {
                let form: HashMap<String, String> =
                    form_urlencoded::parse(body.as_ref()).into_owned().collect();
                // baseline ∪ extra_scopes, sorted (BTreeSet / Python sorted()).
                assert_eq!(
                    form.get("scope").map(String::as_str),
                    Some("api://tiled-api/access_as_user offline_access openid"),
                    "token POST scope must be the sorted union of baseline + extra_scopes"
                );
                axum::Json(serde_json::json!({
                    "id_token": token,
                    "access_token": "a",
                    "refresh_token": "r",
                    "token_type": "Bearer",
                }))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let validator = Arc::new(
        ExternalOidcValidator::new(vec![OidcProvider {
            name: "entra-idp".into(),
            jwks_url: "https://mock-idp.test/jwks".into(),
            issuer: "https://mock-idp.test/".into(),
            audiences: vec!["tiled-code-client".into()],
            subject_claim: "sub".into(),
            identity_mapping: tiled_rs::auth::IdentityMapping::Standard,
            algorithms: vec![Algorithm::HS256],
            scopes_map: HashMap::new(),
            client_id: Some("tiled-code-client".into()),
            client_secret: None,
            authorization_endpoint: Some("https://mock-idp.test/authorize".into()),
            token_endpoint: Some(token_endpoint.clone()),
            extra_scopes: vec!["api://tiled-api/access_as_user".into()],
            end_session_endpoint: None,
            redirect_on_success: None,
            redirect_on_failure: None,
        }])
        .unwrap(),
    );
    validator
        .inject_key_for_test(
            "entra-idp",
            "test-kid",
            jsonwebtoken::DecodingKey::from_secret(b"mock-idp-secret!!"),
            Algorithm::HS256,
        )
        .await;

    let flow = OidcFlowState {
        provider: "entra-idp".into(),
        code_verifier: "verifier".into(),
        nonce: known_nonce.into(),
    };
    let session = validator
        .exchange_code_flow(&flow, "auth-code", "https://app/cb")
        .await
        .expect("code exchange must succeed");
    assert_eq!(
        session.token.sub, "carol",
        "the id_token must still validate (sanity that the exchange completed)"
    );
}

/// G3 OBO: an Entra code-flow login stores the upstream access/refresh tokens
/// and embeds them in the tiled access token's `state` claim — on the initial
/// login AND, unchanged, across a refresh.
#[tokio::test]
async fn oidc_callback_entra_embeds_obo_tokens_and_survives_refresh() {
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::post;
    use chrono::Duration;
    use tower::ServiceExt as _; // app.clone().oneshot twice

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/token");

    let known_nonce = "entra-obo-nonce";
    let known_state = "entra-obo-state";
    let id_token = mint_code_flow_id_token("entra-oid-xyz", known_nonce);
    let id_token_clone = id_token.clone();
    // The mock IdP returns BOTH upstream tokens (Entra issues a refresh_token
    // when offline_access was requested).
    let mock_app = Router::new().route(
        "/token",
        post(move |_body: Bytes| {
            let token = id_token_clone.clone();
            async move {
                axum::Json(serde_json::json!({
                    "id_token": token,
                    "access_token": "entra-upstream-access",
                    "refresh_token": "entra-upstream-refresh",
                    "token_type": "Bearer",
                }))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let (app, _validator, db, _dir) =
        build_code_flow_app_with_mapping(&token_endpoint, tiled_rs::auth::IdentityMapping::Entra)
            .await;
    db.create_oidc_flow_state(
        known_state,
        "mock-idp",
        "verifier",
        known_nonce,
        Duration::minutes(10),
    )
    .await
    .unwrap();

    // --- initial login via callback ---
    let callback_uri =
        format!("/api/v1/auth/provider/mock-idp/callback?code=c&state={known_state}");
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&callback_uri)
                .header("host", "localhost:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Entra callback must mint a session"
    );
    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .expect("access_token present");
    let refresh_token = body
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .expect("refresh_token present")
        .to_string();

    // Decode the tiled access token (same secret build_code_flow_app uses) and
    // assert the OBO state is embedded.
    let issuer = tiled_rs::auth::Issuer::new(b"code-flow-test-secret-32bytes!!!").unwrap();
    let claims = issuer.verify_access(access_token).unwrap();
    assert_eq!(
        claims
            .state
            .pointer("/entra_access_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-access"),
        "access token must carry the upstream Entra access token"
    );
    assert_eq!(
        claims
            .state
            .pointer("/entra_refresh_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-refresh"),
    );

    // --- refresh: the OBO state must survive unchanged ---
    let refresh_resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/refresh")
                .header("host", "localhost:8000")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "refresh_token": refresh_token }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        refresh_resp.status(),
        StatusCode::OK,
        "refresh must succeed"
    );
    let rbytes = axum::body::to_bytes(refresh_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let rbody: serde_json::Value = serde_json::from_slice(&rbytes).unwrap();
    let new_access = rbody
        .get("access_token")
        .and_then(|v| v.as_str())
        .expect("refreshed access_token");
    let new_claims = issuer.verify_access(new_access).unwrap();
    assert_eq!(
        new_claims
            .state
            .pointer("/entra_access_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-access"),
        "the OBO state must survive a token refresh unchanged"
    );
    assert_eq!(
        new_claims
            .state
            .pointer("/entra_refresh_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-refresh"),
    );
}

/// State is consumed on callback — replaying the same state is rejected.
#[tokio::test]
async fn oidc_callback_state_not_replayable() {
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::post;
    use chrono::Duration;
    use tower::ServiceExt as _; // second oneshot

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/token");

    let known_nonce = "replay-nonce";
    let known_state = "replay-state";

    let id_token = mint_code_flow_id_token("carol", known_nonce);
    let id_token_clone = id_token.clone();
    let mock_app = Router::new().route(
        "/token",
        post(move |_body: Bytes| {
            let token = id_token_clone.clone();
            async move {
                axum::Json(serde_json::json!({
                    "id_token": token,
                    "access_token": "mock-at",
                    "token_type": "Bearer",
                }))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let (app, _validator, db, _dir) = build_code_flow_app(&token_endpoint).await;

    db.create_oidc_flow_state(
        known_state,
        "mock-idp",
        "verifier",
        known_nonce,
        Duration::minutes(10),
    )
    .await
    .unwrap();

    let callback_uri =
        format!("/api/v1/auth/provider/mock-idp/callback?code=c&state={known_state}");

    // First request should succeed (consumes the single-use flow state); the
    // second must be rejected as a replay (the row is already deleted).
    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&callback_uri)
                .header("host", "localhost:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // Second request with the same state must be rejected (state consumed).
    let resp2 = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&callback_uri)
                .header("host", "localhost:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// G4 — IdP-brokered device-code flow (end-to-end through the HTTP app)
// ---------------------------------------------------------------------------

/// Spawn a mock IdP token endpoint for the DEVICE flow. Returns `id_token`
/// plus any `extra` top-level fields (e.g. access_token/refresh_token), and
/// asserts the request carries NO PKCE `code_verifier` (the device flow is
/// non-PKCE). Returns the endpoint URL.
async fn spawn_mock_device_token_endpoint(id_token: String, extra: serde_json::Value) -> String {
    use axum::Router;
    use axum::body::Bytes;
    use axum::routing::post;
    use std::collections::HashMap;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let token_endpoint = format!("http://127.0.0.1:{port}/token");
    let mock_app = Router::new().route(
        "/token",
        post(move |body: Bytes| {
            let token = id_token.clone();
            let extra = extra.clone();
            async move {
                let form: HashMap<String, String> =
                    form_urlencoded::parse(body.as_ref()).into_owned().collect();
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("authorization_code")
                );
                assert!(
                    !form.contains_key("code_verifier"),
                    "device flow must NOT send a PKCE code_verifier"
                );
                // G3: the device-flow token POST must also request
                // offline_access (same shared exchange path as the browser
                // flow); sorted set → "offline_access openid".
                assert_eq!(
                    form.get("scope").map(String::as_str),
                    Some("offline_access openid"),
                    "mock IdP: device-flow token POST must request 'offline_access openid'"
                );
                let mut resp = serde_json::json!({ "id_token": token, "token_type": "Bearer" });
                if let Some(obj) = extra.as_object() {
                    for (k, v) in obj {
                        resp[k] = v.clone();
                    }
                }
                axum::Json(resp)
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });
    token_endpoint
}

/// POST /provider/{p}/authorize returns the broker response the CLI's
/// device_code_grant (broker mode) depends on: an IdP `authorization_uri`
/// (no PKCE/nonce/state) + a `verification_uri` to poll, plus device_code,
/// user_code, interval, expires_in.
#[tokio::test]
async fn oidc_device_authorize_returns_broker_response() {
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app("https://mock-idp.test/token").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/provider/mock-idp/authorize")
        .header("host", "localhost:8000")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        body["verification_uri"].as_str(),
        Some("http://localhost:8000/api/v1/auth/provider/mock-idp/token")
    );
    assert_eq!(body["interval"].as_i64(), Some(5));
    assert_eq!(body["expires_in"].as_i64(), Some(900));
    assert_eq!(
        body["device_code"].as_str().map(|s| s.len()),
        Some(64),
        "device_code is 32 bytes hex"
    );
    let user_code = body["user_code"].as_str().unwrap();
    assert!(user_code.contains('-'), "user_code shown in dashed form");

    // authorization_uri = IdP authorize endpoint with the device redirect.
    let auth_url = reqwest::Url::parse(body["authorization_uri"].as_str().unwrap()).unwrap();
    assert_eq!(auth_url.host_str(), Some("mock-idp.test"));
    assert_eq!(auth_url.path(), "/authorize");
    let params: std::collections::HashMap<_, _> = auth_url.query_pairs().collect();
    assert_eq!(
        params.get("response_type").map(|s| s.as_ref()),
        Some("code")
    );
    assert_eq!(params.get("scope").map(|s| s.as_ref()), Some("openid"));
    assert_eq!(
        params.get("client_id").map(|s| s.as_ref()),
        Some("tiled-code-client")
    );
    assert_eq!(
        params.get("redirect_uri").map(|s| s.as_ref()),
        Some("http://localhost:8000/api/v1/auth/provider/mock-idp/device_code")
    );
    // Device flow carries NO PKCE/nonce/state in the authorize URL.
    assert!(!params.contains_key("code_challenge"));
    assert!(!params.contains_key("nonce"));
    assert!(!params.contains_key("state"));
}

/// Full device flow: authorize → poll(authorization_pending) → user-code
/// submit (exchanges the IdP code, binds a session) → poll(tokens) →
/// poll(404, single use).
#[tokio::test]
async fn oidc_device_flow_pending_then_fulfilled() {
    let id_token = mint_code_flow_id_token("device-bob", "ignored-nonce");
    let token_endpoint = spawn_mock_device_token_endpoint(id_token, serde_json::json!({})).await;
    let (app, _validator, _auth_db, _dir) = build_code_flow_app(&token_endpoint).await;

    // 1. authorize
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/authorize")
                .header("host", "localhost:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let device_code = body["device_code"].as_str().unwrap().to_string();
    let user_code = body["user_code"].as_str().unwrap().to_string();

    // 2. poll before the browser login → 400 authorization_pending
    let poll = |app: axum::Router, dc: String| async move {
        app.oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/token")
                .header("host", "localhost:8000")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "device_code": dc,
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
    };

    let pending = poll(app.clone(), device_code.clone()).await;
    assert_eq!(pending.status(), StatusCode::BAD_REQUEST);
    let pbytes = axum::body::to_bytes(pending.into_body(), 1 << 20)
        .await
        .unwrap();
    let pbody: serde_json::Value = serde_json::from_slice(&pbytes).unwrap();
    assert_eq!(
        pbody.pointer("/detail/error").and_then(|v| v.as_str()),
        Some("authorization_pending"),
        "client polls until this exact shape"
    );

    // 3. user submits the code in the browser (the IdP code rides the form).
    let submit = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/device_code")
                .header("host", "localhost:8000")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "code=idp-auth-code&user_code={user_code}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(submit.status(), StatusCode::OK);
    let sbytes = axum::body::to_bytes(submit.into_body(), 1 << 20)
        .await
        .unwrap();
    let shtml = String::from_utf8(sbytes.to_vec()).unwrap();
    assert!(shtml.contains("Success"), "submit returns the success page");

    // 4. poll after login → 200 with tokens
    let granted = poll(app.clone(), device_code.clone()).await;
    assert_eq!(granted.status(), StatusCode::OK);
    let gbytes = axum::body::to_bytes(granted.into_body(), 1 << 20)
        .await
        .unwrap();
    let gbody: serde_json::Value = serde_json::from_slice(&gbytes).unwrap();
    assert!(gbody["access_token"].is_string());
    assert!(gbody["refresh_token"].is_string());
    assert_eq!(gbody["token_type"].as_str(), Some("Bearer"));

    // 5. single use: the pending row is consumed → 404
    let again = poll(app.clone(), device_code.clone()).await;
    assert_eq!(again.status(), StatusCode::NOT_FOUND);
}

/// G3+G4: an Entra device login embeds the upstream OBO tokens in the access
/// token the CLI receives from the /token poll.
#[tokio::test]
async fn oidc_device_flow_entra_embeds_obo() {
    let id_token = mint_code_flow_id_token("entra-device-oid", "ignored-nonce");
    let token_endpoint = spawn_mock_device_token_endpoint(
        id_token,
        serde_json::json!({
            "access_token": "entra-upstream-access",
            "refresh_token": "entra-upstream-refresh",
        }),
    )
    .await;
    let (app, _validator, _auth_db, _dir) =
        build_code_flow_app_with_mapping(&token_endpoint, tiled_rs::auth::IdentityMapping::Entra)
            .await;

    // authorize → user_code/device_code
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/authorize")
                .header("host", "localhost:8000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let device_code = body["device_code"].as_str().unwrap().to_string();
    let user_code = body["user_code"].as_str().unwrap().to_string();

    // submit (browser-side OIDC login)
    let submit = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/device_code")
                .header("host", "localhost:8000")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "code=idp-auth-code&user_code={user_code}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(submit.status(), StatusCode::OK);

    // poll → tokens; decode the access token and assert the OBO state.
    let granted = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/provider/mock-idp/token")
                .header("host", "localhost:8000")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "device_code": device_code }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(granted.status(), StatusCode::OK);
    let gbytes = axum::body::to_bytes(granted.into_body(), 1 << 20)
        .await
        .unwrap();
    let gbody: serde_json::Value = serde_json::from_slice(&gbytes).unwrap();
    let access = gbody["access_token"].as_str().unwrap();

    let issuer = tiled_rs::auth::Issuer::new(b"code-flow-test-secret-32bytes!!!").unwrap();
    let claims = issuer.verify_access(access).unwrap();
    assert_eq!(
        claims
            .state
            .pointer("/entra_access_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-access"),
        "device-flow access token must carry the upstream Entra access token"
    );
    assert_eq!(
        claims
            .state
            .pointer("/entra_refresh_token")
            .and_then(|v| v.as_str()),
        Some("entra-upstream-refresh"),
    );
}
