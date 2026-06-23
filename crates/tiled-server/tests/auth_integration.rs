//! End-to-end auth flow: dummy login → access read endpoint with JWT →
//! create api key → use api key → revoke api key.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_auth::{AuthDb, DummyAuthenticator, Issuer, Scope};
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
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
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
    let narrow = tiled_auth::ScopeSet::read_only();
    let session = auth_db
        .create_session(
            alice.id,
            narrow,
            chrono::Utc::now() + chrono::Duration::hours(1),
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
    let issuer = tiled_auth::Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
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
    Arc<tiled_auth::ExternalOidcValidator>,
    tempfile::TempDir,
) {
    let (state, auth_db, validator, dir) = build_oidc_state(std::collections::HashMap::new()).await;
    (tiled_server::build_app(state), auth_db, validator, dir)
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
    scopes_map: std::collections::HashMap<String, Vec<tiled_auth::Scope>>,
) -> (
    tiled_server::AppState,
    AuthDb,
    Arc<tiled_auth::ExternalOidcValidator>,
    tempfile::TempDir,
) {
    use jsonwebtoken::Algorithm;
    use tiled_auth::{ExternalOidcValidator, OidcProvider};

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
            algorithms: vec![Algorithm::HS256],
            scopes_map,
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
        catalog: Some(catalog),
        auth_db: Some(auth_db.clone()),
        issuer: Some(issuer),
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: Some(validator.clone()),
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: vec![],
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
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
    let ctx = tiled_server::app::validate_bearer(&state, &token)
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
    let ctx = tiled_server::app::validate_bearer(&state, &token)
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
