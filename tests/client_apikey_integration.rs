//! Live-server integration for the client `Context` auth-management methods,
//! exercised against a real auth-enabled tiled server over TCP (the reqwest
//! client talks to `axum::serve`, unlike the `oneshot` harness in
//! `auth_integration.rs`).
//!
//! GAP #2 — API-key management: `which_api_key` / `create_api_key` /
//! `revoke_api_key`.
//! GAP #3 — session management: `revoke_session`.

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::auth::{ApiKeyCreate, AuthDb, DummyAuthenticator, Issuer, ScopeSet};
use tiled_rs::client::{Context, ContextOptions};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::queries::Query;

/// Shared HS256 secret for the test issuer (same value the other auth tests use).
const ISSUER_SECRET: &[u8] = b"this-is-a-test-secret-32-bytes-long!!";

fn build_root() -> MapAdapter {
    let mut mapping = IndexMap::new();
    let data: Vec<f64> = (0..4).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));
    mapping.insert("arr".into(), AnyAdapter::Array(Arc::new(arr)));
    MapAdapter::new(
        mapping,
        serde_json::json!({"description": "auth test catalog"}),
        vec![],
    )
}

/// Spawn an auth-enabled server (dummy authenticator, `alice`/`wonderland`) on
/// an ephemeral port. Returns the base URL and the live `AuthDb` so tests can
/// seed principals and bootstrap API keys directly.
async fn spawn_auth_server() -> (String, AuthDb, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());
    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

    let issuer = Issuer::new(ISSUER_SECRET).unwrap();

    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(build_root());
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: None,
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
        default_login_scopes: ScopeSet::full(),
        enable_web: false,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };

    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base_url, auth_db, dir)
}

/// Bootstrap a DB-backed API key owned by `alice` holding the full `user` role
/// scopes (which include `create:apikeys` / `revoke:apikeys`), so a `Context`
/// keyed with it authenticates AS alice with permission to mint/revoke keys.
async fn bootstrap_alice_key(auth_db: &AuthDb) -> String {
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    let material = auth_db
        .create_api_key(ApiKeyCreate {
            principal_id: alice.id,
            note: Some("bootstrap".into()),
            scopes: ScopeSet::for_role("user"),
            expiration_time: None,
        })
        .await
        .unwrap();
    material.secret
}

/// Status of a bare `GET /api/v1/metadata/` authenticated with `Apikey <key>`,
/// used to prove a key works (or has stopped working) as a general credential.
async fn metadata_status(base: &str, apikey: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("{base}/api/v1/metadata/"))
        .header("authorization", format!("Apikey {apikey}"))
        .send()
        .await
        .unwrap()
        .status()
}

/// Full GAP #2 flow: create a key → `which_api_key` reflects it → the key
/// authenticates a normal request → revoke → the key stops working.
#[tokio::test]
async fn create_which_authenticate_then_revoke() {
    let (base, auth_db, _dir) = spawn_auth_server().await;
    let bootstrap = bootstrap_alice_key(&auth_db).await;

    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().api_key(bootstrap))
            .unwrap();

    // create_api_key: subset scopes + note, no expiry.
    let created = ctx
        .create_api_key(
            Some(vec!["read:metadata".into(), "read:data".into()]),
            None,
            Some("read-only key".into()),
        )
        .await
        .expect("create_api_key must succeed for a create:apikeys holder");
    assert_eq!(created.first_eight.len(), 8);
    assert_eq!(created.secret.len(), 64, "secret is 32 bytes hex");
    let mut created_scopes = created.scopes.clone();
    created_scopes.sort();
    assert_eq!(
        created_scopes,
        vec!["read:data".to_string(), "read:metadata".to_string()],
        "granted scopes echo back on create"
    );
    assert!(
        created.expiration_time.is_none(),
        "no expiry requested → expiration_time is None"
    );

    // A second Context authenticated with the NEW key.
    let (ctx2, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().api_key(created.secret.clone()),
    )
    .unwrap();

    // which_api_key reflects the created key's identity.
    let info = ctx2
        .which_api_key()
        .await
        .expect("which_api_key on a valid key must succeed");
    assert_eq!(info.first_eight, created.first_eight);
    assert_eq!(info.note.as_deref(), Some("read-only key"));
    let mut info_scopes = info.scopes.clone();
    info_scopes.sort();
    assert_eq!(
        info_scopes,
        vec!["read:data".to_string(), "read:metadata".to_string()]
    );
    assert!(info.expiration_time.is_none());
    assert!(
        info.time_created <= chrono::Utc::now(),
        "time_created is populated and in the past"
    );

    // The new key authenticates a normal data request.
    assert_eq!(
        metadata_status(&base, &created.secret).await,
        reqwest::StatusCode::OK,
        "a fresh key must authenticate a metadata read"
    );

    // Revoke it via the bootstrap context (alice owns it).
    ctx.revoke_api_key(&created.first_eight)
        .await
        .expect("owner revoke must succeed");

    // The key has stopped working: which_api_key now 401s, and the data
    // request 401s too.
    let err = ctx2
        .which_api_key()
        .await
        .expect_err("a revoked key must no longer resolve via which_api_key");
    assert!(
        matches!(err, tiled_rs::client::ClientError::AuthRequired(_)),
        "revoked key → AuthRequired, got {err:?}"
    );
    assert_eq!(
        metadata_status(&base, &created.secret).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "a revoked key must stop authenticating data requests"
    );
}

/// create_api_key with scopes + `expires_in_seconds` + note: all three
/// round-trip through the create response and back out of which_api_key.
#[tokio::test]
async fn create_with_scopes_expiry_note_roundtrips() {
    let (base, auth_db, _dir) = spawn_auth_server().await;
    let bootstrap = bootstrap_alice_key(&auth_db).await;
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().api_key(bootstrap))
            .unwrap();

    let created = ctx
        .create_api_key(
            Some(vec!["read:metadata".into()]),
            Some(3600),
            Some("expiring".into()),
        )
        .await
        .expect("create with expiry must succeed");
    assert_eq!(created.scopes, vec!["read:metadata".to_string()]);
    let exp = created
        .expiration_time
        .expect("expires_in_seconds must yield a concrete expiration_time");
    let now = chrono::Utc::now();
    // Boundary: the expiry lands in the future, ~3600s out (allow clock skew).
    assert!(exp > now, "expiration must be in the future");
    assert!(
        exp <= now + chrono::Duration::seconds(3600 + 60),
        "expiration must be ~1h out, not unbounded"
    );

    // which_api_key on the new key echoes note + scopes + the same expiry.
    let (ctx2, _) = Context::from_uri_with_options(
        &base,
        ContextOptions::default().api_key(created.secret.clone()),
    )
    .unwrap();
    let info = ctx2.which_api_key().await.unwrap();
    assert_eq!(info.note.as_deref(), Some("expiring"));
    assert_eq!(info.scopes, vec!["read:metadata".to_string()]);
    assert_eq!(
        info.expiration_time, created.expiration_time,
        "expiration_time round-trips identically through which_api_key"
    );
}

/// Log alice in via the dummy authenticator; returns her refresh token.
async fn login_refresh(base: &str) -> String {
    let body: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/api/v1/auth/dummy/login"))
        .json(&serde_json::json!({"username": "alice", "password": "wonderland"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["refresh_token"].as_str().unwrap().to_string()
}

/// Status of `POST /api/v1/auth/refresh` for a given refresh token.
async fn refresh_status(base: &str, refresh: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("{base}/api/v1/auth/refresh"))
        .json(&serde_json::json!({"refresh_token": refresh}))
        .send()
        .await
        .unwrap()
        .status()
}

/// GAP #3: `revoke_session(session_id)` invalidates exactly that session's
/// refresh token, while an untouched session keeps refreshing. The Context is
/// authenticated as alice via a bootstrap API key (revoke requires the caller
/// to own the session).
#[tokio::test]
async fn revoke_session_invalidates_only_that_sessions_refresh() {
    let (base, auth_db, _dir) = spawn_auth_server().await;

    // Two independent alice sessions (each dummy login mints a fresh session).
    let refresh_a = login_refresh(&base).await;
    let refresh_b = login_refresh(&base).await;

    // The test controls the issuer secret, so decode refresh_a to recover its
    // session UUID.
    let issuer = Issuer::new(ISSUER_SECRET).unwrap();
    let session_a = issuer.verify_refresh(&refresh_a).unwrap().sid;

    // A Context that authenticates AS alice (same principal that owns the
    // sessions), able to revoke by UUID.
    let bootstrap = bootstrap_alice_key(&auth_db).await;
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().api_key(bootstrap))
            .unwrap();

    ctx.revoke_session(&session_a)
        .await
        .expect("owner revoke_session must succeed");

    // Session A's refresh is now dead; session B (untouched) still refreshes —
    // proving the revoke targeted exactly one session.
    assert_eq!(
        refresh_status(&base, &refresh_a).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "a refresh on the revoked session must be 401"
    );
    assert_eq!(
        refresh_status(&base, &refresh_b).await,
        reqwest::StatusCode::OK,
        "an untouched session must keep refreshing (control)"
    );
}

/// GAP #3 ownership: revoking a session the caller does not own is opaque —
/// the Rust server answers 404, surfaced as a `Server { status: 404, .. }`
/// error rather than success.
#[tokio::test]
async fn revoke_session_foreign_session_is_not_found() {
    let (base, auth_db, _dir) = spawn_auth_server().await;

    // bob owns a session; alice (the Context principal) does not.
    let (bob, _) = auth_db.ensure_principal("dummy", "bob").await.unwrap();
    let bob_session = auth_db
        .create_session(
            bob.id,
            ScopeSet::for_role("user"),
            chrono::Utc::now() + chrono::Duration::hours(1),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    let bootstrap = bootstrap_alice_key(&auth_db).await;
    let (ctx, _) =
        Context::from_uri_with_options(&base, ContextOptions::default().api_key(bootstrap))
            .unwrap();

    let err = ctx
        .revoke_session(&bob_session.uuid)
        .await
        .expect_err("revoking another principal's session must fail");
    match err {
        tiled_rs::client::ClientError::Server { status, .. } => {
            assert_eq!(status, 404, "cross-principal revoke must be opaque 404");
        }
        other => panic!("expected Server{{status:404}}, got {other:?}"),
    }
}
