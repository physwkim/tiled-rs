//! Live-server integration for the client `Context::admin()` accessor — admin
//! principal management and per-principal API-key management — exercised
//! against a real auth-enabled tiled server over TCP.
//!
//! Client gap #6: `Admin::list_principals` / `show_principal` /
//! `create_service_principal` / `create_api_key` / `revoke_api_key`.

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
        serde_json::json!({"description": "admin test catalog"}),
        vec![],
    )
}

/// Spawn an auth-enabled server on an ephemeral port. Returns the base URL and
/// the live `AuthDb` so tests can seed principals and bootstrap keys directly.
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

/// Bootstrap an admin-authenticated `Context`: promote alice to the `admin`
/// role and mint her a full-scope DB-backed key. Returns the context and
/// alice's principal UUID.
async fn admin_context(base: &str, auth_db: &AuthDb) -> (Context, String) {
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .update_principal_role(alice.id, "admin")
        .await
        .unwrap();
    let material = auth_db
        .create_api_key(ApiKeyCreate {
            principal_id: alice.id,
            note: Some("admin-bootstrap".into()),
            scopes: ScopeSet::full(),
            expiration_time: None,
        })
        .await
        .unwrap();
    let (ctx, _) =
        Context::from_uri_with_options(base, ContextOptions::default().api_key(material.secret))
            .unwrap();
    (ctx, alice.uuid)
}

/// Bootstrap a non-admin (`user` role) alice `Context`.
async fn user_context(base: &str, auth_db: &AuthDb) -> Context {
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    let material = auth_db
        .create_api_key(ApiKeyCreate {
            principal_id: alice.id,
            note: Some("user-bootstrap".into()),
            scopes: ScopeSet::for_role("user"),
            expiration_time: None,
        })
        .await
        .unwrap();
    let (ctx, _) =
        Context::from_uri_with_options(base, ContextOptions::default().api_key(material.secret))
            .unwrap();
    ctx
}

/// Status of a bare `GET /api/v1/metadata/` authenticated with `Apikey <key>`.
async fn metadata_status(base: &str, apikey: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("{base}/api/v1/metadata/"))
        .header("authorization", format!("Apikey {apikey}"))
        .send()
        .await
        .unwrap()
        .status()
}

/// list_principals reflects existing principals; create_service_principal
/// mints a new one with the requested role; show_principal round-trips it.
#[tokio::test]
async fn list_create_show_principals() {
    let (base, auth_db, _dir) = spawn_auth_server().await;
    let (ctx, alice_uuid) = admin_context(&base, &auth_db).await;
    let admin = ctx.admin();

    // At least the admin principal (alice) is listed, and she appears by UUID.
    let before = admin.list_principals(0, 100).await.expect("admin list");
    assert!(
        !before.is_empty(),
        "list must include at least the admin principal"
    );
    assert!(
        before.iter().any(|p| p.uuid == alice_uuid),
        "alice's principal must appear in the list"
    );

    // Create a service principal with the `user` role.
    let created = admin
        .create_service_principal("user")
        .await
        .expect("admin create_service_principal");
    assert_eq!(created.principal_type, "service");
    assert_eq!(created.role, "user");
    assert!(!created.uuid.is_empty());
    assert!(
        created.identities.is_empty(),
        "a fresh service principal has no linked identities"
    );

    // show_principal round-trips the freshly created principal by UUID.
    let shown = admin
        .show_principal(&created.uuid)
        .await
        .expect("admin show_principal");
    assert_eq!(shown.uuid, created.uuid);
    assert_eq!(shown.principal_type, "service");
    assert_eq!(shown.role, "user");

    // The new principal now appears in the list too.
    let after = admin.list_principals(0, 100).await.expect("admin list");
    assert!(
        after.len() > before.len(),
        "creating a service principal must grow the list"
    );
    assert!(
        after.iter().any(|p| p.uuid == created.uuid),
        "the created service principal must appear in the list"
    );

    // show_principal on an unknown UUID → opaque 404.
    let err = admin
        .show_principal("00000000-0000-0000-0000-000000000000")
        .await
        .expect_err("unknown principal must error");
    match err {
        tiled_rs::client::ClientError::Server { status, .. } => {
            assert_eq!(status, 404, "unknown principal → 404");
        }
        other => panic!("expected Server{{status:404}}, got {other:?}"),
    }
}

/// An admin mints an API key for another principal; the key authenticates a
/// request; the admin revokes it and the key stops working.
#[tokio::test]
async fn mint_and_revoke_apikey_for_principal() {
    let (base, auth_db, _dir) = spawn_auth_server().await;
    let (ctx, _alice_uuid) = admin_context(&base, &auth_db).await;
    let admin = ctx.admin();

    // A service principal to hold the key.
    let svc = admin
        .create_service_principal("user")
        .await
        .expect("create service principal");

    // Mint a read-only key for it.
    let key = admin
        .create_api_key(
            &svc.uuid,
            Some(vec!["read:metadata".into()]),
            None,
            Some("svc key".into()),
        )
        .await
        .expect("admin create_api_key for principal");
    assert_eq!(key.first_eight.len(), 8);
    assert_eq!(key.secret.len(), 64);
    assert_eq!(key.scopes, vec!["read:metadata".to_string()]);

    // The key authenticates a normal request.
    assert_eq!(
        metadata_status(&base, &key.secret).await,
        reqwest::StatusCode::OK,
        "the minted key must authenticate a metadata read"
    );

    // Admin revokes the key for that principal.
    admin
        .revoke_api_key(&svc.uuid, &key.first_eight)
        .await
        .expect("admin revoke_api_key for principal");

    // The key has stopped working.
    assert_eq!(
        metadata_status(&base, &key.secret).await,
        reqwest::StatusCode::UNAUTHORIZED,
        "the revoked key must stop authenticating"
    );

    // Revoking a non-existent key for the same principal → opaque 404.
    let err = admin
        .revoke_api_key(&svc.uuid, "deadbeef")
        .await
        .expect_err("revoking an absent key must error");
    match err {
        tiled_rs::client::ClientError::Server { status, .. } => {
            assert_eq!(status, 404, "absent key → 404");
        }
        other => panic!("expected Server{{status:404}}, got {other:?}"),
    }
}

/// A non-admin caller hitting an admin endpoint is authenticated but lacks the
/// scope, so the server's 403 maps to `ClientError::PermissionDenied`.
#[tokio::test]
async fn non_admin_admin_call_is_permission_denied() {
    let (base, auth_db, _dir) = spawn_auth_server().await;
    let ctx = user_context(&base, &auth_db).await;

    let err = ctx
        .admin()
        .list_principals(0, 100)
        .await
        .expect_err("a user-role caller must be forbidden from listing principals");
    assert!(
        matches!(err, tiled_rs::client::ClientError::PermissionDenied(_)),
        "non-admin admin call → PermissionDenied, got {err:?}"
    );
}
