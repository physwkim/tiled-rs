//! Verify that the access-policy list_filter is injected into search queries
//! so a principal only receives nodes they are permitted to see.
//!
//! Uses TagBasedPolicy: nodes with no tags are public; nodes tagged "team-a"
//! are visible only to principals granted "team-a"; "team-b" analogously.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_access::{ScopeSet, TagBasedPolicy};
use tiled_auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_catalog::{Catalog, node::RegisterRequest};
use tiled_core::adapters::ContainerAdapter;
use tiled_core::queries::Query;

async fn search_json(
    app: &axum::Router,
    auth_header: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/search/?page[offset]=0&page[limit]=100");
    if let Some(h) = auth_header {
        req = req.header("authorization", h);
    }
    let req = req
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
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
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

fn result_keys(body: &serde_json::Value) -> Vec<String> {
    body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|e| e["id"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Verify: a logged-in principal with a TagBasedPolicy only sees
/// public (untagged) nodes and nodes matching their granted tags.
#[tokio::test]
async fn search_respects_tag_based_access_policy() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // Pre-create alice so we know her UUID before building the app.
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();

    // Seed nodes with different access_blob tags.
    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("team_a_node", json!({"tags": ["team-a"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("team_b_node", json!({"tags": ["team-b"]})),
        )
        .await
        .unwrap();

    // Build policy: alice has "team-a" access.
    let mut policy = TagBasedPolicy::new(ScopeSet::full());
    policy.grant(&alice.uuid, "team-a");
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    // Alice logs in — "user" role intersect full = user scopes (includes read:metadata).
    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    // Alice sees public_node (untagged) + team_a_node (matches her tag).
    // team_b_node is NOT visible.
    let (status, body) = search_json(&app, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string()),
        "alice must see public_node, got: {keys:?}"
    );
    assert!(
        keys.contains(&"team_a_node".to_string()),
        "alice must see team_a_node, got: {keys:?}"
    );
    assert!(
        !keys.contains(&"team_b_node".to_string()),
        "alice must NOT see team_b_node, got: {keys:?}"
    );
    assert_eq!(keys.len(), 2, "alice sees exactly 2 nodes, got: {keys:?}");
}

/// Admin principals bypass the row-level filter entirely (ALL_ACCESS):
/// list_filter returns None, so search returns every node — owned by anyone,
/// any tag. Mirrors Python filters() admin → ALL_ACCESS (access_policies.py:387).
#[tokio::test]
async fn admin_search_sees_all_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // Pre-create root and promote to admin role so its login carries full scopes.
    let (root, _) = auth_db.ensure_principal("dummy", "root").await.unwrap();
    auth_db
        .update_principal_role(root.id, "admin")
        .await
        .unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("team_a_node", json!({"tags": ["team-a"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("owned_node", json!({"user": "bob-uuid-1234"})),
        )
        .await
        .unwrap();

    // No tag grants — admin must see everything regardless.
    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("root", "toor").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    let token = login(&app, "root", "toor").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = search_json(&app, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK, "admin search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string())
            && keys.contains(&"team_a_node".to_string())
            && keys.contains(&"owned_node".to_string()),
        "admin must see every node (no row filter), got: {keys:?}"
    );
    assert_eq!(keys.len(), 3, "admin sees all 3 nodes, got: {keys:?}");
}

async fn get_status(app: &axum::Router, uri: &str, bearer: Option<&str>) -> StatusCode {
    let mut builder = Request::builder().method(Method::GET).uri(uri);
    if let Some(b) = bearer {
        builder = builder.header("authorization", b);
    }
    let req = builder.body(Body::empty()).unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

async fn patch_status(app: &axum::Router, uri: &str, bearer: Option<&str>) -> StatusCode {
    let mut builder = Request::builder()
        .method(Method::PATCH)
        .uri(uri)
        .header("content-type", "application/octet-stream");
    if let Some(b) = bearer {
        builder = builder.header("authorization", b);
    }
    let req = builder.body(Body::empty()).unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// Verify: PATCH /array/full on a tag-restricted node returns 404 for a
/// principal that is not granted the required tag (F1 regression).
///
/// F1 wired array_append through resolve_entry(WriteData). resolve_entry_catalog
/// calls narrow_for_node per path segment; TagBasedPolicy returns empty scopes for
/// a principal with no matching tag grant. The narrowed auth then fails the
/// ReadMetadata check → ServerError::NotFound → HTTP 404.
/// The array adapter is never reached — UnresolvedLeaf would produce a different
/// error code if it were instantiated, so the 404 assertion proves the adapter
/// is bypassed entirely.
#[tokio::test]
async fn array_append_denied_by_tag_policy_returns_404() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // Create alice in auth_db so DummyAuthenticator can authenticate her.
    // No tag grant is added to the policy, so the "restricted" node is invisible to her.
    auth_db.ensure_principal("dummy", "alice").await.unwrap();

    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "restricted_array".to_string(),
                structure_family: "array".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({"tags": ["restricted"]}),
            },
        )
        .await
        .unwrap();

    // TagBasedPolicy with NO grants → principal_decision for alice returns empty
    // scopes for any node tagged "restricted".
    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let status = patch_status(&app, "/api/v1/array/full/restricted_array", Some(&bearer)).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "PATCH to a tag-restricted node must return 404 before the adapter is reached"
    );
}

/// Anonymous search: only untagged (public) nodes are visible.
///
/// Uses no auth backend (api_key=None, auth_db=None) so the middleware falls
/// through to the "no auth configured" branch and gives anonymous full scopes.
/// The access policy list_filter(None) still restricts to untagged nodes.
#[tokio::test]
async fn anonymous_search_shows_only_public_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("private_node", json!({"tags": ["secret"]})),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

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
        // No auth backend → middleware's "no_auth_configured" branch →
        // anonymous principal with full scopes. The access policy still
        // applies list_filter(principal=None) which restricts to untagged.
        api_key: None,
        catalog: Some(catalog),
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    // Anonymous search: no auth header.
    let (status, body) = search_json(&app, None).await;
    assert_eq!(status, StatusCode::OK, "anon search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string()),
        "anon must see public_node, got: {keys:?}"
    );
    assert!(
        !keys.contains(&"private_node".to_string()),
        "anon must NOT see private_node, got: {keys:?}"
    );
    assert_eq!(keys.len(), 1, "anon sees exactly 1 node, got: {keys:?}");
}

/// A node tagged with the literal "public" tag is world-readable: anonymous
/// must both LIST it (search) and READ it (direct metadata GET), consistently.
/// A node tagged "secret" stays hidden on both surfaces. Mirrors Python
/// is_tag_public for the built-in public_tag (access_policies.py:354-356).
#[tokio::test]
async fn public_tagged_node_is_listable_and_readable_by_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(
            None,
            vec![],
            make_node("public_tag_node", json!({"tags": ["public"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("secret_node", json!({"tags": ["secret"]})),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

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
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    // List surface: anonymous search sees the public-tagged node, not secret.
    let (status, body) = search_json(&app, None).await;
    assert_eq!(status, StatusCode::OK, "anon search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_tag_node".to_string()),
        "anon must list the public-tagged node, got: {keys:?}"
    );
    assert!(
        !keys.contains(&"secret_node".to_string()),
        "anon must NOT list the secret node, got: {keys:?}"
    );

    // Read surface: anonymous can read the public-tagged node, not the secret.
    let pub_status = get_status(&app, "/api/v1/metadata/public_tag_node", None).await;
    assert_eq!(
        pub_status,
        StatusCode::OK,
        "anon must read the public-tagged node"
    );
    let sec_status = get_status(&app, "/api/v1/metadata/secret_node", None).await;
    assert_eq!(
        sec_status,
        StatusCode::NOT_FOUND,
        "anon must NOT read the secret node"
    );
}

/// Regression (CRITICAL fail-open leak): a user-owned node `{"user": id}` has
/// no `tags` key. The untagged-public arm must NOT treat it as world-readable,
/// so anonymous /search must NOT return another user's owned node.
///
/// Fails on pre-fix code: the SQL untagged arm `tags IS NULL OR length=0`
/// matched `{"user": ...}` rows, leaking every owned node to anonymous.
#[tokio::test]
async fn anonymous_search_excludes_user_owned_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    // A node owned by some user — the normal shape stamped on every
    // authenticated create (creator_access_blob → {"user": uuid}).
    catalog
        .create_node(
            None,
            vec![],
            make_node("owned_node", json!({"user": "bob-uuid-1234"})),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

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
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    let (status, body) = search_json(&app, None).await;
    assert_eq!(status, StatusCode::OK, "anon search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string()),
        "anon must see public_node, got: {keys:?}"
    );
    assert!(
        !keys.contains(&"owned_node".to_string()),
        "anon must NOT see another user's owned node, got: {keys:?}"
    );
    assert_eq!(keys.len(), 1, "anon sees exactly 1 node, got: {keys:?}");
}

/// Regression (CRITICAL fail-open leak): an authenticated principal must NOT
/// see another user's owned node in /search. Alice sees her own owned node and
/// genuinely public nodes, but never bob's owned node.
#[tokio::test]
async fn cross_user_search_excludes_other_users_owned_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    // Pre-create alice so we know her UUID before seeding her owned node.
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("alice_node", json!({"user": alice.uuid})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("bob_node", json!({"user": "bob-uuid-9999"})),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    let (status, body) = search_json(&app, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let keys = result_keys(&body);
    assert!(
        keys.contains(&"public_node".to_string()),
        "alice must see public_node, got: {keys:?}"
    );
    assert!(
        keys.contains(&"alice_node".to_string()),
        "alice must see her own owned node, got: {keys:?}"
    );
    assert!(
        !keys.contains(&"bob_node".to_string()),
        "alice must NOT see bob's owned node, got: {keys:?}"
    );
    assert_eq!(keys.len(), 2, "alice sees exactly 2 nodes, got: {keys:?}");
}

/// Regression (CRITICAL fail-open leak), per-node direct-read vector: a
/// GET /metadata/{owned_node} for another user's node must be 404. The
/// per-node gate (principal_decision/anonymous_decision) previously treated
/// a `{"user": id}` blob (no `tags` key) as untagged-public and granted read.
#[tokio::test]
async fn metadata_read_excludes_other_users_owned_node() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    let catalog = Catalog::connect(&cat_uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let auth_db = AuthDb::connect(&auth_uri).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();

    let make_node = |key: &str, access_blob: serde_json::Value| RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    };
    catalog
        .create_node(None, vec![], make_node("public_node", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("alice_node", json!({"user": alice.uuid})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            None,
            vec![],
            make_node("bob_node", json!({"user": "bob-uuid-9999"})),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(ScopeSet::full());
    let access_policy: Arc<dyn tiled_access::AccessPolicy> = Arc::new(policy);

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(tiled_catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

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
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);

    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    // Alice cannot read bob's owned node — 404 (drops ReadMetadata at the gate).
    let bob = get_status(&app, "/api/v1/metadata/bob_node", Some(&bearer)).await;
    assert_eq!(
        bob,
        StatusCode::NOT_FOUND,
        "alice must NOT read bob's owned node via direct metadata GET"
    );
    // Alice can read her own owned node.
    let mine = get_status(&app, "/api/v1/metadata/alice_node", Some(&bearer)).await;
    assert_eq!(
        mine,
        StatusCode::OK,
        "alice must read her own owned node, got {mine}"
    );
    // Genuinely public node stays readable.
    let public = get_status(&app, "/api/v1/metadata/public_node", Some(&bearer)).await;
    assert_eq!(public, StatusCode::OK, "public node must be readable");
}
