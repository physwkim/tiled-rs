//! F5 regression: an API key's `access_tags` restriction MUST be enforced on
//! the write paths (node create → `init_node`, metadata/tag change →
//! `modify_node`), not only on the read/list paths.
//!
//! Upstream threads `authn_access_tags` into both `init_node`
//! (`router.py:1893`, `_create_node`) and `modify_node` (PATCH/PUT handlers),
//! so a key scoped to `["team-a"]` cannot create or tag a node with `team-b`
//! even when its principal owns `team-b`. The port's write handlers previously
//! passed `None` at all three call sites (`router.rs` init_node + two
//! modify_node), so the restriction was silently dropped: a `[team-a]` key
//! could create/tag `team-b` nodes. These tests drive the real
//! `TagBasedPolicy` end-to-end with a tag-restricted API key.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::access::{ScopeSet, TagBasedPolicy};
use tiled_rs::auth::{ApiKeyCreate, AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Build an app backed by a `TagBasedPolicy` where the principal `alice`
/// (role `user`) owns BOTH `team-a` and `team-b` (both defined tags), and mint
/// an API key for her restricted to `access_tags = ["team-a"]`. Returns the
/// app, the key secret, and a catalog handle for persistence assertions.
async fn build() -> (axum::Router, String, Catalog) {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1: warm the single connection up front so no connection opens
    // mid-request under a saturated CI runner (avoids the SQLite cold-start
    // "unable to open database file" flake — see patch_access_blob_integration).
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    // alice OWNS both tags, and both are defined in the registry — so WITHOUT
    // the key restriction, init_node/modify_node would accept a team-b change.
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string(), "team-b".to_string()])
        .await
        .unwrap();
    auth_db.define_tag("team-a").await.unwrap();
    auth_db.define_tag("team-b").await.unwrap();

    // Key restricted to team-a. `for_role("user")` gives write:metadata +
    // create:node (needed to reach init_node/modify_node) and carries neither
    // admin:apikeys nor inherit, so it is a legal companion to access_tags.
    let material = auth_db
        .create_api_key(ApiKeyCreate {
            principal_id: alice.id,
            note: None,
            scopes: ScopeSet::for_role("user"),
            expiration_time: None,
            access_tags: Some(vec!["team-a".to_string()]),
        })
        .await
        .unwrap();
    let key = material.secret.clone();

    let policy = TagBasedPolicy::new(Arc::new(auth_db.clone()), ScopeSet::full());
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = Arc::new(policy);
    let catalog_handle = catalog.clone();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(tiled_rs::catalog::adapter::UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let dummy = DummyAuthenticator::new("dummy");

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
        access_policy: Some(access_policy),
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
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
    (tiled_rs::server::build_app(state), key, catalog_handle)
}

async fn post_create(
    app: &axum::Router,
    key: &str,
    id: &str,
    tags: serde_json::Value,
) -> StatusCode {
    let body = json!({
        "structure_family": "container",
        "id": id,
        "access_blob": {"tags": tags},
    });
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/metadata/")
        .header("content-type", "application/json")
        .header("authorization", format!("Apikey {key}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// init_node write site: a key restricted to `team-a` must be REJECTED when it
/// tries to create a node tagged `team-b` (even though alice owns team-b), and
/// the node must NOT be persisted. The same key creating a `team-a` node
/// succeeds — proving the restriction narrows rather than blanket-denies.
#[tokio::test]
async fn restricted_key_cannot_create_out_of_tag_node() {
    let (app, key, catalog) = build().await;

    // team-b: outside the key's restriction → rejected (403), not persisted.
    // Upstream catches `init_node`'s rejection and raises HTTP_403_FORBIDDEN
    // (router.py:1896-1899), not 422.
    let status = post_create(&app, &key, "child_b", json!(["team-b"])).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a [team-a]-restricted key must NOT create a team-b node"
    );
    assert!(
        catalog
            .lookup(&["child_b".to_string()])
            .await
            .unwrap()
            .is_none(),
        "the rejected team-b node must NOT be persisted"
    );

    // team-a: inside the restriction → created (positive control).
    let status = post_create(&app, &key, "child_a", json!(["team-a"])).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a [team-a]-restricted key MUST still create a team-a node"
    );
    assert!(
        catalog
            .lookup(&["child_a".to_string()])
            .await
            .unwrap()
            .is_some(),
        "the permitted team-a node must be persisted"
    );
}

/// modify_node write site: a key restricted to `team-a` must be REJECTED when a
/// PATCH tries to add the out-of-restriction `team-b` tag to an existing node.
#[tokio::test]
async fn restricted_key_cannot_add_out_of_tag_via_patch() {
    let (app, key, catalog) = build().await;

    // Seed a node tagged team-a directly (bypassing the policy) so it exists.
    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "shared".to_string(),
                structure_family: "container".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({"tags": ["team-a"]}),
            },
        )
        .await
        .unwrap();

    // PATCH proposing {tags: [team-a, team-b]} — adds team-b, outside the key.
    let body = json!({
        "content-type": "application/merge-patch+json",
        "access_blob": {"tags": ["team-a", "team-b"]},
    });
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/api/v1/metadata/shared")
        .header("content-type", "application/json")
        .header("authorization", format!("Apikey {key}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let status = app.clone().oneshot(req).await.unwrap().status();
    // Upstream catches `modify_node`'s rejection and raises HTTP_403_FORBIDDEN
    // (router.py:2401-2403), not 422.
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a [team-a]-restricted key must NOT add a team-b tag via PATCH"
    );

    // The stored blob must remain team-a only (the rejected change never lands).
    let node = catalog
        .lookup(&["shared".to_string()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.access_blob,
        json!({"tags": ["team-a"]}),
        "the rejected PATCH must not alter the stored access_blob"
    );
}
