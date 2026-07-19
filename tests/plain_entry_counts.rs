//! Wave-36 — the LAST count-leak residual: `/search`'s per-entry child count for
//! PLAIN (non-inline-eligible) container entries.
//!
//! Invariant (extended from PR #141): every caller-facing container count —
//! INCLUDING the per-entry `structure.count` a `/search` listing reports for
//! each plain child container — MUST be computed over the access-filtered child
//! set when an access filter applies. `CatalogAdapter::search_page` batches
//! those per-entry counts (`count_children_batch` on SQLite,
//! `count_children_or_approx` per-parent on Postgres) from the UNFILTERED node
//! table; a restricted caller listing a parent must not be told a plain child's
//! full grandchild cardinality when it may only see a subset.
//!
//! Fixture (catalog + `TagBasedPolicy`): a public `parent` whose public plain
//! child `child` has three grandchildren — `g_pub` (tagged `public`) and
//! `g_b1`/`g_b2` (tagged `team-b`). Alice is granted `team-a` only, so she may
//! see exactly one grandchild (`g_pub`). `/search/parent` must report the
//! `child` entry's `structure.count == 1` (pre-fix: 3), and a direct GET of
//! `/metadata/parent/child` must report the SAME 1 (the merged
//! `caller_facing_child_count` path), so the listing and the direct read agree.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use tiled_rs::access::{ScopeSet, TagBasedPolicy};
use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};
use tiled_rs::catalog::{Catalog, node::RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

async fn get_json(app: &axum::Router, uri: &str, bearer: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("authorization", bearer)
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
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
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

/// A "public" plain container: carries the literal "public" tag so it is
/// readable by all. (Under tag_based an untagged / empty-blob node is NOT
/// public — F3 — so a public fixture must be tagged explicitly.)
fn container(key: &str) -> RegisterRequest {
    RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob: json!({ "tags": ["public"] }),
    }
}

fn tagged_container(key: &str, tags: &[&str]) -> RegisterRequest {
    RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob: json!({ "tags": tags }),
    }
}

#[tokio::test]
async fn search_plain_entry_count_is_principal_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Size-1 warm pools sidestep the SQLite cold-start CANTOPEN flake on
    // login+write integration tests.
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();

    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    // parent (plain container, public)
    //   └── child (plain container, public)   ← a per-entry in /search/parent
    //         ├── g_pub (tagged "public")      ← alice sees
    //         ├── g_b1  (team-b)               ← hidden from alice
    //         └── g_b2  (team-b)               ← hidden from alice
    let parent = catalog
        .create_node(None, vec![], container("parent"))
        .await
        .unwrap();
    let child = catalog
        .create_node(Some(parent.id), vec!["parent".into()], container("child"))
        .await
        .unwrap();
    let child_ancestors = vec!["parent".to_string(), "child".to_string()];
    catalog
        .create_node(Some(child.id), child_ancestors.clone(), container("g_pub"))
        .await
        .unwrap();
    catalog
        .create_node(
            Some(child.id),
            child_ancestors.clone(),
            tagged_container("g_b1", &["team-b"]),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(child.id),
            child_ancestors.clone(),
            tagged_container("g_b2", &["team-b"]),
        )
        .await
        .unwrap();

    let policy = TagBasedPolicy::new(Arc::new(auth_db.clone()), ScopeSet::full());
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = Arc::new(policy);
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
        access_policy: Some(access_policy),
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: false,
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
    let app = tiled_rs::server::build_app(state);

    let token = login(&app, "alice", "wonderland").await;
    let bearer = format!("Bearer {token}");

    // `/search/parent` lists `child` as a PLAIN container entry. Its per-entry
    // `structure.count` MUST be the caller-visible grandchild count (1), NOT the
    // full cardinality (3). Pre-fix, `search_page` batched the count from the
    // unfiltered node table and reported 3 — a cardinality leak.
    let (status, body) = get_json(
        &app,
        "/api/v1/search/parent?page[offset]=0&page[limit]=100",
        &bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let child_entry = body["data"]
        .as_array()
        .expect("search data is a list")
        .iter()
        .find(|e| e["id"] == "child")
        .expect("child entry present in /search/parent");
    let search_count = &child_entry["attributes"]["structure"]["count"];
    assert_eq!(
        search_count, 1,
        "CARDINALITY LEAK: the plain child entry's count must be the caller-visible \
         grandchild count (1), not the full 3: {child_entry}"
    );

    // Consistency: a direct GET of the same plain container reports the SAME
    // principal-scoped count via the merged `caller_facing_child_count` path.
    let (meta_status, meta) = get_json(&app, "/api/v1/metadata/parent/child", &bearer).await;
    assert_eq!(meta_status, StatusCode::OK, "metadata failed: {meta}");
    let meta_count = &meta["data"]["attributes"]["structure"]["count"];
    assert_eq!(
        meta_count, 1,
        "direct GET of the plain child must report the caller-visible count (1): {meta}"
    );
    assert_eq!(
        search_count, meta_count,
        "the /search per-entry count and the direct-GET count must agree \
         (both principal-scoped): search={search_count} metadata={meta_count}"
    );
}
