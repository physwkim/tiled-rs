//! Wave-35 Finding 1 — the recursive `structure.contents` inline walk MUST route
//! child enumeration through the caller's access filter.
//!
//! Invariant: no inline path (`/search` entry inlining, `/metadata` in-memory
//! inlining, the held container/full top-node branch) may expose, under
//! `structure.contents`, a child the caller's `list_filter` would hide from
//! `/search` or a direct GET. The single owner of the walk is
//! `build_container_structure` (`server/core.rs`); it enumerates children via
//! the access-filtered listing (`container.search(&[AccessBlobFilter])`), never
//! raw `keys()`, whenever an access filter is in force. This mirrors the zarr
//! fix invariant (82a7041) for every other caller-facing child listing.
//!
//! Fixture (catalog + `TagBasedPolicy`): a container `ds` carrying the
//! `xarray_dataset` spec (so it opts into inlining) with two child containers —
//! `visible` (tagged `public`) and `secret` (tagged `team-b`). Alice is
//! granted `team-a` only. A root `/search/` returns `ds` (tagged `public`) with its
//! children inlined; `secret` must be absent from `ds`'s inlined contents,
//! exactly as it is absent from a `/search/ds` listing and a direct GET of
//! `ds/secret`.

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

fn child_container(key: &str, metadata: Value, access_blob: Value) -> RegisterRequest {
    RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata,
        specs: json!([]),
        access_blob,
    }
}

#[tokio::test]
async fn search_inline_walk_routes_through_access_filter() {
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

    // ds (xarray_dataset, "public") → { visible ("public"), secret (team-b) }
    let ds = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "ds".into(),
                structure_family: "container".into(),
                metadata: json!({"kind": "dataset"}),
                specs: json!(["xarray_dataset"]),
                access_blob: json!({"tags": ["public"]}),
            },
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(ds.id),
            vec!["ds".into()],
            child_container(
                "visible",
                json!({"role": "public-child"}),
                json!({"tags": ["public"]}),
            ),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(ds.id),
            vec!["ds".into()],
            child_container(
                "secret",
                json!({"role": "restricted-child"}),
                json!({"tags": ["team-b"]}),
            ),
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

    // Root search: `ds` is visible (tagged "public"); its children are inlined. The
    // inline walk MUST route through alice's access filter, so `visible` appears
    // and `secret` (team-b) does NOT.
    let (status, body) = get_json(
        &app,
        "/api/v1/search/?page[offset]=0&page[limit]=100",
        &bearer,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "search failed: {body}");
    let ds_entry = body["data"]
        .as_array()
        .expect("search data is a list")
        .iter()
        .find(|e| e["id"] == "ds")
        .expect("ds entry present in root search");
    let contents = &ds_entry["attributes"]["structure"]["contents"];
    assert!(
        contents.is_object(),
        "ds (xarray_dataset) must inline its children: {ds_entry}"
    );
    assert!(
        contents.get("visible").is_some(),
        "the visible child must be inlined: {contents}"
    );
    assert!(
        contents.get("secret").is_none(),
        "ACCESS LEAK: the access-filtered child `secret` must NOT be inlined: {contents}"
    );
    // Count is principal-scoped: `ds` reports only the children alice may see
    // (`visible`), NOT the full cardinality. The access-filtered `secret` is
    // absent from `contents` AND uncounted, matching upstream `len_or_approx`
    // over the `filter_for_access` view (core.py:509) and this listing's own
    // `meta.count` (already filtered).
    assert_eq!(
        ds_entry["attributes"]["structure"]["count"], 1,
        "count is the caller-visible child count (secret is hidden AND uncounted)"
    );

    // Consistency: searching INTO ds hides `secret` the same way.
    let (_, into) = get_json(
        &app,
        "/api/v1/search/ds?page[offset]=0&page[limit]=100",
        &bearer,
    )
    .await;
    let into_keys: Vec<String> = into["data"]
        .as_array()
        .expect("search data is a list")
        .iter()
        .filter_map(|e| e["id"].as_str().map(String::from))
        .collect();
    assert!(
        into_keys.contains(&"visible".to_string()) && !into_keys.contains(&"secret".to_string()),
        "search-into ds must list `visible` and hide `secret`: {into_keys:?}"
    );

    // Consistency: a direct GET of the hidden child 404s (read:metadata denied).
    let (secret_status, _) = get_json(&app, "/api/v1/metadata/ds/secret", &bearer).await;
    assert_eq!(
        secret_status,
        StatusCode::NOT_FOUND,
        "direct GET of the access-hidden child must 404, so inlining must not surface it either"
    );
}
