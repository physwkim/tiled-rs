//! Catalog TOP-NODE `structure.contents` inlining on `GET /metadata/<node>`.
//!
//! Wave-34 (`#133`) ported the recursive `structure.contents` inlining gate
//! (`tiled/server/core.py:511-556`) into the in-memory metadata path
//! (`construct_resource` / `build_container_structure`) and the search path
//! (`construct_entries_response`). But a **catalog-registered** container read
//! through `GET /metadata/<node>` was still served by `catalog_metadata_resource`
//! (`src/server/router.rs`), which hand-builds `NodeStructure { contents: None }`
//! from a child count and never routes an inline-enabled container through the
//! shared inlining owner. Upstream has NO such asymmetry — `/metadata` inlines
//! the addressed (top) node under the same `core.py:513` gate.
//!
//! The asymmetry these tests pin down: an `xarray_dataset`-tagged container
//! served from the SQL catalog returns `structure.contents == null`, whereas the
//! byte-equivalent in-memory node inlines its children (proven by
//! `tests/max_depth_inlining.rs::metadata_max_depth_none_inlines_recursively`).
//! After the fix the catalog top node inlines under the identical gate
//! (`max_depth`, the 500-child cap, child-Resource shape), while a plain
//! (unspec'd) container keeps the count-only fast path and never resolves a
//! child adapter.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::adapter::UnresolvedLeaf;
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Register one node under `parent_id` and return its DB id. `specs` is the raw
/// stored JSON array (`[]` = plain container, `[{"name":"xarray_dataset"}]` =
/// inline-enabled).
async fn create(
    catalog: &Catalog,
    parent_id: Option<i64>,
    ancestors: Vec<String>,
    key: &str,
    structure_family: &str,
    specs: serde_json::Value,
) -> i64 {
    catalog
        .create_node(
            parent_id,
            ancestors,
            RegisterRequest {
                key: key.into(),
                structure_family: structure_family.into(),
                metadata: serde_json::json!({}),
                specs,
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .expect("create_node")
        .id
}

/// Catalog-backed app with the fixture tree:
/// ```text
/// root
/// ├── ds        (container, spec "xarray_dataset")  → inline-enabled
/// │   ├── x     (empty container)
/// │   └── y     (empty container)
/// ├── plain     (container, no spec)                → count-only fast path
/// │   └── a     (empty container)
/// └── plainleaf (container, no spec)                → count-only fast path
///     └── arr   (array leaf, UnresolvedLeaf)         → must NOT be resolved
/// ```
/// The `UnresolvedLeaf` resolver errors on any leaf resolution, so `plainleaf`
/// staying 200 (contents=null) proves the fast path builds no child adapter.
/// `ds`'s children are containers, resolvable without a leaf resolver.
async fn build_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let ds = create(
        &catalog,
        None,
        vec![],
        "ds",
        "container",
        serde_json::json!([{"name": "xarray_dataset"}]),
    )
    .await;
    create(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "x",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(ds),
        vec!["ds".into()],
        "y",
        "container",
        serde_json::json!([]),
    )
    .await;

    let plain = create(
        &catalog,
        None,
        vec![],
        "plain",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(plain),
        vec!["plain".into()],
        "a",
        "container",
        serde_json::json!([]),
    )
    .await;

    let plainleaf = create(
        &catalog,
        None,
        vec![],
        "plainleaf",
        "container",
        serde_json::json!([]),
    )
    .await;
    create(
        &catalog,
        Some(plainleaf),
        vec!["plainleaf".into()],
        "arr",
        "array",
        serde_json::json!([]),
    )
    .await;

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
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
    (tiled_rs::server::build_app(state), dir)
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// RED pre-fix: an `xarray_dataset` catalog container inlines its children into
/// `structure.contents` on `GET /metadata/<node>`, exactly as the in-memory node
/// does. Pre-fix `catalog_metadata_resource` returns `contents: null`.
#[tokio::test]
async fn catalog_xarray_dataset_topnode_inlines() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/ds").await;
    assert_eq!(status, StatusCode::OK, "metadata/ds must be 200: {body}");
    let s = &body["data"]["attributes"]["structure"];
    assert_eq!(s["count"], 2, "ds has two children");
    assert!(
        s["contents"].is_object(),
        "xarray_dataset top node must inline its children (got contents={}): {s}",
        s["contents"]
    );
    let contents = s["contents"].as_object().unwrap();
    assert!(
        contents.contains_key("x") && contents.contains_key("y"),
        "both children inlined: {s}"
    );
    // Child-Resource shape parity with the in-memory `construct_resource` path.
    assert_eq!(s["contents"]["x"]["id"], "x");
    assert_eq!(
        s["contents"]["x"]["attributes"]["structure_family"],
        "container"
    );
    // `x` is an empty plain container (depth 1): its own contents stay null,
    // count 0 — the recursion respects the per-node gate.
    assert!(
        s["contents"]["x"]["attributes"]["structure"]["contents"].is_null(),
        "empty plain child must not inline: {s}"
    );
    assert_eq!(s["contents"]["x"]["attributes"]["structure"]["count"], 0);
}

/// A plain (unspec'd) catalog container keeps `contents: null` — the count-only
/// fast path — both before and after the fix.
#[tokio::test]
async fn catalog_plain_container_contents_null() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/plain").await;
    assert_eq!(status, StatusCode::OK);
    let s = &body["data"]["attributes"]["structure"];
    assert!(
        s["contents"].is_null(),
        "plain container must not inline: {s}"
    );
    assert_eq!(s["count"], 1, "plain container count");
}

/// `?max_depth=0` disables inlining even on an inline-enabled catalog node
/// (`0 < 0` is false), mirroring the in-memory gate.
#[tokio::test]
async fn catalog_topnode_max_depth_zero_no_inline() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/ds?max_depth=0").await;
    assert_eq!(status, StatusCode::OK);
    let s = &body["data"]["attributes"]["structure"];
    assert!(s["contents"].is_null(), "max_depth=0 must not inline: {s}");
    assert_eq!(s["count"], 2, "count still reported");
}

/// The fast path builds no child adapters: `plainleaf` holds an `UnresolvedLeaf`
/// array child that errors on resolution. A count-only response stays 200 with
/// `contents: null`; had the request resolved the child it would surface the
/// resolver error instead. Guards the "plain containers pay nothing new"
/// invariant observably.
#[tokio::test]
async fn catalog_plain_container_fast_path_no_child_build() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/plainleaf").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fast path must not resolve the unresolvable leaf child: {body}"
    );
    let s = &body["data"]["attributes"]["structure"];
    assert!(
        s["contents"].is_null(),
        "plain container stays non-inlined: {s}"
    );
    assert_eq!(s["count"], 1);
}
