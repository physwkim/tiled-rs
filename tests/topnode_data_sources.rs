//! Catalog CONTAINER `data_sources` on `GET /metadata/<node>?include_data_sources=true`.
//!
//! Upstream attaches `data_sources` for ANY entry whose adapter has them —
//! including a container — under `?include_data_sources=true`
//! (`tiled/server/core.py:483-484`, `catalog/adapter.py:409`). A container CAN
//! own data sources: `Catalog::create_data_source` accepts a container node id
//! (`src/catalog/data_source.rs`).
//!
//! The `/search` page already honors this: `CatalogAdapter::search_page`
//! (`src/catalog/adapter.rs`) computes `node_ds` for every page node regardless
//! of family and sets `data_sources` from it unconditionally — only `structure`
//! is family-branched. The single-node `/metadata` path
//! (`catalog_metadata_resource`) was asymmetric: its container branch returned
//! `(Some(structure), None)`, dropping a container's own data sources even when
//! requested. These tests pin `/metadata` to the `/search` behavior.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::adapter::UnresolvedLeaf;
use tiled_rs::catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

/// Register one top-level container node and return its DB id.
async fn create_container(catalog: &Catalog, key: &str) -> i64 {
    catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "container".into(),
                metadata: serde_json::json!({}),
                specs: serde_json::json!([]),
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .expect("create_node")
        .id
}

/// Catalog-backed app with two top-level containers:
/// - `withds`  — a plain container carrying ONE attached data source (one asset)
/// - `nods`    — a plain container with NO data source
///
/// Both are count-only (no `xarray_dataset` spec), so they exercise the
/// container branch of `catalog_metadata_resource` — the branch that dropped
/// `data_sources`. The `UnresolvedLeaf` resolver never resolves a leaf, proving
/// the data-source list is read straight from the DB, not by building an adapter.
async fn build_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let withds = create_container(&catalog, "withds").await;
    catalog
        .create_data_source(
            withds,
            DataSourceSpec {
                structure_family: "array".into(),
                structure: serde_json::json!({
                    "shape": [10],
                    "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                    "chunks": [[10]],
                }),
                mimetype: "application/x-hdf5".into(),
                parameters: serde_json::json!({}),
                management: "external".into(),
                assets: vec![AssetSpec {
                    data_uri: "file:///tmp/withds.h5".into(),
                    is_directory: false,
                    parameter: "data_uri".into(),
                    num: None,
                }],
            },
        )
        .await
        .expect("create_data_source");

    create_container(&catalog, "nods").await;

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

/// RED pre-fix: a container that owns a data source surfaces it under
/// `?include_data_sources=true`, exactly as the `/search` page does. Pre-fix
/// `catalog_metadata_resource` returns `data_sources: null` for any container.
#[tokio::test]
async fn container_data_sources_present_when_requested() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/withds?include_data_sources=true").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/withds must be 200: {body}"
    );

    // The node itself is a container; its structure stays count-only.
    let attrs = &body["data"]["attributes"];
    assert_eq!(
        attrs["structure_family"], "container",
        "node is a container: {body}"
    );

    let ds = &attrs["data_sources"];
    assert!(
        ds.is_array(),
        "container data_sources must be present (array) when requested, got {ds}: {body}"
    );
    let ds = ds.as_array().unwrap();
    assert_eq!(ds.len(), 1, "exactly one data source attached: {body}");
    assert_eq!(
        ds[0]["structure_family"], "array",
        "data source family preserved: {body}"
    );
    // The full data source carries its asset list (the point of the flag).
    let assets = ds[0]["assets"].as_array().expect("assets array");
    assert_eq!(assets.len(), 1, "one asset on the data source: {body}");
    assert_eq!(
        assets[0]["data_uri"], "file:///tmp/withds.h5",
        "asset data_uri round-trips: {body}"
    );
}

/// Without the flag, a container's `data_sources` stays absent (`None` →
/// `skip_serializing_if`), unchanged by the fix.
#[tokio::test]
async fn container_data_sources_absent_without_flag() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/withds").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "metadata/withds must be 200: {body}"
    );
    assert!(
        body["data"]["attributes"]["data_sources"].is_null(),
        "data_sources must be omitted without the flag: {body}"
    );
}

/// A container with NO data source returns an EMPTY list (not `null`) under the
/// flag — matching the `/search` page, which yields `Some(vec![])` for a node
/// with no sources when `include_data_sources` is set.
#[tokio::test]
async fn container_without_data_sources_is_empty_list_when_requested() {
    let (app, _dir) = build_app().await;
    let (status, body) = get_json(&app, "/api/v1/metadata/nods?include_data_sources=true").await;
    assert_eq!(status, StatusCode::OK, "metadata/nods must be 200: {body}");
    let ds = &body["data"]["attributes"]["data_sources"];
    assert!(
        ds.is_array() && ds.as_array().unwrap().is_empty(),
        "container with no data sources must be an empty list (parity with /search), got {ds}: {body}"
    );
}
