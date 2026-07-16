//! HTTP-level tests for the raw-asset download endpoints
//! (`GET /api/v1/asset/bytes/{*path}` and `/api/v1/asset/manifest/{*path}`),
//! mirroring Python tiled's `get_asset` / `get_asset_manifest`
//! (router.py:2570-2723).
//!
//! Status-code parity with Python:
//!   - missing/non-integer `id` query param → 422 (FastAPI required-query-param)
//!   - `expose_raw_assets = false`           → 403
//!   - node has no catalog (in-memory tree)  → 405
//!   - asset id not found under the node      → 404
//!   - every relative_path / `file://` argument error → 400
//!
//! Two deliberate deviations from the Python *implementation* (documented in
//! the handlers): the traversal guard actually rejects escapes (Python's check
//! at router.py:2645 is inverted), and the manifest returns paths RELATIVE to
//! the asset directory (Python emits absolute paths, which its own
//! `relative_path` consumer then rejects).

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::catalog::adapter::UnresolvedLeaf;
use tiled_rs::catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;

fn make_state(
    root_tree: Arc<dyn ContainerAdapter>,
    catalog: Option<Catalog>,
    expose_raw_assets: bool,
) -> tiled_rs::server::AppState {
    tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog,
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
        streaming_bus: tiled_rs::server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    }
}

/// Catalog-backed app (the only mode that supports asset_by_id).
async fn catalog_app(expose_raw_assets: bool) -> (axum::Router, Catalog, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));
    let state = make_state(root_tree, Some(catalog.clone()), expose_raw_assets);
    (tiled_rs::server::build_app(state), catalog, dir)
}

/// In-memory tree app (no catalog → adapters carry no `asset_by_id`).
fn no_catalog_app() -> axum::Router {
    use indexmap::IndexMap;
    use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
    use tiled_rs::core::adapters::AnyAdapter;

    let mut mapping = IndexMap::new();
    let arr = ArrayAdapter::from_f64_1d(&[1.0, 2.0, 3.0], json!({}));
    mapping.insert("some_array".to_string(), AnyAdapter::Array(Arc::new(arr)));
    let root: Arc<dyn ContainerAdapter> = Arc::new(MapAdapter::new(mapping, json!({}), vec![]));
    tiled_rs::server::build_app(make_state(root, None, true))
}

/// Register a root array node `key` carrying one external asset at `data_uri`.
/// Returns the asset id (to pass as `?id=`).
async fn add_asset(cat: &Catalog, key: &str, data_uri: &str, is_directory: bool) -> i64 {
    let node = cat
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.into(),
                structure_family: "array".into(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({}),
            },
        )
        .await
        .unwrap();
    let ds = cat
        .create_data_source(
            node.id,
            DataSourceSpec {
                structure_family: "array".into(),
                structure: json!({
                    "shape": [10],
                    "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                    "chunks": [[10]],
                }),
                mimetype: "application/x-hdf5".into(),
                parameters: json!({}),
                management: "external".into(),
                assets: vec![AssetSpec {
                    data_uri: data_uri.into(),
                    is_directory,
                    parameter: "data_uri".into(),
                    num: None,
                }],
            },
        )
        .await
        .unwrap();
    cat.list_assets(ds.id).await.unwrap().remove(0).id
}

fn file_uri(path: &std::path::Path) -> String {
    tiled_rs::core::file_uri::path_to_file_uri(path).unwrap()
}

async fn get_parts(app: &axum::Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, body)
}

async fn status_of(app: &axum::Router, uri: &str) -> StatusCode {
    get_parts(app, uri).await.0
}

#[tokio::test]
async fn file_asset_download_returns_bytes_with_attachment() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.bin");
    let payload = b"\x00\x01\x02\x03hello-asset";
    std::fs::write(&file, payload).unwrap();

    let id = add_asset(&cat, "frame", &file_uri(&file), false).await;
    let (status, headers, body) =
        get_parts(&app, &format!("/api/v1/asset/bytes/frame?id={id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream"
    );
    assert_eq!(headers.get("accept-ranges").unwrap(), "bytes");
    assert_eq!(
        headers
            .get(header::CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap(),
        "attachment; filename=\"data.bin\""
    );
    assert_eq!(body.as_ref(), payload);
}

#[tokio::test]
async fn file_asset_range_request_returns_partial_content() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.bin");
    std::fs::write(&file, b"0123456789").unwrap();
    let id = add_asset(&cat, "frame", &file_uri(&file), false).await;

    let req = Request::builder()
        .uri(format!("/api/v1/asset/bytes/frame?id={id}"))
        .header(header::RANGE, "bytes=2-5")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let cr = resp
        .headers()
        .get(header::CONTENT_RANGE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(cr, "bytes 2-5/10");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"2345");
}

#[tokio::test]
async fn file_asset_rejects_relative_path() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.bin");
    std::fs::write(&file, b"x").unwrap();
    let id = add_asset(&cat, "frame", &file_uri(&file), false).await;

    // A single-file asset must not receive a relative_path (Python 400).
    let status = status_of(
        &app,
        &format!("/api/v1/asset/bytes/frame?id={id}&relative_path=foo"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn directory_asset_requires_relative_path() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"AAA").unwrap();
    let id = add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    // A directory asset with no relative_path (Python 400).
    let status = status_of(&app, &format!("/api/v1/asset/bytes/frames?id={id}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn directory_asset_rejects_absolute_relative_path() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"AAA").unwrap();
    let id = add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    let status = status_of(
        &app,
        &format!("/api/v1/asset/bytes/frames?id={id}&relative_path=/etc/passwd"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn directory_asset_rejects_traversal_escape() {
    // Asset dir is a subdir; a sibling file sits in the parent. A
    // `../escape.txt` relative_path resolves outside the asset dir and must be
    // rejected (the inverted Python guard is corrected here).
    let (app, cat, _db) = catalog_app(true).await;
    let root = tempfile::tempdir().unwrap();
    let asset_dir = root.path().join("asset_dir");
    std::fs::create_dir(&asset_dir).unwrap();
    std::fs::write(asset_dir.join("inside.txt"), b"inside").unwrap();
    std::fs::write(root.path().join("escape.txt"), b"SECRET").unwrap();
    let id = add_asset(&cat, "frames", &file_uri(&asset_dir), true).await;

    let (status, _, body) = get_parts(
        &app,
        &format!("/api/v1/asset/bytes/frames?id={id}&relative_path=../escape.txt"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // The escaping file's bytes must NOT be served.
    assert_ne!(body.as_ref(), b"SECRET");
}

#[tokio::test]
async fn directory_asset_manifest_lists_relative_paths_then_downloads() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"AAA").unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub").join("b.txt"), b"BBBB").unwrap();
    let id = add_asset(&cat, "frames", &file_uri(dir.path()), true).await;

    // Manifest lists files as forward-slash paths RELATIVE to the asset dir.
    let (status, _, body) =
        get_parts(&app, &format!("/api/v1/asset/manifest/frames?id={id}")).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["manifest"], json!(["a.txt", "sub/b.txt"]));

    // Each manifest entry feeds straight back as relative_path to /asset/bytes.
    let (status, _, body) = get_parts(
        &app,
        &format!("/api/v1/asset/bytes/frames?id={id}&relative_path=sub/b.txt"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"BBBB");
}

#[tokio::test]
async fn manifest_on_file_asset_is_400() {
    let (app, cat, _db) = catalog_app(true).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.bin");
    std::fs::write(&file, b"x").unwrap();
    let id = add_asset(&cat, "frame", &file_uri(&file), false).await;

    let status = status_of(&app, &format!("/api/v1/asset/manifest/frame?id={id}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn wrong_asset_id_is_404() {
    let (app, cat, _db) = catalog_app(true).await;
    add_asset(&cat, "frame", "file:///tmp/whatever.h5", false).await;

    let status = status_of(&app, "/api/v1/asset/bytes/frame?id=999999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn asset_not_downloadable_via_other_node_path() {
    // Node scoping: node "b" must not be able to download node "a"'s asset by
    // passing a's asset id to b's path, even though that id exists.
    let (app, cat, _db) = catalog_app(true).await;
    let id_a = add_asset(&cat, "a", "file:///tmp/a.h5", false).await;
    add_asset(&cat, "b", "file:///tmp/b.h5", false).await;

    let status = status_of(&app, &format!("/api/v1/asset/bytes/b?id={id_a}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn missing_id_param_is_422() {
    let (app, cat, _db) = catalog_app(true).await;
    add_asset(&cat, "frame", "file:///tmp/whatever.h5", false).await;

    let status = status_of(&app, "/api/v1/asset/bytes/frame").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn expose_raw_assets_false_is_403() {
    let (app, cat, _db) = catalog_app(false).await;
    let id = add_asset(&cat, "frame", "file:///tmp/whatever.h5", false).await;

    let status = status_of(&app, &format!("/api/v1/asset/bytes/frame?id={id}")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let status = status_of(&app, &format!("/api/v1/asset/manifest/frame?id={id}")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn no_catalog_is_405() {
    // The in-memory tree adapters have no `asset_by_id` (Python's `hasattr`
    // check → 405). The node resolves, so this is not a 404.
    let app = no_catalog_app();
    let status = status_of(&app, "/api/v1/asset/bytes/some_array?id=1").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}
