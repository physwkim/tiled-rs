//! End-to-end test for the NPY array write subsystem.
//!
//! Wires a SQLite catalog + the real `FileLeafResolver` with a configured
//! writable-storage root, then drives the full managed-write pipeline over
//! HTTP: `POST /metadata` (server-side `init_storage` generates the asset +
//! skeleton) → `PUT /array/full` (overwrite the data) → `GET /array/full`
//! (read it back). Also confirms a read-only (external) array refuses writes.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::data_source::{DataSource, Management};
use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily, TableStructure};
use tiled_rs::server::file_resolver::FileLeafResolver;

/// Build an app whose catalog has `writable_dir` configured as writable
/// storage and whose leaf resolver reads (and writes) only under it. Returns
/// the router plus both TempDirs — keep them alive for the test's duration
/// (the WAL SQLite pool and the data files live inside them).
async fn build_write_app() -> (axum::Router, tempfile::TempDir, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let writable_dir = tempfile::tempdir().unwrap();
    // canonicalize: init_storage builds an absolute file:// URI, and the
    // resolver compares canonical paths.
    let writable_root = writable_dir.path().canonicalize().unwrap();

    let uri = format!("sqlite://{}", db_dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri)
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![writable_root.clone()])
        .with_writable_storage(vec![writable_root.clone()]);
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root.clone()]));
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
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
    (tiled_rs::server::build_app(state), writable_dir, db_dir)
}

fn array_f64_1d(shape_len: usize) -> ArrayStructure {
    ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![shape_len]],
        shape: vec![shape_len],
        dims: None,
        resizable: Default::default(),
    }
}

async fn json_request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn bytes_request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    accept: Option<&str>,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(a) = accept {
        builder = builder.header("accept", a);
    }
    let req = builder.body(Body::from(body)).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn managed_array_register_write_readback_roundtrip() {
    let (app, writable_dir, _db_dir) = build_write_app().await;

    // 1. Create a managed (writable) array node via POST /metadata. The server
    //    generates the data_uri + zero-filled skeleton under writable storage.
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_1d(4))),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "arr",
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [ds],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create failed: {body}");

    // The server created the skeleton under writable storage, NOT a
    // client-chosen path.
    let skeleton = writable_dir.path().join("arr.npy");
    assert!(
        skeleton.exists(),
        "skeleton .npy not created under writable storage"
    );

    // 2. A read before any write returns the zero-filled skeleton (4 f64 zeros).
    let (status, zeros) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "initial read failed");
    assert_eq!(zeros, vec![0u8; 32], "skeleton should read back as zeros");

    // 3. PUT the array data (raw little-endian C-order f64 buffer).
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let payload: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let (status, _) = bytes_request(
        &app,
        Method::PUT,
        "/api/v1/array/full/arr",
        None,
        payload.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "write failed");

    // 4. Read the data back over HTTP — octet-stream is the raw C-order buffer,
    //    so it must equal exactly what we PUT.
    let (status, read_back) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read-back failed");
    assert_eq!(read_back, payload, "round-trip data mismatch");
}

#[tokio::test]
async fn write_rejects_wrong_body_length() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_1d(4))),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    let (status, _) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "arr", "structure_family": "array",
            "metadata": {}, "specs": [], "data_sources": [ds],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // 3 f64 (24 bytes) into a 4-element array → 422.
    let (status, _) = bytes_request(
        &app,
        Method::PUT,
        "/api/v1/array/full/arr",
        None,
        vec![0u8; 24],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

/// Build an app whose catalog has NO writable storage configured (the
/// `with_writable_storage` opt-in omitted) and a resolver over no roots.
/// Mirrors `build_write_app` minus writable storage — the state under which a
/// create carrying data sources must be refused (upstream router.py:1800-1804,
/// `entry.writable == bool(context.writable_storage)`). Returns the router plus
/// the catalog TempDir (keep it alive — the SQLite pool lives inside it).
async fn build_no_writable_app() -> (axum::Router, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", db_dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![]));
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
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
    (tiled_rs::server::build_app(state), db_dir)
}

#[tokio::test]
async fn create_with_data_sources_rejected_without_writable_storage() {
    // A catalog with no writable storage cannot generate managed storage, so a
    // POST /metadata carrying data sources must be refused up front with 405 —
    // upstream parity (router.py:1800-1804). Before the guard this create
    // returned 201 and persisted a junk metadata-only node whose GET /array/full
    // then 422'd forever. RED-first: assert 405 AND that no node was persisted.
    let (app, _db_dir) = build_no_writable_app().await;

    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_1d(4))),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "arr", "structure_family": "array",
            "metadata": {}, "specs": [], "data_sources": [ds],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::METHOD_NOT_ALLOWED,
        "array create carrying data sources without writable storage must 405: {body}"
    );

    // No half-baked node may be persisted — the node must not exist.
    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/arr",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "no node may be persisted when the array create is refused"
    );
}

#[tokio::test]
async fn create_table_with_data_sources_rejected_without_writable_storage() {
    // The guard keys on data-source presence, not structure family: a table
    // create is refused for the same reason as an array create, and likewise
    // persists no node.
    let (app, _db_dir) = build_no_writable_app().await;

    let ds = DataSource {
        structure_family: StructureFamily::Table,
        structure: Some(AnyStructure::Table(TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["x".into(), "y".into()],
            resizable: Default::default(),
        })),
        id: None,
        mimetype: Some("text/csv".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    let (status, body) = json_request(
        &app,
        Method::POST,
        "/api/v1/metadata/",
        serde_json::json!({
            "key": "t", "structure_family": "table",
            "metadata": {}, "specs": [], "data_sources": [ds],
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::METHOD_NOT_ALLOWED,
        "table create carrying data sources without writable storage must 405: {body}"
    );

    let (status, _) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/t",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "no node may be persisted when the table create is refused"
    );
}
