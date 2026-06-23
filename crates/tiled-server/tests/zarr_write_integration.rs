//! End-to-end test for the zarr array write subsystem.
//!
//! Gated on `zarr-adapter` (zarr is an opt-in adapter, like hdf5/tiff): run with
//! `cargo nextest run -p tiled-server --features zarr-adapter`. Drives the full
//! managed-write pipeline over HTTP against a SQLite catalog + the real
//! `FileLeafResolver` with a configured writable-storage root:
//! `POST /metadata` (server `init_storage_zarr` generates the store skeleton) →
//! `PUT /array/full` (write the data) → `GET /array/full` (read it back). Unlike
//! the npy slice, the array uses a real multi-chunk grid, so this also proves a
//! managed array is not capped at a single chunk. One case omits the mimetype to
//! exercise the parity default (array → zarr when the feature is built).
#![cfg(feature = "zarr-adapter")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_catalog::Catalog;
use tiled_core::adapters::ContainerAdapter;
use tiled_core::data_source::{DataSource, Management};
use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_core::queries::Query;
use tiled_core::structures::{AnyStructure, ArrayStructure, StructureFamily};
use tiled_server::file_resolver::FileLeafResolver;

/// Build an app whose catalog has `writable_dir` configured as writable storage
/// and whose leaf resolver reads (and writes) only under it. Returns the router
/// plus both TempDirs — keep them alive for the test (the WAL SQLite pool and
/// the zarr stores live inside them).
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

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root.clone()]));
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
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    (tiled_server::build_app(state), writable_dir, db_dir)
}

/// 1-D f64 array of length 4 on a real 2-chunk grid (`[[2, 2]]`).
fn array_f64_multichunk() -> ArrayStructure {
    ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::native(), Kind::Float, 8)),
        chunks: vec![vec![2, 2]],
        shape: vec![4],
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
async fn managed_zarr_default_mimetype_multichunk_roundtrip() {
    let (app, writable_dir, _db_dir) = build_write_app().await;

    // Omit the mimetype: the server picks the parity default for array nodes,
    // which is zarr when the zarr writer is built in.
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_multichunk())),
        id: None,
        mimetype: None,
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

    // The server created a zarr STORE DIRECTORY (not a single file) under
    // writable storage — its existence proves the path is server-chosen.
    let store = writable_dir.path().join("arr.zarr");
    assert!(
        store.is_dir(),
        "zarr store dir not created under writable storage"
    );

    // A read before any write returns the zero fill value (4 f64 zeros),
    // assembled across both chunks.
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

    // PUT the whole array (raw little-endian C-order f64 buffer); it spans both
    // chunks of the 2-chunk grid.
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

    // Read back across both chunks — octet-stream is the raw C-order buffer.
    let (status, read_back) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read-back failed");
    assert_eq!(read_back, payload, "multi-chunk round-trip mismatch");
}

#[tokio::test]
async fn zarr_write_rejects_wrong_body_length() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_multichunk())),
        id: None,
        mimetype: Some("application/x-zarr".into()),
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

#[tokio::test]
async fn block_put_writes_one_chunk_leaving_others_intact() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_multichunk())),
        id: None,
        mimetype: Some("application/x-zarr".into()),
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
    assert_eq!(status, StatusCode::CREATED, "create failed: {body}");

    // The grid is [[2, 2]]: chunk 0 = elements [0,1], chunk 1 = [2,3]. Write
    // only chunk 1; chunk 0 must stay at the zero fill.
    let chunk1: Vec<u8> = [7.0f64, 8.0].iter().flat_map(|v| v.to_le_bytes()).collect();
    let (status, _) = bytes_request(
        &app,
        Method::PUT,
        "/api/v1/array/block/arr?block=1",
        None,
        chunk1,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "block 1 write failed");

    let (_status, back) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    let floats: Vec<f64> = back
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(
        floats,
        vec![0.0, 0.0, 7.0, 8.0],
        "only chunk 1 should be set"
    );

    // Now write chunk 0; chunk 1 is preserved.
    let chunk0: Vec<u8> = [5.0f64, 6.0].iter().flat_map(|v| v.to_le_bytes()).collect();
    let (status, _) = bytes_request(
        &app,
        Method::PUT,
        "/api/v1/array/block/arr?block=0",
        None,
        chunk0,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "block 0 write failed");

    let (_status, back) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    let floats: Vec<f64> = back
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(floats, vec![5.0, 6.0, 7.0, 8.0], "both chunks now set");

    // A wrong-size block body is rejected (chunk holds 2 f64 = 16 bytes).
    let (status, _) = bytes_request(
        &app,
        Method::PUT,
        "/api/v1/array/block/arr?block=0",
        None,
        vec![0u8; 24],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn append_grows_array_and_syncs_catalog_structure() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(array_f64_multichunk())),
        id: None,
        mimetype: Some("application/x-zarr".into()),
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
    assert_eq!(status, StatusCode::CREATED, "create failed: {body}");

    // Seed the original 4 elements.
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let payload: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let (status, _) =
        bytes_request(&app, Method::PUT, "/api/v1/array/full/arr", None, payload).await;
    assert_eq!(status, StatusCode::OK, "seed write failed");

    // PATCH /array/full?append_along=0 with 2 new f64 → new length 6.
    let appended: Vec<u8> = [5.5f64, 6.5].iter().flat_map(|v| v.to_le_bytes()).collect();
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/api/v1/array/full/arr?append_along=0")
        .body(Body::from(appended))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "append failed");
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["axis"], 0);
    assert_eq!(json["new_size"], 6);

    // GET /array/full returns all 6 values (data grew on disk).
    let (status, back) = bytes_request(
        &app,
        Method::GET,
        "/api/v1/array/full/arr",
        Some("application/octet-stream"),
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read-back failed");
    let floats: Vec<f64> = back
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(floats, vec![1.5, 2.5, 3.5, 4.5, 5.5, 6.5]);

    // GET /metadata reflects the grown shape — the catalog structure was synced,
    // so a metadata read does not contradict the data read.
    let (status, meta) = json_request(
        &app,
        Method::GET,
        "/api/v1/metadata/arr",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "metadata read failed: {meta}");
    assert_eq!(
        meta["data"]["attributes"]["structure"]["shape"],
        serde_json::json!([6]),
        "catalog structure shape not synced after append"
    );
}
