//! Integration tests for walking a table node by column name — the Rust port
//! of upstream tiled's behavior where a table/dataframe column is addressable as
//! a child array node (`TableAdapter.__getitem__` → `ArrayAdapter.from_array`,
//! adapters/table.py:137-146; the zarr router lists columns as group members,
//! server/zarr.py:209-214).
//!
//! Drives the real HTTP surface (tower `oneshot`) against an in-memory
//! `MapAdapter` tree holding a two-partition Arrow-IPC table with columns
//! `x: Int64`, `y: Float64` (one null), `flag: Boolean`. Covers the metadata
//! route, `/array/full`, `/array/block`, the `/zarr/v2` + `/zarr/v3` column
//! reads, and 404 for a nonexistent column.

#![cfg(feature = "arrow-ipc")]

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrowIpcAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};

/// Write one Arrow IPC file (one partition) with the fixed 3-column schema.
fn write_partition(path: &std::path::Path, xs: Vec<i64>, ys: Vec<Option<f64>>, flags: Vec<bool>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(xs)),
            Arc::new(Float64Array::from(ys)),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = FileWriter::try_new(f, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// Build an app whose root holds a single two-partition table `some_table`
/// (rows [1,2] then [3]). Returns the app plus the tempdir (kept alive so the
/// backing `.arrow` files survive for the request).
fn build_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let p0 = dir.path().join("p0.arrow");
    let p1 = dir.path().join("p1.arrow");
    write_partition(&p0, vec![1, 2], vec![Some(1.5), None], vec![true, false]);
    write_partition(&p1, vec![3], vec![Some(3.5)], vec![true]);

    let table = ArrowIpcAdapter::from_paths(vec![p0, p1], serde_json::json!({})).unwrap();
    let mut mapping = IndexMap::new();
    mapping.insert("some_table".to_string(), AnyAdapter::Table(Arc::new(table)));
    let root: Arc<dyn ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));

    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: None,
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
        enable_web: false,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (tiled_rs::server::build_app(state), dir)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
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
    (status, bytes.to_vec())
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, bytes) = get(app, uri).await;
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

fn i64s(bytes: &[u8]) -> Vec<i64> {
    bytes
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}
fn f64s(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

// ---------------------------------------------------------------------------
// metadata route
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_of_column_is_an_array_with_schema_dtype_and_shape() {
    let (app, _dir) = build_app();

    // Int64 column `x`.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_table/x").await;
    assert_eq!(status, StatusCode::OK);
    let attrs = &body["data"]["attributes"];
    assert_eq!(attrs["structure_family"], "array");
    assert_eq!(body["data"]["id"], "x");
    // ancestors = ["some_table"] (the table node).
    assert_eq!(
        attrs["ancestors"],
        serde_json::json!(["some_table"]),
        "column node's ancestor is the table"
    );
    let structure = &attrs["structure"];
    assert_eq!(structure["shape"], serde_json::json!([3]), "3 rows");
    assert_eq!(
        structure["chunks"],
        serde_json::json!([[3]]),
        "single-chunk column"
    );
    // dtype comes from the Arrow schema: signed 8-byte integer.
    assert_eq!(structure["data_type"]["kind"], "i");
    assert_eq!(structure["data_type"]["itemsize"], 8);

    // Float64 column `y`.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_table/y").await;
    assert_eq!(status, StatusCode::OK);
    let structure = &body["data"]["attributes"]["structure"];
    assert_eq!(structure["data_type"]["kind"], "f");
    assert_eq!(structure["data_type"]["itemsize"], 8);
    assert_eq!(structure["shape"], serde_json::json!([3]));
}

#[tokio::test]
async fn metadata_of_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/api/v1/metadata/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /array/full + /array/block
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_full_reads_column_across_partitions() {
    let (app, _dir) = build_app();

    // Int64 column concatenated across both partitions → [1, 2, 3].
    let (status, body) = get(&app, "/api/v1/array/full/some_table/x").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);

    // Float64 column: the null slot in partition 0 becomes NaN.
    let (status, body) = get(&app, "/api/v1/array/full/some_table/y").await;
    assert_eq!(status, StatusCode::OK);
    let ys = f64s(&body);
    assert_eq!(ys[0], 1.5);
    assert!(ys[1].is_nan(), "arrow null → NaN");
    assert_eq!(ys[2], 3.5);
}

#[tokio::test]
async fn array_block_reads_column_chunk() {
    let (app, _dir) = build_app();
    // The whole column is a single chunk, so block 0 returns every row.
    let (status, body) = get(&app, "/api/v1/array/block/some_table/x?block=0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);
}

#[tokio::test]
async fn array_full_of_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/api/v1/array/full/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zarr_v2_table_lists_columns_and_serves_them() {
    let (app, _dir) = build_app();

    // The table is a zarr group.
    let (status, body) = get_json(&app, "/zarr/v2/some_table/.zgroup").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"zarr_format": 2}));

    // The group listing enumerates the column names as child URLs.
    let (status, body) = get_json(&app, "/zarr/v2/some_table").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert_eq!(urls.len(), 3);
    assert!(urls[0].ends_with("/zarr/v2/some_table/x"), "{urls:?}");
    assert!(urls[1].ends_with("/zarr/v2/some_table/y"), "{urls:?}");
    assert!(urls[2].ends_with("/zarr/v2/some_table/flag"), "{urls:?}");

    // A column resolves to a zarr array with the schema dtype.
    let (status, doc) = get_json(&app, "/zarr/v2/some_table/x/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<i8");
    assert_eq!(doc["shape"], serde_json::json!([3]));
    assert_eq!(doc["chunks"], serde_json::json!([3]));

    // Its chunk bytes are the column values.
    let (status, chunk) = get(&app, "/zarr/v2/some_table/x/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&chunk), vec![1, 2, 3]);
}

#[tokio::test]
async fn zarr_v2_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/zarr/v2/some_table/nope/.zarray").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zarr_v3_table_group_and_column_array() {
    let (app, _dir) = build_app();

    // The table is a v3 group.
    let (status, doc) = get_json(&app, "/zarr/v3/some_table/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["node_type"], "group");

    // The group listing enumerates the columns.
    let (status, body) = get_json(&app, "/zarr/v3/some_table").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert_eq!(urls.len(), 3);
    assert!(urls[0].ends_with("/zarr/v3/some_table/x"), "{urls:?}");

    // A column resolves to a v3 array document with the core dtype name.
    let (status, doc) = get_json(&app, "/zarr/v3/some_table/x/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["node_type"], "array");
    assert_eq!(doc["data_type"], "int64");
    assert_eq!(doc["shape"], serde_json::json!([3]));

    // Its chunk bytes (v3 key `c/0`) are the column values.
    let (status, chunk) = get(&app, "/zarr/v3/some_table/x/c/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&chunk), vec![1, 2, 3]);
}

#[tokio::test]
async fn zarr_v3_missing_column_is_404() {
    let (app, _dir) = build_app();
    let (status, _) = get(&app, "/zarr/v3/some_table/nope/zarr.json").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
