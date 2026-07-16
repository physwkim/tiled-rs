//! Integration tests for walking a table node by column name on a
//! **catalog-backed** server — the SQL-catalog counterpart of
//! `table_column_walk_integration.rs`.
//!
//! Upstream tiled's catalog resolver (`CatalogContainerAdapter.lookup_adapter`,
//! catalog/adapter.py:549-566) has no DB node for a `[table, column]` path, so
//! when the DB lookup misses it falls back to `adapter.get(segment)` on the
//! deepest data-source-backed node — for a table that is
//! `TableAdapter.get(column)`, the synthesized array child. This test drives the
//! real HTTP surface (tower `oneshot`) against a `CatalogAdapter` whose
//! `some_table` node resolves through the `FileLeafResolver` to a real
//! `ArrowIpcAdapter` (columns `x: Int64`, `y: Float64` with one null, `flag:
//! Boolean`; rows [1, 2, 3]). It covers the same routes as the in-memory test —
//! metadata, `/array/full`, `/array/block`, `/zarr/v2` + `/zarr/v3` column
//! reads, and 404 for a nonexistent column — proving the catalog resolver's
//! table-column fallback (`resolve_entry_catalog` + `catalog_metadata_resource`)
//! reuses the same `walk_tree` synthesis the in-memory path does.

#![cfg(feature = "arrow-ipc")]

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::adapter::LeafResolver;
use tiled_rs::catalog::data_source::{AssetSpec, DataSourceSpec};
use tiled_rs::catalog::{Catalog, CatalogAdapter, RegisterRequest};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::file_resolver::FileLeafResolver;

/// Write one Arrow IPC file with the fixed 3-column schema and rows [1, 2, 3].
fn write_table_file(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.5)])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = FileWriter::try_new(f, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// Build a catalog-backed app whose root holds a single table `some_table`,
/// registered as an external Arrow IPC file resolved through the
/// `FileLeafResolver`. Returns the app plus the tempdir (kept alive so the
/// SQLite catalog and the backing `.arrow` file survive for the request).
async fn build_catalog_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // `root_dir` (canonicalized) is used only where a *canonical* path is
    // required: the `FileLeafResolver` allow-list and the `.arrow` file it
    // resolves — `check_allowed` canonicalizes the asset path, so the allowed
    // root must be canonical too, or the containment check would mismatch.
    let root_dir = dir.path().canonicalize().unwrap();
    let table_path = root_dir.join("some_table.arrow");
    write_table_file(&table_path);

    // The SQLite URL must use the RAW tempdir path, never `root_dir`: on Windows
    // `canonicalize()` returns a verbatim `\\?\C:\...` path whose `?` sqlx reads
    // as the URL query separator, failing connect. Every other catalog test
    // builds its `sqlite://` URL from the raw `dir.path()` for the same reason.
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    // Register the table node + its external Arrow IPC data source directly
    // against the catalog (no HTTP create needed).
    let node = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: "some_table".into(),
                structure_family: "table".into(),
                metadata: serde_json::json!({}),
                specs: serde_json::json!([]),
                access_blob: serde_json::json!({}),
            },
        )
        .await
        .unwrap();
    catalog
        .create_data_source(
            node.id,
            DataSourceSpec {
                structure_family: "table".into(),
                structure: serde_json::json!({
                    "arrow_schema": "",
                    "npartitions": 1,
                    "columns": ["x", "y", "flag"],
                }),
                mimetype: "application/vnd.apache.arrow.file".into(),
                parameters: serde_json::json!({}),
                management: "external".into(),
                assets: vec![AssetSpec {
                    data_uri: tiled_rs::core::file_uri::path_to_file_uri(&table_path).unwrap(),
                    is_directory: false,
                    parameter: "data_uri".into(),
                    num: None,
                }],
            },
        )
        .await
        .unwrap();

    let resolver: Arc<dyn LeafResolver> = Arc::new(FileLeafResolver::new(vec![root_dir]));
    let root_tree: Arc<dyn ContainerAdapter> =
        Arc::new(CatalogAdapter::root(catalog.clone(), resolver));

    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".to_string()),
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
async fn catalog_metadata_of_column_is_an_array_with_schema_dtype_and_shape() {
    let (app, _dir) = build_catalog_app().await;

    // Sanity: the table node itself resolves through the catalog.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_table").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["attributes"]["structure_family"], "table");

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
async fn catalog_metadata_of_missing_column_is_404() {
    let (app, _dir) = build_catalog_app().await;
    let (status, _) = get(&app, "/api/v1/metadata/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /array/full + /array/block
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_array_full_reads_column() {
    let (app, _dir) = build_catalog_app().await;

    // Int64 column → [1, 2, 3].
    let (status, body) = get(&app, "/api/v1/array/full/some_table/x").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);

    // Float64 column: the null slot becomes NaN.
    let (status, body) = get(&app, "/api/v1/array/full/some_table/y").await;
    assert_eq!(status, StatusCode::OK);
    let ys = f64s(&body);
    assert_eq!(ys[0], 1.5);
    assert!(ys[1].is_nan(), "arrow null → NaN");
    assert_eq!(ys[2], 3.5);
}

#[tokio::test]
async fn catalog_array_block_reads_column_chunk() {
    let (app, _dir) = build_catalog_app().await;
    // The whole column is a single chunk, so block 0 returns every row.
    let (status, body) = get(&app, "/api/v1/array/block/some_table/x?block=0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(i64s(&body), vec![1, 2, 3]);
}

#[tokio::test]
async fn catalog_array_full_of_missing_column_is_404() {
    let (app, _dir) = build_catalog_app().await;
    let (status, _) = get(&app, "/api/v1/array/full/some_table/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_zarr_v2_table_lists_columns_and_serves_them() {
    let (app, _dir) = build_catalog_app().await;

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
async fn catalog_zarr_v2_missing_column_is_404() {
    let (app, _dir) = build_catalog_app().await;
    let (status, _) = get(&app, "/zarr/v2/some_table/nope/.zarray").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// /zarr/v3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_zarr_v3_table_group_and_column_array() {
    let (app, _dir) = build_catalog_app().await;

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
async fn catalog_zarr_v3_missing_column_is_404() {
    let (app, _dir) = build_catalog_app().await;
    let (status, _) = get(&app, "/zarr/v3/some_table/nope/zarr.json").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
