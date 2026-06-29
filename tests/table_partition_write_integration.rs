//! End-to-end tests for the table partition write routes.
//!
//! Covers:
//!   PUT  /api/v1/table/partition/{path}?partition=N  (overwrite one partition)
//!   PATCH /api/v1/table/partition/{path}?partition=N  (append rows to a partition)
//!
//! Uses the CSV backend (always present) against a SQLite catalog + real
//! FileLeafResolver with a configured writable-storage root.

// Every test here drives the CSV table backend, which is the default table
// serialization only when the parquet table backend is off. With
// `parquet-adapter` enabled (a crate default) the parquet path is the default
// and these CSV-specific assertions do not apply — gate the whole file so its
// helpers compile out alongside its tests (each test is already
// `#[cfg(not(feature = "parquet-adapter"))]`).
#![cfg(not(feature = "parquet-adapter"))]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use tiled_rs::catalog::Catalog;
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::data_source::{DataSource, Management};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::{AnyStructure, StructureFamily, TableStructure};
use tiled_rs::server::file_resolver::FileLeafResolver;

async fn build_write_app() -> (axum::Router, tempfile::TempDir, tempfile::TempDir) {
    let db_dir = tempfile::tempdir().unwrap();
    let writable_dir = tempfile::tempdir().unwrap();
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
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
    };
    (tiled_rs::server::build_app(state), writable_dir, db_dir)
}

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
    ]))
}

fn arrow_ipc_file(schema: &SchemaRef, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
    w.write(batch).unwrap();
    w.finish().unwrap();
    buf
}

fn create_table_body() -> serde_json::Value {
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
    serde_json::json!({
        "key": "t", "structure_family": "table",
        "metadata": {}, "specs": [], "data_sources": [ds],
    })
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// PUT /table/partition: overwrite partition 0, then read back via GET.
#[cfg(not(feature = "parquet-adapter"))]
#[tokio::test]
async fn put_table_partition_write_then_read() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;

    // Create the managed table node.
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&create_table_body()).unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "create failed: {}",
        String::from_utf8_lossy(&body)
    );

    // Build a batch and encode as Arrow IPC.
    let schema = int_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    // PUT partition 0.
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/partition/t?partition=0")
            .body(Body::from(arrow_ipc_file(&schema, &batch)))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PUT partition failed: {}",
        String::from_utf8_lossy(&body)
    );

    // Read back as JSON.
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::GET)
            .uri("/api/v1/table/partition/t?partition=0")
            .header("accept", "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET partition failed");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["x"], serde_json::json!([10, 20, 30]), "x mismatch");
    assert_eq!(json["y"], serde_json::json!([1, 2, 3]), "y mismatch");
}

/// PUT /table/partition out-of-range index returns 400.
#[cfg(not(feature = "parquet-adapter"))]
#[tokio::test]
async fn put_table_partition_oob_returns_400() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;

    // Create the node (npartitions=1 → only partition 0 is valid).
    send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&create_table_body()).unwrap(),
            ))
            .unwrap(),
    )
    .await;

    let schema = int_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![2i64])),
        ],
    )
    .unwrap();

    let (status, _) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/partition/t?partition=1")
            .body(Body::from(arrow_ipc_file(&schema, &batch)))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "OOB partition must be 400");
}

/// PATCH /table/partition: write then append, verify accumulated rows.
#[cfg(not(feature = "parquet-adapter"))]
#[tokio::test]
async fn patch_table_partition_append_rows() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;

    send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&create_table_body()).unwrap(),
            ))
            .unwrap(),
    )
    .await;

    let schema = int_schema();

    // Write initial rows via PUT /table/full.
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/full/t")
            .body(Body::from(arrow_ipc_file(&schema, &batch1)))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initial PUT failed: {}",
        String::from_utf8_lossy(&body)
    );

    // Append more rows via PATCH /table/partition.
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(Int64Array::from(vec![40, 50])),
        ],
    )
    .unwrap();
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::PATCH)
            .uri("/api/v1/table/partition/t?partition=0")
            .body(Body::from(arrow_ipc_file(&schema, &batch2)))
            .unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PATCH append failed: {}",
        String::from_utf8_lossy(&body)
    );

    // Read back — expect 5 rows total.
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::GET)
            .uri("/api/v1/table/partition/t?partition=0")
            .header("accept", "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GET after append failed");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["x"],
        serde_json::json!([1, 2, 3, 4, 5]),
        "x after append"
    );
    assert_eq!(
        json["y"],
        serde_json::json!([10, 20, 30, 40, 50]),
        "y after append"
    );
}
