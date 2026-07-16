//! End-to-end test for the table (CSV) write subsystem.
//!
//! CSV is the table writer this build ships by default (`csv-adapter`), so this
//! runs under the default features — no feature gate. Drives the full
//! managed-write pipeline over HTTP against a SQLite catalog + the real
//! `FileLeafResolver` with a configured writable-storage root:
//! `POST /metadata` (server `init_storage_csv` lays a header-only skeleton) →
//! `PUT /table/full` (write an Arrow IPC body) → `GET /table/full?format=json`
//! (read it back). One case omits the mimetype to exercise the parity default
//! (table → text/csv).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
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
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    (tiled_rs::server::build_app(state), writable_dir, db_dir)
}

/// `x: Int64, y: Utf8` — the schema the managed table is created with. Used
/// only by the CSV default-mimetype test below.
#[cfg(not(feature = "parquet-adapter"))]
fn xy_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, false),
    ]))
}

/// Serialize one record batch as an Arrow IPC FILE stream — the body
/// `PUT /table/full` expects.
fn arrow_ipc_file(schema: &SchemaRef, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// A managed table create body. `arrow_schema` is a placeholder: the CSV
/// adapter re-infers the schema from the written file and never decodes the
/// stored string, and `validate_structure` only checks size — so the table
/// write/read flow does not depend on it.
fn create_table_body(mimetype: Option<&str>) -> serde_json::Value {
    let ds = DataSource {
        structure_family: StructureFamily::Table,
        structure: Some(AnyStructure::Table(TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["x".into(), "y".into()],
            resizable: Default::default(),
        })),
        id: None,
        mimetype: mimetype.map(String::from),
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

// CSV is the table default only when parquet's writer is NOT built in (with
// parquet-adapter the default flips to x-parquet, covered by the parquet e2e).
#[cfg(not(feature = "parquet-adapter"))]
#[tokio::test]
async fn managed_csv_default_mimetype_write_then_read_json() {
    let (app, writable_dir, _db_dir) = build_write_app().await;

    // Omit the mimetype: the server picks the table parity default (text/csv).
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&create_table_body(None)).unwrap(),
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

    // The server laid a CSV file under writable storage (server-chosen path).
    assert!(
        writable_dir.path().join("t.csv").is_file(),
        "csv skeleton not created under writable storage"
    );

    // PUT the whole table as Arrow IPC.
    let schema = xy_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    let (status, _) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/full/t")
            .body(Body::from(arrow_ipc_file(&schema, &batch)))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "table write failed");

    // Read back as column-dict JSON.
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::GET)
            .uri("/api/v1/table/full/t")
            .header("accept", "application/json")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read-back failed");
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["x"], serde_json::json!([1, 2, 3]), "x column mismatch");
    assert_eq!(
        json["y"],
        serde_json::json!(["a", "b", "c"]),
        "y column mismatch"
    );
}

#[tokio::test]
async fn table_write_rejects_mismatched_columns_and_non_ipc_body() {
    let (app, _writable_dir, _db_dir) = build_write_app().await;
    let (status, _) = send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&create_table_body(Some("text/csv"))).unwrap(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Body whose columns (x, z) do not match the node's (x, y) → rejected.
    let wrong_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("z", DataType::Utf8, false),
    ]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let (status, _) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/full/t")
            .body(Body::from(arrow_ipc_file(&wrong_schema, &wrong_batch)))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "column mismatch");

    // A body that is not Arrow IPC at all → rejected, not a 500.
    let (status, _) = send(
        &app,
        Request::builder()
            .method(Method::PUT)
            .uri("/api/v1/table/full/t")
            .body(Body::from(vec![0u8; 16]))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "non-IPC body");
}
