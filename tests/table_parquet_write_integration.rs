//! End-to-end test for the table parquet write subsystem.
//!
//! Gated on `parquet-adapter` (parquet is an opt-in adapter, like zarr): run
//! with `cargo nextest run -p tiled-server --features parquet-adapter`. Drives
//! the full managed-write pipeline over HTTP against a SQLite catalog + the
//! real `FileLeafResolver` with a configured writable-storage root:
//! `POST /metadata` (server `init_storage_parquet` lays an empty skeleton) →
//! `PUT /table/full` (write an Arrow IPC body) → `GET /table/full?format=json`
//! (read it back). The mimetype is omitted to prove the parity default for
//! table nodes is `application/x-parquet` when the parquet writer is built in.
#![cfg(feature = "parquet-adapter")]

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

fn arrow_ipc_file(schema: &SchemaRef, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

#[tokio::test]
async fn managed_parquet_default_mimetype_write_then_read_json() {
    let (app, writable_dir, _db_dir) = build_write_app().await;

    // Omit the mimetype: with the parquet writer built in, the table parity
    // default is application/x-parquet.
    let ds = DataSource {
        structure_family: StructureFamily::Table,
        structure: Some(AnyStructure::Table(TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["x".into(), "y".into()],
            resizable: Default::default(),
        })),
        id: None,
        mimetype: None,
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    let (status, body) = send(
        &app,
        Request::builder()
            .method(Method::POST)
            .uri("/api/v1/metadata/")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_vec(&serde_json::json!({
                    "key": "t", "structure_family": "table",
                    "metadata": {}, "specs": [], "data_sources": [ds],
                }))
                .unwrap(),
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

    // The server laid a parquet FILE under writable storage (server-chosen).
    assert!(
        writable_dir.path().join("t.parquet").is_file(),
        "parquet skeleton not created under writable storage"
    );

    // PUT the whole table as Arrow IPC.
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, false),
    ]));
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
