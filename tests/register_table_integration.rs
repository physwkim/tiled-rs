//! End-to-end tests for the client register *engine* (`tiled_rs::client::register`)
//! against a live, SQLite-catalog-backed `tiled-server`.
//!
//! Regression for the wave-27 finding: registering a CSV or Parquet file through
//! the engine used to POST a `TableStructure` carrying only `columns` +
//! `npartitions` and **no** `arrow_schema`, which the server's
//! family-authoritative `DataSource` parse rejects with
//! `422 Cannot parse TableStructure: missing field arrow_schema`.
//!
//! Each test walks the whole flow: write a real file → register it through the
//! engine over HTTP → read the registered node's data back through the client
//! and assert both the row count and the concrete column values. The read-back
//! (not merely a 200 on POST) is what proves the registered structure carries a
//! valid, servable Arrow schema.

#![cfg(all(feature = "csv-adapter", feature = "parquet-adapter"))]

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use tokio::net::TcpListener;

use tiled_rs::catalog::adapter::LeafResolver;
use tiled_rs::catalog::{Catalog, CatalogAdapter};
use tiled_rs::client::from_uri;
use tiled_rs::client::register::{Settings, register};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::file_resolver::FileLeafResolver;

/// Build a SQLite-catalog-backed server whose `FileLeafResolver` allow-lists
/// `root_dir`, spawn it on an ephemeral TCP port, and return its base URL.
///
/// `base_url: None` so node links derive from the request host (the client must
/// be able to follow links back to the ephemeral port). No `api_key`/`auth_db`
/// ⟹ `no_auth_configured` ⟹ the anonymous caller receives the full scope set,
/// including `register`, so the register engine can POST unauthenticated.
async fn spawn_server(root_dir: std::path::PathBuf, db_path: &std::path::Path) -> String {
    let uri = format!("sqlite://{}", db_path.display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

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
        base_url: None,
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
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    base_url
}

#[tokio::test]
async fn register_csv_via_engine_serves_data() {
    let dir = tempfile::tempdir().unwrap();
    // The SQLite URL uses the RAW tempdir path (never the canonicalized one):
    // on Windows `canonicalize()` yields a verbatim `\\?\C:\...` path whose `?`
    // sqlx reads as the URL query separator. The `FileLeafResolver` allow-list
    // and the data file, by contrast, must be canonical so the read-time
    // containment check matches.
    let db_path = dir.path().join("catalog.db");
    let root_dir = dir.path().canonicalize().unwrap();
    let csv_path = root_dir.join("sales.csv");
    std::fs::write(&csv_path, "x,y\n1,1.5\n2,2.5\n3,3.5\n").unwrap();

    let base = spawn_server(root_dir, &db_path).await;

    // Register the single CSV file at the container root through the engine.
    let node = from_uri(&base).await.unwrap().into_container().unwrap();
    register(&node, &csv_path, "", &Settings::default(), false)
        .await
        .expect("registering a CSV via the engine must succeed (arrow_schema present)");

    // Read the registered node back and assert servable data, not just a 200.
    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let table = root
        .get("sales")
        .await
        .expect("registered CSV node must be listable")
        .into_table()
        .expect("registered CSV node must be a table");
    assert_eq!(table.columns(), &["x", "y"]);

    let part = table.read_partition(0, None).await.unwrap();
    let rows: usize = part.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all 3 CSV rows must be served");

    let batch = &part.batches[0];
    let x = batch
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column x must decode as Int64");
    assert_eq!(x.values(), &[1, 2, 3]);
    let y = batch
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column y must decode as Float64");
    assert_eq!(y.values(), &[1.5, 2.5, 3.5]);
}

#[tokio::test]
async fn register_parquet_via_engine_serves_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("catalog.db");
    let root_dir = dir.path().canonicalize().unwrap();
    let pq_path = root_dir.join("metrics.parquet");
    write_parquet(&pq_path);

    let base = spawn_server(root_dir, &db_path).await;

    let node = from_uri(&base).await.unwrap().into_container().unwrap();
    register(&node, &pq_path, "", &Settings::default(), false)
        .await
        .expect("registering a Parquet via the engine must succeed (arrow_schema present)");

    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let table = root
        .get("metrics")
        .await
        .expect("registered Parquet node must be listable")
        .into_table()
        .expect("registered Parquet node must be a table");
    assert_eq!(table.columns(), &["a", "b"]);

    let part = table.read_partition(0, None).await.unwrap();
    let rows: usize = part.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "all 3 Parquet rows must be served");

    let batch = &part.batches[0];
    let a = batch
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column a must decode as Int64");
    assert_eq!(a.values(), &[10, 20, 30]);
    let b = batch
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("column b must decode as Float64");
    assert_eq!(b.values(), &[1.1, 2.2, 3.3]);
}

/// Write a 3-row, two-column (`a: Int64`, `b: Float64`) single-row-group
/// Parquet file.
fn write_parquet(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
