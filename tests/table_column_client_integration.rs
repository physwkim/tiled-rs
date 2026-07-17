//! Client-side table-column access parity: the tiled-rs CLIENT addressing a
//! table column the way upstream's `DataFrameClient.__getitem__` does
//! (`client["table"].get_column("col")` → an array client).
//!
//! Drives the real client stack (`from_uri` → `ContainerClient::get` →
//! `TableClient::get_column`) against a live in-process `tiled-server` on an
//! ephemeral TCP port, whose root holds a two-partition Arrow-IPC table
//! (`x: Int64`, `y: Float64` with one null, `flag: Boolean`). Covers fetching a
//! column's metadata, reading the full array, reading one block, and the
//! missing-column error.

#![cfg(feature = "arrow-ipc")]

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use indexmap::IndexMap;
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrowIpcAdapter, MapAdapter};
use tiled_rs::client::from_uri;
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::DType;
use tiled_rs::core::structures::StructureFamily;

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

/// Build a root holding a single two-partition table `some_table`
/// (rows [1,2] then [3]). Returns the root plus the tempdir (kept alive so the
/// backing `.arrow` files survive for the duration of the requests).
fn build_table_root() -> (Arc<dyn ContainerAdapter>, tempfile::TempDir) {
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
    (root, dir)
}

/// Spawn `tiled-server` over `root_tree` on an ephemeral port; return base URL.
async fn spawn_server(root_tree: Arc<dyn ContainerAdapter>) -> String {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: vec![],
        base_url: None,
        root_path: String::new(),
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

fn i64s(bytes: &[u8]) -> Vec<i64> {
    bytes
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// Navigate root → table client for the shared fixture.
async fn table_client(base: &str) -> tiled_rs::client::TableClient {
    let root = from_uri(base).await.unwrap().into_container().unwrap();
    root.get("some_table").await.unwrap().into_table().unwrap()
}

#[tokio::test]
async fn get_column_returns_array_client_with_column_metadata() {
    let (root, _dir) = build_table_root();
    let base = spawn_server(root).await;
    let table = table_client(&base).await;
    assert_eq!(table.columns(), &["x", "y", "flag"]);

    // Int64 column `x` resolves to an array node with the schema dtype/shape.
    let col = table.get_column("x").await.unwrap();
    assert_eq!(col.structure_family(), StructureFamily::Array);
    let arr = col.into_array().unwrap();
    assert_eq!(arr.shape(), &[3], "3 rows across both partitions");
    assert_eq!(arr.chunks(), &[vec![3]], "single-chunk column");
    match &arr.structure().data_type {
        DType::Builtin(b) => assert_eq!(b.to_numpy_str(), "<i8"),
        other => panic!("expected builtin int64 dtype, got {other:?}"),
    }
}

#[tokio::test]
async fn get_column_read_full_array_concatenates_partitions() {
    let (root, _dir) = build_table_root();
    let base = spawn_server(root).await;
    let table = table_client(&base).await;

    // Int64 column concatenated across both partitions → [1, 2, 3].
    let arr = table.get_column("x").await.unwrap().into_array().unwrap();
    let blocks = arr.read().await.unwrap();
    assert_eq!(blocks.len(), 1, "single chunk");
    assert_eq!(blocks[0].shape, vec![3]);
    assert_eq!(i64s(&blocks[0].data), vec![1, 2, 3]);

    // Float64 column: the null slot in partition 0 becomes NaN.
    let ycol = table.get_column("y").await.unwrap().into_array().unwrap();
    let yblocks = ycol.read().await.unwrap();
    let ys: Vec<f64> = yblocks[0]
        .data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(ys[0], 1.5);
    assert!(ys[1].is_nan(), "arrow null → NaN");
    assert_eq!(ys[2], 3.5);
}

#[tokio::test]
async fn get_column_read_block_reads_one_chunk() {
    let (root, _dir) = build_table_root();
    let base = spawn_server(root).await;
    let table = table_client(&base).await;

    // The whole column is a single chunk, so block [0] returns every row.
    let arr = table.get_column("x").await.unwrap().into_array().unwrap();
    let block = arr.read_block(&[0]).await.unwrap();
    assert_eq!(block.shape, vec![3]);
    assert_eq!(i64s(&block.data), vec![1, 2, 3]);
}

#[tokio::test]
async fn get_column_missing_column_is_key_not_found() {
    let (root, _dir) = build_table_root();
    let base = spawn_server(root).await;
    let table = table_client(&base).await;

    let err = table.get_column("nope").await.unwrap_err();
    assert!(
        matches!(err, tiled_rs::client::ClientError::KeyNotFound(_)),
        "missing column must be KeyNotFound (upstream KeyError), got {err:?}"
    );
}
