//! End-to-end tests: spin up `tiled-server` on a real TCP port, connect with
//! `tiled-client`, exercise navigation + array + table reads + writes.

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

use bytes::Bytes;
use tiled_adapters::{ArrayAdapter, CooAdapter, MapAdapter};
use tiled_client::{AnyClient, from_uri};
use tiled_core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_core::queries::Query;

fn build_root() -> MapAdapter {
    let mut mapping = IndexMap::new();

    // 1D float64 array.
    let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"element": "Cu"}));
    mapping.insert("some_array".into(), AnyAdapter::Array(Arc::new(arr)));

    // Nested container.
    let mut inner = IndexMap::new();
    let inner_data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let inner_arr = ArrayAdapter::from_f64_1d(&inner_data, serde_json::json!({}));
    inner.insert("nested_arr".into(), AnyAdapter::Array(Arc::new(inner_arr)));
    let inner_container = MapAdapter::new(inner, serde_json::json!({"nested": true}), vec![]);
    mapping.insert(
        "subgroup".into(),
        AnyAdapter::Container(Arc::new(inner_container)),
    );

    // Multi-block COO sparse array: dense shape [4,4] on a 2x2 chunk grid.
    // block [0,0] local (1,1)=10.0 → global (1,1); block [1,1] local (0,0)=20.0
    // → global (2,2). A full read must surface BOTH; read_block([0,0]) sees only
    // the first. Exercises SparseClient::read() across blocks.
    let coo = CooAdapter::from_blocks(
        vec![4, 4],
        vec![vec![2, 2], vec![2, 2]],
        BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        None,
        serde_json::json!({"kind": "coo"}),
        vec![],
        vec![
            (
                vec![0, 0],
                vec![vec![1], vec![1]],
                Bytes::from(10.0f64.to_le_bytes().to_vec()),
            ),
            (
                vec![1, 1],
                vec![vec![0], vec![0]],
                Bytes::from(20.0f64.to_le_bytes().to_vec()),
            ),
        ],
    )
    .expect("build multi-block COO");
    mapping.insert("some_sparse".into(), AnyAdapter::Sparse(Arc::new(coo)));

    MapAdapter::new(
        mapping,
        serde_json::json!({"description": "test catalog"}),
        vec![],
    )
}

/// Spawn the server on an ephemeral port and return its base URL.
async fn spawn_server(api_key: Option<String>) -> String {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_root());
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key,
        catalog: None,
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
        exact_count_limit: u64::MAX,
    };

    let app = tiled_server::build_app(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a tick to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    base_url
}

#[tokio::test]
async fn from_uri_returns_root_container() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.expect("from_uri");
    let root = client.into_container().expect("root is container");
    let keys = root.keys().await.expect("list keys");
    assert!(keys.contains(&"some_array".to_string()));
    assert!(keys.contains(&"subgroup".to_string()));
}

#[tokio::test]
async fn navigate_into_subgroup_and_read_metadata() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let sub = root.get("subgroup").await.unwrap();
    let sub_container = sub.into_container().unwrap();
    let meta = sub_container.base().metadata();
    assert_eq!(meta.get("nested"), Some(&serde_json::json!(true)));

    let inner_keys = sub_container.keys().await.unwrap();
    assert_eq!(inner_keys, vec!["nested_arr".to_string()]);
}

#[tokio::test]
async fn read_full_sparse_assembles_all_blocks() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let node = root.get("some_sparse").await.unwrap();
    let sparse = node.as_sparse().expect("some_sparse is a sparse node");

    let block = sparse.read().await.unwrap();
    assert_eq!(block.shape, vec![4, 4]);

    // Collect ((dim0, dim1), value) order-independently — the full read must
    // surface the non-zeros from BOTH blocks, not just block [0,0].
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got,
        vec![((1, 1), 10.0), ((2, 2), 20.0)],
        "read() must assemble non-zeros from every block of the global frame"
    );
}

#[tokio::test]
async fn read_array_block() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let arr = root.get("some_array").await.unwrap().into_array().unwrap();
    assert_eq!(arr.shape(), &[10]);
    assert_eq!(arr.size(), 10);

    let block = arr.read_block(&[0]).await.unwrap();
    // 10 f64 = 80 bytes.
    assert_eq!(block.data.len(), 80);
    assert_eq!(block.shape, vec![10]);

    // Decode bytes back into f64 to confirm content.
    let mut values = Vec::with_capacity(10);
    for chunk in block.data.chunks_exact(8) {
        let arr: [u8; 8] = chunk.try_into().unwrap();
        values.push(f64::from_le_bytes(arr));
    }
    let expected: Vec<f64> = (0..10).map(|i| i as f64).collect();
    assert_eq!(values, expected);
}

#[tokio::test]
async fn read_full_array_concatenates_blocks() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();
    let arr = root.get("some_array").await.unwrap().into_array().unwrap();

    let blocks = arr.read().await.unwrap();
    assert_eq!(blocks.len(), 1); // single chunk
    assert_eq!(blocks[0].shape, vec![10]);
}

#[tokio::test]
async fn key_not_found_returns_error() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();
    let err = root.get("does_not_exist").await.unwrap_err();
    // Server returns a non-200; client surfaces it as Server or KeyNotFound.
    let msg = format!("{err}");
    assert!(
        msg.contains("not found")
            || msg.contains("404")
            || msg.contains("validation")
            || msg.contains("400"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn auth_failure_when_api_key_required_but_missing() {
    let base = spawn_server(Some("secret123".into())).await;
    // No API key on the client side.
    let result = from_uri(&base).await;
    assert!(result.is_err(), "expected auth error, got: {result:?}");
    let err = result.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("authentication") || msg.contains("401") || msg.contains("403"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn auth_succeeds_with_correct_api_key() {
    let base = spawn_server(Some("secret123".into())).await;
    let client = tiled_client::from_uri_with_options(
        &base,
        tiled_client::ContextOptions::default().api_key("secret123"),
        false,
    )
    .await
    .expect("authenticated");
    let root = client.into_container().unwrap();
    let keys = root.keys().await.unwrap();
    assert!(keys.contains(&"some_array".to_string()));
}

#[tokio::test]
async fn server_info_about_payload() {
    let base = spawn_server(None).await;
    let (ctx, _) = tiled_client::Context::from_uri(&base).unwrap();
    let about = ctx.server_info().await.unwrap();
    assert_eq!(about.api_version, 0);
    assert!(!about.library_version.is_empty());
}

#[tokio::test]
async fn captures_tiled_csrf_cookie_from_about_response() {
    use axum::http::header::SET_COOKIE;
    use axum::response::IntoResponse;
    use axum::routing::get;

    // Spin up a tiny standalone server that just answers `/api/v1/` and
    // sets `tiled_csrf` via Set-Cookie.
    async fn about() -> impl IntoResponse {
        (
            [(SET_COOKIE, "tiled_csrf=abc123; Path=/")],
            axum::Json(serde_json::json!({
                "api_version": 0,
                "library_version": "test",
                "formats": {},
                "aliases": {},
                "queries": [],
                "authentication": {"required": false, "providers": []},
                "links": {},
                "meta": {},
            })),
        )
    }
    let app = axum::Router::new().route("/api/v1/", get(about));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let (ctx, _) = tiled_client::Context::from_uri(&format!("http://{addr}")).unwrap();
    // Trigger the about fetch.
    ctx.server_info().await.unwrap();
    assert_eq!(ctx.csrf_token().await.as_deref(), Some("abc123"));
}

#[tokio::test]
async fn match_on_any_client() {
    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let count = match client {
        AnyClient::Container(c) => c.keys().await.unwrap().len(),
        _ => panic!("expected container at root"),
    };
    assert!(count >= 2);
}

/// Regression: the client builds the upstream WebSocket path
/// `/api/v1/stream/single/{path}`. The server previously served only
/// `/api/v1/{family}/subscribe/{path}`, so the handshake 404'd and no
/// `tiled-client` could subscribe to its own `tiled-server`. Now the
/// server mirrors upstream's single family-agnostic route, so the
/// `Subscription` handshake completes.
#[tokio::test]
async fn client_subscription_connects_to_server() {
    let base = spawn_server(None).await;
    let (ctx, _) = tiled_client::Context::from_uri(&base).unwrap();
    let sub = tiled_client::stream::Subscription::new(ctx, vec!["subgroup".to_string()]);

    let stream = tokio::time::timeout(std::time::Duration::from_secs(5), sub.connect(None))
        .await
        .expect("subscription connect timed out")
        .expect("subscription handshake to /stream/single/ failed");

    // Connection is live; close it cleanly.
    stream.close().await.expect("ws close");
}

// ---------------------------------------------------------------------------
// Write-path helpers and tests (catalog-backed server, real writable storage)
// ---------------------------------------------------------------------------

/// Spin up a catalog-backed server with a writable storage directory.
/// Returns (base_url, writable_dir_handle, db_dir_handle).
async fn spawn_write_server() -> (String, tempfile::TempDir, tempfile::TempDir) {
    use tiled_catalog::Catalog;
    use tiled_server::file_resolver::FileLeafResolver;

    let db_dir = tempfile::tempdir().unwrap();
    let writable_dir = tempfile::tempdir().unwrap();
    let writable_root = writable_dir.path().canonicalize().unwrap();

    let db_uri = format!("sqlite://{}", db_dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&db_uri)
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

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
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
        exact_count_limit: u64::MAX,
    };

    let app = tiled_server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base_url, writable_dir, db_dir)
}

// ---------------------------------------------------------------------------
// Scope 1: ArrayClient::write, write_block, append
// ---------------------------------------------------------------------------

/// POST /metadata creates a managed array node; PUT /array/full writes data;
/// GET /array/full reads it back.
#[tokio::test]
async fn array_write_full_roundtrip() {
    use tiled_core::data_source::{DataSource, Management};
    use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // Create a managed 4-element f64 array via POST /metadata (client-side).
    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![4]],
        shape: vec![4],
        dims: None,
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(structure)),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        "w_arr",
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create_node");

    // Fetch the node as an ArrayClient.
    let arr = root
        .get("w_arr")
        .await
        .unwrap()
        .into_array()
        .expect("into_array");

    // Write 4 f64s.
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let payload: bytes::Bytes = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write(payload.clone()).await.expect("write");

    // Read back and verify.
    let blocks = arr.read().await.expect("read");
    assert_eq!(blocks.len(), 1);
    let data = &blocks[0].data;
    assert_eq!(data.len(), 32, "4 × 8 bytes");
    let got: Vec<f64> = data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(got, values.to_vec(), "round-trip data mismatch");
}

/// PUT /array/block writes one chunk; the block is read back correctly.
#[tokio::test]
async fn array_write_block_roundtrip() {
    use tiled_core::data_source::{DataSource, Management};
    use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![4]],
        shape: vec![4],
        dims: None,
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(structure)),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        "wb_arr",
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create_node");

    let arr = root.get("wb_arr").await.unwrap().into_array().unwrap();

    let values = [10.0f64, 20.0, 30.0, 40.0];
    let payload: bytes::Bytes = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write_block(&[0], payload).await.expect("write_block");

    let block = arr.read_block(&[0]).await.expect("read_block");
    let got: Vec<f64> = block
        .data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(got, values.to_vec());
}

// ---------------------------------------------------------------------------
// Scope 2: TableClient::write
// ---------------------------------------------------------------------------

#[tokio::test]
async fn table_write_full_roundtrip() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_core::data_source::{DataSource, Management};
    use tiled_core::structures::{AnyStructure, StructureFamily, TableStructure};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let table_structure = TableStructure {
        arrow_schema: String::new(),
        npartitions: 1,
        columns: vec!["x".into(), "y".into()],
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Table,
        structure: Some(AnyStructure::Table(table_structure)),
        id: None,
        mimetype: Some("text/csv".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        "w_tbl",
        StructureFamily::Table,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create table node");

    let tbl = root.get("w_tbl").await.unwrap().into_table().unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, false),
    ]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    tbl.write(&schema, &[batch]).await.expect("write table");

    // Read it back.
    let partitions = tbl.read(None).await.expect("read table");
    assert_eq!(partitions.len(), 1);
    let read_batch = &partitions[0].batches[0];
    let x_col = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(x_col.values(), &[1, 2, 3]);
}

// ---------------------------------------------------------------------------
// Scope 3: ContainerClient::create_container, delete_contents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn container_create_and_delete_contents() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // Create a sub-container.
    let sub = root
        .create_container("sub", serde_json::json!({"created_by": "test"}))
        .await
        .expect("create_container");
    assert_eq!(
        sub.base().metadata().get("created_by"),
        Some(&serde_json::json!("test"))
    );

    // Create two grandchildren inside it.
    sub.create_container("a", serde_json::json!({}))
        .await
        .expect("create grandchild a");
    sub.create_container("b", serde_json::json!({}))
        .await
        .expect("create grandchild b");

    let keys = sub.keys().await.unwrap();
    assert_eq!(keys.len(), 2);

    // Delete all grandchildren.
    sub.delete_contents(false).await.expect("delete_contents");
    let keys_after = sub.keys().await.unwrap();
    assert!(
        keys_after.is_empty(),
        "delete_contents must empty container"
    );
}

// ---------------------------------------------------------------------------
// Scope 4: BaseClient::delete, BaseClient::patch_metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn base_delete_removes_node() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container("to_delete", serde_json::json!({}))
        .await
        .expect("create");
    let keys_before = root.keys().await.unwrap();
    assert!(keys_before.contains(&"to_delete".to_string()));

    let node = root.get("to_delete").await.unwrap();
    node.base()
        .expect("base")
        .delete(false)
        .await
        .expect("delete");

    // Must no longer appear in the listing.
    let keys_after = root.keys().await.unwrap();
    assert!(!keys_after.contains(&"to_delete".to_string()));
}

#[tokio::test]
async fn base_patch_metadata_merge() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container("to_patch", serde_json::json!({"a": 1, "b": 2}))
        .await
        .expect("create");
    let node = root.get("to_patch").await.unwrap();
    // Merge-patch: add "c", leave "a" alone, remove "b" with null.
    node.base()
        .expect("base")
        .patch_metadata(serde_json::json!({"b": null, "c": 3}), None)
        .await
        .expect("patch_metadata");

    // Fetch fresh to confirm the server applied the patch.
    let updated = root.get("to_patch").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(meta.get("a"), Some(&serde_json::json!(1)), "a unchanged");
    assert_eq!(meta.get("b"), None, "b removed by null");
    assert_eq!(meta.get("c"), Some(&serde_json::json!(3)), "c added");
}

// ---------------------------------------------------------------------------
// Scope 5: ArrayClient::export, TableClient::export
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_export_to_file() {
    use tiled_core::data_source::{DataSource, Management};
    use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![4]],
        shape: vec![4],
        dims: None,
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(structure)),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        "exp_arr",
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .unwrap();

    let arr = root.get("exp_arr").await.unwrap().into_array().unwrap();
    let values = [1.0f64, 2.0, 3.0, 4.0];
    let payload: bytes::Bytes = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write(payload).await.unwrap();

    let dest = tempfile::NamedTempFile::new().unwrap();
    arr.export(dest.path(), "application/octet-stream")
        .await
        .expect("export");

    let exported = std::fs::read(dest.path()).unwrap();
    assert_eq!(exported.len(), 32, "4 × 8 bytes in octet-stream export");
}

#[tokio::test]
async fn table_export_to_file() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_core::data_source::{DataSource, Management};
    use tiled_core::structures::{AnyStructure, StructureFamily, TableStructure};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let table_structure = TableStructure {
        arrow_schema: String::new(),
        npartitions: 1,
        columns: vec!["x".into(), "y".into()],
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Table,
        structure: Some(AnyStructure::Table(table_structure)),
        id: None,
        mimetype: Some("text/csv".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        "exp_tbl",
        StructureFamily::Table,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .unwrap();

    let tbl = root.get("exp_tbl").await.unwrap().into_table().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Utf8, false),
    ]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["z"])),
        ],
    )
    .unwrap();
    tbl.write(&schema, &[batch]).await.unwrap();

    let dest = tempfile::NamedTempFile::new().unwrap();
    tbl.export(dest.path(), "csv").await.expect("export csv");

    let content = std::fs::read_to_string(dest.path()).unwrap();
    assert!(content.contains("x") && content.contains("y"), "csv header");
    assert!(content.contains('1') && content.contains('z'), "csv data");
}
