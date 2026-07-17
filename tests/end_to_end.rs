//! End-to-end tests: spin up `tiled-server` on a real TCP port, connect with
//! `tiled-client`, exercise navigation + array + table reads + writes.

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

use bytes::Bytes;
use tiled_rs::adapters::{ArrayAdapter, CooAdapter, MapAdapter};
use tiled_rs::client::{AnyClient, from_uri};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::queries::Query;

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
    let root_tree: Arc<dyn tiled_rs::core::adapters::ContainerAdapter> = Arc::new(build_root());
    spawn_server_with_root(root_tree, api_key).await
}

/// Spawn the server over an arbitrary in-memory root tree on an ephemeral port.
async fn spawn_server_with_root(
    root_tree: Arc<dyn tiled_rs::core::adapters::ContainerAdapter>,
    api_key: Option<String>,
) -> String {
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
        api_key,
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
    };

    let app = tiled_rs::server::build_app(state);

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
    got.sort_by_key(|a| a.0);
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
async fn read_array_slice() {
    use tiled_rs::core::ndslice::NDSlice;

    let base = spawn_server(None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();
    let arr = root.get("some_array").await.unwrap().into_array().unwrap();

    fn decode_f64s(bytes: &[u8]) -> Vec<f64> {
        bytes
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    // A mid-array range slice fetches only the requested elements.
    let slice = NDSlice::from_numpy_str("2:5").unwrap();
    let block = arr.read_slice(&slice).await.unwrap();
    assert_eq!(block.shape, vec![3]);
    assert_eq!(decode_f64s(&block.data), vec![2.0, 3.0, 4.0]);

    // An empty NDSlice reads the whole array in one request.
    let block = arr.read_slice(&NDSlice::empty()).await.unwrap();
    assert_eq!(block.shape, vec![10]);
    assert_eq!(
        decode_f64s(&block.data),
        (0..10).map(|i| i as f64).collect::<Vec<_>>()
    );

    // A slice that selects nothing short-circuits without a wire round-trip
    // (mirrors Python's `0 in exp_shape` fast path, array.py:168-173).
    let slice = NDSlice::from_numpy_str("5:2").unwrap();
    let block = arr.read_slice(&slice).await.unwrap();
    assert_eq!(block.shape, vec![0]);
    assert!(block.data.is_empty());
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
    let client = tiled_rs::client::from_uri_with_options(
        &base,
        tiled_rs::client::ContextOptions::default().api_key("secret123"),
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
    use std::collections::BTreeSet;
    use tiled_rs::core::structures::StructureFamily;

    let base = spawn_server(None).await;
    let (ctx, _) = tiled_rs::client::Context::from_uri(&base).unwrap();
    let about = ctx.server_info().await.unwrap();
    assert_eq!(about.api_version, 0);
    assert!(!about.library_version.is_empty());

    // Server-contract regression (client gap #9 follow-up): the About `formats`
    // map must carry *every* family the server can serialize, including
    // `ragged` — it dropped out when the family list was hand-maintained in two
    // places. Tie the expectation to the server's own registry so it tracks the
    // contract, not a hardcoded list.
    let registry = tiled_rs::serialization::default_registry();
    let expected_ragged: BTreeSet<String> = registry
        .media_types(StructureFamily::Ragged)
        .into_iter()
        .collect();
    assert!(
        !expected_ragged.is_empty(),
        "the default registry serves ragged formats — this regression must not be vacuous"
    );
    let about_ragged: BTreeSet<String> = about
        .formats
        .get("ragged")
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect();
    assert_eq!(
        about_ragged, expected_ragged,
        "About `formats` must carry `ragged` with the server's registered ragged media types"
    );
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

    let (ctx, _) = tiled_rs::client::Context::from_uri(&format!("http://{addr}")).unwrap();
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
    let (ctx, _) = tiled_rs::client::Context::from_uri(&base).unwrap();
    let sub = tiled_rs::client::stream::Subscription::new(ctx, vec!["subgroup".to_string()]);

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
    use tiled_rs::catalog::Catalog;
    use tiled_rs::server::file_resolver::FileLeafResolver;

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

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root.clone()]));
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
    };

    let app = tiled_rs::server::build_app(state);
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
// Scope 1: ArrayClient::write, write_block, patch
// ---------------------------------------------------------------------------

/// POST /metadata creates a managed array node; PUT /array/full writes data;
/// GET /array/full reads it back.
#[tokio::test]
async fn array_write_full_roundtrip() {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

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
        Some("w_arr"),
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
    arr.write(payload.clone(), true).await.expect("write");

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
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

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
        Some("wb_arr"),
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
    arr.write_block(&[0], payload, true)
        .await
        .expect("write_block");

    let block = arr.read_block(&[0]).await.expect("read_block");
    let got: Vec<f64> = block
        .data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(got, values.to_vec());
}

/// `ArrayClient::write(_, persist=false)` threads `persist=false` to the request,
/// so the server streams to subscribers but skips the storage commit: after a
/// committing (persist=true) write, a persist=false write over it leaves the
/// stored data unchanged.
#[tokio::test]
async fn array_write_persist_false_does_not_commit() {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    async fn read_f64s(arr: &tiled_rs::client::ArrayClient) -> Vec<f64> {
        arr.read()
            .await
            .expect("read")
            .iter()
            .flat_map(|b| {
                b.data
                    .chunks_exact(8)
                    .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            })
            .collect()
    }

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
        Some("pf_arr"),
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create_node");

    let arr = root.get("pf_arr").await.unwrap().into_array().unwrap();

    // Commit real data (persist=true), then confirm it round-trips.
    let committed = [1.5f64, 2.5, 3.5, 4.5];
    let payload: bytes::Bytes = committed.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write(payload, true).await.expect("committing write");
    assert_eq!(
        read_f64s(&arr).await,
        committed.to_vec(),
        "persist=true commits"
    );

    // A persist=false write must NOT overwrite the stored data.
    let ephemeral = [9.9f64, 9.9, 9.9, 9.9];
    let payload2: bytes::Bytes = ephemeral.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write(payload2, false).await.expect("stream-only write");
    assert_eq!(
        read_f64s(&arr).await,
        committed.to_vec(),
        "persist=false leaves stored data unchanged"
    );
}

/// `ArrayClient::write_block(_, _, persist=false)` threads `persist=false`, so
/// the server streams the block but skips the storage commit: after a committing
/// (persist=true) block write, a persist=false block write over it leaves the
/// stored chunk unchanged.
#[tokio::test]
async fn array_write_block_persist_false_does_not_commit() {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    async fn read_block_f64s(arr: &tiled_rs::client::ArrayClient, block: &[usize]) -> Vec<f64> {
        arr.read_block(block)
            .await
            .expect("read_block")
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

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
        Some("pfb_arr"),
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create_node");

    let arr = root.get("pfb_arr").await.unwrap().into_array().unwrap();

    // Commit block 0 (persist=true), then confirm it round-trips.
    let committed = [10.0f64, 20.0, 30.0, 40.0];
    let payload: bytes::Bytes = committed.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write_block(&[0], payload, true)
        .await
        .expect("committing block write");
    assert_eq!(
        read_block_f64s(&arr, &[0]).await,
        committed.to_vec(),
        "persist=true commits the block"
    );

    // A persist=false block write must NOT overwrite the stored chunk.
    let ephemeral = [99.0f64, 99.0, 99.0, 99.0];
    let payload2: bytes::Bytes = ephemeral.iter().flat_map(|v| v.to_le_bytes()).collect();
    arr.write_block(&[0], payload2, false)
        .await
        .expect("stream-only block write");
    assert_eq!(
        read_block_f64s(&arr, &[0]).await,
        committed.to_vec(),
        "persist=false leaves the stored chunk unchanged"
    );
}

/// PATCH /array/full writes a data block into a slice (`offset`/`shape`) and,
/// with `extend=true`, grows the array. Tested by invariant boundary: an
/// in-bounds slice write (no grow), an overflowing slice without `extend`
/// (409 Conflict), and an overflowing slice with `extend` (grow + read back).
#[tokio::test]
async fn array_patch_slice_write_and_extend_roundtrip() {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

    // Flatten every block of a 1-D array read into one Vec<f64>.
    async fn read_f64s(arr: &tiled_rs::client::ArrayClient) -> Vec<f64> {
        arr.read()
            .await
            .expect("read")
            .iter()
            .flat_map(|b| {
                b.data
                    .chunks_exact(8)
                    .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            })
            .collect()
    }

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // A managed zarr array, shape [4], regular chunk size 2 (so an extend
    // recomputes the chunk grid). npy cannot extend — zarr is the writable
    // array backend.
    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![2, 2]],
        shape: vec![4],
        dims: None,
        resizable: Default::default(),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(structure)),
        id: None,
        mimetype: Some("application/x-zarr".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        Some("z_arr"),
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create_node");

    let arr = root.get("z_arr").await.unwrap().into_array().unwrap();

    // Seed [1.5, 2.5, 3.5, 4.5].
    let seed: bytes::Bytes = [1.5f64, 2.5, 3.5, 4.5]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    arr.write(seed, true).await.expect("seed write");

    // (1) In-bounds slice write: place [9.0, 9.0] at offset [1], no extend.
    let block: bytes::Bytes = [9.0f64, 9.0].iter().flat_map(|v| v.to_le_bytes()).collect();
    let s = arr
        .patch(block, &[2], &[1], false, true)
        .await
        .expect("in-bounds patch");
    assert_eq!(
        s.shape,
        vec![4],
        "in-bounds write must not change the shape"
    );
    assert_eq!(read_f64s(&arr).await, vec![1.5, 9.0, 9.0, 4.5]);

    // (2) Overflowing slice without extend → 409 Conflict, surfaced as an error.
    let overflow: bytes::Bytes = [5.5f64, 6.5].iter().flat_map(|v| v.to_le_bytes()).collect();
    let err = arr.patch(overflow.clone(), &[2], &[4], false, true).await;
    assert!(
        err.is_err(),
        "a slice past the end without extend must be rejected (409)"
    );

    // (3) Overflowing slice with extend → grows to shape [6], chunks [[2,2,2]].
    let grown = arr
        .patch(overflow, &[2], &[4], true, true)
        .await
        .expect("extend patch");
    assert_eq!(
        grown.shape,
        vec![6],
        "extend must grow the array to length 6"
    );
    assert_eq!(grown.chunks, vec![vec![2, 2, 2]]);

    // The client's cached structure is immutable; re-fetch to read the grown
    // array through the refreshed chunk grid.
    let arr2 = root.get("z_arr").await.unwrap().into_array().unwrap();
    assert_eq!(
        read_f64s(&arr2).await,
        vec![1.5, 9.0, 9.0, 4.5, 5.5, 6.5],
        "grown array read-back mismatch"
    );
}

// ---------------------------------------------------------------------------
// Scope 1b: RaggedClient::write, patch, write_block (SQL-backed managed write)
// ---------------------------------------------------------------------------

/// A managed ragged int64 node: shape `[rows, None]`, one chunk of `rows`.
#[cfg(test)]
fn ragged_int64_ds(
    rows: usize,
    chunks0: Vec<usize>,
    size: usize,
) -> tiled_rs::core::data_source::DataSource {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType};
    use tiled_rs::core::structures::{AnyStructure, RaggedStructure, StructureFamily};

    let structure = RaggedStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Integer, 8)),
        shape: vec![Some(rows), None],
        size,
        chunks: vec![Some(chunks0), None],
        dims: None,
        resizable: Default::default(),
    };
    DataSource {
        structure_family: StructureFamily::Ragged,
        structure: Some(AnyStructure::Ragged(structure)),
        id: None,
        mimetype: Some("application/x-ragged+sql".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    }
}

/// POST /metadata creates a managed ragged node; PUT /ragged/full writes the
/// list-of-lists; GET /ragged/full reads it back; PATCH /ragged/full extends it.
#[tokio::test]
async fn ragged_write_full_read_and_patch_roundtrip() {
    use tiled_rs::core::structures::StructureFamily;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // One chunk of 2 rows; 4 leaf elements total ([1,2,3] + [4]).
    let ds = ragged_int64_ds(2, vec![2], 4);
    root.create_node(
        Some("rag"),
        StructureFamily::Ragged,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create ragged node");

    let rag = root.get("rag").await.unwrap().into_ragged().unwrap();

    // Write the whole array, then read it back unchanged.
    let data = serde_json::json!([[1, 2, 3], [4]]);
    rag.write(&data, true).await.expect("ragged write");
    let read_back = rag.read().await.expect("ragged read");
    assert_eq!(read_back, data, "ragged full round-trip mismatch");

    // Extend: append one row at offset [2] (the current leftmost length).
    let new_rows = serde_json::json!([[5, 6]]);
    let new_structure = rag
        .patch(&new_rows, &[2], true, true)
        .await
        .expect("ragged patch");
    assert_eq!(
        new_structure.shape[0],
        Some(3),
        "patch must grow the leftmost dimension to 3"
    );

    // The grown array reads back as the concatenation.
    let after = rag.read().await.expect("ragged read after patch");
    assert_eq!(
        after,
        serde_json::json!([[1, 2, 3], [4], [5, 6]]),
        "ragged read after patch mismatch"
    );
}

/// PUT /ragged/block writes individual chunks of a multi-partition ragged node;
/// GET /ragged/full reads the concatenation.
#[tokio::test]
async fn ragged_write_block_roundtrip() {
    use tiled_rs::core::structures::StructureFamily;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // Two chunks: chunk 0 has 2 rows, chunk 1 has 1 row; 6 leaf elements total.
    let ds = ragged_int64_ds(3, vec![2, 1], 6);
    root.create_node(
        Some("rag_blocks"),
        StructureFamily::Ragged,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create ragged node");

    let rag = root.get("rag_blocks").await.unwrap().into_ragged().unwrap();

    // Write the two chunks out of order to prove chunk_index ordering on read.
    rag.write_block(&serde_json::json!([[4, 5, 6]]), &[1], true)
        .await
        .expect("write_block 1");
    rag.write_block(&serde_json::json!([[1, 2], [3]]), &[0], true)
        .await
        .expect("write_block 0");

    let read_back = rag.read().await.expect("ragged read");
    assert_eq!(
        read_back,
        serde_json::json!([[1, 2], [3], [4, 5, 6]]),
        "ragged block round-trip mismatch (chunks must concatenate in index order)"
    );
}

/// `ContainerClient::write_ragged` creates a managed ragged node and uploads the
/// rows in one call; the list-of-lists reads back verbatim, and `access_tags`
/// is stored via the shared create builder.
#[tokio::test]
async fn write_ragged_helper_roundtrip() {
    use tiled_rs::core::dtype::{BuiltinDType, DType};
    use tiled_rs::core::structures::RaggedStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Two rows ([1,2,3] + [4]), 4 leaf elements, one chunk of 2 rows.
    let structure = RaggedStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Integer, 8)),
        shape: vec![Some(2), None],
        size: 4,
        chunks: vec![Some(vec![2]), None],
        dims: None,
        resizable: Default::default(),
    };
    let data = serde_json::json!([[1, 2, 3], [4]]);
    let tags = vec!["team-r".to_string()];

    let rag = root
        .write_ragged(
            Some("h_rag"),
            structure,
            &data,
            serde_json::json!({"note": "hi"}),
            vec![],
            Some(&tags),
        )
        .await
        .expect("write_ragged");

    // The returned client reads the rows back unchanged.
    let read_back = rag.read().await.expect("ragged read");
    assert_eq!(read_back, data, "ragged round-trip mismatch");

    // access_tags landed as access_blob.tags on the created node.
    let fetched = root.get("h_rag").await.unwrap();
    let blob = fetched
        .base()
        .expect("ragged node has a base client")
        .item()
        .attributes
        .access_blob
        .clone()
        .expect("access_blob present on the created node");
    assert_eq!(
        blob,
        serde_json::json!({"tags": ["team-r"]}),
        "access_tags stored as access_blob.tags"
    );
}

/// `ContainerClient::write_awkward` creates a managed awkward node and uploads
/// its buffer map in one call, threading `access_tags`; the buffers read back
/// verbatim and the tags land as `access_blob.tags`.
#[tokio::test]
async fn write_awkward_helper_roundtrip() {
    use std::collections::HashMap;
    use tiled_rs::core::structures::AwkwardStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    let structure = AwkwardStructure {
        length: 3,
        form: serde_json::json!({
            "class": "NumpyArray",
            "primitive": "float64",
            "form_key": "node0"
        }),
    };
    let mut buffers = HashMap::new();
    buffers.insert("node0-data".to_string(), Bytes::from(vec![7u8; 24]));
    let tags = vec!["team-a".to_string()];

    let ak = root
        .write_awkward(
            Some("h_awk"),
            structure,
            buffers,
            serde_json::json!({"note": "awk"}),
            vec![],
            Some(&tags),
        )
        .await
        .expect("write_awkward");

    // The returned client reads the buffers back unchanged.
    let read_back = ak.read().await.expect("awkward read");
    assert_eq!(read_back.buffers.len(), 1);
    assert_eq!(&read_back.buffers["node0-data"][..], &[7u8; 24][..]);

    // access_tags landed as access_blob.tags on the created node.
    let fetched = root.get("h_awk").await.unwrap();
    let blob = fetched
        .base()
        .expect("awkward node has a base client")
        .item()
        .attributes
        .access_blob
        .clone()
        .expect("access_blob present on the created node");
    assert_eq!(
        blob,
        serde_json::json!({"tags": ["team-a"]}),
        "access_tags stored as access_blob.tags"
    );
}

// ---------------------------------------------------------------------------
// Server-level awkward managed-write: create + write + read-back over a
// catalog node (exercises the resolver's awkward arm + the on-disk buffer
// directory), not the in-memory test tree.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn awkward_managed_write_roundtrip() {
    use std::collections::HashMap;
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::structures::{AnyStructure, AwkwardStructure, StructureFamily};

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // A single NumpyArray leaf: form_key "node0" backs the buffer "node0-data".
    let structure = AwkwardStructure {
        length: 3,
        form: serde_json::json!({
            "class": "NumpyArray",
            "primitive": "float64",
            "form_key": "node0"
        }),
    };
    let ds = DataSource {
        structure_family: StructureFamily::Awkward,
        structure: Some(AnyStructure::Awkward(structure)),
        id: None,
        // No pinned mimetype: exercise `default_creation_mimetype(Awkward)` ->
        // application/x-awkward-buffers, then `managed_init_storage` ->
        // `init_storage_awkward`.
        mimetype: None,
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        Some("ak_managed"),
        StructureFamily::Awkward,
        serde_json::json!({"note": "awk"}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create awkward node");

    let ak = root
        .get("ak_managed")
        .await
        .unwrap()
        .into_awkward()
        .unwrap();

    // Boundary: reading a managed awkward node before any write — the buffer
    // directory was created empty by `init_storage_awkward`, so no buffers.
    let before = ak.read().await.expect("read before write");
    assert!(
        before.buffers.is_empty(),
        "no buffers before the first write"
    );

    // First write to the empty managed directory.
    let mut v1 = HashMap::new();
    v1.insert("node0-data".to_string(), Bytes::from(vec![1u8; 24]));
    ak.write(v1).await.expect("first write to empty node");

    let read1 = ak.read().await.expect("read after first write");
    assert_eq!(read1.buffers.len(), 1, "one buffer after first write");
    assert_eq!(&read1.buffers["node0-data"][..], &[1u8; 24][..]);

    // Boundary: re-write (overwrite) the same key — upstream's DirectoryContainer
    // overwrites per form_key, so the new bytes replace the old on disk.
    let mut v2 = HashMap::new();
    v2.insert("node0-data".to_string(), Bytes::from(vec![2u8; 24]));
    ak.write(v2).await.expect("re-write over existing buffer");

    let read2 = ak.read().await.expect("read after re-write");
    assert_eq!(
        &read2.buffers["node0-data"][..],
        &[2u8; 24][..],
        "re-write overwrites the buffer in place"
    );
}

// ---------------------------------------------------------------------------
// Scope 2: TableClient::write
// ---------------------------------------------------------------------------

#[tokio::test]
async fn table_write_full_roundtrip() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::structures::{AnyStructure, StructureFamily, TableStructure};

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
        Some("w_tbl"),
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
// High-level container write helpers: ContainerClient::write_array / write_table
// ---------------------------------------------------------------------------

/// `ContainerClient::write_array` creates a managed array node and uploads the
/// buffer in one call; the data reads back verbatim.
#[tokio::test]
async fn write_array_helper_roundtrip() {
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::ArrayStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![4]],
        shape: vec![4],
        dims: None,
        resizable: Default::default(),
    };
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let payload: bytes::Bytes = values.iter().flat_map(|v| v.to_le_bytes()).collect();

    let arr = root
        .write_array(
            Some("h_arr"),
            structure,
            payload,
            serde_json::json!({"note": "hi"}),
            vec![],
            None,
        )
        .await
        .expect("write_array");

    // The returned client reads the data back.
    let blocks = arr.read().await.expect("read");
    assert_eq!(blocks.len(), 1);
    let got: Vec<f64> = blocks[0]
        .data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(got, values.to_vec(), "round-trip data mismatch");

    // A fresh fetch also sees the node with its metadata.
    let refetched = root.get("h_arr").await.unwrap().into_array().unwrap();
    assert_eq!(refetched.shape(), &[4]);
}

/// `ContainerClient::write_table` derives the structure from the Arrow schema,
/// creates a managed table node, and uploads the batch in one call.
#[tokio::test]
async fn write_table_helper_roundtrip() {
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

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

    let tbl = root
        .write_table(
            Some("h_tbl"),
            &schema,
            &[batch],
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("write_table");

    // Columns come from the schema field names.
    assert_eq!(tbl.columns(), &["x".to_string(), "y".to_string()]);

    // The data reads back.
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

/// `access_tags` on a write helper is sent as `access_blob: {"tags": [...]}`
/// and stored on the node (verified by reading the access_blob back).
#[tokio::test]
async fn write_array_helper_access_tags_roundtrip() {
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::ArrayStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    let structure = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![vec![2]],
        shape: vec![2],
        dims: None,
        resizable: Default::default(),
    };
    let payload: bytes::Bytes = [1.0f64, 2.0].iter().flat_map(|v| v.to_le_bytes()).collect();
    let tags = vec!["team-a".to_string(), "team-b".to_string()];

    root.write_array(
        Some("tagged"),
        structure,
        payload,
        serde_json::json!({}),
        vec![],
        Some(&tags),
    )
    .await
    .expect("write_array with access_tags");

    // Fetch fresh and confirm the tags landed in access_blob.
    let fetched = root.get("tagged").await.unwrap();
    let blob = fetched
        .base()
        .expect("array node has a base client")
        .item()
        .attributes
        .access_blob
        .clone()
        .expect("access_blob present on the created node");
    assert_eq!(
        blob,
        serde_json::json!({"tags": ["team-a", "team-b"]}),
        "access_tags stored as access_blob.tags"
    );
}

#[tokio::test]
async fn table_write_partition_and_append_roundtrip() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::structures::{AnyStructure, StructureFamily, TableStructure};

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // Managed CSV tables are single-partition (partition 0 == the whole file),
    // so this exercises write_partition(0) + append_partition(0) against the
    // real /table/partition PUT + PATCH routes and the CSV writable adapter.
    let table_structure = TableStructure {
        arrow_schema: String::new(),
        npartitions: 1,
        columns: vec!["n".into()],
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
        Some("w_part"),
        StructureFamily::Table,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create table node");

    let tbl = root.get("w_part").await.unwrap().into_table().unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let make = |vals: Vec<i64>| -> arrow::array::RecordBatch {
        arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vals)) as arrow::array::ArrayRef],
        )
        .unwrap()
    };

    // Concatenate every batch's `n` column across all partitions, in order.
    let read_all = |parts: Vec<tiled_rs::client::TablePartition>| -> Vec<i64> {
        let mut all = Vec::new();
        for part in &parts {
            for b in &part.batches {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("n is Int64");
                all.extend_from_slice(col.values());
            }
        }
        all
    };

    // PUT partition 0 = [1, 2, 3].
    tbl.write_partition(0, &schema, &[make(vec![1, 2, 3])])
        .await
        .expect("write_partition 0");
    let after_write = tbl.read(None).await.expect("read after write_partition");
    assert_eq!(read_all(after_write), vec![1, 2, 3]);

    // PATCH partition 0 appends [4, 5] → [1, 2, 3, 4, 5].
    tbl.append_partition(0, &schema, &[make(vec![4, 5])])
        .await
        .expect("append_partition 0");
    let after_append = tbl.read(None).await.expect("read after append_partition");
    assert_eq!(read_all(after_append), vec![1, 2, 3, 4, 5]);
}

/// Build an in-memory root holding one writable awkward node (`ak`) with a
/// single `node0-data` buffer of 3 float64 values. Awkward is not wired into
/// the catalog/file-resolver write path, so the client write method is
/// exercised against a directly-served in-memory `AwkwardAdapter` (writable).
fn build_awkward_root() -> Arc<dyn tiled_rs::core::adapters::ContainerAdapter> {
    use std::collections::HashMap;
    use tiled_rs::adapters::AwkwardAdapter;
    use tiled_rs::core::structures::AwkwardStructure;

    let structure = AwkwardStructure {
        length: 3,
        form: serde_json::json!({
            "class": "NumpyArray",
            "primitive": "float64",
            "form_key": "node0",
            "inner_shape": [],
            "itemsize": 8
        }),
    };
    let mut buffers: HashMap<String, Bytes> = HashMap::new();
    buffers.insert(
        "node0-data".into(),
        Bytes::from(vec![
            0u8, 0, 0, 0, 0, 0, 0xF0, 0x3F, // 1.0 f64 LE
            0, 0, 0, 0, 0, 0, 0, 0x40, // 2.0 f64 LE
            0, 0, 0, 0, 0, 0, 8, 0x40, // 3.0 f64 LE
        ]),
    );
    let adapter = AwkwardAdapter::new(buffers, structure);
    let mut mapping = IndexMap::new();
    mapping.insert("ak".to_string(), AnyAdapter::Awkward(Arc::new(adapter)));
    Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]))
}

#[tokio::test]
async fn awkward_write_roundtrip() {
    use std::collections::HashMap;

    let base = spawn_server_with_root(build_awkward_root(), None).await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();
    let ak = root.get("ak").await.unwrap().into_awkward().unwrap();

    // Overwrite node0-data with two float64 values [4.0, 5.0].
    let new_data: Vec<u8> = vec![
        0, 0, 0, 0, 0, 0, 0x10, 0x40, // 4.0 f64 LE
        0, 0, 0, 0, 0, 0, 0x14, 0x40, // 5.0 f64 LE
    ];
    let mut buffers: HashMap<String, Bytes> = HashMap::new();
    buffers.insert("node0-data".into(), Bytes::from(new_data.clone()));
    ak.write(buffers).await.expect("awkward write");

    // Read back and verify the buffer map round-trips through the zip wire.
    let back = ak.read().await.expect("awkward read");
    assert_eq!(back.buffers.len(), 1);
    assert_eq!(&back.buffers["node0-data"][..], &new_data[..]);
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
        .create_container(Some("sub"), serde_json::json!({"created_by": "test"}))
        .await
        .expect("create_container");
    assert_eq!(
        sub.base().metadata().get("created_by"),
        Some(&serde_json::json!("test"))
    );

    // Create two grandchildren inside it.
    sub.create_container(Some("a"), serde_json::json!({}))
        .await
        .expect("create grandchild a");
    sub.create_container(Some("b"), serde_json::json!({}))
        .await
        .expect("create grandchild b");

    let keys = sub.keys().await.unwrap();
    assert_eq!(keys.len(), 2);

    // Delete all grandchildren (both empty leaf containers → non-recursive).
    sub.delete_contents(false, false)
        .await
        .expect("delete_contents");
    let keys_after = sub.keys().await.unwrap();
    assert!(
        keys_after.is_empty(),
        "delete_contents must empty container"
    );
}

/// Client recursive delete: `delete(recursive=true)` removes a non-empty
/// container's whole subtree in one call, where the non-recursive path 409s.
/// Mirrors Python `BaseClient.delete(recursive=True)` (base.py:918-936).
#[tokio::test]
async fn base_delete_recursive_removes_subtree() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // sub / sub/a / sub/a/deep  — `sub` is a non-empty container.
    let sub = root
        .create_container(Some("sub"), serde_json::json!({}))
        .await
        .expect("create sub");
    let a = sub
        .create_container(Some("a"), serde_json::json!({}))
        .await
        .expect("create a");
    a.create_container(Some("deep"), serde_json::json!({}))
        .await
        .expect("create deep");

    // Non-recursive delete of the non-empty container is refused (409 → Err).
    let node = root.get("sub").await.unwrap();
    let err = node
        .base()
        .expect("base")
        .delete(false, false)
        .await
        .expect_err("non-recursive delete of a non-empty container must fail");
    let _ = err;

    // Recursive delete succeeds and removes the whole subtree.
    let node = root.get("sub").await.unwrap();
    node.base()
        .expect("base")
        .delete(true, false)
        .await
        .expect("recursive delete");

    let keys_after = root.keys().await.unwrap();
    assert!(
        !keys_after.contains(&"sub".to_string()),
        "recursive delete must remove the subtree root: {keys_after:?}"
    );
}

/// Omitting `key` (`None`) lets the server generate one (Python parity:
/// `Container.new(key=None)` → `entry.context.key_maker()`, a `uuid.uuid4()`
/// string). The client must read the generated key back from the response
/// and use it to fetch the node it just created.
#[tokio::test]
async fn container_create_container_with_generated_key() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let child = root
        .create_container(None, serde_json::json!({"auto": true}))
        .await
        .expect("create_container with generated key");
    let generated_key = child.base().id().to_string();

    assert!(!generated_key.is_empty(), "server must assign a key");
    uuid::Uuid::parse_str(&generated_key)
        .expect("generated key must be a uuid4 string, matching Python's key_maker");

    // Roundtrip: fetch by the generated key and confirm it is the same node.
    let fetched = root
        .get(&generated_key)
        .await
        .expect("fetch by generated key")
        .into_container()
        .expect("generated child is a container");
    assert_eq!(
        fetched.base().metadata().get("auto"),
        Some(&serde_json::json!(true))
    );

    let keys = root.keys().await.unwrap();
    assert!(
        keys.contains(&generated_key),
        "generated key must appear in parent listing"
    );
}

/// Two anonymous creates under the same parent must not collide: each gets
/// its own server-generated key and both children persist independently.
#[tokio::test]
async fn container_two_anonymous_creates_do_not_collide() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let first = root
        .create_container(None, serde_json::json!({"which": "first"}))
        .await
        .expect("create first anonymous container");
    let second = root
        .create_container(None, serde_json::json!({"which": "second"}))
        .await
        .expect("create second anonymous container");

    let first_key = first.base().id().to_string();
    let second_key = second.base().id().to_string();
    assert_ne!(
        first_key, second_key,
        "two anonymous creates must not collide on the same key"
    );

    let keys = root.keys().await.unwrap();
    assert!(keys.contains(&first_key));
    assert!(keys.contains(&second_key));
    assert_eq!(keys.len(), 2, "both anonymous children must persist");
}

// ---------------------------------------------------------------------------
// Scope 4: BaseClient::delete, BaseClient::patch_metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn base_delete_removes_node() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("to_delete"), serde_json::json!({}))
        .await
        .expect("create");
    let keys_before = root.keys().await.unwrap();
    assert!(keys_before.contains(&"to_delete".to_string()));

    let node = root.get("to_delete").await.unwrap();
    node.base()
        .expect("base")
        .delete(false, false)
        .await
        .expect("delete");

    // Must no longer appear in the listing.
    let keys_after = root.keys().await.unwrap();
    assert!(!keys_after.contains(&"to_delete".to_string()));
}

#[tokio::test]
async fn container_distinct_metadata_values() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("red_one"), serde_json::json!({"color": "red"}))
        .await
        .expect("create red_one");
    root.create_container(Some("red_two"), serde_json::json!({"color": "red"}))
        .await
        .expect("create red_two");
    root.create_container(Some("blue_one"), serde_json::json!({"color": "blue"}))
        .await
        .expect("create blue_one");

    let resp = root
        .distinct(&["color"], true, false, true)
        .await
        .expect("distinct");

    let color_values = resp
        .metadata
        .as_ref()
        .and_then(|m| m.get("color"))
        .expect("color facet present");
    let mut by_value: std::collections::HashMap<String, i64> = color_values
        .iter()
        .map(|v| (v.value.as_str().unwrap().to_string(), v.count.unwrap_or(0)))
        .collect();
    assert_eq!(by_value.remove("red"), Some(2), "two red containers");
    assert_eq!(by_value.remove("blue"), Some(1), "one blue container");
    assert!(by_value.is_empty(), "no other color values expected");

    let families = resp
        .structure_families
        .as_ref()
        .expect("structure_families facet present");
    assert!(
        families
            .iter()
            .any(|v| v.value == serde_json::json!("container")),
        "root children are all containers"
    );
    assert!(resp.specs.is_none(), "specs facet not requested");
}

#[tokio::test]
async fn base_patch_metadata_merge() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("to_patch"), serde_json::json!({"a": 1, "b": 2}))
        .await
        .expect("create");
    let node = root.get("to_patch").await.unwrap();
    // Merge-patch: add "c", leave "a" alone, remove "b" with null.
    node.base()
        .expect("base")
        .patch_metadata(
            Some(serde_json::json!({"b": null, "c": 3})),
            None,
            None,
            tiled_rs::client::PatchContentType::MergePatch,
            false,
        )
        .await
        .expect("patch_metadata");

    // Fetch fresh to confirm the server applied the patch.
    let updated = root.get("to_patch").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(meta.get("a"), Some(&serde_json::json!(1)), "a unchanged");
    assert_eq!(meta.get("b"), None, "b removed by null");
    assert_eq!(meta.get("c"), Some(&serde_json::json!(3)), "c added");
}

#[tokio::test]
async fn base_patch_metadata_json_patch_mode() {
    use tiled_rs::client::PatchContentType;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("jp_node"), serde_json::json!({"a": 1, "b": 2}))
        .await
        .expect("create");
    let node = root.get("jp_node").await.unwrap();
    // RFC 6902 ops: replace "a", remove "b", add "c".
    let ops = serde_json::json!([
        {"op": "replace", "path": "/a", "value": 10},
        {"op": "remove", "path": "/b"},
        {"op": "add", "path": "/c", "value": 3},
    ]);
    node.base()
        .expect("base")
        .patch_metadata(Some(ops), None, None, PatchContentType::JsonPatch, false)
        .await
        .expect("patch_metadata");

    let updated = root.get("jp_node").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(meta.get("a"), Some(&serde_json::json!(10)), "a replaced");
    assert_eq!(meta.get("b"), None, "b removed");
    assert_eq!(meta.get("c"), Some(&serde_json::json!(3)), "c added");
}

#[tokio::test]
async fn base_patch_metadata_drop_revision() {
    use tiled_rs::client::PatchContentType;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("dr_node"), serde_json::json!({"a": 1}))
        .await
        .expect("create");
    let node = root.get("dr_node").await.unwrap();
    node.base()
        .expect("base")
        .patch_metadata(
            Some(serde_json::json!({"a": 2})),
            None,
            None,
            PatchContentType::MergePatch,
            true, // drop_revision: discard the pre-patch version instead of recording it.
        )
        .await
        .expect("patch_metadata with drop_revision");

    let updated = root.get("dr_node").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(
        meta.get("a"),
        Some(&serde_json::json!(2)),
        "patch still applied"
    );
}

#[tokio::test]
async fn base_replace_metadata_wholesale() {
    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("replace_node"), serde_json::json!({"a": 1, "b": 2}))
        .await
        .expect("create");
    let node = root.get("replace_node").await.unwrap();
    node.base()
        .expect("base")
        .replace_metadata(Some(serde_json::json!({"c": 3})), None, None, false)
        .await
        .expect("replace_metadata");

    // PUT wholesale-replaces the metadata document: "a" and "b" are gone,
    // not merely unset, since only "c" was ever in the new document.
    let updated = root.get("replace_node").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(meta.get("a"), None, "a gone after wholesale replace");
    assert_eq!(meta.get("b"), None, "b gone after wholesale replace");
    assert_eq!(
        meta.get("c"),
        Some(&serde_json::json!(3)),
        "c is the new document"
    );
}

#[tokio::test]
async fn base_update_metadata_diff_builder() {
    use std::collections::BTreeMap;
    use tiled_rs::client::MetadataUpdate;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(
        Some("upd_node"),
        serde_json::json!({"a": 1, "b": 2, "obj": {"x": 1, "y": 2}}),
    )
    .await
    .expect("create");
    let node = root.get("upd_node").await.unwrap();

    // One diff-built PATCH exercising every boundary at once:
    //   replace b, add c, delete a, and a nested merge that deletes obj/x and
    //   adds obj/z while leaving obj/y untouched.
    let update: BTreeMap<String, MetadataUpdate> = [
        ("a".to_string(), MetadataUpdate::Delete),
        ("b".to_string(), MetadataUpdate::Set(serde_json::json!(20))),
        ("c".to_string(), MetadataUpdate::Set(serde_json::json!(3))),
        (
            "obj".to_string(),
            MetadataUpdate::Merge(
                [
                    ("x".to_string(), MetadataUpdate::Delete),
                    ("z".to_string(), MetadataUpdate::Set(serde_json::json!(9))),
                ]
                .into_iter()
                .collect(),
            ),
        ),
    ]
    .into_iter()
    .collect();

    node.base()
        .expect("base")
        .update_metadata(Some(&update), None, None, false)
        .await
        .expect("update_metadata");

    let updated = root.get("upd_node").await.unwrap();
    let meta = updated.base().expect("base").metadata().clone();
    assert_eq!(
        meta,
        serde_json::json!({"b": 20, "c": 3, "obj": {"y": 2, "z": 9}}),
        "diff-built patch applied add/replace/delete + nested-leaf merge"
    );
}

#[tokio::test]
async fn base_update_metadata_noop_is_accepted() {
    use std::collections::BTreeMap;
    use tiled_rs::client::MetadataUpdate;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    root.create_container(Some("noop_node"), serde_json::json!({"a": 1}))
        .await
        .expect("create");
    let node = root.get("noop_node").await.unwrap();

    // Setting `a` to its current value produces an empty patch; upstream still
    // issues the PATCH and the server accepts it, leaving metadata unchanged.
    let update: BTreeMap<String, MetadataUpdate> =
        [("a".to_string(), MetadataUpdate::Set(serde_json::json!(1)))]
            .into_iter()
            .collect();
    node.base()
        .expect("base")
        .update_metadata(Some(&update), None, None, false)
        .await
        .expect("no-op update_metadata still succeeds");

    let updated = root.get("noop_node").await.unwrap();
    assert_eq!(
        updated.base().expect("base").metadata().clone(),
        serde_json::json!({"a": 1}),
        "no-op update leaves metadata unchanged"
    );
}

#[tokio::test]
async fn base_update_metadata_specs() {
    use tiled_rs::core::structures::StructureFamily;

    let (base, _wd, _db) = spawn_write_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    // Create a node that already carries one spec.
    root.create_node(
        Some("spec_node"),
        StructureFamily::Container,
        serde_json::json!({}),
        vec![serde_json::json!({"name": "alpha"})],
        vec![],
    )
    .await
    .expect("create with spec");
    let node = root.get("spec_node").await.unwrap();
    assert_eq!(
        node.base().expect("base").specs().len(),
        1,
        "node starts with one spec"
    );

    // Update the spec-name set: keep alpha, add beta.
    node.base()
        .expect("base")
        .update_metadata(
            None,
            Some(&["alpha".to_string(), "beta".to_string()]),
            None,
            false,
        )
        .await
        .expect("update_metadata specs");

    let updated = root.get("spec_node").await.unwrap();
    let names: Vec<&str> = updated
        .base()
        .expect("base")
        .specs()
        .iter()
        .map(|s| s.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["alpha", "beta"],
        "specs diff added beta, kept alpha"
    );
}

// ---------------------------------------------------------------------------
// Scope 5: ArrayClient::export, TableClient::export
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_export_to_file() {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, ArrayStructure, StructureFamily};

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
        Some("exp_arr"),
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
    arr.write(payload, true).await.unwrap();

    let out = tempfile::tempdir().unwrap();

    // 1. Explicit media type: raw bytes (4 × 8 = 32 bytes).
    let explicit = out.path().join("explicit.bin");
    arr.export(&explicit, Some("application/octet-stream"))
        .await
        .expect("export octet-stream");
    assert_eq!(
        std::fs::read(&explicit).unwrap().len(),
        32,
        "4 × 8 bytes in octet-stream export"
    );

    // 2. Format inferred from the `.csv` extension (format = None).
    let inferred = out.path().join("values.csv");
    arr.export(&inferred, None)
        .await
        .expect("export inferred csv");
    let csv = std::fs::read_to_string(&inferred).unwrap();
    assert!(!csv.is_empty(), "inferred csv is non-empty");
    assert!(
        csv.contains('1') && csv.contains('4'),
        "csv holds the data: {csv:?}"
    );

    // 3. Explicit format overrides the extension: a `.csv` dest with an explicit
    //    octet-stream request yields the 32 raw bytes, not CSV text.
    let override_dest = out.path().join("override.csv");
    arr.export(&override_dest, Some("application/octet-stream"))
        .await
        .expect("explicit format overrides extension");
    assert_eq!(
        std::fs::read(&override_dest).unwrap().len(),
        32,
        "explicit octet-stream produced raw bytes despite the .csv name"
    );

    // 4. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.bin");
    let err = arr.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

#[tokio::test]
async fn table_export_to_file() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::structures::{AnyStructure, StructureFamily, TableStructure};

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
        Some("exp_tbl"),
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

    let out = tempfile::tempdir().unwrap();

    // 1. Explicit format "csv".
    let explicit = out.path().join("explicit.csv");
    tbl.export(&explicit, Some("csv"))
        .await
        .expect("export csv");
    let content = std::fs::read_to_string(&explicit).unwrap();
    assert!(content.contains("x") && content.contains("y"), "csv header");
    assert!(content.contains('1') && content.contains('z'), "csv data");

    // 2. Format inferred from the `.csv` extension (format = None).
    let inferred = out.path().join("table.csv");
    tbl.export(&inferred, None)
        .await
        .expect("export inferred csv");
    let inferred_csv = std::fs::read_to_string(&inferred).unwrap();
    assert!(
        inferred_csv.contains('1') && inferred_csv.contains('z'),
        "inferred csv holds the data: {inferred_csv:?}"
    );

    // 3. Explicit format overrides the extension: a `.csv` dest with an explicit
    //    `json` request yields JSON (quoted `"x"` key), not CSV.
    let override_dest = out.path().join("override.csv");
    tbl.export(&override_dest, Some("json"))
        .await
        .expect("explicit format overrides extension");
    let override_body = std::fs::read_to_string(&override_dest).unwrap();
    assert!(
        override_body.contains("\"x\""),
        "explicit json produced JSON despite the .csv name: {override_body:?}"
    );

    // 4. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.bin");
    let err = tbl.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

/// Read the entry names out of an in-memory zip archive.
fn zip_entry_names(bytes: &[u8]) -> Vec<String> {
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes.to_vec())).expect("valid zip archive");
    (0..archive.len())
        .map(|i| archive.by_index(i).unwrap().name().to_string())
        .collect()
}

/// `ContainerClient::export` — download a subtree (array + table child) to a
/// local zip file, plus the format-resolution surface (explicit format,
/// extension inference, explicit-over-extension, unknown → error).
#[tokio::test]
async fn container_export_to_zip() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{
        AnyStructure, ArrayStructure, StructureFamily, TableStructure,
    };

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Array child, then write its data.
    let arr_ds = DataSource {
        structure_family: StructureFamily::Array,
        structure: Some(AnyStructure::Array(ArrayStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            chunks: vec![vec![4]],
            shape: vec![4],
            dims: None,
            resizable: Default::default(),
        })),
        id: None,
        mimetype: Some("application/x-npy".into()),
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        Some("arr"),
        StructureFamily::Array,
        serde_json::json!({}),
        vec![],
        vec![arr_ds],
    )
    .await
    .unwrap();
    let arr = root.get("arr").await.unwrap().into_array().unwrap();
    let payload: bytes::Bytes = [1.0f64, 2.0, 3.0, 4.0]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    arr.write(payload, true).await.unwrap();

    // Table child, then write one batch.
    let tbl_ds = DataSource {
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
    root.create_node(
        Some("tbl"),
        StructureFamily::Table,
        serde_json::json!({}),
        vec![],
        vec![tbl_ds],
    )
    .await
    .unwrap();
    let tbl = root.get("tbl").await.unwrap().into_table().unwrap();
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

    let container = from_uri(&base).await.unwrap().into_container().unwrap();
    let out = tempfile::tempdir().unwrap();

    // 1. Explicit format "zip": file exists, non-empty, zip magic, expected entries.
    let explicit = out.path().join("explicit.zip");
    container
        .export(&explicit, Some("zip"))
        .await
        .expect("export zip");
    let bytes = std::fs::read(&explicit).unwrap();
    assert!(!bytes.is_empty(), "zip export is non-empty");
    assert_eq!(&bytes[..4], b"PK\x03\x04", "zip local-file-header magic");
    let names = zip_entry_names(&bytes);
    assert!(
        names.iter().any(|n| n == "arr.bin"),
        "array leaf entry present: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "tbl.arrow"),
        "table leaf entry present: {names:?}"
    );

    // 2. Format inferred from the ".zip" extension (format = None).
    let inferred = out.path().join("inferred.zip");
    container
        .export(&inferred, None)
        .await
        .expect("export inferred zip");
    let inferred_bytes = std::fs::read(&inferred).unwrap();
    assert_eq!(
        &inferred_bytes[..4],
        b"PK\x03\x04",
        "inferred-format zip magic"
    );
    let inferred_names = zip_entry_names(&inferred_bytes);
    assert!(inferred_names.iter().any(|n| n == "arr.bin"));
    assert!(inferred_names.iter().any(|n| n == "tbl.arrow"));

    // 3. Explicit format overrides the extension: a ".bin" dest still gets a zip.
    let override_dest = out.path().join("override.bin");
    container
        .export(&override_dest, Some(".zip"))
        .await
        .expect("explicit format overrides extension");
    let override_bytes = std::fs::read(&override_dest).unwrap();
    assert_eq!(
        &override_bytes[..4],
        b"PK\x03\x04",
        "explicit .zip produced a zip despite the .bin filename"
    );

    // 4. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.zip");
    let err = container.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

/// `AwkwardClient::export` GETs `/awkward/full?format=<fmt>` and writes the
/// zipped buffer archive to a file. Same test shape as the array/container
/// export: explicit media type, inferred-from-extension, and an unknown-format
/// error. The `ak` node is pre-seeded with one `node0-data` buffer of 3 float64
/// values.
#[tokio::test]
async fn awkward_export_to_file() {
    let base = spawn_server_with_root(build_awkward_root(), None).await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let ak = root.get("ak").await.unwrap().into_awkward().unwrap();

    let out = tempfile::tempdir().unwrap();

    // 1. Explicit media type: the zipped buffer archive (a ZIP begins with "PK").
    let explicit = out.path().join("explicit.zip");
    ak.export(&explicit, Some("application/zip"))
        .await
        .expect("export application/zip");
    assert!(
        std::fs::read(&explicit).unwrap().starts_with(b"PK"),
        "awkward export is a ZIP archive"
    );

    // 2. Format inferred from the `.zip` extension (format = None).
    let inferred = out.path().join("buffers.zip");
    ak.export(&inferred, None)
        .await
        .expect("export inferred zip");
    assert!(
        std::fs::read(&inferred).unwrap().starts_with(b"PK"),
        "inferred zip export is a ZIP archive"
    );

    // 3. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.zip");
    let err = ak.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

/// `SparseClient::export` GETs `/array/full?format=<fmt>` and writes the
/// serialized COO frame to a file. Same test shape as the array/container
/// export. The node is seeded via `write_sparse` with (0,1)=1.5, (2,0)=3.7.
#[tokio::test]
async fn sparse_export_to_file() {
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::SparseStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    let structure = SparseStructure {
        chunks: vec![vec![3], vec![3]],
        shape: vec![3, 3],
        data_type: Some(DType::Builtin(BuiltinDType::new(
            Endianness::Little,
            Kind::Float,
            8,
        ))),
        ..Default::default()
    };
    let sc = root
        .write_sparse(
            Some("exp_sparse"),
            structure,
            (&[vec![0, 2], vec![1, 0]], &[1.5, 3.7]),
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("write_sparse");

    let out = tempfile::tempdir().unwrap();

    // 1. Explicit media type: the JSON column-dict {dim0, dim1, data}.
    let explicit = out.path().join("explicit.json");
    sc.export(&explicit, Some("application/json"))
        .await
        .expect("export application/json");
    let json = std::fs::read_to_string(&explicit).unwrap();
    assert!(
        json.contains("data") && json.contains("1.5") && json.contains("3.7"),
        "sparse json export holds the COO frame: {json:?}"
    );

    // 2. Format inferred from the `.json` extension (format = None).
    let inferred = out.path().join("coo.json");
    sc.export(&inferred, None)
        .await
        .expect("export inferred json");
    assert!(
        std::fs::read_to_string(&inferred).unwrap().contains("3.7"),
        "inferred json export holds the data"
    );

    // 3. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.json");
    let err = sc.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

/// `RaggedClient::export` GETs `/ragged/full?format=<fmt>` and writes the
/// serialized list-of-lists to a file. Same test shape as the array/container
/// export. The node is seeded via `write_ragged` with `[[1, 2, 3], [4]]`.
#[tokio::test]
async fn ragged_export_to_file() {
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::RaggedStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // One chunk of 2 rows; 4 leaf elements total ([1,2,3] + [4]).
    let structure = RaggedStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Integer, 8)),
        shape: vec![Some(2), None],
        size: 4,
        chunks: vec![Some(vec![2]), None],
        dims: None,
        resizable: Default::default(),
    };
    let data = serde_json::json!([[1, 2, 3], [4]]);
    let rag = root
        .write_ragged(
            Some("exp_ragged"),
            structure,
            &data,
            serde_json::json!({}),
            vec![],
            None,
        )
        .await
        .expect("write_ragged");

    let out = tempfile::tempdir().unwrap();

    // 1. Explicit media type: the list-of-lists round-trips exactly.
    let explicit = out.path().join("explicit.json");
    rag.export(&explicit, Some("application/json"))
        .await
        .expect("export application/json");
    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&explicit).unwrap()).unwrap();
    assert_eq!(
        v,
        serde_json::json!([[1, 2, 3], [4]]),
        "ragged json export round-trips the rows"
    );

    // 2. Format inferred from the `.json` extension (format = None).
    let inferred = out.path().join("rows.json");
    rag.export(&inferred, None)
        .await
        .expect("export inferred json");
    let v2: serde_json::Value = serde_json::from_slice(&std::fs::read(&inferred).unwrap()).unwrap();
    assert_eq!(v2, serde_json::json!([[1, 2, 3], [4]]));

    // 3. Unknown format → mapped server error; nothing is written through.
    let bad = out.path().join("bad.json");
    let err = rag.export(&bad, Some("bogus")).await;
    assert!(err.is_err(), "unknown format maps to an error, got {err:?}");
    assert!(!bad.exists(), "no file written when the request fails");
}

// ---------------------------------------------------------------------------
// Blosc2 content-encoding tests
// ---------------------------------------------------------------------------

/// Build a catalog with two large arrays, both over the 500-byte floor:
/// - `big`  : 200 f64 = 1 600 bytes of a smooth ramp — compresses well
///            (lz4 ratio ~1.84, blosc2 ~1.29, both above the 1/0.9 gate).
/// - `noise`: 400 f64 = 3 200 bytes of deterministic high-entropy bits (an LCG
///            reinterpreted as f64) — does NOT compress below the ratio gate
///            (lz4 ratio ~0.99, blosc2 ~0.99), so it must be served identity.
fn build_large_array_catalog() -> MapAdapter {
    let data: Vec<f64> = (0..200).map(|i| i as f64 * 1.5).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));

    // Deterministic incompressible bytes: an LCG feeding f64::from_bits so all
    // eight bytes of every value vary. (from_f64_1d stores raw little-endian
    // bytes without interpreting them, so non-finite bit patterns are fine.)
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let noise: Vec<f64> = (0..400)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            f64::from_bits(state)
        })
        .collect();
    let noise_arr = ArrayAdapter::from_f64_1d(&noise, serde_json::json!({}));

    let mut mapping = IndexMap::new();
    mapping.insert("big".into(), AnyAdapter::Array(Arc::new(arr)));
    mapping.insert("noise".into(), AnyAdapter::Array(Arc::new(noise_arr)));
    MapAdapter::new(mapping, serde_json::json!({}), vec![])
}

/// Spin up a server whose root contains only the large array.
async fn spawn_blosc2_server() -> String {
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(build_large_array_catalog());
    let registry = Arc::new(tiled_rs::serialization::default_registry());

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
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

/// Key test: round-trip.  Client advertises blosc2, server compresses the
/// large array (1 600 bytes > 500 minimum), client decompresses, decoded
/// bytes equal the original f64 values.
#[tokio::test]
async fn blosc2_round_trip_decoded_bytes_equal_original() {
    let base = spawn_blosc2_server().await;
    let client = from_uri(&base).await.unwrap();
    let root = client.into_container().unwrap();

    let arr = root.get("big").await.unwrap().into_array().unwrap();
    let block = arr.read_block(&[0]).await.unwrap();

    // 200 f64 × 8 bytes = 1 600 bytes.
    assert_eq!(
        block.data.len(),
        200 * 8,
        "decoded length must match original"
    );

    let values: Vec<f64> = block
        .data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let expected: Vec<f64> = (0..200).map(|i| i as f64 * 1.5).collect();
    assert_eq!(values, expected, "decoded values must equal originals");
}

/// Verify the server actually sends `Content-Encoding: blosc2` when the
/// client advertises it and the body is large enough.
#[tokio::test]
async fn blosc2_server_sets_content_encoding_for_large_body() {
    let base = spawn_blosc2_server().await;

    // Make a raw HTTP request with Accept-Encoding: blosc2.
    let url = format!("{base}/api/v1/array/block/big?block=0");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "blosc2")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let enc = resp
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(enc, "blosc2", "server must set Content-Encoding: blosc2");
}

/// Small body (< 500 bytes) must NOT be blosc2-encoded even when the client
/// accepts it.  The existing `some_array` with 10 f64 = 80 bytes is used.
#[tokio::test]
async fn blosc2_small_body_not_encoded() {
    let base = spawn_server(None).await;

    let url = format!("{base}/api/v1/array/block/some_array?block=0");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "blosc2")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let enc = resp
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    assert_ne!(
        enc, "blosc2",
        "80-byte body (< 500 minimum) must NOT be blosc2-encoded"
    );
    // Body must still be the raw f64 bytes.
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 80, "raw 10×f64 body");
}

/// A client that does NOT advertise blosc2 must receive the raw uncompressed
/// body regardless of body size.
#[tokio::test]
async fn blosc2_not_requested_gets_uncompressed_body() {
    let base = spawn_blosc2_server().await;

    let url = format!("{base}/api/v1/array/block/big?block=0");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Accept", "application/octet-stream")
        // No Accept-Encoding header → server must not blosc2-compress.
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let enc = resp
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none");
    assert_ne!(
        enc, "blosc2",
        "server must not apply blosc2 when client did not request it"
    );
    // Body is the raw 200 f64 bytes.
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 200 * 8);
}

// ===========================================================================
// Server-Timing response header (upstream capture_metrics, app.py:855-888) and
// the lz4 content-encoding arm (media_type_registration.py:289-343). Reuses
// `spawn_blosc2_server` which serves a 1 600-byte octet-stream array ("big").
// ===========================================================================

/// The `app` phase is always emitted; assert the header is present and its
/// `app` entry matches the Server-Timing `name;dur=<ms>` format on a metadata
/// GET (application/json, no compression).
#[tokio::test]
async fn server_timing_header_present_on_metadata_get() {
    let base = spawn_server(None).await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/metadata/"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let header = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header must be present")
        .to_string();

    // Find the `app` phase and assert it is `app;dur=<number with one decimal>`.
    let app_phase = header
        .split(", ")
        .find(|p| p.starts_with("app;"))
        .unwrap_or_else(|| panic!("no app phase in Server-Timing: {header:?}"));
    let dur = app_phase
        .strip_prefix("app;dur=")
        .unwrap_or_else(|| panic!("app phase not `app;dur=<ms>`: {app_phase:?}"));
    // dur is a fixed one-decimal float, e.g. "1.2" or "0.0".
    let (whole, frac) = dur.split_once('.').expect("dur must have a decimal point");
    assert!(!whole.is_empty() && whole.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(frac.len(), 1, "dur must render with one decimal: {dur:?}");
    assert!(frac.chars().all(|c| c.is_ascii_digit()));
}

/// The header must also be stamped on a binary array-block GET.
#[tokio::test]
async fn server_timing_header_present_on_array_get() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let header = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header must be present on array GET");
    assert!(
        header.split(", ").any(|p| p.starts_with("app;dur=")),
        "array GET Server-Timing must contain an app phase: {header:?}"
    );
}

/// When compression is negotiated (blosc2 here), the `compress` phase —
/// recorded in the compression middleware with `dur` and `ratio` — must appear.
#[tokio::test]
async fn server_timing_compress_phase_when_encoding_negotiated() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "blosc2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").unwrap(),
        "blosc2",
        "precondition: blosc2 must be negotiated"
    );

    let header = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header must be present");
    let compress_phase = header
        .split(", ")
        .find(|p| p.starts_with("compress;"))
        .unwrap_or_else(|| panic!("no compress phase in Server-Timing: {header:?}"));
    // Upstream emits `compress;dur=<ms>;ratio=<n>`.
    assert!(
        compress_phase.contains("dur=") && compress_phase.contains("ratio="),
        "compress phase must carry dur and ratio: {compress_phase:?}"
    );
}

/// Round-trip: the client advertises lz4, the server compresses the 1 600-byte
/// array, and the same lz4 crate (block format with 4-byte LE size prefix,
/// matching python-lz4's `lz4.block.compress`) decodes it back to the original
/// f64 bytes.
#[tokio::test]
async fn lz4_round_trip_decode_with_same_crate() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "lz4");

    // reqwest does not know lz4, so the body arrives compressed.
    let compressed = resp.bytes().await.unwrap();
    let decoded = tiled_rs::server::lz4::decompress(&compressed).expect("lz4 decode");

    assert_eq!(decoded.len(), 200 * 8, "decoded length must match original");
    let values: Vec<f64> = decoded
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let expected: Vec<f64> = (0..200).map(|i| i as f64 * 1.5).collect();
    assert_eq!(values, expected, "decoded values must equal originals");
}

/// The lz4 middleware records its compression time into the Server-Timing
/// accumulator, so an lz4-negotiated response carries a `compress` phase.
#[tokio::test]
async fn lz4_emits_compress_server_timing_phase() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "lz4");

    let header = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .expect("Server-Timing header must be present");
    let compress_phase = header
        .split(", ")
        .find(|p| p.starts_with("compress;"))
        .unwrap_or_else(|| panic!("no compress phase in Server-Timing: {header:?}"));
    assert!(
        compress_phase.contains("dur=") && compress_phase.contains("ratio="),
        "lz4 compress phase must carry dur and ratio: {compress_phase:?}"
    );
}

/// Negotiation priority: upstream registers lz4 after gzip and zstd, so lz4 is
/// preferred over both. A client accepting all three must get lz4.
#[tokio::test]
async fn lz4_negotiation_priority_beats_gzip_and_zstd() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "gzip, zstd, lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").unwrap(),
        "lz4",
        "lz4 must win over gzip/zstd (registered later → preferred)"
    );
}

/// Negotiation priority: blosc2 is registered after lz4, so for octet-stream it
/// outranks lz4. A client accepting both must get blosc2, not lz4.
#[tokio::test]
async fn lz4_yields_to_blosc2_for_octet_stream() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4, blosc2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-encoding").unwrap(),
        "blosc2",
        "blosc2 must outrank lz4 for octet-stream"
    );
}

/// An encoding the server does not support must fall through to an
/// uncompressed, unencoded body (identity) — not lz4.
#[tokio::test]
async fn lz4_unsupported_encoding_falls_through() {
    let base = spawn_blosc2_server().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "made-up-encoding")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "unsupported encoding must not be compressed"
    );
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 200 * 8, "body must be the raw f64 bytes");
}

// ===========================================================================
// Compression-ratio threshold gate (upstream compression.py:87-93): compress
// first, then keep the result only if original/compressed > 1/0.9, applied
// UNIFORMLY to both the blosc2 and lz4 encoders. Boundary matrix below.
// ===========================================================================

/// Helper: does the Server-Timing header contain a `compress` phase?
fn has_compress_phase(resp: &reqwest::Response) -> bool {
    resp.headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.split(", ").any(|p| p.starts_with("compress;")))
        .unwrap_or(false)
}

/// Helper: does the Server-Timing header contain the always-present `app` phase?
fn has_app_phase(resp: &reqwest::Response) -> bool {
    resp.headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .map(|h| h.split(", ").any(|p| p.starts_with("app;dur=")))
        .unwrap_or(false)
}

/// Compressible body (the ramp `big`, ratio ~1.29) with blosc2 → compressed,
/// Content-Encoding set, compress phase present.
#[tokio::test]
async fn ratio_gate_compressible_blosc2_compresses_with_phase() {
    let base = spawn_blosc2_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "blosc2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "blosc2");
    assert!(has_app_phase(&resp), "app phase must always be present");
    assert!(
        has_compress_phase(&resp),
        "compressed response must carry a compress phase"
    );
}

/// Compressible body (`big`, lz4 ratio ~1.84) with lz4 → compressed,
/// Content-Encoding set, compress phase present.
#[tokio::test]
async fn ratio_gate_compressible_lz4_compresses_with_phase() {
    let base = spawn_blosc2_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/big?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "lz4");
    assert!(has_app_phase(&resp), "app phase must always be present");
    assert!(
        has_compress_phase(&resp),
        "compressed response must carry a compress phase"
    );
}

/// Incompressible body (`noise`, blosc2 ratio ~0.99, > 500-byte floor) with
/// blosc2 → served identity: no Content-Encoding, no compress phase, but the
/// app phase is still present and the body is intact.
#[tokio::test]
async fn ratio_gate_incompressible_blosc2_served_identity() {
    let base = spawn_blosc2_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/noise?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "blosc2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "below-threshold compression must not set Content-Encoding"
    );
    assert!(has_app_phase(&resp), "app phase must still be present");
    assert!(
        !has_compress_phase(&resp),
        "skipped compression must NOT record a compress phase"
    );
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 400 * 8, "body must be the raw f64 bytes");
}

/// Incompressible body (`noise`, lz4 ratio ~0.99) with lz4 → served identity:
/// no Content-Encoding, no compress phase, app phase present, body intact.
#[tokio::test]
async fn ratio_gate_incompressible_lz4_served_identity() {
    let base = spawn_blosc2_server().await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/noise?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "below-threshold compression must not set Content-Encoding"
    );
    assert!(has_app_phase(&resp), "app phase must still be present");
    assert!(
        !has_compress_phase(&resp),
        "skipped compression must NOT record a compress phase"
    );
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 400 * 8, "body must be the raw f64 bytes");
}

/// Just under the 500-byte floor (`some_array`, 80 bytes) with lz4 → the size
/// floor short-circuits before the ratio gate: no Content-Encoding, body
/// unchanged. (blosc2's under-floor case is covered by
/// `blosc2_small_body_not_encoded`.)
#[tokio::test]
async fn ratio_gate_under_size_floor_lz4_unchanged() {
    let base = spawn_server(None).await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/array/block/some_array?block=0"))
        .header("Accept", "application/octet-stream")
        .header("Accept-Encoding", "lz4")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(
        resp.headers().get("content-encoding").is_none(),
        "80-byte body (< 500 floor) must not be lz4-encoded"
    );
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 80, "raw 10×f64 body");
}

// ---------------------------------------------------------------------------
// Server-level sparse managed create + resolve: create a sparse node over a
// catalog (exercising default_creation_mimetype(Sparse) +
// managed_init_storage's sparse arm + init_storage_sparse_parquet), then
// resolve it back through the FileLeafResolver's sparse branch and read the
// COO data — the catalog sparse-resolution regression (before this wiring, a
// created sparse node could not be resolved to an adapter at all).
// ---------------------------------------------------------------------------

/// Build a `SparseData` from block-local `(coords, data)`: one int64 coord
/// column per dimension plus an f64 value column — the shape the sparse write
/// face consumes.
fn make_sparse_data(coords: Vec<Vec<i64>>, data: Vec<f64>) -> tiled_rs::core::adapters::SparseData {
    use tiled_rs::core::dtype::{BuiltinDType, DynNDArray, Endianness, Kind};
    let nnz = data.len();
    let i64_dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
    let f64_dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
    let coord_dyn: Vec<DynNDArray> = coords
        .into_iter()
        .map(|c| {
            let bytes: Vec<u8> = c.iter().flat_map(|v| v.to_le_bytes()).collect();
            DynNDArray::new(Bytes::from(bytes), i64_dtype.clone(), vec![nnz])
        })
        .collect();
    let data_bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let data_dyn = DynNDArray::new(Bytes::from(data_bytes), f64_dtype, vec![nnz]);
    tiled_rs::core::adapters::SparseData {
        coords: coord_dyn,
        data: data_dyn,
    }
}

#[tokio::test]
async fn sparse_managed_create_resolves_and_reads_back() {
    use tiled_rs::catalog::Catalog;
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, SparseStructure, StructureFamily};
    use tiled_rs::server::file_resolver::FileLeafResolver;

    // --- inline catalog-backed server, keeping the root_tree handle so the
    // test can drive create (HTTP) → resolve/write (server tree) → read (HTTP).
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
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root.clone()]));
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));

    let state = tiled_rs::server::AppState {
        root_tree: root_tree.clone(),
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        root_path: String::new(),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
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
    };
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Single-chunk 3×3 COO node → one block. No pinned mimetype: exercise
    // default_creation_mimetype(Sparse) → application/x-parquet;structure=sparse,
    // then managed_init_storage → init_storage_sparse_parquet.
    let structure = SparseStructure {
        chunks: vec![vec![3], vec![3]],
        shape: vec![3, 3],
        data_type: Some(DType::Builtin(BuiltinDType::new(
            Endianness::Little,
            Kind::Float,
            8,
        ))),
        ..Default::default()
    };
    let ds = DataSource {
        structure_family: StructureFamily::Sparse,
        structure: Some(AnyStructure::Sparse(structure)),
        id: None,
        mimetype: None,
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        Some("sp"),
        StructureFamily::Sparse,
        serde_json::json!({"note": "coo"}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create sparse node");

    // Resolve the created node through the server tree — this exercises the
    // FileLeafResolver sparse branch (build_sparse_blocks_adapter). The managed
    // node must resolve to a writable sparse adapter with the declared structure.
    let seg = vec!["sp".to_string()];
    let adapter = tiled_rs::server::core::walk_tree(root_tree.as_ref(), &seg)
        .await
        .expect("resolve created sparse node");
    let sparse = adapter
        .as_sparse_arc()
        .expect("managed sparse node resolves to a sparse adapter");
    assert_eq!(sparse.structure().shape, vec![3, 3], "resolved shape");
    assert_eq!(
        sparse.structure().chunks,
        vec![vec![3], vec![3]],
        "resolved chunk grid"
    );
    let writable = sparse
        .as_writable()
        .expect("a managed sparse node under writable storage is writable");

    // Write the single block: (0,1)=1.5 and (2,0)=3.7.
    writable
        .write(make_sparse_data(
            vec![vec![0, 2], vec![1, 0]],
            vec![1.5, 3.7],
        ))
        .await
        .expect("write sparse block");

    // Read back through the HTTP client — the server resolves the node again
    // (through build_sparse_blocks_adapter) and serves the COO table.
    let node = root.get("sp").await.unwrap();
    let sc = node.as_sparse().expect("sp resolves to a sparse client");
    let block = sc.read().await.expect("read sparse COO");
    assert_eq!(block.shape, vec![3, 3]);
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by_key(|a| a.0);
    assert_eq!(
        got,
        vec![((0, 1), 1.5), ((2, 0), 3.7)],
        "read-back COO must match what was written to the resolved managed node"
    );
}

// ---------------------------------------------------------------------------
// Scope: PUT /array/full + /array/block sparse arms (Arrow COO deserializer)
//
// The sparse write face is reachable over HTTP: the array PUT routes accept a
// sparse leaf, deserialize the Arrow IPC COO body (dim0…dim{ndim-1} + data)
// through `deserialize_sparse_coo`, and persist it to the per-block parquet
// files. These tests drive a raw Arrow IPC PUT (no client write yet — that is
// the next layer) and read the frame back through the existing GET/serve path.
// ---------------------------------------------------------------------------

/// Encode a COO table — columns `dim0`…`dim{ndim-1}` (Int64) plus `data`
/// (Float64) — as an Arrow IPC *file*, the wire body Python `client/sparse.py`
/// produces (client/sparse.py:107) and the server's sparse PUT arm consumes.
fn encode_coo_arrow(coords: Vec<Vec<i64>>, data: Vec<f64>) -> Vec<u8> {
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let mut fields: Vec<Field> = Vec::with_capacity(coords.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(coords.len() + 1);
    for (i, c) in coords.iter().enumerate() {
        fields.push(Field::new(format!("dim{i}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(c.clone())) as ArrayRef);
    }
    fields.push(Field::new("data", DataType::Float64, false));
    columns.push(Arc::new(Float64Array::from(data)) as ArrayRef);

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buf
}

/// Create a managed (writable) sparse f64 node with the given shape/chunk grid
/// and return the root container for follow-up reads.
async fn create_managed_sparse(
    base: &str,
    name: &str,
    shape: Vec<usize>,
    chunks: Vec<Vec<usize>>,
) -> tiled_rs::client::ContainerClient {
    use tiled_rs::core::data_source::{DataSource, Management};
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::{AnyStructure, SparseStructure, StructureFamily};

    let root = from_uri(base).await.unwrap().into_container().unwrap();
    let structure = SparseStructure {
        chunks,
        shape,
        data_type: Some(DType::Builtin(BuiltinDType::new(
            Endianness::Little,
            Kind::Float,
            8,
        ))),
        ..Default::default()
    };
    let ds = DataSource {
        structure_family: StructureFamily::Sparse,
        structure: Some(AnyStructure::Sparse(structure)),
        id: None,
        mimetype: None,
        parameters: serde_json::json!({}),
        properties: serde_json::json!({}),
        assets: vec![],
        management: Management::Writable,
    };
    root.create_node(
        Some(name),
        StructureFamily::Sparse,
        serde_json::json!({}),
        vec![],
        vec![ds],
    )
    .await
    .expect("create sparse node");
    root
}

const ARROW_FILE_CT: &str = "application/vnd.apache.arrow.file";

/// PUT /array/full on a single-block sparse node: an Arrow IPC COO body is
/// deserialized, written to the block parquet, and read back identically.
#[tokio::test]
async fn sparse_put_array_full_arrow_roundtrips() {
    let (base, _wd, _db) = spawn_write_server().await;
    let root = create_managed_sparse(&base, "sp_full", vec![3, 3], vec![vec![3], vec![3]]).await;

    // Whole 3×3 array as one COO block: (0,1)=1.5, (2,0)=3.7.
    let body = encode_coo_arrow(vec![vec![0, 2], vec![1, 0]], vec![1.5, 3.7]);
    let resp = reqwest::Client::new()
        .put(format!("{base}/api/v1/array/full/sp_full"))
        .header("Content-Type", ARROW_FILE_CT)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "PUT /array/full sparse arm must accept COO"
    );

    // GET reads the parquet back through the sparse serve path.
    let node = root.get("sp_full").await.unwrap();
    let sc = node
        .as_sparse()
        .expect("sp_full resolves to a sparse client");
    let block = sc.read().await.expect("read sparse COO");
    assert_eq!(block.shape, vec![3, 3]);
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by_key(|a| a.0);
    assert_eq!(
        got,
        vec![((0, 1), 1.5), ((2, 0), 3.7)],
        "PUT-then-GET COO must round-trip through the Arrow deserializer"
    );
}

/// PUT /array/block on a multi-block sparse node: each block's local COO is
/// written independently; GET /array/full reassembles the global frame with the
/// block-origin coordinate offsets applied.
#[tokio::test]
async fn sparse_put_array_block_arrow_roundtrips() {
    let (base, _wd, _db) = spawn_write_server().await;
    // 4×2, two blocks along axis 0: block [0,0] covers rows 0..2, [1,0] rows 2..4.
    let root = create_managed_sparse(&base, "sp_blk", vec![4, 2], vec![vec![2, 2], vec![2]]).await;

    let client = reqwest::Client::new();
    // Block [0,0]: local (0,1)=5.0 -> global (0,1).
    let r0 = client
        .put(format!("{base}/api/v1/array/block/sp_blk?block=0,0"))
        .header("Content-Type", ARROW_FILE_CT)
        .body(encode_coo_arrow(vec![vec![0], vec![1]], vec![5.0]))
        .send()
        .await
        .unwrap();
    assert_eq!(r0.status(), 200, "PUT block 0,0");
    // Block [1,0]: local (1,0)=9.0 -> global (1+2, 0) = (3,0).
    let r1 = client
        .put(format!("{base}/api/v1/array/block/sp_blk?block=1,0"))
        .header("Content-Type", ARROW_FILE_CT)
        .body(encode_coo_arrow(vec![vec![1], vec![0]], vec![9.0]))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200, "PUT block 1,0");

    // GET full reassembles both blocks into one global COO frame.
    let node = root.get("sp_blk").await.unwrap();
    let sc = node
        .as_sparse()
        .expect("sp_blk resolves to a sparse client");
    let block = sc.read().await.expect("read sparse COO");
    assert_eq!(block.shape, vec![4, 2]);
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by_key(|a| a.0);
    assert_eq!(
        got,
        vec![((0, 1), 5.0), ((3, 0), 9.0)],
        "block-local writes must reassemble with chunk-origin offsets on read"
    );
}

// ---------------------------------------------------------------------------
// Scope: client sparse write (ContainerClient::write_sparse,
// SparseClient::write/write_block, AnyClient::into_sparse)
//
// The full client → server → parquet → client loop, driven entirely through the
// typed client (no hand-rolled Arrow body). Exercises the encode side of the
// COO wire that commit 4's server deserializer consumes.
// ---------------------------------------------------------------------------

/// `ContainerClient::write_sparse` creates a managed single-block sparse node
/// and uploads its non-zeros; the returned `SparseClient` reads them back.
#[tokio::test]
async fn container_write_sparse_roundtrips() {
    use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_rs::core::structures::SparseStructure;

    let (base, _wd, _db) = spawn_write_server().await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Single-chunk 3×3 COO: (0,1)=1.5, (2,0)=3.7.
    let structure = SparseStructure {
        chunks: vec![vec![3], vec![3]],
        shape: vec![3, 3],
        data_type: Some(DType::Builtin(BuiltinDType::new(
            Endianness::Little,
            Kind::Float,
            8,
        ))),
        ..Default::default()
    };
    let sc = root
        .write_sparse(
            Some("sp_client"),
            structure,
            (&[vec![0, 2], vec![1, 0]], &[1.5, 3.7]),
            serde_json::json!({"note": "coo"}),
            vec![],
            None,
        )
        .await
        .expect("write_sparse");

    let block = sc.read().await.expect("read back through returned client");
    assert_eq!(block.shape, vec![3, 3]);
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by_key(|a| a.0);
    assert_eq!(got, vec![((0, 1), 1.5), ((2, 0), 3.7)]);

    // The coordinate-count guard rejects a structure/coords dimensionality
    // mismatch before any request goes out.
    let err = sc.write(&[vec![0, 1]], &[1.0, 2.0]).await;
    assert!(
        matches!(err, Err(tiled_rs::client::ClientError::Invalid(_))),
        "1 coord column for a 2-D array must be a client-side Invalid error, got {err:?}"
    );
}

/// `SparseClient::write_block` fills a multi-block node one block at a time
/// (via `AnyClient::into_sparse`); `read` reassembles the global frame.
#[tokio::test]
async fn sparse_client_write_block_roundtrips() {
    let (base, _wd, _db) = spawn_write_server().await;
    // 4×2, two blocks along axis 0.
    let root =
        create_managed_sparse(&base, "sp_cli_blk", vec![4, 2], vec![vec![2, 2], vec![2]]).await;

    let sc = root
        .get("sp_cli_blk")
        .await
        .unwrap()
        .into_sparse()
        .expect("into_sparse");

    // Block [0,0]: local (0,1)=5.0 -> global (0,1).
    sc.write_block(&[0, 0], &[vec![0], vec![1]], &[5.0])
        .await
        .expect("write_block 0,0");
    // Block [1,0]: local (1,0)=9.0 -> global (3,0).
    sc.write_block(&[1, 0], &[vec![1], vec![0]], &[9.0])
        .await
        .expect("write_block 1,0");

    let block = sc.read().await.expect("read back");
    assert_eq!(block.shape, vec![4, 2]);
    let mut got: Vec<((i64, i64), f64)> = (0..block.data.len())
        .map(|i| ((block.coords[0][i], block.coords[1][i]), block.data[i]))
        .collect();
    got.sort_by_key(|a| a.0);
    assert_eq!(got, vec![((0, 1), 5.0), ((3, 0), 9.0)]);
}

// ---------------------------------------------------------------------------
// Container navigation: nested-path get + lazy paginated keys/values/items
// (Wave-27 batch-1). Both drive the real server via `spawn_server_with_root`.
// ---------------------------------------------------------------------------

/// `root → a (container) → b (container) → c (array [1,2,3])`. A nested key
/// `"a/b/c"` must walk three path segments, not collapse into one.
fn build_nested_root() -> Arc<dyn ContainerAdapter> {
    let c_arr = ArrayAdapter::from_f64_1d(&[1.0, 2.0, 3.0], serde_json::json!({}));
    let mut b_map = IndexMap::new();
    b_map.insert("c".to_string(), AnyAdapter::Array(Arc::new(c_arr)));
    let b = MapAdapter::new(b_map, serde_json::json!({"depth": 2}), vec![]);

    let mut a_map = IndexMap::new();
    a_map.insert("b".to_string(), AnyAdapter::Container(Arc::new(b)));
    let a = MapAdapter::new(a_map, serde_json::json!({"depth": 1}), vec![]);

    let mut root = IndexMap::new();
    root.insert("a".to_string(), AnyAdapter::Container(Arc::new(a)));
    Arc::new(MapAdapter::new(root, serde_json::json!({}), vec![]))
}

/// A wide container of `n` array children named `k0..k{n-1}`, in insertion
/// order, for exercising paginated listing.
fn build_wide_root(n: usize) -> Arc<dyn ContainerAdapter> {
    let mut root = IndexMap::new();
    for i in 0..n {
        let arr = ArrayAdapter::from_f64_1d(&[i as f64], serde_json::json!({ "i": i }));
        root.insert(format!("k{i}"), AnyAdapter::Array(Arc::new(arr)));
    }
    Arc::new(MapAdapter::new(root, serde_json::json!({}), vec![]))
}

fn decode_f64s(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// FINDING 1: `get("a/b/c")` resolves as a per-segment path walk (previously the
/// slashes were percent-encoded into one `a%2Fb%2Fc` segment → 404).
#[tokio::test]
async fn nested_get_walks_multi_segment_path() {
    use tiled_rs::core::ndslice::NDSlice;

    let base = spawn_server_with_root(build_nested_root(), None).await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // Slash-containing key resolves the leaf array.
    let arr = root
        .get("a/b/c")
        .await
        .expect("nested get resolves")
        .into_array()
        .expect("leaf is an array");
    let block = arr.read_slice(&NDSlice::empty()).await.unwrap();
    assert_eq!(block.shape, vec![3]);
    assert_eq!(decode_f64s(&block.data), vec![1.0, 2.0, 3.0]);

    // An intermediate segment resolves the container it names.
    let mid = root
        .get("a/b")
        .await
        .expect("intermediate get resolves")
        .into_container()
        .expect("a/b is a container");
    assert_eq!(mid.keys().await.unwrap(), vec!["c".to_string()]);

    // Step-wise walk is equivalent to the single-request nested walk.
    let stepwise = root
        .get("a")
        .await
        .unwrap()
        .into_container()
        .unwrap()
        .get("b")
        .await
        .unwrap()
        .into_container()
        .unwrap()
        .get("c")
        .await
        .unwrap()
        .into_array()
        .unwrap();
    let sblock = stepwise.read_slice(&NDSlice::empty()).await.unwrap();
    assert_eq!(decode_f64s(&sblock.data), vec![1.0, 2.0, 3.0]);
}

/// FINDING 1: leading/trailing slashes are trimmed (Python `.strip("/")`), and a
/// missing nested segment surfaces as an error rather than resolving wrongly.
#[tokio::test]
async fn nested_get_trims_slashes_and_reports_missing() {
    use tiled_rs::core::ndslice::NDSlice;

    let base = spawn_server_with_root(build_nested_root(), None).await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    // `/a/b/c/` trims to the same three segments as `a/b/c`.
    let arr = root
        .get("/a/b/c/")
        .await
        .expect("trimmed nested get resolves")
        .into_array()
        .unwrap();
    let block = arr.read_slice(&NDSlice::empty()).await.unwrap();
    assert_eq!(decode_f64s(&block.data), vec![1.0, 2.0, 3.0]);

    // A missing final segment is an error, not a silent resolve.
    let err = root.get("a/b/nope").await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("not found") || msg.contains("404") || msg.contains("400"),
        "unexpected error for missing nested key: {msg}"
    );

    // Single-segment behavior is unchanged: `get("a")` returns the container.
    assert!(root.get("a").await.unwrap().as_container().is_some());
}

/// FINDING 2: `keys_view()` fetches page by page; the lazy sequence equals the
/// eager `keys()`, and `first`/`head` grab bounded prefixes.
#[tokio::test]
async fn keys_view_lazy_paginates_all_children() {
    let base = spawn_server_with_root(build_wide_root(7), None).await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();

    let eager = root.keys().await.unwrap();
    assert_eq!(eager.len(), 7, "wide root has 7 children");

    // A small page size forces multiple fetches; the lazy view must yield the
    // exact same sequence as the eager keys().
    let mut view = root.keys_view().page_size(3);
    let mut lazy = Vec::new();
    while let Some(k) = view.next().await.unwrap() {
        lazy.push(k);
    }
    assert_eq!(lazy, eager, "lazy pagination == eager listing");

    // first() → the first key.
    let first = root.keys_view().first().await.unwrap();
    assert_eq!(first.as_deref(), eager.first().map(String::as_str));

    // head(n) → the first n keys, even when the page size is smaller than n.
    let head = root.keys_view().page_size(2).head(4).await.unwrap();
    assert_eq!(head.as_slice(), &eager[..4]);

    // head past the end clamps to what exists; head(0) does no work.
    assert_eq!(root.keys_view().head(100).await.unwrap(), eager);
    assert!(root.keys_view().head(0).await.unwrap().is_empty());
}

/// FINDING 2: `values_view()` / `items_view()` lazily yield child clients and
/// `(name, client)` pairs, in the same order as `keys()`.
#[tokio::test]
async fn values_and_items_views_yield_children_lazily() {
    let base = spawn_server_with_root(build_wide_root(5), None).await;
    let root = from_uri(&base).await.unwrap().into_container().unwrap();
    let eager = root.keys().await.unwrap();
    assert_eq!(eager.len(), 5);

    // values(): AnyClients, each an array child.
    let mut vview = root.values_view().page_size(2);
    let mut vcount = 0;
    while let Some(v) = vview.next().await.unwrap() {
        assert!(v.as_array().is_some(), "each child is an array");
        vcount += 1;
    }
    assert_eq!(vcount, 5);

    // items(): (name, client) pairs whose names match keys() order.
    let mut iview = root.items_view().page_size(2);
    let mut names = Vec::new();
    while let Some((name, client)) = iview.next().await.unwrap() {
        assert!(client.as_array().is_some());
        names.push(name);
    }
    assert_eq!(names, eager, "items() names match keys() order");

    // Conveniences on the value/item views.
    let (fname, fclient) = root.items_view().first().await.unwrap().unwrap();
    assert_eq!(Some(fname), eager.first().cloned());
    assert!(fclient.as_array().is_some());
    assert_eq!(root.values_view().head(2).await.unwrap().len(), 2);
}
