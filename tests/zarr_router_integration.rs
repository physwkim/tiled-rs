//! Integration tests for the read-only Zarr protocol routers (`/zarr/v2`,
//! `/zarr/v3`) — a port of upstream tiled `tiled/server/zarr.py`
//! (`app.py:419-420`).
//!
//! Drives the routers over HTTP (tower `oneshot`) against an in-memory
//! `MapAdapter` tree holding arrays and a nested container. Covers: v2 group
//! doc, v2 array doc + chunk-byte roundtrip vs the adapter's own `read_block`,
//! boundary-chunk zero padding, v3 array doc + chunk, container listings,
//! `.zattrs`/attributes, 404 on missing/misfamilied keys, and auth enforcement.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrayAdapter, CooAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ArrayAdapterRead, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::ndslice::NDSlice;
use tiled_rs::core::queries::Query;

fn f64_le(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// `arr`: 1-D f64 length 4 on a 2-chunk grid `[[2, 2]]`, values 10..40, with an
/// `attributes` metadata section for the `.zattrs` test.
fn build_arr() -> Arc<ArrayAdapter> {
    Arc::new(ArrayAdapter::from_array(
        Bytes::from(f64_le(&[10.0, 20.0, 30.0, 40.0])),
        BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        vec![4],
        vec![vec![2, 2]],
        serde_json::json!({"attributes": {"units": "m"}}),
        vec![],
    ))
}

/// `arr3`: 1-D f64 length 3 on chunks `[[2, 1]]` → zarr chunk size 2, so chunk 1
/// is a boundary chunk that must be zero-padded to length 2.
fn build_arr3() -> Arc<ArrayAdapter> {
    Arc::new(ArrayAdapter::from_array(
        Bytes::from(f64_le(&[1.0, 2.0, 3.0])),
        BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        vec![3],
        vec![vec![2, 1]],
        serde_json::json!({}),
        vec![],
    ))
}

fn build_root(arr: Arc<ArrayAdapter>, arr3: Arc<ArrayAdapter>) -> Arc<dyn ContainerAdapter> {
    let mut mapping = IndexMap::new();
    mapping.insert("arr".into(), AnyAdapter::Array(arr));
    mapping.insert("arr3".into(), AnyAdapter::Array(arr3));

    let mut inner = IndexMap::new();
    let nested = ArrayAdapter::from_f64_1d(&[1.0, 2.0, 3.0], serde_json::json!({}));
    inner.insert("nested_arr".into(), AnyAdapter::Array(Arc::new(nested)));
    let subgroup = MapAdapter::new(inner, serde_json::json!({"nested": true}), vec![]);
    mapping.insert("subgroup".into(), AnyAdapter::Container(Arc::new(subgroup)));

    Arc::new(MapAdapter::new(
        mapping,
        serde_json::json!({"description": "zarr test"}),
        vec![],
    ))
}

fn f64_dtype() -> BuiltinDType {
    BuiltinDType::new(Endianness::Little, Kind::Float, 8)
}

/// `sparse_known`: 3x3 sparse array, one whole-array chunk (`CooAdapter::from_arrays`),
/// two known non-zeros: (0,1)=1.5, (2,0)=3.7 — for the exact-dense-bytes test.
fn build_sparse_known() -> Arc<CooAdapter> {
    Arc::new(
        CooAdapter::from_arrays(
            vec![vec![0i64, 2], vec![1i64, 0]],
            Bytes::from(f64_le(&[1.5, 3.7])),
            f64_dtype(),
            vec![3, 3],
            None,
            serde_json::json!({}),
            vec![],
        )
        .unwrap(),
    )
}

/// `sparse_boundary`: 1-D shape `[5]` on chunks `[[3, 2]]` → zarr chunk size 3,
/// so chunk 1 covers `[3:6)`, clipped to `[3:5)` at the array's edge and
/// zero-padded up to length 3. One non-zero at global index 4 (chunk 1, local
/// index 1); chunk 0 has no non-zeros at all.
fn build_sparse_boundary() -> Arc<CooAdapter> {
    Arc::new(
        CooAdapter::from_blocks(
            vec![5],
            vec![vec![3, 2]],
            f64_dtype(),
            None,
            serde_json::json!({}),
            vec![],
            vec![(vec![1], vec![vec![1i64]], Bytes::from(f64_le(&[9.0])))],
        )
        .unwrap(),
    )
}

/// `sparse_empty`: 1-D shape `[9]` on chunks `[[3, 3, 3]]` → zarr chunk size 3,
/// exactly tiling the shape (no boundary padding). Non-zeros only in chunks 0
/// and 1 (global indices 1 and 4); chunk 2 (`[6:9)`) has no non-zeros anywhere
/// — a fully empty chunk, distinct from the boundary-padding case above.
fn build_sparse_empty_chunk() -> Arc<CooAdapter> {
    Arc::new(
        CooAdapter::from_blocks(
            vec![9],
            vec![vec![3, 3, 3]],
            f64_dtype(),
            None,
            serde_json::json!({}),
            vec![],
            vec![
                (vec![0], vec![vec![1i64]], Bytes::from(f64_le(&[5.0]))),
                (vec![1], vec![vec![1i64]], Bytes::from(f64_le(&[7.0]))),
            ],
        )
        .unwrap(),
    )
}

fn build_sparse_root() -> Arc<dyn ContainerAdapter> {
    let mut mapping = IndexMap::new();
    mapping.insert(
        "sparse_known".into(),
        AnyAdapter::Sparse(build_sparse_known()),
    );
    mapping.insert(
        "sparse_boundary".into(),
        AnyAdapter::Sparse(build_sparse_boundary()),
    );
    mapping.insert(
        "sparse_empty".into(),
        AnyAdapter::Sparse(build_sparse_empty_chunk()),
    );
    Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]))
}

fn sparse_app() -> axum::Router {
    build_app(build_sparse_root(), None)
}

fn build_app(root: Arc<dyn ContainerAdapter>, api_key: Option<String>) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".into()),
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
    tiled_rs::server::build_app(state)
}

fn default_app() -> axum::Router {
    build_app(build_root(build_arr(), build_arr3()), None)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    get_with_header(app, uri, None).await
}

async fn get_with_header(
    app: &axum::Router,
    uri: &str,
    header: Option<(&str, &str)>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder().method(Method::GET).uri(uri);
    if let Some((k, v)) = header {
        builder = builder.header(k, v);
    }
    let req = builder.body(Body::empty()).unwrap();
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

fn f64s(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

// ---------------------------------------------------------------------------
// v2
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v2_group_doc() {
    let app = default_app();

    // Container → {"zarr_format": 2}.
    let (status, body) = get_json(&app, "/zarr/v2/subgroup/.zgroup").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"zarr_format": 2}));

    // Root container also answers .zgroup.
    let (status, body) = get_json(&app, "/zarr/v2/.zgroup").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"zarr_format": 2}));

    // An array is NOT a group → 404 (structure-family filter, upstream parity).
    let (status, _) = get(&app, "/zarr/v2/arr/.zgroup").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn v2_array_doc_and_chunk_roundtrip() {
    let arr = build_arr();
    let app = build_app(build_root(arr.clone(), build_arr3()), None);

    // .zarray metadata — uncompressed (compressor: null), C order.
    let (status, doc) = get_json(&app, "/zarr/v2/arr/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<f8");
    assert_eq!(doc["chunks"], serde_json::json!([2]));
    assert_eq!(doc["shape"], serde_json::json!([4]));
    assert_eq!(doc["order"], "C");
    assert_eq!(doc["compressor"], serde_json::Value::Null);
    assert_eq!(doc["fill_value"], serde_json::Value::Null);
    assert_eq!(doc["filters"], serde_json::Value::Null);

    // Chunk 0 bytes must equal the adapter's own read_block(0) — the roundtrip.
    let expected0 = arr.read_block(&[0], &NDSlice::empty()).await.unwrap();
    let (status, chunk0) = get(&app, "/zarr/v2/arr/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(chunk0.as_slice(), expected0.data.as_ref());
    assert_eq!(f64s(&chunk0), vec![10.0, 20.0]);

    // Chunk 1 too.
    let expected1 = arr.read_block(&[1], &NDSlice::empty()).await.unwrap();
    let (status, chunk1) = get(&app, "/zarr/v2/arr/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(chunk1.as_slice(), expected1.data.as_ref());
    assert_eq!(f64s(&chunk1), vec![30.0, 40.0]);
}

#[tokio::test]
async fn v2_boundary_chunk_is_zero_padded() {
    let app = default_app();
    // arr3 shape [3], zarr chunk size 2 → chunk 1 covers [2:4], clipped to [2:3]
    // then padded to length 2 with a trailing zero.
    let (status, chunk1) = get(&app, "/zarr/v2/arr3/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk1), vec![3.0, 0.0]);
}

#[tokio::test]
async fn v2_container_listing_lists_children() {
    let app = default_app();
    let (status, body) = get_json(&app, "/zarr/v2/subgroup").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert_eq!(urls.len(), 1);
    assert!(
        urls[0].ends_with("/zarr/v2/subgroup/nested_arr"),
        "{urls:?}"
    );
}

#[tokio::test]
async fn v2_zattrs_returns_attributes_section() {
    let app = default_app();
    let (status, body) = get_json(&app, "/zarr/v2/arr/.zattrs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"units": "m"}));

    // A node without an "attributes" section yields {}.
    let (status, body) = get_json(&app, "/zarr/v2/arr3/.zattrs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({}));
}

// ---------------------------------------------------------------------------
// v3
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v3_array_doc_and_chunk() {
    let arr = build_arr();
    let app = build_app(build_root(arr.clone(), build_arr3()), None);

    let (status, doc) = get_json(&app, "/zarr/v3/arr/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 3);
    assert_eq!(doc["node_type"], "array");
    assert_eq!(doc["data_type"], "float64");
    assert_eq!(doc["shape"], serde_json::json!([4]));
    assert_eq!(
        doc["chunk_grid"]["configuration"]["chunk_shape"],
        serde_json::json!([2])
    );
    assert_eq!(doc["chunk_key_encoding"]["configuration"]["separator"], "/");
    assert_eq!(doc["fill_value"], serde_json::json!(0.0));
    assert_eq!(doc["codecs"][0]["name"], "bytes");
    // Whole metadata is echoed under "attributes" (v3 semantics differ from v2).
    assert_eq!(
        doc["attributes"],
        serde_json::json!({"attributes": {"units": "m"}})
    );

    // v3 chunk key `c/0` → same bytes as the adapter's block 0.
    let expected0 = arr.read_block(&[0], &NDSlice::empty()).await.unwrap();
    let (status, chunk0) = get(&app, "/zarr/v3/arr/c/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(chunk0.as_slice(), expected0.data.as_ref());
    assert_eq!(f64s(&chunk0), vec![10.0, 20.0]);

    let (status, chunk1) = get(&app, "/zarr/v3/arr/c/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk1), vec![30.0, 40.0]);
}

#[tokio::test]
async fn v3_group_doc_and_listing() {
    let app = default_app();

    let (status, doc) = get_json(&app, "/zarr/v3/subgroup/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 3);
    assert_eq!(doc["node_type"], "group");
    assert_eq!(doc["attributes"], serde_json::json!({"nested": true}));

    let (status, body) = get_json(&app, "/zarr/v3/subgroup").await;
    assert_eq!(status, StatusCode::OK);
    let urls: Vec<String> = serde_json::from_value(body).unwrap();
    assert!(
        urls[0].ends_with("/zarr/v3/subgroup/nested_arr"),
        "{urls:?}"
    );
}

// ---------------------------------------------------------------------------
// sparse arrays — densified via `SparseData::densify` (core/adapters.rs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v2_sparse_array_doc_and_chunk_known_coo_points() {
    let app = sparse_app();

    // .zarray metadata — same shape as the dense-array doc, dtype/shape/chunks
    // sourced from the sparse structure.
    let (status, doc) = get_json(&app, "/zarr/v2/sparse_known/.zarray").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 2);
    assert_eq!(doc["dtype"], "<f8");
    assert_eq!(doc["chunks"], serde_json::json!([3, 3]));
    assert_eq!(doc["shape"], serde_json::json!([3, 3]));
    assert_eq!(doc["order"], "C");
    assert_eq!(doc["compressor"], serde_json::Value::Null);
    assert_eq!(doc["fill_value"], serde_json::Value::Null);

    // Dense C-order bytes for the whole 3x3 array (one chunk, "0.0"), zeros
    // included: (0,1)=1.5 and (2,0)=3.7, everything else implicit zero.
    let (status, chunk) = get(&app, "/zarr/v2/sparse_known/0.0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        f64s(&chunk),
        vec![0.0, 1.5, 0.0, 0.0, 0.0, 0.0, 3.7, 0.0, 0.0]
    );
}

#[tokio::test]
async fn v3_sparse_array_doc_and_chunk() {
    let app = sparse_app();

    let (status, doc) = get_json(&app, "/zarr/v3/sparse_known/zarr.json").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["zarr_format"], 3);
    assert_eq!(doc["node_type"], "array");
    assert_eq!(doc["data_type"], "float64");
    assert_eq!(doc["shape"], serde_json::json!([3, 3]));
    assert_eq!(
        doc["chunk_grid"]["configuration"]["chunk_shape"],
        serde_json::json!([3, 3])
    );
    assert_eq!(doc["chunk_key_encoding"]["configuration"]["separator"], "/");
    // A sparse array's implicit fill value is its zero element.
    assert_eq!(doc["fill_value"], serde_json::json!(0.0));
    assert_eq!(doc["codecs"][0]["name"], "bytes");

    let (status, chunk) = get(&app, "/zarr/v3/sparse_known/c/0/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        f64s(&chunk),
        vec![0.0, 1.5, 0.0, 0.0, 0.0, 0.0, 3.7, 0.0, 0.0]
    );
}

#[tokio::test]
async fn sparse_boundary_chunk_is_zero_padded() {
    let app = sparse_app();
    // shape [5], zarr chunk size 3 -> chunk 1 covers global [3,6), clipped to
    // [3,5): local 0 = global 3 (implicit zero), local 1 = global 4 (9.0),
    // local 2 is padding past the array's actual end (there is no global 5).
    let (status, chunk1) = get(&app, "/zarr/v2/sparse_boundary/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk1), vec![0.0, 9.0, 0.0]);

    // Chunk 0 [0,3) is a full, non-boundary chunk with no non-zeros at all.
    let (status, chunk0) = get(&app, "/zarr/v2/sparse_boundary/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk0), vec![0.0, 0.0, 0.0]);

    // v3 chunk key for the same boundary chunk agrees.
    let (status, chunk1_v3) = get(&app, "/zarr/v3/sparse_boundary/c/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk1_v3), vec![0.0, 9.0, 0.0]);
}

#[tokio::test]
async fn sparse_chunk_with_no_nonzeros_is_all_zero() {
    let app = sparse_app();
    // shape [9], chunks [3,3,3] tile exactly (no boundary padding involved).
    // Chunk 2 ([6,9)) has no non-zeros anywhere in the array's data.
    let (status, chunk2) = get(&app, "/zarr/v2/sparse_empty/2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk2), vec![0.0, 0.0, 0.0]);

    // Sanity: the other two chunks do carry their known values, so chunk 2's
    // zeros are not a symptom of the whole array reading as empty.
    let (status, chunk0) = get(&app, "/zarr/v2/sparse_empty/0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk0), vec![0.0, 5.0, 0.0]);

    let (status, chunk1) = get(&app, "/zarr/v2/sparse_empty/1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f64s(&chunk1), vec![0.0, 7.0, 0.0]);
}

// ---------------------------------------------------------------------------
// 404s
// ---------------------------------------------------------------------------

#[tokio::test]
async fn missing_key_returns_404() {
    let app = default_app();

    for uri in [
        "/zarr/v2/nope/.zarray",
        "/zarr/v2/nope/.zattrs",
        "/zarr/v2/nope/0",
        "/zarr/v3/nope/zarr.json",
        "/zarr/v3/nope/c/0",
    ] {
        let (status, _) = get(&app, uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "expected 404 for {uri}");
    }

    // .zarray on a container is a family mismatch → 404 (upstream parity).
    let (status, _) = get(&app, "/zarr/v2/subgroup/.zarray").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auth_enforced_on_zarr_routes() {
    let app = build_app(build_root(build_arr(), build_arr3()), Some("secret".into()));

    // No credential → 401 from the auth middleware.
    let (status, _) = get(&app, "/zarr/v2/arr/.zarray").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Correct key via query param → 200.
    let (status, _) = get(&app, "/zarr/v2/arr/.zarray?api_key=secret").await;
    assert_eq!(status, StatusCode::OK);

    // Correct key via Authorization header → 200.
    let (status, _) = get_with_header(
        &app,
        "/zarr/v2/arr/.zarray",
        Some(("authorization", "Apikey secret")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Wrong key → 401.
    let (status, _) = get(&app, "/zarr/v2/arr/.zarray?api_key=wrong").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
