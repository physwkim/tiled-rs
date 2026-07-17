//! Non-arrow field projection on `/container/full` — the `?field=`/`?column=`
//! GET query and the bare-list POST body restrict which top-level children each
//! non-arrow format serializes (json, json-seq/html, zip, hdf5).
//!
//! Upstream applies `entry.read(fields=field)` ONCE, before
//! `construct_data_response` dispatches on format (router.py:1440), so the
//! projection is format-agnostic: every output honors it identically. An unknown
//! field raises `KeyError`, which the shared router turns into HTTP 400 "No such
//! field {key}." (router.py:1442-1445). `MapAdapter.read(fields)` returns the
//! requested fields in request order and touches only the TOP-LEVEL mapping —
//! nested children of a selected child are untouched (mapping.py:280-294).
//!
//! These cases drive the HTTP surface directly (tower `oneshot`). The arrow
//! projection lives in `container_arrow_wide_table.rs`.

use std::io::Cursor;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::json;
use tower::ServiceExt;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A 1-D `f64` array child (no spec — plain container children).
fn f64_arr(data: &[f64]) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
    AnyAdapter::Array(Arc::new(ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        json!({}),
        vec![],
    )))
}

/// A 1-D `i64` array child.
fn i64_arr(data: &[i64]) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
    AnyAdapter::Array(Arc::new(ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        json!({}),
        vec![],
    )))
}

fn plain_container(children: Vec<(&str, AnyAdapter)>) -> AnyAdapter {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(m, json!({}), vec![])))
}

/// The tree under test: a plain container `tree` with three top-level children
/// in insertion order — `alpha` (f64), `beta` (i64), and `nested` (a container
/// holding `n1`/`n2`). The interleaving lets a projection reorder and drop
/// children observably, and `nested`'s own children verify that projection is
/// top-level only.
fn tree() -> AnyAdapter {
    plain_container(vec![
        ("alpha", f64_arr(&[1.0, 2.0, 3.0])),
        ("beta", i64_arr(&[4, 5, 6])),
        (
            "nested",
            plain_container(vec![
                ("n1", f64_arr(&[7.0, 8.0])),
                ("n2", f64_arr(&[9.0, 10.0])),
            ]),
        ),
    ])
}

fn root_with(children: Vec<(&str, AnyAdapter)>) -> Arc<dyn ContainerAdapter> {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

fn app_for_root(root: Arc<dyn ContainerAdapter>) -> axum::Router {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: tiled_rs::core::queries::Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
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
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    };
    tiled_rs::server::build_app(state)
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("accept", "*/*")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn post(app: &axum::Router, uri: &str, body: serde_json::Value) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .header("accept", "*/*")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

/// The `contents` object keys of the `application/json` `{contents, metadata}`
/// tree.
fn json_content_keys(bytes: &[u8]) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    v["contents"].as_object().unwrap().keys().cloned().collect()
}

/// The child `id`s of an `application/json-seq` listing, in emitted order. Each
/// RS(0x1E)-framed record is one `Resource`.
fn json_seq_ids(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|b| *b == 0x1E)
        .filter(|chunk| chunk.iter().any(|b| !b.is_ascii_whitespace()))
        .map(|chunk| {
            let v: serde_json::Value = serde_json::from_slice(chunk).unwrap();
            v["id"].as_str().unwrap().to_string()
        })
        .collect()
}

/// Entry names inside a zip archive, in archive order.
fn zip_entry_names(bytes: &[u8]) -> Vec<String> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes.to_vec())).unwrap();
    (0..archive.len())
        .map(|i| archive.by_index(i).unwrap().name().to_string())
        .collect()
}

/// The `error.message` string of a `ServerError` JSON body.
fn error_message(bytes: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    v["error"]["message"].as_str().unwrap().to_string()
}

// ---------------------------------------------------------------------------
// application/json tree
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_projection_restricts_top_level_and_keeps_nested() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/json&field=alpha&field=nested",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Only the projected top-level children; `beta` is dropped.
    let mut keys = json_content_keys(&body);
    keys.sort();
    assert_eq!(keys, vec!["alpha".to_string(), "nested".to_string()]);

    // Projection is top-level only: `nested` keeps BOTH its own children.
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let mut nested_keys: Vec<String> = v["contents"]["nested"]["contents"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    nested_keys.sort();
    assert_eq!(nested_keys, vec!["n1".to_string(), "n2".to_string()]);
}

#[tokio::test]
async fn json_no_projection_lists_all_children() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(&app, "/api/v1/container/full/tree?format=application/json").await;
    assert_eq!(status, StatusCode::OK);
    let mut keys = json_content_keys(&body);
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "alpha".to_string(),
            "beta".to_string(),
            "nested".to_string()
        ],
        "no field= → every child"
    );
}

#[tokio::test]
async fn json_unknown_field_is_400() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/json&field=nope",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_message(&body), "No such field nope.");
}

// ---------------------------------------------------------------------------
// application/json-seq listing (also the text/html listing shape)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_seq_projection_orders_children_by_field() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    // Request order beta, alpha — the reverse of container (insertion) order.
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/json-seq&field=beta&field=alpha",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_seq_ids(&body),
        vec!["beta".to_string(), "alpha".to_string()],
        "projected children in requested field order"
    );
}

#[tokio::test]
async fn json_seq_column_alias_projects() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/json-seq&column=nested",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_seq_ids(&body), vec!["nested".to_string()]);
}

#[tokio::test]
async fn json_seq_unknown_field_is_400() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/json-seq&field=ghost",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_message(&body), "No such field ghost.");
}

// ---------------------------------------------------------------------------
// application/zip deep export
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zip_projection_restricts_top_level_subtree() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=zip&field=alpha&field=nested",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut names = zip_entry_names(&body);
    names.sort();
    // `alpha` leaf plus the whole `nested` subtree; `beta` is dropped.
    assert_eq!(
        names,
        vec![
            "alpha.bin".to_string(),
            "nested/n1.bin".to_string(),
            "nested/n2.bin".to_string(),
        ],
        "only the projected top-level children (nested subtree kept whole)"
    );
}

#[tokio::test]
async fn zip_no_projection_exports_all() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(&app, "/api/v1/container/full/tree?format=zip").await;
    assert_eq!(status, StatusCode::OK);
    let mut names = zip_entry_names(&body);
    names.sort();
    assert_eq!(
        names,
        vec![
            "alpha.bin".to_string(),
            "beta.bin".to_string(),
            "nested/n1.bin".to_string(),
            "nested/n2.bin".to_string(),
        ]
    );
}

#[tokio::test]
async fn zip_unknown_field_is_400() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(&app, "/api/v1/container/full/tree?format=zip&field=missing").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_message(&body), "No such field missing.");
}

// ---------------------------------------------------------------------------
// application/x-hdf5 deep export
// ---------------------------------------------------------------------------

#[cfg(feature = "hdf5-serializer")]
#[tokio::test]
async fn hdf5_projection_restricts_top_level_datasets() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/x-hdf5&field=alpha&field=nested",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..8], b"\x89HDF\r\n\x1a\n", "HDF5 magic signature");

    let tmp = tempfile::Builder::new().suffix(".h5").tempfile().unwrap();
    std::fs::write(tmp.path(), &body).unwrap();
    let file = rust_hdf5::H5File::open(tmp.path()).unwrap();

    // Projected top-level `alpha` present; the whole `nested` subtree present.
    assert!(file.dataset("alpha").is_ok(), "alpha dataset present");
    assert!(file.dataset("nested/n1").is_ok(), "nested/n1 present");
    assert!(file.dataset("nested/n2").is_ok(), "nested/n2 present");
    // `beta` dropped by the projection.
    assert!(file.dataset("beta").is_err(), "beta dropped by projection");
}

#[cfg(feature = "hdf5-serializer")]
#[tokio::test]
async fn hdf5_unknown_field_is_400() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = get(
        &app,
        "/api/v1/container/full/tree?format=application/x-hdf5&field=nope",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_message(&body), "No such field nope.");
}

// ---------------------------------------------------------------------------
// POST bare-list body — forwarded to every non-arrow format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_body_projection_applies_to_json() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    // The bare-list body is the projection; `format` stays a query param.
    let (status, body) = post(
        &app,
        "/api/v1/container/full/tree?format=application/json",
        json!(["alpha"]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json_content_keys(&body), vec!["alpha".to_string()]);
}

#[tokio::test]
async fn post_body_projection_applies_to_zip() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = post(
        &app,
        "/api/v1/container/full/tree?format=zip",
        json!(["nested"]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut names = zip_entry_names(&body);
    names.sort();
    assert_eq!(
        names,
        vec!["nested/n1.bin".to_string(), "nested/n2.bin".to_string()]
    );
}

#[tokio::test]
async fn post_empty_body_lists_all() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = post(
        &app,
        "/api/v1/container/full/tree?format=application/json",
        json!([]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json_content_keys(&body).len(),
        3,
        "empty body → all children"
    );
}

#[tokio::test]
async fn post_body_unknown_field_is_400() {
    let app = app_for_root(root_with(vec![("tree", tree())]));
    let (status, body) = post(
        &app,
        "/api/v1/container/full/tree?format=application/json",
        json!(["nope"]),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error_message(&body), "No such field nope.");
}
