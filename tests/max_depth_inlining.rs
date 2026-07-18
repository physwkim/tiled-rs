//! `?max_depth=` validation and `structure.contents` inlining on the metadata
//! and search endpoints — full parity with upstream tiled
//! (`tiled/server/router.py:322,460`, `tiled/server/core.py:468-563`).
//!
//! Upstream types the query param as `Query(None, ge=0, le=DEPTH_LIMIT)` on BOTH
//! routes, so an out-of-range or non-integer value is a 422 before the handler
//! body runs; a valid value threads into `construct_resource` /
//! `construct_entries_response`, where the gate
//! `((max_depth is None) or (depth < max_depth)) and
//! inlined_contents_enabled(depth) and depth <= DEPTH_LIMIT` decides whether a
//! container's children are inlined into `structure.contents`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use indexmap::IndexMap;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use tiled_rs::adapters::{ArrayAdapter, MapAdapter};
use tiled_rs::core::adapters::{AnyAdapter, ContainerAdapter};
use tiled_rs::core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::Spec;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A 1-D `f64` array leaf carrying `metadata`.
fn arr(data: &[f64], metadata: Value) -> AnyAdapter {
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
    let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
    let a = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![data.len()],
        vec![vec![data.len()]],
        metadata,
        vec![],
    );
    AnyAdapter::Array(Arc::new(a))
}

/// A container carrying `specs` (empty = plain container).
fn container(children: Vec<(&str, AnyAdapter)>, specs: Vec<Spec>, metadata: Value) -> AnyAdapter {
    let mut m = IndexMap::new();
    for (k, v) in children {
        m.insert(k.to_string(), v);
    }
    AnyAdapter::Container(Arc::new(MapAdapter::new(m, metadata, specs)))
}

/// The test tree:
/// ```text
/// root
/// ├── ds     (container, spec "xarray_dataset")   → inlining-enabled
/// │   ├── x  (array)
/// │   └── y  (array)
/// ├── plain  (container, no spec)                  → contents stays None
/// │   └── a  (array)
/// └── leaf   (array)
/// ```
fn build_root() -> Arc<dyn ContainerAdapter> {
    let ds = container(
        vec![
            ("x", arr(&[1.0, 2.0, 3.0], json!({"units": "K"}))),
            ("y", arr(&[4.0, 5.0, 6.0], json!({}))),
        ],
        vec![Spec::new("xarray_dataset")],
        json!({"kind": "dataset"}),
    );
    let plain = container(vec![("a", arr(&[7.0, 8.0], json!({})))], vec![], json!({}));
    let mut m = IndexMap::new();
    m.insert("ds".to_string(), ds);
    m.insert("plain".to_string(), plain);
    m.insert("leaf".to_string(), arr(&[0.0], json!({})));
    Arc::new(MapAdapter::new(m, json!({}), vec![]))
}

async fn spawn(root: Arc<dyn ContainerAdapter>) -> String {
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    let state = tiled_rs::server::AppState {
        root_tree: root,
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
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    base
}

/// GET `url`, returning `(status, parsed-json-body)`.
async fn get_json(url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new().get(url).send().await.unwrap();
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    (status, body)
}

// ---------------------------------------------------------------------------
// Commit 1: ?max_depth= parse + validation (Query(None, ge=0, le=DEPTH_LIMIT)).
//
// Boundaries (pydantic v2 message parity):
//   absent          → 200 (None)
//   0               → 200 (lower bound)
//   5 (= DEPTH_LIMIT) → 200 (upper bound)
//   6               → 422 "Input should be less than or equal to 5"
//   -1              → 422 "Input should be greater than or equal to 0"
//   abc             → 422 "Input should be a valid integer, unable to parse
//                          string as an integer"
// Applied identically on /metadata and /search.
// ---------------------------------------------------------------------------

fn error_message(body: &Value) -> String {
    body.get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string()
}

#[tokio::test]
async fn metadata_max_depth_absent_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/")).await;
    assert_eq!(status, 200, "absent max_depth must serve normally");
}

#[tokio::test]
async fn metadata_max_depth_zero_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/?max_depth=0")).await;
    assert_eq!(status, 200, "max_depth=0 is the valid lower bound");
}

#[tokio::test]
async fn metadata_max_depth_five_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/metadata/?max_depth=5")).await;
    assert_eq!(
        status, 200,
        "max_depth=5 (= DEPTH_LIMIT) is the valid upper bound"
    );
}

#[tokio::test]
async fn metadata_max_depth_six_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=6")).await;
    assert_eq!(status, 422, "max_depth=6 exceeds DEPTH_LIMIT");
    assert_eq!(
        error_message(&body),
        "Input should be less than or equal to 5"
    );
}

#[tokio::test]
async fn metadata_max_depth_negative_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=-1")).await;
    assert_eq!(status, 422, "negative max_depth violates ge=0");
    assert_eq!(
        error_message(&body),
        "Input should be greater than or equal to 0"
    );
}

#[tokio::test]
async fn metadata_max_depth_non_integer_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/metadata/?max_depth=abc")).await;
    assert_eq!(status, 422, "non-integer max_depth cannot parse");
    assert_eq!(
        error_message(&body),
        "Input should be a valid integer, unable to parse string as an integer"
    );
}

#[tokio::test]
async fn search_max_depth_absent_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/")).await;
    assert_eq!(status, 200, "absent max_depth must serve normally");
}

#[tokio::test]
async fn search_max_depth_zero_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/?max_depth=0")).await;
    assert_eq!(status, 200, "max_depth=0 is the valid lower bound");
}

#[tokio::test]
async fn search_max_depth_five_is_200() {
    let base = spawn(build_root()).await;
    let (status, _) = get_json(&format!("{base}/api/v1/search/?max_depth=5")).await;
    assert_eq!(
        status, 200,
        "max_depth=5 (= DEPTH_LIMIT) is the valid upper bound"
    );
}

#[tokio::test]
async fn search_max_depth_six_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=6")).await;
    assert_eq!(status, 422, "max_depth=6 exceeds DEPTH_LIMIT");
    assert_eq!(
        error_message(&body),
        "Input should be less than or equal to 5"
    );
}

#[tokio::test]
async fn search_max_depth_negative_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=-1")).await;
    assert_eq!(status, 422, "negative max_depth violates ge=0");
    assert_eq!(
        error_message(&body),
        "Input should be greater than or equal to 0"
    );
}

#[tokio::test]
async fn search_max_depth_non_integer_is_422() {
    let base = spawn(build_root()).await;
    let (status, body) = get_json(&format!("{base}/api/v1/search/?max_depth=abc")).await;
    assert_eq!(status, 422, "non-integer max_depth cannot parse");
    assert_eq!(
        error_message(&body),
        "Input should be a valid integer, unable to parse string as an integer"
    );
}
