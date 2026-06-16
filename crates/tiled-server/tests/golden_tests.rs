//! Golden tests — verify the Rust server produces Python-compatible JSON responses.
//!
//! Uses `tower::ServiceExt::oneshot` for in-process testing with no TCP bind.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_adapters::{ArrayAdapter, CooAdapter, MapAdapter, NpyFrameOpener, SequenceAdapter};
use tiled_core::adapters::AnyAdapter;
use tiled_core::dtype::{BuiltinDType, Endianness, Kind};
use tiled_core::queries::Query;

/// Build a demo tree matching what we'd test against.
fn build_test_tree() -> MapAdapter {
    let mut mapping = IndexMap::new();

    // A small 1D array
    let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"element": "Cu"}));
    mapping.insert("some_array".to_string(), AnyAdapter::Array(Arc::new(arr)));

    // A nested container
    let mut inner = IndexMap::new();
    let inner_data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let inner_arr = ArrayAdapter::from_f64_1d(&inner_data, serde_json::json!({}));
    inner.insert(
        "nested_arr".to_string(),
        AnyAdapter::Array(Arc::new(inner_arr)),
    );
    let inner_container = MapAdapter::new(inner, serde_json::json!({"nested": true}), vec![]);
    mapping.insert(
        "subgroup".to_string(),
        AnyAdapter::Container(Arc::new(inner_container)),
    );

    MapAdapter::new(
        mapping,
        serde_json::json!({"description": "test catalog"}),
        vec![],
    )
}

fn build_app() -> axum::Router {
    build_app_with_limit(300_000_000)
}

/// Like `build_app` but with a caller-chosen `response_bytesize_limit`, so the
/// L4 size-cap behavior can be exercised with a tiny limit.
fn build_app_with_limit(response_bytesize_limit: usize) -> axum::Router {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
    build_app_for_root(root_tree, response_bytesize_limit)
}

/// Build an app over a caller-supplied root tree (used by the deep-export zip
/// test, which needs a nested array/table/container layout).
fn build_app_for_root(
    root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter>,
    response_bytesize_limit: usize,
) -> axum::Router {
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: None,
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
    };

    tiled_server::build_app(state)
}

/// Helper: build app with no static base_url and given trust_forwarded_headers setting.
fn build_app_dynamic(trust_forwarded: bool) -> axum::Router {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_server::state::CorsOriginPolicy::AllowList(Vec::new()),
        trust_forwarded_headers: trust_forwarded,
        api_key: None,
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
    };

    tiled_server::build_app(state)
}

/// Build the app with a CORS AllowList containing exactly `origin`, so the
/// preflight path (which only emits CORS headers for an allowed origin) is
/// exercised. Used by the server-L6 regression test.
fn build_app_with_cors_origin(origin: &str) -> axum::Router {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_server::state::CorsOriginPolicy::AllowList(vec![origin.to_string()]),
        trust_forwarded_headers: false,
        api_key: None,
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
    };

    tiled_server::build_app(state)
}

/// Send a GET request through the app in-process and return (status, body bytes).
async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Bytes) {
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body)
}

/// Send GET and parse JSON.
async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, body) = get(app, uri).await;
    let json: serde_json::Value = serde_json::from_slice(&body)
        .unwrap_or_else(|e| panic!("Failed to parse JSON from {uri}: {e}\nbody: {body:?}"));
    (status, json)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_about_endpoint() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/").await;
    assert_eq!(status, 200);

    // api_version must be 0
    assert_eq!(body["api_version"], 0);

    // queries must be an array of strings
    assert!(body["queries"].is_array());
    let queries = body["queries"].as_array().unwrap();
    assert!(queries.contains(&serde_json::json!("fulltext")));
    assert!(queries.contains(&serde_json::json!("eq")));

    // authentication.required must be false
    assert_eq!(body["authentication"]["required"], false);
    assert!(body["authentication"]["providers"].is_array());

    // links must have "self"
    assert!(body["links"]["self"].is_string());

    // aliases must be present
    assert!(body.get("aliases").is_some());

    // formats must be present
    assert!(body.get("formats").is_some());

    // meta must have root_path
    assert!(body["meta"].get("root_path").is_some());
}

#[tokio::test]
async fn test_root_metadata() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/metadata/").await;
    assert_eq!(status, 200);

    // Response envelope
    assert!(body["data"].is_object());
    let data = &body["data"];

    // ancestors must be present and empty for root
    assert!(data["attributes"]["ancestors"].is_array());
    assert_eq!(data["attributes"]["ancestors"].as_array().unwrap().len(), 0);

    // structure_family must be "container"
    assert_eq!(data["attributes"]["structure_family"], "container");

    // structure must have count
    assert!(data["attributes"]["structure"].is_object());
    assert_eq!(data["attributes"]["structure"]["count"], 2); // some_array + subgroup

    // sorting must serialize with integer directions
    let sorting = &data["attributes"]["sorting"];
    assert!(sorting.is_array());
    let first_sort = &sorting[0];
    assert_eq!(first_sort["key"], "_");
    assert_eq!(first_sort["direction"], 1); // Ascending = 1

    // links must have self, search
    assert!(data["links"]["self"].is_string());
    assert!(data["links"]["search"].is_string());
}

#[tokio::test]
async fn test_array_metadata() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/metadata/some_array").await;
    assert_eq!(status, 200);

    let data = &body["data"];

    assert_eq!(data["id"], "some_array");
    assert_eq!(data["attributes"]["structure_family"], "array");

    // ancestors for a top-level child = [] (Python `path_parts[:-1]`)
    let ancestors = data["attributes"]["ancestors"].as_array().unwrap();
    assert!(ancestors.is_empty());

    // structure must be the ArrayStructure
    let structure = &data["attributes"]["structure"];
    assert!(structure.is_object());
    assert_eq!(structure["shape"], serde_json::json!([10]));

    // links must have block and full
    assert!(data["links"]["block"].is_string());
    assert!(data["links"]["full"].is_string());
}

#[tokio::test]
async fn test_search_root() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/search/?page[offset]=0&page[limit]=10").await;
    assert_eq!(status, 200);

    // data should be an array of resources
    assert!(body["data"].is_array());
    let entries = body["data"].as_array().unwrap();
    assert_eq!(entries.len(), 2); // some_array + subgroup

    // Each entry should have ancestors
    for entry in entries {
        assert!(entry["attributes"]["ancestors"].is_array());
        assert!(entry["attributes"]["structure_family"].is_string());
    }

    // meta should have count
    assert_eq!(body["meta"]["count"], 2);

    // links should have pagination format with page[offset] and page[limit]
    let links = &body["links"];
    assert!(links["self"].as_str().unwrap().contains("page[offset]"));
    assert!(links["self"].as_str().unwrap().contains("page[limit]"));
}

/// `?omit_links=true` on /metadata drops the per-node `links` key entirely
/// (not an empty `{}`), matching Python `core.py:616` which only sets
/// `d["links"]` when `not omit_links`.
#[tokio::test]
async fn test_metadata_omit_links() {
    let app = build_app();

    // Control: without omit_links the node carries a `links` object.
    let (status, body) = get_json(&app, "/api/v1/metadata/some_array").await;
    assert_eq!(status, 200);
    assert!(body["data"]["links"].is_object());

    // With omit_links=true the `links` key is absent (not `{}`).
    let (status, body) = get_json(&app, "/api/v1/metadata/some_array?omit_links=true").await;
    assert_eq!(status, 200);
    assert!(
        body["data"].get("links").is_none(),
        "omit_links must drop the per-node links key, got: {}",
        body["data"]
    );
}

/// `?omit_links=true` on /search drops each entry's `links` key but leaves
/// the envelope pagination links intact (Python `core.py:577` gates only the
/// per-entry links; the page links come from the paginated-links builder).
#[tokio::test]
async fn test_search_omit_links() {
    let app = build_app();
    let (status, body) = get_json(
        &app,
        "/api/v1/search/?page[offset]=0&page[limit]=10&omit_links=true",
    )
    .await;
    assert_eq!(status, 200);

    let entries = body["data"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    for entry in entries {
        assert!(
            entry.get("links").is_none(),
            "omit_links must drop each entry's links key, got: {entry}"
        );
    }

    // Envelope pagination links are unaffected.
    assert!(
        body["links"]["self"]
            .as_str()
            .unwrap()
            .contains("page[offset]")
    );
}

#[tokio::test]
async fn test_array_block_data() {
    let app = build_app();
    let (status, body) = get(&app, "/api/v1/array/block/some_array?block=0").await;
    assert_eq!(status, 200);

    // 10 f64 values = 80 bytes
    assert_eq!(body.len(), 80);

    // Verify first value is 0.0
    let first_val = f64::from_le_bytes(body[0..8].try_into().unwrap());
    assert_eq!(first_val, 0.0);

    // Verify last value is 9.0
    let last_val = f64::from_le_bytes(body[72..80].try_into().unwrap());
    assert_eq!(last_val, 9.0);
}

#[tokio::test]
async fn test_nested_container_metadata() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/metadata/subgroup").await;
    assert_eq!(status, 200);

    let data = &body["data"];
    assert_eq!(data["attributes"]["structure_family"], "container");
    assert_eq!(data["attributes"]["structure"]["count"], 1);
}

#[tokio::test]
async fn test_nested_array_metadata() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/metadata/subgroup/nested_arr").await;
    assert_eq!(status, 200);

    let data = &body["data"];
    assert_eq!(data["id"], "nested_arr");
    assert_eq!(data["attributes"]["structure_family"], "array");
    assert_eq!(
        data["attributes"]["structure"]["shape"],
        serde_json::json!([3])
    );

    // ancestors for "subgroup/nested_arr" = ["subgroup"] (parent segments)
    let ancestors = data["attributes"]["ancestors"].as_array().unwrap();
    assert_eq!(ancestors, &[serde_json::json!("subgroup")]);
}

#[tokio::test]
async fn test_not_found() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/metadata/nonexistent").await;
    assert_eq!(status, 404);

    assert!(body["error"].is_object());
    assert_eq!(body["error"]["code"], 404);
}

#[tokio::test]
async fn test_search_pagination() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/search/?page[offset]=0&page[limit]=1").await;
    assert_eq!(status, 200);

    let entries = body["data"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(body["meta"]["count"], 2);

    // Should have a "next" link
    assert!(body["links"]["next"].is_string());
}

// ---------------------------------------------------------------------------
// ancestors correctness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ancestors_correctness() {
    let app = build_app();

    // Root: no ancestors
    let (_, body) = get_json(&app, "/api/v1/metadata/").await;
    let ancestors = body["data"]["attributes"]["ancestors"].as_array().unwrap();
    assert!(ancestors.is_empty(), "root should have no ancestors");

    // Top-level child: ancestors = [] (Python tiled `path_parts[:-1]`)
    let (_, body) = get_json(&app, "/api/v1/metadata/some_array").await;
    let ancestors = body["data"]["attributes"]["ancestors"].as_array().unwrap();
    assert!(ancestors.is_empty(), "top-level child has no ancestors");

    // Two-level child: ancestors = ["subgroup"] (parent segments only)
    let (_, body) = get_json(&app, "/api/v1/metadata/subgroup/nested_arr").await;
    let ancestors = body["data"]["attributes"]["ancestors"].as_array().unwrap();
    assert_eq!(ancestors, &[serde_json::json!("subgroup")]);
}

// ---------------------------------------------------------------------------
// base_url links sanity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_links_use_configured_base_url() {
    let app = build_app();
    let (_, body) = get_json(&app, "/api/v1/metadata/").await;

    let self_link = body["data"]["links"]["self"].as_str().unwrap();
    assert!(
        self_link.starts_with("http://localhost:8000/"),
        "links should use configured base_url, got: {self_link}"
    );
    assert!(
        !self_link.contains("0.0.0.0"),
        "links must not contain 0.0.0.0, got: {self_link}"
    );
}

#[tokio::test]
async fn test_links_derived_from_host_header() {
    // No static base_url — links derive from Host header.
    let app = build_app_dynamic(false);

    let req = Request::builder()
        .uri("/api/v1/metadata/")
        .header("host", "data.example.com:9000")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let self_link = body["data"]["links"]["self"].as_str().unwrap();
    assert!(
        self_link.starts_with("http://data.example.com:9000/"),
        "links should derive from Host header, got: {self_link}"
    );
}

#[tokio::test]
async fn test_forwarded_headers_ignored_without_trust() {
    // trust_forwarded_headers = false — X-Forwarded-* must be ignored.
    let app = build_app_dynamic(false);

    let req = Request::builder()
        .uri("/api/v1/metadata/")
        .header("host", "internal:8000")
        .header("x-forwarded-host", "evil.example.com")
        .header("x-forwarded-proto", "https")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let self_link = body["data"]["links"]["self"].as_str().unwrap();
    assert!(
        self_link.starts_with("http://internal:8000/"),
        "without trust, forwarded headers should be ignored, got: {self_link}"
    );
    assert!(
        !self_link.contains("evil.example.com"),
        "must not use spoofed X-Forwarded-Host, got: {self_link}"
    );
}

#[tokio::test]
async fn test_forwarded_headers_used_with_trust() {
    // trust_forwarded_headers = true — X-Forwarded-* should be honoured.
    let app = build_app_dynamic(true);

    let req = Request::builder()
        .uri("/api/v1/metadata/")
        .header("host", "internal:8000")
        .header("x-forwarded-host", "public.example.com")
        .header("x-forwarded-proto", "https")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let self_link = body["data"]["links"]["self"].as_str().unwrap();
    assert!(
        self_link.starts_with("https://public.example.com/"),
        "with trust, should use X-Forwarded-Host/Proto, got: {self_link}"
    );
}

// ---------------------------------------------------------------------------
// Health / Ready
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_endpoint() {
    let app = build_app();
    let (status, body) = get_json(&app, "/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_ready_endpoint() {
    let app = build_app();
    let (status, body) = get_json(&app, "/ready").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "ok");
    assert!(body["nodes"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_empty_container() {
    // Build app with an empty root container
    let root = MapAdapter::new(IndexMap::new(), serde_json::json!({}), vec![]);
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(root);
    let registry = Arc::new(tiled_serialization::default_registry());
    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
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
    };
    let app = tiled_server::build_app(state);

    // Metadata: count=0
    let (status, body) = get_json(&app, "/api/v1/metadata/").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["attributes"]["structure"]["count"], 0);

    // Search: empty data array
    let (status, body) = get_json(&app, "/api/v1/search/").await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
    assert_eq!(body["meta"]["count"], 0);
}

#[tokio::test]
async fn test_search_on_non_container() {
    let app = build_app();
    // some_array is an array, not a container. Searching it is a
    // wrong-type-for-route: Python answers 404 (structure_families dependency,
    // dependencies.py:138-149), not 422. (server-H4)
    let (status, body) = get_json(&app, "/api/v1/search/some_array").await;
    assert_eq!(status, 404);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not a container")
    );
}

#[tokio::test]
async fn test_search_unsupported_query_type_returns_400() {
    let app = build_app();
    // `lookup` (Python class KeyLookup) has no in-memory evaluation on the
    // MapAdapter search path. Python tiled raises UnsupportedQueryType and
    // answers HTTP 400 with this exact detail string (app.py:355-365).
    let (status, body) = get_json(&app, "/api/v1/search/?filter[lookup][condition][key]=foo").await;
    assert_eq!(status, 400, "unsupported query must be HTTP 400: {body}");
    assert_eq!(
        body["error"]["message"].as_str().unwrap(),
        "The query type 'KeyLookup' is not supported on this node."
    );
}

#[tokio::test]
async fn test_search_supported_query_type_ok() {
    // A supported variant (eq) must still return 200 — the 400 path is
    // specific to variants the adapter cannot evaluate.
    let app = build_app();
    let (status, _body) = get_json(
        &app,
        "/api/v1/search/?filter[eq][condition][key]=element&filter[eq][condition][value]=%22Cu%22",
    )
    .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_block_wrong_dimension_count() {
    let app = build_app();
    // some_array is 1D but we pass 2 block indices
    let (status, body) = get_json(&app, "/api/v1/array/block/some_array?block=0,0").await;
    assert_eq!(status, 422);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("block indices")
    );
}

#[tokio::test]
async fn test_deeply_nested_not_found() {
    let app = build_app();
    let (status, _) = get_json(&app, "/api/v1/metadata/subgroup/nonexistent/deep/path").await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn test_traverse_through_non_container() {
    let app = build_app();
    // some_array is a leaf — can't traverse further
    let (status, body) = get_json(&app, "/api/v1/metadata/some_array/child").await;
    assert_eq!(status, 404);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not a container")
    );
}

// ---------------------------------------------------------------------------
// API key authentication
// ---------------------------------------------------------------------------

fn build_app_with_api_key(key: &str) -> axum::Router {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
    let registry = Arc::new(tiled_serialization::default_registry());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: Some(key.to_string()),
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
    };

    tiled_server::build_app(state)
}

// `/api/v1/` is intentionally public (discovery endpoint — clients
// fetch it before they have credentials to learn which auth providers
// exist). These tests target `/api/v1/metadata/` instead, which is on
// the auth-gated lane.

#[tokio::test]
async fn test_api_key_rejects_without_key() {
    let app = build_app_with_api_key("secret123");
    let (status, _) = get(&app, "/api/v1/metadata/").await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn test_api_key_accepts_query_param() {
    let app = build_app_with_api_key("secret123");
    let (status, _) = get(&app, "/api/v1/metadata/?api_key=secret123").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_api_key_accepts_header() {
    let app = build_app_with_api_key("secret123");
    let req = Request::builder()
        .uri("/api/v1/metadata/")
        .header("authorization", "Apikey secret123")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn test_api_key_rejects_wrong_key() {
    let app = build_app_with_api_key("secret123");
    let (status, _) = get(&app, "/api/v1/metadata/?api_key=wrong").await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn test_about_is_public() {
    let app = build_app_with_api_key("secret123");
    let (status, _) = get(&app, "/api/v1/").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_health_bypasses_auth() {
    let app = build_app_with_api_key("secret123");
    let (status, _) = get_json(&app, "/health").await;
    assert_eq!(status, 200);
}

// ---------------------------------------------------------------------------
// POST /api/v1/register — accept-only stub
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_register_post_accepted() {
    let app = build_app();
    let body = serde_json::json!({
        "structure_family": "container",
        "metadata": {"key": "new_node", "title": "demo"},
        "specs": [],
        "data_sources": [],
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/register/")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 201);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(parsed["id"], "new_node");
    assert!(parsed["links"]["self"].is_string());
}

#[tokio::test]
async fn test_register_post_under_path() {
    let app = build_app();
    let body = serde_json::json!({
        "structure_family": "array",
        "metadata": {"key": "scan_001"},
        "specs": [],
        "data_sources": [],
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/register/sample_data")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 201);
}

// ---------------------------------------------------------------------------
// Percent-encoded slash in keys is preserved (P1 fix)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_metadata_handles_percent_encoded_slash_in_key() {
    // Build a tree with a key that contains a literal '/'.
    let mut mapping = IndexMap::new();
    let data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({}));
    mapping.insert("a/b".to_string(), AnyAdapter::Array(Arc::new(arr)));
    let root: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));
    let registry = Arc::new(tiled_serialization::default_registry());
    let state = tiled_server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
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
    };
    let app = tiled_server::build_app(state);

    // %2F is the percent-encoded form of '/'. The handler must treat
    // it as part of the key, not as a path separator.
    let (status, body) = get_json(&app, "/api/v1/metadata/a%2Fb").await;
    assert_eq!(status, 200);
    let data = &body["data"];
    assert_eq!(data["id"], "a/b");
    assert_eq!(data["attributes"]["structure_family"], "array");
}

// ---------------------------------------------------------------------------
// H1 regression — array_full must read ALL chunks, not just chunk 0
// ---------------------------------------------------------------------------

/// Write a minimal NPY v1.0 file containing exactly one f64 value in a
/// (1, 1) shaped array.  Matches the format used in sequence_adapter tests.
fn write_npy_1x1(path: &std::path::Path, value: f64) {
    use std::io::Write;
    let header_str = "{'descr': '<f8', 'fortran_order': False, 'shape': (1, 1), }";
    let mut header = header_str.as_bytes().to_vec();
    // Pad so that (10 + header.len()) % 64 == 0 (NPY spec alignment).
    while (10 + header.len()) % 64 != 63 {
        header.push(b' ');
    }
    header.push(b'\n');
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(b"\x93NUMPY").unwrap();
    f.write_all(&[1, 0]).unwrap();
    f.write_all(&(header.len() as u16).to_le_bytes()).unwrap();
    f.write_all(&header).unwrap();
    f.write_all(&value.to_le_bytes()).unwrap();
}

/// H1 regression: before the fix, array_full rewired to array_block with
/// block=[0,0,...] and silently returned only the first chunk.  After the
/// fix, array_full calls ArrayAdapterRead::read() which concatenates all
/// chunks.  SequenceAdapter (one chunk per file) exercises this: three files
/// are stacked along axis 0; only the non-first-chunk sentinel (99.0 in
/// frame 2) proves that all chunks were read.
#[tokio::test]
async fn array_full_returns_all_chunks_not_just_first() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<std::path::PathBuf> = (0..3)
        .map(|i| dir.path().join(format!("f{i}.npy")))
        .collect();
    // Frame 0 = 1.0, Frame 1 = 2.0, Frame 2 = sentinel 99.0.
    write_npy_1x1(&paths[0], 1.0);
    write_npy_1x1(&paths[1], 2.0);
    write_npy_1x1(&paths[2], 99.0);

    let seq = SequenceAdapter::from_paths(paths, Arc::new(NpyFrameOpener), serde_json::json!({}))
        .unwrap();

    let mut mapping = IndexMap::new();
    mapping.insert("seq_array".to_string(), AnyAdapter::Array(Arc::new(seq)));
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));
    let registry = Arc::new(tiled_serialization::default_registry());
    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
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
    };
    let app = tiled_server::build_app(state);

    let (status, body) = get(&app, "/api/v1/array/full/seq_array").await;
    assert_eq!(status, 200);

    // 3 frames × shape(1,1) × 8 bytes/f64 = 24 bytes total.
    assert_eq!(
        body.len(),
        24,
        "full read must return all 3 chunks (24 bytes), not just chunk 0 (8 bytes)"
    );

    // Frame 0 sanity check.
    let v0 = f64::from_le_bytes(body[0..8].try_into().unwrap());
    assert_eq!(v0, 1.0, "frame 0 must be 1.0");

    // Non-first-chunk sentinel: frame 2 must be present.
    let v2 = f64::from_le_bytes(body[16..24].try_into().unwrap());
    assert_eq!(
        v2, 99.0,
        "sentinel from non-first chunk (frame 2 = 99.0) must appear in full read"
    );
}

// ---------------------------------------------------------------------------
// H2 regression — ?format= query param must beat the Accept header
// ---------------------------------------------------------------------------

/// Like `get()` but lets the caller set extra request headers and returns
/// the response header map alongside status + body.
async fn get_with_headers(
    app: &axum::Router,
    uri: &str,
    extra: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Bytes) {
    let mut builder = Request::builder().uri(uri);
    for (k, v) in extra {
        builder = builder.header(*k, *v);
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, body)
}

/// H2 regression: before the fix, array_full / array_block ignored the
/// `?format=` query parameter and negotiated entirely from the Accept header.
/// After the fix, `format_param` is extracted and passed to
/// `negotiate_media_type`, which resolves it before consulting Accept.
///
/// This test sends `Accept: application/octet-stream` (the default) alongside
/// `?format=csv` (bare extension shorthand).  The format param must win →
/// response Content-Type must be text/csv and the body must be CSV-shaped.
/// The shorthand resolves via `tiled_core::media_type::resolve_alias` (step 3
/// in negotiate_media_type), which maps "csv" → "text/csv".
#[tokio::test]
async fn array_full_format_param_beats_accept_header() {
    let app = build_app();
    // Accept says raw bytes; bare-extension format param says CSV — format param must win.
    let (status, headers, body) = get_with_headers(
        &app,
        "/api/v1/array/full/some_array?format=csv",
        &[("accept", "application/octet-stream")],
    )
    .await;
    assert_eq!(status, 200);

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/csv"),
        "?format= must override Accept header; got Content-Type: {content_type}"
    );

    // some_array is [0.0 .. 9.0] — CSV for a 1-D array is one value per line.
    let body_str = std::str::from_utf8(&body).expect("CSV body must be valid UTF-8");
    let lines: Vec<&str> = body_str.trim().split('\n').collect();
    assert_eq!(
        lines.len(),
        10,
        "CSV body for a 10-element array must have 10 lines; got: {body_str:?}"
    );
    // Whole-number floats keep the decimal point ("0.0", not "0"), matching
    // Python numpy.savetxt(fmt="%s") — see tiled-serialization L1 (ensure_decimal).
    assert_eq!(lines[0].trim(), "0.0", "first CSV line must be 0.0");
    assert_eq!(lines[9].trim(), "9.0", "last CSV line must be 9.0");
}

/// Finding 1 (H2 export-corruption family, unsupported-format case): a
/// `?format=` that resolves to a media type with no serializer for this family
/// must return HTTP 406, NOT HTTP 200 with the raw payload mislabeled under the
/// foreign Content-Type. Mirrors Python's `UnsupportedMediaTypes` → 406
/// (tiled/server/router.py:642-643). Here `?format=zip` resolves `.zip` →
/// `application/zip` (a router/container format the array family cannot encode).
#[tokio::test]
async fn array_export_unsupported_format_returns_406_not_raw_bytes() {
    let app = build_app();
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/array/full/some_array?format=zip", &[]).await;
    assert_eq!(
        status, 406,
        "array.export with an unsupported format must be 406, not 200-with-raw-bytes"
    );
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        !content_type.contains("application/zip"),
        "must not label the response with the foreign Content-Type; got {content_type}"
    );
    // The 80-byte little-endian array buffer would never parse as JSON; this
    // proves the raw payload is not served.
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("406 body must be a JSON error, not raw array bytes");
    assert_eq!(json["error"]["code"], 406);
}

/// S6/H1 (Accept-header case of the same family): a concrete `Accept` the array
/// family cannot serve must return HTTP 406 — NOT 200 with the octet-stream
/// default served under the client's unwanted Content-Type. Mirrors Python's
/// `UnsupportedMediaTypes` → 406 (core.py:413-419). A *missing* Accept, by
/// contrast, expresses no preference and still resolves to the default (200).
#[tokio::test]
async fn array_unsupported_accept_header_returns_406() {
    let app = build_app();

    // Concrete, unserviceable Accept → 406 (not a silent octet-stream default).
    let (status, _headers, body) = get_with_headers(
        &app,
        "/api/v1/array/full/some_array",
        &[("accept", "text/xml")],
    )
    .await;
    assert_eq!(
        status, 406,
        "an unsupported concrete Accept must be 406, not 200-with-octet-stream"
    );
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("406 body must be a JSON error, not raw array bytes");
    assert_eq!(json["error"]["code"], 406);

    // No Accept header → no preference → family default (octet-stream), 200.
    let (status, headers, _body) =
        get_with_headers(&app, "/api/v1/array/full/some_array", &[]).await;
    assert_eq!(status, 200, "a missing Accept must still serve the default");
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("application/octet-stream"),
        "missing Accept must default to octet-stream; got {content_type}"
    );
}

/// Finding 4: a >2-D array exported as CSV must return HTTP 406
/// (UnsupportedShape, mirroring Python serialize_csv array.py:42-43), not 200
/// with a silently-flattened single-column CSV.
#[tokio::test]
async fn array_csv_export_rejects_ndim_gt_2_with_406() {
    // 2x2x2 f64 array (ndim 3) in its own tree (leaves build_test_tree intact).
    let bytes: Vec<u8> = (0..8u64).flat_map(|i| (i as f64).to_le_bytes()).collect();
    let dtype = tiled_core::dtype::BuiltinDType::new(
        tiled_core::dtype::Endianness::Little,
        tiled_core::dtype::Kind::Float,
        8,
    );
    let arr = ArrayAdapter::from_array(
        Bytes::from(bytes),
        dtype,
        vec![2, 2, 2],
        vec![vec![2, 2, 2]],
        serde_json::json!({}),
        vec![],
    );
    let mut mapping = IndexMap::new();
    mapping.insert("cube".to_string(), AnyAdapter::Array(Arc::new(arr)));
    let root: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));
    let app = build_app_for_root(root, 300_000_000);

    let (status, _, body) = get_with_headers(&app, "/api/v1/array/full/cube?format=csv", &[]).await;
    assert_eq!(
        status, 406,
        "3-D array exported as CSV must be 406 (UnsupportedShape), not flattened 200"
    );
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("406 body must be a JSON error");
    assert_eq!(json["error"]["code"], 406);
}

// ---------------------------------------------------------------------------
// /table/full — read the whole table (all partitions) with optional column
// projection and format negotiation. Mirrors the upstream `table_full`
// endpoint (router.py:1296). Uses a CSV-backed single-partition table.
// ---------------------------------------------------------------------------

#[cfg(feature = "csv-adapter")]
fn build_table_app(csv_path: std::path::PathBuf, response_bytesize_limit: usize) -> axum::Router {
    let csv = tiled_adapters::CsvAdapter::from_path(csv_path, serde_json::json!({"kind": "demo"}))
        .expect("build CsvAdapter");
    let mut mapping = IndexMap::new();
    mapping.insert("some_table".to_string(), AnyAdapter::Table(Arc::new(csv)));
    let root: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));
    let registry = Arc::new(tiled_serialization::default_registry());
    let state = tiled_server::AppState {
        root_tree: root,
        serialization_registry: registry,
        query_names: vec![],
        base_url: Some("http://localhost:8000".to_string()),
        cors_policy: tiled_server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: None,
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
    };
    tiled_server::build_app(state)
}

/// Decode an Arrow IPC file body into (column names, total row count).
#[cfg(feature = "csv-adapter")]
fn decode_arrow(body: &Bytes) -> (Vec<String>, usize) {
    let cursor = std::io::Cursor::new(body.to_vec());
    let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
        .expect("response body must be valid Arrow IPC");
    let cols: Vec<String> = reader
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let rows: usize = reader.map(|b| b.expect("arrow batch").num_rows()).sum();
    (cols, rows)
}

/// POST a JSON body and return (status, body bytes).
#[cfg(feature = "csv-adapter")]
async fn post_json(app: &axum::Router, uri: &str, body: serde_json::Value) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes)
}

#[cfg(feature = "csv-adapter")]
#[tokio::test]
async fn test_table_full_endpoint() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "x,y").unwrap();
    writeln!(f, "1,10").unwrap();
    writeln!(f, "2,20").unwrap();
    writeln!(f, "3,30").unwrap();
    f.flush().unwrap();

    let app = build_table_app(path, 300_000_000);

    // (1) Full read → Arrow IPC, all 3 rows, both columns.
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/table/full/some_table", &[]).await;
    assert_eq!(status, 200);
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("arrow"),
        "default Content-Type must be Arrow IPC; got {ct}"
    );
    let (cols, rows) = decode_arrow(&body);
    assert_eq!(cols, vec!["x", "y"], "full read returns every column");
    assert_eq!(rows, 3, "full read returns every row across all partitions");

    // (2) ?column= projection (GET).
    let (status, _, body) =
        get_with_headers(&app, "/api/v1/table/full/some_table?column=y", &[]).await;
    assert_eq!(status, 200);
    let (cols, rows) = decode_arrow(&body);
    assert_eq!(cols, vec!["y"], "?column=y projects to column y only");
    assert_eq!(rows, 3);

    // (3) deprecated ?field= alias also projects.
    let (status, _, body) =
        get_with_headers(&app, "/api/v1/table/full/some_table?field=x", &[]).await;
    assert_eq!(status, 200);
    let (cols, _) = decode_arrow(&body);
    assert_eq!(cols, vec!["x"], "deprecated ?field=x projects to column x");

    // (4) POST long-request form: columns carried in the JSON body.
    let (status, body) = post_json(
        &app,
        "/api/v1/table/full",
        serde_json::json!({"path": "some_table", "columns": ["y"]}),
    )
    .await;
    assert_eq!(status, 200);
    let (cols, rows) = decode_arrow(&body);
    assert_eq!(cols, vec!["y"], "POST columns body projects to column y");
    assert_eq!(rows, 3);

    // (5) POST with no columns reads the whole table.
    let (status, body) = post_json(
        &app,
        "/api/v1/table/full",
        serde_json::json!({"path": "some_table"}),
    )
    .await;
    assert_eq!(status, 200);
    let (cols, rows) = decode_arrow(&body);
    assert_eq!(cols, vec!["x", "y"]);
    assert_eq!(rows, 3);

    // (6) ?format= is honored by the endpoint's negotiation. parquet is a
    // registered Table serializer, so the output changes shape — proving the
    // format param flows through (not silently ignored).
    if cfg!(feature = "parquet-serializer") {
        let (status, headers, body) =
            get_with_headers(&app, "/api/v1/table/full/some_table?format=parquet", &[]).await;
        assert_eq!(status, 200);
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("parquet"),
            "?format=parquet must negotiate parquet; got {ct}"
        );
        assert_eq!(&body[..4], b"PAR1", "parquet file magic");
    }

    // ?format=csv is served as real CSV: Content-Type text/csv, header row + data rows.
    {
        let (status, headers, body) =
            get_with_headers(&app, "/api/v1/table/full/some_table?format=csv", &[]).await;
        assert_eq!(status, 200);
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("text/csv"),
            "?format=csv must negotiate text/csv; got {ct}"
        );
        let text = std::str::from_utf8(&body).expect("csv must be valid utf-8");
        let lines: Vec<&str> = text.lines().collect();
        assert!(
            !lines.is_empty(),
            "csv body must have at least a header row"
        );
        assert_eq!(lines[0], "x,y", "csv header row must list columns");
        assert_eq!(lines.len(), 4, "csv must have header + 3 data rows");
    }
}

/// Finding 1 (table side): `?format=png` resolves `.png` → `image/png`, which
/// the table family cannot serialize. Must be HTTP 406, not 200 with raw Arrow
/// IPC bytes mislabeled as `image/png`.
#[cfg(feature = "csv-adapter")]
#[tokio::test]
async fn table_export_unsupported_format_returns_406_not_raw_ipc() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "x,y").unwrap();
    writeln!(f, "1,10").unwrap();
    f.flush().unwrap();

    let app = build_table_app(path, 300_000_000);
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/table/full/some_table?format=png", &[]).await;
    assert_eq!(
        status, 406,
        "table.export with an unsupported format must be 406, not 200-with-raw-IPC"
    );
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        !content_type.contains("image/png"),
        "must not label raw Arrow IPC as image/png; got {content_type}"
    );
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("406 body must be a JSON error, not raw Arrow IPC");
    assert_eq!(json["error"]["code"], 406);
}

// ---------------------------------------------------------------------------
// L4 — response_bytesize_limit: a data response whose decoded size exceeds the
// configured limit returns 400 BEFORE serialization; under the limit it works.
// Mirrors Python tiled (router.py:621/701/1185/1315) which raises HTTP 400.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_response_bytesize_limit_array_returns_400() {
    // some_array is 10 f64 = 80 decoded bytes.
    // Over the cap (limit = 10) → 400 on both array_full and array_block.
    let app = build_app_with_limit(10);

    let (status, body) = get_json(&app, "/api/v1/array/full/some_array").await;
    assert_eq!(status, 400, "array_full over the byte limit must be 400");
    assert_eq!(body["error"]["code"], 400);
    let msg = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("Response would exceed"),
        "must contain prefix; got: {body:?}"
    );
    assert!(
        msg.contains("?slice="),
        "array 400 must carry slice hint (Python router.py:626); got: {msg:?}"
    );

    let (status, _) = get(&app, "/api/v1/array/block/some_array?block=0").await;
    assert_eq!(status, 400, "array_block over the byte limit must be 400");

    // Under the cap (limit = 1 MiB) → the same requests serve 200.
    let app = build_app_with_limit(1024 * 1024);
    let (status, body) = get(&app, "/api/v1/array/full/some_array").await;
    assert_eq!(
        status, 200,
        "array_full under the limit must still serve 200"
    );
    assert_eq!(body.len(), 80, "10 f64 = 80 bytes");
}

#[cfg(feature = "csv-adapter")]
#[tokio::test]
async fn test_response_bytesize_limit_table_returns_400() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "x,y").unwrap();
    writeln!(f, "1,10").unwrap();
    writeln!(f, "2,20").unwrap();
    f.flush().unwrap();

    // limit = 1 byte → the table's in-memory size exceeds it → 400 before encode.
    let app = build_table_app(path.clone(), 1);
    let (status, body) = get_json(&app, "/api/v1/table/full/some_table").await;
    assert_eq!(status, 400, "table_full over the byte limit must be 400");
    assert_eq!(body["error"]["code"], 400);
    let msg = body["error"]["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("Response would exceed"),
        "must contain prefix; got: {body:?}"
    );
    assert!(
        msg.contains("columns"),
        "table 400 must carry column-subset hint (Python router.py:1320); got: {msg:?}"
    );

    // Generous limit → 200.
    let app = build_table_app(path, 1024 * 1024);
    let (status, _, _) = get_with_headers(&app, "/api/v1/table/full/some_table", &[]).await;
    assert_eq!(
        status, 200,
        "table_full under the limit must still serve 200"
    );
}

// ---------------------------------------------------------------------------
// Deep-export zip: container/full?format=zip bundles every leaf. The two-phase
// path reads each leaf on the executor (no block_on inside spawn_blocking) and
// must still produce a correct, ordered zip across nested containers, arrays,
// and tables.
// ---------------------------------------------------------------------------

#[cfg(feature = "csv-adapter")]
#[tokio::test]
async fn test_container_full_zip_deep_export() {
    use std::io::{Read, Write};

    // A CSV-backed table leaf.
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("t.csv");
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "x,y").unwrap();
        writeln!(f, "1,10").unwrap();
        writeln!(f, "2,20").unwrap();
    }
    let csv = tiled_adapters::CsvAdapter::from_path(csv_path, serde_json::json!({})).unwrap();

    // Nested container holding one array.
    let mut inner = IndexMap::new();
    let nested = ArrayAdapter::from_f64_1d(&[1.0, 2.0, 3.0], serde_json::json!({}));
    inner.insert(
        "nested_arr".to_string(),
        AnyAdapter::Array(Arc::new(nested)),
    );
    let subgroup = MapAdapter::new(inner, serde_json::json!({}), vec![]);

    // Root: array, subgroup (container), table — in this insertion order.
    let mut mapping = IndexMap::new();
    let arr = ArrayAdapter::from_f64_1d(&[0.0, 1.0, 2.0, 3.0], serde_json::json!({}));
    mapping.insert("some_array".to_string(), AnyAdapter::Array(Arc::new(arr)));
    mapping.insert(
        "subgroup".to_string(),
        AnyAdapter::Container(Arc::new(subgroup)),
    );
    mapping.insert("some_table".to_string(), AnyAdapter::Table(Arc::new(csv)));
    let root: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));

    let app = build_app_for_root(root, 300_000_000);

    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/container/full/?format=zip", &[]).await;
    assert_eq!(status, 200);
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("zip"),
        "deep export Content-Type must be zip; got {ct}"
    );

    // Parse the produced zip.
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(body.to_vec())).expect("valid zip");

    // Entry order is preserved depth-first in container key (insertion) order.
    let ordered: Vec<String> = (0..zip.len())
        .map(|i| zip.by_index(i).unwrap().name().to_string())
        .collect();
    assert_eq!(
        ordered,
        vec![
            "some_array.bin".to_string(),
            "subgroup/nested_arr.bin".to_string(),
            "some_table.arrow".to_string(),
        ],
        "zip entries must be the depth-first ordered leaves"
    );

    // some_array.bin = 4 f64 little-endian = 32 raw bytes; first value 0.0.
    let mut bin = Vec::new();
    zip.by_name("some_array.bin")
        .unwrap()
        .read_to_end(&mut bin)
        .unwrap();
    assert_eq!(bin.len(), 32);
    assert_eq!(f64::from_le_bytes(bin[0..8].try_into().unwrap()), 0.0);

    // some_table.arrow is a valid Arrow IPC file with columns x, y.
    let mut arrow_bytes = Vec::new();
    zip.by_name("some_table.arrow")
        .unwrap()
        .read_to_end(&mut arrow_bytes)
        .unwrap();
    let rdr = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(arrow_bytes), None)
        .expect("table leaf must be valid Arrow IPC");
    let cols: Vec<String> = rdr
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(cols, vec!["x", "y"]);
}

// ---------------------------------------------------------------------------
// L4 (zip): cumulative decoded bytesize across zip leaves must respect
// response_bytesize_limit.  A single leaf under the limit succeeds; two leaves
// whose combined decoded size exceeds the limit returns 400.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_zip_deep_export_cumulative_bytesize_limit() {
    // arr1 = 4 f64 = 32 decoded bytes; arr2 = 3 f64 = 24 bytes → total 56.
    // Limit = 40: first leaf (32) passes, second leaf (32+24=56) exceeds → 400.
    let arr1 = ArrayAdapter::from_f64_1d(&[0.0, 1.0, 2.0, 3.0], serde_json::json!({}));
    let arr2 = ArrayAdapter::from_f64_1d(&[10.0, 20.0, 30.0], serde_json::json!({}));
    let mut mapping = IndexMap::new();
    mapping.insert("arr1".to_string(), AnyAdapter::Array(Arc::new(arr1)));
    mapping.insert("arr2".to_string(), AnyAdapter::Array(Arc::new(arr2)));
    let root: Arc<dyn tiled_core::adapters::ContainerAdapter> =
        Arc::new(MapAdapter::new(mapping, serde_json::json!({}), vec![]));

    let app = build_app_for_root(root.clone(), 40);
    let (status, _, body) = get_with_headers(&app, "/api/v1/container/full/?format=zip", &[]).await;
    assert_eq!(
        status, 400,
        "cumulative decoded size 56 > limit 40 must return 400; got {status}"
    );
    let err: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("Response would exceed"),
        "error body must mention the limit; got {err:?}"
    );

    // Generous limit (400 bytes) → 200 and valid zip.
    let app2 = build_app_for_root(root, 400);
    let (status2, _, _) = get_with_headers(&app2, "/api/v1/container/full/?format=zip", &[]).await;
    assert_eq!(status2, 200, "under limit must succeed");
}

// ---------------------------------------------------------------------------
// server-H4: a path that resolves to a real node whose structure family does
// not match the route must return 404 (not 422), matching Python tiled's
// structure_families dependency (dependencies.py:138-149) and WrongTypeForRoute
// handler (router.py:393-394). `subgroup` is a Container in build_test_tree().
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_route_on_a_container_returns_404_not_422() {
    let app = build_app();
    let (status, _) = get(&app, "/api/v1/array/full/subgroup").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "requesting the array route on a container is a wrong-type-for-route \
         (404 in Python), not a 422 validation error"
    );
}

#[tokio::test]
async fn table_route_on_a_container_returns_404_not_422() {
    let app = build_app();
    let (status, _) = get(&app, "/api/v1/table/full/subgroup").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "requesting the table route on a container is a wrong-type-for-route (404)"
    );
}

#[tokio::test]
async fn array_route_on_a_missing_path_still_returns_404() {
    // Guard the distinct case: a genuinely-absent path is already 404 via
    // walk_tree's NotFound — the H4 change must not regress it.
    let app = build_app();
    let (status, _) = get(&app, "/api/v1/array/full/does_not_exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// server-L6: the CORS AllowList preflight must advertise PATCH. The data API
// exposes PATCH routes (array_append at /array/full, patch_metadata at
// /metadata), so a browser preflight for a PATCH request must succeed. Before
// the fix, allow_methods omitted PATCH and the preflight's
// Access-Control-Allow-Methods header excluded it, so browsers blocked every
// cross-origin PATCH.
#[tokio::test]
async fn cors_preflight_advertises_patch_in_allow_methods() {
    let app = build_app_with_cors_origin("http://example.com");
    let req = Request::builder()
        .method("OPTIONS")
        .uri("/api/v1/metadata/some_array")
        .header("origin", "http://example.com")
        .header("access-control-request-method", "PATCH")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    // tower_http short-circuits the preflight with 200 and echoes the
    // configured method list in Access-Control-Allow-Methods.
    assert_eq!(resp.status(), StatusCode::OK);
    let allow_methods = resp
        .headers()
        .get("access-control-allow-methods")
        .expect("preflight must carry Access-Control-Allow-Methods for an allowed origin")
        .to_str()
        .unwrap()
        .to_ascii_uppercase();
    assert!(
        allow_methods.contains("PATCH"),
        "CORS preflight must advertise PATCH (array_append + patch_metadata are PATCH routes); \
         got Access-Control-Allow-Methods: {allow_methods}"
    );
}

// ---------------------------------------------------------------------------
// M3: OOB block-range and partition index → HTTP 400 (parity with Python
// tiled which catches IndexError from read_block/read_partition and returns
// HTTP_400_BAD_REQUEST; router.py:609-613, 1176-1179).
// ---------------------------------------------------------------------------

/// Block range with stop > chunk count must be HTTP 400.
/// `some_array` has 1 chunk on axis 0; `?block=0:2` requests stop=2 > 1.
#[tokio::test]
async fn test_block_range_oob_is_400() {
    let app = build_app();
    let (status, body) = get_json(&app, "/api/v1/array/block/some_array?block=0:2").await;
    assert_eq!(
        status, 400,
        "OOB block range stop must be HTTP 400 (parity with Python IndexError→400): {body}"
    );
    assert_eq!(body["error"]["code"], 400);
}

/// Partition index >= npartitions must be HTTP 400.
/// CsvAdapter has npartitions=1; `?partition=1` is out of range.
#[cfg(feature = "csv-adapter")]
#[tokio::test]
async fn test_partition_oob_is_400() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "x").unwrap();
    writeln!(f, "1").unwrap();
    f.flush().unwrap();

    let app = build_table_app(path, 300_000_000);
    let (status, body) = get_json(&app, "/api/v1/table/partition/some_table?partition=1").await;
    assert_eq!(
        status, 400,
        "OOB partition index must be HTTP 400 (parity with Python IndexError→400): {body}"
    );
    assert_eq!(body["error"]["code"], 400);
}

// ---------------------------------------------------------------------------
// M5: ?max_depth query parameter caps the zip-export walk depth, clamped to
// DEPTH_LIMIT=5 — parity with Python tiled's DEPTH_LIMIT (core.py:62).
// ---------------------------------------------------------------------------

/// `?max_depth=0` exports only root-level leaves; containers at the root
/// become crumb entries instead of being descended into.
/// Tree: some_array (leaf) + subgroup/nested_arr (container → array).
/// With max_depth=0: some_array.bin present, subgroup.json crumb present,
/// subgroup/nested_arr.bin absent.
#[tokio::test]
async fn test_zip_max_depth_0_stops_at_root() {
    use std::io::Read;

    let app = build_app();
    let (status, _, body) =
        get_with_headers(&app, "/api/v1/container/full/?format=zip&max_depth=0", &[]).await;
    assert_eq!(status, 200, "zip export with max_depth=0 must succeed");

    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(body.to_vec())).expect("valid zip");
    let names: Vec<String> = (0..zip.len())
        .map(|i| zip.by_index(i).unwrap().name().to_string())
        .collect();

    assert!(
        names.contains(&"some_array.bin".to_string()),
        "root-level array must be present; got: {names:?}"
    );
    assert!(
        names.contains(&"subgroup.json".to_string()),
        "capped container must emit a crumb; got: {names:?}"
    );
    assert!(
        !names.contains(&"subgroup/nested_arr.bin".to_string()),
        "nested array below capped container must not be exported; got: {names:?}"
    );

    // Verify the crumb carries the truncation note.
    let mut crumb_bytes = Vec::new();
    zip.by_name("subgroup.json")
        .unwrap()
        .read_to_end(&mut crumb_bytes)
        .unwrap();
    let crumb: serde_json::Value = serde_json::from_slice(&crumb_bytes).unwrap();
    assert!(
        crumb["note"].as_str().unwrap_or("").contains("max_depth"),
        "crumb note must mention max_depth; got: {crumb}"
    );
}

/// `?max_depth=99` is silently clamped to DEPTH_LIMIT=5; a tree of depth 1
/// exports fully regardless of the over-large input.
#[tokio::test]
async fn test_zip_max_depth_clamped_to_depth_limit() {
    let app = build_app();
    let (status, _, body) =
        get_with_headers(&app, "/api/v1/container/full/?format=zip&max_depth=99", &[]).await;
    assert_eq!(status, 200, "zip export with max_depth=99 must succeed");

    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(body.to_vec())).expect("valid zip");
    let names: Vec<String> = (0..zip.len())
        .map(|i| zip.by_index(i).unwrap().name().to_string())
        .collect();

    // The test tree is only depth-1 so max_depth=5 (after clamp) still
    // exports everything: both the root array and the nested array.
    assert!(
        names.contains(&"some_array.bin".to_string()),
        "some_array.bin must be present; got: {names:?}"
    );
    assert!(
        names.contains(&"subgroup/nested_arr.bin".to_string()),
        "subgroup/nested_arr.bin must be present (depth 1 ≤ 5); got: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Sparse (COO) read stack — end-to-end integration (H3)
//
// Proves the full chain `CooAdapter` -> `array_full`/`array_block` route ->
// `build_sparse_response` (Arrow IPC encode) -> Sparse serializer dispatch.
// A 2-D COO with shape [3, 3] and two non-zeros — (0, 1) = 5.0, (2, 0) = 7.0 —
// must come back as an Arrow table with columns `dim0`, `dim1`, `data`.
// ---------------------------------------------------------------------------

/// Build an app whose root tree holds one sparse leaf `sparse_arr`.
fn build_sparse_app() -> axum::Router {
    // dim0 (rows) = [0, 2], dim1 (cols) = [1, 0], data = [5.0, 7.0]
    let coords: Vec<Vec<i64>> = vec![vec![0, 2], vec![1, 0]];
    let mut data_bytes = Vec::new();
    data_bytes.extend_from_slice(&5.0f64.to_le_bytes());
    data_bytes.extend_from_slice(&7.0f64.to_le_bytes());

    let coo = CooAdapter::from_arrays(
        coords,
        Bytes::from(data_bytes),
        BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        vec![3, 3],
        None,
        serde_json::json!({"element": "Cu"}),
        vec![],
    )
    .expect("valid COO inputs");

    let mut mapping = IndexMap::new();
    mapping.insert("sparse_arr".to_string(), AnyAdapter::Sparse(Arc::new(coo)));
    let root = MapAdapter::new(mapping, serde_json::json!({}), vec![]);
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(root);
    build_app_for_root(root_tree, 300_000_000)
}

/// Decode an Arrow IPC file body into the COO `(dim0, dim1, data)` columns.
fn decode_coo_arrow(body: &Bytes) -> (Vec<i64>, Vec<i64>, Vec<f64>) {
    use arrow::array::{Float64Array, Int64Array};
    use arrow::ipc::reader::FileReader;
    use std::io::Cursor;

    let reader =
        FileReader::try_new(Cursor::new(body.as_ref()), None).expect("body is a valid Arrow file");

    let (mut dim0, mut dim1, mut data) = (Vec::new(), Vec::new(), Vec::new());
    for batch in reader {
        let batch = batch.expect("valid record batch");
        let schema = batch.schema();
        let c0 = batch
            .column(schema.index_of("dim0").expect("dim0 column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("dim0 is Int64");
        let c1 = batch
            .column(schema.index_of("dim1").expect("dim1 column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("dim1 is Int64");
        let cd = batch
            .column(schema.index_of("data").expect("data column"))
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("data is Float64");
        for i in 0..batch.num_rows() {
            dim0.push(c0.value(i));
            dim1.push(c1.value(i));
            data.push(cd.value(i));
        }
    }
    (dim0, dim1, data)
}

#[tokio::test]
async fn test_sparse_array_full_returns_coo_arrow_table() {
    let app = build_sparse_app();
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/array/full/sparse_arr", &[]).await;

    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("arrow"), "unexpected content-type: {ct}");

    let (dim0, dim1, data) = decode_coo_arrow(&body);
    assert_eq!(dim0, vec![0, 2], "dim0 coordinates");
    assert_eq!(dim1, vec![1, 0], "dim1 coordinates");
    assert_eq!(data, vec![5.0, 7.0], "data values");
}

#[tokio::test]
async fn test_sparse_array_block_returns_coo_arrow_table() {
    let app = build_sparse_app();
    // Single-block COO: block 0,0 carries the whole table.
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/array/block/sparse_arr?block=0,0", &[]).await;

    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("arrow"), "unexpected content-type: {ct}");

    let (dim0, dim1, data) = decode_coo_arrow(&body);
    assert_eq!(dim0, vec![0, 2], "dim0 coordinates");
    assert_eq!(dim1, vec![1, 0], "dim1 coordinates");
    assert_eq!(data, vec![5.0, 7.0], "data values");
}

#[tokio::test]
async fn test_sparse_array_full_applies_partial_slice() {
    // Regression for the formerly-rejected partial sparse slice path: a
    // `?slice=0:2` over the [3,3] fixture selects rows [0,2), so only the
    // non-zero at (0,1)=5.0 survives; (2,0)=7.0 is dropped. The trailing
    // column axis is kept whole (numpy `arr[0:2]` == `arr[0:2, :]`).
    let app = build_sparse_app();
    let (status, headers, body) =
        get_with_headers(&app, "/api/v1/array/full/sparse_arr?slice=0:2", &[]).await;

    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("arrow"), "unexpected content-type: {ct}");

    let (dim0, dim1, data) = decode_coo_arrow(&body);
    assert_eq!(dim0, vec![0], "only the row-0 non-zero survives");
    assert_eq!(dim1, vec![1], "its column coordinate is preserved");
    assert_eq!(data, vec![5.0], "its value is preserved");
}

/// Server M1: an in-memory (no-catalog) node does not support `replace_metadata`,
/// so `PUT /metadata/{path}` answers 405 — matching Python's "This node does not
/// support update of metadata." (router.py:2446-2450), not a generic 404/422.
#[tokio::test]
async fn test_put_metadata_405_without_catalog() {
    let app = build_app();
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/metadata/some_array")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"metadata":{"x":1}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

/// Server M2: an in-memory (no-catalog) node persists no revisions, so
/// GET /revisions/{path} answers 405 — matching Python's "This node does not
/// support revisions." (router.py:2521-2525).
#[tokio::test]
async fn test_get_revisions_405_without_catalog() {
    let app = build_app();
    let req = Request::builder()
        .uri("/api/v1/revisions/some_array")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}
