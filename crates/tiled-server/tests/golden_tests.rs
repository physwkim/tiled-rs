//! Golden tests — verify the Rust server produces Python-compatible JSON responses.
//!
//! Uses `tower::ServiceExt::oneshot` for in-process testing with no TCP bind.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_adapters::{ArrayAdapter, MapAdapter, NpyFrameOpener, SequenceAdapter};
use tiled_core::adapters::AnyAdapter;
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
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
    // some_array is an array, not a container — search should fail
    let (status, body) = get_json(&app, "/api/v1/search/some_array").await;
    assert_eq!(status, 422);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not a container")
    );
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
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
    assert_eq!(lines[0].trim(), "0", "first CSV line must be 0");
    assert_eq!(lines[9].trim(), "9", "last CSV line must be 9");
}
