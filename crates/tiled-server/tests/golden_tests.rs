//! Golden tests — verify the Rust server produces Python-compatible JSON responses.
//!
//! Uses `tower::ServiceExt::oneshot` for in-process testing with no TCP bind.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use indexmap::IndexMap;
use tower::ServiceExt;

use tiled_adapters::{ArrayAdapter, MapAdapter};
use tiled_core::adapters::AnyAdapter;
use tiled_core::queries::Query;

/// Build a demo tree matching what we'd test against.
fn build_test_tree() -> MapAdapter {
    let mut mapping = IndexMap::new();

    // A small 1D array
    let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"element": "Cu"}));
    mapping.insert("some_array".to_string(), AnyAdapter::Array(Box::new(arr)));

    // A nested container
    let mut inner = IndexMap::new();
    let inner_data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let inner_arr = ArrayAdapter::from_f64_1d(&inner_data, serde_json::json!({}));
    inner.insert(
        "nested_arr".to_string(),
        AnyAdapter::Array(Box::new(inner_arr)),
    );
    let inner_container = MapAdapter::new(inner, serde_json::json!({"nested": true}), vec![]);
    mapping.insert(
        "subgroup".to_string(),
        AnyAdapter::Container(Box::new(inner_container)),
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
    let base_url = "http://localhost:8000".to_string();

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url,
        allow_origins: Vec::new(),
    };

    tiled_server::build_app(state)
}

/// Send a GET request through the app in-process and return (status, body bytes).
async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap();
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

    // ancestors for a top-level child should be [""]
    let ancestors = data["attributes"]["ancestors"].as_array().unwrap();
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0], "");

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
    let (status, body) =
        get_json(&app, "/api/v1/search/?page[offset]=0&page[limit]=10").await;
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

    // ancestors for "subgroup/nested_arr" should be ["", "subgroup"]
    let ancestors = data["attributes"]["ancestors"].as_array().unwrap();
    assert_eq!(ancestors.len(), 2);
    assert_eq!(ancestors[0], "");
    assert_eq!(ancestors[1], "subgroup");
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
    let (status, body) =
        get_json(&app, "/api/v1/search/?page[offset]=0&page[limit]=1").await;
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
    let ancestors = body["data"]["attributes"]["ancestors"]
        .as_array()
        .unwrap();
    assert!(ancestors.is_empty(), "root should have no ancestors");

    // Top-level child: ancestors = [""]
    let (_, body) = get_json(&app, "/api/v1/metadata/some_array").await;
    let ancestors = body["data"]["attributes"]["ancestors"]
        .as_array()
        .unwrap();
    assert_eq!(ancestors, &[serde_json::json!("")]);

    // Two-level child: ancestors = ["", "subgroup"]
    let (_, body) = get_json(&app, "/api/v1/metadata/subgroup/nested_arr").await;
    let ancestors = body["data"]["attributes"]["ancestors"]
        .as_array()
        .unwrap();
    assert_eq!(
        ancestors,
        &[serde_json::json!(""), serde_json::json!("subgroup")]
    );
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
