//! Golden tests — verify the Rust server produces Python-compatible JSON responses.

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

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
    inner.insert("nested_arr".to_string(), AnyAdapter::Array(Box::new(inner_arr)));
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

/// Start a test server and return (base_url, JoinHandle).
async fn start_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let root_tree: Arc<dyn tiled_core::adapters::ContainerAdapter> = Arc::new(build_test_tree());
    let registry = Arc::new(tiled_serialization::default_registry());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    let state = tiled_server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: base_url.clone(),
    };

    let app = tiled_server::build_app(state);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (base_url, handle)
}

#[tokio::test]
async fn test_about_endpoint() {
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/api/v1/"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();

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
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/api/v1/metadata/"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();

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
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/api/v1/metadata/some_array"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let data = &body["data"];

    assert_eq!(data["id"], "some_array");
    assert_eq!(data["attributes"]["structure_family"], "array");

    // ancestors should contain parent path info
    assert!(data["attributes"]["ancestors"].is_array());

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
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!(
        "{base_url}/api/v1/search/?page[offset]=0&page[limit]=10"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();

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
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!(
        "{base_url}/api/v1/array/block/some_array?block=0"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);

    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(content_type, "application/octet-stream");

    let bytes = resp.bytes().await.unwrap();
    // 10 f64 values = 80 bytes
    assert_eq!(bytes.len(), 80);

    // Verify first value is 0.0
    let first_val = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
    assert_eq!(first_val, 0.0);

    // Verify last value is 9.0
    let last_val = f64::from_le_bytes(bytes[72..80].try_into().unwrap());
    assert_eq!(last_val, 9.0);
}

#[tokio::test]
async fn test_nested_container_metadata() {
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/api/v1/metadata/subgroup"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let data = &body["data"];

    assert_eq!(data["attributes"]["structure_family"], "container");
    assert_eq!(data["attributes"]["structure"]["count"], 1);
}

#[tokio::test]
async fn test_nested_array_metadata() {
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!(
        "{base_url}/api/v1/metadata/subgroup/nested_arr"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let data = &body["data"];

    assert_eq!(data["id"], "nested_arr");
    assert_eq!(data["attributes"]["structure_family"], "array");
    assert_eq!(data["attributes"]["structure"]["shape"], serde_json::json!([3]));
}

#[tokio::test]
async fn test_not_found() {
    let (base_url, _handle) = start_test_server().await;

    let resp = reqwest::get(format!("{base_url}/api/v1/metadata/nonexistent"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].is_object());
    assert_eq!(body["error"]["code"], 404);
}

#[tokio::test]
async fn test_search_pagination() {
    let (base_url, _handle) = start_test_server().await;

    // Request with limit=1
    let resp = reqwest::get(format!(
        "{base_url}/api/v1/search/?page[offset]=0&page[limit]=1"
    ))
    .await
    .unwrap();
    assert_eq!(resp.status(), 200);

    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body["data"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(body["meta"]["count"], 2);

    // Should have a "next" link
    assert!(body["links"]["next"].is_string());
}
