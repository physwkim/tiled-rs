//! End-to-end tests: spin up `tiled-server` on a real TCP port, connect with
//! `tiled-client`, exercise navigation + array + table reads.

use std::sync::Arc;

use indexmap::IndexMap;
use tokio::net::TcpListener;

use tiled_adapters::{ArrayAdapter, MapAdapter};
use tiled_client::{AnyClient, from_uri};
use tiled_core::adapters::AnyAdapter;
use tiled_core::queries::Query;

fn build_root() -> MapAdapter {
    let mut mapping = IndexMap::new();

    // 1D float64 array.
    let data: Vec<f64> = (0..10).map(|i| i as f64).collect();
    let arr = ArrayAdapter::from_f64_1d(&data, serde_json::json!({"element": "Cu"}));
    mapping.insert("some_array".into(), AnyAdapter::Array(Box::new(arr)));

    // Nested container.
    let mut inner = IndexMap::new();
    let inner_data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let inner_arr = ArrayAdapter::from_f64_1d(&inner_data, serde_json::json!({}));
    inner.insert("nested_arr".into(), AnyAdapter::Array(Box::new(inner_arr)));
    let inner_container = MapAdapter::new(inner, serde_json::json!({"nested": true}), vec![]);
    mapping.insert(
        "subgroup".into(),
        AnyAdapter::Container(Box::new(inner_container)),
    );

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
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
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
