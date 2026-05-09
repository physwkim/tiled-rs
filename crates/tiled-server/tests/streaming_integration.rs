//! WebSocket subscription end-to-end: spawn server, connect a client,
//! register a node, expect a ChildCreated update on the parent's stream.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use tiled_catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_core::adapters::ContainerAdapter;
use tiled_core::queries::Query;

async fn spawn_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(
        tiled_catalog::CatalogAdapter::root(catalog.clone(), resolver),
    );
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
        api_key: None,
        catalog: Some(catalog),
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (format!("http://{addr}"), dir)
}

#[tokio::test]
async fn subscribe_then_register_emits_child_created() {
    let (base, _dir) = spawn_server().await;

    // Pre-create an "expt" container so we can subscribe to it.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/v1/register/"))
        .json(&serde_json::json!({
            "key": "expt",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Connect a websocket subscriber to /expt.
    let ws_url = format!(
        "{}/api/v1/container/subscribe/expt",
        base.replacen("http://", "ws://", 1)
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .unwrap_or_else(|e| panic!("ws connect to {ws_url}: {e}"));

    // Read the subscription-ready message.
    let initial = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("initial message timed out")
        .expect("ws closed early")
        .unwrap();
    let bytes = match initial {
        Message::Binary(b) => b,
        other => panic!("expected binary frame, got {other:?}"),
    };
    let initial: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(initial["type"], "subscription-ready");

    // Register a child under /expt.
    let resp = client
        .post(format!("{base}/api/v1/register/expt"))
        .json(&serde_json::json!({
            "key": "scan1",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "register: {}", resp.status());

    // Subscriber should see a ChildCreated frame.
    let next = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("update timed out")
        .expect("ws closed early")
        .unwrap();
    let payload = match next {
        Message::Binary(b) => b,
        other => panic!("expected binary frame, got {other:?}"),
    };
    let env: serde_json::Value = rmp_serde::from_slice(&payload).unwrap();
    assert_eq!(env["kind"]["type"], "child-created");
    assert_eq!(env["kind"]["key"], "scan1");
}

#[tokio::test]
async fn unrelated_subtree_does_not_receive_publish() {
    let (base, _dir) = spawn_server().await;
    // Subscribe at /unused — should not hear about a node registered at
    // the root.
    let ws_url = format!(
        "{}/api/v1/container/subscribe/unused",
        base.replacen("http://", "ws://", 1).trim_end_matches('/')
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let _initial = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let client = reqwest::Client::new();
    let _ = client
        .post(format!("{base}/api/v1/register/"))
        .json(&serde_json::json!({
            "key": "elsewhere",
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }))
        .send()
        .await
        .unwrap();

    // No update should arrive within the timeout window.
    let result = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
    assert!(
        result.is_err(),
        "subscription at unrelated path should stay quiet, got {result:?}"
    );
}
