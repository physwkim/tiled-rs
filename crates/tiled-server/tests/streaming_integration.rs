//! WebSocket subscription end-to-end: spawn server, connect a client,
//! register a node, expect a ChildCreated update on the parent's stream.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use tiled_access::{ScopeSet, TagBasedPolicy};
use tiled_catalog::node::RegisterRequest;
use tiled_catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_core::adapters::ContainerAdapter;
use tiled_core::queries::Query;
use tiled_server::streaming::{StreamingBus, UpdateKind};

async fn spawn_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
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
        response_bytesize_limit: 300_000_000,
        streaming_bus: tiled_server::streaming::StreamingBus::new(),
        access_policy: None,
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
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
        "{}/api/v1/stream/single/expt",
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
        "{}/api/v1/stream/single/unused",
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

// ---------------------------------------------------------------------------
// F4: per-event delivery authorization.
//
// `publish` fans every event up to all ancestor channels, so a subscriber
// of a container receives events sourced from its descendants. Authorizing
// only the subscription point leaks descendant metadata; the fix
// authorizes each delivered event against the node it actually concerns.
//
// Note on the root case (hole 1 in the finding): a literal root
// subscription to `/api/v1/stream/single/` is not reachable — axum's
// `{*path}` catch-all rejects the empty path and no bare-root subscribe
// route exists. The tests below exercise the reachable analog: a container
// subscriber (`/pub`) receiving fanned descendant events. The fix still
// covers the root channel uniformly.
// ---------------------------------------------------------------------------

/// Spawn a server with a `TagBasedPolicy` and no auth backend (anonymous
/// principal → full scopes, narrowed per node by the policy: untagged
/// nodes are public, tagged nodes are denied to the anonymous principal).
/// Returns the catalog (to seed nodes) and a clone of the streaming bus
/// (to inject fanned events directly, bypassing write-path authorization).
async fn spawn_server_with_tag_policy() -> (String, Catalog, StreamingBus, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let access_policy: Arc<dyn tiled_access::AccessPolicy> =
        Arc::new(TagBasedPolicy::new(ScopeSet::full()));

    let resolver: Arc<dyn tiled_catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_serialization::default_registry());
    let bus = StreamingBus::new();
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
        // No auth backend → WS handshake grants the anonymous principal full
        // scopes, which the policy then narrows per node.
        api_key: None,
        catalog: Some(catalog.clone()),
        auth_db: None,
        issuer: None,
        authenticators: vec![],
        proxied_header_auth: None,
        external_oidc: None,
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_bus: bus.clone(),
        access_policy: Some(access_policy),
        default_login_scopes: tiled_auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
    };
    let app = tiled_server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (format!("http://{addr}"), catalog, bus, dir)
}

fn container_node(key: &str, access_blob: serde_json::Value) -> RegisterRequest {
    RegisterRequest {
        key: key.to_string(),
        structure_family: "container".to_string(),
        metadata: json!({}),
        specs: json!([]),
        access_blob,
    }
}

/// Next decoded envelope on the socket, or `None` on timeout/close.
async fn next_envelope<S>(ws: &mut S) -> Option<serde_json::Value>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) => Some(rmp_serde::from_slice(&b).expect("decode")),
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => None,
        Ok(Some(Ok(other))) => panic!("unexpected ws frame: {other:?}"),
        Ok(Some(Err(e))) => panic!("ws error: {e}"),
    }
}

/// A subscriber to `/pub` must NOT receive an event fanned up from the
/// denied descendant `/pub/secret`, but must still receive permitted
/// descendant events from `/pub/open`.
#[tokio::test]
async fn ws_subscriber_does_not_receive_denied_descendant_events() {
    let (base, catalog, bus, _dir) = spawn_server_with_tag_policy().await;

    let pub_node = catalog
        .create_node(None, vec![], container_node("pub", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            Some(pub_node.id),
            vec!["pub".into()],
            container_node("open", json!({})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(pub_node.id),
            vec!["pub".into()],
            container_node("secret", json!({"tags": ["secret"]})),
        )
        .await
        .unwrap();

    let ws_url = format!(
        "{}/api/v1/stream/single/pub",
        base.replacen("http://", "ws://", 1)
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    assert_eq!(
        next_envelope(&mut ws).await.expect("subscription-ready")["type"],
        "subscription-ready"
    );
    // Let `run_subscription` reach `bus.subscribe("pub")` before we publish.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Denied descendant first, then a permitted one. Broadcast preserves
    // order, so the subscriber would see `secret` first if it were not
    // filtered.
    bus.publish(
        "pub/secret",
        UpdateKind::MetadataUpdated {
            metadata: json!({"leak": true}),
            specs: json!([]),
        },
    );
    bus.publish(
        "pub/open",
        UpdateKind::MetadataUpdated {
            metadata: json!({"ok": true}),
            specs: json!([]),
        },
    );

    let env = next_envelope(&mut ws).await.expect("permitted event");
    assert_eq!(env["kind"]["type"], "metadata-updated");
    assert_eq!(
        env["path"], "pub/open",
        "leaked a denied descendant event: {env}"
    );
    assert!(
        next_envelope(&mut ws).await.is_none(),
        "subscriber received an extra (denied) descendant event"
    );
}

/// `ChildCreated` is published on the parent path (readable) but reveals
/// the new child. A subscriber to `/pub` must NOT receive `ChildCreated`
/// for the denied child `/pub/secret_child`, but must receive it for the
/// permitted child `/pub/open_child`.
#[tokio::test]
async fn ws_subscriber_does_not_receive_child_created_for_denied_child() {
    let (base, catalog, bus, _dir) = spawn_server_with_tag_policy().await;

    let pub_node = catalog
        .create_node(None, vec![], container_node("pub", json!({})))
        .await
        .unwrap();
    catalog
        .create_node(
            Some(pub_node.id),
            vec!["pub".into()],
            container_node("secret_child", json!({"tags": ["secret"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(pub_node.id),
            vec!["pub".into()],
            container_node("open_child", json!({})),
        )
        .await
        .unwrap();

    let ws_url = format!(
        "{}/api/v1/stream/single/pub",
        base.replacen("http://", "ws://", 1)
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    assert_eq!(
        next_envelope(&mut ws).await.expect("subscription-ready")["type"],
        "subscription-ready"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    bus.publish(
        "pub",
        UpdateKind::ChildCreated {
            key: "secret_child".into(),
            structure_family: "container".into(),
        },
    );
    bus.publish(
        "pub",
        UpdateKind::ChildCreated {
            key: "open_child".into(),
            structure_family: "container".into(),
        },
    );

    let env = next_envelope(&mut ws)
        .await
        .expect("permitted child-created");
    assert_eq!(env["kind"]["type"], "child-created");
    assert_eq!(
        env["kind"]["key"], "open_child",
        "leaked child-created for a denied child: {env}"
    );
    assert!(
        next_envelope(&mut ws).await.is_none(),
        "subscriber received child-created for a denied child"
    );
}
