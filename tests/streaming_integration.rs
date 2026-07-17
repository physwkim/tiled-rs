//! WebSocket subscription end-to-end (Wave-24 PR2b): spawn a server, connect a
//! client, and drive events through the per-node streaming cache. The first
//! message is the node's per-family schema; subsequent messages are the flat
//! event metadata (`{"type": ..., "sequence": n, ...}`) in either JSON text
//! frames (default) or msgpack binary frames (`?envelope_format=msgpack`).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use tiled_rs::access::{ScopeSet, TagBasedPolicy};
use tiled_rs::catalog::node::RegisterRequest;
use tiled_rs::catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::queries::Query;
use tiled_rs::server::streaming_cache::{InMemoryStreamingCache, StreamEvent, StreamingCache};

/// Build a test `AppState` backed by the given catalog, streaming cache, and
/// optional access policy. No auth backend is configured, so the WS handshake
/// grants the anonymous principal full scopes (the policy, when present, then
/// narrows per node).
fn build_state(
    catalog: Catalog,
    cache: Arc<dyn StreamingCache>,
    access_policy: Option<Arc<dyn tiled_rs::access::AccessPolicy>>,
) -> tiled_rs::server::AppState {
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let registry = Arc::new(tiled_rs::serialization::default_registry());
    tiled_rs::server::AppState {
        root_tree,
        serialization_registry: registry,
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
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
        streaming_cache: cache,
        access_policy,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
    }
}

/// A generously-sized in-memory streaming cache for tests (long TTLs so cached
/// events survive for replay; ample ring so nothing is evicted mid-test).
fn test_cache() -> Arc<dyn StreamingCache> {
    Arc::new(InMemoryStreamingCache::new(
        Duration::from_secs(60),
        Duration::from_secs(60),
        1024,
    ))
}

async fn serve(state: tiled_rs::server::AppState) -> String {
    let app = tiled_rs::server::build_app(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://{addr}")
}

async fn spawn_server() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let state = build_state(catalog, test_cache(), None);
    (serve(state).await, dir)
}

fn ws_url(base: &str, path: &str) -> String {
    format!(
        "{}/api/v1/stream/single/{path}",
        base.replacen("http://", "ws://", 1)
    )
}

async fn register(client: &reqwest::Client, base: &str, parent_path: &str, key: &str) {
    let resp = client
        .post(format!("{base}/api/v1/register/{parent_path}"))
        .json(&json!({
            "key": key,
            "structure_family": "container",
            "metadata": {},
            "specs": [],
            "data_sources": [],
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "register {key}: {}",
        resp.status()
    );
}

/// Next JSON text frame decoded, or `None` on timeout/close.
async fn next_text_json<S>(ws: &mut S) -> Option<Value>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => Some(serde_json::from_str(t.as_str()).expect("json")),
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => None,
        Ok(Some(Ok(other))) => panic!("expected text frame, got {other:?}"),
        Ok(Some(Err(e))) => panic!("ws error: {e}"),
    }
}

/// Next msgpack binary frame decoded, or `None` on timeout/close.
async fn next_binary_msgpack<S>(ws: &mut S) -> Option<Value>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) => Some(rmp_serde::from_slice(&b).expect("msgpack")),
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => None,
        Ok(Some(Ok(other))) => panic!("expected binary frame, got {other:?}"),
        Ok(Some(Err(e))) => panic!("ws error: {e}"),
    }
}

/// Default (JSON) envelope: subscribe to a container, then register a child
/// under it and expect a flat `container-child-created` text frame.
#[tokio::test]
async fn subscribe_then_register_emits_child_created_json() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "expt").await;

    let url = ws_url(&base, "expt");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .unwrap_or_else(|e| panic!("ws connect to {url}: {e}"));

    // First message: the container's schema, as a JSON text frame.
    let schema = next_text_json(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "container-schema");

    // Give `run_subscription` time to reach its live loop (subscribe()).
    tokio::time::sleep(Duration::from_millis(150)).await;
    register(&client, &base, "expt", "scan1").await;

    let ev = next_text_json(&mut ws).await.expect("child-created");
    assert_eq!(ev["type"], "container-child-created");
    assert_eq!(ev["key"], "scan1");
    assert_eq!(ev["structure_family"], "container");
}

/// `?envelope_format=msgpack`: the schema and events arrive as msgpack binary
/// frames instead of JSON text frames.
#[tokio::test]
async fn subscribe_then_register_emits_child_created_msgpack() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "expt").await;

    let url = format!("{}?envelope_format=msgpack", ws_url(&base, "expt"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .unwrap_or_else(|e| panic!("ws connect to {url}: {e}"));

    let schema = next_binary_msgpack(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "container-schema");

    tokio::time::sleep(Duration::from_millis(150)).await;
    register(&client, &base, "expt", "scan1").await;

    let ev = next_binary_msgpack(&mut ws).await.expect("child-created");
    assert_eq!(ev["type"], "container-child-created");
    assert_eq!(ev["key"], "scan1");
}

/// `?start=` replays cached events without a live race: register the child
/// first (cached at sequence 1 on the parent), then subscribe with `?start=1`.
#[tokio::test]
async fn start_replay_delivers_cached_child_created() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "expt").await;
    register(&client, &base, "expt", "scan1").await;

    let url = format!("{}?start=1", ws_url(&base, "expt"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .unwrap_or_else(|e| panic!("ws connect to {url}: {e}"));

    let schema = next_text_json(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "container-schema");

    let ev = next_text_json(&mut ws)
        .await
        .expect("replayed child-created");
    assert_eq!(ev["type"], "container-child-created");
    assert_eq!(ev["key"], "scan1");
}

/// The cache is node_id-keyed: a subscriber on `/a` never sees an event
/// published on a sibling `/b`'s stream — there is no ancestor / sibling
/// fan-out.
#[tokio::test]
async fn sibling_subtree_does_not_receive_events() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "a").await;
    register(&client, &base, "", "b").await;

    let url = ws_url(&base, "a");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let schema = next_text_json(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "container-schema");

    tokio::time::sleep(Duration::from_millis(150)).await;
    // A child under /b publishes `container-child-created` on /b's node id.
    register(&client, &base, "b", "child").await;

    assert!(
        next_text_json(&mut ws).await.is_none(),
        "subscriber on /a leaked a sibling /b event"
    );
}

/// End-to-end for the PATCH publish site: a merge-patch on a child publishes
/// `container-child-metadata-updated` on the child's *parent* stream, carrying
/// the child's key, the new metadata, and the freshly-created revision number.
#[tokio::test]
async fn patch_child_metadata_emits_metadata_updated() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "expt").await;
    register(&client, &base, "expt", "scan1").await;

    // Subscribe to the parent (`/expt`) — child_metadata_updated is published
    // on the parent's node id.
    let url = ws_url(&base, "expt");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let schema = next_text_json(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "container-schema");

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Merge-patch the child's metadata. The patch mode is a *body* field
    // `content-type` (upstream tiled #688), not the HTTP header;
    // `drop_revision` defaults false, so the update writes revision 1.
    let resp = client
        .patch(format!("{base}/api/v1/metadata/expt/scan1"))
        .json(&json!({
            "content-type": "application/merge-patch+json",
            "metadata": {"note": "updated"},
        }))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "patch: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("metadata-updated");
    assert_eq!(ev["type"], "container-child-metadata-updated");
    assert_eq!(ev["key"], "scan1");
    assert_eq!(ev["metadata"], json!({"note": "updated"}));
    assert_eq!(ev["revision_number"], 1);
}

// ---------------------------------------------------------------------------
// F4: per-event delivery authorization (D10).
//
// `container-child-created` / `container-child-metadata-updated` events are
// published on the *parent* node's stream but name a child (`key`). A
// subscriber permitted on the parent but not on that child must NOT receive
// the event — otherwise it learns of a restricted child. The handler
// re-authorizes each delivered event against the child node it concerns
// (`parent_path + key`), not merely the subscription point.
// ---------------------------------------------------------------------------

/// Spawn a server with a `TagBasedPolicy` and no auth backend (anonymous
/// principal → full scopes, narrowed per node by the policy: untagged nodes
/// are public, tagged nodes are denied to the anonymous principal). Returns the
/// catalog (to seed nodes) and the streaming cache (to inject parent-stream
/// events directly, bypassing the write path).
async fn spawn_server_with_tag_policy()
-> (String, Catalog, Arc<dyn StreamingCache>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();

    let streaming_auth_db = tiled_rs::auth::AuthDb::connect("sqlite::memory:")
        .await
        .unwrap();
    streaming_auth_db.migrate().await.unwrap();
    let access_policy: Arc<dyn tiled_rs::access::AccessPolicy> = Arc::new(TagBasedPolicy::new(
        Arc::new(streaming_auth_db),
        ScopeSet::full(),
    ));

    let cache = test_cache();
    let state = build_state(catalog.clone(), cache.clone(), Some(access_policy));
    (serve(state).await, catalog, cache, dir)
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

/// A subscriber to `/pub` must NOT receive a `container-child-metadata-updated`
/// event naming the denied child `secret`, but must still receive one naming
/// the permitted child `open`.
#[tokio::test]
async fn ws_subscriber_does_not_receive_denied_descendant_events() {
    let (base, catalog, cache, _dir) = spawn_server_with_tag_policy().await;

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

    let url = ws_url(&base, "pub");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "container-schema"
    );
    // Let `run_subscription` reach `subscribe(pub_id)` before we inject.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Denied child first, then a permitted one — the cache preserves order, so
    // the subscriber would see `secret` first if it were not filtered.
    let seq = cache.incr_seq(pub_node.id).await;
    cache
        .set(
            pub_node.id,
            seq,
            StreamEvent::child_metadata_updated(
                seq,
                "secret",
                json!([]),
                json!({"leak": true}),
                None,
            ),
        )
        .await;
    let seq = cache.incr_seq(pub_node.id).await;
    cache
        .set(
            pub_node.id,
            seq,
            StreamEvent::child_metadata_updated(seq, "open", json!([]), json!({"ok": true}), None),
        )
        .await;

    let ev = next_text_json(&mut ws).await.expect("permitted event");
    assert_eq!(ev["type"], "container-child-metadata-updated");
    assert_eq!(ev["key"], "open", "leaked a denied descendant event: {ev}");
    assert!(
        next_text_json(&mut ws).await.is_none(),
        "subscriber received an extra (denied) descendant event"
    );
}

/// `container-child-created` is published on the parent (readable) but reveals
/// the new child. A subscriber to `/pub` must NOT receive it for the denied
/// child `secret_child`, but must receive it for the permitted `open_child`.
#[tokio::test]
async fn ws_subscriber_does_not_receive_child_created_for_denied_child() {
    let (base, catalog, cache, _dir) = spawn_server_with_tag_policy().await;

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

    let url = ws_url(&base, "pub");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "container-schema"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    let seq = cache.incr_seq(pub_node.id).await;
    cache
        .set(
            pub_node.id,
            seq,
            StreamEvent::child_created(
                seq,
                "secret_child",
                "container",
                json!([]),
                json!({}),
                json!([]),
                json!({"tags": ["secret"]}),
            ),
        )
        .await;
    let seq = cache.incr_seq(pub_node.id).await;
    cache
        .set(
            pub_node.id,
            seq,
            StreamEvent::child_created(
                seq,
                "open_child",
                "container",
                json!([]),
                json!({}),
                json!([]),
                json!({}),
            ),
        )
        .await;

    let ev = next_text_json(&mut ws)
        .await
        .expect("permitted child-created");
    assert_eq!(ev["type"], "container-child-created");
    assert_eq!(
        ev["key"], "open_child",
        "leaked child-created for a denied child: {ev}"
    );
    assert!(
        next_text_json(&mut ws).await.is_none(),
        "subscriber received child-created for a denied child"
    );
}
