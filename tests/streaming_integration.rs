//! WebSocket subscription end-to-end (Wave-24 PR2b): spawn a server, connect a
//! client, and drive events through the per-node streaming cache. The first
//! message is the node's per-family schema; subsequent messages are the flat
//! event metadata (`{"type": ..., "sequence": n, ...}`) in either JSON text
//! frames (default) or msgpack binary frames (`?envelope_format=msgpack`).

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

use tiled_rs::access::{Scope, ScopeSet, TagBasedPolicy};
use tiled_rs::catalog::node::RegisterRequest;
use tiled_rs::catalog::webhook::WebhookCreate;
use tiled_rs::catalog::{Catalog, adapter::UnresolvedLeaf};
use tiled_rs::core::adapters::ContainerAdapter;
use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::{ArrayStructure, RaggedStructure};
use tiled_rs::server::file_resolver::FileLeafResolver;
use tiled_rs::server::streaming_cache::{InMemoryStreamingCache, StreamEvent, StreamingCache};

/// Build a test `AppState` backed by the given catalog, streaming cache, leaf
/// resolver, and optional access policy. No auth backend is configured, so the
/// WS handshake grants the anonymous principal full scopes (the policy, when
/// present, then narrows per node).
fn build_state(
    catalog: Catalog,
    cache: Arc<dyn StreamingCache>,
    access_policy: Option<Arc<dyn tiled_rs::access::AccessPolicy>>,
    resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver>,
) -> tiled_rs::server::AppState {
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
        root_path: String::new(),
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
        allow_anonymous_access: false,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
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
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let state = build_state(catalog, test_cache(), None, resolver);
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

/// Finding #2 parity: the `container-child-created` event streams each child data
/// source stamped with its DB-assigned primary key (upstream adapter.py:847-855).
/// Before the fix the event carried the raw request objects, whose `id` is null,
/// so a subscriber could not address the newly created data source.
#[tokio::test]
async fn child_created_streams_data_sources_with_db_ids() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;

    // A parent container so the child create streams on a subscribable node — a
    // root-level create has no parent node id and does not stream.
    let resp = client
        .post(format!("{base}/api/v1/metadata/"))
        .json(&json!({
            "key": "expt",
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
        "create parent: {}",
        resp.status()
    );

    let url = ws_url(&base, "expt");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "container-schema"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A child array WITH one managed data source under the subscribed parent.
    let resp = client
        .post(format!("{base}/api/v1/metadata/expt"))
        .json(&json!({
            "key": "scan1",
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "array",
                "structure": f64_array_structure(4, vec![4]),
                "id": null,
                "mimetype": "application/x-npy",
                "parameters": {},
                "properties": {},
                "assets": [],
                "management": "writable",
            }],
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create child: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("child-created");
    assert_eq!(ev["type"], "container-child-created");
    assert_eq!(ev["key"], "scan1");

    // The streamed data source carries the DB-assigned id, matching what the
    // catalog persisted (read back via ?include_data_sources=true).
    let persisted = data_source_id(&client, &base, "expt/scan1").await;
    assert!(persisted > 0, "persisted id must be a real primary key");
    assert_eq!(
        ev["data_sources"][0]["id"].as_i64(),
        Some(persisted),
        "streamed data_source id must match the persisted DB id, event: {ev}"
    );
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
/// principal → full scopes, narrowed per node by the policy: `"public"`-tagged
/// nodes are world-readable, every other node — untagged or otherwise tagged —
/// is denied to the anonymous principal). Returns the catalog (to seed nodes)
/// and the streaming cache (to inject parent-stream events directly, bypassing
/// the write path).
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
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let state = build_state(
        catalog.clone(),
        cache.clone(),
        Some(access_policy),
        resolver,
    );
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
        .create_node(
            None,
            vec![],
            container_node("pub", json!({"tags": ["public"]})),
        )
        .await
        .unwrap();
    catalog
        .create_node(
            Some(pub_node.id),
            vec!["pub".into()],
            container_node("open", json!({"tags": ["public"]})),
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
        .create_node(
            None,
            vec![],
            container_node("pub", json!({"tags": ["public"]})),
        )
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
            container_node("open_child", json!({"tags": ["public"]})),
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
                json!({"tags": ["public"]}),
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

// ---------------------------------------------------------------------------
// PR3: array-data payload events (Wave-24).
//
// A managed writable array node streams an `array-data` event on its OWN node
// id at each write site (full / block / patch). The json envelope transcodes
// the raw C-order payload into nested lists identical to the read path; the
// msgpack envelope embeds the raw bytes as a msgpack **bin** (byte string).
// These tests need BOTH internally-managed writable storage (to accept array
// writes) and a live streaming cache, so they use a dedicated server harness.
// ---------------------------------------------------------------------------

/// Spawn a TCP server whose catalog has `writable_dir` as writable storage and
/// whose leaf resolver reads/writes only under it, backed by an in-memory
/// streaming cache. Returns the http base, a reqwest client, and both TempDirs
/// (keep them alive for the test's duration — the SQLite pool and data files
/// live inside them).
async fn spawn_write_server() -> (
    String,
    reqwest::Client,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let db_dir = tempfile::tempdir().unwrap();
    let writable_dir = tempfile::tempdir().unwrap();
    // canonicalize: init_storage builds an absolute file:// URI and the resolver
    // compares canonical paths.
    let writable_root = writable_dir.path().canonicalize().unwrap();
    let uri = format!("sqlite://{}", db_dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri)
        .await
        .unwrap()
        .with_managed_delete_dirs(vec![writable_root.clone()])
        .with_writable_storage(vec![writable_root.clone()]);
    catalog.migrate().await.unwrap();
    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> =
        Arc::new(FileLeafResolver::new(vec![writable_root]));
    let state = build_state(catalog, test_cache(), None, resolver);
    (
        serve(state).await,
        reqwest::Client::new(),
        writable_dir,
        db_dir,
    )
}

/// A 1-D little-endian f64 array structure with the given shape and per-axis
/// chunk sizes, serialized to the JSON the node-create endpoint expects.
fn f64_array_structure(shape: usize, chunk_sizes: Vec<usize>) -> Value {
    let st = ArrayStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        chunks: vec![chunk_sizes],
        shape: vec![shape],
        dims: None,
        resizable: Default::default(),
    };
    serde_json::to_value(st).unwrap()
}

/// POST `/metadata` to create a managed (writable) f64 array node; the server
/// generates the skeleton under writable storage. `mimetype` selects the
/// backing store: `application/x-npy` (whole-array writes only) or
/// `application/x-zarr` (chunked, so it accepts block writes and PATCH).
async fn create_managed_array(
    client: &reqwest::Client,
    base: &str,
    key: &str,
    mimetype: &str,
    structure: Value,
) {
    let resp = client
        .post(format!("{base}/api/v1/metadata/"))
        .json(&json!({
            "key": key,
            "structure_family": "array",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "array",
                "structure": structure,
                "id": null,
                "mimetype": mimetype,
                "parameters": {},
                "properties": {},
                "assets": [],
                "management": "writable",
            }],
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "create {key}: {status} {text}");
}

/// Little-endian byte buffer of an f64 slice (the dense array wire form).
fn f64_bytes(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// GET `/array/full/{path}` with `Accept: application/json` — the read-path
/// nested-list serialization the json envelope transcode must match.
async fn read_array_json(client: &reqwest::Client, base: &str, path: &str) -> Value {
    client
        .get(format!("{base}/api/v1/array/full/{path}"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// A decoded msgpack `array-data` frame. `payload` is a `serde_bytes::ByteBuf`,
/// so decoding SUCCEEDS only if the wire payload is a msgpack **bin** — an
/// array-of-ints (the wrong encoding PR3 fixes) would fail this deserialize.
#[derive(serde::Deserialize)]
struct MsgpackArrayData {
    #[serde(rename = "type")]
    typ: String,
    mimetype: String,
    shape: Vec<usize>,
    #[serde(default)]
    offset: Option<Vec<usize>>,
    #[serde(default)]
    block: Option<Vec<usize>>,
    payload: serde_bytes::ByteBuf,
}

/// Next msgpack binary frame decoded as an `array-data` envelope (payload as a
/// msgpack bin). Panics on timeout/close/non-binary.
async fn next_binary_array_data<S>(ws: &mut S) -> MsgpackArrayData
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) => {
            rmp_serde::from_slice(&b).expect("array-data msgpack (bin payload)")
        }
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => panic!("no array-data frame"),
        Ok(Some(Ok(other))) => panic!("expected binary frame, got {other:?}"),
        Ok(Some(Err(e))) => panic!("ws error: {e}"),
    }
}

/// A full-array write streams an `array-data` event on the array's own stream
/// (json envelope): mimetype octet-stream, the whole shape, no offset/block,
/// and a `payload` of nested lists identical to the read-path serialization.
#[tokio::test]
async fn array_full_write_streams_array_data_json() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let schema = next_text_json(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "array-schema");

    // Let `run_subscription` reach its live loop before the write publishes.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let resp = client
        .put(format!("{base}/api/v1/array/full/arr"))
        .body(f64_bytes(&values))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "write: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("array-data");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["mimetype"], "application/octet-stream");
    assert_eq!(ev["shape"], json!([4]));
    assert_eq!(ev["offset"], Value::Null);
    assert_eq!(ev["block"], Value::Null);
    assert_eq!(ev["content-type"], "application/json");
    // The transcoded payload is nested lists byte-identical to the read path.
    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(ev["payload"], read_json);
    assert_eq!(ev["payload"], json!([1.5, 2.5, 3.5, 4.5]));
}

/// The msgpack envelope embeds the same write as a msgpack **bin** payload equal
/// to the raw C-order bytes written (proves the bin encoding, not array-of-ints).
#[tokio::test]
async fn array_full_write_streams_array_data_msgpack_bin() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = format!("{}?envelope_format=msgpack", ws_url(&base, "arr"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let schema = next_binary_msgpack(&mut ws).await.expect("schema");
    assert_eq!(schema["type"], "array-schema");

    tokio::time::sleep(Duration::from_millis(150)).await;
    let values = [1.5f64, 2.5, 3.5, 4.5];
    let raw = f64_bytes(&values);
    let resp = client
        .put(format!("{base}/api/v1/array/full/arr"))
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "write: {}", resp.status());

    let ev = next_binary_array_data(&mut ws).await;
    assert_eq!(ev.typ, "array-data");
    assert_eq!(ev.mimetype, "application/octet-stream");
    assert_eq!(ev.shape, vec![4]);
    assert_eq!(ev.offset, None);
    assert_eq!(ev.block, None);
    // Payload is a msgpack bin equal to the raw little-endian buffer written.
    assert_eq!(ev.payload.into_vec(), raw);
}

/// A single-chunk block write streams `array-data` with the chunk coordinate in
/// `block`, the chunk's dense shape, and the chunk's transcoded nested lists.
#[tokio::test]
async fn array_block_write_streams_array_data_with_block() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // shape [4] in two chunks of 2 — block index 1 targets the second chunk.
    // Zarr backs chunked block writes (npy is single-chunk-per-axis).
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-zarr",
        f64_array_structure(4, vec![2, 2]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/array/block/arr?block=1"))
        .body(f64_bytes(&[5.5, 6.5]))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "block write: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("array-data");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["mimetype"], "application/octet-stream");
    assert_eq!(ev["shape"], json!([2]));
    assert_eq!(ev["block"], json!([1]));
    assert_eq!(ev["offset"], Value::Null);
    assert_eq!(ev["payload"], json!([5.5, 6.5]));
}

/// A PATCH streams `array-data` carrying the incoming block's `offset`/`shape`
/// and its transcoded nested-list payload.
#[tokio::test]
async fn array_patch_streams_array_data_with_offset() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // Zarr backs PATCH (npy has no patch face).
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-zarr",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Place a 2-element block at offset 1 within the length-4 array.
    let resp = client
        .patch(format!("{base}/api/v1/array/full/arr?shape=2&offset=1"))
        .body(f64_bytes(&[7.5, 8.5]))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "patch: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("array-data");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["mimetype"], "application/octet-stream");
    assert_eq!(ev["shape"], json!([2]));
    assert_eq!(ev["offset"], json!([1]));
    assert_eq!(ev["block"], Value::Null);
    assert_eq!(ev["payload"], json!([7.5, 8.5]));
}

/// A PATCH with `persist=false` (stream-only) still streams `array-data`:
/// upstream `patch` calls `_stream` BEFORE the `if not persist: return`
/// (catalog/adapter.py:1702-1706), so subscribers see the block regardless.
#[tokio::test]
async fn array_patch_persist_false_still_streams() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // Zarr can actually persist a PATCH, so a zeros read-back below proves the
    // `persist=false` path streamed but genuinely skipped the write.
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-zarr",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .patch(format!(
            "{base}/api/v1/array/full/arr?shape=2&offset=1&persist=false"
        ))
        .body(f64_bytes(&[9.5, 10.5]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream-only patch: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("array-data (persist=false)");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["shape"], json!([2]));
    assert_eq!(ev["offset"], json!([1]));
    assert_eq!(ev["payload"], json!([9.5, 10.5]));

    // persist=false must NOT have written the data: a read-back stays zeros.
    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(read_json, json!([0.0, 0.0, 0.0, 0.0]));
}

/// A whole-array PUT with `persist=false` (stream-only) streams `array-data` to
/// live subscribers but skips the storage commit; the default `persist=true`
/// commits AND streams. Upstream `write` calls `_stream` BEFORE `if not persist:
/// return` (catalog/adapter.py:1665-1670), and `put_array_full` threads `persist`
/// (router.py:2022).
#[tokio::test]
async fn array_full_write_persist_false_streams_but_skips_storage() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // Zarr genuinely persists a full write, so a zeros read-back below proves the
    // `persist=false` path streamed but skipped the write.
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-zarr",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // persist=false: streams the payload but must NOT commit.
    let resp = client
        .put(format!("{base}/api/v1/array/full/arr?persist=false"))
        .body(f64_bytes(&[9.5, 10.5, 11.5, 12.5]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream-only write: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("array-data (persist=false)");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["shape"], json!([4]));
    assert_eq!(ev["offset"], Value::Null);
    assert_eq!(ev["block"], Value::Null);
    assert_eq!(ev["payload"], json!([9.5, 10.5, 11.5, 12.5]));

    // persist=false must NOT have written: a read-back stays zeros.
    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(read_json, json!([0.0, 0.0, 0.0, 0.0]));

    // Default persist=true commits AND streams (regression: behavior unchanged).
    let resp = client
        .put(format!("{base}/api/v1/array/full/arr"))
        .body(f64_bytes(&[1.5, 2.5, 3.5, 4.5]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "committing write: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("array-data (persist=true)");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["payload"], json!([1.5, 2.5, 3.5, 4.5]));

    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(read_json, json!([1.5, 2.5, 3.5, 4.5]));
}

/// A single-chunk block PUT with `persist=false` streams `array-data` (carrying
/// the chunk coordinate) to live subscribers but skips the storage commit; the
/// default `persist=true` commits AND streams. Mirrors the full-write case for
/// the per-chunk `write_block` path (catalog/adapter.py:1682-1699).
#[tokio::test]
async fn array_block_write_persist_false_streams_but_skips_storage() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // shape [4] in two chunks of 2; block index 1 targets the second chunk.
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-zarr",
        f64_array_structure(4, vec![2, 2]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // persist=false: streams the block but must NOT commit.
    let resp = client
        .put(format!(
            "{base}/api/v1/array/block/arr?block=1&persist=false"
        ))
        .body(f64_bytes(&[5.5, 6.5]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream-only block write: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("array-data (persist=false)");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["shape"], json!([2]));
    assert_eq!(ev["block"], json!([1]));
    assert_eq!(ev["payload"], json!([5.5, 6.5]));

    // persist=false must NOT have written: the whole array stays zeros.
    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(read_json, json!([0.0, 0.0, 0.0, 0.0]));

    // Default persist=true commits block 1 AND streams (regression).
    let resp = client
        .put(format!("{base}/api/v1/array/block/arr?block=1"))
        .body(f64_bytes(&[5.5, 6.5]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "committing block write: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("array-data (persist=true)");
    assert_eq!(ev["type"], "array-data");
    assert_eq!(ev["block"], json!([1]));
    assert_eq!(ev["payload"], json!([5.5, 6.5]));

    // Block 1 committed; block 0 remains zeros.
    let read_json = read_array_json(&client, &base, "arr").await;
    assert_eq!(read_json, json!([0.0, 0.0, 5.5, 6.5]));
}

// ---------------------------------------------------------------------------
// PR4: table-data + ragged-data payload events (Wave-24).
//
// A managed writable table node streams `table-data` at each write site (full /
// partition-put / partition-patch); a managed writable ragged node streams
// `ragged-data` (full / block / patch). The json envelope transcodes the payload
// via the read-path serializer — a table becomes a column-name→values map, a
// ragged array a nested list; the msgpack envelope embeds the raw wire bytes as
// a msgpack **bin**. Reuses the PR3 `spawn_write_server` harness.
// ---------------------------------------------------------------------------

/// Two-Int64-column (`x`, `y`) Arrow schema for the table tests.
fn xy_int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
    ]))
}

/// Encode `(x, y)` column vectors as an Arrow IPC FILE stream — the table write
/// wire form.
fn xy_arrow_ipc(x: Vec<i64>, y: Vec<i64>) -> Vec<u8> {
    let schema = xy_int_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(x)), Arc::new(Int64Array::from(y))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut w = arrow::ipc::writer::FileWriter::try_new(&mut buf, schema.as_ref()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    buf
}

/// POST `/metadata` to create a managed (writable) CSV-backed table node with
/// columns `x`, `y` and one partition.
async fn create_managed_table(client: &reqwest::Client, base: &str, key: &str) {
    let resp = client
        .post(format!("{base}/api/v1/metadata/"))
        .json(&json!({
            "key": key,
            "structure_family": "table",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "table",
                "structure": {
                    "arrow_schema": "",
                    "npartitions": 1,
                    "columns": ["x", "y"],
                },
                "id": null,
                "mimetype": "text/csv",
                "parameters": {},
                "properties": {},
                "assets": [],
                "management": "writable",
            }],
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "create table {key}: {status} {text}");
}

/// A float64 ragged structure with axis-0 chunk sizes `chunk_rows` (summing to
/// the row count) and total leaf-element count `size`, as the create-endpoint
/// JSON.
fn ragged_f64_structure(chunk_rows: Vec<usize>, size: usize) -> Value {
    let rows: usize = chunk_rows.iter().sum();
    let st = RaggedStructure {
        data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
        shape: vec![Some(rows), None],
        size,
        chunks: vec![Some(chunk_rows), None],
        dims: None,
        resizable: Default::default(),
    };
    serde_json::to_value(st).unwrap()
}

/// POST `/metadata` to create a managed (writable) SQL-backed ragged node.
async fn create_managed_ragged(client: &reqwest::Client, base: &str, key: &str, structure: Value) {
    let resp = client
        .post(format!("{base}/api/v1/metadata/"))
        .json(&json!({
            "key": key,
            "structure_family": "ragged",
            "metadata": {},
            "specs": [],
            "data_sources": [{
                "structure_family": "ragged",
                "structure": structure,
                "id": null,
                "mimetype": "application/x-ragged+sql",
                "parameters": {},
                "properties": {},
                "assets": [],
                "management": "writable",
            }],
        }))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "create ragged {key}: {status} {text}");
}

/// GET `/table/partition/{path}?partition=N` as JSON — the read-path columns
/// dict the json envelope transcode must match.
async fn read_table_partition_json(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    partition: usize,
) -> Value {
    client
        .get(format!(
            "{base}/api/v1/table/partition/{path}?partition={partition}"
        ))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// GET `/ragged/full/{path}` as JSON — the read-path nested list.
async fn read_ragged_json(client: &reqwest::Client, base: &str, path: &str) -> Value {
    client
        .get(format!("{base}/api/v1/ragged/full/{path}"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Decode the next msgpack binary frame into `T`. `serde_bytes::ByteBuf` payload
/// fields succeed only if the wire payload is a msgpack **bin**.
async fn next_binary_typed<S, T>(ws: &mut S) -> T
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    T: serde::de::DeserializeOwned,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Binary(b)))) => rmp_serde::from_slice(&b).expect("msgpack bin frame"),
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Err(_) => panic!("no data frame"),
        Ok(Some(Ok(other))) => panic!("expected binary frame, got {other:?}"),
        Ok(Some(Err(e))) => panic!("ws error: {e}"),
    }
}

/// A decoded msgpack `table-data` frame (payload as a msgpack bin).
#[derive(serde::Deserialize)]
struct MsgpackTableData {
    #[serde(rename = "type")]
    typ: String,
    mimetype: String,
    #[serde(default)]
    partition: Option<usize>,
    append: bool,
    payload: serde_bytes::ByteBuf,
}

/// A decoded msgpack `ragged-data` frame (payload as a msgpack bin).
#[derive(serde::Deserialize)]
struct MsgpackRaggedData {
    #[serde(rename = "type")]
    typ: String,
    mimetype: String,
    shape: Vec<Option<usize>>,
    #[serde(default)]
    offset: Option<Vec<usize>>,
    #[serde(default)]
    block: Option<Vec<usize>>,
    payload: serde_bytes::ByteBuf,
}

/// A whole-table write streams `table-data` (json): partition null, append
/// false, Arrow mimetype, and a `payload` columns dict matching the read path.
#[tokio::test]
async fn table_full_write_streams_table_data_json() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_table(&client, &base, "t").await;

    let url = ws_url(&base, "t");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "table-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/table/full/t"))
        .body(xy_arrow_ipc(vec![10, 20, 30], vec![1, 2, 3]))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "table write: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("table-data");
    assert_eq!(ev["type"], "table-data");
    assert_eq!(ev["mimetype"], "application/vnd.apache.arrow.file");
    assert_eq!(ev["partition"], Value::Null);
    assert_eq!(ev["append"], json!(false));
    assert_eq!(ev["content-type"], "application/json");
    // The transcoded payload is the read-path column-dict for partition 0.
    let read_json = read_table_partition_json(&client, &base, "t", 0).await;
    assert_eq!(ev["payload"], read_json);
    assert_eq!(ev["payload"], json!({"x": [10, 20, 30], "y": [1, 2, 3]}));
}

/// The msgpack envelope ships the same table write as a msgpack **bin** payload
/// equal to the raw Arrow IPC bytes written.
#[tokio::test]
async fn table_full_write_streams_table_data_msgpack_bin() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_table(&client, &base, "t").await;

    let url = format!("{}?envelope_format=msgpack", ws_url(&base, "t"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_binary_msgpack(&mut ws).await.expect("schema")["type"],
        "table-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let arrow = xy_arrow_ipc(vec![10, 20, 30], vec![1, 2, 3]);
    let resp = client
        .put(format!("{base}/api/v1/table/full/t"))
        .body(arrow.clone())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "table write: {}", resp.status());

    let ev: MsgpackTableData = next_binary_typed(&mut ws).await;
    assert_eq!(ev.typ, "table-data");
    assert_eq!(ev.mimetype, "application/vnd.apache.arrow.file");
    assert_eq!(ev.partition, None);
    assert!(!ev.append);
    assert_eq!(ev.payload.into_vec(), arrow);
}

/// PUT `/table/partition?partition=0` streams `table-data` with the partition
/// index and `append=false`.
#[tokio::test]
async fn table_partition_put_streams_table_data() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_table(&client, &base, "t").await;

    let url = ws_url(&base, "t");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "table-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/table/partition/t?partition=0"))
        .body(xy_arrow_ipc(vec![7, 8], vec![70, 80]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "partition put: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("table-data");
    assert_eq!(ev["type"], "table-data");
    assert_eq!(ev["partition"], json!(0));
    assert_eq!(ev["append"], json!(false));
    assert_eq!(ev["payload"], json!({"x": [7, 8], "y": [70, 80]}));
}

/// PATCH `/table/partition?partition=0` streams `table-data` with `append=true`
/// — the append flag is set ONLY on the patch path — and a payload of just the
/// appended rows (not the accumulated partition).
#[tokio::test]
async fn table_partition_patch_streams_append_true() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_table(&client, &base, "t").await;

    // Seed partition 0 so the append has something to extend.
    let seed = client
        .put(format!("{base}/api/v1/table/full/t"))
        .body(xy_arrow_ipc(vec![1, 2], vec![10, 20]))
        .send()
        .await
        .unwrap();
    assert!(seed.status().is_success(), "seed: {}", seed.status());

    let url = ws_url(&base, "t");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "table-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .patch(format!("{base}/api/v1/table/partition/t?partition=0"))
        .body(xy_arrow_ipc(vec![3, 4], vec![30, 40]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "partition patch: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("table-data");
    assert_eq!(ev["type"], "table-data");
    assert_eq!(ev["partition"], json!(0));
    assert_eq!(
        ev["append"],
        json!(true),
        "append must be true on the patch path"
    );
    // The payload is the appended rows only, not the accumulated partition.
    assert_eq!(ev["payload"], json!({"x": [3, 4], "y": [30, 40]}));
}

/// A whole-ragged write streams `ragged-data` (json): the structure shape (with
/// its variable axis as `null`), no offset/block, and a `payload` nested list
/// matching the read path.
#[tokio::test]
async fn ragged_full_write_streams_ragged_data_json() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // 2 rows, one chunk of 2 rows, 3 total leaf elements.
    create_managed_ragged(&client, &base, "rag", ragged_f64_structure(vec![2], 3)).await;

    let url = ws_url(&base, "rag");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "ragged-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/ragged/full/rag"))
        .json(&json!([[1.5, 2.5], [3.5]]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "ragged write: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("ragged-data");
    assert_eq!(ev["type"], "ragged-data");
    assert_eq!(ev["mimetype"], "application/json");
    // Structure shape: 2 rows, variable inner axis (null).
    assert_eq!(ev["shape"], json!([2, null]));
    assert_eq!(ev["offset"], Value::Null);
    assert_eq!(ev["block"], Value::Null);
    // The write body was already `application/json`, so upstream adds no
    // `content-type` transcode signal (core.py:782-787).
    assert!(
        ev.get("content-type").is_none(),
        "content-type must be absent for a JSON-bodied ragged write, got {:?}",
        ev.get("content-type")
    );
    let read_json = read_ragged_json(&client, &base, "rag").await;
    assert_eq!(ev["payload"], read_json);
    assert_eq!(ev["payload"], json!([[1.5, 2.5], [3.5]]));
}

/// The msgpack envelope ships the ragged write as a msgpack **bin** payload equal
/// to the raw JSON list-of-lists bytes written.
#[tokio::test]
async fn ragged_full_write_streams_ragged_data_msgpack_bin() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_ragged(&client, &base, "rag", ragged_f64_structure(vec![2], 3)).await;

    let url = format!("{}?envelope_format=msgpack", ws_url(&base, "rag"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_binary_msgpack(&mut ws).await.expect("schema")["type"],
        "ragged-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let raw = serde_json::to_vec(&json!([[1.5, 2.5], [3.5]])).unwrap();
    let resp = client
        .put(format!("{base}/api/v1/ragged/full/rag"))
        .header("content-type", "application/json")
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "ragged write: {}",
        resp.status()
    );

    let ev: MsgpackRaggedData = next_binary_typed(&mut ws).await;
    assert_eq!(ev.typ, "ragged-data");
    assert_eq!(ev.mimetype, "application/json");
    assert_eq!(ev.shape, vec![Some(2), None]);
    assert_eq!(ev.offset, None);
    assert_eq!(ev.block, None);
    assert_eq!(ev.payload.into_vec(), raw);
}

/// A ragged block write streams `ragged-data` with the block coordinate and the
/// full structure shape.
#[tokio::test]
async fn ragged_block_write_streams_ragged_data_with_block() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    // 3 rows in two chunks (2 rows + 1 row), 6 total leaf elements.
    create_managed_ragged(&client, &base, "rag", ragged_f64_structure(vec![2, 1], 6)).await;

    let url = ws_url(&base, "rag");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "ragged-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Write chunk 1 (its single row of length 3).
    let resp = client
        .put(format!("{base}/api/v1/ragged/block/rag?block=1"))
        .json(&json!([[4.5, 5.5, 6.5]]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "ragged block: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("ragged-data");
    assert_eq!(ev["type"], "ragged-data");
    // The event carries the full structure shape (3 rows, variable inner axis).
    assert_eq!(ev["shape"], json!([3, null]));
    assert_eq!(ev["block"], json!([1]));
    assert_eq!(ev["offset"], Value::Null);
    assert_eq!(ev["payload"], json!([[4.5, 5.5, 6.5]]));
}

/// A ragged PATCH with `persist=false` still streams `ragged-data` (upstream
/// `patch` calls `_stream` before `if not persist: return`), carrying the
/// incoming block's `?shape=`/`?offset=`.
#[tokio::test]
async fn ragged_patch_persist_false_still_streams() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_ragged(&client, &base, "rag", ragged_f64_structure(vec![2], 3)).await;

    let url = ws_url(&base, "rag");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "ragged-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .patch(format!(
            "{base}/api/v1/ragged/full/rag?shape=1&offset=2&persist=false"
        ))
        .json(&json!([[7.5]]))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "stream-only ragged patch: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("ragged-data (persist=false)");
    assert_eq!(ev["type"], "ragged-data");
    // `?shape=` axes are concrete (query-param ints), `?offset=` where it lands.
    assert_eq!(ev["shape"], json!([1]));
    assert_eq!(ev["offset"], json!([2]));
    assert_eq!(ev["block"], Value::Null);
    assert_eq!(ev["payload"], json!([[7.5]]));
}

// ---------------------------------------------------------------------------
// PR5: array-ref slice-URI streaming events (Wave-24).
//
// Registering/rewriting a data source via PUT /data_source on an ARRAY node
// publishes a metadata-only `array-ref` event on that node's own stream. At WS
// send time the deliverable `?slice=` URI is derived from the event's
// patch/shape: with a patch each axis is `offset:offset+shape`, otherwise each
// full dimension is `:dim`. Non-array families emit nothing.
// ---------------------------------------------------------------------------

/// GET the node's metadata (with data sources) and return its single data
/// source id — the target of a PUT /data_source rewrite.
async fn data_source_id(client: &reqwest::Client, base: &str, key: &str) -> i64 {
    let meta: Value = client
        .get(format!(
            "{base}/api/v1/metadata/{key}?include_data_sources=true"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    meta["data"]["attributes"]["data_sources"][0]["id"]
        .as_i64()
        .unwrap_or_else(|| panic!("no data_source id in metadata: {meta}"))
}

/// A 2-D float64 array structure as the PUT /data_source rewrite carries it.
fn f64_2d_structure() -> Value {
    json!({
        "shape": [4, 3],
        "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
        "chunks": [[4], [3]],
    })
}

/// A `PUT /data_source` rewrite (no patch) on an array node streams an
/// `array-ref`: patch null, the request's shape, a metadata-only frame (no
/// payload), and a delivered `?slice=` URI of the full array (`:dim` per axis).
#[tokio::test]
async fn put_data_source_array_no_patch_streams_array_ref() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;
    let ds_id = data_source_id(&client, &base, "arr").await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/data_source/arr"))
        .json(&json!({
            "data_source": {"id": ds_id, "structure": f64_2d_structure(), "parameters": {}}
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "put_data_source: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("array-ref");
    assert_eq!(ev["type"], "array-ref");
    assert_eq!(ev["patch"], Value::Null);
    assert_eq!(ev["shape"], json!([4, 3]));
    // No patch -> each full dimension as `:dim`.
    assert_eq!(
        ev["uri"],
        Value::from(format!("{base}/api/v1/array/full/arr?slice=:4,:3"))
    );
    // `array-ref` is metadata-only.
    assert!(
        ev.get("payload").is_none(),
        "array-ref must carry no payload, got {:?}",
        ev.get("payload")
    );
    // The event echoes the request's data-source object.
    assert_eq!(ev["data_source"]["id"], json!(ds_id));
}

/// A `PUT /data_source` rewrite WITH `patch_shape`+`patch_offset` streams an
/// `array-ref` whose patch is `{shape, offset}` and whose delivered slice is the
/// per-axis `offset:offset+shape` window.
#[tokio::test]
async fn put_data_source_array_with_patch_streams_slice() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;
    let ds_id = data_source_id(&client, &base, "arr").await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!(
            "{base}/api/v1/data_source/arr?patch_shape=2,3&patch_offset=1,0"
        ))
        .json(&json!({
            "data_source": {"id": ds_id, "structure": f64_2d_structure(), "parameters": {}}
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "put_data_source: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws).await.expect("array-ref");
    assert_eq!(ev["type"], "array-ref");
    assert_eq!(ev["patch"], json!({"shape": [2, 3], "offset": [1, 0]}));
    assert_eq!(ev["shape"], json!([4, 3]));
    // With a patch -> per-axis `offset:offset+shape` => `1:3,0:3`.
    assert_eq!(
        ev["uri"],
        Value::from(format!("{base}/api/v1/array/full/arr?slice=1:3,0:3"))
    );
    assert!(ev.get("payload").is_none());
}

/// Exactly one of `patch_shape`/`patch_offset` is a 400 — they go together.
#[tokio::test]
async fn put_data_source_one_sided_patch_returns_400() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;
    let ds_id = data_source_id(&client, &base, "arr").await;

    let resp = client
        .put(format!("{base}/api/v1/data_source/arr?patch_shape=2"))
        .json(&json!({
            "data_source": {"id": ds_id, "structure": f64_2d_structure(), "parameters": {}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a one-sided patch must be 400"
    );
}

/// A `PUT /data_source` on a NON-array node (table) emits no `array-ref`.
#[tokio::test]
async fn put_data_source_non_array_streams_nothing() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_table(&client, &base, "t").await;
    let ds_id = data_source_id(&client, &base, "t").await;

    let url = ws_url(&base, "t");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "table-schema"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = client
        .put(format!("{base}/api/v1/data_source/t"))
        .json(&json!({
            "data_source": {
                "id": ds_id,
                "structure": {"arrow_schema": "", "npartitions": 1, "columns": ["x", "y"]},
                "parameters": {},
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "put_data_source table: {}",
        resp.status()
    );

    // A non-array family must not stream an `array-ref` (nothing arrives).
    assert!(
        next_text_json(&mut ws).await.is_none(),
        "non-array put_data_source must not stream an event"
    );
}

// ---------------------------------------------------------------------------
// PR6: /stream/single subscribe requires BOTH read:data AND read:metadata.
//
// Upstream resolves the WS stream entry with
// `get_entry(path, ["read:data", "read:metadata"])` once at subscribe
// (server/router.py:808-810) — a single gate that is both a token-scope check
// and a node access-policy check. tiled-rs mirrors it in two layers: a global
// token-scope gate (both scopes must be on the token) and a subscribe-time node
// gate (`subscribe_allowed`: the node must grant both under the access policy).
// The per-event `delivery_allowed` stays read:metadata-only (a metadata-
// visibility gate exercised by the F4 tests above) and is NOT raised here.
// ---------------------------------------------------------------------------

/// Seed an external array node `key` with a single f64 data source — enough for
/// the WS handler to build an `array-schema` first message. Returns the node id.
///
/// The node carries the `"team-a"` tag: under `TagBasedPolicy` (F3, untagged →
/// empty scope set) a principal-tagged node the principal holds resolves to
/// `session_scopes ∩ default_scopes`, which is exactly the per-node grant the
/// tag-policy subscribe/delete tests below exercise. `spawn_auth_stream_server`
/// grants `alice` the `"team-a"` tag in the SAME auth backend the policy reads,
/// so her grant on `arr` is controlled purely by the policy's `default_scopes`.
/// For the no-policy harnesses (webhook / global-gate tests) the tag is inert.
async fn seed_array_node(catalog: &Catalog, key: &str) -> i64 {
    let node = catalog
        .create_node(
            None,
            vec![],
            RegisterRequest {
                key: key.to_string(),
                structure_family: "array".to_string(),
                metadata: json!({}),
                specs: json!([]),
                access_blob: json!({"tags": ["team-a"]}),
            },
        )
        .await
        .unwrap();
    catalog
        .create_data_source(
            node.id,
            tiled_rs::catalog::data_source::DataSourceSpec {
                structure_family: "array".into(),
                structure: json!({
                    "shape": [4],
                    "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                    "chunks": [[4]],
                }),
                mimetype: "application/x-hdf5".into(),
                parameters: json!({}),
                management: "external".into(),
                assets: vec![],
            },
        )
        .await
        .unwrap();
    node.id
}

/// Spawn a live server with a real dummy authenticator (user `alice`, default
/// `user` role) whose per-login session scopes are capped by
/// `default_login_scopes`, an optional `TagBasedPolicy` whose `default_scopes`
/// is `policy_scopes`, and a pre-seeded external array node `arr`. With
/// `policy_scopes = None` no access policy is installed, so the global
/// token-scope gate is the only subscribe check; with `Some(scopes)` a
/// `TagBasedPolicy` is built over the SAME auth backend that authenticates
/// `alice` (so her `"team-a"` tag resolves) and the subscribe-time node gate
/// (`subscribe_allowed`) also runs against her. Returns the http base and the
/// TempDir holding the SQLite files (keep it alive for the test's duration).
async fn spawn_auth_stream_server(
    default_login_scopes: ScopeSet,
    policy_scopes: Option<ScopeSet>,
) -> (String, tempfile::TempDir) {
    spawn_auth_stream_server_cfg(default_login_scopes, policy_scopes, false).await
}

/// Same as [`spawn_auth_stream_server`] but with `allow_anonymous_access`
/// configurable. With the flag set, an unauthenticated (or invalid-token) WS
/// connect is admitted with `PUBLIC_SCOPES` (both read scopes) via the
/// anonymous fallback — the substrate for testing that a PRESENTED-but-invalid
/// `?access_token=` denies rather than silently downgrading to anonymous.
async fn spawn_auth_stream_server_cfg(
    default_login_scopes: ScopeSet,
    policy_scopes: Option<ScopeSet>,
    allow_anonymous_access: bool,
) -> (String, tempfile::TempDir) {
    use tiled_rs::auth::{AuthDb, DummyAuthenticator, Issuer};

    let dir = tempfile::tempdir().unwrap();
    let cat_uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let auth_uri = format!("sqlite://{}", dir.path().join("auth.db").display());

    // Pool size 1: warm the pool at setup so no connection is opened mid-request
    // (avoids the SQLite cold-start CANTOPEN flake on small CI runners).
    let catalog = Catalog::connect_with_pool_size(&cat_uri, 1).await.unwrap();
    catalog.migrate().await.unwrap();
    seed_array_node(&catalog, "arr").await;

    let auth_db = AuthDb::connect_with_pool_size(&auth_uri, 1).await.unwrap();
    auth_db.migrate().await.unwrap();
    // `alice` keeps the default `user` role (read:metadata + read:data + write).
    let (alice, _) = auth_db.ensure_principal("dummy", "alice").await.unwrap();
    // Grant `alice` the `"team-a"` tag `arr` carries, so a `TagBasedPolicy`
    // built over THIS same auth backend resolves her per-node grant on `arr` to
    // `session_scopes ∩ default_scopes`. (`ensure_principal` mints a random
    // UUID per DB, so the policy MUST share this backend, not a throwaway one.)
    auth_db
        .set_principal_tags(alice.id, &["team-a".to_string()])
        .await
        .unwrap();

    // Build the optional access policy over the SHARED auth backend so it sees
    // `alice`'s tag. `default_scopes = policy_scopes` caps her grant on `arr`.
    let access_policy: Option<Arc<dyn tiled_rs::access::AccessPolicy>> = policy_scopes.map(|s| {
        Arc::new(TagBasedPolicy::new(Arc::new(auth_db.clone()), s))
            as Arc<dyn tiled_rs::access::AccessPolicy>
    });

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let issuer = Issuer::new(b"this-is-a-test-secret-32-bytes-long!!").unwrap();
    let mut dummy = DummyAuthenticator::new("dummy");
    dummy.add_user("alice", "wonderland").unwrap();

    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        root_path: String::new(),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog),
        auth_db: Some(auth_db),
        issuer: Some(issuer),
        authenticators: vec![Arc::new(dummy)],
        proxied_header_auth: None,
        external_oidc: None,
        #[cfg(feature = "saml")]
        saml_providers: vec![],
        forwarded_allow_ips: None,
        max_request_body_bytes: 10 * 1024 * 1024,
        response_bytesize_limit: 300_000_000,
        streaming_cache: test_cache(),
        access_policy,
        default_login_scopes,
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: None,
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        allow_anonymous_access,
        background_tasks: tiled_rs::server::state::BackgroundTasks::new(),
        validation: Default::default(),
    };
    (serve(state).await, dir)
}

/// Log in over HTTP and return the raw access token (no `Bearer ` prefix).
async fn login_token(client: &reqwest::Client, base: &str, user: &str, pw: &str) -> String {
    let body: Value = client
        .post(format!("{base}/api/v1/auth/dummy/login"))
        .json(&json!({"username": user, "password": pw}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["access_token"]
        .as_str()
        .unwrap_or_else(|| panic!("no access_token in login response: {body}"))
        .to_string()
}

/// Connect a WS subscription to `path` presenting the bearer JWT as an
/// `?access_token=` query param and NO Authorization header — the browser
/// transport upstream decodes at connect time (`get_decoded_access_token_websocket`,
/// authentication.py:297-311; wired via `get_current_scopes_websocket`, :449-455).
/// The upgrade always succeeds (HTTP 101); any denial arrives as a text frame
/// after it. A JWT is URL-safe base64url, so it needs no percent-encoding.
async fn connect_ws_access_token_query(
    base: &str,
    path: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("{}?access_token={token}", ws_url(base, path));
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws
}

/// Create an API key for the logged-in principal (bearer `token`) with the given
/// scopes via `POST /api/v1/auth/apikeys`, returning the raw secret. Presented on
/// WS as `Apikey {secret}` — the only credential upstream's `get_api_key_websocket`
/// (authentication.py:283-294) accepts in the WS Authorization header.
async fn create_apikey(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    scopes: &[&str],
) -> String {
    let body: Value = client
        .post(format!("{base}/api/v1/auth/apikeys"))
        .header("authorization", format!("Bearer {token}"))
        .json(&json!({"note": "ws-test", "scopes": scopes}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["secret"]
        .as_str()
        .unwrap_or_else(|| panic!("no secret in apikey create response: {body}"))
        .to_string()
}

/// Connect a WS subscription to `path` presenting `Apikey {api_key}` in the
/// Authorization header — the ONLY scheme upstream's `get_api_key_websocket`
/// (authentication.py:283-294) accepts on the WS Authorization header. The
/// upgrade succeeds (HTTP 101); any authorization denial arrives as a text frame.
async fn connect_ws_apikey(
    base: &str,
    path: &str,
    api_key: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
    let mut req = ws_url(base, path).as_str().into_client_request().unwrap();
    req.headers_mut().insert(
        HeaderName::from_static("authorization"),
        HeaderValue::from_str(&format!("Apikey {api_key}")).unwrap(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

/// Connect a WS subscription presenting `Apikey {api_key}` in the Authorization
/// header AND `?access_token={query_token}` in the query — the upstream-faithful
/// coexistence of a valid WS header credential and a query token. The WS header
/// is apikey-only (`get_api_key_websocket`), so this replaces the former
/// bearer-header variant. Exercises the connect-time precedence: upstream
/// validates the query token unconditionally even behind the valid header (F1).
/// The upgrade succeeds (HTTP 101); any denial arrives as a text frame after it.
async fn connect_ws_apikey_and_query(
    base: &str,
    path: &str,
    api_key: &str,
    query_token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
    let url = format!("{}?access_token={query_token}", ws_url(base, path));
    let mut req = url.as_str().into_client_request().unwrap();
    req.headers_mut().insert(
        HeaderName::from_static("authorization"),
        HeaderValue::from_str(&format!("Apikey {api_key}")).unwrap(),
    );
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

/// Attempt a WS upgrade to `path` with a raw Authorization header value and
/// return the HTTP status of a REJECTED upgrade (the WS was never accepted).
/// Panics if the upgrade unexpectedly succeeds. Used to assert upstream's
/// `get_api_key_websocket` 400 for a non-`apikey` scheme, raised during WS
/// dependency resolution before the accept.
async fn ws_upgrade_status_with_auth(base: &str, path: &str, auth_value: &str) -> u16 {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
    let mut req = ws_url(base, path).as_str().into_client_request().unwrap();
    req.headers_mut().insert(
        HeaderName::from_static("authorization"),
        HeaderValue::from_str(auth_value).unwrap(),
    );
    match tokio_tungstenite::connect_async(req).await {
        Ok(_) => panic!("WS upgrade unexpectedly succeeded for Authorization: {auth_value:?}"),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
        Err(other) => panic!("expected an HTTP error response, got: {other:?}"),
    }
}

/// Read the next frame as raw text (no JSON parse), or `None` on close/timeout.
/// Used for the plain-text `forbidden:` / `subscription denied:` rejection
/// frames the handler sends before closing (which `next_text_json` would panic
/// on, as they are not JSON).
async fn next_frame_text<S>(ws: &mut S) -> Option<String>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => Some(t.to_string()),
        _ => None,
    }
}

/// (a) A token carrying `read:metadata` but NOT `read:data` is REJECTED at the
/// global token-scope gate (before PR6 it was allowed on read:metadata alone).
/// The first frame is the plain-text forbidden notice, not a schema.
#[tokio::test]
async fn subscribe_without_read_data_scope_is_rejected() {
    let (base, _dir) =
        spawn_auth_stream_server(ScopeSet::from_iter([Scope::ReadMetadata]), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    let frame = next_frame_text(&mut ws)
        .await
        .expect("a forbidden text frame before close");
    assert!(
        frame.contains("forbidden") && frame.contains("read:data"),
        "expected the read:data/read:metadata forbidden frame, got: {frame:?}"
    );
}

/// (b) A token carrying BOTH read:data AND read:metadata subscribes normally:
/// the array-schema first message arrives (no regression on the legitimate
/// path).
#[tokio::test]
async fn subscribe_with_both_read_scopes_proceeds() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::read_only(), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// A bearer JWT presented via the `?access_token=` query param (and NO
/// Authorization header) authenticates the subscription — upstream's WS query
/// transport for a token, since a browser cannot set Authorization on a WS
/// upgrade (`get_decoded_access_token_websocket`, authentication.py:297-311;
/// resolved by `get_current_scopes_websocket`, :449-455). With anonymous access
/// OFF, a token carrying both read scopes must yield the array-schema first
/// message; before the query transport was wired the connect fell through to the
/// (empty) first-message handshake and no schema arrived.
#[tokio::test]
async fn subscribe_via_access_token_query_authenticates() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::read_only(), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message via ?access_token=");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// The WS first-message auth handshake uses upstream's key names: a
/// `{"type":"auth","access_token":"<jwt>"}` message authenticates the
/// subscription. Upstream `authenticate_websocket_first_message` reads
/// `access_token`/`api_key` (authentication.py:460, docstring :488-490); the port
/// previously read `bearer`/`apikey`, so an upstream-shaped first message was
/// rejected and no schema arrived. No header and no query are presented, forcing
/// the first-message handshake path.
#[tokio::test]
async fn first_message_access_token_authenticates() {
    use futures::SinkExt;

    let (base, _dir) = spawn_auth_stream_server(ScopeSet::read_only(), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(&base, "arr"))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"type": "auth", "access_token": token}).to_string(),
    ))
    .await
    .unwrap();

    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message after an access_token first message");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// A PRESENTED-but-invalid `?access_token=` query JWT must be DENIED, NOT
/// silently downgraded to the anonymous `PUBLIC_SCOPES` fallback — even on an
/// `allow_anonymous_access` server. Upstream treats a presented token as a
/// committed credential: `decode_token` (authentication.py:153-177) raises
/// `HTTPException(401, "Could not validate credentials")` on any JWT error (and
/// `get_decoded_access_token_websocket`, :297-311, raises 401 "Access token has
/// expired" on expiry) DURING WS dependency resolution — before
/// `get_current_scopes_websocket`'s `allow_anonymous_access` fallback (:454-457)
/// is ever reached. So an invalid token is a hard deny, never an anonymous
/// admission.
///
/// With anonymous access ON, `PUBLIC_SCOPES` carries BOTH read scopes, so the
/// pre-fix fall-through admitted the connection as anonymous and the
/// array-schema first message arrived — the silent downgrade. The fix denies:
/// a plain-text `access_token: …` reject frame arrives and NO schema is sent.
#[tokio::test]
async fn subscribe_invalid_access_token_query_denied_even_when_anonymous_allowed() {
    let (base, _dir) = spawn_auth_stream_server_cfg(ScopeSet::read_only(), None, true).await;

    let mut ws = connect_ws_access_token_query(&base, "arr", "not-a-valid-jwt").await;
    let frame = next_frame_text(&mut ws)
        .await
        .expect("a deny text frame, not an anonymous-downgrade array-schema");
    assert!(
        frame.starts_with("access_token:"),
        "expected an access_token reject frame (no anonymous downgrade), got: {frame:?}"
    );
}

/// A PRESENTED-but-invalid `?access_token=` on an auth-REQUIRED server
/// (anonymous OFF) must be denied IMMEDIATELY. The pre-fix code set the resolved
/// context to `None` and fell through to `handshake_auth`, which blocks up to
/// 10s waiting for a first message before timing out. Upstream denies at
/// connect-time dependency resolution (401), with no first-message window. After
/// the fix the deny frame arrives at once (well inside `next_frame_text`'s
/// 500 ms budget); pre-fix, no frame arrives in that window because the
/// handshake is still waiting.
#[tokio::test]
async fn subscribe_invalid_access_token_query_denied_immediately() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::read_only(), None).await;

    let mut ws = connect_ws_access_token_query(&base, "arr", "not-a-valid-jwt").await;
    let frame = next_frame_text(&mut ws)
        .await
        .expect("an immediate deny frame, not a 10s first-message wait");
    assert!(
        frame.starts_with("access_token:"),
        "expected an access_token reject frame, got: {frame:?}"
    );
}

/// F1 (wave-35) — a PRESENTED-but-invalid `?access_token=` query JWT must be
/// DENIED even when a VALID header credential is ALSO present. Upstream declares
/// `get_decoded_access_token_websocket` as a direct WS dependency
/// (`router.py:766-768`), resolved BEFORE the endpoint body, so a present-but-
/// invalid/expired query token raises 401 (`authentication.py:303-311` expiry,
/// `:153-177` other JWT errors) REGARDLESS of a valid Apikey/Bearer header — the
/// header's `api_key` branch only shadows the decoded token for SCOPE SELECTION
/// (`:449-455`); it does not skip the token's validation. The port previously let
/// the valid header win and never validated the query token, silently admitting
/// (RED before this fix: the array-schema arrives, no deny frame). The header is
/// an Apikey — the only credential the WS Authorization header accepts
/// (`get_api_key_websocket`) — and exercises the port's `real_header` path.
#[tokio::test]
async fn subscribe_valid_header_with_invalid_access_token_query_is_denied() {
    let (base, _dir) = spawn_auth_stream_server(
        ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData, Scope::CreateApiKeys]),
        None,
    )
    .await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;
    let apikey = create_apikey(&client, &base, &token, &["read:metadata", "read:data"]).await;

    // Valid Apikey header + garbage query token: upstream denies at connect-time
    // dependency resolution; the port must deny too, not admit on the header. The
    // header is an Apikey because the WS Authorization header is apikey-only
    // (upstream `get_api_key_websocket`, authentication.py:283-294).
    let mut ws = connect_ws_apikey_and_query(&base, "arr", &apikey, "not-a-valid-jwt").await;
    let frame = next_frame_text(&mut ws)
        .await
        .expect("a deny text frame, not a header-wins array-schema admit");
    assert!(
        frame.starts_with("access_token:"),
        "expected an access_token reject frame (present-but-invalid query token \
         denies even behind a valid header), got: {frame:?}"
    );
}

/// F1 guard (wave-35) — a valid header credential AND a valid `?access_token=`
/// query token together still ADMIT: the query token validates cleanly, so there
/// is nothing to deny, and the header supplies the scopes (upstream's `api_key`
/// branch shadows the decoded token for scope selection, `:449-455`). Pins that
/// the invalid-token deny does not over-fire on a valid coexisting token.
#[tokio::test]
async fn subscribe_valid_header_with_valid_access_token_query_admits() {
    let (base, _dir) = spawn_auth_stream_server(
        ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData, Scope::CreateApiKeys]),
        None,
    )
    .await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;
    let apikey = create_apikey(&client, &base, &token, &["read:metadata", "read:data"]).await;

    // A valid Apikey header + a valid query token — must authenticate.
    let mut ws = connect_ws_apikey_and_query(&base, "arr", &apikey, &token).await;
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message with valid Apikey header + valid query token");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// COMMIT 5 (F4 family — WS): upstream `get_api_key_websocket`
/// (authentication.py:283-294) makes the WS Authorization header apikey-ONLY. A
/// present header whose scheme is not `apikey` — `Bearer` (which reaches WS via
/// `?access_token=`, never the header) or anything else — raises 400 during WS
/// dependency resolution, BEFORE the upgrade, rather than silently falling
/// through to anonymous/handshake. A valid `Apikey` header still authenticates;
/// an absent header is unchanged (covered by the access_token/handshake tests).
#[tokio::test]
async fn ws_non_apikey_authorization_scheme_is_400() {
    let (base, _dir) = spawn_auth_stream_server(
        ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData, Scope::CreateApiKeys]),
        None,
    )
    .await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    // A Bearer header on WS is a 400 — bearer belongs in ?access_token=.
    let status = ws_upgrade_status_with_auth(&base, "arr", &format!("Bearer {token}")).await;
    assert_eq!(
        status, 400,
        "a Bearer Authorization header on WS must be 400 (upstream authentication.py:289)"
    );

    // A non-auth scheme is likewise a 400.
    let status = ws_upgrade_status_with_auth(&base, "arr", "Basic dXNlcjpwYXNz").await;
    assert_eq!(
        status, 400,
        "a Basic Authorization header on WS must be 400 (upstream authentication.py:289)"
    );

    // Positive control: a valid Apikey header authenticates → array-schema.
    let apikey = create_apikey(&client, &base, &token, &["read:metadata", "read:data"]).await;
    let mut ws = connect_ws_apikey(&base, "arr", &apikey).await;
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message via a valid Apikey header");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// (c) A subscriber whose TOKEN carries BOTH read scopes (so the global gate
/// passes) but who is narrowed to read:metadata-only on the node by the access
/// policy is REJECTED at the subscribe-time node gate (`subscribe_allowed`). The
/// authenticated `user` principal (holding the `"team-a"` tag `arr` carries)
/// resolves the node through `principal_decision`, whose grant is
/// `session_scopes ∩ default_scopes` = `{read:metadata}` — so the node's
/// read:data requirement fails.
#[tokio::test]
async fn subscribe_node_denied_read_data_is_rejected() {
    let (base, _dir) = spawn_auth_stream_server(
        ScopeSet::read_only(),
        Some(ScopeSet::from_iter([Scope::ReadMetadata])),
    )
    .await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    let frame = next_frame_text(&mut ws)
        .await
        .expect("a subscription-denied text frame before close");
    assert!(
        frame.contains("subscription denied"),
        "expected the node-gate denial frame, got: {frame:?}"
    );
}

/// (c, positive control) The SAME seeded node and token, but the policy's
/// `default_scopes` grants read:data on the `"team-a"` node → subscribe proceeds
/// and the schema arrives. This proves the rejection above is specifically the
/// node's read:data denial (the node exists and resolves for the same
/// principal), not a missing/invisible node or the global token gate.
#[tokio::test]
async fn subscribe_node_grants_read_data_proceeds() {
    let (base, _dir) =
        spawn_auth_stream_server(ScopeSet::read_only(), Some(ScopeSet::read_only())).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

/// (d) The no-access-policy path is unchanged: with no policy and no auth
/// backend, the anonymous principal gets full scopes (both read scopes), so both
/// gates pass and the schema arrives.
#[tokio::test]
async fn subscribe_no_policy_anonymous_receives_schema() {
    let (base, client, _wdir, _dbdir) = spawn_write_server().await;
    create_managed_array(
        &client,
        &base,
        "arr",
        "application/x-npy",
        f64_array_structure(4, vec![4]),
    )
    .await;

    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let schema = next_text_json(&mut ws)
        .await
        .expect("array-schema first message");
    assert_eq!(schema["type"], "array-schema", "schema: {schema}");
}

// ---------------------------------------------------------------------------
// PR7: DELETE /stream/close/{path} — a producer ends a node's stream.
//
// Upstream `close_stream` (server/router.py:725-748), gated by write:data,
// calls `entry.close_stream()` (catalog/adapter.py:1365-1380): it
// `streaming_cache.close(node.id)` — emitting `end_of_stream` so live
// subscribers disconnect (WS close 1000 "Producer ended stream") — and fires a
// `stream-closed` webhook on the node's own id. StreamClosedEvent
// (server/schemas.py:657-661) carries only the node key.
// ---------------------------------------------------------------------------

/// Read WebSocket frames until a Close arrives; return `(code, reason)`. Ignores
/// any interleaved data frame. `None` on timeout / bodyless close. The producer
/// end-of-stream close is `(1000, "Producer ended stream")` (streaming.rs).
async fn next_close_frame<S>(ws: &mut S) -> Option<(u16, String)>
where
    S: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match tokio::time::timeout(Duration::from_millis(1500), ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(cf))))) => {
                return Some((u16::from(cf.code), cf.reason.to_string()));
            }
            Ok(Some(Ok(Message::Close(None)))) | Ok(None) | Err(_) => return None,
            // A stray data/ping frame before the close — keep reading.
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) => return None,
        }
    }
}

/// Spawn a live no-auth catalog server with a real webhook dispatcher wired and
/// a pre-seeded external array node `arr`. No auth backend → the anonymous
/// principal holds full scopes (incl. write:data), so DELETE /stream/close needs
/// no credential. Returns the http base, an HTTP client, the catalog (to
/// register webhooks), the `arr` node id, and the TempDir (keep alive).
async fn spawn_webhook_stream_server() -> (String, reqwest::Client, Catalog, i64, tempfile::TempDir)
{
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite://{}", dir.path().join("catalog.db").display());
    let catalog = Catalog::connect(&uri).await.unwrap();
    catalog.migrate().await.unwrap();
    let node_id = seed_array_node(&catalog, "arr").await;

    // A real dispatcher task, owned by this state's `BackgroundTasks`, delivers
    // matching webhooks over HTTP. Default config (HTTPS-only / no private
    // addresses) is irrelevant here: those checks run at the *register* route,
    // and this test seeds the target directly via `create_webhook`.
    let background = tiled_rs::server::state::BackgroundTasks::new();
    let dispatcher = tiled_rs::server::webhook_dispatch::spawn(
        catalog.clone(),
        tiled_rs::server::webhook_dispatch::WebhookConfig::default(),
        &background,
    );

    let resolver: Arc<dyn tiled_rs::catalog::adapter::LeafResolver> = Arc::new(UnresolvedLeaf);
    let root_tree: Arc<dyn ContainerAdapter> = Arc::new(tiled_rs::catalog::CatalogAdapter::root(
        catalog.clone(),
        resolver,
    ));
    let state = tiled_rs::server::AppState {
        root_tree,
        serialization_registry: Arc::new(tiled_rs::serialization::default_registry()),
        query_names: Query::all_query_names()
            .into_iter()
            .map(String::from)
            .collect(),
        base_url: None,
        root_path: String::new(),
        cors_policy: tiled_rs::server::state::CorsOriginPolicy::Permissive,
        trust_forwarded_headers: false,
        api_key: None,
        catalog: Some(catalog.clone()),
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
        streaming_cache: test_cache(),
        access_policy: None,
        default_login_scopes: tiled_rs::auth::ScopeSet::full(),
        enable_web: true,
        web_assets_dir: None,
        spec_views: Vec::new(),
        webhook_config: None,
        webhook_dispatcher: Some(dispatcher),
        request_timeout_secs: 30,
        expose_raw_assets: true,
        exact_count_limit: u64::MAX,
        allow_anonymous_access: false,
        background_tasks: background,
        validation: Default::default(),
    };
    (
        serve(state).await,
        reqwest::Client::new(),
        catalog,
        node_id,
        dir,
    )
}

/// Spawn a local HTTP receiver that captures each delivered webhook body onto an
/// mpsc channel. Returns the target URL (`http://127.0.0.1:PORT/hook`) and the
/// receiver end.
async fn spawn_webhook_receiver() -> (String, tokio::sync::mpsc::UnboundedReceiver<Value>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
    let app = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(body);
                axum::http::StatusCode::OK
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (format!("http://{addr}/hook"), rx)
}

/// The key end-to-end test: DELETE /stream/close on a node with write:data (a)
/// disconnects a LIVE subscriber with the producer end-of-stream close
/// (1000 / "Producer ended stream"), and (b) fires the `stream-closed` webhook
/// registered on that node, carrying the node key.
#[tokio::test]
async fn close_stream_disconnects_subscriber_and_fires_webhook() {
    let (hook_url, mut hook_rx) = spawn_webhook_receiver().await;
    let (base, client, catalog, node_id, _dir) = spawn_webhook_stream_server().await;

    // Register a webhook for `stream-closed` on the `arr` node (direct seed
    // bypasses the register route's URL validation, so http/127.0.0.1 is fine).
    catalog
        .create_webhook(WebhookCreate {
            node_id,
            url: hook_url,
            secret: None,
            events: Some(vec!["stream-closed".into()]),
        })
        .await
        .unwrap();

    // A live subscriber on `arr`: consume the schema, then let the handler reach
    // its live loop before we close.
    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Close the stream (anonymous → full scopes include write:data).
    let resp = client
        .delete(format!("{base}/api/v1/stream/close/arr"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "close should succeed with write:data: {}",
        resp.status()
    );

    // (a) The subscriber is disconnected with the producer end-of-stream close.
    let close = next_close_frame(&mut ws).await;
    assert_eq!(
        close,
        Some((1000, "Producer ended stream".to_string())),
        "subscriber must receive the producer end-of-stream close"
    );

    // (b) The `stream-closed` webhook fires with the right envelope + key.
    let payload = tokio::time::timeout(Duration::from_secs(5), hook_rx.recv())
        .await
        .expect("webhook must be delivered within 5s")
        .expect("webhook payload");
    assert_eq!(payload["event_type"], "stream-closed", "payload: {payload}");
    // tiled-rs delivers `path` as the joined node path (existing webhook
    // convention), where upstream StreamClosedEvent.path is a list[str].
    assert_eq!(payload["path"], json!("arr"), "payload: {payload}");
    assert_eq!(
        payload["data"]["type"], "stream-closed",
        "payload: {payload}"
    );
    assert_eq!(payload["data"]["key"], "arr", "payload: {payload}");
}

/// DELETE /stream/close without write:data is refused (401). An authenticated
/// principal capped to read-only session scopes lacks write:data. The route-level
/// `check_scopes(["write:data"])` gate (upstream router.py:734) raises 401 for the
/// missing scope, before the per-node `get_entry` gate is reached.
#[tokio::test]
async fn close_stream_without_write_data_is_unauthorized() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::read_only(), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let resp = client
        .delete(format!("{base}/api/v1/stream/close/arr"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        401,
        "read-only principal must be refused (upstream check_scopes → 401) closing a stream"
    );
}

/// DELETE /stream/close on a missing path returns 404 (the caller has write:data
/// via the default `user` role, so the failure is path resolution, not scope).
#[tokio::test]
async fn close_stream_missing_path_is_not_found() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::full(), None).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let resp = client
        .delete(format!("{base}/api/v1/stream/close/does-not-exist"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        404,
        "closing a missing node must be 404"
    );
}

/// Client-driven end-of-stream: the CLIENT method `BaseClient::close_stream`
/// (upstream `client/base.py:940`) issues the `DELETE /api/v1/stream/close/{path}`
/// a stream *producer* uses to end its stream, and the server disconnects a live
/// subscriber with the producer end-of-stream close (1000 / "Producer ended
/// stream"). The raw-HTTP test above drives the same route directly; this proves
/// the client method builds the correct request (right method + rewritten path)
/// and reaches it.
#[tokio::test]
async fn client_close_stream_ends_a_live_subscriber() {
    let (base, _client, _catalog, _node_id, _dir) = spawn_webhook_stream_server().await;

    // Resolve the producer's node client up front (two round-trips) so the window
    // between opening the subscriber and closing the stream stays tight.
    let node = tiled_rs::client::from_uri(&base)
        .await
        .unwrap()
        .into_container()
        .unwrap()
        .get("arr")
        .await
        .unwrap();

    // A live subscriber on `arr`: consume the schema, then let the handler reach
    // its live loop before the producer closes.
    let url = ws_url(&base, "arr");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "array-schema"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The producer ends the stream through the client method (anonymous → full
    // scopes include write:data). A 200 maps to `Ok(())`.
    node.base()
        .unwrap()
        .close_stream()
        .await
        .expect("client close_stream must issue the DELETE and succeed");

    // The subscriber is disconnected with the producer end-of-stream close.
    let close = next_close_frame(&mut ws).await;
    assert_eq!(
        close,
        Some((1000, "Producer ended stream".to_string())),
        "client close_stream must end the stream for a live subscriber"
    );
}

// ---------------------------------------------------------------------------
// Finding #1: a subscribed node's OWN lifecycle events (node-deleted, then
// end_of_stream) always reach the subscriber and are followed by the stream
// closing — regardless of whether an access policy is configured.
//
// Invariant: DELETE /metadata/{path} on a live-subscribed node delivers
// `node-deleted` THEN closes the WS (1000, "Producer ended stream"). The
// per-event `delivery_allowed` re-gate is exempt for the node's own lifecycle
// announcement (its row is already gone, so a re-lookup would 404 and drop it),
// and the delete handler follows the event with `close()` (an end_of_stream) so
// the subscriber is disconnected rather than left hanging.
// ---------------------------------------------------------------------------

/// No access policy: deleting a subscribed node delivers `node-deleted` and then
/// closes the WS with the producer end-of-stream close (1000).
#[tokio::test]
async fn delete_node_delivers_node_deleted_then_closes() {
    let (base, _dir) = spawn_server().await;
    let client = reqwest::Client::new();
    register(&client, &base, "", "doomed").await;

    let url = ws_url(&base, "doomed");
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(
        next_text_json(&mut ws).await.expect("schema")["type"],
        "container-schema"
    );
    // Let `run_subscription` reach its live loop before the delete publishes.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let resp = client
        .delete(format!("{base}/api/v1/metadata/doomed"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "delete: {}", resp.status());

    let ev = next_text_json(&mut ws).await.expect("node-deleted");
    assert_eq!(ev["type"], "node-deleted", "event: {ev}");
    let close = next_close_frame(&mut ws).await;
    assert_eq!(
        close,
        Some((1000, "Producer ended stream".to_string())),
        "the stream must close after node-deleted"
    );
}

/// WITH an access policy configured: the same delete must STILL deliver
/// `node-deleted` (this is the finding). Before the fix, the per-event
/// `delivery_allowed` re-looked-up the now-deleted node, got a 404, and dropped
/// the event — so the subscriber never learned of the deletion. The exemption
/// for the node's own lifecycle events makes delivery uniform (policy or not),
/// and the stream then closes.
///
/// Uses an authenticated `alice` (default `user` role carries
/// delete:node/delete:revision) against a tag policy whose `default_scopes` is
/// full: her `"team-a"` grant on the seeded `arr` is thus full, making `arr`
/// both subscribable and deletable, while the policy is live so
/// `delivery_allowed` does NOT short-circuit.
#[tokio::test]
async fn delete_node_delivers_node_deleted_under_access_policy() {
    let (base, _dir) = spawn_auth_stream_server(ScopeSet::full(), Some(ScopeSet::full())).await;
    let client = reqwest::Client::new();
    let token = login_token(&client, &base, "alice", "wonderland").await;

    let mut ws = connect_ws_access_token_query(&base, "arr", &token).await;
    assert_eq!(
        next_text_json(&mut ws).await.expect("array-schema")["type"],
        "array-schema"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;

    let resp = client
        .delete(format!("{base}/api/v1/metadata/arr"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "delete under policy: {}",
        resp.status()
    );

    let ev = next_text_json(&mut ws)
        .await
        .expect("node-deleted must reach the subscriber even with an access policy");
    assert_eq!(ev["type"], "node-deleted", "event: {ev}");
    let close = next_close_frame(&mut ws).await;
    assert_eq!(
        close,
        Some((1000, "Producer ended stream".to_string())),
        "the stream must close after node-deleted under a policy too"
    );
}
