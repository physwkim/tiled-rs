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
use tiled_rs::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use tiled_rs::core::queries::Query;
use tiled_rs::core::structures::ArrayStructure;
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
