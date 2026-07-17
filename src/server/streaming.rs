//! Server-side WebSocket subscriptions (Wave-24 PR2b).
//!
//! A subscriber connects to `/api/v1/stream/single/{path}` and receives the
//! node's data stream from the per-node
//! [`StreamingCache`](crate::server::streaming_cache::StreamingCache): the same
//! cache the catalog write handlers publish tree events (and, from PR3 onward,
//! data events) into. There is no notification bus — events are keyed by the
//! catalog `node_id`, so a subscriber sees exactly the events published on the
//! node it watches (its own updates plus the child-created / child-metadata
//! events its container emits), never a fanned-out ancestor feed.
//!
//! Wire format mirrors upstream `tiled.server.streaming`:
//! - The first message is the node's per-family schema
//!   (`container-schema` / `array-schema` / `table-schema` / `ragged-schema`),
//!   sent through the same envelope formatter as the events.
//! - Subsequent messages are the flat event metadata (`{"type": ..., "sequence":
//!   n, ...}`), optionally carrying a binary payload for data events.
//! - The producer closes the stream with an `end_of_stream` marker, which the
//!   handler turns into a WebSocket close (code 1000, "Producer ended stream").
//!
//! The envelope format is chosen by `?envelope_format=`: `json` (default) sends
//! JSON text frames; `msgpack` sends msgpack binary frames (upstream
//! `EnvelopeFormat`, `tiled/server/schemas.py:619`).

use std::sync::Arc;

use axum::extract::OriginalUri;
use axum::extract::{
    Query, State, WebSocketUpgrade,
    ws::{CloseFrame, Message, WebSocket},
};
use bytes::Bytes;
use serde::Deserialize;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::core::dtype::BuiltinDType;
use crate::core::structures::StructureFamily;
use crate::serialization::registry::SerializationRegistry;
use crate::server::auth_context::{AuthContext, AuthKind};
use crate::server::error::ServerError;
use crate::server::state::AppState;

/// Per-subscription context for transcoding a data event's raw payload into a
/// JSON-native value on the `json` envelope. Fixed for the life of a
/// subscription: the shared serializer registry plus, when the subscribed node
/// is an array/sparse node, its element dtype (needed to decode the raw C-order
/// payload back into nested lists). `array_dtype` is `None` for every other
/// family, so no array-data transcode is attempted there.
struct PayloadCtx {
    registry: Arc<SerializationRegistry>,
    array_dtype: Option<BuiltinDType>,
}

/// WebSocket envelope format (upstream `EnvelopeFormat`,
/// `tiled/server/schemas.py:619`). `Json` (the default) sends JSON text frames;
/// `Msgpack` sends msgpack binary frames.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnvelopeFormat {
    #[default]
    Json,
    Msgpack,
}

#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    /// Sequence number from which to resume. When set, the handler replays the
    /// cached events with `sequence >= start` up to the node's current
    /// sequence, then follows live ones. Sequences that have expired out of the
    /// cache are silently skipped (the client detects the gap by sequence).
    #[serde(default)]
    pub start: Option<u64>,
    /// Wire envelope format; defaults to [`EnvelopeFormat::Json`].
    #[serde(default)]
    pub envelope_format: EnvelopeFormat,
}

pub async fn ws_subscribe(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(q): Query<SubscribeQuery>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, ServerError> {
    // The route is mounted OUTSIDE the auth middleware (tiled#1351) so
    // browsers — which can't set Authorization on WS — can authenticate
    // via a first JSON message after the upgrade. We still accept
    // header-based auth here as a fast path so non-browser clients keep
    // working.
    let header_auth = crate::server::app::resolve_header_auth(&state, &headers).await;

    // Path mirrors upstream: `/api/v1/stream/single/{path}`. Strip the
    // prefix to recover the node-path segments (the family is resolved
    // from the node lookup, not the URL).
    const PREFIX: &str = "/api/v1/stream/single/";
    let path = uri.path();
    let segments: Vec<String> = path
        .find(PREFIX)
        .map(|idx| {
            let after = &path[idx + PREFIX.len()..];
            after
                .split('/')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    percent_encoding::percent_decode_str(s)
                        .decode_utf8_lossy()
                        .into_owned()
                })
                .collect()
        })
        .unwrap_or_default();

    // Build the per-family schema now while we still have the AppState.
    // For catalog-backed deployments we hit the DB; for in-memory trees we
    // walk the existing tree. Either way we end up with the flat schema blob
    // the client needs to interpret subsequent events.
    let schema = build_schema_payload(&state, &segments).await;

    // The node's `.../api/v1/array/full/{path}` URL, from which an `array-ref`
    // event's deliverable `?slice=` URI is built at send time (upstream
    // router.py:829-833). Uses the request base (honouring `base_url` /
    // forwarded headers) plus the raw, still-percent-encoded path segments so
    // the URI carries the same encoding the client used.
    let path_str = path
        .find(PREFIX)
        .map(|idx| {
            path[idx + PREFIX.len()..]
                .split('/')
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_default();
    let base_uri = format!(
        "{}/api/v1/array/full/{}",
        state.resolve_base_url(&headers),
        path_str
    );

    let start = q.start;
    let envelope_format = q.envelope_format;
    let state_for_handshake = state.clone();
    Ok(ws.on_upgrade(move |socket| {
        run_subscription(
            socket,
            state_for_handshake,
            segments,
            schema,
            start,
            envelope_format,
            header_auth,
            base_uri,
        )
    }))
}

/// Build the flat first-message schema for a node, shaped per structure family
/// (upstream `make_ws_schema`, `tiled/catalog/adapter.py:1624/1658/1763/1851`).
async fn build_schema_payload(state: &AppState, segments: &[String]) -> Value {
    if let Some(catalog) = state.catalog.as_ref() {
        if segments.is_empty() {
            return family_schema("container", None);
        }
        if let Ok(Some(node)) = catalog.lookup(segments).await {
            // array / sparse / ragged / table schemas carry the data_type /
            // arrow_schema drawn from the node's first data source structure.
            let structure = match node.structure_family.as_str() {
                "array" | "sparse" | "ragged" | "table" => catalog
                    .list_data_sources(node.id)
                    .await
                    .ok()
                    .and_then(|rows| rows.into_iter().next())
                    .map(|ds| ds.structure),
                _ => None,
            };
            return family_schema(&node.structure_family, structure.as_ref());
        }
    }
    // Fall back to walking the in-memory tree.
    if segments.is_empty() {
        return family_schema("container", None);
    }
    match crate::server::core::walk_tree(state.root_tree.as_ref(), segments).await {
        Ok(adapter) => {
            let structure = adapter.structure_json().await.ok().flatten();
            family_schema(&adapter.structure_family().to_string(), structure.as_ref())
        }
        Err(_) => serde_json::json!({
            "type": "subscription-error",
            "message": "node not found",
            "path": segments.join("/"),
        }),
    }
}

/// Shape the per-family schema message. `container`, `array`/`sparse` (sparse
/// inherits array upstream, `adapter.py:1846`), `ragged`, and `table` mirror
/// upstream's `make_ws_schema`. Any other family (e.g. `awkward`, `bytes`,
/// which upstream does not stream) gets a minimal typed header naming the
/// family — a tiled-rs extension so the handshake still succeeds.
fn family_schema(family: &str, structure: Option<&Value>) -> Value {
    let data_type = || {
        structure
            .and_then(|s| s.get("data_type"))
            .cloned()
            .unwrap_or(Value::Null)
    };
    match family {
        "container" => serde_json::json!({"type": "container-schema", "version": 1}),
        "array" | "sparse" => {
            serde_json::json!({"type": "array-schema", "version": 1, "data_type": data_type()})
        }
        "ragged" => {
            serde_json::json!({"type": "ragged-schema", "version": 1, "data_type": data_type()})
        }
        "table" => serde_json::json!({
            "type": "table-schema",
            "version": 1,
            "arrow_schema": structure
                .and_then(|s| s.get("arrow_schema"))
                .cloned()
                .unwrap_or(Value::Null),
        }),
        other => serde_json::json!({"type": format!("{other}-schema"), "version": 1}),
    }
}

/// Resolve the subscribed path to its catalog `node_id`, the key the streaming
/// cache is indexed by. Returns `None` for the root (`segments` empty), for
/// non-catalog deployments, and for paths with no catalog node — in every one
/// of those cases the subscriber gets the schema then no data events (D3).
async fn resolve_stream_node_id(state: &AppState, segments: &[String]) -> Option<i64> {
    if segments.is_empty() {
        return None;
    }
    let catalog = state.catalog.as_ref()?;
    match catalog.lookup(segments).await {
        Ok(Some(node)) => Some(node.id),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription(
    socket: WebSocket,
    state: AppState,
    segments: Vec<String>,
    schema: Value,
    start: Option<u64>,
    format: EnvelopeFormat,
    header_auth: Option<AuthContext>,
    base_uri: String,
) {
    let (mut tx, mut rx) = futures::StreamExt::split(socket);
    use futures::SinkExt;

    // Resolve the authentication context. Prefer the header-based auth
    // (Bearer JWT, Apikey) collected before the upgrade — that's how
    // non-browser clients usually arrive. Fall back to a first-message
    // handshake (tiled#1351) if the headers carried nothing usable.
    let auth_ctx = match header_auth {
        Some(ctx) if !matches!(ctx.kind, AuthKind::Anonymous) || state.no_auth_configured() => ctx,
        _ => match handshake_auth(&state, &mut tx, &mut rx).await {
            Ok(ctx) => ctx,
            Err(close_reason) => {
                tracing::info!(target: "tiled.streaming", "ws auth failed: {close_reason}");
                let _ = tx.send(Message::Text(close_reason.into())).await;
                let _ = tx.send(Message::Close(None)).await;
                return;
            }
        },
    };
    // The base principal must hold read:metadata at all. With no access
    // policy this is the only gate — subscriptions to non-existent paths
    // are still allowed (they simply never receive anything). The base gate
    // rises to read:data in a later wave (PR6).
    if !auth_ctx.scopes.contains(crate::auth::Scope::ReadMetadata) {
        let _ = tx
            .send(Message::Text("forbidden: missing read:metadata".into()))
            .await;
        let _ = tx.send(Message::Close(None)).await;
        return;
    }

    // Authorize the subscription node itself — the same per-message delivery
    // rule applied to every event below. Root and non-root resolve uniformly
    // here (an empty path folds to the base scope check inside `resolve_entry`).
    // With no access policy `delivery_allowed` is a single `is_none` check, so
    // behavior is unchanged (including subscriptions to missing paths).
    if !delivery_allowed(&state, &auth_ctx, &segments).await {
        let _ = tx
            .send(Message::Text(
                "subscription denied: node not found or access denied".into(),
            ))
            .await;
        let _ = tx.send(Message::Close(None)).await;
        return;
    }

    // Resolve the array element dtype once for the life of the subscription:
    // the json envelope needs it to transcode array-data payloads into nested
    // lists (upstream reads it from `entry.structure()` per format call). It is
    // carried in the array-schema (`data_type`); sparse inherits array-schema,
    // so both resolve here. Non-array subscriptions leave it `None`.
    let array_dtype = if schema.get("type").and_then(Value::as_str) == Some("array-schema") {
        schema
            .get("data_type")
            .and_then(|dt| BuiltinDType::from_json(dt).ok())
    } else {
        None
    };
    let payload_ctx = PayloadCtx {
        registry: state.serialization_registry.clone(),
        array_dtype,
    };

    // First message: the node's per-family schema, sent through the same
    // envelope formatter as the events (upstream `formatter(websocket, schema,
    // None)`).
    match encode(&schema, None, format, &payload_ctx) {
        Some(msg) => {
            if tx.send(msg).await.is_err() {
                return;
            }
        }
        None => {
            tracing::warn!(target: "tiled.streaming", "encode schema failed");
            return;
        }
    }

    // Resolve the cache key. Without a catalog node (root, in-memory tree, or an
    // unknown path) there is no stream to follow: the client keeps the schema
    // and simply waits (D3).
    let node_id = match resolve_stream_node_id(&state, &segments).await {
        Some(id) => id,
        None => {
            wait_for_close(&mut rx).await;
            return;
        }
    };

    // Subscribe BEFORE snapshotting `current_seq` so any event published during
    // replay is buffered on the broadcast channel and deduped by `last_sent`
    // rather than lost (upstream starts its live iterator before the replay).
    let mut receiver = state.streaming_cache.subscribe(node_id);
    let mut last_sent = 0u64;
    let mut ended = false;

    if let Some(start) = start {
        let current = state.streaming_cache.current_seq(node_id).await;
        last_sent = current;
        for seq in start..=current {
            match send_seq(
                &mut tx,
                &state,
                &auth_ctx,
                &segments,
                node_id,
                seq,
                format,
                &payload_ctx,
                &base_uri,
            )
            .await
            {
                SendOutcome::EndOfStream => {
                    ended = true;
                    break;
                }
                SendOutcome::ClientGone => return,
                SendOutcome::Sent | SendOutcome::Skipped => {}
            }
        }
    }

    // Follow live sequences until the producer ends the stream, the channel
    // closes, or the client disconnects. Skipped when replay already saw the
    // `end_of_stream` marker (`ended`).
    if !ended {
        loop {
            tokio::select! {
                live = receiver.recv() => {
                    match live {
                        Ok(seq) => {
                            // Skip anything already replayed (upstream's `<= last_sent`).
                            if seq <= last_sent {
                                continue;
                            }
                            match send_seq(
                                &mut tx,
                                &state,
                                &auth_ctx,
                                &segments,
                                node_id,
                                seq,
                                format,
                                &payload_ctx,
                                &base_uri,
                            )
                            .await
                            {
                                SendOutcome::EndOfStream => break,
                                SendOutcome::ClientGone => return,
                                SendOutcome::Sent | SendOutcome::Skipped => last_sent = seq,
                            }
                        }
                        // A slow subscriber that lagged past the channel depth: skip
                        // the gap and keep following (a miss is far less harmful than
                        // dropping the connection).
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                incoming = futures::StreamExt::next(&mut rx) => {
                    match incoming {
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                        _ => {}
                    }
                }
            }
        }
    }

    // The producer ended the stream (end_of_stream marker): close cleanly with
    // the upstream code/reason (`websocket.close(1000, "Producer ended stream")`).
    let _ = tx
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: "Producer ended stream".into(),
        })))
        .await;
}

/// Outcome of attempting to deliver one cached sequence.
enum SendOutcome {
    /// The event was encoded and sent.
    Sent,
    /// The event was absent/expired, encode-failed, or denied by policy —
    /// nothing sent, but the stream continues.
    Skipped,
    /// The cached event is the producer's `end_of_stream` marker.
    EndOfStream,
    /// The client's socket is gone; the caller should stop.
    ClientGone,
}

/// Fetch the cached event at `(node_id, seq)`, authorize it, and send it.
#[allow(clippy::too_many_arguments)]
async fn send_seq(
    tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    state: &AppState,
    auth_ctx: &AuthContext,
    segments: &[String],
    node_id: i64,
    seq: u64,
    format: EnvelopeFormat,
    payload_ctx: &PayloadCtx,
    base_uri: &str,
) -> SendOutcome {
    use futures::SinkExt;
    let event = match state.streaming_cache.get(node_id, seq).await {
        Some(e) => e,
        // Expired out of the cache or never stored — skip (upstream returns
        // early when `metadata` is None).
        None => return SendOutcome::Skipped,
    };
    if event.metadata.get("end_of_stream").and_then(Value::as_bool) == Some(true) {
        return SendOutcome::EndOfStream;
    }
    // Per-event authorization (D10). A `container-child-*` event is published on
    // the parent but concerns the named child, so authorize that child;
    // everything else concerns the subscribed node itself.
    if !delivery_allowed(
        state,
        auth_ctx,
        &event_target_segments(segments, &event.metadata),
    )
    .await
    {
        return SendOutcome::Skipped;
    }
    // `array-ref` events ship no payload; the deliverable `?slice=` URI is built
    // at send time from the event's patch/shape (upstream `stream_data`,
    // streaming.py:248-259). Only these events need the base URI, so clone the
    // metadata to inject `uri` and leave every other event untouched.
    let metadata: std::borrow::Cow<'_, Value> =
        if event.metadata.get("type").and_then(Value::as_str) == Some("array-ref") {
            let mut m = event.metadata.clone();
            inject_array_ref_slice_uri(&mut m, base_uri);
            std::borrow::Cow::Owned(m)
        } else {
            std::borrow::Cow::Borrowed(&event.metadata)
        };
    match encode(&metadata, event.payload.as_ref(), format, payload_ctx) {
        Some(msg) => {
            if tx.send(msg).await.is_err() {
                SendOutcome::ClientGone
            } else {
                SendOutcome::Sent
            }
        }
        None => {
            tracing::warn!(target: "tiled.streaming", "encode event failed (seq {seq})");
            SendOutcome::Skipped
        }
    }
}

/// Set an `array-ref` event's deliverable `uri` (`{base_uri}?slice={s}`),
/// mirroring upstream `stream_data` (streaming.py:248-259). With a patch, each
/// axis is `offset:offset+shape` over `patch.offset`/`patch.shape`; without one,
/// each full dimension is `:dim` over the event `shape`. `base_uri` is the
/// node's `.../array/full/{path}` URL. A malformed patch/shape leaves `uri`
/// unset rather than emitting a truncated slice.
fn inject_array_ref_slice_uri(metadata: &mut Value, base_uri: &str) {
    let slice = match metadata.get("patch") {
        // A patch descriptor `{shape, offset}` -> per-axis `offset:offset+shape`.
        Some(patch) if patch.is_object() => {
            let (Some(offsets), Some(shapes)) = (
                patch.get("offset").and_then(Value::as_array),
                patch.get("shape").and_then(Value::as_array),
            ) else {
                return;
            };
            if offsets.len() != shapes.len() {
                return;
            }
            let mut parts = Vec::with_capacity(offsets.len());
            for (o, s) in offsets.iter().zip(shapes.iter()) {
                let (Some(o), Some(s)) = (o.as_u64(), s.as_u64()) else {
                    return;
                };
                parts.push(format!("{o}:{}", o + s));
            }
            parts.join(",")
        }
        // No patch -> the full array, each dimension as `:dim`.
        _ => {
            let Some(dims) = metadata.get("shape").and_then(Value::as_array) else {
                return;
            };
            let mut parts = Vec::with_capacity(dims.len());
            for d in dims {
                let Some(d) = d.as_u64() else {
                    return;
                };
                parts.push(format!(":{d}"));
            }
            parts.join(",")
        }
    };
    if let Some(obj) = metadata.as_object_mut() {
        obj.insert(
            "uri".into(),
            Value::from(format!("{base_uri}?slice={slice}")),
        );
    }
}

/// Wait for the client to close the socket, ignoring any other inbound frames.
/// Used for schema-only subscriptions (no catalog node ⇒ no data events).
async fn wait_for_close(rx: &mut futures::stream::SplitStream<WebSocket>) {
    loop {
        match futures::StreamExt::next(rx).await {
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
            _ => {}
        }
    }
}

/// Encode one event (or the schema) into a WebSocket frame for `format`,
/// mirroring upstream `get_websocket_envelope_formatter` (core.py:754-820).
///
/// - `Json`: transcode a data payload into a JSON-native value under
///   `metadata["payload"]` (array-data → nested lists), then send the metadata
///   as a text frame. Tree/EOS events and the schema carry no payload.
/// - `Msgpack`: pack the metadata as a binary frame; a payload is embedded under
///   `"payload"` as a msgpack **bin** (byte string), matching
///   `msgpack.packb({..., "payload": <bytes>})`.
fn encode(
    metadata: &Value,
    payload: Option<&Bytes>,
    format: EnvelopeFormat,
    ctx: &PayloadCtx,
) -> Option<Message> {
    match format {
        EnvelopeFormat::Json => {
            let text = match payload {
                // Transcode the raw payload into JSON-native values. If the
                // family/mimetype can't be transcoded here, fall through to the
                // metadata alone (no `payload`) rather than emit raw bytes.
                Some(bytes) => {
                    let mut m = metadata.clone();
                    if let Some(json_payload) = transcode_payload_to_json(&m, bytes, ctx)
                        && let Some(obj) = m.as_object_mut()
                    {
                        obj.insert("payload".into(), json_payload);
                        // Mark `content-type: application/json` only when the
                        // source mimetype was NOT already JSON — upstream sets it
                        // as a transcode signal and skips it for a JSON-bodied
                        // event (core.py:782-787). Array/table bodies are never
                        // JSON, so this only spares the JSON-bodied ragged path.
                        let already_json = obj
                            .get("mimetype")
                            .and_then(Value::as_str)
                            .map(|mt| {
                                mt.split(';').next().unwrap_or(mt).trim() == "application/json"
                            })
                            .unwrap_or(false);
                        if !already_json {
                            obj.insert("content-type".into(), Value::from("application/json"));
                        }
                    }
                    serde_json::to_string(&m)
                }
                None => serde_json::to_string(metadata),
            };
            match text {
                Ok(s) => Some(Message::Text(s.into())),
                Err(e) => {
                    tracing::warn!(target: "tiled.streaming", "json encode: {e}");
                    None
                }
            }
        }
        EnvelopeFormat::Msgpack => {
            let packed = match (payload, metadata.as_object()) {
                // Embed the payload as a msgpack bin alongside the metadata map.
                (Some(bytes), Some(map)) => rmp_serde::to_vec_named(&MsgpackEnvelope {
                    map,
                    payload: bytes,
                }),
                _ => rmp_serde::to_vec_named(metadata),
            };
            match packed {
                Ok(v) => Some(Message::Binary(v.into())),
                Err(e) => {
                    tracing::warn!(target: "tiled.streaming", "msgpack encode: {e}");
                    None
                }
            }
        }
    }
}

/// A metadata map plus a binary payload, serialized as a single msgpack map that
/// preserves the metadata field order and encodes `payload` as a msgpack **bin**
/// (via `serialize_bytes`), not an array of ints. Mirrors upstream
/// `msgpack.packb({**metadata, "payload": payload_bytes})`.
struct MsgpackEnvelope<'a> {
    map: &'a serde_json::Map<String, Value>,
    payload: &'a [u8],
}

impl Serialize for MsgpackEnvelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // +1 for the payload entry, unless the metadata already carries a
        // `payload` key (it never does — the constructors omit it — but guard so
        // we never emit a duplicate map key).
        let has_payload_key = self.map.contains_key("payload");
        let len = self.map.len() + usize::from(!has_payload_key);
        let mut map = serializer.serialize_map(Some(len))?;
        for (k, v) in self.map {
            if k == "payload" {
                continue;
            }
            map.serialize_entry(k, v)?;
        }
        map.serialize_entry("payload", serde_bytes::Bytes::new(self.payload))?;
        map.end()
    }
}

/// Transcode a data event's raw `payload` into a JSON-native value, dispatched
/// by the event `type` (upstream `stream_json`, core.py:772-816): `array-data` →
/// nested lists, `table-data` → column-name→values map, `ragged-data` → nested
/// list. Returns `None` when the event carries no transcodable payload for this
/// build (e.g. a sparse Arrow body, or a non-Arrow/-JSON wire form) — the caller
/// then omits `payload` (msgpack still ships the raw bin).
fn transcode_payload_to_json(metadata: &Value, payload: &Bytes, ctx: &PayloadCtx) -> Option<Value> {
    match metadata.get("type").and_then(Value::as_str) {
        Some("array-data") => transcode_array_payload_to_json(metadata, payload, ctx),
        Some("table-data") => transcode_table_payload_to_json(metadata, payload, ctx),
        Some("ragged-data") => transcode_ragged_payload_to_json(metadata, payload, ctx),
        _ => None,
    }
}

/// Transcode a raw C-order array payload into nested JSON lists, REUSING the
/// read-path array→JSON serializer from the registry so the output is identical
/// to `GET /array/full` with `Accept: application/json`. Only the octet-stream
/// (dense) wire form is transcodable here; a sparse (Arrow) `array-data` payload
/// has no array-family JSON serializer, so this returns `None` and the payload
/// is omitted from the json envelope (msgpack still ships the raw bin).
fn transcode_array_payload_to_json(
    metadata: &Value,
    payload: &Bytes,
    ctx: &PayloadCtx,
) -> Option<Value> {
    // Dense arrays travel as a raw C-order buffer; anything else (sparse Arrow)
    // is not decodable by the array serializer.
    if metadata.get("mimetype").and_then(Value::as_str)
        != Some(crate::core::media_type::mime::OCTET_STREAM)
    {
        return None;
    }
    let dtype = ctx.array_dtype.as_ref()?;
    let shape = metadata.get("shape").cloned().unwrap_or(Value::Null);
    // Build the serializer metadata exactly as the read path does
    // (`build_array_response`), so the nested-list encoding matches byte-for-byte.
    let ser_meta = serde_json::json!({
        "itemsize": dtype.element_size(),
        "kind": String::from(dtype.kind.to_numpy_char()),
        "byteorder": String::from(dtype.endianness.to_numpy_char()),
        "dt_units": dtype.dt_units,
        "shape": shape,
    });
    let serializer = ctx
        .registry
        .dispatch(StructureFamily::Array, crate::core::media_type::mime::JSON)?;
    let json_bytes = match serializer(payload, &ser_meta) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(target: "tiled.streaming", "array-data json transcode: {e}");
            return None;
        }
    };
    serde_json::from_slice(&json_bytes).ok()
}

/// Transcode a table partition's Arrow IPC payload into a column-name → values
/// map (upstream `stream_json` table arm, core.py:789-794:
/// `{col: df[col].tolist() for col in df}`), REUSING the read-path table→JSON
/// serializer so the output is identical to `GET /table/partition` with
/// `Accept: application/json`. Table writes always travel as Arrow IPC; a
/// non-Arrow body has no table JSON serializer, so this returns `None`.
fn transcode_table_payload_to_json(
    metadata: &Value,
    payload: &Bytes,
    ctx: &PayloadCtx,
) -> Option<Value> {
    if metadata.get("mimetype").and_then(Value::as_str)
        != Some(crate::core::media_type::mime::ARROW_FILE)
    {
        return None;
    }
    // The table JSON serializer decodes the Arrow IPC body itself and ignores
    // its metadata argument, so no per-subscription context is needed.
    let serializer = ctx
        .registry
        .dispatch(StructureFamily::Table, crate::core::media_type::mime::JSON)?;
    let json_bytes = match serializer(payload, &Value::Null) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(target: "tiled.streaming", "table-data json transcode: {e}");
            return None;
        }
    };
    serde_json::from_slice(&json_bytes).ok()
}

/// Transcode a ragged write's payload into a nested JSON list (upstream
/// `stream_json` ragged arm, core.py:795-801: `deserializer(body, structure)
/// .tolist()`). The wire body is the raw request encoding — a JSON list-of-lists
/// (`application/json`) or zipped Awkward buffers (`application/zip`); decode it
/// to the canonical JSON list, then route through the read-path ragged JSON
/// serializer (a pass-through) so the output matches `GET /ragged/full`. Any
/// other wire form returns `None`.
fn transcode_ragged_payload_to_json(
    metadata: &Value,
    payload: &Bytes,
    ctx: &PayloadCtx,
) -> Option<Value> {
    let media = metadata
        .get("mimetype")
        .and_then(Value::as_str)
        .unwrap_or("");
    // Strip Content-Type parameters (`application/json; charset=utf-8`), as the
    // write path's `deserialize_ragged_body` does.
    let media = media.split(';').next().unwrap_or(media).trim();
    let json_bytes: Bytes = match media {
        "" | crate::core::media_type::mime::JSON => payload.clone(),
        crate::core::media_type::mime::ZIP => {
            match crate::serialization::ragged::from_zipped_buffers(payload) {
                Ok(list) => serde_json::to_vec(&list).ok()?.into(),
                Err(e) => {
                    tracing::warn!(target: "tiled.streaming", "ragged-data zip decode: {e}");
                    return None;
                }
            }
        }
        _ => return None,
    };
    let serializer = ctx
        .registry
        .dispatch(StructureFamily::Ragged, crate::core::media_type::mime::JSON)?;
    let out = match serializer(&json_bytes, &Value::Null) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(target: "tiled.streaming", "ragged-data json transcode: {e}");
            return None;
        }
    };
    serde_json::from_slice(&out).ok()
}

/// Per-message delivery authorization.
///
/// Authorize the node a delivered message concerns against the subscriber's
/// *base* auth context — re-narrowing from the principal each call, exactly as
/// the HTTP read surface does in `resolve_entry`. Returns `false` (skip) when
/// the node is denied or no longer resolves.
///
/// With no access policy there is nothing to narrow: a single `is_none` check
/// short-circuits to `true`, so delivery cost is unchanged.
async fn delivery_allowed(state: &AppState, auth_ctx: &AuthContext, segments: &[String]) -> bool {
    if state.access_policy.is_none() {
        return true;
    }
    crate::server::router::resolve_entry(
        state,
        auth_ctx.clone(),
        segments,
        crate::auth::Scope::ReadMetadata,
    )
    .await
    .is_ok()
}

/// The node an event concerns, used as the authorization target. A
/// `container-child-created` / `container-child-metadata-updated` event is
/// published on the *parent* node but names a child (`key`), so the authorized
/// node is that child (`subscribed_path + key`) — otherwise a subscriber
/// permitted on the parent but not the child would learn the restricted child's
/// existence. Every other event concerns the subscribed node itself.
fn event_target_segments(subscribed: &[String], metadata: &Value) -> Vec<String> {
    let mut segments = subscribed.to_vec();
    let is_child_event = matches!(
        metadata.get("type").and_then(Value::as_str),
        Some("container-child-created") | Some("container-child-metadata-updated")
    );
    if let Some(key) = metadata
        .get("key")
        .and_then(Value::as_str)
        .filter(|_| is_child_event)
    {
        segments.push(key.to_string());
    }
    segments
}

async fn handshake_auth(
    state: &AppState,
    tx: &mut futures::stream::SplitSink<WebSocket, Message>,
    rx: &mut futures::stream::SplitStream<WebSocket>,
) -> Result<AuthContext, String> {
    use tokio::time::Duration;

    // Anonymous mode: no auth backend at all → grant full scopes
    // immediately, matching the HTTP middleware policy. Skip the
    // handshake to keep latency down for unprotected demos.
    if state.no_auth_configured() {
        return Ok(AuthContext {
            principal: None,
            scopes: crate::auth::ScopeSet::full(),
            kind: AuthKind::Anonymous,
            authn_access_tags: None,
        });
    }
    // Otherwise wait briefly for the client's first message — must be a
    // JSON object: {"type": "auth", "apikey"|"bearer": "..."}.
    let first =
        match tokio::time::timeout(Duration::from_secs(10), futures::StreamExt::next(rx)).await {
            Ok(Some(Ok(Message::Text(text)))) => text,
            Ok(Some(Ok(Message::Binary(bytes)))) => match std::str::from_utf8(&bytes) {
                Ok(s) => s.to_string().into(),
                Err(_) => return Err("auth handshake: non-utf8 binary".into()),
            },
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                return Err("auth handshake: client closed".into());
            }
            Ok(Some(Err(e))) => return Err(format!("auth handshake: {e}")),
            Ok(Some(Ok(_))) => return Err("auth handshake: unexpected frame type".into()),
            Err(_) => return Err("auth handshake: timeout".into()),
        };
    let parsed: serde_json::Value = match serde_json::from_str(&first) {
        Ok(v) => v,
        Err(e) => return Err(format!("auth handshake: invalid JSON: {e}")),
    };
    if parsed.get("type").and_then(|v| v.as_str()) != Some("auth") {
        return Err("auth handshake: first message must be {\"type\": \"auth\"}".into());
    }
    if let Some(token) = parsed.get("bearer").and_then(|v| v.as_str()) {
        return crate::server::app::validate_bearer(state, token)
            .await
            .map_err(|e| format!("bearer: {e}"));
    }
    if let Some(key) = parsed.get("apikey").and_then(|v| v.as_str()) {
        return crate::server::app::validate_apikey(state, key)
            .await
            .map_err(|e| format!("apikey: {e}"));
    }
    let _ = tx;
    Err("auth handshake: provide 'bearer' or 'apikey'".into())
}
