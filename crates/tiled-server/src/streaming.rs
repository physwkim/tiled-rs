//! Server-side WebSocket subscriptions.
//!
//! `StreamingBus` is the in-process pub/sub mediator. Write handlers call
//! `publish(path, update)` after a successful catalog write, and connected
//! WS clients listening at the same path (or a prefix) receive the
//! update. Out-of-process pub/sub (Redis, NATS, …) would extend this by
//! mirroring publishes onto an external channel; the trait shape lets a
//! deployment swap in a different bus without touching the handlers.
//!
//! Wire format mirrors `tiled.streaming.protocol`:
//! - First message after connect is a JSON `{"type": "schema", ...}` with
//!   the node's structure.
//! - Subsequent messages are `{"type": "...", "sequence": n, ...}`.
//!
//! Encoded as MsgPack (matching the tiled-client `Update::parse` path).

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::extract::OriginalUri;
use axum::extract::{Query, State, WebSocketUpgrade, ws::Message, ws::WebSocket};
use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::auth_context::{AuthContext, AuthKind};
use crate::error::ServerError;
use crate::state::AppState;

/// Hand-rolled DashMap keyed by node path → broadcast channel. A
/// `subscribe` request at path P joins the channel for P; a `publish`
/// targeting any P' that is P or a descendant of P delivers to that
/// subscriber.
#[derive(Clone, Default)]
pub struct StreamingBus {
    inner: Arc<BusInner>,
}

struct BusInner {
    /// path → (broadcast::Sender, sequence counter).
    channels: DashMap<String, ChannelEntry>,
}

impl Default for BusInner {
    fn default() -> Self {
        Self {
            channels: DashMap::new(),
        }
    }
}

struct ChannelEntry {
    sender: broadcast::Sender<UpdateEnvelope>,
    sequence: AtomicU64,
    /// Bounded ring of recent updates so a reconnecting client can
    /// resume from `?start=<seq>` (tiled#1218). The broadcast channel
    /// itself doesn't keep history; we mirror the last `HISTORY_CAPACITY`
    /// envelopes here under a Mutex.
    history: std::sync::Mutex<std::collections::VecDeque<UpdateEnvelope>>,
}

const CHANNEL_CAPACITY: usize = 256;
const HISTORY_CAPACITY: usize = 256;

impl StreamingBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BusInner::default()),
        }
    }

    /// Subscribe to updates whose published path equals `path` or names
    /// a descendant of `path`. The receiver is a single broadcast
    /// channel; `publish` is responsible for fanning into ancestor
    /// channels so a watcher at `expt` sees events at `expt/scan_1/x`.
    pub fn subscribe(&self, path: &str) -> broadcast::Receiver<UpdateEnvelope> {
        let entry = self
            .inner
            .channels
            .entry(path.to_string())
            .or_insert_with(|| ChannelEntry {
                sender: broadcast::channel(CHANNEL_CAPACITY).0,
                sequence: AtomicU64::new(0),
                history: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                    HISTORY_CAPACITY,
                )),
            });
        entry.sender.subscribe()
    }

    /// Return every envelope in this path's history with sequence > `start`,
    /// oldest first. Used to replay missed updates when a client reconnects
    /// with `?start=<n>` (tiled#1218). If the requested sequence is older
    /// than the buffer's oldest entry the caller must accept some loss —
    /// they receive only what's still in the ring.
    pub fn history_since(&self, path: &str, start: u64) -> Vec<UpdateEnvelope> {
        match self.inner.channels.get(path) {
            Some(entry) => entry
                .history
                .lock()
                .map(|hist| {
                    hist.iter()
                        .filter(|e| e.sequence > start)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    /// Publish an update for `path`. Delivers to the channel for `path`
    /// AND every ancestor channel, so a watcher one level up still hears
    /// about descendant changes. Sequence numbers come from the
    /// publishing path's own counter.
    pub fn publish(&self, path: &str, kind: UpdateKind) {
        let env = {
            let entry = self
                .inner
                .channels
                .entry(path.to_string())
                .or_insert_with(|| ChannelEntry {
                    sender: broadcast::channel(CHANNEL_CAPACITY).0,
                    sequence: AtomicU64::new(0),
                    history: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                        HISTORY_CAPACITY,
                    )),
                });
            let seq = entry.sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let env = UpdateEnvelope {
                sequence: seq,
                timestamp: Utc::now().to_rfc3339(),
                path: path.to_string(),
                kind,
            };
            // Append to the bounded replay buffer before broadcasting so
            // a fast reconnect after a network blip sees the just-sent
            // event in `history_since`.
            if let Ok(mut hist) = entry.history.lock() {
                if hist.len() == HISTORY_CAPACITY {
                    hist.pop_front();
                }
                hist.push_back(env.clone());
            }
            let _ = entry.sender.send(env.clone());
            env
        };

        for prefix in path_prefixes(path) {
            if prefix == path {
                continue;
            }
            if let Some(parent) = self.inner.channels.get(&prefix) {
                let _ = parent.sender.send(env.clone());
            }
        }

        // Garbage-collect dead channels — entries whose Sender has no
        // active receivers anymore. Without this every distinct path
        // ever subscribed to leaks a channel for the lifetime of the
        // process. `remove_if` is atomic so a concurrent subscribe at
        // the same key stays consistent.
        self.gc(path);
        for prefix in path_prefixes(path) {
            if prefix != path {
                self.gc(&prefix);
            }
        }
    }

    fn gc(&self, path: &str) {
        self.inner
            .channels
            .remove_if(path, |_, entry| entry.sender.receiver_count() == 0);
    }

    /// Number of channels currently held in the bus — useful for tests
    /// that want to confirm the GC ran.
    pub fn channel_count(&self) -> usize {
        self.inner.channels.len()
    }
}

fn path_prefixes(path: &str) -> Vec<String> {
    let mut out = vec![String::new()];
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return out;
    }
    let mut cumulative = String::new();
    for seg in trimmed.split('/') {
        if !cumulative.is_empty() {
            cumulative.push('/');
        }
        cumulative.push_str(seg);
        out.push(cumulative.clone());
    }
    out
}

/// JSON-serialisable update wrapper sent over the WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEnvelope {
    pub sequence: u64,
    pub timestamp: String,
    pub path: String,
    pub kind: UpdateKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum UpdateKind {
    /// A new child was created under this path.
    ChildCreated {
        key: String,
        structure_family: String,
    },
    /// Metadata + specs were patched on this node. Mirrors upstream
    /// tiled PR #1176: `specs` is published alongside `metadata` so a
    /// subscriber can re-render its spec view without an extra
    /// metadata fetch round-trip.
    MetadataUpdated {
        metadata: serde_json::Value,
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        specs: serde_json::Value,
    },
    /// Node (and descendants) were deleted.
    NodeDeleted,
    /// A new partition / block of data is available.
    DataAppended { partition: Option<usize> },
}

#[derive(Debug, Deserialize)]
pub struct SubscribeQuery {
    /// Sequence number from which to resume. Replays anything still in
    /// the bus's bounded history with `sequence > start`. Older entries
    /// have been evicted; clients that supply a `start` older than the
    /// ring's oldest entry must be prepared to detect the gap (their
    /// next live `sequence` will be > `start + history.len()`).
    #[serde(default)]
    pub start: Option<u64>,
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
    let header_auth = crate::app::resolve_header_auth(&state, &headers).await;

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
    let path_str = segments.join("/");

    // Build the schema payload now while we still have the AppState.
    // For catalog-backed deployments we hit the DB; for in-memory trees
    // we walk the existing tree. Either way we end up with a JSON blob
    // describing the node's structure at subscription time.
    let schema = build_schema_payload(&state, &segments).await;

    let bus = state.streaming_bus.clone();
    let replay = match q.start {
        Some(seq) => bus.history_since(&path_str, seq),
        None => Vec::new(),
    };
    let state_for_handshake = state.clone();
    let segments_for_ws = segments.to_vec();
    Ok(ws.on_upgrade(move |socket| {
        run_subscription(
            socket,
            state_for_handshake,
            bus,
            segments_for_ws,
            schema,
            replay,
            header_auth,
        )
    }))
}

async fn build_schema_payload(state: &AppState, segments: &[String]) -> serde_json::Value {
    if let Some(ref catalog) = state.catalog {
        if segments.is_empty() {
            return serde_json::json!({
                "structure_family": "container",
                "path": "",
            });
        }
        if let Ok(Some(node)) = catalog.lookup(segments).await {
            return serde_json::json!({
                "structure_family": node.structure_family,
                "specs": node.specs,
                "path": segments.join("/"),
            });
        }
    }
    // Fall back to walking the in-memory tree. The async walk resolves each
    // hop on the executor and any blocking backend offloads internally.
    if segments.is_empty() {
        return serde_json::json!({
            "structure_family": "container",
            "path": "",
        });
    }
    match crate::core::walk_tree(state.root_tree.as_ref(), segments).await {
        Ok(adapter) => {
            let structure = adapter.structure_json().await.ok().flatten();
            serde_json::json!({
                "structure_family": adapter.structure_family().to_string(),
                "specs": adapter.specs(),
                "path": segments.join("/"),
                "structure": structure,
            })
        }
        Err(_) => serde_json::json!({
            "type": "subscription-error",
            "message": "node not found",
            "path": segments.join("/"),
        }),
    }
}

async fn run_subscription(
    socket: WebSocket,
    state: AppState,
    bus: StreamingBus,
    segments: Vec<String>,
    schema: serde_json::Value,
    replay: Vec<UpdateEnvelope>,
    header_auth: Option<AuthContext>,
) {
    let path = segments.join("/");
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
    // are still allowed (they simply never receive anything).
    if !auth_ctx.scopes.contains(tiled_auth::Scope::ReadMetadata) {
        let _ = tx
            .send(Message::Text("forbidden: missing read:metadata".into()))
            .await;
        let _ = tx.send(Message::Close(None)).await;
        return;
    }

    // F4: authorize the initial schema by the subscription node itself —
    // the same per-message delivery rule applied to every fanned event in
    // the loop below. `publish` fans events up to every ancestor channel
    // (including the root ""), so authorizing only the subscription point
    // leaks descendant and whole-tree metadata. Root and non-root resolve
    // uniformly here: there is no `is_empty` special case — an empty path
    // resolves to the base scope check inside `resolve_entry`. With no
    // access policy `delivery_allowed` is a single `is_none` check, so
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

    // Initial schema message — full structure of the node so the
    // client can interpret subsequent updates without an extra GET.
    // Mirrors tiled's "subscription-ready" wire shape: type + path +
    // structure_family + structure (when known) + timestamp.
    let initial = serde_json::json!({
        "type": "subscription-ready",
        "path": path,
        "timestamp": Utc::now().to_rfc3339(),
        "schema": schema,
        "resumed": !replay.is_empty(),
    });
    let bytes = match rmp_serde::to_vec_named(&initial) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "tiled.streaming", "encode initial: {e}");
            return;
        }
    };
    if tx.send(Message::Binary(bytes.into())).await.is_err() {
        return;
    }

    // Replay any history snapshotted before subscribe so the client
    // resuming from `?start=N` sees the missed updates in order. The
    // live channel may also still hold a buffered copy of these events;
    // sequence numbers are monotonic so the client dedupes by `sequence`.
    for env in replay {
        // Replayed history is fanned the same way as live events, so it
        // carries descendant updates too — authorize each one (F4).
        if !delivery_allowed(&state, &auth_ctx, &event_target_segments(&env)).await {
            continue;
        }
        let payload = match rmp_serde::to_vec_named(&env) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(target: "tiled.streaming", "encode replay: {e}");
                continue;
            }
        };
        if tx.send(Message::Binary(payload.into())).await.is_err() {
            return;
        }
    }

    let mut receiver = bus.subscribe(&path);

    loop {
        tokio::select! {
            update = receiver.recv() => {
                match update {
                    Ok(env) => {
                        // F4: every event was fanned up from its own source
                        // node, which may be a descendant the subscriber is
                        // not authorized for. Authorize delivery per event.
                        if !delivery_allowed(&state, &auth_ctx, &event_target_segments(&env)).await {
                            continue;
                        }
                        let payload = match rmp_serde::to_vec_named(&env) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(target: "tiled.streaming", "encode update: {e}");
                                continue;
                            }
                        };
                        if tx.send(Message::Binary(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = futures::StreamExt::next(&mut rx) => {
                match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}

/// F4 per-message delivery authorization.
///
/// `publish` fans every event up to all ancestor channels (seeded with
/// the root `""`), so a subscriber's channel receives events sourced from
/// arbitrary descendants. Authorize the node a delivered message concerns
/// against the subscriber's *base* auth context — re-narrowing from the
/// principal each call, exactly as the HTTP read surface does in
/// `resolve_entry`. Returns `false` (skip) when the node is denied or no
/// longer resolves.
///
/// With no access policy there is nothing to narrow: a single `is_none`
/// check short-circuits to `true`, so delivery cost is unchanged.
async fn delivery_allowed(state: &AppState, auth_ctx: &AuthContext, segments: &[String]) -> bool {
    if state.access_policy.is_none() {
        return true;
    }
    crate::router::resolve_entry(
        state,
        auth_ctx.clone(),
        segments,
        tiled_auth::Scope::ReadMetadata,
    )
    .await
    .is_ok()
}

/// The node a fanned event actually concerns, used as the authorization
/// target. For most kinds this is the published source path. A
/// `ChildCreated` event is published on the *parent* path but reveals a
/// new child (its key and structure family), so the authorized node is
/// that child (`path + key`) — otherwise a subscriber permitted on the
/// parent but not the child would learn the restricted child's existence.
fn event_target_segments(env: &UpdateEnvelope) -> Vec<String> {
    let mut segments: Vec<String> = env
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    if let UpdateKind::ChildCreated { key, .. } = &env.kind {
        segments.push(key.clone());
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
            scopes: tiled_auth::ScopeSet::full(),
            kind: AuthKind::Anonymous,
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
        return crate::app::validate_bearer(state, token)
            .await
            .map_err(|e| format!("bearer: {e}"));
    }
    if let Some(key) = parsed.get("apikey").and_then(|v| v.as_str()) {
        return crate::app::validate_apikey(state, key)
            .await
            .map_err(|e| format!("apikey: {e}"));
    }
    let _ = tx;
    Err("auth handshake: provide 'bearer' or 'apikey'".into())
}
