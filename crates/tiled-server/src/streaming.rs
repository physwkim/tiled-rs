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

use axum::extract::{Query, State, WebSocketUpgrade, ws::Message, ws::WebSocket};
use axum::extract::OriginalUri;
use axum::response::IntoResponse;
use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::auth_context::AuthContext;
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
                    history: std::sync::Mutex::new(
                        std::collections::VecDeque::with_capacity(HISTORY_CAPACITY),
                    ),
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
    /// Metadata + specs were patched on this node.
    MetadataUpdated {
        metadata: serde_json::Value,
    },
    /// Node (and descendants) were deleted.
    NodeDeleted,
    /// A new partition / block of data is available.
    DataAppended {
        partition: Option<usize>,
    },
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
    auth: AuthContext,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, ServerError> {
    auth.require(tiled_auth::Scope::ReadMetadata)?;
    let prefix_starts = ["/api/v1/array/subscribe/", "/api/v1/container/subscribe/", "/api/v1/table/subscribe/"];
    let path = uri.path();
    let segments: Vec<String> = prefix_starts
        .iter()
        .find_map(|prefix| {
            path.find(prefix).map(|idx| {
                let after = &path[idx + prefix.len()..];
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
    Ok(ws.on_upgrade(move |socket| {
        run_subscription(socket, bus, path_str, schema, replay)
    }))
}

async fn build_schema_payload(
    state: &AppState,
    segments: &[String],
) -> serde_json::Value {
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
    // Fall back to walking the in-memory tree on the blocking pool — the
    // adapter trait is sync, so we can't do this on the async runtime.
    let segments = segments.to_vec();
    let state = state.clone();
    tokio::task::spawn_blocking(move || -> serde_json::Value {
        if segments.is_empty() {
            return serde_json::json!({
                "structure_family": "container",
                "path": "",
            });
        }
        match crate::core::walk_tree(state.root_tree.as_ref(), &segments) {
            Ok(adapter) => serde_json::json!({
                "structure_family": adapter.structure_family().to_string(),
                "specs": adapter.specs(),
                "path": segments.join("/"),
                "structure": adapter.structure_json(),
            }),
            Err(_) => serde_json::json!({
                "type": "subscription-error",
                "message": "node not found",
                "path": segments.join("/"),
            }),
        }
    })
    .await
    .unwrap_or_else(|_| serde_json::json!({"type": "subscription-error", "message": "blocking task failed"}))
}

async fn run_subscription(
    socket: WebSocket,
    bus: StreamingBus,
    path: String,
    schema: serde_json::Value,
    replay: Vec<UpdateEnvelope>,
) {
    let (mut tx, mut rx) = futures::StreamExt::split(socket);
    use futures::SinkExt;

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
