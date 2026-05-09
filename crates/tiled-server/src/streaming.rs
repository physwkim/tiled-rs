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
}

const CHANNEL_CAPACITY: usize = 256;

impl StreamingBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BusInner::default()),
        }
    }

    /// Subscribe to updates published at exactly `path` or any of its
    /// ancestors. Returns the broadcast receiver. Sequence numbers are
    /// per-path so a subscriber can detect lost messages.
    pub fn subscribe(&self, path: &str) -> Vec<broadcast::Receiver<UpdateEnvelope>> {
        let mut receivers = Vec::new();
        for prefix in path_prefixes(path) {
            let entry = self
                .inner
                .channels
                .entry(prefix.clone())
                .or_insert_with(|| ChannelEntry {
                    sender: broadcast::channel(CHANNEL_CAPACITY).0,
                    sequence: AtomicU64::new(0),
                });
            receivers.push(entry.sender.subscribe());
        }
        receivers
    }

    /// Publish an update for `path`. The envelope's `sequence` is taken
    /// from the per-channel counter so subscribers can detect drops.
    pub fn publish(&self, path: &str, kind: UpdateKind) {
        let entry = self
            .inner
            .channels
            .entry(path.to_string())
            .or_insert_with(|| ChannelEntry {
                sender: broadcast::channel(CHANNEL_CAPACITY).0,
                sequence: AtomicU64::new(0),
            });
        let seq = entry.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let env = UpdateEnvelope {
            sequence: seq,
            timestamp: Utc::now().to_rfc3339(),
            path: path.to_string(),
            kind,
        };
        let _ = entry.sender.send(env);
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
    /// Optional sequence number from which to resume. Not yet honoured —
    /// we always start from "now".
    #[serde(default)]
    pub start: Option<u64>,
}

pub async fn ws_subscribe(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(_q): Query<SubscribeQuery>,
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

    let bus = state.streaming_bus.clone();
    Ok(ws.on_upgrade(move |socket| run_subscription(socket, bus, path_str)))
}

async fn run_subscription(
    socket: WebSocket,
    bus: StreamingBus,
    path: String,
) {
    let (mut tx, mut rx) = futures::StreamExt::split(socket);
    use futures::SinkExt;

    // Initial schema message — minimal placeholder for now.
    let initial = serde_json::json!({
        "type": "subscription-ready",
        "path": path,
        "timestamp": Utc::now().to_rfc3339(),
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

    let mut receivers = bus.subscribe(&path);
    // Merge multiple ancestor receivers into one stream of envelopes by
    // running a small fanout loop.
    let (merged_tx, mut merged_rx) =
        tokio::sync::mpsc::unbounded_channel::<UpdateEnvelope>();
    for mut r in receivers.drain(..) {
        let merged = merged_tx.clone();
        tokio::spawn(async move {
            while let Ok(env) = r.recv().await {
                if merged.send(env).is_err() {
                    break;
                }
            }
        });
    }
    drop(merged_tx);

    loop {
        tokio::select! {
            // Forward every update to the client.
            update = merged_rx.recv() => {
                let Some(env) = update else { break; };
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
            // Honor client-initiated close.
            incoming = futures::StreamExt::next(&mut rx) => {
                match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}
