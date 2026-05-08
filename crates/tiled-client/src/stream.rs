//! WebSocket-based streaming subscriptions.
//!
//! Mirrors `tiled/client/stream.py` (`Subscription`, `ContainerSubscription`,
//! `ArraySubscription`, `TableSubscription`) plus the message shapes from
//! `tiled/stream_messages.py`.
//!
//! ## Connection
//!
//! `/api/v1/stream/single<path>?envelope_format=msgpack` — switch scheme to
//! `ws`/`wss` based on the api_uri.
//!
//! ## Wire format
//!
//! 1. First message after handshake: a `Schema` (msgpack-encoded).
//! 2. Subsequent messages: `Update`s, also msgpack-encoded. Each carries a
//!    `sequence: u64` so the client can resume after disconnect.
//!
//! ## Rust API
//!
//! Async-first. Instead of the blocking thread + callbacks layout, the Rust
//! API exposes the connection as an async `Stream<Item = Result<Update>>`.
//! Callers handle dispatch with `match update { ... }`.

use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use bytes::Bytes;
use futures::stream::Stream;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use url::Url;

use tiled_core::dtype::DType;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

use crate::context::Context;
use crate::error::{ClientError, Result};

// ---------------------------------------------------------------------------
// Schema messages (first frame on every connect/reconnect)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Schema {
    #[serde(rename = "array-schema")]
    Array {
        version: u32,
        data_type: DType,
    },
    #[serde(rename = "container-schema")]
    Container { version: u32 },
    #[serde(rename = "table-schema")]
    Table {
        version: u32,
        arrow_schema: String,
    },
}

// ---------------------------------------------------------------------------
// Update messages (everything after schema)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct ChildCreated {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub key: String,
    pub structure_family: StructureFamily,
    #[serde(default)]
    pub specs: Vec<Spec>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub data_sources: serde_json::Value,
    #[serde(default)]
    pub access_blob: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChildMetadataUpdated {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub key: String,
    #[serde(default)]
    pub specs: Vec<Spec>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArrayData {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub mimetype: String,
    pub shape: Vec<i64>,
    #[serde(default)]
    pub offset: Option<Vec<i64>>,
    #[serde(default)]
    pub block: Option<Vec<i64>>,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub data_type: DType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArrayPatch {
    pub offset: Vec<i64>,
    pub shape: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArrayRef {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub data_source: serde_json::Value,
    #[serde(default)]
    pub patch: Option<ArrayPatch>,
    #[serde(default)]
    pub uri: Option<String>,
    pub shape: Vec<i64>,
    pub data_type: DType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableData {
    pub sequence: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub mimetype: String,
    #[serde(default)]
    pub partition: Option<i64>,
    pub append: bool,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub arrow_schema: String,
}

/// Single-decode tagged enum. The wire `type` field is consumed by serde and
/// dispatches to the matching variant payload — no double-deserialise.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Update {
    #[serde(rename = "container-child-created")]
    ChildCreated(ChildCreated),
    #[serde(rename = "container-child-metadata-updated")]
    ChildMetadataUpdated(ChildMetadataUpdated),
    #[serde(rename = "array-data")]
    ArrayData(ArrayData),
    #[serde(rename = "array-ref")]
    ArrayRef(ArrayRef),
    #[serde(rename = "table-data")]
    TableData(TableData),
}

impl Update {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::ChildCreated(u) => u.sequence,
            Self::ChildMetadataUpdated(u) => u.sequence,
            Self::ArrayData(u) => u.sequence,
            Self::ArrayRef(u) => u.sequence,
            Self::TableData(u) => u.sequence,
        }
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(ClientError::from)
    }
}

// ---------------------------------------------------------------------------
// Decoders for ArrayData payload — keep DType handy without numpy.
// ---------------------------------------------------------------------------

impl ArrayData {
    /// Convert payload to a 1-D `Vec<f64>` if the dtype is `f8 little-endian`.
    /// Only useful for the simplest ophyd-style 1-D streams. Other dtypes:
    /// caller decodes from `payload` + `data_type` themselves.
    pub fn as_f64_vec(&self) -> Result<Vec<f64>> {
        let need = match &self.data_type {
            DType::Builtin(b) => b.itemsize == 8,
            _ => false,
        };
        if !need {
            return Err(ClientError::Invalid(format!(
                "as_f64_vec only supports 8-byte builtin dtype, got {:?}",
                self.data_type
            )));
        }
        let chunks = self.payload.chunks_exact(8);
        if !chunks.remainder().is_empty() {
            return Err(ClientError::Invalid(
                "payload length not a multiple of 8 bytes".into(),
            ));
        }
        Ok(chunks
            .map(|c| {
                let arr: [u8; 8] = c.try_into().expect("chunks_exact");
                f64::from_le_bytes(arr)
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// Sentinel for "no sequence seen yet". Real sequences are 1-indexed
/// (see `tiled/stream_messages.py`) so `u64::MAX` is unambiguous.
const NO_SEQ: u64 = u64::MAX;

/// Subscription handle. `connect()` opens the WebSocket and returns a
/// `SubscriptionStream` that yields `Update`s as they arrive.
#[derive(Debug, Clone)]
pub struct Subscription {
    context: Context,
    segments: Vec<String>,
    last_sequence: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Subscription {
    pub fn new(context: Context, segments: Vec<String>) -> Self {
        Self {
            context,
            segments,
            last_sequence: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(NO_SEQ)),
        }
    }

    /// Compute the WebSocket URI for `/api/v1/stream/single<path>`.
    fn ws_uri(&self) -> Result<Url> {
        let api = self.context.api_uri();
        let mut url = api.clone();
        // Switch http→ws / https→wss.
        let new_scheme = match api.scheme() {
            "https" => "wss",
            _ => "ws",
        };
        url.set_scheme(new_scheme)
            .map_err(|_| ClientError::Invalid("cannot switch URL scheme".into()))?;
        let path = format!(
            "{}stream/single/{}",
            api.path(),
            self.segments.join("/").trim_start_matches('/')
        );
        url.set_path(&path);
        url.query_pairs_mut()
            .append_pair("envelope_format", "msgpack");
        Ok(url)
    }

    /// Connect to the stream. `start` matches Python: `None` = newest, `0` =
    /// from earliest, `n > 0` = from sequence `n`.
    ///
    /// If the server rejects the WS handshake with HTTP 401 *and* this
    /// `Context` has an OIDC `TiledAuth`, we refresh the token once and
    /// retry. Other failures bubble up untouched.
    pub async fn connect(&self, start: Option<i64>) -> Result<SubscriptionStream> {
        match self.connect_once(start).await {
            Err(ClientError::AuthRequired(_)) => {
                if let Some(auth) = self.context.auth().await {
                    auth.refresh(self.context.http()).await?;
                    return self.connect_once(start).await;
                }
                Err(ClientError::AuthRequired(
                    "ws handshake rejected with 401 and no OIDC auth configured".into(),
                ))
            }
            other => other,
        }
    }

    async fn connect_once(&self, start: Option<i64>) -> Result<SubscriptionStream> {
        let mut url = self.ws_uri()?;
        let last = self.last_sequence.load(std::sync::atomic::Ordering::Relaxed);
        let effective_start: Option<i64> = if last != NO_SEQ {
            Some((last as i64).saturating_add(1))
        } else {
            start
        };
        if let Some(s) = effective_start {
            url.query_pairs_mut().append_pair("start", &s.to_string());
        }

        let mut req = url.as_str().into_client_request().map_err(|e| {
            ClientError::Invalid(format!("ws request build: {e}"))
        })?;

        if let Some(key) = self.context.api_key().await {
            let value = HeaderValue::from_str(&format!("Apikey {key}"))
                .map_err(|e| ClientError::Invalid(format!("ws auth: {e}")))?;
            req.headers_mut()
                .insert(HeaderName::from_static("authorization"), value);
        } else if let Some(auth) = self.context.auth().await {
            if let Some(h) = auth.auth_header().await {
                let value = HeaderValue::from_str(&h)
                    .map_err(|e| ClientError::Invalid(format!("ws bearer: {e}")))?;
                req.headers_mut()
                    .insert(HeaderName::from_static("authorization"), value);
            }
        }

        let cfg = WebSocketConfig::default();
        let result =
            tokio_tungstenite::connect_async_with_config(req, Some(cfg), false).await;
        let (ws, _resp) = match result {
            Ok(pair) => pair,
            Err(tokio_tungstenite::tungstenite::Error::Http(resp))
                if resp.status() == 401 =>
            {
                return Err(ClientError::AuthRequired(
                    "ws handshake 401".into(),
                ));
            }
            Err(e) => {
                return Err(ClientError::Invalid(format!("ws connect: {e}")));
            }
        };

        Ok(SubscriptionStream {
            ws: Box::pin(ws),
            schema: None,
            last_sequence: self.last_sequence.clone(),
        })
    }

    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    pub fn context(&self) -> &Context {
        &self.context
    }
}

/// Stream of updates. Yields `Update`s; the schema is captured implicitly
/// from the first message and exposed via [`SubscriptionStream::schema`].
pub struct SubscriptionStream {
    ws: Pin<
        Box<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    >,
    schema: Option<Schema>,
    last_sequence: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl SubscriptionStream {
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub async fn close(mut self) -> Result<()> {
        use futures::SinkExt;
        let _ = self
            .ws
            .send(Message::Close(None))
            .await
            .map_err(|e| ClientError::Invalid(format!("ws close: {e}")));
        Ok(())
    }
}

impl Stream for SubscriptionStream {
    type Item = Result<Update>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let item = match self.ws.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(msg))) => msg,
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(ClientError::Invalid(format!(
                        "ws read: {e}"
                    )))));
                }
            };
            let bytes = match item {
                Message::Binary(b) => Bytes::from(b.to_vec()),
                Message::Text(s) => Bytes::from(s.to_string().into_bytes()),
                Message::Close(_) => return Poll::Ready(None),
                // Pings/Pongs handled by tungstenite automatically when
                // possible; otherwise loop.
                _ => continue,
            };

            if self.schema.is_none() {
                match rmp_serde::from_slice::<Schema>(&bytes) {
                    Ok(s) => {
                        self.schema = Some(s);
                        continue;
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(ClientError::Invalid(format!(
                            "schema decode: {e}"
                        )))));
                    }
                }
            }

            match Update::parse(&bytes) {
                Ok(update) => {
                    let seq = update.sequence();
                    self.last_sequence
                        .store(seq, std::sync::atomic::Ordering::Relaxed);
                    return Poll::Ready(Some(Ok(update)));
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

// Silence dead-code on ArrayStructure import (used only for stream-message
// docs at the moment; kept for API symmetry with Python).
#[allow(dead_code)]
const _: fn() = || {
    let _ = std::mem::size_of::<ArrayStructure>();
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_decode_array() {
        // Build a tiny msgpack payload.
        let v = serde_json::json!({
            "type": "array-schema",
            "version": 1,
            "data_type": {
                "endianness": "little",
                "kind": "f",
                "itemsize": 8,
            }
        });
        let bytes = rmp_serde::to_vec_named(&v).unwrap();
        let s: Schema = rmp_serde::from_slice(&bytes).unwrap();
        match s {
            Schema::Array { version, .. } => assert_eq!(version, 1),
            _ => panic!("expected Array schema"),
        }
    }

    #[test]
    fn schema_decode_container() {
        let v = serde_json::json!({
            "type": "container-schema",
            "version": 1,
        });
        let bytes = rmp_serde::to_vec_named(&v).unwrap();
        let s: Schema = rmp_serde::from_slice(&bytes).unwrap();
        assert!(matches!(s, Schema::Container { .. }));
    }

    #[test]
    fn parse_child_created_update() {
        let v = serde_json::json!({
            "type": "container-child-created",
            "sequence": 1,
            "timestamp": "2026-05-09T00:00:00Z",
            "key": "foo",
            "structure_family": "array",
            "specs": [],
            "metadata": {},
            "data_sources": [],
            "access_blob": {}
        });
        let bytes = rmp_serde::to_vec_named(&v).unwrap();
        let u = Update::parse(&bytes).unwrap();
        assert_eq!(u.sequence(), 1);
        match u {
            Update::ChildCreated(c) => assert_eq!(c.key, "foo"),
            _ => panic!("expected ChildCreated"),
        }
    }
}
