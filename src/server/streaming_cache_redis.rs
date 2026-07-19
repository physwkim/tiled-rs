//! Redis-backed per-node data-streaming cache (Wave-24 PR8, upstream
//! `tiled.streaming.RedisStreamingDatastore`, `tiled/streaming.py:430-527`).
//!
//! This is the shared, multi-process sibling of
//! [`InMemoryStreamingCache`](super::streaming_cache::InMemoryStreamingCache):
//! several server processes point at one Redis instance so a write on any
//! process is replayable and live-notifiable from every process. It implements
//! the same [`StreamingCache`](super::streaming_cache::StreamingCache) trait and
//! uses the **exact** Redis key layout upstream uses, so a tiled-rs and an
//! upstream-Python process can interoperate against the same Redis:
//!
//! - `sequence:{node_id}` — the monotonic per-node counter (`INCR` / `GET`),
//!   with an idle `seq_ttl` expiry, extended to `1 + data_ttl` by `close` so the
//!   counter outlives the cached data (upstream streaming.py:488).
//! - `data:{node_id}:{seq}` — a hash holding `sequence`, `metadata` (a JSON
//!   string), and, when the event carries bytes, `payload` (raw). Expires after
//!   `data_ttl`.
//! - `notify:{node_id}` — a pub/sub channel; every stored sequence is
//!   `PUBLISH`ed here so live subscribers learn of it.
//!
//! Feature-gated behind `streaming-redis` (default OFF): the module and its
//! `redis` dependency compile only when that feature is enabled. The default
//! build ships the in-memory and disabled backends only.
//!
//! Error policy: the [`StreamingCache`] trait is deliberately `Result`-free —
//! every method here handles its own Redis errors internally (log at `warn` and
//! return the best-effort default: `0` for the counters, no-op for `set`,
//! `None` for `get`) rather than propagating. A transient Redis outage degrades
//! streaming, it does not fail the request path that produced the event.

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde_json::Value;
use tokio::sync::{OnceCell, broadcast};

use super::streaming_cache::{StreamEvent, StreamingCache};

/// Broadcast channel depth for a subscriber's live sequence notifications.
/// Matches the in-memory backend's capacity so a slow consumer lags or drops
/// the same way regardless of backend.
const CHANNEL_CAPACITY: usize = 256;

/// Redis-backed [`StreamingCache`].
///
/// Holds a [`redis::Client`] (cheap, connectionless) plus a lazily-established
/// [`ConnectionManager`] — a multiplexed, auto-reconnecting connection shared by
/// every command method. The manager is created on first use (the trait's
/// constructor is synchronous but its methods are async) and cached in the
/// [`OnceCell`]; a failure to connect leaves the cell empty so the next call
/// retries rather than caching a dead handle.
pub struct RedisStreamingCache {
    client: redis::Client,
    manager: OnceCell<ConnectionManager>,
    /// Idle expiry (seconds) refreshed on the `sequence:{node_id}` key each
    /// `incr_seq`. Upstream `seq_ttl`.
    seq_ttl: i64,
    /// Expiry (seconds) set on each `data:{node_id}:{seq}` hash. Upstream
    /// `data_ttl`.
    data_ttl: i64,
}

/// Clamp a `u64` seconds TTL to the `i64` Redis `EXPIRE` accepts, saturating at
/// `i64::MAX` rather than wrapping negative (a negative TTL would delete the key
/// immediately).
fn ttl_secs(secs: u64) -> i64 {
    i64::try_from(secs).unwrap_or(i64::MAX)
}

impl RedisStreamingCache {
    /// Build a cache against `uri` (e.g. `redis://host:6379/0`) with the given
    /// counter/data TTLs (seconds). Only parses/validates the URI — no
    /// connection is opened until the first cache operation. Fails only on an
    /// unparseable URI.
    pub fn new(uri: &str, seq_ttl: u64, data_ttl: u64) -> redis::RedisResult<Self> {
        let client = redis::Client::open(uri)?;
        Ok(Self {
            client,
            manager: OnceCell::new(),
            seq_ttl: ttl_secs(seq_ttl),
            data_ttl: ttl_secs(data_ttl),
        })
    }

    /// A clone of the shared connection manager, establishing it on first use.
    /// Returns `None` (after logging) when Redis is unreachable, so callers fall
    /// back to their best-effort default. `ConnectionManager` is `Clone` and
    /// internally reference-counted, so the clone is cheap and shares the one
    /// multiplexed socket.
    async fn conn(&self) -> Option<ConnectionManager> {
        match self
            .manager
            .get_or_try_init(|| self.client.get_connection_manager())
            .await
        {
            Ok(mgr) => Some(mgr.clone()),
            Err(e) => {
                tracing::warn!("streaming redis: connection failed: {e}");
                None
            }
        }
    }
}

#[async_trait]
impl StreamingCache for RedisStreamingCache {
    async fn incr_seq(&self, node_id: i64) -> u64 {
        let Some(mut conn) = self.conn().await else {
            return 0;
        };
        let key = format!("sequence:{node_id}");
        // `INCR` returns the new value; refresh the counter's idle TTL in the
        // same round trip. Atomic so a reader between the two never sees the key
        // without its refreshed expiry.
        let result: redis::RedisResult<(i64,)> = redis::pipe()
            .atomic()
            .incr(&key, 1_i64)
            .expire(&key, self.seq_ttl)
            .ignore()
            .query_async(&mut conn)
            .await;
        match result {
            Ok((seq,)) => u64::try_from(seq).unwrap_or(0),
            Err(e) => {
                tracing::warn!("streaming redis: incr_seq(node {node_id}) failed: {e}");
                0
            }
        }
    }

    async fn current_seq(&self, node_id: i64) -> u64 {
        let Some(mut conn) = self.conn().await else {
            return 0;
        };
        let key = format!("sequence:{node_id}");
        // Absent/expired key → `nil` → `None` → 0, matching upstream's
        // `int(current_seq) if current_seq is not None else 0`.
        let result: redis::RedisResult<Option<i64>> = conn.get(&key).await;
        match result {
            Ok(Some(seq)) => u64::try_from(seq).unwrap_or(0),
            Ok(None) => 0,
            Err(e) => {
                tracing::warn!("streaming redis: current_seq(node {node_id}) failed: {e}");
                0
            }
        }
    }

    async fn set(&self, node_id: i64, seq: u64, event: StreamEvent) {
        let Some(mut conn) = self.conn().await else {
            return;
        };
        let key = format!("data:{node_id}:{seq}");
        let channel = format!("notify:{node_id}");
        // `metadata` is stored as a JSON string (upstream `safe_json_dump`); a
        // `serde_json::Value` always serializes, so the fallback is unreachable
        // but keeps this infallible.
        let metadata_json =
            serde_json::to_string(&event.metadata).unwrap_or_else(|_| "null".to_string());

        // One HSET with `sequence` + `metadata`, plus `payload` (raw bytes) only
        // when the event carries some — mirroring upstream's conditional
        // `mapping["payload"]`. Then bound the hash's lifetime and publish the
        // sequence to live subscribers. `payload`'s value is binary, so it is
        // added via `.arg(&[u8])` rather than any UTF-8 path.
        let mut pipe = redis::pipe();
        pipe.atomic();
        {
            let hset = pipe.cmd("HSET");
            hset.arg(&key)
                .arg("sequence")
                .arg(seq)
                .arg("metadata")
                .arg(&metadata_json);
            if let Some(payload) = event.payload.as_ref() {
                hset.arg("payload").arg(payload.as_ref());
            }
            hset.ignore();
        }
        pipe.expire(&key, self.data_ttl).ignore();
        pipe.publish(&channel, seq).ignore();

        let result: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
        if let Err(e) = result {
            tracing::warn!("streaming redis: set(node {node_id}, seq {seq}) failed: {e}");
        }
    }

    async fn get(&self, node_id: i64, seq: u64) -> Option<StreamEvent> {
        let mut conn = self.conn().await?;
        let key = format!("data:{node_id}:{seq}");
        // `HGETALL` reads the hash as raw bytes so the binary `payload` field
        // survives; field names are always UTF-8. A missing/expired key returns
        // an empty map.
        let result: redis::RedisResult<HashMap<String, Vec<u8>>> = conn.hgetall(&key).await;
        let map = match result {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!("streaming redis: get(node {node_id}, seq {seq}) failed: {e}");
                return None;
            }
        };
        if map.is_empty() {
            // Key absent or expired.
            return None;
        }
        // A stored event always has a `metadata` field; without one the hash is
        // malformed and there is nothing to reconstruct.
        let metadata: Value = match map.get("metadata") {
            Some(bytes) => match serde_json::from_slice(bytes) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        "streaming redis: get(node {node_id}, seq {seq}): metadata parse failed: {e}"
                    );
                    return None;
                }
            },
            None => {
                tracing::warn!(
                    "streaming redis: get(node {node_id}, seq {seq}): hash missing metadata field"
                );
                return None;
            }
        };
        let payload = map.get("payload").map(|b| Bytes::copy_from_slice(b));
        Some(StreamEvent { metadata, payload })
    }

    fn subscribe(&self, node_id: i64) -> broadcast::Receiver<u64> {
        // Bridge one dedicated Redis pub/sub connection into a broadcast
        // channel, faithful to upstream's per-handler `live_sequence_source`
        // (each websocket handler opens its own pub/sub). The spawned task owns
        // the only `Sender`, so its lifetime is tied to the returned `Receiver`:
        // once the consumer drops it, the next publish (or the connection
        // closing) ends the task.
        let (tx, rx) = broadcast::channel(CHANNEL_CAPACITY);
        let client = self.client.clone();
        tokio::spawn(async move {
            pubsub_bridge(client, node_id, tx).await;
        });
        rx
    }

    async fn close(&self, node_id: i64) {
        // Append an end-of-stream marker at a fresh sequence and publish it, so
        // every live subscriber sees the producer has ended (upstream
        // `RedisStreamingDatastore.close`, streaming.py:466-490).
        let seq = self.incr_seq(node_id).await;
        self.set(node_id, seq, StreamEvent::end_of_stream()).await;
        // Extend the sequence counter to outlive the cached data (upstream
        // streaming.py:488, `expire(sequence:{node_id}, 1 + data_ttl)`).
        // `incr_seq` above stamped only the idle `seq_ttl` (~1 h), and `set`
        // never touches the sequence key; left at `seq_ttl` the counter would
        // expire long before the `data_ttl`-lived (~30 d) EOS + backlog, so a
        // later `?start=` resumer would read `current_seq → 0`, replay nothing,
        // and never receive the cached EOS — hanging instead of closing 1000.
        // The extension is owned here in `close`, not in `incr_seq`/`set`, so no
        // non-owner pokes the sequence TTL. `+ 1` matches upstream (outlive the
        // last data by 1 s); `saturating_add` guards the `i64::MAX` clamp that
        // `ttl_secs` may have produced from a very large `data_ttl`.
        let Some(mut conn) = self.conn().await else {
            return;
        };
        let key = format!("sequence:{node_id}");
        let ttl = self.data_ttl.saturating_add(1);
        let result: redis::RedisResult<()> = conn.expire(&key, ttl).await;
        if let Err(e) = result {
            tracing::warn!("streaming redis: close(node {node_id}) seq expire failed: {e}");
        }
    }
}

/// Subscribe to `notify:{node_id}` on a dedicated Redis pub/sub connection and
/// forward each message (a decimal sequence number) into `tx`. Runs until the
/// pub/sub connection closes or the consumer drops its receiver (detected when a
/// send finds no receivers). Every failure path logs and ends the task; the
/// consumer reconnects by calling [`RedisStreamingCache::subscribe`] again.
async fn pubsub_bridge(client: redis::Client, node_id: i64, tx: broadcast::Sender<u64>) {
    use futures::StreamExt;

    let channel = format!("notify:{node_id}");
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(pubsub) => pubsub,
        Err(e) => {
            tracing::warn!("streaming redis: pubsub connect for node {node_id} failed: {e}");
            return;
        }
    };
    if let Err(e) = pubsub.subscribe(&channel).await {
        tracing::warn!("streaming redis: subscribe {channel} failed: {e}");
        return;
    }
    // `into_on_message` owns the connection, so the stream needs no borrow of
    // `pubsub` and can live for the whole task.
    let mut stream = pubsub.into_on_message();
    while let Some(msg) = stream.next().await {
        match msg.get_payload::<u64>() {
            Ok(seq) => {
                // `Err` means the receiver was dropped: the consumer is gone, so
                // tear down this bridge and its pub/sub connection.
                if tx.send(seq).is_err() {
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("streaming redis: bad notify payload on {channel}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Spec test for the Redis backend. Runtime is exercised only when a real
    //! Redis is reachable via `TILED_TEST_REDIS_URL`; absent that (CI and any
    //! machine without a Redis) it early-returns. The value of the test is
    //! (a) it compiles the whole backend under the feature and (b) when a Redis
    //! *is* provided it asserts the upstream-exact key layout end to end.

    use super::*;
    use std::time::Duration;

    /// The Redis URL to test against, or `None` to skip (no Redis available).
    fn test_url() -> Option<String> {
        std::env::var("TILED_TEST_REDIS_URL").ok()
    }

    #[tokio::test]
    async fn redis_backend_matches_upstream_key_format() {
        let Some(url) = test_url() else {
            eprintln!(
                "skipping redis_backend_matches_upstream_key_format: \
                 TILED_TEST_REDIS_URL not set (no Redis available)"
            );
            return;
        };

        // A node id unlikely to collide with anything else in a shared Redis.
        let node: i64 = 1_902_400_517;
        let seq_key = format!("sequence:{node}");
        let notify_channel = format!("notify:{node}");

        // Independent verification connection (not the cache's manager).
        let client = redis::Client::open(url.as_str()).expect("open redis client");
        let mut probe = client
            .get_multiplexed_async_connection()
            .await
            .expect("connect probe");

        // Clean slate so the counter starts at 1 and stale keys don't skew the
        // assertions.
        let _: () = redis::cmd("DEL")
            .arg(&seq_key)
            .query_async(&mut probe)
            .await
            .expect("clean sequence key");

        let cache = RedisStreamingCache::new(&url, 3600, 2_592_000).expect("build cache");

        // Subscribe BEFORE producing, then give the SUBSCRIBE a moment to land
        // so the later publish is delivered live.
        let mut rx = cache.subscribe(node);
        tokio::time::sleep(Duration::from_millis(150)).await;

        // --- incr_seq is monotonic and lands on `sequence:{node_id}` ---
        assert_eq!(cache.incr_seq(node).await, 1, "first INCR yields 1");
        assert_eq!(cache.incr_seq(node).await, 2, "counter is monotonic");
        assert_eq!(
            cache.current_seq(node).await,
            2,
            "current_seq reads the counter"
        );
        let raw_seq: i64 = redis::cmd("GET")
            .arg(&seq_key)
            .query_async(&mut probe)
            .await
            .expect("GET sequence key");
        assert_eq!(raw_seq, 2, "counter stored under sequence:{{node_id}}");

        // --- set writes data:{node_id}:{seq} as a hash with the exact fields ---
        let seq = cache.incr_seq(node).await; // 3
        let data_key = format!("data:{node}:{seq}");
        let payload = Bytes::from_static(b"\x00\x01\x02\xff");
        let event = StreamEvent::array_data(
            seq,
            "application/octet-stream",
            &[2, 2],
            None,
            None,
            payload.clone(),
        );
        cache.set(node, seq, event).await;

        // The probe reads the exact key and field names upstream uses.
        let stored: HashMap<String, Vec<u8>> =
            probe.hgetall(&data_key).await.expect("HGETALL data key");
        assert!(
            stored.contains_key("sequence"),
            "hash carries a `sequence` field"
        );
        assert!(
            stored.contains_key("metadata"),
            "hash carries a `metadata` field"
        );
        assert_eq!(
            stored.get("payload").map(Vec::as_slice),
            Some(payload.as_ref()),
            "payload round-trips as raw bytes under the `payload` field"
        );
        let stored_meta: Value =
            serde_json::from_slice(stored.get("metadata").unwrap()).expect("metadata is JSON");
        assert_eq!(stored_meta["type"], "array-data");
        assert_eq!(stored_meta["sequence"], seq);

        // --- get() reconstructs the StreamEvent (metadata + payload) ---
        let got = cache.get(node, seq).await.expect("event present");
        assert_eq!(got.metadata["type"], "array-data");
        assert_eq!(got.metadata["shape"], serde_json::json!([2, 2]));
        assert_eq!(got.payload, Some(payload));
        // A sequence that was never written is absent.
        assert!(cache.get(node, seq + 100).await.is_none());

        // --- publish → subscribe delivers the sequence number ---
        let recv = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("subscribe delivered within timeout")
            .expect("received a sequence");
        // The first live event our subscriber sees is the first set() after it
        // subscribed, i.e. `seq`.
        assert_eq!(recv, seq, "subscribe forwards the published sequence");

        // --- close() appends an end_of_stream marker at a fresh sequence ---
        cache.close(node).await;
        let eos_seq = cache.current_seq(node).await;
        let eos = cache.get(node, eos_seq).await.expect("eos event present");
        assert_eq!(eos.metadata["end_of_stream"], true);
        assert!(eos.payload.is_none());

        // Regression (Redis close() seq-TTL extension, upstream streaming.py:488):
        // after close(), the sequence counter must be extended to `1 + data_ttl`
        // so it outlives the `data_ttl`-lived cached EOS/backlog. Left at the
        // idle `seq_ttl` (3600) that `incr_seq` stamped, a later `?start=`
        // resumer would read `current_seq → 0`, replay nothing, and hang. The
        // TTL counts down from `1 + data_ttl` (2_592_001), so it must be far
        // above `seq_ttl`; assert a generous lower bound tolerant of elapsed time.
        let seq_ttl_after_close: i64 = redis::cmd("TTL")
            .arg(&seq_key)
            .query_async(&mut probe)
            .await
            .expect("TTL sequence key");
        assert!(
            seq_ttl_after_close > 3600,
            "close() must extend the sequence TTL beyond seq_ttl (3600) to ~1+data_ttl; \
             got {seq_ttl_after_close} — the counter would expire before the cached data \
             and break post-close ?start= replay"
        );

        // Cleanup every key this test created.
        let _: () = redis::cmd("DEL")
            .arg(&seq_key)
            .arg(&data_key)
            .arg(format!("data:{node}:{eos_seq}"))
            .query_async(&mut probe)
            .await
            .expect("cleanup keys");
        // Drop the subscriber so the bridge task can wind down; the channel name
        // is referenced above to keep the format assertions self-documenting.
        let _ = notify_channel;
        drop(rx);
    }
}
