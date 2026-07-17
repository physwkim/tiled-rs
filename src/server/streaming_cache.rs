//! Per-node data-streaming cache (Wave-24, upstream `tiled.streaming`).
//!
//! Upstream tiled keeps a *data* stream per catalog node. This per-node cache
//! is the sole streaming primitive after Wave-24 PR2b retired the old
//! notification-only streaming bus; it now backs the WebSocket path directly.
//! Each node has a monotonic sequence counter and a cache of the actual event
//! payloads (array blocks, table partitions, tree events, …) keyed by
//! `(node_id, sequence)`. A WebSocket handler replays cached events from a
//! requested sequence, then follows live ones. The backend is pluggable —
//! upstream ships an in-process `cachetools.TTLCache` variant and a Redis
//! variant (`tiled/streaming.py:314-527`); this module mirrors the in-process
//! one. Redis is Wave-24 PR8.
//!
//! This PR (PR1) lands the trait, the in-memory backend, the disabled no-op
//! backend, and the [`StreamEvent`] shape with typed constructors that build
//! the flat upstream metadata field names (`tiled/catalog/adapter.py`,
//! `tiled/server/core.py:754-820`). Nothing is wired to the write path or the
//! WebSocket yet — the cache is constructed and stored on `AppState` but not
//! consumed; that wiring is PR2 onwards.

use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use serde_json::{Map, Value, json};
use tokio::sync::broadcast;

/// One event in a node's data stream: a flat JSON metadata header plus an
/// optional binary payload.
///
/// The `metadata` map holds exactly the field names upstream emits on the
/// wire (`type`, `sequence`, `timestamp`, plus per-event fields). The
/// `payload` carries the encoded bytes for data events (array blocks, table
/// partitions); tree/ref/end-of-stream events have no payload. The typed
/// constructors below are the only sanctioned way to build one, so the field
/// names stay in sync with upstream.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    /// Flat metadata header, upstream field names.
    pub metadata: Value,
    /// Encoded payload bytes for data events; `None` for tree/ref/EOS events.
    pub payload: Option<Bytes>,
}

/// ISO-8601 timestamp, matching the `timestamp` field upstream stamps on every
/// event (`datetime` isoformat; tiled-rs uses RFC 3339 UTC throughout).
fn now_timestamp() -> String {
    Utc::now().to_rfc3339()
}

/// Render an optional index vector as a JSON array or `null`, matching
/// upstream's `offset` / `block` / `partition` nullable fields.
fn opt_indices(indices: Option<&[usize]>) -> Value {
    match indices {
        Some(v) => Value::from(v.to_vec()),
        None => Value::Null,
    }
}

impl StreamEvent {
    /// `array-data` event (`tiled/catalog/adapter.py:1642-1656`): a written
    /// array block. `payload` is the encoded block bytes.
    pub fn array_data(
        sequence: u64,
        mimetype: &str,
        shape: &[usize],
        offset: Option<&[usize]>,
        block: Option<&[usize]>,
        payload: Bytes,
    ) -> Self {
        Self {
            metadata: json!({
                "type": "array-data",
                "sequence": sequence,
                "timestamp": now_timestamp(),
                "mimetype": mimetype,
                "shape": shape,
                "offset": opt_indices(offset),
                "block": opt_indices(block),
            }),
            payload: Some(payload),
        }
    }

    /// `ragged-data` event (`tiled/catalog/adapter.py:1770-1783`): a written
    /// ragged block. Same field set as `array-data`, but a ragged structure's
    /// `shape` has variable dimensions (upstream `entry.structure().shape`),
    /// so each axis is `Option<usize>` and a `None` axis serializes as JSON
    /// `null` (e.g. `[3, null]`).
    pub fn ragged_data(
        sequence: u64,
        mimetype: &str,
        shape: &[Option<usize>],
        offset: Option<&[usize]>,
        block: Option<&[usize]>,
        payload: Bytes,
    ) -> Self {
        Self {
            metadata: json!({
                "type": "ragged-data",
                "sequence": sequence,
                "timestamp": now_timestamp(),
                "mimetype": mimetype,
                "shape": shape,
                "offset": opt_indices(offset),
                "block": opt_indices(block),
            }),
            payload: Some(payload),
        }
    }

    /// `table-data` event (`tiled/catalog/adapter.py:1858-1871`): a written
    /// table partition. `append` distinguishes a partition append from a full
    /// replace. `payload` is the encoded partition bytes.
    pub fn table_data(
        sequence: u64,
        mimetype: &str,
        partition: Option<usize>,
        append: bool,
        payload: Bytes,
    ) -> Self {
        Self {
            metadata: json!({
                "type": "table-data",
                "sequence": sequence,
                "timestamp": now_timestamp(),
                "mimetype": mimetype,
                "partition": match partition {
                    Some(p) => Value::from(p),
                    None => Value::Null,
                },
                "append": append,
            }),
            payload: Some(payload),
        }
    }

    /// `array-ref` event (`tiled/catalog/adapter.py:976-992`): a reference to a
    /// newly-registered data source rather than an inline block. Carries the
    /// data source, an optional patch descriptor, and the resulting shape; the
    /// wire handler derives a slice URI from these at send time (PR2). No
    /// payload.
    pub fn array_ref(
        sequence: u64,
        data_source: Value,
        patch: Option<Value>,
        shape: &[usize],
    ) -> Self {
        Self {
            metadata: json!({
                "type": "array-ref",
                "sequence": sequence,
                "timestamp": now_timestamp(),
                "data_source": data_source,
                "patch": patch.unwrap_or(Value::Null),
                "shape": shape,
            }),
            payload: None,
        }
    }

    /// `container-child-created` event (`tiled/catalog/adapter.py:859-873`):
    /// published on the *parent* container's node id when a child is created.
    /// No payload.
    #[allow(clippy::too_many_arguments)]
    pub fn child_created(
        sequence: u64,
        key: &str,
        structure_family: &str,
        specs: Value,
        metadata: Value,
        data_sources: Value,
        access_blob: Value,
    ) -> Self {
        Self {
            metadata: json!({
                "type": "container-child-created",
                "sequence": sequence,
                "timestamp": now_timestamp(),
                "key": key,
                "structure_family": structure_family,
                "specs": specs,
                "metadata": metadata,
                "data_sources": data_sources,
                "access_blob": access_blob,
            }),
            payload: None,
        }
    }

    /// `container-child-metadata-updated` event
    /// (`tiled/catalog/adapter.py:1322-1334`): published on the parent when a
    /// child's metadata/specs change. `revision_number` is omitted (upstream
    /// `drop_revision`) when `None`. No payload.
    pub fn child_metadata_updated(
        sequence: u64,
        key: &str,
        specs: Value,
        metadata: Value,
        revision_number: Option<i64>,
    ) -> Self {
        let mut map = Map::new();
        map.insert("type".into(), "container-child-metadata-updated".into());
        map.insert("sequence".into(), sequence.into());
        map.insert("timestamp".into(), now_timestamp().into());
        map.insert("key".into(), key.into());
        map.insert("specs".into(), specs);
        map.insert("metadata".into(), metadata);
        if let Some(rev) = revision_number {
            map.insert("revision_number".into(), rev.into());
        }
        Self {
            metadata: Value::Object(map),
            payload: None,
        }
    }

    /// `node-deleted` event: published on the deleted node's own id when the
    /// node is removed. tiled-rs extension (Wave-24 PR2b, D9) — upstream's
    /// `delete()` (`tiled/catalog/adapter.py:1042`) emits no streaming event;
    /// tiled-rs keeps its delete notification. No payload.
    pub fn node_deleted(sequence: u64) -> Self {
        Self {
            metadata: json!({
                "type": "node-deleted",
                "sequence": sequence,
                "timestamp": now_timestamp(),
            }),
            payload: None,
        }
    }

    /// `end_of_stream` marker (`tiled/streaming.py` `close`): the final event,
    /// carrying only a timestamp and the `end_of_stream: true` flag. No `type`
    /// and no `sequence` inside the header (the sequence is the cache key). No
    /// payload.
    pub fn end_of_stream() -> Self {
        Self {
            metadata: json!({
                "timestamp": now_timestamp(),
                "end_of_stream": true,
            }),
            payload: None,
        }
    }
}

/// Pluggable per-node data-stream cache.
///
/// The producer side (a write handler, PR3+) does
/// `let seq = cache.incr_seq(node_id).await; cache.set(node_id, seq, event).await;`
/// The consumer side (the WS handler, PR2) replays `get(node_id, seq)` for
/// `seq` up to `current_seq`, then follows `subscribe(node_id)` for live
/// sequence numbers and fetches each with `get`.
#[async_trait]
pub trait StreamingCache: Send + Sync {
    /// Bump the node's sequence counter and return the new value (starts at 1).
    async fn incr_seq(&self, node_id: i64) -> u64;

    /// The node's current (last-issued) sequence, or 0 if none.
    async fn current_seq(&self, node_id: i64) -> u64;

    /// Store `event` at `(node_id, seq)` and notify subscribers of `seq`.
    async fn set(&self, node_id: i64, seq: u64, event: StreamEvent);

    /// Fetch the event at `(node_id, seq)`, or `None` if absent/expired.
    async fn get(&self, node_id: i64, seq: u64) -> Option<StreamEvent>;

    /// Subscribe to the node's live sequence numbers. Each `set` publishes the
    /// stored sequence to every current subscriber.
    fn subscribe(&self, node_id: i64) -> broadcast::Receiver<u64>;

    /// Close the node's stream by appending an [`StreamEvent::end_of_stream`]
    /// event at a fresh sequence and notifying subscribers.
    async fn close(&self, node_id: i64);
}

/// A cached data event plus its expiry deadline (`data_ttl` from insertion).
struct DataEntry {
    deadline: Instant,
    event: StreamEvent,
}

/// Per-node state: sequence counter (with `seq_ttl` idle expiry), the ring of
/// cached events, and the live-notification sender.
struct NodeEntry {
    seq: AtomicU64,
    /// When the sequence counter expires if the node stays idle. Refreshed on
    /// every `incr_seq`. Mirrors upstream's `seq_ttl` on the counter cache.
    seq_deadline: std::sync::Mutex<Instant>,
    /// `sequence -> (deadline, event)`, ordered so the oldest sequence evicts
    /// first when `maxsize` is exceeded — a bounded history ring for replay.
    data: std::sync::Mutex<BTreeMap<u64, DataEntry>>,
    notify: broadcast::Sender<u64>,
}

/// Broadcast channel depth for live sequence notifications.
const CHANNEL_CAPACITY: usize = 256;

/// Number of `set` mutations between opportunistic slot-reclamation sweeps.
/// The sweep is lazy (piggybacked on the producer's own housekeeping) rather
/// than a background timer, so it must be cheap-per-`set` and only occasionally
/// pay for a full pass.
const RECLAIM_INTERVAL: u64 = 512;

/// In-process data-streaming cache backed by [`DashMap`], mirroring upstream's
/// `cachetools.TTLCache` variant (`tiled/streaming.py:346-357`).
pub struct InMemoryStreamingCache {
    nodes: DashMap<i64, NodeEntry>,
    /// Idle expiry for a node's sequence counter (upstream `seq_ttl`, 3600 s).
    seq_ttl: Duration,
    /// Expiry for a cached event (upstream `data_ttl`, 2 592 000 s / 30 d).
    data_ttl: Duration,
    /// Maximum cached events retained per node (upstream `maxsize`, 1000).
    maxsize: usize,
    /// Mutations since the last reclamation sweep; drives lazy, background-task-
    /// free reclamation of idle node slots (see [`Self::maybe_reclaim`]).
    reclaim_counter: AtomicU64,
}

impl InMemoryStreamingCache {
    /// Construct with the given TTLs and per-node cache bound.
    pub fn new(seq_ttl: Duration, data_ttl: Duration, maxsize: usize) -> Self {
        Self {
            nodes: DashMap::new(),
            seq_ttl,
            data_ttl,
            // A cache bound of 0 would drop every event on insert; clamp to at
            // least 1 so a stored event is retrievable at least until its TTL.
            maxsize: maxsize.max(1),
            reclaim_counter: AtomicU64::new(0),
        }
    }

    /// Get-or-create the per-node entry, seeding a fresh counter and channel.
    fn entry(&self, node_id: i64) -> dashmap::mapref::one::RefMut<'_, i64, NodeEntry> {
        self.nodes.entry(node_id).or_insert_with(|| NodeEntry {
            seq: AtomicU64::new(0),
            seq_deadline: std::sync::Mutex::new(Instant::now() + self.seq_ttl),
            data: std::sync::Mutex::new(BTreeMap::new()),
            notify: broadcast::channel(CHANNEL_CAPACITY).0,
        })
    }

    /// A node slot is reclaimable IFF *all three* terms hold: the sequence
    /// counter has lapsed (`now >= seq_deadline`), the replay ring is empty
    /// after purging any TTL-lapsed events, and no live subscriber holds a
    /// receiver on its channel (`receiver_count() == 0`). Each term guards a
    /// consumer path — a live receiver may still `get` a just-broadcast seq, a
    /// non-empty ring may still be replayed, and a fresh counter means the node
    /// is active — so reclaiming without all three would drop state a consumer
    /// or replay still needs.
    fn is_reclaimable(entry: &NodeEntry, now: Instant) -> bool {
        // A live WS subscriber still holds a receiver: never reclaim, or the
        // consumer's next `get` after a broadcast would miss the slot.
        if entry.notify.receiver_count() != 0 {
            return false;
        }
        // The sequence counter is still within its idle TTL: node is active.
        if now < *entry.seq_deadline.lock().unwrap() {
            return false;
        }
        // Purge lapsed events; a non-empty ring means replay is still possible.
        let mut data = entry.data.lock().unwrap();
        data.retain(|_, e| e.deadline > now);
        data.is_empty()
    }

    /// Reclaim every node slot that is currently [`Self::is_reclaimable`],
    /// bounding the `nodes` map so idle nodes do not leak after their counter
    /// and data both lapse. This mirrors upstream's `TTLCache` auto-eviction
    /// (`tiled/streaming.py:346-357`), where an idle `sequence:{node_id}` key
    /// simply expires. `remove_if` re-checks the predicate under the shard
    /// write lock, so it cannot race a concurrent `entry()` that just
    /// recreated or refreshed the slot: such a slot fails `is_reclaimable`
    /// (fresh deadline / non-empty ring / live receiver) and is kept.
    fn reclaim_expired(&self) {
        let now = Instant::now();
        // Snapshot keys first so the shard read locks from iteration are
        // released before `remove_if` takes shard write locks (no self-deadlock).
        let candidates: Vec<i64> = self.nodes.iter().map(|r| *r.key()).collect();
        for id in candidates {
            self.nodes
                .remove_if(&id, |_, entry| Self::is_reclaimable(entry, now));
        }
    }

    /// Opportunistic, lazy reclamation: every `RECLAIM_INTERVAL`-th mutation
    /// runs one sweep. There is deliberately no background task — the sweep
    /// piggybacks on the producer's own `set` housekeeping. The threshold race
    /// (two callers both crossing it) is benign: it costs at most one extra
    /// idempotent sweep.
    fn maybe_reclaim(&self) {
        if self.reclaim_counter.fetch_add(1, Ordering::Relaxed) + 1 >= RECLAIM_INTERVAL {
            self.reclaim_counter.store(0, Ordering::Relaxed);
            self.reclaim_expired();
        }
    }

    /// Number of node slots currently held (test-only introspection).
    #[cfg(test)]
    fn tracked_nodes(&self) -> usize {
        self.nodes.len()
    }
}

#[async_trait]
impl StreamingCache for InMemoryStreamingCache {
    async fn incr_seq(&self, node_id: i64) -> u64 {
        let entry = self.entry(node_id);
        let now = Instant::now();
        // Serialize the expiry check + counter reset under the deadline lock so
        // two concurrent `incr_seq` calls agree on whether the counter lapsed.
        let mut deadline = entry.seq_deadline.lock().unwrap();
        if now >= *deadline {
            entry.seq.store(0, Ordering::SeqCst);
        }
        *deadline = now + self.seq_ttl;
        entry.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn current_seq(&self, node_id: i64) -> u64 {
        match self.nodes.get(&node_id) {
            Some(entry) => {
                let deadline = *entry.seq_deadline.lock().unwrap();
                // An expired counter reads as 0, matching upstream's lapsed
                // `sequence:{node_id}` key falling back to 0.
                if Instant::now() >= deadline {
                    0
                } else {
                    entry.seq.load(Ordering::SeqCst)
                }
            }
            None => 0,
        }
    }

    async fn set(&self, node_id: i64, seq: u64, event: StreamEvent) {
        {
            let entry = self.entry(node_id);
            {
                let mut data = entry.data.lock().unwrap();
                let now = Instant::now();
                // Lazy purge of lapsed events, then insert, then bound the ring.
                data.retain(|_, e| e.deadline > now);
                data.insert(
                    seq,
                    DataEntry {
                        deadline: now + self.data_ttl,
                        event,
                    },
                );
                while data.len() > self.maxsize {
                    let oldest = *data.keys().next().expect("non-empty after insert");
                    data.remove(&oldest);
                }
            }
            // Notify live subscribers; `Err` just means no receivers are attached.
            let _ = entry.notify.send(seq);
        }
        // The `entry` RefMut (shard lock) is dropped above; only now is it safe
        // to run a reclamation sweep, which takes shard write locks of its own.
        self.maybe_reclaim();
    }

    async fn get(&self, node_id: i64, seq: u64) -> Option<StreamEvent> {
        let entry = self.nodes.get(&node_id)?;
        let mut data = entry.data.lock().unwrap();
        match data.get(&seq) {
            Some(e) if e.deadline > Instant::now() => Some(e.event.clone()),
            Some(_) => {
                // Expired: drop it so memory is reclaimed on the read path too.
                data.remove(&seq);
                None
            }
            None => None,
        }
    }

    fn subscribe(&self, node_id: i64) -> broadcast::Receiver<u64> {
        self.entry(node_id).notify.subscribe()
    }

    async fn close(&self, node_id: i64) {
        let seq = self.incr_seq(node_id).await;
        self.set(node_id, seq, StreamEvent::end_of_stream()).await;
    }
}

/// No-op cache used when streaming is not configured (the default). Every
/// operation is a no-op; `subscribe` returns a receiver on a channel that never
/// emits, so a consumer simply waits forever rather than seeing a closed error.
pub struct DisabledStreamingCache {
    notify: broadcast::Sender<u64>,
}

impl DisabledStreamingCache {
    pub fn new() -> Self {
        Self {
            notify: broadcast::channel(1).0,
        }
    }
}

impl Default for DisabledStreamingCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StreamingCache for DisabledStreamingCache {
    async fn incr_seq(&self, _node_id: i64) -> u64 {
        0
    }

    async fn current_seq(&self, _node_id: i64) -> u64 {
        0
    }

    async fn set(&self, _node_id: i64, _seq: u64, _event: StreamEvent) {}

    async fn get(&self, _node_id: i64, _seq: u64) -> Option<StreamEvent> {
        None
    }

    fn subscribe(&self, _node_id: i64) -> broadcast::Receiver<u64> {
        self.notify.subscribe()
    }

    async fn close(&self, _node_id: i64) {}
}

/// The default streaming cache: a [`DisabledStreamingCache`] behind an `Arc`.
pub fn disabled() -> Arc<dyn StreamingCache> {
    Arc::new(DisabledStreamingCache::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_memory() -> InMemoryStreamingCache {
        InMemoryStreamingCache::new(
            Duration::from_secs(3600),
            Duration::from_secs(2_592_000),
            1000,
        )
    }

    #[tokio::test]
    async fn incr_seq_is_monotonic_per_node() {
        let cache = in_memory();
        assert_eq!(cache.incr_seq(1).await, 1);
        assert_eq!(cache.incr_seq(1).await, 2);
        assert_eq!(cache.incr_seq(1).await, 3);
        assert_eq!(cache.current_seq(1).await, 3);

        // A different node has its own independent counter.
        assert_eq!(cache.incr_seq(2).await, 1);
        assert_eq!(cache.current_seq(2).await, 1);
        assert_eq!(cache.current_seq(1).await, 3);
    }

    #[tokio::test]
    async fn set_get_roundtrips_metadata_and_payload() {
        let cache = in_memory();
        let seq = cache.incr_seq(7).await;
        let payload = Bytes::from_static(b"\x01\x02\x03\x04");
        let event = StreamEvent::array_data(
            seq,
            "application/octet-stream",
            &[2, 2],
            None,
            None,
            payload.clone(),
        );
        cache.set(7, seq, event).await;

        let got = cache.get(7, seq).await.expect("event present");
        assert_eq!(got.metadata["type"], "array-data");
        assert_eq!(got.metadata["sequence"], seq);
        assert_eq!(got.metadata["mimetype"], "application/octet-stream");
        assert_eq!(got.metadata["shape"], json!([2, 2]));
        assert_eq!(got.metadata["offset"], Value::Null);
        assert_eq!(got.payload, Some(payload));

        // A sequence that was never set is absent.
        assert!(cache.get(7, seq + 1).await.is_none());
    }

    #[tokio::test]
    async fn expired_event_is_not_returned() {
        // A very short data TTL: the event lapses almost immediately.
        let cache =
            InMemoryStreamingCache::new(Duration::from_secs(3600), Duration::from_millis(5), 1000);
        let seq = cache.incr_seq(1).await;
        cache
            .set(
                1,
                seq,
                StreamEvent::table_data(
                    seq,
                    "text/csv",
                    Some(0),
                    true,
                    Bytes::from_static(b"a,b\n"),
                ),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            cache.get(1, seq).await.is_none(),
            "an event past its data_ttl must not be returned"
        );
    }

    #[tokio::test]
    async fn maxsize_evicts_oldest_sequence() {
        let cache = InMemoryStreamingCache::new(
            Duration::from_secs(3600),
            Duration::from_secs(2_592_000),
            2,
        );
        for _ in 0..3 {
            let seq = cache.incr_seq(1).await;
            cache
                .set(
                    1,
                    seq,
                    StreamEvent::array_data(
                        seq,
                        "application/octet-stream",
                        &[1],
                        None,
                        None,
                        Bytes::new(),
                    ),
                )
                .await;
        }
        // Only the two newest sequences (2 and 3) survive; 1 was evicted.
        assert!(cache.get(1, 1).await.is_none(), "oldest sequence evicted");
        assert!(cache.get(1, 2).await.is_some());
        assert!(cache.get(1, 3).await.is_some());
    }

    #[tokio::test]
    async fn subscribe_receives_published_sequence() {
        let cache = in_memory();
        let mut rx = cache.subscribe(5);
        let seq = cache.incr_seq(5).await;
        cache
            .set(
                5,
                seq,
                StreamEvent::array_data(
                    seq,
                    "application/octet-stream",
                    &[1],
                    None,
                    None,
                    Bytes::new(),
                ),
            )
            .await;
        assert_eq!(rx.recv().await.unwrap(), seq);
    }

    #[tokio::test]
    async fn close_yields_end_of_stream_event() {
        let cache = in_memory();
        let mut rx = cache.subscribe(9);
        cache.close(9).await;
        let seq = rx.recv().await.expect("close notifies subscribers");
        let event = cache.get(9, seq).await.expect("end_of_stream cached");
        assert_eq!(event.metadata["end_of_stream"], true);
        assert!(event.metadata.get("type").is_none(), "EOS carries no type");
        assert!(event.payload.is_none());
    }

    #[tokio::test]
    async fn slot_reclaimed_after_seq_deadline_with_no_receivers() {
        // Both the sequence counter and the cached event lapse almost at once.
        let cache =
            InMemoryStreamingCache::new(Duration::from_millis(5), Duration::from_millis(5), 1000);
        let seq = cache.incr_seq(1).await;
        cache
            .set(
                1,
                seq,
                StreamEvent::array_data(
                    seq,
                    "application/octet-stream",
                    &[1],
                    None,
                    None,
                    Bytes::new(),
                ),
            )
            .await;
        assert_eq!(cache.tracked_nodes(), 1, "slot present right after set");

        tokio::time::sleep(Duration::from_millis(40)).await;
        cache.reclaim_expired();
        assert_eq!(
            cache.tracked_nodes(),
            0,
            "an idle slot is reclaimed once its counter and data both lapse with no receivers"
        );
    }

    #[tokio::test]
    async fn slot_not_reclaimed_while_receiver_live() {
        let cache =
            InMemoryStreamingCache::new(Duration::from_millis(5), Duration::from_millis(5), 1000);
        let rx = cache.subscribe(2); // a live subscriber holds a receiver
        let seq = cache.incr_seq(2).await;
        cache
            .set(
                2,
                seq,
                StreamEvent::array_data(
                    seq,
                    "application/octet-stream",
                    &[1],
                    None,
                    None,
                    Bytes::new(),
                ),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(40)).await;
        cache.reclaim_expired();
        assert_eq!(
            cache.tracked_nodes(),
            1,
            "a slot with a live subscriber is never reclaimed, even after both TTLs lapse"
        );

        // Once the last receiver drops, the now-idle slot becomes reclaimable.
        drop(rx);
        cache.reclaim_expired();
        assert_eq!(
            cache.tracked_nodes(),
            0,
            "slot reclaimed after the last receiver drops"
        );
    }

    #[tokio::test]
    async fn slot_not_reclaimed_while_data_nonempty() {
        // Short seq_ttl so the counter lapses, but a long data_ttl so the replay
        // ring stays populated — the data-empty term must veto reclamation.
        let cache =
            InMemoryStreamingCache::new(Duration::from_millis(5), Duration::from_secs(3600), 1000);
        let seq = cache.incr_seq(3).await;
        cache
            .set(
                3,
                seq,
                StreamEvent::array_data(
                    seq,
                    "application/octet-stream",
                    &[1],
                    None,
                    None,
                    Bytes::new(),
                ),
            )
            .await;

        tokio::time::sleep(Duration::from_millis(40)).await; // counter lapses; data does not
        cache.reclaim_expired();
        assert_eq!(
            cache.tracked_nodes(),
            1,
            "a slot whose replay ring still holds a live event is not reclaimed, even with the \
             counter lapsed and no receivers"
        );
    }

    #[tokio::test]
    async fn node_deleted_then_close_still_delivers_under_reclamation() {
        // COMMIT-1 invariant regression: a live consumer subscribes, then the
        // node is deleted (node-deleted event) and the stream closed (EOS). A
        // reclamation sweep running in between must not evict the slot while the
        // consumer's receiver is live, so both terminal events stay replayable.
        let cache =
            InMemoryStreamingCache::new(Duration::from_secs(3600), Duration::from_secs(3600), 1000);
        let mut rx = cache.subscribe(4);

        let s1 = cache.incr_seq(4).await;
        cache.set(4, s1, StreamEvent::node_deleted(s1)).await;
        // close() must stay a pure appender: incr_seq + EOS, no eviction.
        cache.close(4).await;
        let s2 = cache.current_seq(4).await;

        // A sweep while the consumer is still attached must be a no-op here.
        cache.reclaim_expired();
        assert_eq!(
            cache.tracked_nodes(),
            1,
            "slot with a live receiver survives the sweep"
        );

        assert_eq!(rx.recv().await.unwrap(), s1);
        assert_eq!(rx.recv().await.unwrap(), s2);
        let deleted = cache
            .get(4, s1)
            .await
            .expect("node-deleted still cached under reclamation");
        assert_eq!(deleted.metadata["type"], "node-deleted");
        let eos = cache
            .get(4, s2)
            .await
            .expect("EOS still cached under reclamation");
        assert_eq!(eos.metadata["end_of_stream"], true);
    }

    #[tokio::test]
    async fn disabled_cache_is_inert() {
        let cache = DisabledStreamingCache::new();
        assert_eq!(cache.incr_seq(1).await, 0);
        cache.set(1, 1, StreamEvent::end_of_stream()).await;
        assert!(cache.get(1, 1).await.is_none());
        assert_eq!(cache.current_seq(1).await, 0);
    }
}
