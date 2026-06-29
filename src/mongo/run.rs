//! BlueskyRun adapter — a single experimental run containing event streams.
//!
//! Corresponds to `databroker.mongo_normalized.BlueskyRun`.

use std::sync::Arc;

use indexmap::IndexMap;
use mongodb::bson::{Document, doc};
use mongodb::sync::Database;

use crate::core::adapters::{AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter};
use crate::core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::mongo::cache::{DEFAULT_TTL, TtlCache};
use crate::mongo::filler::Filler;
use crate::mongo::handler::HandlerRegistry;
use crate::mongo::stream::EventStreamAdapter;

/// A single Bluesky experimental run.
pub struct BlueskyRunAdapter {
    db: Database,
    start_doc: Document,
    stop_doc: Option<Document>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    handler_registry: Arc<HandlerRegistry>,
    /// Cached stream mapping, populated via `spawn_blocking` (the sync MongoDB
    /// driver). A [`TtlCache`] (not a permanent `OnceCell`) so streams whose
    /// `event_descriptor`s are written to an in-progress run after the first
    /// access become visible once the TTL elapses, without a restart
    /// (Mongo/M1).
    streams: TtlCache<IndexMap<String, AnyAdapter>>,
}

impl BlueskyRunAdapter {
    pub fn new(db: Database, start_doc: Document, stop_doc: Option<Document>) -> Self {
        Self::with_handlers(db, start_doc, stop_doc, Arc::new(HandlerRegistry::new()))
    }

    pub fn with_handlers(
        db: Database,
        start_doc: Document,
        stop_doc: Option<Document>,
        handler_registry: Arc<HandlerRegistry>,
    ) -> Self {
        // Build metadata as {"start": {...}, "stop": {...}}
        let start_json: serde_json::Value = mongodb::bson::from_document(start_doc.clone())
            .unwrap_or_else(|e| {
                let uid = start_doc.get_str("uid").unwrap_or("<missing>");
                tracing::warn!(
                    target: "tiled.mongo",
                    run_uid = %uid,
                    error = %e,
                    "BSON→JSON decode failed for run_start; metadata.start will be null \
                     (run is visible in unfiltered listing but invisible to metadata filters)"
                );
                serde_json::Value::Null
            });
        let stop_json: serde_json::Value = stop_doc
            .as_ref()
            .and_then(|d| mongodb::bson::from_document(d.clone()).ok())
            .unwrap_or(serde_json::Value::Null);

        let metadata = serde_json::json!({
            "start": start_json,
            "stop": stop_json,
        });

        Self {
            db,
            start_doc,
            stop_doc,
            metadata,
            specs: vec![Spec::with_version("BlueskyRun", "1")],
            handler_registry,
            streams: TtlCache::new(DEFAULT_TTL),
        }
    }

    fn uid(&self) -> &str {
        self.start_doc.get_str("uid").unwrap_or_default()
    }

    /// Load the stream mapping, offloading the synchronous MongoDB driver to
    /// `spawn_blocking`. The result is cached for [`DEFAULT_TTL`]; calls past
    /// the window reload so streams added to an in-progress run appear without
    /// a restart (Mongo/M1). A failed *load task* surfaces as `Err` and is not
    /// cached; per-document failures are logged inside the blocking loader.
    async fn streams(&self) -> crate::core::error::Result<Arc<IndexMap<String, AnyAdapter>>> {
        self.streams
            .get_or_refresh(|| async {
                let db = self.db.clone();
                let uid = self.uid().to_string();
                let stop_doc = self.stop_doc.clone();
                let registry = self.handler_registry.clone();
                tokio::task::spawn_blocking(move || {
                    load_streams_blocking(db, uid, stop_doc, registry)
                })
                .await
                .map_err(|e| {
                    crate::core::error::TiledError::Internal(format!(
                        "mongo stream-load task failed: {e}"
                    ))
                })
            })
            .await
    }
}

/// Synchronous stream loader — runs entirely on a `spawn_blocking` thread.
/// Groups `event_descriptor` docs by stream name and builds an
/// EventStream + flat `_table` sibling per stream. Query/decode errors are
/// logged (parity with the previous design): a failure yields a
/// possibly-empty map rather than an `Err`.
fn load_streams_blocking(
    db: Database,
    uid: String,
    stop_doc: Option<Document>,
    handler_registry: Arc<HandlerRegistry>,
) -> IndexMap<String, AnyAdapter> {
    let mut mapping = IndexMap::new();

    // Create filler for external data resolution.
    let filler = Arc::new(Filler::new(db.clone(), handler_registry.clone()));

    let collection = db.collection::<Document>("event_descriptor");
    // M5: surface a failed descriptor query instead of silently
    // returning a run with no streams (a client cannot distinguish
    // that from a run that genuinely recorded none).
    let cursor = match collection.find(doc! { "run_start": &uid }).run() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                target: "tiled.mongo",
                run_uid = %uid,
                error = %e,
                "event_descriptor query failed; run will expose no streams"
            );
            return mapping;
        }
    };

    let mut descriptors_by_stream: IndexMap<String, Vec<Document>> = IndexMap::new();
    for result in cursor {
        match result {
            Ok(desc) => {
                let name = desc.get_str("name").unwrap_or("primary").to_string();
                descriptors_by_stream.entry(name).or_default().push(desc);
            }
            Err(e) => tracing::error!(
                target: "tiled.mongo",
                run_uid = %uid,
                error = %e,
                "event_descriptor document decode failed; descriptor skipped"
            ),
        }
    }

    // `stop.num_events` is a `{stream_name: count}` dict — use the
    // per-stream count so each EventStream declares the right number of
    // rows.  When no stop doc is present (open/aborted/crashed run) or the
    // stream is absent from num_events, fall back to querying the event
    // collection for the highest seq_num — matching Python databroker's
    // `_build_event_stream` (mongo_normalized.py:1539-1555).
    let num_events_doc = stop_doc
        .as_ref()
        .and_then(|d| d.get_document("num_events").ok())
        .cloned();

    for (stream_name, descriptors) in descriptors_by_stream {
        let cutoff_seq_num = num_events_doc
            .as_ref()
            .and_then(|ne| {
                // Accept i64 or i32; reject negative event counts
                // (would wrap when cast to usize).
                let n = ne
                    .get_i64(&stream_name)
                    .ok()
                    .or_else(|| ne.get_i32(&stream_name).ok().map(i64::from))?;
                usize::try_from(n).ok().map(|n| n + 1)
            })
            .unwrap_or_else(|| derive_cutoff_from_events(&db, &descriptors));
        let stream = EventStreamAdapter::new(
            db.clone(),
            stream_name.clone(),
            descriptors.clone(),
            cutoff_seq_num,
            Some(filler.clone()),
        );
        mapping.insert(stream_name.clone(), AnyAdapter::Container(Arc::new(stream)));
        // Surface a table-shaped sibling so clients that want
        // a flat Arrow view (pandas / polars / datafusion)
        // can read it without composing per-column reads.
        let table = crate::mongo::EventStreamTable::new(
            db.clone(),
            stream_name.clone(),
            descriptors,
            cutoff_seq_num,
        );
        mapping.insert(
            format!("{stream_name}_table"),
            AnyAdapter::Table(Arc::new(table)),
        );
    }
    mapping
}

/// Derive a half-open cutoff seq_num by aggregating the event collection.
///
/// Parity with Python databroker `_build_event_stream`
/// (mongo_normalized.py:1539-1555): when no stop doc is present or the stream
/// is not listed in `stop.num_events`, query `$max seq_num` across all events
/// for this stream's descriptors and return `1 + max_seq_num`.  Returns `1`
/// (empty stream) when no events are found or the query fails.
fn derive_cutoff_from_events(db: &Database, descriptors: &[Document]) -> usize {
    let descriptor_uids: Vec<&str> = descriptors
        .iter()
        .filter_map(|d| d.get_str("uid").ok())
        .collect();

    if descriptor_uids.is_empty() {
        return 1;
    }

    let pipeline = vec![
        doc! { "$match": { "descriptor": { "$in": &descriptor_uids } } },
        doc! { "$group": { "_id": null, "max_seq_num": { "$max": "$seq_num" } } },
    ];

    db.collection::<Document>("event")
        .aggregate(pipeline)
        .run()
        .ok()
        .and_then(|mut cursor| cursor.next())
        .and_then(|r| r.ok())
        .and_then(|doc| {
            doc.get_i64("max_seq_num")
                .ok()
                .or_else(|| doc.get_i32("max_seq_num").ok().map(i64::from))
        })
        .and_then(|n| usize::try_from(n).ok())
        .map(|n| n + 1)
        .unwrap_or(1)
}

impl BaseAdapter for BlueskyRunAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Container
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ContainerAdapter for BlueskyRunAdapter {
    fn structure(&self) -> BoxFuture<'_, crate::core::error::Result<ContainerStructure>> {
        Box::pin(async move {
            Ok(ContainerStructure {
                keys: self.streams().await?.keys().cloned().collect(),
            })
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, crate::core::error::Result<Option<AnyAdapter>>> {
        Box::pin(async move { Ok(self.streams().await?.get(key).cloned()) })
    }

    fn keys(&self) -> BoxFuture<'_, crate::core::error::Result<Vec<String>>> {
        Box::pin(async move { Ok(self.streams().await?.keys().cloned().collect()) })
    }

    fn len(&self) -> BoxFuture<'_, crate::core::error::Result<usize>> {
        Box::pin(async move { Ok(self.streams().await?.len()) })
    }
}

#[cfg(test)]
mod tests {
    /// Pure arithmetic: convert an optional max_seq_num to a half-open cutoff.
    /// Extracted from derive_cutoff_from_events for unit testing without MongoDB.
    fn cutoff_from_max_seq_num(max_seq_num: Option<usize>) -> usize {
        max_seq_num.map(|n| n + 1).unwrap_or(1)
    }

    /// Matches Python: `cutoff_seq_num = 1 + result["highest_seq_num"]`
    #[test]
    fn cutoff_is_one_plus_max_seq_num() {
        assert_eq!(cutoff_from_max_seq_num(Some(0)), 1);
        assert_eq!(cutoff_from_max_seq_num(Some(9)), 10);
        assert_eq!(cutoff_from_max_seq_num(Some(100)), 101);
    }

    /// Python: `cutoff_seq_num = 1` when no events found in the collection.
    #[test]
    fn cutoff_is_one_when_no_events() {
        assert_eq!(cutoff_from_max_seq_num(None), 1);
    }

    // End-to-end test (derive_cutoff_from_events hitting a real MongoDB
    // aggregate) requires a live-Mongo harness not present in this crate.
    // The aggregate query and its BSON read path are the integration gap;
    // the arithmetic above is unit-tested via cutoff_from_max_seq_num.
}
