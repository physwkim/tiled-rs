//! BlueskyRun adapter — a single experimental run containing event streams.
//!
//! Corresponds to `databroker.mongo_normalized.BlueskyRun`.

use std::sync::Arc;

use indexmap::IndexMap;
use mongodb::bson::{Document, doc};
use mongodb::sync::Database;
use tokio::sync::OnceCell;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, BoxFuture, ContainerAdapter};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::filler::Filler;
use crate::handler::HandlerRegistry;
use crate::stream::EventStreamAdapter;

/// A single Bluesky experimental run.
pub struct BlueskyRunAdapter {
    db: Database,
    start_doc: Document,
    stop_doc: Option<Document>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    handler_registry: Arc<HandlerRegistry>,
    /// Cached stream mapping, populated once via `spawn_blocking` (the sync
    /// MongoDB driver) and awaited through an async [`OnceCell`].
    streams: OnceCell<IndexMap<String, AnyAdapter>>,
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
            streams: OnceCell::new(),
        }
    }

    fn uid(&self) -> &str {
        self.start_doc.get_str("uid").unwrap_or_default()
    }

    /// Load the stream mapping once, offloading the synchronous MongoDB
    /// driver to `spawn_blocking`. A failed *load task* surfaces as `Err`;
    /// per-document failures are logged inside the blocking loader.
    async fn streams(&self) -> tiled_core::error::Result<&IndexMap<String, AnyAdapter>> {
        self.streams
            .get_or_try_init(|| async {
                let db = self.db.clone();
                let uid = self.uid().to_string();
                let stop_doc = self.stop_doc.clone();
                let registry = self.handler_registry.clone();
                tokio::task::spawn_blocking(move || {
                    load_streams_blocking(db, uid, stop_doc, registry)
                })
                .await
                .map_err(|e| {
                    tiled_core::error::TiledError::Internal(format!(
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
    // per-stream count so each EventStream declares the right
    // number of rows. Streams not listed there default to 1 (no
    // events), matching Bluesky's "stop emitted before any events
    // arrived" case.
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
            .unwrap_or(1);
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
        let table = crate::EventStreamTable::new(
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
    fn structure(&self) -> BoxFuture<'_, tiled_core::error::Result<ContainerStructure>> {
        Box::pin(async move {
            Ok(ContainerStructure {
                keys: self.streams().await?.keys().cloned().collect(),
            })
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, tiled_core::error::Result<Option<AnyAdapter>>> {
        Box::pin(async move { Ok(self.streams().await?.get(key).cloned()) })
    }

    fn keys(&self) -> BoxFuture<'_, tiled_core::error::Result<Vec<String>>> {
        Box::pin(async move { Ok(self.streams().await?.keys().cloned().collect()) })
    }

    fn len(&self) -> BoxFuture<'_, tiled_core::error::Result<usize>> {
        Box::pin(async move { Ok(self.streams().await?.len()) })
    }
}
