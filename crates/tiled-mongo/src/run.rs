//! BlueskyRun adapter — a single experimental run containing event streams.
//!
//! Corresponds to `databroker.mongo_normalized.BlueskyRun`.

use std::sync::OnceLock;

use indexmap::IndexMap;
use mongodb::bson::{doc, Document};
use mongodb::sync::Database;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::stream::EventStreamAdapter;

/// A single Bluesky experimental run.
pub struct BlueskyRunAdapter {
    db: Database,
    start_doc: Document,
    stop_doc: Option<Document>,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Lazily loaded streams (e.g. "primary", "baseline").
    streams: OnceLock<IndexMap<String, AnyAdapter>>,
}

impl BlueskyRunAdapter {
    pub fn new(db: Database, start_doc: Document, stop_doc: Option<Document>) -> Self {
        // Build metadata as {"start": {...}, "stop": {...}}
        let start_json: serde_json::Value =
            mongodb::bson::from_document(start_doc.clone()).unwrap_or_default();
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
            streams: OnceLock::new(),
        }
    }

    fn uid(&self) -> &str {
        self.start_doc.get_str("uid").unwrap_or_default()
    }

    fn load_streams(&self) -> &IndexMap<String, AnyAdapter> {
        self.streams.get_or_init(|| {
            let mut mapping = IndexMap::new();
            let uid = self.uid().to_string();

            // Find all event_descriptors for this run, grouped by stream name.
            let collection = self.db.collection::<Document>("event_descriptor");
            if let Ok(cursor) = collection.find(doc! { "run_start": &uid }).run() {
                let mut descriptors_by_stream: IndexMap<String, Vec<Document>> =
                    IndexMap::new();
                for desc in cursor.flatten() {
                    let name = desc
                        .get_str("name")
                        .unwrap_or("primary")
                        .to_string();
                    descriptors_by_stream
                        .entry(name)
                        .or_default()
                        .push(desc);
                }

                // Determine cutoff seq_num from the stop document.
                let cutoff_seq_num = self
                    .stop_doc
                    .as_ref()
                    .and_then(|d| d.get_document("num_events").ok())
                    .map(|num_events| {
                        num_events
                            .iter()
                            .map(|(_, v)| v.as_i64().unwrap_or(0) as usize + 1)
                            .max()
                            .unwrap_or(1)
                    })
                    .unwrap_or(1);

                for (stream_name, descriptors) in descriptors_by_stream {
                    let stream = EventStreamAdapter::new(
                        self.db.clone(),
                        stream_name.clone(),
                        descriptors,
                        cutoff_seq_num,
                    );
                    mapping.insert(stream_name, AnyAdapter::Container(Box::new(stream)));
                }
            }
            mapping
        })
    }
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
    fn structure(&self) -> &ContainerStructure {
        let keys: Vec<String> = self.load_streams().keys().cloned().collect();
        Box::leak(Box::new(ContainerStructure { keys }))
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.load_streams().get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.load_streams().keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.load_streams().len()
    }
}
