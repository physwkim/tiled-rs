//! Top-level MongoDB catalog adapter.
//!
//! Corresponds to `databroker.mongo_normalized.MongoAdapter`.
//! Lists BlueskyRuns from the `run_start` collection.

use std::sync::OnceLock;

use indexmap::IndexMap;
use mongodb::bson::{doc, Document};
use mongodb::sync::Database;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::queries::Query;
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::run::BlueskyRunAdapter;

/// Top-level catalog: lists all BlueskyRuns in a MongoDB database.
pub struct MongoCatalog {
    db: Database,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Cached mapping of uid → BlueskyRunAdapter (populated on first access).
    runs: OnceLock<IndexMap<String, AnyAdapter>>,
}

impl MongoCatalog {
    /// Create from a connected MongoDB database.
    pub fn new(db: Database, metadata: serde_json::Value) -> Self {
        Self {
            db,
            metadata,
            specs: vec![Spec::with_version("CatalogOfBlueskyRuns", "1")],
            runs: OnceLock::new(),
        }
    }

    /// Connect to MongoDB and create a catalog.
    pub fn from_uri(uri: &str) -> Result<Self, mongodb::error::Error> {
        let client = mongodb::sync::Client::with_uri_str(uri)?;
        // Extract database name from URI (last path segment).
        let db_name = uri
            .rsplit('/')
            .next()
            .and_then(|s| s.split('?').next())
            .unwrap_or("databroker");
        let db = client.database(db_name);
        Ok(Self::new(db, serde_json::json!({})))
    }

    fn load_runs(&self) -> &IndexMap<String, AnyAdapter> {
        self.runs.get_or_init(|| {
            let mut mapping = IndexMap::new();
            let collection = self.db.collection::<Document>("run_start");

            // Find all run_start docs, sorted by time descending (newest first).
            let opts = mongodb::options::FindOptions::builder()
                .sort(doc! { "time": -1 })
                .build();

            if let Ok(cursor) = collection.find(doc! {}).with_options(opts).run() {
                for result in cursor {
                    if let Ok(start_doc) = result {
                        let uid = start_doc
                            .get_str("uid")
                            .unwrap_or_default()
                            .to_string();
                        if uid.is_empty() {
                            continue;
                        }

                        // Look up the corresponding stop document.
                        let stop_doc = self
                            .db
                            .collection::<Document>("run_stop")
                            .find_one(doc! { "run_start": &uid })
                            .run()
                            .ok()
                            .flatten();

                        let run = BlueskyRunAdapter::new(
                            self.db.clone(),
                            start_doc,
                            stop_doc,
                        );
                        mapping.insert(uid, AnyAdapter::Container(Box::new(run)));
                    }
                }
            }
            mapping
        })
    }
}

impl BaseAdapter for MongoCatalog {
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

impl ContainerAdapter for MongoCatalog {
    fn structure(&self) -> &ContainerStructure {
        // Leak is acceptable here — catalog lives for process lifetime.
        let keys: Vec<String> = self.load_runs().keys().cloned().collect();
        Box::leak(Box::new(ContainerStructure { keys }))
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.load_runs().get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.load_runs().keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.load_runs().len()
    }

    fn search(&self, queries: &[Query]) -> Vec<String> {
        if queries.is_empty() {
            return self.keys();
        }
        // For MongoDB-backed catalog, filter runs by metadata.
        self.load_runs()
            .iter()
            .filter(|(_, adapter)| {
                let meta = adapter.metadata();
                queries.iter().all(|q| match q {
                    Query::Eq(eq) => {
                        // Search in start document metadata.
                        meta.get("start")
                            .and_then(|s| s.get(&eq.key))
                            .is_some_and(|v| v == &eq.value)
                    }
                    Query::FullText(ft) => meta.to_string().contains(&ft.text),
                    Query::StructureFamily(sf) => adapter.structure_family() == sf.value,
                    _ => true,
                })
            })
            .map(|(k, _)| k.clone())
            .collect()
    }
}
