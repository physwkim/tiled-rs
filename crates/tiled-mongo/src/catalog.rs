//! Top-level MongoDB catalog adapter.
//!
//! Corresponds to `databroker.mongo_normalized.MongoAdapter`.
//! Lists BlueskyRuns from the `run_start` collection.

use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;
use mongodb::bson::{Document, doc};
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
    /// Cached structure (keys list) — kept in stable storage so `structure()`
    /// can return `&ContainerStructure` without `Box::leak`.
    structure_cache: OnceLock<ContainerStructure>,
}

impl MongoCatalog {
    /// Create from a connected MongoDB database.
    pub fn new(db: Database, metadata: serde_json::Value) -> Self {
        Self {
            db,
            metadata,
            specs: vec![Spec::with_version("CatalogOfBlueskyRuns", "1")],
            runs: OnceLock::new(),
            structure_cache: OnceLock::new(),
        }
    }

    /// Connect to MongoDB and create a catalog.
    pub fn from_uri(uri: &str) -> Result<Self, mongodb::error::Error> {
        let client = mongodb::sync::Client::with_uri_str(uri)?;
        // Extract database name from URI: `mongodb://host[:port][/db][?opts]`.
        // The last `/`-separated segment may be empty (no db given), include
        // a `?`-prefixed options block, or simply be the db name.
        let after_scheme = uri.split_once("://").map(|(_, r)| r).unwrap_or(uri);
        let path = after_scheme.split_once('/').map(|(_, p)| p).unwrap_or("");
        let path_no_opts = path.split('?').next().unwrap_or("").trim_matches('/');
        let db_name = if path_no_opts.is_empty() {
            "databroker"
        } else {
            path_no_opts
        };
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
                for start_doc in cursor.flatten() {
                    let uid = start_doc.get_str("uid").unwrap_or_default().to_string();
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

                    let run = BlueskyRunAdapter::new(self.db.clone(), start_doc, stop_doc);
                    mapping.insert(uid, AnyAdapter::Container(Arc::new(run)));
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
        self.structure_cache.get_or_init(|| ContainerStructure {
            keys: self.load_runs().keys().cloned().collect(),
        })
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
        self.load_runs()
            .iter()
            .filter(|(_, adapter)| queries.iter().all(|q| matches_run_query(adapter, q)))
            .map(|(k, _)| k.clone())
            .collect()
    }
}

/// Match a query against a BlueskyRun's metadata. Searches keys inside
/// `metadata.start` first (the natural place for run-level fields like
/// `plan_name`, `sample`, …), falling back to top-level metadata.
fn matches_run_query(adapter: &AnyAdapter, query: &Query) -> bool {
    let meta = adapter.metadata();
    let lookup = |key: &str| -> Option<&serde_json::Value> {
        meta.get("start")
            .and_then(|s| s.get(key))
            .or_else(|| meta.get(key))
    };
    match query {
        Query::FullText(ft) => meta.to_string().contains(&ft.text),
        Query::StructureFamily(sf) => adapter.structure_family() == sf.value,
        Query::Eq(eq) => lookup(&eq.key).is_some_and(|v| v == &eq.value),
        Query::NotEq(neq) => lookup(&neq.key).is_none_or(|v| v != &neq.value),
        Query::KeyPresent(kp) => lookup(&kp.key).is_some() == kp.exists,
        Query::Contains(c) => lookup(&c.key)
            .and_then(|v| v.as_array())
            .is_some_and(|arr| arr.contains(&c.value)),
        Query::In(q) => lookup(&q.key).is_some_and(|v| q.value.iter().any(|x| x == v)),
        Query::NotIn(q) => lookup(&q.key).is_some_and(|v| !q.value.iter().any(|x| x == v)),
        Query::Comparison(c) => lookup(&c.key).is_some_and(|v| {
            use std::cmp::Ordering;
            use tiled_core::queries::Operator;
            let cmp = match (v.as_f64(), c.value.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => match (v.as_str(), c.value.as_str()) {
                    (Some(a), Some(b)) => Some(a.cmp(b)),
                    _ => None,
                },
            };
            matches!(
                (c.operator, cmp),
                (Operator::Lt, Some(Ordering::Less))
                    | (Operator::Gt, Some(Ordering::Greater))
                    | (Operator::Le, Some(Ordering::Less | Ordering::Equal))
                    | (Operator::Ge, Some(Ordering::Greater | Ordering::Equal))
            )
        }),
        Query::Specs(s) => {
            let names: std::collections::HashSet<_> =
                adapter.specs().iter().map(|sp| sp.name.as_str()).collect();
            s.include.iter().all(|n| names.contains(n.as_str()))
                && !s.exclude.iter().any(|n| names.contains(n.as_str()))
        }
        // Other variants (Like/Regex/Lookup/KeysFilter/AccessBlobFilter):
        // not implemented for the catalog scan (would need MongoDB-side
        // query translation). Keep results unchanged rather than silently
        // dropping all runs.
        _ => true,
    }
}
