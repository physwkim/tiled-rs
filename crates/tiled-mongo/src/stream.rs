//! Event stream adapter — exposes "data" and "timestamps" sub-containers.
//!
//! Corresponds to `databroker.mongo_normalized.BlueskyEventStream` (partially)
//! and `DatasetFromDocuments`.

use std::sync::OnceLock;

use indexmap::IndexMap;
use mongodb::bson::Document;
use mongodb::sync::Database;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::array_col::ArrayColumnAdapter;

/// An event stream (e.g. "primary") containing data columns.
pub struct EventStreamAdapter {
    db: Database,
    stream_name: String,
    descriptors: Vec<Document>,
    cutoff_seq_num: usize,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Lazily loaded data columns.
    columns: OnceLock<IndexMap<String, AnyAdapter>>,
}

impl EventStreamAdapter {
    pub fn new(
        db: Database,
        stream_name: String,
        descriptors: Vec<Document>,
        cutoff_seq_num: usize,
    ) -> Self {
        let descriptor_meta: Vec<serde_json::Value> = descriptors
            .iter()
            .filter_map(|d| mongodb::bson::from_document(d.clone()).ok())
            .collect();

        let metadata = serde_json::json!({
            "stream_name": &stream_name,
            "descriptors": descriptor_meta,
        });

        Self {
            db,
            stream_name,
            descriptors,
            cutoff_seq_num,
            metadata,
            specs: vec![Spec::new("xarray_dataset")],
            columns: OnceLock::new(),
        }
    }

    fn load_columns(&self) -> &IndexMap<String, AnyAdapter> {
        self.columns.get_or_init(|| {
            let mut mapping = IndexMap::new();

            if self.descriptors.is_empty() || self.cutoff_seq_num <= 1 {
                return mapping;
            }

            let descriptor = &self.descriptors[0];
            let descriptor_uids: Vec<String> = self
                .descriptors
                .iter()
                .filter_map(|d| d.get_str("uid").ok().map(String::from))
                .collect();

            let num_events = self.cutoff_seq_num - 1;

            // Add "time" coordinate column.
            let time_col = ArrayColumnAdapter::new_time(
                self.db.clone(),
                descriptor_uids.clone(),
                num_events,
            );
            mapping.insert("time".to_string(), AnyAdapter::Array(Box::new(time_col)));

            // Add data columns from data_keys.
            if let Ok(data_keys) = descriptor.get_document("data_keys") {
                for (key, value) in data_keys {
                    let field_meta = match value.as_document() {
                        Some(d) => d,
                        None => continue,
                    };

                    let shape: Vec<usize> = field_meta
                        .get_array("shape")
                        .ok()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_i64().map(|n| n as usize))
                                .collect()
                        })
                        .unwrap_or_default();

                    let dtype_str = field_meta
                        .get_str("dtype")
                        .unwrap_or("number");

                    let col = ArrayColumnAdapter::new_data(
                        self.db.clone(),
                        descriptor_uids.clone(),
                        key.clone(),
                        num_events,
                        shape,
                        dtype_str.to_string(),
                    );
                    mapping.insert(key.clone(), AnyAdapter::Array(Box::new(col)));
                }
            }

            mapping
        })
    }
}

impl BaseAdapter for EventStreamAdapter {
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

impl ContainerAdapter for EventStreamAdapter {
    fn structure(&self) -> &ContainerStructure {
        let keys: Vec<String> = self.load_columns().keys().cloned().collect();
        Box::leak(Box::new(ContainerStructure { keys }))
    }

    fn get(&self, key: &str) -> Option<&AnyAdapter> {
        self.load_columns().get(key)
    }

    fn keys(&self) -> Vec<String> {
        self.load_columns().keys().cloned().collect()
    }

    fn len(&self) -> usize {
        self.load_columns().len()
    }
}
