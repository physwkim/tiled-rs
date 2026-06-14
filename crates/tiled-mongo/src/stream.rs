//! Event stream adapter — exposes data columns as array adapters.
//!
//! Detects whether each column is inline (scalar in MongoDB) or external
//! (datum_id referencing files via Resource/Datum).

use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;
use mongodb::bson::Document;
use mongodb::sync::Database;

use tiled_core::adapters::{AnyAdapter, BaseAdapter, ContainerAdapter};
use tiled_core::structures::{ContainerStructure, Spec, StructureFamily};

use crate::array_col::{ArrayColumnAdapter, DataColumnConfig};
use crate::bson_ext::bson_to_i64;
use crate::filler::Filler;

/// An event stream (e.g. "primary") containing data columns.
pub struct EventStreamAdapter {
    db: Database,
    descriptors: Vec<Document>,
    cutoff_seq_num: usize,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    filler: Option<Arc<Filler>>,
    columns: OnceLock<IndexMap<String, AnyAdapter>>,
    structure_cache: OnceLock<ContainerStructure>,
}

impl EventStreamAdapter {
    pub fn new(
        db: Database,
        stream_name: String,
        descriptors: Vec<Document>,
        cutoff_seq_num: usize,
        filler: Option<Arc<Filler>>,
    ) -> Self {
        let descriptor_meta: Vec<serde_json::Value> = descriptors
            .iter()
            .filter_map(|d| mongodb::bson::from_document(d.clone()).ok())
            .collect();

        let metadata = serde_json::json!({
            "stream_name": stream_name,
            "descriptors": descriptor_meta,
        });

        Self {
            db,
            descriptors,
            cutoff_seq_num,
            metadata,
            specs: vec![Spec::new("xarray_dataset")],
            filler,
            columns: OnceLock::new(),
            structure_cache: OnceLock::new(),
        }
    }

    fn load_columns(&self) -> &IndexMap<String, AnyAdapter> {
        self.columns.get_or_init(|| {
            let mut mapping = IndexMap::new();

            if self.descriptors.is_empty() || self.cutoff_seq_num <= 1 {
                return mapping;
            }

            let descriptor_uids: Vec<String> = self
                .descriptors
                .iter()
                .filter_map(|d| d.get_str("uid").ok().map(String::from))
                .collect();

            let num_events = self.cutoff_seq_num - 1;

            // Add "time" coordinate column (always inline).
            let time_col =
                ArrayColumnAdapter::new_time(self.db.clone(), descriptor_uids.clone(), num_events);
            mapping.insert("time".to_string(), AnyAdapter::Array(Arc::new(time_col)));

            // Aggregate data_keys across every descriptor in the stream.
            // A run that swaps descriptors mid-stream still exposes every
            // declared field; the FIRST descriptor that mentions a key wins
            // (matches IndexMap insertion-order semantics — later
            // descriptors don't shadow earlier ones).
            for descriptor in &self.descriptors {
                let Ok(data_keys) = descriptor.get_document("data_keys") else {
                    continue;
                };
                for (key, value) in data_keys {
                    if mapping.contains_key(key) {
                        continue;
                    }
                    let Some(field_meta) = value.as_document() else {
                        continue;
                    };

                    let shape: Vec<usize> = field_meta
                        .get_array("shape")
                        .ok()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| bson_to_i64(v).map(|n| n as usize))
                                .collect()
                        })
                        .unwrap_or_default();

                    let dtype_str = field_meta.get_str("dtype").unwrap_or("number");

                    let is_external = field_meta.get_str("external").is_ok()
                        || field_meta.contains_key("external");

                    let col = ArrayColumnAdapter::new_data(
                        self.db.clone(),
                        descriptor_uids.clone(),
                        DataColumnConfig {
                            field_name: key.clone(),
                            num_events,
                            inner_shape: shape,
                            dtype_str: dtype_str.to_string(),
                            is_external,
                            filler: if is_external {
                                self.filler.clone()
                            } else {
                                None
                            },
                        },
                    );
                    mapping.insert(key.clone(), AnyAdapter::Array(Arc::new(col)));
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
        self.structure_cache.get_or_init(|| ContainerStructure {
            keys: self.load_columns().keys().cloned().collect(),
        })
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
