//! Array column adapter — reads a single data column from MongoDB events.
//!
//! Corresponds to `databroker.mongo_normalized.ArrayFromDocuments` and
//! `DatasetFromDocuments.get_columns` / `_get_time_coord`.


use mongodb::bson::{doc, Document};
use mongodb::sync::Database;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

/// A single array column backed by MongoDB event documents.
pub struct ArrayColumnAdapter {
    db: Database,
    descriptor_uids: Vec<String>,
    field_name: String,
    num_events: usize,
    shape: Vec<usize>,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    is_time: bool,
}

impl ArrayColumnAdapter {
    /// Create a "time" coordinate column.
    pub fn new_time(db: Database, descriptor_uids: Vec<String>, num_events: usize) -> Self {
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let shape = vec![num_events];
        let chunks = vec![vec![num_events]];
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: Some(vec!["time".to_string()]),
            resizable: Default::default(),
        };

        Self {
            db,
            descriptor_uids,
            field_name: "time".to_string(),
            num_events,
            shape,
            dtype,
            structure,
            metadata: serde_json::json!({"attrs": {}}),
            specs: vec![Spec::new("xarray_coord")],
            is_time: true,
        }
    }

    /// Create a data variable column.
    pub fn new_data(
        db: Database,
        descriptor_uids: Vec<String>,
        field_name: String,
        num_events: usize,
        inner_shape: Vec<usize>,
        dtype_str: String,
    ) -> Self {
        let dtype = guess_dtype(&dtype_str);

        // Full shape = [num_events, ...inner_shape]
        let mut shape = vec![num_events];
        shape.extend(&inner_shape);

        let chunks: Vec<Vec<usize>> = shape.iter().map(|&s| vec![s]).collect();

        let mut dims = vec!["time".to_string()];
        for i in 0..inner_shape.len() {
            dims.push(format!("dim_{i}"));
        }

        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: Some(dims),
            resizable: Default::default(),
        };

        Self {
            db,
            descriptor_uids,
            field_name,
            num_events,
            shape,
            dtype,
            structure,
            metadata: serde_json::json!({"attrs": {}}),
            specs: vec![Spec::new("xarray_data_var")],
            is_time: false,
        }
    }

    /// Fetch the time coordinate column from MongoDB.
    fn fetch_time_column(&self) -> Result<Vec<f64>> {
        let collection = self.db.collection::<Document>("event");
        let pipeline = vec![
            doc! {
                "$match": {
                    "descriptor": { "$in": &self.descriptor_uids },
                    "seq_num": { "$gte": 1, "$lt": (self.num_events + 1) as i64 },
                }
            },
            doc! { "$sort": { "time": 1 } },
            doc! {
                "$group": {
                    "_id": "$seq_num",
                    "doc": { "$last": "$$ROOT" },
                }
            },
            doc! { "$sort": { "doc.seq_num": 1 } },
            doc! {
                "$group": {
                    "_id": null,
                    "column": { "$push": "$doc.time" },
                }
            },
        ];

        let cursor = collection
            .aggregate(pipeline)
            .run()
            .map_err(|e| TiledError::Internal(format!("MongoDB aggregate error: {e}")))?;

        for result in cursor {
            let doc = result.map_err(|e| TiledError::Internal(e.to_string()))?;
            if let Ok(arr) = doc.get_array("column") {
                let values: Vec<f64> = arr
                    .iter()
                    .filter_map(|v| v.as_f64())
                    .collect();
                return Ok(values);
            }
        }

        Ok(vec![0.0; self.num_events])
    }

    /// Fetch a data column from MongoDB.
    fn fetch_data_column(&self) -> Result<Vec<f64>> {
        let collection = self.db.collection::<Document>("event");
        let field_path = format!("data.{}", self.field_name);
        let push_path = format!("$doc.data.{}", self.field_name);

        let pipeline = vec![
            doc! {
                "$match": {
                    "descriptor": { "$in": &self.descriptor_uids },
                    "seq_num": { "$gte": 1, "$lt": (self.num_events + 1) as i64 },
                }
            },
            doc! {
                "$project": {
                    "descriptor": 1,
                    "seq_num": 1,
                    &field_path: 1,
                }
            },
            doc! { "$sort": { "time": 1 } },
            doc! {
                "$group": {
                    "_id": "$seq_num",
                    "doc": { "$last": "$$ROOT" },
                }
            },
            doc! { "$sort": { "doc.seq_num": 1 } },
            doc! {
                "$group": {
                    "_id": null,
                    "column": { "$push": &push_path },
                }
            },
        ];

        let cursor = collection
            .aggregate(pipeline)
            .run()
            .map_err(|e| TiledError::Internal(format!("MongoDB aggregate error: {e}")))?;

        for result in cursor {
            let doc = result.map_err(|e| TiledError::Internal(e.to_string()))?;
            if let Ok(arr) = doc.get_array("column") {
                let values: Vec<f64> = arr
                    .iter()
                    .map(|v| v.as_f64().unwrap_or(0.0))
                    .collect();
                return Ok(values);
            }
        }

        Ok(vec![0.0; self.num_events])
    }
}

impl BaseAdapter for ArrayColumnAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Array
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl ArrayAdapterRead for ArrayColumnAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            let values = if self.is_time {
                self.fetch_time_column()?
            } else {
                self.fetch_data_column()?
            };

            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(DynNDArray::new(
                bytes::Bytes::from(raw),
                self.dtype.clone(),
                self.shape.clone(),
            ))
        })
    }

    fn read_block<'a>(
        &'a self,
        _block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        // For simplicity, return full column for any block request.
        // Proper block slicing would use seq_num ranges.
        Box::pin(async move {
            let values = if self.is_time {
                self.fetch_time_column()?
            } else {
                self.fetch_data_column()?
            };

            let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            Ok(DynNDArray::new(
                bytes::Bytes::from(raw),
                self.dtype.clone(),
                self.shape.clone(),
            ))
        })
    }
}

/// Map Bluesky dtype strings to Rust BuiltinDType.
fn guess_dtype(dtype_str: &str) -> BuiltinDType {
    match dtype_str {
        "number" | "integer" => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        "string" => BuiltinDType::new(Endianness::Little, Kind::Unicode, 40), // <U10
        "boolean" => BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1),
        "array" => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        _ => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
    }
}
