//! Array column adapter — reads a single data column from MongoDB events.
//!
//! Supports both inline data (scalars stored in MongoDB) and external data
//! (datum_ids resolved through Resource/Datum → file handlers).

use std::sync::Arc;

use mongodb::bson::{Bson, Document, doc};
use mongodb::sync::Database;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

use crate::filler::Filler;

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
    /// True if data is stored externally (datum_ids instead of inline values).
    is_external: bool,
    /// Filler for resolving external datum_ids. `None` for inline data.
    filler: Option<Arc<Filler>>,
}

impl ArrayColumnAdapter {
    /// Create a "time" coordinate column (always inline).
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
            is_external: false,
            filler: None,
        }
    }

    /// Create a data variable column (may be inline or external).
    pub fn new_data(
        db: Database,
        descriptor_uids: Vec<String>,
        field_name: String,
        num_events: usize,
        inner_shape: Vec<usize>,
        dtype_str: String,
        is_external: bool,
        filler: Option<Arc<Filler>>,
    ) -> Self {
        let dtype = guess_dtype(&dtype_str);

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
            is_external,
            filler,
        }
    }

    /// Fetch the time coordinate column from MongoDB.
    /// Fetch (seq_num, value) pairs for a projected event field. Pairs come
    /// out **possibly with gaps** — the caller must scatter them into a
    /// fixed-size column indexed by `seq_num - 1`.
    fn fetch_seq_value_pairs(&self, project: &Document, push_path: &str) -> Result<Vec<(i64, f64)>> {
        let collection = self.db.collection::<Document>("event");
        let pipeline = vec![
            doc! {
                "$match": {
                    "descriptor": { "$in": &self.descriptor_uids },
                    "seq_num": { "$gte": 1, "$lt": (self.num_events + 1) as i64 },
                }
            },
            doc! { "$project": project.clone() },
            doc! { "$sort": { "time": 1 } },
            doc! {
                "$group": {
                    "_id": "$seq_num",
                    "doc": { "$last": "$$ROOT" },
                }
            },
            doc! { "$sort": { "doc.seq_num": 1 } },
            doc! {
                "$project": {
                    "seq_num": "$doc.seq_num",
                    "value": push_path,
                }
            },
        ];

        let cursor = collection
            .aggregate(pipeline)
            .run()
            .map_err(|e| TiledError::Internal(format!("MongoDB aggregate error: {e}")))?;

        let mut out = Vec::new();
        for result in cursor {
            let doc = result.map_err(|e| TiledError::Internal(e.to_string()))?;
            let seq = doc.get_i64("seq_num").or_else(|_| doc.get_i32("seq_num").map(i64::from)).unwrap_or(0);
            let value = doc.get("value").and_then(|v| v.as_f64()).unwrap_or(f64::NAN);
            if seq >= 1 {
                out.push((seq, value));
            }
        }
        Ok(out)
    }

    /// Scatter `(seq_num, value)` pairs into a column of length `num_events`.
    /// Missing seq_nums become `NaN` so the column shape always matches the
    /// declared structure shape.
    fn scatter_pairs(&self, pairs: Vec<(i64, f64)>) -> Vec<f64> {
        let mut col = vec![f64::NAN; self.num_events];
        for (seq, value) in pairs {
            let idx = (seq - 1) as usize;
            if idx < col.len() {
                col[idx] = value;
            }
        }
        col
    }

    fn fetch_time_column(&self) -> Result<Vec<f64>> {
        let project = doc! {"descriptor": 1, "seq_num": 1, "time": 1};
        let pairs = self.fetch_seq_value_pairs(&project, "$doc.time")?;
        Ok(self.scatter_pairs(pairs))
    }

    /// Fetch inline scalar data column from MongoDB.
    fn fetch_inline_column(&self) -> Result<Vec<f64>> {
        let field_path = format!("data.{}", self.field_name);
        let push_path = format!("$doc.data.{}", self.field_name);
        let project = doc! {
            "descriptor": 1,
            "seq_num": 1,
            "time": 1,
            &field_path: 1,
        };
        let pairs = self.fetch_seq_value_pairs(&project, &push_path)?;
        Ok(self.scatter_pairs(pairs))
    }

    /// Fetch external data column: get datum_ids from MongoDB, then fill via handlers.
    fn fetch_external_column(&self) -> Result<Vec<u8>> {
        let filler = self
            .filler
            .as_ref()
            .ok_or_else(|| TiledError::Internal("External data but no filler configured".into()))?;

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
                    "time": 1,
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

        let mut datum_ids = Vec::new();
        for result in cursor {
            let doc = result.map_err(|e| TiledError::Internal(e.to_string()))?;
            if let Ok(arr) = doc.get_array("column") {
                for v in arr {
                    match v {
                        Bson::String(s) => datum_ids.push(s.clone()),
                        _ => datum_ids.push(v.to_string()),
                    }
                }
            }
        }

        let inner_shape = &self.shape[1..]; // skip the time dimension
        filler.fill_column(&datum_ids, inner_shape)
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
            if self.is_time {
                let values = self.fetch_time_column()?;
                let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                Ok(DynNDArray::new(
                    bytes::Bytes::from(raw),
                    self.dtype.clone(),
                    self.shape.clone(),
                ))
            } else if self.is_external {
                let raw = self.fetch_external_column()?;
                Ok(DynNDArray::new(
                    bytes::Bytes::from(raw),
                    self.dtype.clone(),
                    self.shape.clone(),
                ))
            } else {
                let values = self.fetch_inline_column()?;
                let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
                Ok(DynNDArray::new(
                    bytes::Bytes::from(raw),
                    self.dtype.clone(),
                    self.shape.clone(),
                ))
            }
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            // Each dim is a single chunk equal to the full extent (see
            // ArrayColumnAdapter::new_*), so the only valid block index is
            // all zeros. Reject everything else instead of silently
            // returning the full column.
            if block.len() != self.shape.len() {
                return Err(TiledError::Validation(format!(
                    "expected {} block indices, got {}",
                    self.shape.len(),
                    block.len()
                )));
            }
            if block.iter().any(|&b| b != 0) {
                return Err(TiledError::Validation(format!(
                    "ArrayColumnAdapter has a single chunk per dimension; valid block index is all zeros, got {block:?}"
                )));
            }
            self.read(slice).await
        })
    }
}

/// Map Bluesky dtype strings to Rust BuiltinDType.
fn guess_dtype(dtype_str: &str) -> BuiltinDType {
    match dtype_str {
        "number" | "integer" => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        "string" => BuiltinDType::new(Endianness::Little, Kind::Unicode, 40),
        "boolean" => BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1),
        "array" => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        _ => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
    }
}
