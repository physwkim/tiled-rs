//! Array column adapter — reads a single data column from MongoDB events.
//!
//! Supports both inline data (scalars stored in MongoDB) and external data
//! (datum_ids resolved through Resource/Datum → file handlers).

use std::sync::Arc;

use mongodb::bson::{Bson, Document, doc};
use mongodb::sync::Database;

/// Chunk size along the first (event) axis. Bounds memory per response;
/// chosen to fit a few MB of f64 frames on average.
const ROW_CHUNK: usize = 1024;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

use crate::filler::Filler;

/// A single array column backed by MongoDB event documents.
#[derive(Clone)]
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
        let chunks = vec![row_chunks(num_events)];
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

        // First axis (events) is chunked by ROW_CHUNK; inner axes remain a
        // single chunk per dim (we don't split inside a frame).
        let mut chunks: Vec<Vec<usize>> = Vec::with_capacity(shape.len());
        chunks.push(row_chunks(num_events));
        for &s in &inner_shape {
            chunks.push(vec![s]);
        }

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

    /// Fetch (seq_num, value) pairs in `[seq_start, seq_end_excl)`.
    /// Caller scatters into a fixed-size column.
    fn fetch_seq_value_pairs(
        &self,
        project: &Document,
        push_path: &str,
        seq_start: i64,
        seq_end_excl: i64,
    ) -> Result<Vec<(i64, f64)>> {
        let collection = self.db.collection::<Document>("event");
        let pipeline = vec![
            doc! {
                "$match": {
                    "descriptor": { "$in": &self.descriptor_uids },
                    "seq_num": { "$gte": seq_start, "$lt": seq_end_excl },
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

    /// Scatter `(seq_num, value)` pairs into a `len`-element column with the
    /// row indexed by `seq_num - seq_offset - 1`. Missing seq_nums stay
    /// `NaN`, preserving the declared shape regardless of MongoDB gaps.
    fn scatter_pairs(pairs: Vec<(i64, f64)>, seq_offset: i64, len: usize) -> Vec<f64> {
        let mut col = vec![f64::NAN; len];
        for (seq, value) in pairs {
            let idx = seq - seq_offset - 1;
            if idx >= 0 && (idx as usize) < col.len() {
                col[idx as usize] = value;
            }
        }
        col
    }

    fn fetch_time_column(&self) -> Result<Vec<f64>> {
        self.fetch_time_column_range(0, self.num_events)
    }

    fn fetch_time_column_range(&self, row_start: usize, row_end: usize) -> Result<Vec<f64>> {
        let project = doc! {"descriptor": 1, "seq_num": 1, "time": 1};
        let pairs = self.fetch_seq_value_pairs(
            &project,
            "$doc.time",
            (row_start as i64) + 1,
            (row_end as i64) + 1,
        )?;
        Ok(Self::scatter_pairs(pairs, row_start as i64, row_end - row_start))
    }

    fn fetch_inline_column(&self) -> Result<Vec<f64>> {
        self.fetch_inline_column_range(0, self.num_events)
    }

    fn fetch_inline_column_range(&self, row_start: usize, row_end: usize) -> Result<Vec<f64>> {
        let field_path = format!("data.{}", self.field_name);
        let push_path = format!("$doc.data.{}", self.field_name);
        let project = doc! {
            "descriptor": 1,
            "seq_num": 1,
            "time": 1,
            &field_path: 1,
        };
        let pairs = self.fetch_seq_value_pairs(
            &project,
            &push_path,
            (row_start as i64) + 1,
            (row_end as i64) + 1,
        )?;
        Ok(Self::scatter_pairs(pairs, row_start as i64, row_end - row_start))
    }

    fn fetch_external_column(&self) -> Result<Vec<u8>> {
        self.fetch_external_column_range(0, self.num_events)
    }

    /// Fetch external data column: get datum_ids from MongoDB, then fill via handlers.
    fn fetch_external_column_range(&self, row_start: usize, row_end: usize) -> Result<Vec<u8>> {
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
                    "seq_num": { "$gte": (row_start as i64) + 1, "$lt": (row_end as i64) + 1 },
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
            // Move a clone of self onto the blocking thread pool so the
            // synchronous MongoDB queries don't pin an async worker thread.
            let me = self.clone();
            let dtype = me.dtype.clone();
            let shape = me.shape.clone();
            let raw = tokio::task::spawn_blocking(move || -> std::result::Result<Vec<u8>, TiledError> {
                if me.is_time {
                    let values = me.fetch_time_column()?;
                    Ok(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                } else if me.is_external {
                    me.fetch_external_column()
                } else {
                    let values = me.fetch_inline_column()?;
                    Ok(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                }
            })
            .await
            .map_err(|e| TiledError::Internal(format!("blocking read: {e}")))??;

            Ok(DynNDArray::new(bytes::Bytes::from(raw), dtype, shape))
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            if block.len() != self.shape.len() {
                return Err(TiledError::Validation(format!(
                    "expected {} block indices, got {}",
                    self.shape.len(),
                    block.len()
                )));
            }
            // Inner axes are single-chunked; only block[0] (events) varies.
            for (axis, &b) in block.iter().enumerate().skip(1) {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "axis {axis} is a single chunk; valid block index is 0, got {b}"
                    )));
                }
            }

            let chunk_sizes = &self.structure.chunks[0];
            let block0 = block[0];
            if block0 >= chunk_sizes.len() {
                return Err(TiledError::Validation(format!(
                    "row block {block0} out of range ({} chunks)",
                    chunk_sizes.len()
                )));
            }
            let row_start: usize = chunk_sizes[..block0].iter().sum();
            let row_end = row_start + chunk_sizes[block0];

            // Move the chunk fetch onto the blocking pool — same pattern as
            // `read`, but bounded to the requested row range.
            let me = self.clone();
            let dtype = me.dtype.clone();
            let mut block_shape = me.shape.clone();
            block_shape[0] = row_end - row_start;
            let raw =
                tokio::task::spawn_blocking(move || -> std::result::Result<Vec<u8>, TiledError> {
                    if me.is_time {
                        let values = me.fetch_time_column_range(row_start, row_end)?;
                        Ok(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                    } else if me.is_external {
                        me.fetch_external_column_range(row_start, row_end)
                    } else {
                        let values = me.fetch_inline_column_range(row_start, row_end)?;
                        Ok(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                    }
                })
                .await
                .map_err(|e| TiledError::Internal(format!("blocking read_block: {e}")))??;

            Ok(DynNDArray::new(bytes::Bytes::from(raw), dtype, block_shape))
        })
    }
}

/// Build a chunk-size list for `num_events` along axis 0.
///
/// `[ROW_CHUNK, ROW_CHUNK, …, remainder]`. Empty for `num_events == 0`.
fn row_chunks(num_events: usize) -> Vec<usize> {
    if num_events == 0 {
        return vec![];
    }
    let full = num_events / ROW_CHUNK;
    let rem = num_events % ROW_CHUNK;
    let mut chunks = vec![ROW_CHUNK; full];
    if rem > 0 {
        chunks.push(rem);
    }
    chunks
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
