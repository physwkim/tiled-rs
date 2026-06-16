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

use crate::bson_ext::bson_to_f64;
use crate::filler::Filler;

/// Arguments for [`ArrayColumnAdapter::new_data`].
pub struct DataColumnConfig {
    pub field_name: String,
    pub num_events: usize,
    pub inner_shape: Vec<usize>,
    pub dtype_str: String,
    pub is_external: bool,
    pub filler: Option<Arc<Filler>>,
}

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
    pub fn new_data(db: Database, descriptor_uids: Vec<String>, cfg: DataColumnConfig) -> Self {
        let DataColumnConfig {
            field_name,
            num_events,
            inner_shape,
            dtype_str,
            is_external,
            filler,
        } = cfg;

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

    /// Fetch raw (seq_num, Bson-value) pairs in `[seq_start, seq_end_excl)`.
    /// Returns the value field as an owned `Bson` so callers can encode it
    /// to any target dtype without a second trip to MongoDB.
    fn fetch_seq_bson_pairs(
        &self,
        project: &Document,
        push_path: &str,
        seq_start: i64,
        seq_end_excl: i64,
    ) -> Result<Vec<(i64, Bson)>> {
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
            let seq = doc
                .get_i64("seq_num")
                .or_else(|_| doc.get_i32("seq_num").map(i64::from))
                .unwrap_or(0);
            if seq >= 1 {
                let value = doc.get("value").cloned().unwrap_or(Bson::Null);
                out.push((seq, value));
            }
        }
        Ok(out)
    }

    /// Scatter (seq_num, f64) pairs into a NaN-filled column.
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

    /// Scatter (seq_num, Bson) pairs into a raw byte buffer sized for `len`
    /// elements of `dtype`. Missing seq_nums leave the pre-filled default
    /// (NaN for Float, zero bytes for Boolean / Unicode / other).
    fn scatter_bson_to_bytes(
        pairs: Vec<(i64, Bson)>,
        seq_offset: i64,
        len: usize,
        dtype: &BuiltinDType,
    ) -> Vec<u8> {
        let itemsize = dtype.itemsize;
        let mut buf = vec![0u8; len * itemsize];
        if dtype.kind == Kind::Float && itemsize == 8 {
            let nan = f64::NAN.to_le_bytes();
            for chunk in buf.chunks_exact_mut(8) {
                chunk.copy_from_slice(&nan);
            }
        }
        for (seq, value) in pairs {
            let idx = seq - seq_offset - 1;
            if idx >= 0 && (idx as usize) < len {
                let off = (idx as usize) * itemsize;
                encode_bson_into(value, dtype.kind, itemsize, &mut buf[off..off + itemsize]);
            }
        }
        buf
    }

    fn fetch_time_column(&self) -> Result<Vec<f64>> {
        self.fetch_time_column_range(0, self.num_events)
    }

    fn fetch_time_column_range(&self, row_start: usize, row_end: usize) -> Result<Vec<f64>> {
        let project = doc! {"descriptor": 1, "seq_num": 1, "time": 1};
        let bson_pairs = self.fetch_seq_bson_pairs(
            &project,
            "$doc.time",
            (row_start as i64) + 1,
            (row_end as i64) + 1,
        )?;
        let f64_pairs: Vec<(i64, f64)> = bson_pairs
            .into_iter()
            .map(|(s, v)| (s, bson_to_f64(&v).unwrap_or(f64::NAN)))
            .collect();
        Ok(Self::scatter_pairs(
            f64_pairs,
            row_start as i64,
            row_end - row_start,
        ))
    }

    fn fetch_inline_column(&self) -> Result<Vec<u8>> {
        self.fetch_inline_column_range(0, self.num_events)
    }

    fn fetch_inline_column_range(&self, row_start: usize, row_end: usize) -> Result<Vec<u8>> {
        let field_path = format!("data.{}", self.field_name);
        let push_path = format!("$doc.data.{}", self.field_name);
        let project = doc! {
            "descriptor": 1,
            "seq_num": 1,
            "time": 1,
            &field_path: 1,
        };
        let pairs = self.fetch_seq_bson_pairs(
            &project,
            &push_path,
            (row_start as i64) + 1,
            (row_end as i64) + 1,
        )?;
        Ok(Self::scatter_bson_to_bytes(
            pairs,
            row_start as i64,
            row_end - row_start,
            &self.dtype,
        ))
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

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            // Move a clone of self onto the blocking thread pool so the
            // synchronous MongoDB queries don't pin an async worker thread.
            let me = self.clone();
            let dtype = me.dtype.clone();
            let shape = me.shape.clone();
            let raw =
                tokio::task::spawn_blocking(move || -> std::result::Result<Vec<u8>, TiledError> {
                    if me.is_time {
                        let values = me.fetch_time_column()?;
                        Ok(values.iter().flat_map(|v| v.to_le_bytes()).collect())
                    } else if me.is_external {
                        me.fetch_external_column()
                    } else {
                        me.fetch_inline_column()
                    }
                })
                .await
                .map_err(|e| TiledError::Internal(format!("blocking read: {e}")))??;

            // Sub-slice the assembled column. Every other array adapter applies
            // the requested NDSlice here (sequence_adapter.rs:211,
            // zarr_adapter.rs:130, tiff_adapter.rs:147); dropping it made
            // `?slice=` silently return the full array on Mongo-backed nodes.
            DynNDArray::new(bytes::Bytes::from(raw), dtype, shape).apply_slice(slice)
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
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
                        me.fetch_inline_column_range(row_start, row_end)
                    }
                })
                .await
                .map_err(|e| TiledError::Internal(format!("blocking read_block: {e}")))??;

            // Sub-slice within the block, block-relative — same contract as
            // sequence_adapter.rs:276. Previously the slice was dropped.
            DynNDArray::new(bytes::Bytes::from(raw), dtype, block_shape).apply_slice(slice)
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

/// Write one BSON value into `slot` according to `kind` and `itemsize`.
/// Slot bytes not written by this function keep their pre-filled default
/// (caller is responsible for initializing the buffer, e.g. NaN or zeros).
fn encode_bson_into(value: Bson, kind: Kind, itemsize: usize, slot: &mut [u8]) {
    use crate::bson_ext::bson_to_f64;
    match kind {
        Kind::Float if itemsize == 8 => {
            let f = bson_to_f64(&value).unwrap_or(f64::NAN);
            slot.copy_from_slice(&f.to_le_bytes());
        }
        Kind::Boolean if itemsize == 1 => {
            slot[0] = match value {
                Bson::Boolean(b) => b as u8,
                Bson::Int32(n) => u8::from(n != 0),
                Bson::Int64(n) => u8::from(n != 0),
                _ => 0,
            };
        }
        Kind::Unicode => {
            // Numpy '<U10' layout: each Unicode code point is 4 bytes LE,
            // max_chars = itemsize / 4.  Remaining bytes stay zero (null pad).
            if let Bson::String(s) = value {
                let max_chars = itemsize / 4;
                for (i, c) in s.chars().take(max_chars).enumerate() {
                    let bytes = (c as u32).to_le_bytes();
                    slot[i * 4..i * 4 + 4].copy_from_slice(&bytes);
                }
            }
        }
        _ => {} // leave slot at its pre-filled default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::Bson;

    // ---- Finding 2 regressions ----

    /// Inline Int32 values must not become NaN.
    #[test]
    fn int32_inline_value_is_not_nan() {
        assert!(bson_to_f64(&Bson::Int32(42)).unwrap().is_finite());
        assert_eq!(bson_to_f64(&Bson::Int32(0)), Some(0.0));
        assert_eq!(bson_to_f64(&Bson::Int32(-7)), Some(-7.0));
    }

    #[test]
    fn int64_inline_value_is_not_nan() {
        assert!(bson_to_f64(&Bson::Int64(1_000_000)).unwrap().is_finite());
    }

    // ---- H4: dtype-correct byte width ----

    fn float_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Float, 8)
    }
    fn bool_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1)
    }
    fn unicode_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Unicode, 40)
    }

    #[test]
    fn scatter_float_int32_emits_8_bytes_not_nan() {
        let dtype = float_dtype();
        let pairs = vec![(1i64, Bson::Int32(99))];
        let bytes = ArrayColumnAdapter::scatter_bson_to_bytes(pairs, 0, 1, &dtype);
        assert_eq!(bytes.len(), 8);
        let f = f64::from_le_bytes(bytes.try_into().unwrap());
        assert_eq!(f, 99.0);
    }

    #[test]
    fn scatter_float_missing_slot_is_nan() {
        let dtype = float_dtype();
        let pairs: Vec<(i64, Bson)> = vec![];
        let bytes = ArrayColumnAdapter::scatter_bson_to_bytes(pairs, 0, 1, &dtype);
        assert_eq!(bytes.len(), 8);
        let f = f64::from_le_bytes(bytes.try_into().unwrap());
        assert!(f.is_nan());
    }

    #[test]
    fn scatter_boolean_emits_one_byte_per_event() {
        let dtype = bool_dtype();
        let pairs = vec![(1i64, Bson::Boolean(true)), (3i64, Bson::Boolean(false))];
        let bytes = ArrayColumnAdapter::scatter_bson_to_bytes(pairs, 0, 4, &dtype);
        assert_eq!(bytes.len(), 4, "4 events × 1 byte");
        assert_eq!(bytes[0], 1, "seq 1 → true");
        assert_eq!(bytes[1], 0, "seq 2 → missing → 0");
        assert_eq!(bytes[2], 0, "seq 3 → false");
        assert_eq!(bytes[3], 0, "seq 4 → missing → 0");
    }

    #[test]
    fn scatter_string_emits_40_bytes_per_event_utf32le() {
        let dtype = unicode_dtype();
        let pairs = vec![(1i64, Bson::String("AB".to_string()))];
        let bytes = ArrayColumnAdapter::scatter_bson_to_bytes(pairs, 0, 2, &dtype);
        assert_eq!(bytes.len(), 80, "2 events × 40 bytes");
        // 'A' (U+0041) in UTF-32-LE
        assert_eq!(&bytes[0..4], &[0x41, 0x00, 0x00, 0x00]);
        // 'B' (U+0042) in UTF-32-LE
        assert_eq!(&bytes[4..8], &[0x42, 0x00, 0x00, 0x00]);
        // rest of first event: zero-padded
        assert_eq!(&bytes[8..40], &[0u8; 32]);
        // second event missing → all zeros
        assert_eq!(&bytes[40..80], &[0u8; 40]);
    }

    #[test]
    fn guess_dtype_byte_widths() {
        assert_eq!(guess_dtype("number").itemsize, 8);
        assert_eq!(guess_dtype("integer").itemsize, 8);
        assert_eq!(guess_dtype("boolean").itemsize, 1);
        assert_eq!(guess_dtype("string").itemsize, 40);
        assert_eq!(guess_dtype("array").itemsize, 8);
    }

    /// Regression: `read`/`read_block` must apply the requested `NDSlice` to the
    /// assembled column — every other array adapter does, and dropping it made
    /// `?slice=` silently return the full array on Mongo-backed nodes. This
    /// exercises the exact post-fetch composition `read` performs (a column
    /// assembled by `scatter_bson_to_bytes`, wrapped as a `DynNDArray`, then
    /// sub-sliced). The Mongo fetch half is not reachable in unit tests, so the
    /// end-to-end `read()` path needs a live-Mongo integration harness the crate
    /// does not have; this guards the assembled-column↔slice contract.
    #[test]
    fn assembled_column_apply_slice_selects_rows() {
        let dtype = float_dtype();
        // Five events (seq 1..=5) → values 10,20,30,40,50.
        let pairs = vec![
            (1i64, Bson::Double(10.0)),
            (2, Bson::Double(20.0)),
            (3, Bson::Double(30.0)),
            (4, Bson::Double(40.0)),
            (5, Bson::Double(50.0)),
        ];
        let bytes = ArrayColumnAdapter::scatter_bson_to_bytes(pairs, 0, 5, &dtype);
        let full = DynNDArray::new(bytes::Bytes::from(bytes), dtype, vec![5]);

        // A `1:4` row slice must select events 2,3,4 and report shape [3].
        let sliced = full
            .apply_slice(&NDSlice::from_numpy_str("1:4").unwrap())
            .unwrap();
        assert_eq!(sliced.shape, vec![3]);
        let got: Vec<f64> = sliced
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(got, vec![20.0, 30.0, 40.0]);
    }
}
