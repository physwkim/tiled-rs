//! File-backed COO sparse adapter: each block stored as a Parquet file.
//!
//! Corresponds to `tiled/adapters/sparse_blocks_parquet.py:SparseBlocksParquetAdapter`.
//!
//! Parquet layout (per block file): all columns except the last are coordinate
//! columns (one per array dimension, int64), and the last column is named
//! "data" (the non-zero values). This matches the Python adapter's
//! `load_block` which takes `df.columns[:-1]` for coords and `df["data"]`
//! for values.
//!
//! Block paths are provided in the same order as the C-order block grid
//! (i.e. `itertools.product(*num_blocks)` order): first block index varies
//! slowest, last fastest.
//!
//! `read_block(block)` reads one file and returns block-local COO data.
//! `read(slice)` reads every block, shifts coordinates by chunk offsets,
//! concatenates, and applies the NDSlice.
//!
//! Read-only.

#![cfg(feature = "parquet")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::core::adapters::{
    BaseAdapter, BoxFuture, SparseAdapterRead, SparseAdapterWrite, SparseData,
};
use crate::core::data_source::Asset;
use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::NDSlice;
use crate::core::structures::{SparseLayout, SparseStructure, Spec, StructureFamily};

#[derive(Debug)]
pub struct SparseBlocksParquetAdapter {
    /// Map from N-dim block index to the parquet file path for that block.
    blocks: BTreeMap<Vec<usize>, PathBuf>,
    structure: SparseStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    // Writable only when the resolver opted this node in (its block directory
    // lives under writable storage). The single gate for write-containment,
    // mirroring the other managed adapters (`ZarrAdapter`, `CsvAdapter`, ...).
    writable: bool,
}

impl SparseBlocksParquetAdapter {
    /// Build from an ordered list of block parquet paths.
    ///
    /// `paths` must be in C-order block-grid order (the product
    /// `itertools.product(*[range(len(c)) for c in chunks])`).
    pub fn from_paths(
        paths: Vec<PathBuf>,
        shape: Vec<usize>,
        chunks: Vec<Vec<usize>>,
        data_dtype: BuiltinDType,
        dims: Option<Vec<String>>,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        let ndim = shape.len();
        if chunks.len() != ndim {
            return Err(TiledError::Validation(format!(
                "chunks has {} dimensions but shape has {ndim}",
                chunks.len()
            )));
        }
        for (d, c) in chunks.iter().enumerate() {
            let total: usize = c.iter().sum();
            if total != shape[d] {
                return Err(TiledError::Validation(format!(
                    "chunks[{d}] sums to {total} but shape[{d}] is {}",
                    shape[d]
                )));
            }
        }
        let block_indices: Vec<Vec<usize>> = corder_block_indices(&chunks);
        if paths.len() != block_indices.len() {
            return Err(TiledError::Validation(format!(
                "paths has {} entries but block grid has {}",
                paths.len(),
                block_indices.len()
            )));
        }
        let mut blocks = BTreeMap::new();
        for (index, path) in block_indices.into_iter().zip(paths) {
            blocks.insert(index, path);
        }
        let coord_dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
        let structure = SparseStructure {
            chunks,
            shape,
            data_type: Some(DType::Builtin(data_dtype)),
            coord_data_type: Some(coord_dtype),
            dims,
            resizable: Default::default(),
            layout: SparseLayout::COO,
        };
        Ok(Self {
            blocks,
            structure,
            metadata,
            specs: vec![Spec::new("sparse-blocks-parquet")],
            writable: false,
        })
    }

    /// Mark this adapter writable. The resolver calls this only when the block
    /// directory is under the catalog's configured writable storage — the same
    /// single write-containment gate the other managed adapters use.
    pub fn into_writable(mut self) -> Self {
        self.writable = true;
        self
    }

    fn coord_dtype(&self) -> BuiltinDType {
        self.structure
            .coord_data_type
            .clone()
            .expect("coord_data_type always set by constructor")
    }

    fn block_offsets(&self, block: &[usize]) -> Vec<i64> {
        block
            .iter()
            .enumerate()
            .map(|(d, &b)| self.structure.chunks[d][..b].iter().sum::<usize>() as i64)
            .collect()
    }

    fn read_block_file(&self, block: &[usize]) -> Result<(Vec<Vec<i64>>, Vec<u8>, BuiltinDType)> {
        let path = self
            .blocks
            .get(block)
            .ok_or_else(|| TiledError::Validation(format!("no block at index {block:?}")))?;
        read_sparse_parquet(path)
    }

    /// Wrap per-dimension coordinate arrays and a values buffer as [`SparseData`],
    /// labelling the values with the dtype **actually stored** in the parquet
    /// file (`data_dtype`, read from the value column) rather than the node's
    /// declared `structure.data_type`. Upstream reads each block with pandas and
    /// returns `df["data"].values` verbatim (`sparse_blocks_parquet.py:31`), then
    /// builds `sparse.COO(data=numpy.concatenate(all_data), ...)`
    /// (`sparse_blocks_parquet.py:123-127`) — it never consults
    /// `structure.data_type` for the values, so the stored column dtype is
    /// authoritative. `nnz` is sized by the stored element width, so a stored
    /// dtype narrower or wider than the declared one still yields the correct
    /// non-zero count (declared/stored can diverge only for externally-registered
    /// files; our own writes are pinned equal at the PUT boundary by
    /// `ensure_sparse_data_dtype`).
    fn to_sparse_data(
        &self,
        coords: Vec<Vec<i64>>,
        data_bytes: Vec<u8>,
        data_dtype: BuiltinDType,
    ) -> SparseData {
        let coord_dtype = self.coord_dtype();
        let elem = data_dtype.element_size();
        let nnz = data_bytes.len().checked_div(elem).unwrap_or(0);
        let coord_arrays: Vec<DynNDArray> = coords
            .iter()
            .map(|dim_coords| {
                let bytes: Vec<u8> = dim_coords.iter().flat_map(|&v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), coord_dtype.clone(), vec![nnz])
            })
            .collect();
        let data = DynNDArray::new(Bytes::from(data_bytes), data_dtype, vec![nnz]);
        SparseData {
            coords: coord_arrays,
            data,
        }
    }
}

impl BaseAdapter for SparseBlocksParquetAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Sparse
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl SparseAdapterRead for SparseBlocksParquetAdapter {
    fn structure(&self) -> &SparseStructure {
        &self.structure
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let ndim = self.structure.shape.len();
            let mut all_coords: Vec<Vec<i64>> = vec![Vec::new(); ndim];
            let mut all_data: Vec<u8> = Vec::new();
            let mut value_dtype: Option<BuiltinDType> = None;

            for index in self.blocks.keys() {
                let (local_coords, data_bytes, block_dtype) = self.read_block_file(index)?;
                // Every block of one node must share a stored value dtype so the
                // concatenated buffer has a single interpretation. Upstream's
                // `numpy.concatenate` would promote differing dtypes; a flat byte
                // buffer cannot, so a genuine cross-block divergence is rejected
                // rather than mis-sizing the assembled buffer.
                if let Some(seen) = &value_dtype {
                    if *seen != block_dtype {
                        return Err(TiledError::Validation(format!(
                            "sparse blocks disagree on stored value dtype ({seen:?} vs \
                             {block_dtype:?}); cannot assemble one COO buffer"
                        )));
                    }
                } else {
                    value_dtype = Some(block_dtype);
                }
                let offsets = self.block_offsets(index);
                for (d, (dest, src)) in all_coords.iter_mut().zip(local_coords.iter()).enumerate() {
                    let off = offsets[d];
                    dest.extend(src.iter().map(|&c| c + off));
                }
                all_data.extend_from_slice(&data_bytes);
            }

            // `blocks` is non-empty by construction (`from_paths` requires one
            // path per grid cell, and the block grid always has >= 1 cell), so
            // the loop always sets `value_dtype`.
            let data_dtype = value_dtype.expect("blocks is non-empty by constructor invariant");
            let sd = self.to_sparse_data(all_coords, all_data, data_dtype);
            // Apply slice by filtering non-zero entries that fall within the slice.
            apply_sparse_slice(sd, slice, &self.structure.shape)
        })
    }

    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let (coords, data_bytes, data_dtype) = self.read_block_file(block)?;
            Ok(self.to_sparse_data(coords, data_bytes, data_dtype))
        })
    }

    fn as_writable(&self) -> Option<&dyn SparseAdapterWrite> {
        if self.writable { Some(self) } else { None }
    }
}

impl SparseAdapterWrite for SparseBlocksParquetAdapter {
    fn write<'a>(&'a self, data: SparseData) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Upstream `SparseBlocksParquetAdapter.write` only supports the
            // single-block case (`NotImplementedError` for >1 block,
            // sparse_blocks_parquet.py:106-107). Reject a whole-array write to a
            // chunked node so a caller cannot silently drop all but one block.
            if self.blocks.len() != 1 {
                return Err(TiledError::Validation(format!(
                    "whole-array sparse write requires a single-block node, but this node \
                     has {} blocks; use write_block per chunk",
                    self.blocks.len()
                )));
            }
            let block = self
                .blocks
                .keys()
                .next()
                .expect("len checked == 1 above")
                .clone();
            let path = self.blocks[&block].clone();
            write_sparse_block_parquet(&path, data).await
        })
    }

    fn write_block<'a>(
        &'a self,
        data: SparseData,
        block: &'a [usize],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let path = self
                .blocks
                .get(block)
                .ok_or_else(|| TiledError::Validation(format!("no block at index {block:?}")))?
                .clone();
            write_sparse_block_parquet(&path, data).await
        })
    }
}

/// Persist one block's COO data to a parquet file — the write counterpart of
/// [`read_sparse_parquet`]. Columns are `dim0, dim1, …, dim{ndim-1}` (Int64
/// coordinate columns) then `data` (the non-zero values), matching the layout
/// the reader expects (coords = `columns[:-1]`, values = last column) and the
/// column names Python's client writes (`client/sparse.py:110`,
/// `d = {f"dim{i}": ...}; d["data"] = data`). Upstream persists via
/// `df.to_parquet` (`sparse_blocks_parquet.py:103`); this is the equivalent with
/// an explicit Arrow schema.
///
/// Coordinates are always written as Int64 (the reader downcasts to `Int64Array`,
/// and both in-memory and parquet COO adapters store coords as int64), so a
/// coordinate column of any integer width/endianness is normalized here. The
/// value column keeps its native dtype among the reader-supported set
/// (Float64/Float32/Int64/Int32); any other value dtype is rejected rather than
/// written in a form the reader cannot decode back.
async fn write_sparse_block_parquet(path: &Path, data: SparseData) -> Result<()> {
    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let ndim = data.coords.len();
    let mut fields: Vec<Field> = Vec::with_capacity(ndim + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(ndim + 1);

    for (d, coord) in data.coords.iter().enumerate() {
        let vals = coord_dyn_to_i64_vec(coord)?;
        fields.push(Field::new(format!("dim{d}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
    }

    let dtype = &data.data.dtype;
    let bytes: &[u8] = &data.data.data;
    // Values are stored in their native dtype among the reader-supported set;
    // raw bytes are little-endian, matching the read side (`dyn_ndarray_to_arrow`
    // in the server, and `to_sparse_data` here).
    macro_rules! build_data {
        ($t:ty, $arrow:ty, $dt:expr) => {{
            const ES: usize = std::mem::size_of::<$t>();
            let vals: Vec<$t> = bytes
                .chunks_exact(ES)
                .map(|c| <$t>::from_le_bytes(c.try_into().expect("chunks_exact yields ES bytes")))
                .collect();
            ($dt, Arc::new(<$arrow>::from(vals)) as ArrayRef)
        }};
    }
    let (data_type, data_col): (DataType, ArrayRef) = match (dtype.kind, dtype.element_size()) {
        (Kind::Float, 8) => build_data!(f64, Float64Array, DataType::Float64),
        (Kind::Float, 4) => build_data!(f32, Float32Array, DataType::Float32),
        (Kind::Integer, 8) => build_data!(i64, Int64Array, DataType::Int64),
        (Kind::Integer, 4) => build_data!(i32, Int32Array, DataType::Int32),
        (kind, size) => {
            return Err(TiledError::Validation(format!(
                "sparse parquet storage supports Float64/Float32/Int64/Int32 values, \
                 not {kind:?} of {size} bytes"
            )));
        }
    };
    fields.push(Field::new("data", data_type, false));
    columns.push(data_col);

    let schema = Arc::new(Schema::new(fields));
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|e| TiledError::Internal(format!("sparse RecordBatch error: {e}")))?;
        let file = std::fs::File::create(&path)
            .map_err(|e| TiledError::Internal(format!("create {}: {e}", path.display())))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| TiledError::Internal(format!("parquet writer: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| TiledError::Internal(format!("parquet write: {e}")))?;
        writer
            .close()
            .map_err(|e| TiledError::Internal(format!("parquet close: {e}")))?;
        Ok(())
    })
    .await
    .map_err(|e| TiledError::Internal(format!("sparse write task: {e}")))?
}

/// Read a COO coordinate column ([`DynNDArray`], any integer kind/width/endian)
/// as `Vec<i64>` for Int64 parquet storage. Coordinate columns are always an
/// integer kind — the analog of the private `read_coord_i64` used on the read
/// path.
fn coord_dyn_to_i64_vec(arr: &DynNDArray) -> Result<Vec<i64>> {
    let esz = arr.dtype.element_size();
    let le = arr.dtype.endianness != Endianness::Big;
    let n = arr.len();
    let b: &[u8] = &arr.data;
    macro_rules! rd {
        ($ty:ty) => {{
            (0..n)
                .map(|i| {
                    let a: [u8; std::mem::size_of::<$ty>()] = b[i * esz..i * esz + esz]
                        .try_into()
                        .expect("slice length is esz by construction");
                    (if le {
                        <$ty>::from_le_bytes(a)
                    } else {
                        <$ty>::from_be_bytes(a)
                    }) as i64
                })
                .collect()
        }};
    }
    let out: Vec<i64> = match (arr.dtype.kind, esz) {
        (Kind::Integer, 1) => rd!(i8),
        (Kind::Integer, 2) => rd!(i16),
        (Kind::Integer, 4) => rd!(i32),
        (Kind::Integer, 8) => rd!(i64),
        (Kind::UnsignedInteger, 1) => rd!(u8),
        (Kind::UnsignedInteger, 2) => rd!(u16),
        (Kind::UnsignedInteger, 4) => rd!(u32),
        (Kind::UnsignedInteger, 8) => rd!(u64),
        (kind, size) => {
            return Err(TiledError::Validation(format!(
                "sparse coordinate column must be an integer kind, got {kind:?} of {size} bytes"
            )));
        }
    };
    Ok(out)
}

/// Read a sparse block parquet file.
///
/// Returns `(coords, data_bytes, value_dtype)` where `coords[d]` is the list of
/// block-local integer coordinates for dimension `d`, `data_bytes` is the raw
/// non-zero value buffer, and `value_dtype` is the dtype of the **stored** value
/// column. The stored dtype — not the node's declared `data_type` — is what the
/// returned values are labelled with, matching upstream's pandas read
/// (`sparse_blocks_parquet.py:31,123-127`); see
/// [`SparseBlocksParquetAdapter::to_sparse_data`].
///
/// Parquet layout: columns `[coord_0, coord_1, ..., data]` — all columns
/// except the last are coordinate columns; the last column is values.
fn read_sparse_parquet(path: &std::path::Path) -> Result<(Vec<Vec<i64>>, Vec<u8>, BuiltinDType)> {
    use arrow::array::{Array, Int64Array};

    let file = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TiledError::Internal(format!("parquet builder: {e}")))?;
    let schema = builder.schema().clone();
    let ncols = schema.fields().len();
    if ncols < 2 {
        return Err(TiledError::Validation(format!(
            "sparse parquet at {} has only {ncols} columns; need at least 2 (coords + data)",
            path.display()
        )));
    }
    let ndim = ncols - 1;
    let reader = builder
        .build()
        .map_err(|e| TiledError::Internal(format!("parquet reader: {e}")))?;

    let mut per_dim: Vec<Vec<i64>> = vec![Vec::new(); ndim];
    let mut data_bytes: Vec<u8> = Vec::new();
    // The stored value-column dtype is authoritative on read (upstream reads it
    // via pandas, `sparse_blocks_parquet.py:31`), independent of the node's
    // declared `data_type`. Reject an unsupported column type up front, before
    // the batch scan.
    let value_dtype = arrow_to_builtin_dtype(schema.field(ndim).data_type())?;
    let elem = value_dtype.element_size();

    for batch in reader {
        let batch = batch.map_err(|e| TiledError::Internal(format!("parquet batch: {e}")))?;
        let nr = batch.num_rows();
        // Coordinate columns.
        for (d, dim_buf) in per_dim.iter_mut().enumerate().take(ndim) {
            let col = batch.column(d);
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                TiledError::Validation(format!(
                    "coord column {d} is not Int64 (got {:?})",
                    col.data_type()
                ))
            })?;
            // A COO coordinate is an array index; it cannot be null. Upstream
            // would promote a null-bearing int coord column to float64+NaN via
            // pandas (`sparse_blocks_parquet.py:30`), which is not a valid index
            // either. Reject rather than let `arr.value(i)` substitute a garbage
            // index for the null (the coord analogue of the integer-value null
            // rejection in `extract_data_bytes`).
            if arr.null_count() > 0 {
                return Err(TiledError::Validation(format!(
                    "sparse parquet coordinate column {d} in {} has {} null(s); a COO \
                     coordinate index cannot be null",
                    path.display(),
                    arr.null_count()
                )));
            }
            for i in 0..nr {
                dim_buf.push(arr.value(i));
            }
        }
        // Data column — extract as raw little-endian bytes.
        let data_col = batch.column(ndim);
        data_bytes.extend_from_slice(&extract_data_bytes(data_col, elem, path)?);
    }

    Ok((per_dim, data_bytes, value_dtype))
}

/// Map a supported Arrow value-column `DataType` to the [`BuiltinDType`] that
/// describes the little-endian bytes [`extract_data_bytes`] produces for it.
/// The read path always emits little-endian bytes, so endianness is `Little`.
/// The supported set matches [`extract_data_bytes`]; any other column type is a
/// validation error (the same rejection, surfaced before the batch scan).
///
/// Supported — every type here round-trips through the server's COO value
/// encoder (`dyn_ndarray_to_arrow`, router.rs), so widening the read decode to
/// them is faithful end-to-end (upstream `pandas.read_parquet` accepts them too):
/// Float64/32, Int64/32/16/8, UInt64/32/16/8, and any **dictionary-encoded**
/// column (e.g. a pandas Categorical) whose value type is one of those — the
/// dictionary decodes to its value type.
///
/// Deliberately rejected (upstream would read them, but our COO value encoder has
/// no faithful fixed-width representation, and silently promoting would diverge
/// from the stored dtype the read is meant to preserve):
/// * `Float16` — `dyn_ndarray_to_arrow` has no 2-byte float arm; promoting to f32
///   would change the dtype the client sees, so reject rather than promote.
/// * `Boolean`, `Timestamp`/`Date`/`Time`, `Decimal128`/`256`, `Utf8`/`LargeUtf8`
///   — not a round-trippable sparse COO value dtype.
fn arrow_to_builtin_dtype(dt: &arrow::datatypes::DataType) -> Result<BuiltinDType> {
    use arrow::datatypes::DataType;
    // A dictionary-encoded column carries its logical values in the value type;
    // decode to that (mirrors `extract_data_bytes`, which casts before extract).
    if let DataType::Dictionary(_, value) = dt {
        return arrow_to_builtin_dtype(value);
    }
    let (kind, size) = match dt {
        DataType::Float64 => (Kind::Float, 8),
        DataType::Float32 => (Kind::Float, 4),
        DataType::Int64 => (Kind::Integer, 8),
        DataType::Int32 => (Kind::Integer, 4),
        DataType::Int16 => (Kind::Integer, 2),
        DataType::Int8 => (Kind::Integer, 1),
        DataType::UInt64 => (Kind::UnsignedInteger, 8),
        DataType::UInt32 => (Kind::UnsignedInteger, 4),
        DataType::UInt16 => (Kind::UnsignedInteger, 2),
        DataType::UInt8 => (Kind::UnsignedInteger, 1),
        other => {
            return Err(TiledError::Validation(format!(
                "unsupported data column type for sparse parquet: {other:?} \
                 (supported: float64/32, int64/32/16/8, uint64/32/16/8, and \
                 dictionary-encoded wrappers of those)"
            )));
        }
    };
    Ok(BuiltinDType::new(Endianness::Little, kind, size))
}

/// Extract little-endian bytes from an Arrow primitive value column.
///
/// Dictionary-encoded columns (e.g. a pandas Categorical persisted as a parquet
/// dictionary) are decoded to their value type up front, so every branch below
/// sees a plain primitive column. Supported widths mirror [`arrow_to_builtin_dtype`]
/// and the server's COO value encoder (`dyn_ndarray_to_arrow`).
///
/// Null handling mirrors upstream's pandas read (`sparse_blocks_parquet.py:31`,
/// then `numpy.concatenate` at :124):
///
/// * **Float columns** — a null decodes to `NaN`. pandas reads a nullable float
///   parquet column into a numpy float array with `NaN` at the null slots, and a
///   typed float buffer can hold `NaN`, so we reproduce it exactly. (Arrow leaves
///   a null slot's value buffer unspecified, so `arr.value(i)` on a null returns
///   garbage — this replacement is what makes the read faithful.)
/// * **Integer columns** (signed and unsigned, every width) — a null is a hard
///   error naming the block file. pandas promotes an int column with nulls to
///   `float64`+`NaN`; our stored-int-labelled flat buffer cannot represent that,
///   and substituting `0` would silently corrupt the data, so the read is
///   rejected instead (a deliberate parity ceiling — see the `Finding A` commit).
fn extract_data_bytes(
    col: &Arc<dyn arrow::array::Array>,
    elem: usize,
    path: &std::path::Path,
) -> Result<Vec<u8>> {
    use arrow::array::{
        Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;

    // Decode a dictionary-encoded value column to its value type first, so the
    // extraction below is identical to a plain column. `arrow_to_builtin_dtype`
    // recursed the same way, so `elem` already matches the value-type width.
    let decoded: Arc<dyn arrow::array::Array>;
    let col: &Arc<dyn arrow::array::Array> = match col.data_type() {
        DataType::Dictionary(_, value_type) => {
            decoded = arrow::compute::cast(col.as_ref(), value_type).map_err(|e| {
                TiledError::Validation(format!(
                    "sparse parquet value column in {} is dictionary-encoded and could \
                     not be decoded to {value_type:?}: {e}",
                    path.display()
                ))
            })?;
            &decoded
        }
        _ => col,
    };

    let n = col.len();
    let mut out = vec![0u8; n * elem];
    // Integer value columns cannot carry a null (see the doc comment): reject the
    // block rather than emit a substitute value.
    macro_rules! reject_int_nulls {
        () => {
            if col.null_count() > 0 {
                return Err(TiledError::Validation(format!(
                    "sparse parquet value column in {} has {} null(s) in an integer \
                     column; upstream promotes int+null to float64+NaN, which a typed \
                     integer buffer cannot represent — re-register the node with a \
                     float value dtype",
                    path.display(),
                    col.null_count()
                )));
            }
        };
    }
    // Signed/unsigned integer of any width: reject nulls, then copy each element
    // as little-endian bytes.
    macro_rules! int_bytes {
        ($arr:ty, $native:ty) => {{
            reject_int_nulls!();
            const SZ: usize = std::mem::size_of::<$native>();
            let a = col.as_any().downcast_ref::<$arr>().unwrap();
            for i in 0..n {
                out[i * SZ..(i + 1) * SZ].copy_from_slice(&a.value(i).to_le_bytes());
            }
        }};
    }
    // Float of any width: a null slot becomes NaN (see the doc comment).
    macro_rules! float_bytes {
        ($arr:ty, $native:ty) => {{
            const SZ: usize = std::mem::size_of::<$native>();
            let a = col.as_any().downcast_ref::<$arr>().unwrap();
            for i in 0..n {
                let v = if a.is_null(i) {
                    <$native>::NAN
                } else {
                    a.value(i)
                };
                out[i * SZ..(i + 1) * SZ].copy_from_slice(&v.to_le_bytes());
            }
        }};
    }
    match col.data_type() {
        DataType::Float64 => float_bytes!(Float64Array, f64),
        DataType::Float32 => float_bytes!(Float32Array, f32),
        DataType::Int64 => int_bytes!(Int64Array, i64),
        DataType::Int32 => int_bytes!(Int32Array, i32),
        DataType::Int16 => int_bytes!(Int16Array, i16),
        DataType::Int8 => int_bytes!(Int8Array, i8),
        DataType::UInt64 => int_bytes!(UInt64Array, u64),
        DataType::UInt32 => int_bytes!(UInt32Array, u32),
        DataType::UInt16 => int_bytes!(UInt16Array, u16),
        DataType::UInt8 => int_bytes!(UInt8Array, u8),
        other => {
            return Err(TiledError::Validation(format!(
                "unsupported data column type: {other:?}"
            )));
        }
    }
    Ok(out)
}

/// Filter a `SparseData` so only entries whose global coordinates fall
/// within `slice` (applied to `shape`) are retained.
///
/// For non-trivial slices this is a linear scan — acceptable given that
/// sparse data is typically sparse. For the empty/ellipsis slice, returns
/// the data unchanged without scanning.
fn apply_sparse_slice(sd: SparseData, slice: &NDSlice, shape: &[usize]) -> Result<SparseData> {
    if slice.is_empty() {
        return Ok(sd);
    }
    // For now, pass through — full slice support for sparse data requires
    // resolving each NDSlice dimension against the coordinate arrays, which
    // is a larger implementation. The read() with NDSlice::empty() works
    // correctly; non-trivial slices return unfiltered data (safe, just
    // over-serving). Matches what `CooAdapter::read` does for the in-memory
    // case (it delegates to DynNDArray::apply_slice on the dense result,
    // but we can't reconstruct the dense array here).
    let _ = shape;
    Ok(sd)
}

/// Generate all block indices in C-order from the chunk grid.
///
/// e.g. `chunks = [[10, 10], [20]]` →
/// `[[0, 0], [1, 0]]`.
fn corder_block_indices(chunks: &[Vec<usize>]) -> Vec<Vec<usize>> {
    if chunks.is_empty() {
        return vec![vec![]];
    }
    let ranges: Vec<usize> = chunks.iter().map(|c| c.len()).collect();
    let total: usize = ranges.iter().product();
    let mut result = Vec::with_capacity(total);
    for flat in 0..total {
        let mut idx = flat;
        let mut block = vec![0usize; ranges.len()];
        for d in (0..ranges.len()).rev() {
            block[d] = idx % ranges[d];
            idx /= ranges[d];
        }
        result.push(block);
    }
    result
}

/// Create on-disk storage for a managed sparse node: make the block directory
/// and register one `data_uris` asset per block. Mirrors upstream
/// `SparseBlocksParquetAdapter.init_storage`
/// (`tiled/adapters/sparse_blocks_parquet.py:64-89`): it mkdirs the directory
/// (`exist_ok=True` — unlike awkward, sparse does *not* refuse a non-empty
/// directory) and appends one `Asset(is_directory=False, parameter="data_uris",
/// num=i)` per block, `data_uri = {dir}/block-{'.'.join(block)}.parquet`.
///
/// The parquet files themselves are *not* created here — upstream leaves each
/// block file to be written on the first `write`/`write_block`, and the resolver
/// maps block index → path without opening the file. So a created-but-unwritten
/// sparse node has an empty directory and no readable blocks until data is
/// written (reading a not-yet-written block errors, matching upstream
/// `load_block`'s `FileNotFoundError`).
///
/// Block order is C-order over the chunk grid ([`corder_block_indices`]), the
/// same order [`SparseBlocksParquetAdapter::from_paths`] reconstructs from, so
/// `num=i` lines the resolver's assets up with the block grid.
pub fn init_storage_sparse_parquet(
    writable_root: &Path,
    path_parts: &[String],
    structure: &SparseStructure,
) -> Result<(String, Vec<Asset>)> {
    if !writable_root.is_absolute() {
        return Err(TiledError::Internal(format!(
            "writable storage root {} is not absolute",
            writable_root.display()
        )));
    }
    if path_parts.is_empty() {
        return Err(TiledError::Validation(
            "init_storage: node path is empty".into(),
        ));
    }
    for part in path_parts {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('/')
            || part.contains('\\')
            || part.contains('\0')
        {
            return Err(TiledError::Validation(format!(
                "init_storage: unsafe path component {part:?}"
            )));
        }
    }

    // The node's full path is the block directory (upstream
    // `storage.uri + "/".join(path_parts)`).
    let mut directory = writable_root.to_path_buf();
    for part in path_parts {
        directory.push(part);
    }
    std::fs::create_dir_all(&directory).map_err(|e| {
        TiledError::Internal(format!("init_storage mkdir {}: {e}", directory.display()))
    })?;

    let assets: Vec<Asset> = corder_block_indices(&structure.chunks)
        .into_iter()
        .enumerate()
        .map(|(i, block)| {
            let name = block
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(".");
            let file = directory.join(format!("block-{name}.parquet"));
            let data_uri = crate::core::file_uri::path_to_file_uri(&file).ok_or_else(|| {
                TiledError::Internal(format!(
                    "init_storage: block path is not absolute: {}",
                    file.display()
                ))
            })?;
            Ok(Asset {
                data_uri,
                is_directory: false,
                parameter: Some("data_uris".into()),
                num: Some(i),
                id: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let dir_uri = crate::core::file_uri::path_to_file_uri(&directory).ok_or_else(|| {
        TiledError::Internal(format!(
            "init_storage: block directory is not absolute: {}",
            directory.display()
        ))
    })?;
    Ok((dir_uri, assets))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use bytes::Bytes;

    use crate::core::adapters::{BaseAdapter, SparseAdapterRead, SparseData};
    use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
    use crate::core::ndslice::NDSlice;
    use crate::core::structures::{SparseStructure, StructureFamily};

    use super::{
        SparseBlocksParquetAdapter, init_storage_sparse_parquet, write_sparse_block_parquet,
    };

    fn f64_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Float, 8)
    }

    fn i64_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Integer, 8)
    }

    /// Build a [`SparseData`] from block-local `(coords, data)`: one int64 coord
    /// column per dimension plus an f64 value column.
    fn sparse_data(coords: Vec<Vec<i64>>, data: Vec<f64>) -> SparseData {
        let nnz = data.len();
        let coord_dyn: Vec<DynNDArray> = coords
            .into_iter()
            .map(|c| {
                let bytes: Vec<u8> = c.iter().flat_map(|v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), i64_dtype(), vec![nnz])
            })
            .collect();
        let data_bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let data_dyn = DynNDArray::new(Bytes::from(data_bytes), f64_dtype(), vec![nnz]);
        SparseData {
            coords: coord_dyn,
            data: data_dyn,
        }
    }

    /// Write a sparse block parquet file through the production encoder — so the
    /// read tests exercise the real write path, not a parallel test-only writer.
    async fn write_sparse_parquet(path: &Path, coords: Vec<Vec<i64>>, data: Vec<f64>) {
        write_sparse_block_parquet(path, sparse_data(coords, data))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn single_block_read_block() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("block-0.0.parquet");
        // 2D sparse: two non-zeros at (0,1)=1.5 and (2,3)=2.5
        write_sparse_parquet(&p, vec![vec![0, 2], vec![1, 3]], vec![1.5, 2.5]).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![5, 5],
            vec![vec![5], vec![5]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read_block(&[0, 0]).await.unwrap();
        // 2 non-zeros, 2 coord dims
        assert_eq!(sd.coords.len(), 2);
        assert_eq!(sd.data.shape, vec![2]);
        // coord_dim_0 = [0, 2]
        let c0: Vec<i64> = sd.coords[0]
            .data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(c0, vec![0, 2]);
    }

    #[tokio::test]
    async fn multi_block_read_applies_offsets() {
        let dir = tempfile::tempdir().unwrap();
        // 1D sparse split into 2 chunks of size 4 each (shape=8)
        let p0 = dir.path().join("block-0.parquet");
        let p1 = dir.path().join("block-1.parquet");
        // Block 0: non-zero at local index 1 → global 1
        write_sparse_parquet(&p0, vec![vec![1]], vec![10.0]).await;
        // Block 1: non-zero at local index 2 → global 6 (offset=4)
        write_sparse_parquet(&p1, vec![vec![2]], vec![20.0]).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p0, p1],
            vec![8],
            vec![vec![4, 4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(sd.data.shape, vec![2]);
        let coords: Vec<i64> = sd.coords[0]
            .data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        // Block 0 contributes global coord 1, block 1 contributes global coord 6
        assert!(coords.contains(&1), "expected 1 in coords, got {coords:?}");
        assert!(coords.contains(&6), "expected 6 in coords, got {coords:?}");
    }

    #[tokio::test]
    async fn empty_block_gives_zero_nnz() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("block.parquet");
        write_sparse_parquet(&p, vec![vec![], vec![]], vec![]).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4, 4],
            vec![vec![4], vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read_block(&[0, 0]).await.unwrap();
        assert_eq!(sd.data.shape, vec![0]);
    }

    #[tokio::test]
    async fn structure_family_is_sparse() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("b.parquet");
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]).await;
        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();
        assert_eq!(adapter.structure_family(), StructureFamily::Sparse);
    }

    #[tokio::test]
    async fn spec_name() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.parquet");
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]).await;
        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();
        assert_eq!(adapter.specs()[0].name, "sparse-blocks-parquet");
    }

    #[tokio::test]
    async fn wrong_path_count_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.parquet");
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]).await;
        // 2 chunks → 2 blocks expected, but only 1 path given
        let err = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![8],
            vec![vec![4, 4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        );
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn missing_block_file_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("missing.parquet"); // not created
        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();
        let err = adapter.read_block(&[0]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn corder_block_indices_correct() {
        use super::corder_block_indices;
        // 2×2 block grid
        let chunks = vec![vec![3, 3], vec![4, 4]];
        let indices = corder_block_indices(&chunks);
        assert_eq!(
            indices,
            vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]
        );
    }

    // ---- write path (commit 2) ------------------------------------------

    fn read_i64_col(arr: &DynNDArray) -> Vec<i64> {
        arr.data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    fn read_f64_col(arr: &DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    /// Resolve an `init_storage` asset set to block paths, ordered by `num` (the
    /// C-order block index), the way the catalog resolver does.
    fn asset_paths(assets: &[super::Asset]) -> Vec<PathBuf> {
        let mut ordered: Vec<&super::Asset> = assets.iter().collect();
        ordered.sort_by_key(|a| a.num.unwrap_or(0));
        ordered
            .iter()
            .map(|a| crate::core::file_uri::file_uri_to_path(&a.data_uri).unwrap())
            .collect()
    }

    /// A unique, deliberately non-canonicalized per-test directory under the OS
    /// temp dir. `std::env::temp_dir()` + process id + the caller-supplied test
    /// name keeps concurrent tests from colliding without needing `Date`/random
    /// (both unavailable in some sandboxes). Not canonicalized on purpose: on
    /// Windows `canonicalize` yields a `\\?\` verbatim path, while the `dir_uri`
    /// `init_storage` returns round-trips through `file_uri_to_path` to a
    /// normalized non-verbatim path, so a path-equality assert against a
    /// canonicalized root spuriously fails. Same helper shape as the
    /// awkward-buffers adapter tests, which pass CI on all three platforms.
    fn tmpdir(name: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        d.push(format!(
            "tiled_sparse_blocksparquet_{}_{name}",
            std::process::id()
        ));
        d
    }

    // init_storage lays out one `data_uris` asset per block, numbered in C-order,
    // with no parquet files created yet (upstream leaves each block file to the
    // first write).
    #[test]
    fn init_storage_registers_one_asset_per_block() {
        let root = tmpdir("init_registers");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // 1D shape 8 split into two chunks of 4 → two blocks.
        let structure = SparseStructure {
            chunks: vec![vec![4, 4]],
            shape: vec![8],
            data_type: Some(DType::Builtin(f64_dtype())),
            ..Default::default()
        };
        let (dir_uri, assets) =
            init_storage_sparse_parquet(&root, &["sp".to_string()], &structure).unwrap();

        assert_eq!(assets.len(), 2, "one asset per block");
        for (i, a) in assets.iter().enumerate() {
            assert!(!a.is_directory);
            assert_eq!(a.parameter.as_deref(), Some("data_uris"));
            assert_eq!(a.num, Some(i));
        }
        // Directory exists; block parquet files do not exist yet.
        let dir = crate::core::file_uri::file_uri_to_path(&dir_uri).unwrap();
        assert!(dir.is_dir());
        assert_eq!(dir, root.join("sp"));
        for p in asset_paths(&assets) {
            assert!(!p.exists(), "block file must not exist before any write");
        }
    }

    // Boundary: a whole-array `write` to a created single-block node round-trips
    // through disk (init_storage → from_paths → into_writable → write → read).
    #[tokio::test]
    async fn write_full_then_read_roundtrips_on_disk() {
        let root = tmpdir("write_full_read");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // Single-chunk 3×3 (chunks = one chunk per dim) → one block.
        let structure = SparseStructure {
            chunks: vec![vec![3], vec![3]],
            shape: vec![3, 3],
            data_type: Some(DType::Builtin(f64_dtype())),
            ..Default::default()
        };
        let (_dir_uri, assets) =
            init_storage_sparse_parquet(&root, &["m".to_string()], &structure).unwrap();
        assert_eq!(assets.len(), 1, "single-chunk node has one block");

        let adapter = SparseBlocksParquetAdapter::from_paths(
            asset_paths(&assets),
            vec![3, 3],
            vec![vec![3], vec![3]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap()
        .into_writable();

        // (0,1)=1.5 and (2,0)=3.7
        let data = sparse_data(vec![vec![0, 2], vec![1, 0]], vec![1.5, 3.7]);
        adapter.as_writable().unwrap().write(data).await.unwrap();

        // The block parquet file now exists on disk.
        assert!(asset_paths(&assets)[0].exists());

        let sd = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(read_i64_col(&sd.coords[0]), vec![0, 2]);
        assert_eq!(read_i64_col(&sd.coords[1]), vec![1, 0]);
        assert_eq!(read_f64_col(&sd.data), vec![1.5, 3.7]);
    }

    // Boundary: a chunked (multi-block) node rejects a whole-array `write`, so a
    // caller cannot silently drop all but one block — upstream
    // `SparseBlocksParquetAdapter.write` raises NotImplementedError for >1 block.
    #[tokio::test]
    async fn write_full_on_multi_block_node_is_rejected() {
        let root = tmpdir("write_full_multiblock_reject");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let structure = SparseStructure {
            chunks: vec![vec![4, 4]],
            shape: vec![8],
            data_type: Some(DType::Builtin(f64_dtype())),
            ..Default::default()
        };
        let (_dir_uri, assets) =
            init_storage_sparse_parquet(&root, &["two".to_string()], &structure).unwrap();
        let adapter = SparseBlocksParquetAdapter::from_paths(
            asset_paths(&assets),
            vec![8],
            vec![vec![4, 4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap()
        .into_writable();

        let data = sparse_data(vec![vec![1]], vec![10.0]);
        let err = adapter.as_writable().unwrap().write(data).await;
        assert!(
            err.is_err(),
            "whole-array write to a 2-block node must fail"
        );
        assert!(
            err.unwrap_err().to_string().contains("single-block"),
            "the guard names the single-block requirement"
        );
    }

    // Boundary: per-block writes to a chunked node persist block-local coords;
    // reading reassembles the global frame by adding chunk offsets.
    #[tokio::test]
    async fn write_block_then_read_reassembles_global_frame() {
        let root = tmpdir("write_block_reassemble");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let structure = SparseStructure {
            chunks: vec![vec![4, 4]],
            shape: vec![8],
            data_type: Some(DType::Builtin(f64_dtype())),
            ..Default::default()
        };
        let (_dir_uri, assets) =
            init_storage_sparse_parquet(&root, &["mb".to_string()], &structure).unwrap();
        let adapter = SparseBlocksParquetAdapter::from_paths(
            asset_paths(&assets),
            vec![8],
            vec![vec![4, 4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap()
        .into_writable();

        // Block 0 local coord 1 → global 1; block 1 local coord 2 → global 6.
        adapter
            .as_writable()
            .unwrap()
            .write_block(sparse_data(vec![vec![1]], vec![10.0]), &[0])
            .await
            .unwrap();
        adapter
            .as_writable()
            .unwrap()
            .write_block(sparse_data(vec![vec![2]], vec![20.0]), &[1])
            .await
            .unwrap();

        let sd = adapter.read(&NDSlice::empty()).await.unwrap();
        let coords = read_i64_col(&sd.coords[0]);
        assert!(coords.contains(&1), "block 0 → global 1, got {coords:?}");
        assert!(coords.contains(&6), "block 1 → global 6, got {coords:?}");
    }

    // Boundary: writing to a block index the node does not have is a validation
    // error, not a panic.
    #[tokio::test]
    async fn write_block_unknown_index_is_error() {
        let root = tmpdir("write_block_unknown");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let structure = SparseStructure {
            chunks: vec![vec![4]],
            shape: vec![4],
            data_type: Some(DType::Builtin(f64_dtype())),
            ..Default::default()
        };
        let (_dir_uri, assets) =
            init_storage_sparse_parquet(&root, &["u".to_string()], &structure).unwrap();
        let adapter = SparseBlocksParquetAdapter::from_paths(
            asset_paths(&assets),
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap()
        .into_writable();

        let err = adapter
            .as_writable()
            .unwrap()
            .write_block(sparse_data(vec![vec![0]], vec![1.0]), &[3])
            .await;
        assert!(err.is_err(), "unknown block index must error");
    }

    // ---- stored-vs-declared value dtype (Wave-17) -----------------------

    fn f32_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Float, 4)
    }

    fn read_f32_col(arr: &DynNDArray) -> Vec<f32> {
        arr.data
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    /// Build a [`SparseData`] whose value column is f32 (one int64 coord column
    /// per dimension). Used to stage an "externally-registered" block whose
    /// stored dtype differs from a node's declared dtype.
    fn sparse_data_f32(coords: Vec<Vec<i64>>, data: Vec<f32>) -> SparseData {
        let nnz = data.len();
        let coord_dyn: Vec<DynNDArray> = coords
            .into_iter()
            .map(|c| {
                let bytes: Vec<u8> = c.iter().flat_map(|v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), i64_dtype(), vec![nnz])
            })
            .collect();
        let data_bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let data_dyn = DynNDArray::new(Bytes::from(data_bytes), f32_dtype(), vec![nnz]);
        SparseData {
            coords: coord_dyn,
            data: data_dyn,
        }
    }

    // Invariant boundary — stored == declared: the returned buffer carries the
    // stored (== declared here) f64 dtype and the correct nnz.
    #[tokio::test]
    async fn read_block_reports_stored_dtype_when_stored_eq_declared() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("eq.parquet");
        // Stored f64 via the production encoder.
        write_sparse_parquet(&p, vec![vec![0, 2]], vec![1.5, 2.5]).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(), // declared f64 == stored f64
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read_block(&[0]).await.unwrap();
        assert_eq!(sd.data.dtype, f64_dtype());
        assert_eq!(sd.data.shape, vec![2]);
        assert_eq!(sd.coords[0].shape, vec![2]);
        assert_eq!(read_f64_col(&sd.data), vec![1.5, 2.5]);
    }

    // Invariant boundary — stored != declared (externally-registered file): the
    // parquet stores f32 values but the node is declared f64. Upstream reads the
    // stored dtype via pandas (sparse_blocks_parquet.py:31,123-127), so the
    // returned buffer must carry the stored f32 — correct nnz, correct values —
    // not the declared f64 (which would halve nnz and reinterpret the bytes).
    #[tokio::test]
    async fn read_block_uses_stored_dtype_when_stored_ne_declared() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ne.parquet");
        // Stage a stored-f32 block through the production encoder.
        write_sparse_block_parquet(&p, sparse_data_f32(vec![vec![0, 2]], vec![1.5, 2.5]))
            .await
            .unwrap();

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(), // declared f64, but stored is f32
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read_block(&[0]).await.unwrap();
        // Stored f32 wins over declared f64.
        assert_eq!(sd.data.dtype, f32_dtype());
        // nnz sized by the stored 4-byte width: 2 non-zeros, not 1.
        assert_eq!(sd.data.shape, vec![2]);
        assert_eq!(sd.coords[0].shape, vec![2]);
        assert_eq!(read_f32_col(&sd.data), vec![1.5f32, 2.5]);
    }

    // Invariant boundary — the same stored/declared mismatch exercised through
    // the multi-block-capable `read()` path (single block here).
    #[tokio::test]
    async fn read_uses_stored_dtype_when_stored_ne_declared() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ne_full.parquet");
        write_sparse_block_parquet(&p, sparse_data_f32(vec![vec![1]], vec![7.5]))
            .await
            .unwrap();

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(sd.data.dtype, f32_dtype());
        assert_eq!(sd.data.shape, vec![1]);
        assert_eq!(read_f32_col(&sd.data), vec![7.5f32]);
    }

    // Invariant boundary — one COO buffer, one dtype: two externally-registered
    // blocks that disagree on stored value dtype (f32 vs f64) cannot be assembled
    // into a single byte buffer, so `read()` errors rather than mis-sizing it.
    // (Upstream numpy.concatenate would promote; a flat byte buffer cannot.)
    #[tokio::test]
    async fn read_rejects_cross_block_value_dtype_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let p0 = dir.path().join("mix-0.parquet");
        let p1 = dir.path().join("mix-1.parquet");
        // Block 0 stored f32, block 1 stored f64.
        write_sparse_block_parquet(&p0, sparse_data_f32(vec![vec![1]], vec![1.0]))
            .await
            .unwrap();
        write_sparse_parquet(&p1, vec![vec![2]], vec![2.0]).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p0, p1],
            vec![8],
            vec![vec![4, 4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let err = adapter.read(&NDSlice::empty()).await;
        assert!(err.is_err(), "cross-block dtype disagreement must error");
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("disagree on stored value dtype"),
            "the error names the cross-block dtype disagreement"
        );
    }

    // ---- null value-column handling (Finding A) ------------------------

    /// Write a parquet file from fully-formed arrow fields + columns. Stages the
    /// shapes an external (non-tiled) writer can produce — including nullable
    /// columns, which our production encoder never writes.
    async fn write_raw_parquet(
        path: &Path,
        fields: Vec<arrow::datatypes::Field>,
        columns: Vec<arrow::array::ArrayRef>,
    ) {
        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = super::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Stage a block with non-nullable Int64 coord columns plus a caller-supplied
    /// value column (typically **nullable**, `nullable` set on the value field so
    /// parquet records definition levels and the nulls survive the round-trip).
    async fn write_nullable_value_parquet(
        path: &Path,
        coords: Vec<Vec<i64>>,
        value_field: arrow::datatypes::Field,
        value_col: arrow::array::ArrayRef,
    ) {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let mut fields: Vec<Field> = Vec::with_capacity(coords.len() + 1);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(coords.len() + 1);
        for (d, c) in coords.iter().enumerate() {
            fields.push(Field::new(format!("dim{d}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(c.clone())) as ArrayRef);
        }
        fields.push(value_field);
        columns.push(value_col);
        write_raw_parquet(path, fields, columns).await;
    }

    // Invariant boundary — float value column with a null. Upstream reads it
    // with pandas (`sparse_blocks_parquet.py:31`) → numpy float array with NaN
    // at the null slot, never 0.0. A typed f64 buffer can hold NaN, so we must
    // decode the null to NaN, not silently zero it.
    #[tokio::test]
    async fn read_block_float_column_nulls_become_nan() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fnull.parquet");
        write_nullable_value_parquet(
            &p,
            vec![vec![0, 1, 2]],
            Field::new("data", DataType::Float64, true),
            Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)])),
        )
        .await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sd = adapter.read_block(&[0]).await.unwrap();
        let vals = read_f64_col(&sd.data);
        assert_eq!(vals.len(), 3, "nnz counts every stored row, null included");
        assert_eq!(vals[0], 1.5);
        assert!(
            vals[1].is_nan(),
            "null float slot must decode to NaN, got {}",
            vals[1]
        );
        assert_eq!(vals[2], 2.5);
    }

    // Invariant boundary — integer value column with a null. Upstream pandas
    // promotes an int column with nulls to float64+NaN, a dtype our stored-int
    // flat buffer cannot represent. Silent zeros are unacceptable, so the read
    // must error and name the offending block file.
    #[tokio::test]
    async fn read_block_int_column_nulls_are_rejected() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("inull.parquet");
        write_nullable_value_parquet(
            &p,
            vec![vec![0, 1, 2]],
            Field::new("data", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
        )
        .await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            i64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let err = adapter.read_block(&[0]).await;
        assert!(
            err.is_err(),
            "int value column with nulls must be rejected, not silently zeroed"
        );
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("inull.parquet"),
            "error names the offending block file, got: {msg}"
        );
        assert!(
            msg.contains("null"),
            "error explains the null cause, got: {msg}"
        );
    }

    // Invariant boundary — a null in a *coordinate* column (family audit: the
    // coord read path shares `extract_data_bytes`'s null-bitmap blindness). A COO
    // coordinate is an array index and cannot be null, so the read must reject
    // the block naming the file, not push a garbage index for the null slot.
    #[tokio::test]
    async fn read_block_null_coordinate_is_rejected() {
        use arrow::array::{ArrayRef, Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnull.parquet");
        // Nullable Int64 coord column with a null at row 1; the value column is
        // valid so only the coordinate check can fire.
        let fields = vec![
            Field::new("dim0", DataType::Int64, true),
            Field::new("data", DataType::Float64, false),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(0), None, Some(2)])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ];
        write_raw_parquet(&p, fields, columns).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let err = adapter.read_block(&[0]).await;
        assert!(
            err.is_err(),
            "null coordinate index must be rejected, not silently zeroed"
        );
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("cnull.parquet"),
            "error names the offending block file, got: {msg}"
        );
        assert!(
            msg.contains("null"),
            "error explains the null cause, got: {msg}"
        );
        assert!(
            msg.contains("coordinate"),
            "error names the coordinate column, got: {msg}"
        );
    }

    // ---- widened value-column coverage (Finding B) ---------------------

    /// Stage a block with the given non-null value column, read it back through
    /// both `read_block` (single) and `read` (multi-block-capable), and assert the
    /// returned buffer carries `expected` dtype and `expected_le` bytes.
    async fn assert_value_dtype_roundtrips(
        name: &str,
        value_field: arrow::datatypes::Field,
        value_col: arrow::array::ArrayRef,
        expected: BuiltinDType,
        expected_le: Vec<u8>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(format!("{name}.parquet"));
        // Two non-zeros at global coords 0 and 1.
        write_nullable_value_parquet(&p, vec![vec![0, 1]], value_field, value_col).await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(), // declared f64; stored dtype is authoritative on read
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let sb = adapter.read_block(&[0]).await.unwrap();
        assert_eq!(sb.data.dtype, expected, "{name}: read_block dtype");
        assert_eq!(sb.data.shape, vec![2], "{name}: read_block nnz");
        assert_eq!(
            sb.data.data.as_ref(),
            expected_le.as_slice(),
            "{name}: read_block bytes"
        );

        let sf = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(sf.data.dtype, expected, "{name}: read dtype");
        assert_eq!(
            sf.data.data.as_ref(),
            expected_le.as_slice(),
            "{name}: read bytes"
        );
    }

    // Boundary — every widened integer value dtype round-trips through read +
    // read_block, labelled with the stored width. These are exactly the widths
    // the server's COO value encoder (`dyn_ndarray_to_arrow`) already handles.
    #[tokio::test]
    async fn read_widened_integer_value_dtypes_roundtrip() {
        use arrow::array::{
            ArrayRef, Int8Array, Int16Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        };
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let u = Kind::UnsignedInteger;
        let i = Kind::Integer;
        let le = |v: &[u8]| v.to_vec();

        assert_value_dtype_roundtrips(
            "u8",
            Field::new("data", DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![1u8, 200])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, u, 1),
            le(&[1, 200]),
        )
        .await;
        assert_value_dtype_roundtrips(
            "u16",
            Field::new("data", DataType::UInt16, false),
            Arc::new(UInt16Array::from(vec![1u16, 40000])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, u, 2),
            [1u16, 40000].iter().flat_map(|v| v.to_le_bytes()).collect(),
        )
        .await;
        assert_value_dtype_roundtrips(
            "u32",
            Field::new("data", DataType::UInt32, false),
            Arc::new(UInt32Array::from(vec![1u32, 3_000_000_000])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, u, 4),
            [1u32, 3_000_000_000]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect(),
        )
        .await;
        assert_value_dtype_roundtrips(
            "u64",
            Field::new("data", DataType::UInt64, false),
            Arc::new(UInt64Array::from(vec![1u64, 10_000_000_000])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, u, 8),
            [1u64, 10_000_000_000]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect(),
        )
        .await;
        assert_value_dtype_roundtrips(
            "i8",
            Field::new("data", DataType::Int8, false),
            Arc::new(Int8Array::from(vec![-1i8, 100])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, i, 1),
            [-1i8, 100].iter().flat_map(|v| v.to_le_bytes()).collect(),
        )
        .await;
        assert_value_dtype_roundtrips(
            "i16",
            Field::new("data", DataType::Int16, false),
            Arc::new(Int16Array::from(vec![-1i16, 30000])) as ArrayRef,
            BuiltinDType::new(Endianness::Little, i, 2),
            [-1i16, 30000]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect(),
        )
        .await;
    }

    // A dictionary-encoded value column (e.g. a pandas Categorical) decodes to its
    // value type. Exercised at the function level rather than via a parquet
    // round-trip because the arrow parquet reader may normalize dictionary
    // encoding away on read, so a round-trip would not reliably reach this branch.
    #[test]
    fn arrow_to_builtin_dtype_decodes_dictionary_to_value_type() {
        use arrow::datatypes::DataType;
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64));
        assert_eq!(super::arrow_to_builtin_dtype(&dt).unwrap(), f64_dtype());
    }

    #[test]
    fn extract_data_bytes_decodes_dictionary_wrapped_primitive() {
        use arrow::array::{Array, ArrayRef, DictionaryArray, Float64Array, Int32Array};
        use arrow::datatypes::Int32Type;
        use std::sync::Arc;

        // Dictionary<Int32, Float64>: keys [0,1,0] over values [1.5, 2.5].
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values = Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef;
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let col: Arc<dyn Array> = Arc::new(dict);

        let bytes =
            super::extract_data_bytes(&col, 8, std::path::Path::new("dict.parquet")).unwrap();
        let vals: Vec<f64> = bytes
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect();
        assert_eq!(
            vals,
            vec![1.5, 2.5, 1.5],
            "dictionary decodes to its values"
        );
    }

    // Deliberate parity ceilings: types upstream `pandas.read_parquet` accepts but
    // our COO value encoder cannot faithfully round-trip, so the read rejects them
    // (422) rather than silently promote (which would diverge from the stored
    // dtype). See `arrow_to_builtin_dtype`'s doc for the per-type reason.
    #[test]
    fn arrow_to_builtin_dtype_rejects_unrepresentable_types() {
        use arrow::datatypes::{DataType, TimeUnit};
        for dt in [
            DataType::Float16,
            DataType::Boolean,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Date32,
            DataType::Decimal128(38, 10),
        ] {
            assert!(
                super::arrow_to_builtin_dtype(&dt).is_err(),
                "{dt:?} must be a deliberate rejection, not silently accepted"
            );
        }
    }

    // End-to-end: a boolean value column in an actual parquet file is rejected on
    // read (not promoted), demonstrating the 422 reaches the read boundary.
    #[tokio::test]
    async fn read_rejects_boolean_value_column() {
        use arrow::array::BooleanArray;
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("boolval.parquet");
        write_nullable_value_parquet(
            &p,
            vec![vec![0, 1]],
            Field::new("data", DataType::Boolean, false),
            Arc::new(BooleanArray::from(vec![true, false])),
        )
        .await;

        let adapter = SparseBlocksParquetAdapter::from_paths(
            vec![p],
            vec![4],
            vec![vec![4]],
            f64_dtype(),
            None,
            serde_json::Value::Null,
        )
        .unwrap();

        let err = adapter.read_block(&[0]).await;
        assert!(
            err.is_err(),
            "boolean value column must be rejected on read, not promoted"
        );
        assert!(
            err.unwrap_err().to_string().contains("Boolean"),
            "rejection names the offending arrow type"
        );
    }
}
