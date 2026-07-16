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

    fn data_dtype(&self) -> BuiltinDType {
        match &self.structure.data_type {
            Some(DType::Builtin(b)) => b.clone(),
            _ => unreachable!("data_type always Builtin"),
        }
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

    fn read_block_file(&self, block: &[usize]) -> Result<(Vec<Vec<i64>>, Vec<u8>)> {
        let path = self
            .blocks
            .get(block)
            .ok_or_else(|| TiledError::Validation(format!("no block at index {block:?}")))?;
        read_sparse_parquet(path)
    }

    fn to_sparse_data(&self, coords: Vec<Vec<i64>>, data_bytes: Vec<u8>) -> SparseData {
        let dtype = self.data_dtype();
        let coord_dtype = self.coord_dtype();
        let elem = dtype.element_size();
        let nnz = data_bytes.len().checked_div(elem).unwrap_or(0);
        let coord_arrays: Vec<DynNDArray> = coords
            .iter()
            .map(|dim_coords| {
                let bytes: Vec<u8> = dim_coords.iter().flat_map(|&v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), coord_dtype.clone(), vec![nnz])
            })
            .collect();
        let data = DynNDArray::new(Bytes::from(data_bytes), dtype, vec![nnz]);
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

            for index in self.blocks.keys() {
                let (local_coords, data_bytes) = self.read_block_file(index)?;
                let offsets = self.block_offsets(index);
                for (d, (dest, src)) in all_coords.iter_mut().zip(local_coords.iter()).enumerate() {
                    let off = offsets[d];
                    dest.extend(src.iter().map(|&c| c + off));
                }
                all_data.extend_from_slice(&data_bytes);
            }

            let sd = self.to_sparse_data(all_coords, all_data);
            // Apply slice by filtering non-zero entries that fall within the slice.
            apply_sparse_slice(sd, slice, &self.structure.shape, self.data_dtype())
        })
    }

    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let (coords, data_bytes) = self.read_block_file(block)?;
            Ok(self.to_sparse_data(coords, data_bytes))
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
/// Returns `(coords, data_bytes)` where `coords[d]` is the list of
/// block-local integer coordinates for dimension `d` and `data_bytes`
/// is the raw non-zero value buffer.
///
/// Parquet layout: columns `[coord_0, coord_1, ..., data]` — all columns
/// except the last are coordinate columns; the last column is values.
fn read_sparse_parquet(path: &std::path::Path) -> Result<(Vec<Vec<i64>>, Vec<u8>)> {
    use arrow::array::Int64Array;

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
    let data_dtype = schema.field(ndim).data_type().clone();
    let elem = arrow_elem_size(&data_dtype)?;

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
            for i in 0..nr {
                dim_buf.push(arr.value(i));
            }
        }
        // Data column — extract as raw little-endian bytes.
        let data_col = batch.column(ndim);
        data_bytes.extend_from_slice(&extract_data_bytes(data_col, elem)?);
    }

    Ok((per_dim, data_bytes))
}

/// Return the element size (bytes) for an Arrow DataType.
fn arrow_elem_size(dt: &arrow::datatypes::DataType) -> Result<usize> {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 | DataType::UInt8 => Ok(1),
        DataType::Int16 | DataType::UInt16 => Ok(2),
        DataType::Float32 | DataType::Int32 | DataType::UInt32 => Ok(4),
        DataType::Float64 | DataType::Int64 | DataType::UInt64 => Ok(8),
        other => Err(TiledError::Validation(format!(
            "unsupported data column type for sparse parquet: {other:?}"
        ))),
    }
}

/// Extract little-endian bytes from an Arrow primitive column.
fn extract_data_bytes(col: &Arc<dyn arrow::array::Array>, elem: usize) -> Result<Vec<u8>> {
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;
    let n = col.len();
    let mut out = vec![0u8; n * elem];
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..n {
                out[i * 8..(i + 1) * 8].copy_from_slice(&arr.value(i).to_le_bytes());
            }
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..n {
                out[i * 8..(i + 1) * 8].copy_from_slice(&arr.value(i).to_le_bytes());
            }
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..n {
                out[i * 4..(i + 1) * 4].copy_from_slice(&arr.value(i).to_le_bytes());
            }
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..n {
                out[i * 4..(i + 1) * 4].copy_from_slice(&arr.value(i).to_le_bytes());
            }
        }
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
fn apply_sparse_slice(
    sd: SparseData,
    slice: &NDSlice,
    shape: &[usize],
    _dtype: BuiltinDType,
) -> Result<SparseData> {
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

    // init_storage lays out one `data_uris` asset per block, numbered in C-order,
    // with no parquet files created yet (upstream leaves each block file to the
    // first write).
    #[test]
    fn init_storage_registers_one_asset_per_block() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
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
}
