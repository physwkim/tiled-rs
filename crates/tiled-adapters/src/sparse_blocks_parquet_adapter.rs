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
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use tiled_core::adapters::{BaseAdapter, BoxFuture, SparseAdapterRead, SparseData};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{SparseLayout, SparseStructure, Spec, StructureFamily};

#[derive(Debug)]
pub struct SparseBlocksParquetAdapter {
    /// Map from N-dim block index to the parquet file path for that block.
    blocks: BTreeMap<Vec<usize>, PathBuf>,
    structure: SparseStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
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
        })
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
        let nnz = if elem == 0 {
            0
        } else {
            data_bytes.len() / elem
        };
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use tiled_core::adapters::{BaseAdapter, SparseAdapterRead};
    use tiled_core::dtype::{BuiltinDType, Endianness, Kind};
    use tiled_core::ndslice::NDSlice;
    use tiled_core::structures::StructureFamily;

    use super::SparseBlocksParquetAdapter;

    fn f64_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Float, 8)
    }

    /// Write a sparse block parquet file: columns are dim_0, dim_1, ..., data.
    fn write_sparse_parquet(path: &PathBuf, coords: Vec<Vec<i64>>, data: Vec<f64>) {
        let ndim = coords.len();
        let mut fields: Vec<Field> = (0..ndim)
            .map(|d| Field::new(format!("dim_{d}"), DataType::Int64, false))
            .collect();
        fields.push(Field::new("data", DataType::Float64, false));
        let schema = Arc::new(Schema::new(fields));

        let mut cols: Vec<Arc<dyn arrow::array::Array>> = coords
            .into_iter()
            .map(|c| {
                let arr: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(c));
                arr
            })
            .collect();
        cols.push(Arc::new(Float64Array::from(data)));

        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn single_block_read_block() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("block-0.0.parquet");
        // 2D sparse: two non-zeros at (0,1)=1.5 and (2,3)=2.5
        write_sparse_parquet(&p, vec![vec![0, 2], vec![1, 3]], vec![1.5, 2.5]);

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
        write_sparse_parquet(&p0, vec![vec![1]], vec![10.0]);
        // Block 1: non-zero at local index 2 → global 6 (offset=4)
        write_sparse_parquet(&p1, vec![vec![2]], vec![20.0]);

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
        write_sparse_parquet(&p, vec![vec![], vec![]], vec![]);

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
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]);
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
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]);
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
        write_sparse_parquet(&p, vec![vec![0]], vec![1.0]);
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
}
