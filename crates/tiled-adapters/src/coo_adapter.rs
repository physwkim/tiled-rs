//! In-memory COO sparse adapter.
//!
//! Corresponds to `tiled/adapters/sparse.py:COOAdapter`.
//!
//! A `CooAdapter` wraps a single in-memory COO block (all-zero block indices,
//! matching Python `COOAdapter.from_arrays` which stores
//! `{(0, 0, ...): (coords, data)}`).

use bytes::Bytes;

use tiled_core::adapters::{BaseAdapter, BoxFuture, SparseAdapterRead, SparseData};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{SparseLayout, SparseStructure, Spec, StructureFamily};

/// In-memory COO sparse adapter.
///
/// Holds a single block (block index = all zeros). Per-dimension coordinate
/// arrays are stored as `Vec<Vec<i64>>` and promoted to `DynNDArray` on read.
///
/// Python parity: `COOAdapter.from_arrays` / `read_block` / `read`
/// in `tiled/adapters/sparse.py`.
#[derive(Debug)]
pub struct CooAdapter {
    /// Per-dimension coordinate arrays, each of length `nnz`.
    /// `coords[d][i]` is the index in dimension `d` for non-zero `i`.
    /// Mirrors Python's `coords[d, :]` from the `(ndim, nnz)` coords matrix.
    coords: Vec<Vec<i64>>,
    /// Non-zero values, shape `[nnz]`.
    data: DynNDArray,
    structure: SparseStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl CooAdapter {
    /// Build from per-dimension coordinate arrays and a flat data array.
    ///
    /// - `coord_arrays` — one `Vec<i64>` per dimension, each length `nnz`.
    /// - `data_bytes` / `data_dtype` — raw non-zero values (`nnz` elements).
    /// - `shape` — full shape of the sparse array (one entry per dimension).
    /// - `dims` — optional dimension names.
    ///
    /// Mirrors Python `COOAdapter.from_arrays(coords, data, shape, dims=None)`.
    pub fn from_arrays(
        coord_arrays: Vec<Vec<i64>>,
        data_bytes: Bytes,
        data_dtype: BuiltinDType,
        shape: Vec<usize>,
        dims: Option<Vec<String>>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Result<Self> {
        let ndim = shape.len();
        if coord_arrays.len() != ndim {
            return Err(TiledError::Validation(format!(
                "coord_arrays has {} entries but shape has {} dimensions",
                coord_arrays.len(),
                ndim
            )));
        }

        // nnz is the length of the first dim's coordinate array (or 0 for
        // a scalar/0-dim shape). All per-dim arrays must agree.
        let nnz = coord_arrays.first().map_or(0, Vec::len);
        for (d, c) in coord_arrays.iter().enumerate() {
            if c.len() != nnz {
                return Err(TiledError::Validation(format!(
                    "coord_arrays[{d}] has length {} but expected {nnz}",
                    c.len()
                )));
            }
        }

        let expected_bytes = nnz * data_dtype.element_size();
        if data_bytes.len() != expected_bytes {
            return Err(TiledError::Validation(format!(
                "data has {} bytes but expected {} ({nnz} elements × {} bytes/elem)",
                data_bytes.len(),
                expected_bytes,
                data_dtype.element_size()
            )));
        }

        // Single chunk covering the whole shape — matches Python's
        //   chunks = tuple((dim,) for dim in shape)
        let chunks: Vec<Vec<usize>> = shape.iter().map(|&s| vec![s]).collect();

        // Coordinates are stored / served as signed int64 little-endian.
        let coord_dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);

        let structure = SparseStructure {
            chunks,
            shape: shape.clone(),
            data_type: Some(DType::Builtin(data_dtype.clone())),
            coord_data_type: Some(coord_dtype),
            dims,
            resizable: Default::default(),
            layout: SparseLayout::COO,
        };
        let data = DynNDArray::new(data_bytes, data_dtype, vec![nnz]);

        Ok(Self {
            coords: coord_arrays,
            data,
            structure,
            metadata,
            specs,
        })
    }

    /// Number of non-zeros.
    fn nnz(&self) -> usize {
        self.data.shape.first().copied().unwrap_or(0)
    }

    /// Materialise per-dimension `DynNDArray` coord arrays from the stored
    /// `Vec<Vec<i64>>` and return the full `SparseData` for the single block.
    fn full_block_data(&self) -> SparseData {
        let coord_dtype = self
            .structure
            .coord_data_type
            .clone()
            .expect("coord_data_type always set by from_arrays");
        let nnz = self.nnz();
        let coords: Vec<DynNDArray> = self
            .coords
            .iter()
            .map(|dim_coords| {
                let bytes: Vec<u8> = dim_coords.iter().flat_map(|&v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), coord_dtype.clone(), vec![nnz])
            })
            .collect();
        SparseData {
            coords,
            data: self.data.clone(),
        }
    }
}

impl BaseAdapter for CooAdapter {
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

impl SparseAdapterRead for CooAdapter {
    fn structure(&self) -> &SparseStructure {
        &self.structure
    }

    /// Return the stored COO data for `block`.
    ///
    /// The adapter holds exactly one block (all-zero indices). Any other block
    /// key is out of range. Mirrors Python `read_block`:
    /// ```python
    /// coords, data = self.blocks[block]
    /// arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
    /// return arr[slice] if slice else arr
    /// ```
    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let ndim = self.structure.shape.len();
            if block.len() != ndim {
                return Err(TiledError::Validation(format!(
                    "expected {ndim} block indices, got {}",
                    block.len()
                )));
            }
            if block.iter().any(|&b| b != 0) {
                return Err(TiledError::Validation(format!(
                    "block {:?} is out of range: this adapter has a single block at all-zero indices",
                    block
                )));
            }
            Ok(self.full_block_data())
        })
    }

    /// Return the full COO data optionally filtered by `slice`.
    ///
    /// Empty (full) slices return all non-zeros unchanged. Partial slices are
    /// not supported — Python does `sparse.COO[slice]` which filters non-zeros
    /// by coordinate bounds; reproducing that here would require a full
    /// coordinate scan. Callers should use `read_block` for sub-block access.
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            if !slice.is_empty() {
                // Partial COO slicing (sparse.COO[slice] in Python) filters
                // non-zeros by their coordinates, which requires iterating
                // all nnz entries. Not implemented — return an explicit error
                // rather than silently returning wrong data.
                return Err(TiledError::InvalidSlice(
                    "partial slicing of sparse COO arrays is not supported; \
                     use read_block with block=[0,...,0] and apply slicing client-side"
                        .into(),
                ));
            }
            Ok(self.full_block_data())
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tiled_core::structures::Resizable;

    fn f64_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Float, 8)
    }

    fn i64_coord_dtype() -> BuiltinDType {
        BuiltinDType::new(Endianness::Little, Kind::Integer, 8)
    }

    /// 3x3 fixture with 2 non-zeros:
    ///   (0, 1) -> 1.5
    ///   (2, 0) -> 3.7
    fn make_3x3_adapter() -> CooAdapter {
        let shape = vec![3usize, 3];
        // coord_arrays[0] = row indices, coord_arrays[1] = col indices
        let coord_arrays = vec![vec![0i64, 2], vec![1i64, 0]];
        let data_values: Vec<f64> = vec![1.5, 3.7];
        let data_bytes: Vec<u8> = data_values.iter().flat_map(|v| v.to_le_bytes()).collect();

        CooAdapter::from_arrays(
            coord_arrays,
            Bytes::from(data_bytes),
            f64_dtype(),
            shape,
            None,
            serde_json::json!({}),
            vec![],
        )
        .expect("valid fixture")
    }

    #[test]
    fn test_structure_family() {
        let adapter = make_3x3_adapter();
        assert_eq!(adapter.structure_family(), StructureFamily::Sparse);
    }

    #[test]
    fn test_from_arrays_structure() {
        let adapter = make_3x3_adapter();
        let s = adapter.structure();

        assert_eq!(s.shape, vec![3, 3]);
        // single chunk per dimension covering the full shape
        assert_eq!(s.chunks, vec![vec![3], vec![3]]);
        assert_eq!(s.layout, SparseLayout::COO);
        assert_eq!(s.resizable, Resizable::Uniform(false));

        // data_type should be f64
        match &s.data_type {
            Some(DType::Builtin(b)) => {
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.itemsize, 8);
            }
            other => panic!("unexpected data_type: {other:?}"),
        }

        // coord_data_type should be i64
        let coord_dt = s.coord_data_type.as_ref().expect("coord_data_type set");
        assert_eq!(coord_dt.kind, Kind::Integer);
        assert_eq!(coord_dt.itemsize, 8);
        assert_eq!(coord_dt.endianness, Endianness::Little);
    }

    #[test]
    fn test_from_arrays_with_dims() {
        let adapter = CooAdapter::from_arrays(
            vec![vec![0i64], vec![0i64]],
            Bytes::from(1.0f64.to_le_bytes().to_vec()),
            f64_dtype(),
            vec![5, 5],
            Some(vec!["x".into(), "y".into()]),
            serde_json::json!({}),
            vec![],
        )
        .unwrap();
        assert_eq!(adapter.structure().dims, Some(vec!["x".into(), "y".into()]));
    }

    #[tokio::test]
    async fn test_read_block_returns_correct_nnz() {
        let adapter = make_3x3_adapter();
        let sd = adapter.read_block(&[0, 0]).await.expect("read_block ok");

        // 2 non-zeros
        assert_eq!(sd.coords.len(), 2); // one coord array per dimension
        assert_eq!(sd.coords[0].shape, vec![2]); // row indices, length nnz=2
        assert_eq!(sd.coords[1].shape, vec![2]); // col indices, length nnz=2
        assert_eq!(sd.data.shape, vec![2]); // values, length nnz=2
    }

    #[tokio::test]
    async fn test_read_block_coord_values() {
        let adapter = make_3x3_adapter();
        let sd = adapter.read_block(&[0, 0]).await.expect("read_block ok");

        // Row coords: [0, 2]
        let row_bytes = &sd.coords[0].data;
        let row0 = i64::from_le_bytes(row_bytes[0..8].try_into().unwrap());
        let row1 = i64::from_le_bytes(row_bytes[8..16].try_into().unwrap());
        assert_eq!(row0, 0);
        assert_eq!(row1, 2);

        // Col coords: [1, 0]
        let col_bytes = &sd.coords[1].data;
        let col0 = i64::from_le_bytes(col_bytes[0..8].try_into().unwrap());
        let col1 = i64::from_le_bytes(col_bytes[8..16].try_into().unwrap());
        assert_eq!(col0, 1);
        assert_eq!(col1, 0);
    }

    #[tokio::test]
    async fn test_read_block_data_values() {
        let adapter = make_3x3_adapter();
        let sd = adapter.read_block(&[0, 0]).await.expect("read_block ok");

        let v0 = f64::from_le_bytes(sd.data.data[0..8].try_into().unwrap());
        let v1 = f64::from_le_bytes(sd.data.data[8..16].try_into().unwrap());
        assert!((v0 - 1.5).abs() < f64::EPSILON);
        assert!((v1 - 3.7).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_read_full_slice_returns_all() {
        let adapter = make_3x3_adapter();
        let slice = NDSlice::empty();
        let sd = adapter.read(&slice).await.expect("read ok");

        assert_eq!(sd.coords.len(), 2);
        assert_eq!(sd.data.shape, vec![2]);
    }

    #[tokio::test]
    async fn test_read_block_wrong_ndim() {
        let adapter = make_3x3_adapter();
        let err = adapter.read_block(&[0]).await.unwrap_err();
        assert!(err.to_string().contains("expected 2 block indices"));
    }

    #[tokio::test]
    async fn test_read_block_out_of_range() {
        let adapter = make_3x3_adapter();
        let err = adapter.read_block(&[1, 0]).await.unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[tokio::test]
    async fn test_read_partial_slice_errors() {
        let adapter = make_3x3_adapter();
        let slice = NDSlice::from_numpy_str("0:2,0:2").unwrap();
        let err = adapter.read(&slice).await.unwrap_err();
        assert!(matches!(err, TiledError::InvalidSlice(_)));
    }

    #[test]
    fn test_from_arrays_coord_len_mismatch_errors() {
        let err = CooAdapter::from_arrays(
            vec![vec![0i64, 1], vec![0i64]],
            Bytes::from(1.0f64.to_le_bytes().to_vec()),
            f64_dtype(),
            vec![3, 3],
            None,
            serde_json::json!({}),
            vec![],
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected 2"));
    }

    #[test]
    fn test_from_arrays_data_len_mismatch_errors() {
        // 2 non-zeros but only 1 f64 worth of bytes
        let err = CooAdapter::from_arrays(
            vec![vec![0i64, 1], vec![0i64, 1]],
            Bytes::from(1.0f64.to_le_bytes().to_vec()),
            f64_dtype(),
            vec![3, 3],
            None,
            serde_json::json!({}),
            vec![],
        )
        .unwrap_err();
        assert!(err.to_string().contains("bytes"));
    }

    #[test]
    fn test_coord_dtype_in_structure() {
        let adapter = make_3x3_adapter();
        let coord_dt = adapter.structure().coord_data_type.clone().expect("set");
        assert_eq!(coord_dt, i64_coord_dtype());
    }

    #[tokio::test]
    async fn test_empty_sparse_array() {
        let adapter = CooAdapter::from_arrays(
            vec![vec![], vec![]],
            Bytes::new(),
            f64_dtype(),
            vec![3, 3],
            None,
            serde_json::json!({}),
            vec![],
        )
        .expect("empty array is valid");

        assert_eq!(adapter.nnz(), 0);
        let sd = adapter.read_block(&[0, 0]).await.unwrap();
        assert_eq!(sd.data.shape, vec![0]);
    }
}
