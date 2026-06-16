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
use tiled_core::ndslice::{NDSlice, SliceDim};
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

    /// Return the COO data selected by `slice`.
    ///
    /// A full slice returns all non-zeros unchanged. A partial slice applies
    /// numpy *basic* indexing to the sparse array, exactly as Python
    /// `COOAdapter.read` does `arr[slice]` (`tiled/adapters/sparse.py:175-191`):
    /// non-zeros whose coordinates fall outside the selected region are
    /// dropped, surviving coordinates are remapped into the sliced frame
    /// (`(coord - start) / step`), and `Index` dimensions are removed from the
    /// result. `NDSlice` carries only `Index`/`Slice`/`Ellipsis`, so this is
    /// numpy basic indexing — no boolean or advanced indexing.
    ///
    /// The returned [`SparseData`] holds the surviving coordinates and data;
    /// the grid shape of the sliced result is derived by the caller from the
    /// slice it supplied (`SparseData` carries no shape), the same way the
    /// array read path pairs a slice with its computed shape.
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            if slice.is_empty() {
                return Ok(self.full_block_data());
            }

            let ndim = self.structure.shape.len();
            let dims = expand_ellipsis(&slice.0, ndim)?;

            // Resolve each dimension into a selection: an `Index` (drops the
            // dim, keeps only matching non-zeros) or a normalised `Slice`
            // (keeps the dim, remaps coordinates into the sliced frame).
            let mut sels: Vec<DimSel> = Vec::with_capacity(ndim);
            for (d, sd) in dims.iter().enumerate() {
                let len = self.structure.shape[d];
                match sd {
                    SliceDim::Index(i) => {
                        sels.push(DimSel::Index(normalize_index(*i, len)? as i64))
                    }
                    SliceDim::Slice { start, stop, step } => {
                        let (s, t, st) = normalize_slice(*start, *stop, *step, len)?;
                        sels.push(DimSel::Slice {
                            start: s,
                            stop: t,
                            step: st,
                        });
                    }
                    SliceDim::Ellipsis => {
                        return Err(TiledError::InvalidSlice(
                            "unexpected Ellipsis after expansion".into(),
                        ));
                    }
                }
            }

            let kept_dims = sels
                .iter()
                .filter(|s| matches!(s, DimSel::Slice { .. }))
                .count();

            // Scan every non-zero, keeping those selected on all axes and
            // remapping their coordinates into the sliced frame.
            let nnz = self.nnz();
            let mut new_coords: Vec<Vec<i64>> = vec![Vec::new(); kept_dims];
            let mut kept: Vec<usize> = Vec::new();
            'nz: for i in 0..nnz {
                let mut projected: Vec<i64> = Vec::with_capacity(kept_dims);
                for (d, sel) in sels.iter().enumerate() {
                    let c = self.coords[d][i];
                    match sel {
                        DimSel::Index(idx) => {
                            if c != *idx {
                                continue 'nz;
                            }
                        }
                        DimSel::Slice { start, stop, step } => {
                            match project_coord(c, *start, *stop, *step) {
                                Some(nc) => projected.push(nc),
                                None => continue 'nz,
                            }
                        }
                    }
                }
                for (k, nc) in projected.into_iter().enumerate() {
                    new_coords[k].push(nc);
                }
                kept.push(i);
            }

            // Materialise filtered coordinate buffers (int64 LE) ...
            let coord_dtype = self
                .structure
                .coord_data_type
                .clone()
                .expect("coord_data_type always set by from_arrays");
            let new_nnz = kept.len();
            let coords: Vec<DynNDArray> = new_coords
                .iter()
                .map(|dim_coords| {
                    let bytes: Vec<u8> = dim_coords.iter().flat_map(|&v| v.to_le_bytes()).collect();
                    DynNDArray::new(Bytes::from(bytes), coord_dtype.clone(), vec![new_nnz])
                })
                .collect();

            // ... and the filtered data buffer (preserving element order).
            let elem = self.data.dtype.element_size();
            let mut data_bytes: Vec<u8> = Vec::with_capacity(new_nnz * elem);
            for &i in &kept {
                data_bytes.extend_from_slice(&self.data.data[i * elem..(i + 1) * elem]);
            }
            let data = DynNDArray::new(
                Bytes::from(data_bytes),
                self.data.dtype.clone(),
                vec![new_nnz],
            );

            Ok(SparseData { coords, data })
        })
    }
}

// ---------------------------------------------------------------------------
// Slicing — numpy basic indexing over COO coordinates
// ---------------------------------------------------------------------------

/// Per-dimension selection resolved from an [`NDSlice`] dim.
enum DimSel {
    /// Integer index: keep non-zeros whose coordinate equals this, drop the dim.
    Index(i64),
    /// Strided range (normalised via `slice.indices`): keep the dim, remapping
    /// surviving coordinates into the sliced frame.
    Slice {
        start: isize,
        stop: isize,
        step: isize,
    },
}

/// Expand `dims` to exactly `ndim` per-axis selections: a single `Ellipsis`
/// becomes as many full slices as needed, and any *missing trailing* axes are
/// filled with full slices (numpy treats `arr[0:2]` on a 2-D array as
/// `arr[0:2, :]`). The COO scan addresses axes by index, so every axis must be
/// present. Errors on >1 ellipsis or too many dims.
fn expand_ellipsis(dims: &[SliceDim], ndim: usize) -> Result<Vec<SliceDim>> {
    let ellipsis = dims
        .iter()
        .filter(|d| matches!(d, SliceDim::Ellipsis))
        .count();
    if ellipsis > 1 {
        return Err(TiledError::InvalidSlice(
            "NDSlice can only contain one Ellipsis".into(),
        ));
    }
    let provided = dims.len() - ellipsis;
    if provided > ndim {
        return Err(TiledError::InvalidSlice(format!(
            "slice specifies {provided} dimensions but the sparse array has {ndim}"
        )));
    }
    let mut out = Vec::with_capacity(ndim);
    for d in dims {
        match d {
            SliceDim::Ellipsis => {
                for _ in 0..(ndim - provided) {
                    out.push(SliceDim::full());
                }
            }
            other => out.push(other.clone()),
        }
    }
    // Missing trailing axes (no ellipsis present) default to full slices.
    while out.len() < ndim {
        out.push(SliceDim::full());
    }
    Ok(out)
}

/// Normalize a possibly-negative index into `[0, len)`, erroring out of bounds.
fn normalize_index(i: isize, len: usize) -> Result<usize> {
    let n = len as isize;
    let idx = if i < 0 { i + n } else { i };
    if idx < 0 || idx >= n {
        return Err(TiledError::InvalidSlice(format!(
            "index {i} is out of bounds for an axis of length {len}"
        )));
    }
    Ok(idx as usize)
}

/// Normalize `start:stop:step` over `len` elements, mirroring Python
/// `slice(start, stop, step).indices(len)`. Returns `(start, stop, step)`.
fn normalize_slice(
    start: Option<isize>,
    stop: Option<isize>,
    step: Option<isize>,
    len: usize,
) -> Result<(isize, isize, isize)> {
    let step = step.unwrap_or(1);
    if step == 0 {
        return Err(TiledError::InvalidSlice("slice step cannot be zero".into()));
    }
    let n = len as isize;
    let (lo, hi, start_def, stop_def) = if step > 0 {
        (0isize, n, 0isize, n)
    } else {
        (-1isize, n - 1, n - 1, -1isize)
    };
    let clamp = |v: isize| v.max(lo).min(hi);
    let norm = |v: Option<isize>, default: isize| match v {
        None => default,
        Some(x) => clamp(if x < 0 { x + n } else { x }),
    };
    Ok((norm(start, start_def), norm(stop, stop_def), step))
}

/// Project a coordinate into the sliced frame, returning `Some(new_index)` if
/// `c` is selected by the normalised `start:stop:step` and `None` otherwise.
/// `new_index = (c - start) / step` (the position of `c` within the slice).
fn project_coord(c: i64, start: isize, stop: isize, step: isize) -> Option<i64> {
    let c = c as isize;
    let projected = if step > 0 {
        if c < start || c >= stop {
            return None;
        }
        let off = c - start;
        if off % step != 0 {
            return None;
        }
        off / step
    } else {
        // step < 0: selected coords run start, start+step, … while > stop.
        if c > start || c <= stop {
            return None;
        }
        let nstep = -step;
        let off = start - c;
        if off % nstep != 0 {
            return None;
        }
        off / nstep
    };
    Some(projected as i64)
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

    // ------------------------------------------------------------------
    // read() partial slicing — numpy basic indexing over COO coordinates.
    // Fixture (3x3): (0,1)->1.5, (2,0)->3.7.
    // Tests are organised by invariant boundary, not narrative scenario.
    // ------------------------------------------------------------------

    // boundary: row range filters out-of-range non-zeros, remaps survivors
    #[tokio::test]
    async fn test_read_row_range_filters_and_remaps() {
        let adapter = make_3x3_adapter();
        // rows [0,2): keeps (0,1)->1.5, drops (2,0)
        let sd = adapter
            .read(&NDSlice::from_numpy_str("0:2,:").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.coords.len(), 2);
        assert_eq!(sd.data.shape, vec![1]);
        let row = i64::from_le_bytes(sd.coords[0].data[0..8].try_into().unwrap());
        let col = i64::from_le_bytes(sd.coords[1].data[0..8].try_into().unwrap());
        assert_eq!((row, col), (0, 1));
        let v = f64::from_le_bytes(sd.data.data[0..8].try_into().unwrap());
        assert!((v - 1.5).abs() < f64::EPSILON);
    }

    // boundary: integer index on an axis drops that axis from the result
    #[tokio::test]
    async fn test_read_column_index_drops_dimension() {
        let adapter = make_3x3_adapter();
        // [:, 0]: keeps (2,0)->3.7, drops (0,1); column dim removed
        let sd = adapter
            .read(&NDSlice::from_numpy_str(":,0").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.coords.len(), 1); // one dim remaining
        assert_eq!(sd.data.shape, vec![1]);
        let row = i64::from_le_bytes(sd.coords[0].data[0..8].try_into().unwrap());
        assert_eq!(row, 2);
        let v = f64::from_le_bytes(sd.data.data[0..8].try_into().unwrap());
        assert!((v - 3.7).abs() < f64::EPSILON);
    }

    // boundary: stepped range remaps coordinates by (coord - start) / step
    #[tokio::test]
    async fn test_read_stepped_rows_remaps_indices() {
        let adapter = make_3x3_adapter();
        // [::2, :]: rows 0 and 2 -> remapped to 0 and 1
        let sd = adapter
            .read(&NDSlice::from_numpy_str("::2,:").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.data.shape, vec![2]);
        let r0 = i64::from_le_bytes(sd.coords[0].data[0..8].try_into().unwrap());
        let r1 = i64::from_le_bytes(sd.coords[0].data[8..16].try_into().unwrap());
        // (0,1) row 0 -> 0 ; (2,0) row 2 -> 1
        assert_eq!((r0, r1), (0, 1));
    }

    // boundary: full integer indexing leaves no coordinate dims (a scalar cell)
    #[tokio::test]
    async fn test_read_full_index_returns_scalar_nonzero() {
        let adapter = make_3x3_adapter();
        // [0,1] selects the stored value at (0,1); no coordinate dims remain
        let sd = adapter
            .read(&NDSlice::from_numpy_str("0,1").unwrap())
            .await
            .unwrap();
        assert!(sd.coords.is_empty());
        assert_eq!(sd.data.shape, vec![1]);
        let v = f64::from_le_bytes(sd.data.data[0..8].try_into().unwrap());
        assert!((v - 1.5).abs() < f64::EPSILON);
    }

    // boundary: indexing an implicit-zero cell yields zero non-zeros
    #[tokio::test]
    async fn test_read_index_at_zero_cell_yields_no_nonzeros() {
        let adapter = make_3x3_adapter();
        // (1,1) is an implicit zero -> no stored non-zero survives
        let sd = adapter
            .read(&NDSlice::from_numpy_str("1,1").unwrap())
            .await
            .unwrap();
        assert!(sd.coords.is_empty());
        assert_eq!(sd.data.shape, vec![0]);
    }

    // boundary: negative index wraps relative to the axis length
    #[tokio::test]
    async fn test_read_negative_index_wraps() {
        let adapter = make_3x3_adapter();
        // [-1, :] -> row 2: keeps (2,0)->3.7, drops dim 0
        let sd = adapter
            .read(&NDSlice::from_numpy_str("-1,:").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.coords.len(), 1);
        assert_eq!(sd.data.shape, vec![1]);
        let col = i64::from_le_bytes(sd.coords[0].data[0..8].try_into().unwrap());
        assert_eq!(col, 0);
    }

    // boundary: a slice with fewer dims than ndim keeps trailing axes whole
    // (numpy `arr[0:2]` == `arr[0:2, :]`)
    #[tokio::test]
    async fn test_read_short_slice_keeps_trailing_axes() {
        let adapter = make_3x3_adapter();
        // "0:2" addresses only dim0; dim1 must stay a full axis (kept + remapped)
        let sd = adapter
            .read(&NDSlice::from_numpy_str("0:2").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.coords.len(), 2, "trailing column axis must be kept");
        assert_eq!(sd.data.shape, vec![1]);
        let row = i64::from_le_bytes(sd.coords[0].data[0..8].try_into().unwrap());
        let col = i64::from_le_bytes(sd.coords[1].data[0..8].try_into().unwrap());
        assert_eq!((row, col), (0, 1)); // (0,1)->5.0 kept, (2,0) dropped
    }

    // boundary: out-of-bounds index errors
    #[tokio::test]
    async fn test_read_out_of_bounds_index_errors() {
        let adapter = make_3x3_adapter();
        let err = adapter
            .read(&NDSlice::from_numpy_str("5,:").unwrap())
            .await
            .unwrap_err();
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
