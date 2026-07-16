//! In-memory COO sparse adapter.
//!
//! Corresponds to `tiled/adapters/sparse.py:COOAdapter`.
//!
//! A `CooAdapter` holds one or more COO blocks keyed by their N-dimensional
//! block index, each storing *block-local* coordinates (matching Python
//! `COOAdapter.__init__`, whose `blocks` dict is block-local). `read` assembles
//! every block into the global coordinate frame — shifting each block's
//! coordinates by its chunk offset `sum(chunks[d][..b[d]])` — before applying
//! any slice; `read_block` returns one block's local coordinates.
//! `from_arrays` builds the common single-block-at-origin case.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::core::adapters::{BaseAdapter, BoxFuture, SparseAdapterRead, SparseData};
use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::{NDSlice, SliceDim};
use crate::core::structures::{SparseLayout, SparseStructure, Spec, StructureFamily};

/// In-memory COO sparse adapter.
///
/// Holds one or more blocks keyed by their N-dimensional block index, each
/// storing *block-local* per-dimension coordinate arrays. `read` assembles all
/// blocks into the global coordinate frame; `read_block` returns one block's
/// local data.
///
/// Python parity: `COOAdapter.__init__` (block-local `blocks` dict) /
/// `from_arrays` / `read_block` / `read` in `tiled/adapters/sparse.py`.
#[derive(Debug)]
pub struct CooAdapter {
    /// Blocks keyed by their N-dim block index, ordered for deterministic
    /// assembly. Coordinates are *block-local* (mirrors Python `self.blocks`).
    blocks: BTreeMap<Vec<usize>, CooBlock>,
    structure: SparseStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

/// One COO block: block-local coordinates and the matching values buffer.
#[derive(Debug, Clone)]
struct CooBlock {
    /// One `Vec<i64>` per dimension, each length `nnz` (block-local indices).
    /// `coords[d][i]` is the index in dimension `d` for non-zero `i`.
    coords: Vec<Vec<i64>>,
    /// Raw non-zero values for this block (`nnz` elements, little-endian).
    data: Bytes,
}

/// Validate per-dimension coord arrays against a data buffer and build a block.
fn make_block(
    coord_arrays: Vec<Vec<i64>>,
    data: Bytes,
    ndim: usize,
    elem: usize,
) -> Result<CooBlock> {
    if coord_arrays.len() != ndim {
        return Err(TiledError::Validation(format!(
            "coord_arrays has {} entries but shape has {ndim} dimensions",
            coord_arrays.len()
        )));
    }
    // nnz is the length of the first dim's coordinate array (or 0 for a
    // scalar/0-dim shape). All per-dim arrays must agree.
    let nnz = coord_arrays.first().map_or(0, Vec::len);
    for (d, c) in coord_arrays.iter().enumerate() {
        if c.len() != nnz {
            return Err(TiledError::Validation(format!(
                "coord_arrays[{d}] has length {} but expected {nnz}",
                c.len()
            )));
        }
    }
    let expected = nnz * elem;
    if data.len() != expected {
        return Err(TiledError::Validation(format!(
            "data has {} bytes but expected {expected} ({nnz} elements × {elem} bytes/elem)",
            data.len()
        )));
    }
    Ok(CooBlock {
        coords: coord_arrays,
        data,
    })
}

impl CooAdapter {
    /// Build a single-block adapter (block at all-zero indices) — the common
    /// case. Mirrors Python `COOAdapter.from_arrays(coords, data, shape)`.
    ///
    /// - `coord_arrays` — one `Vec<i64>` per dimension, each length `nnz`.
    /// - `data_bytes` / `data_dtype` — raw non-zero values (`nnz` elements).
    /// - `shape` — full shape of the sparse array (one entry per dimension).
    /// - `dims` — optional dimension names.
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
        let elem = data_dtype.element_size();
        let block = make_block(coord_arrays, data_bytes, ndim, elem)?;

        // Single chunk covering the whole shape — matches Python's
        //   chunks = tuple((dim,) for dim in shape)
        let chunks: Vec<Vec<usize>> = shape.iter().map(|&s| vec![s]).collect();
        let structure = Self::build_structure(shape, chunks, data_dtype, dims);

        let mut blocks = BTreeMap::new();
        blocks.insert(vec![0usize; ndim], block);
        Ok(Self {
            blocks,
            structure,
            metadata,
            specs,
        })
    }

    /// Build a multi-block adapter from *block-local* coordinates.
    ///
    /// - `chunks` is the chunk grid (`chunks[d]` lists the sizes of the chunks
    ///   along dimension `d`, summing to `shape[d]`).
    /// - Each `(block_index, coord_arrays, data)` gives one block whose
    ///   coordinates are in its block-local frame.
    ///
    /// `read` reassembles the global frame by adding the chunk offset
    /// `sum(chunks[d][..block[d]])` to each coordinate, mirroring Python
    /// `COOAdapter.read` (`tiled/adapters/sparse.py:175-191`); the blocks dict
    /// itself holds block-local coordinates (`COOAdapter.__init__`).
    pub fn from_blocks(
        shape: Vec<usize>,
        chunks: Vec<Vec<usize>>,
        data_dtype: BuiltinDType,
        dims: Option<Vec<String>>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
        blocks: Vec<(Vec<usize>, Vec<Vec<i64>>, Bytes)>,
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

        let elem = data_dtype.element_size();
        let mut map: BTreeMap<Vec<usize>, CooBlock> = BTreeMap::new();
        for (index, coord_arrays, data) in blocks {
            if index.len() != ndim {
                return Err(TiledError::Validation(format!(
                    "block index {index:?} has {} dimensions, expected {ndim}",
                    index.len()
                )));
            }
            for (d, &bi) in index.iter().enumerate() {
                if bi >= chunks[d].len() {
                    return Err(TiledError::Validation(format!(
                        "block index {bi} is out of range for dimension {d} ({} chunks)",
                        chunks[d].len()
                    )));
                }
            }
            let block = make_block(coord_arrays, data, ndim, elem)?;
            if map.insert(index.clone(), block).is_some() {
                return Err(TiledError::Validation(format!(
                    "duplicate block index {index:?}"
                )));
            }
        }

        let structure = Self::build_structure(shape, chunks, data_dtype, dims);
        Ok(Self {
            blocks: map,
            structure,
            metadata,
            specs,
        })
    }

    /// Assemble the COO `SparseStructure` shared by both constructors.
    fn build_structure(
        shape: Vec<usize>,
        chunks: Vec<Vec<usize>>,
        data_dtype: BuiltinDType,
        dims: Option<Vec<String>>,
    ) -> SparseStructure {
        // Coordinates are stored / served as signed int64 little-endian.
        let coord_dtype = BuiltinDType::new(Endianness::Little, Kind::Integer, 8);
        SparseStructure {
            chunks,
            shape,
            data_type: Some(DType::Builtin(data_dtype)),
            coord_data_type: Some(coord_dtype),
            dims,
            resizable: Default::default(),
            layout: SparseLayout::COO,
        }
    }

    /// Element size (bytes) of the value dtype.
    fn elem(&self) -> usize {
        match &self.structure.data_type {
            Some(DType::Builtin(b)) => b.element_size(),
            _ => 0,
        }
    }

    /// Total number of non-zeros across all blocks.
    fn nnz(&self) -> usize {
        let elem = self.elem();
        if elem == 0 {
            return 0;
        }
        self.blocks.values().map(|b| b.data.len() / elem).sum()
    }

    /// Global coordinate offset of `block` per dimension:
    /// `sum(chunks[d][..block[d]])`.
    fn block_offsets(&self, block: &[usize]) -> Vec<i64> {
        block
            .iter()
            .enumerate()
            .map(|(d, &b)| self.structure.chunks[d][..b].iter().sum::<usize>() as i64)
            .collect()
    }

    /// Concatenate every block into the global coordinate frame, shifting each
    /// block's coordinates by its chunk offset. Returns `(global_coords,
    /// data_bytes)` with one coordinate per non-zero across all blocks.
    fn assemble_global(&self) -> (Vec<Vec<i64>>, Vec<u8>) {
        let ndim = self.structure.shape.len();
        let total = self.nnz();
        let mut coords: Vec<Vec<i64>> = vec![Vec::with_capacity(total); ndim];
        let mut data: Vec<u8> = Vec::with_capacity(total * self.elem());
        for (index, block) in &self.blocks {
            let offsets = self.block_offsets(index);
            for d in 0..ndim {
                let off = offsets[d];
                coords[d].extend(block.coords[d].iter().map(|&c| c + off));
            }
            data.extend_from_slice(&block.data);
        }
        (coords, data)
    }

    /// Wrap per-dimension coordinate arrays and a values buffer as `SparseData`.
    fn build_sparse_data(&self, coords: Vec<Vec<i64>>, data_bytes: &[u8]) -> SparseData {
        let coord_dtype = self
            .structure
            .coord_data_type
            .clone()
            .expect("coord_data_type always set by constructors");
        let data_dtype = match &self.structure.data_type {
            Some(DType::Builtin(b)) => b.clone(),
            _ => unreachable!("data_type is always a builtin dtype"),
        };
        let elem = data_dtype.element_size();
        let nnz = data_bytes.len().checked_div(elem).unwrap_or(0);
        let coord_arrays: Vec<DynNDArray> = coords
            .iter()
            .map(|dim_coords| {
                let bytes: Vec<u8> = dim_coords.iter().flat_map(|&v| v.to_le_bytes()).collect();
                DynNDArray::new(Bytes::from(bytes), coord_dtype.clone(), vec![nnz])
            })
            .collect();
        let data = DynNDArray::new(Bytes::copy_from_slice(data_bytes), data_dtype, vec![nnz]);
        SparseData {
            coords: coord_arrays,
            data,
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

    /// Return one block's *block-local* COO data.
    ///
    /// Mirrors Python `read_block`, which returns the block's local coordinates
    /// (`tiled/adapters/sparse.py:169-173`):
    /// ```python
    /// coords, data = self.blocks[block]
    /// arr = sparse.COO(data=data[:], coords=coords[:], shape=shape)
    /// return arr[slice] if slice else arr
    /// ```
    /// An unknown block index is out of range (Python raises `KeyError`).
    fn read_block<'a>(&'a self, block: &'a [usize]) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let ndim = self.structure.shape.len();
            if block.len() != ndim {
                return Err(TiledError::Validation(format!(
                    "expected {ndim} block indices, got {}",
                    block.len()
                )));
            }
            match self.blocks.get(block) {
                Some(b) => Ok(self.build_sparse_data(b.coords.clone(), &b.data)),
                None => Err(TiledError::Validation(format!(
                    "block {block:?} is out of range: no such block in this sparse array"
                ))),
            }
        })
    }

    /// Return the COO data selected by `slice`.
    ///
    /// All blocks are first assembled into the global coordinate frame
    /// (Python `COOAdapter.read`, `tiled/adapters/sparse.py:175-191`). A full
    /// slice returns every non-zero unchanged. A partial slice applies numpy
    /// *basic* indexing, exactly as Python does `arr[slice]`: non-zeros whose
    /// coordinates fall outside the selected region are dropped, surviving
    /// coordinates are remapped into the sliced frame (`(coord - start) /
    /// step`), and `Index` dimensions are removed from the result. `NDSlice`
    /// carries only `Index`/`Slice`/`Ellipsis`, so this is numpy basic indexing
    /// — no boolean or advanced indexing.
    ///
    /// The returned [`SparseData`] holds the surviving coordinates and data;
    /// the grid shape of the sliced result is derived by the caller from the
    /// slice it supplied (`SparseData` carries no shape), the same way the
    /// array read path pairs a slice with its computed shape.
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<SparseData>> {
        Box::pin(async move {
            let (global_coords, data_bytes) = self.assemble_global();
            if slice.is_empty() {
                return Ok(self.build_sparse_data(global_coords, &data_bytes));
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

            // View the assembled non-zeros as coordinate rows (one tuple per
            // non-zero) so the scan reads each non-zero's coordinates together
            // instead of range-indexing parallel column arrays.
            let total = self.nnz();
            let elem = self.elem();
            let rows: Vec<Vec<i64>> = (0..total)
                .map(|i| global_coords.iter().map(|col| col[i]).collect())
                .collect();

            // Scan every non-zero, keeping those selected on all axes and
            // remapping their coordinates into the sliced frame.
            let mut new_coords: Vec<Vec<i64>> = vec![Vec::new(); kept_dims];
            let mut kept: Vec<usize> = Vec::new();
            'nz: for (i, row) in rows.iter().enumerate() {
                let mut projected: Vec<i64> = Vec::with_capacity(kept_dims);
                for (sel, &c) in sels.iter().zip(row.iter()) {
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

            // Filter the assembled data buffer to the kept non-zeros, preserving
            // order, then wrap the remapped coordinates + data as `SparseData`.
            let mut filtered: Vec<u8> = Vec::with_capacity(kept.len() * elem);
            for &i in &kept {
                filtered.extend_from_slice(&data_bytes[i * elem..(i + 1) * elem]);
            }
            Ok(self.build_sparse_data(new_coords, &filtered))
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
    use crate::core::structures::Resizable;
    use bytes::Bytes;

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

    // ------------------------------------------------------------------
    // Multi-block COO — global-offset assembly (review M5).
    // 4x4 grid, 2x2 chunks. Block [0,0] holds local (1,1)=10.0 -> global (1,1);
    // block [1,1] holds local (0,0)=20.0 -> global (2,2). Coordinates stored in
    // each block are *block-local*; `read` adds the chunk offset.
    // ------------------------------------------------------------------

    fn make_multiblock_adapter() -> CooAdapter {
        let bytes = |v: f64| Bytes::from(v.to_le_bytes().to_vec());
        CooAdapter::from_blocks(
            vec![4, 4],
            vec![vec![2, 2], vec![2, 2]],
            f64_dtype(),
            None,
            serde_json::json!({}),
            vec![],
            vec![
                (vec![0, 0], vec![vec![1], vec![1]], bytes(10.0)),
                (vec![1, 1], vec![vec![0], vec![0]], bytes(20.0)),
            ],
        )
        .expect("valid multi-block COO")
    }

    fn read_i64_vec(arr: &DynNDArray) -> Vec<i64> {
        arr.data
            .chunks_exact(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    fn read_f64_vec(arr: &DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
            .collect()
    }

    // boundary: full read assembles block-local coords into the global frame
    #[tokio::test]
    async fn test_multiblock_read_assembles_global_offsets() {
        let adapter = make_multiblock_adapter();
        assert_eq!(adapter.nnz(), 2);
        let sd = adapter.read(&NDSlice::empty()).await.unwrap();
        // BTreeMap order: block [0,0] then [1,1].
        // (1,1)=10.0 then (2,2)=20.0 (local (0,0) + offset (2,2)).
        assert_eq!(read_i64_vec(&sd.coords[0]), vec![1, 2]);
        assert_eq!(read_i64_vec(&sd.coords[1]), vec![1, 2]);
        assert_eq!(read_f64_vec(&sd.data), vec![10.0, 20.0]);
    }

    // boundary: read_block returns the block's *local* coordinates, not global
    #[tokio::test]
    async fn test_multiblock_read_block_returns_local_coords() {
        let adapter = make_multiblock_adapter();
        let sd = adapter.read_block(&[1, 1]).await.unwrap();
        assert_eq!(sd.data.shape, vec![1]);
        assert_eq!(read_i64_vec(&sd.coords[0]), vec![0]); // local row, not 2
        assert_eq!(read_i64_vec(&sd.coords[1]), vec![0]); // local col, not 2
        assert_eq!(read_f64_vec(&sd.data), vec![20.0]);
    }

    // boundary: a grid position with no stored block is out of range
    #[tokio::test]
    async fn test_multiblock_read_block_unknown_errors() {
        let adapter = make_multiblock_adapter();
        let err = adapter.read_block(&[0, 1]).await.unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    // boundary: a slice spanning blocks drops/remaps non-zeros across the grid
    #[tokio::test]
    async fn test_multiblock_read_slice_across_blocks() {
        let adapter = make_multiblock_adapter();
        // rows [2,4): drops global (1,1)=10.0, keeps (2,2)=20.0 remapped row 2->0
        let sd = adapter
            .read(&NDSlice::from_numpy_str("2:4").unwrap())
            .await
            .unwrap();
        assert_eq!(sd.data.shape, vec![1]);
        assert_eq!(read_i64_vec(&sd.coords[0]), vec![0]); // row remapped 2 -> 0
        assert_eq!(read_i64_vec(&sd.coords[1]), vec![2]); // col unchanged
        assert_eq!(read_f64_vec(&sd.data), vec![20.0]);
    }

    // boundary: chunk grid that does not tile the shape is rejected
    #[test]
    fn test_from_blocks_rejects_bad_chunk_sum() {
        let err = CooAdapter::from_blocks(
            vec![4, 4],
            vec![vec![2, 1], vec![2, 2]], // dim 0 chunks sum to 3, not 4
            f64_dtype(),
            None,
            serde_json::json!({}),
            vec![],
            vec![],
        )
        .unwrap_err();
        assert!(err.to_string().contains("sums to"));
    }

    // boundary: a block index outside the chunk grid is rejected
    #[test]
    fn test_from_blocks_rejects_out_of_grid_block() {
        let err = CooAdapter::from_blocks(
            vec![4, 4],
            vec![vec![2, 2], vec![2, 2]],
            f64_dtype(),
            None,
            serde_json::json!({}),
            vec![],
            vec![(
                vec![2, 0],
                vec![vec![0], vec![0]],
                Bytes::from(1.0f64.to_le_bytes().to_vec()),
            )],
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    // The in-memory COO adapter is read-only: the `SparseAdapterRead::as_writable`
    // hook returns `None`, so the write route answers 405 rather than mutating an
    // adapter that has no backing store. Only the managed parquet-backed sparse
    // adapter overrides this hook.
    #[test]
    fn coo_adapter_is_not_writable() {
        let adapter = make_3x3_adapter();
        assert!(
            SparseAdapterRead::as_writable(&adapter).is_none(),
            "the in-memory CooAdapter must report itself read-only"
        );
    }
}
