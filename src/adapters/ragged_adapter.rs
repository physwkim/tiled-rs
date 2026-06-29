//! In-memory ragged (variable-length row) array adapter.
//!
//! Corresponds to Python `RaggedAdapter` (`tiled/adapters/ragged.py:39-78`).
//! The adapter trait [`RaggedAdapterRead`] and its [`RaggedData`] return type
//! live in `tiled-core::adapters` (alongside `AnyAdapter::Ragged`); this module
//! provides the concrete in-memory implementation.

use crate::core::adapters::{BaseAdapter, BoxFuture, RaggedAdapterRead, RaggedData};
use crate::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::{NDSlice, SliceDim};
use crate::core::structures::{RaggedStructure, Resizable, Spec, StructureFamily};

// ---------------------------------------------------------------------------
// RaggedAdapter — in-memory implementation
// ---------------------------------------------------------------------------

/// In-memory ragged adapter.
///
/// Stores rows as a `serde_json::Value` (JSON array of arrays).  Float64 is
/// the primary concrete constructor; arbitrary JSON data and an explicit
/// [`RaggedStructure`] can also be supplied via [`RaggedAdapter::new`].
///
/// Corresponds to Python `RaggedAdapter` (`tiled/adapters/ragged.py:39`).
pub struct RaggedAdapter {
    data: serde_json::Value,
    structure: RaggedStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl RaggedAdapter {
    /// Build an adapter from `float64` rows (produces shape `[N, None]`).
    ///
    /// Each inner `Vec<f64>` becomes one variable-length row.  The resulting
    /// [`RaggedStructure`] has:
    /// * `shape = [n_rows, None]`
    /// * `size = total element count`
    /// * `chunks = [[n_rows], None]` (single chunk along axis 0)
    /// * `data_type = little-endian float64`
    ///
    /// Corresponds to Python `RaggedAdapter.from_array` for a list-of-lists
    /// of floats (`tiled/adapters/ragged.py:57-71`).
    pub fn from_rows_f64(
        rows: Vec<Vec<f64>>,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        let n = rows.len();
        let size: usize = rows.iter().map(|r| r.len()).sum();
        let structure = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(n), None],
            size,
            chunks: vec![Some(vec![n]), None],
            dims: None,
            resizable: Resizable::default(),
        };
        let json_rows: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|r| {
                serde_json::Value::Array(
                    r.into_iter()
                        .map(|v| {
                            serde_json::Number::from_f64(v)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .collect(),
                )
            })
            .collect();
        Self {
            data: serde_json::Value::Array(json_rows),
            structure,
            metadata,
            specs,
        }
    }

    /// Build an adapter from an explicit JSON list-of-lists and structure.
    ///
    /// Use this when the dtype is not float64 or the structure is pre-computed.
    pub fn new(
        data: serde_json::Value,
        structure: RaggedStructure,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Self {
        Self {
            data,
            structure,
            metadata,
            specs,
        }
    }
}

impl BaseAdapter for RaggedAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Ragged
    }

    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl RaggedAdapterRead for RaggedAdapter {
    fn structure(&self) -> &RaggedStructure {
        &self.structure
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>> {
        Box::pin(async move {
            // A full slice (no dims, or every dim `:`) is the identity, just
            // like Python `make_ragged_array(self._array, slice=None)`.
            if slice.is_empty() {
                return Ok(RaggedData {
                    json_value: self.data.clone(),
                    structure: self.structure.clone(),
                });
            }

            // Apply `array[slice]` to the list-of-lists, then recompute the
            // structure so it stays consistent with the (possibly
            // dimension-reduced) result — downstream serializers read
            // `structure.shape` (e.g. the ZIP buffer serializer).
            let ndim = self.structure.shape.len();
            let dims = expand_ellipsis(&slice.0, ndim)?;
            let dropped = dims
                .iter()
                .filter(|d| matches!(d, SliceDim::Index(_)))
                .count();
            let result_ndim = ndim - dropped;
            let json_value = apply_dims(&self.data, &dims)?;
            let structure = sliced_structure(&json_value, result_ndim, &self.structure)?;
            Ok(RaggedData {
                json_value,
                structure,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Slicing — numpy/awkward basic indexing over a JSON list-of-lists
// ---------------------------------------------------------------------------

/// Expand a single `Ellipsis` in `dims` into as many full slices as needed to
/// span `ndim`, leaving a dim list with no `Ellipsis`. Mirrors numpy's `...`.
///
/// Errors if more than one `Ellipsis` is present (matching `NDSlice` parsing)
/// or if the non-ellipsis dims already exceed `ndim` ("too many indices").
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
            "slice specifies {provided} dimensions but the ragged array has {ndim}"
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
    Ok(out)
}

/// Recursively apply `dims` (already ellipsis-free) to `value` with numpy
/// *basic* indexing semantics. `NDSlice` only carries `Index`, `Slice`, and
/// `Ellipsis`, so this is numpy basic indexing generalized to ragged
/// (variable-length) inner axes — no boolean/advanced indexing or `newaxis`.
///
/// * `Index` selects one element and drops the axis.
/// * `Slice` keeps the axis, selecting a strided sub-range; on a ragged axis
///   it is applied independently to each row (awkward broadcasts the index
///   across the variable dimension).
/// * When `dims` is exhausted the remaining axes pass through whole (numpy
///   fills trailing axes with `:`).
fn apply_dims(value: &serde_json::Value, dims: &[SliceDim]) -> Result<serde_json::Value> {
    let Some((first, rest)) = dims.split_first() else {
        return Ok(value.clone());
    };
    let arr = value.as_array().ok_or_else(|| {
        TiledError::InvalidSlice("slice has more dimensions than the ragged array".into())
    })?;
    match first {
        SliceDim::Index(i) => {
            let idx = normalize_index(*i, arr.len())?;
            apply_dims(&arr[idx], rest)
        }
        SliceDim::Slice { start, stop, step } => {
            let indices = slice_indices(*start, *stop, *step, arr.len())?;
            let mut out = Vec::with_capacity(indices.len());
            for k in indices {
                out.push(apply_dims(&arr[k], rest)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        // Removed by `expand_ellipsis` before this point.
        SliceDim::Ellipsis => Err(TiledError::InvalidSlice(
            "unexpected Ellipsis after expansion".into(),
        )),
    }
}

/// Normalize a possibly-negative index into `[0, len)`, erroring if out of
/// bounds — awkward raises `IndexError`, which Python surfaces as
/// `RaggedSlicingError`.
fn normalize_index(i: isize, len: usize) -> Result<usize> {
    let n = len as isize;
    let idx = if i < 0 { i + n } else { i };
    if idx < 0 || idx >= n {
        return Err(TiledError::InvalidSlice(format!(
            "index {i} is out of bounds for a ragged axis of length {len}"
        )));
    }
    Ok(idx as usize)
}

/// Indices selected by `start:stop:step` over `len` elements, mirroring
/// Python `range(*slice(start, stop, step).indices(len))`.
fn slice_indices(
    start: Option<isize>,
    stop: Option<isize>,
    step: Option<isize>,
    len: usize,
) -> Result<Vec<usize>> {
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
    let s = norm(start, start_def);
    let t = norm(stop, stop_def);
    let mut out = Vec::new();
    let mut k = s;
    if step > 0 {
        while k < t {
            out.push(k as usize);
            k += step;
        }
    } else {
        while k > t {
            out.push(k as usize);
            k += step;
        }
    }
    Ok(out)
}

/// Recompute a [`RaggedStructure`] consistent with the sliced `value`.
///
/// The leading axis is always a known length (awkward materializes it); every
/// inner axis is variable-length (`None`) — matching how
/// `RaggedStructure.from_array` represents a list-of-lists, where awkward
/// infers `ListOffsetArray` (ragged) for inner dims regardless of current
/// uniformity (`tiled/structures/ragged.py:100-138`). `data_type` is preserved
/// (slicing never changes dtype); `dims` is preserved only when the
/// dimensionality is unchanged.
///
/// A slice that fully reduces to a scalar (e.g. `array[i, j]`, `result_ndim`
/// == 0) cannot be represented by a `RaggedStructure` (its first dim must be a
/// known integer); Python returns a 0-d ragged array, but the in-memory Rust
/// adapter rejects it rather than emit an invalid structure.
fn sliced_structure(
    value: &serde_json::Value,
    result_ndim: usize,
    original: &RaggedStructure,
) -> Result<RaggedStructure> {
    if result_ndim == 0 {
        return Err(TiledError::InvalidSlice(
            "ragged slice reduced to a scalar, which is not representable as a ragged array".into(),
        ));
    }
    let arr = value.as_array().ok_or_else(|| {
        TiledError::InvalidSlice("internal: sliced ragged value is not an array".into())
    })?;
    let outer = arr.len();

    // shape: leading axis known, every inner axis variable-length.
    let mut shape: Vec<Option<usize>> = Vec::with_capacity(result_ndim);
    shape.push(Some(outer));
    shape.resize(result_ndim, None);

    // chunks: single chunk along axis 0; inner axes are null (only axis 0 may
    // be partitioned — enforced by `validate_ragged_structure`).
    let mut chunks: Vec<Option<Vec<usize>>> = Vec::with_capacity(result_ndim);
    chunks.push(Some(vec![outer]));
    chunks.resize(result_ndim, None);

    let dims = if result_ndim == original.shape.len() {
        original.dims.clone()
    } else {
        None
    };

    Ok(RaggedStructure {
        data_type: original.data_type.clone(),
        shape,
        size: json_leaf_count(value),
        chunks,
        dims,
        resizable: original.resizable.clone(),
    })
}

/// Count scalar (non-array) leaves in a nested JSON value — the ragged array's
/// total element count (`RaggedStructure.size`).
pub(crate) fn json_leaf_count(value: &serde_json::Value) -> usize {
    match value.as_array() {
        Some(arr) => arr.iter().map(json_leaf_count).sum(),
        None => 1,
    }
}

// ---------------------------------------------------------------------------
// Validation helper
// ---------------------------------------------------------------------------

/// Validate a `RaggedStructure` against its declared constraints.
///
/// Returns `Err` if any invariant from `RaggedStructure.__post_init__` is
/// violated (corresponds to `tiled/structures/ragged.py:79-98`).
pub fn validate_ragged_structure(s: &RaggedStructure) -> Result<()> {
    if s.shape.is_empty() {
        return Err(TiledError::Validation(
            "ragged array must have at least one dimension".into(),
        ));
    }
    if s.chunks.is_empty() {
        return Err(TiledError::Validation(
            "ragged array must have at least one chunk dimension".into(),
        ));
    }
    if s.shape[0].is_none() {
        return Err(TiledError::Validation(
            "first dimension of a ragged array must be a known integer".into(),
        ));
    }
    if s.chunks[0].is_none() {
        return Err(TiledError::Validation(
            "first chunks dimension must be a known integer partitioning".into(),
        ));
    }
    if s.shape.len() != s.chunks.len() {
        return Err(TiledError::Validation(
            "shape and chunks must have the same number of dimensions".into(),
        ));
    }
    for v in s.chunks.iter().skip(1).flatten() {
        if v.len() != 1 {
            return Err(TiledError::Validation(
                "only the first dimension can be partitioned into chunks".into(),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ndslice::NDSlice;

    // ------------------------------------------------------------------
    // ragged_adapter_structure_from_rows_f64
    // Boundary: N=0 (empty) and N>0 (non-empty)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn ragged_adapter_structure_from_rows_f64_nonempty() {
        let rows = vec![vec![1.0_f64, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        assert_eq!(adapter.structure_family(), StructureFamily::Ragged);
        assert_eq!(adapter.structure().shape, vec![Some(3), None]);
        assert_eq!(adapter.structure().size, 6);
        assert_eq!(adapter.structure().chunks, vec![Some(vec![3]), None]);
        assert!(adapter.metadata().is_object());

        // Verify dtype: little-endian float64
        match &adapter.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.endianness, Endianness::Little);
                assert_eq!(b.kind, Kind::Float);
                assert_eq!(b.itemsize, 8);
            }
            DType::Struct(_) => panic!("expected builtin dtype"),
        }
    }

    #[tokio::test]
    async fn ragged_adapter_structure_from_rows_f64_empty() {
        let adapter = RaggedAdapter::from_rows_f64(vec![], serde_json::json!({}), vec![]);
        assert_eq!(adapter.structure().shape, vec![Some(0), None]);
        assert_eq!(adapter.structure().size, 0);
        assert_eq!(adapter.structure().chunks, vec![Some(vec![0]), None]);
    }

    // ------------------------------------------------------------------
    // ragged_adapter_read_returns_json
    // Boundary: correct list-of-lists JSON output
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn ragged_adapter_read_returns_json_list_of_lists() {
        let rows = vec![vec![1.0_f64, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        let slice = NDSlice::empty();
        let data = adapter.read(&slice).await.unwrap();

        let arr = data
            .json_value
            .as_array()
            .expect("json_value must be array");
        assert_eq!(arr.len(), 3);

        // Row 0: [1.0, 2.0, 3.0]
        let r0: Vec<f64> = arr[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r0, vec![1.0, 2.0, 3.0]);

        // Row 1: [4.0]
        let r1: Vec<f64> = arr[1]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r1, vec![4.0]);

        // Row 2: [5.0, 6.0]
        let r2: Vec<f64> = arr[2]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(r2, vec![5.0, 6.0]);
    }

    #[tokio::test]
    async fn ragged_adapter_read_json_bytes_round_trips() {
        let rows = vec![vec![1.0_f64, 2.0], vec![3.0, 4.0, 5.0]];
        let adapter = RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![]);

        let slice = NDSlice::empty();
        let data = adapter.read(&slice).await.unwrap();

        let bytes = data.to_json_bytes().expect("serialize to JSON bytes");
        let back: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, data.json_value);
    }

    // ------------------------------------------------------------------
    // validate_ragged_structure boundary cases
    // ------------------------------------------------------------------

    #[test]
    fn validate_ragged_structure_valid_passes() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(3), None],
            size: 6,
            chunks: vec![Some(vec![3]), None],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_ok());
    }

    #[test]
    fn validate_ragged_structure_empty_shape_fails() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![],
            size: 0,
            chunks: vec![],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_err());
    }

    #[test]
    fn validate_ragged_structure_first_dim_null_fails() {
        let s = RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![None],
            size: 0,
            chunks: vec![None],
            dims: None,
            resizable: Resizable::default(),
        };
        assert!(validate_ragged_structure(&s).is_err());
    }

    // ------------------------------------------------------------------
    // read() slicing — numpy/awkward basic indexing over the list-of-lists.
    // Fixture rows: [[1,2,3],[4],[5,6]]  (shape [3, None]).
    // Tests are organised by invariant boundary, not narrative scenario.
    // ------------------------------------------------------------------

    fn slicing_fixture() -> RaggedAdapter {
        let rows = vec![vec![1.0_f64, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]];
        RaggedAdapter::from_rows_f64(rows, serde_json::json!({}), vec![])
    }

    fn row_values(v: &serde_json::Value) -> Vec<Vec<f64>> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|row| {
                row.as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_f64().unwrap())
                    .collect()
            })
            .collect()
    }

    fn flat_values(v: &serde_json::Value) -> Vec<f64> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect()
    }

    // boundary: full slice `:` is the identity (no reshaping)
    #[tokio::test]
    async fn read_full_slice_is_identity() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str(":").unwrap())
            .await
            .unwrap();
        assert_eq!(
            row_values(&data.json_value),
            vec![vec![1.0, 2.0, 3.0], vec![4.0], vec![5.0, 6.0]]
        );
        assert_eq!(data.structure.shape, vec![Some(3), None]);
        assert_eq!(data.structure.size, 6);
    }

    // boundary: integer index on axis 0 -> one row, ndim reduced 2 -> 1
    #[tokio::test]
    async fn read_index_axis0_returns_single_row() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("0").unwrap())
            .await
            .unwrap();
        assert_eq!(flat_values(&data.json_value), vec![1.0, 2.0, 3.0]);
        assert_eq!(data.structure.shape, vec![Some(3)]);
        assert_eq!(data.structure.chunks, vec![Some(vec![3])]);
        assert_eq!(data.structure.size, 3);
    }

    // boundary: negative index on axis 0 wraps
    #[tokio::test]
    async fn read_negative_index_axis0() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("-1").unwrap())
            .await
            .unwrap();
        assert_eq!(flat_values(&data.json_value), vec![5.0, 6.0]);
        assert_eq!(data.structure.shape, vec![Some(2)]);
    }

    // boundary: row range on axis 0 keeps the ragged shape
    #[tokio::test]
    async fn read_range_axis0_keeps_ragged() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("0:2").unwrap())
            .await
            .unwrap();
        assert_eq!(
            row_values(&data.json_value),
            vec![vec![1.0, 2.0, 3.0], vec![4.0]]
        );
        assert_eq!(data.structure.shape, vec![Some(2), None]);
        assert_eq!(data.structure.size, 4);
    }

    // boundary: stepped row range selects rows 0 and 2
    #[tokio::test]
    async fn read_stepped_range_axis0() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("::2").unwrap())
            .await
            .unwrap();
        assert_eq!(
            row_values(&data.json_value),
            vec![vec![1.0, 2.0, 3.0], vec![5.0, 6.0]]
        );
        assert_eq!(data.structure.shape, vec![Some(2), None]);
    }

    // boundary: index on the ragged inner axis projects across rows -> 1-D
    #[tokio::test]
    async fn read_index_inner_axis_projects_each_row() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str(":,0").unwrap())
            .await
            .unwrap();
        assert_eq!(flat_values(&data.json_value), vec![1.0, 4.0, 5.0]);
        assert_eq!(data.structure.shape, vec![Some(3)]);
        assert_eq!(data.structure.size, 3);
    }

    // boundary: slice on the ragged inner axis clamps per row
    #[tokio::test]
    async fn read_slice_inner_axis_clamps_per_row() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str(":,0:2").unwrap())
            .await
            .unwrap();
        // row [4] has only one element, so its [0:2] clamps to [4]
        assert_eq!(
            row_values(&data.json_value),
            vec![vec![1.0, 2.0], vec![4.0], vec![5.0, 6.0]]
        );
        assert_eq!(data.structure.shape, vec![Some(3), None]);
        assert_eq!(data.structure.size, 5);
    }

    // boundary: integer index on both axes -> scalar -> rejected
    #[tokio::test]
    async fn read_scalar_reduction_is_rejected() {
        let adapter = slicing_fixture();
        let err = adapter
            .read(&NDSlice::from_numpy_str("0,1").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::InvalidSlice(_)));
    }

    // boundary: out-of-bounds index on axis 0 errors
    #[tokio::test]
    async fn read_out_of_bounds_index_errors() {
        let adapter = slicing_fixture();
        let err = adapter
            .read(&NDSlice::from_numpy_str("9").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::InvalidSlice(_)));
    }

    // boundary: index past a short row on the inner axis errors
    #[tokio::test]
    async fn read_inner_index_past_short_row_errors() {
        let adapter = slicing_fixture();
        // row 1 == [4] has no index 1
        let err = adapter
            .read(&NDSlice::from_numpy_str(":,1").unwrap())
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::InvalidSlice(_)));
    }

    // boundary: empty row range yields a zero-length ragged array
    #[tokio::test]
    async fn read_empty_range_axis0() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("2:2").unwrap())
            .await
            .unwrap();
        assert_eq!(data.json_value, serde_json::json!([]));
        assert_eq!(data.structure.shape, vec![Some(0), None]);
        assert_eq!(data.structure.size, 0);
    }

    // boundary: trailing ellipsis behaves like the bare index
    #[tokio::test]
    async fn read_ellipsis_matches_plain_index() {
        let adapter = slicing_fixture();
        let data = adapter
            .read(&NDSlice::from_numpy_str("1,...").unwrap())
            .await
            .unwrap();
        assert_eq!(flat_values(&data.json_value), vec![4.0]);
        assert_eq!(data.structure.shape, vec![Some(1)]);
    }
}
