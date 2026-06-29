//! HDF5 array adapter (using `rust-hdf5`).
//!
//! Reads a single dataset by `path/to/dataset` from an HDF5 file. Caller
//! supplies the dataset name (typical AreaDetector convention is
//! `"entry/data/data"`). The adapter exposes the dataset as a chunked
//! array — chunk layout falls back to one chunk per axis if HDF5 reports
//! a contiguous dataset.
//!
//! Slice-aware reads (upstream tiled PR #1330): `read` and `read_block`
//! translate the requested `NDSlice` to an HDF5 hyperslab `(offsets,
//! counts)` pair so the storage layer only materialises the requested
//! window. Strided slices (`step > 1`) read the full window and stride
//! down in Rust, since `rust-hdf5` doesn't expose hyperslab `stride` —
//! correctness over a one-PR scope win.

#![cfg(feature = "hdf5")]

use std::path::PathBuf;

use bytes::Bytes;

use crate::core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use crate::core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use crate::core::error::{Result, TiledError};
use crate::core::ndslice::{NDSlice, SliceDim};
use crate::core::structures::{ArrayStructure, Spec, StructureFamily};

/// File-locking policy for the HDF5 reader. Mirrors upstream tiled
/// PR #1164 + rust-hdf5 0.2.8's `FileLocking` enum:
///
/// * `Default` — defer to `HDF5_USE_FILE_LOCKING` env var, else
///   acquire the lock (the libhdf5 default).
/// * `Disabled` — skip locking entirely. Useful on filesystems
///   without working flock (some NFS exports, certain FUSE mounts).
/// * `BestEffort` — try to lock; if it fails, proceed without one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Hdf5Locking {
    #[default]
    Default,
    Disabled,
    BestEffort,
}

impl Hdf5Locking {
    fn open(
        self,
        path: &std::path::Path,
    ) -> std::result::Result<rust_hdf5::H5File, rust_hdf5::Hdf5Error> {
        match self {
            Self::Default => rust_hdf5::H5File::open(path),
            Self::Disabled => rust_hdf5::H5File::options().no_locking().open(path),
            Self::BestEffort => rust_hdf5::H5File::options()
                .best_effort_locking()
                .open(path),
        }
    }
}

pub struct Hdf5Adapter {
    path: PathBuf,
    dataset: String,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// `true` for HDF5 scalar (zero-rank) datasets. We promote them to
    /// shape=[1] for the structure but pass empty offsets/counts to
    /// `read_slice` so libhdf5 accepts the call.
    scalar_promoted: bool,
    locking: Hdf5Locking,
}

impl Hdf5Adapter {
    pub fn from_path(path: PathBuf, dataset: &str, metadata: serde_json::Value) -> Result<Self> {
        Self::from_path_with_locking(path, dataset, metadata, Hdf5Locking::default())
    }

    /// Open with an explicit file-locking policy (upstream tiled #1164).
    /// Use `Disabled` on filesystems without working flock; `BestEffort`
    /// when you'd rather still serve the file than fail to open it.
    pub fn from_path_with_locking(
        path: PathBuf,
        dataset: &str,
        metadata: serde_json::Value,
        locking: Hdf5Locking,
    ) -> Result<Self> {
        let file = locking
            .open(&path)
            .map_err(|e| TiledError::Internal(format!("hdf5 open: {e}")))?;
        let ds = file
            .dataset(dataset)
            .map_err(|e| TiledError::Internal(format!("hdf5 dataset {dataset}: {e}")))?;
        // Upstream tiled #944 covers scalar (`shape=()`) and
        // shape-with-zero datasets. We surface zero-rank as a 1-element
        // 1-D array so callers don't need to special-case it. Truly
        // empty arrays (`shape=(...,0,...)`) are reported with their
        // shape but read paths return an empty buffer.
        let raw_shape: Vec<usize> = ds.shape();
        let scalar_promoted = raw_shape.is_empty();
        let shape = if scalar_promoted {
            vec![1usize]
        } else {
            raw_shape.clone()
        };
        // Read the dtype from the dataset's true HDF5 datatype class: Kind
        // (integer / unsigned / float) and signedness come from the stored
        // type via rust-hdf5's `H5Dataset::datatype()` accessor, NOT guessed
        // from the element byte size. (FINDING A-1 closed — the byte-size
        // guess could not tell `u8` from `i8` or `i32` from `f32`.) This
        // needs no data read, so it is correct for empty/zero-axis datasets
        // too.
        let dtype = dtype_from_hdf5(&ds)?;

        // Pull the dataset's HDF5 attributes into the node metadata, mirroring
        // upstream tiled's `metadata.update(get_hdf5_attrs(...))` in
        // `HDF5ArrayAdapter.from_catalog` (hdf5.py:335). Attributes win on key
        // collision, matching Python `dict.update`.
        let metadata = merge_hdf5_attrs(metadata, read_hdf5_attrs(&ds));

        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: None,
            resizable: Default::default(),
        };
        Ok(Self {
            path,
            dataset: dataset.to_string(),
            dtype,
            structure,
            metadata,
            specs: vec![Spec::new("hdf5")],
            scalar_promoted,
            locking,
        })
    }
}

fn read_hdf5_slice(
    path: std::path::PathBuf,
    dataset: String,
    dtype: BuiltinDType,
    shape: Vec<usize>,
    scalar_promoted: bool,
    locking: Hdf5Locking,
    slice: NDSlice,
) -> Result<DynNDArray> {
    if shape.contains(&0) {
        return Ok(DynNDArray::new(Bytes::new(), dtype, shape));
    }
    let file = locking
        .open(&path)
        .map_err(|e| TiledError::Internal(format!("hdf5 reopen: {e}")))?;
    let ds = file
        .dataset(&dataset)
        .map_err(|e| TiledError::Internal(format!("hdf5 dataset {dataset}: {e}")))?;

    if scalar_promoted {
        let raw = read_native(&ds, &dtype, &[], &[])?;
        return Ok(DynNDArray::new(Bytes::from(raw), dtype, vec![1]));
    }

    let plan = SlicePlan::from_ndslice(&slice, &shape)?;
    let raw = read_native(&ds, &dtype, &plan.offsets, &plan.counts)?;
    let (final_bytes, final_shape) = postprocess(raw, &plan, dtype.element_size());
    Ok(DynNDArray::new(
        Bytes::from(final_bytes),
        dtype,
        final_shape,
    ))
}

/// Computed hyperslab + post-processing instructions for a slice read.
#[derive(Debug)]
struct SlicePlan {
    /// HDF5 starts (one per ndim of source dataset).
    offsets: Vec<usize>,
    /// HDF5 counts (one per ndim of source dataset).
    counts: Vec<usize>,
    /// Strides applied in Rust after the hyperslab read. `1` means no
    /// stride. Same length as `counts`.
    strides: Vec<usize>,
    /// `true` for axes that should be removed (Index dim) after read.
    /// Same length as `counts`.
    drop_axis: Vec<bool>,
}

impl SlicePlan {
    fn from_ndslice(slice: &NDSlice, shape: &[usize]) -> Result<Self> {
        let ndim = shape.len();
        let mut dims = expand_ellipsis(slice, ndim)?;
        // Pad missing trailing dims with full slices.
        while dims.len() < ndim {
            dims.push(SliceDim::full());
        }
        if dims.len() > ndim {
            return Err(TiledError::InvalidSlice(format!(
                "slice has {} dims but dataset has {} dims",
                dims.len(),
                ndim
            )));
        }

        let mut offsets = Vec::with_capacity(ndim);
        let mut counts = Vec::with_capacity(ndim);
        let mut strides = Vec::with_capacity(ndim);
        let mut drop_axis = Vec::with_capacity(ndim);

        for (axis, dim) in dims.iter().enumerate() {
            let axis_len = shape[axis];
            match dim {
                SliceDim::Index(i) => {
                    let normalised = if *i < 0 { i + axis_len as isize } else { *i };
                    if normalised < 0 || (normalised as usize) >= axis_len {
                        return Err(TiledError::InvalidSlice(format!(
                            "index {i} out of bounds for axis {axis} (len {axis_len})"
                        )));
                    }
                    offsets.push(normalised as usize);
                    counts.push(1);
                    strides.push(1);
                    drop_axis.push(true);
                }
                SliceDim::Slice { start, stop, step } => {
                    let step = step.unwrap_or(1);
                    if step <= 0 {
                        // Negative-step slices need a separate code
                        // path — punt for now (rare in API usage).
                        return Err(TiledError::InvalidSlice(
                            "negative-step slices not supported in HDF5 slice plan".into(),
                        ));
                    }
                    let start_n = match start {
                        Some(s) if *s < 0 => (s + axis_len as isize).max(0) as usize,
                        Some(s) => (*s as usize).min(axis_len),
                        None => 0,
                    };
                    let stop_n = match stop {
                        Some(s) if *s < 0 => (s + axis_len as isize).max(0) as usize,
                        Some(s) => (*s as usize).min(axis_len),
                        None => axis_len,
                    };
                    let count = stop_n.saturating_sub(start_n);
                    offsets.push(start_n);
                    counts.push(count);
                    strides.push(step as usize);
                    drop_axis.push(false);
                }
                SliceDim::Ellipsis => unreachable!("expanded above"),
            }
        }

        Ok(SlicePlan {
            offsets,
            counts,
            strides,
            drop_axis,
        })
    }
}

fn expand_ellipsis(slice: &NDSlice, ndim: usize) -> Result<Vec<SliceDim>> {
    let mut out = Vec::with_capacity(ndim);
    let n_ellipsis = slice
        .0
        .iter()
        .filter(|d| matches!(d, SliceDim::Ellipsis))
        .count();
    if n_ellipsis > 1 {
        return Err(TiledError::InvalidSlice(
            "more than one ellipsis in slice".into(),
        ));
    }
    let non_ellipsis_count = slice.0.len() - n_ellipsis;
    if non_ellipsis_count > ndim {
        return Err(TiledError::InvalidSlice(format!(
            "slice has more non-ellipsis dims ({non_ellipsis_count}) than dataset ndim ({ndim})"
        )));
    }
    let fill = ndim - non_ellipsis_count;
    for d in &slice.0 {
        if matches!(d, SliceDim::Ellipsis) {
            for _ in 0..fill {
                out.push(SliceDim::full());
            }
        } else {
            out.push(d.clone());
        }
    }
    Ok(out)
}

/// Stride down + collapse Index dims after the HDF5 read.
fn postprocess(raw: Vec<u8>, plan: &SlicePlan, element_size: usize) -> (Vec<u8>, Vec<usize>) {
    // If every stride is 1, just drop integer-indexed dims — no copy needed.
    if plan.strides.iter().all(|&s| s == 1) {
        let final_shape: Vec<usize> = plan
            .counts
            .iter()
            .zip(&plan.drop_axis)
            .filter_map(|(c, drop)| if *drop { None } else { Some(*c) })
            .collect();
        return (raw, final_shape);
    }

    // Strided: walk the raw buffer in row-major order, picking elements
    // whose per-axis index is a multiple of stride.
    let strided_counts: Vec<usize> = plan
        .counts
        .iter()
        .zip(&plan.strides)
        .map(|(c, s)| c.div_ceil(*s))
        .collect();
    let mut out = Vec::with_capacity(strided_counts.iter().product::<usize>() * element_size);
    let mut idx = vec![0usize; plan.counts.len()];
    let total: usize = plan.counts.iter().product();
    let mut linear = 0usize;
    while linear < total {
        // Are all axes on a stride boundary?
        let take = idx.iter().zip(&plan.strides).all(|(i, s)| i % s == 0);
        if take {
            let start = linear * element_size;
            out.extend_from_slice(&raw[start..start + element_size]);
        }
        // Advance index in row-major order.
        for axis in (0..idx.len()).rev() {
            idx[axis] += 1;
            if idx[axis] < plan.counts[axis] {
                break;
            }
            idx[axis] = 0;
        }
        linear += 1;
    }
    let final_shape: Vec<usize> = strided_counts
        .iter()
        .zip(&plan.drop_axis)
        .filter_map(|(c, drop)| if *drop { None } else { Some(*c) })
        .collect();
    (out, final_shape)
}

/// Map the dataset's stored HDF5 datatype to a numpy-style [`BuiltinDType`].
///
/// `Kind` and signedness come from the datatype CLASS
/// (`DatatypeMessage::FixedPoint { signed, .. }` / `FloatingPoint`), not from
/// the element byte size — this is what closes FINDING A-1 (the byte-size
/// guess could not distinguish `u8` from `i8` or `i32` from `f32`). The
/// reported endianness is `Little` (or `NotApplicable` for single-byte
/// types) because [`read_native`] normalises every value to little-endian
/// via `to_le_bytes`; the stored byte order is consumed by `read_slice`
/// during the read itself.
fn dtype_from_hdf5(ds: &rust_hdf5::H5Dataset) -> Result<BuiltinDType> {
    use rust_hdf5::DatatypeMessage;
    let datatype = ds
        .datatype()
        .map_err(|e| TiledError::Internal(format!("hdf5 datatype: {e}")))?;
    let element_size = ds.element_size();
    let kind = match datatype {
        DatatypeMessage::FixedPoint { signed: true, .. } => Kind::Integer,
        DatatypeMessage::FixedPoint { signed: false, .. } => Kind::UnsignedInteger,
        DatatypeMessage::FloatingPoint { .. } => Kind::Float,
        other => {
            return Err(TiledError::Internal(format!(
                "hdf5 datatype {other:?} not supported by tiled-rs adapter"
            )));
        }
    };
    // Single-byte dtypes are byte-order agnostic — numpy reports '|'.
    let endianness = if element_size == 1 {
        Endianness::NotApplicable
    } else {
        Endianness::Little
    };
    Ok(BuiltinDType::new(endianness, kind, element_size))
}

/// Read a dataset's HDF5 attributes into a JSON object, mirroring upstream
/// tiled's `get_hdf5_attrs` (hdf5.py:70-87): `dict(node.attrs)` with bytes
/// decoded to `str`.
///
/// The element type of each attribute is read from its HDF5 datatype class via
/// [`H5Attribute::datatype`](rust_hdf5::H5Attribute::datatype) (added in
/// rust-hdf5 0.2.20), NOT guessed from the byte width — the same reasoning that
/// closed FINDING A-1 for the dataset dtype: a 1-byte attribute could be `u8`
/// or `i8`, a 4-byte one `i32` or `f32`. Scalar integer / float / string
/// attributes are converted to the matching `serde_json::Value`. Non-scalar
/// (compound, enum, array) or otherwise unrepresentable attributes are skipped
/// rather than guessed — omitting an attribute is safe, misreading it is not.
///
/// Failure to *enumerate* attributes yields an empty object; a per-attribute
/// read failure drops that one attribute (best-effort, matching the tolerance
/// of Python's `dict(attrs)`).
fn read_hdf5_attrs(ds: &rust_hdf5::H5Dataset) -> serde_json::Map<String, serde_json::Value> {
    use rust_hdf5::DatatypeMessage;
    let mut out = serde_json::Map::new();
    let Ok(names) = ds.attr_names() else {
        return out;
    };
    for name in names {
        let Ok(attr) = ds.attr(&name) else { continue };
        let Ok(datatype) = attr.datatype() else {
            continue;
        };
        let value: Option<serde_json::Value> = match datatype {
            DatatypeMessage::FixedPoint {
                signed: true, size, ..
            } => match size {
                1 => attr.read_numeric::<i8>().ok().map(Into::into),
                2 => attr.read_numeric::<i16>().ok().map(Into::into),
                4 => attr.read_numeric::<i32>().ok().map(Into::into),
                8 => attr.read_numeric::<i64>().ok().map(Into::into),
                _ => None,
            },
            DatatypeMessage::FixedPoint {
                signed: false,
                size,
                ..
            } => match size {
                1 => attr.read_numeric::<u8>().ok().map(Into::into),
                2 => attr.read_numeric::<u16>().ok().map(Into::into),
                4 => attr.read_numeric::<u32>().ok().map(Into::into),
                8 => attr.read_numeric::<u64>().ok().map(Into::into),
                _ => None,
            },
            // NaN / ±inf have no JSON number representation, so `from_f64`
            // returns `None` and the attribute is dropped rather than emitted
            // as a misleading `null`.
            DatatypeMessage::FloatingPoint { size, .. } => match size {
                4 => attr
                    .read_numeric::<f32>()
                    .ok()
                    .and_then(|v| serde_json::Number::from_f64(v as f64))
                    .map(serde_json::Value::Number),
                8 => attr
                    .read_numeric::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(serde_json::Value::Number),
                _ => None,
            },
            DatatypeMessage::FixedString { .. } | DatatypeMessage::VarLenString { .. } => {
                attr.read_string().ok().map(serde_json::Value::String)
            }
            // Compound / enum / array attributes are not representable as a
            // scalar JSON value — skip them.
            _ => None,
        };
        if let Some(v) = value {
            out.insert(name, v);
        }
    }
    out
}

/// Merge HDF5 attributes into the node metadata, mirroring Python
/// `metadata.update(attrs)`: attributes overwrite same-named keys in the base
/// metadata. An empty attribute set leaves `metadata` untouched; non-object
/// metadata (e.g. `null`) is replaced by the attribute object.
fn merge_hdf5_attrs(
    metadata: serde_json::Value,
    attrs: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    if attrs.is_empty() {
        return metadata;
    }
    match metadata {
        serde_json::Value::Object(mut base) => {
            for (k, v) in attrs {
                base.insert(k, v);
            }
            serde_json::Value::Object(base)
        }
        _ => serde_json::Value::Object(attrs),
    }
}

/// Read raw element bytes for a hyperslab using the dataset's KNOWN element
/// type (from [`dtype_from_hdf5`]). Reading with the correct type — rather
/// than guessing by byte size — is required: `read_slice::<T>` converts the
/// stored values to `T`, so reading an `i64` dataset as `f64` would corrupt
/// it. Every value is emitted little-endian (`to_le_bytes`) to match the
/// endianness reported by `dtype_from_hdf5`.
fn read_native(
    ds: &rust_hdf5::H5Dataset,
    dtype: &BuiltinDType,
    offsets: &[usize],
    counts: &[usize],
) -> Result<Vec<u8>> {
    macro_rules! read_as {
        ($t:ty) => {{
            let values = ds
                .read_slice::<$t>(offsets, counts)
                .map_err(|e| TiledError::Internal(format!("hdf5 read: {e}")))?;
            let mut buf = Vec::with_capacity(values.len() * dtype.element_size());
            for v in &values {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Ok(buf)
        }};
    }
    match (dtype.kind, dtype.element_size()) {
        (Kind::Float, 8) => read_as!(f64),
        (Kind::Float, 4) => read_as!(f32),
        (Kind::Integer, 8) => read_as!(i64),
        (Kind::Integer, 4) => read_as!(i32),
        (Kind::Integer, 2) => read_as!(i16),
        (Kind::Integer, 1) => read_as!(i8),
        (Kind::UnsignedInteger, 8) => read_as!(u64),
        (Kind::UnsignedInteger, 4) => read_as!(u32),
        (Kind::UnsignedInteger, 2) => read_as!(u16),
        (Kind::UnsignedInteger, 1) => read_as!(u8),
        (kind, size) => Err(TiledError::Internal(format!(
            "hdf5 dtype {kind:?} of {size} bytes not supported by tiled-rs adapter"
        ))),
    }
}

impl BaseAdapter for Hdf5Adapter {
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

impl ArrayAdapterRead for Hdf5Adapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        let path = self.path.clone();
        let dataset = self.dataset.clone();
        let dtype = self.dtype.clone();
        let shape = self.structure.shape.clone();
        let scalar_promoted = self.scalar_promoted;
        let locking = self.locking;
        let slice = slice.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                read_hdf5_slice(path, dataset, dtype, shape, scalar_promoted, locking, slice)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("hdf5 spawn: {e}")))?
        })
    }
    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        let path = self.path.clone();
        let dataset = self.dataset.clone();
        let dtype = self.dtype.clone();
        let shape = self.structure.shape.clone();
        let scalar_promoted = self.scalar_promoted;
        let locking = self.locking;
        let slice = slice.clone();
        let block = block.to_vec();
        Box::pin(async move {
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "hdf5 adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            tokio::task::spawn_blocking(move || {
                read_hdf5_slice(path, dataset, dtype, shape, scalar_promoted, locking, slice)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("hdf5 spawn: {e}")))?
        })
    }
}

#[cfg(test)]
mod dtype_class {
    //! Regression tests for HDF5 dtype Kind / signedness detection
    //! (FINDING A-1, CLOSED).
    //!
    //! `dtype_from_hdf5` reads the dataset's true datatype CLASS via
    //! rust-hdf5's `H5Dataset::datatype()` accessor, so the reported `Kind`
    //! and signedness reflect the stored type — `u8` is not confused with
    //! `i8`, nor `i64`/`i32` with `f64`/`f32`. Before the accessor existed,
    //! `read_native` guessed the type by element byte size and mislabelled
    //! these; these tests asserted the correct Kind and used to fail.
    use std::path::PathBuf;

    use super::*;

    fn write_dataset<T: rust_hdf5::types::H5Type>(
        name: &str,
        data: &[T],
    ) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dt.h5");
        let file = rust_hdf5::H5File::create(&path).unwrap();
        let ds = file
            .new_dataset::<T>()
            .shape([data.len()])
            .create(name)
            .unwrap();
        ds.write_raw(data).unwrap();
        // Drop the handles so the adapter can reopen the flushed file.
        drop(ds);
        drop(file);
        (dir, path)
    }

    fn adapter_kind(path: PathBuf, dataset: &str) -> Kind {
        let adapter = Hdf5Adapter::from_path(path, dataset, serde_json::json!({})).unwrap();
        match &adapter.structure().data_type {
            DType::Builtin(b) => b.kind,
            other => panic!("expected builtin dtype, got {other:?}"),
        }
    }

    /// A `uint8` dataset must report `Kind::UnsignedInteger` (not signed `i8`,
    /// which the old byte-size guess picked first for 1-byte elements).
    #[test]
    fn uint8_dataset_reports_unsigned_kind() {
        let (_dir, path) = write_dataset::<u8>("u8", &[1u8, 2, 3]);
        assert_eq!(adapter_kind(path, "u8"), Kind::UnsignedInteger);
    }

    /// An `int64` dataset must report `Kind::Integer` (not `f64`, which the
    /// old byte-size guess picked first for 8-byte elements).
    #[test]
    fn int64_dataset_reports_integer_kind() {
        let (_dir, path) = write_dataset::<i64>("i64", &[1i64, 2, 3]);
        assert_eq!(adapter_kind(path, "i64"), Kind::Integer);
    }

    /// An `int32` dataset must report `Kind::Integer` (not `f32`); confirms
    /// the 4-byte int-vs-float class is resolved from the datatype, not size.
    #[test]
    fn int32_dataset_reports_integer_kind() {
        let (_dir, path) = write_dataset::<i32>("i32", &[1i32, 2, 3]);
        assert_eq!(adapter_kind(path, "i32"), Kind::Integer);
    }

    /// A `uint16` dataset must report `Kind::UnsignedInteger` (not signed
    /// `i16`).
    #[test]
    fn uint16_dataset_reports_unsigned_kind() {
        let (_dir, path) = write_dataset::<u16>("u16", &[1u16, 2, 3]);
        assert_eq!(adapter_kind(path, "u16"), Kind::UnsignedInteger);
    }

    /// A genuine `f64` dataset still reports `Kind::Float`.
    #[test]
    fn float64_dataset_reports_float_kind() {
        let (_dir, path) = write_dataset::<f64>("f64", &[1.0f64, 2.0, 3.0]);
        assert_eq!(adapter_kind(path, "f64"), Kind::Float);
    }

    /// Write an `i32` dataset carrying one attribute of each scalar class
    /// (string / signed int / unsigned int / float) so the attribute-reading
    /// path can be exercised end to end.
    fn write_dataset_with_attrs(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attrs.h5");
        let file = rust_hdf5::H5File::create(&path).unwrap();
        let ds = file.new_dataset::<i32>().shape([3]).create(name).unwrap();
        ds.write_raw(&[10i32, 20, 30]).unwrap();
        ds.new_attr::<rust_hdf5::types::VarLenUnicode>()
            .shape(())
            .create("units")
            .unwrap()
            .write_string("meters")
            .unwrap();
        ds.new_attr::<i32>()
            .shape(())
            .create("count")
            .unwrap()
            .write_numeric(&42i32)
            .unwrap();
        ds.new_attr::<u16>()
            .shape(())
            .create("flags")
            .unwrap()
            .write_numeric(&7u16)
            .unwrap();
        ds.new_attr::<f64>()
            .shape(())
            .create("scale")
            .unwrap()
            .write_numeric(&1.5f64)
            .unwrap();
        drop(ds);
        drop(file);
        (dir, path)
    }

    /// Dataset attributes are merged into the node metadata with the JSON type
    /// dictated by each attribute's HDF5 datatype class — string → string,
    /// signed/unsigned int → integer, float → float (parity with Python
    /// `get_hdf5_attrs`). The integer kinds must NOT round-trip as JSON floats.
    #[test]
    fn dataset_attributes_are_merged_into_metadata() {
        let (_dir, path) = write_dataset_with_attrs("img");
        let adapter = Hdf5Adapter::from_path(path, "img", serde_json::json!({})).unwrap();
        let meta = adapter.metadata();
        assert_eq!(meta["units"], serde_json::json!("meters"));
        assert_eq!(meta["count"], serde_json::json!(42));
        assert_eq!(meta["flags"], serde_json::json!(7));
        assert_eq!(meta["scale"], serde_json::json!(1.5));
        assert!(meta["count"].is_i64(), "signed int attr must be a JSON int");
        assert!(
            meta["flags"].is_u64(),
            "unsigned int attr must be a JSON int"
        );
        assert!(meta["scale"].is_f64(), "float attr must be a JSON float");
    }

    /// On a key collision the HDF5 attribute wins over the passed-in node
    /// metadata (Python `metadata.update(get_hdf5_attrs(...))`), while
    /// unrelated base keys survive.
    #[test]
    fn dataset_attributes_overwrite_base_metadata() {
        let (_dir, path) = write_dataset_with_attrs("img");
        let base = serde_json::json!({"units": "PLACEHOLDER", "source": "catalog"});
        let adapter = Hdf5Adapter::from_path(path, "img", base).unwrap();
        let meta = adapter.metadata();
        assert_eq!(meta["units"], serde_json::json!("meters"));
        assert_eq!(meta["source"], serde_json::json!("catalog"));
    }

    /// A dataset with no attributes leaves the passed-in metadata untouched.
    #[test]
    fn dataset_without_attributes_preserves_base_metadata() {
        let (_dir, path) = write_dataset::<i64>("i64", &[1i64, 2, 3]);
        let base = serde_json::json!({"source": "catalog"});
        let adapter = Hdf5Adapter::from_path(path, "i64", base.clone()).unwrap();
        assert_eq!(adapter.metadata(), &base);
    }
}
