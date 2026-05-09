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

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::{NDSlice, SliceDim};
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct Hdf5Adapter {
    path: PathBuf,
    dataset: String,
    dtype: BuiltinDType,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl Hdf5Adapter {
    pub fn from_path(
        path: PathBuf,
        dataset: &str,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        let file = rust_hdf5::H5File::open(&path)
            .map_err(|e| TiledError::Internal(format!("hdf5 open: {e}")))?;
        let ds = file
            .dataset(dataset)
            .map_err(|e| TiledError::Internal(format!("hdf5 dataset {dataset}: {e}")))?;
        let shape: Vec<usize> = ds.shape();
        if shape.is_empty() {
            return Err(TiledError::Validation(
                "hdf5 dataset has zero rank".into(),
            ));
        }
        let element_size = ds.element_size();
        // Probe dtype with a 1-element read; cache for subsequent slice reads.
        let probe_offsets = vec![0usize; shape.len()];
        let probe_counts = vec![1usize; shape.len()];
        let (_probe_bytes, dtype) = read_native(&ds, element_size, &probe_offsets, &probe_counts)?;

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
        })
    }

    fn read_with_slice(&self, slice: &NDSlice) -> Result<DynNDArray> {
        let shape = &self.structure.shape;
        let plan = SlicePlan::from_ndslice(slice, shape)?;
        let file = rust_hdf5::H5File::open(&self.path)
            .map_err(|e| TiledError::Internal(format!("hdf5 reopen: {e}")))?;
        let ds = file
            .dataset(&self.dataset)
            .map_err(|e| TiledError::Internal(format!("hdf5 dataset {}: {e}", self.dataset)))?;
        let (raw, _dtype) = read_native(
            &ds,
            self.dtype.element_size(),
            &plan.offsets,
            &plan.counts,
        )?;
        // Apply striding + integer-index dim reduction in Rust where the
        // HDF5 layer can't (stride>1, Index() collapse).
        let (final_bytes, final_shape) =
            postprocess(raw, &plan, self.dtype.element_size());
        Ok(DynNDArray::new(
            Bytes::from(final_bytes),
            self.dtype.clone(),
            final_shape,
        ))
    }
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
                    let normalised = if *i < 0 {
                        i + axis_len as isize
                    } else {
                        *i
                    };
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
                            "negative-step slices not supported in HDF5 slice plan"
                                .into(),
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
fn postprocess(
    raw: Vec<u8>,
    plan: &SlicePlan,
    element_size: usize,
) -> (Vec<u8>, Vec<usize>) {
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
        let take = idx
            .iter()
            .zip(&plan.strides)
            .all(|(i, s)| i % s == 0);
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

fn read_native(
    ds: &rust_hdf5::H5Dataset,
    element_size: usize,
    offsets: &[usize],
    counts: &[usize],
) -> Result<(Vec<u8>, BuiltinDType)> {
    macro_rules! try_read {
        ($t:ty, $kind:expr) => {{
            match ds.read_slice::<$t>(offsets, counts) {
                Ok(values) => {
                    let mut buf = Vec::with_capacity(values.len() * element_size);
                    for v in &values {
                        buf.extend_from_slice(&v.to_le_bytes());
                    }
                    return Ok((
                        buf,
                        BuiltinDType::new(Endianness::Little, $kind, element_size),
                    ));
                }
                Err(_) => {} // wrong type — try the next candidate
            }
        }};
    }
    match element_size {
        8 => {
            try_read!(f64, Kind::Float);
            try_read!(i64, Kind::Integer);
            try_read!(u64, Kind::UnsignedInteger);
        }
        4 => {
            try_read!(f32, Kind::Float);
            try_read!(i32, Kind::Integer);
            try_read!(u32, Kind::UnsignedInteger);
        }
        2 => {
            try_read!(i16, Kind::Integer);
            try_read!(u16, Kind::UnsignedInteger);
        }
        1 => {
            try_read!(i8, Kind::Integer);
            try_read!(u8, Kind::UnsignedInteger);
        }
        _ => {}
    }
    Err(TiledError::Internal(format!(
        "hdf5 dataset element size {element_size} not supported by tiled-rs adapter"
    )))
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
        Box::pin(async move { self.read_with_slice(slice) })
    }
    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "hdf5 adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            self.read_with_slice(slice)
        })
    }
}
