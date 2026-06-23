//! Zarr (V2 + V3) array adapter using `zarrs` + `zarrs_filesystem`.
//!
//! Reads the array shape, chunk grid, and dtype from the store, then
//! exposes per-chunk reads through `read_block`. `read` retrieves the
//! whole array subset in one shot — fine for small arrays, the caller
//! should prefer `read_block` for large stores.

#![cfg(feature = "zarr")]

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use zarrs::array::Array;
use zarrs::array::ArrayBytes;
use zarrs::array::{ArrayBuilder, DataType, FillValue};
use zarrs::array_subset::ArraySubset;
use zarrs::filesystem::FilesystemStore;

use tiled_core::adapters::{ArrayAdapterRead, ArrayAdapterWrite, BaseAdapter, BoxFuture};
use tiled_core::data_source::Asset;
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

/// Relative path of the array inside a server-managed zarr store. `init_storage`
/// always creates the array here, and the read resolver defaults to it, so a
/// managed zarr round-trips without threading an `array_path` parameter through
/// the catalog. Externally-registered stores can still override it via the
/// `array_path` data-source parameter.
pub const MANAGED_ARRAY_PATH: &str = "/data";

pub struct ZarrAdapter {
    array: Arc<Array<FilesystemStore>>,
    /// Store root + array path, kept so `append` can re-open a *mutable* Array
    /// (`zarrs::Array` is not `Clone` and `set_shape` needs `&mut`); the cached
    /// `array` above is read-only-shared behind an `Arc`.
    store_root: PathBuf,
    array_path: String,
    structure: ArrayStructure,
    dtype: BuiltinDType,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Set only by the resolver (via [`ZarrAdapter::into_writable`]) when the
    /// backing store lives under the server's writable storage. Gates
    /// [`ArrayAdapterRead::as_writable`] so a read-only store can never be
    /// written through this adapter.
    writable: bool,
}

impl ZarrAdapter {
    /// `path` points at the zarr store root (a directory). `array_path`
    /// is the relative path to the array inside the store, e.g. `/data`.
    pub fn from_path(
        store_root: PathBuf,
        array_path: &str,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        let store = Arc::new(
            FilesystemStore::new(&store_root)
                .map_err(|e| TiledError::Internal(format!("zarr store: {e}")))?,
        );
        let array = Array::open(store, array_path)
            .map_err(|e| TiledError::Internal(format!("zarr open: {e}")))?;
        let shape: Vec<usize> = array.shape().iter().map(|&d| d as usize).collect();
        let chunks: Vec<Vec<usize>> = build_chunk_grid(&array, &shape);
        let dtype = parse_data_type(array.data_type())?;
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: array.dimension_names().as_ref().map(|names| {
                names
                    .iter()
                    .map(|n| n.clone().unwrap_or_default())
                    .collect()
            }),
            resizable: Default::default(),
        };
        Ok(Self {
            array: Arc::new(array),
            store_root,
            array_path: array_path.to_string(),
            structure,
            dtype,
            metadata,
            specs: vec![Spec::new("zarr")],
            writable: false,
        })
    }

    /// Mark this adapter writable. The resolver calls this only when the store
    /// is under the catalog's configured writable storage, so
    /// `as_writable().is_some()` ⟹ the store is write-contained.
    pub fn into_writable(mut self) -> Self {
        self.writable = true;
        self
    }

    fn array_subset_for_block(&self, block: &[usize]) -> Result<ArraySubset> {
        if block.len() != self.structure.shape.len() {
            return Err(TiledError::Validation(format!(
                "expected {} block indices, got {}",
                self.structure.shape.len(),
                block.len()
            )));
        }
        let mut start = Vec::with_capacity(block.len());
        let mut shape_inner = Vec::with_capacity(block.len());
        for (axis, (&b, chunks)) in block.iter().zip(self.structure.chunks.iter()).enumerate() {
            if b >= chunks.len() {
                return Err(TiledError::Validation(format!(
                    "block index {b} out of range on axis {axis} ({} chunks)",
                    chunks.len()
                )));
            }
            let offset: usize = chunks[..b].iter().sum();
            start.push(offset as u64);
            shape_inner.push(chunks[b] as u64);
        }
        ArraySubset::new_with_start_shape(start, shape_inner)
            .map_err(|e| TiledError::Validation(format!("zarr subset: {e}")))
    }
}

impl BaseAdapter for ZarrAdapter {
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

impl ArrayAdapterRead for ZarrAdapter {
    fn structure(&self) -> &ArrayStructure {
        &self.structure
    }

    fn as_writable(&self) -> Option<&dyn ArrayAdapterWrite> {
        // Writable only when the resolver opted this store in (under writable
        // storage). The single gate for write-containment.
        if self.writable { Some(self) } else { None }
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        // `retrieve_array_subset` does blocking store I/O + decode; offload it
        // to the blocking pool so it never stalls an async worker thread (S7,
        // matching the HDF5/Sequence adapters).
        let array = self.array.clone();
        let dtype = self.dtype.clone();
        let shape = self.structure.shape.clone();
        let slice = slice.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let subset_shape: Vec<u64> = shape.iter().map(|&d| d as u64).collect();
                let subset = ArraySubset::new_with_shape(subset_shape);
                let bytes = array
                    .retrieve_array_subset(&subset)
                    .map_err(|e| TiledError::Internal(format!("zarr retrieve: {e}")))?;
                let full = DynNDArray::new(bytes_from_array_bytes(bytes)?, dtype, shape);
                full.apply_slice(&slice)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("zarr spawn: {e}")))?
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        // Compute the subset (pure arithmetic) up front, then offload the
        // blocking store I/O + decode to the blocking pool (S7).
        let array = self.array.clone();
        let dtype = self.dtype.clone();
        let slice = slice.clone();
        let subset = self.array_subset_for_block(block);
        Box::pin(async move {
            let subset = subset?;
            tokio::task::spawn_blocking(move || {
                let block_shape: Vec<usize> = subset.shape().iter().map(|&d| d as usize).collect();
                let bytes = array
                    .retrieve_array_subset(&subset)
                    .map_err(|e| TiledError::Internal(format!("zarr retrieve: {e}")))?;
                // Sub-slice within the block (Python zarr.py:114-117).
                DynNDArray::new(bytes_from_array_bytes(bytes)?, dtype, block_shape)
                    .apply_slice(&slice)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("zarr spawn: {e}")))?
        })
    }
}

impl ArrayAdapterWrite for ZarrAdapter {
    fn write<'a>(&'a self, data: DynNDArray) -> BoxFuture<'a, Result<()>> {
        // Whole-array write: `store_array_subset` over the full extent splits
        // the buffer across the real chunk grid, so a managed zarr array is not
        // capped at a single chunk (unlike npy).
        Box::pin(async move {
            if data.shape != self.structure.shape {
                return Err(TiledError::Validation(format!(
                    "zarr write shape {:?} does not match the array shape {:?}",
                    data.shape, self.structure.shape
                )));
            }
            let array = self.array.clone();
            let shape: Vec<u64> = self.structure.shape.iter().map(|&d| d as u64).collect();
            // The body is already in the dtype's native byte order (what the
            // read path also forwards verbatim), which is exactly what
            // `store_array_subset` expects before it re-encodes to the store.
            let bytes = data.data.to_vec();
            store_subset_blocking(array, ArraySubset::new_with_shape(shape), bytes).await
        })
    }

    fn write_block<'a>(
        &'a self,
        data: DynNDArray,
        block: &'a [usize],
    ) -> BoxFuture<'a, Result<()>> {
        // One chunk addressed by `block`; the subset is exactly that chunk's
        // region (same arithmetic `read_block` uses), and `data` must match the
        // chunk's shape — no whole-array sentinel.
        let subset = self.array_subset_for_block(block);
        let array = self.array.clone();
        Box::pin(async move {
            let subset = subset?;
            let expected: Vec<usize> = subset.shape().iter().map(|&d| d as usize).collect();
            if data.shape != expected {
                return Err(TiledError::Validation(format!(
                    "zarr block write shape {:?} does not match the chunk shape {expected:?}",
                    data.shape
                )));
            }
            let bytes = data.data.to_vec();
            store_subset_blocking(array, subset, bytes).await
        })
    }

    fn append<'a>(&'a self, data: DynNDArray, axis: usize) -> BoxFuture<'a, Result<usize>> {
        // Grow the array along `axis` by `data`'s extent on that axis. zarrs has
        // no Clone and `set_shape` needs `&mut`, so re-open a fresh mutable
        // Array from the store, resize + persist metadata, then write the new
        // region. Mirrors upstream tiled PR #802's appendable zarr.
        let store_root = self.store_root.clone();
        let array_path = self.array_path.clone();
        let old_shape = self.structure.shape.clone();
        Box::pin(async move {
            if axis >= old_shape.len() {
                return Err(TiledError::Validation(format!(
                    "append axis {axis} out of range (ndim={})",
                    old_shape.len()
                )));
            }
            if data.shape.len() != old_shape.len() {
                return Err(TiledError::Validation(format!(
                    "append data ndim {} does not match array ndim {}",
                    data.shape.len(),
                    old_shape.len()
                )));
            }
            // Every non-append axis must match the existing extent.
            for (ax, (&d, &o)) in data.shape.iter().zip(old_shape.iter()).enumerate() {
                if ax != axis && d != o {
                    return Err(TiledError::Validation(format!(
                        "append: non-append axis {ax} length {d} does not match array length {o}"
                    )));
                }
            }
            let new_len = old_shape[axis] + data.shape[axis];
            // Appended region starts at the old extent along `axis`; zarrs
            // read-modify-writes any partially filled boundary chunk.
            let start: Vec<u64> = old_shape
                .iter()
                .enumerate()
                .map(|(ax, &o)| if ax == axis { o as u64 } else { 0 })
                .collect();
            let block_shape: Vec<u64> = data.shape.iter().map(|&d| d as u64).collect();
            let mut new_shape_u64: Vec<u64> = old_shape.iter().map(|&d| d as u64).collect();
            new_shape_u64[axis] = new_len as u64;
            let bytes = data.data.to_vec();
            tokio::task::spawn_blocking(move || {
                let store = Arc::new(
                    FilesystemStore::new(&store_root)
                        .map_err(|e| TiledError::Internal(format!("zarr store: {e}")))?,
                );
                let mut array = Array::open(store, &array_path)
                    .map_err(|e| TiledError::Internal(format!("zarr open: {e}")))?;
                array.set_shape(new_shape_u64);
                array
                    .store_metadata()
                    .map_err(|e| TiledError::Internal(format!("zarr store_metadata: {e}")))?;
                let subset = ArraySubset::new_with_start_shape(start, block_shape)
                    .map_err(|e| TiledError::Validation(format!("zarr subset: {e}")))?;
                array
                    .store_array_subset(&subset, bytes)
                    .map_err(|e| TiledError::Internal(format!("zarr store: {e}")))?;
                Ok::<usize, TiledError>(new_len)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("zarr append spawn: {e}")))?
        })
    }
}

/// Store `bytes` into `subset` of `array` on the blocking pool (store I/O +
/// encode). Shared by the whole-array and per-chunk write paths.
async fn store_subset_blocking(
    array: Arc<Array<FilesystemStore>>,
    subset: ArraySubset,
    bytes: Vec<u8>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        array
            .store_array_subset(&subset, bytes)
            .map_err(|e| TiledError::Internal(format!("zarr store: {e}")))
    })
    .await
    .map_err(|e| TiledError::Internal(format!("zarr write spawn: {e}")))?
}

/// Generate a `data_uri` and create a zero-filled zarr store skeleton under
/// `writable_root` for a new internally-managed array node — the zarr analogue
/// of [`crate::init_storage_npy`]. The server, not the client, decides the
/// on-disk location, so a managed register cannot point physical storage at an
/// arbitrary path. Each `path_parts` entry becomes exactly one path component
/// (validated safe — no empty, `.`/`..`, or separator — so the result stays
/// under `writable_root` by construction); the store directory is
/// `<root>/<ancestors>/<key>.zarr` with the array at [`MANAGED_ARRAY_PATH`].
/// Only metadata is written: a zero fill value means reads before the first
/// write return zeros, and the declared chunk grid is preserved so multi-chunk
/// arrays round-trip. Returns the store-root `data_uri` and its (directory)
/// asset.
pub fn init_storage_zarr(
    writable_root: &Path,
    path_parts: &[String],
    structure: &ArrayStructure,
) -> Result<(String, Vec<Asset>)> {
    if !writable_root.is_absolute() {
        return Err(TiledError::Internal(format!(
            "writable storage root {} is not absolute",
            writable_root.display()
        )));
    }
    let dtype = match &structure.data_type {
        DType::Builtin(b) => b.clone(),
        _ => {
            return Err(TiledError::Validation(
                "zarr storage supports only builtin (non-struct) dtypes".into(),
            ));
        }
    };
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
    let (key, ancestors) = path_parts.split_last().expect("non-empty checked above");
    let mut store_root = writable_root.to_path_buf();
    for a in ancestors {
        store_root.push(a);
    }
    store_root.push(format!("{key}.zarr"));
    std::fs::create_dir_all(&store_root).map_err(|e| {
        TiledError::Internal(format!("init_storage mkdir {}: {e}", store_root.display()))
    })?;

    let data_type = to_zarr_data_type(&dtype)?;
    let shape: Vec<u64> = structure.shape.iter().map(|&d| d as u64).collect();
    let chunk_shape = regular_chunk_shape(&structure.shape, &structure.chunks);
    let fill = FillValue::from(vec![0u8; dtype.element_size()]);

    let store = Arc::new(
        FilesystemStore::new(&store_root)
            .map_err(|e| TiledError::Internal(format!("zarr store: {e}")))?,
    );
    let array = ArrayBuilder::new(shape, data_type, chunk_shape.into(), fill)
        .build(store, MANAGED_ARRAY_PATH)
        .map_err(|e| TiledError::Internal(format!("zarr build: {e}")))?;
    array
        .store_metadata()
        .map_err(|e| TiledError::Internal(format!("zarr store_metadata: {e}")))?;

    // `store_root` is under the absolute `writable_root`, so `display()` begins
    // with `/` and yields the `file:///abs/...` form `uri_to_path` expects.
    let data_uri = format!("file://{}", store_root.display());
    let asset = Asset {
        data_uri: data_uri.clone(),
        is_directory: true,
        parameter: Some("data_uri".into()),
        num: None,
        id: None,
    };
    Ok((data_uri, vec![asset]))
}

/// Map a tiled builtin dtype to the zarrs `DataType`. Inverse of
/// [`parse_data_type`]; only the fixed-size numeric/bool types this adapter can
/// round-trip are accepted.
fn to_zarr_data_type(dt: &BuiltinDType) -> Result<DataType> {
    Ok(match (dt.kind, dt.element_size()) {
        (Kind::Boolean, 1) => DataType::Bool,
        (Kind::Integer, 1) => DataType::Int8,
        (Kind::Integer, 2) => DataType::Int16,
        (Kind::Integer, 4) => DataType::Int32,
        (Kind::Integer, 8) => DataType::Int64,
        (Kind::UnsignedInteger, 1) => DataType::UInt8,
        (Kind::UnsignedInteger, 2) => DataType::UInt16,
        (Kind::UnsignedInteger, 4) => DataType::UInt32,
        (Kind::UnsignedInteger, 8) => DataType::UInt64,
        (Kind::Float, 4) => DataType::Float32,
        (Kind::Float, 8) => DataType::Float64,
        _ => {
            return Err(TiledError::Validation(format!(
                "zarr storage: unsupported dtype {}",
                dt.to_numpy_str()
            )));
        }
    })
}

/// Derive a regular (one-size-per-axis) zarr chunk shape from the tiled
/// structure's chunk grid. tiled reports each axis as a list of per-chunk
/// sizes; zarr's regular grid needs a single size per axis, so the first chunk
/// along each axis is the regular size. Falls back to the full extent (a single
/// chunk) when an axis declares no grid, and clamps to ≥1 so a zero-length axis
/// still yields a valid `NonZeroU64`.
fn regular_chunk_shape(shape: &[usize], chunks: &[Vec<usize>]) -> Vec<NonZeroU64> {
    shape
        .iter()
        .enumerate()
        .map(|(axis, &dim)| {
            let regular = chunks
                .get(axis)
                .and_then(|sizes| sizes.first())
                .copied()
                .unwrap_or(dim)
                .max(1) as u64;
            NonZeroU64::new(regular).expect("chunk size clamped to >= 1")
        })
        .collect()
}

fn bytes_from_array_bytes(b: ArrayBytes<'_>) -> Result<Bytes> {
    match b {
        ArrayBytes::Fixed(cow) => Ok(Bytes::copy_from_slice(cow.as_ref())),
        ArrayBytes::Variable(_, _) => Err(TiledError::Validation(
            "variable-length zarr arrays not supported by this adapter".into(),
        )),
    }
}

fn build_chunk_grid(array: &Array<FilesystemStore>, shape: &[usize]) -> Vec<Vec<usize>> {
    let mut grids: Vec<Vec<usize>> = Vec::with_capacity(shape.len());
    if let Some(grid) = array.chunk_grid_shape() {
        // grid is the per-axis number of chunks; combine with chunk shape
        // to recover per-chunk lengths along each axis. Last chunk may be
        // smaller than the regular chunk if shape doesn't divide evenly.
        let chunk_shape = array.chunk_shape(&vec![0; shape.len()]).ok();
        for (axis, dim) in shape.iter().enumerate() {
            let n_chunks = grid[axis] as usize;
            let regular = chunk_shape
                .as_ref()
                .map(|cs| cs[axis].get() as usize)
                .unwrap_or(*dim);
            let mut sizes = vec![regular; n_chunks];
            if let Some(last) = sizes.last_mut() {
                let consumed = regular * (n_chunks - 1);
                if consumed < *dim {
                    *last = *dim - consumed;
                }
            }
            grids.push(sizes);
        }
    } else {
        // No chunk grid declared — fall back to single chunk per axis.
        for &dim in shape {
            grids.push(vec![dim]);
        }
    }
    grids
}

fn parse_data_type(dt: &zarrs::array::DataType) -> Result<BuiltinDType> {
    use zarrs::array::DataType as DT;
    // `retrieve_array_subset` hands back the chunk bytes in HOST-NATIVE order:
    // zarrs' bytes codec reverses the stored endianness to native on decode
    // (`do_encode_or_decode`), and this adapter forwards those bytes verbatim
    // (no `to_le_bytes` normalisation, unlike the HDF5/in-memory adapters). So
    // a multi-byte dtype must be reported with the host's native endianness,
    // not a hardcoded `Little` — the two coincide only on little-endian hosts.
    let native = Endianness::native();
    Ok(match dt {
        DT::Bool => BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1),
        DT::Int8 => BuiltinDType::new(Endianness::NotApplicable, Kind::Integer, 1),
        DT::Int16 => BuiltinDType::new(native, Kind::Integer, 2),
        DT::Int32 => BuiltinDType::new(native, Kind::Integer, 4),
        DT::Int64 => BuiltinDType::new(native, Kind::Integer, 8),
        DT::UInt8 => BuiltinDType::new(Endianness::NotApplicable, Kind::UnsignedInteger, 1),
        DT::UInt16 => BuiltinDType::new(native, Kind::UnsignedInteger, 2),
        DT::UInt32 => BuiltinDType::new(native, Kind::UnsignedInteger, 4),
        DT::UInt64 => BuiltinDType::new(native, Kind::UnsignedInteger, 8),
        DT::Float32 => BuiltinDType::new(native, Kind::Float, 4),
        DT::Float64 => BuiltinDType::new(native, Kind::Float, 8),
        other => {
            return Err(TiledError::Validation(format!(
                "zarr dtype not supported by tiled adapter: {other:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;
    use tiled_core::ndslice::NDSlice;
    use zarrs::array::{ArrayBuilder, DataType, FillValue};

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    #[tokio::test]
    async fn read_block_within_block_slice() {
        // 4x4 f64 array on a 2x2 chunk grid; arr[r][c] = r*4 + c.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FilesystemStore::new(dir.path()).unwrap());
        let array = ArrayBuilder::new(
            vec![4, 4],
            DataType::Float64,
            vec![nz(2), nz(2)].into(),
            FillValue::from(0.0_f64),
        )
        .build(store, "/data")
        .unwrap();
        array.store_metadata().unwrap();
        let elements: Vec<f64> = (0..16).map(|i| i as f64).collect();
        array
            .store_array_subset_elements(&ArraySubset::new_with_shape(vec![4, 4]), &elements)
            .unwrap();

        let adapter =
            ZarrAdapter::from_path(dir.path().to_path_buf(), "/data", serde_json::json!({}))
                .unwrap();
        // 2x2 chunk grid recovered from the store.
        assert_eq!(adapter.structure().chunks, vec![vec![2, 2], vec![2, 2]]);

        // Block [1,1] covers rows 2-3, cols 2-3 → [[10,11],[14,15]].
        // Within-block slice "0,:" selects row 0 of the block → [10, 11].
        let slice = NDSlice::from_numpy_str("0,:").unwrap();
        let result = adapter.read_block(&[1, 1], &slice).await.unwrap();
        assert_eq!(result.shape, vec![2]);
        let floats: Vec<f64> = result
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(floats, vec![10.0, 11.0]);
    }

    #[tokio::test]
    async fn read_full_applies_slice_offloaded() {
        // `read` now runs its blocking store I/O on the blocking pool; this
        // guards that the offload preserves correctness across all chunks +
        // the within-array slice (S7 regression).
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FilesystemStore::new(dir.path()).unwrap());
        let array = ArrayBuilder::new(
            vec![4, 4],
            DataType::Float64,
            vec![nz(2), nz(2)].into(),
            FillValue::from(0.0_f64),
        )
        .build(store, "/data")
        .unwrap();
        array.store_metadata().unwrap();
        let elements: Vec<f64> = (0..16).map(|i| i as f64).collect();
        array
            .store_array_subset_elements(&ArraySubset::new_with_shape(vec![4, 4]), &elements)
            .unwrap();

        let adapter =
            ZarrAdapter::from_path(dir.path().to_path_buf(), "/data", serde_json::json!({}))
                .unwrap();
        // Full read with slice "1,:" → row 1 of the whole 4x4 = [4,5,6,7].
        let slice = NDSlice::from_numpy_str("1,:").unwrap();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![4]);
        let floats: Vec<f64> = result
            .data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(floats, vec![4.0, 5.0, 6.0, 7.0]);
    }

    #[tokio::test]
    async fn uint8_dtype_is_byteorder_agnostic() {
        // numpy reports single-byte dtypes with byte-order '|' (NotApplicable),
        // not '<' (Little).
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FilesystemStore::new(dir.path()).unwrap());
        let array = ArrayBuilder::new(
            vec![2, 2],
            DataType::UInt8,
            vec![nz(2), nz(2)].into(),
            FillValue::from(0u8),
        )
        .build(store, "/data")
        .unwrap();
        array.store_metadata().unwrap();
        array
            .store_array_subset_elements(&ArraySubset::new_with_shape(vec![2, 2]), &[1u8, 2, 3, 4])
            .unwrap();

        let adapter =
            ZarrAdapter::from_path(dir.path().to_path_buf(), "/data", serde_json::json!({}))
                .unwrap();
        match &adapter.structure().data_type {
            DType::Builtin(b) => {
                assert_eq!(b.endianness, Endianness::NotApplicable);
                assert_eq!(b.to_numpy_str(), "|u1");
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multibyte_dtype_reports_native_byte_order() {
        // Zarr/M7: the adapter forwards zarrs' decoded bytes verbatim, and
        // zarrs decodes to host-native order. So the reported endianness must
        // be the host's native order, and a client decoding the emitted bytes
        // with that reported order must recover the original value. (On a
        // little-endian host native == Little; on a big-endian host a revert to
        // a hardcoded `Little` would make this decode produce garbage.)
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FilesystemStore::new(dir.path()).unwrap());
        let array = ArrayBuilder::new(
            vec![1],
            DataType::Float64,
            vec![nz(1)].into(),
            FillValue::from(0.0_f64),
        )
        .build(store, "/data")
        .unwrap();
        array.store_metadata().unwrap();
        let value = 1234.5_f64;
        array
            .store_array_subset_elements(&ArraySubset::new_with_shape(vec![1]), &[value])
            .unwrap();

        let adapter =
            ZarrAdapter::from_path(dir.path().to_path_buf(), "/data", serde_json::json!({}))
                .unwrap();

        let endianness = match &adapter.structure().data_type {
            DType::Builtin(b) => {
                // Reported order is the host's native order, by construction.
                assert_eq!(b.endianness, Endianness::native());
                b.endianness
            }
            other => panic!("expected builtin dtype, got {other:?}"),
        };

        let slice = NDSlice::from_numpy_str("").unwrap();
        let result = adapter.read(&slice).await.unwrap();
        let raw: [u8; 8] = result.data[..8].try_into().unwrap();
        // Decode using the *reported* endianness — must recover the value.
        let decoded = match endianness {
            Endianness::Big => f64::from_be_bytes(raw),
            Endianness::Little => f64::from_le_bytes(raw),
            Endianness::NotApplicable => panic!("f64 must report a byte order"),
        };
        assert_eq!(decoded, value);
    }

    fn f64_structure(shape: Vec<usize>, chunks: Vec<Vec<usize>>) -> ArrayStructure {
        ArrayStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::native(), Kind::Float, 8)),
            chunks,
            shape,
            dims: None,
            resizable: Default::default(),
        }
    }

    fn f64_le(values: &[f64]) -> Bytes {
        Bytes::from(
            values
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect::<Vec<u8>>(),
        )
    }

    fn read_f64(arr: &DynNDArray) -> Vec<f64> {
        arr.data
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn init_storage_creates_store_skeleton_and_resolves_zeros() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = f64_structure(vec![4], vec![vec![2, 2]]);
        let (data_uri, assets) =
            init_storage_zarr(&root_abs, &["grp".into(), "arr".into()], &structure).unwrap();

        // store dir is <root>/grp/arr.zarr; the single asset points at it.
        let store_root = root_abs.join("grp").join("arr.zarr");
        assert!(store_root.is_dir(), "zarr store dir not created");
        assert_eq!(assets.len(), 1);
        assert!(assets[0].is_directory, "zarr asset must be a directory");
        assert_eq!(data_uri, format!("file://{}", store_root.display()));

        // Read back: zero fill, correct shape, and the multi-chunk grid recovered.
        let adapter =
            ZarrAdapter::from_path(store_root, MANAGED_ARRAY_PATH, serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().shape, vec![4]);
        assert_eq!(adapter.structure().chunks, vec![vec![2, 2]]);
        let all = adapter
            .read(&NDSlice::from_numpy_str("").unwrap())
            .await
            .unwrap();
        assert_eq!(read_f64(&all), vec![0.0; 4]);
    }

    #[tokio::test]
    async fn write_persists_multi_chunk_array_gated_by_into_writable() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = f64_structure(vec![4], vec![vec![2, 2]]);
        init_storage_zarr(&root_abs, &["arr".into()], &structure).unwrap();
        let store_root = root_abs.join("arr.zarr");

        // Not opted in → not writable.
        let ro = ZarrAdapter::from_path(
            store_root.clone(),
            MANAGED_ARRAY_PATH,
            serde_json::json!({}),
        )
        .unwrap();
        assert!(
            ro.as_writable().is_none(),
            "must not be writable by default"
        );

        // Opted in → writable; write the whole array (spans both chunks).
        let rw = ZarrAdapter::from_path(store_root, MANAGED_ARRAY_PATH, serde_json::json!({}))
            .unwrap()
            .into_writable();
        let writer = rw.as_writable().expect("writable after into_writable");
        let values = [1.5f64, 2.5, 3.5, 4.5];
        let data = DynNDArray::new(
            f64_le(&values),
            BuiltinDType::new(Endianness::native(), Kind::Float, 8),
            vec![4],
        );
        writer.write(data).await.unwrap();

        let back = rw
            .read(&NDSlice::from_numpy_str("").unwrap())
            .await
            .unwrap();
        assert_eq!(read_f64(&back), values.to_vec());
    }

    #[tokio::test]
    async fn write_block_targets_one_chunk_leaving_others_intact() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        // 4 elements on a 2-chunk grid: chunk 0 = [0,1], chunk 1 = [2,3].
        init_storage_zarr(
            &root_abs,
            &["arr".into()],
            &f64_structure(vec![4], vec![vec![2, 2]]),
        )
        .unwrap();
        let rw = ZarrAdapter::from_path(
            root_abs.join("arr.zarr"),
            MANAGED_ARRAY_PATH,
            serde_json::json!({}),
        )
        .unwrap()
        .into_writable();
        let w = rw.as_writable().unwrap();
        let dt = || BuiltinDType::new(Endianness::native(), Kind::Float, 8);

        // Write only chunk 1 (shape [2]); chunk 0 stays at the zero fill.
        let chunk1 = DynNDArray::new(f64_le(&[7.0, 8.0]), dt(), vec![2]);
        w.write_block(chunk1, &[1usize]).await.unwrap();
        let back = rw
            .read(&NDSlice::from_numpy_str("").unwrap())
            .await
            .unwrap();
        assert_eq!(read_f64(&back), vec![0.0, 0.0, 7.0, 8.0]);

        // Then write chunk 0 (shape [2]); chunk 1 is preserved.
        let chunk0 = DynNDArray::new(f64_le(&[5.0, 6.0]), dt(), vec![2]);
        w.write_block(chunk0, &[0usize]).await.unwrap();
        let back = rw
            .read(&NDSlice::from_numpy_str("").unwrap())
            .await
            .unwrap();
        assert_eq!(read_f64(&back), vec![5.0, 6.0, 7.0, 8.0]);
    }

    #[tokio::test]
    async fn write_block_rejects_wrong_chunk_shape_and_out_of_range_block() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        init_storage_zarr(
            &root_abs,
            &["arr".into()],
            &f64_structure(vec![4], vec![vec![2, 2]]),
        )
        .unwrap();
        let rw = ZarrAdapter::from_path(
            root_abs.join("arr.zarr"),
            MANAGED_ARRAY_PATH,
            serde_json::json!({}),
        )
        .unwrap()
        .into_writable();
        let w = rw.as_writable().unwrap();
        let dt = || BuiltinDType::new(Endianness::native(), Kind::Float, 8);

        // Whole-array data into a single chunk → shape mismatch (chunk is [2]).
        let full = DynNDArray::new(Bytes::from(vec![0u8; 32]), dt(), vec![4]);
        assert!(w.write_block(full, &[0usize]).await.is_err());
        // Block index past the grid (only chunks 0,1 exist).
        let chunk = DynNDArray::new(Bytes::from(vec![0u8; 16]), dt(), vec![2]);
        assert!(w.write_block(chunk, &[2usize]).await.is_err());
    }

    #[test]
    fn init_storage_rejects_traversal_components() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = f64_structure(vec![4], vec![vec![4]]);
        for bad in [
            vec!["..".to_string()],
            vec!["a/b".to_string()],
            vec![String::new()],
            vec!["ok".to_string(), "..".to_string()],
        ] {
            assert!(
                init_storage_zarr(&root_abs, &bad, &structure).is_err(),
                "expected rejection for {bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn append_grows_array_along_axis_and_persists() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        // 1-D length 4, chunk size 2.
        init_storage_zarr(
            &root_abs,
            &["arr".into()],
            &f64_structure(vec![4], vec![vec![2, 2]]),
        )
        .unwrap();
        let store_root = root_abs.join("arr.zarr");
        let dt = || BuiltinDType::new(Endianness::native(), Kind::Float, 8);

        let rw = ZarrAdapter::from_path(
            store_root.clone(),
            MANAGED_ARRAY_PATH,
            serde_json::json!({}),
        )
        .unwrap()
        .into_writable();
        let w = rw.as_writable().unwrap();
        w.write(DynNDArray::new(
            f64_le(&[1.0, 2.0, 3.0, 4.0]),
            dt(),
            vec![4],
        ))
        .await
        .unwrap();

        // Append 2 elements along axis 0 → new length 6.
        let new_len = w
            .append(DynNDArray::new(f64_le(&[5.0, 6.0]), dt(), vec![2]), 0)
            .await
            .unwrap();
        assert_eq!(new_len, 6);

        // A freshly opened adapter reads the grown shape + all values from the store.
        let fresh =
            ZarrAdapter::from_path(store_root, MANAGED_ARRAY_PATH, serde_json::json!({})).unwrap();
        assert_eq!(fresh.structure().shape, vec![6]);
        let all = fresh
            .read(&NDSlice::from_numpy_str("").unwrap())
            .await
            .unwrap();
        assert_eq!(read_f64(&all), vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    }

    #[tokio::test]
    async fn append_rejects_axis_out_of_range_and_mismatched_width() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        // 2x3 on a 1x3 chunk grid (chunk = one row).
        init_storage_zarr(
            &root_abs,
            &["arr".into()],
            &f64_structure(vec![2, 3], vec![vec![1, 1], vec![3]]),
        )
        .unwrap();
        let rw = ZarrAdapter::from_path(
            root_abs.join("arr.zarr"),
            MANAGED_ARRAY_PATH,
            serde_json::json!({}),
        )
        .unwrap()
        .into_writable();
        let w = rw.as_writable().unwrap();
        let dt = || BuiltinDType::new(Endianness::native(), Kind::Float, 8);

        // Axis out of range.
        let row = DynNDArray::new(Bytes::from(vec![0u8; 24]), dt(), vec![1, 3]);
        assert!(w.append(row, 5).await.is_err());
        // Non-append axis width mismatch (width 2 != 3).
        let bad = DynNDArray::new(Bytes::from(vec![0u8; 16]), dt(), vec![1, 2]);
        assert!(w.append(bad, 0).await.is_err());
    }
}
