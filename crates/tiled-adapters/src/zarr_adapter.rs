//! Zarr (V2 + V3) array adapter using `zarrs` + `zarrs_filesystem`.
//!
//! Reads the array shape, chunk grid, and dtype from the store, then
//! exposes per-chunk reads through `read_block`. `read` retrieves the
//! whole array subset in one shot — fine for small arrays, the caller
//! should prefer `read_block` for large stores.

#![cfg(feature = "zarr")]

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use zarrs::array::Array;
use zarrs::array::ArrayBytes;
use zarrs::array_subset::ArraySubset;
use zarrs::filesystem::FilesystemStore;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct ZarrAdapter {
    array: Arc<Array<FilesystemStore>>,
    structure: ArrayStructure,
    dtype: BuiltinDType,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
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
            structure,
            dtype,
            metadata,
            specs: vec![Spec::new("zarr")],
        })
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

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            let subset_shape: Vec<u64> = self.structure.shape.iter().map(|&d| d as u64).collect();
            let subset = ArraySubset::new_with_shape(subset_shape);
            let bytes = self
                .array
                .retrieve_array_subset(&subset)
                .map_err(|e| TiledError::Internal(format!("zarr retrieve: {e}")))?;
            let full = DynNDArray::new(
                bytes_from_array_bytes(bytes)?,
                self.dtype.clone(),
                self.structure.shape.clone(),
            );
            full.apply_slice(slice)
        })
    }

    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            let subset = self.array_subset_for_block(block)?;
            let block_shape: Vec<usize> = subset.shape().iter().map(|&d| d as usize).collect();
            let bytes = self
                .array
                .retrieve_array_subset(&subset)
                .map_err(|e| TiledError::Internal(format!("zarr retrieve: {e}")))?;
            Ok(DynNDArray::new(
                bytes_from_array_bytes(bytes)?,
                self.dtype.clone(),
                block_shape,
            ))
        })
    }
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
    Ok(match dt {
        DT::Bool => BuiltinDType::new(Endianness::NotApplicable, Kind::Boolean, 1),
        DT::Int8 => BuiltinDType::new(Endianness::Little, Kind::Integer, 1),
        DT::Int16 => BuiltinDType::new(Endianness::Little, Kind::Integer, 2),
        DT::Int32 => BuiltinDType::new(Endianness::Little, Kind::Integer, 4),
        DT::Int64 => BuiltinDType::new(Endianness::Little, Kind::Integer, 8),
        DT::UInt8 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 1),
        DT::UInt16 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 2),
        DT::UInt32 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 4),
        DT::UInt64 => BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 8),
        DT::Float32 => BuiltinDType::new(Endianness::Little, Kind::Float, 4),
        DT::Float64 => BuiltinDType::new(Endianness::Little, Kind::Float, 8),
        other => {
            return Err(TiledError::Validation(format!(
                "zarr dtype not supported by tiled adapter: {other:?}"
            )));
        }
    })
}
