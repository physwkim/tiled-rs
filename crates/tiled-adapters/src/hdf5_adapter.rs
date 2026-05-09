//! HDF5 array adapter (using `rust-hdf5`).
//!
//! Reads a single dataset by `path/to/dataset` from an HDF5 file. Caller
//! supplies the dataset name (typical AreaDetector convention is
//! `"entry/data/data"`). The adapter exposes the dataset as a chunked
//! array — chunk layout falls back to one chunk per axis if HDF5 reports
//! a contiguous dataset.

#![cfg(feature = "hdf5")]

use std::path::PathBuf;

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct Hdf5Adapter {
    /// Cached array (read once into memory). HDF5 chunked reads on demand
    /// require keeping a file handle live across awaits, which conflicts
    /// with the trait's `Send + 'static` future requirement; the typical
    /// AD frame fits in memory.
    array: DynNDArray,
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
        // Always read as f64 for now — matches AD images and downstream
        // serialisers. Future work: detect dtype from h5 header and
        // preserve it.
        let counts = shape.clone();
        let offsets = vec![0usize; shape.len()];
        let data: Vec<f64> = ds
            .read_slice::<f64>(&offsets, &counts)
            .map_err(|e| TiledError::Internal(format!("hdf5 read: {e}")))?;
        let dtype = BuiltinDType::new(Endianness::Little, Kind::Float, 8);
        let mut bytes = Vec::with_capacity(data.len() * 8);
        for v in &data {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: None,
            resizable: Default::default(),
        };
        let array = DynNDArray::new(Bytes::from(bytes), dtype, shape);
        Ok(Self {
            array,
            structure,
            metadata,
            specs: vec![Spec::new("hdf5")],
        })
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
    fn read<'a>(&'a self, _slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { Ok(self.array.clone()) })
    }
    fn read_block<'a>(
        &'a self,
        block: &'a [usize],
        _slice: &'a NDSlice,
    ) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move {
            for (axis, &b) in block.iter().enumerate() {
                if b != 0 {
                    return Err(TiledError::Validation(format!(
                        "hdf5 adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            Ok(self.array.clone())
        })
    }
}
