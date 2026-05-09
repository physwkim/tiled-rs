//! TIFF array adapter.
//!
//! Decodes TIFF (single-page) into a u16 grayscale buffer using the
//! `tiff` crate. Multi-page TIFFs aren't supported here — register such
//! files as a sequence and let the file-sequence adapter stack them.

#![cfg(feature = "tiff")]

use std::path::PathBuf;

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct TiffAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl TiffAdapter {
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let file = std::fs::File::open(&path).map_err(|e| {
            TiledError::Internal(format!("open {}: {e}", path.display()))
        })?;
        let mut decoder = tiff::decoder::Decoder::new(file)
            .map_err(|e| TiledError::Internal(format!("tiff open: {e}")))?;
        let (w, h) = decoder
            .dimensions()
            .map_err(|e| TiledError::Internal(format!("tiff dims: {e}")))?;

        // Pull the first IFD's pixel data. We coerce to u16 (matches what
        // the AreaDetector tooling produces) so downstream serialisers can
        // assume a fixed dtype.
        let result = decoder
            .read_image()
            .map_err(|e| TiledError::Internal(format!("tiff decode: {e}")))?;
        let raw_u16: Vec<u16> = match result {
            tiff::decoder::DecodingResult::U8(v) => v.into_iter().map(u16::from).collect(),
            tiff::decoder::DecodingResult::U16(v) => v,
            tiff::decoder::DecodingResult::U32(v) => v.into_iter().map(|x| x as u16).collect(),
            other => {
                return Err(TiledError::Validation(format!(
                    "unsupported tiff sample format: {other:?}"
                )));
            }
        };
        let mut bytes = Vec::with_capacity(raw_u16.len() * 2);
        for v in &raw_u16 {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let dtype = BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 2);
        let shape = vec![h as usize, w as usize];
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
            specs: vec![Spec::new("tiff")],
        })
    }
}

impl BaseAdapter for TiffAdapter {
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

impl ArrayAdapterRead for TiffAdapter {
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
                        "tiff adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            Ok(self.array.clone())
        })
    }
}
