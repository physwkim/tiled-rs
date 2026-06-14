//! TIFF array adapter.
//!
//! Decodes TIFF (single-page) into a typed buffer using the `tiff`
//! crate. Multi-page TIFFs aren't supported here — register such
//! files as a sequence and let the file-sequence adapter stack them.
//!
//! Grayscale TIFFs surface as `[h, w]`; RGB/RGBA/CMYK as
//! `[h, w, channels]` (mirrors upstream tiled #143 — RGB TIFFs were
//! previously coerced to grayscale, dropping channel data).

#![cfg(feature = "tiff")]

use std::path::PathBuf;

use bytes::Bytes;
use tiff::ColorType;

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
        let file = std::fs::File::open(&path)
            .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
        let mut decoder = tiff::decoder::Decoder::new(file)
            .map_err(|e| TiledError::Internal(format!("tiff open: {e}")))?;
        let (w, h) = decoder
            .dimensions()
            .map_err(|e| TiledError::Internal(format!("tiff dims: {e}")))?;
        let color = decoder
            .colortype()
            .map_err(|e| TiledError::Internal(format!("tiff colortype: {e}")))?;
        let channels = match color {
            ColorType::Gray(_) => 1usize,
            ColorType::RGB(_) => 3,
            ColorType::RGBA(_) => 4,
            ColorType::CMYK(_) => 4,
            other => {
                return Err(TiledError::Validation(format!(
                    "tiff color type {other:?} is not supported"
                )));
            }
        };

        let result = decoder
            .read_image()
            .map_err(|e| TiledError::Internal(format!("tiff decode: {e}")))?;
        // Encode native sample width per pixel — preserve dtype rather
        // than uniformly coercing to u16 (which loses precision for f32
        // micrographs and overflows for u32 thermal sensors).
        let (bytes, dtype): (Vec<u8>, BuiltinDType) = match result {
            tiff::decoder::DecodingResult::U8(v) => (
                v,
                BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 1),
            ),
            tiff::decoder::DecodingResult::U16(v) => {
                let mut buf = Vec::with_capacity(v.len() * 2);
                for s in &v {
                    buf.extend_from_slice(&s.to_le_bytes());
                }
                (
                    buf,
                    BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 2),
                )
            }
            tiff::decoder::DecodingResult::U32(v) => {
                let mut buf = Vec::with_capacity(v.len() * 4);
                for s in &v {
                    buf.extend_from_slice(&s.to_le_bytes());
                }
                (
                    buf,
                    BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, 4),
                )
            }
            tiff::decoder::DecodingResult::F32(v) => {
                let mut buf = Vec::with_capacity(v.len() * 4);
                for s in &v {
                    buf.extend_from_slice(&s.to_le_bytes());
                }
                (buf, BuiltinDType::new(Endianness::Little, Kind::Float, 4))
            }
            tiff::decoder::DecodingResult::F64(v) => {
                let mut buf = Vec::with_capacity(v.len() * 8);
                for s in &v {
                    buf.extend_from_slice(&s.to_le_bytes());
                }
                (buf, BuiltinDType::new(Endianness::Little, Kind::Float, 8))
            }
            other => {
                return Err(TiledError::Validation(format!(
                    "unsupported tiff sample format: {other:?}"
                )));
            }
        };
        let shape: Vec<usize> = if channels > 1 {
            vec![h as usize, w as usize, channels]
        } else {
            vec![h as usize, w as usize]
        };
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
    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<DynNDArray>> {
        Box::pin(async move { self.array.apply_slice(slice) })
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
                        "tiff adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            self.array.apply_slice(slice)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tiled_core::ndslice::NDSlice;

    /// Build a minimal grayscale u8 TIFF with w=4, h=3, values 0..11.
    fn make_tiff_3x4_gray8() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let cursor = Cursor::new(&mut buf);
            let mut encoder = tiff::encoder::TiffEncoder::new(cursor).unwrap();
            let data: Vec<u8> = (0..12u8).collect();
            encoder
                .write_image::<tiff::encoder::colortype::Gray8>(4, 3, &data)
                .unwrap();
        }
        buf
    }

    #[tokio::test]
    async fn read_with_slice_returns_subarray() {
        let tiff_bytes = make_tiff_3x4_gray8();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &tiff_bytes).unwrap();
        let adapter =
            TiffAdapter::from_path(tmp.path().to_path_buf(), serde_json::json!({})).unwrap();
        // arr shape is [H=3, W=4] for grayscale
        // arr[1:3, 1:3] → rows 1-2, cols 1-2 → [5,6,9,10]
        let slice = NDSlice::from_numpy_str("1:3,1:3").unwrap();
        let result = adapter.read(&slice).await.unwrap();
        assert_eq!(result.shape, vec![2, 2]);
        assert_eq!(&result.data[..], &[5u8, 6, 9, 10]);
    }

    #[tokio::test]
    async fn read_block_with_slice_returns_subarray() {
        let tiff_bytes = make_tiff_3x4_gray8();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &tiff_bytes).unwrap();
        let adapter =
            TiffAdapter::from_path(tmp.path().to_path_buf(), serde_json::json!({})).unwrap();
        // arr[0, :] → row 0 = [0,1,2,3]
        let slice = NDSlice::from_numpy_str("0,:").unwrap();
        let result = adapter.read_block(&[0, 0], &slice).await.unwrap();
        assert_eq!(result.shape, vec![4]);
        assert_eq!(&result.data[..], &[0u8, 1, 2, 3]);
    }
}
