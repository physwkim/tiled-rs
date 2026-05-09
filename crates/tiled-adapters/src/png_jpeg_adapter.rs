//! PNG / JPEG (and other `image` crate–supported) adapter.
//!
//! Decodes the file once at construction time and exposes the pixel buffer
//! as a u8 (or u16) [`ArrayAdapterRead`]. Shape is `[H, W]` for grayscale,
//! `[H, W, C]` for colour. RGB/RGBA images are kept in their native channel
//! count; floating-point conversion is the caller's job.

use std::path::PathBuf;

use bytes::Bytes;

use tiled_core::adapters::{ArrayAdapterRead, BaseAdapter, BoxFuture};
use tiled_core::dtype::{BuiltinDType, DType, DynNDArray, Endianness, Kind};
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{ArrayStructure, Spec, StructureFamily};

pub struct ImageAdapter {
    array: DynNDArray,
    structure: ArrayStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl ImageAdapter {
    /// Decode whatever format the path's bytes describe (PNG, JPEG, BMP,
    /// …) using the `image` crate.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        #[cfg(feature = "tiff")]
        {
            let img = ::image::open(&path).map_err(|e| {
                TiledError::Internal(format!("decode {}: {e}", path.display()))
            })?;
            Self::from_dynamic(img, metadata)
        }
        #[cfg(not(feature = "tiff"))]
        {
            let _ = path;
            let _ = metadata;
            Err(TiledError::Validation(
                "image adapter requires the 'tiff' feature (which pulls the image crate)".into(),
            ))
        }
    }

    #[cfg(feature = "tiff")]
    pub fn from_dynamic(
        img: ::image::DynamicImage,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        use ::image::{ColorType, GenericImageView};
        let (w, h) = img.dimensions();
        let (raw, dtype, channels) = match img.color() {
            ColorType::L8 => (img.to_luma8().into_raw(), pixel_dtype(1), 1usize),
            ColorType::La8 => (img.to_luma_alpha8().into_raw(), pixel_dtype(1), 2),
            ColorType::Rgb8 => (img.to_rgb8().into_raw(), pixel_dtype(1), 3),
            ColorType::Rgba8 => (img.to_rgba8().into_raw(), pixel_dtype(1), 4),
            ColorType::L16 => (
                u16_to_le(&img.to_luma16().into_raw()),
                pixel_dtype(2),
                1,
            ),
            ColorType::La16 => (
                u16_to_le(&img.to_luma_alpha16().into_raw()),
                pixel_dtype(2),
                2,
            ),
            ColorType::Rgb16 => (
                u16_to_le(&img.to_rgb16().into_raw()),
                pixel_dtype(2),
                3,
            ),
            ColorType::Rgba16 => (
                u16_to_le(&img.to_rgba16().into_raw()),
                pixel_dtype(2),
                4,
            ),
            other => {
                return Err(TiledError::Validation(format!(
                    "unsupported image color type: {other:?}"
                )));
            }
        };
        let mut shape = vec![h as usize, w as usize];
        if channels > 1 {
            shape.push(channels);
        }
        let chunks: Vec<Vec<usize>> = shape.iter().map(|d| vec![*d]).collect();
        let structure = ArrayStructure {
            data_type: DType::Builtin(dtype.clone()),
            chunks,
            shape: shape.clone(),
            dims: None,
            resizable: Default::default(),
        };
        let array = DynNDArray::new(Bytes::from(raw), dtype, shape);
        Ok(Self {
            array,
            structure,
            metadata,
            specs: vec![Spec::new("image")],
        })
    }
}

fn pixel_dtype(itemsize: usize) -> BuiltinDType {
    BuiltinDType::new(Endianness::Little, Kind::UnsignedInteger, itemsize)
}

#[cfg(feature = "tiff")]
fn u16_to_le(buf: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(buf.len() * 2);
    for &v in buf {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

impl BaseAdapter for ImageAdapter {
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

impl ArrayAdapterRead for ImageAdapter {
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
                        "image adapter is single-chunk per axis; block[{axis}] = {b}"
                    )));
                }
            }
            Ok(self.array.clone())
        })
    }
}
