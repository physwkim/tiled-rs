//! PNG / JPEG / TIFF serializers for `array` and `sparse` families.
//!
//! Reads the raw bytes + metadata produced by an `ArrayAdapterRead::read`
//! call (`{"itemsize", "kind", "shape"}`) and writes a 2-D image. Higher-
//! dimensional arrays are flattened to a 2-D heatmap by squeezing
//! everything past the first two axes into "rows" — same fallback
//! Python tiled uses.

#![cfg(feature = "image")]

use std::io::Cursor;

use bytes::Bytes;
use image::{
    ExtendedColorType, ImageBuffer, ImageEncoder, Luma, codecs::jpeg::JpegEncoder,
    codecs::png::PngEncoder,
};

use crate::core::media_type::mime;
use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializerFn};

pub fn register_image_serializers(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Array, mime::PNG, png_serializer());
    reg.register(StructureFamily::Array, "image/jpeg", jpeg_serializer());
    reg.register(StructureFamily::Array, "image/tiff", tiff_serializer());
    reg.register(StructureFamily::Sparse, mime::PNG, png_serializer());

    reg.register_alias(".png", mime::PNG);
    reg.register_alias(".jpg", "image/jpeg");
    reg.register_alias(".jpeg", "image/jpeg");
    reg.register_alias(".tiff", "image/tiff");
    reg.register_alias(".tif", "image/tiff");
}

fn png_serializer() -> SerializerFn {
    Box::new(|data, meta| encode_image(data, meta, ImageFormat::Png))
}

fn jpeg_serializer() -> SerializerFn {
    Box::new(|data, meta| encode_image(data, meta, ImageFormat::Jpeg))
}

fn tiff_serializer() -> SerializerFn {
    Box::new(|data, meta| encode_image(data, meta, ImageFormat::Tiff))
}

#[derive(Debug, Clone, Copy)]
enum ImageFormat {
    Png,
    Jpeg,
    Tiff,
}

fn encode_image(
    data: &[u8],
    metadata: &serde_json::Value,
    format: ImageFormat,
) -> Result<Bytes, crate::serialization::registry::SerializeError> {
    let itemsize = metadata
        .get("itemsize")
        .and_then(|v| v.as_u64())
        .unwrap_or(8) as usize;
    let kind = metadata.get("kind").and_then(|v| v.as_str()).unwrap_or("f");
    let shape: Vec<usize> = metadata
        .get("shape")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    // Honor the source byte order: the array adapter may emit big-endian (`>`)
    // buffers (zarr/numpy `>` dtypes). Without this every multi-byte decode used
    // `from_le_bytes`, so a big-endian source rendered byte-swapped pixels.
    let big_endian = metadata
        .get("byteorder")
        .and_then(|v| v.as_str())
        .unwrap_or("<")
        == ">";

    let (h, w) = match shape.len() {
        2 => (shape[0], shape[1]),
        n if n >= 1 => {
            // Flatten to (rows*..., cols).
            let cols = *shape.last().unwrap();
            let rows: usize = shape[..n - 1].iter().product();
            (rows, cols)
        }
        _ => {
            return Err("array has zero rank — can't render as image".into());
        }
    };

    // Convert pixel buffer to u8 grayscale (single channel) regardless of
    // input dtype. Floats are clipped to [0, 1] and scaled; ints are
    // shifted/clamped into u8 range.
    let pixels = match (kind, itemsize) {
        ("u", 1) => data.to_vec(),
        ("u", 2) => downscale_u16(data, big_endian),
        ("u", 4) => downscale_u32(data, big_endian),
        ("i", 1) => data
            .iter()
            .map(|&b| b as i8 as i32 + 128)
            .map(|v| v as u8)
            .collect(),
        ("i", 2) => downscale_i16(data, big_endian),
        ("f", 2) => normalize_f16(data, big_endian),
        ("f", 4) => normalize_f32(data, big_endian),
        ("f", 8) => normalize_f64(data, big_endian),
        ("b", _) => data.iter().map(|&b| if b != 0 { 255 } else { 0 }).collect(),
        _ => data.to_vec(),
    };

    // Truncate / pad to expected size so a malformed buffer doesn't crash.
    let want = h.checked_mul(w).ok_or("h*w overflow")?;
    let mut buf = pixels;
    buf.resize(want, 0);

    let img: ImageBuffer<Luma<u8>, Vec<u8>> =
        ImageBuffer::from_raw(w as u32, h as u32, buf).ok_or("bad pixel buffer")?;

    let mut out = Cursor::new(Vec::new());
    match format {
        ImageFormat::Png => {
            PngEncoder::new(&mut out)
                .write_image(
                    img.as_raw(),
                    img.width(),
                    img.height(),
                    ExtendedColorType::L8,
                )
                .map_err(|e| format!("png encode: {e}"))?;
        }
        ImageFormat::Jpeg => {
            let mut enc = JpegEncoder::new_with_quality(&mut out, 90);
            enc.encode(
                img.as_raw(),
                img.width(),
                img.height(),
                ExtendedColorType::L8,
            )
            .map_err(|e| format!("jpeg encode: {e}"))?;
        }
        ImageFormat::Tiff => {
            // image crate's TIFF encoder works through DynamicImage.
            let dyn_img = image::DynamicImage::ImageLuma8(img);
            dyn_img
                .write_to(&mut out, image::ImageFormat::Tiff)
                .map_err(|e| format!("tiff encode: {e}"))?;
        }
    }
    Ok(Bytes::from(out.into_inner()))
}

fn downscale_u16(data: &[u8], big_endian: bool) -> Vec<u8> {
    data.chunks_exact(2)
        .map(|c| {
            let arr = [c[0], c[1]];
            let v = if big_endian {
                u16::from_be_bytes(arr)
            } else {
                u16::from_le_bytes(arr)
            };
            (v >> 8) as u8
        })
        .collect()
}

fn downscale_u32(data: &[u8], big_endian: bool) -> Vec<u8> {
    data.chunks_exact(4)
        .map(|c| {
            let arr = [c[0], c[1], c[2], c[3]];
            let v = if big_endian {
                u32::from_be_bytes(arr)
            } else {
                u32::from_le_bytes(arr)
            };
            (v >> 24) as u8
        })
        .collect()
}

fn downscale_i16(data: &[u8], big_endian: bool) -> Vec<u8> {
    data.chunks_exact(2)
        .map(|c| {
            let arr = [c[0], c[1]];
            let v = if big_endian {
                i16::from_be_bytes(arr)
            } else {
                i16::from_le_bytes(arr)
            };
            (v.saturating_add(i16::MIN.unsigned_abs() as i16) as i32 / 257) as u8
        })
        .collect()
}

/// Decode numpy half-precision (float16) pixels, widening each to f32
/// (lossless) before the shared [`normalize_floats`] scaling — mirroring
/// upstream's `astype(numpy.float32)` (array.py:76) rather than reinterpreting
/// the raw 2-byte pattern as u8 pixels.
fn normalize_f16(data: &[u8], big_endian: bool) -> Vec<u8> {
    let values: Vec<f32> = data
        .chunks_exact(2)
        .map(|c| {
            let bits = if big_endian {
                u16::from_be_bytes([c[0], c[1]])
            } else {
                u16::from_le_bytes([c[0], c[1]])
            };
            half::f16::from_bits(bits).to_f32()
        })
        .collect();
    normalize_floats(&values)
}

fn normalize_f32(data: &[u8], big_endian: bool) -> Vec<u8> {
    let values: Vec<f32> = data
        .chunks_exact(4)
        .map(|c| {
            let arr = [c[0], c[1], c[2], c[3]];
            if big_endian {
                f32::from_be_bytes(arr)
            } else {
                f32::from_le_bytes(arr)
            }
        })
        .collect();
    normalize_floats(&values)
}

fn normalize_f64(data: &[u8], big_endian: bool) -> Vec<u8> {
    let values: Vec<f32> = data
        .chunks_exact(8)
        .map(|c| {
            let arr = [c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]];
            let v = if big_endian {
                f64::from_be_bytes(arr)
            } else {
                f64::from_le_bytes(arr)
            };
            v as f32
        })
        .collect();
    normalize_floats(&values)
}

fn normalize_floats(values: &[f32]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    for &v in values {
        if v.is_finite() {
            min = min.min(v);
            max = max.max(v);
        }
    }
    if !min.is_finite() || !max.is_finite() || (max - min).abs() < f32::EPSILON {
        return vec![0u8; values.len()];
    }
    values
        .iter()
        .map(|&v| {
            if !v.is_finite() {
                0
            } else {
                let normalized = ((v - min) / (max - min)) * 255.0;
                normalized.clamp(0.0, 255.0) as u8
            }
        })
        .collect()
}

/// PNG encoding entry point for the Array HTML serializer's try-then-fallback.
pub(crate) fn encode_array_png(
    data: &[u8],
    metadata: &serde_json::Value,
) -> Result<bytes::Bytes, crate::serialization::registry::SerializeError> {
    encode_image(data, metadata, ImageFormat::Png)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Finding 3: a big-endian decode must read the true value, not a
    /// byte-swapped one. 0x1234's high byte (the downscaled pixel) is 0x12.
    #[test]
    fn downscale_u16_honors_byteorder() {
        let le = [0x34u8, 0x12]; // little-endian 0x1234
        let be = [0x12u8, 0x34]; // big-endian 0x1234
        assert_eq!(downscale_u16(&le, false), vec![0x12]);
        assert_eq!(downscale_u16(&be, true), vec![0x12]);
        // Ignoring byte order (the bug) reads the BE buffer as 0x3412 → 0x34.
        assert_eq!(downscale_u16(&be, false), vec![0x34]);
    }

    #[test]
    fn normalize_f32_honors_byteorder() {
        let vals = [0.0f32, 1.0];
        let le: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let be: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(normalize_f32(&le, false), vec![0, 255]);
        assert_eq!(normalize_f32(&be, true), vec![0, 255]);
    }

    /// End-to-end: a big-endian image and its little-endian twin encode to
    /// identical PNG bytes — proving `encode_image` threads byteorder through.
    #[test]
    fn encode_image_le_and_be_match() {
        let vals = [0x0102u16, 0x0304, 0x0506, 0x0708];
        let le: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let be: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        let meta_le =
            serde_json::json!({"itemsize": 2, "kind": "u", "byteorder": "<", "shape": [2, 2]});
        let meta_be =
            serde_json::json!({"itemsize": 2, "kind": "u", "byteorder": ">", "shape": [2, 2]});
        let png_le = encode_image(&le, &meta_le, ImageFormat::Png).unwrap();
        let png_be = encode_image(&be, &meta_be, ImageFormat::Png).unwrap();
        assert_eq!(
            png_le, png_be,
            "BE and LE encodings of the same image must render identically"
        );
    }

    /// float16 arrays render as images by widening f16 -> f32 (upstream
    /// `astype(numpy.float32)`, array.py:76), not by reinterpreting the raw f16
    /// bytes as u8 pixels. A float16 image must produce the same pixels as its
    /// f32-widened twin.
    #[test]
    fn encode_image_float16_matches_f32_widening() {
        // 2x2 image; values are exact in float16 so widening is lossless.
        let vals = [0.0f32, 0.25, 0.5, 1.0];
        let f16_bytes: Vec<u8> = vals
            .iter()
            .flat_map(|&v| half::f16::from_f32(v).to_bits().to_le_bytes())
            .collect();
        let f32_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta16 =
            serde_json::json!({"itemsize": 2, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let meta32 =
            serde_json::json!({"itemsize": 4, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let png16 = encode_image(&f16_bytes, &meta16, ImageFormat::Png).unwrap();
        let png32 = encode_image(&f32_bytes, &meta32, ImageFormat::Png).unwrap();
        assert_eq!(
            png16, png32,
            "float16 image must render identically to its f32 widening, not raw-byte garbage"
        );
    }

    /// `normalize_f16` widens each half to f32 and scales like `normalize_f32`,
    /// honoring source byte order (mirrors `normalize_f32_honors_byteorder`).
    #[test]
    fn normalize_f16_honors_byteorder() {
        let vals = [0.0f32, 1.0];
        let le: Vec<u8> = vals
            .iter()
            .flat_map(|v| half::f16::from_f32(*v).to_bits().to_le_bytes())
            .collect();
        let be: Vec<u8> = vals
            .iter()
            .flat_map(|v| half::f16::from_f32(*v).to_bits().to_be_bytes())
            .collect();
        assert_eq!(normalize_f16(&le, false), vec![0, 255]);
        assert_eq!(normalize_f16(&be, true), vec![0, 255]);
    }
}
