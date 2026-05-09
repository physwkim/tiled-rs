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
use image::{ImageBuffer, Luma, codecs::png::PngEncoder, codecs::jpeg::JpegEncoder, ExtendedColorType, ImageEncoder};

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

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
) -> Result<Bytes, crate::registry::SerializeError> {
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
        ("u", 2) => downscale_u16(data),
        ("u", 4) => downscale_u32(data),
        ("i", 1) => data.iter().map(|&b| b as i8 as i32 + 128).map(|v| v as u8).collect(),
        ("i", 2) => downscale_i16(data),
        ("f", 4) => normalize_f32(data),
        ("f", 8) => normalize_f64(data),
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

fn downscale_u16(data: &[u8]) -> Vec<u8> {
    data.chunks_exact(2)
        .map(|c| {
            let v = u16::from_le_bytes([c[0], c[1]]);
            (v >> 8) as u8
        })
        .collect()
}

fn downscale_u32(data: &[u8]) -> Vec<u8> {
    data.chunks_exact(4)
        .map(|c| {
            let v = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            (v >> 24) as u8
        })
        .collect()
}

fn downscale_i16(data: &[u8]) -> Vec<u8> {
    data.chunks_exact(2)
        .map(|c| {
            let v = i16::from_le_bytes([c[0], c[1]]);
            ((v.saturating_add(i16::MIN.unsigned_abs() as i16) as i32 / 257) as u8) as u8
        })
        .collect()
}

fn normalize_f32(data: &[u8]) -> Vec<u8> {
    let values: Vec<f32> = data
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    normalize_floats(&values)
}

fn normalize_f64(data: &[u8]) -> Vec<u8> {
    let values: Vec<f32> = data
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]) as f32)
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
    if !min.is_finite() || !max.is_finite() || (max - min) < f32::EPSILON {
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
