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

    // Convert the pixel buffer to u8 grayscale. Mirrors upstream
    // `save_to_buffer_PIL` (array.py:76), which widens EVERY numeric dtype to
    // float32 (`array.astype(numpy.float32)`) before scaling: a single uniform
    // "decode element → f32 → normalize" path serves u/i/f at any width and
    // complex (real part), so no integer width can fall through to a raw-byte
    // reinterpretation. Non-renderable kinds (U/S/M/m/…) are a loud error,
    // matching upstream where `.astype(numpy.float32)` raises and `serialize_html`
    // then falls back to CSV (array.py:143-153).
    let pixels: Vec<u8> = if kind == "b" {
        // Booleans map directly to 0/255. Upstream `astype(float32)` yields the
        // {0.0, 1.0} two-level array, which percentile-scales to the same two
        // extremes; the direct mapping is equivalent and clearer.
        data.iter().map(|&b| if b != 0 { 255 } else { 0 }).collect()
    } else {
        let values = decode_numeric_to_f32(data, kind, itemsize, big_endian)?;
        normalize_floats(&values)
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

/// Decode a numeric array buffer into `f32` values for image normalization,
/// mirroring upstream `save_to_buffer_PIL`'s `array.astype(numpy.float32)`
/// (array.py:76): every numeric dtype is widened to float32 before scaling, so
/// integer widths that once fell through to a raw-byte reinterpretation now
/// render correctly. `kind` is a numpy kind char; `big_endian` selects each
/// element's byte order.
///
/// Renderable kinds: unsigned int (`u`, 1/2/4/8), signed int (`i`, 1/2/4/8),
/// float (`f`, 2/4/8), and complex (`c`, 8/16 → real part only, matching numpy's
/// `astype(float32)`, which discards the imaginary part with a ComplexWarning).
/// Any other kind/width is a hard error so the caller answers 406/500 and the
/// Array HTML path falls back to CSV, rather than emitting a garbage image.
fn decode_numeric_to_f32(
    data: &[u8],
    kind: &str,
    itemsize: usize,
    big_endian: bool,
) -> Result<Vec<f32>, crate::serialization::registry::SerializeError> {
    if itemsize == 0 {
        return Err("itemsize must be > 0 to render an image".into());
    }

    // Read `N`-byte little-endian elements (reversing each element's bytes when
    // the source is big-endian) and map each to f32 via `conv`.
    fn map_le<const N: usize>(
        data: &[u8],
        big_endian: bool,
        conv: impl Fn([u8; N]) -> f32,
    ) -> Vec<f32> {
        data.chunks_exact(N)
            .map(|c| {
                let mut b: [u8; N] = c.try_into().expect("chunks_exact(N) yields N bytes");
                if big_endian {
                    b.reverse();
                }
                conv(b)
            })
            .collect()
    }

    let values: Vec<f32> = match (kind, itemsize) {
        ("u", 1) => data.iter().map(|&b| b as f32).collect(),
        ("u", 2) => map_le::<2>(data, big_endian, |b| u16::from_le_bytes(b) as f32),
        ("u", 4) => map_le::<4>(data, big_endian, |b| u32::from_le_bytes(b) as f32),
        ("u", 8) => map_le::<8>(data, big_endian, |b| u64::from_le_bytes(b) as f32),
        ("i", 1) => data.iter().map(|&b| b as i8 as f32).collect(),
        ("i", 2) => map_le::<2>(data, big_endian, |b| i16::from_le_bytes(b) as f32),
        ("i", 4) => map_le::<4>(data, big_endian, |b| i32::from_le_bytes(b) as f32),
        ("i", 8) => map_le::<8>(data, big_endian, |b| i64::from_le_bytes(b) as f32),
        ("f", 2) => map_le::<2>(data, big_endian, |b| {
            half::f16::from_bits(u16::from_le_bytes(b)).to_f32()
        }),
        ("f", 4) => map_le::<4>(data, big_endian, f32::from_le_bytes),
        ("f", 8) => map_le::<8>(data, big_endian, |b| f64::from_le_bytes(b) as f32),
        // Complex: numpy `astype(float32)` keeps the real component (the first
        // half of each element's bytes; re/im are interleaved) and discards the
        // imaginary part. Reverse only the real half for big-endian sources —
        // reversing the whole element would swap re and im.
        ("c", 8) => data
            .chunks_exact(8)
            .map(|c| {
                let mut re: [u8; 4] = c[..4].try_into().expect("4 bytes");
                if big_endian {
                    re.reverse();
                }
                f32::from_le_bytes(re)
            })
            .collect(),
        ("c", 16) => data
            .chunks_exact(16)
            .map(|c| {
                let mut re: [u8; 8] = c[..8].try_into().expect("8 bytes");
                if big_endian {
                    re.reverse();
                }
                f64::from_le_bytes(re) as f32
            })
            .collect(),
        _ => {
            return Err(format!("cannot render dtype {kind}{itemsize} as image").into());
        }
    };
    Ok(values)
}

/// Auto-scale float pixel values to `u8` grayscale using **percentile clipping**,
/// mirroring upstream `save_to_buffer_PIL` (array.py:78-83):
///
/// ```python
/// low  = numpy.percentile(array.ravel(), 1)
/// high = numpy.percentile(array.ravel(), 99)
/// scaled = numpy.clip((array - low) / (high - low), 0, 1)
/// ... img_as_ubyte(scaled)
/// ```
///
/// Clipping at the 1st/99th percentiles instead of the raw min/max keeps a
/// single extreme outlier pixel from washing out the whole preview: the outlier
/// is clipped to white while the body of the image keeps its contrast. (Under
/// min/max a lone hot pixel sets `high`, crushing every other pixel toward 0.)
///
/// Percentiles use numpy's default `method="linear"` — see [`percentile_sorted`].
/// The `[0, 1]` → `u8` step mirrors scikit-image's `img_as_ubyte`
/// (image_serializer_helpers.py): `rint(scaled * 255)` (round half to even) then
/// clip to `[0, 255]`. When `high == low` (degenerate, e.g. an all-equal image)
/// the division yields `0/0 = NaN` for `v == low` and `±inf` for `v ≷ low`,
/// exactly as numpy's `clip` sees them: NaN casts to 0 (black) and the infinities
/// clip to 0/255 — no special-casing needed.
///
/// Non-finite handling: upstream feeds every raveled value to `percentile`, so a
/// single NaN poisons both percentiles and the whole preview goes black. This
/// port instead takes the percentiles over the finite values only and maps each
/// non-finite pixel to 0; on all-finite data (the common case, and every boundary
/// test below) the two agree.
fn normalize_floats(values: &[f32]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    // Percentiles over the finite values (numpy `method="linear"`).
    let mut finite: Vec<f32> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if finite.is_empty() {
        return vec![0u8; values.len()];
    }
    finite.sort_by(f32::total_cmp);
    let low = percentile_sorted(&finite, 1.0);
    let high = percentile_sorted(&finite, 99.0);
    let span = high - low;
    values
        .iter()
        .map(|&v| {
            if !v.is_finite() {
                return 0;
            }
            // clip((v - low) / span, 0, 1). NaN (0/0 when span == 0) propagates
            // through `clamp` and casts to 0; ±inf clip to 0/255.
            let scaled = ((v - low) / span).clamp(0.0, 1.0);
            // img_as_ubyte: rint(scaled * 255), clip to [0, 255], cast. Rust's
            // saturating float→int cast maps NaN → 0, matching numpy's
            // `astype(uint8)` of NaN.
            (scaled * 255.0).round_ties_even().clamp(0.0, 255.0) as u8
        })
        .collect()
}

/// numpy `percentile(sorted, q)` with the default `method="linear"`: for an
/// ascending, non-empty slice of length `n`, interpolate at the virtual index
/// `q/100 * (n - 1)` between the two neighbouring samples. `q` is in `[0, 100]`.
fn percentile_sorted(sorted: &[f32], q: f32) -> f32 {
    let n = sorted.len();
    debug_assert!(n > 0, "percentile_sorted requires a non-empty slice");
    let virtual_index = (q / 100.0) * (n as f32 - 1.0);
    let lo = virtual_index.floor() as usize;
    let hi = (lo + 1).min(n - 1);
    let frac = virtual_index - lo as f32;
    sorted[lo] + frac * (sorted[hi] - sorted[lo])
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

    /// Finding 3 (updated for the unified decoder): a big-endian decode must read
    /// the true value, not a byte-swapped one. 0x1234 decodes to 4660.0 as both
    /// LE (bytes 34,12) and BE (bytes 12,34); reading the BE buffer as LE would
    /// instead yield 0x3412 = 13330.0.
    #[test]
    fn decode_u16_honors_byteorder() {
        let le = [0x34u8, 0x12]; // little-endian 0x1234
        let be = [0x12u8, 0x34]; // big-endian 0x1234
        assert_eq!(
            decode_numeric_to_f32(&le, "u", 2, false).unwrap(),
            vec![4660.0]
        );
        assert_eq!(
            decode_numeric_to_f32(&be, "u", 2, true).unwrap(),
            vec![4660.0]
        );
        // Ignoring byte order (the bug) reads the BE buffer as 0x3412 = 13330.
        assert_eq!(
            decode_numeric_to_f32(&be, "u", 2, false).unwrap(),
            vec![13330.0]
        );
    }

    /// The unified numeric decoder honors source byte order for f32.
    #[test]
    fn decode_f32_honors_byteorder() {
        let vals = [0.0f32, 1.5];
        let le: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let be: Vec<u8> = vals.iter().flat_map(|v| v.to_be_bytes()).collect();
        assert_eq!(
            decode_numeric_to_f32(&le, "f", 4, false).unwrap(),
            vec![0.0, 1.5]
        );
        assert_eq!(
            decode_numeric_to_f32(&be, "f", 4, true).unwrap(),
            vec![0.0, 1.5]
        );
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

    /// The unified numeric decoder widens each float16 to f32 and honors source
    /// byte order (mirrors `decode_f32_honors_byteorder`).
    #[test]
    fn decode_f16_honors_byteorder() {
        let vals = [0.0f32, 1.0];
        let le: Vec<u8> = vals
            .iter()
            .flat_map(|v| half::f16::from_f32(*v).to_bits().to_le_bytes())
            .collect();
        let be: Vec<u8> = vals
            .iter()
            .flat_map(|v| half::f16::from_f32(*v).to_bits().to_be_bytes())
            .collect();
        assert_eq!(
            decode_numeric_to_f32(&le, "f", 2, false).unwrap(),
            vec![0.0, 1.0]
        );
        assert_eq!(
            decode_numeric_to_f32(&be, "f", 2, true).unwrap(),
            vec![0.0, 1.0]
        );
    }

    /// FAILING-TEST-FIRST (numpy's default integer dtype): a 2-D int64 array must
    /// render the same image as its float32-widened twin — mirroring upstream
    /// `save_to_buffer_PIL` (`array.astype(numpy.float32)`, array.py:76). Before
    /// the fix, `("i", 8)` fell to the `_ => data.to_vec()` catch-all and produced
    /// a truncated raw-byte garbage image (200 OK, wrong pixels).
    #[test]
    fn encode_image_int64_matches_f32_widening() {
        let vals = [0i64, 25, 50, 100];
        let i64_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let f32_bytes: Vec<u8> = vals
            .iter()
            .flat_map(|&v| (v as f32).to_le_bytes())
            .collect();
        let meta_i =
            serde_json::json!({"itemsize": 8, "kind": "i", "byteorder": "<", "shape": [2, 2]});
        let meta_f =
            serde_json::json!({"itemsize": 4, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let png_i = encode_image(&i64_bytes, &meta_i, ImageFormat::Png).unwrap();
        let png_f = encode_image(&f32_bytes, &meta_f, ImageFormat::Png).unwrap();
        assert_eq!(
            png_i, png_f,
            "int64 image must render as its f32 widening, not raw-byte garbage"
        );
    }

    /// int32 (`("i", 4)`) — also unhandled before the fix — must render as its
    /// f32-widened twin.
    #[test]
    fn encode_image_int32_matches_f32_widening() {
        let vals = [0i32, 25, 50, 100];
        let i32_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let f32_bytes: Vec<u8> = vals
            .iter()
            .flat_map(|&v| (v as f32).to_le_bytes())
            .collect();
        let meta_i =
            serde_json::json!({"itemsize": 4, "kind": "i", "byteorder": "<", "shape": [2, 2]});
        let meta_f =
            serde_json::json!({"itemsize": 4, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let png_i = encode_image(&i32_bytes, &meta_i, ImageFormat::Png).unwrap();
        let png_f = encode_image(&f32_bytes, &meta_f, ImageFormat::Png).unwrap();
        assert_eq!(png_i, png_f, "int32 image must render as its f32 widening");
    }

    /// uint64 (`("u", 8)`) — also unhandled before the fix — must render as its
    /// f32-widened twin.
    #[test]
    fn encode_image_uint64_matches_f32_widening() {
        let vals = [0u64, 25, 50, 100];
        let u64_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let f32_bytes: Vec<u8> = vals
            .iter()
            .flat_map(|&v| (v as f32).to_le_bytes())
            .collect();
        let meta_u =
            serde_json::json!({"itemsize": 8, "kind": "u", "byteorder": "<", "shape": [2, 2]});
        let meta_f =
            serde_json::json!({"itemsize": 4, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let png_u = encode_image(&u64_bytes, &meta_u, ImageFormat::Png).unwrap();
        let png_f = encode_image(&f32_bytes, &meta_f, ImageFormat::Png).unwrap();
        assert_eq!(png_u, png_f, "uint64 image must render as its f32 widening");
    }

    /// Complex arrays render as an image of the REAL part — matching numpy's
    /// `astype(numpy.float32)` (array.py:76), which discards the imaginary part
    /// (ComplexWarning) and keeps the real component. A complex64 array must
    /// render identically to an f32 image of just its real values, regardless of
    /// the imaginary parts.
    #[test]
    fn encode_image_complex64_renders_real_part() {
        // complex64: [re: f32, im: f32] per element. Vary imag to prove it's dropped.
        let re = [0.0f32, 25.0, 50.0, 100.0];
        let im = [9.0f32, -3.0, 7.0, 1.0];
        let c_bytes: Vec<u8> = re
            .iter()
            .zip(im.iter())
            .flat_map(|(r, i)| r.to_le_bytes().into_iter().chain(i.to_le_bytes()))
            .collect();
        let f_bytes: Vec<u8> = re.iter().flat_map(|v| v.to_le_bytes()).collect();
        let meta_c =
            serde_json::json!({"itemsize": 8, "kind": "c", "byteorder": "<", "shape": [2, 2]});
        let meta_f =
            serde_json::json!({"itemsize": 4, "kind": "f", "byteorder": "<", "shape": [2, 2]});
        let png_c = encode_image(&c_bytes, &meta_c, ImageFormat::Png).unwrap();
        let png_f = encode_image(&f_bytes, &meta_f, ImageFormat::Png).unwrap();
        assert_eq!(
            png_c, png_f,
            "complex image must render the real part only (numpy astype drops imaginary)"
        );
    }

    /// numpy `percentile(sorted, q)` parity for the `method="linear"` default.
    /// Reference values are what `numpy.percentile([0,1,2,3,4], q)` returns.
    #[test]
    fn percentile_sorted_matches_numpy_linear() {
        let a = [0.0f32, 1.0, 2.0, 3.0, 4.0]; // n = 5, so virtual = q/100 * 4
        assert_eq!(percentile_sorted(&a, 0.0), 0.0);
        assert_eq!(percentile_sorted(&a, 100.0), 4.0);
        assert_eq!(percentile_sorted(&a, 50.0), 2.0);
        // q=1 → virtual index 0.04 → 0 + 0.04*(1-0) = 0.04.
        assert!((percentile_sorted(&a, 1.0) - 0.04).abs() < 1e-5);
        // q=99 → virtual index 3.96 → 3 + 0.96*(4-3) = 3.96.
        assert!((percentile_sorted(&a, 99.0) - 3.96).abs() < 1e-5);
        // Single element: every percentile is that element.
        assert_eq!(percentile_sorted(&[7.5f32], 1.0), 7.5);
        assert_eq!(percentile_sorted(&[7.5f32], 99.0), 7.5);
    }

    /// Finding 15 (meta-sweep): a single extreme outlier must be clipped by the
    /// percentile(1,99) rule while the body of the image keeps its contrast —
    /// the defect min/max scaling caused (the outlier sets `high`, crushing every
    /// body pixel to 0). 199 body pixels valued 0..=198 plus one 1e9 outlier; the
    /// outlier sits above p99 (n=200 ⇒ p99's virtual index 197.01 never reaches
    /// index 199), so it is excluded from `high`.
    #[test]
    fn normalize_floats_clips_outlier_preserves_body_contrast() {
        let mut values: Vec<f32> = (0..199).map(|i| i as f32).collect();
        values.push(1e9); // outlier at index 199
        let out = normalize_floats(&values);
        // low = p1 ≈ 1.99, high = p99 ≈ 197.01, span ≈ 195.02.
        assert_eq!(out[199], 255, "outlier must clip to white");
        assert_eq!(out[198], 255, "brightest body pixel (≥ p99) clips to white");
        assert_eq!(out[0], 0, "darkest body pixel (≤ p1) is black");
        // Mid-body (value 100): (100 - 1.99)/195.02 ≈ 0.5026 → ~128, NOT crushed.
        assert!(
            (126..=130).contains(&out[100]),
            "mid body pixel should keep contrast (~128), got {}",
            out[100]
        );
        // Contrast is real: under the old min/max rule, high = 1e9 would map the
        // same mid-body pixel to 0.
        let minmax = ((100.0f32 - 0.0) / (1e9 - 0.0) * 255.0) as u8;
        assert_eq!(minmax, 0, "sanity: min/max scaling washes the body out");
    }

    /// A uniform gradient (no outliers) still spans the full 0..255 range: the
    /// bottom 1% clips to black, the top 1% to white, the middle to mid-grey.
    #[test]
    fn normalize_floats_uniform_gradient_spans_full_range() {
        let values: Vec<f32> = (0..=255).map(|i| i as f32).collect();
        let out = normalize_floats(&values);
        assert_eq!(out[0], 0, "lowest value maps to black");
        assert_eq!(out[255], 255, "highest value maps to white");
        // Middle value ≈ mid-grey.
        assert!(
            (126..=130).contains(&out[128]),
            "gradient midpoint should be mid-grey, got {}",
            out[128]
        );
    }

    /// All-equal-values image: p1 == p99, so `high - low == 0` and every pixel is
    /// `(v - low)/0 = 0/0 = NaN` → clamp → NaN → cast → 0. numpy does the same
    /// (`clip(nan,0,1)` = NaN, `astype(uint8)` of NaN = 0), so the preview is a
    /// black frame rather than a crash or garbage.
    #[test]
    fn normalize_floats_all_equal_is_black() {
        let out = normalize_floats(&[42.0f32; 64]);
        assert_eq!(out.len(), 64);
        assert!(
            out.iter().all(|&b| b == 0),
            "degenerate all-equal image must render as black (numpy NaN → 0)"
        );
    }

    /// A non-renderable dtype (fixed-width unicode `U`) is now a loud error, not a
    /// garbage image — so the Array HTML serializer falls back to CSV, matching
    /// upstream where `array.astype(numpy.float32)` raises for such dtypes and
    /// `serialize_html` catches it (array.py:143-153).
    #[test]
    fn encode_image_rejects_non_renderable_dtype() {
        // <U2: 2 code points × 4 bytes = one element.
        let data = vec![0u8; 8];
        let meta =
            serde_json::json!({"itemsize": 8, "kind": "U", "byteorder": "<", "shape": [1, 1]});
        let err = encode_image(&data, &meta, ImageFormat::Png)
            .expect_err("non-renderable dtype must error, not emit a garbage image");
        assert!(
            err.to_string().contains("cannot render dtype U8"),
            "error must name the unsupported dtype: {err}"
        );
    }
}
