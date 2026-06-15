//! Array serializers.
//!
//! Corresponds to `tiled/serialization/array.py`.

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::SerializationRegistry;

/// Register built-in array serializers.
pub fn register_array_serializers(registry: &SerializationRegistry) {
    // application/octet-stream → raw bytes (zero-copy)
    registry.register(
        StructureFamily::Array,
        mime::OCTET_STREAM,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    // text/csv → CSV for 1D/2D arrays
    registry.register(
        StructureFamily::Array,
        mime::CSV,
        Box::new(|data: &[u8], metadata: &serde_json::Value| {
            // metadata: {"itemsize": N, "kind": "f"|"i"|"u"|..., "byteorder": "<"|">"|"|", "shape": [...]}
            // 1-D → one value per line
            // 2-D → rows of comma-separated values (matches Python tiled CSV)
            let itemsize = metadata
                .get("itemsize")
                .and_then(|v| v.as_u64())
                .unwrap_or(8) as usize;
            if itemsize == 0 {
                return Err("itemsize must be > 0".into());
            }
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
            // Python `serialize_csv` raises `UnsupportedShape` for ndim > 2
            // (array.py:42-43) — it has NO flatten fallback. Match it: a >2-D
            // array must error (→ HTTP 406), not silently reshape to one column.
            if shape.len() > 2 {
                return Err(crate::registry::UnsupportedShape {
                    shape: shape.clone(),
                }
                .into());
            }
            let big_endian = metadata
                .get("byteorder")
                .and_then(|v| v.as_str())
                .unwrap_or("<")
                == ">";

            let format_value = |bytes: &[u8]| -> String {
                match (kind, itemsize) {
                    ("f", 8) => {
                        let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                        if big_endian {
                            b.reverse();
                        }
                        f64::from_le_bytes(b).to_string()
                    }
                    ("f", 4) => {
                        let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                        if big_endian {
                            b.reverse();
                        }
                        f32::from_le_bytes(b).to_string()
                    }
                    ("i", 8) => {
                        let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                        if big_endian {
                            b.reverse();
                        }
                        i64::from_le_bytes(b).to_string()
                    }
                    ("i", 4) => {
                        let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                        if big_endian {
                            b.reverse();
                        }
                        i32::from_le_bytes(b).to_string()
                    }
                    ("i", 2) => {
                        let mut b: [u8; 2] = bytes.try_into().unwrap_or([0u8; 2]);
                        if big_endian {
                            b.reverse();
                        }
                        i16::from_le_bytes(b).to_string()
                    }
                    ("i", 1) => i8::from_le_bytes(bytes.try_into().unwrap_or([0u8; 1])).to_string(),
                    ("u", 8) => {
                        let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                        if big_endian {
                            b.reverse();
                        }
                        u64::from_le_bytes(b).to_string()
                    }
                    ("u", 4) => {
                        let mut b: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
                        if big_endian {
                            b.reverse();
                        }
                        u32::from_le_bytes(b).to_string()
                    }
                    ("u", 2) => {
                        let mut b: [u8; 2] = bytes.try_into().unwrap_or([0u8; 2]);
                        if big_endian {
                            b.reverse();
                        }
                        u16::from_le_bytes(b).to_string()
                    }
                    ("u", 1) => u8::from_le_bytes(bytes.try_into().unwrap_or([0u8; 1])).to_string(),
                    ("b", _) => (bytes.iter().any(|&b| b != 0)).to_string(),
                    ("c", _) if itemsize >= 2 => {
                        // Complex float: real then imaginary, each `itemsize/2` bytes.
                        let half = itemsize / 2;
                        if bytes.len() < itemsize {
                            return format!("unsupported-complex{itemsize}");
                        }
                        match half {
                            8 => {
                                let mut re = [0u8; 8];
                                let mut im = [0u8; 8];
                                re.copy_from_slice(&bytes[..8]);
                                im.copy_from_slice(&bytes[8..16]);
                                if big_endian {
                                    re.reverse();
                                    im.reverse();
                                }
                                format!("({}+{}j)", f64::from_le_bytes(re), f64::from_le_bytes(im))
                            }
                            4 => {
                                let mut re = [0u8; 4];
                                let mut im = [0u8; 4];
                                re.copy_from_slice(&bytes[..4]);
                                im.copy_from_slice(&bytes[4..8]);
                                if big_endian {
                                    re.reverse();
                                    im.reverse();
                                }
                                format!("({}+{}j)", f32::from_le_bytes(re), f32::from_le_bytes(im))
                            }
                            _ => format!("unsupported-complex{itemsize}"),
                        }
                    }
                    ("M", 8) | ("m", 8) => {
                        // Datetime/timedelta: underlying i64 epoch value in dtype units.
                        let mut b: [u8; 8] = bytes.try_into().unwrap_or([0u8; 8]);
                        if big_endian {
                            b.reverse();
                        }
                        i64::from_le_bytes(b).to_string()
                    }
                    ("S", _) => {
                        // Fixed-length byte string: decode as UTF-8, strip trailing nulls.
                        std::str::from_utf8(bytes)
                            .unwrap_or("")
                            .trim_end_matches('\0')
                            .to_string()
                    }
                    ("U", _) => {
                        // UCS-4 (4 bytes per character); honor byte order per character.
                        bytes
                            .chunks(4)
                            .filter_map(|c| {
                                let arr: [u8; 4] = c.try_into().ok()?;
                                let cp = if big_endian {
                                    u32::from_be_bytes(arr)
                                } else {
                                    u32::from_le_bytes(arr)
                                };
                                if cp == 0 {
                                    return None;
                                }
                                char::from_u32(cp)
                            })
                            .collect::<String>()
                    }
                    _ => format!("unsupported dtype {kind}{itemsize}"),
                }
            };

            let num_elements = data.len() / itemsize;
            let mut output = String::new();

            // 2-D: rows × cols layout.
            if shape.len() == 2 {
                let cols = shape[1];
                for row in 0..shape[0] {
                    if row > 0 {
                        output.push('\n');
                    }
                    for col in 0..cols {
                        if col > 0 {
                            output.push(',');
                        }
                        let i = row * cols + col;
                        let start = i * itemsize;
                        let end = start + itemsize;
                        if end > data.len() {
                            break;
                        }
                        output.push_str(&format_value(&data[start..end]));
                    }
                }
            } else {
                // 1-D or fallback: one value per line, row-major.
                for i in 0..num_elements {
                    let start = i * itemsize;
                    let end = start + itemsize;
                    if end > data.len() {
                        break;
                    }
                    if i > 0 {
                        output.push('\n');
                    }
                    output.push_str(&format_value(&data[start..end]));
                }
            }

            Ok(bytes::Bytes::from(output))
        }),
    );

    // Sparse arrays also use octet-stream
    registry.register(
        StructureFamily::Sparse,
        mime::OCTET_STREAM,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{SerializationRegistry, UnsupportedShape};

    fn csv_serializer() -> std::sync::Arc<crate::registry::SerializerFn> {
        let reg = SerializationRegistry::new();
        register_array_serializers(&reg);
        reg.dispatch(StructureFamily::Array, mime::CSV).unwrap()
    }

    /// Finding 4: a >2-D array must error like Python `serialize_csv`
    /// (UnsupportedShape, array.py:42-43), not silently flatten to one column.
    #[test]
    fn csv_rejects_ndim_gt_2_as_unsupported_shape() {
        let ser = csv_serializer();
        // 2x2x2 f64 (ndim 3).
        let data: Vec<u8> = (0..8u64).flat_map(|i| (i as f64).to_le_bytes()).collect();
        let meta = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [2, 2, 2]});
        let err = ser(&data, &meta).expect_err("ndim>2 CSV must error, not flatten");
        let shape = err
            .downcast_ref::<UnsupportedShape>()
            .expect("error must be UnsupportedShape so the router answers 406");
        assert_eq!(shape.shape, vec![2, 2, 2]);
    }

    /// 1-D and 2-D arrays remain valid CSV (the supported shapes).
    #[test]
    fn csv_accepts_1d_and_2d() {
        let ser = csv_serializer();
        let data: Vec<u8> = (0..6u64).flat_map(|i| (i as f64).to_le_bytes()).collect();

        let meta_1d = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [6]});
        let out_1d = ser(&data, &meta_1d).expect("1-D CSV is supported");
        assert_eq!(std::str::from_utf8(&out_1d).unwrap().lines().count(), 6);

        let meta_2d = serde_json::json!({"itemsize": 8, "kind": "f", "shape": [2, 3]});
        let out_2d = ser(&data, &meta_2d).expect("2-D CSV is supported");
        assert_eq!(std::str::from_utf8(&out_2d).unwrap().lines().count(), 2);
    }
}
