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
            // metadata shape: {"itemsize": N, "kind": "f"|"i"|"u", "shape": [...]}
            // 1-D → one value per line
            // 2-D → rows of comma-separated values (matches Python tiled CSV)
            // ND  → flatten to 1-D row-major (Python tiled fallback)
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
                .map(|arr| arr.iter().filter_map(|v| v.as_u64().map(|n| n as usize)).collect())
                .unwrap_or_default();

            let format_value = |bytes: &[u8]| -> String {
                match (kind, itemsize) {
                    ("f", 8) => f64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])).to_string(),
                    ("f", 4) => f32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])).to_string(),
                    ("i", 8) => i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])).to_string(),
                    ("i", 4) => i32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])).to_string(),
                    ("i", 2) => i16::from_le_bytes(bytes.try_into().unwrap_or([0; 2])).to_string(),
                    ("i", 1) => i8::from_le_bytes(bytes.try_into().unwrap_or([0; 1])).to_string(),
                    ("u", 8) => u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])).to_string(),
                    ("u", 4) => u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])).to_string(),
                    ("u", 2) => u16::from_le_bytes(bytes.try_into().unwrap_or([0; 2])).to_string(),
                    ("u", 1) => u8::from_le_bytes(bytes.try_into().unwrap_or([0; 1])).to_string(),
                    ("b", _) => (bytes.iter().any(|&b| b != 0)).to_string(),
                    _ => format!("{bytes:?}"),
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
