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
            // Simple CSV: one value per line for 1D, or rows for 2D
            // metadata should contain "shape" and "dtype" info
            let itemsize = metadata
                .get("itemsize")
                .and_then(|v| v.as_u64())
                .unwrap_or(8) as usize;
            if itemsize == 0 {
                return Err("itemsize must be > 0".into());
            }
            let kind = metadata
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("f");

            let mut output = String::new();
            let num_elements = data.len() / itemsize;

            for i in 0..num_elements {
                let start = i * itemsize;
                let end = start + itemsize;
                if end > data.len() {
                    break;
                }
                let bytes = &data[start..end];

                let value = match (kind, itemsize) {
                    ("f", 8) => {
                        let v = f64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        format!("{v}")
                    }
                    ("f", 4) => {
                        let v = f32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]));
                        format!("{v}")
                    }
                    ("i", 8) => {
                        let v = i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        format!("{v}")
                    }
                    ("i", 4) => {
                        let v = i32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]));
                        format!("{v}")
                    }
                    _ => format!("{:?}", bytes),
                };

                if i > 0 {
                    output.push('\n');
                }
                output.push_str(&value);
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
