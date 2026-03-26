//! Table serializers.
//!
//! Corresponds to `tiled/serialization/table.py`.

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::SerializationRegistry;

/// Register built-in table serializers.
pub fn register_table_serializers(registry: &SerializationRegistry) {
    // Arrow IPC format
    registry.register(
        StructureFamily::Table,
        mime::ARROW_FILE,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            // Data is already Arrow IPC bytes when coming from ArrowTable serialization
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );
}
