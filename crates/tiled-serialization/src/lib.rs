pub mod array;
pub mod registry;
pub mod table;

pub use registry::{resolve_media_type, SerializationRegistry};

/// Create a registry with all built-in serializers registered.
pub fn default_registry() -> SerializationRegistry {
    let reg = SerializationRegistry::new();
    array::register_array_serializers(&reg);
    table::register_table_serializers(&reg);
    reg
}
