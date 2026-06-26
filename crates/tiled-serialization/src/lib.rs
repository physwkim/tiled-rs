pub mod array;
pub mod awkward;
pub mod container_json;
pub mod html_container;
pub mod json_seq;
pub mod ragged;
pub mod registry;
pub mod sparse;
pub mod table;

#[cfg(feature = "csv")]
pub mod csv_table;
#[cfg(feature = "csv")]
pub mod excel_table;
#[cfg(feature = "hdf5")]
pub mod hdf5_array;
#[cfg(feature = "hdf5")]
pub mod hdf5_table;
#[cfg(feature = "image")]
pub mod image_array;
#[cfg(feature = "parquet")]
pub mod parquet_table;

pub use registry::{
    SerializationRegistry, SerializeError, UnsupportedShape, negotiate_media_type,
    resolve_media_type,
};

/// Create a registry with all built-in serializers registered.
pub fn default_registry() -> SerializationRegistry {
    let reg = SerializationRegistry::new();
    array::register_array_serializers(&reg);
    table::register_table_serializers(&reg);
    sparse::register_sparse_serializers(&reg);
    ragged::register_ragged_serializers(&reg);
    awkward::register_awkward_serializers(&reg);
    html_container::register_html_serializer(&reg);
    json_seq::register_json_seq_serializer(&reg);
    container_json::register_container_json_serializer(&reg);
    #[cfg(feature = "image")]
    image_array::register_image_serializers(&reg);
    #[cfg(feature = "parquet")]
    parquet_table::register_parquet_serializer(&reg);
    #[cfg(feature = "csv")]
    csv_table::register_csv_table_serializer(&reg);
    #[cfg(feature = "csv")]
    excel_table::register_excel_serializer(&reg);
    #[cfg(feature = "hdf5")]
    hdf5_array::register_hdf5_serializer(&reg);
    #[cfg(feature = "hdf5")]
    hdf5_table::register_hdf5_table_serializer(&reg);
    reg
}
