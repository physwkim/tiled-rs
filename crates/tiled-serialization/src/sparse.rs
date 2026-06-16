//! Sparse serializers.
//!
//! Corresponds to `tiled/serialization/sparse.py`.
//!
//! The tiled-server converts SparseData into a COO Arrow IPC table
//! (columns dim0..dimN, data) before calling the serializer, so every
//! serializer here is identical to the table variant — just registered
//! under `StructureFamily::Sparse`.

use tiled_core::media_type::mime;
use tiled_core::structures::StructureFamily;

use crate::registry::SerializationRegistry;

/// Register built-in sparse serializers.
///
/// Python sparse.py delegates all DataFrame formats to the table serializers
/// after converting the sparse array to a COO DataFrame.  Rust does the same
/// conversion upstream (in tiled-server), so the serializer functions are
/// identical to those registered for `StructureFamily::Table`.
pub fn register_sparse_serializers(reg: &SerializationRegistry) {
    // Arrow IPC — canonical sparse format, mirrors APACHE_ARROW_FILE_MIME_TYPE.
    reg.register(
        StructureFamily::Sparse,
        mime::ARROW_FILE,
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    #[cfg(feature = "csv")]
    {
        for media_type in [
            mime::CSV,
            "text/x-comma-separated-values",
            mime::PLAIN,
            mime::EXCEL,
        ] {
            reg.register(
                StructureFamily::Sparse,
                media_type,
                crate::csv_table::csv_table_serializer(true),
            );
            let absent = format!("{media_type};header=absent");
            reg.register(
                StructureFamily::Sparse,
                &absent,
                crate::csv_table::csv_table_serializer(false),
            );
        }
    }

    #[cfg(feature = "parquet")]
    reg.register(
        StructureFamily::Sparse,
        mime::PARQUET,
        crate::parquet_table::parquet_serializer(),
    );
}

#[cfg(test)]
mod tests {
    use tiled_core::media_type::mime;
    use tiled_core::structures::StructureFamily;

    use crate::registry::{SerializationRegistry, default_media_type};

    fn sparse_registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        super::register_sparse_serializers(&reg);
        reg
    }

    #[test]
    fn sparse_default_media_type_is_arrow_file() {
        assert_eq!(
            default_media_type(StructureFamily::Sparse).as_deref(),
            Some(mime::ARROW_FILE),
        );
    }

    #[test]
    fn sparse_arrow_file_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::ARROW_FILE)
                .is_some(),
            "Sparse must have an Arrow IPC serializer"
        );
    }

    #[cfg(feature = "csv")]
    #[test]
    fn sparse_csv_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::CSV).is_some(),
            "Sparse must have a text/csv serializer"
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn sparse_parquet_registered() {
        let reg = sparse_registry();
        assert!(
            reg.dispatch(StructureFamily::Sparse, mime::PARQUET)
                .is_some(),
            "Sparse must have a parquet serializer"
        );
    }
}
