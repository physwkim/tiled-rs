//! Awkward array serializers.
//!
//! Corresponds to `tiled/serialization/awkward.py`.
//!
//! The wire format for the awkward family is a ZIP archive in which each entry
//! is one named buffer (the same `node{N}-data` / `node{N}-offsets` layout
//! that `awkward.to_buffers` produces).  The tiled-server handler pre-packs
//! the buffer map into a ZIP and passes the archive bytes to the serializer;
//! the `application/zip` serializer returns them unchanged (identity).
//!
//! Python's `application/json` serializer reconstructs the full nested
//! structure via `awkward.from_buffers(form, length, container)` before
//! calling `awkward.to_json` — that requires the `awkward` library, which has
//! no Rust port.  Only `application/zip` is registered here; any other request
//! returns HTTP 406.

use crate::core::structures::StructureFamily;

use crate::serialization::registry::SerializationRegistry;

/// Register built-in awkward serializers.
///
/// The handler pre-packs `HashMap<String, Bytes>` into a ZIP archive before
/// calling into the registry, so the `application/zip` serializer is an
/// identity function.  Metadata (`AwkwardStructure` JSON) is available in the
/// `_metadata` argument for future serializers that need it (e.g. a JSON
/// serializer that reconstructs the array without the awkward library).
pub fn register_awkward_serializers(reg: &SerializationRegistry) {
    // application/zip — named-buffer ZIP archive, matching Python's
    // `to_zipped_buffers` (tiled/serialization/awkward.py:14-25).
    reg.register(
        StructureFamily::Awkward,
        "application/zip",
        Box::new(|data: &[u8], _metadata: &serde_json::Value| {
            Ok(bytes::Bytes::copy_from_slice(data))
        }),
    );

    reg.register_alias(".zip", "application/zip");
}

#[cfg(test)]
mod tests {
    use crate::core::structures::StructureFamily;

    use crate::serialization::registry::SerializationRegistry;

    fn awkward_registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        super::register_awkward_serializers(&reg);
        reg
    }

    /// `application/zip` is registered for the awkward family.
    #[test]
    fn awkward_zip_registered() {
        let reg = awkward_registry();
        assert!(
            reg.dispatch(StructureFamily::Awkward, "application/zip")
                .is_some(),
            "Awkward must have an application/zip serializer"
        );
    }

    /// The `application/zip` serializer is an identity function.
    #[test]
    fn awkward_zip_serializer_is_identity() {
        let reg = awkward_registry();
        let serializer = reg
            .dispatch(StructureFamily::Awkward, "application/zip")
            .unwrap();
        let data = b"fake zip bytes";
        let out = serializer(data, &serde_json::Value::Null).unwrap();
        assert_eq!(&out[..], data);
    }

    /// The `.zip` alias resolves to `application/zip`.
    #[test]
    fn zip_alias_resolves() {
        let reg = awkward_registry();
        assert_eq!(
            reg.resolve_alias(".zip").as_deref(),
            Some("application/zip"),
        );
    }
}
