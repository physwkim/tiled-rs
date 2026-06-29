//! `application/json` serializer for the `container` family.
//!
//! Python registers `serialize_json` for `StructureFamily.container`
//! (`tiled/serialization/container.py:91-115`, under the `orjson` guard): it
//! exports the subtree as a recursive `{"contents": {...}, "metadata": {...}}`
//! JSON tree, one such node per child, with table columns expanded into
//! synthetic array children.
//!
//! That recursive walk is async — it descends the live adapter tree, applies
//! the per-node access filter, and reads each node's metadata. The registry's
//! [`SerializerFn`] is a synchronous byte transform with no adapter access, so
//! (exactly as the router produces `application/zip` itself) the **router**
//! assembles and encodes the tree in `container_full`. This serializer is the
//! registration that makes `application/json` resolvable for the container
//! family — both `?format=json`/`Accept: application/json` negotiation
//! (`negotiate_media_type`) and the About `formats` advertisement draw from the
//! registered media types — and it returns the router-encoded tree bytes
//! unchanged.

use bytes::Bytes;

use crate::core::structures::StructureFamily;

use crate::serialization::registry::{SerializationRegistry, SerializerFn};

pub const APPLICATION_JSON: &str = "application/json";

pub fn register_container_json_serializer(reg: &SerializationRegistry) {
    // No `.json` extension alias: `?format=json` resolves through the core
    // alias table (`crate::core::media_type::resolve_alias("json")`) once a
    // serializer is registered for the family, and `Accept: application/json`
    // resolves through the registered media type. This matches how the array
    // and table `application/json` serializers are registered.
    reg.register(
        StructureFamily::Container,
        APPLICATION_JSON,
        container_json_serializer(),
    );
}

fn container_json_serializer() -> SerializerFn {
    Box::new(
        |data, _meta| -> Result<Bytes, crate::serialization::registry::SerializeError> {
            // `data` is the `{contents, metadata}` tree the container handler
            // already assembled and encoded (it owns the async adapter walk and
            // access filter). Hand it back verbatim — see the module docs for
            // why the recursive build cannot live in this sync serializer.
            Ok(Bytes::copy_from_slice(data))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M4b: `application/json` must be registered for the container family so
    /// `negotiate_media_type` resolves it (Accept + `?format=json`) and the
    /// About `formats` list advertises it. Parity with array/table JSON.
    #[test]
    fn json_registered_for_container() {
        let reg = SerializationRegistry::new();
        register_container_json_serializer(&reg);
        assert!(
            reg.dispatch(StructureFamily::Container, APPLICATION_JSON)
                .is_some(),
            "container application/json must be registered (Python container.py:91-115)"
        );
    }

    /// The serializer hands the router-assembled tree back unchanged.
    #[test]
    fn serializer_returns_tree_verbatim() {
        let reg = SerializationRegistry::new();
        register_container_json_serializer(&reg);
        let serializer = reg
            .dispatch(StructureFamily::Container, APPLICATION_JSON)
            .expect("must be registered");
        let tree = serde_json::to_vec(&serde_json::json!({
            "contents": {"a": {"contents": {}, "metadata": {}}},
            "metadata": {"k": "v"}
        }))
        .unwrap();
        let out = serializer(&tree, &serde_json::Value::Null).unwrap();
        assert_eq!(&out[..], &tree[..]);
    }
}
