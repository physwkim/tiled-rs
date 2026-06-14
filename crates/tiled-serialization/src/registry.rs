//! Serialization registry — maps (StructureFamily, media_type) → serializer function.
//!
//! Corresponds to `tiled/media_type_registration.py`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use tiled_core::structures::StructureFamily;

/// Serialization error type.
pub type SerializeError = Box<dyn std::error::Error + Send + Sync>;

/// A serializer function that converts raw data + metadata into bytes.
pub type SerializerFn =
    Box<dyn Fn(&[u8], &serde_json::Value) -> Result<bytes::Bytes, SerializeError> + Send + Sync>;

/// Registry mapping (StructureFamily, media_type) → serializer.
pub struct SerializationRegistry {
    lookup: DashMap<(StructureFamily, Arc<str>), Arc<SerializerFn>>,
    aliases: DashMap<Arc<str>, Arc<str>>,
}

impl SerializationRegistry {
    pub fn new() -> Self {
        Self {
            lookup: DashMap::new(),
            aliases: DashMap::new(),
        }
    }

    pub fn register(&self, family: StructureFamily, media_type: &str, serializer: SerializerFn) {
        self.lookup
            .insert((family, Arc::from(media_type)), Arc::new(serializer));
    }

    pub fn register_alias(&self, extension: &str, media_type: &str) {
        self.aliases
            .insert(Arc::from(extension), Arc::from(media_type));
    }

    /// Dispatch: get the serializer for a given (family, media_type).
    pub fn dispatch(&self, family: StructureFamily, media_type: &str) -> Option<Arc<SerializerFn>> {
        // Avoid allocating a key by scanning — the lookup table is small.
        self.lookup
            .iter()
            .find(|entry| entry.key().0 == family && &*entry.key().1 == media_type)
            .map(|entry| entry.value().clone())
    }

    /// Get all registered media types for a given structure family.
    pub fn media_types(&self, family: StructureFamily) -> Vec<String> {
        self.lookup
            .iter()
            .filter(|entry| entry.key().0 == family)
            .map(|entry| entry.key().1.to_string())
            .collect()
    }

    /// Get aliases for a given structure family.
    pub fn aliases(&self, family: StructureFamily) -> HashMap<String, Vec<String>> {
        let media_types = self.media_types(family);
        let mut result: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.aliases.iter() {
            let mt = entry.value().to_string();
            if media_types.iter().any(|m| m == &mt) {
                result.entry(mt).or_default().push(entry.key().to_string());
            }
        }
        result
    }

    /// Resolve an extension alias to a media type.
    pub fn resolve_alias(&self, extension: &str) -> Option<String> {
        self.aliases
            .iter()
            .find(|entry| &**entry.key() == extension)
            .map(|entry| entry.value().to_string())
    }

    /// Get all formats as a HashMap (family_name → Vec<media_type>).
    pub fn all_formats(&self) -> HashMap<String, Vec<String>> {
        let families = [
            StructureFamily::Array,
            StructureFamily::Table,
            StructureFamily::Sparse,
            StructureFamily::Awkward,
            StructureFamily::Container,
        ];
        families
            .iter()
            .map(|f| (f.to_string(), self.media_types(*f)))
            .collect()
    }

    /// Get all aliases grouped by family.
    pub fn all_aliases(&self) -> HashMap<String, HashMap<String, Vec<String>>> {
        let families = [
            StructureFamily::Array,
            StructureFamily::Table,
            StructureFamily::Sparse,
            StructureFamily::Awkward,
            StructureFamily::Container,
        ];
        families
            .iter()
            .filter_map(|f| {
                let a = self.aliases(*f);
                if a.is_empty() {
                    None
                } else {
                    Some((f.to_string(), a))
                }
            })
            .collect()
    }
}

impl Default for SerializationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve the appropriate media type, honoring an explicit `?format=` query
/// param before falling back to the Accept header.
///
/// Resolution order for `format_param`:
///  1. Verbatim full MIME type (e.g. `"text/csv"`) — accepted only when a
///     serializer is registered for this `family`.
///  2. Registry alias table — bare extensions and dotted forms
///     (e.g. `"png"` / `".png"`) registered via [`SerializationRegistry::register_alias`].
///     No family check here; the router may handle some formats (e.g. `application/zip`)
///     outside the serializer registry.
///  3. [`tiled_core::media_type::resolve_alias`] — for bare extensions known to
///     the core alias table but not explicitly registered in this registry instance
///     (e.g. `"csv"` → `"text/csv"`). Accepted only when a serializer is registered
///     for this `family`.
///  4. Fall back to the `Accept` header via [`resolve_media_type`].
pub fn negotiate_media_type(
    format_param: Option<&str>,
    accept: &str,
    family: StructureFamily,
    registry: &SerializationRegistry,
) -> Option<String> {
    if let Some(fmt) = format_param {
        // (1) Verbatim full MIME type — only accepted if a serializer for this family exists.
        if fmt.contains('/') && registry.dispatch(family, fmt).is_some() {
            return Some(fmt.to_string());
        }

        // (2) Registry alias table — bare or dotted extension.
        if let Some(mt) = registry.resolve_alias(fmt) {
            return Some(mt);
        }
        if !fmt.starts_with('.')
            && let Some(mt) = registry.resolve_alias(&format!(".{fmt}"))
        {
            return Some(mt);
        }

        // (3) Core alias table — handles extensions not explicitly registered in the
        // registry (e.g. "csv" → "text/csv"). Only accepted when a serializer is
        // registered for this family.
        if let Some(mt) = tiled_core::media_type::resolve_alias(fmt)
            && registry.dispatch(family, mt).is_some()
        {
            return Some(mt.to_string());
        }
    }
    resolve_media_type(accept, family, registry)
}

/// Resolve the appropriate media type from an Accept header.
pub fn resolve_media_type(
    accept: &str,
    family: StructureFamily,
    registry: &SerializationRegistry,
) -> Option<String> {
    let available = registry.media_types(family);
    for part in accept.split(',') {
        let media_type = part.trim().split(';').next().unwrap_or("").trim();
        if media_type == "*/*" {
            return default_media_type(family);
        }
        if available.iter().any(|m| m == media_type) {
            return Some(media_type.to_string());
        }
    }
    default_media_type(family)
}

fn default_media_type(family: StructureFamily) -> Option<String> {
    match family {
        StructureFamily::Array | StructureFamily::Sparse => {
            Some(tiled_core::media_type::mime::OCTET_STREAM.to_string())
        }
        StructureFamily::Table => Some(tiled_core::media_type::mime::ARROW_FILE.to_string()),
        _ => None,
    }
}
