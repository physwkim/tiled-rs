//! Serialization registry — maps (StructureFamily, media_type) → serializer function.
//!
//! Corresponds to `tiled/media_type_registration.py`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use tiled_core::structures::StructureFamily;

/// A serializer function that converts raw data + metadata into bytes.
pub type SerializerFn =
    Box<dyn Fn(&[u8], &serde_json::Value) -> Result<bytes::Bytes, anyhow::Error> + Send + Sync>;

/// Registry mapping (StructureFamily, media_type) → serializer.
pub struct SerializationRegistry {
    lookup: DashMap<(StructureFamily, String), Arc<SerializerFn>>,
    aliases: DashMap<String, String>,
}

impl SerializationRegistry {
    pub fn new() -> Self {
        Self {
            lookup: DashMap::new(),
            aliases: DashMap::new(),
        }
    }

    /// Register a serializer for a (family, media_type) pair.
    pub fn register(
        &self,
        family: StructureFamily,
        media_type: &str,
        serializer: SerializerFn,
    ) {
        self.lookup
            .insert((family, media_type.to_string()), Arc::new(serializer));
    }

    /// Register a file extension alias for a media type.
    pub fn register_alias(&self, extension: &str, media_type: &str) {
        self.aliases
            .insert(extension.to_string(), media_type.to_string());
    }

    /// Dispatch: get the serializer for a given (family, media_type).
    pub fn dispatch(
        &self,
        family: StructureFamily,
        media_type: &str,
    ) -> Option<Arc<SerializerFn>> {
        self.lookup
            .get(&(family, media_type.to_string()))
            .map(|r| r.value().clone())
    }

    /// Get all registered media types for a given structure family.
    pub fn media_types(&self, family: StructureFamily) -> Vec<String> {
        self.lookup
            .iter()
            .filter(|entry| entry.key().0 == family)
            .map(|entry| entry.key().1.clone())
            .collect()
    }

    /// Get aliases for a given structure family.
    pub fn aliases(&self, family: StructureFamily) -> HashMap<String, Vec<String>> {
        let media_types = self.media_types(family);
        let mut result: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.aliases.iter() {
            if media_types.contains(entry.value()) {
                result
                    .entry(entry.value().clone())
                    .or_default()
                    .push(entry.key().clone());
            }
        }
        result
    }

    /// Resolve an extension alias to a media type.
    pub fn resolve_alias(&self, extension: &str) -> Option<String> {
        self.aliases.get(extension).map(|r| r.value().clone())
    }

    /// Get all formats as a HashMap (family_name → Vec<media_type>).
    pub fn all_formats(&self) -> HashMap<String, Vec<String>> {
        let mut formats = HashMap::new();
        for family in &[
            StructureFamily::Array,
            StructureFamily::Table,
            StructureFamily::Sparse,
            StructureFamily::Awkward,
            StructureFamily::Container,
        ] {
            formats.insert(family.to_string(), self.media_types(*family));
        }
        formats
    }

    /// Get all aliases grouped by family.
    pub fn all_aliases(&self) -> HashMap<String, HashMap<String, Vec<String>>> {
        let mut result = HashMap::new();
        for family in &[
            StructureFamily::Array,
            StructureFamily::Table,
            StructureFamily::Sparse,
            StructureFamily::Awkward,
            StructureFamily::Container,
        ] {
            let family_aliases = self.aliases(*family);
            if !family_aliases.is_empty() {
                result.insert(family.to_string(), family_aliases);
            }
        }
        result
    }
}

impl Default for SerializationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Resolve the appropriate media type from an Accept header.
pub fn resolve_media_type(
    accept: &str,
    family: StructureFamily,
    registry: &SerializationRegistry,
) -> Option<String> {
    // Check for explicit media types in Accept header
    let available = registry.media_types(family);
    for part in accept.split(',') {
        let media_type = part.trim().split(';').next().unwrap_or("").trim();
        if media_type == "*/*" {
            return default_media_type(family);
        }
        if available.contains(&media_type.to_string()) {
            return Some(media_type.to_string());
        }
    }
    // Fallback to default
    default_media_type(family)
}

fn default_media_type(family: StructureFamily) -> Option<String> {
    match family {
        StructureFamily::Array | StructureFamily::Sparse => {
            Some(tiled_core::media_type::mime::OCTET_STREAM.to_string())
        }
        StructureFamily::Table => {
            Some(tiled_core::media_type::mime::ARROW_FILE.to_string())
        }
        _ => None,
    }
}
