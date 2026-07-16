//! Serialization registry — maps (StructureFamily, media_type) → serializer function.
//!
//! Corresponds to `tiled/media_type_registration.py`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use crate::core::structures::StructureFamily;

/// Serialization error type.
pub type SerializeError = Box<dyn std::error::Error + Send + Sync>;

/// A serializer-level error signalling that the data's *shape* is incompatible
/// with the requested format (e.g. a >2-D array requested as CSV). Mirrors
/// Python tiled's `UnsupportedShape` (tiled/utils.py:597), which the server
/// maps to HTTP 406 (core.py:441-445). Kept distinct from a generic
/// [`SerializeError`] (I/O, encode failure) so the server can answer 406 vs 500
/// — a serializer returns this boxed and the router downcasts it.
#[derive(Debug)]
pub struct UnsupportedShape {
    pub shape: Vec<usize>,
}

impl std::fmt::Display for UnsupportedShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shape {:?} is incompatible with the requested format",
            self.shape
        )
    }
}

impl std::error::Error for UnsupportedShape {}

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
    ///
    /// Iterates [`StructureFamily::ALL`] — the canonical family set — so every
    /// family the registry can serialize (including `ragged`) appears in the
    /// server's About `formats` map, matching upstream's dynamic
    /// `serialization_registry.structure_families` enumeration.
    pub fn all_formats(&self) -> HashMap<String, Vec<String>> {
        StructureFamily::ALL
            .iter()
            .map(|f| (f.to_string(), self.media_types(*f)))
            .collect()
    }

    /// Get all aliases grouped by family. Iterates [`StructureFamily::ALL`] for
    /// the same reason as [`all_formats`](Self::all_formats); a family with no
    /// registered aliases is omitted from the map.
    pub fn all_aliases(&self) -> HashMap<String, HashMap<String, Vec<String>>> {
        StructureFamily::ALL
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

/// Resolve the appropriate media type. An explicit `?format=` query param is
/// given **hard priority**: if it is present, the `Accept` header is never
/// consulted, matching Python tiled (`tiled/server/core.py:374-419`, which
/// raises `UnsupportedMediaTypes` rather than falling back to `Accept`).
///
/// Resolution order for `format_param` (returns `None` if none apply):
///  1. Verbatim full MIME type (e.g. `"text/csv"`) — accepted only when a
///     serializer is registered for this `family`.
///  2. Registry alias table — bare extensions and dotted forms
///     (e.g. `"png"` / `".png"`) registered via [`SerializationRegistry::register_alias`].
///     No family/dispatch check here: some aliases (e.g. `".zip"` →
///     `"application/zip"`) name a format the **router** produces *outside* the
///     serializer registry, so producibility is the router's concern. The router
///     errors (HTTP 406) when the resolved media type is neither registry-
///     dispatchable nor one it handles itself — it must never serve raw bytes
///     under a foreign Content-Type (see `tiled-server` `build_array_response`/
///     `build_table_response`).
///  3. [`crate::core::media_type::resolve_alias`] — for bare extensions known to
///     the core alias table but not explicitly registered in this registry instance
///     (e.g. `"csv"` → `"text/csv"`). Accepted only when a serializer is registered
///     for this `family`.
///
/// When `format_param` is `None`, fall back to the `Accept` header via
/// [`resolve_media_type`].
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
        if let Some(mt) = crate::core::media_type::resolve_alias(fmt)
            && registry.dispatch(family, mt).is_some()
        {
            return Some(mt.to_string());
        }

        // An explicit `?format=` was given but resolved to nothing serviceable for
        // this family. Give `format` hard priority (Python parity): return `None`
        // so the caller raises an error, rather than silently falling back to the
        // `Accept` default and serving the wrong representation.
        return None;
    }
    resolve_media_type(accept, family, registry)
}

/// Resolve the appropriate media type from an Accept header.
///
/// Each Accept entry is matched in two passes:
/// 1. **Exact match** (including MIME params like `;header=absent`) — lets
///    the caller select a variant-specific serializer registered under the
///    full string (e.g. `"text/csv;header=absent"`).
/// 2. **Base-type match** (params stripped) — the standard case.
///
/// Quality (`q=`) factors are intentionally not parsed; entries are tried
/// in the order the client listed them.
pub fn resolve_media_type(
    accept: &str,
    family: StructureFamily,
    registry: &SerializationRegistry,
) -> Option<String> {
    // An absent/blank Accept header expresses no preference — serve the family
    // default. Python substitutes the family default for a missing Accept
    // before negotiating (`request.headers.get("Accept", default_media_type)`,
    // core.py:387), so a no-Accept request resolves to the default, never 406.
    if accept.trim().is_empty() {
        return default_media_type(family);
    }
    let available = registry.media_types(family);
    for part in accept.split(',') {
        let part_trimmed = part.trim();
        let media_type = part_trimmed.split(';').next().unwrap_or("").trim();

        // Pass 1: exact match preserving MIME params (e.g. "text/csv;header=absent").
        // Skip wildcard entries — "*/*" and "image/*" are not registered media types.
        if media_type != "*/*"
            && media_type != "image/*"
            && available.iter().any(|m| m == part_trimmed)
        {
            return Some(part_trimmed.to_string());
        }

        // Pass 2: base-type match (standard case).
        if media_type == "*/*" {
            return default_media_type(family);
        }
        // Python maps the `image/*` wildcard to image/png for the array family
        // (core.py:397-398, DEFAULT_MEDIA_TYPES[array]["image/*"]), then checks
        // whether image/png is actually serviceable: if no image/png serializer
        // is registered (the `image` feature is off) it falls through to the
        // next Accept entry rather than committing to a format it cannot
        // produce. Mirror both halves — map, then verify before returning.
        if media_type == "image/*" && family == StructureFamily::Array {
            let png = crate::core::media_type::mime::PNG;
            if available.iter().any(|m| m == png) {
                return Some(png.to_string());
            }
            continue;
        }
        if available.iter().any(|m| m == media_type) {
            return Some(media_type.to_string());
        }
    }
    // Every concrete media type the client listed is unserviceable for this
    // family. Python raises `UnsupportedMediaTypes` → HTTP 406 (core.py:413-419);
    // return `None` so the caller does the same, rather than silently serving
    // the family default under a Content-Type the client never asked for.
    None
}

pub(crate) fn default_media_type(family: StructureFamily) -> Option<String> {
    match family {
        StructureFamily::Array => Some(crate::core::media_type::mime::OCTET_STREAM.to_string()),
        StructureFamily::Sparse | StructureFamily::Table => {
            Some(crate::core::media_type::mime::ARROW_FILE.to_string())
        }
        // A no-preference request (blank/`*/*` Accept) on a container resolves to
        // the HTML listing, the only representation the Rust container handler
        // serves by default. This codifies the behavior the `container_full`
        // handler previously obtained via a hardcoded `text/html` fallback, so
        // that an *explicit* unsupported format can now resolve to `None` (→ 406)
        // instead of being silently coerced to HTML. (Python's container default
        // is `application/x-hdf5`, which the Rust server does not implement.)
        StructureFamily::Container => Some("text/html".to_string()),
        // A no-preference request (blank/`*/*` Accept) on a ragged node resolves
        // to JSON list-of-lists (Python DEFAULT_MEDIA_TYPES[ragged]["*/*"] =
        // "application/json", core.py:323).
        StructureFamily::Ragged => Some(crate::core::media_type::mime::JSON.to_string()),
        // A no-preference request on an awkward node resolves to a ZIP archive of
        // raw buffers (Python DEFAULT_MEDIA_TYPES[awkward]["*/*"] = "application/zip",
        // core.py:327 — the only awkward format the Rust server implements).
        StructureFamily::Awkward => Some("application/zip".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn array_registry() -> SerializationRegistry {
        let reg = SerializationRegistry::new();
        crate::serialization::array::register_array_serializers(&reg);
        reg
    }

    #[test]
    fn format_priority_unresolved_returns_none_no_accept_fallback() {
        let reg = array_registry();
        // An explicit but unresolvable `?format=` must NOT fall back to the
        // Accept header (which here asks for the serviceable octet-stream).
        // Python gives `format` hard priority and raises UnsupportedMediaTypes.
        let got = negotiate_media_type(
            Some("definitely-not-a-format"),
            "application/octet-stream",
            StructureFamily::Array,
            &reg,
        );
        assert_eq!(
            got, None,
            "unresolved ?format= must not fall back to Accept"
        );
    }

    #[test]
    fn format_priority_resolvable_returns_media_type() {
        let reg = array_registry();
        // "csv" resolves via the core alias table to text/csv, which the array
        // family serializes → returned despite Accept asking for octet-stream.
        let got = negotiate_media_type(
            Some("csv"),
            "application/octet-stream",
            StructureFamily::Array,
            &reg,
        );
        assert_eq!(got.as_deref(), Some(crate::core::media_type::mime::CSV));
    }

    #[test]
    fn no_format_falls_back_to_accept() {
        let reg = array_registry();
        let got = negotiate_media_type(
            None,
            crate::core::media_type::mime::CSV,
            StructureFamily::Array,
            &reg,
        );
        assert_eq!(got.as_deref(), Some(crate::core::media_type::mime::CSV));
    }

    #[test]
    fn format_alias_resolves_optimistically_router_enforces() {
        // With the full registry, `.zip` is a globally-registered alias
        // (html_container) → `application/zip`. negotiate resolves it even for
        // the array family (which cannot serialize it): producibility is the
        // router's concern (it errors HTTP 406). This documents the split.
        let reg = crate::serialization::default_registry();
        let got = negotiate_media_type(
            Some("zip"),
            "application/octet-stream",
            StructureFamily::Array,
            &reg,
        );
        assert_eq!(got.as_deref(), Some("application/zip"));
        assert!(
            reg.dispatch(StructureFamily::Array, "application/zip")
                .is_none(),
            "no array serializer for application/zip → router must reject"
        );
    }

    /// Finding 6: Python maps the `image/*` Accept wildcard to image/png for the
    /// array family (core.py:397-398). When an image/png serializer is
    /// registered, `image/*` must resolve to it instead of the octet-stream
    /// default.
    #[test]
    fn image_wildcard_resolves_to_png_for_array_when_registered() {
        let reg = array_registry();
        // Register a stand-in image/png serializer. The real one is feature-
        // gated, but wildcard resolution must not depend on the `image` feature
        // being compiled into this test — only on a serializer being present.
        reg.register(
            StructureFamily::Array,
            crate::core::media_type::mime::PNG,
            Box::new(|_d: &[u8], _m: &serde_json::Value| Ok(bytes::Bytes::new())),
        );
        let got = resolve_media_type("image/*", StructureFamily::Array, &reg);
        assert_eq!(got.as_deref(), Some(crate::core::media_type::mime::PNG));
    }

    /// `image/*` with no registered image/png serializer must NOT invent the
    /// format: it falls through to the next Accept entry (mirroring Python's
    /// inner `for…else: continue`), and — when `image/*` is the ONLY requested
    /// type — yields `None` so the caller raises HTTP 406. Python maps
    /// `image/*` → `image/png`, finds no serializer, and raises
    /// `UnsupportedMediaTypes` (core.py:397-419); it does NOT fall back to the
    /// octet-stream default.
    #[test]
    fn image_wildcard_falls_through_when_png_unregistered() {
        let reg = array_registry(); // octet-stream + CSV, but no image/png
        // `image/*` alone, no png serializer → None (Python 406), not a silent
        // octet-stream default the client never asked for.
        let only = resolve_media_type("image/*", StructureFamily::Array, &reg);
        assert_eq!(only, None);
        // `image/*, application/octet-stream` → the second, serviceable entry
        // wins (faithful Accept-list fallback, not a short-circuit on image/*).
        let listed = resolve_media_type(
            "image/*, application/octet-stream",
            StructureFamily::Array,
            &reg,
        );
        assert_eq!(
            listed.as_deref(),
            Some(crate::core::media_type::mime::OCTET_STREAM)
        );
    }

    /// S6/H1: a concrete Accept the family cannot serve must resolve to `None`
    /// (caller → HTTP 406), never the family default. A blank/absent Accept,
    /// by contrast, expresses no preference and resolves to the family default.
    #[test]
    fn unsupported_concrete_accept_returns_none_not_default() {
        let reg = array_registry(); // octet-stream + CSV
        // Concrete unsupported type → None (Python UnsupportedMediaTypes/406).
        assert_eq!(
            resolve_media_type("text/xml", StructureFamily::Array, &reg),
            None,
            "an unsupported concrete Accept must 406, not serve octet-stream",
        );
        // Empty/absent Accept → family default (no preference).
        assert_eq!(
            resolve_media_type("", StructureFamily::Array, &reg).as_deref(),
            Some(crate::core::media_type::mime::OCTET_STREAM),
            "a missing Accept must resolve to the family default",
        );
        // Table family: unsupported concrete → None; empty → Arrow default.
        let treg = crate::serialization::default_registry();
        assert_eq!(
            resolve_media_type("text/xml", StructureFamily::Table, &treg),
            None,
        );
        assert_eq!(
            resolve_media_type("", StructureFamily::Table, &treg).as_deref(),
            Some(crate::core::media_type::mime::ARROW_FILE),
        );
    }

    /// M5: `Accept: text/csv;header=absent` must resolve to the exact
    /// registered variant — not fall back to plain `text/csv`.
    /// `Accept: text/csv;q=0.5` (quality param) must fall back to `text/csv`
    /// because `q` is a negotiation weight, not a format selector.
    #[test]
    fn accept_mime_params_select_exact_variant_if_registered() {
        // Build a minimal table registry that has both "text/csv" and
        // "text/csv;header=absent" registered.
        let reg = SerializationRegistry::new();
        reg.register(
            StructureFamily::Table,
            crate::core::media_type::mime::CSV,
            Box::new(|_d: &[u8], _m: &serde_json::Value| Ok(bytes::Bytes::new())),
        );
        reg.register(
            StructureFamily::Table,
            "text/csv;header=absent",
            Box::new(|_d: &[u8], _m: &serde_json::Value| Ok(bytes::Bytes::new())),
        );

        // Exact variant requested via Accept.
        let got = resolve_media_type("text/csv;header=absent", StructureFamily::Table, &reg);
        assert_eq!(
            got.as_deref(),
            Some("text/csv;header=absent"),
            "exact MIME param must select the variant serializer"
        );

        // Quality param (q=) is NOT a format selector — must fall back to base.
        let got_q = resolve_media_type("text/csv;q=0.5", StructureFamily::Table, &reg);
        assert_eq!(
            got_q.as_deref(),
            Some(crate::core::media_type::mime::CSV),
            "q= quality factor must not prevent base-type match"
        );

        // Plain base type still resolves normally.
        let got_plain = resolve_media_type("text/csv", StructureFamily::Table, &reg);
        assert_eq!(
            got_plain.as_deref(),
            Some(crate::core::media_type::mime::CSV)
        );
    }

    /// The wildcard rewrite is array-only — Python guards on
    /// `structure_family == array`, so `image/*` is not special for the table
    /// family even if an image/png serializer happens to be registered there.
    #[test]
    fn image_wildcard_not_special_for_non_array_family() {
        let reg = SerializationRegistry::new();
        reg.register(
            StructureFamily::Table,
            crate::core::media_type::mime::PNG,
            Box::new(|_d: &[u8], _m: &serde_json::Value| Ok(bytes::Bytes::new())),
        );
        let got = resolve_media_type("image/*", StructureFamily::Table, &reg);
        assert_ne!(got.as_deref(), Some(crate::core::media_type::mime::PNG));
    }
}
