//! API request/response schemas (Pydantic models → serde structs).
//!
//! These correspond to the Pydantic schemas in `tiled/server/schemas.py` and `tiled/schemas.py`.
//! They define the wire format for the REST API.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::data_source::DataSource;
use crate::core::structures::{Spec, StructureFamily};

// ---------------------------------------------------------------------------
// Node metadata response
// ---------------------------------------------------------------------------

/// Attributes of a node, returned in search results and metadata endpoints.
///
/// Matches Python `NodeAttributes` (server/schemas.py lines 177-196).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAttributes {
    /// Path ancestors (REQUIRED). E.g. `["root", "subgroup"]`.
    pub ancestors: Vec<String>,
    /// Structure family of this node (optional in wire format).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structure_family: Option<StructureFamily>,
    /// Specs this node conforms to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub specs: Option<Vec<Spec>>,
    /// User-supplied metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// Structure payload — type depends on `structure_family`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structure: Option<serde_json::Value>,
    /// Access blob (auth-related).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_blob: Option<serde_json::Value>,
    /// Sorting direction for container children.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sorting: Option<Vec<SortingItem>>,
    /// Data sources (populated when requested).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_sources: Option<Vec<DataSource>>,
}

/// Sorting item with key and direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortingItem {
    pub key: String,
    pub direction: SortDirection,
}

/// Sorting direction — serializes as integer (1 = ascending, -1 = descending)
/// to match Python `SortingDirection(int, enum.Enum)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl Serialize for SortDirection {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Ascending => serializer.serialize_i8(1),
            Self::Descending => serializer.serialize_i8(-1),
        }
    }
}

impl<'de> Deserialize<'de> for SortDirection {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v = i8::deserialize(deserializer)?;
        match v {
            1 => Ok(Self::Ascending),
            -1 => Ok(Self::Descending),
            _ => Err(serde::de::Error::custom(format!(
                "invalid sort direction: {v}, expected 1 or -1"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Generic Response wrapper (matches Python tiled/schemas.py lines 38-50)
// ---------------------------------------------------------------------------

/// Top-level API response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response<D = serde_json::Value> {
    pub data: Option<D>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

/// Error payload inside a Response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Per-family link types (Python lines 204-243)
// ---------------------------------------------------------------------------

/// Links for a container node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub search: String,
    pub full: String,
}

/// Links for an array node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub full: String,
    pub block: String,
}

/// Links for a dataframe/table node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFrameLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub full: String,
    pub partition: String,
}

/// Links for a sparse array node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub full: String,
    pub block: String,
}

/// Links for an awkward array node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwkwardLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub full: String,
    pub buffers: String,
}

// ---------------------------------------------------------------------------
// Node links — generic (kept for backwards compat / flexible use)
// ---------------------------------------------------------------------------

/// Links associated with a node response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeLinks {
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_link: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

impl NodeLinks {
    /// True when no link is set. Used to omit the `links` key entirely for
    /// `omit_links` responses — parity with Python, which skips the key rather
    /// than emitting an empty object (`server/core.py:577,616`). In normal
    /// responses `self` is always populated, so this only fires when a handler
    /// deliberately clears links to honor `?omit_links=true`.
    pub fn is_empty(&self) -> bool {
        self.self_link.is_none()
            && self.search.is_none()
            && self.full.is_none()
            && self.extra.is_empty()
    }
}

/// A single resource in the API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource<A = NodeAttributes> {
    pub id: String,
    pub attributes: A,
    // Omit `links` entirely when empty so an `omit_links` response drops the key
    // (Python parity); a normal node always has `self`, so this never fires off
    // the omit_links path.
    #[serde(default, skip_serializing_if = "NodeLinks::is_empty")]
    pub links: NodeLinks,
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

/// Pagination links (Python lines 53-58).
///
/// Key-presence contract mirrors upstream `pagination_links`
/// (tiled/server/core.py:122-147), which seeds `{"self", "first", "next"}` as
/// always-present keys (value `null` when there is no next/first page) and
/// only conditionally adds `last`/`prev`. The Python client bracket-indexes
/// `content["links"]["next"]` (client/container.py:255/480/547, base.py:108,
/// composite.py:42), so `next` and `first` MUST always serialize — an explicit
/// `null` on the terminal page, never a dropped key, or the client raises
/// `KeyError: 'next'`. `last`/`prev` keep `skip_serializing_if` because
/// upstream never seeds those keys (they are absent, not null, when unset).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    pub first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
    pub next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
}

/// Container metadata (count of children).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerMeta {
    pub count: usize,
}

// ---------------------------------------------------------------------------
// About / Discovery (tiled/schemas.py lines 28-37)
// ---------------------------------------------------------------------------

/// Server information returned by `GET /api/v1/`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct About {
    pub api_version: u32,
    pub library_version: String,
    pub formats: HashMap<String, Vec<String>>,
    pub aliases: HashMap<String, HashMap<String, Vec<String>>>,
    /// Query type names (just strings, not objects).
    pub queries: Vec<String>,
    pub authentication: AboutAuthentication,
    pub links: HashMap<String, String>,
    pub meta: HashMap<String, serde_json::Value>,
}

/// Authentication info in About response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AboutAuthentication {
    pub required: bool,
    pub providers: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// NodeStructure (Python schemas.py line 73-77)
// ---------------------------------------------------------------------------

/// Wire-format structure for containers in API responses.
///
/// `contents` is serialized ALWAYS, as an explicit JSON `null` when the
/// children are not inlined — never skipped. Upstream's pydantic `NodeStructure`
/// dumps `Optional` fields as explicit `null` (`NodeStructure(contents=None)` →
/// `{"contents": null, "count": N}`), and the port's own inlining owner
/// ([`crate::server::core::build_container_structure`]) already emits that shape
/// via a `json!`. Omitting the key on the count-only paths diverged from both;
/// `skip_serializing_if` is deliberately absent so every container structure
/// carries the key uniformly. This struct is container-only (leaves serialize
/// their data-source structure instead), so the always-present key never leaks
/// onto non-container families. It is never deserialized from a payload that
/// could omit `contents`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStructure {
    pub contents: Option<serde_json::Value>,
    pub count: usize,
}

// ---------------------------------------------------------------------------
// EntryFields (Python lines 61-70)
// ---------------------------------------------------------------------------

/// Fields that can be requested for each entry in a search response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryFields {
    Metadata,
    Structure,
    StructureFamily,
    Specs,
    DataSources,
    Count,
    Sorting,
    None,
}

// ---------------------------------------------------------------------------
// Write endpoints
// ---------------------------------------------------------------------------

/// Request body for creating a new node.
///
/// `id` is the requested name (key) of the new child under the parent path
/// (POST `/api/v1/register/<parent>` registers `<parent>/<id>`). The wire
/// field is named `id`, matching Python tiled's `PostMetadataRequest.id`
/// (tiled/server/schemas.py:462) — a real Python tiled server ignores any
/// top-level `key` field, so a client must send `id` to request an explicit
/// name. `key` is still accepted on deserialize (`serde(alias)`) for
/// back-compat with pre-existing Rust clients that sent the old shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMetadataRequest {
    pub structure_family: StructureFamily,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub specs: Vec<Spec>,
    #[serde(default)]
    pub data_sources: Vec<DataSource>,
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "key")]
    pub id: Option<String>,
    /// Optional access blob the client wants to attach to this node.
    /// Passed to `AccessPolicy::init_node` for validation / modification
    /// before being stored. Mirrors Python `PostMetadataRequest.access_blob`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_blob: Option<serde_json::Value>,
}

/// Response for creating a new node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMetadataResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_sources: Option<Vec<DataSource>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_blob: Option<serde_json::Value>,
}

/// Request body for updating metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutMetadataRequest {
    pub metadata: serde_json::Value,
}

/// Request body for updating specs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutSpecsRequest {
    pub specs: Vec<Spec>,
}

// ---------------------------------------------------------------------------
// Distinct response (`GET /api/v1/distinct/{path}`)
// ---------------------------------------------------------------------------

/// One distinct value, with an optional occurrence count. Mirrors Python
/// `DistinctValueInfo` (server/schemas.py:504-506). `count` is `null` unless
/// the request set `?counts=true`. `value` is `null` for the group of rows
/// where the key is absent (json_extract → NULL, like Python).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistinctValueInfo {
    pub value: serde_json::Value,
    pub count: Option<i64>,
}

/// Response body for `GET /api/v1/distinct/{path}`. Mirrors Python
/// `GetDistinctResponse` (server/schemas.py:509-512). Each facet is always
/// present on the wire (possibly `null`), matching Python's `model_dump()`
/// default; a facet is `null` when its query flag was not requested.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetDistinctResponse {
    pub metadata: Option<HashMap<String, Vec<DistinctValueInfo>>>,
    pub structure_families: Option<Vec<DistinctValueInfo>>,
    pub specs: Option<Vec<DistinctValueInfo>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_direction_serde() {
        let item = SortingItem {
            key: "_".into(),
            direction: SortDirection::Ascending,
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["direction"], 1);

        let item = SortingItem {
            key: "name".into(),
            direction: SortDirection::Descending,
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["direction"], -1);

        // Roundtrip
        let back: SortingItem = serde_json::from_value(json).unwrap();
        assert_eq!(back.direction, SortDirection::Descending);
    }

    #[test]
    fn test_response_envelope() {
        let resp: Response<Resource> = Response {
            data: Some(Resource {
                id: "test".into(),
                attributes: NodeAttributes {
                    ancestors: vec![],
                    structure_family: Some(StructureFamily::Array),
                    specs: None,
                    metadata: Some(serde_json::json!({"sample": "Cu"})),
                    structure: None,
                    access_blob: None,
                    sorting: None,
                    data_sources: None,
                },
                links: NodeLinks::default(),
            }),
            error: None,
            links: None,
            meta: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["data"]["id"], "test");
        assert!(json["data"]["attributes"]["ancestors"].is_array());
    }

    #[test]
    fn test_post_metadata_request_id_field_wire_shape() {
        // Upstream shape (Python tiled `PostMetadataRequest.id`,
        // server/schemas.py:462): top-level `id`.
        let json = serde_json::json!({
            "structure_family": "container",
            "id": "explicit_id",
        });
        let req: PostMetadataRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.id.as_deref(), Some("explicit_id"));

        // Legacy Rust-client shape: top-level `key` is still accepted via
        // serde alias for back-compat.
        let json = serde_json::json!({
            "structure_family": "container",
            "key": "legacy_key",
        });
        let req: PostMetadataRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.id.as_deref(), Some("legacy_key"));

        // Serialization must always emit the canonical `id` field, never
        // `key` — this is what makes the Rust HTTP client compatible with a
        // real Python tiled server.
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["id"], "legacy_key");
        assert!(json.get("key").is_none());
    }

    #[test]
    fn test_node_attributes_ancestors_required() {
        // ancestors is required — deserialization without it should fail
        let json = serde_json::json!({
            "structure_family": "array",
        });
        let result: Result<NodeAttributes, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_about_schema() {
        let about = About {
            api_version: 0,
            library_version: "0.1.0".into(),
            formats: HashMap::from([("array".into(), vec!["application/octet-stream".into()])]),
            aliases: HashMap::new(),
            queries: vec!["fulltext".into(), "lookup".into()],
            authentication: AboutAuthentication {
                required: false,
                providers: vec![],
                links: None,
            },
            links: HashMap::from([("self".into(), "http://localhost:8000/api/v1/".into())]),
            meta: HashMap::new(),
        };
        let json = serde_json::to_value(&about).unwrap();
        assert_eq!(json["api_version"], 0);
        assert!(json["queries"].is_array());
        assert_eq!(json["queries"][0], "fulltext");
        assert_eq!(json["authentication"]["required"], false);
    }

    #[test]
    fn test_pagination_links() {
        let links = PaginationLinks {
            self_link: "http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=10".into(),
            first: Some(
                "http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=10".into(),
            ),
            last: Some(
                "http://localhost:8000/api/v1/search/?page[offset]=90&page[limit]=10".into(),
            ),
            next: Some(
                "http://localhost:8000/api/v1/search/?page[offset]=10&page[limit]=10".into(),
            ),
            prev: None,
        };
        let json = serde_json::to_value(&links).unwrap();
        assert!(json["self"].is_string());
        assert!(json["prev"].is_null());
    }

    /// Wire-contract boundary: which keys are *present* in the serialized
    /// `links` object. Upstream `pagination_links`
    /// (tiled/server/core.py:122-147) seeds `{"self", "first", "next"}` as
    /// always-present keys (value `null` when there is no next/first page) and
    /// only conditionally adds `last`/`prev`. The Python client bracket-indexes
    /// `content["links"]["next"]` (client/container.py:255/480/547,
    /// base.py:108, composite.py:42), so a missing `next` key raises
    /// `KeyError` on the terminal page. `next` and `first` must therefore
    /// serialize an explicit `null`, never be dropped; `last`/`prev` stay
    /// omitted-when-None (upstream never seeds those keys).
    #[test]
    fn test_pagination_links_key_presence() {
        // Offset single/last page: next=None, prev=None. `next` MUST be an
        // explicit null key; `prev` MUST be absent.
        let offset_page = PaginationLinks {
            self_link: "http://x/api/v1/search/?page[offset]=0&page[limit]=10".into(),
            first: Some("http://x/api/v1/search/?page[offset]=0&page[limit]=10".into()),
            last: Some("http://x/api/v1/search/?page[offset]=0&page[limit]=10".into()),
            next: None,
            prev: None,
        };
        let obj = serde_json::to_value(&offset_page).unwrap();
        let obj = obj.as_object().unwrap();
        assert!(obj.contains_key("next"), "next key must always be present");
        assert!(obj["next"].is_null(), "next is explicit null on last page");
        assert!(
            obj.contains_key("first"),
            "first key must always be present"
        );
        assert!(obj["first"].is_string());
        assert!(obj.contains_key("last"), "last present when Some");
        assert!(!obj.contains_key("prev"), "prev omitted when None");

        // Cursor (forward-only) page: last=None, prev=None. `next`/`first`
        // still present; `last`/`prev` absent.
        let cursor_page = PaginationLinks {
            self_link: "http://x/api/v1/search/?page[cursor]=7&page[limit]=10".into(),
            first: Some("http://x/api/v1/search/?page[limit]=10".into()),
            last: None,
            next: None,
            prev: None,
        };
        let obj = serde_json::to_value(&cursor_page).unwrap();
        let obj = obj.as_object().unwrap();
        assert!(obj.contains_key("next"), "next key present on cursor page");
        assert!(obj["next"].is_null());
        assert!(
            obj.contains_key("first"),
            "first key present on cursor page"
        );
        assert!(!obj.contains_key("last"), "last omitted on cursor page");
        assert!(!obj.contains_key("prev"), "prev omitted on cursor page");
    }
}
