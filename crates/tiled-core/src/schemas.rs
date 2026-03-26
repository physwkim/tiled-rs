//! API request/response schemas (Pydantic models → serde structs).
//!
//! These correspond to the Pydantic schemas in `tiled/server/schemas.py` and `tiled/schemas.py`.
//! They define the wire format for the REST API.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::data_source::DataSource;
use crate::structures::{Spec, StructureFamily};

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

/// A single resource in the API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource<A = NodeAttributes> {
    pub id: String,
    pub attributes: A,
    #[serde(default)]
    pub links: NodeLinks,
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

/// Pagination links (Python lines 53-58).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationLinks {
    #[serde(rename = "self")]
    pub self_link: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStructure {
    #[serde(skip_serializing_if = "Option::is_none")]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMetadataRequest {
    pub structure_family: StructureFamily,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(default)]
    pub specs: Vec<Spec>,
    #[serde(default)]
    pub data_sources: Vec<DataSource>,
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
            formats: HashMap::from([
                ("array".into(), vec!["application/octet-stream".into()]),
            ]),
            aliases: HashMap::new(),
            queries: vec!["fulltext".into(), "lookup".into()],
            authentication: AboutAuthentication {
                required: false,
                providers: vec![],
                links: None,
            },
            links: HashMap::from([
                ("self".into(), "http://localhost:8000/api/v1/".into()),
            ]),
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
            first: Some("http://localhost:8000/api/v1/search/?page[offset]=0&page[limit]=10".into()),
            last: Some("http://localhost:8000/api/v1/search/?page[offset]=90&page[limit]=10".into()),
            next: Some("http://localhost:8000/api/v1/search/?page[offset]=10&page[limit]=10".into()),
            prev: None,
        };
        let json = serde_json::to_value(&links).unwrap();
        assert!(json["self"].is_string());
        assert!(json["prev"].is_null());
    }
}
