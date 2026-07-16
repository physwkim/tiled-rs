//! Structure definitions for the five data families.
//!
//! Corresponds to `tiled/structures/core.py`, `array.py`, `table.py`, `sparse.py`,
//! `awkward.py`, `container.py`.

use serde::{Deserialize, Serialize};

use crate::core::dtype::{BuiltinDType, DType, Endianness, Kind};
use crate::core::error::{Result, TiledError};

// ---------------------------------------------------------------------------
// StructureFamily
// ---------------------------------------------------------------------------

/// The six families of data structures that Tiled supports.
///
/// Maps to Python `StructureFamily(str, enum.Enum)`
/// (`tiled/structures/core.py:18-24`). `ragged` was added upstream in
/// feature #1104; without it here, deserializing a node whose
/// `structure_family == "ragged"` is a hard serde error, which breaks a
/// whole container listing on a single ragged child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructureFamily {
    Array,
    Awkward,
    Container,
    Ragged,
    Sparse,
    Table,
}

impl StructureFamily {
    /// Every structure family, in declaration order. The single source of truth
    /// for "all families": enumerations that must cover the whole set — e.g. the
    /// server About payload's `formats` / `aliases` maps
    /// ([`SerializationRegistry::all_formats`](crate::serialization::SerializationRegistry::all_formats))
    /// — iterate this so a newly added variant cannot silently drop out of one
    /// copy of a hand-maintained list.
    pub const ALL: [StructureFamily; 6] = [
        Self::Array,
        Self::Awkward,
        Self::Container,
        Self::Ragged,
        Self::Sparse,
        Self::Table,
    ];
}

impl std::fmt::Display for StructureFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array => write!(f, "array"),
            Self::Awkward => write!(f, "awkward"),
            Self::Container => write!(f, "container"),
            Self::Ragged => write!(f, "ragged"),
            Self::Sparse => write!(f, "sparse"),
            Self::Table => write!(f, "table"),
        }
    }
}

impl std::str::FromStr for StructureFamily {
    type Err = TiledError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "array" => Ok(Self::Array),
            "awkward" => Ok(Self::Awkward),
            "container" => Ok(Self::Container),
            "ragged" => Ok(Self::Ragged),
            "sparse" => Ok(Self::Sparse),
            "table" => Ok(Self::Table),
            _ => Err(TiledError::Validation(format!(
                "Unknown structure family: '{s}'"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// A named specification that a node conforms to.
///
/// Maps to Python `Spec` frozen dataclass.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Spec {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl Spec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
        }
    }

    pub fn with_version(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: Some(version.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// ArrayStructure
// ---------------------------------------------------------------------------

/// Describes the structure of an N-dimensional array.
///
/// Maps to Python `ArrayStructure` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArrayStructure {
    /// Data type of array elements.
    pub data_type: DType,
    /// Chunk sizes per dimension, e.g. `[[100], [100]]` for a (100, 100) array
    /// split into one chunk per dimension.
    pub chunks: Vec<Vec<usize>>,
    /// Overall shape, e.g. `[1000, 1000]`.
    pub shape: Vec<usize>,
    /// Optional dimension names, e.g. `["x", "y"]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dims: Option<Vec<String>>,
    /// Whether dimensions are resizable.
    #[serde(default)]
    pub resizable: Resizable,
}

impl ArrayStructure {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        let data_type = DType::from_json(&value["data_type"])?;

        let chunks: Vec<Vec<usize>> = value["chunks"]
            .as_array()
            .ok_or_else(|| TiledError::Validation("ArrayStructure missing 'chunks'".into()))?
            .iter()
            .map(|dim| {
                dim.as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect()
            })
            .collect();

        let shape: Vec<usize> = value["shape"]
            .as_array()
            .ok_or_else(|| TiledError::Validation("ArrayStructure missing 'shape'".into()))?
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as usize))
            .collect();

        let dims = value
            .get("dims")
            .filter(|v| !v.is_null())
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        let resizable = value
            .get("resizable")
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();

        Ok(Self {
            data_type,
            chunks,
            shape,
            dims,
            resizable,
        })
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }
}

// ---------------------------------------------------------------------------
// TableStructure
// ---------------------------------------------------------------------------

/// Base64 prefix for Arrow schema encoding (matches Python constant).
pub const B64_ENCODED_PREFIX: &str = "data:application/vnd.apache.arrow.file;base64,";

/// Describes the structure of a tabular dataset.
///
/// Maps to Python `TableStructure` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableStructure {
    /// Base64-encoded Arrow schema, prefixed with `B64_ENCODED_PREFIX`.
    pub arrow_schema: String,
    /// Number of partitions.
    pub npartitions: usize,
    /// Column names.
    pub columns: Vec<String>,
    /// Whether columns/rows are resizable.
    #[serde(default)]
    pub resizable: Resizable,
}

impl TableStructure {
    /// Decode the base64-encoded Arrow schema bytes.
    pub fn decode_arrow_schema_bytes(&self) -> Result<Vec<u8>> {
        use base64::Engine;

        if !self.arrow_schema.starts_with(B64_ENCODED_PREFIX) {
            return Err(TiledError::Validation(format!(
                "Expected base64-encoded data prefixed with '{B64_ENCODED_PREFIX}'"
            )));
        }

        let payload = &self.arrow_schema[B64_ENCODED_PREFIX.len()..];
        base64::engine::general_purpose::STANDARD
            .decode(payload)
            .map_err(|e| TiledError::Validation(format!("Invalid base64 in arrow_schema: {e}")))
    }

    /// Encode Arrow schema bytes to the prefixed base64 string.
    pub fn encode_arrow_schema_bytes(schema_bytes: &[u8]) -> String {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(schema_bytes);
        format!("{B64_ENCODED_PREFIX}{encoded}")
    }

    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| TiledError::Validation(format!("Cannot parse TableStructure: {e}")))
    }
}

// ---------------------------------------------------------------------------
// SparseStructure / COOStructure
// ---------------------------------------------------------------------------

/// Layout of a sparse array.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SparseLayout {
    #[default]
    COO,
}

/// Describes the structure of a sparse array (COO format).
///
/// Maps to Python `COOStructure` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseStructure {
    /// Chunk sizes per dimension.
    pub chunks: Vec<Vec<usize>>,
    /// Overall shape.
    pub shape: Vec<usize>,
    /// Data type of values (optional, can be inferred).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<DType>,
    /// Data type of coordinate indices (default: uint64 little-endian).
    /// When the wire omits this field, default to uint64-LE so the derived
    /// `Deserialize` matches both the manual `from_json` and Python's
    /// `COOStructure.from_json` (sparse.py) instead of leaving it `None`.
    #[serde(
        default = "default_coord_data_type",
        skip_serializing_if = "Option::is_none"
    )]
    pub coord_data_type: Option<BuiltinDType>,
    /// Optional dimension names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dims: Option<Vec<String>>,
    /// Whether dimensions are resizable.
    #[serde(default)]
    pub resizable: Resizable,
    /// Sparse layout format.
    #[serde(default)]
    pub layout: SparseLayout,
}

impl Default for SparseStructure {
    fn default() -> Self {
        Self {
            chunks: vec![],
            shape: vec![],
            data_type: None,
            coord_data_type: Some(BuiltinDType::new(
                Endianness::Little,
                Kind::UnsignedInteger,
                8,
            )),
            dims: None,
            resizable: Resizable::default(),
            layout: SparseLayout::COO,
        }
    }
}

/// serde default for [`SparseStructure::coord_data_type`]: uint64
/// little-endian, matching Python's `COOStructure.from_json` default.
fn default_coord_data_type() -> Option<BuiltinDType> {
    Some(BuiltinDType::new(
        Endianness::Little,
        Kind::UnsignedInteger,
        8,
    ))
}

impl SparseStructure {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        let chunks: Vec<Vec<usize>> = value["chunks"]
            .as_array()
            .ok_or_else(|| TiledError::Validation("SparseStructure missing 'chunks'".into()))?
            .iter()
            .map(|dim| {
                dim.as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect()
            })
            .collect();

        let shape: Vec<usize> = value["shape"]
            .as_array()
            .ok_or_else(|| TiledError::Validation("SparseStructure missing 'shape'".into()))?
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as usize))
            .collect();

        let data_type = value
            .get("data_type")
            .filter(|v| !v.is_null())
            .and_then(|v| DType::from_json(v).ok());

        let coord_data_type = value
            .get("coord_data_type")
            .filter(|v| !v.is_null())
            .and_then(|v| BuiltinDType::from_json(v).ok())
            .or_else(|| {
                Some(BuiltinDType::new(
                    Endianness::Little,
                    Kind::UnsignedInteger,
                    8,
                ))
            });

        let dims = value
            .get("dims")
            .filter(|v| !v.is_null())
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        let resizable = value
            .get("resizable")
            .map(|v| serde_json::from_value(v.clone()).unwrap_or_default())
            .unwrap_or_default();

        Ok(Self {
            chunks,
            shape,
            data_type,
            coord_data_type,
            dims,
            resizable,
            layout: SparseLayout::COO,
        })
    }
}

// ---------------------------------------------------------------------------
// AwkwardStructure
// ---------------------------------------------------------------------------

/// Describes the structure of an Awkward Array.
///
/// Maps to Python `AwkwardStructure` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AwkwardStructure {
    /// Length of the top-level array.
    pub length: usize,
    /// Awkward array form (schema), stored as arbitrary JSON.
    pub form: serde_json::Value,
}

impl AwkwardStructure {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| TiledError::Validation(format!("Cannot parse AwkwardStructure: {e}")))
    }
}

// ---------------------------------------------------------------------------
// RaggedStructure
// ---------------------------------------------------------------------------

/// Describes the structure of a ragged array (variable-length trailing rows).
///
/// Maps to Python `RaggedStructure` dataclass (`tiled/structures/ragged.py`).
/// The first dimension is always a known integer; variable-length dimensions
/// are encoded as `None` in `shape` and `chunks`.
///
/// This is the family/structure representation only — it makes a `ragged` node
/// representable, serializable, and non-breaking in listings. The full ragged
/// read/write adapter (feature #1104) is intentionally not ported here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RaggedStructure {
    /// Serializable representation of the array's data type.
    pub data_type: DType,
    /// Overall shape; the first entry is a known integer, variable dimensions
    /// are `None`.
    pub shape: Vec<Option<usize>>,
    /// Total number of elements in the array.
    pub size: usize,
    /// Dask-like chunks; the first entry is a known integer partitioning,
    /// variable dimensions are `None`.
    pub chunks: Vec<Option<Vec<usize>>>,
    /// Optional dimension names, e.g. `["time", "x"]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dims: Option<Vec<String>>,
    /// Whether the array is resizable along any dimension.
    #[serde(default)]
    pub resizable: Resizable,
}

impl RaggedStructure {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|e| TiledError::Validation(format!("Cannot parse RaggedStructure: {e}")))
    }
}

// ---------------------------------------------------------------------------
// ContainerStructure
// ---------------------------------------------------------------------------

/// Describes the structure of a container (directory/group of nodes).
///
/// Maps to Python `ContainerStructure` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerStructure {
    /// Keys of contained items.
    pub keys: Vec<String>,
}

// ---------------------------------------------------------------------------
// Resizable
// ---------------------------------------------------------------------------

/// Whether a structure's dimensions are resizable.
///
/// Can be a single boolean (all dimensions) or per-dimension.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Resizable {
    Uniform(bool),
    PerDimension(Vec<bool>),
}

impl Default for Resizable {
    fn default() -> Self {
        Self::Uniform(false)
    }
}

// ---------------------------------------------------------------------------
// AnyStructure
// ---------------------------------------------------------------------------

/// Wire-format structure for containers in API responses.
///
/// Not the same as `ContainerStructure { keys }` — this is the response shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStructure {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contents: Option<serde_json::Value>,
    pub count: usize,
}

/// Any structure variant.
///
/// **Serialize only.** `#[serde(untagged)]` emits the held variant's structure
/// object verbatim — with no tag — because in the wire format `structure_family`
/// and `structure` are separate sibling fields (on `NodeAttributes` and
/// `DataSource`), not nested.
///
/// `Deserialize` is deliberately *not* derived. An untagged `Deserialize` picks
/// a variant by trying each in order and taking the first that fits, which
/// silently mislabels a COO [`SparseStructure`] carrying a `data_type` as
/// [`ArrayStructure`] (a sparse structure is a structural superset of an array
/// one). Because the authoritative discriminator — `structure_family` — lives
/// *outside* the structure object, it cannot steer such a parse. Callers must
/// parse under family authority via [`AnyStructure::from_family_json`] (or, for
/// a whole data source, [`DataSource`](crate::core::data_source::DataSource)'s
/// family-aware `Deserialize`), making the mislabel unrepresentable by
/// construction rather than guarded at runtime.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AnyStructure {
    Ragged(RaggedStructure),
    Array(ArrayStructure),
    Table(TableStructure),
    Sparse(SparseStructure),
    Awkward(AwkwardStructure),
    Container(ContainerStructure),
}

impl AnyStructure {
    /// Parse a raw structure JSON under the authority of its sibling
    /// `structure_family`, choosing the variant by family rather than by
    /// field-shape guessing.
    ///
    /// This is the single owner of the family → variant mapping. It exists
    /// because the derived `#[serde(untagged)]` `Deserialize` picks a variant by
    /// trying each in order and taking the first that fits — and a COO
    /// [`SparseStructure`] carrying a `data_type` is a structurally valid
    /// [`ArrayStructure`] (`Array` is ordered before `Sparse`, and the extra
    /// `coord_data_type`/`layout` fields are ignored), so an untagged parse
    /// silently mislabels every typed sparse structure as `Array`, dropping
    /// `coord_data_type` and `layout`. The authoritative discriminator is the
    /// `structure_family` field, which lives *outside* the structure object and
    /// so cannot steer an untagged parse.
    ///
    /// Mirrors upstream, which never parses `structure` as a discriminated
    /// union: `DataSource.structure` is generic over `StructureT`
    /// (`tiled/structures/data_source.py:30-33`) and is always narrowed by
    /// `STRUCTURE_TYPES[structure_family].from_json(...)` — on DB read
    /// (`DataSource.from_orm`, `tiled/server/schemas.py:185-187`) and on wire
    /// ingest (`PostMetadataRequest.narrow_structure_type`,
    /// `tiled/server/schemas.py:479-490`).
    ///
    /// `Container` returns `None`: upstream's `narrow_structure_type` skips the
    /// container family (`tiled/server/schemas.py:484`), a data source never
    /// carries a `ContainerStructure` (a container's wire `structure` is the
    /// response-shaped [`NodeStructure`] `{contents, count}`, not
    /// `{keys}`), and the client already treats a container's parsed structure
    /// as `None` (`ParsedStructure::from_item`). Each other family delegates to
    /// its own lenient `from_json`.
    pub fn from_family_json(
        family: StructureFamily,
        value: &serde_json::Value,
    ) -> Result<Option<Self>> {
        Ok(match family {
            StructureFamily::Array => Some(Self::Array(ArrayStructure::from_json(value)?)),
            StructureFamily::Table => Some(Self::Table(TableStructure::from_json(value)?)),
            StructureFamily::Sparse => Some(Self::Sparse(SparseStructure::from_json(value)?)),
            StructureFamily::Awkward => Some(Self::Awkward(AwkwardStructure::from_json(value)?)),
            StructureFamily::Ragged => Some(Self::Ragged(RaggedStructure::from_json(value)?)),
            StructureFamily::Container => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structure_family_roundtrip() {
        for sf in [
            StructureFamily::Array,
            StructureFamily::Awkward,
            StructureFamily::Container,
            StructureFamily::Ragged,
            StructureFamily::Sparse,
            StructureFamily::Table,
        ] {
            let s = sf.to_string();
            let parsed: StructureFamily = s.parse().unwrap();
            assert_eq!(sf, parsed);
        }
    }

    #[test]
    fn test_structure_family_ragged_serde() {
        // A node whose structure_family is "ragged" must deserialize, not error.
        let parsed: StructureFamily = serde_json::from_str("\"ragged\"").unwrap();
        assert_eq!(parsed, StructureFamily::Ragged);
        assert_eq!(serde_json::to_string(&parsed).unwrap(), "\"ragged\"");
        assert_eq!(
            "ragged".parse::<StructureFamily>().unwrap(),
            StructureFamily::Ragged
        );
    }

    fn ragged_json() -> serde_json::Value {
        serde_json::json!({
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "shape": [3, null],
            "size": 7,
            "chunks": [[3], null],
            "dims": null,
            "resizable": false
        })
    }

    #[test]
    fn test_ragged_structure_from_json() {
        let s = RaggedStructure::from_json(&ragged_json()).unwrap();
        assert_eq!(s.shape, vec![Some(3), None]);
        assert_eq!(s.size, 7);
        assert_eq!(s.chunks, vec![Some(vec![3]), None]);
        assert_eq!(s.dims, None);
        // Round-trips back out without losing the variable (null) dimensions.
        let back: RaggedStructure =
            serde_json::from_value(serde_json::to_value(&s).unwrap()).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn from_family_json_sparse_with_data_type_stays_sparse() {
        // The core defect this change closes: a COO sparse structure carrying a
        // `data_type` is a structural superset of an ArrayStructure, so a
        // field-shape-guessing parse would mislabel it Array and drop
        // `coord_data_type` + `layout`. Under family authority it stays Sparse and
        // every field survives — including a NON-default (uint32) coord dtype.
        // (AnyStructure no longer derives an untagged `Deserialize`, so the wrong
        // parse is not merely avoided but unrepresentable.)
        let raw = serde_json::json!({
            "chunks": [[3], [3]],
            "shape": [3, 3],
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "coord_data_type": {"endianness": "little", "kind": "u", "itemsize": 4},
            "dims": null,
            "resizable": false,
            "layout": "COO"
        });

        let parsed = AnyStructure::from_family_json(StructureFamily::Sparse, &raw)
            .unwrap()
            .expect("sparse family yields Some(structure)");
        match parsed {
            AnyStructure::Sparse(s) => {
                assert_eq!(s.shape, vec![3, 3]);
                assert_eq!(s.chunks, vec![vec![3], vec![3]]);
                assert_eq!(
                    s.coord_data_type,
                    Some(BuiltinDType::new(
                        Endianness::Little,
                        Kind::UnsignedInteger,
                        4
                    )),
                    "non-default uint32 coord_data_type must survive"
                );
                assert_eq!(s.layout, SparseLayout::COO);
            }
            other => panic!("expected Sparse, got {other:?}"),
        }
    }

    #[test]
    fn from_family_json_sparse_without_data_type_stays_sparse() {
        // Without a `data_type`, the untagged parse already lands on Sparse
        // (Array requires data_type). Family authority agrees.
        let raw = serde_json::json!({
            "chunks": [[3], [3]],
            "shape": [3, 3]
        });
        let parsed = AnyStructure::from_family_json(StructureFamily::Sparse, &raw)
            .unwrap()
            .unwrap();
        assert!(matches!(parsed, AnyStructure::Sparse(_)));
    }

    #[test]
    fn from_family_json_dispatches_each_family() {
        let array = serde_json::json!({
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "chunks": [[4]],
            "shape": [4],
            "dims": null,
            "resizable": false
        });
        assert!(matches!(
            AnyStructure::from_family_json(StructureFamily::Array, &array)
                .unwrap()
                .unwrap(),
            AnyStructure::Array(_)
        ));

        let ragged = serde_json::json!({
            "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
            "shape": [3, null],
            "size": 7,
            "chunks": [[3], null],
            "dims": null,
            "resizable": false
        });
        assert!(matches!(
            AnyStructure::from_family_json(StructureFamily::Ragged, &ragged)
                .unwrap()
                .unwrap(),
            AnyStructure::Ragged(_)
        ));

        let awkward = serde_json::json!({"length": 5, "form": {}});
        assert!(matches!(
            AnyStructure::from_family_json(StructureFamily::Awkward, &awkward)
                .unwrap()
                .unwrap(),
            AnyStructure::Awkward(_)
        ));

        let table = serde_json::json!({
            "arrow_schema": "data:application/vnd.apache.arrow.file;base64,",
            "npartitions": 1,
            "columns": ["a", "b"],
            "resizable": false
        });
        assert!(matches!(
            AnyStructure::from_family_json(StructureFamily::Table, &table)
                .unwrap()
                .unwrap(),
            AnyStructure::Table(_)
        ));
    }

    #[test]
    fn from_family_json_container_is_none() {
        // A container carries no data-source structure; upstream's
        // narrow_structure_type skips the container family entirely.
        let raw = serde_json::json!({"contents": null, "count": 3});
        assert!(
            AnyStructure::from_family_json(StructureFamily::Container, &raw)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn any_structure_serialize_wire_snapshot_unchanged() {
        // Wire-format contract: removing AnyStructure's untagged Deserialize must
        // NOT change serialized output. The retained untagged Serialize emits the
        // held variant's structure object verbatim, with no tag. Snapshot the
        // sparse shape (the variant the parse fix was about) and the array shape.
        let sparse = AnyStructure::Sparse(SparseStructure {
            chunks: vec![vec![3], vec![3]],
            shape: vec![3, 3],
            data_type: Some(DType::Builtin(BuiltinDType::new(
                Endianness::Little,
                Kind::Float,
                8,
            ))),
            coord_data_type: Some(BuiltinDType::new(
                Endianness::Little,
                Kind::UnsignedInteger,
                4,
            )),
            ..Default::default()
        });
        assert_eq!(
            serde_json::to_value(&sparse).unwrap(),
            serde_json::json!({
                "chunks": [[3], [3]],
                "shape": [3, 3],
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "coord_data_type": {"endianness": "little", "kind": "u", "itemsize": 4},
                "resizable": false,
                "layout": "COO"
            }),
            "sparse structure must serialize untagged, with coord_data_type + layout"
        );

        let array = AnyStructure::Array(ArrayStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            chunks: vec![vec![4]],
            shape: vec![4],
            dims: None,
            resizable: Resizable::default(),
        });
        assert_eq!(
            serde_json::to_value(&array).unwrap(),
            serde_json::json!({
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "chunks": [[4]],
                "shape": [4],
                "resizable": false
            }),
            "array structure must serialize untagged"
        );
    }

    #[test]
    fn test_structure_family_serde() {
        let json = serde_json::to_string(&StructureFamily::Array).unwrap();
        assert_eq!(json, "\"array\"");
        let parsed: StructureFamily = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, StructureFamily::Array);
    }

    #[test]
    fn test_spec() {
        let s = Spec::new("xdi");
        assert_eq!(s.name, "xdi");
        assert_eq!(s.version, None);

        let s = Spec::with_version("xdi", "1.0");
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(json["name"], "xdi");
        assert_eq!(json["version"], "1.0");
    }

    #[test]
    fn test_array_structure_from_json() {
        let json = serde_json::json!({
            "data_type": {
                "endianness": "little",
                "kind": "f",
                "itemsize": 8
            },
            "chunks": [[100], [100]],
            "shape": [100, 100],
            "dims": ["x", "y"],
            "resizable": false
        });
        let s = ArrayStructure::from_json(&json).unwrap();
        assert_eq!(s.shape, vec![100, 100]);
        assert_eq!(s.ndim(), 2);
        assert_eq!(s.dims, Some(vec!["x".to_string(), "y".to_string()]));
    }

    #[test]
    fn test_table_structure_b64_roundtrip() {
        let original_bytes = b"test arrow schema bytes";
        let encoded = TableStructure::encode_arrow_schema_bytes(original_bytes);
        let ts = TableStructure {
            arrow_schema: encoded,
            npartitions: 1,
            columns: vec!["a".into(), "b".into()],
            resizable: Resizable::default(),
        };
        let decoded = ts.decode_arrow_schema_bytes().unwrap();
        assert_eq!(decoded, original_bytes);
    }

    #[test]
    fn test_sparse_structure_from_json() {
        let json = serde_json::json!({
            "chunks": [[10], [10]],
            "shape": [10, 10],
            "data_type": null,
            "dims": null,
            "resizable": false
        });
        let s = SparseStructure::from_json(&json).unwrap();
        assert_eq!(s.shape, vec![10, 10]);
        assert_eq!(s.layout, SparseLayout::COO);
        // Default coord_data_type should be uint64 little-endian
        let ct = s.coord_data_type.unwrap();
        assert_eq!(ct.kind, crate::core::dtype::Kind::UnsignedInteger);
        assert_eq!(ct.itemsize, 8);
    }

    #[test]
    fn sparse_structure_serde_defaults_coord_data_type() {
        // The derived Deserialize path (not just the manual from_json) must
        // default coord_data_type to uint64-LE when the wire omits it,
        // matching Python COOStructure.from_json.
        let json = serde_json::json!({
            "chunks": [[10], [10]],
            "shape": [10, 10],
        });
        let s: SparseStructure = serde_json::from_value(json).unwrap();
        let ct = s
            .coord_data_type
            .expect("absent coord_data_type must default to uint64-LE, not None");
        assert_eq!(ct.endianness, crate::core::dtype::Endianness::Little);
        assert_eq!(ct.kind, crate::core::dtype::Kind::UnsignedInteger);
        assert_eq!(ct.itemsize, 8);
    }

    #[test]
    fn test_awkward_structure() {
        let json = serde_json::json!({
            "length": 42,
            "form": {"class": "NumpyForm", "inner_shape": [], "itemsize": 8}
        });
        let s = AwkwardStructure::from_json(&json).unwrap();
        assert_eq!(s.length, 42);
        assert_eq!(s.form["class"], "NumpyForm");
    }

    #[test]
    fn test_resizable_serde() {
        let r: Resizable = serde_json::from_str("false").unwrap();
        assert_eq!(r, Resizable::Uniform(false));

        let r: Resizable = serde_json::from_str("[true, false]").unwrap();
        assert_eq!(r, Resizable::PerDimension(vec![true, false]));
    }
}
