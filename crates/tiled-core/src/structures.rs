//! Structure definitions for the five data families.
//!
//! Corresponds to `tiled/structures/core.py`, `array.py`, `table.py`, `sparse.py`,
//! `awkward.py`, `container.py`.

use serde::{Deserialize, Serialize};

use crate::dtype::{BuiltinDType, DType, Endianness, Kind};
use crate::error::{Result, TiledError};

// ---------------------------------------------------------------------------
// StructureFamily
// ---------------------------------------------------------------------------

/// The five families of data structures that Tiled supports.
///
/// Maps to Python `StructureFamily(str, enum.Enum)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructureFamily {
    Array,
    Awkward,
    Container,
    Sparse,
    Table,
}

impl std::fmt::Display for StructureFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array => write!(f, "array"),
            Self::Awkward => write!(f, "awkward"),
            Self::Container => write!(f, "container"),
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

        let dims = value.get("dims").filter(|v| !v.is_null())
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect());

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
    #[serde(skip_serializing_if = "Option::is_none")]
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

        let dims = value.get("dims").filter(|v| !v.is_null())
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect());

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
/// Uses `#[serde(untagged)]` because in the wire format, `structure_family` and `structure`
/// are separate sibling fields on `NodeAttributes`, not nested.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyStructure {
    Array(ArrayStructure),
    Table(TableStructure),
    Sparse(SparseStructure),
    Awkward(AwkwardStructure),
    Container(ContainerStructure),
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
            StructureFamily::Sparse,
            StructureFamily::Table,
        ] {
            let s = sf.to_string();
            let parsed: StructureFamily = s.parse().unwrap();
            assert_eq!(sf, parsed);
        }
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
        assert_eq!(ct.kind, crate::dtype::Kind::UnsignedInteger);
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
