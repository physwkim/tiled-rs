//! Data source and asset definitions.
//!
//! Corresponds to `tiled/structures/data_source.py`.

use serde::{Deserialize, Serialize};

use crate::core::structures::{AnyStructure, StructureFamily};

/// Management mode for a data source.
///
/// Controls whether the data can be modified through the Tiled server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Management {
    /// Data is managed outside of Tiled (read-only, no asset tracking).
    External,
    /// Data was imported but is now immutable.
    Immutable,
    /// Data is locked (read-only, assets tracked by Tiled).
    Locked,
    /// Data can be read and written through Tiled.
    Writable,
}

/// A physical storage location for data.
///
/// Maps to Python `Asset` dataclass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Asset {
    /// URI pointing to the data (e.g. `file:///path/to/data.h5`).
    pub data_uri: String,
    /// Whether this asset is a directory.
    pub is_directory: bool,
    /// Parameter name this asset maps to in the adapter constructor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
    /// Ordering index when multiple assets map to the same parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num: Option<usize>,
    /// Database ID (populated when loaded from catalog).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
}

/// A data source describes how to access one representation of a node's data.
///
/// A single node may have multiple data sources (e.g. different file formats).
/// Each data source specifies a MIME type, adapter parameters, and physical assets.
///
/// Maps to Python `DataSource` generic dataclass.
///
/// `Deserialize` is hand-written (see below) rather than derived: the
/// `structure` field must be parsed under the authority of the sibling
/// `structure_family`, not by `AnyStructure`'s field-shape-guessing untagged
/// `Deserialize`. `Serialize` stays derived — serialization was never
/// ambiguous (the held variant's fields are emitted verbatim), and the wire
/// bytes are unchanged by the custom parse.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DataSource {
    /// Which structure family this data source produces.
    pub structure_family: StructureFamily,
    /// Structural metadata (shape, dtype, schema, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structure: Option<AnyStructure>,
    /// Database ID (populated when loaded from catalog).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    /// MIME type identifying the data format (e.g. `"application/x-hdf5"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mimetype: Option<String>,
    /// Parameters passed to the adapter constructor.
    #[serde(default)]
    pub parameters: serde_json::Value,
    /// Properties derived from the data (cached metadata).
    #[serde(default)]
    pub properties: serde_json::Value,
    /// Physical storage locations.
    #[serde(default)]
    pub assets: Vec<Asset>,
    /// Management mode.
    #[serde(default = "default_management")]
    pub management: Management,
}

/// Deserialization mirror of [`DataSource`] with `structure` left as raw JSON.
///
/// Every serde default/naming rule stays declarative here (the only footgun of
/// a hand-written `Deserialize` is silently dropping a `#[serde(default)]`); the
/// custom impl only re-parses `structure` under `structure_family` authority via
/// [`AnyStructure::from_family_json`]. `Option` fields default to `None` without
/// an explicit attribute (serde treats missing `Option<T>` as `None`).
#[derive(Deserialize)]
struct DataSourceWire {
    structure_family: StructureFamily,
    #[serde(default)]
    structure: Option<serde_json::Value>,
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    mimetype: Option<String>,
    #[serde(default)]
    parameters: serde_json::Value,
    #[serde(default)]
    properties: serde_json::Value,
    #[serde(default)]
    assets: Vec<Asset>,
    #[serde(default = "default_management")]
    management: Management,
}

impl<'de> Deserialize<'de> for DataSource {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = DataSourceWire::deserialize(deserializer)?;
        // Narrow `structure` by the sibling `structure_family` — the illegal
        // combination (family=Sparse holding an Array-shaped variant) is
        // unrepresentable because the variant is a function of the family, not
        // of field shape.
        let structure = match &wire.structure {
            Some(v) => AnyStructure::from_family_json(wire.structure_family, v)
                .map_err(serde::de::Error::custom)?,
            None => None,
        };
        Ok(DataSource {
            structure_family: wire.structure_family,
            structure,
            id: wire.id,
            mimetype: wire.mimetype,
            parameters: wire.parameters,
            properties: wire.properties,
            assets: wire.assets,
            management: wire.management,
        })
    }
}

fn default_management() -> Management {
    Management::Writable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_management_serde() {
        let m = Management::External;
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(json, "\"external\"");
        let m2: Management = serde_json::from_str(&json).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn test_asset_serde() {
        let a = Asset {
            data_uri: "file:///data/scan001.h5".into(),
            is_directory: false,
            parameter: Some("data_path".into()),
            num: None,
            id: None,
        };
        let json = serde_json::to_value(&a).unwrap();
        assert_eq!(json["data_uri"], "file:///data/scan001.h5");
        assert!(!json["is_directory"].as_bool().unwrap());
    }

    #[test]
    fn test_data_source_minimal() {
        let ds = DataSource {
            structure_family: crate::core::structures::StructureFamily::Array,
            structure: None,
            id: None,
            mimetype: Some("application/x-hdf5".into()),
            parameters: serde_json::json!({}),
            properties: serde_json::json!({}),
            assets: vec![],
            management: Management::External,
        };
        let json = serde_json::to_value(&ds).unwrap();
        assert_eq!(json["structure_family"], "array");
        assert_eq!(json["management"], "external");
    }

    #[test]
    fn sparse_data_source_roundtrips_as_sparse_with_nondefault_coord() {
        use crate::core::dtype::{BuiltinDType, DType, Endianness, Kind};
        use crate::core::structures::{SparseLayout, SparseStructure};

        // A sparse structure with a data_type AND a non-default (uint32)
        // coord_data_type. Under the old untagged Deserialize this survived
        // serialization but came back as AnyStructure::Array, dropping
        // coord_data_type and layout. Family authority keeps it Sparse.
        let structure = SparseStructure {
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
        };
        let ds = DataSource {
            structure_family: StructureFamily::Sparse,
            structure: Some(AnyStructure::Sparse(structure)),
            id: None,
            mimetype: None,
            parameters: serde_json::json!({}),
            properties: serde_json::json!({}),
            assets: vec![],
            management: Management::Writable,
        };

        let json = serde_json::to_value(&ds).unwrap();
        let back: DataSource = serde_json::from_value(json).unwrap();
        match back.structure {
            Some(AnyStructure::Sparse(s)) => {
                assert_eq!(s.shape, vec![3, 3]);
                assert_eq!(s.chunks, vec![vec![3], vec![3]]);
                assert_eq!(
                    s.coord_data_type,
                    Some(BuiltinDType::new(
                        Endianness::Little,
                        Kind::UnsignedInteger,
                        4
                    )),
                    "non-default uint32 coord_data_type must survive the round-trip"
                );
                assert_eq!(s.layout, SparseLayout::COO);
            }
            other => panic!("expected Sparse, got {other:?}"),
        }
    }

    #[test]
    fn vec_data_source_deserializes_sparse_by_family() {
        // The ingest shape: a `Vec<DataSource>` (as in PostMetadataRequest and
        // NodeAttributes) routes through the custom Deserialize per element.
        let raw = serde_json::json!([{
            "structure_family": "sparse",
            "structure": {
                "chunks": [[3], [3]],
                "shape": [3, 3],
                "data_type": {"endianness": "little", "kind": "f", "itemsize": 8},
                "coord_data_type": {"endianness": "little", "kind": "u", "itemsize": 4},
                "layout": "COO"
            },
            "management": "writable"
        }]);
        let dss: Vec<DataSource> = serde_json::from_value(raw).unwrap();
        assert_eq!(dss.len(), 1);
        assert!(
            matches!(dss[0].structure, Some(AnyStructure::Sparse(_))),
            "family-authoritative parse must yield Sparse, not Array"
        );
    }
}
