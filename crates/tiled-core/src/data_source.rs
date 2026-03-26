//! Data source and asset definitions.
//!
//! Corresponds to `tiled/structures/data_source.py`.

use serde::{Deserialize, Serialize};

use crate::structures::{AnyStructure, StructureFamily};

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
            structure_family: crate::structures::StructureFamily::Array,
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
}
