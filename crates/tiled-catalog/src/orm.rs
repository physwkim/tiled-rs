//! Row types for catalog tables.
//!
//! These mirror the SQL schema column-for-column. Rather than implement
//! `sqlx::FromRow` (which is dialect-specific), the read modules pull rows
//! by named columns and convert into these structs by hand. Keeps the model
//! types free of sqlx generics so they're easy to share with the server.

use serde::{Deserialize, Serialize};

/// One row of `nodes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: i64,
    pub key: String,
    pub parent_id: Option<i64>,
    pub ancestors: Vec<String>,
    pub structure_family: String,
    pub metadata: serde_json::Value,
    pub specs: serde_json::Value,
    pub access_blob: serde_json::Value,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_updated: chrono::DateTime<chrono::Utc>,
}

/// One row of `data_sources`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    pub id: i64,
    pub node_id: i64,
    pub structure_family: String,
    pub structure: serde_json::Value,
    pub mimetype: String,
    pub parameters: serde_json::Value,
    pub management: String,
}

/// One row of `assets`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: i64,
    pub data_source_id: i64,
    pub data_uri: String,
    pub is_directory: bool,
    pub parameter: String,
    pub num: Option<i32>,
}

/// One row of `revisions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revision {
    pub id: i64,
    pub node_id: i64,
    pub revision: i32,
    pub metadata: serde_json::Value,
    pub specs: serde_json::Value,
    pub time_created: chrono::DateTime<chrono::Utc>,
}
