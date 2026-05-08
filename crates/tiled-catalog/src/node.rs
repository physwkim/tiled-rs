//! Node CRUD operations.
//!
//! Each public fn dispatches on the active [`DbPool`] variant so callers
//! don't have to think about dialect. SQLite stores JSON as `TEXT`, Postgres
//! as `JSONB`; both branches return the same `Node` shape after decoding.

use serde_json::Value;
use sqlx::Row;

use crate::db::{Catalog, DbPool};
use crate::error::{CatalogError, Result};
use crate::orm::Node;

/// What a caller needs to create a new node.
#[derive(Debug, Clone)]
pub struct RegisterRequest {
    pub key: String,
    pub structure_family: String,
    pub metadata: Value,
    pub specs: Value,
    pub access_blob: Value,
}

/// Public-ish view of a node + the surface fields used by API responses.
/// We keep `Node` raw for storage and let the adapter layer assemble higher
/// representations.
pub type NodeRecord = Node;

const MAX_METADATA_BYTES: usize = 10 * 1024 * 1024;
const MAX_SPECS: usize = 20;
const MAX_SPEC_CHARS: usize = 255;

/// Validate metadata + specs against the size limits Python tiled enforces
/// (bluesky/tiled#342). Caught early so a misbehaving client can't push a
/// 100 MB blob through to disk.
pub fn validate_payload(metadata: &Value, specs: &Value) -> Result<()> {
    let metadata_bytes = serde_json::to_vec(metadata)?.len();
    if metadata_bytes > MAX_METADATA_BYTES {
        return Err(CatalogError::Validation(format!(
            "metadata is {metadata_bytes} bytes; limit is {MAX_METADATA_BYTES}",
        )));
    }
    if let Some(arr) = specs.as_array() {
        if arr.len() > MAX_SPECS {
            return Err(CatalogError::Validation(format!(
                "{} specs supplied; limit is {MAX_SPECS}",
                arr.len()
            )));
        }
        for s in arr {
            // Accept plain strings or {name, version} shapes — match the
            // Python validators.
            let name = s
                .as_str()
                .or_else(|| s.get("name").and_then(|v| v.as_str()))
                .ok_or_else(|| CatalogError::Validation("spec missing name".into()))?;
            if name.len() > MAX_SPEC_CHARS {
                return Err(CatalogError::Validation(format!(
                    "spec name length {} > {MAX_SPEC_CHARS}",
                    name.len()
                )));
            }
        }
    }
    Ok(())
}

impl Catalog {
    /// Find the node identified by `segments`. `[]` is the root sentinel and
    /// returns `None` (callers should special-case the catalog root, which
    /// is a virtual node, not a row).
    pub async fn lookup(&self, segments: &[String]) -> Result<Option<Node>> {
        let mut parent_id: Option<i64> = None;
        let mut found: Option<Node> = None;
        for seg in segments {
            let row = self.fetch_child(parent_id, seg).await?;
            match row {
                Some(node) => {
                    parent_id = Some(node.id);
                    found = Some(node);
                }
                None => return Ok(None),
            }
        }
        Ok(found)
    }

    pub async fn fetch_child(&self, parent_id: Option<i64>, key: &str) -> Result<Option<Node>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                // SQLite distinguishes `parent_id IS NULL` vs `= ?` — the
                // bound parameter would never match NULL otherwise.
                let row = if parent_id.is_some() {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id = ? AND key = ?",
                    )
                    .bind(parent_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id IS NULL AND key = ?",
                    )
                    .bind(key)
                    .fetch_optional(pool)
                    .await?
                };
                row.map(|r| node_from_sqlite_row(&r)).transpose()
            }
            DbPool::Postgres(pool) => {
                let row = if parent_id.is_some() {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id = $1 AND key = $2",
                    )
                    .bind(parent_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id IS NULL AND key = $1",
                    )
                    .bind(key)
                    .fetch_optional(pool)
                    .await?
                };
                row.map(|r| node_from_postgres_row(&r)).transpose()
            }
        }
    }

    pub async fn list_children(
        &self,
        parent_id: Option<i64>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Node>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = if parent_id.is_some() {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id = ?
                           ORDER BY id LIMIT ? OFFSET ?",
                    )
                    .bind(parent_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id IS NULL
                           ORDER BY id LIMIT ? OFFSET ?",
                    )
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?
                };
                rows.iter().map(node_from_sqlite_row).collect()
            }
            DbPool::Postgres(pool) => {
                let rows = if parent_id.is_some() {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id = $1
                           ORDER BY id LIMIT $2 OFFSET $3",
                    )
                    .bind(parent_id)
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?
                } else {
                    sqlx::query(
                        "SELECT id, key, parent_id, ancestors, structure_family, metadata,
                                specs, access_blob, time_created, time_updated
                           FROM nodes WHERE parent_id IS NULL
                           ORDER BY id LIMIT $1 OFFSET $2",
                    )
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(pool)
                    .await?
                };
                rows.iter().map(node_from_postgres_row).collect()
            }
        }
    }

    pub async fn count_children(&self, parent_id: Option<i64>) -> Result<i64> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let n: i64 = if parent_id.is_some() {
                    sqlx::query_scalar("SELECT COUNT(*) FROM nodes WHERE parent_id = ?")
                        .bind(parent_id)
                        .fetch_one(pool)
                        .await?
                } else {
                    sqlx::query_scalar("SELECT COUNT(*) FROM nodes WHERE parent_id IS NULL")
                        .fetch_one(pool)
                        .await?
                };
                Ok(n)
            }
            DbPool::Postgres(pool) => {
                let n: i64 = if parent_id.is_some() {
                    sqlx::query_scalar("SELECT COUNT(*) FROM nodes WHERE parent_id = $1")
                        .bind(parent_id)
                        .fetch_one(pool)
                        .await?
                } else {
                    sqlx::query_scalar("SELECT COUNT(*) FROM nodes WHERE parent_id IS NULL")
                        .fetch_one(pool)
                        .await?
                };
                Ok(n)
            }
        }
    }

    /// Insert a new node under `parent_id`. Returns the inserted record.
    /// Raises `Conflict` on duplicate (parent_id, key).
    pub async fn create_node(
        &self,
        parent_id: Option<i64>,
        ancestors: Vec<String>,
        req: RegisterRequest,
    ) -> Result<Node> {
        validate_payload(&req.metadata, &req.specs)?;
        let ancestors_json = serde_json::to_string(&ancestors)?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                // SQLite 3.35+ supports RETURNING; we rely on it because the
                // sqlx pool is configured for a recent SQLite.
                let metadata_text = serde_json::to_string(&req.metadata)?;
                let specs_text = serde_json::to_string(&req.specs)?;
                let access_text = serde_json::to_string(&req.access_blob)?;
                let row = sqlx::query(
                    "INSERT INTO nodes (key, parent_id, ancestors, structure_family,
                                        metadata, specs, access_blob)
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                               specs, access_blob, time_created, time_updated",
                )
                .bind(&req.key)
                .bind(parent_id)
                .bind(&ancestors_json)
                .bind(&req.structure_family)
                .bind(&metadata_text)
                .bind(&specs_text)
                .bind(&access_text)
                .fetch_one(pool)
                .await
                .map_err(map_unique_violation)?;
                node_from_sqlite_row(&row)
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "INSERT INTO nodes (key, parent_id, ancestors, structure_family,
                                        metadata, specs, access_blob)
                     VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, $7::jsonb)
                     RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                               specs, access_blob, time_created, time_updated",
                )
                .bind(&req.key)
                .bind(parent_id)
                .bind(&ancestors_json)
                .bind(&req.structure_family)
                .bind(serde_json::to_string(&req.metadata)?)
                .bind(serde_json::to_string(&req.specs)?)
                .bind(serde_json::to_string(&req.access_blob)?)
                .fetch_one(pool)
                .await
                .map_err(map_unique_violation)?;
                node_from_postgres_row(&row)
            }
        }
    }

    /// Replace metadata + specs on a node. Pushes the previous (metadata,
    /// specs) onto the revisions table so undo is possible. Returns the
    /// updated node.
    pub async fn update_metadata(
        &self,
        node_id: i64,
        metadata: Value,
        specs: Value,
    ) -> Result<Node> {
        validate_payload(&metadata, &specs)?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let prev: (String, String) =
                    sqlx::query_as("SELECT metadata, specs FROM nodes WHERE id = ?")
                        .bind(node_id)
                        .fetch_optional(pool)
                        .await?
                        .ok_or_else(|| CatalogError::NotFound(format!("node id {node_id}")))?;
                let next_revision: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(revision), 0) + 1 FROM revisions WHERE node_id = ?",
                )
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                sqlx::query(
                    "INSERT INTO revisions (node_id, revision, metadata, specs)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(node_id)
                .bind(next_revision as i32)
                .bind(prev.0)
                .bind(prev.1)
                .execute(pool)
                .await?;
                let row = sqlx::query(
                    "UPDATE nodes
                        SET metadata = ?, specs = ?,
                            time_updated = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                      WHERE id = ?
                     RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                               specs, access_blob, time_created, time_updated",
                )
                .bind(serde_json::to_string(&metadata)?)
                .bind(serde_json::to_string(&specs)?)
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                node_from_sqlite_row(&row)
            }
            DbPool::Postgres(pool) => {
                let row = sqlx::query(
                    "SELECT metadata::text AS metadata, specs::text AS specs FROM nodes WHERE id = $1",
                )
                .bind(node_id)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound(format!("node id {node_id}")))?;
                let prev_meta: String = row.get("metadata");
                let prev_specs: String = row.get("specs");
                let next_revision: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(revision), 0) + 1 FROM revisions WHERE node_id = $1",
                )
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                sqlx::query(
                    "INSERT INTO revisions (node_id, revision, metadata, specs)
                     VALUES ($1, $2, $3::jsonb, $4::jsonb)",
                )
                .bind(node_id)
                .bind(next_revision as i32)
                .bind(prev_meta)
                .bind(prev_specs)
                .execute(pool)
                .await?;
                let row = sqlx::query(
                    "UPDATE nodes
                        SET metadata = $1::jsonb, specs = $2::jsonb, time_updated = now()
                      WHERE id = $3
                     RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                               specs, access_blob, time_created, time_updated",
                )
                .bind(serde_json::to_string(&metadata)?)
                .bind(serde_json::to_string(&specs)?)
                .bind(node_id)
                .fetch_one(pool)
                .await?;
                node_from_postgres_row(&row)
            }
        }
    }

    /// Delete a node and all its descendants. Cascades through data sources
    /// + assets via the FK ON DELETE CASCADE wiring.
    pub async fn delete_node(&self, node_id: i64) -> Result<()> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let res = sqlx::query("DELETE FROM nodes WHERE id = ?")
                    .bind(node_id)
                    .execute(pool)
                    .await?;
                if res.rows_affected() == 0 {
                    return Err(CatalogError::NotFound(format!("node id {node_id}")));
                }
            }
            DbPool::Postgres(pool) => {
                let res = sqlx::query("DELETE FROM nodes WHERE id = $1")
                    .bind(node_id)
                    .execute(pool)
                    .await?;
                if res.rows_affected() == 0 {
                    return Err(CatalogError::NotFound(format!("node id {node_id}")));
                }
            }
        }
        Ok(())
    }
}

fn node_from_sqlite_row(row: &sqlx::sqlite::SqliteRow) -> Result<Node> {
    use chrono::{DateTime, Utc};
    let parse_dt = |s: String| -> Result<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&s)
            .map(|d| d.with_timezone(&Utc))
            .or_else(|_| {
                // SQLite default uses fractional seconds with Z suffix; handle
                // the no-fraction case the same.
                chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .map(|n| n.and_utc())
            })
            .map_err(|e| CatalogError::Validation(format!("bad timestamp {s}: {e}")))
    };
    Ok(Node {
        id: row.get("id"),
        key: row.get("key"),
        parent_id: row.try_get("parent_id").ok(),
        ancestors: serde_json::from_str(&row.get::<String, _>("ancestors"))?,
        structure_family: row.get("structure_family"),
        metadata: serde_json::from_str(&row.get::<String, _>("metadata"))?,
        specs: serde_json::from_str(&row.get::<String, _>("specs"))?,
        access_blob: serde_json::from_str(&row.get::<String, _>("access_blob"))?,
        time_created: parse_dt(row.get::<String, _>("time_created"))?,
        time_updated: parse_dt(row.get::<String, _>("time_updated"))?,
    })
}

fn node_from_postgres_row(row: &sqlx::postgres::PgRow) -> Result<Node> {
    Ok(Node {
        id: row.get("id"),
        key: row.get("key"),
        parent_id: row.try_get("parent_id").ok(),
        ancestors: serde_json::from_value(row.get::<Value, _>("ancestors"))?,
        structure_family: row.get("structure_family"),
        metadata: row.get("metadata"),
        specs: row.get("specs"),
        access_blob: row.get("access_blob"),
        time_created: row.get("time_created"),
        time_updated: row.get("time_updated"),
    })
}

fn map_unique_violation(err: sqlx::Error) -> CatalogError {
    let s = err.to_string().to_lowercase();
    if s.contains("unique") || s.contains("duplicate key") {
        CatalogError::Conflict("a node with this key already exists at the same level".into())
    } else {
        CatalogError::Database(err)
    }
}
