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

/// One historical version of a node's metadata, as pushed onto the `revisions`
/// table by [`Catalog::update_metadata`]. Mirrors Python's `Revision`
/// (server/schemas.py:147-165): `revision_number` is the per-node sequential
/// `revision` column, and `time_updated` is the stored `time_created` (the
/// instant that version was superseded). `access_blob` is stored but omitted
/// from the listing response, matching `construct_revisions_response`
/// (server/core.py:330-353).
#[derive(Debug, Clone)]
pub struct Revision {
    pub revision_number: i64,
    pub metadata: Value,
    pub specs: Value,
    pub time_updated: String,
}

const MAX_METADATA_BYTES: usize = 10 * 1024 * 1024;
const MAX_STRUCTURE_BYTES: usize = 10 * 1024 * 1024;
const MAX_SPECS: usize = 20;
const MAX_SPEC_CHARS: usize = 255;
const MAX_REFERENCES: usize = 20;
const MAX_REFERENCE_LABEL_CHARS: usize = 255;
const MAX_REFERENCE_URL_CHARS: usize = 2047;

/// Refusal message for `delete_node` when `external_only` blocks a delete that
/// would orphan internally-managed storage. Verbatim from Python
/// `tiled.catalog.adapter` (adapter.py:1051-1055).
const WOULD_DELETE_DATA_MSG: &str = "Some items in this tree are internally managed. \
Deleting the records will also delete the underlying data files. \
If you want to delete them, pass external_only=False.";

/// Count data sources in the subtree rooted at the bound node id (inclusive)
/// whose `management` is not `external`. The recursive CTE walks `parent_id`,
/// which is exactly the set the FK `ON DELETE CASCADE` would remove — so the
/// gate covers precisely what the delete would destroy.
const SUBTREE_INTERNAL_COUNT_SQLITE: &str = "WITH RECURSIVE subtree(id) AS (
    SELECT id FROM nodes WHERE id = ?
    UNION ALL
    SELECT n.id FROM nodes n JOIN subtree s ON n.parent_id = s.id
)
SELECT COUNT(*) FROM data_sources ds
  JOIN subtree s ON ds.node_id = s.id
 WHERE ds.management <> 'external'";

/// Postgres variant of [`SUBTREE_INTERNAL_COUNT_SQLITE`] (positional `$1`).
const SUBTREE_INTERNAL_COUNT_POSTGRES: &str = "WITH RECURSIVE subtree(id) AS (
    SELECT id FROM nodes WHERE id = $1
    UNION ALL
    SELECT n.id FROM nodes n JOIN subtree s ON n.parent_id = s.id
)
SELECT COUNT(*) FROM data_sources ds
  JOIN subtree s ON ds.node_id = s.id
 WHERE ds.management <> 'external'";

/// Validate metadata + specs against the size limits Python tiled enforces
/// (bluesky/tiled#342, #262). Caught early so a misbehaving client can't
/// push a 100 MB blob through to disk.
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
    // tiled#342: reference list cap (when callers nest references inside
    // metadata). The Python schema uses a top-level `references` array;
    // we don't have that field but apply the same caps when present.
    if let Some(refs) = metadata.get("references").and_then(|v| v.as_array()) {
        if refs.len() > MAX_REFERENCES {
            return Err(CatalogError::Validation(format!(
                "{} references supplied; limit is {MAX_REFERENCES}",
                refs.len()
            )));
        }
        for r in refs {
            let label = r.get("label").and_then(|v| v.as_str()).unwrap_or("");
            if label.len() > MAX_REFERENCE_LABEL_CHARS {
                return Err(CatalogError::Validation(format!(
                    "reference label length {} > {MAX_REFERENCE_LABEL_CHARS}",
                    label.len()
                )));
            }
            let url = r.get("url").and_then(|v| v.as_str()).unwrap_or("");
            if url.len() > MAX_REFERENCE_URL_CHARS {
                return Err(CatalogError::Validation(format!(
                    "reference url length {} > {MAX_REFERENCE_URL_CHARS}",
                    url.len()
                )));
            }
        }
    }
    Ok(())
}

/// Cap structure JSON payload — tiled#262. A 100MB structure is almost
/// always a sign of accidental misuse (e.g. embedding pixel data into
/// the structure object instead of a data source). Treated separately
/// from metadata so the limit can be tuned independently if a real
/// workload needs it.
pub fn validate_structure(structure: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(structure)?.len();
    if bytes > MAX_STRUCTURE_BYTES {
        return Err(CatalogError::Validation(format!(
            "data_source.structure is {bytes} bytes; limit is {MAX_STRUCTURE_BYTES}",
        )));
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
    /// specs, access_blob) onto the revisions table so undo is possible —
    /// unless `drop_revision` is set, in which case the prior version is
    /// discarded. Mirrors upstream tiled PR #972's `?drop_revision=true`
    /// flag. Returns the updated node.
    pub async fn update_metadata(
        &self,
        node_id: i64,
        metadata: Value,
        specs: Value,
        drop_revision: bool,
    ) -> Result<Node> {
        validate_payload(&metadata, &specs)?;
        match self.pool() {
            DbPool::Sqlite(pool) => {
                if !drop_revision {
                    let prev: (String, String, String) = sqlx::query_as(
                        "SELECT metadata, specs, access_blob FROM nodes WHERE id = ?",
                    )
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
                        "INSERT INTO revisions (node_id, revision, metadata, specs, access_blob)
                         VALUES (?, ?, ?, ?, ?)",
                    )
                    .bind(node_id)
                    .bind(next_revision as i32)
                    .bind(prev.0)
                    .bind(prev.1)
                    .bind(prev.2)
                    .execute(pool)
                    .await?;
                }
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
                if !drop_revision {
                    let row = sqlx::query(
                        "SELECT metadata::text AS metadata, specs::text AS specs,
                                access_blob::text AS access_blob
                           FROM nodes WHERE id = $1",
                    )
                    .bind(node_id)
                    .fetch_optional(pool)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound(format!("node id {node_id}")))?;
                    let prev_meta: String = row.get("metadata");
                    let prev_specs: String = row.get("specs");
                    let prev_access: String = row.get("access_blob");
                    let next_revision: i64 = sqlx::query_scalar(
                        "SELECT COALESCE(MAX(revision), 0) + 1 FROM revisions WHERE node_id = $1",
                    )
                    .bind(node_id)
                    .fetch_one(pool)
                    .await?;
                    sqlx::query(
                        "INSERT INTO revisions (node_id, revision, metadata, specs, access_blob)
                         VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb)",
                    )
                    .bind(node_id)
                    .bind(next_revision as i32)
                    .bind(prev_meta)
                    .bind(prev_specs)
                    .bind(prev_access)
                    .execute(pool)
                    .await?;
                }
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
    ///
    /// When `external_only` is true (the safe default), refuse the delete if
    /// any data source in the subtree is internally managed (`management !=
    /// 'external'`): cascading the catalog rows would orphan the underlying
    /// managed storage files. The subtree is enumerated with a recursive CTE
    /// over `parent_id`, which is exactly the set the FK `ON DELETE CASCADE`
    /// will remove. Mirrors Python
    /// `tiled.catalog.adapter.CatalogNodeAdapter.delete` (adapter.py:1037-1055,
    /// raising `WouldDeleteData`).
    pub async fn delete_node(&self, node_id: i64, external_only: bool) -> Result<()> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                if external_only {
                    let internal: i64 = sqlx::query_scalar(SUBTREE_INTERNAL_COUNT_SQLITE)
                        .bind(node_id)
                        .fetch_one(pool)
                        .await?;
                    if internal > 0 {
                        return Err(CatalogError::WouldDeleteData(
                            WOULD_DELETE_DATA_MSG.to_string(),
                        ));
                    }
                }
                let res = sqlx::query("DELETE FROM nodes WHERE id = ?")
                    .bind(node_id)
                    .execute(pool)
                    .await?;
                if res.rows_affected() == 0 {
                    return Err(CatalogError::NotFound(format!("node id {node_id}")));
                }
            }
            DbPool::Postgres(pool) => {
                if external_only {
                    let internal: i64 = sqlx::query_scalar(SUBTREE_INTERNAL_COUNT_POSTGRES)
                        .bind(node_id)
                        .fetch_one(pool)
                        .await?;
                    if internal > 0 {
                        return Err(CatalogError::WouldDeleteData(
                            WOULD_DELETE_DATA_MSG.to_string(),
                        ));
                    }
                }
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

    /// List a node's metadata revisions, oldest first (ascending `revision`),
    /// windowed by `[offset, offset + limit)`. Mirrors Python
    /// `CatalogNodeAdapter.revisions` (catalog/adapter.py:972-982), which
    /// selects `orm.Revision` for the node with `.offset(offset).limit(limit)`.
    pub async fn list_revisions(
        &self,
        node_id: i64,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Revision>> {
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let rows = sqlx::query(
                    "SELECT revision, metadata, specs, time_created
                       FROM revisions
                      WHERE node_id = ?
                      ORDER BY revision
                      LIMIT ? OFFSET ?",
                )
                .bind(node_id)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(pool)
                .await?;
                rows.iter()
                    .map(|row| {
                        Ok(Revision {
                            // SQLite INTEGER decodes natively to i64.
                            revision_number: row.get::<i64, _>("revision"),
                            metadata: serde_json::from_str(&row.get::<String, _>("metadata"))?,
                            specs: serde_json::from_str(&row.get::<String, _>("specs"))?,
                            time_updated: row.get::<String, _>("time_created"),
                        })
                    })
                    .collect()
            }
            DbPool::Postgres(pool) => {
                let rows = sqlx::query(
                    "SELECT revision, metadata::text AS metadata, specs::text AS specs,
                            time_created::text AS time_created
                       FROM revisions
                      WHERE node_id = $1
                      ORDER BY revision
                      LIMIT $2 OFFSET $3",
                )
                .bind(node_id)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(pool)
                .await?;
                rows.iter()
                    .map(|row| {
                        Ok(Revision {
                            // Postgres `revision` is INTEGER (i32); widen to i64.
                            revision_number: row.get::<i32, _>("revision") as i64,
                            metadata: serde_json::from_str(&row.get::<String, _>("metadata"))?,
                            specs: serde_json::from_str(&row.get::<String, _>("specs"))?,
                            time_updated: row.get::<String, _>("time_created"),
                        })
                    })
                    .collect()
            }
        }
    }

    /// Delete one revision of a node by its `revision_number`. Returns
    /// `Ok(false)` when no such revision exists (the caller maps that to 404),
    /// `Ok(true)` on success. Mirrors Python
    /// `CatalogNodeAdapter.delete_revision` (catalog/adapter.py:1200-1217),
    /// which raises 404 when `rowcount == 0`.
    pub async fn delete_revision(&self, node_id: i64, number: i64) -> Result<bool> {
        let affected = match self.pool() {
            DbPool::Sqlite(pool) => {
                sqlx::query("DELETE FROM revisions WHERE node_id = ? AND revision = ?")
                    .bind(node_id)
                    .bind(number)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
            DbPool::Postgres(pool) => {
                sqlx::query("DELETE FROM revisions WHERE node_id = $1 AND revision = $2")
                    .bind(node_id)
                    .bind(number as i32)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
        };
        Ok(affected > 0)
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
