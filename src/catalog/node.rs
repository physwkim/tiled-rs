//! Node CRUD operations.
//!
//! Each public fn dispatches on the active [`DbPool`] variant so callers
//! don't have to think about dialect. SQLite stores JSON as `TEXT`, Postgres
//! as `JSONB`; both branches return the same `Node` shape after decoding.

use std::path::{Component, Path, PathBuf};

use serde_json::Value;
use sqlx::Row;

use crate::catalog::db::{Catalog, DbPool, DeleteScope};
use crate::catalog::error::{CatalogError, Result};
use crate::catalog::orm::Node;

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

/// List the assets in the subtree rooted at the bound node id (inclusive) that
/// belong to a non-`external` (internally-managed) data source — the files
/// Python's `delete()` reclaims after the rows are gone (the
/// `if management != external: delete_physical_asset(...)` loop,
/// adapter.py:1188-1191). External assets are deliberately excluded: the owner
/// never handed their storage to tiled, so a delete must leave their files in
/// place. The recursive CTE walks the same `parent_id` chain the FK
/// `ON DELETE CASCADE` removes, so the set is exactly what the delete destroys.
const SUBTREE_MANAGED_ASSETS_SQLITE: &str = "WITH RECURSIVE subtree(id) AS (
    SELECT id FROM nodes WHERE id = ?
    UNION ALL
    SELECT n.id FROM nodes n JOIN subtree s ON n.parent_id = s.id
)
SELECT a.data_uri AS data_uri, a.is_directory AS is_directory
  FROM assets a
  JOIN data_sources ds ON ds.id = a.data_source_id
  JOIN subtree s ON ds.node_id = s.id
 WHERE ds.management <> 'external'";

/// Postgres variant of [`SUBTREE_MANAGED_ASSETS_SQLITE`] (positional `$1`).
const SUBTREE_MANAGED_ASSETS_POSTGRES: &str = "WITH RECURSIVE subtree(id) AS (
    SELECT id FROM nodes WHERE id = $1
    UNION ALL
    SELECT n.id FROM nodes n JOIN subtree s ON n.parent_id = s.id
)
SELECT a.data_uri AS data_uri, a.is_directory AS is_directory
  FROM assets a
  JOIN data_sources ds ON ds.id = a.data_source_id
  JOIN subtree s ON ds.node_id = s.id
 WHERE ds.management <> 'external'";

/// Decode a `file://` URI to its absolute path via the shared cross-platform
/// parser ([`crate::core::file_uri`]), the same one the server's read resolver
/// (`uri_to_path`) and asset endpoint (`path_from_file_uri`) use, so a stored
/// `data_uri` maps to the same path on delete as on read. Returns `None` for
/// any non-`file://` scheme or a URI with no path, so the caller skips assets
/// it cannot (and must not) remove — e.g. sqlite/duckdb/postgresql storage
/// URIs, which this port never writes.
fn file_uri_to_path(uri: &str) -> Option<PathBuf> {
    crate::core::file_uri::file_uri_to_path(uri)
}

/// Resolve the real filesystem path to remove for a managed asset, enforcing the
/// deletion containment `scope`.
///
/// A client can register an internally-managed (`management != external`) data
/// source with an arbitrary `file://` `data_uri` (the create path stores it
/// verbatim, and `Management` defaults to writable), so the delete path must NOT
/// trust that URI: without this gate a `DELETE …?external_only=false` would
/// `remove_dir_all` whatever path the attacker chose. Mirrors the read-side
/// `check_allowed` in tiled-server `file_resolver.rs`.
///
/// - [`Unrestricted`](DeleteScope::Unrestricted) → return the path unchanged,
///   no filesystem check (the historical embedded behaviour).
/// - [`Restricted`](DeleteScope::Restricted) → canonicalise the path (resolving
///   symlinks so a link cannot escape) and require the real location to live
///   under one of the configured dirs; refuse otherwise. An empty dir list
///   refuses every existing path (deny-by-default). The canonical path is
///   returned so the subsequent removal targets the resolved location, closing
///   the validate-then-remove TOCTOU window.
///
/// Returns `Ok(None)` when the path does not exist (`NotFound`): there is
/// nothing to remove, so a missing managed file is neither an error nor a
/// containment decision — you cannot destroy what is absent.
fn resolve_contained_target(scope: &DeleteScope, path: &Path) -> Result<Option<PathBuf>> {
    let dirs = match scope {
        DeleteScope::Unrestricted => return Ok(Some(path.to_path_buf())),
        DeleteScope::Restricted(dirs) => dirs,
    };
    let canonical = match path.canonicalize() {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(CatalogError::Io(e)),
    };
    if dirs.is_empty() {
        return Err(CatalogError::Validation(format!(
            "refusing to delete managed asset {}: no managed-delete directory is configured \
             (pass --allowed-data-dir, or --allow-unrestricted-reads to disable the check)",
            canonical.display()
        )));
    }
    if is_under_allowed(&canonical, dirs) {
        return Ok(Some(canonical));
    }
    Err(CatalogError::Validation(format!(
        "refusing to delete managed asset {}: outside the configured managed-delete directories",
        canonical.display()
    )))
}

/// True when `canonical` (an already-canonicalised path) lives under one of
/// `dirs`. Each allow-listed dir is itself canonicalised so a symlinked storage
/// root matches. Shared by the delete-time ([`resolve_contained_target`]) and
/// write-time ([`resolve_write_target`]) containment checks so both consult one
/// rule. Mirrors the read-side `check_allowed` in tiled-server `file_resolver.rs`.
fn is_under_allowed(canonical: &Path, dirs: &[PathBuf]) -> bool {
    dirs.iter().any(|dir| {
        let dir_canon = dir.canonicalize().unwrap_or_else(|_| dir.clone());
        canonical.starts_with(&dir_canon)
    })
}

/// Validate that a managed asset's `data_uri` *path* lies under one of `dirs`,
/// for the write/register path where the file may not exist yet — the
/// fail-fast counterpart of [`resolve_contained_target`] (S2 source side). A
/// client posting a `DataSource` with a managed (`management != external`)
/// `file://` asset must not be able to point it outside storage and later turn
/// it into an arbitrary-file delete.
///
/// Unlike the delete path, a not-yet-created leaf is legitimate, so existence is
/// NOT required. To stay symlink-safe without canonicalising a missing file:
/// reject any `..` component outright (a managed write path never needs one),
/// then canonicalise the deepest existing ancestor — resolving symlinks in the
/// real portion — and re-attach the remaining components before the containment
/// check. An empty `dirs` list permits nothing (deny-by-default).
fn resolve_write_target(dirs: &[PathBuf], path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(CatalogError::Validation(format!(
            "managed asset data_uri must be an absolute path, got {}",
            path.display()
        )));
    }
    // A `..` in the requested path could escape the allow-list once the
    // non-existent tail is joined; managed write paths never need one.
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(CatalogError::Validation(format!(
            "managed asset data_uri must not contain '..': {}",
            path.display()
        )));
    }
    // Walk up to the deepest ancestor that exists on disk so symlinks in the
    // real prefix are resolved; the leaf (and possibly some parents) may be
    // created later by the writer.
    let mut existing = path;
    while !existing.exists() {
        match existing.parent() {
            Some(parent) => existing = parent,
            None => break,
        }
    }
    let base = existing
        .canonicalize()
        .unwrap_or_else(|_| existing.to_path_buf());
    let tail = path
        .strip_prefix(existing)
        .unwrap_or_else(|_| Path::new(""));
    let candidate = base.join(tail);
    if is_under_allowed(&candidate, dirs) {
        return Ok(candidate);
    }
    Err(CatalogError::Validation(format!(
        "refusing to register managed asset {}: outside the configured storage directories",
        path.display()
    )))
}

/// Remove the physical files backing internally-managed assets, after their
/// catalog rows have already been deleted. Mirrors Python `delete_physical_asset`
/// for the `file://` scheme (adapter.py:1841-1850): a directory asset is removed
/// recursively, a plain asset is unlinked. The `std::fs` calls run on a blocking
/// thread — the workspace `tokio` has no `fs` feature, and this also keeps the
/// I/O off the async runtime. A file that is already absent is treated as
/// success (the post-condition — file gone — already holds), but any other I/O
/// error is surfaced so the caller learns the managed storage was not fully
/// reclaimed.
///
/// Every path is first run through [`resolve_contained_target`] with `scope`, so
/// a managed asset whose `data_uri` resolves outside the configured storage
/// directories is refused (returns `Err`) instead of being removed — the
/// destructive counterpart of the read-side containment.
async fn delete_physical_managed_assets(
    assets: Vec<(String, bool)>,
    scope: DeleteScope,
) -> Result<()> {
    if assets.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || -> Result<()> {
        for (data_uri, is_directory) in assets {
            let Some(path) = file_uri_to_path(&data_uri) else {
                continue;
            };
            // Containment gate: refuse out-of-storage paths, skip already-gone
            // ones (Ok(None)), and remove the canonicalised target otherwise.
            let Some(target) = resolve_contained_target(&scope, &path)? else {
                continue;
            };
            let result = if is_directory {
                std::fs::remove_dir_all(&target)
            } else {
                std::fs::remove_file(&target)
            };
            if let Err(e) = result
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(CatalogError::Io(e));
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| {
        CatalogError::Io(std::io::Error::other(format!(
            "asset removal task failed: {e}"
        )))
    })?
}

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
    /// Reject, at register time, a managed asset whose `file://` `data_uri`
    /// resolves outside the configured storage directories — the write-time,
    /// fail-fast counterpart of the delete-time containment (S2). Callers (the
    /// server `create_node` path) invoke this for every asset of a data source
    /// whose `management != external` before persisting it.
    ///
    /// Honours the same [`DeleteScope`] as deletion (one source of truth):
    /// `Unrestricted` accepts anything; `Restricted` requires the path under an
    /// allowed dir (empty list denies all). A non-`file://` URI (e.g. an
    /// sqlite/duckdb storage URI) is not a managed filesystem path and is
    /// accepted — it is never a physical-delete target.
    pub fn validate_managed_data_uri(&self, data_uri: &str) -> Result<()> {
        let dirs = match self.delete_scope() {
            DeleteScope::Unrestricted => return Ok(()),
            DeleteScope::Restricted(dirs) => dirs,
        };
        let Some(path) = file_uri_to_path(data_uri) else {
            return Ok(());
        };
        resolve_write_target(dirs, &path).map(|_| ())
    }

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

    /// Fast lower bound on the number of children of `parent_id`, counting at
    /// most `threshold + 1` rows. When the result is `<= threshold` it is the
    /// exact count; otherwise it is exactly `threshold + 1`, signalling "more
    /// than `threshold`". Cheap even for huge containers because the inner
    /// `SELECT ... LIMIT threshold+1` stops scanning early. Mirrors Python
    /// `lbound_len` (catalog/adapter.py:506-523).
    pub async fn lbound_count_children(
        &self,
        parent_id: Option<i64>,
        threshold: i64,
    ) -> Result<i64> {
        let limit = threshold.saturating_add(1);
        match self.pool() {
            DbPool::Sqlite(pool) => {
                let n: i64 = if parent_id.is_some() {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM \
                         (SELECT 1 FROM nodes WHERE parent_id = ? LIMIT ?) AS sub",
                    )
                    .bind(parent_id)
                    .bind(limit)
                    .fetch_one(pool)
                    .await?
                } else {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM \
                         (SELECT 1 FROM nodes WHERE parent_id IS NULL LIMIT ?) AS sub",
                    )
                    .bind(limit)
                    .fetch_one(pool)
                    .await?
                };
                Ok(n)
            }
            DbPool::Postgres(pool) => {
                let n: i64 = if parent_id.is_some() {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM \
                         (SELECT 1 FROM nodes WHERE parent_id = $1 LIMIT $2) AS sub",
                    )
                    .bind(parent_id)
                    .bind(limit)
                    .fetch_one(pool)
                    .await?
                } else {
                    sqlx::query_scalar(
                        "SELECT COUNT(*) FROM \
                         (SELECT 1 FROM nodes WHERE parent_id IS NULL LIMIT $1) AS sub",
                    )
                    .bind(limit)
                    .fetch_one(pool)
                    .await?
                };
                Ok(n)
            }
        }
    }

    /// Approximate number of children of `parent_id` from table statistics,
    /// **Postgres only**. Reads the sampled frequency of this parent among the
    /// `parent_id` column's `most_common_vals` (from `pg_stats`) and scales it
    /// by the table's estimated live-row count (`pg_class.reltuples`). Mirrors
    /// Python `approx_len` (catalog/adapter.py:463-504).
    ///
    /// Returns `None` — so the caller falls back to an exact/bounded count —
    /// when any of these hold:
    /// * the backend is SQLite (no statistics tables);
    /// * `parent_id` is `None` (the root): root children carry a `NULL`
    ///   `parent_id`, which `pg_stats` tracks via `null_frac`, not in
    ///   `most_common_vals`, so there is no common-value entry to match
    ///   (upstream's node always has a concrete id, so this case cannot arise
    ///   there);
    /// * the table has never been `ANALYZE`d, or this parent is not frequent
    ///   enough to appear among the most-common values.
    ///
    /// Requires the `nodes` table to have been vacuumed/analyzed (at least
    /// once) for the estimate to be meaningful.
    pub async fn approx_count_children(&self, parent_id: Option<i64>) -> Result<Option<i64>> {
        let DbPool::Postgres(pool) = self.pool() else {
            // SQLite (and any non-Postgres backend): no statistics tables.
            return Ok(None);
        };
        let Some(parent_id) = parent_id else {
            // Root: NULL parent_id is not represented in most_common_vals.
            return Ok(None);
        };
        // Sampled frequency of this parent among the parent_id column values.
        let freq: Option<f32> = sqlx::query_scalar(
            "SELECT s.freq FROM \
             (SELECT unnest(most_common_vals::text::bigint[]) AS parent, \
                     unnest(most_common_freqs) AS freq \
              FROM pg_stats \
              WHERE schemaname = 'public' AND tablename = 'nodes' \
                AND attname = 'parent_id') AS s \
             WHERE s.parent = $1",
        )
        .bind(parent_id)
        .fetch_optional(pool)
        .await?;
        let Some(freq) = freq else {
            // Statistics unavailable, or this parent is not among the
            // most-common values: caller falls back to the exact/bounded count.
            return Ok(None);
        };
        // Estimated live row count for the whole `nodes` table.
        let total: i64 = sqlx::query_scalar(
            "SELECT reltuples::bigint FROM pg_class WHERE oid = 'public.nodes'::regclass",
        )
        .fetch_one(pool)
        .await?;
        // Mirror Python `int(total * freq)` (adapter.py:498): truncate toward 0.
        Ok(Some((total as f64 * freq as f64) as i64))
    }

    /// Child count that is exact for small containers and approximate for large
    /// ones. Mirrors Python `len_or_approx` (server/core.py:65-118) invoked with
    /// `threshold = exact_count_limit`:
    ///
    /// * **SQLite** — always the exact `COUNT(*)`. Unlike upstream (whose SQLite
    ///   path returns the `threshold + 1` lower bound for large containers), the
    ///   Rust port deliberately keeps the exact count for SQLite; only Postgres
    ///   gets the statistics-based estimate.
    /// * **Postgres** — take a fast lower bound first. If it is `<=`
    ///   `exact_count_limit` it is the exact count and is returned as-is. For a
    ///   larger container, use [`Self::approx_count_children`]; if statistics are
    ///   unavailable (never analyzed, or the root node), fall back to the lower
    ///   bound (`exact_count_limit + 1`).
    pub async fn count_children_or_approx(
        &self,
        parent_id: Option<i64>,
        exact_count_limit: i64,
    ) -> Result<i64> {
        match self.pool() {
            DbPool::Sqlite(_) => self.count_children(parent_id).await,
            DbPool::Postgres(_) => {
                let lbound = self
                    .lbound_count_children(parent_id, exact_count_limit)
                    .await?;
                if lbound <= exact_count_limit {
                    // Within the threshold → this lower bound is exact.
                    return Ok(lbound);
                }
                // Large container: prefer the statistics-based estimate.
                if let Some(approx) = self.approx_count_children(parent_id).await? {
                    return Ok(approx);
                }
                // Statistics unavailable: report the lower bound (threshold + 1).
                Ok(lbound)
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

    /// Replace metadata + specs (and optionally access_blob) on a node.
    /// Pushes the previous (metadata, specs, access_blob) onto the revisions
    /// table so undo is possible — unless `drop_revision` is set.
    /// Mirrors upstream tiled PR #972's `?drop_revision=true` flag.
    ///
    /// `new_access_blob`: when `Some`, the stored `access_blob` is replaced
    /// atomically with the metadata update — matching Python's combined
    /// `replace_metadata(... access_blob=...)`. When `None`, the stored blob
    /// is left unchanged.
    pub async fn update_metadata(
        &self,
        node_id: i64,
        metadata: Value,
        specs: Value,
        new_access_blob: Option<Value>,
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
                let row = if let Some(blob) = new_access_blob {
                    sqlx::query(
                        "UPDATE nodes
                            SET metadata = ?, specs = ?, access_blob = ?,
                                time_updated = strftime('%Y-%m-%dT%H:%M:%fZ','now')
                          WHERE id = ?
                         RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                                   specs, access_blob, time_created, time_updated",
                    )
                    .bind(serde_json::to_string(&metadata)?)
                    .bind(serde_json::to_string(&specs)?)
                    .bind(serde_json::to_string(&blob)?)
                    .bind(node_id)
                    .fetch_one(pool)
                    .await?
                } else {
                    sqlx::query(
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
                    .await?
                };
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
                let row = if let Some(blob) = new_access_blob {
                    sqlx::query(
                        "UPDATE nodes
                            SET metadata = $1::jsonb, specs = $2::jsonb,
                                access_blob = $3::jsonb, time_updated = now()
                          WHERE id = $4
                         RETURNING id, key, parent_id, ancestors, structure_family, metadata,
                                   specs, access_blob, time_created, time_updated",
                    )
                    .bind(serde_json::to_string(&metadata)?)
                    .bind(serde_json::to_string(&specs)?)
                    .bind(serde_json::to_string(&blob)?)
                    .bind(node_id)
                    .fetch_one(pool)
                    .await?
                } else {
                    sqlx::query(
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
                    .await?
                };
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
    ///
    /// When the delete does proceed (`external_only=false`, or no managed
    /// sources are present), the physical files backing the internally-managed
    /// `file://` assets are removed after the rows are gone — Python reclaims
    /// them in the `if management != external: delete_physical_asset(...)` loop
    /// (adapter.py:1188-1191). External assets are never touched. Dropping the
    /// rows without this step left managed storage orphaned on disk (catalog
    /// M5). The managed-asset set is read before the cascade removes the rows,
    /// and the files are deleted only after the row delete succeeds.
    pub async fn delete_node(&self, node_id: i64, external_only: bool) -> Result<()> {
        let to_remove: Vec<(String, bool)>;
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
                // Capture the managed file:// assets BEFORE the cascade drops
                // their rows; empty when external_only gated them out above.
                let managed = sqlx::query(SUBTREE_MANAGED_ASSETS_SQLITE)
                    .bind(node_id)
                    .fetch_all(pool)
                    .await?;
                to_remove = managed
                    .iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("data_uri"),
                            r.get::<i64, _>("is_directory") != 0,
                        )
                    })
                    .collect();
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
                let managed = sqlx::query(SUBTREE_MANAGED_ASSETS_POSTGRES)
                    .bind(node_id)
                    .fetch_all(pool)
                    .await?;
                to_remove = managed
                    .iter()
                    .map(|r| {
                        (
                            r.get::<String, _>("data_uri"),
                            r.get::<bool, _>("is_directory"),
                        )
                    })
                    .collect();
                let res = sqlx::query("DELETE FROM nodes WHERE id = $1")
                    .bind(node_id)
                    .execute(pool)
                    .await?;
                if res.rows_affected() == 0 {
                    return Err(CatalogError::NotFound(format!("node id {node_id}")));
                }
            }
        }
        // Rows (incl. data_sources + assets via cascade) are gone; reclaim the
        // managed storage files. Done after the row delete, matching Python's
        // out-of-transaction physical deletion. The configured delete scope
        // contains each path so a client-registered managed `data_uri` outside
        // storage cannot turn this into an arbitrary-file delete.
        delete_physical_managed_assets(to_remove, self.delete_scope().clone()).await
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
