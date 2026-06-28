//! SQL-backed ragged-array storage (SQLite).
//!
//! Mirrors the storage half of Python `RaggedSQLAdapter`
//! (`tiled/adapters/ragged.py:81-356`). Python stores one SQL row per chunk:
//! the columns are the Awkward buffer keys (`node0-offsets`, `node1-data`, …)
//! plus a `chunk_index`, and a duplicate `chunk_index` write raises `Conflicts`.
//! Python's tabular backend (ADBC/DuckDB) stores each buffer as a typed *array*
//! column. SQLite has no array type, so this store keeps each buffer as a
//! **`BLOB` of its raw little-endian bytes** — the same bytes the L1 codec
//! produces ([`tiled_serialization::ragged::BufferMap`]), so the wire protocol
//! (`awkward.from_buffers` / `to_buffers`) is byte-for-byte identical; only the
//! at-rest column type differs.
//!
//! This is the first SQL-backed *managed* storage in the Rust port (the
//! file-backed [`crate::file_resolver`] path rejects `sqlite://`). The store
//! owns only the chunk table; the [`crate::ragged_sql_adapter::RaggedSqlStore`]
//! the higher layers wrap it with read/write/patch live above it (L3).

#![cfg(feature = "sql-adapter")]

use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use tiled_core::adapters::{
    BaseAdapter, BoxFuture, RaggedAdapterRead, RaggedAdapterWrite, RaggedData,
};
use tiled_core::data_source::Asset;
use tiled_core::error::{Result, TiledError};
use tiled_core::ndslice::NDSlice;
use tiled_core::structures::{RaggedStructure, Spec, StructureFamily};
use tiled_serialization::ragged::{
    BufferMap, awkward_form_json, buffers_to_json, expected_from_buffers, json_to_buffers,
};

use crate::ragged_adapter::{RaggedAdapter, json_leaf_count};

/// One chunk loaded from storage: its `chunk_index` and its buffer map.
pub type ChunkRow = (i64, BufferMap);

/// SQLite-backed store for one ragged dataset's chunks.
///
/// Layout: one table, one row per chunk. Columns are `_dataset_id`,
/// `chunk_index`, and one `BLOB` per Awkward buffer key. `PRIMARY KEY
/// (_dataset_id, chunk_index)` enforces the single-producer-per-chunk
/// invariant Python relies on — a duplicate write surfaces as
/// [`TiledError::Conflict`] (HTTP 409). The composite key (vs Python's
/// `chunk_index`-only key) additionally lets independent datasets share a
/// table without colliding, while keeping same-dataset conflict identical.
///
/// Connections are opened per operation (matching Python's
/// `closing(storage.connect())`), so `new` does no IO and the store is cheap
/// to construct and clone.
#[derive(Debug, Clone)]
pub struct RaggedSqlStore {
    database_url: String,
    table_name: String,
    dataset_id: i64,
    buffer_keys: Vec<String>,
}

impl RaggedSqlStore {
    /// Construct a store handle. `buffer_keys` are the Awkward buffer column
    /// names (e.g. from [`tiled_serialization::ragged::expected_from_buffers`]).
    /// The table name and every buffer key are validated as safe SQL
    /// identifiers up front so the dynamic DDL/DML below can quote them
    /// without risking injection.
    pub fn new(
        database_url: impl Into<String>,
        table_name: impl Into<String>,
        dataset_id: i64,
        buffer_keys: Vec<String>,
    ) -> Result<Self> {
        let table_name = table_name.into();
        validate_identifier(&table_name)?;
        for key in &buffer_keys {
            validate_identifier(key)?;
        }
        Ok(Self {
            database_url: database_url.into(),
            table_name,
            dataset_id,
            buffer_keys,
        })
    }

    /// The buffer-key columns this store reads and writes, in order.
    pub fn buffer_keys(&self) -> &[String] {
        &self.buffer_keys
    }

    /// Open a fresh connection pool, creating the database file if missing.
    /// `create_if_missing` covers `init_storage` on a brand-new node; reopen
    /// on append/load finds the existing file.
    async fn connect(&self) -> Result<SqlitePool> {
        let opts = SqliteConnectOptions::from_str(&self.database_url)
            .map_err(|e| TiledError::Database(format!("sqlite url {}: {e}", self.database_url)))?
            .create_if_missing(true);
        SqlitePoolOptions::new()
            .connect_with(opts)
            .await
            .map_err(|e| TiledError::Database(format!("sqlite connect {}: {e}", self.database_url)))
    }

    /// `CREATE TABLE IF NOT EXISTS` with one `BLOB` column per buffer key plus
    /// `_dataset_id` / `chunk_index` and the composite primary key. Idempotent,
    /// so two datasets sharing a table each call it harmlessly.
    pub async fn init_storage(&self) -> Result<()> {
        let pool = self.connect().await?;
        let result = self.create_table(&pool).await;
        pool.close().await;
        result
    }

    async fn create_table(&self, pool: &SqlitePool) -> Result<()> {
        let blob_cols: String = self
            .buffer_keys
            .iter()
            .map(|key| format!(", \"{key}\" BLOB NOT NULL"))
            .collect();
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (\
             _dataset_id INTEGER NOT NULL, \
             chunk_index INTEGER NOT NULL{blob_cols}, \
             PRIMARY KEY (_dataset_id, chunk_index))",
            self.table_name
        );
        sqlx::query(&ddl)
            .execute(pool)
            .await
            .map_err(|e| TiledError::Database(format!("create ragged table: {e}")))?;
        Ok(())
    }

    /// Insert one chunk row. A duplicate `(_dataset_id, chunk_index)` is a
    /// [`TiledError::Conflict`] (Python `Conflicts` → HTTP 409). Every declared
    /// buffer key must be present in `buffers`, else a [`TiledError::Validation`].
    pub async fn append_chunk(&self, chunk_index: i64, buffers: &BufferMap) -> Result<()> {
        let pool = self.connect().await?;
        let result = self.insert_chunk(&pool, chunk_index, buffers).await;
        pool.close().await;
        result
    }

    async fn insert_chunk(
        &self,
        pool: &SqlitePool,
        chunk_index: i64,
        buffers: &BufferMap,
    ) -> Result<()> {
        let col_list: String = self
            .buffer_keys
            .iter()
            .map(|key| format!(", \"{key}\""))
            .collect();
        let placeholders: String = self.buffer_keys.iter().map(|_| ", ?").collect();
        let sql = format!(
            "INSERT INTO \"{}\" (_dataset_id, chunk_index{col_list}) VALUES (?, ?{placeholders})",
            self.table_name
        );
        let mut query = sqlx::query(&sql).bind(self.dataset_id).bind(chunk_index);
        for key in &self.buffer_keys {
            let buf = buffers.get(key).ok_or_else(|| {
                TiledError::Validation(format!("ragged chunk is missing expected buffer {key:?}"))
            })?;
            query = query.bind(buf.clone());
        }
        query
            .execute(pool)
            .await
            .map_err(|e| map_insert_error(e, chunk_index))?;
        Ok(())
    }

    /// Load chunks ordered by `chunk_index` ascending. A non-empty `indexes`
    /// pushes a `chunk_index IN (...)` filter into SQL (Python `_load_chunks`);
    /// an empty `indexes` loads every chunk for this dataset.
    pub async fn load_chunks(&self, indexes: &[i64]) -> Result<Vec<ChunkRow>> {
        let pool = self.connect().await?;
        let result = self.select_chunks(&pool, indexes).await;
        pool.close().await;
        result
    }

    async fn select_chunks(&self, pool: &SqlitePool, indexes: &[i64]) -> Result<Vec<ChunkRow>> {
        let col_list: String = self
            .buffer_keys
            .iter()
            .map(|key| format!(", \"{key}\""))
            .collect();
        // `indexes` are i64 we format directly; no user strings enter the SQL.
        let where_idx = if indexes.is_empty() {
            String::new()
        } else {
            let in_list = indexes
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" AND chunk_index IN ({in_list})")
        };
        let sql = format!(
            "SELECT chunk_index{col_list} FROM \"{}\" \
             WHERE _dataset_id = {}{where_idx} ORDER BY chunk_index ASC",
            self.table_name, self.dataset_id
        );
        let rows = sqlx::query(&sql)
            .fetch_all(pool)
            .await
            .map_err(|e| TiledError::Database(format!("load ragged chunks: {e}")))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let chunk_index: i64 = row
                .try_get("chunk_index")
                .map_err(|e| TiledError::Database(format!("chunk_index column: {e}")))?;
            let mut buffers = BufferMap::new();
            for key in &self.buffer_keys {
                let blob: Vec<u8> = row
                    .try_get(key.as_str())
                    .map_err(|e| TiledError::Database(format!("buffer column {key:?}: {e}")))?;
                buffers.insert(key.clone(), blob);
            }
            out.push((chunk_index, buffers));
        }
        Ok(out)
    }
}

/// Classify an INSERT failure: a unique/primary-key violation is the
/// duplicate-chunk conflict Python raises as `Conflicts`; anything else is a
/// plain database error.
fn map_insert_error(err: sqlx::Error, chunk_index: i64) -> TiledError {
    if let sqlx::Error::Database(db) = &err
        && db.is_unique_violation()
    {
        return TiledError::Conflict(format!(
            "Cannot write chunk with chunk_index={chunk_index}: a chunk with this index \
             already exists. This typically indicates a concurrent write to the same \
             dataset; ragged arrays are designed for a single producer per dataset."
        ));
    }
    TiledError::Database(format!("append ragged chunk: {err}"))
}

/// Reject identifiers that could break out of a quoted SQL name. Mirrors the
/// read-side `sql_adapter::validate_identifier`. Hyphens (in Awkward keys like
/// `node0-offsets`) are safe inside double-quoted identifiers.
fn validate_identifier(s: &str) -> Result<()> {
    if s.is_empty() || s.contains('"') || s.contains(';') || s.contains('\0') {
        return Err(TiledError::Validation(format!(
            "unsafe SQL identifier: {s:?}"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RaggedSQLAdapter — read/write/patch over RaggedSqlStore
// ---------------------------------------------------------------------------

/// SQL-backed ragged adapter — the read+write counterpart of Python
/// `RaggedSQLAdapter` (`tiled/adapters/ragged.py:81-356`).
///
/// Holds a [`RaggedSqlStore`] (chunk storage) plus the node's
/// [`RaggedStructure`], metadata and specs. The structure fixes the Awkward
/// form (dtype + raggedness pattern), so this adapter owns the JSON↔buffer
/// encoding: write paths encode a JSON list-of-lists chunk into buffers and
/// `append_chunk` it; the read path loads every chunk, decodes each back to
/// JSON, concatenates, then reuses the in-memory [`RaggedAdapter`]'s tested
/// slicing.
///
/// The catalog persists the structure, so `patch` does not mutate `self`; it
/// returns the grown structure for the caller to write back (L4/L5).
pub struct RaggedSQLAdapter {
    store: RaggedSqlStore,
    structure: RaggedStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Whether write/patch are exposed via [`RaggedAdapterRead::as_writable`].
    /// Set by the resolver only when the backing SQLite file lives under
    /// writable storage — the same containment gate the file-backed adapters
    /// (`NpyAdapter::into_writable`, etc.) use. Defaults to read-only.
    writable: bool,
}

impl RaggedSQLAdapter {
    /// Build an adapter over the chunk table named `table_name` in
    /// `database_url`, scoped to `dataset_id`. The buffer-key columns are
    /// derived from `structure`'s Awkward form (so they match what the write
    /// path produces), via [`expected_from_buffers`] over [`awkward_form_json`].
    ///
    /// Read-only by default; call [`into_writable`](Self::into_writable) to
    /// expose the write face.
    pub fn new(
        database_url: impl Into<String>,
        table_name: impl Into<String>,
        dataset_id: i64,
        structure: RaggedStructure,
        metadata: serde_json::Value,
        specs: Vec<Spec>,
    ) -> Result<Self> {
        let form = awkward_form_json(&structure)
            .map_err(|e| TiledError::Serialization(format!("ragged form: {e}")))?;
        let buffer_keys: Vec<String> = expected_from_buffers(&form)
            .into_iter()
            .map(|(key, _dtype)| key)
            .collect();
        let store = RaggedSqlStore::new(database_url, table_name, dataset_id, buffer_keys)?;
        Ok(Self {
            store,
            structure,
            metadata,
            specs,
            writable: false,
        })
    }

    /// Mark this adapter writable, exposing `write`/`write_block`/`patch` via
    /// [`RaggedAdapterRead::as_writable`]. The resolver calls this only when the
    /// backing file is under writable storage.
    #[must_use]
    pub fn into_writable(mut self) -> Self {
        self.writable = true;
        self
    }

    /// Create the backing chunk table (delegates to the store). Used by the
    /// catalog create path.
    pub async fn init_storage(&self) -> Result<()> {
        self.store.init_storage().await
    }

    /// Load every chunk and reconstruct the full JSON list-of-lists, in
    /// `chunk_index` order — Python `read`'s load+`from_buffers`+`concatenate`
    /// (without the axis-0 chunk-narrowing, which is a pure load optimization;
    /// concatenating all chunks then slicing yields the identical result).
    async fn reconstruct_full(&self) -> Result<serde_json::Value> {
        let form = awkward_form_json(&self.structure)
            .map_err(|e| TiledError::Serialization(format!("ragged form: {e}")))?;
        let chunks0 = self
            .structure
            .chunks
            .first()
            .and_then(|c| c.as_ref())
            .ok_or_else(|| {
                TiledError::Validation("ragged structure has no axis-0 chunk partitioning".into())
            })?;

        let rows = self.store.load_chunks(&[]).await?;
        let mut out: Vec<serde_json::Value> = Vec::new();
        for (chunk_index, buffers) in &rows {
            let idx = usize::try_from(*chunk_index).map_err(|_| {
                TiledError::Validation(format!("negative ragged chunk_index {chunk_index}"))
            })?;
            let length = *chunks0.get(idx).ok_or_else(|| {
                TiledError::Validation(format!(
                    "chunk_index {chunk_index} has no declared length in structure.chunks[0]"
                ))
            })?;
            let chunk_json = buffers_to_json(&form, length, buffers)
                .map_err(|e| TiledError::Serialization(format!("ragged from_buffers: {e}")))?;
            let arr = chunk_json.as_array().ok_or_else(|| {
                TiledError::Serialization("reconstructed ragged chunk is not a JSON array".into())
            })?;
            out.extend(arr.iter().cloned());
        }
        Ok(serde_json::Value::Array(out))
    }

    /// Encode one JSON list-of-lists chunk into Awkward buffers using this
    /// adapter's form, and append it at `chunk_index`. Shared by `write_block`
    /// and `patch`. Returns the chunk's row count (Awkward length).
    async fn encode_and_append(&self, chunk_index: i64, data: &serde_json::Value) -> Result<usize> {
        let (length, buffers) = json_to_buffers(&self.structure, data)
            .map_err(|e| TiledError::Serialization(format!("ragged to_buffers: {e}")))?;
        self.store.append_chunk(chunk_index, &buffers).await?;
        Ok(length)
    }
}

impl BaseAdapter for RaggedSQLAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Ragged
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl RaggedAdapterRead for RaggedSQLAdapter {
    fn structure(&self) -> &RaggedStructure {
        &self.structure
    }

    fn read<'a>(&'a self, slice: &'a NDSlice) -> BoxFuture<'a, Result<RaggedData>> {
        Box::pin(async move {
            let full = self.reconstruct_full().await?;
            // Delegate slicing + sliced-structure recomputation to the
            // in-memory adapter, which already implements and tests the exact
            // numpy/awkward basic-indexing semantics.
            let mem = RaggedAdapter::new(
                full,
                self.structure.clone(),
                self.metadata.clone(),
                self.specs.clone(),
            );
            mem.read(slice).await
        })
    }

    fn as_writable(&self) -> Option<&dyn RaggedAdapterWrite> {
        self.writable.then_some(self as &dyn RaggedAdapterWrite)
    }
}

impl RaggedAdapterWrite for RaggedSQLAdapter {
    fn write_block<'a>(
        &'a self,
        data: &'a serde_json::Value,
        block: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.encode_and_append(block as i64, data).await?;
            Ok(())
        })
    }

    fn patch<'a>(
        &'a self,
        data: &'a serde_json::Value,
        offset: &'a [usize],
        extend: bool,
    ) -> BoxFuture<'a, Result<RaggedStructure>> {
        Box::pin(async move {
            // Python rejects overwrite and empty offset up front.
            if !extend {
                return Err(TiledError::Validation(
                    "Overwriting existing data is not supported".into(),
                ));
            }
            if offset.is_empty() {
                return Err(TiledError::Validation(
                    "`offset` must contain at least one dimension".into(),
                ));
            }

            let s = &self.structure;
            let shape0 = s.shape.first().copied().flatten().ok_or_else(|| {
                TiledError::Validation("first dimension of a ragged array must be known".into())
            })?;
            let ndim_fixed = ndim_fixed(s);

            // Only appending along the leftmost dimension is supported: the
            // offset must start exactly at the current length, all trailing
            // offsets zero, and it must not address a ragged axis.
            if offset[0] != shape0
                || offset[1..].iter().any(|&x| x != 0)
                || offset.len() > ndim_fixed
            {
                return Err(TiledError::Validation(
                    "Only appending along the leftmost dimension is supported".into(),
                ));
            }

            // The appended data must match the existing array along the fixed
            // (non-ragged) dimensions shape[1..ndim_fixed].
            let data_fixed = fixed_dim_lengths(data, ndim_fixed)?;
            let self_fixed: Vec<usize> = s.shape[1..ndim_fixed]
                .iter()
                .map(|d| d.unwrap_or(0))
                .collect();
            if data_fixed != self_fixed {
                return Err(TiledError::Validation(
                    "The shape of the data does not match the existing array along the fixed dimensions"
                        .into(),
                ));
            }

            // Append at the next chunk index (number of existing chunks).
            let chunk_index = chunks0_len(s);
            let length = self.encode_and_append(chunk_index as i64, data).await?;

            // Grow the structure for the caller to persist: axis-0 length and
            // chunk list extend by `length`, size grows by the appended leaf
            // count (Python `data.size`).
            let mut grown = s.clone();
            grown.shape[0] = Some(shape0 + length);
            match grown.chunks.first_mut() {
                Some(Some(c0)) => c0.push(length),
                _ => {
                    return Err(TiledError::Validation(
                        "ragged structure has no axis-0 chunk partitioning to extend".into(),
                    ));
                }
            }
            grown.size += json_leaf_count(data);
            Ok(grown)
        })
    }
}

/// Number of leading fixed-size (known-integer) dimensions — Python
/// `RaggedStructure.ndim_fixed` (`structures/ragged.py:219-221`).
fn ndim_fixed(s: &RaggedStructure) -> usize {
    s.shape
        .iter()
        .position(|d| d.is_none())
        .unwrap_or(s.shape.len())
}

/// Current number of axis-0 chunks — Python `len(chunks[0] or ())`.
fn chunks0_len(s: &RaggedStructure) -> usize {
    s.chunks
        .first()
        .and_then(|c| c.as_ref())
        .map_or(0, Vec::len)
}

/// Lengths of the appended data along the fixed dimensions `1..ndim_fixed`,
/// read by descending into the first element at each level. Used to validate a
/// `patch` against `shape[1..ndim_fixed]`. Empty for the common `[N, None]`
/// case (`ndim_fixed == 1`).
fn fixed_dim_lengths(data: &serde_json::Value, ndim_fixed: usize) -> Result<Vec<usize>> {
    let mut out = Vec::with_capacity(ndim_fixed.saturating_sub(1));
    let mut cur = data;
    for d in 1..ndim_fixed {
        let first = cur.as_array().and_then(|a| a.first()).ok_or_else(|| {
            TiledError::Validation(format!(
                "appended data cannot determine fixed dimension {d} (not nested deeply enough \
                 or empty)"
            ))
        })?;
        let len = first.as_array().map(Vec::len).ok_or_else(|| {
            TiledError::Validation(format!(
                "appended data is not nested deeply enough for fixed dimension {d}"
            ))
        })?;
        out.push(len);
        cur = first;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// init_storage — the managed-create entry point
// ---------------------------------------------------------------------------

/// Fixed table name inside a ragged node's dedicated SQLite file. Unlike
/// Python's shared SQLStorage (one database, MD5-hashed table names, a
/// `_dataset_id` sequence), each Rust ragged node gets its own `.sqlite` file,
/// so a constant table name and dataset id are unambiguous.
const RAGGED_TABLE_NAME: &str = "ragged_data";
const RAGGED_DATASET_ID: i64 = 1;

/// What [`init_storage_ragged_sql`] generated: the SQLite `data_uri`, the
/// table name and dataset id to persist as the data-source parameters, and the
/// single asset describing the database file.
#[derive(Debug, Clone)]
pub struct RaggedSqlInit {
    pub data_uri: String,
    pub table_name: String,
    pub dataset_id: i64,
    pub assets: Vec<Asset>,
}

/// Render `path` for embedding in a `sqlite://` data_uri. Strips the Windows
/// extended-length (verbatim) `\\?\` prefix that `std::fs::canonicalize` emits
/// on the writable-storage root: it contains '?', the URL query delimiter, so
/// `sqlite://\\?\C:\...` parses as a bogus query and sqlx rejects the connection
/// (managed ragged create → HTTP 500). The remaining `C:\...` form is what both
/// sqlx and the resolver's `sqlite_uri_to_path` accept. No-op on Unix and on
/// non-verbatim Windows paths. (A local writable-storage dir canonicalizes to a
/// verbatim *disk* path `\\?\C:\...`, never a verbatim UNC path, so the simple
/// prefix strip is sufficient here.)
fn sqlite_uri_path(path: &Path) -> String {
    let s = path.to_string_lossy();
    #[cfg(windows)]
    if let Some(stripped) = s.strip_prefix(r"\\?\") {
        return stripped.to_string();
    }
    s.into_owned()
}

/// Initialize SQLite storage for a managed ragged node and create its chunk
/// table. The ragged analog of [`crate::init_storage_npy`]: it places a
/// per-node `{key}.sqlite` file under `writable_root` (path components
/// validated to forbid traversal), creates the chunk table sized to
/// `structure`'s Awkward buffer keys, and returns the `sqlite://` `data_uri`
/// plus the `table_name`/`dataset_id` parameters the resolver later reads.
///
/// Mirrors Python `RaggedSQLAdapter.init_storage` → `SQLAdapter.init_storage`
/// (`tiled/adapters/ragged.py:128-151`, `sql.py`), with SQLite-per-node
/// storage standing in for Python's shared SQLStorage.
pub async fn init_storage_ragged_sql(
    writable_root: &Path,
    path_parts: &[String],
    structure: &RaggedStructure,
) -> Result<RaggedSqlInit> {
    if !writable_root.is_absolute() {
        return Err(TiledError::Internal(format!(
            "writable storage root {} is not absolute",
            writable_root.display()
        )));
    }
    if path_parts.is_empty() {
        return Err(TiledError::Validation(
            "init_storage: node path is empty".into(),
        ));
    }
    for part in path_parts {
        if part.is_empty()
            || part == "."
            || part == ".."
            || part.contains('/')
            || part.contains('\\')
            || part.contains('\0')
        {
            return Err(TiledError::Validation(format!(
                "init_storage: unsafe path component {part:?}"
            )));
        }
    }

    let (key, ancestors) = path_parts.split_last().expect("non-empty checked above");
    let mut dir = writable_root.to_path_buf();
    for a in ancestors {
        dir.push(a);
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| TiledError::Internal(format!("init_storage mkdir {}: {e}", dir.display())))?;
    let file = dir.join(format!("{key}.sqlite"));

    // `file` is under the absolute `writable_root`. `sqlite://` + that path is
    // the form sqlx's SqliteConnectOptions::from_str accepts (and the resolver
    // re-derives the path from for the writable-containment check):
    // `sqlite:///abs/...` on Unix, `sqlite://C:\...` on Windows. The path must
    // be rendered without the Windows extended-length `\\?\` verbatim prefix —
    // see `sqlite_uri_path` — because that prefix contains '?', the URL query
    // delimiter, and would otherwise truncate the data_uri.
    let data_uri = format!("sqlite://{}", sqlite_uri_path(&file));

    let adapter = RaggedSQLAdapter::new(
        data_uri.clone(),
        RAGGED_TABLE_NAME,
        RAGGED_DATASET_ID,
        structure.clone(),
        serde_json::Value::Null,
        Vec::new(),
    )?;
    adapter.init_storage().await?;

    let asset = Asset {
        data_uri: data_uri.clone(),
        is_directory: false,
        parameter: Some("data_uri".into()),
        num: None,
        id: None,
    };
    Ok(RaggedSqlInit {
        data_uri,
        table_name: RAGGED_TABLE_NAME.to_string(),
        dataset_id: RAGGED_DATASET_ID,
        assets: vec![asset],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(dir: &tempfile::TempDir) -> String {
        format!("sqlite://{}", dir.path().join("ragged.db").display())
    }

    fn keys() -> Vec<String> {
        vec!["node0-offsets".to_string(), "node1-data".to_string()]
    }

    fn chunk(offsets: &[u8], data: &[u8]) -> BufferMap {
        let mut m = BufferMap::new();
        m.insert("node0-offsets".to_string(), offsets.to_vec());
        m.insert("node1-data".to_string(), data.to_vec());
        m
    }

    #[tokio::test]
    async fn init_append_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();

        let c0 = chunk(&[0, 1, 2, 3], &[10, 20, 30]);
        store.append_chunk(0, &c0).await.unwrap();

        let loaded = store.load_chunks(&[]).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, 0);
        assert_eq!(loaded[0].1, c0, "buffer bytes must round-trip unchanged");
    }

    #[tokio::test]
    async fn load_filters_by_chunk_index() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();
        store.append_chunk(0, &chunk(&[0], &[1])).await.unwrap();
        store.append_chunk(1, &chunk(&[2], &[3])).await.unwrap();
        store.append_chunk(2, &chunk(&[4], &[5])).await.unwrap();

        let loaded = store.load_chunks(&[1]).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, 1);
        assert_eq!(loaded[0].1, chunk(&[2], &[3]));
    }

    #[tokio::test]
    async fn load_orders_by_chunk_index_ascending() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();
        // Insert out of order; load must return ascending.
        store.append_chunk(2, &chunk(&[4], &[5])).await.unwrap();
        store.append_chunk(0, &chunk(&[0], &[1])).await.unwrap();
        store.append_chunk(1, &chunk(&[2], &[3])).await.unwrap();

        let loaded = store.load_chunks(&[]).await.unwrap();
        let order: Vec<i64> = loaded.iter().map(|(ci, _)| *ci).collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn duplicate_chunk_index_is_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();
        store.append_chunk(0, &chunk(&[0], &[1])).await.unwrap();

        let err = store.append_chunk(0, &chunk(&[2], &[3])).await.unwrap_err();
        assert!(
            matches!(err, TiledError::Conflict(_)),
            "duplicate chunk_index must be a Conflict, got {err:?}"
        );
    }

    #[tokio::test]
    async fn dataset_id_scopes_rows() {
        let dir = tempfile::tempdir().unwrap();
        // Two datasets share one table file; loads must not cross over.
        let ds1 = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        let ds2 = RaggedSqlStore::new(url(&dir), "ragged_data", 2, keys()).unwrap();
        ds1.init_storage().await.unwrap();
        ds2.init_storage().await.unwrap(); // IF NOT EXISTS → no-op

        ds1.append_chunk(0, &chunk(&[0], &[11])).await.unwrap();
        ds2.append_chunk(0, &chunk(&[0], &[22])).await.unwrap();

        let l1 = ds1.load_chunks(&[]).await.unwrap();
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].1, chunk(&[0], &[11]));

        let l2 = ds2.load_chunks(&[]).await.unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].1, chunk(&[0], &[22]));
    }

    #[tokio::test]
    async fn empty_store_loads_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();
        let loaded = store.load_chunks(&[]).await.unwrap();
        assert!(loaded.is_empty());
    }

    #[tokio::test]
    async fn missing_buffer_key_is_validation_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = RaggedSqlStore::new(url(&dir), "ragged_data", 1, keys()).unwrap();
        store.init_storage().await.unwrap();
        let mut partial = BufferMap::new();
        partial.insert("node0-offsets".to_string(), vec![0]);
        let err = store.append_chunk(0, &partial).await.unwrap_err();
        assert!(
            matches!(err, TiledError::Validation(_)),
            "missing buffer key must be a Validation error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn unsafe_table_name_is_rejected() {
        let err = RaggedSqlStore::new("sqlite::memory:", "ragged\"; DROP TABLE x; --", 1, keys())
            .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    #[tokio::test]
    async fn unsafe_buffer_key_is_rejected() {
        let err = RaggedSqlStore::new(
            "sqlite::memory:",
            "ragged_data",
            1,
            vec!["node0\"; DROP".to_string()],
        )
        .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }
}

#[cfg(test)]
mod adapter_tests {
    use super::*;
    use tiled_core::dtype::{BuiltinDType, DType, Endianness, Kind};
    use tiled_core::ndslice::NDSlice;
    use tiled_core::structures::Resizable;

    fn url(dir: &tempfile::TempDir) -> String {
        format!("sqlite://{}", dir.path().join("ragged.db").display())
    }

    /// A `[N, None]` float64 ragged structure with the given axis-0 chunking.
    fn f64_structure(chunk_lengths: Vec<usize>, size: usize) -> RaggedStructure {
        let n: usize = chunk_lengths.iter().sum();
        RaggedStructure {
            data_type: DType::Builtin(BuiltinDType::new(Endianness::Little, Kind::Float, 8)),
            shape: vec![Some(n), None],
            size,
            chunks: vec![Some(chunk_lengths), None],
            dims: None,
            resizable: Resizable::default(),
        }
    }

    fn make(dir: &tempfile::TempDir, structure: RaggedStructure) -> RaggedSQLAdapter {
        RaggedSQLAdapter::new(
            url(dir),
            "ragged_data",
            1,
            structure,
            serde_json::json!({}),
            vec![],
        )
        .unwrap()
    }

    // boundary: write the whole array as one chunk, read it back unchanged.
    #[tokio::test]
    async fn write_full_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![3], 6));
        adapter.init_storage().await.unwrap();

        let data = serde_json::json!([[1.0, 2.0, 3.0], [4.0], [5.0, 6.0]]);
        adapter.write(&data).await.unwrap();

        let read = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(read.json_value, data);
    }

    // boundary: two separately-written blocks reconstruct in chunk order.
    #[tokio::test]
    async fn write_block_multi_chunk_then_read() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![2, 1], 6));
        adapter.init_storage().await.unwrap();

        adapter
            .write_block(&serde_json::json!([[1.0, 2.0], [3.0]]), 0)
            .await
            .unwrap();
        adapter
            .write_block(&serde_json::json!([[4.0, 5.0, 6.0]]), 1)
            .await
            .unwrap();

        let read = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(
            read.json_value,
            serde_json::json!([[1.0, 2.0], [3.0], [4.0, 5.0, 6.0]])
        );
    }

    // boundary: a partial slice is applied to the reconstructed array.
    #[tokio::test]
    async fn read_with_slice_selects_rows() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![3], 6));
        adapter.init_storage().await.unwrap();
        adapter
            .write(&serde_json::json!([[1.0, 2.0, 3.0], [4.0], [5.0, 6.0]]))
            .await
            .unwrap();

        let read = adapter
            .read(&NDSlice::from_numpy_str("0:2").unwrap())
            .await
            .unwrap();
        assert_eq!(read.json_value, serde_json::json!([[1.0, 2.0, 3.0], [4.0]]));
        assert_eq!(read.structure.shape, vec![Some(2), None]);
    }

    // boundary: patch(extend) on an empty array appends a chunk and grows shape,
    // chunks, and size; the grown structure reads back the appended rows.
    #[tokio::test]
    async fn patch_extend_appends_and_grows() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![], 0));
        adapter.init_storage().await.unwrap();

        let data = serde_json::json!([[1.0, 2.0], [3.0]]);
        let grown = adapter.patch(&data, &[0], true).await.unwrap();
        assert_eq!(grown.shape, vec![Some(2), None]);
        assert_eq!(grown.chunks, vec![Some(vec![2]), None]);
        assert_eq!(grown.size, 3);

        // Re-resolve with the grown structure (as the catalog would) and read.
        let reader = make(&dir, grown);
        let read = reader.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(read.json_value, data);
    }

    // boundary: a second patch appends at the next chunk index; full read sees
    // both chunks concatenated.
    #[tokio::test]
    async fn patch_extend_twice_concatenates() {
        let dir = tempfile::tempdir().unwrap();
        let a0 = make(&dir, f64_structure(vec![], 0));
        a0.init_storage().await.unwrap();

        let g1 = a0
            .patch(&serde_json::json!([[1.0, 2.0], [3.0]]), &[0], true)
            .await
            .unwrap();
        let a1 = make(&dir, g1);
        let g2 = a1
            .patch(&serde_json::json!([[4.0, 5.0, 6.0]]), &[2], true)
            .await
            .unwrap();
        assert_eq!(g2.shape, vec![Some(3), None]);
        assert_eq!(g2.chunks, vec![Some(vec![2, 1]), None]);
        assert_eq!(g2.size, 6);

        let reader = make(&dir, g2);
        let read = reader.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(
            read.json_value,
            serde_json::json!([[1.0, 2.0], [3.0], [4.0, 5.0, 6.0]])
        );
    }

    // boundary: extend=false (overwrite) is rejected — Python NotImplementedError.
    #[tokio::test]
    async fn patch_without_extend_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![], 0));
        adapter.init_storage().await.unwrap();
        let err = adapter
            .patch(&serde_json::json!([[1.0]]), &[0], false)
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    // boundary: an offset that does not start at the current length is rejected
    // (only leftmost append is supported).
    #[tokio::test]
    async fn patch_non_appending_offset_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        // shape0 == 0, so offset [5] is not an append at the end.
        let adapter = make(&dir, f64_structure(vec![], 0));
        adapter.init_storage().await.unwrap();
        let err = adapter
            .patch(&serde_json::json!([[1.0]]), &[5], true)
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    // boundary: a plain adapter hides its write face; into_writable() exposes it.
    #[tokio::test]
    async fn writability_gate_respects_flag() {
        let dir = tempfile::tempdir().unwrap();
        let read_only = make(&dir, f64_structure(vec![1], 1));
        assert!(
            read_only.as_writable().is_none(),
            "default adapter must be read-only"
        );
        let writable = make(&dir, f64_structure(vec![1], 1)).into_writable();
        assert!(
            writable.as_writable().is_some(),
            "into_writable must expose the write face"
        );
    }

    // boundary: init_storage_ragged_sql creates a per-node sqlite file + table
    // that an adapter can empty-read, write, and read back through.
    #[tokio::test]
    async fn init_storage_ragged_sql_creates_working_table() {
        let dir = tempfile::tempdir().unwrap();
        let structure = f64_structure(vec![2], 3);
        let init = init_storage_ragged_sql(dir.path(), &["mynode".to_string()], &structure)
            .await
            .unwrap();
        assert!(init.data_uri.starts_with("sqlite://"));
        assert_eq!(init.table_name, "ragged_data");
        assert_eq!(init.dataset_id, 1);
        assert_eq!(init.assets.len(), 1);
        assert!(
            dir.path().join("mynode.sqlite").exists(),
            "the per-node sqlite file must be created on disk"
        );

        let adapter = RaggedSQLAdapter::new(
            init.data_uri,
            init.table_name,
            init.dataset_id,
            structure,
            serde_json::json!({}),
            vec![],
        )
        .unwrap();
        // A brand-new table reads back as an empty array.
        let empty = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(empty.json_value, serde_json::json!([]));
        // Write then read round-trips through the created table.
        adapter
            .write(&serde_json::json!([[1.0, 2.0], [3.0]]))
            .await
            .unwrap();
        let read = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(read.json_value, serde_json::json!([[1.0, 2.0], [3.0]]));
    }

    // boundary: a *canonicalized* writable_root. On Windows std::fs::canonicalize
    // emits the extended-length verbatim form (\\?\C:\...) whose '?' is the URL
    // query delimiter, so the old format!("sqlite://{}", display()) produced a
    // data_uri sqlx rejected (managed ragged create → HTTP 500). The server
    // canonicalizes its writable-storage root, so this is the real boundary the
    // earlier raw-tempdir test did not cover. On Unix canonicalize adds no such
    // prefix, so this is a happy-path no-regression there.
    #[tokio::test]
    async fn init_storage_ragged_sql_works_with_canonicalized_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let structure = f64_structure(vec![2], 3);
        let init = init_storage_ragged_sql(&root, &["node".to_string()], &structure)
            .await
            .unwrap();
        // The data_uri must carry no '?' before the (optional) sqlx query, else
        // the path was truncated by the verbatim prefix.
        let after_scheme = init.data_uri.strip_prefix("sqlite://").unwrap();
        assert!(
            !after_scheme.contains('?'),
            "sqlite data_uri path must not contain '?': {}",
            init.data_uri
        );

        // The adapter built from that data_uri must connect, write, and read back.
        let adapter = RaggedSQLAdapter::new(
            init.data_uri,
            init.table_name,
            init.dataset_id,
            structure,
            serde_json::json!({}),
            vec![],
        )
        .unwrap();
        adapter
            .write(&serde_json::json!([[1.0, 2.0], [3.0]]))
            .await
            .unwrap();
        let read = adapter.read(&NDSlice::empty()).await.unwrap();
        assert_eq!(read.json_value, serde_json::json!([[1.0, 2.0], [3.0]]));
    }

    // sqlite_uri_path strips only the Windows verbatim prefix; everything else
    // is rendered verbatim (and contains no '?').
    #[test]
    fn sqlite_uri_path_strips_verbatim_prefix() {
        #[cfg(windows)]
        {
            assert_eq!(
                sqlite_uri_path(Path::new(r"\\?\C:\data\rag.sqlite")),
                r"C:\data\rag.sqlite"
            );
            // A non-verbatim Windows path is unchanged.
            assert_eq!(
                sqlite_uri_path(Path::new(r"C:\data\rag.sqlite")),
                r"C:\data\rag.sqlite"
            );
        }
        #[cfg(unix)]
        assert_eq!(
            sqlite_uri_path(Path::new("/data/rag.sqlite")),
            "/data/rag.sqlite"
        );
    }

    // boundary: nested path parts place the file under the ancestor dirs; a
    // traversal component is rejected.
    #[tokio::test]
    async fn init_storage_ragged_sql_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let structure = f64_structure(vec![1], 1);
        let err =
            init_storage_ragged_sql(dir.path(), &["..".to_string(), "x".to_string()], &structure)
                .await
                .unwrap_err();
        assert!(matches!(err, TiledError::Validation(_)));
    }

    // boundary: re-writing an existing block is a Conflict at the adapter level.
    #[tokio::test]
    async fn write_block_duplicate_is_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = make(&dir, f64_structure(vec![2], 3));
        adapter.init_storage().await.unwrap();
        adapter
            .write_block(&serde_json::json!([[1.0, 2.0], [3.0]]), 0)
            .await
            .unwrap();
        let err = adapter
            .write_block(&serde_json::json!([[4.0], [5.0]]), 0)
            .await
            .unwrap_err();
        assert!(matches!(err, TiledError::Conflict(_)));
    }
}
