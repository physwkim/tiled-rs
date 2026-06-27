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

use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use tiled_core::error::{Result, TiledError};
use tiled_serialization::ragged::BufferMap;

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
