//! SQL table read adapter.
//!
//! Reads a table from a SQLite database and serves it as a table family.
//! Corresponds to `tiled/adapters/sql.py:SQLAdapter` (read path).
//!
//! Scope: SQLite only. PostgreSQL requires a live server and is not tested
//! here; the same crate dep would work but is gated by `postgres` sqlx feature.
//!
//! Schema inference uses `PRAGMA table_info` (SQLite type affinity):
//!   INTEGER → Int64, REAL → Float64, TEXT → Utf8, BLOB → LargeBinary,
//!   NUMERIC → Float64.  Columns with the tiled-internal prefix `_` are
//!   excluded from the output schema (they carry `_dataset_id`,
//!   `_partition_id` etc.).
//!
//! Read-only.

#![cfg(feature = "sql-adapter")]

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqlx::{Row, SqlitePool};

use crate::core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite};
use crate::core::dtype::ArrowTable;
use crate::core::error::{Result, TiledError};
use crate::core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

/// Read adapter for a single SQL table.
///
/// `dataset_id`: when `Some(id)`, limits rows to those where
/// `_dataset_id = id` (matches the tiled SQLAdapter internal layout).
/// When `None`, reads the entire table.
#[derive(Debug)]
pub struct SqlTableAdapter {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl SqlTableAdapter {
    /// Connect to a SQLite `database_url`, read `table_name` (optionally
    /// filtered by `dataset_id`), and cache all rows.
    pub async fn from_sqlite(
        database_url: &str,
        table_name: &str,
        dataset_id: Option<i64>,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        let pool = SqlitePool::connect(database_url)
            .await
            .map_err(|e| TiledError::Internal(format!("sqlite connect {database_url}: {e}")))?;
        let result = Self::load(&pool, table_name, dataset_id, metadata).await;
        pool.close().await;
        result
    }

    async fn load(
        pool: &SqlitePool,
        table_name: &str,
        dataset_id: Option<i64>,
        metadata: serde_json::Value,
    ) -> Result<Self> {
        validate_identifier(table_name)?;

        // Infer schema via PRAGMA.
        let schema = infer_schema(pool, table_name).await?;

        // Build SELECT list (only user-visible columns — no tiled internal ones).
        let col_list: String = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let query = if let Some(id) = dataset_id {
            format!("SELECT {col_list} FROM \"{table_name}\" WHERE _dataset_id = {id}")
        } else {
            format!("SELECT {col_list} FROM \"{table_name}\"")
        };

        let rows = sqlx::query(&query)
            .fetch_all(pool)
            .await
            .map_err(|e| TiledError::Internal(format!("sql query: {e}")))?;

        let batches = build_batches(&schema, &rows)?;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(&schema),
            npartitions: 1,
            columns,
            resizable: Default::default(),
        };
        Ok(Self {
            schema,
            batches,
            structure,
            metadata,
            specs: vec![Spec::new("sql")],
        })
    }
}

impl BaseAdapter for SqlTableAdapter {
    fn structure_family(&self) -> StructureFamily {
        StructureFamily::Table
    }
    fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }
    fn specs(&self) -> &[Spec] {
        &self.specs
    }
}

impl TableAdapterRead for SqlTableAdapter {
    fn structure(&self) -> &TableStructure {
        &self.structure
    }

    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move { project(&self.schema, &self.batches, fields) })
    }

    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move {
            if partition != 0 {
                return Err(TiledError::Validation(format!(
                    "sql adapter has 1 partition; got {partition}"
                )));
            }
            project(&self.schema, &self.batches, fields)
        })
    }

    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        None
    }
}

/// Infer an Arrow schema from `PRAGMA table_info(table_name)`.
/// Columns whose names start with `_` (tiled-internal) are excluded.
async fn infer_schema(pool: &SqlitePool, table_name: &str) -> Result<SchemaRef> {
    let pragma = format!("PRAGMA table_info(\"{table_name}\")");
    let rows = sqlx::query(&pragma)
        .fetch_all(pool)
        .await
        .map_err(|e| TiledError::Internal(format!("PRAGMA table_info: {e}")))?;

    if rows.is_empty() {
        return Err(TiledError::Validation(format!(
            "table \"{table_name}\" not found or has no columns"
        )));
    }

    let mut fields = Vec::new();
    for row in &rows {
        let name: &str = row
            .try_get("name")
            .map_err(|e| TiledError::Internal(format!("pragma name col: {e}")))?;
        // Skip tiled-internal columns.
        if name.starts_with('_') {
            continue;
        }
        let col_type: &str = row
            .try_get("type")
            .map_err(|e| TiledError::Internal(format!("pragma type col: {e}")))?;
        let notnull: i64 = row
            .try_get("notnull")
            .map_err(|e| TiledError::Internal(format!("pragma notnull col: {e}")))?;
        let dt = sqlite_affinity_to_arrow(col_type);
        fields.push(Field::new(name, dt, notnull == 0));
    }

    if fields.is_empty() {
        return Err(TiledError::Validation(format!(
            "table \"{table_name}\" has no user-visible columns (all start with '_')"
        )));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// Map a SQLite column type string to an Arrow DataType via affinity rules.
fn sqlite_affinity_to_arrow(col_type: &str) -> DataType {
    let up = col_type.to_uppercase();
    if up.contains("INT") {
        DataType::Int64
    } else if up.contains("REAL")
        || up.contains("FLOAT")
        || up.contains("DOUBLE")
        || up.contains("NUMERIC")
        || up.contains("DECIMAL")
    {
        DataType::Float64
    } else if up.contains("TEXT")
        || up.contains("CHAR")
        || up.contains("CLOB")
        || up.contains("VARCHAR")
        || up.is_empty()
    {
        DataType::Utf8
    } else if up.contains("BLOB") || up.contains("BINARY") {
        DataType::LargeBinary
    } else {
        // Unknown affinity → text (SQLite's own fallback rule)
        DataType::Utf8
    }
}

/// Build Arrow RecordBatch(es) from sqlx rows according to `schema`.
fn build_batches(schema: &SchemaRef, rows: &[sqlx::sqlite::SqliteRow]) -> Result<Vec<RecordBatch>> {
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(schema.clone());
        return Ok(vec![batch]);
    }

    let n = rows.len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let name = field.name().as_str();
        let col: ArrayRef = match field.data_type() {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(n);
                for row in rows {
                    match row.try_get::<Option<i64>, _>(name) {
                        Ok(Some(v)) => b.append_value(v),
                        Ok(None) => b.append_null(),
                        Err(_) => {
                            // Try as f64 and truncate (SQLite stores ints as reals sometimes)
                            match row.try_get::<Option<f64>, _>(name) {
                                Ok(Some(v)) => b.append_value(v as i64),
                                _ => b.append_null(),
                            }
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(n);
                for row in rows {
                    match row.try_get::<Option<f64>, _>(name) {
                        Ok(Some(v)) => b.append_value(v),
                        Ok(None) => b.append_null(),
                        Err(_) => match row.try_get::<Option<i64>, _>(name) {
                            Ok(Some(v)) => b.append_value(v as f64),
                            _ => b.append_null(),
                        },
                    }
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary => {
                let mut b = BinaryBuilder::with_capacity(n, n * 8);
                for row in rows {
                    match row.try_get::<Option<Vec<u8>>, _>(name) {
                        Ok(Some(v)) => b.append_value(&v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                // Utf8 and anything else
                let mut b = StringBuilder::with_capacity(n, n * 16);
                for row in rows {
                    match row.try_get::<Option<String>, _>(name) {
                        Ok(Some(v)) => b.append_value(&v),
                        Ok(None) => b.append_null(),
                        Err(_) => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        cols.push(col);
    }

    let batch = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| TiledError::Internal(format!("build batch: {e}")))?;
    Ok(vec![batch])
}

fn project(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    fields: Option<&[String]>,
) -> Result<ArrowTable> {
    let (out_schema, out_batches) = if let Some(names) = fields {
        let indices: Vec<usize> = names
            .iter()
            .map(|n| {
                schema
                    .index_of(n)
                    .map_err(|_| TiledError::Validation(format!("unknown field: {n}")))
            })
            .collect::<Result<_>>()?;
        let proj_schema = Arc::new(
            schema
                .project(&indices)
                .map_err(|e| TiledError::Internal(format!("project schema: {e}")))?,
        );
        let proj_batches: Vec<RecordBatch> = batches
            .iter()
            .map(|b| {
                b.project(&indices)
                    .map_err(|e| TiledError::Internal(format!("project batch: {e}")))
            })
            .collect::<Result<_>>()?;
        (proj_schema, proj_batches)
    } else {
        (schema.clone(), batches.to_vec())
    };
    Ok(ArrowTable {
        schema: out_schema,
        batches: out_batches,
    })
}

fn encode_schema(schema: &Schema) -> String {
    use base64::Engine;
    let buf = arrow::ipc::convert::IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec();
    let b64 = base64::engine::general_purpose::STANDARD.encode(buf);
    format!("{B64_ENCODED_PREFIX}{b64}")
}

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
    use crate::core::adapters::{BaseAdapter, TableAdapterRead};
    use crate::core::structures::StructureFamily;

    use super::SqlTableAdapter;

    const DB_URL: &str = "sqlite::memory:";

    async fn make_pool() -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect(DB_URL).await.unwrap()
    }

    async fn seed_pool(pool: &sqlx::SqlitePool) {
        sqlx::query(
            "CREATE TABLE measurements (id INTEGER NOT NULL, temperature REAL NOT NULL, label TEXT)",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO measurements (id, temperature, label) VALUES (1, 20.5, 'alpha'), (2, 21.0, 'beta'), (3, 19.8, NULL)",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reads_all_rows_and_columns() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        assert_eq!(adapter.structure().npartitions, 1);
        assert_eq!(
            adapter.structure().columns,
            vec!["id", "temperature", "label"]
        );
        let table = adapter.read(None).await.unwrap();
        let total: usize = table.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn column_projection() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        let table = adapter
            .read(Some(&["id".to_string(), "label".to_string()]))
            .await
            .unwrap();
        assert_eq!(table.schema.fields().len(), 2);
        assert_eq!(table.schema.field(0).name(), "id");
        assert_eq!(table.schema.field(1).name(), "label");
    }

    #[tokio::test]
    async fn unknown_column_is_error() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        let err = adapter.read(Some(&["nonexistent".to_string()])).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn out_of_range_partition_is_error() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        let err = adapter.read_partition(1, None).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn is_read_only() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        assert!(adapter.as_table_writable().is_none());
    }

    #[tokio::test]
    async fn structure_family_is_table() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        assert_eq!(adapter.structure_family(), StructureFamily::Table);
    }

    #[tokio::test]
    async fn spec_name_is_sql() {
        let pool = make_pool().await;
        seed_pool(&pool).await;
        let adapter = SqlTableAdapter::load(&pool, "measurements", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        assert_eq!(adapter.specs()[0].name, "sql");
    }

    #[tokio::test]
    async fn dataset_id_filter() {
        let pool = make_pool().await;
        // Tiled-internal layout: table has _dataset_id column.
        sqlx::query("CREATE TABLE tiled_data (_dataset_id INTEGER, x REAL, label TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tiled_data VALUES (1, 1.0, 'a'), (2, 2.0, 'b'), (1, 3.0, 'c')")
            .execute(&pool)
            .await
            .unwrap();
        let adapter = SqlTableAdapter::load(&pool, "tiled_data", Some(1), serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        // Only columns NOT starting with '_' are in schema.
        assert_eq!(adapter.structure().columns, vec!["x", "label"]);
        let table = adapter.read(None).await.unwrap();
        let total: usize = table.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // dataset_id=1 has rows 1 and 3
    }

    #[tokio::test]
    async fn empty_table_gives_zero_rows() {
        let pool = make_pool().await;
        sqlx::query("CREATE TABLE empty_table (a INTEGER, b TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        let adapter = SqlTableAdapter::load(&pool, "empty_table", None, serde_json::Value::Null)
            .await
            .unwrap();
        pool.close().await;
        let table = adapter.read(None).await.unwrap();
        let total: usize = table.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn missing_table_is_error() {
        let pool = make_pool().await;
        let err =
            SqlTableAdapter::load(&pool, "no_such_table", None, serde_json::Value::Null).await;
        pool.close().await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn unsafe_table_name_is_error() {
        let pool = make_pool().await;
        let err = SqlTableAdapter::load(
            &pool,
            "table\"; DROP TABLE measurements; --",
            None,
            serde_json::Value::Null,
        )
        .await;
        pool.close().await;
        assert!(err.is_err());
    }
}
