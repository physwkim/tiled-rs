//! Parquet table adapter.
//!
//! Each Parquet row group becomes one partition so the existing
//! `/api/v1/table/partition/...` endpoint can stream them individually.

#![cfg(feature = "parquet")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use tiled_core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite};
use tiled_core::data_source::Asset;
use tiled_core::dtype::ArrowTable;
use tiled_core::error::{Result, TiledError};
use tiled_core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

pub struct ParquetAdapter {
    path: PathBuf,
    schema: SchemaRef,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Set only by the resolver (via [`ParquetAdapter::into_writable`]) when the
    /// file lives under the server's writable storage. Gates
    /// [`TableAdapterRead::as_table_writable`] so a read-only file can never be
    /// written through this adapter.
    writable: bool,
}

impl ParquetAdapter {
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let file = std::fs::File::open(&path)
            .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| TiledError::Internal(format!("parquet builder: {e}")))?;
        let schema = builder.schema().clone();
        // npartitions must equal exactly what `read_partition` can serve:
        // one partition per row group. A zero-row-group file (the
        // arrow/parquet `ArrowWriter` emits one when no rows are written, or
        // when only empty batches are written) therefore advertises 0
        // partitions — never a phantom partition 0 that `read_partition`
        // would reject as out of range (see `read_parquet_file` :104-109).
        let npartitions = builder.metadata().num_row_groups();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions,
            columns,
            resizable: Default::default(),
        };
        Ok(Self {
            path,
            schema,
            structure,
            metadata,
            specs: vec![Spec::new("parquet")],
            writable: false,
        })
    }

    /// Mark this file-backed adapter as writable. The leaf resolver calls this
    /// only when the backing path is contained in the server's writable
    /// storage, so the write invariant holds by construction:
    /// `as_table_writable().is_some()` ⟹ the file is under writable storage.
    pub fn into_writable(mut self) -> Self {
        self.writable = true;
        self
    }
}

impl BaseAdapter for ParquetAdapter {
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

impl TableAdapterRead for ParquetAdapter {
    fn structure(&self) -> &TableStructure {
        &self.structure
    }
    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>> {
        let path = self.path.clone();
        let schema = self.schema.clone();
        let fields = fields.map(<[String]>::to_vec);
        Box::pin(async move {
            let batches = tokio::task::spawn_blocking(move || read_parquet_file(path, None))
                .await
                .map_err(|e| TiledError::Internal(format!("parquet spawn: {e}")))??;
            project(&schema, &batches, fields.as_deref())
        })
    }
    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>> {
        let path = self.path.clone();
        let schema = self.schema.clone();
        let fields = fields.map(<[String]>::to_vec);
        Box::pin(async move {
            let batches =
                tokio::task::spawn_blocking(move || read_parquet_file(path, Some(partition)))
                    .await
                    .map_err(|e| TiledError::Internal(format!("parquet spawn: {e}")))??;
            project(&schema, &batches, fields.as_deref())
        })
    }

    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        if self.writable { Some(self) } else { None }
    }
}

impl TableAdapterWrite for ParquetAdapter {
    fn write<'a>(&'a self, data: ArrowTable) -> BoxFuture<'a, Result<()>> {
        // Overwrite the whole file with `data`. The managed parquet table is
        // single-partition (one row group), so this is the only granularity.
        let path = self.path.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || write_parquet_atomic(&path, &data))
                .await
                .map_err(|e| TiledError::Internal(format!("parquet write spawn: {e}")))?
        })
    }

    fn write_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if partition != 0 {
                return Err(TiledError::Validation(format!(
                    "managed parquet table is single-partition; cannot write partition {partition}"
                )));
            }
            self.write(data).await
        })
    }
}

/// Per-process counter making temp filenames unique within this process
/// (paired with the PID for cross-process uniqueness).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write `table` to `path` atomically as a single-row-group parquet file: write
/// a uniquely-named sibling temp file via `parquet::arrow::ArrowWriter`, then
/// rename it over `path` (same-directory rename is atomic on POSIX/Windows). A
/// crash mid-write leaves the previous file intact.
fn write_parquet_atomic(path: &Path, table: &ArrowTable) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        TiledError::Internal(format!("parquet path {} has no parent dir", path.display()))
    })?;
    let stem = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("table.parquet");
    let pid = std::process::id();
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{stem}.{pid}.{n}.parquettmp"));
    {
        let f = std::fs::File::create(&tmp)
            .map_err(|e| TiledError::Internal(format!("create {}: {e}", tmp.display())))?;
        let mut writer = ArrowWriter::try_new(f, table.schema.clone(), None)
            .map_err(|e| TiledError::Internal(format!("parquet writer: {e}")))?;
        for b in &table.batches {
            writer
                .write(b)
                .map_err(|e| TiledError::Internal(format!("parquet write: {e}")))?;
        }
        writer
            .close()
            .map_err(|e| TiledError::Internal(format!("parquet close: {e}")))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        TiledError::Internal(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;
    Ok(())
}

/// Create a managed parquet storage skeleton under `writable_root` and return
/// its `file://` data URI plus the single backing asset. Mirrors
/// [`crate::init_storage_csv`]: rejects unsafe path components, lays the file at
/// `<root>/<ancestors>/<key>.parquet`, and writes an empty (zero-row-group)
/// parquet whose schema is a placeholder built from the declared column names
/// (all nullable Utf8). A later `PUT /table/full` overwrites it with the typed
/// data; the read adapter takes its schema from whatever was written.
pub fn init_storage_parquet(
    writable_root: &Path,
    path_parts: &[String],
    structure: &TableStructure,
) -> Result<(String, Vec<Asset>)> {
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
    let file = dir.join(format!("{key}.parquet"));
    std::fs::create_dir_all(&dir)
        .map_err(|e| TiledError::Internal(format!("init_storage mkdir {}: {e}", dir.display())))?;
    // Placeholder schema from the declared column names; the first write
    // establishes the real column types.
    let fields: Vec<Field> = structure
        .columns
        .iter()
        .map(|c| Field::new(c, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let f = std::fs::File::create(&file).map_err(|e| {
        TiledError::Internal(format!("init_storage create {}: {e}", file.display()))
    })?;
    let writer = ArrowWriter::try_new(f, schema, None)
        .map_err(|e| TiledError::Internal(format!("init_storage parquet writer: {e}")))?;
    writer
        .close()
        .map_err(|e| TiledError::Internal(format!("init_storage parquet close: {e}")))?;
    let data_uri = format!("file://{}", file.display());
    let asset = Asset {
        data_uri: data_uri.clone(),
        is_directory: false,
        parameter: Some("data_uri".into()),
        num: None,
        id: None,
    };
    Ok((data_uri, vec![asset]))
}

fn read_parquet_file(path: PathBuf, partition: Option<usize>) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(&path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| TiledError::Internal(format!("parquet builder: {e}")))?;
    if let Some(p) = partition {
        let n = builder.metadata().num_row_groups();
        if p >= n {
            return Err(TiledError::Validation(format!(
                "partition {p} out of range ({n} groups)"
            )));
        }
        builder = builder.with_row_groups(vec![p]);
    }
    let reader = builder
        .build()
        .map_err(|e| TiledError::Internal(format!("parquet build: {e}")))?;
    let mut batches = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| TiledError::Internal(format!("parquet read: {e}")))?);
    }
    Ok(batches)
}

fn project(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    fields: Option<&[String]>,
) -> Result<ArrowTable> {
    let Some(cols) = fields else {
        return Ok(ArrowTable {
            schema: schema.clone(),
            batches: batches.to_vec(),
        });
    };
    let indices: Vec<usize> = cols
        .iter()
        .map(|name| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| TiledError::Validation(format!("unknown column: {name}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let projected_schema = Arc::new(
        schema
            .project(&indices)
            .map_err(|e| TiledError::Internal(format!("project schema: {e}")))?,
    );
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        out.push(
            b.project(&indices)
                .map_err(|e| TiledError::Internal(format!("project batch: {e}")))?,
        );
    }
    Ok(ArrowTable {
        schema: projected_schema,
        batches: out,
    })
}

fn encode_schema(schema: &arrow::datatypes::Schema) -> String {
    use base64::Engine;
    let buf = arrow::ipc::convert::IpcSchemaEncoder::new()
        .schema_to_fb(schema)
        .finished_data()
        .to_vec();
    let b64 = base64::engine::general_purpose::STANDARD.encode(buf);
    format!("{B64_ENCODED_PREFIX}{b64}")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use tiled_core::adapters::TableAdapterRead;
    use tiled_core::dtype::ArrowTable;
    use tiled_core::error::TiledError;
    use tiled_core::structures::TableStructure;

    use super::{ParquetAdapter, init_storage_parquet};

    fn one_col_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    /// Write a parquet file with the given batches, return its path (kept
    /// alive by the returned `TempDir`).
    fn write_parquet(batches: &[RecordBatch], schema: Arc<Schema>) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
        (dir, path)
    }

    /// A parquet file written with no batches has zero row groups; the
    /// adapter must advertise `npartitions == 0`, matching what
    /// `read_partition` can serve — not a phantom partition 0.
    #[tokio::test]
    async fn zero_row_group_file_reports_zero_partitions() {
        let (_dir, path) = write_parquet(&[], one_col_schema());

        let adapter = ParquetAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(
            adapter.structure().npartitions,
            0,
            "zero-row-group file must advertise 0 partitions, not a phantom 1"
        );

        // read_partition(0) must reject — there is no partition 0 to serve.
        let err = adapter.read_partition(0, None).await.unwrap_err();
        assert!(
            matches!(err, TiledError::Validation(_)),
            "read_partition(0) on a 0-partition file should be a Validation error, got {err:?}"
        );

        // A full read still succeeds and yields no row batches.
        let table = adapter.read(None).await.unwrap();
        assert!(table.batches.is_empty());
    }

    /// A file with one real row group still reports one partition that
    /// `read_partition(0)` can serve — guards against over-correcting the
    /// zero-row-group fix.
    #[tokio::test]
    async fn one_row_group_file_reports_one_readable_partition() {
        let schema = one_col_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let (_dir, path) = write_parquet(&[batch], schema);

        let adapter = ParquetAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().npartitions, 1);

        let table = adapter.read_partition(0, None).await.unwrap();
        let rows: usize = table.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
    }

    /// `init_storage_parquet` lays an empty (zero-row-group) skeleton; a fresh
    /// adapter reads the declared columns and zero rows, only an `into_writable`
    /// adapter exposes a writer, and a write round-trips through a fresh read.
    #[tokio::test]
    async fn init_storage_skeleton_then_write_roundtrip_gated_by_into_writable() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["a".into()],
            resizable: Default::default(),
        };
        let (_uri, assets) = init_storage_parquet(&root_abs, &["t".into()], &structure).unwrap();
        let file = root_abs.join("t.parquet");
        assert!(file.is_file(), "skeleton file not created");

        // Read-only adapter: declared columns, zero rows, no writer.
        let ro = ParquetAdapter::from_path(file.clone(), serde_json::json!({})).unwrap();
        assert_eq!(ro.structure().columns, vec!["a"]);
        assert_eq!(
            ro.structure().npartitions,
            0,
            "empty skeleton has 0 row groups"
        );
        assert!(
            ro.as_table_writable().is_none(),
            "a non-writable adapter must not expose a writer"
        );

        // Writable adapter persists the whole table (typed Int64).
        let schema = one_col_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let rw = ParquetAdapter::from_path(file.clone(), serde_json::json!({}))
            .unwrap()
            .into_writable();
        let w = rw
            .as_table_writable()
            .expect("into_writable exposes a writer");
        w.write(ArrowTable {
            schema,
            batches: vec![batch],
        })
        .await
        .unwrap();

        // A fresh read reflects the written rows + the real (Int64) schema.
        let back = ParquetAdapter::from_path(file, serde_json::json!({})).unwrap();
        assert_eq!(back.structure().npartitions, 1);
        let table = back.read(None).await.unwrap();
        let rows: usize = table.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
        assert_eq!(table.schema.field(0).data_type(), &DataType::Int64);

        assert_eq!(assets.len(), 1);
        assert!(assets[0].data_uri.ends_with("/t.parquet"));
        assert!(!assets[0].is_directory);
    }

    /// `write_partition` is single-partition: partition 0 writes the file,
    /// anything else is rejected.
    #[tokio::test]
    async fn write_partition_only_accepts_partition_zero() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["a".into()],
            resizable: Default::default(),
        };
        init_storage_parquet(&root_abs, &["t".into()], &structure).unwrap();
        let file = root_abs.join("t.parquet");
        let rw = ParquetAdapter::from_path(file, serde_json::json!({}))
            .unwrap()
            .into_writable();
        let w = rw.as_table_writable().unwrap();
        let schema = one_col_schema();
        let mk = || ArrowTable {
            schema: schema.clone(),
            batches: vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap(),
            ],
        };
        assert!(w.write_partition(mk(), 0).await.is_ok());
        assert!(w.write_partition(mk(), 1).await.is_err());
    }
}
