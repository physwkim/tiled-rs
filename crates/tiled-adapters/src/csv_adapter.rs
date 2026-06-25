//! CSV table adapter.
//!
//! Reads a CSV file into Arrow `RecordBatch`es using `arrow::csv::Reader`.
//! Schema is inferred from the first ~64 rows. Single partition only —
//! large multi-GB CSVs should use parquet/h5 instead.

#![cfg(feature = "csv-adapter")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::SchemaRef;

use tiled_core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite};
use tiled_core::data_source::Asset;
use tiled_core::dtype::ArrowTable;
use tiled_core::error::{Result, TiledError};
use tiled_core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

pub struct CsvAdapter {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
    /// Backing file path, kept so a writable adapter can overwrite it.
    path: PathBuf,
    /// Set only by the resolver (via [`CsvAdapter::into_writable`]) when the
    /// file lives under the server's writable storage. Gates
    /// [`TableAdapterRead::as_table_writable`] so a read-only file can never be
    /// written through this adapter.
    writable: bool,
}

impl CsvAdapter {
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        let file = std::fs::File::open(&path)
            .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
        let format = arrow::csv::reader::Format::default().with_header(true);
        let mut header_reader = std::io::BufReader::new(
            std::fs::File::open(&path)
                .map_err(|e| TiledError::Internal(format!("reopen {}: {e}", path.display())))?,
        );
        let (schema, _) = format
            .infer_schema(&mut header_reader, Some(64))
            .map_err(|e| TiledError::Internal(format!("infer schema: {e}")))?;
        let schema = Arc::new(schema);
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(file)
            .map_err(|e| TiledError::Internal(format!("csv build: {e}")))?;
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| TiledError::Internal(format!("csv read: {e}")))?);
        }
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions: 1,
            columns,
            resizable: Default::default(),
        };
        Ok(Self {
            schema,
            batches,
            structure,
            metadata,
            specs: vec![Spec::new("csv")],
            path,
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

impl BaseAdapter for CsvAdapter {
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

impl TableAdapterRead for CsvAdapter {
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
                    "csv adapter has 1 partition; got {partition}"
                )));
            }
            project(&self.schema, &self.batches, fields)
        })
    }

    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        if self.writable { Some(self) } else { None }
    }
}

impl TableAdapterWrite for CsvAdapter {
    fn write<'a>(&'a self, data: ArrowTable) -> BoxFuture<'a, Result<()>> {
        // The whole CSV file is overwritten with `data`. CSV is single-file /
        // single-partition, so this is the only write granularity.
        let path = self.path.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || write_csv_atomic(&path, &data))
                .await
                .map_err(|e| TiledError::Internal(format!("csv write spawn: {e}")))?
        })
    }

    fn write_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // One partition: partition 0 is the whole file; anything else has
            // no backing row group.
            if partition != 0 {
                return Err(TiledError::Validation(format!(
                    "csv adapter has 1 partition; cannot write partition {partition}"
                )));
            }
            self.write(data).await
        })
    }

    fn append_partition<'a>(
        &'a self,
        data: ArrowTable,
        partition: usize,
    ) -> BoxFuture<'a, Result<()>> {
        let path = self.path.clone();
        Box::pin(async move {
            if partition != 0 {
                return Err(TiledError::Validation(format!(
                    "csv adapter has 1 partition; cannot append to partition {partition}"
                )));
            }
            tokio::task::spawn_blocking(move || append_csv(&path, data))
                .await
                .map_err(|e| TiledError::Internal(format!("csv append spawn: {e}")))?
        })
    }
}

/// Per-process counter making temp filenames unique within this process
/// (paired with the PID for cross-process uniqueness).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write `table` to `path` atomically as RFC 4180 CSV (header + rows): stream
/// the batches into a uniquely-named sibling temp file via
/// `arrow::csv::Writer`, then rename it over `path` (same-directory rename is
/// atomic on POSIX/Windows). A crash mid-write leaves the previous file intact.
fn write_csv_atomic(path: &Path, table: &ArrowTable) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        TiledError::Internal(format!("csv path {} has no parent dir", path.display()))
    })?;
    let stem = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("table.csv");
    let pid = std::process::id();
    let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".{stem}.{pid}.{n}.csvtmp"));
    {
        let f = std::fs::File::create(&tmp)
            .map_err(|e| TiledError::Internal(format!("create {}: {e}", tmp.display())))?;
        let mut writer = arrow::csv::WriterBuilder::new().with_header(true).build(f);
        for b in &table.batches {
            writer
                .write(b)
                .map_err(|e| TiledError::Internal(format!("csv write: {e}")))?;
        }
        // `writer` (and the file it owns) is dropped here, flushing before the
        // rename below.
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

/// Read `path` as a CSV using `schema` (from the incoming Arrow IPC batch),
/// concat with `data`, then atomically write the result back. Reading with
/// the incoming schema ensures all batches share the same column types so
/// the combined write is schema-consistent. If the file is empty or has only
/// a header with no data rows, the existing content is treated as empty.
fn append_csv(path: &Path, data: ArrowTable) -> Result<()> {
    let existing_batches: Vec<RecordBatch> = if path.exists() {
        let file = std::fs::File::open(path)
            .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
        let reader = ReaderBuilder::new(data.schema.clone())
            .with_header(true)
            .build(file)
            .map_err(|e| TiledError::Internal(format!("csv build: {e}")))?;
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(|e| TiledError::Internal(format!("csv read: {e}")))?);
        }
        batches
    } else {
        Vec::new()
    };
    let all_batches: Vec<RecordBatch> = existing_batches.into_iter().chain(data.batches).collect();
    let combined = ArrowTable {
        schema: data.schema,
        batches: all_batches,
    };
    write_csv_atomic(path, &combined)
}

/// Create a managed CSV storage skeleton under `writable_root` and return its
/// `file://` data URI plus the single backing asset. Mirrors
/// [`crate::init_storage_npy`]: rejects unsafe path components, lays the file at
/// `<root>/<ancestors>/<key>.csv`, and writes a header-only file (column names,
/// no data rows) so a later `PUT /table/full` supplies the typed data. The read
/// adapter re-infers the schema from whatever was written.
pub fn init_storage_csv(
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
    let file = dir.join(format!("{key}.csv"));
    std::fs::create_dir_all(&dir)
        .map_err(|e| TiledError::Internal(format!("init_storage mkdir {}: {e}", dir.display())))?;
    // Header-only skeleton from the declared column names.
    let header = format!("{}\n", structure.columns.join(","));
    std::fs::write(&file, header)
        .map_err(|e| TiledError::Internal(format!("init_storage write {}: {e}", file.display())))?;
    // `file` is under `writable_root` (absolute), so `display()` starts with
    // `/` and `file://` + that path yields the `file:///abs/...` form the read
    // resolver's `uri_to_path` expects.
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
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn read_simple_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "x,y").unwrap();
        writeln!(f, "1,a").unwrap();
        writeln!(f, "2,b").unwrap();
        let adapter = CsvAdapter::from_path(path, serde_json::json!({})).unwrap();
        let s = adapter.structure();
        assert_eq!(s.columns, vec!["x", "y"]);
        assert_eq!(s.npartitions, 1);
        let table = adapter
            .read_partition(0, Some(&["x".into()]))
            .await
            .unwrap();
        assert_eq!(table.schema.fields().len(), 1);
        assert_eq!(table.batches[0].num_rows(), 2);
    }

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn xy_table(xs: Vec<i64>, ys: Vec<&str>) -> ArrowTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(xs)),
                Arc::new(StringArray::from(ys)),
            ],
        )
        .unwrap();
        ArrowTable {
            schema,
            batches: vec![batch],
        }
    }

    /// `init_storage_csv` lays a header-only skeleton; a fresh adapter reads the
    /// declared columns and zero rows, and only an `into_writable` adapter
    /// exposes a writer.
    #[tokio::test]
    async fn init_storage_skeleton_then_write_roundtrip_gated_by_into_writable() {
        let root = tempfile::tempdir().unwrap();
        let root_abs = root.path().canonicalize().unwrap();
        let structure = TableStructure {
            arrow_schema: String::new(),
            npartitions: 1,
            columns: vec!["x".into(), "y".into()],
            resizable: Default::default(),
        };
        let (_uri, assets) = init_storage_csv(&root_abs, &["t".into()], &structure).unwrap();
        let file = root_abs.join("t.csv");
        assert!(file.is_file(), "skeleton file not created");

        // Read-only adapter: correct columns, zero rows, no writer.
        let ro = CsvAdapter::from_path(file.clone(), serde_json::json!({})).unwrap();
        assert_eq!(ro.structure().columns, vec!["x", "y"]);
        assert_eq!(ro.read(None).await.unwrap().num_rows(), 0);
        assert!(
            ro.as_table_writable().is_none(),
            "a non-writable adapter must not expose a writer"
        );

        // Writable adapter persists the whole table.
        let rw = CsvAdapter::from_path(file.clone(), serde_json::json!({}))
            .unwrap()
            .into_writable();
        let w = rw
            .as_table_writable()
            .expect("into_writable exposes a writer");
        w.write(xy_table(vec![1, 2, 3], vec!["a", "b", "c"]))
            .await
            .unwrap();

        // A fresh read reflects the written rows.
        let back = CsvAdapter::from_path(file, serde_json::json!({})).unwrap();
        assert_eq!(back.structure().columns, vec!["x", "y"]);
        let table = back.read(None).await.unwrap();
        assert_eq!(table.num_rows(), 3);
        let xs = table.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(xs.values(), &[1, 2, 3]);

        // The single asset points at the file under writable storage.
        assert_eq!(assets.len(), 1);
        assert!(assets[0].data_uri.ends_with("/t.csv"));
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
            columns: vec!["x".into(), "y".into()],
            resizable: Default::default(),
        };
        init_storage_csv(&root_abs, &["t".into()], &structure).unwrap();
        let file = root_abs.join("t.csv");
        let rw = CsvAdapter::from_path(file, serde_json::json!({}))
            .unwrap()
            .into_writable();
        let w = rw.as_table_writable().unwrap();
        assert!(
            w.write_partition(xy_table(vec![1], vec!["a"]), 0)
                .await
                .is_ok()
        );
        assert!(
            w.write_partition(xy_table(vec![1], vec!["a"]), 1)
                .await
                .is_err()
        );
    }
}
