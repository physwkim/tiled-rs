//! Arrow IPC table adapter.
//!
//! Serves Arrow IPC files (`.arrow` / `.feather`) as a table family.  Multiple
//! files map to multiple partitions — one file per partition — matching the
//! Python `ArrowAdapter` which accepts a `data_uris` list.
//!
//! Read-only: external Arrow files are served but not mutated through this
//! adapter.  A managed writable Arrow table is outside scope; write through
//! the parquet adapter instead.

#![cfg(feature = "arrow-ipc")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;

use crate::core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead, TableAdapterWrite};
use crate::core::dtype::ArrowTable;
use crate::core::error::{Result, TiledError};
use crate::core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

#[derive(Debug)]
pub struct ArrowIpcAdapter {
    paths: Vec<PathBuf>,
    schema: SchemaRef,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
}

impl ArrowIpcAdapter {
    /// Construct from a list of Arrow IPC file paths (one per partition).
    ///
    /// The schema is inferred from the first file; all partitions must share
    /// the same schema (matching `pyarrow.ipc` convention).
    pub fn from_paths(paths: Vec<PathBuf>, metadata: serde_json::Value) -> Result<Self> {
        if paths.is_empty() {
            return Err(TiledError::Validation(
                "ArrowIpcAdapter requires at least one file path".into(),
            ));
        }
        let schema = infer_schema(&paths[0])?;
        let npartitions = paths.len();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let structure = TableStructure {
            arrow_schema: encode_schema(schema.as_ref()),
            npartitions,
            columns,
            resizable: Default::default(),
        };
        Ok(Self {
            paths,
            schema,
            structure,
            metadata,
            specs: vec![Spec::new("arrow")],
        })
    }

    /// Convenience constructor for the single-file case.
    pub fn from_path(path: PathBuf, metadata: serde_json::Value) -> Result<Self> {
        Self::from_paths(vec![path], metadata)
    }
}

impl BaseAdapter for ArrowIpcAdapter {
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

impl TableAdapterRead for ArrowIpcAdapter {
    fn structure(&self) -> &TableStructure {
        &self.structure
    }

    fn read<'a>(&'a self, fields: Option<&'a [String]>) -> BoxFuture<'a, Result<ArrowTable>> {
        let paths = self.paths.clone();
        let schema = self.schema.clone();
        let fields = fields.map(<[String]>::to_vec);
        Box::pin(async move {
            let all_batches = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
                let mut all = Vec::new();
                for path in &paths {
                    all.extend(read_arrow_ipc_file(path)?);
                }
                Ok(all)
            })
            .await
            .map_err(|e| TiledError::Internal(format!("arrow ipc spawn: {e}")))??;
            project(&schema, &all_batches, fields.as_deref())
        })
    }

    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>> {
        let npartitions = self.paths.len();
        if partition >= npartitions {
            return Box::pin(async move {
                Err(TiledError::Validation(format!(
                    "partition {partition} out of range ({npartitions} partitions)"
                )))
            });
        }
        let path = self.paths[partition].clone();
        let schema = self.schema.clone();
        let fields = fields.map(<[String]>::to_vec);
        Box::pin(async move {
            let batches = tokio::task::spawn_blocking(move || read_arrow_ipc_file(&path))
                .await
                .map_err(|e| TiledError::Internal(format!("arrow ipc spawn: {e}")))??;
            project(&schema, &batches, fields.as_deref())
        })
    }

    fn as_table_writable(&self) -> Option<&dyn TableAdapterWrite> {
        None
    }
}

/// Read all record batches from an Arrow IPC file.
fn read_arrow_ipc_file(path: &std::path::Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let reader = FileReader::try_new(std::io::BufReader::new(file), None)
        .map_err(|e| TiledError::Internal(format!("arrow ipc reader {}: {e}", path.display())))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| TiledError::Internal(format!("arrow ipc batch: {e}")))?);
    }
    Ok(batches)
}

/// Read only the schema from the first bytes of an Arrow IPC file (fast path:
/// `FileReader` reads the footer on construction, before any record batches).
fn infer_schema(path: &std::path::Path) -> Result<SchemaRef> {
    let file = std::fs::File::open(path)
        .map_err(|e| TiledError::Internal(format!("open {}: {e}", path.display())))?;
    let reader = FileReader::try_new(std::io::BufReader::new(file), None)
        .map_err(|e| TiledError::Internal(format!("arrow ipc reader {}: {e}", path.display())))?;
    Ok(reader.schema())
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
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use crate::core::adapters::{BaseAdapter, TableAdapterRead};

    use super::ArrowIpcAdapter;

    fn write_arrow_ipc(path: &std::path::Path, batches: &[RecordBatch], schema: Arc<Schema>) {
        let f = std::fs::File::create(path).unwrap();
        let mut w = FileWriter::try_new(f, &schema).unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
    }

    fn two_col_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Float64, false),
        ]))
    }

    fn make_batch(schema: Arc<Schema>, xs: Vec<i64>, ys: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(xs)),
                Arc::new(Float64Array::from(ys)),
            ],
        )
        .unwrap()
    }

    /// Single-file read: structure, full read, partition read, column projection.
    #[tokio::test]
    async fn single_file_read_and_project() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        let schema = two_col_schema();
        let batch = make_batch(schema.clone(), vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        write_arrow_ipc(&path, &[batch], schema);

        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        let s = adapter.structure();
        assert_eq!(s.npartitions, 1);
        assert_eq!(s.columns, vec!["x", "y"]);

        // Full read.
        let table = adapter.read(None).await.unwrap();
        assert_eq!(table.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        // Partition read.
        let part = adapter.read_partition(0, None).await.unwrap();
        assert_eq!(part.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

        // Column projection — only "y".
        let proj = adapter.read(Some(&["y".into()])).await.unwrap();
        assert_eq!(proj.schema.fields().len(), 1);
        assert_eq!(proj.schema.field(0).name(), "y");

        // Out-of-range partition.
        assert!(adapter.read_partition(1, None).await.is_err());
    }

    /// Multi-file (multi-partition): read() concatenates; read_partition() selects.
    #[tokio::test]
    async fn multi_partition_read() {
        let dir = tempfile::tempdir().unwrap();
        let schema = two_col_schema();
        let p0 = dir.path().join("p0.arrow");
        let p1 = dir.path().join("p1.arrow");
        write_arrow_ipc(
            &p0,
            &[make_batch(schema.clone(), vec![1, 2], vec![1.0, 2.0])],
            schema.clone(),
        );
        write_arrow_ipc(
            &p1,
            &[make_batch(
                schema.clone(),
                vec![3, 4, 5],
                vec![3.0, 4.0, 5.0],
            )],
            schema.clone(),
        );

        let adapter = ArrowIpcAdapter::from_paths(vec![p0, p1], serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().npartitions, 2);

        // read() yields 5 rows across both files.
        let full = adapter.read(None).await.unwrap();
        assert_eq!(full.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);

        // read_partition(0) → 2 rows; read_partition(1) → 3 rows.
        let r0 = adapter.read_partition(0, None).await.unwrap();
        assert_eq!(r0.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        let r1 = adapter.read_partition(1, None).await.unwrap();
        assert_eq!(r1.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    /// Empty Arrow IPC file: zero rows, correct columns, zero partitions still
    /// means partition 0 is out of range (consistent with parquet npartitions rule).
    #[tokio::test]
    async fn empty_file_reads_zero_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        // Write an empty IPC file (no batches).
        let f = std::fs::File::create(&path).unwrap();
        let mut w = FileWriter::try_new(f, &schema).unwrap();
        w.finish().unwrap();

        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(adapter.structure().npartitions, 1);
        assert_eq!(adapter.structure().columns, vec!["s"]);

        let table = adapter.read(None).await.unwrap();
        assert_eq!(table.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    /// from_paths with an empty list is a Validation error.
    #[test]
    fn empty_paths_is_error() {
        let err = ArrowIpcAdapter::from_paths(vec![], serde_json::json!({})).unwrap_err();
        assert!(
            matches!(err, crate::core::error::TiledError::Validation(_)),
            "empty paths must be a Validation error, got {err:?}"
        );
    }

    /// from_path on a missing file is an Internal error.
    #[test]
    fn missing_file_is_error() {
        let err = ArrowIpcAdapter::from_path(
            std::path::PathBuf::from("/does/not/exist.arrow"),
            serde_json::json!({}),
        )
        .unwrap_err();
        assert!(
            matches!(err, crate::core::error::TiledError::Internal(_)),
            "missing file must be an Internal error, got {err:?}"
        );
    }

    /// The adapter only returns string column names in the spec.
    #[test]
    fn spec_name_is_arrow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = FileWriter::try_new(f, &schema).unwrap();
        w.finish().unwrap();
        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert_eq!(adapter.specs()[0].name, "arrow");
    }

    /// as_table_writable returns None (read-only adapter).
    #[tokio::test]
    async fn is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ro.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = FileWriter::try_new(f, &schema).unwrap();
        w.finish().unwrap();
        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        assert!(
            adapter.as_table_writable().is_none(),
            "ArrowIpcAdapter is read-only; as_table_writable must be None"
        );
    }

    /// Feather v2 is Arrow IPC file format — same reader, same test.
    #[tokio::test]
    async fn feather_v2_is_arrow_ipc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.feather");
        let schema = two_col_schema();
        let batch = make_batch(schema.clone(), vec![10, 20], vec![1.5, 2.5]);
        // Write with Arrow IPC FileWriter — same on-disk format as
        // `pyarrow.feather.write_feather` v2 / `pyarrow.ipc.new_file`.
        write_arrow_ipc(&path, &[batch], schema);

        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        let table = adapter.read(None).await.unwrap();
        assert_eq!(table.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    }

    /// Projection of an unknown column name returns a Validation error.
    #[tokio::test]
    async fn unknown_column_projection_is_validation_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.arrow");
        let schema = two_col_schema();
        let batch = make_batch(schema.clone(), vec![1], vec![1.0]);
        write_arrow_ipc(&path, &[batch], schema);
        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        let err = adapter
            .read(Some(&["nonexistent".into()]))
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::core::error::TiledError::Validation(_)),
            "unknown column must be Validation error, got {err:?}"
        );
    }

    /// read_partition with a string column works (validates column type coverage).
    #[tokio::test]
    async fn string_column_partition_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("str.arrow");
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = FileWriter::try_new(f, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();

        let adapter = ArrowIpcAdapter::from_path(path, serde_json::json!({})).unwrap();
        let table = adapter.read_partition(0, None).await.unwrap();
        assert_eq!(table.batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        assert_eq!(table.schema.field(0).data_type(), &DataType::Utf8);
    }
}
