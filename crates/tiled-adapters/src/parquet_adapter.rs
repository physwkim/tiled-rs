//! Parquet table adapter.
//!
//! Each Parquet row group becomes one partition so the existing
//! `/api/v1/table/partition/...` endpoint can stream them individually.

#![cfg(feature = "parquet")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use tiled_core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead};
use tiled_core::dtype::ArrowTable;
use tiled_core::error::{Result, TiledError};
use tiled_core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

pub struct ParquetAdapter {
    path: PathBuf,
    schema: SchemaRef,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
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
        })
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
    use tiled_core::error::TiledError;

    use super::ParquetAdapter;

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
}
