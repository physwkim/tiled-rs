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
        let npartitions = builder.metadata().num_row_groups().max(1);
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

    fn read_partition_inner(&self, partition: Option<usize>) -> Result<Vec<RecordBatch>> {
        let file = std::fs::File::open(&self.path)
            .map_err(|e| TiledError::Internal(format!("open {}: {e}", self.path.display())))?;
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
        Box::pin(async move {
            let batches = self.read_partition_inner(None)?;
            project(&self.schema, &batches, fields)
        })
    }
    fn read_partition<'a>(
        &'a self,
        partition: usize,
        fields: Option<&'a [String]>,
    ) -> BoxFuture<'a, Result<ArrowTable>> {
        Box::pin(async move {
            let batches = self.read_partition_inner(Some(partition))?;
            project(&self.schema, &batches, fields)
        })
    }
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
