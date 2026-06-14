//! CSV table adapter.
//!
//! Reads a CSV file into Arrow `RecordBatch`es using `arrow::csv::Reader`.
//! Schema is inferred from the first ~64 rows. Single partition only —
//! large multi-GB CSVs should use parquet/h5 instead.

#![cfg(feature = "csv-adapter")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::SchemaRef;

use tiled_core::adapters::{BaseAdapter, BoxFuture, TableAdapterRead};
use tiled_core::dtype::ArrowTable;
use tiled_core::error::{Result, TiledError};
use tiled_core::structures::{B64_ENCODED_PREFIX, Spec, StructureFamily, TableStructure};

pub struct CsvAdapter {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    structure: TableStructure,
    metadata: serde_json::Value,
    specs: Vec<Spec>,
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
        })
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
}
